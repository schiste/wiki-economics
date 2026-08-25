use anyhow::{Context, Result, ensure};
use polars::prelude::*;
use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::process::Command;
use tracing::info;

use crate::fingerprint::{self, StageSpec, TrackedPath};
use crate::observability::MemorySnapshot;
use crate::storage;
use crate::wiki_lifecycle;

const MERGE_BATCH_ROWS: usize = 250_000;
const MERGE_ALGORITHM_VERSION: &str = "merged-metrics-v7-all-wikis-default";
const GENERATOR_DEPENDENCIES: [&str; 1] = ["manifest.json.cjs"];
const MANIFEST_GENERATOR: &str = "manifest.json.sh";
const PARTITION_ONLY_METRICS: [&str; 1] = ["page_weekly_edits.parquet"];

fn is_partition_only_metric(name: &str) -> bool {
    PARTITION_ONLY_METRICS.contains(&name)
}

/// Merge per-wiki metric parquet files into combined files at the output root.
/// e.g., output/nlwiki/inequality.parquet + output/dewiki/inequality.parquet
///     → output/inequality.parquet (with wiki column distinguishing them)
pub fn merge_outputs(output_dir: &Path, run_id: Option<&str>) -> Result<()> {
    let generator_dir = env::var("WIKI_ECON_GENERATOR_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| Path::new("site").join("data-build"));
    merge_outputs_from_dir(output_dir, &generator_dir, run_id)
}

fn merge_outputs_from_dir(
    output_dir: &Path,
    generator_dir: &Path,
    run_id: Option<&str>,
) -> Result<()> {
    info!(output_dir = %output_dir.display(), "merging wiki outputs");
    let lifecycle_path = env::var_os("WIKI_ECON_WIKI_LIFECYCLE_FILE").map(PathBuf::from);
    let published_wikis = wiki_lifecycle::published_wikis(lifecycle_path.as_deref())?;
    let metric_files = collect_metric_files(output_dir, published_wikis.as_ref())?;
    let mut artifact_names: Vec<String> = metric_files
        .iter()
        .flat_map(|(metric_name, paths)| {
            if is_partition_only_metric(metric_name) {
                paths
                    .iter()
                    .map(|path| {
                        path.strip_prefix(output_dir)
                            .map(|relative| relative.to_string_lossy().into_owned())
                            .context("partitioned metric is outside output directory")
                    })
                    .collect::<Vec<_>>()
            } else {
                vec![Ok(metric_name.clone())]
            }
        })
        .collect::<Result<Vec<_>>>()?;

    artifact_names.extend(
        crate::dashboard::ARTIFACTS
            .iter()
            .map(|name| (*name).to_string()),
    );
    artifact_names.push("manifest.json".to_string());
    artifact_names.push(crate::browser_data::INDEX_FILENAME.to_string());
    artifact_names.sort();
    let mut inputs = Vec::new();
    for paths in metric_files.values() {
        for path in paths {
            let wiki = path
                .parent()
                .and_then(Path::file_name)
                .and_then(|name| name.to_str())
                .context("metric input has no wiki directory")?;
            let filename = path
                .file_name()
                .and_then(|name| name.to_str())
                .context("metric input has no filename")?;
            inputs.push(TrackedPath::new(
                format!("wiki-output/{wiki}/{filename}"),
                path,
            ));
        }
    }
    inputs.push(TrackedPath::new(
        format!("generator/{MANIFEST_GENERATOR}"),
        generator_dir.join(MANIFEST_GENERATOR),
    ));
    for dependency in GENERATOR_DEPENDENCIES {
        inputs.push(TrackedPath::new(
            format!("generator/{dependency}"),
            generator_dir.join(dependency),
        ));
    }
    let lifecycle_file = lifecycle_path
        .clone()
        .unwrap_or_else(|| PathBuf::from("config/wiki-lifecycle.json"));
    inputs.push(TrackedPath::new(
        "config/wiki-lifecycle.json",
        lifecycle_file,
    ));
    let outputs: Vec<_> = metric_files
        .keys()
        .filter(|name| !is_partition_only_metric(name))
        .cloned()
        .chain(
            crate::dashboard::ARTIFACTS
                .iter()
                .map(|name| (*name).to_string()),
        )
        .chain([
            "manifest.json".to_string(),
            crate::browser_data::INDEX_FILENAME.to_string(),
        ])
        .map(|name| TrackedPath::new(format!("merged/{name}"), output_dir.join(name)))
        .collect();
    let compute_receipts = output_dir.join("_stages").join("compute");
    let mut snapshots = Vec::new();
    if compute_receipts.is_dir() {
        for entry in fs::read_dir(&compute_receipts)? {
            let path = entry?.path();
            if path
                .extension()
                .is_some_and(|extension| extension == "json")
                && let Ok(receipt) = fingerprint::read_receipt(&path)
                && published_wikis
                    .as_ref()
                    .is_none_or(|published| published.contains(&receipt.scope))
                && let Some(snapshot) = receipt.selected_snapshot
            {
                snapshots.push(format!("{}={snapshot}", receipt.scope));
            }
        }
    }
    snapshots.sort();
    let selected_snapshot = (!snapshots.is_empty()).then(|| snapshots.join(","));
    let receipt_path = output_dir.join("_stages").join("merge.json");
    let spec = StageSpec {
        stage: "merge",
        scope: "published-wikis",
        selected_snapshot: selected_snapshot.as_deref(),
        algorithm_version: MERGE_ALGORITHM_VERSION,
    };
    if fingerprint::reusable(&receipt_path, spec, &inputs, &outputs)? {
        crate::observability::record_stage_reused("merge", None);
        crate::publication::record_candidate(output_dir, run_id, &artifact_names)?;
        info!(
            receipt = %receipt_path.display(),
            "reusing deterministic merge stage"
        );
        return Ok(());
    }

    for (metric_name, mut paths) in metric_files {
        if is_partition_only_metric(&metric_name) {
            continue;
        }
        paths.sort();
        let dest = output_dir.join(&metric_name);
        merge_metric_batched(&metric_name, &paths, &dest, MERGE_BATCH_ROWS, run_id)?;
    }

    let obsolete_weekly = output_dir.join("page_weekly_edits.parquet");
    if obsolete_weekly.is_file() {
        fs::remove_file(&obsolete_weekly)?;
        File::open(output_dir)?.sync_all()?;
    }

    crate::dashboard::materialize(output_dir)?;
    crate::browser_data::materialize(output_dir, published_wikis.as_ref())?;
    materialize_manifest_from_dir(output_dir, generator_dir)?;
    fingerprint::record(&receipt_path, spec, &inputs, &outputs)?;
    crate::publication::record_candidate(output_dir, run_id, &artifact_names)?;

    info!(output_dir = %output_dir.display(), "finished merge");
    Ok(())
}

fn collect_metric_files(
    output_dir: &Path,
    published_wikis: Option<&BTreeSet<String>>,
) -> Result<BTreeMap<String, Vec<PathBuf>>> {
    let mut metric_files: BTreeMap<String, Vec<PathBuf>> = BTreeMap::new();

    for entry in fs::read_dir(output_dir)? {
        let entry = entry?;
        // Published per-wiki directories may be relative symlinks to immutable
        // candidates. Follow the target here; internal candidate roots remain
        // excluded by the leading-underscore guard below.
        if !entry.path().is_dir() {
            continue;
        }
        // Skip leading-underscore directories (markers, internal scratch). The
        // current layout never reads them but the filter is defensive against
        // future per-wiki sidecar dirs (e.g. `_patrol_parts`) being
        // accidentally treated as wiki output dirs and dragged into the merge
        // because their name happens to lack a leading underscore.
        if entry
            .file_name()
            .to_str()
            .is_some_and(|name| name.starts_with('_'))
        {
            continue;
        }
        let wiki = entry.file_name().to_string_lossy().to_string();
        if published_wikis.is_some_and(|published| !published.contains(&wiki)) {
            info!(wiki, "excluding non-published wiki from merged outputs");
            continue;
        }
        let wiki_dir = entry.path();
        for file_entry in fs::read_dir(&wiki_dir)? {
            let file_entry = file_entry?;
            let path = file_entry.path();
            if path.extension().is_some_and(|e| e == "parquet") {
                let metric_name = path.file_name().unwrap().to_string_lossy().to_string();
                metric_files.entry(metric_name).or_default().push(path);
            }
        }
    }
    Ok(metric_files)
}

pub(crate) fn merge_metric_batched(
    metric_name: &str,
    paths: &[PathBuf],
    dest: &Path,
    batch_rows: usize,
    run_id: Option<&str>,
) -> Result<()> {
    ensure!(
        !paths.is_empty(),
        "cannot merge {metric_name} without inputs"
    );
    ensure!(batch_rows > 0, "merge batch size must be positive");
    ensure!(
        paths.windows(2).all(|pair| pair[0] <= pair[1]),
        "merge inputs for {metric_name} are not in deterministic path order"
    );

    let run_id = run_id
        .map(str::to_string)
        .unwrap_or_else(|| format!("local-{}", std::process::id()));
    ensure!(
        !run_id.is_empty()
            && run_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.')),
        "unsafe merge run ID {run_id:?}"
    );
    let temp_path = dest.with_file_name(format!(".{metric_name}.merge.{run_id}.tmp"));
    if temp_path.exists() {
        fs::remove_file(&temp_path)
            .with_context(|| format!("failed to remove abandoned merge output {temp_path:?}"))?;
    }

    let merge_result = (|| -> Result<(usize, usize, u64)> {
        let first_reader = storage::SequentialParquetReader::new(&paths[0], None, batch_rows)?;
        let schema_frame = first_reader.schema_frame()?;
        drop(first_reader);
        let columns = schema_frame.width();
        let output_file = File::create(&temp_path)?;
        let writer = ParquetWriter::new(output_file)
            .with_compression(ParquetCompression::Zstd(None))
            .with_row_group_size(Some(batch_rows))
            .set_parallel(false);
        let mut writer = writer.batched(schema_frame.schema())?;
        let mut total_rows = 0_usize;
        let mut expected_rows = 0_usize;
        let mut last_wiki: Option<String> = None;

        for (input_index, path) in paths.iter().enumerate() {
            let mut reader = storage::SequentialParquetReader::new(path, None, batch_rows)?;
            let input_rows = reader.rows();
            expected_rows = expected_rows
                .checked_add(input_rows)
                .context("merged metric row count overflow")?;
            let mut batches = 0_usize;

            while let Some(batch) = reader.next_batch()? {
                validate_wiki_major_order(&batch, &mut last_wiki, path)?;
                writer.write_batch(&batch)?;
                total_rows = total_rows
                    .checked_add(batch.height())
                    .context("merged metric row count overflow")?;
                batches += 1;
            }

            let memory = MemorySnapshot::capture();
            info!(
                metric = metric_name,
                input = input_index + 1,
                total_inputs = paths.len(),
                path = %path.display(),
                input_rows,
                batches,
                total_rows,
                rss_bytes = ?memory.rss_bytes,
                cgroup_current_bytes = ?memory.cgroup_current_bytes,
                cgroup_peak_bytes = ?memory.cgroup_peak_bytes,
                cgroup_limit_bytes = ?memory.cgroup_limit_bytes,
                "merged metric input in bounded batches"
            );
        }
        ensure!(
            total_rows == expected_rows,
            "{metric_name} merge row conservation failed: expected {expected_rows}, wrote {total_rows}"
        );

        let bytes = writer.finish()?;
        drop(writer);
        File::open(&temp_path)?.sync_all()?;
        let mut output_reader = ParquetReader::new(File::open(&temp_path)?);
        let output_rows = output_reader.num_rows()?;
        ensure!(
            output_rows == total_rows,
            "{metric_name} output footer row count {output_rows} disagrees with {total_rows} written rows"
        );
        Ok((total_rows, columns, bytes))
    })();

    let (rows, columns, bytes) = match merge_result {
        Ok(summary) => summary,
        Err(error) => {
            let _ = fs::remove_file(&temp_path);
            return Err(error);
        }
    };
    fs::rename(&temp_path, dest)?;
    // The merged output can be larger than a gigabyte. It has already been
    // synced above, so retaining its clean pages in the cgroup cache only
    // steals headroom from the fail-closed publication validator that reads
    // it next. This is a best-effort Linux hint and never affects durability.
    storage::discard_path_cache(dest);

    let memory = MemorySnapshot::capture();
    info!(
        metric = metric_name,
        path = %dest.display(),
        wikis = paths.len(),
        rows,
        columns,
        bytes,
        batch_rows,
        rss_bytes = ?memory.rss_bytes,
        cgroup_current_bytes = ?memory.cgroup_current_bytes,
        cgroup_peak_bytes = ?memory.cgroup_peak_bytes,
        cgroup_limit_bytes = ?memory.cgroup_limit_bytes,
        "published merged metric output"
    );
    Ok(())
}

fn validate_wiki_major_order(
    batch: &DataFrame,
    last_wiki: &mut Option<String>,
    path: &Path,
) -> Result<()> {
    let wiki_column = anyhow::Context::with_context(batch.column("wiki"), || {
        format!("{} is missing wiki column", path.display())
    })?;
    let wikis = anyhow::Context::with_context(wiki_column.str(), || {
        format!("{} wiki column is not a string", path.display())
    })?;
    for wiki in wikis.iter() {
        let wiki = wiki.with_context(|| format!("{} contains a null wiki", path.display()))?;
        ensure!(
            last_wiki.as_deref().is_none_or(|previous| previous <= wiki),
            "{} violates deterministic wiki-major merge order: {:?} precedes {wiki:?}",
            path.display(),
            last_wiki
        );
        if last_wiki.as_deref() != Some(wiki) {
            *last_wiki = Some(wiki.to_owned());
        }
    }
    Ok(())
}

fn manifest_row_counts(output_dir: &Path) -> Result<BTreeMap<String, usize>> {
    let data_dir = env::var("WIKI_ECON_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("data"));
    let mut paths = Vec::new();
    for (root, names) in [
        (
            data_dir.join("patrol"),
            &["patrol.parquet", "rights.parquet"][..],
        ),
        (output_dir.to_path_buf(), &["patrol.parquet"][..]),
    ] {
        if !root.is_dir() {
            continue;
        }
        for entry in fs::read_dir(root)? {
            let directory = entry?.path();
            if !directory.is_dir() {
                continue;
            }
            for name in names {
                let path = directory.join(name);
                if path.is_file() {
                    paths.push(path);
                }
            }
        }
    }
    paths.sort();
    let mut counts = BTreeMap::new();
    for path in paths {
        let rows = ParquetReader::new(File::open(&path)?).num_rows()?;
        counts.insert(path.to_string_lossy().into_owned(), rows);
    }
    Ok(counts)
}

fn materialize_manifest_from_dir(output_dir: &Path, generator_dir: &Path) -> Result<()> {
    materialize_manifest_with_runner(output_dir, generator_dir, |script_path, counts_path| {
        Command::new("bash")
            .arg(script_path)
            .env("WIKI_ECON_OUTPUT_DIR", output_dir)
            .env("WIKI_ECON_PARQUET_ROW_COUNTS_FILE", counts_path)
            .output()
    })
}

fn materialize_manifest_with_runner<F>(
    output_dir: &Path,
    generator_dir: &Path,
    mut run: F,
) -> Result<()>
where
    F: FnMut(&Path, &Path) -> std::io::Result<std::process::Output>,
{
    let script_path = generator_dir.join(MANIFEST_GENERATOR);
    ensure!(
        script_path.is_file(),
        "required manifest generator is missing: {}",
        script_path.display()
    );
    let counts_path = output_dir.join(format!(".manifest-row-counts.{}.json", std::process::id()));
    fs::write(
        &counts_path,
        serde_json::to_vec(&manifest_row_counts(output_dir)?)?,
    )?;
    let run_result = run(&script_path, &counts_path);
    let _ = fs::remove_file(&counts_path);
    let output = run_result.with_context(|| {
        format!(
            "failed to spawn manifest generator {}",
            script_path.display()
        )
    })?;
    ensure!(
        output.status.success(),
        "manifest generator {} failed: {}",
        script_path.display(),
        String::from_utf8_lossy(&output.stderr).trim()
    );
    serde_json::from_slice::<serde_json::Value>(&output.stdout).with_context(|| {
        format!(
            "manifest generator {} emitted invalid JSON",
            script_path.display()
        )
    })?;
    let json_path = output_dir.join("manifest.json");
    let temp_path = output_dir.join(".manifest.json.generator.tmp");
    let write_result = (|| -> Result<()> {
        fs::write(&temp_path, output.stdout)?;
        File::open(&temp_path)?.sync_all()?;
        fs::rename(&temp_path, &json_path)?;
        Ok(())
    })();
    if let Err(error) = write_result {
        let _ = fs::remove_file(&temp_path);
        return Err(error)
            .with_context(|| format!("failed to publish manifest {}", json_path.display()));
    }
    info!(script = %script_path.display(), path = %json_path.display(), "materialized manifest");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{TestDir, init_test_tracing};

    fn write_metric(output_dir: &Path, wiki: &str, metric: &str, value: i64) -> Result<()> {
        let wiki_dir = output_dir.join(wiki);
        fs::create_dir_all(&wiki_dir)?;
        let path = wiki_dir.join(format!("{metric}.parquet"));
        let mut file = fs::File::create(path)?;
        let columns = vec![
            Column::new("wiki".into(), vec![wiki]),
            Column::new("value".into(), vec![value]),
        ];
        let mut df = DataFrame::new_infer_height(columns)?;
        ParquetWriter::new(&mut file).finish(&mut df)?;
        Ok(())
    }

    fn write_generators(generator_dir: &Path) -> Result<()> {
        fs::write(
            generator_dir.join(MANIFEST_GENERATOR),
            "#!/bin/sh\nprintf '{\"ok\":true}'\n",
        )
        .expect("manifest generator fixture should be writable");
        for dependency in GENERATOR_DEPENDENCIES {
            fs::write(
                generator_dir.join(dependency),
                "// fingerprint dependency\n",
            )
            .expect("generator dependency fixture should be writable");
        }
        Ok(())
    }

    #[test]
    fn manifest_row_counts_skips_non_directory_roots() -> Result<()> {
        let output = TestDir::new()?;
        let file = output.path().join("not-a-directory");
        fs::write(&file, "fixture")?;
        manifest_row_counts(&file)?;
        Ok(())
    }

    #[test]
    fn manifest_materialization_reports_row_count_staging_failures() -> Result<()> {
        let output = TestDir::new()?;
        let generators = TestDir::new()?;
        write_generators(generators.path())?;
        fs::create_dir(
            output
                .path()
                .join(format!(".manifest-row-counts.{}.json", std::process::id())),
        )
        .expect("the row-count staging collision fixture can be created");

        assert!(materialize_manifest_from_dir(output.path(), generators.path()).is_err());
        Ok(())
    }

    fn write_dashboard_fixture(output_dir: &Path) -> Result<()> {
        crate::dashboard::write_site_fixture(output_dir)
    }

    #[test]
    fn merge_outputs_combines_per_wiki_metrics() -> Result<()> {
        init_test_tracing();
        let output_dir = TestDir::new()?;
        let generator_dir = TestDir::new()?;
        write_generators(generator_dir.path())?;
        write_dashboard_fixture(output_dir.path())?;
        write_metric(output_dir.path(), "enwiki", "metric", 1)?;
        write_metric(output_dir.path(), "frwiki", "metric", 2)?;
        let obsolete_weekly = output_dir.path().join("page_weekly_edits.parquet");
        fs::write(obsolete_weekly, b"obsolete combined weekly artifact")?;

        merge_outputs_from_dir(output_dir.path(), generator_dir.path(), None)?;

        let merged_path = output_dir.path().join("metric.parquet");
        let merged_path = merged_path.to_string_lossy().to_string();
        let merged =
            LazyFrame::scan_parquet(merged_path.as_str().into(), Default::default())?.collect()?;
        assert_eq!(merged.height(), 2);
        assert!(
            !output_dir.path().join("page_weekly_edits.parquet").exists(),
            "weekly publication must remain per-wiki instead of consuming a redundant root artifact"
        );

        Ok(())
    }

    #[test]
    fn merge_reuses_content_but_reissues_candidate_for_current_run() -> Result<()> {
        init_test_tracing();
        let output_dir = TestDir::new()?;
        let generator_dir = TestDir::new()?;
        write_generators(generator_dir.path())?;
        write_dashboard_fixture(output_dir.path())?;
        write_metric(output_dir.path(), "testwiki", "metric", 1)?;
        let metric_input = output_dir.path().join("testwiki/metric.parquet");
        fingerprint::record(
            &output_dir.path().join("_stages/compute/testwiki.json"),
            StageSpec {
                stage: "compute",
                scope: "testwiki",
                selected_snapshot: Some("2026-08"),
                algorithm_version: "fixture-v1",
            },
            &[],
            &[TrackedPath::new(
                "output/testwiki/metric.parquet",
                metric_input,
            )],
        )
        .expect("compute fixture receipt should be recorded");
        crate::publication::begin_run(
            output_dir.path(),
            Some("run-one"),
            &["testwiki".to_string()],
            Some("2026-08"),
        )
        .expect("first publication run should begin");
        merge_outputs_from_dir(output_dir.path(), generator_dir.path(), Some("run-one"))?;
        let merged = output_dir.path().join("metric.parquet");
        let modified = fs::metadata(&merged)?.modified()?;

        crate::publication::begin_run(
            output_dir.path(),
            Some("run-two"),
            &["testwiki".to_string()],
            Some("2026-08"),
        )
        .expect("second publication run should begin");
        merge_outputs_from_dir(output_dir.path(), generator_dir.path(), Some("run-two"))?;

        assert_eq!(fs::metadata(merged)?.modified()?, modified);
        let candidate_bytes = fs::read(output_dir.path().join(".publication-candidate.json"))?;
        let candidate: serde_json::Value = serde_json::from_slice(&candidate_bytes)?;
        assert_eq!(candidate["run_id"], "run-two");
        Ok(())
    }

    #[test]
    fn metric_discovery_includes_only_published_lifecycle_entries() -> Result<()> {
        let output_dir = TestDir::new()?;
        write_metric(output_dir.path(), "nlwiki", "metric", 1)?;
        write_metric(output_dir.path(), "frwiki", "metric", 2)?;
        write_metric(output_dir.path(), "hiddenwiki", "metric", 3)?;
        let published = BTreeSet::from(["frwiki".to_string(), "nlwiki".to_string()]);

        let files = collect_metric_files(output_dir.path(), Some(&published))?;
        let metric = files.get("metric.parquet").expect("metric should exist");
        assert_eq!(metric.len(), 2);
        assert!(
            metric
                .iter()
                .any(|path| path.starts_with(output_dir.path().join("frwiki")))
        );
        assert!(
            metric
                .iter()
                .any(|path| path.starts_with(output_dir.path().join("nlwiki")))
        );
        assert!(
            !metric
                .iter()
                .any(|path| path.to_string_lossy().contains("hiddenwiki"))
        );
        Ok(())
    }

    #[test]
    fn merge_metric_batched_preserves_deterministic_input_order() -> Result<()> {
        init_test_tracing();
        let output_dir = TestDir::new()?;
        write_metric(output_dir.path(), "frwiki", "metric", 2)?;
        let enwiki_dir = output_dir.path().join("enwiki");
        fs::create_dir_all(&enwiki_dir)?;
        let wikis = Column::new("wiki".into(), &["enwiki", "enwiki", "enwiki"]);
        let values = Column::new("value".into(), &[1_i64, 3, 5]);
        let mut enwiki = DataFrame::new(3, vec![wikis, values])?;
        let mut enwiki_file = File::create(enwiki_dir.join("metric.parquet"))?;
        ParquetWriter::new(&mut enwiki_file).finish(&mut enwiki)?;
        let paths = vec![
            output_dir.path().join("enwiki/metric.parquet"),
            output_dir.path().join("frwiki/metric.parquet"),
        ];
        let dest = output_dir.path().join("metric.parquet");
        let abandoned = output_dir.path().join(".metric.parquet.merge.test-run.tmp");
        fs::write(&abandoned, b"abandoned")?;

        merge_metric_batched("metric.parquet", &paths, &dest, 1, Some("test-run"))?;

        let merged = ParquetReader::new(File::open(&dest)?).finish()?;
        assert_eq!(merged.height(), 4);
        let wikis = merged.column("wiki")?.str()?;
        assert_eq!(wikis.get(0), Some("enwiki"));
        assert_eq!(wikis.get(1), Some("enwiki"));
        assert_eq!(wikis.get(2), Some("enwiki"));
        assert_eq!(wikis.get(3), Some("frwiki"));
        assert_eq!(
            merged
                .column("value")?
                .i64()?
                .into_no_null_iter()
                .collect::<Vec<_>>(),
            vec![1, 3, 5, 2]
        );
        assert!(!abandoned.exists());
        Ok(())
    }

    #[test]
    fn merge_metric_batched_keeps_existing_output_on_failure() -> Result<()> {
        init_test_tracing();
        let output_dir = TestDir::new()?;
        write_metric(output_dir.path(), "enwiki", "metric", 1)?;
        let corrupt_dir = output_dir.path().join("frwiki");
        fs::create_dir_all(&corrupt_dir)?;
        let corrupt = corrupt_dir.join("metric.parquet");
        fs::write(&corrupt, b"not parquet")?;
        let dest = output_dir.path().join("metric.parquet");
        fs::write(&dest, b"known-good")?;
        let paths = vec![output_dir.path().join("enwiki/metric.parquet"), corrupt];

        let error = merge_metric_batched("metric.parquet", &paths, &dest, 1, Some("test-run"))
            .expect_err("a corrupt later input must fail the merge");
        assert!(!error.to_string().is_empty());
        assert_eq!(fs::read(&dest)?, b"known-good");
        assert!(
            !output_dir
                .path()
                .join(".metric.parquet.merge.test-run.tmp")
                .exists()
        );
        Ok(())
    }

    #[test]
    fn merge_metric_batched_reports_abandoned_temp_cleanup_failure() -> Result<()> {
        let output_dir = TestDir::new()?;
        write_metric(output_dir.path(), "enwiki", "metric", 1)?;
        let dest = output_dir.path().join("metric.parquet");
        let abandoned = output_dir.path().join(".metric.parquet.merge.test-run.tmp");
        fs::create_dir(&abandoned)?;

        let error = merge_metric_batched(
            "metric.parquet",
            &[output_dir.path().join("enwiki/metric.parquet")],
            &dest,
            1,
            Some("test-run"),
        )
        .expect_err("a directory cannot be removed as an abandoned output file");

        assert!(error.to_string().contains("failed to remove abandoned"));
        assert!(!dest.exists());
        Ok(())
    }

    #[test]
    fn merge_metric_batched_rejects_missing_inputs_and_zero_batch_size() {
        let dest = Path::new("unused.parquet");
        assert!(merge_metric_batched("metric.parquet", &[], dest, 1, None).is_err());
        assert!(
            merge_metric_batched("metric.parquet", &[PathBuf::from("unused")], dest, 0, None)
                .is_err()
        );
        assert!(
            merge_metric_batched(
                "metric.parquet",
                &[PathBuf::from("z.parquet"), PathBuf::from("a.parquet")],
                dest,
                1,
                None,
            )
            .is_err()
        );
    }

    #[test]
    fn merge_metric_batched_rejects_invalid_wiki_ordering_contracts() -> Result<()> {
        let directory = TestDir::new()?;
        let cases = vec![
            df!("value" => &[1_i64])?,
            df!("wiki" => &[1_i64], "value" => &[1_i64])?,
            df!("wiki" => &[None::<&str>], "value" => &[1_i64])?,
            df!("wiki" => &["nlwiki", "frwiki"], "value" => &[1_i64, 2])?,
        ];
        for (index, mut frame) in cases.into_iter().enumerate() {
            let source = directory.path().join(format!("case-{index}.parquet"));
            ParquetWriter::new(File::create(&source)?).finish(&mut frame)?;
            let destination = directory.path().join(format!("merged-{index}.parquet"));
            assert!(
                merge_metric_batched(
                    &format!("merged-{index}.parquet"),
                    &[source],
                    &destination,
                    1,
                    Some("invalid-wiki"),
                )
                .is_err()
            );
            assert!(!destination.exists());
        }
        Ok(())
    }

    #[test]
    fn merge_outputs_skips_underscore_prefixed_subdirectories() -> Result<()> {
        init_test_tracing();
        let output_dir = TestDir::new()?;
        let generator_dir = TestDir::new()?;
        write_generators(generator_dir.path())?;
        write_dashboard_fixture(output_dir.path())?;
        write_metric(output_dir.path(), "frwiki", "metric", 1)?;
        // Sidecar dir mimicking _patrol_parts at the wiki-output root level.
        // Without the underscore filter, merge would walk it and try to
        // concatenate its parquets into the merged output with a foreign
        // schema.
        let sidecar = output_dir.path().join("_internal");
        fs::create_dir_all(&sidecar)?;
        let mut sidecar_df =
            DataFrame::new_infer_height(vec![Column::new("unrelated_col".into(), vec!["x"])])?;
        let mut sidecar_file = fs::File::create(sidecar.join("strange.parquet"))?;
        ParquetWriter::new(&mut sidecar_file).finish(&mut sidecar_df)?;

        merge_outputs_from_dir(output_dir.path(), generator_dir.path(), None)?;

        let merged = output_dir.path().join("metric.parquet");
        let merged_path = merged.to_string_lossy().to_string();
        let df =
            LazyFrame::scan_parquet(merged_path.as_str().into(), Default::default())?.collect()?;
        // Only the legitimate wiki dir contributes rows.
        assert_eq!(df.height(), 1);
        // The sidecar's "strange" parquet must not have been promoted to the
        // root.
        assert!(!output_dir.path().join("strange.parquet").exists());
        Ok(())
    }

    #[test]
    fn merge_outputs_ignores_non_directory_entries_and_non_parquet_files() -> Result<()> {
        init_test_tracing();
        let output_dir = TestDir::new()?;
        let generator_dir = TestDir::new()?;
        write_generators(generator_dir.path())?;
        write_dashboard_fixture(output_dir.path())?;
        fs::write(output_dir.path().join("README.txt"), b"not a wiki dir")?;
        let wiki_dir = output_dir.path().join("enwiki");
        fs::create_dir_all(&wiki_dir)?;
        fs::write(wiki_dir.join("notes.txt"), b"not parquet")?;

        merge_outputs_from_dir(output_dir.path(), generator_dir.path(), None)?;

        assert!(!output_dir.path().join("notes.txt").exists());
        assert!(!output_dir.path().join("README.txt.parquet").exists());
        Ok(())
    }

    #[test]
    fn materialize_manifest_runs_generator() -> Result<()> {
        init_test_tracing();
        let output_dir = TestDir::new()?;
        let generator_dir = TestDir::new()?;
        write_generators(generator_dir.path())?;
        let script = generator_dir.path().join("manifest.json.sh");
        fs::write(&script, "#!/bin/sh\nprintf '{\"ok\":true}'\n")?;

        materialize_manifest_from_dir(output_dir.path(), generator_dir.path())?;

        assert_eq!(
            fs::read_to_string(output_dir.path().join("manifest.json"))?,
            "{\"ok\":true}"
        );
        Ok(())
    }

    #[test]
    fn materialize_manifest_fails_closed_and_preserves_previous_json() -> Result<()> {
        init_test_tracing();
        let output_dir = TestDir::new()?;
        let generator_dir = TestDir::new()?;
        write_generators(generator_dir.path())?;
        let failing = generator_dir.path().join(MANIFEST_GENERATOR);
        let failing_script = "#!/bin/sh\nprintf 'broken input' >&2\nexit 1\n";
        fs::write(&failing, failing_script)?;
        let previous = output_dir.path().join("manifest.json");
        fs::write(&previous, "{\"old\":true}")?;

        let error = materialize_manifest_from_dir(output_dir.path(), generator_dir.path())
            .expect_err("a critical generator failure must stop publication");

        assert!(error.to_string().contains("broken input"));
        assert_eq!(fs::read_to_string(previous)?, "{\"old\":true}");
        Ok(())
    }

    #[test]
    fn materialize_manifest_propagates_spawn_failures() -> Result<()> {
        init_test_tracing();
        let output_dir = TestDir::new()?;
        let generator_dir = TestDir::new()?;
        write_generators(generator_dir.path())?;
        let mut attempts = 0;

        let error =
            materialize_manifest_with_runner(output_dir.path(), generator_dir.path(), |_, _| {
                attempts += 1;
                Err(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "injected missing interpreter",
                ))
            })
            .expect_err("spawn failures must fail publication");

        assert_eq!(attempts, 1);
        assert!(error.to_string().contains("failed to spawn"));
        Ok(())
    }

    #[test]
    fn materialize_manifest_rejects_missing_and_invalid_generators() -> Result<()> {
        let output_dir = TestDir::new()?;
        let generator_dir = TestDir::new()?;
        let missing = materialize_manifest_from_dir(output_dir.path(), generator_dir.path())
            .expect_err("missing required generator must fail");
        assert!(missing.to_string().contains("required manifest"));

        write_generators(generator_dir.path())?;
        let invalid_script = generator_dir.path().join(MANIFEST_GENERATOR);
        fs::write(invalid_script, "#!/bin/sh\nprintf 'not json'\n")?;
        let invalid = materialize_manifest_from_dir(output_dir.path(), generator_dir.path())
            .expect_err("invalid JSON must fail");
        assert!(invalid.to_string().contains("invalid JSON"));
        assert!(!output_dir.path().join("manifest.json").exists());
        Ok(())
    }

    #[test]
    fn materialize_manifest_cleans_failed_atomic_write() -> Result<()> {
        let output_dir = TestDir::new()?;
        let generator_dir = TestDir::new()?;
        write_generators(generator_dir.path())?;
        let blocked_temp = output_dir.path().join(".manifest.json.generator.tmp");
        fs::create_dir(&blocked_temp)?;

        let error = materialize_manifest_from_dir(output_dir.path(), generator_dir.path())
            .expect_err("a blocked temporary path must fail");

        assert!(error.to_string().contains("failed to publish"));
        assert!(blocked_temp.is_dir());
        Ok(())
    }
}
