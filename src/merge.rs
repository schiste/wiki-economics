use anyhow::{Context, Result, ensure};
use polars::prelude::*;
use std::collections::BTreeMap;
use std::env;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::process::Command;
use tracing::{info, warn};

use crate::observability::MemorySnapshot;

const MERGE_BATCH_ROWS: usize = 250_000;

/// Merge per-wiki metric parquet files into combined files at the output root.
/// e.g., output/nlwiki/inequality.parquet + output/dewiki/inequality.parquet
///     → output/inequality.parquet (with wiki column distinguishing them)
pub fn merge_outputs(output_dir: &Path) -> Result<()> {
    info!(output_dir = %output_dir.display(), "merging wiki outputs");

    // Discover all metric names from subdirectories
    let mut metric_files: BTreeMap<String, Vec<PathBuf>> = BTreeMap::new();

    for entry in fs::read_dir(output_dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
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

    for (metric_name, mut paths) in metric_files {
        paths.sort();
        let dest = output_dir.join(&metric_name);
        merge_metric_batched(&metric_name, &paths, &dest, MERGE_BATCH_ROWS)?;
    }

    materialize_dashboard_artifacts(output_dir)?;

    info!(output_dir = %output_dir.display(), "finished merge");
    Ok(())
}

fn merge_metric_batched(
    metric_name: &str,
    paths: &[PathBuf],
    dest: &Path,
    batch_rows: usize,
) -> Result<()> {
    ensure!(
        !paths.is_empty(),
        "cannot merge {metric_name} without inputs"
    );
    ensure!(batch_rows > 0, "merge batch size must be positive");

    let temp_path = dest.with_file_name(format!(".{metric_name}.merge.tmp"));
    if temp_path.exists() {
        fs::remove_file(&temp_path).with_context(|| {
            format!(
                "failed to remove abandoned merge output {}",
                temp_path.display()
            )
        })?;
    }

    let merge_result = (|| -> Result<(usize, usize, u64)> {
        let first_file = File::open(&paths[0])?;
        let schema_frame = ParquetReader::new(first_file)
            .with_slice(Some((0, 0)))
            .finish()?;
        let columns = schema_frame.width();
        let output_file = File::create(&temp_path)?;
        let writer = ParquetWriter::new(output_file)
            .with_compression(ParquetCompression::Zstd(None))
            .with_row_group_size(Some(batch_rows))
            .set_parallel(false);
        let mut writer = writer.batched(schema_frame.schema())?;
        let mut total_rows = 0_usize;

        for (input_index, path) in paths.iter().enumerate() {
            let mut metadata_reader = ParquetReader::new(File::open(path)?);
            let input_rows = metadata_reader.num_rows()?;
            let mut offset = 0_usize;
            let mut batches = 0_usize;

            while offset < input_rows {
                let rows = (input_rows - offset).min(batch_rows);
                let batch = ParquetReader::new(File::open(path)?)
                    .with_slice(Some((offset, rows)))
                    .set_low_memory(true)
                    .read_parallel(ParallelStrategy::None)
                    .finish()?;
                ensure!(
                    batch.height() == rows,
                    "merge read {} rows from {} at offset {}, expected {}",
                    batch.height(),
                    path.display(),
                    offset,
                    rows
                );
                writer.write_batch(&batch)?;
                offset += rows;
                total_rows += rows;
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

        let bytes = writer.finish()?;
        drop(writer);
        File::open(&temp_path)?.sync_all()?;
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

fn materialize_dashboard_artifacts(output_dir: &Path) -> Result<()> {
    let generator_dir = env::var("WIKI_ECON_GENERATOR_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| Path::new("site").join("data-build"));
    materialize_dashboard_artifacts_from_dir(output_dir, &generator_dir)
}

// Dashboard artifact generation is best-effort: a single generator's input
// data being incomplete (e.g. a metric not yet computed for every wiki)
// must not block the metric-parquet merge that already succeeded. Failures
// are logged and that artifact is skipped rather than propagated.
fn materialize_dashboard_artifacts_from_dir(output_dir: &Path, generator_dir: &Path) -> Result<()> {
    materialize_dashboard_artifacts_with_runner(
        output_dir,
        generator_dir,
        |interpreter, script_path, output_dir| {
            Command::new(interpreter)
                .arg(script_path)
                .env("WIKI_ECON_OUTPUT_DIR", output_dir)
                .output()
        },
    )
}

fn materialize_dashboard_artifacts_with_runner<F>(
    output_dir: &Path,
    generator_dir: &Path,
    mut run: F,
) -> Result<()>
where
    F: FnMut(&str, &Path, &Path) -> std::io::Result<std::process::Output>,
{
    for script_name in [
        "defaults_business.json.cjs",
        "defaults_edit_variation.json.cjs",
        "defaults_gdp.json.cjs",
        "defaults_inequality.json.cjs",
        "defaults_labor.json.cjs",
        "defaults_patrol.json.cjs",
        "manifest.json.sh",
    ] {
        let script_path = generator_dir.join(script_name);
        if !script_path.is_file() {
            continue;
        }
        let interpreter = if script_name.ends_with(".cjs") {
            "node"
        } else {
            "bash"
        };
        let output = match run(interpreter, &script_path, output_dir) {
            Ok(output) => output,
            Err(err) => {
                warn!(script = %script_path.display(), error = %err, "failed to spawn dashboard artifact generator");
                continue;
            }
        };
        if !output.status.success() {
            warn!(
                script = %script_path.display(),
                stderr = %String::from_utf8_lossy(&output.stderr),
                "dashboard artifact generator failed"
            );
            continue;
        }
        let json_path =
            output_dir.join(script_name.trim_end_matches(".sh").trim_end_matches(".cjs"));
        fs::write(&json_path, output.stdout)?;
        info!(
            script = %script_path.display(),
            path = %json_path.display(),
            "materialized dashboard artifact"
        );
    }
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

    #[test]
    fn merge_outputs_combines_per_wiki_metrics() -> Result<()> {
        init_test_tracing();
        let output_dir = TestDir::new()?;
        write_metric(output_dir.path(), "enwiki", "metric", 1)?;
        write_metric(output_dir.path(), "frwiki", "metric", 2)?;

        merge_outputs(output_dir.path())?;

        let merged_path = output_dir.path().join("metric.parquet");
        let merged_path = merged_path.to_string_lossy().to_string();
        let merged =
            LazyFrame::scan_parquet(merged_path.as_str().into(), Default::default())?.collect()?;
        assert_eq!(merged.height(), 2);

        Ok(())
    }

    #[test]
    fn merge_metric_batched_preserves_deterministic_input_order() -> Result<()> {
        init_test_tracing();
        let output_dir = TestDir::new()?;
        write_metric(output_dir.path(), "frwiki", "metric", 2)?;
        let enwiki_dir = output_dir.path().join("enwiki");
        fs::create_dir_all(&enwiki_dir)?;
        let mut enwiki = df!(
            "wiki" => &["enwiki", "enwiki", "enwiki"],
            "value" => &[1_i64, 3, 5]
        )?;
        let mut enwiki_file = File::create(enwiki_dir.join("metric.parquet"))?;
        ParquetWriter::new(&mut enwiki_file).finish(&mut enwiki)?;
        let paths = vec![
            output_dir.path().join("enwiki/metric.parquet"),
            output_dir.path().join("frwiki/metric.parquet"),
        ];
        let dest = output_dir.path().join("metric.parquet");
        let abandoned = output_dir.path().join(".metric.parquet.merge.tmp");
        fs::write(&abandoned, b"abandoned")?;

        merge_metric_batched("metric.parquet", &paths, &dest, 1)?;

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

        let error = merge_metric_batched("metric.parquet", &paths, &dest, 1)
            .expect_err("a corrupt later input must fail the merge");
        assert!(!error.to_string().is_empty());
        assert_eq!(fs::read(&dest)?, b"known-good");
        assert!(!output_dir.path().join(".metric.parquet.merge.tmp").exists());
        Ok(())
    }

    #[test]
    fn merge_metric_batched_rejects_missing_inputs_and_zero_batch_size() {
        let dest = Path::new("unused.parquet");
        assert!(merge_metric_batched("metric.parquet", &[], dest, 1).is_err());
        assert!(
            merge_metric_batched("metric.parquet", &[PathBuf::from("unused")], dest, 0).is_err()
        );
    }

    #[test]
    fn merge_outputs_skips_underscore_prefixed_subdirectories() -> Result<()> {
        init_test_tracing();
        let output_dir = TestDir::new()?;
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

        merge_outputs(output_dir.path())?;

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
        fs::write(output_dir.path().join("README.txt"), b"not a wiki dir")?;
        let wiki_dir = output_dir.path().join("enwiki");
        fs::create_dir_all(&wiki_dir)?;
        fs::write(wiki_dir.join("notes.txt"), b"not parquet")?;

        merge_outputs(output_dir.path())?;

        assert!(!output_dir.path().join("notes.txt").exists());
        assert!(!output_dir.path().join("README.txt.parquet").exists());
        Ok(())
    }

    #[test]
    fn materialize_dashboard_artifacts_runs_json_generators() -> Result<()> {
        init_test_tracing();
        let output_dir = TestDir::new()?;
        let generator_dir = TestDir::new()?;
        let script = generator_dir.path().join("manifest.json.sh");
        fs::write(&script, "#!/bin/sh\nprintf '{\"ok\":true}'\n")?;

        materialize_dashboard_artifacts_from_dir(output_dir.path(), generator_dir.path())?;

        assert_eq!(
            fs::read_to_string(output_dir.path().join("manifest.json"))?,
            "{\"ok\":true}"
        );
        Ok(())
    }

    #[test]
    fn materialize_dashboard_artifacts_runs_node_generators() -> Result<()> {
        init_test_tracing();
        let output_dir = TestDir::new()?;
        let generator_dir = TestDir::new()?;
        let script = generator_dir.path().join("defaults_gdp.json.cjs");
        fs::write(&script, "process.stdout.write(JSON.stringify({ok:true}))\n")?;

        materialize_dashboard_artifacts_from_dir(output_dir.path(), generator_dir.path())?;

        assert_eq!(
            fs::read_to_string(output_dir.path().join("defaults_gdp.json"))?,
            "{\"ok\":true}"
        );
        Ok(())
    }

    #[test]
    fn materialize_dashboard_artifacts_skips_failed_generator_without_erroring() -> Result<()> {
        init_test_tracing();
        let output_dir = TestDir::new()?;
        let generator_dir = TestDir::new()?;
        let failing = generator_dir.path().join("manifest.json.sh");
        fs::write(&failing, "#!/bin/sh\nexit 1\n")?;
        let ok = generator_dir.path().join("defaults_gdp.json.cjs");
        fs::write(&ok, "process.stdout.write(JSON.stringify({ok:true}))\n")?;

        materialize_dashboard_artifacts_from_dir(output_dir.path(), generator_dir.path())?;

        assert!(!output_dir.path().join("manifest.json").exists());
        assert_eq!(
            fs::read_to_string(output_dir.path().join("defaults_gdp.json"))?,
            "{\"ok\":true}"
        );
        Ok(())
    }

    #[test]
    fn materialize_dashboard_artifacts_skips_spawn_failures() -> Result<()> {
        init_test_tracing();
        let output_dir = TestDir::new()?;
        let generator_dir = TestDir::new()?;
        fs::write(
            generator_dir.path().join("manifest.json.sh"),
            "#!/bin/sh\nprintf '{}'\n",
        )
        .expect("write generator fixture");
        let mut attempts = 0;

        materialize_dashboard_artifacts_with_runner(
            output_dir.path(),
            generator_dir.path(),
            |_, _, _| {
                attempts += 1;
                Err(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "injected missing interpreter",
                ))
            },
        )
        .expect("spawn failures are best-effort");

        assert_eq!(attempts, 1);
        assert!(!output_dir.path().join("manifest.json").exists());
        Ok(())
    }
}
