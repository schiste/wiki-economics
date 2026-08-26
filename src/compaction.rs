use anyhow::{Context, Result, ensure};
use polars::prelude::{
    DataFrame, ParallelStrategy, ParquetCompression, ParquetReader, ParquetWriter, SerReader,
    SortMultipleOptions,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsStr;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Component, Path, PathBuf};
use std::sync::{Mutex, OnceLock};

use crate::fingerprint::{self, StageSpec, TrackedPath};
use crate::storage::{self, GenerationFragment, GenerationLayer, GenerationManifest};

pub(crate) const COMPACTION_ALGORITHM_VERSION: &str =
    "metric-input-month-size-compaction-v1-sort-all-columns";
const COMPACTION_MANIFEST_SCHEMA_VERSION: u64 = 1;
const COMPACTION_TRANSACTION_SCHEMA_VERSION: u64 = 1;
const COMPACTION_MANIFEST_FILENAME: &str = "compaction-manifest.json";
const COMPACTION_TRANSACTION_FILENAME: &str = "compaction-transaction.json";
const COMPACTED_DIRNAME: &str = "_compacted";
const COMPACTION_STAGING_DIRNAME: &str = "_compaction-staging";
const TARGET_BYTES_ENV: &str = "WIKI_ECON_COMPACTION_TARGET_BYTES";
const MAXIMUM_BYTES_ENV: &str = "WIKI_ECON_COMPACTION_MAX_BYTES";
const MIB: u64 = 1_048_576;
const MINIMUM_TARGET_BYTES: u64 = 128 * MIB;
const DEFAULT_TARGET_BYTES: u64 = 192 * MIB;
const LARGEST_TARGET_BYTES: u64 = 256 * MIB;
const DEFAULT_MAXIMUM_BYTES: u64 = 512 * MIB;
const ROW_GROUP_ROWS: usize = 100_000;
type ManifestCacheKey = (PathBuf, String, String);
type ManifestCache = BTreeMap<ManifestCacheKey, CompactionManifest>;
static RECEIPTED_MANIFEST_CACHE: OnceLock<Mutex<ManifestCache>> = OnceLock::new();

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CompactionPolicy {
    pub(crate) target_bytes: u64,
    pub(crate) maximum_bytes: u64,
}

impl CompactionPolicy {
    pub(crate) fn from_environment() -> Result<Self> {
        Self::from_values(
            std::env::var_os(TARGET_BYTES_ENV).as_deref(),
            std::env::var_os(MAXIMUM_BYTES_ENV).as_deref(),
        )
    }

    fn from_values(target: Option<&OsStr>, maximum: Option<&OsStr>) -> Result<Self> {
        let parse = |name: &str, value: Option<&OsStr>, default| -> Result<u64> {
            value.map_or(Ok(default), |value| {
                value
                    .to_str()
                    .context(format!("{name} is not UTF-8"))?
                    .parse::<u64>()
                    .with_context(|| format!("{name} must be a positive byte count"))
            })
        };
        let policy = Self {
            target_bytes: parse(TARGET_BYTES_ENV, target, DEFAULT_TARGET_BYTES)?,
            maximum_bytes: parse(MAXIMUM_BYTES_ENV, maximum, DEFAULT_MAXIMUM_BYTES)?,
        };
        ensure!(
            (MINIMUM_TARGET_BYTES..=LARGEST_TARGET_BYTES).contains(&policy.target_bytes),
            "compaction target must be between 128 and 256 MiB"
        );
        ensure!(
            policy.maximum_bytes >= policy.target_bytes
                && policy.maximum_bytes <= DEFAULT_MAXIMUM_BYTES,
            "compaction maximum must be at least the target and no more than 512 MiB"
        );
        Ok(policy)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CompactionSource {
    pub(crate) source_id: String,
    pub(crate) marker_path: String,
    pub(crate) marker_sha256: String,
    pub(crate) rows: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CompactionManifest {
    pub(crate) schema_version: u64,
    pub(crate) wiki: String,
    pub(crate) snapshot_version: String,
    pub(crate) algorithm_version: String,
    pub(crate) ordering_contract: String,
    pub(crate) source_manifest_sha256: String,
    pub(crate) target_bytes: u64,
    pub(crate) maximum_bytes: u64,
    pub(crate) sources: Vec<CompactionSource>,
    pub(crate) source_fragments: Vec<GenerationFragment>,
    pub(crate) compacted_fragments: Vec<GenerationFragment>,
    pub(crate) source_rows: u64,
    pub(crate) compacted_rows: u64,
    pub(crate) source_bytes: u64,
    pub(crate) compacted_bytes: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct CompactionTransaction {
    schema_version: u64,
    wiki: String,
    snapshot_version: String,
    state: String,
    staging_relative: String,
    final_relative: String,
    manifest: CompactionManifest,
}

pub(crate) fn manifest_path(data_dir: &Path, wiki: &str, snapshot: &str) -> Result<PathBuf> {
    storage::validate_snapshot_version(snapshot)?;
    Ok(data_dir
        .join("snapshots")
        .join(wiki)
        .join(snapshot)
        .join(COMPACTION_MANIFEST_FILENAME))
}

fn transaction_path(data_dir: &Path, wiki: &str, snapshot: &str) -> Result<PathBuf> {
    Ok(manifest_path(data_dir, wiki, snapshot)?.with_file_name(COMPACTION_TRANSACTION_FILENAME))
}

fn metric_input_root(data_dir: &Path, wiki: &str, snapshot: &str) -> Result<PathBuf> {
    storage::snapshot_metric_input_wiki_dir(data_dir, wiki, snapshot)
}

fn path_string(path: &Path) -> Result<String> {
    ensure!(
        path.components()
            .all(|component| matches!(component, Component::Normal(_))),
        "compaction path is not a safe relative path"
    );
    path.to_str()
        .map(str::to_string)
        .context("compaction path is not UTF-8")
}

fn relative_path(data_dir: &Path, path: &Path) -> Result<String> {
    path_string(
        path.strip_prefix(data_dir)
            .context("compaction artifact is outside the data directory")?,
    )
}

fn checked_path(data_dir: &Path, relative: &str) -> Result<PathBuf> {
    let relative = Path::new(relative);
    ensure!(
        relative
            .components()
            .all(|component| matches!(component, Component::Normal(_))),
        "compaction manifest contains an unsafe path"
    );
    Ok(data_dir.join(relative))
}

fn atomic_json<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    let parent = path.parent().context("compaction JSON has no parent")?;
    fs::create_dir_all(parent)?;
    let filename = path
        .file_name()
        .context("compaction JSON has no filename")?
        .to_string_lossy();
    let temporary = parent.join(format!(".{filename}.{}.tmp", std::process::id()));
    let mut bytes = serde_json::to_vec_pretty(value)?;
    bytes.push(b'\n');
    let result = (|| -> Result<()> {
        let mut file = File::create(&temporary)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        fs::rename(&temporary, path)?;
        File::open(parent)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(temporary);
    }
    result
}

pub(crate) fn canonical_sha256<T: Serialize>(value: &T) -> Result<String> {
    use sha2::{Digest, Sha256};
    Ok(hex::encode(Sha256::digest(serde_json::to_vec(value)?)))
}

fn fragment_month(fragment: &GenerationFragment) -> Result<(i32, String)> {
    let path = Path::new(&fragment.path);
    let month = path
        .parent()
        .and_then(Path::file_name)
        .and_then(OsStr::to_str)
        .and_then(|value| value.strip_prefix("year_month="))
        .context("metric-input fragment has no month partition")?;
    let year = path
        .parent()
        .and_then(Path::parent)
        .and_then(Path::file_name)
        .and_then(OsStr::to_str)
        .and_then(|value| value.strip_prefix("year="))
        .and_then(|value| value.parse::<i32>().ok())
        .context("metric-input fragment has no year partition")?;
    ensure!(
        month.starts_with(&format!("{year:04}-")),
        "metric-input fragment year/month partition disagrees"
    );
    Ok((year, month.to_string()))
}

fn pack_fragment_groups(
    inputs: Vec<GenerationFragment>,
    target_bytes: u64,
) -> Result<Vec<Vec<GenerationFragment>>> {
    let mut packed = Vec::new();
    let mut group = Vec::new();
    let mut group_bytes = 0_u64;
    for input in inputs {
        if !group.is_empty()
            && group_bytes
                .checked_add(input.bytes)
                .context("compaction group byte count overflow")?
                > target_bytes
        {
            packed.push(std::mem::take(&mut group));
            group_bytes = 0;
        }
        group_bytes = group_bytes
            .checked_add(input.bytes)
            .context("compaction group byte count overflow")?;
        group.push(input);
    }
    if !group.is_empty() {
        packed.push(group);
    }
    Ok(packed)
}

fn read_fragment(path: &Path) -> Result<DataFrame> {
    ParquetReader::new(File::open(path)?)
        .set_low_memory(true)
        .read_parallel(ParallelStrategy::None)
        .finish()
        .with_context(|| format!("failed to read compaction input {}", path.display()))
}

fn append_frame(target: &mut Option<DataFrame>, frame: DataFrame) -> Result<()> {
    match target {
        Some(target) => {
            target.vstack_mut(&frame)?;
        }
        None => *target = Some(frame),
    }
    Ok(())
}

struct CompactGroupContext<'a> {
    data_dir: &'a Path,
    staging_root: &'a Path,
    final_root: &'a Path,
    year: i32,
    month: &'a str,
    maximum_bytes: u64,
}

fn compact_group(
    context: &CompactGroupContext<'_>,
    shard: usize,
    inputs: &[GenerationFragment],
) -> Result<GenerationFragment> {
    let expected_rows = inputs.iter().try_fold(0_u64, |total, fragment| {
        total
            .checked_add(fragment.rows)
            .context("compaction input row count overflow")
    })?;
    let mut frame = None;
    for input in inputs {
        let path = checked_path(context.data_dir, &input.path)?;
        append_frame(&mut frame, read_fragment(&path)?)?;
        storage::discard_path_cache(&path);
    }
    let mut frame = frame.context("compaction group has no input frame")?;
    ensure!(
        u64::try_from(frame.height())? == expected_rows,
        "compaction input materialization lost rows"
    );
    let timestamps = frame.column("event_timestamp")?.str()?;
    ensure!(
        timestamps
            .iter()
            .all(|value| value.is_some_and(|value| value.starts_with(context.month))),
        "compaction input escaped its event-month partition"
    );
    let sort_options = SortMultipleOptions::default().with_maintain_order(true);
    let sorted = frame.sort(
        crate::schema::METRIC_INPUT_COLUMNS.iter().copied(),
        sort_options,
    );
    frame = sorted?;

    let relative = PathBuf::from(format!("year={:04}", context.year))
        .join(format!("year_month={}", context.month))
        .join(format!(
            "compacted-{}.part-{shard:05}.parquet",
            context.month
        ));
    let staging = context.staging_root.join(&relative);
    staging.parent().map(fs::create_dir_all).transpose()?;
    let mut file = File::create(&staging)?;
    ParquetWriter::new(&mut file)
        .with_compression(ParquetCompression::Zstd(None))
        .with_row_group_size(Some(ROW_GROUP_ROWS))
        .set_parallel(false)
        .finish(&mut frame)?;
    file.sync_all()?;
    let rows = ParquetReader::new(File::open(&staging)?).num_rows()?;
    ensure!(
        u64::try_from(rows)? == expected_rows,
        "compacted Parquet footer row count disagrees"
    );
    let (bytes, sha256) = storage::sha256_file(&staging)?;
    ensure!(
        bytes <= context.maximum_bytes,
        "compacted shard {}/{shard} is {bytes} bytes, above the configured maximum {}; lower the target size",
        context.month,
        context.maximum_bytes
    );
    Ok(GenerationFragment {
        layer: GenerationLayer::MetricInput,
        source_id: format!("compacted-{}", context.month),
        path: relative_path(context.data_dir, &context.final_root.join(relative))?,
        rows: expected_rows,
        bytes,
        sha256,
    })
}

fn source_proofs(
    data_dir: &Path,
    wiki: &str,
    snapshot: &str,
    source_manifest: &GenerationManifest,
) -> Result<Vec<CompactionSource>> {
    let (plan, _) = crate::snapshot_plan::SnapshotPlan::load_or_resolve(data_dir, wiki, snapshot)?;
    let analytical = storage::snapshot_analytical_wiki_dir(data_dir, wiki, snapshot)?;
    let mut rows_by_source = BTreeMap::new();
    for fragment in &source_manifest.fragments {
        let rows = rows_by_source
            .entry(fragment.source_id.as_str())
            .or_insert(0_u64);
        *rows = rows
            .checked_add(fragment.rows)
            .context("source fragment row count overflow")?;
    }
    let mut sources = Vec::with_capacity(plan.sources.len());
    for source in plan.sources {
        ensure!(
            storage::marker_manifest_is_valid_in(data_dir, &analytical, &source.source_id)?,
            "source {} lost its strict marker before compaction",
            source.source_id
        );
        let marker = storage::marker_path_in(&analytical, &source.source_id);
        let (_, marker_sha256) = storage::sha256_file(&marker)?;
        let marker_rows =
            storage::read_marker_manifest_in(data_dir, &analytical, &source.source_id)?
                .context("validated source marker disappeared")?
                .rows;
        let rows = rows_by_source
            .get(source.source_id.as_str())
            .copied()
            .unwrap_or(0);
        ensure!(
            rows == u64::try_from(marker_rows)?,
            "source marker rows disagree with the source fragment allowlist"
        );
        sources.push(CompactionSource {
            source_id: source.source_id,
            marker_path: relative_path(data_dir, &marker)?,
            marker_sha256,
            rows,
        });
    }
    Ok(sources)
}

pub(crate) fn validate_structure(
    data_dir: &Path,
    wiki: &str,
    snapshot: &str,
    manifest: &CompactionManifest,
) -> Result<()> {
    ensure!(
        manifest.schema_version == COMPACTION_MANIFEST_SCHEMA_VERSION
            && manifest.wiki == wiki
            && manifest.snapshot_version == snapshot
            && manifest.algorithm_version == COMPACTION_ALGORITHM_VERSION,
        "compaction manifest identity or algorithm mismatch"
    );
    CompactionPolicy {
        target_bytes: manifest.target_bytes,
        maximum_bytes: manifest.maximum_bytes,
    }
    .validate()?;
    ensure!(
        manifest.ordering_contract == "event-month-then-all-metric-input-columns-v1",
        "compaction ordering contract mismatch"
    );
    ensure!(
        !manifest.sources.is_empty()
            && !manifest.source_fragments.is_empty()
            && !manifest.compacted_fragments.is_empty(),
        "compaction manifest has an empty proof set"
    );
    ensure!(
        manifest
            .sources
            .windows(2)
            .all(|pair| pair[0].source_id < pair[1].source_id)
            && manifest
                .source_fragments
                .windows(2)
                .all(|pair| pair[0] < pair[1])
            && manifest
                .compacted_fragments
                .windows(2)
                .all(|pair| pair[0] < pair[1]),
        "compaction proof sets are not uniquely sorted"
    );
    let (plan, _) = crate::snapshot_plan::SnapshotPlan::load_or_resolve(data_dir, wiki, snapshot)?;
    let expected_sources: BTreeSet<_> = plan
        .sources
        .into_iter()
        .map(|source| source.source_id)
        .collect();
    let actual_sources: BTreeSet<_> = manifest
        .sources
        .iter()
        .map(|source| source.source_id.clone())
        .collect();
    ensure!(
        actual_sources == expected_sources && actual_sources.len() == manifest.sources.len(),
        "compaction source proof does not exactly cover the snapshot plan"
    );
    ensure!(
        manifest.source_fragments.iter().all(|fragment| {
            fragment.layer == GenerationLayer::MetricInput
                && actual_sources.contains(&fragment.source_id)
        }),
        "compaction source fragment is not owned by a planned source"
    );
    ensure!(
        manifest.compacted_fragments.iter().all(|fragment| {
            fragment.layer == GenerationLayer::MetricInput
                && fragment.source_id.starts_with("compacted-")
                && fragment.path.contains(&format!("/{COMPACTED_DIRNAME}/"))
                && fragment.bytes <= manifest.maximum_bytes
        }),
        "compaction output violates its storage contract"
    );
    let sum = |fragments: &[GenerationFragment], rows: bool| -> Result<u64> {
        fragments.iter().try_fold(0_u64, |total, fragment| {
            total
                .checked_add(if rows { fragment.rows } else { fragment.bytes })
                .context("compaction conservation total overflow")
        })
    };
    ensure!(
        sum(&manifest.source_fragments, true)? == manifest.source_rows
            && sum(&manifest.compacted_fragments, true)? == manifest.compacted_rows
            && manifest.source_rows == manifest.compacted_rows
            && sum(&manifest.source_fragments, false)? == manifest.source_bytes
            && sum(&manifest.compacted_fragments, false)? == manifest.compacted_bytes,
        "compaction conservation totals disagree"
    );
    for source in &manifest.sources {
        let marker = checked_path(data_dir, &source.marker_path)?;
        let expected_marker = storage::marker_path_in(
            &storage::snapshot_analytical_wiki_dir(data_dir, wiki, snapshot)?,
            &source.source_id,
        );
        ensure!(
            source.marker_sha256.len() == 64
                && source
                    .marker_sha256
                    .bytes()
                    .all(|byte| byte.is_ascii_hexdigit())
                && marker == expected_marker
                && marker.is_file(),
            "compaction source marker proof is invalid"
        );
    }
    Ok(())
}

fn validate_source_marker_hashes(data_dir: &Path, manifest: &CompactionManifest) -> Result<()> {
    for source in &manifest.sources {
        let marker = checked_path(data_dir, &source.marker_path)?;
        let (_, marker_sha256) = storage::sha256_file(&marker)?;
        ensure!(
            marker_sha256 == source.marker_sha256,
            "compaction source marker hash changed"
        );
    }
    Ok(())
}

impl CompactionPolicy {
    fn validate(self) -> Result<()> {
        Self::from_values(
            Some(OsStr::new(&self.target_bytes.to_string())),
            Some(OsStr::new(&self.maximum_bytes.to_string())),
        )
        .map(|_| ())
    }
}

fn compacted_relative(fragment: &GenerationFragment) -> Result<PathBuf> {
    let path = Path::new(&fragment.path);
    let components: Vec<_> = path.components().collect();
    let compacted = components
        .iter()
        .position(|component| component.as_os_str() == COMPACTED_DIRNAME)
        .context("compaction output path has no compacted directory")?;
    ensure!(
        compacted + 4 == components.len(),
        "compaction output has an invalid partition path"
    );
    Ok(components[compacted + 1..].iter().collect())
}

fn validate_outputs_at(root: &Path, manifest: &CompactionManifest) -> Result<()> {
    for fragment in &manifest.compacted_fragments {
        let path = root.join(compacted_relative(fragment)?);
        let metadata = fs::metadata(&path)
            .with_context(|| format!("compacted fragment is missing: {}", path.display()))?;
        ensure!(
            metadata.is_file() && metadata.len() == fragment.bytes,
            "compacted fragment size changed"
        );
        let rows = ParquetReader::new(File::open(&path)?).num_rows()?;
        ensure!(
            u64::try_from(rows)? == fragment.rows,
            "compacted fragment row count changed"
        );
        let (_, sha256) = storage::sha256_file(&path)?;
        ensure!(sha256 == fragment.sha256, "compacted fragment hash changed");
    }
    Ok(())
}

pub(crate) fn read_manifest(
    data_dir: &Path,
    wiki: &str,
    snapshot: &str,
) -> Result<CompactionManifest> {
    let path = manifest_path(data_dir, wiki, snapshot)?;
    let manifest: CompactionManifest = serde_json::from_slice(&fs::read(&path)?)
        .with_context(|| format!("invalid compaction manifest JSON in {}", path.display()))?;
    validate_structure(data_dir, wiki, snapshot, &manifest)?;
    validate_source_marker_hashes(data_dir, &manifest)?;
    let compacted_root = metric_input_root(data_dir, wiki, snapshot)?.join(COMPACTED_DIRNAME);
    validate_outputs_at(&compacted_root, &manifest)?;
    Ok(manifest)
}

fn recover_prepared_transaction(
    data_dir: &Path,
    wiki: &str,
    snapshot: &str,
) -> Result<Option<CompactionManifest>> {
    let transaction_path = transaction_path(data_dir, wiki, snapshot)?;
    if !transaction_path.is_file() {
        return Ok(None);
    }
    let transaction: CompactionTransaction = serde_json::from_slice(&fs::read(&transaction_path)?)
        .context("invalid compaction transaction JSON")?;
    ensure!(
        transaction.schema_version == COMPACTION_TRANSACTION_SCHEMA_VERSION
            && transaction.wiki == wiki
            && transaction.snapshot_version == snapshot
            && transaction.state == "prepared",
        "compaction transaction identity or state mismatch"
    );
    validate_structure(data_dir, wiki, snapshot, &transaction.manifest)?;
    validate_source_marker_hashes(data_dir, &transaction.manifest)?;
    let staging = checked_path(data_dir, &transaction.staging_relative)?;
    let final_root = checked_path(data_dir, &transaction.final_relative)?;
    ensure!(
        staging.is_dir() != final_root.is_dir(),
        "compaction transaction has ambiguous staging/final state"
    );
    if staging.is_dir() {
        validate_outputs_at(&staging, &transaction.manifest)?;
        fs::rename(&staging, &final_root)?;
        let final_parent = final_root
            .parent()
            .expect("validated compacted root must have a parent");
        File::open(final_parent)?.sync_all()?;
    } else {
        validate_outputs_at(&final_root, &transaction.manifest)?;
    }
    let committed_manifest = manifest_path(data_dir, wiki, snapshot)?;
    atomic_json(&committed_manifest, &transaction.manifest)?;
    fs::remove_file(transaction_path)?;
    Ok(Some(transaction.manifest))
}

pub(crate) fn compact_generation(
    data_dir: &Path,
    wiki: &str,
    snapshot: &str,
    source_manifest: &GenerationManifest,
) -> Result<CompactionManifest> {
    compact_generation_with_fault(
        data_dir,
        wiki,
        snapshot,
        source_manifest,
        CompactionFault::None,
    )
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CompactionFault {
    None,
    #[cfg(test)]
    AfterPrepared,
    #[cfg(test)]
    AfterRename,
}

#[cfg(test)]
pub(crate) fn compact_generation_for_test(
    data_dir: &Path,
    wiki: &str,
    snapshot: &str,
    source_manifest: &GenerationManifest,
    after_rename: bool,
) -> Result<CompactionManifest> {
    compact_generation_with_fault(
        data_dir,
        wiki,
        snapshot,
        source_manifest,
        if after_rename {
            CompactionFault::AfterRename
        } else {
            CompactionFault::AfterPrepared
        },
    )
}

fn compact_generation_with_fault(
    data_dir: &Path,
    wiki: &str,
    snapshot: &str,
    source_manifest: &GenerationManifest,
    fault: CompactionFault,
) -> Result<CompactionManifest> {
    let _ = fault;
    if manifest_path(data_dir, wiki, snapshot)?.is_file() {
        return read_manifest(data_dir, wiki, snapshot);
    }
    if let Some(recovered) = recover_prepared_transaction(data_dir, wiki, snapshot)? {
        return Ok(recovered);
    }
    ensure!(
        source_manifest.schema_version == 2
            && source_manifest
                .fragments
                .iter()
                .all(|fragment| fragment.layer == GenerationLayer::MetricInput),
        "only a source-fragment schema-v2 metric-input generation can be compacted"
    );
    let policy = CompactionPolicy::from_environment()?;
    let root = metric_input_root(data_dir, wiki, snapshot)?;
    let staging = root.join(COMPACTION_STAGING_DIRNAME);
    let final_root = root.join(COMPACTED_DIRNAME);
    ensure!(
        !staging.exists() && !final_root.exists(),
        "unowned compaction artifacts require quarantine before retry"
    );
    fs::create_dir_all(&staging)?;

    let result = (|| -> Result<CompactionManifest> {
        let mut by_month: BTreeMap<(i32, String), Vec<GenerationFragment>> = BTreeMap::new();
        for fragment in &source_manifest.fragments {
            by_month
                .entry(fragment_month(fragment)?)
                .or_default()
                .push(fragment.clone());
        }
        let mut compacted = Vec::new();
        for ((year, month), inputs) in by_month {
            let context = CompactGroupContext {
                data_dir,
                staging_root: &staging,
                final_root: &final_root,
                year,
                month: &month,
                maximum_bytes: policy.maximum_bytes,
            };
            for (shard, group) in pack_fragment_groups(inputs, policy.target_bytes)?
                .into_iter()
                .enumerate()
            {
                let output = compact_group(&context, shard, &group)?;
                compacted.push(output);
            }
        }
        compacted.sort();
        let mut source_fragments = source_manifest.fragments.clone();
        source_fragments.sort();
        let sources = source_proofs(data_dir, wiki, snapshot, source_manifest)?;
        let sum = |fragments: &[GenerationFragment], rows: bool| -> Result<u64> {
            fragments.iter().try_fold(0_u64, |total, fragment| {
                total
                    .checked_add(if rows { fragment.rows } else { fragment.bytes })
                    .context("compaction total overflow")
            })
        };
        let manifest = CompactionManifest {
            schema_version: COMPACTION_MANIFEST_SCHEMA_VERSION,
            wiki: wiki.to_string(),
            snapshot_version: snapshot.to_string(),
            algorithm_version: COMPACTION_ALGORITHM_VERSION.to_string(),
            ordering_contract: "event-month-then-all-metric-input-columns-v1".to_string(),
            source_manifest_sha256: canonical_sha256(source_manifest)?,
            target_bytes: policy.target_bytes,
            maximum_bytes: policy.maximum_bytes,
            sources,
            source_rows: sum(&source_fragments, true)?,
            compacted_rows: sum(&compacted, true)?,
            source_bytes: sum(&source_fragments, false)?,
            compacted_bytes: sum(&compacted, false)?,
            source_fragments,
            compacted_fragments: compacted,
        };
        validate_structure(data_dir, wiki, snapshot, &manifest)?;
        validate_outputs_at(&staging, &manifest)?;
        let transaction = CompactionTransaction {
            schema_version: COMPACTION_TRANSACTION_SCHEMA_VERSION,
            wiki: wiki.to_string(),
            snapshot_version: snapshot.to_string(),
            state: "prepared".to_string(),
            staging_relative: relative_path(data_dir, &staging)?,
            final_relative: relative_path(data_dir, &final_root)?,
            manifest: manifest.clone(),
        };
        atomic_json(&transaction_path(data_dir, wiki, snapshot)?, &transaction)?;
        #[cfg(test)]
        ensure!(
            fault != CompactionFault::AfterPrepared,
            "injected fault after compaction transaction preparation"
        );
        fs::rename(&staging, &final_root)?;
        File::open(root.as_path())?.sync_all()?;
        #[cfg(test)]
        ensure!(
            fault != CompactionFault::AfterRename,
            "injected fault after compaction rename"
        );
        atomic_json(&manifest_path(data_dir, wiki, snapshot)?, &manifest)?;
        fs::remove_file(transaction_path(data_dir, wiki, snapshot)?)?;
        Ok(manifest)
    })();
    if result.is_err() && staging.is_dir() && !transaction_path(data_dir, wiki, snapshot)?.is_file()
    {
        let _ = fs::remove_dir_all(staging);
    }
    result
}

pub(crate) fn receipted_manifest(
    data_dir: &Path,
    wiki: &str,
    snapshot: &str,
) -> Result<Option<CompactionManifest>> {
    let cache_key = (
        data_dir.to_path_buf(),
        wiki.to_string(),
        snapshot.to_string(),
    );
    if let Some(manifest) = RECEIPTED_MANIFEST_CACHE
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .expect("compaction manifest cache lock poisoned")
        .get(&cache_key)
        .cloned()
    {
        return Ok(Some(manifest));
    }
    let receipt_path = fingerprint::data_stage_receipt_path(data_dir, wiki, snapshot, "ingest");
    let spec = StageSpec {
        stage: "ingest",
        scope: wiki,
        selected_snapshot: Some(snapshot),
        algorithm_version: crate::ingest::INGEST_ALGORITHM_VERSION,
    };
    let Some(receipt) = fingerprint::validated_receipt(&receipt_path, spec)? else {
        return Ok(None);
    };
    let compaction_path = manifest_path(data_dir, wiki, snapshot)?;
    let generation_path = storage::generation_manifest_path(data_dir, wiki, snapshot)?;
    for (identity, path) in [
        ("generation-manifest", generation_path),
        ("compaction-manifest", compaction_path.clone()),
    ] {
        let Some(recorded) = receipt
            .outputs
            .iter()
            .find(|recorded| recorded.identity == identity)
        else {
            return Ok(None);
        };
        let tracked = TrackedPath::new(identity, path);
        if !fingerprint::artifact_matches(recorded, &tracked)? {
            return Ok(None);
        }
    }
    let manifest: CompactionManifest = serde_json::from_slice(&fs::read(compaction_path)?)?;
    validate_structure(data_dir, wiki, snapshot, &manifest)?;
    for fragment in &manifest.compacted_fragments {
        let metadata = fs::metadata(checked_path(data_dir, &fragment.path)?)?;
        ensure!(
            metadata.is_file() && metadata.len() == fragment.bytes,
            "receipted compacted fragment size changed"
        );
    }
    RECEIPTED_MANIFEST_CACHE
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .expect("compaction manifest cache lock poisoned")
        .insert(cache_key, manifest.clone());
    Ok(Some(manifest))
}

pub(crate) fn source_is_represented(
    data_dir: &Path,
    wiki: &str,
    snapshot: &str,
    source_id: &str,
) -> Result<bool> {
    let analytical = storage::snapshot_analytical_wiki_dir(data_dir, wiki, snapshot)?;
    if storage::marker_manifest_is_valid_in(data_dir, &analytical, source_id)? {
        return Ok(true);
    }
    Ok(
        receipted_manifest(data_dir, wiki, snapshot)?.is_some_and(|manifest| {
            manifest
                .sources
                .iter()
                .any(|source| source.source_id == source_id)
        }),
    )
}

#[cfg(test)]
pub(crate) fn clear_manifest_cache_for_test() {
    RECEIPTED_MANIFEST_CACHE
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .expect("compaction manifest cache lock poisoned")
        .clear();
}

pub(crate) fn retire_source_fragments(
    data_dir: &Path,
    wiki: &str,
    snapshot: &str,
) -> Result<usize> {
    let Some(manifest) = receipted_manifest(data_dir, wiki, snapshot)? else {
        return Ok(0);
    };
    let compacted_root = metric_input_root(data_dir, wiki, snapshot)?.join(COMPACTED_DIRNAME);
    validate_outputs_at(&compacted_root, &manifest)?;
    let mut removed = 0_usize;
    let mut parents = BTreeSet::new();
    for fragment in manifest.source_fragments {
        let path = checked_path(data_dir, &fragment.path)?;
        if path.is_file() {
            parents.extend(path.ancestors().skip(1).take(2).map(Path::to_path_buf));
            fs::remove_file(path)?;
            removed = removed.saturating_add(1);
        }
    }
    for parent in parents.into_iter().rev() {
        if parent.is_dir() && fs::read_dir(&parent)?.next().is_none() {
            fs::remove_dir(&parent)?;
        }
    }
    Ok(removed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::TestDir;

    #[test]
    fn policy_enforces_the_qualified_size_window() -> Result<()> {
        assert_eq!(
            CompactionPolicy::from_values(None, None)?,
            CompactionPolicy {
                target_bytes: DEFAULT_TARGET_BYTES,
                maximum_bytes: DEFAULT_MAXIMUM_BYTES,
            }
        );
        assert!(CompactionPolicy::from_values(Some(OsStr::new("1")), None).is_err());
        assert!(
            CompactionPolicy::from_values(
                Some(OsStr::new(&(256 * MIB).to_string())),
                Some(OsStr::new(&(128 * MIB).to_string())),
            )
            .is_err()
        );
        assert!(CompactionPolicy::from_values(Some(OsStr::new("invalid")), None).is_err());
        Ok(())
    }

    #[test]
    fn helpers_reject_unsafe_paths_clean_failed_json_and_pack_by_size() -> Result<()> {
        assert!(path_string(Path::new("../escape")).is_err());
        assert!(checked_path(Path::new("data"), "../escape").is_err());
        assert!(relative_path(Path::new("data"), Path::new("outside")).is_err());

        let root = TestDir::new()?;
        let blocked = root.path().join("blocked.json");
        fs::create_dir(&blocked)?;
        assert!(atomic_json(&blocked, &BTreeMap::from([("value", 1_u64)])).is_err());
        assert_eq!(fs::read_dir(root.path())?.count(), 1);

        let fragment = |name: &str, bytes| GenerationFragment {
            layer: GenerationLayer::MetricInput,
            source_id: name.to_string(),
            path: format!("year=2026/year_month=2026-08/{name}.parquet"),
            rows: 1,
            bytes,
            sha256: "0".repeat(64),
        };
        let groups = pack_fragment_groups(
            vec![
                fragment("a", 100 * MIB),
                fragment("b", 100 * MIB),
                fragment("c", 50 * MIB),
            ],
            192 * MIB,
        )
        .expect("qualified fragment sizes should pack");
        assert_eq!(groups.iter().map(Vec::len).collect::<Vec<_>>(), [1, 2]);
        assert!(
            pack_fragment_groups(vec![fragment("overflow", u64::MAX)], 1)
                .and_then(|mut groups| {
                    groups[0].push(fragment("one", 1));
                    pack_fragment_groups(groups.remove(0), 1)
                })
                .is_err()
        );
        Ok(())
    }
}
