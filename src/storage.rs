use anyhow::{Context, Result, ensure};
use polars::io::parquet::metadata::FileMetadataRef;
use polars::prelude::{DataFrame, ParallelStrategy, ParquetReader, SerReader};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, SystemTime};

#[cfg(target_os = "linux")]
use std::num::NonZeroU64;

pub const ANALYTICAL_DIRNAME: &str = "parquet";
pub const WAREHOUSE_DIRNAME: &str = "warehouse";
pub const METRIC_INPUT_DIRNAME: &str = "metric-input";
const MARKERS_DIRNAME: &str = "_markers";
const SNAPSHOTS_DIRNAME: &str = "_snapshots";
const SNAPSHOT_STATE_DIRNAME: &str = "snapshots";
const CURRENT_SNAPSHOT_FILENAME: &str = "current-snapshot.json";
const GENERATION_MANIFEST_FILENAME: &str = "generation-manifest.json";
const SNAPSHOT_POINTER_SCHEMA_VERSION: u64 = 1;
const MARKER_SCHEMA_VERSION: u64 = 2;
const DIRECT_METRIC_INPUT_MANIFEST_SCHEMA_VERSION: u64 = 2;
const COMPACTED_METRIC_INPUT_MANIFEST_SCHEMA_VERSION: u64 = 3;
static GENERATION_MANIFEST_TEMP_NONCE: AtomicU64 = AtomicU64::new(0);
static GENERATION_VALIDATION_CACHE: OnceLock<
    Mutex<BTreeMap<GenerationValidationKey, GenerationManifest>>,
> = OnceLock::new();

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct GenerationValidationKey {
    data_dir: PathBuf,
    wiki: String,
    snapshot_version: String,
    manifest_fingerprint: String,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct PartitionSpec {
    pub year: i32,
    pub year_month: String,
    pub dir: PathBuf,
    pub files: Vec<PathBuf>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum GenerationLayer {
    Analytical,
    Warehouse,
    MetricInput,
}

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct GenerationFragment {
    pub(crate) layer: GenerationLayer,
    pub(crate) source_id: String,
    pub(crate) path: String,
    pub(crate) rows: u64,
    pub(crate) bytes: u64,
    pub(crate) sha256: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct GenerationManifest {
    pub(crate) schema_version: u64,
    pub(crate) wiki: String,
    pub(crate) snapshot_version: String,
    pub(crate) source_plan_sha256: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) compaction_manifest_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) compaction_manifest_sha256: Option<String>,
    pub(crate) fragments: Vec<GenerationFragment>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MarkerManifest {
    pub snapshot_version: Option<String>,
    pub source: PathBuf,
    pub source_size_bytes: u64,
    pub source_sha256: String,
    pub rows: usize,
    pub allow_empty: bool,
    pub analytical_paths: Vec<PathBuf>,
    pub warehouse_paths: Vec<PathBuf>,
    pub metric_input_paths: Vec<PathBuf>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct StoredMarkerManifest {
    schema_version: u64,
    snapshot_version: Option<String>,
    source_id: String,
    source: StoredSourceIdentity,
    rows: u64,
    allow_empty: bool,
    analytical_outputs: Vec<StoredOutput>,
    warehouse_outputs: Vec<StoredOutput>,
    #[serde(default)]
    metric_input_outputs: Vec<StoredOutput>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct StoredSourceIdentity {
    path: String,
    size_bytes: u64,
    sha256: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct StoredOutput {
    path: String,
    rows: u64,
}

pub fn analytical_wiki_dir(data_dir: &Path, wiki: &str) -> PathBuf {
    data_dir.join(ANALYTICAL_DIRNAME).join(wiki)
}

pub fn warehouse_wiki_dir(data_dir: &Path, wiki: &str) -> PathBuf {
    data_dir.join(WAREHOUSE_DIRNAME).join(wiki)
}

pub fn metric_input_wiki_dir(data_dir: &Path, wiki: &str) -> PathBuf {
    data_dir.join(METRIC_INPUT_DIRNAME).join(wiki)
}

pub fn snapshot_analytical_wiki_dir(
    data_dir: &Path,
    wiki: &str,
    snapshot_version: &str,
) -> Result<PathBuf> {
    validate_snapshot_version(snapshot_version)?;
    Ok(analytical_wiki_dir(data_dir, wiki)
        .join(SNAPSHOTS_DIRNAME)
        .join(snapshot_version))
}

pub fn snapshot_warehouse_wiki_dir(
    data_dir: &Path,
    wiki: &str,
    snapshot_version: &str,
) -> Result<PathBuf> {
    validate_snapshot_version(snapshot_version)?;
    Ok(warehouse_wiki_dir(data_dir, wiki)
        .join(SNAPSHOTS_DIRNAME)
        .join(snapshot_version))
}

pub fn snapshot_metric_input_wiki_dir(
    data_dir: &Path,
    wiki: &str,
    snapshot_version: &str,
) -> Result<PathBuf> {
    validate_snapshot_version(snapshot_version)?;
    Ok(metric_input_wiki_dir(data_dir, wiki)
        .join(SNAPSHOTS_DIRNAME)
        .join(snapshot_version))
}

pub(crate) fn snapshot_pointer_path(data_dir: &Path, wiki: &str) -> PathBuf {
    data_dir
        .join(SNAPSHOT_STATE_DIRNAME)
        .join(wiki)
        .join(CURRENT_SNAPSHOT_FILENAME)
}

pub(crate) fn generation_manifest_path(
    data_dir: &Path,
    wiki: &str,
    snapshot_version: &str,
) -> Result<PathBuf> {
    validate_snapshot_version(snapshot_version)?;
    Ok(data_dir
        .join(SNAPSHOT_STATE_DIRNAME)
        .join(wiki)
        .join(snapshot_version)
        .join(GENERATION_MANIFEST_FILENAME))
}

pub(crate) fn validate_snapshot_version(snapshot_version: &str) -> Result<()> {
    let bytes = snapshot_version.as_bytes();
    ensure!(
        bytes.len() == 7
            && bytes[0..4].iter().all(u8::is_ascii_digit)
            && bytes[4] == b'-'
            && bytes[5..7].iter().all(u8::is_ascii_digit),
        "invalid snapshot version {snapshot_version:?}; expected YYYY-MM"
    );
    let month: u8 = snapshot_version[5..7].parse()?;
    ensure!(
        (1..=12).contains(&month),
        "invalid snapshot month in {snapshot_version:?}"
    );
    Ok(())
}

pub fn current_snapshot_version(data_dir: &Path, wiki: &str) -> Result<Option<String>> {
    let pointer = snapshot_pointer_path(data_dir, wiki);
    if !pointer.exists() {
        return Ok(None);
    }
    let value: Value = serde_json::from_slice(
        &fs::read(&pointer)
            .with_context(|| format!("failed to read snapshot pointer {}", pointer.display()))?,
    )
    .with_context(|| format!("invalid snapshot pointer JSON in {}", pointer.display()))?;
    ensure!(
        value.get("schema_version").and_then(Value::as_u64)
            == Some(SNAPSHOT_POINTER_SCHEMA_VERSION),
        "unsupported snapshot pointer schema in {}",
        pointer.display()
    );
    ensure!(
        value.get("wiki").and_then(Value::as_str) == Some(wiki),
        "snapshot pointer wiki mismatch in {}",
        pointer.display()
    );
    let snapshot_version = value
        .get("snapshot_version")
        .and_then(Value::as_str)
        .context("snapshot pointer is missing snapshot_version")?;
    validate_snapshot_version(snapshot_version)?;
    Ok(Some(snapshot_version.to_string()))
}

pub fn active_analytical_wiki_dir(data_dir: &Path, wiki: &str) -> Result<PathBuf> {
    match current_snapshot_version(data_dir, wiki)? {
        Some(snapshot_version) => snapshot_analytical_wiki_dir(data_dir, wiki, &snapshot_version),
        None => Ok(analytical_wiki_dir(data_dir, wiki)),
    }
}

pub fn active_warehouse_wiki_dir(data_dir: &Path, wiki: &str) -> Result<PathBuf> {
    match current_snapshot_version(data_dir, wiki)? {
        Some(snapshot_version) => snapshot_warehouse_wiki_dir(data_dir, wiki, &snapshot_version),
        None => Ok(warehouse_wiki_dir(data_dir, wiki)),
    }
}

pub fn active_metric_input_wiki_dir(data_dir: &Path, wiki: &str) -> Result<PathBuf> {
    match current_snapshot_version(data_dir, wiki)? {
        Some(snapshot_version) => snapshot_metric_input_wiki_dir(data_dir, wiki, &snapshot_version),
        None => Ok(metric_input_wiki_dir(data_dir, wiki)),
    }
}

pub(crate) fn snapshot_layer_wiki_dir(
    data_dir: &Path,
    wiki: &str,
    snapshot_version: &str,
    layer: GenerationLayer,
) -> Result<PathBuf> {
    fragment_layer_root(data_dir, wiki, snapshot_version, layer)
}

pub(crate) fn active_layer_wiki_dir(
    data_dir: &Path,
    wiki: &str,
    layer: GenerationLayer,
) -> Result<PathBuf> {
    match layer {
        GenerationLayer::Analytical => active_analytical_wiki_dir(data_dir, wiki),
        GenerationLayer::Warehouse => active_warehouse_wiki_dir(data_dir, wiki),
        GenerationLayer::MetricInput => active_metric_input_wiki_dir(data_dir, wiki),
    }
}

fn fragment_layer_root(
    data_dir: &Path,
    wiki: &str,
    snapshot_version: &str,
    layer: GenerationLayer,
) -> Result<PathBuf> {
    match layer {
        GenerationLayer::Analytical => {
            snapshot_analytical_wiki_dir(data_dir, wiki, snapshot_version)
        }
        GenerationLayer::Warehouse => snapshot_warehouse_wiki_dir(data_dir, wiki, snapshot_version),
        GenerationLayer::MetricInput => {
            snapshot_metric_input_wiki_dir(data_dir, wiki, snapshot_version)
        }
    }
}

fn fragment_from_stored_output(
    data_dir: &Path,
    wiki: &str,
    snapshot_version: &str,
    layer: GenerationLayer,
    source_id: &str,
    output: &StoredOutput,
) -> Result<GenerationFragment> {
    let path = checked_stored_path(data_dir, &output.path)?;
    let root = fragment_layer_root(data_dir, wiki, snapshot_version, layer)?;
    ensure!(
        path.starts_with(&root) && is_source_output(&path, source_id),
        "source {source_id} records a fragment outside its immutable {layer:?} generation"
    );
    let metadata = fs::metadata(&path)
        .with_context(|| format!("failed to inspect generation fragment {}", path.display()))?;
    ensure!(metadata.is_file(), "generation fragment is not a file");
    let (hashed_bytes, sha256) = sha256_file(&path)?;
    ensure!(
        hashed_bytes == metadata.len(),
        "generation fragment size changed while it was being hashed"
    );
    Ok(GenerationFragment {
        layer,
        source_id: source_id.to_string(),
        path: path_to_string(&relative_path(data_dir, &path)?)?,
        rows: output.rows,
        bytes: metadata.len(),
        sha256,
    })
}

/// Materialize the complete immutable fragment allowlist after strict source
/// marker validation. The manifest is content-addressed and atomically
/// replaced; downstream readers never discover generation data by walking the
/// filesystem once a snapshot pointer exists.
pub(crate) fn write_generation_manifest(
    data_dir: &Path,
    wiki: &str,
    snapshot_version: &str,
) -> Result<PathBuf> {
    let path = generation_manifest_path(data_dir, wiki, snapshot_version)?;
    if path.is_file()
        && read_receipted_generation_manifest(data_dir, wiki, snapshot_version)?.is_some()
    {
        return Ok(path);
    }
    if path.is_file()
        && current_snapshot_version(data_dir, wiki)?.as_deref() == Some(snapshot_version)
    {
        let selected = read_generation_manifest(data_dir, wiki, snapshot_version)
            .context("selected generation manifest is immutable and invalid")?;
        validate_selected_generation(data_dir, wiki, snapshot_version, &selected)?;
        return Ok(path);
    }
    if path.is_file() {
        let existing = read_generation_manifest(data_dir, wiki, snapshot_version)?;
        if existing.schema_version == COMPACTED_METRIC_INPUT_MANIFEST_SCHEMA_VERSION {
            return Ok(path);
        }
    }
    validate_snapshot_generation(data_dir, wiki, snapshot_version)?;
    let (plan, plan_path) =
        crate::snapshot_plan::SnapshotPlan::load_or_resolve(data_dir, wiki, snapshot_version)?;
    let analytical = snapshot_analytical_wiki_dir(data_dir, wiki, snapshot_version)?;
    let mut fragments = Vec::new();
    for source in &plan.sources {
        let marker = marker_path_in(&analytical, &source.source_id);
        let stored =
            read_stored_marker(&marker)?.context("generation source marker disappeared")?;
        let fragment = |layer, output| {
            fragment_from_stored_output(
                data_dir,
                wiki,
                snapshot_version,
                layer,
                &source.source_id,
                output,
            )
        };
        for output in &stored.analytical_outputs {
            fragments.push(fragment(GenerationLayer::Analytical, output)?);
        }
        for output in &stored.warehouse_outputs {
            fragments.push(fragment(GenerationLayer::Warehouse, output)?);
        }
        for output in &stored.metric_input_outputs {
            fragments.push(fragment(GenerationLayer::MetricInput, output)?);
        }
    }
    fragments.sort();
    ensure!(
        fragments
            .iter()
            .map(|fragment| fragment.path.as_str())
            .collect::<BTreeSet<_>>()
            .len()
            == fragments.len(),
        "generation manifest contains duplicate fragment paths"
    );
    let (_, source_plan_sha256) = sha256_file(&plan_path)?;
    let metric_input_generation = fragments
        .iter()
        .any(|fragment| fragment.layer == GenerationLayer::MetricInput);
    let legacy_generation = fragments.iter().any(|fragment| {
        matches!(
            fragment.layer,
            GenerationLayer::Analytical | GenerationLayer::Warehouse
        )
    });
    ensure!(
        metric_input_generation != legacy_generation,
        "generation must use exactly one storage layout"
    );
    ensure!(
        metric_input_generation
            || fragments
                .iter()
                .any(|fragment| fragment.layer == GenerationLayer::Analytical),
        "generation contains no supported metric input fragments"
    );
    let manifest = GenerationManifest {
        schema_version: if metric_input_generation {
            DIRECT_METRIC_INPUT_MANIFEST_SCHEMA_VERSION
        } else {
            1
        },
        wiki: wiki.to_string(),
        snapshot_version: snapshot_version.to_string(),
        source_plan_sha256,
        compaction_manifest_path: None,
        compaction_manifest_sha256: None,
        fragments,
    };
    let parent = path.parent().context("generation manifest has no parent")?;
    fs::create_dir_all(parent)?;
    let mut bytes = serde_json::to_vec_pretty(&manifest)?;
    bytes.push(b'\n');
    if path.is_file() && fs::read(&path)? == bytes {
        return Ok(path);
    }
    let temporary = parent.join(format!(
        ".{GENERATION_MANIFEST_FILENAME}.{}.{}.tmp",
        std::process::id(),
        GENERATION_MANIFEST_TEMP_NONCE.fetch_add(1, Ordering::Relaxed)
    ));
    let write_result = (|| -> Result<()> {
        let mut file = File::create(&temporary)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        fs::rename(&temporary, &path)?;
        File::open(parent)?.sync_all()?;
        Ok(())
    })();
    if write_result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    write_result?;
    Ok(path)
}

fn validate_selected_generation(
    data_dir: &Path,
    wiki: &str,
    snapshot_version: &str,
    selected: &GenerationManifest,
) -> Result<()> {
    if selected.schema_version == COMPACTED_METRIC_INPUT_MANIFEST_SCHEMA_VERSION {
        return Ok(());
    }
    validate_snapshot_generation(data_dir, wiki, snapshot_version)
        .context("selected generation manifest is immutable and invalid")?;
    Ok(())
}

/// Replace an unselected direct schema-v2 source-fragment allowlist with the
/// independently validated compacted allowlist. The source manifest identity
/// is retained inside the compaction receipt, so this transition cannot mix
/// fragments from another generation.
pub(crate) fn publish_compacted_generation_manifest(
    data_dir: &Path,
    wiki: &str,
    snapshot_version: &str,
    source_manifest: &GenerationManifest,
    compaction: &crate::compaction::CompactionManifest,
) -> Result<PathBuf> {
    ensure!(
        current_snapshot_version(data_dir, wiki)?.as_deref() != Some(snapshot_version),
        "selected generation manifest is immutable; compact the next snapshot instead"
    );
    ensure!(
        source_manifest.schema_version == DIRECT_METRIC_INPUT_MANIFEST_SCHEMA_VERSION
            && source_manifest.compaction_manifest_path.is_none()
            && source_manifest.compaction_manifest_sha256.is_none(),
        "compaction source must be a direct schema-v2 metric-input manifest"
    );
    crate::compaction::validate_structure(data_dir, wiki, snapshot_version, compaction)?;
    ensure!(
        crate::compaction::canonical_sha256(source_manifest)? == compaction.source_manifest_sha256,
        "compaction was prepared from another source manifest"
    );
    let compaction_path = crate::compaction::manifest_path(data_dir, wiki, snapshot_version)?;
    let (_, compaction_sha256) = sha256_file(&compaction_path)?;
    let mut fragments = compaction.compacted_fragments.clone();
    fragments.sort();
    let manifest = GenerationManifest {
        schema_version: COMPACTED_METRIC_INPUT_MANIFEST_SCHEMA_VERSION,
        wiki: wiki.to_string(),
        snapshot_version: snapshot_version.to_string(),
        source_plan_sha256: source_manifest.source_plan_sha256.clone(),
        compaction_manifest_path: {
            let relative = relative_path(data_dir, &compaction_path)?;
            Some(path_to_string(&relative)?)
        },
        compaction_manifest_sha256: Some(compaction_sha256),
        fragments,
    };
    validate_generation_manifest(data_dir, wiki, snapshot_version, &manifest)?;

    let path = generation_manifest_path(data_dir, wiki, snapshot_version)?;
    let parent = path.parent().context("generation manifest has no parent")?;
    fs::create_dir_all(parent)?;
    let temporary = parent.join(format!(
        ".{GENERATION_MANIFEST_FILENAME}.{}.{}.compact.tmp",
        std::process::id(),
        GENERATION_MANIFEST_TEMP_NONCE.fetch_add(1, Ordering::Relaxed)
    ));
    let mut bytes = serde_json::to_vec_pretty(&manifest)?;
    bytes.push(b'\n');
    let result = (|| -> Result<()> {
        let mut file = File::create(&temporary)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        fs::rename(&temporary, &path)?;
        File::open(parent)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(temporary);
    }
    result?;
    Ok(path)
}

fn validate_generation_manifest(
    data_dir: &Path,
    wiki: &str,
    snapshot_version: &str,
    manifest: &GenerationManifest,
) -> Result<()> {
    validate_generation_manifest_structure(data_dir, wiki, snapshot_version, manifest)?;
    for fragment in &manifest.fragments {
        let path = checked_stored_path(data_dir, &fragment.path)?;
        let metadata = fs::metadata(&path)
            .with_context(|| format!("generation fragment is missing: {}", path.display()))?;
        ensure!(
            metadata.is_file() && metadata.len() == fragment.bytes,
            "generation fragment size changed"
        );
        let rows = ParquetReader::new(File::open(&path)?).num_rows()?;
        ensure!(
            u64::try_from(rows)? == fragment.rows,
            "generation fragment row count changed"
        );
    }
    Ok(())
}

fn validate_generation_manifest_structure(
    data_dir: &Path,
    wiki: &str,
    snapshot_version: &str,
    manifest: &GenerationManifest,
) -> Result<()> {
    let supported_schema = matches!(
        manifest.schema_version,
        1 | DIRECT_METRIC_INPUT_MANIFEST_SCHEMA_VERSION
            | COMPACTED_METRIC_INPUT_MANIFEST_SCHEMA_VERSION
    );
    ensure!(supported_schema, "unsupported generation manifest schema");
    ensure!(
        manifest.wiki == wiki && manifest.snapshot_version == snapshot_version,
        "generation manifest identity mismatch"
    );
    let (plan, plan_path) =
        crate::snapshot_plan::SnapshotPlan::load_or_resolve(data_dir, wiki, snapshot_version)?;
    let (_, plan_sha256) = sha256_file(&plan_path)?;
    ensure!(
        manifest.source_plan_sha256 == plan_sha256,
        "generation manifest source plan identity changed"
    );
    ensure!(
        !manifest.fragments.is_empty(),
        "generation manifest has no fragments"
    );
    ensure!(
        manifest.fragments.windows(2).all(|pair| pair[0] < pair[1]),
        "generation manifest fragments are not unique and deterministically sorted"
    );
    let expected_sources: BTreeSet<_> = plan
        .sources
        .iter()
        .map(|source| source.source_id.as_str())
        .collect();
    let mut analytical_sources = BTreeSet::new();
    let mut warehouse_sources = BTreeSet::new();
    let mut metric_input_sources = BTreeSet::new();
    let mut fragment_paths = BTreeSet::new();
    for fragment in &manifest.fragments {
        ensure!(
            fragment_paths.insert(fragment.path.as_str()),
            "generation manifest contains duplicate fragment paths"
        );
        ensure!(
            fragment.sha256.len() == 64
                && fragment.sha256.bytes().all(|byte| byte.is_ascii_hexdigit()),
            "generation fragment has an invalid SHA-256"
        );
        let path = checked_stored_path(data_dir, &fragment.path)?;
        let root = fragment_layer_root(data_dir, wiki, snapshot_version, fragment.layer)?;
        ensure!(
            path.starts_with(&root) && is_source_output(&path, &fragment.source_id),
            "generation manifest contains a fragment outside its declared layer or source"
        );
        match fragment.layer {
            GenerationLayer::Analytical => analytical_sources.insert(fragment.source_id.as_str()),
            GenerationLayer::Warehouse => warehouse_sources.insert(fragment.source_id.as_str()),
            GenerationLayer::MetricInput => {
                metric_input_sources.insert(fragment.source_id.as_str())
            }
        };
    }
    if manifest.schema_version == 1 {
        ensure!(
            analytical_sources == expected_sources
                && warehouse_sources == expected_sources
                && metric_input_sources.is_empty(),
            "schema-v1 generation does not cover every planned source in both legacy layers"
        );
        ensure!(
            manifest.compaction_manifest_path.is_none()
                && manifest.compaction_manifest_sha256.is_none(),
            "schema-v1 generation cannot reference compaction"
        );
    } else if manifest.schema_version == DIRECT_METRIC_INPUT_MANIFEST_SCHEMA_VERSION {
        ensure!(
            metric_input_sources == expected_sources
                && analytical_sources.is_empty()
                && warehouse_sources.is_empty(),
            "schema-v2 generation must contain exactly one metric-input layer"
        );
        ensure!(
            manifest.compaction_manifest_path.is_none()
                && manifest.compaction_manifest_sha256.is_none(),
            "direct schema-v2 generation cannot reference compaction"
        );
    } else {
        ensure!(
            analytical_sources.is_empty()
                && warehouse_sources.is_empty()
                && metric_input_sources
                    .iter()
                    .all(|source| source.starts_with("compacted-")),
            "schema-v3 generation must contain only compacted metric-input fragments"
        );
        let compaction_relative = manifest
            .compaction_manifest_path
            .as_deref()
            .context("schema-v3 generation has no compaction manifest path")?;
        let expected_compaction_path =
            crate::compaction::manifest_path(data_dir, wiki, snapshot_version)?;
        ensure!(
            checked_stored_path(data_dir, compaction_relative)? == expected_compaction_path,
            "schema-v3 generation references the wrong compaction manifest"
        );
        let expected_hash = manifest
            .compaction_manifest_sha256
            .as_deref()
            .context("schema-v3 generation has no compaction manifest hash")?;
        let (_, actual_hash) = sha256_file(&expected_compaction_path)?;
        ensure!(
            expected_hash == actual_hash,
            "schema-v3 compaction manifest hash changed"
        );
        let compaction: crate::compaction::CompactionManifest =
            serde_json::from_slice(&fs::read(&expected_compaction_path)?)?;
        crate::compaction::validate_structure(data_dir, wiki, snapshot_version, &compaction)?;
        ensure!(
            compaction.compacted_fragments == manifest.fragments,
            "generation and compaction fragment allowlists disagree"
        );
    }
    Ok(())
}

fn generation_validation_cache()
-> &'static Mutex<BTreeMap<GenerationValidationKey, GenerationManifest>> {
    GENERATION_VALIDATION_CACHE.get_or_init(|| Mutex::new(BTreeMap::new()))
}

/// Authenticate the compact generation manifest through the ingest receipt.
/// This proves the immutable allowlist without reopening or hashing any of the
/// Parquet fragments. `None` means an older generation needs the strict
/// compatibility path.
fn read_receipted_generation_manifest(
    data_dir: &Path,
    wiki: &str,
    snapshot_version: &str,
) -> Result<Option<GenerationManifest>> {
    let receipt_path =
        crate::fingerprint::data_stage_receipt_path(data_dir, wiki, snapshot_version, "ingest");
    let spec = crate::fingerprint::StageSpec {
        stage: "ingest",
        scope: wiki,
        selected_snapshot: Some(snapshot_version),
        algorithm_version: crate::ingest::INGEST_ALGORITHM_VERSION,
    };
    let Some(receipt) = crate::fingerprint::validated_receipt(&receipt_path, spec)? else {
        return Ok(None);
    };
    let Some(manifest_identity) = receipt
        .outputs
        .iter()
        .find(|identity| identity.identity == "generation-manifest")
    else {
        return Ok(None);
    };
    let manifest_path = generation_manifest_path(data_dir, wiki, snapshot_version)?;
    if !crate::fingerprint::artifact_matches(
        manifest_identity,
        &crate::fingerprint::TrackedPath::new("generation-manifest", &manifest_path),
    )
    .is_ok_and(|matches| matches)
    {
        return Ok(None);
    }
    let key = GenerationValidationKey {
        data_dir: data_dir.to_path_buf(),
        wiki: wiki.to_string(),
        snapshot_version: snapshot_version.to_string(),
        manifest_fingerprint: manifest_identity.sha256.clone(),
    };
    if let Some(manifest) = generation_validation_cache()
        .lock()
        .expect("generation validation cache lock poisoned")
        .get(&key)
        .cloned()
    {
        return Ok(Some(manifest));
    }
    let Some(plan_identity) = receipt
        .inputs
        .iter()
        .find(|identity| identity.identity == "snapshot-plan")
    else {
        return Ok(None);
    };
    let (_, plan_path) =
        crate::snapshot_plan::SnapshotPlan::load_or_resolve(data_dir, wiki, snapshot_version)?;
    if !crate::fingerprint::artifact_matches(
        plan_identity,
        &crate::fingerprint::TrackedPath::new("snapshot-plan", plan_path),
    )
    .is_ok_and(|matches| matches)
    {
        return Ok(None);
    }
    let manifest_bytes = fs::read(&manifest_path).with_context(|| {
        format!(
            "failed to read receipted generation manifest {}",
            manifest_path.display()
        )
    })?;
    let manifest: GenerationManifest =
        serde_json::from_slice(&manifest_bytes).with_context(|| {
            format!(
                "invalid receipted generation manifest JSON in {}",
                manifest_path.display()
            )
        })?;
    validate_generation_manifest_structure(data_dir, wiki, snapshot_version, &manifest)?;
    generation_validation_cache()
        .lock()
        .expect("generation validation cache lock poisoned")
        .insert(key, manifest.clone());
    Ok(Some(manifest))
}

pub(crate) fn read_generation_manifest(
    data_dir: &Path,
    wiki: &str,
    snapshot_version: &str,
) -> Result<GenerationManifest> {
    let path = generation_manifest_path(data_dir, wiki, snapshot_version)?;
    let manifest: GenerationManifest = serde_json::from_slice(
        &fs::read(&path)
            .with_context(|| format!("failed to read generation manifest {}", path.display()))?,
    )
    .with_context(|| format!("invalid generation manifest JSON in {}", path.display()))?;
    validate_generation_manifest(data_dir, wiki, snapshot_version, &manifest)?;
    Ok(manifest)
}

pub(crate) fn ensure_generation_manifest(
    data_dir: &Path,
    wiki: &str,
    snapshot_version: &str,
) -> Result<GenerationManifest> {
    if let Some(manifest) = read_receipted_generation_manifest(data_dir, wiki, snapshot_version)? {
        return Ok(manifest);
    }
    let path = generation_manifest_path(data_dir, wiki, snapshot_version)?;
    if !path.is_file() {
        // One-time migration for generations published before manifest-owned
        // reads were introduced. Strict marker validation runs before writing.
        write_generation_manifest(data_dir, wiki, snapshot_version)?;
    }
    read_generation_manifest(data_dir, wiki, snapshot_version)
}

pub(crate) fn active_fragment_files(
    data_dir: &Path,
    wiki: &str,
    layer: GenerationLayer,
) -> Result<Vec<PathBuf>> {
    let Some(snapshot_version) = current_snapshot_version(data_dir, wiki)? else {
        let root = match layer {
            GenerationLayer::Analytical => analytical_wiki_dir(data_dir, wiki),
            GenerationLayer::Warehouse => warehouse_wiki_dir(data_dir, wiki),
            GenerationLayer::MetricInput => metric_input_wiki_dir(data_dir, wiki),
        };
        return collect_parquet_files(&root);
    };
    let manifest = ensure_generation_manifest(data_dir, wiki, &snapshot_version)?;
    manifest
        .fragments
        .into_iter()
        .filter(|fragment| fragment.layer == layer)
        .map(|fragment| checked_stored_path(data_dir, &fragment.path))
        .collect()
}

pub(crate) fn snapshot_fragment_files(
    data_dir: &Path,
    wiki: &str,
    snapshot_version: &str,
    layer: GenerationLayer,
) -> Result<Vec<PathBuf>> {
    let manifest = ensure_generation_manifest(data_dir, wiki, snapshot_version)?;
    manifest
        .fragments
        .into_iter()
        .filter(|fragment| fragment.layer == layer)
        .map(|fragment| checked_stored_path(data_dir, &fragment.path))
        .collect()
}

/// Select the single authoritative compute layer while retaining read
/// compatibility with immutable schema-v1 generations.
pub(crate) fn snapshot_compute_layer(
    data_dir: &Path,
    wiki: &str,
    snapshot_version: &str,
    legacy_layer: GenerationLayer,
) -> Result<GenerationLayer> {
    let manifest = ensure_generation_manifest(data_dir, wiki, snapshot_version)?;
    Ok(if manifest.schema_version != 1 {
        GenerationLayer::MetricInput
    } else {
        legacy_layer
    })
}

pub(crate) fn active_compute_layer(
    data_dir: &Path,
    wiki: &str,
    legacy_layer: GenerationLayer,
) -> Result<GenerationLayer> {
    match current_snapshot_version(data_dir, wiki)? {
        Some(snapshot) => snapshot_compute_layer(data_dir, wiki, &snapshot, legacy_layer),
        None => Ok(legacy_layer),
    }
}

pub fn publish_current_snapshot(data_dir: &Path, wiki: &str, snapshot_version: &str) -> Result<()> {
    validate_snapshot_version(snapshot_version)?;
    let manifest = ensure_generation_manifest(data_dir, wiki, snapshot_version)
        .context("cannot publish snapshot without a valid generation manifest")?;
    let required_layers: BTreeSet<_> = manifest
        .fragments
        .iter()
        .map(|fragment| fragment.layer)
        .collect();
    for layer in required_layers {
        ensure!(
            fragment_layer_root(data_dir, wiki, snapshot_version, layer)?.is_dir(),
            "cannot publish incomplete snapshot {snapshot_version} for {wiki}"
        );
    }

    write_current_snapshot_pointer(data_dir, wiki, snapshot_version)
}

pub(crate) fn restore_current_snapshot(
    data_dir: &Path,
    wiki: &str,
    snapshot_version: Option<&str>,
) -> Result<()> {
    if let Some(snapshot_version) = snapshot_version {
        return publish_current_snapshot(data_dir, wiki, snapshot_version);
    }
    let pointer = snapshot_pointer_path(data_dir, wiki);
    if pointer.is_file() {
        fs::remove_file(&pointer)?;
        let parent = pointer
            .parent()
            .expect("snapshot pointer path always has a state directory");
        let directory_result = File::open(parent);
        let directory = directory_result?;
        let sync_result = directory.sync_all();
        sync_result?;
    }
    Ok(())
}

fn write_current_snapshot_pointer(
    data_dir: &Path,
    wiki: &str,
    snapshot_version: &str,
) -> Result<()> {
    validate_snapshot_version(snapshot_version)?;

    let pointer = snapshot_pointer_path(data_dir, wiki);
    let parent = pointer.parent().context("snapshot pointer has no parent")?;
    fs::create_dir_all(parent)?;
    let temp = parent.join(format!(
        ".{CURRENT_SNAPSHOT_FILENAME}.{}.tmp",
        std::process::id()
    ));
    let bytes = serde_json::to_vec_pretty(&json!({
        "schema_version": SNAPSHOT_POINTER_SCHEMA_VERSION,
        "wiki": wiki,
        "snapshot_version": snapshot_version,
    }))?;
    let write_result = (|| -> Result<()> {
        let mut file = File::create(&temp)?;
        file.write_all(&bytes)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        fs::rename(&temp, &pointer)?;
        File::open(parent)?.sync_all()?;
        Ok(())
    })();
    if write_result.is_err() {
        let _ = fs::remove_file(&temp);
    }
    write_result
}

#[cfg(test)]
pub(crate) fn write_current_snapshot_pointer_for_test(
    data_dir: &Path,
    wiki: &str,
    snapshot_version: &str,
) -> Result<()> {
    write_current_snapshot_pointer(data_dir, wiki, snapshot_version)
}

#[cfg(test)]
pub(crate) fn publish_test_snapshot_pointer(
    data_dir: &Path,
    wiki: &str,
    snapshot_version: &str,
) -> Result<()> {
    let analytical = snapshot_analytical_wiki_dir(data_dir, wiki, snapshot_version)?;
    let warehouse = snapshot_warehouse_wiki_dir(data_dir, wiki, snapshot_version)?;
    ensure!(
        analytical.is_dir() && warehouse.is_dir(),
        "cannot publish incomplete test snapshot {snapshot_version} for {wiki}"
    );
    write_current_snapshot_pointer(data_dir, wiki, snapshot_version)
}

#[cfg(test)]
pub(crate) fn write_test_generation_manifest_from_files(
    data_dir: &Path,
    wiki: &str,
    snapshot_version: &str,
) -> Result<PathBuf> {
    use polars::prelude::ParquetWriter;

    let (plan, plan_path) =
        crate::snapshot_plan::SnapshotPlan::load_or_resolve(data_dir, wiki, snapshot_version)?;
    let first_source = plan.sources.first().context("test source plan is empty")?;
    let analytical_root = fragment_layer_root(
        data_dir,
        wiki,
        snapshot_version,
        GenerationLayer::Analytical,
    )
    .expect("test analytical generation should resolve");
    let warehouse_root =
        fragment_layer_root(data_dir, wiki, snapshot_version, GenerationLayer::Warehouse)?;
    let metric_input_root_result = fragment_layer_root(
        data_dir,
        wiki,
        snapshot_version,
        GenerationLayer::MetricInput,
    );
    let metric_input_root = metric_input_root_result?;
    let existing_analytical = collect_parquet_files(&analytical_root)?;
    let existing_warehouse = collect_parquet_files(&warehouse_root)?;
    let existing_metric_input = collect_parquet_files(&metric_input_root)?;
    let metric_input_generation = !existing_metric_input.is_empty();
    let template = existing_metric_input
        .first()
        .or_else(|| existing_analytical.first())
        .or_else(|| existing_warehouse.first())
        .context("test generation has no Parquet template")?;
    let mut empty_template = ParquetReader::new(File::open(template)?)
        .with_slice(Some((0, 0)))
        .finish()?;
    let mut fragments = Vec::new();
    let layers: &[GenerationLayer] = if metric_input_generation {
        &[GenerationLayer::MetricInput]
    } else {
        &[GenerationLayer::Analytical, GenerationLayer::Warehouse]
    };
    for &layer in layers {
        let root = fragment_layer_root(data_dir, wiki, snapshot_version, layer)?;
        let mut files = collect_parquet_files(&root)?;
        if files.is_empty() {
            let destination = month_partition_dir(&root, 2026, "2026-01")
                .join(format!("{}.part-00000.parquet", first_source.source_id));
            destination.parent().map(fs::create_dir_all).transpose()?;
            ParquetWriter::new(File::create(&destination)?).finish(&mut empty_template)?;
            files.push(destination);
        }
        for (index, file) in files.into_iter().enumerate() {
            let destination = file
                .parent()
                .context("test fragment has no parent")?
                .join(format!(
                    "{}.part-{index:05}.parquet",
                    first_source.source_id
                ));
            if file != destination {
                fs::rename(&file, &destination)?;
            }
            let rows = ParquetReader::new(File::open(&destination)?).num_rows()?;
            let (bytes, sha256) = sha256_file(&destination)?;
            fragments.push(GenerationFragment {
                layer,
                source_id: first_source.source_id.clone(),
                path: path_to_string(&relative_path(data_dir, &destination)?)?,
                rows: u64::try_from(rows)?,
                bytes,
                sha256,
            });
        }
        for source in plan.sources.iter().skip(1) {
            let destination = month_partition_dir(&root, 2026, "2026-01")
                .join(format!("{}.part-00000.parquet", source.source_id));
            destination.parent().map(fs::create_dir_all).transpose()?;
            ParquetWriter::new(File::create(&destination)?).finish(&mut empty_template)?;
            let (bytes, sha256) = sha256_file(&destination)?;
            fragments.push(GenerationFragment {
                layer,
                source_id: source.source_id.clone(),
                path: path_to_string(&relative_path(data_dir, &destination)?)?,
                rows: 0,
                bytes,
                sha256,
            });
        }
    }
    fragments.sort();
    let (_, source_plan_sha256) = sha256_file(&plan_path)?;
    let manifest = GenerationManifest {
        schema_version: if metric_input_generation { 2 } else { 1 },
        wiki: wiki.to_string(),
        snapshot_version: snapshot_version.to_string(),
        source_plan_sha256,
        compaction_manifest_path: None,
        compaction_manifest_sha256: None,
        fragments,
    };
    let path = generation_manifest_path(data_dir, wiki, snapshot_version)?;
    path.parent().map(fs::create_dir_all).transpose()?;
    let mut bytes = serde_json::to_vec_pretty(&manifest)?;
    bytes.push(b'\n');
    fs::write(&path, bytes)?;
    read_generation_manifest(data_dir, wiki, snapshot_version)?;
    Ok(path)
}

/// Repair a missing or corrupt current-generation pointer only after the
/// immutable generation's complete marker/output inventory validates.
pub fn repair_current_snapshot(
    data_dir: &Path,
    wiki: &str,
    snapshot_version: &str,
) -> Result<usize> {
    validate_and_publish_snapshot(data_dir, wiki, snapshot_version)
}

/// Validate the complete source-marker and Parquet inventory before making a
/// candidate generation visible to compute. This is shared by normal ingest
/// finalization and the explicit pointer-repair command so their readiness
/// semantics cannot drift.
pub(crate) fn validate_and_publish_snapshot(
    data_dir: &Path,
    wiki: &str,
    snapshot_version: &str,
) -> Result<usize> {
    write_generation_manifest(data_dir, wiki, snapshot_version)?;
    let marker_count =
        crate::snapshot_plan::SnapshotPlan::load_or_resolve(data_dir, wiki, snapshot_version)?
            .0
            .sources
            .len();
    publish_current_snapshot(data_dir, wiki, snapshot_version)?;
    Ok(marker_count)
}

/// Validate an immutable generation without making it current. Normal ingest
/// uses this before committing its stage receipt, so a receipt failure cannot
/// expose a candidate whose deterministic provenance was not recorded.
pub(crate) fn validate_snapshot_generation(
    data_dir: &Path,
    wiki: &str,
    snapshot_version: &str,
) -> Result<usize> {
    validate_snapshot_version(snapshot_version)?;
    let (source_plan, _) =
        crate::snapshot_plan::SnapshotPlan::load_or_resolve(data_dir, wiki, snapshot_version)?;
    let expected_source_ids: std::collections::BTreeSet<_> = source_plan
        .sources
        .iter()
        .map(|source| source.source_id.clone())
        .collect();
    let analytical = snapshot_analytical_wiki_dir(data_dir, wiki, snapshot_version)?;
    let warehouse = snapshot_warehouse_wiki_dir(data_dir, wiki, snapshot_version)?;
    let metric_input = snapshot_metric_input_wiki_dir(data_dir, wiki, snapshot_version)?;
    ensure!(
        analytical.is_dir() && (warehouse.is_dir() || metric_input.is_dir()),
        "cannot repair incomplete snapshot {snapshot_version} for {wiki}"
    );
    let markers = analytical.join(MARKERS_DIRNAME);
    ensure!(
        markers.is_dir(),
        "snapshot {snapshot_version} for {wiki} has no marker inventory"
    );
    let mut marker_count = 0usize;
    let mut actual_source_ids = std::collections::BTreeSet::new();
    let mut analytical_allowlist = std::collections::BTreeSet::new();
    let mut warehouse_allowlist = std::collections::BTreeSet::new();
    let mut metric_input_allowlist = std::collections::BTreeSet::new();
    for entry in fs::read_dir(&markers)? {
        let entry = entry?;
        ensure!(
            entry.file_type()?.is_file(),
            "snapshot marker inventory contains a non-file: {}",
            entry.path().display()
        );
        let name = entry.file_name();
        let source_id = name
            .to_str()
            .and_then(|value| value.strip_suffix(".done"))
            .filter(|value| !value.is_empty())
            .with_context(|| {
                format!(
                    "unexpected snapshot marker name: {}",
                    entry.path().display()
                )
            })?;
        ensure!(
            marker_manifest_is_valid_in(data_dir, &analytical, source_id)?,
            "snapshot marker is invalid: {}",
            entry.path().display()
        );
        actual_source_ids.insert(source_id.to_string());
        let stored =
            read_stored_marker(&entry.path())?.context("validated snapshot marker disappeared")?;
        for output in stored.analytical_outputs {
            analytical_allowlist.insert(checked_stored_path(data_dir, &output.path)?);
        }
        for output in stored.warehouse_outputs {
            warehouse_allowlist.insert(checked_stored_path(data_dir, &output.path)?);
        }
        for output in stored.metric_input_outputs {
            metric_input_allowlist.insert(checked_stored_path(data_dir, &output.path)?);
        }
        marker_count += 1;
    }
    ensure!(marker_count > 0, "snapshot marker inventory is empty");
    ensure!(
        actual_source_ids == expected_source_ids,
        "snapshot marker inventory does not match its immutable source plan"
    );
    let actual_analytical: std::collections::BTreeSet<_> =
        collect_parquet_files(&analytical)?.into_iter().collect();
    let actual_warehouse: std::collections::BTreeSet<_> =
        collect_parquet_files(&warehouse)?.into_iter().collect();
    let actual_metric_input: std::collections::BTreeSet<_> =
        collect_parquet_files(&metric_input)?.into_iter().collect();
    ensure!(
        actual_analytical == analytical_allowlist
            && actual_warehouse == warehouse_allowlist
            && actual_metric_input == metric_input_allowlist,
        "snapshot marker inventory does not exactly account for generation Parquet files"
    );
    Ok(marker_count)
}

pub fn retire_inactive_snapshots(data_dir: &Path, wiki: &str) -> Result<usize> {
    let Some(active) = current_snapshot_version(data_dir, wiki)? else {
        return Ok(0);
    };
    let mut removed = 0;
    for layer_root in [
        analytical_wiki_dir(data_dir, wiki),
        warehouse_wiki_dir(data_dir, wiki),
        metric_input_wiki_dir(data_dir, wiki),
    ] {
        let snapshots_root = layer_root.join(SNAPSHOTS_DIRNAME);
        if !snapshots_root.is_dir() {
            continue;
        }
        for entry in fs::read_dir(&snapshots_root)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() && entry.file_name() != active.as_str() {
                fs::remove_dir_all(entry.path())?;
                removed += 1;
            }
        }
        for entry in fs::read_dir(&layer_root)? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if entry.file_type()?.is_dir() && (name == MARKERS_DIRNAME || name.starts_with("year="))
            {
                fs::remove_dir_all(entry.path())?;
                removed += 1;
            }
        }
    }
    let state_root = data_dir.join(SNAPSHOT_STATE_DIRNAME).join(wiki);
    for entry in fs::read_dir(&state_root)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() && entry.file_name() != active.as_str() {
            fs::remove_dir_all(entry.path())?;
            removed += 1;
        }
    }
    Ok(removed)
}

pub(crate) fn clean_stale_inactive_snapshots(
    data_dir: &Path,
    wiki: &str,
    protected_versions: &std::collections::BTreeSet<String>,
    minimum_age: Duration,
    now: SystemTime,
    removed_paths: &mut Vec<String>,
) -> Result<usize> {
    let Some(active) = current_snapshot_version(data_dir, wiki)? else {
        // Without an authoritative pointer there is no safe way to distinguish
        // an abandoned generation from one awaiting first publication.
        return Ok(0);
    };
    let layer_roots = [
        analytical_wiki_dir(data_dir, wiki).join(SNAPSHOTS_DIRNAME),
        warehouse_wiki_dir(data_dir, wiki).join(SNAPSHOTS_DIRNAME),
        metric_input_wiki_dir(data_dir, wiki).join(SNAPSHOTS_DIRNAME),
    ];
    let mut versions = std::collections::BTreeSet::new();
    for root in &layer_roots {
        if !root.is_dir() {
            continue;
        }
        for entry in fs::read_dir(root)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                let version = entry.file_name().to_string_lossy().into_owned();
                if version != active
                    && !protected_versions.contains(&version)
                    && validate_snapshot_version(&version).is_ok()
                {
                    versions.insert(version);
                }
            }
        }
    }
    let mut removed = 0;
    for version in versions {
        let paths: Vec<_> = layer_roots
            .iter()
            .map(|root| root.join(&version))
            .filter(|path| path.is_dir())
            .collect();
        let all_expired = paths.iter().all(|path| {
            fs::metadata(path)
                .and_then(|metadata| metadata.modified())
                .ok()
                .is_some_and(|modified| {
                    now.duration_since(modified).unwrap_or_default() >= minimum_age
                })
        });
        if !paths.is_empty() && all_expired {
            for path in paths {
                fs::remove_dir_all(&path)?;
                removed_paths.push(path.to_string_lossy().into_owned());
                removed += 1;
            }
            let state = data_dir
                .join(SNAPSHOT_STATE_DIRNAME)
                .join(wiki)
                .join(&version);
            state
                .is_dir()
                .then_some(state)
                .into_iter()
                .try_for_each(|state| -> Result<()> {
                    fs::remove_dir_all(&state)?;
                    removed_paths.push(state.to_string_lossy().into_owned());
                    removed += 1;
                    Ok(())
                })?;
        }
    }
    Ok(removed)
}

#[cfg(test)]
pub fn marker_path(data_dir: &Path, wiki: &str, source_id: &str) -> PathBuf {
    marker_path_in(&analytical_wiki_dir(data_dir, wiki), source_id)
}

pub fn marker_path_in(analytical_root: &Path, source_id: &str) -> PathBuf {
    analytical_root
        .join(MARKERS_DIRNAME)
        .join(format!("{source_id}.done"))
}

#[cfg(test)]
pub fn write_test_marker_in(
    data_dir: &Path,
    analytical_root: &Path,
    source_id: &str,
) -> Result<PathBuf> {
    use polars::prelude::{Column, DataFrame, ParquetWriter};

    let suffix = analytical_root
        .strip_prefix(data_dir.join(ANALYTICAL_DIRNAME))
        .context("test analytical root is outside data directory")?;
    let warehouse_root = data_dir.join(WAREHOUSE_DIRNAME).join(suffix);
    let source = data_dir
        .join("raw")
        .join("marker-fixtures")
        .join(format!("{source_id}.tsv.bz2"));
    source.parent().map(fs::create_dir_all).transpose()?;
    fs::write(&source, b"strict-marker-source")?;
    let snapshot_version = analytical_root
        .components()
        .collect::<Vec<_>>()
        .windows(2)
        .find_map(|parts| {
            (parts[0].as_os_str() == SNAPSHOTS_DIRNAME)
                .then(|| parts[1].as_os_str().to_str())
                .flatten()
                .map(str::to_string)
        });
    let mut paths = Vec::new();
    for root in [analytical_root, warehouse_root.as_path()] {
        let path = root
            .join("year=2026/year_month=2026-01")
            .join(format!("{source_id}.part-00000.parquet"));
        path.parent().map(fs::create_dir_all).transpose()?;
        let mut frame = DataFrame::new_infer_height(vec![Column::new("row".into(), [1_i64])])?;
        ParquetWriter::new(File::create(&path)?).finish(&mut frame)?;
        paths.push(path);
    }
    let (source_size_bytes, source_sha256) = sha256_file(&source)?;
    write_marker_manifest_in(
        data_dir,
        analytical_root,
        source_id,
        &MarkerManifest {
            snapshot_version,
            source,
            source_size_bytes,
            source_sha256,
            rows: 1,
            allow_empty: false,
            analytical_paths: vec![paths[0].clone()],
            warehouse_paths: vec![paths[1].clone()],
            metric_input_paths: Vec::new(),
        },
    )
}

#[cfg(test)]
pub fn write_marker_manifest(
    data_dir: &Path,
    wiki: &str,
    source_id: &str,
    manifest: &MarkerManifest,
) -> Result<PathBuf> {
    write_marker_manifest_in(
        data_dir,
        &analytical_wiki_dir(data_dir, wiki),
        source_id,
        manifest,
    )
}

pub fn write_marker_manifest_in(
    data_dir: &Path,
    analytical_root: &Path,
    source_id: &str,
    manifest: &MarkerManifest,
) -> Result<PathBuf> {
    write_marker_manifest_internal(data_dir, analytical_root, source_id, manifest, true)
}

/// Commit a strict marker after the caller has already removed every output
/// owned by this source inside its canonical event range. Snapshot-window
/// ingest uses this to avoid recursively scanning the complete generation for
/// every source; the final generation validator still performs one exact
/// allowlist comparison before publication.
pub(crate) fn write_precleaned_marker_manifest_in(
    data_dir: &Path,
    analytical_root: &Path,
    source_id: &str,
    manifest: &MarkerManifest,
) -> Result<PathBuf> {
    write_marker_manifest_internal(data_dir, analytical_root, source_id, manifest, false)
}

fn write_marker_manifest_internal(
    data_dir: &Path,
    analytical_root: &Path,
    source_id: &str,
    manifest: &MarkerManifest,
    remove_unexpected: bool,
) -> Result<PathBuf> {
    let marker = marker_path_in(analytical_root, source_id);
    let parent = marker.parent().context("marker path has no parent")?;
    fs::create_dir_all(parent)?;

    validate_source_identity(manifest)?;
    ensure!(
        manifest.source.file_name().and_then(|name| name.to_str())
            == Some(format!("{source_id}.tsv.bz2").as_str()),
        "marker source filename does not match source ID {source_id}"
    );
    let (expected_snapshot, analytical_suffix) =
        analytical_root_identity(data_dir, analytical_root)?;
    ensure!(
        manifest.snapshot_version == expected_snapshot,
        "marker snapshot does not match its analytical generation"
    );
    if let Some(snapshot) = manifest.snapshot_version.as_deref() {
        validate_snapshot_version(snapshot)?;
    }
    ensure!(
        manifest.rows > 0 || (manifest.allow_empty && manifest.snapshot_version.is_none()),
        "source {source_id} unexpectedly produced zero rows"
    );
    let analytical_outputs = stored_outputs(data_dir, analytical_root, &manifest.analytical_paths)?;
    let warehouse_root = data_dir.join(WAREHOUSE_DIRNAME).join(analytical_suffix);
    let warehouse_outputs = stored_outputs(data_dir, &warehouse_root, &manifest.warehouse_paths)?;
    let metric_input_root = data_dir.join(METRIC_INPUT_DIRNAME).join(
        analytical_root
            .strip_prefix(data_dir.join(ANALYTICAL_DIRNAME))
            .context("analytical marker root is outside the data directory")?,
    );
    let metric_input_outputs =
        stored_outputs(data_dir, &metric_input_root, &manifest.metric_input_paths)?;
    ensure_output_totals(
        source_id,
        manifest.rows,
        &analytical_outputs,
        &warehouse_outputs,
        &metric_input_outputs,
    )?;
    if remove_unexpected {
        remove_unexpected_source_outputs(analytical_root, source_id, &manifest.analytical_paths)?;
        remove_unexpected_source_outputs(&warehouse_root, source_id, &manifest.warehouse_paths)?;
        let metric_cleanup = remove_unexpected_source_outputs(
            &metric_input_root,
            source_id,
            &manifest.metric_input_paths,
        );
        metric_cleanup?;
    }
    let source = relative_path(data_dir, &manifest.source)?;
    let stored = StoredMarkerManifest {
        schema_version: MARKER_SCHEMA_VERSION,
        snapshot_version: manifest.snapshot_version.clone(),
        source_id: source_id.to_string(),
        source: StoredSourceIdentity {
            path: path_to_string(&source)?,
            size_bytes: manifest.source_size_bytes,
            sha256: manifest.source_sha256.clone(),
        },
        rows: u64::try_from(manifest.rows)?,
        allow_empty: manifest.allow_empty,
        analytical_outputs,
        warehouse_outputs,
        metric_input_outputs,
    };
    let mut bytes = serde_json::to_vec_pretty(&stored)?;
    bytes.push(b'\n');
    let temp = parent.join(format!(".{source_id}.done.{}.tmp", std::process::id()));
    let write_result = (|| -> Result<()> {
        let mut file = File::create(&temp)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        fs::rename(&temp, &marker)?;
        File::open(parent)?.sync_all()?;
        Ok(())
    })();
    if write_result.is_err() {
        let _ = fs::remove_file(&temp);
    }
    write_result?;
    Ok(marker)
}

fn stored_outputs(
    data_dir: &Path,
    layer_root: &Path,
    paths: &[PathBuf],
) -> Result<Vec<StoredOutput>> {
    paths
        .iter()
        .map(|path| {
            ensure!(
                path.starts_with(layer_root),
                "marker output {} is outside expected layer {}",
                path.display(),
                layer_root.display()
            );
            let relative = relative_path(data_dir, path)?;
            ensure!(
                path.extension()
                    .is_some_and(|extension| extension == "parquet"),
                "marker output is not Parquet: {}",
                path.display()
            );
            let mut reader = ParquetReader::new(
                File::open(path)
                    .with_context(|| format!("failed to open marker output {}", path.display()))?,
            );
            let rows = reader
                .num_rows()
                .with_context(|| format!("unreadable Parquet footer in {}", path.display()))?;
            Ok(StoredOutput {
                path: path_to_string(&relative)?,
                rows: u64::try_from(rows)?,
            })
        })
        .collect()
}

fn ensure_output_totals(
    source_id: &str,
    expected_rows: usize,
    analytical: &[StoredOutput],
    warehouse: &[StoredOutput],
    metric_input: &[StoredOutput],
) -> Result<()> {
    let uses_legacy_layers = !analytical.is_empty() || !warehouse.is_empty();
    let uses_metric_input = !metric_input.is_empty();
    ensure!(
        expected_rows == 0
            || (uses_legacy_layers && !uses_metric_input)
            || (!uses_legacy_layers && uses_metric_input),
        "source {source_id} must use exactly one storage layout"
    );
    ensure!(
        !uses_legacy_layers || (!analytical.is_empty() && !warehouse.is_empty()),
        "source {source_id} has an incomplete legacy output layout"
    );
    let expected_rows = u64::try_from(expected_rows)?;
    let layers: &[(&str, &[StoredOutput])] = if uses_metric_input {
        &[("metric_input", metric_input)]
    } else {
        &[("analytical", analytical), ("warehouse", warehouse)]
    };
    for (layer, outputs) in layers {
        let actual = outputs.iter().try_fold(0_u64, |total, output| {
            total
                .checked_add(output.rows)
                .context("marker row count overflow")
        })?;
        ensure!(
            actual == expected_rows,
            "{layer} marker rows for {source_id} are {actual}, expected {expected_rows}"
        );
    }
    Ok(())
}

fn remove_unexpected_source_outputs(
    layer_root: &Path,
    source_id: &str,
    expected: &[PathBuf],
) -> Result<()> {
    let expected: std::collections::BTreeSet<_> = expected.iter().collect();
    for path in collect_parquet_files(layer_root)? {
        if is_source_output(&path, source_id) && !expected.contains(&path) {
            fs::remove_file(&path)?;
        }
    }
    Ok(())
}

pub(crate) fn is_source_output(path: &Path, source_id: &str) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .and_then(|name| name.strip_prefix(source_id))
        .is_some_and(|suffix| suffix.starts_with(".part-") && suffix.ends_with(".parquet"))
}

fn validate_source_identity(manifest: &MarkerManifest) -> Result<()> {
    ensure!(
        !manifest.source.as_os_str().is_empty(),
        "marker source is empty"
    );
    ensure!(
        manifest.source_size_bytes > 0,
        "marker source size must be positive"
    );
    ensure!(
        manifest.source_sha256.len() == 64
            && manifest
                .source_sha256
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit()),
        "marker source SHA-256 must contain exactly 64 hexadecimal characters"
    );
    Ok(())
}

fn relative_path(data_dir: &Path, path: &Path) -> Result<PathBuf> {
    let relative = path.strip_prefix(data_dir).with_context(|| {
        format!(
            "marker path {} is outside data directory {}",
            path.display(),
            data_dir.display()
        )
    })?;
    ensure!(
        relative
            .components()
            .all(|component| matches!(component, Component::Normal(_))),
        "marker path is not a normalized relative path: {}",
        relative.display()
    );
    Ok(relative.to_path_buf())
}

fn path_to_string(path: &Path) -> Result<String> {
    path.to_str()
        .map(str::to_string)
        .context("marker path is not valid UTF-8")
}

fn analytical_root_identity(
    data_dir: &Path,
    analytical_root: &Path,
) -> Result<(Option<String>, PathBuf)> {
    let suffix = analytical_root
        .strip_prefix(data_dir.join(ANALYTICAL_DIRNAME))
        .context("analytical marker root is outside the analytical layer")?;
    let parts: Vec<_> = suffix.components().collect();
    if let Some(index) = parts
        .iter()
        .position(|part| part.as_os_str() == SNAPSHOTS_DIRNAME)
    {
        ensure!(
            index + 2 == parts.len(),
            "invalid snapshot analytical marker root {}",
            analytical_root.display()
        );
        let snapshot = parts[index + 1]
            .as_os_str()
            .to_str()
            .context("snapshot generation is not valid UTF-8")?;
        validate_snapshot_version(snapshot)?;
        Ok((Some(snapshot.to_string()), suffix.to_path_buf()))
    } else {
        Ok((None, suffix.to_path_buf()))
    }
}

pub fn sha256_file(path: &Path) -> Result<(u64, String)> {
    let mut file = File::open(path)?;
    prepare_sequential_read(&file);
    let mut hasher = Sha256::new();
    let mut buffer = vec![0_u8; 8 * 1024 * 1024];
    let mut bytes = 0_u64;
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        discard_file_cache(&file, bytes, read as u64);
        bytes = bytes
            .checked_add(u64::try_from(read)?)
            .context("source size overflow")?;
    }
    Ok((bytes, hex::encode(hasher.finalize())))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ParquetBatchSlice {
    offset: usize,
    rows: usize,
    completed_byte_range: Option<(u64, u64)>,
}

/// A projected Parquet reader that parses metadata once and advances through
/// physical row groups in file order. A large row group is subdivided without
/// rereading preceding groups, so live DataFrame state is capped by
/// `maximum_batch_rows` rather than the file's total row count.
pub(crate) struct SequentialParquetReader {
    path: PathBuf,
    file: File,
    metadata: FileMetadataRef,
    columns: Option<Vec<String>>,
    slices: Vec<ParquetBatchSlice>,
    next_slice: usize,
    rows: usize,
    bytes: u64,
    cache_released: bool,
}

impl SequentialParquetReader {
    pub(crate) fn new(
        path: &Path,
        columns: Option<Vec<String>>,
        maximum_batch_rows: usize,
    ) -> Result<Self> {
        ensure!(
            maximum_batch_rows > 0,
            "sequential Parquet batch size must be positive"
        );
        let file = File::open(path)
            .with_context(|| format!("failed to open Parquet input {}", path.display()))?;
        prepare_sequential_read(&file);
        let bytes = file.metadata()?.len();
        let mut metadata_reader = ParquetReader::new(file.try_clone()?);
        let metadata = metadata_reader
            .get_metadata()
            .with_context(|| format!("unreadable Parquet footer in {}", path.display()))?
            .clone();
        let rows = metadata.num_rows;
        let mut slices = Vec::new();
        let mut offset = 0usize;
        for row_group in &metadata.row_groups {
            let row_group_rows = row_group.num_rows();
            let byte_range = row_group.full_byte_range();
            let byte_length = byte_range
                .end
                .checked_sub(byte_range.start)
                .context("invalid Parquet row-group byte range")?;
            let mut remaining = row_group_rows;
            while remaining > 0 {
                let batch_rows = remaining.min(maximum_batch_rows);
                remaining -= batch_rows;
                slices.push(ParquetBatchSlice {
                    offset,
                    rows: batch_rows,
                    completed_byte_range: (remaining == 0)
                        .then_some((byte_range.start, byte_length)),
                });
                offset = offset
                    .checked_add(batch_rows)
                    .context("Parquet batch row offset overflow")?;
            }
        }
        ensure!(
            offset == rows,
            "Parquet row-group totals disagree with footer"
        );
        Ok(Self {
            path: path.to_path_buf(),
            file,
            metadata,
            columns,
            slices,
            next_slice: 0,
            rows,
            bytes,
            cache_released: false,
        })
    }

    pub(crate) fn rows(&self) -> usize {
        self.rows
    }

    pub(crate) fn schema_frame(&self) -> Result<DataFrame> {
        self.read_slice(0, 0)
    }

    pub(crate) fn set_projection(&mut self, columns: Vec<String>) {
        self.columns = Some(columns);
    }

    pub(crate) fn next_batch(&mut self) -> Result<Option<DataFrame>> {
        let Some(slice) = self.slices.get(self.next_slice).copied() else {
            self.release_cache();
            return Ok(None);
        };
        let frame = self.read_slice(slice.offset, slice.rows)?;
        ensure!(
            frame.height() == slice.rows,
            "short sequential Parquet read"
        );
        self.next_slice += 1;
        if let Some((offset, length)) = slice.completed_byte_range {
            discard_file_cache(&self.file, offset, length);
        }
        Ok(Some(frame))
    }

    fn read_slice(&self, offset: usize, rows: usize) -> Result<DataFrame> {
        let mut reader = ParquetReader::new(self.file.try_clone()?)
            .with_columns(self.columns.clone())
            .with_slice(Some((offset, rows)))
            .set_low_memory(true)
            .read_parallel(ParallelStrategy::None);
        reader.set_metadata(self.metadata.clone());
        reader
            .finish()
            .map_err(anyhow::Error::from)
            .with_context(|| format!("failed reading Parquet input {}", self.path.display()))
    }

    fn release_cache(&mut self) {
        if !self.cache_released {
            discard_file_cache(&self.file, 0, self.bytes);
            self.cache_released = true;
        }
    }
}

impl Drop for SequentialParquetReader {
    fn drop(&mut self) {
        self.release_cache();
    }
}

/// Tell Linux that a file is being consumed sequentially and that completed
/// ranges need not remain in the cgroup's page cache. These are best-effort
/// performance hints: hashing and durability never depend on kernel support.
#[cfg(target_os = "linux")]
pub(crate) fn prepare_sequential_read(file: &File) {
    let _ = rustix::fs::fadvise(file, 0, None, rustix::fs::Advice::Sequential);
}

#[cfg(not(target_os = "linux"))]
pub(crate) fn prepare_sequential_read(_file: &File) {}

#[cfg(target_os = "linux")]
pub(crate) fn discard_file_cache(file: &File, offset: u64, length: u64) {
    let _ = rustix::fs::fadvise(
        file,
        offset,
        NonZeroU64::new(length),
        rustix::fs::Advice::DontNeed,
    );
}

#[cfg(not(target_os = "linux"))]
pub(crate) fn discard_file_cache(_file: &File, _offset: u64, _length: u64) {}

/// Release cache pages for a file that has already been fully consumed.
/// Opening a second descriptor is intentional: readers such as Polars take
/// ownership of their `File`, so the original descriptor is no longer
/// available after materialization. Cache advice remains best-effort and can
/// never turn a successful data operation into a failure.
pub(crate) fn discard_path_cache(path: &Path) {
    if let Ok(file) = File::open(path) {
        let length = file.metadata().map(|metadata| metadata.len()).unwrap_or(0);
        discard_file_cache(&file, 0, length);
    }
}

/// Release completed Parquet input pages below a partition directory.
#[cfg(test)]
pub(crate) fn discard_parquet_cache_in_dir(dir: &Path) {
    if let Ok(files) = collect_parquet_files(dir) {
        for path in files {
            discard_path_cache(&path);
        }
    }
}

#[cfg(test)]
pub fn read_marker_manifest(
    data_dir: &Path,
    wiki: &str,
    source_id: &str,
) -> Result<Option<MarkerManifest>> {
    read_marker_manifest_in(data_dir, &analytical_wiki_dir(data_dir, wiki), source_id)
}

pub(crate) fn read_marker_manifest_in(
    data_dir: &Path,
    analytical_root: &Path,
    source_id: &str,
) -> Result<Option<MarkerManifest>> {
    let marker = marker_path_in(analytical_root, source_id);
    if !marker.exists() {
        return Ok(None);
    }

    let stored: StoredMarkerManifest = serde_json::from_slice(&fs::read(&marker)?)
        .with_context(|| format!("invalid ingest marker JSON in {}", marker.display()))?;
    ensure!(
        matches!(stored.schema_version, 1 | MARKER_SCHEMA_VERSION),
        "unsupported ingest marker schema in {}",
        marker.display()
    );
    ensure!(
        stored.source_id == source_id,
        "ingest marker source ID mismatch"
    );
    if let Some(snapshot) = stored.snapshot_version.as_deref() {
        validate_snapshot_version(snapshot)?;
    }
    Ok(Some(MarkerManifest {
        snapshot_version: stored.snapshot_version,
        source: checked_stored_path(data_dir, &stored.source.path)?,
        source_size_bytes: stored.source.size_bytes,
        source_sha256: stored.source.sha256,
        rows: usize::try_from(stored.rows)?,
        allow_empty: stored.allow_empty,
        analytical_paths: stored
            .analytical_outputs
            .iter()
            .map(|output| checked_stored_path(data_dir, &output.path))
            .collect::<Result<_>>()?,
        warehouse_paths: stored
            .warehouse_outputs
            .iter()
            .map(|output| checked_stored_path(data_dir, &output.path))
            .collect::<Result<_>>()?,
        metric_input_paths: stored
            .metric_input_outputs
            .iter()
            .map(|output| checked_stored_path(data_dir, &output.path))
            .collect::<Result<_>>()?,
    }))
}

fn checked_stored_path(data_dir: &Path, value: &str) -> Result<PathBuf> {
    let path = Path::new(value);
    ensure!(
        path.components()
            .all(|component| matches!(component, Component::Normal(_))),
        "ingest marker contains unsafe path {value:?}"
    );
    Ok(data_dir.join(path))
}

#[cfg(test)]
pub fn marker_manifest_is_valid(data_dir: &Path, wiki: &str, source_id: &str) -> Result<bool> {
    marker_manifest_is_valid_in(data_dir, &analytical_wiki_dir(data_dir, wiki), source_id)
}

pub fn marker_manifest_is_valid_in(
    data_dir: &Path,
    analytical_root: &Path,
    source_id: &str,
) -> Result<bool> {
    let marker = marker_path_in(analytical_root, source_id);
    let stored = match read_stored_marker(&marker) {
        Ok(Some(stored)) => stored,
        Ok(None) => return Ok(false),
        Err(error) => {
            tracing::warn!(path = %marker.display(), error = %error, "invalid ingest marker");
            return Ok(false);
        }
    };
    if !matches!(stored.schema_version, 1 | MARKER_SCHEMA_VERSION) || stored.source_id != source_id
    {
        return Ok(false);
    }
    let Ok((expected_snapshot, analytical_suffix)) =
        analytical_root_identity(data_dir, analytical_root)
    else {
        return Ok(false);
    };
    if stored.snapshot_version != expected_snapshot
        || Path::new(&stored.source.path)
            .file_name()
            .and_then(|name| name.to_str())
            != Some(format!("{source_id}.tsv.bz2").as_str())
    {
        return Ok(false);
    }
    if stored
        .snapshot_version
        .as_deref()
        .is_some_and(|snapshot| validate_snapshot_version(snapshot).is_err())
        || stored.source.size_bytes == 0
        || stored.source.sha256.len() != 64
        || !stored
            .source
            .sha256
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
        || (stored.rows == 0 && (!stored.allow_empty || stored.snapshot_version.is_some()))
    {
        return Ok(false);
    }
    let source_path = match checked_stored_path(data_dir, &stored.source.path) {
        Ok(path) => path,
        Err(_) => return Ok(false),
    };
    if source_path.exists() {
        let identity = sha256_file(&source_path)?;
        if identity != (stored.source.size_bytes, stored.source.sha256.clone()) {
            return Ok(false);
        }
    }
    let warehouse_root = data_dir.join(WAREHOUSE_DIRNAME).join(&analytical_suffix);
    let metric_input_root = data_dir.join(METRIC_INPUT_DIRNAME).join(&analytical_suffix);
    let require_exact_source_inventory = stored.snapshot_version.is_none();
    let legacy_layout =
        !stored.analytical_outputs.is_empty() || !stored.warehouse_outputs.is_empty();
    let metric_input_layout = !stored.metric_input_outputs.is_empty();
    if stored.schema_version == 1 && metric_input_layout {
        return Ok(false);
    }
    if stored.rows > 0
        && (legacy_layout == metric_input_layout
            || (legacy_layout
                && (stored.analytical_outputs.is_empty() || stored.warehouse_outputs.is_empty())))
    {
        return Ok(false);
    }
    let outputs_valid = if metric_input_layout {
        stored.analytical_outputs.is_empty()
            && stored.warehouse_outputs.is_empty()
            && validate_stored_outputs(
                data_dir,
                &metric_input_root,
                source_id,
                stored.rows,
                &stored.metric_input_outputs,
                require_exact_source_inventory,
            )
    } else {
        stored.metric_input_outputs.is_empty()
            && validate_stored_outputs(
                data_dir,
                analytical_root,
                source_id,
                stored.rows,
                &stored.analytical_outputs,
                require_exact_source_inventory,
            )
            && validate_stored_outputs(
                data_dir,
                &warehouse_root,
                source_id,
                stored.rows,
                &stored.warehouse_outputs,
                require_exact_source_inventory,
            )
    };
    if !outputs_valid {
        return Ok(false);
    }
    Ok(true)
}

/// Return true only when a strict ingest marker validates and names this exact
/// source path. Cleanup callers need the path check because a source ID alone
/// does not prove which raw-file instance the marker covered.
pub fn marker_manifest_covers_source_in(
    data_dir: &Path,
    analytical_root: &Path,
    source_id: &str,
    source_path: &Path,
) -> bool {
    let marker = marker_path_in(analytical_root, source_id);
    let stored = match read_stored_marker(&marker) {
        Ok(Some(stored)) => stored,
        Ok(None) => return false,
        Err(error) => {
            tracing::warn!(path = %marker.display(), error = %error, "invalid ingest marker");
            return false;
        }
    };
    let stored_source = match checked_stored_path(data_dir, &stored.source.path) {
        Ok(path) => path,
        Err(_) => return false,
    };
    if stored_source != source_path {
        return false;
    }
    marker_manifest_is_valid_in(data_dir, analytical_root, source_id).unwrap_or(false)
}

fn read_stored_marker(path: &Path) -> Result<Option<StoredMarkerManifest>> {
    if !path.exists() {
        return Ok(None);
    }
    Ok(Some(serde_json::from_slice(&fs::read(path)?)?))
}

fn validate_stored_outputs(
    data_dir: &Path,
    layer_root: &Path,
    source_id: &str,
    expected_rows: u64,
    outputs: &[StoredOutput],
    require_exact_source_inventory: bool,
) -> bool {
    if expected_rows > 0 && outputs.is_empty() {
        return false;
    }
    let mut recorded = std::collections::BTreeSet::new();
    let mut actual_total = 0_u64;
    let mut footer_rows = Vec::with_capacity(outputs.len());
    for output in outputs {
        let Ok(path) = checked_stored_path(data_dir, &output.path) else {
            return false;
        };
        if !path.starts_with(layer_root)
            || !path
                .extension()
                .is_some_and(|extension| extension == "parquet")
            || !path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with(source_id))
        {
            return false;
        }
        let Some(total) = actual_total.checked_add(output.rows) else {
            return false;
        };
        actual_total = total;
        let Ok(file) = File::open(&path) else {
            return false;
        };
        let Ok(rows) = ParquetReader::new(file).num_rows() else {
            return false;
        };
        footer_rows.push((u64::try_from(rows).ok(), output.rows));
        recorded.insert(path);
    }
    let recorded_outputs_are_valid = footer_rows
        .iter()
        .all(|(actual, stored)| *actual == Some(*stored))
        && recorded.len() == outputs.len()
        && actual_total == expected_rows;
    if !recorded_outputs_are_valid || !require_exact_source_inventory {
        return recorded_outputs_are_valid;
    }
    let discovered: std::collections::BTreeSet<_> = collect_parquet_files(layer_root)
        .unwrap_or_default()
        .into_iter()
        .filter(|path| is_source_output(path, source_id))
        .collect();
    recorded == discovered
}

pub fn month_partition_dir(root: &Path, year: i32, year_month: &str) -> PathBuf {
    root.join(format!("year={year}"))
        .join(format!("year_month={year_month}"))
}

pub fn collect_parquet_files(root: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    collect_parquet_files_recursive(root, &mut files)?;
    files.sort();
    Ok(files)
}

fn collect_parquet_files_recursive(root: &Path, files: &mut Vec<PathBuf>) -> Result<()> {
    if !root.exists() {
        return Ok(());
    }

    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let path = entry.path();
        if entry.file_type()?.is_dir() {
            if path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with('_'))
            {
                continue;
            }
            collect_parquet_files_recursive(&path, files)?;
        } else if path.extension().is_some_and(|ext| ext == "parquet") {
            files.push(path);
        }
    }

    Ok(())
}

pub fn collect_partition_specs(root: &Path) -> Result<Vec<PartitionSpec>> {
    let mut partitions: BTreeMap<(i32, String), PathBuf> = BTreeMap::new();
    collect_partition_specs_recursive(root, &mut partitions)?;
    let mut specs = Vec::with_capacity(partitions.len());
    for ((year, year_month), dir) in partitions {
        let files = collect_parquet_files(&dir)?;
        specs.push(PartitionSpec {
            year,
            year_month,
            dir,
            files,
        });
    }
    Ok(specs)
}

pub(crate) fn active_partition_specs(
    data_dir: &Path,
    wiki: &str,
    layer: GenerationLayer,
) -> Result<Vec<PartitionSpec>> {
    let root = active_layer_wiki_dir(data_dir, wiki, layer)?;
    if current_snapshot_version(data_dir, wiki)?.is_none() {
        return collect_partition_specs(&root);
    }
    let files = active_fragment_files(data_dir, wiki, layer)?;
    partition_specs_from_generation_files(&root, files)
}

fn partition_specs_from_generation_files(
    root: &Path,
    files: Vec<PathBuf>,
) -> Result<Vec<PartitionSpec>> {
    let mut partitions: BTreeMap<(i32, String, PathBuf), Vec<PathBuf>> = BTreeMap::new();
    for file in files {
        let relative = file
            .strip_prefix(root)
            .context("active generation fragment is outside its layer root")?;
        let parts: Vec<_> = relative.components().collect();
        let parts = if parts
            .first()
            .is_some_and(|component| component.as_os_str() == "_compacted")
        {
            &parts[1..]
        } else {
            &parts[..]
        };
        ensure!(
            parts.len() == 3,
            "generation fragment has an invalid partition path"
        );
        let year = parts[0]
            .as_os_str()
            .to_str()
            .and_then(|value| value.strip_prefix("year="))
            .and_then(|value| value.parse::<i32>().ok())
            .with_context(|| format!("invalid fragment year partition: {}", file.display()))?;
        let year_month = parts[1]
            .as_os_str()
            .to_str()
            .and_then(|value| value.strip_prefix("year_month="))
            .with_context(|| format!("invalid fragment month partition: {}", file.display()))?
            .to_string();
        let dir = file
            .parent()
            .context("generation fragment has no parent")?
            .to_path_buf();
        partitions
            .entry((year, year_month, dir))
            .or_default()
            .push(file);
    }
    Ok(partitions
        .into_iter()
        .map(|((year, year_month, dir), mut files)| {
            files.sort();
            PartitionSpec {
                year,
                year_month,
                dir,
                files,
            }
        })
        .collect())
}

pub(crate) fn snapshot_partition_specs(
    data_dir: &Path,
    wiki: &str,
    snapshot_version: &str,
    layer: GenerationLayer,
) -> Result<Vec<PartitionSpec>> {
    let root = fragment_layer_root(data_dir, wiki, snapshot_version, layer)?;
    let files = snapshot_fragment_files(data_dir, wiki, snapshot_version, layer)?;
    partition_specs_from_generation_files(&root, files)
}

fn collect_partition_specs_recursive(
    root: &Path,
    partitions: &mut BTreeMap<(i32, String), PathBuf>,
) -> Result<()> {
    if !root.exists() {
        return Ok(());
    }

    for year_entry in fs::read_dir(root)? {
        let year_entry = year_entry?;
        if !year_entry.file_type()?.is_dir() {
            continue;
        }
        let year_path = year_entry.path();
        let year_name = year_path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("");
        if year_name == MARKERS_DIRNAME {
            continue;
        }
        let Some(year) = year_name
            .strip_prefix("year=")
            .and_then(|value| value.parse().ok())
        else {
            continue;
        };

        for month_entry in fs::read_dir(&year_path)? {
            let month_entry = month_entry?;
            if !month_entry.file_type()?.is_dir() {
                continue;
            }
            let month_path = month_entry.path();
            let month_name = month_path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("");
            let Some(year_month) = month_name.strip_prefix("year_month=") else {
                continue;
            };

            partitions.insert((year, year_month.to_string()), month_path);
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::TestDir;
    use polars::prelude::*;
    use std::os::unix::fs::PermissionsExt;

    type MarkerMutation = Box<dyn FnOnce(&mut Value)>;

    #[test]
    fn sequential_parquet_reader_projects_and_advances_through_row_groups() -> Result<()> {
        let directory = TestDir::new()?;
        let path = directory.path().join("sequential.parquet");
        let mut frame = df!(
            "key" => &[1_i64, 2, 3, 4, 5],
            "payload" => &["a", "b", "c", "d", "e"],
        )
        .expect("sequential reader fixture columns have equal lengths");
        ParquetWriter::new(File::create(&path)?)
            .with_row_group_size(Some(2))
            .finish(&mut frame)?;

        let mut reader = SequentialParquetReader::new(&path, Some(vec!["key".to_string()]), 1)?;
        assert_eq!(reader.rows(), 5);
        let schema = reader.schema_frame()?;
        assert_eq!(schema.width(), 1);
        assert_eq!(schema.get_column_names()[0].as_str(), "key");
        let mut values = Vec::new();
        while let Some(batch) = reader.next_batch()? {
            assert!(batch.height() <= 1);
            values.extend(batch.column("key")?.i64()?.into_no_null_iter());
        }
        assert_eq!(values, [1, 2, 3, 4, 5]);
        assert!(reader.next_batch()?.is_none());
        Ok(())
    }

    #[test]
    fn sequential_parquet_reader_handles_empty_and_invalid_inputs() -> Result<()> {
        let directory = TestDir::new()?;
        let empty_path = directory.path().join("empty.parquet");
        let mut empty = df!("key" => Vec::<i64>::new())?;
        ParquetWriter::new(File::create(&empty_path)?).finish(&mut empty)?;
        let mut empty_reader = SequentialParquetReader::new(&empty_path, None, 2)?;
        assert_eq!(empty_reader.rows(), 0);
        let schema = empty_reader.schema_frame()?;
        assert_eq!(schema.width(), 1);
        assert_eq!(schema.get_column_names()[0].as_str(), "key");
        assert!(empty_reader.next_batch()?.is_none());

        assert!(SequentialParquetReader::new(&empty_path, None, 0).is_err());
        let corrupt_path = directory.path().join("corrupt.parquet");
        fs::write(&corrupt_path, b"not parquet")?;
        assert!(SequentialParquetReader::new(&corrupt_path, None, 2).is_err());
        assert!(
            SequentialParquetReader::new(&directory.path().join("missing.parquet"), None, 2)
                .is_err()
        );
        Ok(())
    }

    fn marker_fixture(
        root: &Path,
        wiki: &str,
        source_id: &str,
        rows: usize,
    ) -> Result<MarkerManifest> {
        let source = root
            .join("raw")
            .join(wiki)
            .join(format!("{source_id}.tsv.bz2"));
        source.parent().map(fs::create_dir_all).transpose()?;
        fs::write(&source, b"compressed-source-fixture")?;
        let analytical = analytical_wiki_dir(root, wiki)
            .join("year=2024/year_month=2024-01")
            .join(format!("{source_id}.part-00000.parquet"));
        let warehouse = warehouse_wiki_dir(root, wiki)
            .join("year=2024/year_month=2024-01")
            .join(format!("{source_id}.part-00000.parquet"));
        let mut paths = Vec::new();
        if rows > 0 {
            for path in [&analytical, &warehouse] {
                path.parent().map(fs::create_dir_all).transpose()?;
                let mut frame = DataFrame::new_infer_height(vec![Column::new(
                    "row".into(),
                    (0..i64::try_from(rows).expect("fixture row count fits i64"))
                        .collect::<Vec<_>>(),
                )])
                .expect("marker fixture frame should be valid");
                ParquetWriter::new(File::create(path)?).finish(&mut frame)?;
                paths.push(path.clone());
            }
        }
        let (source_size_bytes, source_sha256) = sha256_file(&source)?;
        Ok(MarkerManifest {
            snapshot_version: None,
            source,
            source_size_bytes,
            source_sha256,
            rows,
            allow_empty: rows == 0,
            analytical_paths: paths.first().cloned().into_iter().collect(),
            warehouse_paths: paths.get(1).cloned().into_iter().collect(),
            metric_input_paths: Vec::new(),
        })
    }

    fn rewrite_marker(
        marker: &Path,
        original: &[u8],
        mutate: impl FnOnce(&mut Value),
    ) -> Result<()> {
        let mut value: Value = serde_json::from_slice(original)?;
        mutate(&mut value);
        fs::write(marker, serde_json::to_vec(&value)?)?;
        Ok(())
    }

    #[test]
    fn collect_parquet_files_recurses_and_skips_markers() -> Result<()> {
        let temp_dir = TestDir::new()?;
        let root = temp_dir.path();
        fs::create_dir_all(root.join("_markers"))?;
        fs::create_dir_all(root.join("_snapshots").join("2026-07"))?;
        fs::create_dir_all(root.join("year=2024").join("year_month=2024-01"))?;
        fs::write(root.join("_markers").join("skip.parquet"), b"")?;
        fs::write(
            root.join("_snapshots").join("2026-07").join("skip.parquet"),
            b"",
        )
        .expect("snapshot fixture should be writable");
        let parquet_path = root
            .join("year=2024")
            .join("year_month=2024-01")
            .join("part-0.parquet");
        fs::write(parquet_path, b"")?;

        let files = collect_parquet_files(root)?;
        assert_eq!(files.len(), 1);
        assert!(files[0].ends_with("part-0.parquet"));

        discard_parquet_cache_in_dir(root);
        assert!(files[0].is_file());
        discard_path_cache(&root.join("missing.parquet"));
        discard_parquet_cache_in_dir(&files[0]);
        Ok(())
    }

    #[test]
    fn snapshot_pointer_atomically_selects_generation_and_rejects_incomplete_publish() -> Result<()>
    {
        let temp_dir = TestDir::new()?;
        let root = temp_dir.path();
        let wiki = "nlwiki";
        assert_eq!(current_snapshot_version(root, wiki)?, None);
        assert_eq!(
            active_analytical_wiki_dir(root, wiki)?,
            analytical_wiki_dir(root, wiki)
        );
        assert_eq!(
            active_warehouse_wiki_dir(root, wiki)?,
            warehouse_wiki_dir(root, wiki)
        );
        assert!(snapshot_analytical_wiki_dir(root, wiki, "2026/07").is_err());
        assert!(snapshot_warehouse_wiki_dir(root, wiki, "2026-13").is_err());

        let analytical = snapshot_analytical_wiki_dir(root, wiki, "2026-07")?;
        fs::create_dir_all(&analytical)?;
        assert!(publish_current_snapshot(root, wiki, "2026-07").is_err());
        let warehouse = snapshot_warehouse_wiki_dir(root, wiki, "2026-07")?;
        fs::create_dir_all(&warehouse)?;

        publish_test_snapshot_pointer(root, wiki, "2026-07")?;

        assert_eq!(
            current_snapshot_version(root, wiki)?.as_deref(),
            Some("2026-07")
        );
        assert_eq!(active_analytical_wiki_dir(root, wiki)?, analytical);
        assert_eq!(active_warehouse_wiki_dir(root, wiki)?, warehouse);
        let pointer = fs::read_to_string(snapshot_pointer_path(root, wiki))?;
        assert!(pointer.ends_with('\n'));
        Ok(())
    }

    #[test]
    fn snapshot_repair_validates_the_complete_generation_before_repointing() -> Result<()> {
        let temp_dir = TestDir::new()?;
        let root = temp_dir.path();
        let wiki = "enwiki";
        let version = "2001-01";
        let analytical = snapshot_analytical_wiki_dir(root, wiki, version)?;
        write_test_marker_in(root, &analytical, "2001-01.enwiki.2001-01")?;
        assert!(repair_current_snapshot(root, wiki, version).is_err());
        write_test_marker_in(root, &analytical, "2001-01.enwiki.2001-02")?;

        fs::create_dir_all(
            snapshot_pointer_path(root, wiki)
                .parent()
                .expect("pointer parent"),
        )
        .expect("pointer parent should be created");
        fs::write(snapshot_pointer_path(root, wiki), b"{truncated")?;
        assert_eq!(repair_current_snapshot(root, wiki, version)?, 2);
        assert_eq!(
            current_snapshot_version(root, wiki)?.as_deref(),
            Some(version)
        );

        let marker_dir = analytical.join(MARKERS_DIRNAME);
        fs::create_dir(marker_dir.join("unexpected-directory"))?;
        assert!(repair_current_snapshot(root, wiki, version).is_err());
        fs::remove_dir(marker_dir.join("unexpected-directory"))?;
        fs::write(marker_dir.join("unexpected-name"), b"not-a-marker")?;
        assert!(repair_current_snapshot(root, wiki, version).is_err());
        fs::remove_file(marker_dir.join("unexpected-name"))?;

        let stray = analytical.join("year=2026/year_month=2026-01/stray.parquet");
        fs::copy(collect_parquet_files(&analytical)?.remove(0), &stray)?;
        assert!(repair_current_snapshot(root, wiki, version).is_err());
        fs::remove_file(stray)?;
        fs::write(
            marker_path_in(&analytical, "2001-01.enwiki.2001-01"),
            b"{truncated",
        )
        .expect("marker corruption fixture should be written");
        assert!(repair_current_snapshot(root, wiki, version).is_err());
        Ok(())
    }

    #[test]
    fn snapshot_pointer_validation_fails_closed() -> Result<()> {
        let temp_dir = TestDir::new()?;
        let root = temp_dir.path();
        let wiki = "nlwiki";
        let pointer = snapshot_pointer_path(root, wiki);
        pointer.parent().map(fs::create_dir_all).transpose()?;

        for invalid in [
            "not json",
            r#"{"schema_version":2,"wiki":"nlwiki","snapshot_version":"2026-07"}"#,
            r#"{"schema_version":1,"wiki":"frwiki","snapshot_version":"2026-07"}"#,
            r#"{"schema_version":1,"wiki":"nlwiki"}"#,
            r#"{"schema_version":1,"wiki":"nlwiki","snapshot_version":"2026-00"}"#,
        ] {
            fs::write(&pointer, invalid)?;
            assert!(current_snapshot_version(root, wiki).is_err());
        }
        Ok(())
    }

    #[test]
    fn generation_manifest_is_deterministic_authoritative_and_fail_closed() -> Result<()> {
        let data = TestDir::new()?;
        let wiki = "testwiki";
        let snapshot = "2026-08";
        let (plan, _) =
            crate::snapshot_plan::SnapshotPlan::load_or_resolve(data.path(), wiki, snapshot)?;
        let analytical = snapshot_analytical_wiki_dir(data.path(), wiki, snapshot)?;
        write_test_marker_in(data.path(), &analytical, &plan.sources[0].source_id)?;

        let manifest_path = write_generation_manifest(data.path(), wiki, snapshot)?;
        let first_bytes = fs::read(&manifest_path)?;
        write_generation_manifest(data.path(), wiki, snapshot)?;
        assert_eq!(fs::read(&manifest_path)?, first_bytes);
        let manifest = read_generation_manifest(data.path(), wiki, snapshot)?;
        assert_eq!(manifest.fragments.len(), 2);
        assert!(manifest.fragments.iter().all(|fragment| {
            fragment.bytes > 0 && fragment.sha256.len() == 64 && fragment.rows == 1
        }));

        let mut unsupported: Value = serde_json::from_slice(&first_bytes)?;
        unsupported["schema_version"] = json!(3);
        fs::write(&manifest_path, serde_json::to_vec(&unsupported)?)?;
        assert!(read_generation_manifest(data.path(), wiki, snapshot).is_err());
        fs::write(&manifest_path, &first_bytes)?;

        publish_current_snapshot(data.path(), wiki, snapshot)?;
        assert_eq!(
            write_generation_manifest(data.path(), wiki, snapshot)?,
            manifest_path
        );
        fs::write(&manifest_path, b"{}\n")?;
        let immutable = write_generation_manifest(data.path(), wiki, snapshot)
            .expect_err("selected manifest must not be replaced with different bytes");
        assert!(immutable.to_string().contains("immutable"));
        fs::write(&manifest_path, &first_bytes)?;
        let listed = active_fragment_files(data.path(), wiki, GenerationLayer::Analytical)?;
        let stray =
            listed[0].with_file_name(format!("{}.part-99999.parquet", plan.sources[0].source_id));
        fs::copy(&listed[0], &stray)?;
        assert_eq!(
            active_fragment_files(data.path(), wiki, GenerationLayer::Analytical)?,
            listed
        );

        fs::write(&manifest_path, b"{truncated")?;
        assert!(active_fragment_files(data.path(), wiki, GenerationLayer::Analytical).is_err());
        Ok(())
    }

    #[test]
    fn receipted_generation_fast_path_rejects_every_control_plane_mismatch() -> Result<()> {
        let data = TestDir::new()?;
        let wiki = "testwiki";
        let snapshot = "2026-08";
        let (plan, plan_path) =
            crate::snapshot_plan::SnapshotPlan::load_or_resolve(data.path(), wiki, snapshot)?;
        let analytical = snapshot_analytical_wiki_dir(data.path(), wiki, snapshot)?;
        write_test_marker_in(data.path(), &analytical, &plan.sources[0].source_id)?;
        let manifest_path = write_generation_manifest(data.path(), wiki, snapshot)?;
        let receipt_path =
            crate::fingerprint::data_stage_receipt_path(data.path(), wiki, snapshot, "ingest");
        let spec = crate::fingerprint::StageSpec {
            stage: "ingest",
            scope: wiki,
            selected_snapshot: Some(snapshot),
            algorithm_version: crate::ingest::INGEST_ALGORITHM_VERSION,
        };
        let record = |input_identity: &str, output_identity: &str| {
            crate::fingerprint::record(
                &receipt_path,
                spec,
                &[crate::fingerprint::TrackedPath::new(
                    input_identity,
                    &plan_path,
                )],
                &[crate::fingerprint::TrackedPath::new(
                    output_identity,
                    &manifest_path,
                )],
            )
        };

        record("snapshot-plan", "wrong-manifest")?;
        assert!(read_receipted_generation_manifest(data.path(), wiki, snapshot)?.is_none());

        record("snapshot-plan", "generation-manifest")?;
        let manifest_bytes = fs::read(&manifest_path)?;
        fs::write(&manifest_path, b"changed-generation-manifest")?;
        assert!(read_receipted_generation_manifest(data.path(), wiki, snapshot)?.is_none());
        fs::write(&manifest_path, &manifest_bytes)?;

        record("wrong-plan", "generation-manifest")?;
        assert!(read_receipted_generation_manifest(data.path(), wiki, snapshot)?.is_none());

        record("snapshot-plan", "generation-manifest")?;
        let plan_bytes = fs::read(&plan_path)?;
        let mut reformatted_plan = plan_bytes.clone();
        reformatted_plan.push(b'\n');
        fs::write(&plan_path, reformatted_plan)?;
        assert!(read_receipted_generation_manifest(data.path(), wiki, snapshot)?.is_none());
        fs::write(&plan_path, &plan_bytes)?;

        record("snapshot-plan", "generation-manifest")?;
        let mut unreadable_permissions = fs::metadata(&manifest_path)?.permissions();
        unreadable_permissions.set_mode(0o000);
        fs::set_permissions(&manifest_path, unreadable_permissions)?;
        assert!(read_receipted_generation_manifest(data.path(), wiki, snapshot).is_err());
        let mut readable_permissions = fs::metadata(&manifest_path)?.permissions();
        readable_permissions.set_mode(0o600);
        fs::set_permissions(&manifest_path, readable_permissions)?;

        fs::write(&manifest_path, b"{invalid")?;
        record("snapshot-plan", "generation-manifest")?;
        assert!(read_receipted_generation_manifest(data.path(), wiki, snapshot).is_err());
        Ok(())
    }

    #[test]
    fn pre_manifest_generation_migrates_strictly_and_failed_commit_cleans_staging() -> Result<()> {
        let data = TestDir::new()?;
        let wiki = "testwiki";
        let snapshot = "2026-08";
        let (plan, _) =
            crate::snapshot_plan::SnapshotPlan::load_or_resolve(data.path(), wiki, snapshot)?;
        let analytical = snapshot_analytical_wiki_dir(data.path(), wiki, snapshot)?;
        write_test_marker_in(data.path(), &analytical, &plan.sources[0].source_id)?;
        publish_test_snapshot_pointer(data.path(), wiki, snapshot)?;
        let manifest = generation_manifest_path(data.path(), wiki, snapshot)?;
        assert!(!manifest.exists());

        assert_eq!(
            active_fragment_files(data.path(), wiki, GenerationLayer::Warehouse)?.len(),
            1
        );
        assert!(manifest.is_file());

        fs::remove_file(&manifest)?;
        fs::create_dir(&manifest)?;
        assert!(write_generation_manifest(data.path(), wiki, snapshot).is_err());
        let parent = manifest.parent().context("manifest parent")?;
        assert!(fs::read_dir(parent)?.all(|entry| {
            !entry
                .expect("manifest staging entry")
                .file_name()
                .to_string_lossy()
                .ends_with(".tmp")
        }));
        Ok(())
    }

    #[test]
    fn failed_snapshot_pointer_write_cleans_temporary_file() -> Result<()> {
        let temp_dir = TestDir::new()?;
        let root = temp_dir.path();
        let wiki = "nlwiki";
        fs::create_dir_all(snapshot_analytical_wiki_dir(root, wiki, "2026-07")?)?;
        fs::create_dir_all(snapshot_warehouse_wiki_dir(root, wiki, "2026-07")?)?;
        let pointer = snapshot_pointer_path(root, wiki);
        let parent = pointer.parent().context("pointer parent")?;
        fs::create_dir_all(parent)?;
        let temp = parent.join(format!(
            ".{CURRENT_SNAPSHOT_FILENAME}.{}.tmp",
            std::process::id()
        ));
        fs::create_dir(&temp)?;

        assert!(publish_test_snapshot_pointer(root, wiki, "2026-07").is_err());
        assert!(temp.is_dir());
        assert!(!pointer.exists());
        Ok(())
    }

    #[test]
    fn retire_inactive_snapshots_preserves_current_and_removes_legacy_data() -> Result<()> {
        let temp_dir = TestDir::new()?;
        let root = temp_dir.path();
        let wiki = "nlwiki";
        assert_eq!(retire_inactive_snapshots(root, wiki)?, 0);

        for version in ["2026-06", "2026-07"] {
            fs::create_dir_all(snapshot_analytical_wiki_dir(root, wiki, version)?)?;
            fs::create_dir_all(snapshot_warehouse_wiki_dir(root, wiki, version)?)?;
        }
        for layer in [
            analytical_wiki_dir(root, wiki),
            warehouse_wiki_dir(root, wiki),
        ] {
            fs::create_dir_all(layer.join("year=2025"))?;
            fs::create_dir_all(layer.join(MARKERS_DIRNAME))?;
            fs::write(layer.join("keep.txt"), b"not snapshot data")?;
        }
        publish_test_snapshot_pointer(root, wiki, "2026-07")?;

        assert_eq!(retire_inactive_snapshots(root, wiki)?, 6);
        assert!(snapshot_analytical_wiki_dir(root, wiki, "2026-07")?.is_dir());
        assert!(snapshot_warehouse_wiki_dir(root, wiki, "2026-07")?.is_dir());
        assert!(!snapshot_analytical_wiki_dir(root, wiki, "2026-06")?.exists());
        assert!(!snapshot_warehouse_wiki_dir(root, wiki, "2026-06")?.exists());
        assert!(analytical_wiki_dir(root, wiki).join("keep.txt").is_file());
        assert!(warehouse_wiki_dir(root, wiki).join("keep.txt").is_file());
        Ok(())
    }

    #[test]
    fn collect_partition_specs_discovers_partition_dirs() -> Result<()> {
        let temp_dir = TestDir::new()?;
        let root = temp_dir.path();
        fs::create_dir_all(root.join("year=2024").join("year_month=2024-01"))?;
        fs::create_dir_all(root.join("year=2023").join("year_month=2023-12"))?;

        let partitions = collect_partition_specs(root)?;
        assert_eq!(partitions.len(), 2);
        assert_eq!(partitions[0].year, 2023);
        assert_eq!(partitions[0].year_month, "2023-12");
        assert_eq!(partitions[1].year, 2024);
        assert_eq!(partitions[1].year_month, "2024-01");
        Ok(())
    }

    #[test]
    fn collect_partition_specs_returns_empty_for_missing_root() -> Result<()> {
        let temp_dir = TestDir::new()?;
        let partitions = collect_partition_specs(&temp_dir.path().join("missing"))?;
        assert!(partitions.is_empty());
        Ok(())
    }

    #[test]
    fn collect_partition_specs_skips_invalid_entries() -> Result<()> {
        let temp_dir = TestDir::new()?;
        let root = temp_dir.path();
        fs::create_dir_all(root.join("_markers"))?;
        fs::write(root.join("root-file.txt"), b"ignored")?;
        fs::create_dir_all(root.join("year=bad"))?;
        fs::create_dir_all(root.join("year=2024"))?;
        fs::write(root.join("year=2024").join("month.txt"), b"ignored")?;
        fs::create_dir_all(root.join("year=2024").join("bad-month"))?;
        fs::create_dir_all(root.join("year=2024").join("year_month=2024-03"))?;

        let partitions = collect_partition_specs(root)?;
        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0].year, 2024);
        assert_eq!(partitions[0].year_month, "2024-03");
        Ok(())
    }

    #[test]
    fn marker_path_lives_under_analytical_markers_dir() {
        let marker = marker_path(Path::new("data"), "frwiki", "source");
        assert_eq!(
            marker,
            Path::new("data/parquet/frwiki/_markers/source.done")
        );
    }

    #[test]
    fn marker_manifest_round_trips_relative_paths() -> Result<()> {
        let temp_dir = TestDir::new()?;
        let manifest = marker_fixture(temp_dir.path(), "frwiki", "source", 12)?;

        write_marker_manifest(temp_dir.path(), "frwiki", "source", &manifest)?;
        let loaded = read_marker_manifest(temp_dir.path(), "frwiki", "source")?
            .expect("marker should exist");

        assert_eq!(loaded, manifest);
        Ok(())
    }

    #[test]
    fn marker_manifest_validation_requires_both_output_layers() -> Result<()> {
        let temp_dir = TestDir::new()?;
        let root = temp_dir.path();
        let manifest = marker_fixture(root, "frwiki", "source", 1)?;
        write_marker_manifest(root, "frwiki", "source", &manifest)?;
        assert!(marker_manifest_is_valid(root, "frwiki", "source")?);

        fs::remove_file(&manifest.warehouse_paths[0])?;
        assert!(!marker_manifest_is_valid(root, "frwiki", "source")?);
        Ok(())
    }

    #[test]
    fn marker_validation_detects_changed_source_and_parquet_row_counts() -> Result<()> {
        let temp_dir = TestDir::new()?;
        let root = temp_dir.path();
        let manifest = marker_fixture(root, "frwiki", "source", 1)?;
        write_marker_manifest(root, "frwiki", "source", &manifest)?;

        fs::write(&manifest.source, b"different-source-with-same-purpose")?;
        assert!(!marker_manifest_is_valid(root, "frwiki", "source")?);
        fs::write(&manifest.source, b"compressed-source-fixture")?;
        assert!(marker_manifest_is_valid(root, "frwiki", "source")?);
        fs::remove_file(&manifest.source)?;
        assert!(marker_manifest_is_valid(root, "frwiki", "source")?);
        fs::write(&manifest.source, b"compressed-source-fixture")?;

        let extra = manifest.analytical_paths[0].with_file_name("source.part-99999.parquet");
        fs::copy(&manifest.analytical_paths[0], &extra)?;
        assert!(!marker_manifest_is_valid(root, "frwiki", "source")?);
        fs::remove_file(extra)?;

        let mut changed = DataFrame::new_infer_height(vec![Column::new("row".into(), [1_i64, 2])])?;
        ParquetWriter::new(File::create(&manifest.analytical_paths[0])?).finish(&mut changed)?;
        assert!(!marker_manifest_is_valid(root, "frwiki", "source")?);
        Ok(())
    }

    #[test]
    fn marker_validation_rejects_every_identity_and_schema_mismatch() -> Result<()> {
        let temp_dir = TestDir::new()?;
        let root = temp_dir.path();
        let manifest = marker_fixture(root, "frwiki", "source", 1)?;
        let marker = write_marker_manifest(root, "frwiki", "source", &manifest)?;
        let original = fs::read(&marker)?;

        let mutations: Vec<MarkerMutation> = vec![
            Box::new(|value| value["schema_version"] = json!(3)),
            Box::new(|value| value["source_id"] = json!("different")),
            Box::new(|value| value["snapshot_version"] = json!("2026-13")),
            Box::new(|value| value["source"]["path"] = json!("raw/frwiki/different.tsv.bz2")),
            Box::new(|value| value["source"]["path"] = json!("../source.tsv.bz2")),
            Box::new(|value| value["source"]["size_bytes"] = json!(0)),
            Box::new(|value| value["source"]["sha256"] = json!("short")),
            Box::new(|value| value["source"]["sha256"] = json!("z".repeat(64))),
            Box::new(|value| {
                value["rows"] = json!(0);
                value["allow_empty"] = json!(false);
            }),
            Box::new(|value| value["unknown_field"] = json!(true)),
        ];
        for mutation in mutations {
            rewrite_marker(&marker, &original, mutation)?;
            assert!(!marker_manifest_is_valid(root, "frwiki", "source")?);
        }
        fs::write(&marker, &original)?;
        let outside_root = root.join("outside");
        let outside_marker = marker_path_in(&outside_root, "source");
        outside_marker
            .parent()
            .map(fs::create_dir_all)
            .transpose()?;
        fs::copy(&marker, outside_marker)?;
        assert!(!marker_manifest_is_valid_in(root, &outside_root, "source")?);
        Ok(())
    }

    #[test]
    fn marker_validation_rejects_unsafe_missing_and_unreadable_outputs() -> Result<()> {
        let temp_dir = TestDir::new()?;
        let root = temp_dir.path();
        let manifest = marker_fixture(root, "frwiki", "source", 1)?;
        let marker = write_marker_manifest(root, "frwiki", "source", &manifest)?;
        let original = fs::read(&marker)?;
        let stored: Value = serde_json::from_slice(&original)?;
        let valid_path = PathBuf::from(
            stored["analytical_outputs"][0]["path"]
                .as_str()
                .context("stored analytical path")?,
        );
        let missing = valid_path.with_file_name("source.part-99998.parquet");
        let corrupt = valid_path.with_file_name("source.part-99999.parquet");
        fs::write(root.join(&corrupt), b"not parquet")?;

        let mutations: Vec<MarkerMutation> = vec![
            Box::new(|value| value["analytical_outputs"] = json!([])),
            Box::new(|value| value["analytical_outputs"][0]["path"] = json!("../unsafe.parquet")),
            Box::new(|value| {
                value["analytical_outputs"][0]["path"] =
                    json!("warehouse/frwiki/year=2024/year_month=2024-01/source.part-00000.parquet")
            }),
            Box::new(|value| {
                value["analytical_outputs"][0]["path"] =
                    json!("parquet/frwiki/year=2024/year_month=2024-01/source.part-00000.bin")
            }),
            Box::new(|value| {
                value["analytical_outputs"][0]["path"] =
                    json!("parquet/frwiki/year=2024/year_month=2024-01/other.part-00000.parquet")
            }),
            Box::new(move |value| {
                value["analytical_outputs"][0]["path"] =
                    json!(missing.to_string_lossy().into_owned())
            }),
            Box::new(move |value| {
                value["analytical_outputs"][0]["path"] =
                    json!(corrupt.to_string_lossy().into_owned())
            }),
        ];
        for mutation in mutations {
            rewrite_marker(&marker, &original, mutation)?;
            assert!(!marker_manifest_is_valid(root, "frwiki", "source")?);
        }
        Ok(())
    }

    #[test]
    fn marker_writer_rejects_invalid_source_and_output_contracts() -> Result<()> {
        let temp_dir = TestDir::new()?;
        let root = temp_dir.path();
        let base = marker_fixture(root, "frwiki", "source", 1)?;

        let mut invalid = base.clone();
        invalid.source = PathBuf::new();
        assert!(write_marker_manifest(root, "frwiki", "source", &invalid).is_err());
        invalid = base.clone();
        invalid.source_size_bytes = 0;
        assert!(write_marker_manifest(root, "frwiki", "source", &invalid).is_err());
        invalid = base.clone();
        invalid.source_sha256 = "invalid".to_string();
        assert!(write_marker_manifest(root, "frwiki", "source", &invalid).is_err());
        invalid = base.clone();
        invalid.source = root.join("raw/frwiki/different.tsv.bz2");
        assert!(write_marker_manifest(root, "frwiki", "source", &invalid).is_err());
        invalid = base.clone();
        invalid.snapshot_version = Some("2026-07".to_string());
        assert!(write_marker_manifest(root, "frwiki", "source", &invalid).is_err());
        invalid = base.clone();
        invalid.analytical_paths = base.warehouse_paths.clone();
        assert!(write_marker_manifest(root, "frwiki", "source", &invalid).is_err());
        invalid = base.clone();
        let wrong_extension = base.analytical_paths[0].with_extension("bin");
        fs::copy(&base.analytical_paths[0], &wrong_extension)?;
        invalid.analytical_paths = vec![wrong_extension];
        assert!(write_marker_manifest(root, "frwiki", "source", &invalid).is_err());
        invalid = base.clone();
        invalid.rows = 2;
        assert!(write_marker_manifest(root, "frwiki", "source", &invalid).is_err());
        invalid = base;
        let outside = TestDir::new()?;
        invalid.source = outside.path().join("source.tsv.bz2");
        assert!(write_marker_manifest(root, "frwiki", "source", &invalid).is_err());
        invalid.source = root.join("nested/../source.tsv.bz2");
        assert!(write_marker_manifest(root, "frwiki", "source", &invalid).is_err());
        assert!(
            write_marker_manifest_in(
                root,
                &analytical_wiki_dir(root, "frwiki").join("_snapshots/2026-07/extra"),
                "source",
                &invalid,
            )
            .is_err()
        );
        Ok(())
    }

    #[test]
    fn marker_commit_removes_only_unrecorded_outputs_for_its_source() -> Result<()> {
        let temp_dir = TestDir::new()?;
        let root = temp_dir.path();
        let manifest = marker_fixture(root, "frwiki", "source", 1)?;
        let stale = manifest.analytical_paths[0].with_file_name("source.part-99999.parquet");
        let unrelated = manifest.analytical_paths[0].with_file_name("other.part-99999.parquet");
        fs::copy(&manifest.analytical_paths[0], &stale)?;
        fs::copy(&manifest.analytical_paths[0], &unrelated)?;

        write_marker_manifest(root, "frwiki", "source", &manifest)?;

        assert!(!stale.exists());
        assert!(unrelated.exists());
        Ok(())
    }

    #[test]
    fn output_validation_rejects_row_total_overflow() -> Result<()> {
        let temp_dir = TestDir::new()?;
        let root = temp_dir.path();
        let manifest = marker_fixture(root, "frwiki", "source", 1)?;
        let relative = manifest.analytical_paths[0]
            .strip_prefix(root)?
            .to_string_lossy()
            .into_owned();
        let outputs = vec![
            StoredOutput {
                path: relative.clone(),
                rows: u64::MAX,
            },
            StoredOutput {
                path: relative,
                rows: 1,
            },
        ];
        assert!(!validate_stored_outputs(
            root,
            &analytical_wiki_dir(root, "frwiki"),
            "source",
            u64::MAX,
            &outputs,
            true,
        ));
        Ok(())
    }

    #[test]
    fn strict_marker_reader_reports_missing_and_mismatched_receipts() -> Result<()> {
        let temp_dir = TestDir::new()?;
        let root = temp_dir.path();
        assert!(read_marker_manifest(root, "frwiki", "missing")?.is_none());
        let manifest = marker_fixture(root, "frwiki", "source", 1)?;
        let marker = write_marker_manifest(root, "frwiki", "source", &manifest)?;
        let original = fs::read(&marker)?;
        let mutations: [fn(&mut Value); 3] = [
            |value: &mut Value| value["schema_version"] = json!(3),
            |value: &mut Value| value["source_id"] = json!("other"),
            |value: &mut Value| value["snapshot_version"] = json!("2026-99"),
        ];
        for mutation in mutations {
            rewrite_marker(&marker, &original, mutation)?;
            assert!(read_marker_manifest(root, "frwiki", "source").is_err());
        }
        Ok(())
    }

    #[test]
    fn marker_manifest_allows_only_explicitly_empty_sources_without_outputs() -> Result<()> {
        let temp_dir = TestDir::new()?;
        let root = temp_dir.path();
        let manifest = marker_fixture(root, "frwiki", "source", 0)?;
        write_marker_manifest(root, "frwiki", "source", &manifest)?;

        assert!(marker_manifest_is_valid(root, "frwiki", "source")?);
        let mut forbidden = manifest;
        forbidden.allow_empty = false;
        assert!(write_marker_manifest(root, "frwiki", "forbidden", &forbidden).is_err());
        Ok(())
    }

    #[test]
    fn truncated_marker_is_invalid_never_a_zero_row_success() -> Result<()> {
        let temp_dir = TestDir::new()?;
        let root = temp_dir.path();
        let marker = marker_path(root, "frwiki", "source");
        marker.parent().map(fs::create_dir_all).transpose()?;
        fs::write(&marker, br#"{"schema_version":1,"rows":0"#)?;

        assert!(read_marker_manifest(root, "frwiki", "source").is_err());
        assert!(!marker_manifest_is_valid(root, "frwiki", "source")?);
        Ok(())
    }

    #[test]
    fn interrupted_marker_write_preserves_last_complete_receipt() -> Result<()> {
        let temp_dir = TestDir::new()?;
        let root = temp_dir.path();
        let manifest = marker_fixture(root, "frwiki", "source", 1)?;
        let marker = write_marker_manifest(root, "frwiki", "source", &manifest)?;
        let committed = fs::read(&marker)?;
        let staging = marker
            .parent()
            .context("marker parent")?
            .join(format!(".source.done.{}.tmp", std::process::id()));
        fs::create_dir(&staging)?;

        assert!(write_marker_manifest(root, "frwiki", "source", &manifest).is_err());
        assert_eq!(fs::read(&marker)?, committed);
        assert!(marker_manifest_is_valid(root, "frwiki", "source")?);
        Ok(())
    }

    #[test]
    fn marker_manifest_is_invalid_when_rows_exist_without_output_paths() -> Result<()> {
        let temp_dir = TestDir::new()?;
        let root = temp_dir.path();
        let mut manifest = marker_fixture(root, "frwiki", "source", 2)?;
        manifest.warehouse_paths.clear();
        assert!(write_marker_manifest(root, "frwiki", "source", &manifest).is_err());
        assert!(!marker_manifest_is_valid(root, "frwiki", "source")?);
        Ok(())
    }

    #[test]
    fn unversioned_metric_input_helpers_and_marker_contract_are_strict() -> Result<()> {
        let temp_dir = TestDir::new()?;
        let root = temp_dir.path();
        let wiki = "metricwiki";
        let source_id = "source";
        let source = root.join("raw/metricwiki/source.tsv.bz2");
        source.parent().map(fs::create_dir_all).transpose()?;
        fs::write(&source, b"metric-input-source")?;

        let metric_root = metric_input_wiki_dir(root, wiki);
        let output =
            month_partition_dir(&metric_root, 2026, "2026-01").join("source.part-00000.parquet");
        output.parent().map(fs::create_dir_all).transpose()?;
        let mut frame = df!("row" => &[1_i64])?;
        ParquetWriter::new(File::create(&output)?).finish(&mut frame)?;
        let (source_size_bytes, source_sha256) = sha256_file(&source)?;
        let marker_result = write_marker_manifest_in(
            root,
            &analytical_wiki_dir(root, wiki),
            source_id,
            &MarkerManifest {
                snapshot_version: None,
                source,
                source_size_bytes,
                source_sha256,
                rows: 1,
                allow_empty: false,
                analytical_paths: Vec::new(),
                warehouse_paths: Vec::new(),
                metric_input_paths: vec![output.clone()],
            },
        );
        let marker = marker_result?;
        let valid_result =
            marker_manifest_is_valid_in(root, &analytical_wiki_dir(root, wiki), source_id);
        assert!(valid_result?);
        assert_eq!(active_metric_input_wiki_dir(root, wiki)?, metric_root);
        assert_eq!(
            active_fragment_files(root, wiki, GenerationLayer::MetricInput)?,
            vec![output]
        );
        assert_eq!(
            active_partition_specs(root, wiki, GenerationLayer::MetricInput)?.len(),
            1
        );
        assert!(!validate_stored_outputs(
            root,
            &metric_root,
            source_id,
            1,
            &[],
            true
        ));

        let mut value: Value = serde_json::from_slice(&fs::read(&marker)?)?;
        value["schema_version"] = json!(1);
        fs::write(&marker, serde_json::to_vec(&value)?)?;
        let downgraded_result =
            marker_manifest_is_valid_in(root, &analytical_wiki_dir(root, wiki), source_id);
        assert!(!downgraded_result?);
        Ok(())
    }
}
