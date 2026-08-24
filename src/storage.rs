use anyhow::{Context, Result, ensure};
use polars::prelude::{ParquetReader, SerReader};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};
use std::time::{Duration, SystemTime};

#[cfg(target_os = "linux")]
use std::num::NonZeroU64;

pub const ANALYTICAL_DIRNAME: &str = "parquet";
pub const WAREHOUSE_DIRNAME: &str = "warehouse";
const MARKERS_DIRNAME: &str = "_markers";
const SNAPSHOTS_DIRNAME: &str = "_snapshots";
const SNAPSHOT_STATE_DIRNAME: &str = "snapshots";
const CURRENT_SNAPSHOT_FILENAME: &str = "current-snapshot.json";
const SNAPSHOT_POINTER_SCHEMA_VERSION: u64 = 1;
const MARKER_SCHEMA_VERSION: u64 = 1;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct PartitionSpec {
    pub year: i32,
    pub year_month: String,
    pub dir: PathBuf,
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

pub(crate) fn snapshot_pointer_path(data_dir: &Path, wiki: &str) -> PathBuf {
    data_dir
        .join(SNAPSHOT_STATE_DIRNAME)
        .join(wiki)
        .join(CURRENT_SNAPSHOT_FILENAME)
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

pub fn publish_current_snapshot(data_dir: &Path, wiki: &str, snapshot_version: &str) -> Result<()> {
    validate_snapshot_version(snapshot_version)?;
    let analytical = snapshot_analytical_wiki_dir(data_dir, wiki, snapshot_version)?;
    let warehouse = snapshot_warehouse_wiki_dir(data_dir, wiki, snapshot_version)?;
    ensure!(
        analytical.is_dir() && warehouse.is_dir(),
        "cannot publish incomplete snapshot {snapshot_version} for {wiki}"
    );

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
    let marker_count = validate_snapshot_generation(data_dir, wiki, snapshot_version)?;
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
    ensure!(
        analytical.is_dir() && warehouse.is_dir(),
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
    ensure!(
        actual_analytical == analytical_allowlist && actual_warehouse == warehouse_allowlist,
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
    ] {
        let snapshots_root = layer_root.join(SNAPSHOTS_DIRNAME);
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
    Ok(removed)
}

pub(crate) fn clean_stale_inactive_snapshots(
    data_dir: &Path,
    wiki: &str,
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
                if version != active && validate_snapshot_version(&version).is_ok() {
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
    ensure_output_totals(
        source_id,
        manifest.rows,
        &analytical_outputs,
        &warehouse_outputs,
    )?;
    if remove_unexpected {
        remove_unexpected_source_outputs(analytical_root, source_id, &manifest.analytical_paths)?;
        remove_unexpected_source_outputs(&warehouse_root, source_id, &manifest.warehouse_paths)?;
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
) -> Result<()> {
    if expected_rows > 0 {
        ensure!(
            !analytical.is_empty() && !warehouse.is_empty(),
            "source {source_id} has rows but is missing an output layer"
        );
    }
    let expected_rows = u64::try_from(expected_rows)?;
    for (layer, outputs) in [("analytical", analytical), ("warehouse", warehouse)] {
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
        stored.schema_version == MARKER_SCHEMA_VERSION,
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
    if stored.schema_version != MARKER_SCHEMA_VERSION || stored.source_id != source_id {
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
    let warehouse_root = data_dir.join(WAREHOUSE_DIRNAME).join(analytical_suffix);
    let require_exact_source_inventory = stored.snapshot_version.is_none();
    if !validate_stored_outputs(
        data_dir,
        analytical_root,
        source_id,
        stored.rows,
        &stored.analytical_outputs,
        require_exact_source_inventory,
    ) || !validate_stored_outputs(
        data_dir,
        &warehouse_root,
        source_id,
        stored.rows,
        &stored.warehouse_outputs,
        require_exact_source_inventory,
    ) {
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
    Ok(partitions
        .into_iter()
        .map(|((year, year_month), dir)| PartitionSpec {
            year,
            year_month,
            dir,
        })
        .collect())
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

    type MarkerMutation = Box<dyn FnOnce(&mut Value)>;

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

        publish_current_snapshot(root, wiki, "2026-07")?;

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

        assert!(publish_current_snapshot(root, wiki, "2026-07").is_err());
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
        publish_current_snapshot(root, wiki, "2026-07")?;

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
            Box::new(|value| value["schema_version"] = json!(2)),
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
            |value: &mut Value| value["schema_version"] = json!(2),
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
}
