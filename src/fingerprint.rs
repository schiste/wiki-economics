use anyhow::{Context, Result, ensure};
use polars::prelude::*;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;
use tracing::{info, warn};

const RECEIPT_SCHEMA_VERSION: u32 = 1;
const SITE_ALGORITHM_VERSION: &str = "observable-static-site-v5-publication-receipt-identity";
const PARQUET_SUMMARY_BATCH_ROWS: usize = 250_000;
const DATE_COLUMNS: [&str; 6] = [
    "week_start",
    "year_month",
    "cohort_month",
    "period",
    "month",
    "date",
];

pub fn data_stage_receipt_path(
    data_dir: &Path,
    wiki: &str,
    snapshot: &str,
    stage: &str,
) -> PathBuf {
    data_dir
        .join("stages")
        .join(wiki)
        .join(snapshot)
        .join(format!("{stage}.json"))
}

#[derive(Clone, Debug)]
pub struct TrackedPath {
    pub identity: String,
    pub path: PathBuf,
}

impl TrackedPath {
    pub fn new(identity: impl Into<String>, path: impl Into<PathBuf>) -> Self {
        Self {
            identity: identity.into(),
            path: path.into(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ArtifactIdentity {
    pub identity: String,
    pub bytes: u64,
    pub sha256: String,
    pub output_schema: Vec<String>,
    pub rows: Option<u64>,
    pub minimum_date: Option<String>,
    pub maximum_date: Option<String>,
    pub observed_modified_unix_nanos: u128,
    #[serde(default)]
    pub artifact_receipt_sha256: Option<String>,
    #[serde(default)]
    pub conservation_totals: std::collections::BTreeMap<String, i128>,
    #[serde(default)]
    pub minimum_wiki: String,
    #[serde(default)]
    pub maximum_wiki: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct StageReceipt {
    pub schema_version: u32,
    pub stage: String,
    pub scope: String,
    pub selected_snapshot: Option<String>,
    pub algorithm_version: String,
    pub computation_version: String,
    pub binary_commit: Option<String>,
    pub inputs: Vec<ArtifactIdentity>,
    pub outputs: Vec<ArtifactIdentity>,
    pub fingerprint: String,
}

#[derive(Serialize)]
struct DeterministicArtifact<'a> {
    identity: &'a str,
    bytes: u64,
    sha256: &'a str,
    output_schema: &'a [String],
    rows: Option<u64>,
    minimum_date: Option<&'a str>,
    maximum_date: Option<&'a str>,
    artifact_receipt_sha256: Option<&'a str>,
    conservation_totals: &'a std::collections::BTreeMap<String, i128>,
    minimum_wiki: &'a str,
    maximum_wiki: &'a str,
}

#[derive(Serialize)]
struct FingerprintSeed<'a> {
    schema_version: u32,
    stage: &'a str,
    scope: &'a str,
    selected_snapshot: Option<&'a str>,
    algorithm_version: &'a str,
    computation_version: &'a str,
    inputs: Vec<DeterministicArtifact<'a>>,
    outputs: Vec<DeterministicArtifact<'a>>,
}

pub fn collect_tracked_files(root: &Path, prefix: &str) -> Result<Vec<TrackedPath>> {
    let mut paths = Vec::new();
    collect_tracked_files_recursive(root, root, prefix, &mut paths)?;
    paths.sort_by(|left, right| left.identity.cmp(&right.identity));
    Ok(paths)
}

fn collect_tracked_files_recursive(
    root: &Path,
    current: &Path,
    prefix: &str,
    paths: &mut Vec<TrackedPath>,
) -> Result<()> {
    if !current.exists() {
        return Ok(());
    }
    for entry in fs::read_dir(current)? {
        let entry = entry?;
        let path = entry.path();
        if entry.file_type()?.is_dir() {
            collect_tracked_files_recursive(root, &path, prefix, paths)?;
        } else if entry.file_type()?.is_file() {
            let relative = path
                .strip_prefix(root)
                .expect("recursive stage artifact must remain beneath its root");
            paths.push(TrackedPath::new(
                format!("{prefix}/{}", relative.to_string_lossy()),
                path,
            ));
        }
    }
    Ok(())
}

fn modified_nanos(path: &Path) -> Result<u128> {
    Ok(fs::metadata(path)?
        .modified()?
        .duration_since(UNIX_EPOCH)
        .with_context(|| format!("{} has a pre-epoch modification time", path.display()))?
        .as_nanos())
}

fn sha256_file(path: &Path) -> Result<String> {
    crate::storage::sha256_file(path).map(|(_, hash)| hash)
}

type ParquetSummary = (Vec<String>, u64, Option<String>, Option<String>);

fn parquet_summary(path: &Path) -> Result<ParquetSummary> {
    let mut reader =
        crate::storage::SequentialParquetReader::new(path, None, PARQUET_SUMMARY_BATCH_ROWS)?;
    let rows = u64::try_from(reader.rows())?;
    let schema_frame = reader.schema_frame()?;
    let schema = schema_frame
        .schema()
        .iter_fields()
        .map(|field| format!("{}:{:?}", field.name(), field.dtype()))
        .collect::<Vec<_>>();
    let date_column = DATE_COLUMNS
        .iter()
        .find(|candidate| schema_frame.schema().contains(candidate));
    let Some(date_column) = date_column else {
        return Ok((schema, rows, None, None));
    };
    if rows == 0 {
        return Ok((schema, rows, None, None));
    }
    reader.set_projection(vec![(*date_column).to_string()]);
    let mut minimum: Option<String> = None;
    let mut maximum: Option<String> = None;
    let mut observed_rows = 0_u64;
    while let Some(batch) = reader.next_batch()? {
        observed_rows = observed_rows
            .checked_add(u64::try_from(batch.height())?)
            .context("fingerprint Parquet row count overflow")?;
        let dates = batch.column(date_column)?.cast(&DataType::String)?;
        for date in dates.str()?.iter().flatten() {
            if minimum.as_deref().is_none_or(|value| date < value) {
                minimum = Some(date.to_owned());
            }
            if maximum.as_deref().is_none_or(|value| date > value) {
                maximum = Some(date.to_owned());
            }
        }
    }
    ensure!(
        observed_rows == rows,
        "Parquet row conservation failed while fingerprinting"
    );
    Ok((schema, rows, minimum, maximum))
}

fn inspect(path: &TrackedPath) -> Result<ArtifactIdentity> {
    let metadata = fs::metadata(&path.path)
        .with_context(|| format!("failed to inspect stage artifact {}", path.path.display()))?;
    ensure!(
        metadata.is_file(),
        "stage artifact is not a file: {}",
        path.path.display()
    );
    if path
        .path
        .extension()
        .is_some_and(|extension| extension == "parquet")
        && crate::artifact_receipt::sidecar_path(&path.path)?.is_file()
    {
        let document = crate::artifact_receipt::read(&path.path)?;
        let document = crate::artifact_receipt::verify(
            &path.path,
            &document.receipt.identity,
            Some(&document.receipt_sha256),
            crate::artifact_receipt::VerificationMode::Fast,
        )?;
        let receipt = document.receipt;
        return Ok(ArtifactIdentity {
            identity: path.identity.clone(),
            bytes: receipt.bytes,
            sha256: receipt.artifact_sha256,
            output_schema: receipt
                .parquet_schema
                .into_iter()
                .map(|field| format!("{}:{}", field.name, field.data_type))
                .collect(),
            rows: Some(receipt.rows),
            minimum_date: receipt.minimum_date,
            maximum_date: receipt.maximum_date,
            observed_modified_unix_nanos: modified_nanos(&path.path)?,
            artifact_receipt_sha256: Some(document.receipt_sha256),
            conservation_totals: receipt.conservation_totals,
            minimum_wiki: receipt.minimum_wiki,
            maximum_wiki: receipt.maximum_wiki,
        });
    }
    let (output_schema, rows, minimum_date, maximum_date) = if path
        .path
        .extension()
        .is_some_and(|extension| extension == "parquet")
    {
        let (schema, rows, minimum, maximum) = parquet_summary(&path.path)?;
        (schema, Some(rows), minimum, maximum)
    } else {
        (Vec::new(), None, None, None)
    };
    Ok(ArtifactIdentity {
        identity: path.identity.clone(),
        bytes: metadata.len(),
        sha256: sha256_file(&path.path)?,
        output_schema,
        rows,
        minimum_date,
        maximum_date,
        observed_modified_unix_nanos: modified_nanos(&path.path)?,
        artifact_receipt_sha256: None,
        conservation_totals: std::collections::BTreeMap::new(),
        minimum_wiki: String::new(),
        maximum_wiki: String::new(),
    })
}

fn inspect_receipted_output(
    path: &TrackedPath,
    algorithm_version: &str,
    input_fingerprint: &str,
) -> Result<ArtifactIdentity> {
    if path
        .path
        .extension()
        .is_none_or(|extension| extension != "parquet")
    {
        return inspect(path);
    }
    let document = if crate::artifact_receipt::sidecar_path(&path.path)?.is_file() {
        let existing = crate::artifact_receipt::read(&path.path)?;
        let existing = crate::artifact_receipt::verify(
            &path.path,
            &existing.receipt.identity,
            None,
            crate::artifact_receipt::VerificationMode::Fast,
        )?;
        if existing.receipt.algorithm_version == algorithm_version
            && existing.receipt.input_fingerprint == input_fingerprint
        {
            existing
        } else {
            crate::artifact_receipt::scan_and_write(
                &path.path,
                &path.identity,
                algorithm_version,
                input_fingerprint,
            )?
        }
    } else {
        crate::artifact_receipt::scan_and_write(
            &path.path,
            &path.identity,
            algorithm_version,
            input_fingerprint,
        )?
    };
    let receipt = document.receipt;
    Ok(ArtifactIdentity {
        identity: path.identity.clone(),
        bytes: receipt.bytes,
        sha256: receipt.artifact_sha256,
        output_schema: receipt
            .parquet_schema
            .into_iter()
            .map(|field| format!("{}:{}", field.name, field.data_type))
            .collect(),
        rows: Some(receipt.rows),
        minimum_date: receipt.minimum_date,
        maximum_date: receipt.maximum_date,
        observed_modified_unix_nanos: modified_nanos(&path.path)?,
        artifact_receipt_sha256: Some(document.receipt_sha256),
        conservation_totals: receipt.conservation_totals,
        minimum_wiki: receipt.minimum_wiki,
        maximum_wiki: receipt.maximum_wiki,
    })
}

fn inspect_all(paths: &[TrackedPath]) -> Result<Vec<ArtifactIdentity>> {
    let mut sorted = paths.to_vec();
    sorted.sort_by(|left, right| left.identity.cmp(&right.identity));
    let mut identities = Vec::with_capacity(sorted.len());
    for path in &sorted {
        identities.push(inspect(path)?);
    }
    Ok(identities)
}

fn inspect_receipted_outputs(
    paths: &[TrackedPath],
    algorithm_version: &str,
    input_fingerprint: &str,
) -> Result<Vec<ArtifactIdentity>> {
    let mut sorted = paths.to_vec();
    sorted.sort_by(|left, right| left.identity.cmp(&right.identity));
    sorted
        .iter()
        .map(|path| inspect_receipted_output(path, algorithm_version, input_fingerprint))
        .collect()
}

fn deterministic_artifacts(records: &[ArtifactIdentity]) -> Vec<DeterministicArtifact<'_>> {
    records
        .iter()
        .map(|record| DeterministicArtifact {
            identity: &record.identity,
            bytes: record.bytes,
            sha256: &record.sha256,
            output_schema: &record.output_schema,
            rows: record.rows,
            minimum_date: record.minimum_date.as_deref(),
            maximum_date: record.maximum_date.as_deref(),
            artifact_receipt_sha256: record.artifact_receipt_sha256.as_deref(),
            conservation_totals: &record.conservation_totals,
            minimum_wiki: &record.minimum_wiki,
            maximum_wiki: &record.maximum_wiki,
        })
        .collect()
}

fn receipt_fingerprint(receipt: &StageReceipt) -> Result<String> {
    let seed = FingerprintSeed {
        schema_version: receipt.schema_version,
        stage: &receipt.stage,
        scope: &receipt.scope,
        selected_snapshot: receipt.selected_snapshot.as_deref(),
        algorithm_version: &receipt.algorithm_version,
        computation_version: &receipt.computation_version,
        inputs: deterministic_artifacts(&receipt.inputs),
        outputs: deterministic_artifacts(&receipt.outputs),
    };
    Ok(hex::encode(Sha256::digest(serde_json::to_vec(&seed)?)))
}

fn paths_match(records: &[ArtifactIdentity], paths: &[TrackedPath]) -> Result<bool> {
    if records.len() != paths.len() {
        info!(
            expected = records.len(),
            observed = paths.len(),
            "stage artifact count changed"
        );
        return Ok(false);
    }
    let mut sorted = paths.to_vec();
    sorted.sort_by(|left, right| left.identity.cmp(&right.identity));
    for (record, path) in records.iter().zip(sorted) {
        if record.identity != path.identity || !path.path.is_file() {
            info!(
                expected_identity = record.identity,
                observed_identity = path.identity,
                path = %path.path.display(),
                exists = path.path.is_file(),
                "stage artifact identity changed"
            );
            return Ok(false);
        }
        let metadata = fs::metadata(&path.path)?;
        let receipt_verified = if let Some(receipt_sha256) = &record.artifact_receipt_sha256 {
            let document = match crate::artifact_receipt::read(&path.path) {
                Ok(document) => document,
                Err(error) => {
                    info!(identity = record.identity, error = %error, "artifact receipt is invalid");
                    return Ok(false);
                }
            };
            if crate::artifact_receipt::verify(
                &path.path,
                &document.receipt.identity,
                Some(receipt_sha256),
                crate::artifact_receipt::VerificationMode::Fast,
            )
            .is_err()
            {
                info!(identity = record.identity, "artifact receipt pair changed");
                return Ok(false);
            }
            true
        } else {
            false
        };
        if metadata.len() != record.bytes {
            info!(
                identity = record.identity,
                expected_bytes = record.bytes,
                observed_bytes = metadata.len(),
                "stage artifact size changed"
            );
            return Ok(false);
        }
        if modified_nanos(&path.path)? != record.observed_modified_unix_nanos {
            let content_matches = if receipt_verified {
                true
            } else {
                sha256_file(&path.path)? == record.sha256
            };
            if !content_matches {
                info!(
                    identity = record.identity,
                    expected_sha256 = record.sha256,
                    "stage artifact content changed"
                );
                return Ok(false);
            }
        }
    }
    Ok(true)
}

#[derive(Clone, Copy)]
pub struct StageSpec<'a> {
    pub stage: &'a str,
    pub scope: &'a str,
    pub selected_snapshot: Option<&'a str>,
    pub algorithm_version: &'a str,
}

pub fn reusable(
    receipt_path: &Path,
    spec: StageSpec<'_>,
    inputs: &[TrackedPath],
    outputs: &[TrackedPath],
) -> Result<bool> {
    if !receipt_path.is_file() {
        return Ok(false);
    }
    let receipt: StageReceipt = match serde_json::from_slice(&fs::read(receipt_path)?) {
        Ok(receipt) => receipt,
        Err(error) => {
            warn!(path = %receipt_path.display(), error = %error, "ignoring invalid stage receipt");
            return Ok(false);
        }
    };
    let metadata_matches = receipt_matches_spec(&receipt, spec)?;
    if !metadata_matches {
        info!(
            stage = spec.stage,
            scope = spec.scope,
            receipt = %receipt_path.display(),
            "stage receipt metadata changed"
        );
        return Ok(false);
    }
    if !paths_match(&receipt.inputs, inputs)? {
        info!(
            stage = spec.stage,
            scope = spec.scope,
            "stage inputs changed"
        );
        return Ok(false);
    }
    if !paths_match(&receipt.outputs, outputs)? {
        info!(
            stage = spec.stage,
            scope = spec.scope,
            "stage outputs changed"
        );
        return Ok(false);
    }
    Ok(true)
}

fn receipt_matches_spec(receipt: &StageReceipt, spec: StageSpec<'_>) -> Result<bool> {
    Ok(receipt.schema_version == RECEIPT_SCHEMA_VERSION
        && receipt.stage == spec.stage
        && receipt.scope == spec.scope
        && receipt.selected_snapshot.as_deref() == spec.selected_snapshot
        && receipt.algorithm_version == spec.algorithm_version
        && receipt.computation_version == env!("CARGO_PKG_VERSION")
        && receipt_fingerprint(receipt)? == receipt.fingerprint)
}

pub fn read_receipt(path: &Path) -> Result<StageReceipt> {
    serde_json::from_slice(&fs::read(path)?).with_context(|| {
        format!(
            "failed to read deterministic stage receipt {}",
            path.display()
        )
    })
}

pub fn outputs_reusable(
    receipt_path: &Path,
    spec: StageSpec<'_>,
    outputs: &[TrackedPath],
) -> Result<bool> {
    if !receipt_path.is_file() {
        return Ok(false);
    }
    let receipt = match read_receipt(receipt_path) {
        Ok(receipt) => receipt,
        Err(error) => {
            warn!(path = %receipt_path.display(), error = %error, "ignoring invalid stage receipt");
            return Ok(false);
        }
    };
    Ok(receipt_matches_spec(&receipt, spec)? && paths_match(&receipt.outputs, outputs)?)
}

pub fn record(
    receipt_path: &Path,
    spec: StageSpec<'_>,
    inputs: &[TrackedPath],
    outputs: &[TrackedPath],
) -> Result<StageReceipt> {
    ensure!(
        !outputs.is_empty(),
        "stage {} produced no outputs",
        spec.stage
    );
    let inspected_inputs = inspect_all(inputs)?;
    let deterministic_inputs = deterministic_artifacts(&inspected_inputs);
    let serialized_inputs = serde_json::to_vec(&deterministic_inputs)?;
    let input_fingerprint = hex::encode(Sha256::digest(serialized_inputs));
    let mut receipt = StageReceipt {
        schema_version: RECEIPT_SCHEMA_VERSION,
        stage: spec.stage.to_string(),
        scope: spec.scope.to_string(),
        selected_snapshot: spec.selected_snapshot.map(str::to_string),
        algorithm_version: spec.algorithm_version.to_string(),
        computation_version: env!("CARGO_PKG_VERSION").to_string(),
        binary_commit: option_env!("WIKI_ECON_BUILD_COMMIT").map(str::to_string),
        inputs: inspected_inputs,
        outputs: inspect_receipted_outputs(outputs, spec.algorithm_version, &input_fingerprint)?,
        fingerprint: String::new(),
    };
    receipt.fingerprint = receipt_fingerprint(&receipt)?;
    let parent = receipt_path
        .parent()
        .context("stage receipt has no parent")?;
    fs::create_dir_all(parent)?;
    let temp = parent.join(format!(".stage-receipt-{}.tmp", std::process::id()));
    let write_result = (|| -> Result<()> {
        let mut file = File::create(&temp)?;
        serde_json::to_writer_pretty(&mut file, &receipt)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        fs::rename(&temp, receipt_path)?;
        File::open(parent)?.sync_all()?;
        Ok(())
    })();
    if write_result.is_err() {
        let _ = fs::remove_file(&temp);
    }
    write_result?;
    info!(
        stage = spec.stage,
        scope = spec.scope,
        fingerprint = receipt.fingerprint,
        inputs = receipt.inputs.len(),
        outputs = receipt.outputs.len(),
        "recorded deterministic stage fingerprint"
    );
    Ok(receipt)
}

fn site_selected_snapshots(output_dir: &Path) -> Result<String> {
    let gate_bytes = fs::read(output_dir.join(crate::publication::RECEIPT_FILE))?;
    let gate: serde_json::Value = serde_json::from_slice(&gate_bytes)?;
    let snapshots = gate
        .get("selected_snapshot_versions")
        .and_then(serde_json::Value::as_object)
        .context("publication gate receipt is missing selected snapshot versions")?;
    let mut selected: Vec<_> = snapshots
        .iter()
        .filter_map(|(wiki, version)| version.as_str().map(|version| format!("{wiki}={version}")))
        .collect();
    selected.sort();
    let selected = selected.join(",");
    Ok(selected)
}

fn site_stage_inputs(output_dir: &Path, site_dir: &Path) -> Result<Vec<TrackedPath>> {
    // scripts/build-site.sh verifies every candidate artifact against the
    // publication gate before it reaches this fingerprint check. The two
    // atomic receipts therefore form a compact, already-verified identity for
    // the full data set and avoid hashing multi-gigabyte Parquets a second time.
    let mut inputs = vec![
        TrackedPath::new(
            "data/.publication-candidate.json",
            output_dir.join(".publication-candidate.json"),
        ),
        TrackedPath::new(
            format!("data/{}", crate::publication::RECEIPT_FILE),
            output_dir.join(crate::publication::RECEIPT_FILE),
        ),
    ];
    let mut site_sources = collect_tracked_files(&site_dir.join("src"), "site/src")?;
    site_sources.retain(|source| !source.identity.starts_with("site/src/.observablehq/"));
    inputs.extend(site_sources);
    let data_build = collect_tracked_files(&site_dir.join("data-build"), "site/data-build")?;
    inputs.extend(data_build);
    for name in ["observablehq.config.js", "package.json"] {
        inputs.push(TrackedPath::new(
            format!("site/{name}"),
            site_dir.join(name),
        ));
    }
    let workspace_dir = site_dir
        .parent()
        .context("site directory has no npm workspace parent")?;
    for name in ["package.json", "package-lock.json"] {
        inputs.push(TrackedPath::new(
            format!("workspace/{name}"),
            workspace_dir.join(name),
        ));
    }
    inputs.sort_by(|left, right| left.identity.cmp(&right.identity));
    Ok(inputs)
}

fn site_receipt_path(output_dir: &Path) -> PathBuf {
    output_dir.join("_stages").join("site.json")
}

/// Prove that the currently served distribution was built from the current
/// publication candidate and gate. Unlike `site_is_reusable`, this deliberately
/// ignores site source files: recovery is concerned with publication identity,
/// not whether a fresh build can be skipped after source-code changes.
pub(crate) fn current_site_matches_publication(output_dir: &Path, dist_dir: &Path) -> Result<bool> {
    let path = site_receipt_path(output_dir);
    if !path.is_file() || !dist_dir.is_dir() {
        return Ok(false);
    }
    let receipt = match read_receipt(&path) {
        Ok(receipt) => receipt,
        Err(error) => {
            warn!(path = %path.display(), error = %error, "invalid site receipt during publication recovery");
            return Ok(false);
        }
    };
    let selected = site_selected_snapshots(output_dir)?;
    if receipt.schema_version != RECEIPT_SCHEMA_VERSION
        || receipt.stage != "site"
        || receipt.scope != "published-site"
        || receipt.selected_snapshot.as_deref() != Some(&selected)
        || receipt_fingerprint(&receipt)? != receipt.fingerprint
    {
        return Ok(false);
    }

    let publication_inputs = [
        TrackedPath::new(
            "data/.publication-candidate.json",
            output_dir.join(".publication-candidate.json"),
        ),
        TrackedPath::new(
            format!("data/{}", crate::publication::RECEIPT_FILE),
            output_dir.join(crate::publication::RECEIPT_FILE),
        ),
    ];
    for input in publication_inputs {
        let Some(record) = receipt
            .inputs
            .iter()
            .find(|record| record.identity == input.identity)
        else {
            return Ok(false);
        };
        if !paths_match(std::slice::from_ref(record), std::slice::from_ref(&input))? {
            return Ok(false);
        }
    }
    let outputs = collect_tracked_files(dist_dir, "site-dist")?;
    paths_match(&receipt.outputs, &outputs)
}

/// Validate the deployed bytes against their own signed-by-hash stage receipt,
/// even when an interrupted publisher has already replaced the data gate that
/// originally fed the site. This is the evidence recovery needs before it may
/// restore the site's previous data generation.
pub(crate) fn current_site_has_valid_receipt(output_dir: &Path, dist_dir: &Path) -> Result<bool> {
    let path = site_receipt_path(output_dir);
    if !path.is_file() || !dist_dir.is_dir() {
        return Ok(false);
    }
    let receipt = match read_receipt(&path) {
        Ok(receipt) => receipt,
        Err(_) => return Ok(false),
    };
    let required_inputs = [
        "data/.publication-candidate.json",
        "data/publication-gate.json",
    ];
    if receipt.schema_version != RECEIPT_SCHEMA_VERSION
        || receipt.stage != "site"
        || receipt.scope != "published-site"
        || receipt_fingerprint(&receipt)? != receipt.fingerprint
        || required_inputs.iter().any(|required| {
            !receipt
                .inputs
                .iter()
                .any(|input| input.identity == *required)
        })
    {
        return Ok(false);
    }
    let outputs = collect_tracked_files(dist_dir, "site-dist")?;
    paths_match(&receipt.outputs, &outputs)
}

pub fn site_is_reusable(output_dir: &Path, site_dir: &Path, dist_dir: &Path) -> Result<bool> {
    let selected = site_selected_snapshots(output_dir)?;
    let spec = StageSpec {
        stage: "site",
        scope: "published-site",
        selected_snapshot: Some(&selected),
        algorithm_version: SITE_ALGORITHM_VERSION,
    };
    let inputs = site_stage_inputs(output_dir, site_dir)?;
    let outputs = collect_tracked_files(dist_dir, "site-dist")?;
    reusable(&site_receipt_path(output_dir), spec, &inputs, &outputs)
}

pub fn record_site(output_dir: &Path, site_dir: &Path, dist_dir: &Path) -> Result<StageReceipt> {
    let selected = site_selected_snapshots(output_dir)?;
    let spec = StageSpec {
        stage: "site",
        scope: "published-site",
        selected_snapshot: Some(&selected),
        algorithm_version: SITE_ALGORITHM_VERSION,
    };
    let inputs = site_stage_inputs(output_dir, site_dir)?;
    let outputs = collect_tracked_files(dist_dir, "site-dist")?;
    record(&site_receipt_path(output_dir), spec, &inputs, &outputs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::TestDir;

    #[test]
    fn receipt_is_content_addressed_and_quickly_reusable() -> Result<()> {
        let dir = TestDir::new()?;
        let input = dir.path().join("input.txt");
        let output = dir.path().join("output.txt");
        let receipt_path = dir.path().join("state/stage.json");
        fs::write(&input, "input")?;
        fs::write(&output, "output")?;
        let inputs = [TrackedPath::new("input/a", &input)];
        let outputs = [TrackedPath::new("output/a", &output)];
        let spec = StageSpec {
            stage: "compute",
            scope: "testwiki",
            selected_snapshot: Some("2026-08"),
            algorithm_version: "test-v1",
        };

        let first = record(&receipt_path, spec, &inputs, &outputs)?;
        assert!(reusable(&receipt_path, spec, &inputs, &outputs)?);
        let second = record(&receipt_path, spec, &inputs, &outputs)?;
        assert_eq!(first.fingerprint, second.fingerprint);

        std::thread::sleep(std::time::Duration::from_millis(10));
        fs::write(&input, "input")?;
        assert!(reusable(&receipt_path, spec, &inputs, &outputs)?);

        std::thread::sleep(std::time::Duration::from_millis(10));
        fs::write(&output, "change")?;
        assert!(!reusable(&receipt_path, spec, &inputs, &outputs)?);
        let missing_reusable = reusable(&dir.path().join("missing.json"), spec, &inputs, &outputs)
            .expect("missing receipt is a cache miss");
        assert!(!missing_reusable);
        Ok(())
    }

    #[test]
    fn receipt_validation_fails_closed_for_every_corruption_class() -> Result<()> {
        let dir = TestDir::new()?;
        let input = dir.path().join("input.txt");
        let output = dir.path().join("output.txt");
        let receipt_path = dir.path().join("stage.json");
        fs::write(&input, "input")?;
        fs::write(&output, "output")?;
        let inputs = [TrackedPath::new("input", &input)];
        let outputs = [TrackedPath::new("output", &output)];
        let spec = StageSpec {
            stage: "compute",
            scope: "wiki",
            selected_snapshot: Some("2026-08"),
            algorithm_version: "test-v1",
        };
        record(&receipt_path, spec, &inputs, &outputs)?;

        let wrong_spec = StageSpec {
            algorithm_version: "test-v2",
            ..spec
        };
        assert!(!reusable(&receipt_path, wrong_spec, &inputs, &outputs)?);
        assert!(!reusable(&receipt_path, spec, &inputs, &[])?);
        assert!(record(&dir.path().join("empty-output.json"), spec, &inputs, &[]).is_err());
        let identity_mismatch = reusable(
            &receipt_path,
            spec,
            &[TrackedPath::new("wrong", &input)],
            &outputs,
        )
        .expect("identity mismatch is a cache miss");
        assert!(!identity_mismatch);

        fs::write(&receipt_path, "not-json")?;
        assert!(!reusable(&receipt_path, spec, &inputs, &outputs)?);
        assert!(!outputs_reusable(&receipt_path, spec, &outputs)?);
        assert!(read_receipt(&receipt_path).is_err());
        let missing_outputs = outputs_reusable(&dir.path().join("missing.json"), spec, &outputs)
            .expect("missing upstream receipt is a cache miss");
        assert!(!missing_outputs);

        let directory_artifact = dir.path().join("directory-artifact");
        fs::create_dir(&directory_artifact)?;
        assert!(
            record(
                &dir.path().join("directory.json"),
                spec,
                &inputs,
                &[TrackedPath::new("directory", directory_artifact)],
            )
            .is_err()
        );

        let directory_receipt = dir.path().join("receipt-directory");
        fs::create_dir(&directory_receipt)?;
        assert!(record(&directory_receipt, spec, &inputs, &outputs).is_err());
        assert!(
            !dir.path()
                .join(format!(".stage-receipt-{}.tmp", std::process::id()))
                .exists()
        );

        let invalid_parquet = dir.path().join("invalid.parquet");
        fs::write(&invalid_parquet, "not parquet")?;
        assert!(
            record(
                &dir.path().join("invalid-parquet.json"),
                spec,
                &inputs,
                &[TrackedPath::new("invalid.parquet", &invalid_parquet)],
            )
            .is_err()
        );
        Ok(())
    }

    #[test]
    fn parquet_receipt_records_schema_rows_and_date_range() -> Result<()> {
        let dir = TestDir::new()?;
        let path = dir.path().join("metric.parquet");
        let mut frame = DataFrame::new_infer_height(vec![
            Column::new("wiki".into(), ["testwiki", "testwiki"]),
            Column::new("year_month".into(), ["2026-01", "2026-02"]),
        ])
        .expect("valid Parquet fixture");
        ParquetWriter::new(File::create(&path)?).finish(&mut frame)?;
        let receipt = record(
            &dir.path().join("stage.json"),
            StageSpec {
                stage: "merge",
                scope: "all",
                selected_snapshot: None,
                algorithm_version: "test-v1",
            },
            &[],
            &[TrackedPath::new("metric", &path)],
        )
        .expect("Parquet receipt should record");
        assert_eq!(receipt.outputs[0].rows, Some(2));
        assert_eq!(receipt.outputs[0].minimum_date.as_deref(), Some("2026-01"));
        assert_eq!(receipt.outputs[0].maximum_date.as_deref(), Some("2026-02"));
        assert!(receipt.outputs[0].output_schema.len() == 2);

        let receipt_path = dir.path().join("stage.json");
        let tracked = [TrackedPath::new("metric", &path)];
        let sidecar = crate::artifact_receipt::sidecar_path(&path)?;
        let original_sidecar = fs::read(&sidecar)?;
        fs::write(&sidecar, "truncated")?;
        assert!(
            !reusable(
                &receipt_path,
                StageSpec {
                    stage: "merge",
                    scope: "all",
                    selected_snapshot: None,
                    algorithm_version: "test-v1",
                },
                &[],
                &tracked,
            )
            .expect("invalid sidecar is a cache miss")
        );
        assert!(
            record(
                &dir.path().join("bad-input-stage.json"),
                StageSpec {
                    stage: "merge",
                    scope: "all",
                    selected_snapshot: None,
                    algorithm_version: "test-v1",
                },
                &tracked,
                &[TrackedPath::new("plain-output", dir.path().join("missing"))],
            )
            .is_err()
        );
        fs::write(&sidecar, &original_sidecar)?;

        let original = fs::read(&path)?;
        let mut corrupt = original.clone();
        corrupt[0] ^= 1;
        fs::write(&path, corrupt)?;
        assert!(
            record(
                &dir.path().join("corrupt-input-stage.json"),
                StageSpec {
                    stage: "merge",
                    scope: "all",
                    selected_snapshot: None,
                    algorithm_version: "test-v1",
                },
                &tracked,
                &[TrackedPath::new("plain-output", dir.path().join("missing"))],
            )
            .is_err()
        );
        assert!(inspect_receipted_output(&tracked[0], "different-v2", "inputs").is_err());
        fs::write(&path, &original)?;

        std::thread::sleep(std::time::Duration::from_millis(10));
        fs::write(&path, &original)?;
        assert!(
            reusable(
                &receipt_path,
                StageSpec {
                    stage: "merge",
                    scope: "all",
                    selected_snapshot: None,
                    algorithm_version: "test-v1",
                },
                &[],
                &tracked,
            )
            .expect("metadata-only Parquet change reuses receipt identity")
        );
        inspect_receipted_output(&tracked[0], "different-v2", "inputs")
            .expect("verified artifact can be reissued for a new algorithm version");
        assert!(
            inspect_receipted_output(
                &TrackedPath::new("../unsafe", &path),
                "different-v3",
                "inputs",
            )
            .is_err()
        );

        let empty_path = dir.path().join("empty.parquet");
        let mut empty = DataFrame::new_infer_height(vec![
            Column::new("wiki".into(), Vec::<String>::new()),
            Column::new("year_month".into(), Vec::<String>::new()),
        ])
        .expect("valid empty Parquet fixture");
        ParquetWriter::new(File::create(&empty_path)?).finish(&mut empty)?;
        let (_, rows, minimum, maximum) = parquet_summary(&empty_path)?;
        assert_eq!((rows, minimum, maximum), (0, None, None));
        Ok(())
    }

    #[test]
    fn site_receipt_reuses_only_identical_data_source_and_distribution() -> Result<()> {
        let dir = TestDir::new()?;
        let output = dir.path().join("output");
        let site = dir.path().join("site");
        let dist = dir.path().join("dist");
        fs::create_dir_all(site.join("src"))?;
        fs::create_dir_all(site.join("src/nested"))?;
        fs::create_dir_all(site.join("src/.observablehq/cache"))?;
        fs::create_dir_all(site.join("data-build"))?;
        fs::create_dir_all(&dist)?;
        fs::create_dir_all(&output)?;
        fs::write(output.join("metric.json"), "{}")?;
        fs::write(
            output.join(".publication-candidate.json"),
            r#"{"artifacts":[{"name":"metric.json"}]}"#,
        )
        .expect("candidate fixture should be written");
        fs::write(
            output.join(crate::publication::RECEIPT_FILE),
            r#"{"selected_snapshot_versions":{"nlwiki":"2026-07"}}"#,
        )
        .expect("gate fixture should be written");
        fs::write(site.join("src/nested/index.md"), "# Site")?;
        let generated_cache = site.join("src/.observablehq/cache/generated.js");
        fs::write(generated_cache, "transient")?;
        fs::write(site.join("data-build/manifest.sh"), "true")?;
        fs::write(site.join("observablehq.config.js"), "export default {}")?;
        fs::write(site.join("package.json"), "{}")?;
        fs::write(
            dir.path().join("package.json"),
            "{\"workspaces\":[\"site\"]}",
        )
        .expect("workspace package fixture should be written");
        fs::write(dir.path().join("package-lock.json"), "{}")?;
        fs::write(dist.join("index.html"), "published")?;

        assert!(
            !current_site_matches_publication(&output, &site.join("missing"))
                .expect("missing distribution is not a current publication")
        );
        assert!(
            !current_site_has_valid_receipt(&output, &site.join("missing"))
                .expect("missing distribution has no valid receipt")
        );

        let receipt = record_site(&output, &site, &dist)?;
        assert_eq!(receipt.selected_snapshot.as_deref(), Some("nlwiki=2026-07"));
        assert!(
            receipt
                .inputs
                .iter()
                .all(|input| !input.identity.starts_with("site/src/.observablehq/"))
        );
        assert!(
            receipt
                .inputs
                .iter()
                .any(|input| input.identity == "workspace/package-lock.json")
        );
        assert!(site_is_reusable(&output, &site, &dist)?);
        assert!(current_site_matches_publication(&output, &dist)?);
        assert!(current_site_has_valid_receipt(&output, &dist)?);
        let receipt_path = site_receipt_path(&output);
        let original_receipt = fs::read(&receipt_path)?;
        fs::write(&receipt_path, "not-json")?;
        assert!(!current_site_matches_publication(&output, &dist)?);
        assert!(!current_site_has_valid_receipt(&output, &dist)?);
        fs::write(&receipt_path, &original_receipt)?;
        let mut missing_input: StageReceipt = read_receipt(&receipt_path)?;
        missing_input
            .inputs
            .retain(|input| input.identity != "data/publication-gate.json");
        missing_input.fingerprint = receipt_fingerprint(&missing_input)?;
        fs::write(&receipt_path, serde_json::to_vec_pretty(&missing_input)?)?;
        assert!(!current_site_matches_publication(&output, &dist)?);
        assert!(!current_site_has_valid_receipt(&output, &dist)?);
        fs::write(&receipt_path, original_receipt)?;
        fs::remove_dir_all(site.join("src/.observablehq"))?;
        assert!(site_is_reusable(&output, &site, &dist)?);

        fs::write(site.join("src/nested/index.md"), "# Changed")?;
        assert!(!site_is_reusable(&output, &site, &dist)?);
        assert!(current_site_matches_publication(&output, &dist)?);
        fs::write(site.join("src/nested/index.md"), "# Site")?;
        fs::write(dir.path().join("package-lock.json"), "{\"changed\":true}")?;
        assert!(!site_is_reusable(&output, &site, &dist)?);
        fs::write(dir.path().join("package-lock.json"), "{}")?;
        fs::write(output.join("metric.json"), "{\"changed\":true}")?;
        assert!(
            site_is_reusable(&output, &site, &dist)?,
            "publication verification owns artifact validation; the site fingerprint consumes its receipt identity"
        );
        assert!(current_site_matches_publication(&output, &dist)?);
        let changed_gate = r#"{"selected_snapshot_versions":{"nlwiki":"2026-08"}}"#;
        fs::write(output.join(crate::publication::RECEIPT_FILE), changed_gate)?;
        assert!(!site_is_reusable(&output, &site, &dist)?);
        assert!(!current_site_matches_publication(&output, &dist)?);
        assert!(current_site_has_valid_receipt(&output, &dist)?);
        fs::write(dist.join("index.html"), "corrupt")?;
        assert!(!current_site_has_valid_receipt(&output, &dist)?);
        Ok(())
    }
}
