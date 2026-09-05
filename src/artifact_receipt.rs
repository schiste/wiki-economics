use anyhow::{Context, Result, ensure};
use chrono::NaiveDate;
use polars::prelude::*;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Component, Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::storage;

pub const ARTIFACT_RECEIPT_SCHEMA_VERSION: u32 = 1;
const RECEIPT_DOCUMENT_SCHEMA_VERSION: u32 = 1;
const SEMANTIC_BATCH_ROWS: usize = 250_000;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct FieldIdentity {
    pub name: String,
    pub data_type: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ArtifactReceipt {
    pub schema_version: u32,
    pub identity: String,
    pub artifact_sha256: String,
    pub bytes: u64,
    pub parquet_schema: Vec<FieldIdentity>,
    pub rows: u64,
    pub minimum_date: Option<String>,
    pub maximum_date: Option<String>,
    pub conservation_totals: BTreeMap<String, i128>,
    pub minimum_wiki: String,
    pub maximum_wiki: String,
    pub ordering_contract: String,
    pub algorithm_version: String,
    pub input_fingerprint: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ArtifactReceiptDocument {
    pub schema_version: u32,
    pub receipt_sha256: String,
    pub observed_modified_unix_nanos: u128,
    pub receipt: ArtifactReceipt,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct SemanticSummary {
    parquet_schema: Vec<FieldIdentity>,
    rows: u64,
    minimum_date: Option<String>,
    maximum_date: Option<String>,
    conservation_totals: BTreeMap<String, i128>,
    minimum_wiki: String,
    maximum_wiki: String,
    ordering_contract: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct SemanticDraft {
    schema_version: u32,
    artifact_bytes: u64,
    observed_modified_unix_nanos: u128,
    summary: SemanticSummary,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VerificationMode {
    Fast,
    Scrub,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ScrubbedArtifact {
    pub path: String,
    pub receipt_sha256: String,
    pub artifact_sha256: String,
    pub bytes: u64,
    pub rows: u64,
    pub minimum_date: Option<String>,
    pub maximum_date: Option<String>,
    pub conservation_totals: BTreeMap<String, i128>,
    pub minimum_wiki: String,
    pub maximum_wiki: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ScrubReport {
    pub schema_version: u32,
    pub scrubbed_at_unix: u64,
    pub artifacts: Vec<ScrubbedArtifact>,
    pub total_bytes: u64,
    pub total_rows: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ScrubStatus {
    pub schema_version: u32,
    pub state: String,
    pub run_id: String,
    pub updated_at_unix: u64,
    pub report_sha256: Option<String>,
    pub error: Option<String>,
}

const SCRUB_STATUS_PATH: &str = "_scrubs/status.json";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SemanticSpec {
    pub date_column: Option<String>,
    pub conservation_columns: Vec<String>,
    pub ordering_contract: String,
    pub page_week_consistency: bool,
}

impl SemanticSpec {
    pub fn for_identity(identity: &str) -> Self {
        let metric = Path::new(identity)
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or(identity)
            .strip_suffix(".parquet")
            .unwrap_or(identity);
        let (date, totals, ordering, page_week) = match metric {
            "gdp" => (
                Some("year_month"),
                vec!["total_edits"],
                "wiki-major/v1",
                false,
            ),
            "gdp_activity_tiers" => (
                Some("period_start"),
                vec!["total_edits"],
                "wiki-major/v1",
                false,
            ),
            "gdp_user_type_share" => (Some("year_month"), vec!["edits"], "wiki-major/v1", false),
            "inequality" => (Some("period_start"), vec![], "wiki-major/v1", false),
            "labor_churn" => (Some("period"), vec![], "wiki-major/v1", false),
            "labor_monthly" => (
                Some("year_month"),
                vec!["total_edits"],
                "wiki-major/v1",
                false,
            ),
            "page_weekly_edits" => (
                Some("week_start"),
                vec!["edits"],
                "stable-page-hash-bucket/page-key/week/v1",
                true,
            ),
            "patrol" => (
                Some("year_month"),
                vec!["total_patrols"],
                "wiki-major/v1",
                false,
            ),
            "business_funnel" => (Some("cohort_year"), vec![], "wiki-major/v1", false),
            "labor_cohorts" => (Some("year"), vec![], "wiki-major/v1", false),
            _ => (None, vec![], "writer-order/v1", false),
        };
        Self {
            date_column: date.map(str::to_string),
            conservation_columns: totals.into_iter().map(str::to_string).collect(),
            ordering_contract: ordering.to_string(),
            page_week_consistency: page_week,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PreviousPageWeek {
    page_id: Option<i64>,
    page_namespace: Option<i32>,
    page_title: Option<String>,
    week: NaiveDate,
    edits: u32,
}

pub struct SemanticAccumulator {
    spec: SemanticSpec,
    schema: Option<Vec<FieldIdentity>>,
    rows: u64,
    minimum_date: Option<String>,
    maximum_date: Option<String>,
    conservation_totals: BTreeMap<String, i128>,
    minimum_wiki: Option<String>,
    maximum_wiki: Option<String>,
    previous_wiki: Option<String>,
    previous_page_week: Option<PreviousPageWeek>,
}

impl SemanticAccumulator {
    pub fn new(spec: SemanticSpec) -> Self {
        let conservation_totals = spec
            .conservation_columns
            .iter()
            .map(|column| (column.clone(), 0_i128))
            .collect();
        Self {
            spec,
            schema: None,
            rows: 0,
            minimum_date: None,
            maximum_date: None,
            conservation_totals,
            minimum_wiki: None,
            maximum_wiki: None,
            previous_wiki: None,
            previous_page_week: None,
        }
    }

    pub fn observe(&mut self, frame: &DataFrame) -> Result<()> {
        if self.spec.date_column.is_none() {
            self.spec.date_column = [
                "week_start",
                "year_month",
                "period_start",
                "cohort_month",
                "period",
                "month",
                "date",
            ]
            .into_iter()
            .find(|candidate| frame.schema().contains(candidate))
            .map(str::to_string);
        }
        let schema = field_identities(frame.schema());
        if let Some(expected) = &self.schema {
            ensure!(
                expected == &schema,
                "Parquet writer schema changed between batches"
            );
        } else {
            self.schema = Some(schema);
        }
        self.rows = self
            .rows
            .checked_add(u64::try_from(frame.height())?)
            .context("artifact receipt row count overflow")?;
        self.observe_wikis(frame)?;
        self.observe_dates(frame)?;
        self.observe_totals(frame)?;
        if self.spec.page_week_consistency {
            self.observe_page_weeks(frame)?;
        }
        Ok(())
    }

    fn observe_wikis(&mut self, frame: &DataFrame) -> Result<()> {
        if !frame.schema().contains("wiki") {
            return Ok(());
        }
        for wiki in frame.column("wiki")?.str()?.iter() {
            let wiki = wiki.context("artifact contains a null wiki")?;
            if self.spec.ordering_contract == "wiki-major/v1" {
                ensure!(
                    self.previous_wiki
                        .as_deref()
                        .is_none_or(|previous| previous <= wiki),
                    "artifact violates deterministic wiki-major ordering"
                );
            }
            update_range(wiki, &mut self.minimum_wiki, &mut self.maximum_wiki);
            self.previous_wiki = Some(wiki.to_string());
        }
        Ok(())
    }

    fn observe_dates(&mut self, frame: &DataFrame) -> Result<()> {
        let Some(column) = self.spec.date_column.as_deref() else {
            return Ok(());
        };
        for value in frame.column(column)?.str()?.iter() {
            let value = value.with_context(|| format!("artifact contains a null {column}"))?;
            update_range(value, &mut self.minimum_date, &mut self.maximum_date);
        }
        Ok(())
    }

    fn observe_totals(&mut self, frame: &DataFrame) -> Result<()> {
        for column in &self.spec.conservation_columns {
            let batch_total = sum_numeric(frame.column(column)?, column)?;
            let total = self
                .conservation_totals
                .get_mut(column)
                .context("missing initialized conservation total")?;
            *total = total
                .checked_add(batch_total)
                .with_context(|| format!("{column} conservation total overflow"))?;
        }
        Ok(())
    }

    fn observe_page_weeks(&mut self, frame: &DataFrame) -> Result<()> {
        let page_ids = frame.column("page_id")?.i64()?;
        let namespaces = frame.column("page_namespace")?.i32()?;
        let titles = frame.column("page_title")?.str()?;
        let weeks = frame.column("week_start")?.str()?;
        let edits = frame.column("edits")?.u32()?;
        let previous = frame.column("previous_week_edits")?.u32()?;
        for row in 0..frame.height() {
            let current = PreviousPageWeek {
                page_id: page_ids.get(row),
                page_namespace: namespaces.get(row),
                page_title: titles.get(row).map(str::to_string),
                week: NaiveDate::parse_from_str(
                    weeks
                        .get(row)
                        .context("null week_start in page-week output")?,
                    "%Y-%m-%d",
                )?,
                edits: edits.get(row).context("null edits in page-week output")?,
            };
            let expected_previous = self
                .previous_page_week
                .as_ref()
                .filter(|prior| {
                    prior.page_id == current.page_id
                        && prior.page_namespace == current.page_namespace
                        && prior.page_title == current.page_title
                        && current.week.signed_duration_since(prior.week).num_days() == 7
                })
                .map_or(0, |prior| prior.edits);
            ensure!(
                previous
                    .get(row)
                    .context("null previous_week_edits in page-week output")?
                    == expected_previous,
                "page-week previous_week_edits is inconsistent at receipt row {}",
                self.rows - u64::try_from(frame.height())? + u64::try_from(row)?
            );
            if let Some(prior) = &self.previous_page_week
                && prior.page_id == current.page_id
                && prior.page_namespace == current.page_namespace
                && prior.page_title == current.page_title
            {
                ensure!(
                    prior.week < current.week,
                    "page-week output is not strictly ordered within a page"
                );
            }
            self.previous_page_week = Some(current);
        }
        Ok(())
    }

    fn finish_summary(self) -> Result<SemanticSummary> {
        Ok(SemanticSummary {
            parquet_schema: self
                .schema
                .context("artifact receipt has no observed schema")?,
            rows: self.rows,
            minimum_date: self.minimum_date,
            maximum_date: self.maximum_date,
            conservation_totals: self.conservation_totals,
            minimum_wiki: self.minimum_wiki.unwrap_or_default(),
            maximum_wiki: self.maximum_wiki.unwrap_or_default(),
            ordering_contract: self.spec.ordering_contract,
        })
    }

    pub fn finish(
        self,
        identity: String,
        artifact_sha256: String,
        bytes: u64,
        algorithm_version: String,
        input_fingerprint: String,
    ) -> Result<ArtifactReceipt> {
        let summary = self.finish_summary()?;
        Ok(ArtifactReceipt {
            schema_version: ARTIFACT_RECEIPT_SCHEMA_VERSION,
            identity,
            artifact_sha256,
            bytes,
            parquet_schema: summary.parquet_schema,
            rows: summary.rows,
            minimum_date: summary.minimum_date,
            maximum_date: summary.maximum_date,
            conservation_totals: summary.conservation_totals,
            minimum_wiki: summary.minimum_wiki,
            maximum_wiki: summary.maximum_wiki,
            ordering_contract: summary.ordering_contract,
            algorithm_version,
            input_fingerprint,
        })
    }
}

fn field_identities(schema: &Schema) -> Vec<FieldIdentity> {
    schema
        .iter_fields()
        .map(|field| FieldIdentity {
            name: field.name().to_string(),
            data_type: format!("{:?}", field.dtype()),
        })
        .collect()
}

fn sum_numeric(column: &Column, name: &str) -> Result<i128> {
    ensure!(
        column.null_count() == 0,
        "null conservation value in {name}"
    );
    macro_rules! sum_typed {
        ($values:expr) => {{
            $values.iter().try_fold(0_i128, |total, value| {
                total
                    .checked_add(i128::from(value.context("validated non-null value")?))
                    .context("conservation total overflow")
            })
        }};
    }
    match column.dtype() {
        DataType::UInt32 => sum_typed!(column.u32()?),
        DataType::UInt64 => sum_typed!(column.u64()?),
        DataType::Int32 => sum_typed!(column.i32()?),
        DataType::Int64 => sum_typed!(column.i64()?),
        dtype => anyhow::bail!("unsupported conservation type {dtype:?} for {name}"),
    }
}

fn update_range(value: &str, minimum: &mut Option<String>, maximum: &mut Option<String>) {
    if minimum.as_deref().is_none_or(|current| value < current) {
        *minimum = Some(value.to_string());
    }
    if maximum.as_deref().is_none_or(|current| value > current) {
        *maximum = Some(value.to_string());
    }
}

pub fn sidecar_path(artifact: &Path) -> Result<PathBuf> {
    let name = artifact
        .file_name()
        .context("artifact path has no filename")?
        .to_string_lossy();
    Ok(artifact.with_file_name(format!("{name}.receipt.json")))
}

fn draft_path(artifact: &Path) -> Result<PathBuf> {
    let name = artifact
        .file_name()
        .context("artifact path has no filename")?
        .to_string_lossy();
    Ok(artifact.with_file_name(format!(".{name}.semantic-draft.json")))
}

pub fn write_semantic_draft(artifact: &Path, accumulator: SemanticAccumulator) -> Result<()> {
    let metadata = fs::metadata(artifact)?;
    let draft = SemanticDraft {
        schema_version: 1,
        artifact_bytes: metadata.len(),
        observed_modified_unix_nanos: modified_nanos(artifact)?,
        summary: accumulator.finish_summary()?,
    };
    let path = draft_path(artifact)?;
    let mut file = File::create(&path)?;
    serde_json::to_writer(&mut file, &draft)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    Ok(())
}

pub(crate) fn finalize_semantic_draft(
    artifact: &Path,
    identity: &str,
    algorithm_version: &str,
    input_fingerprint: &str,
) -> Result<Option<ArtifactReceiptDocument>> {
    let path = draft_path(artifact)?;
    if !path.is_file() {
        return Ok(None);
    }
    let draft: SemanticDraft = match serde_json::from_slice(&fs::read(&path)?) {
        Ok(draft) => draft,
        Err(_) => {
            fs::remove_file(path)?;
            return Ok(None);
        }
    };
    if draft.schema_version != 1 {
        fs::remove_file(path)?;
        return Ok(None);
    }
    let metadata = fs::metadata(artifact)?;
    if metadata.len() != draft.artifact_bytes
        || modified_nanos(artifact)? != draft.observed_modified_unix_nanos
    {
        fs::remove_file(path)?;
        return Ok(None);
    }
    let (bytes, artifact_sha256) = storage::sha256_file(artifact)?;
    let summary = draft.summary;
    let receipt = ArtifactReceipt {
        schema_version: ARTIFACT_RECEIPT_SCHEMA_VERSION,
        identity: identity.to_string(),
        artifact_sha256,
        bytes,
        parquet_schema: summary.parquet_schema,
        rows: summary.rows,
        minimum_date: summary.minimum_date,
        maximum_date: summary.maximum_date,
        conservation_totals: summary.conservation_totals,
        minimum_wiki: summary.minimum_wiki,
        maximum_wiki: summary.maximum_wiki,
        ordering_contract: summary.ordering_contract,
        algorithm_version: algorithm_version.to_string(),
        input_fingerprint: input_fingerprint.to_string(),
    };
    let document = write(artifact, receipt)?;
    fs::remove_file(path)?;
    Ok(Some(document))
}

pub fn canonical_receipt_sha256(receipt: &ArtifactReceipt) -> Result<String> {
    Ok(hex::encode(Sha256::digest(serde_json::to_vec(receipt)?)))
}

fn modified_nanos(path: &Path) -> Result<u128> {
    Ok(fs::metadata(path)?
        .modified()?
        .duration_since(UNIX_EPOCH)
        .with_context(|| format!("{} has a pre-epoch mtime", path.display()))?
        .as_nanos())
}

pub fn scan(
    artifact: &Path,
    identity: &str,
    algorithm_version: &str,
    input_fingerprint: &str,
) -> Result<ArtifactReceipt> {
    let spec = SemanticSpec::for_identity(identity);
    let mut reader = storage::SequentialParquetReader::new(artifact, None, SEMANTIC_BATCH_ROWS)?;
    let expected_rows = u64::try_from(reader.rows())?;
    let mut accumulator = SemanticAccumulator::new(spec);
    accumulator.observe(&reader.schema_frame()?)?;
    while let Some(batch) = reader.next_batch()? {
        accumulator.observe(&batch)?;
    }
    let (bytes, artifact_sha256) = storage::sha256_file(artifact)?;
    let identity = identity.to_string();
    let algorithm = algorithm_version.to_string();
    let inputs = input_fingerprint.to_string();
    let receipt = accumulator.finish(identity, artifact_sha256, bytes, algorithm, inputs)?;
    ensure!(
        receipt.rows == expected_rows,
        "receipt row count disagrees with Parquet footer"
    );
    Ok(receipt)
}

pub fn scan_and_write_with_spec(
    artifact: &Path,
    identity: &str,
    algorithm_version: &str,
    input_fingerprint: &str,
    spec: SemanticSpec,
) -> Result<ArtifactReceiptDocument> {
    let mut reader = storage::SequentialParquetReader::new(artifact, None, SEMANTIC_BATCH_ROWS)?;
    let expected_rows = u64::try_from(reader.rows())?;
    let mut accumulator = SemanticAccumulator::new(spec);
    accumulator.observe(&reader.schema_frame()?)?;
    while let Some(batch) = reader.next_batch()? {
        accumulator.observe(&batch)?;
    }
    let (bytes, artifact_sha256) = storage::sha256_file(artifact)?;
    let identity = identity.to_string();
    let algorithm = algorithm_version.to_string();
    let inputs = input_fingerprint.to_string();
    let receipt = accumulator.finish(identity, artifact_sha256, bytes, algorithm, inputs)?;
    ensure!(
        receipt.rows == expected_rows,
        "receipt row count disagrees with Parquet footer"
    );
    write(artifact, receipt)
}

pub fn write(artifact: &Path, receipt: ArtifactReceipt) -> Result<ArtifactReceiptDocument> {
    validate_identity(&receipt.identity)?;
    File::open(artifact)?.sync_all()?;
    let metadata = fs::metadata(artifact)?;
    ensure!(metadata.is_file(), "receipt artifact is not a file");
    ensure!(
        metadata.len() == receipt.bytes,
        "receipt artifact size changed before publication"
    );
    let document = ArtifactReceiptDocument {
        schema_version: RECEIPT_DOCUMENT_SCHEMA_VERSION,
        receipt_sha256: canonical_receipt_sha256(&receipt)?,
        observed_modified_unix_nanos: modified_nanos(artifact)?,
        receipt,
    };
    let path = sidecar_path(artifact)?;
    let parent = path.parent().context("receipt path has no parent")?;
    let temp = parent.join(format!(".artifact-receipt-{}.tmp", std::process::id()));
    let result = (|| -> Result<()> {
        let mut file = File::create(&temp)?;
        serde_json::to_writer_pretty(&mut file, &document)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        fs::rename(&temp, &path)?;
        File::open(parent)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temp);
    }
    result?;
    Ok(document)
}

pub fn scan_and_write(
    artifact: &Path,
    identity: &str,
    algorithm_version: &str,
    input_fingerprint: &str,
) -> Result<ArtifactReceiptDocument> {
    if let Some(document) =
        finalize_semantic_draft(artifact, identity, algorithm_version, input_fingerprint)?
    {
        return Ok(document);
    }
    let receipt = scan(artifact, identity, algorithm_version, input_fingerprint)?;
    write(artifact, receipt)
}

pub fn read(artifact: &Path) -> Result<ArtifactReceiptDocument> {
    let path = sidecar_path(artifact)?;
    let document: ArtifactReceiptDocument = serde_json::from_slice(
        &fs::read(&path).with_context(|| format!("failed to read {}", path.display()))?,
    )
    .with_context(|| format!("failed to parse {}", path.display()))?;
    ensure!(
        document.schema_version == RECEIPT_DOCUMENT_SCHEMA_VERSION,
        "unsupported artifact receipt document schema"
    );
    ensure!(
        document.receipt.schema_version == ARTIFACT_RECEIPT_SCHEMA_VERSION,
        "unsupported artifact receipt schema"
    );
    ensure!(
        canonical_receipt_sha256(&document.receipt)? == document.receipt_sha256,
        "artifact receipt canonical hash mismatch"
    );
    validate_identity(&document.receipt.identity)?;
    Ok(document)
}

pub fn verify(
    artifact: &Path,
    expected_identity: &str,
    expected_receipt_sha256: Option<&str>,
    mode: VerificationMode,
) -> Result<ArtifactReceiptDocument> {
    let document = read(artifact)?;
    ensure!(
        document.receipt.identity == expected_identity,
        "artifact receipt identity mismatch"
    );
    if let Some(expected) = expected_receipt_sha256 {
        ensure!(
            document.receipt_sha256 == expected,
            "artifact receipt reference mismatch"
        );
    }
    let metadata = fs::metadata(artifact)?;
    ensure!(metadata.is_file(), "artifact receipt target is not a file");
    let metadata_changed = metadata.len() != document.receipt.bytes
        || modified_nanos(artifact)? != document.observed_modified_unix_nanos;
    if mode == VerificationMode::Scrub || metadata_changed {
        let (bytes, sha256) = storage::sha256_file(artifact)?;
        ensure!(
            bytes == document.receipt.bytes && sha256 == document.receipt.artifact_sha256,
            "artifact and authoritative receipt do not match"
        );
    }
    Ok(document)
}

fn validate_identity(identity: &str) -> Result<()> {
    let path = Path::new(identity);
    ensure!(
        !identity.is_empty()
            && !path.is_absolute()
            && path
                .components()
                .all(|component| matches!(component, Component::Normal(_))),
        "unsafe artifact receipt identity {identity:?}"
    );
    Ok(())
}

pub fn scrub_published(output_dir: &Path) -> Result<ScrubReport> {
    let mut artifacts = Vec::new();
    for entry in fs::read_dir(output_dir)? {
        let entry = entry?;
        let path = entry.path();
        if path
            .extension()
            .is_some_and(|extension| extension == "parquet")
        {
            artifacts.push(path);
            continue;
        }
        if !path.is_dir()
            || entry
                .file_name()
                .to_str()
                .is_none_or(|name| name.starts_with('_'))
        {
            continue;
        }
        for metric in fs::read_dir(&path)? {
            let metric = metric?.path();
            if metric
                .extension()
                .is_some_and(|extension| extension == "parquet")
            {
                artifacts.push(metric);
            }
        }
    }
    artifacts.sort();
    ensure!(
        !artifacts.is_empty(),
        "no published Parquet artifacts to scrub"
    );
    let mut scrubbed = Vec::with_capacity(artifacts.len());
    let mut total_bytes = 0_u64;
    let mut total_rows = 0_u64;
    for artifact in artifacts {
        let document = read(&artifact)?;
        let scanned = scan(
            &artifact,
            &document.receipt.identity,
            &document.receipt.algorithm_version,
            &document.receipt.input_fingerprint,
        )?;
        ensure!(
            scanned == document.receipt,
            "deep semantic scrub disagrees with authoritative receipt for {}",
            artifact.display()
        );
        total_bytes = total_bytes
            .checked_add(scanned.bytes)
            .context("scrub byte total overflow")?;
        total_rows = total_rows
            .checked_add(scanned.rows)
            .context("scrub row total overflow")?;
        scrubbed.push(ScrubbedArtifact {
            path: artifact
                .strip_prefix(output_dir)
                .context("scrub artifact escaped output directory")?
                .to_string_lossy()
                .into_owned(),
            receipt_sha256: document.receipt_sha256,
            artifact_sha256: scanned.artifact_sha256,
            bytes: scanned.bytes,
            rows: scanned.rows,
            minimum_date: scanned.minimum_date,
            maximum_date: scanned.maximum_date,
            conservation_totals: scanned.conservation_totals,
            minimum_wiki: scanned.minimum_wiki,
            maximum_wiki: scanned.maximum_wiki,
        });
    }
    Ok(ScrubReport {
        schema_version: 2,
        scrubbed_at_unix: SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
        artifacts: scrubbed,
        total_bytes,
        total_rows,
    })
}

fn scrub_status_path(output_dir: &Path) -> PathBuf {
    output_dir.join(SCRUB_STATUS_PATH)
}

fn write_scrub_status(output_dir: &Path, status: &ScrubStatus) -> Result<()> {
    let path = scrub_status_path(output_dir);
    let parent = path.parent().context("scrub status path has no parent")?;
    fs::create_dir_all(parent)?;
    let temporary = parent.join(format!(".scrub-status-{}.tmp", std::process::id()));
    let result = (|| -> Result<()> {
        let mut file = File::create(&temporary)?;
        serde_json::to_writer_pretty(&mut file, status)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        fs::rename(&temporary, &path)?;
        File::open(parent)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

pub fn record_scrub_success(output_dir: &Path, run_id: &str, report: &ScrubReport) -> Result<()> {
    ensure!(!run_id.trim().is_empty(), "scrub run ID cannot be empty");
    let report_sha256 = hex::encode(Sha256::digest(serde_json::to_vec(report)?));
    write_scrub_status(
        output_dir,
        &ScrubStatus {
            schema_version: 1,
            state: "succeeded".to_string(),
            run_id: run_id.to_string(),
            updated_at_unix: SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
            report_sha256: Some(report_sha256),
            error: None,
        },
    )
}

pub fn record_scrub_failure(output_dir: &Path, run_id: &str, error: &anyhow::Error) -> Result<()> {
    ensure!(!run_id.trim().is_empty(), "scrub run ID cannot be empty");
    let concise = format!("{error:#}")
        .lines()
        .next()
        .unwrap_or("artifact scrub failed")
        .chars()
        .take(500)
        .collect();
    write_scrub_status(
        output_dir,
        &ScrubStatus {
            schema_version: 1,
            state: "failed".to_string(),
            run_id: run_id.to_string(),
            updated_at_unix: SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
            report_sha256: None,
            error: Some(concise),
        },
    )
}

pub fn ensure_publication_allowed(output_dir: &Path) -> Result<()> {
    let path = scrub_status_path(output_dir);
    if !path.is_file() {
        return Ok(());
    }
    let status: ScrubStatus = serde_json::from_slice(&fs::read(&path)?)?;
    ensure!(
        status.schema_version == 1 && matches!(status.state.as_str(), "succeeded" | "failed"),
        "invalid artifact scrub status"
    );
    ensure!(
        status.state != "failed",
        "publication is blocked by failed artifact scrub {}: {}",
        status.run_id,
        status.error.as_deref().unwrap_or("unknown scrub failure")
    );
    Ok(())
}

pub fn write_scrub_report(path: &Path, report: &ScrubReport) -> Result<()> {
    let parent = path.parent().context("scrub report path has no parent")?;
    fs::create_dir_all(parent)?;
    let temporary = parent.join(format!(".artifact-scrub-{}.tmp", std::process::id()));
    let result = (|| -> Result<()> {
        let mut file = File::create(&temporary)?;
        serde_json::to_writer_pretty(&mut file, report)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        fs::rename(&temporary, path)?;
        File::open(parent)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::TestDir;

    fn write_gdp(path: &Path, wiki: &str) -> Result<()> {
        let mut frame = df!(
            "year_month" => &["2026-01", "2026-02"],
            "total_edits" => &[3_u32, 5],
            "wiki" => &[wiki, wiki],
        )
        .expect("valid GDP receipt fixture");
        ParquetWriter::new(File::create(path).expect("create GDP receipt fixture"))
            .finish(&mut frame)
            .expect("write GDP receipt fixture");
        Ok(())
    }

    fn write_weekly(path: &Path, previous: &[u32], weeks: &[&str]) -> Result<()> {
        let mut frame = df!(
            "week_start" => weeks,
            "page_id" => &[Some(7_i64), Some(7)],
            "page_title" => &[Some("Page"), Some("Page")],
            "page_namespace" => &[Some(0_i32), Some(0)],
            "edits" => &[2_u32, 4],
            "previous_week_edits" => previous,
            "wiki" => &["nlwiki", "nlwiki"],
        )
        .expect("valid weekly receipt fixture");
        ParquetWriter::new(File::create(path).expect("create weekly receipt fixture"))
            .finish(&mut frame)
            .expect("write weekly receipt fixture");
        Ok(())
    }

    #[test]
    fn semantic_receipt_is_canonical_transactional_and_fail_closed() -> Result<()> {
        let directory = TestDir::new()?;
        let artifact = directory.path().join("gdp.parquet");
        write_gdp(&artifact, "nlwiki")?;
        let original = fs::read(&artifact)?;
        let document = scan_and_write(&artifact, "nlwiki/gdp.parquet", "gdp-v1", "inputs-v1")?;
        assert_eq!(document.receipt.rows, 2);
        assert_eq!(document.receipt.minimum_date.as_deref(), Some("2026-01"));
        assert_eq!(document.receipt.maximum_date.as_deref(), Some("2026-02"));
        assert_eq!(document.receipt.conservation_totals["total_edits"], 8);
        assert_eq!(document.receipt.minimum_wiki, "nlwiki");
        assert_eq!(document.receipt.maximum_wiki, "nlwiki");
        assert_eq!(document.receipt.parquet_schema.len(), 3);
        assert_eq!(
            canonical_receipt_sha256(&document.receipt)?,
            document.receipt_sha256
        );
        assert_eq!(
            verify(
                &artifact,
                "nlwiki/gdp.parquet",
                Some(&document.receipt_sha256),
                VerificationMode::Fast,
            )
            .expect("unchanged receipt pair verifies"),
            document
        );

        std::thread::sleep(std::time::Duration::from_millis(10));
        fs::write(&artifact, &original)?;
        verify(
            &artifact,
            "nlwiki/gdp.parquet",
            Some(&document.receipt_sha256),
            VerificationMode::Fast,
        )
        .expect("metadata-only change rehashes to the same identity");
        let mut changed = original.clone();
        changed[0] ^= 1;
        fs::write(&artifact, changed)?;
        assert!(
            verify(
                &artifact,
                "nlwiki/gdp.parquet",
                Some(&document.receipt_sha256),
                VerificationMode::Scrub,
            )
            .is_err()
        );
        fs::write(&artifact, original)?;

        let receipt_path = sidecar_path(&artifact)?;
        let mut corrupt = document.clone();
        corrupt.receipt.rows += 1;
        fs::write(&receipt_path, serde_json::to_vec(&corrupt)?)?;
        assert!(read(&artifact).is_err());
        assert!(sidecar_path(Path::new("/")).is_err());
        let mut unsafe_receipt = document.receipt.clone();
        unsafe_receipt.identity = "../escape".to_string();
        assert!(write(&artifact, unsafe_receipt).is_err());

        let write_failure = directory.path().join("write-failure.parquet");
        write_gdp(&write_failure, "nlwiki")?;
        let receipt = scan(&write_failure, "write-failure.parquet", "v1", "input")?;
        fs::create_dir(sidecar_path(&write_failure)?)?;
        assert!(write(&write_failure, receipt).is_err());
        assert!(
            !write_failure
                .parent()
                .context("write failure parent")?
                .join(format!(".artifact-receipt-{}.tmp", std::process::id()))
                .exists()
        );
        Ok(())
    }

    #[test]
    fn page_week_semantics_validate_order_previous_values_and_schema() -> Result<()> {
        let directory = TestDir::new()?;
        let valid = directory.path().join("page_weekly_edits.parquet");
        write_weekly(&valid, &[0, 2], &["2026-01-05", "2026-01-12"])?;
        let receipt = scan(
            &valid,
            "nlwiki/page_weekly_edits.parquet",
            "weekly-v1",
            "input",
        )
        .expect("valid page-week semantics scan");
        assert_eq!(receipt.rows, 2);
        assert_eq!(receipt.conservation_totals["edits"], 6);
        assert!(receipt.ordering_contract.contains("stable-page-hash"));

        let wrong_previous = directory
            .path()
            .join("wrong-previous/page_weekly_edits.parquet");
        fs::create_dir_all(wrong_previous.parent().context("fixture parent")?)?;
        write_weekly(&wrong_previous, &[1, 2], &["2026-01-05", "2026-01-12"])?;
        assert!(scan(&wrong_previous, "page_weekly_edits.parquet", "v", "i").is_err());

        let reversed = directory.path().join("reversed/page_weekly_edits.parquet");
        fs::create_dir_all(reversed.parent().context("fixture parent")?)?;
        write_weekly(&reversed, &[0, 0], &["2026-01-12", "2026-01-05"])?;
        assert!(scan(&reversed, "page_weekly_edits.parquet", "v", "i").is_err());

        let invalid_date = directory
            .path()
            .join("invalid-date/page_weekly_edits.parquet");
        fs::create_dir_all(invalid_date.parent().context("fixture parent")?)?;
        write_weekly(&invalid_date, &[0, 0], &["not-a-date", "still-not-a-date"])?;
        assert!(scan(&invalid_date, "page_weekly_edits.parquet", "v", "i").is_err());

        let mut accumulator = SemanticAccumulator::new(SemanticSpec::for_identity("gdp.parquet"));
        let first =
            df!("year_month" => &["2026-01"], "total_edits" => &[1_u32], "wiki" => &["nlwiki"])?;
        accumulator.observe(&first)?;
        let changed_schema =
            df!("year_month" => &["2026-02"], "total_edits" => &[1_i64], "wiki" => &["nlwiki"])?;
        assert!(accumulator.observe(&changed_schema).is_err());

        let mut null_wiki = SemanticAccumulator::new(SemanticSpec::for_identity("gdp.parquet"));
        let frame = df!("year_month" => &["2026-01"], "total_edits" => &[1_u32], "wiki" => &[None::<&str>])?;
        assert!(null_wiki.observe(&frame).is_err());

        assert_eq!(sum_numeric(&Column::new("v".into(), [1_u64, 2]), "v")?, 3);
        assert_eq!(sum_numeric(&Column::new("v".into(), [-1_i32, 2]), "v")?, 1);
        assert!(sum_numeric(&Column::new("v".into(), [true]), "v").is_err());
        Ok(())
    }

    #[test]
    fn semantic_drafts_and_scrubs_cover_writer_and_independent_hash_paths() -> Result<()> {
        let directory = TestDir::new()?;
        let root_artifact = directory.path().join("gdp.parquet");
        write_gdp(&root_artifact, "nlwiki")?;
        let frame = ParquetReader::new(File::open(&root_artifact)?).finish()?;
        let mut accumulator = SemanticAccumulator::new(SemanticSpec::for_identity("gdp.parquet"));
        accumulator.observe(&frame)?;
        write_semantic_draft(&root_artifact, accumulator)?;
        let root_receipt = scan_and_write(&root_artifact, "gdp.parquet", "merge-v1", "inputs")?;
        assert!(!draft_path(&root_artifact)?.exists());
        assert_eq!(root_receipt.receipt.algorithm_version, "merge-v1");

        fs::write(draft_path(&root_artifact)?, b"truncated")?;
        scan_and_write(&root_artifact, "gdp.parquet", "merge-v2", "inputs")?;
        let mut stale = SemanticAccumulator::new(SemanticSpec::for_identity("gdp.parquet"));
        stale.observe(&frame)?;
        write_semantic_draft(&root_artifact, stale)?;
        std::thread::sleep(std::time::Duration::from_millis(10));
        write_gdp(&root_artifact, "nlwiki")?;
        scan_and_write(&root_artifact, "gdp.parquet", "merge-v3", "inputs")?;

        let mut unsupported = SemanticAccumulator::new(SemanticSpec::for_identity("gdp.parquet"));
        unsupported.observe(&frame)?;
        write_semantic_draft(&root_artifact, unsupported)?;
        let draft = draft_path(&root_artifact)?;
        let mut draft_json: serde_json::Value = serde_json::from_slice(&fs::read(&draft)?)?;
        draft_json["schema_version"] = serde_json::json!(99);
        fs::write(&draft, serde_json::to_vec(&draft_json)?)?;
        scan_and_write(&root_artifact, "gdp.parquet", "merge-v4", "inputs")?;

        scan_and_write_with_spec(
            &root_artifact,
            "gdp.parquet",
            "explicit-spec-v1",
            "inputs",
            SemanticSpec::for_identity("gdp.parquet"),
        )
        .expect("explicit semantic specification scans and writes");

        let wiki_dir = directory.path().join("nlwiki");
        fs::create_dir_all(&wiki_dir)?;
        let wiki_artifact = wiki_dir.join("gdp.parquet");
        write_gdp(&wiki_artifact, "nlwiki")?;
        scan_and_write(&wiki_artifact, "nlwiki/gdp.parquet", "compute-v1", "inputs")?;
        fs::create_dir_all(directory.path().join("_ignored"))?;
        fs::write(directory.path().join("notes.txt"), "not an artifact")?;

        let report = scrub_published(directory.path())?;
        assert_eq!(report.schema_version, 2);
        assert_eq!(report.artifacts.len(), 2);
        assert_eq!(report.total_rows, 4);
        assert!(report.total_bytes > 0);
        assert!(report.artifacts.iter().all(|artifact| {
            artifact.minimum_wiki == "nlwiki"
                && artifact.maximum_wiki == "nlwiki"
                && artifact.conservation_totals.get("total_edits") == Some(&8)
        }));
        let report_path = directory.path().join("reports/scrub.json");
        write_scrub_report(&report_path, &report)?;
        let reread: ScrubReport = serde_json::from_slice(&fs::read(report_path)?)?;
        assert_eq!(reread, report);

        record_scrub_success(directory.path(), "scrub-success", &report)?;
        ensure_publication_allowed(directory.path())?;
        let failure = anyhow::anyhow!("semantic mismatch");
        record_scrub_failure(directory.path(), "scrub-failure", &failure)?;
        assert!(ensure_publication_allowed(directory.path()).is_err());
        record_scrub_success(directory.path(), "scrub-recovery", &report)?;
        ensure_publication_allowed(directory.path())?;
        fs::write(scrub_status_path(directory.path()), b"{invalid")?;
        assert!(ensure_publication_allowed(directory.path()).is_err());
        record_scrub_success(directory.path(), "scrub-restored", &report)?;

        let blocked_report = directory.path().join("blocked-report");
        fs::create_dir(&blocked_report)?;
        assert!(write_scrub_report(&blocked_report, &report).is_err());
        assert!(
            !directory
                .path()
                .join(format!(".artifact-scrub-{}.tmp", std::process::id()))
                .exists()
        );

        let empty = TestDir::new()?;
        assert!(scrub_published(empty.path()).is_err());
        let original = fs::read(&root_artifact)?;
        let mut corrupt = original.clone();
        corrupt[0] ^= 1;
        fs::write(&root_artifact, corrupt)?;
        assert!(scrub_published(directory.path()).is_err());
        fs::write(&root_artifact, original)?;
        let valid_root = fs::read(&root_artifact)?;
        fs::write(&root_artifact, b"not parquet")?;
        assert!(scrub_published(directory.path()).is_err());
        fs::write(&root_artifact, valid_root)?;
        fs::remove_file(sidecar_path(&wiki_artifact)?)?;
        assert!(scrub_published(directory.path()).is_err());

        let status_path = scrub_status_path(directory.path());
        fs::remove_file(&status_path)?;
        fs::create_dir(&status_path)?;
        assert!(record_scrub_success(directory.path(), "blocked-status", &report).is_err());
        assert!(
            !status_path
                .parent()
                .context("scrub status parent")?
                .join(format!(".scrub-status-{}.tmp", std::process::id()))
                .exists()
        );
        Ok(())
    }
}
