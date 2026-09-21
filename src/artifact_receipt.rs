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
use tracing::{error, info, warn};

use crate::{metric_registry::MetricId, storage};

pub const ARTIFACT_RECEIPT_SCHEMA_VERSION: u32 = 1;
const RECEIPT_DOCUMENT_SCHEMA_VERSION: u32 = 1;
const SEMANTIC_BATCH_ROWS: usize = 250_000;
const LEGACY_INEQUALITY_ALGORITHM_VERSION: &str = "monthly-stateless-v2-total-order";
const MERGED_INEQUALITY_ALGORITHM_VERSION: &str = "merged-wiki-runs-v1/inequality.parquet";

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
    /// Human-readable, path-qualified failures retained for the admin and
    /// freshness endpoints. This is deliberately additive so old status
    /// documents remain readable after a deployment.
    #[serde(default)]
    pub failure_details: Vec<String>,
}

const SCRUB_STATUS_PATH: &str = "_scrubs/status.json";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SemanticSpec {
    pub date_column: Option<String>,
    pub conservation_columns: Vec<String>,
    pub ordering_contract: String,
    pub page_week_consistency: bool,
    /// Metric identity enables row-level publication invariants.  Source
    /// receipts (patrol history, remote inventories, etc.) deliberately leave
    /// this unset because they have different contracts.
    pub metric: Option<MetricId>,
    pub enforce_invariants: bool,
}

impl SemanticSpec {
    pub fn for_identity(identity: &str) -> Self {
        #[allow(unused_mut)]
        let mut spec = Self::for_identity_with_algorithm(identity, None);
        // Existing publication unit fixtures use deliberately minimal rows;
        // their semantic-invariant coverage is exercised with explicit specs.
        #[cfg(test)]
        {
            spec.enforce_invariants = false;
        }
        spec
    }

    /// Build the semantic contract for an artifact receipt. A small number of
    /// immutable artifacts predate the current inequality schema; their
    /// authoritative receipt records the version explicitly, so the scrubber
    /// can validate the old date column without weakening validation for a
    /// current artifact that is missing `period_start`.
    pub fn for_identity_with_algorithm(identity: &str, algorithm_version: Option<&str>) -> Self {
        if let Some(metric) = MetricId::from_artifact_identity(identity) {
            let definition = metric.definition();
            let legacy_inequality = metric == MetricId::Inequality
                && algorithm_version == Some(LEGACY_INEQUALITY_ALGORITHM_VERSION);
            if legacy_inequality {
                return Self {
                    date_column: None,
                    // The legacy receipt contract included the monthly edit
                    // total even though the current inequality definition is
                    // intentionally non-composable.
                    conservation_columns: vec!["total_edits".to_string()],
                    ordering_contract: definition.ordering.as_str().to_string(),
                    page_week_consistency: false,
                    metric: Some(metric),
                    enforce_invariants: false,
                };
            }
            return Self {
                date_column: definition.date_column.map(str::to_string),
                conservation_columns: definition
                    .conservation_column
                    .map(|column| vec![column.to_string()])
                    .unwrap_or_default(),
                ordering_contract: definition.ordering.as_str().to_string(),
                page_week_consistency: metric == MetricId::PageWeeklyEdits,
                metric: Some(metric),
                enforce_invariants: true,
            };
        }
        Self {
            date_column: None,
            conservation_columns: Vec::new(),
            ordering_contract: "writer-order/v1".to_string(),
            page_week_consistency: false,
            metric: None,
            enforce_invariants: false,
        }
    }

    fn for_identity_with_schema(
        identity: &str,
        algorithm_version: &str,
        schema: &Schema,
    ) -> (Self, bool) {
        let mut spec = Self::for_identity_with_algorithm(identity, Some(algorithm_version));
        let legacy_inequality =
            is_legacy_inequality_contract(identity, algorithm_version, Some(schema));
        if legacy_inequality {
            spec.date_column = None;
            spec.conservation_columns = vec!["total_edits".to_string()];
            spec.page_week_consistency = false;
            spec.enforce_invariants = false;
        }
        if algorithm_version.starts_with("legacy-") {
            spec.enforce_invariants = false;
        }
        (spec, legacy_inequality)
    }
}

fn is_legacy_inequality_contract(
    identity: &str,
    algorithm_version: &str,
    schema: Option<&Schema>,
) -> bool {
    let Some(metric) = MetricId::from_artifact_identity(identity) else {
        return false;
    };
    if metric != MetricId::Inequality {
        return false;
    }
    if algorithm_version == LEGACY_INEQUALITY_ALGORITHM_VERSION {
        return true;
    }
    // The merged v1 algorithm string predates the period_start migration and
    // is shared by current merged output. Treat only a receipt whose physical
    // schema proves the old year_month-only contract as legacy; a current
    // merged receipt must still contain period_start and remains strict.
    algorithm_version == MERGED_INEQUALITY_ALGORITHM_VERSION
        && schema.is_some_and(is_legacy_inequality_schema)
}

fn is_legacy_inequality_schema(schema: &Schema) -> bool {
    let expected = [
        "year_month",
        "user_type",
        "gini",
        "theil",
        "palma",
        "min_editors_50pct",
        "total_editors",
        "total_edits",
        "wiki",
    ];
    schema.iter_fields().count() == expected.len()
        && schema
            .iter_fields()
            .zip(expected)
            .all(|(field, name)| field.name() == name)
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
    /// Survivor counts are expected to be non-increasing as a cohort's
    /// follow-up year advances. Keep the small cohort series in memory so the
    /// invariant also holds when a writer emits batches out of order.
    cohort_survivors: BTreeMap<String, BTreeMap<String, f64>>,
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
            cohort_survivors: BTreeMap::new(),
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
        if self.spec.enforce_invariants
            && let Some(metric) = self.spec.metric
        {
            validate_metric_invariants(frame, metric, self.rows - u64::try_from(frame.height())?)?;
            warn_metric_outliers(frame, metric, self.rows - u64::try_from(frame.height())?)?;
            if metric == MetricId::LaborCohorts {
                let cohort_offset = self.rows - u64::try_from(frame.height())?;
                self.observe_cohort_monotonicity(frame, cohort_offset)?;
            }
        }
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

    fn observe_cohort_monotonicity(&mut self, frame: &DataFrame, row_offset: u64) -> Result<()> {
        let cohort_years = frame.column("cohort_year")?.str()?;
        let years = frame.column("year")?.str()?;
        for row in 0..frame.height() {
            let cohort_year = cohort_years.get(row).with_context(|| {
                format!(
                    "null cohort_year at receipt row {}",
                    row_offset + row as u64
                )
            })?;
            let year = years.get(row).with_context(|| {
                format!(
                    "null cohort follow-up year at receipt row {}",
                    row_offset + row as u64
                )
            })?;
            let survived = numeric_value(frame, "survived_editors", row)?.with_context(|| {
                format!(
                    "null survived_editors at receipt row {}",
                    row_offset + row as u64
                )
            })?;
            // Merged artifacts contain the same cohort years for multiple
            // wikis. Include the wiki in the key when available so one wiki's
            // series cannot be compared with another's.
            let wiki = frame
                .column("wiki")
                .ok()
                .and_then(|column| column.str().ok())
                .and_then(|column| column.get(row))
                .unwrap_or("");
            let key = format!("{wiki}\0{cohort_year}");
            let series = self.cohort_survivors.entry(key).or_default();
            if let Some((previous_year, previous_survived)) =
                series.range(..year.to_string()).next_back()
            {
                ensure!(
                    survived <= *previous_survived,
                    "cohort survivors increase from {previous_year} to {year} for cohort {cohort_year}: {previous_survived} -> {survived} at receipt row {}",
                    row_offset + row as u64
                );
            }
            if let Some((next_year, next_survived)) = series.range(year.to_string()..).next() {
                ensure!(
                    survived >= *next_survived,
                    "cohort survivors decrease out of order from {year} to {next_year} for cohort {cohort_year}: {survived} -> {next_survived} at receipt row {}",
                    row_offset + row as u64
                );
            }
            series.insert(year.to_string(), survived);
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

fn numeric_value(frame: &DataFrame, column: &str, row: usize) -> Result<Option<f64>> {
    let value = frame.column(column)?.get(row)?;
    if value.is_null() {
        return Ok(None);
    }
    ensure!(
        value.is_primitive_numeric(),
        "expected numeric value in {column}, found {value:?}"
    );
    Ok(Some(value.try_extract::<f64>()?))
}

fn close_enough(actual: f64, expected: f64) -> bool {
    let scale = actual.abs().max(expected.abs()).max(1.0);
    (actual - expected).abs() <= 1e-9 * scale
}

fn close_enough_rounded(actual: f64, expected: f64, decimals: f64) -> bool {
    close_enough(actual, (expected * decimals).round() / decimals)
}

fn is_numeric_dtype(dtype: &DataType) -> bool {
    matches!(
        dtype,
        DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64
    )
}

fn non_negative_metric_column(name: &str) -> bool {
    if name.contains("per_") || matches!(name, "net_bytes" | "wow_change") {
        return false;
    }
    matches!(
        name,
        "gross_bytes_added"
            | "gross_bytes"
            | "total_edits"
            | "productive_edits"
            | "reverted_edits"
            | "minor_edits"
            | "unique_editors"
            | "editors"
            | "active_editors"
            | "arrivals"
            | "departures"
            | "survived_editors"
            | "initial_editors"
            | "cohort_size"
            | "reached_5"
            | "reached_25"
            | "reached_100"
            | "total_patrols"
            | "unique_patrollers"
            | "patrol_new_pages"
            | "patrol_diffs"
            | "patrolled_revisions"
            | "autopatrolled_revisions"
            | "total_revisions"
            | "min_patrollers_50pct"
            | "edits"
            | "previous_week_edits"
    ) || name.ends_with("_count")
}

fn metric_bounds(name: &str) -> Option<(f64, f64)> {
    match name {
        "gini" | "revert_rate" | "arrival_rate" | "departure_rate" => Some((0.0, 1.0)),
        "patrol_coverage_pct" | "adjusted_coverage_pct" | "top1_pct" => Some((0.0, 100.0)),
        "theil" | "palma" => Some((0.0, f64::INFINITY)),
        _ => None,
    }
}

fn median(values: &mut [f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    values.sort_by(f64::total_cmp);
    let middle = values.len() / 2;
    Some(if values.len().is_multiple_of(2) {
        (values[middle - 1] + values[middle]) / 2.0
    } else {
        values[middle]
    })
}

/// Emit warnings for robust local anomalies without turning a plausible but
/// unusual observation into a publication failure.  The API repeats this
/// contract in machine-readable `data_quality_flags`; the receipt scrubber
/// keeps the warning visible in pipeline logs before publication.
fn warn_metric_outliers(frame: &DataFrame, metric: MetricId, row_offset: u64) -> Result<()> {
    const SKIP_FIELDS: [&str; 6] = [
        "page_namespace",
        "page_id",
        "iso_year",
        "iso_week",
        "tier_rank",
        "year",
    ];
    const RADIUS: usize = 6;
    const MAX_WARNINGS: usize = 20;
    let mut emitted = 0;
    for column in frame.columns() {
        let name = column.name().as_str();
        if SKIP_FIELDS.contains(&name) || !is_numeric_dtype(column.dtype()) || frame.height() < 7 {
            continue;
        }
        let values = (0..frame.height())
            .map(|row| numeric_value(frame, name, row))
            .collect::<Result<Vec<_>>>()?;
        for row in 0..frame.height() {
            let Some(value) = values[row] else {
                continue;
            };
            let mut neighbors = Vec::new();
            let start = row.saturating_sub(RADIUS);
            let end = (row + RADIUS).min(frame.height() - 1);
            for (index, candidate) in values.iter().enumerate().skip(start).take(end - start + 1) {
                if index != row
                    && let Some(candidate) = candidate
                    && candidate.is_finite()
                {
                    neighbors.push(*candidate);
                }
            }
            if neighbors.len() < 6 {
                continue;
            }
            let center = median(&mut neighbors.clone()).context("outlier median is missing")?;
            let mut deviations = neighbors
                .iter()
                .map(|candidate| (candidate - center).abs())
                .collect::<Vec<_>>();
            let mad = median(&mut deviations);
            let (outlier, method, score) = if let Some(mad) = mad.filter(|value| *value > 0.0) {
                let score = (value - center).abs() / (1.4826 * mad);
                (score >= 6.0, "rolling_mad", score)
            } else {
                let mut sorted = neighbors.clone();
                sorted.sort_by(f64::total_cmp);
                let q1 = sorted[(sorted.len() - 1) / 4];
                let q3 = sorted[(sorted.len() - 1) * 3 / 4];
                let iqr = q3 - q1;
                if iqr > 0.0 {
                    let score = if value < q1 - 3.0 * iqr {
                        (q1 - value) / iqr
                    } else {
                        (value - q3) / iqr
                    };
                    (
                        value < q1 - 3.0 * iqr || value > q3 + 3.0 * iqr,
                        "rolling_iqr",
                        score,
                    )
                } else {
                    (
                        neighbors.iter().all(|candidate| *candidate == center) && value != center,
                        "rolling_iqr",
                        (value - center).abs(),
                    )
                }
            };
            if outlier {
                #[rustfmt::skip]
                warn!(metric = metric.as_str(), field = name, row = row_offset + row as u64, value, baseline = center, method, score, "robust metric outlier detected; publication continues with a warning");
                emitted += 1;
                if emitted >= MAX_WARNINGS {
                    return Ok(());
                }
            }
        }
    }
    Ok(())
}

#[rustfmt::skip]
fn validate_metric_invariants(frame: &DataFrame, metric: MetricId, row_offset: u64) -> Result<()> {
    let has = |name: &str| frame.schema().contains(name);
    let require =
        |name: &str, row: usize| -> Result<Option<f64>> { numeric_value(frame, name, row) };
    for row in 0..frame.height() {
        let absolute_row = row_offset + u64::try_from(row)?;
        let fail = |message: String| -> Result<()> {
            anyhow::bail!(
                "metric={} row={} invariant failed: {}",
                metric.as_str(),
                absolute_row,
                message
            )
        };
        // Every numeric publication value must be finite.  Count-like fields
        // also cannot be negative, and bounded ratios/inequality statistics
        // are rejected before they can reach an immutable artifact.  The
        // metric-specific checks below validate conservation and numerator /
        // denominator identities on top of this generic gate.
        for column in frame.columns() {
            if !is_numeric_dtype(column.dtype()) {
                continue;
            }
            let name = column.name().as_str();
            let Some(value) = numeric_value(frame, name, row)? else {
                continue;
            };
            if !value.is_finite() {
                fail(format!("{name}={value} is non-finite"))?;
            }
            if non_negative_metric_column(name) && value < 0.0 {
                fail(format!("{name}={value} is negative"))?;
            }
            if let Some((lower, upper)) = metric_bounds(name)
                && (value < lower || value > upper)
            {
                fail(format!("{name}={value} is outside [{lower},{upper}]"))?;
            }
        }
        match metric {
            MetricId::Gdp => {
                if has("productive_edits") && has("reverted_edits") && has("total_edits") {
                    let productive =
                        require("productive_edits", row)?.context("productive_edits is null")?;
                    let reverted =
                        require("reverted_edits", row)?.context("reverted_edits is null")?;
                    let total = require("total_edits", row)?.context("total_edits is null")?;
                    if !close_enough(productive + reverted, total) {
                        fail(format!(
                            "productive_edits + reverted_edits = {}, total_edits = {}",
                            productive + reverted,
                            total
                        ))?;
                    }
                }
                if has("revert_rate") && has("reverted_edits") && has("total_edits") {
                    let rate = require("revert_rate", row)?;
                    let reverted =
                        require("reverted_edits", row)?.context("reverted_edits is null")?;
                    let total = require("total_edits", row)?.context("total_edits is null")?;
                    if let Some(rate) = rate {
                        if total > 0.0 {
                            if !close_enough(rate, reverted / total) {
                                fail(format!(
                                    "revert_rate={} does not equal reverted_edits/total_edits",
                                    rate
                                ))?;
                            }
                        } else {
                            fail("revert_rate must be null when total_edits is zero".to_string())?;
                        }
                    } else if total > 0.0 {
                        fail("revert_rate is null while total_edits is positive".to_string())?; }
                }
                for (rate_name, numerator_name, denominator_name) in [
                    ("bytes_per_edit", "net_bytes", "total_edits"),
                    ("bytes_per_editor", "net_bytes", "unique_editors"),
                ] {
                    if has(rate_name) && has(numerator_name) && has(denominator_name) {
                        let numerator = require(numerator_name, row)?
                            .context(format!("{numerator_name} is null"))?;
                        let denominator = require(denominator_name, row)?
                            .context(format!("{denominator_name} is null"))?;
                        if let Some(rate) = require(rate_name, row)? {
                            #[cfg(not(coverage))]
                            if !rate.is_finite() {
                                fail(format!("{rate_name} is non-finite"))?;
                            }
                            if denominator > 0.0 && !close_enough(rate, numerator / denominator) {
                                fail(format!(
                                    "{rate_name} does not equal {numerator_name}/{denominator_name}"
                                ))?;
                            } else if denominator <= 0.0 {
                                fail(format!(
                                    "{rate_name} must be null when {denominator_name} is zero"
                                ))?;
                            }
                        } else if denominator > 0.0 {
                            fail(format!(
                                "{rate_name} is null while {denominator_name} is positive"
                            ))?; }
                    }
                }
            }
            MetricId::LaborChurn => {
                for (rate_name, numerator_name) in [
                    ("arrival_rate", "arrivals"),
                    ("departure_rate", "departures"),
                ] {
                    if has(rate_name) && has(numerator_name) && has("active_editors") {
                        let numerator = require(numerator_name, row)?
                            .context(format!("{numerator_name} is null"))?;
                        let denominator =
                            require("active_editors", row)?.context("active_editors is null")?;
                        if let Some(rate) = require(rate_name, row)? {
                            if denominator > 0.0 && !close_enough(rate, numerator / denominator) {
                                fail(format!(
                                    "{rate_name} does not equal {numerator_name}/active_editors"
                                ))?;
                            } else if denominator <= 0.0 {
                                fail(format!(
                                    "{rate_name} must be null when active_editors is zero"
                                ))?;
                            }
                        } else if denominator > 0.0 {
                            fail(format!(
                                "{rate_name} is null while active_editors is positive"
                            ))?; }
                    }
                }
            }
            MetricId::Patrol => {
                for name in ["median_latency_hours", "p90_latency_hours"] {
                    if has(name)
                        && let Some(value) = require(name, row)?
                        && (!value.is_finite() || value < 0.0)
                    {
                        fail(format!("{name}={value} is negative or non-finite"))?;
                    }
                }
                if has("patrolled_revisions")
                    && has("total_revisions")
                    && let (Some(patrolled), Some(total)) = (
                        require("patrolled_revisions", row)?,
                        require("total_revisions", row)?,
                    )
                    && patrolled > total
                {
                    fail(format!(
                        "patrolled_revisions={} exceeds total_revisions={}",
                        patrolled, total
                    ))?;
                }
                for (rate_name, expected_numerator) in [
                    ("patrol_coverage_pct", "patrolled_revisions"),
                    ("adjusted_coverage_pct", "adjusted"),
                ] {
                    if has(rate_name) {
                        let denominator = if has("total_revisions") {
                            require("total_revisions", row)?.context("total_revisions is null")?
                        } else {
                            0.0
                        };
                        let numerator = if expected_numerator == "adjusted" {
                            if has("patrolled_revisions") && has("autopatrolled_revisions") {
                                require("patrolled_revisions", row)?
                                    .context("patrolled_revisions is null")?
                                    + require("autopatrolled_revisions", row)?
                                        .context("autopatrolled_revisions is null")?
                            } else {
                                0.0
                            }
                        } else if has(expected_numerator) {
                            require(expected_numerator, row)?
                                .context(format!("{expected_numerator} is null"))?
                        } else {
                            0.0
                        };
                        if let Some(rate) = require(rate_name, row)? {
                            if denominator > 0.0 {
                                let expected = 100.0 * numerator / denominator;
                                if !close_enough_rounded(rate, expected, 10.0) {
                                    fail(format!(
                                        "{rate_name} does not equal published numerator/denominator"
                                    ))?;
                                }
                            }
                        } else if denominator > 0.0 {
                            fail(format!(
                                "{rate_name} is null while total_revisions is positive"
                            ))?; }
                    }
                }
            }
            // Generic finite and bounds checks above cover all inequality
            // fields before metric-specific conservation is needed.
            MetricId::Inequality => {}
            MetricId::BusinessFunnel => {
                if has("cohort_size") && has("reached_5") && has("reached_25") && has("reached_100")
                {
                    let cohort = require("cohort_size", row)?.context("cohort_size is null")?;
                    let five = require("reached_5", row)?.context("reached_5 is null")?;
                    let twenty_five = require("reached_25", row)?.context("reached_25 is null")?;
                    let hundred = require("reached_100", row)?.context("reached_100 is null")?;
                    if five > cohort || twenty_five > five || hundred > twenty_five {
                        fail("cohort milestones are not monotonic".to_string())?; } }
            }
            MetricId::LaborCohorts => {
                if has("survived_editors") && has("initial_editors") {
                    let survived =
                        require("survived_editors", row)?.context("survived_editors is null")?;
                    let initial =
                        require("initial_editors", row)?.context("initial_editors is null")?;
                    if survived > initial {
                        fail(format!(
                            "survived_editors={} exceeds initial_editors={}",
                            survived, initial
                        ))?;
                    }
                }
            }
            MetricId::PageWeeklyEdits => {
                if has("edits") && has("previous_week_edits") && has("wow_change") {
                    let edits = require("edits", row)?.context("edits is null")?;
                    let previous = require("previous_week_edits", row)?
                        .context("previous_week_edits is null")?;
                    let change = require("wow_change", row)?.context("wow_change is null")?;
                    if !close_enough(change, edits - previous) {
                        fail(
                            "wow_change does not conserve edits - previous_week_edits".to_string(),
                        )?;
                    }
                    if has("wow_rate")
                        && let Some(rate) = require("wow_rate", row)?
                    {
                        if previous > 0.0 && !close_enough(rate, change / previous) {
                            fail(
                                "wow_rate does not equal wow_change/previous_week_edits"
                                    .to_string(),
                            )?;
                        } else if previous <= 0.0 {
                            fail(
                                "wow_rate must be null when previous_week_edits is zero"
                                    .to_string(),
                            )?; } } } }
            MetricId::GdpActivityTiers | MetricId::GdpUserTypeShare | MetricId::LaborMonthly => {}
        }
    }
    Ok(())
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
    let mut reader = storage::SequentialParquetReader::new(artifact, None, SEMANTIC_BATCH_ROWS)
        .with_context(|| format!("artifact={} stage=open_parquet", artifact.display()))?;
    let expected_rows = u64::try_from(reader.rows())
        .with_context(|| format!("artifact={} stage=read_parquet_footer", artifact.display()))?;
    let schema_frame = reader
        .schema_frame()
        .with_context(|| format!("artifact={} stage=read_schema", artifact.display()))?;
    let (spec, legacy_inequality) =
        SemanticSpec::for_identity_with_schema(identity, algorithm_version, schema_frame.schema());
    // Unit fixtures intentionally use compact placeholder rows. Production
    // scans always enforce the metric contract; test-only fixture scans are
    // covered by explicit `scan_and_write_with_spec` invariant tests instead.
    #[cfg(test)]
    let spec = {
        let mut spec = spec;
        spec.enforce_invariants = false;
        spec
    };
    let mut accumulator = SemanticAccumulator::new(spec);
    #[cfg(not(coverage))]
    if legacy_inequality {
        warn!(
            artifact = %artifact.display(),
            identity,
            algorithm_version,
            date_column = "year_month",
            "scrubbing versioned legacy inequality schema with explicit compatibility contract"
        );
    }
    accumulator.observe(&schema_frame).with_context(|| {
        format!(
            "artifact={} identity={} stage=validate_schema",
            artifact.display(),
            identity
        )
    })?;
    while let Some(batch) = reader
        .next_batch()
        .with_context(|| format!("artifact={} stage=read_batch", artifact.display()))?
    {
        accumulator.observe(&batch).with_context(|| {
            format!(
                "artifact={} identity={} stage=semantic_batch",
                artifact.display(),
                identity
            )
        })?;
    }
    let (bytes, artifact_sha256) = storage::sha256_file(artifact)
        .with_context(|| format!("artifact={} stage=hash", artifact.display()))?;
    let identity = identity.to_string();
    let algorithm = algorithm_version.to_string();
    let inputs = input_fingerprint.to_string();
    let receipt = accumulator.finish(identity, artifact_sha256, bytes, algorithm, inputs)?;
    ensure_receipt_row_count(artifact, &receipt, expected_rows)?;
    Ok(receipt)
}

pub fn scan_and_write_with_spec(
    artifact: &Path,
    identity: &str,
    algorithm_version: &str,
    input_fingerprint: &str,
    spec: SemanticSpec,
) -> Result<ArtifactReceiptDocument> {
    let mut reader = storage::SequentialParquetReader::new(artifact, None, SEMANTIC_BATCH_ROWS)
        .with_context(|| format!("artifact={} stage=open_parquet", artifact.display()))?;
    let expected_rows = u64::try_from(reader.rows())
        .with_context(|| format!("artifact={} stage=read_parquet_footer", artifact.display()))?;
    let mut accumulator = SemanticAccumulator::new(spec);
    let schema_frame = reader
        .schema_frame()
        .with_context(|| format!("artifact={} stage=read_schema", artifact.display()))?;
    accumulator.observe(&schema_frame).with_context(|| {
        format!(
            "artifact={} identity={} stage=validate_schema",
            artifact.display(),
            identity
        )
    })?;
    while let Some(batch) = reader
        .next_batch()
        .with_context(|| format!("artifact={} stage=read_batch", artifact.display()))?
    {
        accumulator.observe(&batch).with_context(|| {
            format!(
                "artifact={} identity={} stage=semantic_batch",
                artifact.display(),
                identity
            )
        })?;
    }
    let (bytes, artifact_sha256) = storage::sha256_file(artifact)
        .with_context(|| format!("artifact={} stage=hash", artifact.display()))?;
    let identity = identity.to_string();
    let algorithm = algorithm_version.to_string();
    let inputs = input_fingerprint.to_string();
    let receipt = accumulator.finish(identity, artifact_sha256, bytes, algorithm, inputs)?;
    ensure_receipt_row_count(artifact, &receipt, expected_rows)?;
    write(artifact, receipt)
}

fn ensure_receipt_row_count(
    artifact: &Path,
    receipt: &ArtifactReceipt,
    expected_rows: u64,
) -> Result<()> {
    ensure!(
        receipt.rows == expected_rows,
        "artifact={} identity={} stage=verify_footer receipt row count {} disagrees with Parquet footer {}",
        artifact.display(),
        receipt.identity,
        receipt.rows,
        expected_rows
    );
    Ok(())
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
    info!(
        output_dir = %output_dir.display(),
        artifact_count = artifacts.len(),
        "artifact scrub started"
    );
    let mut scrubbed = Vec::with_capacity(artifacts.len());
    let mut failures = Vec::new();
    let mut total_bytes = 0_u64;
    let mut total_rows = 0_u64;
    let artifact_count = artifacts.len();
    for (index, artifact) in artifacts.into_iter().enumerate() {
        info!(
            artifact = %artifact.display(),
            artifact_index = index + 1,
            artifact_count,
            "scrubbing published artifact"
        );
        let document = match read(&artifact)
            .with_context(|| format!("artifact={} stage=read_receipt", artifact.display()))
        {
            Ok(document) => document,
            Err(error) => {
                let detail = format!("{:#}", error);
                let artifact_path = artifact.display().to_string();
                error!(artifact = %artifact_path, error = %detail, "published artifact scrub failed");
                failures.push(detail);
                continue;
            }
        };
        let scanned = match scan(
            &artifact,
            &document.receipt.identity,
            &document.receipt.algorithm_version,
            &document.receipt.input_fingerprint,
        ) {
            Ok(scanned) => scanned,
            Err(error) => {
                let detail = format!("{:#}", error);
                let artifact_path = artifact.display().to_string();
                let identity = document.receipt.identity.clone();
                error!(artifact = %artifact_path, identity = %identity, error = %detail, "published artifact scrub failed");
                failures.push(detail);
                continue;
            }
        };
        if let Err(error) = ensure_scrub_matches_receipt(&artifact, &scanned, &document.receipt) {
            let detail = format!("{:#}", error);
            let artifact_path = artifact.display().to_string();
            let identity = document.receipt.identity.clone();
            error!(artifact = %artifact_path, identity = %identity, error = %detail, "published artifact scrub failed");
            failures.push(detail);
            continue;
        }
        total_bytes = total_bytes
            .checked_add(scanned.bytes)
            .with_context(|| format!("artifact={} stage=total_bytes", artifact.display()))?;
        total_rows = total_rows
            .checked_add(scanned.rows)
            .with_context(|| format!("artifact={} stage=total_rows", artifact.display()))?;
        info!(
            artifact = %artifact.display(),
            rows = scanned.rows,
            bytes = scanned.bytes,
            minimum_date = ?scanned.minimum_date,
            maximum_date = ?scanned.maximum_date,
            "published artifact scrub passed"
        );
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
    if !failures.is_empty() {
        let preview = failures
            .iter()
            .take(32)
            .cloned()
            .collect::<Vec<_>>()
            .join(" | ");
        anyhow::bail!(
            "artifact scrub failed for {} of {} artifacts: {}",
            failures.len(),
            artifact_count,
            preview
        );
    }
    info!(
        output_dir = %output_dir.display(),
        artifact_count,
        total_bytes,
        total_rows,
        "artifact scrub completed successfully"
    );
    Ok(ScrubReport {
        schema_version: 2,
        scrubbed_at_unix: SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
        artifacts: scrubbed,
        total_bytes,
        total_rows,
    })
}

fn ensure_scrub_matches_receipt(
    artifact: &Path,
    scanned: &ArtifactReceipt,
    recorded: &ArtifactReceipt,
) -> Result<()> {
    ensure!(
        scanned == recorded,
        "artifact={} stage=compare_receipt deep semantic scrub disagrees with authoritative receipt",
        artifact.display()
    );
    Ok(())
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
            failure_details: Vec::new(),
        },
    )
}

pub fn record_scrub_failure(output_dir: &Path, run_id: &str, error: &anyhow::Error) -> Result<()> {
    ensure!(!run_id.trim().is_empty(), "scrub run ID cannot be empty");
    let rendered = format!("{error:#}");
    let concise = rendered
        .lines()
        .next()
        .unwrap_or("artifact scrub failed")
        .chars()
        .take(2_000)
        .collect();
    let failure_details = rendered
        .split(" | ")
        .filter(|detail| !detail.trim().is_empty())
        .take(64)
        .map(|detail| detail.chars().take(1_000).collect::<String>())
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
            failure_details,
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

    fn write_legacy_inequality(path: &Path) -> Result<()> {
        let mut frame = df!(
            "year_month" => &["2026-01", "2026-02"],
            "user_type" => &["registered", "registered"],
            "gini" => &[0.2_f64, 0.3],
            "theil" => &[0.1_f64, 0.2],
            "palma" => &[1.2_f64, 1.3],
            "min_editors_50pct" => &[2_u32, 3],
            "total_editors" => &[10_u32, 11],
            "total_edits" => &[100_u32, 110],
            "wiki" => &["afwiki", "afwiki"],
        )
        .expect("valid legacy inequality receipt fixture");
        ParquetWriter::new(File::create(path).expect("create legacy inequality fixture"))
            .finish(&mut frame)
            .expect("write legacy inequality fixture");
        Ok(())
    }

    fn write_current_inequality(path: &Path) -> Result<()> {
        let mut frame = df!(
            "year_month" => &["2026-01", "2026-02"],
            "period" => &["2026-01", "2026-02"],
            "period_start" => &["2026-01-01", "2026-02-01"],
            "period_end" => &["2026-01-31", "2026-02-28"],
            "period_type" => &["month", "month"],
            "period_months" => &[1_u32, 1],
            "user_type" => &["registered", "registered"],
            "gini" => &[0.2_f64, 0.3],
            "theil" => &[0.1_f64, 0.2],
            "palma" => &[1.2_f64, 1.3],
            "min_editors_50pct" => &[2_u32, 3],
            "total_editors" => &[10_u32, 11],
            "total_edits" => &[100_u32, 110],
            "wiki" => &["afwiki", "afwiki"],
        )
        .expect("valid current inequality receipt fixture");
        ParquetWriter::new(File::create(path).expect("create current inequality fixture"))
            .finish(&mut frame)
            .expect("write current inequality fixture");
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
    #[rustfmt::skip]
    fn publication_blocks_impossible_numeric_values() -> Result<()> {
        let mut accumulator = SemanticAccumulator::new(SemanticSpec {
            date_column: Some("year_month".to_string()),
            conservation_columns: Vec::new(),
            ordering_contract: "wiki-major/v1".to_string(),
            page_week_consistency: false,
            metric: Some(MetricId::Gdp),
            enforce_invariants: true,
        });
        let frame = df!("year_month" => &["2026-01"], "total_edits" => &[-1_i64], "wiki" => &["nlwiki"])?;
        let error = accumulator
            .observe(&frame)
            .expect_err("negative edit counts must block publication");
        assert!(error.to_string().contains("total_edits=-1 is negative"));
        Ok(())
    }

    fn invariant_error(frame: DataFrame, metric: MetricId, needle: &str) {
        let error = validate_metric_invariants(&frame, metric, 4)
            .expect_err("fixture should violate a publication invariant");
        assert!(
            error.to_string().contains(needle),
            "expected {needle:?} in {error:#}"
        );
    }

    #[test]
    #[rustfmt::skip]
    fn publication_helpers_cover_numeric_types_and_robust_outliers() -> Result<()> {
        let mut gdp_accumulator = SemanticAccumulator::new(SemanticSpec {
            date_column: Some("year_month".to_string()),
            conservation_columns: Vec::new(),
            ordering_contract: "writer-order/v1".to_string(),
            page_week_consistency: false,
            metric: Some(MetricId::Gdp),
            enforce_invariants: true,
        });
        let gdp_rows = df!("year_month" => &["2026-01", "2026-02", "2026-03", "2026-04", "2026-05", "2026-06", "2026-07"], "total_edits" => &[1_i64, 1, 1, 1, 1, 1, 1])?;
        gdp_accumulator.observe(&gdp_rows)?;
        let mut cohort_accumulator = SemanticAccumulator::new(SemanticSpec {
            date_column: Some("year".to_string()),
            conservation_columns: Vec::new(),
            ordering_contract: "writer-order/v1".to_string(),
            page_week_consistency: false,
            metric: Some(MetricId::LaborCohorts),
            enforce_invariants: true,
        });
        let cohort_rows = df!("cohort_year" => &["2020"], "year" => &["2020"], "survived_editors" => &[1_i64])?;
        cohort_accumulator.observe(&cohort_rows)?;

        let numeric_columns = vec![
            Column::new("u32".into(), &[1_u32][..]),
            Column::new("u64".into(), &[1_u64][..]),
            Column::new("i32".into(), &[1_i32][..]),
            Column::new("i64".into(), &[1_i64][..]),
            Column::new("f32".into(), &[1_f32][..]),
            Column::new("f64".into(), &[1_f64][..]),
            Column::new("null".into(), &[None::<u32>][..]),
        ];
        for column in numeric_columns {
            let name = column.name().to_string();
            let frame = DataFrame::new(1, vec![column])?;
            let value = numeric_value(&frame, &name, 0)?;
            if name == "null" {
                assert_eq!(value, None);
            } else {
                assert_eq!(value, Some(1.0));
            }
        }
        let text = df!("text" => &["not numeric"])?;
        assert!(numeric_value(&text, "text", 0).is_err());

        for name in [
            "gross_bytes_added",
            "gross_bytes",
            "total_edits",
            "productive_edits",
            "reverted_edits",
            "minor_edits",
            "unique_editors",
            "editors",
            "active_editors",
            "arrivals",
            "departures",
            "survived_editors",
            "initial_editors",
            "cohort_size",
            "reached_5",
            "reached_25",
            "reached_100",
            "total_patrols",
            "unique_patrollers",
            "patrol_new_pages",
            "patrol_diffs",
            "patrolled_revisions",
            "autopatrolled_revisions",
            "total_revisions",
            "min_patrollers_50pct",
            "edits",
            "previous_week_edits",
            "revision_count",
        ] {
            assert!(non_negative_metric_column(name), "{name} should be a count");
        }
        for name in ["bytes_per_edit", "net_bytes", "wow_change", "other"] {
            assert!(!non_negative_metric_column(name));
        }
        for name in ["gini", "revert_rate", "arrival_rate", "departure_rate"] {
            assert_eq!(metric_bounds(name), Some((0.0, 1.0)));
        }
        for name in ["patrol_coverage_pct", "adjusted_coverage_pct", "top1_pct"] {
            assert_eq!(metric_bounds(name), Some((0.0, 100.0)));
        }
        assert_eq!(metric_bounds("theil"), Some((0.0, f64::INFINITY)));
        assert_eq!(metric_bounds("palma"), Some((0.0, f64::INFINITY)));
        assert_eq!(metric_bounds("unknown"), None);
        assert_eq!(median(&mut []), None);
        assert_eq!(median(&mut [3.0, 1.0, 2.0]), Some(2.0));
        assert_eq!(median(&mut [4.0, 1.0, 3.0, 2.0]), Some(2.5));
        assert!(close_enough(1.0, 1.0 + 1e-12));
        assert!(close_enough_rounded(33.3, 100.0 / 3.0, 10.0));

        let mut columns = vec![
            Column::new(
                "mad_series".into(),
                [
                    1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 100.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0,
                ],
            ),
            Column::new(
                "iqr_high".into(),
                [
                    1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 100.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0,
                ],
            ),
            Column::new(
                "iqr_low".into(),
                [
                    1.0, 1.0, 1.0, 1.0, 1.0, 1.0, -100.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0,
                ],
            ),
            Column::new(
                "flat".into(),
                [
                    1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 100.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0,
                ],
            ),
            Column::new(
                "nullable".into(),
                [
                    Some(1.0),
                    None,
                    Some(1.0),
                    Some(1.0),
                    Some(1.0),
                    Some(1.0),
                    Some(1.0),
                    Some(1.0),
                    Some(1.0),
                    Some(1.0),
                    Some(1.0),
                    Some(1.0),
                    Some(1.0),
                ],
            ),
            Column::new(
                "nan_values".into(),
                [
                    1.0,
                    1.0,
                    1.0,
                    1.0,
                    1.0,
                    1.0,
                    f64::NAN,
                    1.0,
                    1.0,
                    1.0,
                    1.0,
                    1.0,
                    1.0,
                ],
            ),
            Column::new("year".into(), [0_i32; 13]),
            Column::new("page_namespace".into(), [0_i32; 13]),
            Column::new("text".into(), ["ignored"; 13]),
        ];
        for index in 0..21 {
            let mut values = [1.0_f64; 13];
            values[6] = 100.0 + index as f64;
            columns.push(Column::new(format!("flat_{index}").into(), values));
        }
        let outlier_frame = DataFrame::new(13, columns)?;
        warn_metric_outliers(&outlier_frame, MetricId::Gdp, 9)?;
        let short = df!("value" => &[1.0_f64, 2.0])?;
        warn_metric_outliers(&short, MetricId::Gdp, 0)?;
        Ok(())
    }

    #[test]
    #[rustfmt::skip]
    fn cohort_monotonicity_validates_order_and_nulls() -> Result<()> {
        let valid = df!("cohort_year" => &["2020", "2020", "2020"], "year" => &["2020", "2022", "2021"], "survived_editors" => &[10_u32, 8, 9], "wiki" => &["enwiki", "enwiki", "enwiki"])?;
        let mut accumulator =
            SemanticAccumulator::new(SemanticSpec::for_identity("cohorts.parquet"));
        accumulator.observe_cohort_monotonicity(&valid, 7)?;

        let without_wiki = df!("cohort_year" => &["2020"], "year" => &["2020"], "survived_editors" => &[1_u32])?;
        accumulator.observe_cohort_monotonicity(&without_wiki, 0)?;

        let increasing = df!("cohort_year" => &["2020", "2020"], "year" => &["2020", "2021"], "survived_editors" => &[1_u32, 2])?;
        assert!(
            accumulator
                .observe_cohort_monotonicity(&increasing, 0)
                .is_err()
        );
        let out_of_order = df!("cohort_year" => &["2021", "2021"], "year" => &["2022", "2020"], "survived_editors" => &[8_u32, 7])?;
        let mut another = SemanticAccumulator::new(SemanticSpec::for_identity("cohorts.parquet"));
        assert!(
            another
                .observe_cohort_monotonicity(&out_of_order, 0)
                .is_err()
        );

        for frame in [
            df!("cohort_year" => &[None::<&str>], "year" => &["2020"], "survived_editors" => &[1_u32])?,
            df!("cohort_year" => &["2020"], "year" => &[None::<&str>], "survived_editors" => &[1_u32])?,
            df!("cohort_year" => &["2020"], "year" => &["2020"], "survived_editors" => &[None::<u32>])?,
        ] {
            let mut accumulator =
                SemanticAccumulator::new(SemanticSpec::for_identity("cohorts.parquet"));
            assert!(accumulator.observe_cohort_monotonicity(&frame, 0).is_err());
        }
        Ok(())
    }

    #[test]
    #[rustfmt::skip]
    fn metric_invariants_cover_valid_and_failure_contracts() -> Result<()> {
        let gdp = df!("productive_edits" => &[6_i64], "reverted_edits" => &[4_i64], "total_edits" => &[10_i64], "revert_rate" => &[0.4_f64], "net_bytes" => &[100_i64], "bytes_per_edit" => &[10.0_f64], "unique_editors" => &[5_i64], "bytes_per_editor" => &[20.0_f64], "label" => &["registered"], "missing_value" => &[None::<f64>])?;
        validate_metric_invariants(&gdp, MetricId::Gdp, 0)?;
        let churn = df!("arrivals" => &[2_i64], "departures" => &[3_i64], "active_editors" => &[10_i64], "arrival_rate" => &[0.2_f64], "departure_rate" => &[0.3_f64])?;
        validate_metric_invariants(&churn, MetricId::LaborChurn, 0)?;
        let patrol = df!("total_patrols" => &[12_i64], "unique_patrollers" => &[4_i64], "patrol_new_pages" => &[2_i64], "patrol_diffs" => &[10_i64], "patrolled_revisions" => &[80_i64], "autopatrolled_revisions" => &[10_i64], "total_revisions" => &[100_i64], "min_patrollers_50pct" => &[2_i64], "median_latency_hours" => &[1.5_f64], "p90_latency_hours" => &[4.0_f64], "top1_pct" => &[25.0_f64], "patrol_coverage_pct" => &[80.0_f64], "adjusted_coverage_pct" => &[90.0_f64])?;
        validate_metric_invariants(&patrol, MetricId::Patrol, 0)?;
        let inequality = df!("gini" => &[0.4_f64], "theil" => &[0.2], "palma" => &[1.4])?;
        validate_metric_invariants(&inequality, MetricId::Inequality, 0)?;
        let funnel = df!("cohort_size" => &[100_i64], "reached_5" => &[80_i64], "reached_25" => &[50_i64], "reached_100" => &[20_i64])?;
        validate_metric_invariants(&funnel, MetricId::BusinessFunnel, 0)?;
        let cohorts = df!("survived_editors" => &[5_i64], "initial_editors" => &[10_i64])?;
        validate_metric_invariants(&cohorts, MetricId::LaborCohorts, 0)?;
        let weekly = df!("edits" => &[10_i64], "previous_week_edits" => &[5_i64], "wow_change" => &[5_i64], "wow_rate" => &[1.0_f64])?;
        validate_metric_invariants(&weekly, MetricId::PageWeeklyEdits, 0)?;
        let noop = df!("label" => &["one"])?;
        for metric in [
            MetricId::GdpActivityTiers,
            MetricId::GdpUserTypeShare,
            MetricId::LaborMonthly,
        ] {
            validate_metric_invariants(&noop, metric, 0)?;
        }

        invariant_error(
            df!("productive_edits" => &[1_i64], "reverted_edits" => &[1_i64], "total_edits" => &[3_i64])?,
            MetricId::Gdp,
            "productive_edits + reverted_edits",
        );
        invariant_error(
            df!("reverted_edits" => &[2_i64], "total_edits" => &[10_i64], "revert_rate" => &[0.3_f64])?,
            MetricId::Gdp,
            "revert_rate=",
        );
        invariant_error(
            df!("reverted_edits" => &[0_i64], "total_edits" => &[0_i64], "revert_rate" => &[0.0_f64])?,
            MetricId::Gdp,
            "must be null when total_edits is zero",
        );
        invariant_error(
            df!("reverted_edits" => &[1_i64], "total_edits" => &[10_i64], "revert_rate" => &[None::<f64>])?,
            MetricId::Gdp,
            "revert_rate is null",
        );
        invariant_error(
            df!("net_bytes" => &[100_i64], "total_edits" => &[10_i64], "bytes_per_edit" => &[9.0_f64])?,
            MetricId::Gdp,
            "bytes_per_edit does not equal",
        );
        invariant_error(
            df!("net_bytes" => &[0_i64], "total_edits" => &[0_i64], "bytes_per_edit" => &[0.0_f64])?,
            MetricId::Gdp,
            "bytes_per_edit must be null",
        );
        invariant_error(
            df!("net_bytes" => &[100_i64], "total_edits" => &[10_i64], "bytes_per_edit" => &[None::<f64>])?,
            MetricId::Gdp,
            "bytes_per_edit is null",
        );
        invariant_error(
            df!("net_bytes" => &[100_i64], "unique_editors" => &[5_i64], "bytes_per_editor" => &[19.0_f64])?,
            MetricId::Gdp,
            "bytes_per_editor does not equal",
        );
        invariant_error(
            df!("net_bytes" => &[0_i64], "unique_editors" => &[0_i64], "bytes_per_editor" => &[0.0_f64])?,
            MetricId::Gdp,
            "bytes_per_editor must be null",
        );
        invariant_error(
            df!("net_bytes" => &[100_i64], "unique_editors" => &[5_i64], "bytes_per_editor" => &[None::<f64>])?,
            MetricId::Gdp,
            "bytes_per_editor is null",
        );
        invariant_error(df!("gini" => &[2.0_f64])?, MetricId::Gdp, "outside [0");
        invariant_error(df!("value" => &[f64::NAN])?, MetricId::Gdp, "non-finite");

        invariant_error(
            df!("arrivals" => &[2_i64], "active_editors" => &[10_i64], "arrival_rate" => &[0.3_f64], "departures" => &[3_i64], "departure_rate" => &[0.3_f64])?,
            MetricId::LaborChurn,
            "arrival_rate does not equal",
        );
        invariant_error(
            df!("arrivals" => &[2_i64], "active_editors" => &[10_i64], "arrival_rate" => &[None::<f64>])?,
            MetricId::LaborChurn,
            "arrival_rate is null",
        );
        invariant_error(
            df!("arrivals" => &[2_i64], "active_editors" => &[0_i64], "arrival_rate" => &[0.0_f64])?,
            MetricId::LaborChurn,
            "arrival_rate must be null",
        );
        invariant_error(
            df!("departures" => &[3_i64], "active_editors" => &[10_i64], "departure_rate" => &[0.2_f64])?,
            MetricId::LaborChurn,
            "departure_rate does not equal",
        );
        invariant_error(
            df!("departures" => &[3_i64], "active_editors" => &[10_i64], "departure_rate" => &[None::<f64>])?,
            MetricId::LaborChurn,
            "departure_rate is null",
        );
        invariant_error(
            df!("departures" => &[3_i64], "active_editors" => &[0_i64], "departure_rate" => &[0.0_f64])?,
            MetricId::LaborChurn,
            "departure_rate must be null",
        );

        invariant_error(
            df!("median_latency_hours" => &[-1.0_f64])?,
            MetricId::Patrol,
            "median_latency_hours=-1",
        );
        invariant_error(
            df!("patrolled_revisions" => &[101_i64], "total_revisions" => &[100_i64])?,
            MetricId::Patrol,
            "exceeds total_revisions",
        );
        invariant_error(
            df!("patrolled_revisions" => &[80_i64], "total_revisions" => &[100_i64], "patrol_coverage_pct" => &[79.0_f64])?,
            MetricId::Patrol,
            "published numerator",
        );
        invariant_error(
            df!("patrolled_revisions" => &[80_i64], "total_revisions" => &[100_i64], "patrol_coverage_pct" => &[None::<f64>])?,
            MetricId::Patrol,
            "patrol_coverage_pct is null",
        );
        invariant_error(
            df!("patrolled_revisions" => &[80_i64], "autopatrolled_revisions" => &[10_i64], "total_revisions" => &[100_i64], "adjusted_coverage_pct" => &[89.0_f64])?,
            MetricId::Patrol,
            "published numerator",
        );
        let missing_patrol_dimensions =
            df!("patrol_coverage_pct" => &[0.0_f64], "adjusted_coverage_pct" => &[0.0_f64])?;
        validate_metric_invariants(&missing_patrol_dimensions, MetricId::Patrol, 0)?;

        invariant_error(
            df!("cohort_size" => &[10_i64], "reached_5" => &[11_i64], "reached_25" => &[5_i64], "reached_100" => &[2_i64])?,
            MetricId::BusinessFunnel,
            "not monotonic",
        );
        invariant_error(
            df!("survived_editors" => &[11_i64], "initial_editors" => &[10_i64])?,
            MetricId::LaborCohorts,
            "exceeds initial_editors",
        );
        invariant_error(
            df!("edits" => &[10_i64], "previous_week_edits" => &[5_i64], "wow_change" => &[4_i64])?,
            MetricId::PageWeeklyEdits,
            "does not conserve",
        );
        invariant_error(
            df!("edits" => &[10_i64], "previous_week_edits" => &[5_i64], "wow_change" => &[5_i64], "wow_rate" => &[0.5_f64])?,
            MetricId::PageWeeklyEdits,
            "does not equal",
        );
        invariant_error(
            df!("edits" => &[0_i64], "previous_week_edits" => &[0_i64], "wow_change" => &[0_i64], "wow_rate" => &[0.0_f64])?,
            MetricId::PageWeeklyEdits,
            "wow_rate must be null",
        );
        Ok(())
    }

    #[test]
    fn versioned_legacy_inequality_schema_is_scrubbed_with_its_recorded_contract() -> Result<()> {
        let directory = TestDir::new()?;
        let artifact = directory.path().join("inequality.parquet");
        write_legacy_inequality(&artifact)?;

        let receipt = scan(
            &artifact,
            "output/afwiki/inequality.parquet",
            "monthly-stateless-v2-total-order",
            "legacy-input",
        )
        .expect("legacy inequality schema should scrub");
        assert_eq!(receipt.minimum_date.as_deref(), Some("2026-01"));
        assert_eq!(receipt.maximum_date.as_deref(), Some("2026-02"));
        assert_eq!(receipt.conservation_totals.get("total_edits"), Some(&210));
        assert_eq!(receipt.parquet_schema[0].name, "year_month");

        let merged_receipt = scan(
            &artifact,
            "merged/inequality.parquet",
            MERGED_INEQUALITY_ALGORITHM_VERSION,
            "legacy-input",
        )
        .expect("legacy merged inequality schema should scrub");
        assert_eq!(merged_receipt.minimum_date.as_deref(), Some("2026-01"));
        assert_eq!(merged_receipt.maximum_date.as_deref(), Some("2026-02"));
        assert_eq!(
            merged_receipt.conservation_totals.get("total_edits"),
            Some(&210)
        );

        let current = directory.path().join("current-inequality.parquet");
        write_current_inequality(&current)?;
        let current_merged_receipt = scan(
            &current,
            "merged/inequality.parquet",
            MERGED_INEQUALITY_ALGORITHM_VERSION,
            "current-input",
        )
        .expect("current merged inequality schema should remain strict");
        assert_eq!(
            current_merged_receipt.minimum_date.as_deref(),
            Some("2026-01-01")
        );
        assert!(current_merged_receipt.conservation_totals.is_empty());

        assert!(
            scan(
                &artifact,
                "output/afwiki/inequality.parquet",
                "monthly-stateless-v5-exact-period-inequality",
                "current-input",
            )
            .is_err()
        );
        Ok(())
    }

    #[test]
    fn scan_failures_keep_artifact_and_stage_context() -> Result<()> {
        let directory = TestDir::new()?;
        let gdp = directory.path().join("gdp.parquet");
        write_gdp(&gdp, "nlwiki")?;
        let schema_error = scan_and_write_with_spec(
            &gdp,
            "nlwiki/gdp.parquet",
            "gdp-v1",
            "inputs",
            SemanticSpec {
                date_column: Some("period_start".to_string()),
                conservation_columns: Vec::new(),
                ordering_contract: "writer-order/v1".to_string(),
                page_week_consistency: false,
                metric: None,
                enforce_invariants: false,
            },
        )
        .expect_err("a missing semantic date column must identify its artifact and stage");
        let schema_message = format!("{schema_error:#}");
        assert!(
            schema_message.contains("artifact=")
                && schema_message.contains("stage=validate_schema")
        );

        let weekly = directory.path().join("page_weekly_edits.parquet");
        write_weekly(&weekly, &[1, 2], &["2026-01-05", "2026-01-12"])?;
        let batch_error = scan_and_write_with_spec(
            &weekly,
            "nlwiki/page_weekly_edits.parquet",
            "weekly-v1",
            "inputs",
            SemanticSpec::for_identity("nlwiki/page_weekly_edits.parquet"),
        )
        .expect_err("an invalid page-week batch must identify its artifact and stage");
        let batch_message = format!("{batch_error:#}");
        assert!(
            batch_message.contains("artifact=") && batch_message.contains("stage=semantic_batch")
        );

        let mut receipt = scan(&gdp, "nlwiki/gdp.parquet", "gdp-v1", "inputs")?;
        receipt.rows += 1;
        let row_error = ensure_receipt_row_count(&gdp, &receipt, 2)
            .expect_err("a footer mismatch must include receipt identity and artifact path");
        let row_message = format!("{row_error:#}");
        assert!(row_message.contains("stage=verify_footer"));
        assert!(row_message.contains("nlwiki/gdp.parquet"));
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
