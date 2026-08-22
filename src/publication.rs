use anyhow::{Context, Result, ensure};
use polars::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::info;

use crate::storage;

const RUN_CONTEXT_FILE: &str = ".publication-run.json";
const CANDIDATE_FILE: &str = ".publication-candidate.json";
pub const RECEIPT_FILE: &str = "publication-gate.json";
const JSON_ARTIFACTS: [&str; 12] = [
    "defaults_business.json",
    "defaults_edit_variation.json",
    "defaults_gdp.json",
    "defaults_inequality.json",
    "defaults_labor.json",
    "defaults_patrol.json",
    "meta_business.json",
    "meta_gdp.json",
    "meta_inequality.json",
    "meta_labor.json",
    "meta_patrol.json",
    "manifest.json",
];

#[derive(Clone, Copy)]
enum Kind {
    String,
    I32,
    I64,
    U32,
    F64,
}

struct MetricSpec {
    name: &'static str,
    date_column: Option<&'static str>,
    conservation_column: Option<&'static str>,
    schema: &'static [(&'static str, Kind)],
}

const BUSINESS_SCHEMA: &[(&str, Kind)] = &[
    ("cohort_year", Kind::String),
    ("cohort_size", Kind::U32),
    ("reached_5", Kind::U32),
    ("reached_25", Kind::U32),
    ("reached_100", Kind::U32),
    ("wiki", Kind::String),
];
const GDP_SCHEMA: &[(&str, Kind)] = &[
    ("year_month", Kind::String),
    ("page_namespace", Kind::I32),
    ("user_type", Kind::String),
    ("gross_bytes_added", Kind::I64),
    ("net_bytes", Kind::I64),
    ("total_edits", Kind::U32),
    ("productive_edits", Kind::U32),
    ("reverted_edits", Kind::U32),
    ("unique_editors", Kind::U32),
    ("minor_edits", Kind::U32),
    ("bytes_per_edit", Kind::F64),
    ("bytes_per_editor", Kind::F64),
    ("revert_rate", Kind::F64),
    ("wiki", Kind::String),
];
const GDP_TIERS_SCHEMA: &[(&str, Kind)] = &[
    ("year_month", Kind::String),
    ("user_type", Kind::String),
    ("activity_tier", Kind::String),
    ("editors", Kind::U32),
    ("total_edits", Kind::U32),
    ("net_bytes", Kind::I64),
    ("gross_bytes", Kind::I64),
    ("wiki", Kind::String),
];
const GDP_SHARE_SCHEMA: &[(&str, Kind)] = &[
    ("year_month", Kind::String),
    ("user_type", Kind::String),
    ("edits", Kind::U32),
    ("net_bytes", Kind::I64),
    ("editors", Kind::U32),
    ("wiki", Kind::String),
];
const INEQUALITY_SCHEMA: &[(&str, Kind)] = &[
    ("year_month", Kind::String),
    ("user_type", Kind::String),
    ("gini", Kind::F64),
    ("theil", Kind::F64),
    ("palma", Kind::F64),
    ("min_editors_50pct", Kind::U32),
    ("total_editors", Kind::U32),
    ("total_edits", Kind::U32),
    ("wiki", Kind::String),
];
const CHURN_SCHEMA: &[(&str, Kind)] = &[
    ("period", Kind::String),
    ("active_editors", Kind::U32),
    ("arrivals", Kind::U32),
    ("departures", Kind::U32),
    ("period_type", Kind::String),
    ("arrival_rate", Kind::F64),
    ("departure_rate", Kind::F64),
    ("wiki", Kind::String),
];
const COHORTS_SCHEMA: &[(&str, Kind)] = &[
    ("cohort_year", Kind::String),
    ("year", Kind::String),
    ("survived_editors", Kind::U32),
    ("initial_editors", Kind::U32),
    ("wiki", Kind::String),
];
const LABOR_SCHEMA: &[(&str, Kind)] = &[
    ("year_month", Kind::String),
    ("page_namespace", Kind::I32),
    ("user_type", Kind::String),
    ("unique_editors", Kind::U32),
    ("total_edits", Kind::U32),
    ("net_bytes", Kind::I64),
    ("reverted_edits", Kind::U32),
    ("wiki", Kind::String),
];
const WEEKLY_SCHEMA: &[(&str, Kind)] = &[
    ("week_start", Kind::String),
    ("iso_year", Kind::I32),
    ("iso_week", Kind::I32),
    ("page_id", Kind::I64),
    ("page_title", Kind::String),
    ("page_namespace", Kind::I32),
    ("edits", Kind::U32),
    ("previous_week_edits", Kind::U32),
    ("wow_change", Kind::I64),
    ("wow_rate", Kind::F64),
    ("wiki", Kind::String),
];
const PATROL_SCHEMA: &[(&str, Kind)] = &[
    ("year_month", Kind::String),
    ("wiki", Kind::String),
    ("page_namespace", Kind::I32),
    ("user_type", Kind::String),
    ("total_patrols", Kind::I64),
    ("unique_patrollers", Kind::I32),
    ("patrol_new_pages", Kind::I64),
    ("patrol_diffs", Kind::I64),
    ("median_latency_hours", Kind::F64),
    ("p90_latency_hours", Kind::F64),
    ("patrolled_revisions", Kind::I64),
    ("autopatrolled_revisions", Kind::I64),
    ("total_revisions", Kind::I64),
    ("patrol_coverage_pct", Kind::F64),
    ("adjusted_coverage_pct", Kind::F64),
    ("top1_pct", Kind::F64),
    ("min_patrollers_50pct", Kind::I32),
];

const METRICS: [MetricSpec; 10] = [
    MetricSpec {
        name: "business_funnel",
        date_column: None,
        conservation_column: None,
        schema: BUSINESS_SCHEMA,
    },
    MetricSpec {
        name: "gdp",
        date_column: Some("year_month"),
        conservation_column: Some("total_edits"),
        schema: GDP_SCHEMA,
    },
    MetricSpec {
        name: "gdp_activity_tiers",
        date_column: Some("year_month"),
        conservation_column: Some("total_edits"),
        schema: GDP_TIERS_SCHEMA,
    },
    MetricSpec {
        name: "gdp_user_type_share",
        date_column: Some("year_month"),
        conservation_column: Some("edits"),
        schema: GDP_SHARE_SCHEMA,
    },
    MetricSpec {
        name: "inequality",
        date_column: Some("year_month"),
        conservation_column: Some("total_edits"),
        schema: INEQUALITY_SCHEMA,
    },
    MetricSpec {
        name: "labor_churn",
        date_column: Some("period"),
        conservation_column: None,
        schema: CHURN_SCHEMA,
    },
    MetricSpec {
        name: "labor_cohorts",
        date_column: None,
        conservation_column: None,
        schema: COHORTS_SCHEMA,
    },
    MetricSpec {
        name: "labor_monthly",
        date_column: Some("year_month"),
        conservation_column: Some("total_edits"),
        schema: LABOR_SCHEMA,
    },
    MetricSpec {
        name: "page_weekly_edits",
        date_column: Some("week_start"),
        conservation_column: Some("edits"),
        schema: WEEKLY_SCHEMA,
    },
    MetricSpec {
        name: "patrol",
        date_column: Some("year_month"),
        conservation_column: Some("total_patrols"),
        schema: PATROL_SCHEMA,
    },
];

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq)]
struct ArtifactRecord {
    name: String,
    bytes: u64,
    modified_secs: u64,
    modified_nanos: u32,
}

#[derive(Serialize, Deserialize)]
struct RunContext {
    schema_version: u8,
    run_id: String,
    started_at_unix: u64,
    refresh_wikis: BTreeSet<String>,
    requested_snapshot_version: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct Candidate {
    schema_version: u8,
    run_id: String,
    artifacts: Vec<ArtifactRecord>,
}

#[derive(Serialize, Deserialize)]
struct LifecycleRegistry {
    schema_version: u8,
    publication_contract: PublicationContract,
    wikis: BTreeMap<String, LifecycleWiki>,
}

#[derive(Serialize, Deserialize)]
struct PublicationContract {
    datasets: BTreeMap<String, DatasetContract>,
}

#[derive(Serialize, Deserialize)]
struct DatasetContract {
    coverage: Option<String>,
    wikis: Option<BTreeSet<String>>,
    minimum_rows_per_wiki: u64,
}

#[derive(Serialize, Deserialize)]
struct LifecycleWiki {
    publication: String,
    refresh: String,
    imported_cutoff: Option<String>,
    freshness_sla_days: Option<u64>,
}

#[derive(Serialize, Deserialize)]
struct WikiMetricReport {
    rows: u64,
    minimum_date: Option<String>,
    maximum_date: Option<String>,
    conservation_total: Option<i64>,
}

#[derive(Serialize, Deserialize)]
struct MetricReport {
    rows: u64,
    conservation_total: Option<i64>,
    wikis: BTreeMap<String, WikiMetricReport>,
}

#[derive(Serialize, Deserialize)]
struct PatrolSourceReport {
    patrol_events: u64,
    rights_events: u64,
}

#[derive(Serialize, Deserialize)]
struct GateReceipt {
    schema_version: u8,
    run_id: String,
    validated_at_unix: u64,
    selected_snapshot_versions: BTreeMap<String, String>,
    cutoff_dates: BTreeMap<String, String>,
    metrics: BTreeMap<String, MetricReport>,
    patrol_sources: BTreeMap<String, PatrolSourceReport>,
    artifacts: Vec<ArtifactRecord>,
}

struct FileSummary {
    wiki_min: String,
    wiki_max: String,
    minimum_date: Option<String>,
    maximum_date: Option<String>,
    conservation_total: Option<i64>,
}

fn now_unix() -> Result<u64> {
    Ok(SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs())
}

fn atomic_json<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    let parent = path
        .parent()
        .context("publication JSON path has no parent")?;
    fs::create_dir_all(parent)?;
    let file_name = path
        .file_name()
        .context("publication JSON path has no filename")?;
    let temp = parent.join(format!(
        ".{}.{}.tmp",
        file_name.to_string_lossy(),
        std::process::id()
    ));
    let result = (|| -> Result<()> {
        let mut file = File::create(&temp)?;
        serde_json::to_writer_pretty(&mut file, value)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        fs::rename(&temp, path)?;
        File::open(parent)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temp);
    }
    result
}

fn read_json<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T> {
    serde_json::from_slice(
        &fs::read(path).with_context(|| format!("failed to read {}", path.display()))?,
    )
    .with_context(|| format!("failed to parse {}", path.display()))
}

fn artifact_record(path: &Path) -> Result<ArtifactRecord> {
    let metadata = fs::metadata(path)
        .with_context(|| format!("missing publication artifact {}", path.display()))?;
    ensure!(
        metadata.is_file(),
        "publication artifact is not a file: {}",
        path.display()
    );
    let modified = metadata.modified()?.duration_since(UNIX_EPOCH)?;
    Ok(ArtifactRecord {
        name: path
            .file_name()
            .context("artifact path has no filename")?
            .to_string_lossy()
            .into_owned(),
        bytes: metadata.len(),
        modified_secs: modified.as_secs(),
        modified_nanos: modified.subsec_nanos(),
    })
}

pub fn begin_run(
    output_dir: &Path,
    run_id: Option<&str>,
    refresh_wikis: &[String],
    requested_snapshot_version: Option<&str>,
) -> Result<()> {
    let Some(run_id) = run_id else {
        return Ok(());
    };
    ensure!(
        !run_id.trim().is_empty(),
        "publication run ID cannot be empty"
    );
    let context = RunContext {
        schema_version: 1,
        run_id: run_id.to_string(),
        started_at_unix: now_unix()?,
        refresh_wikis: refresh_wikis.iter().cloned().collect(),
        requested_snapshot_version: requested_snapshot_version.map(str::to_string),
    };
    atomic_json(&output_dir.join(RUN_CONTEXT_FILE), &context)
}

pub fn record_candidate(output_dir: &Path, run_id: Option<&str>, names: &[String]) -> Result<()> {
    let Some(run_id) = run_id else {
        return Ok(());
    };
    let context: RunContext = read_json(&output_dir.join(RUN_CONTEXT_FILE))?;
    ensure!(
        context.run_id == run_id,
        "publication run context does not match run ID {run_id}"
    );
    let unique: BTreeSet<_> = names.iter().cloned().collect();
    ensure!(
        unique.len() == names.len(),
        "publication candidate contains duplicate artifacts"
    );
    let artifacts = unique
        .iter()
        .map(|name| artifact_record(&output_dir.join(name)))
        .collect::<Result<Vec<_>>>()?;
    atomic_json(
        &output_dir.join(CANDIDATE_FILE),
        &Candidate {
            schema_version: 1,
            run_id: run_id.to_string(),
            artifacts,
        },
    )
}

fn load_lifecycle(path: &Path) -> Result<LifecycleRegistry> {
    let registry: LifecycleRegistry = read_json(path)?;
    ensure!(
        registry.schema_version == 1,
        "unsupported lifecycle schema version"
    );
    let expected: BTreeSet<_> = METRICS
        .iter()
        .map(|metric| metric.name.to_string())
        .collect();
    let actual: BTreeSet<_> = registry
        .publication_contract
        .datasets
        .keys()
        .cloned()
        .collect();
    ensure!(
        actual == expected,
        "publication dataset contracts do not match Rust metric contracts"
    );
    Ok(registry)
}

fn expected_wikis(
    registry: &LifecycleRegistry,
    contract: &DatasetContract,
) -> Result<BTreeSet<String>> {
    let published: BTreeSet<_> = registry
        .wikis
        .iter()
        .filter(|(_, entry)| entry.publication == "published")
        .map(|(wiki, _)| wiki.clone())
        .collect();
    match (contract.coverage.as_deref(), contract.wikis.as_ref()) {
        (Some("all_published"), None) => Ok(published),
        (None, Some(wikis)) if !wikis.is_empty() && wikis.is_subset(&published) => {
            Ok(wikis.clone())
        }
        _ => anyhow::bail!("invalid publication dataset coverage contract"),
    }
}

fn kind_matches(kind: Kind, dtype: &DataType) -> bool {
    matches!(
        (kind, dtype),
        (Kind::String, DataType::String)
            | (Kind::I32, DataType::Int32)
            | (Kind::I64, DataType::Int64)
            | (Kind::U32, DataType::UInt32)
            | (Kind::F64, DataType::Float64)
    )
}

fn validate_schema(path: &Path, spec: &MetricSpec) -> Result<u64> {
    let mut reader = ParquetReader::new(File::open(path)?);
    let rows = reader.num_rows()? as u64;
    let frame = ParquetReader::new(File::open(path)?)
        .with_slice(Some((0, 0)))
        .finish()?;
    ensure!(
        frame.width() == spec.schema.len(),
        "{} has {} columns; expected {}",
        path.display(),
        frame.width(),
        spec.schema.len()
    );
    for (name, kind) in spec.schema {
        let column = anyhow::Context::with_context(frame.column(name), || {
            format!("{} is missing required column {name}", path.display())
        })?;
        ensure!(
            kind_matches(*kind, column.dtype()),
            "{} column {name} has type {:?}",
            path.display(),
            column.dtype()
        );
    }
    Ok(rows)
}

fn string_cell(frame: &DataFrame, name: &str) -> Result<String> {
    frame
        .column(name)?
        .str()?
        .get(0)
        .map(str::to_string)
        .with_context(|| format!("aggregate {name} is null"))
}

fn summarize(path: &Path, spec: &MetricSpec) -> Result<FileSummary> {
    let path_text = path.to_string_lossy().to_string();
    let mut expressions = vec![
        col("wiki").min().alias("wiki_min"),
        col("wiki").max().alias("wiki_max"),
    ];
    if let Some(date) = spec.date_column {
        expressions.push(col(date).min().alias("minimum_date"));
        expressions.push(col(date).max().alias("maximum_date"));
    }
    if let Some(total) = spec.conservation_column {
        expressions.push(
            col(total)
                .cast(DataType::Int64)
                .sum()
                .alias("conservation_total"),
        );
    }
    let frame = LazyFrame::scan_parquet(path_text.as_str().into(), Default::default())?
        .select(expressions)
        .collect()?;
    let minimum_date = spec
        .date_column
        .map(|_| string_cell(&frame, "minimum_date"))
        .transpose()?;
    let maximum_date = spec
        .date_column
        .map(|_| string_cell(&frame, "maximum_date"))
        .transpose()?;
    let conservation_total = spec
        .conservation_column
        .map(|_| {
            frame
                .column("conservation_total")?
                .i64()?
                .get(0)
                .context("conservation total is null")
        })
        .transpose()?;
    Ok(FileSummary {
        wiki_min: string_cell(&frame, "wiki_min")?,
        wiki_max: string_cell(&frame, "wiki_max")?,
        minimum_date,
        maximum_date,
        conservation_total,
    })
}

fn validate_date(value: &str, column: &str) -> Result<()> {
    let valid = if column == "week_start" {
        chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d").is_ok()
    } else if column == "period" {
        let bytes = value.as_bytes();
        let valid_year =
            |year: &[u8]| year.len() == 4 && year.iter().all(u8::is_ascii_digit) && year != b"0000";
        valid_year(bytes)
            || (bytes.len() == 7
                && valid_year(&bytes[..4])
                && &bytes[4..6] == b"-Q"
                && matches!(bytes[6], b'1'..=b'4'))
            || storage::validate_snapshot_version(value).is_ok()
    } else {
        storage::validate_snapshot_version(value).is_ok()
    };
    ensure!(valid, "invalid {column} value {value:?}");
    Ok(())
}

fn validate_artifact_inventory(output_dir: &Path, candidate: &Candidate) -> Result<()> {
    ensure!(
        candidate.schema_version == 1,
        "unsupported publication candidate schema"
    );
    let expected: BTreeSet<_> = METRICS
        .iter()
        .map(|metric| format!("{}.parquet", metric.name))
        .chain(JSON_ARTIFACTS.iter().map(|name| name.to_string()))
        .collect();
    let candidate_names: BTreeSet<_> = candidate
        .artifacts
        .iter()
        .map(|artifact| artifact.name.clone())
        .collect();
    ensure!(
        candidate_names.len() == candidate.artifacts.len(),
        "publication candidate contains duplicate artifact records"
    );
    ensure!(
        candidate_names == expected,
        "publication candidate artifact set is incomplete or stale"
    );
    let actual_parquets: BTreeSet<_> = fs::read_dir(output_dir)?
        .filter_map(|entry| entry.ok())
        .filter(|entry| {
            entry
                .path()
                .extension()
                .is_some_and(|extension| extension == "parquet")
        })
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .collect();
    let expected_parquets: BTreeSet<_> = METRICS
        .iter()
        .map(|metric| format!("{}.parquet", metric.name))
        .collect();
    ensure!(
        actual_parquets == expected_parquets,
        "root metric set contains a missing or stale Parquet artifact"
    );
    for recorded in &candidate.artifacts {
        ensure!(
            &artifact_record(&output_dir.join(&recorded.name))? == recorded,
            "artifact {} changed after merge",
            recorded.name
        );
    }
    for name in JSON_ARTIFACTS {
        let value: serde_json::Value = read_json(&output_dir.join(name))?;
        ensure!(
            value.as_object().is_some_and(|object| !object.is_empty()),
            "dashboard artifact {name} must be a non-empty JSON object"
        );
    }
    Ok(())
}

fn snapshot_month_index(value: &str) -> Result<i32> {
    storage::validate_snapshot_version(value)?;
    let year: i32 = value[..4].parse()?;
    let month: i32 = value[5..].parse()?;
    Ok(year * 12 + month)
}

fn validate_snapshots(
    data_dir: &Path,
    registry: &LifecycleRegistry,
    context: &RunContext,
    cutoffs: &BTreeMap<String, String>,
) -> Result<BTreeMap<String, String>> {
    let scheduled: BTreeSet<_> = registry
        .wikis
        .iter()
        .filter(|(_, entry)| entry.publication == "published" && entry.refresh == "scheduled")
        .map(|(wiki, _)| wiki.clone())
        .collect();
    ensure!(
        context.refresh_wikis.is_empty() || context.refresh_wikis == scheduled,
        "full publication run must refresh every scheduled wiki"
    );
    let mut selected = BTreeMap::new();
    for (wiki, lifecycle) in &registry.wikis {
        if lifecycle.publication != "published" {
            continue;
        }
        let cutoff = cutoffs
            .get(wiki)
            .with_context(|| format!("no GDP cutoff for published wiki {wiki}"))?;
        if lifecycle.refresh == "paused" {
            let expected = lifecycle
                .imported_cutoff
                .as_deref()
                .with_context(|| format!("paused wiki {wiki} has no imported cutoff"))?;
            ensure!(
                cutoff == expected,
                "{wiki} cutoff {cutoff} does not match imported cutoff {expected}"
            );
            continue;
        }
        if lifecycle.refresh == "scheduled" {
            let snapshot = storage::current_snapshot_version(data_dir, wiki)?
                .with_context(|| format!("scheduled wiki {wiki} has no selected snapshot"))?;
            if context.refresh_wikis.contains(wiki) {
                ensure!(
                    context.requested_snapshot_version.as_deref() == Some(snapshot.as_str()),
                    "selected snapshot for {wiki} does not match requested snapshot"
                );
            }
            let lag = snapshot_month_index(&snapshot)? - snapshot_month_index(cutoff)?;
            ensure!(
                (0..=2).contains(&lag),
                "{wiki} cutoff {cutoff} is not plausible for snapshot {snapshot}"
            );
            let pointer = storage::snapshot_pointer_path(data_dir, wiki);
            let age_days =
                now_unix()?.saturating_sub(artifact_record(&pointer)?.modified_secs) / 86_400;
            let sla = lifecycle
                .freshness_sla_days
                .context("scheduled wiki has no freshness SLA")?;
            ensure!(
                age_days <= sla,
                "selected snapshot pointer for {wiki} is {age_days} days old (SLA {sla})"
            );
            selected.insert(wiki.clone(), snapshot);
        }
    }
    Ok(selected)
}

pub fn validate(
    data_dir: &Path,
    output_dir: &Path,
    lifecycle_path: &Path,
    run_id: &str,
) -> Result<()> {
    ensure!(
        !run_id.trim().is_empty(),
        "publication validation requires a run ID"
    );
    let context: RunContext = read_json(&output_dir.join(RUN_CONTEXT_FILE))?;
    let candidate: Candidate = read_json(&output_dir.join(CANDIDATE_FILE))?;
    ensure!(
        context.schema_version == 1,
        "unsupported publication run schema"
    );
    ensure!(
        context.run_id == run_id && candidate.run_id == run_id,
        "publication state does not belong to run ID {run_id}"
    );
    validate_artifact_inventory(output_dir, &candidate)?;
    let registry = load_lifecycle(lifecycle_path)?;
    let mut reports = BTreeMap::new();
    let mut cutoffs = BTreeMap::new();
    for spec in &METRICS {
        let contract = registry
            .publication_contract
            .datasets
            .get(spec.name)
            .context("missing dataset contract")?;
        let wikis = expected_wikis(&registry, contract)?;
        let root = output_dir.join(format!("{}.parquet", spec.name));
        let root_rows = validate_schema(&root, spec)?;
        let root_summary = summarize(&root, spec)?;
        let mut wiki_reports = BTreeMap::new();
        let mut source_rows = 0_u64;
        let mut source_total = 0_i64;
        for wiki in wikis {
            let path = output_dir
                .join(&wiki)
                .join(format!("{}.parquet", spec.name));
            let rows = validate_schema(&path, spec)?;
            ensure!(
                rows >= contract.minimum_rows_per_wiki,
                "{} has {rows} rows for {wiki}; minimum is {}",
                spec.name,
                contract.minimum_rows_per_wiki
            );
            let summary = summarize(&path, spec)?;
            ensure!(
                summary.wiki_min == wiki && summary.wiki_max == wiki,
                "{} contains rows for the wrong wiki",
                path.display()
            );
            if let (Some(column), Some(minimum), Some(maximum)) = (
                spec.date_column,
                summary.minimum_date.as_deref(),
                summary.maximum_date.as_deref(),
            ) {
                validate_date(minimum, column)?;
                validate_date(maximum, column)?;
            }
            if spec.name == "gdp" {
                cutoffs.insert(
                    wiki.clone(),
                    summary
                        .maximum_date
                        .clone()
                        .context("GDP maximum date is missing")?,
                );
            }
            source_rows += rows;
            source_total += summary.conservation_total.unwrap_or(0);
            wiki_reports.insert(
                wiki,
                WikiMetricReport {
                    rows,
                    minimum_date: summary.minimum_date,
                    maximum_date: summary.maximum_date,
                    conservation_total: summary.conservation_total,
                },
            );
        }
        ensure!(
            root_rows == source_rows,
            "{} row conservation failed: root={root_rows}, sources={source_rows}",
            spec.name
        );
        if let Some(root_total) = root_summary.conservation_total {
            ensure!(
                root_total > 0 && root_total == source_total,
                "{} value conservation failed: root={root_total}, sources={source_total}",
                spec.name
            );
        }
        reports.insert(
            spec.name.to_string(),
            MetricReport {
                rows: root_rows,
                conservation_total: root_summary.conservation_total,
                wikis: wiki_reports,
            },
        );
    }
    let selected_snapshots = validate_snapshots(data_dir, &registry, &context, &cutoffs)?;
    let patrol_contract = registry
        .publication_contract
        .datasets
        .get("patrol")
        .context("missing patrol contract")?;
    let patrol_wikis = expected_wikis(&registry, patrol_contract)?;
    let mut patrol_sources = BTreeMap::new();
    for wiki in patrol_wikis.into_iter().filter(|wiki| {
        registry
            .wikis
            .get(wiki)
            .is_some_and(|lifecycle| lifecycle.refresh == "scheduled")
    }) {
        let patrol_path = data_dir.join("patrol").join(&wiki).join("patrol.parquet");
        let rights_path = data_dir.join("patrol").join(&wiki).join("rights.parquet");
        let patrol_rows = ParquetReader::new(File::open(patrol_path)?).num_rows()? as u64;
        let rights_rows = ParquetReader::new(File::open(rights_path)?).num_rows()? as u64;
        ensure!(
            patrol_rows > 0 && rights_rows > 0,
            "scheduled wiki {wiki} has empty patrol or rights source data"
        );
        patrol_sources.insert(
            wiki,
            PatrolSourceReport {
                patrol_events: patrol_rows,
                rights_events: rights_rows,
            },
        );
    }
    let receipt = GateReceipt {
        schema_version: 1,
        run_id: run_id.to_string(),
        validated_at_unix: now_unix()?,
        selected_snapshot_versions: selected_snapshots,
        cutoff_dates: cutoffs,
        metrics: reports,
        patrol_sources,
        artifacts: candidate.artifacts,
    };
    atomic_json(&output_dir.join(RECEIPT_FILE), &receipt)?;
    info!(run_id, receipt = %output_dir.join(RECEIPT_FILE).display(), "publication gate passed");
    Ok(())
}

pub fn verify(output_dir: &Path, run_id: &str) -> Result<()> {
    let context: RunContext = read_json(&output_dir.join(RUN_CONTEXT_FILE))?;
    let candidate: Candidate = read_json(&output_dir.join(CANDIDATE_FILE))?;
    let receipt: GateReceipt = read_json(&output_dir.join(RECEIPT_FILE))?;
    ensure!(
        context.run_id == run_id && candidate.run_id == run_id && receipt.run_id == run_id,
        "publication receipt does not belong to run ID {run_id}"
    );
    ensure!(
        receipt.schema_version == 1 && receipt.artifacts == candidate.artifacts,
        "publication receipt does not match candidate artifacts"
    );
    for artifact in &receipt.artifacts {
        ensure!(
            &artifact_record(&output_dir.join(&artifact.name))? == artifact,
            "artifact {} changed after validation",
            artifact.name
        );
    }
    info!(run_id, "publication receipt verified");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::TestDir;
    use serde_json::{Value, json};
    use std::path::PathBuf;

    struct Fixture {
        data: TestDir,
        output: TestDir,
        lifecycle: TestDir,
        lifecycle_path: PathBuf,
    }

    impl Fixture {
        fn new() -> Result<Self> {
            let data = TestDir::new()?;
            let output = TestDir::new()?;
            let lifecycle = TestDir::new()?;
            let lifecycle_path = lifecycle.path().join("lifecycle.json");
            let datasets: serde_json::Map<String, Value> = METRICS
                .iter()
                .map(|metric| {
                    let contract = if metric.name == "patrol" {
                        json!({"wikis":["nlwiki"],"minimum_rows_per_wiki":1})
                    } else {
                        json!({"coverage":"all_published","minimum_rows_per_wiki":1})
                    };
                    (metric.name.to_string(), contract)
                })
                .collect();
            let lifecycle_json = serde_json::to_vec(&json!({
                "schema_version": 1,
                "publication_contract": {"datasets": datasets},
                "wikis": {
                    "nlwiki": {
                        "publication": "published",
                        "refresh": "scheduled",
                        "freshness_sla_days": 10
                    }
                }
            }))?;
            fs::write(&lifecycle_path, lifecycle_json)?;

            let analytical_result =
                storage::snapshot_analytical_wiki_dir(data.path(), "nlwiki", "2026-03");
            let analytical = analytical_result.expect("valid analytical snapshot path");
            let warehouse = storage::snapshot_warehouse_wiki_dir(data.path(), "nlwiki", "2026-03")?;
            fs::create_dir_all(analytical)?;
            fs::create_dir_all(warehouse)?;
            storage::publish_current_snapshot(data.path(), "nlwiki", "2026-03")?;
            let patrol_dir = data.path().join("patrol/nlwiki");
            fs::create_dir_all(&patrol_dir)?;
            write_single_i64(&patrol_dir.join("patrol.parquet"))?;
            write_single_i64(&patrol_dir.join("rights.parquet"))?;

            let wiki_dir = output.path().join("nlwiki");
            fs::create_dir_all(&wiki_dir)?;
            for spec in &METRICS {
                let wiki_path = wiki_dir.join(format!("{}.parquet", spec.name));
                write_metric(&wiki_path, spec, "nlwiki")?;
                let root_path = output.path().join(format!("{}.parquet", spec.name));
                write_metric(&root_path, spec, "nlwiki")?;
            }
            for name in JSON_ARTIFACTS {
                fs::write(output.path().join(name), b"{\"ok\":true}\n")?;
            }
            Ok(Self {
                data,
                output,
                lifecycle,
                lifecycle_path,
            })
        }

        fn names(&self) -> Vec<String> {
            METRICS
                .iter()
                .map(|metric| format!("{}.parquet", metric.name))
                .chain(JSON_ARTIFACTS.iter().map(|name| name.to_string()))
                .collect()
        }

        fn prepare(&self, run_id: &str) -> Result<()> {
            let result = begin_run(
                self.output.path(),
                Some(run_id),
                &["nlwiki".to_string()],
                Some("2026-03"),
            );
            result?;
            record_candidate(self.output.path(), Some(run_id), &self.names())
        }
    }

    fn write_single_i64(path: &Path) -> Result<()> {
        let mut frame = DataFrame::new_infer_height(vec![Column::new("value".into(), [1_i64])])?;
        ParquetWriter::new(File::create(path)?).finish(&mut frame)?;
        Ok(())
    }

    fn string_value(name: &str) -> &str {
        match name {
            "wiki" => "nlwiki",
            "year_month" => "2026-03",
            "period" => "2001",
            "week_start" => "2026-03-02",
            "cohort_year" | "year" => "2026",
            _ => "value",
        }
    }

    fn write_metric(path: &Path, spec: &MetricSpec, _wiki: &str) -> Result<()> {
        let columns = spec
            .schema
            .iter()
            .map(|(name, kind)| match kind {
                Kind::String => Column::new((*name).into(), [string_value(name)]),
                Kind::I32 => Column::new((*name).into(), [1_i32]),
                Kind::I64 => Column::new((*name).into(), [1_i64]),
                Kind::U32 => Column::new((*name).into(), [1_u32]),
                Kind::F64 => Column::new((*name).into(), [1_f64]),
            })
            .collect();
        let mut frame = DataFrame::new(1, columns)?;
        ParquetWriter::new(File::create(path)?).finish(&mut frame)?;
        Ok(())
    }

    #[test]
    fn publication_gate_validates_and_rechecks_a_complete_run() -> Result<()> {
        let fixture = Fixture::new()?;
        fixture.prepare("run-good")?;

        let validation = validate(
            fixture.data.path(),
            fixture.output.path(),
            &fixture.lifecycle_path,
            "run-good",
        );
        validation?;
        verify(fixture.output.path(), "run-good")?;

        let receipt: Value = read_json(&fixture.output.path().join(RECEIPT_FILE))?;
        assert_eq!(receipt["run_id"], "run-good");
        assert_eq!(receipt["cutoff_dates"]["nlwiki"], "2026-03");
        assert_eq!(
            receipt["metrics"]["page_weekly_edits"]["conservation_total"],
            1
        );
        assert_eq!(receipt["patrol_sources"]["nlwiki"]["rights_events"], 1);
        let registry = load_lifecycle(&fixture.lifecycle_path)?;
        let merge_only = RunContext {
            schema_version: 1,
            run_id: "merge-only".to_string(),
            started_at_unix: now_unix()?,
            refresh_wikis: BTreeSet::new(),
            requested_snapshot_version: None,
        };
        let cutoffs = BTreeMap::from([("nlwiki".to_string(), "2026-03".to_string())]);
        assert_eq!(
            validate_snapshots(fixture.data.path(), &registry, &merge_only, &cutoffs)?
                .get("nlwiki")
                .map(String::as_str),
            Some("2026-03")
        );
        assert!(fixture.lifecycle.path().is_dir());
        Ok(())
    }

    #[test]
    fn publication_state_is_optional_without_a_run_id_and_strict_with_one() -> Result<()> {
        let fixture = Fixture::new()?;
        begin_run(fixture.output.path(), None, &[], None)?;
        record_candidate(fixture.output.path(), None, &[])?;
        assert!(begin_run(fixture.output.path(), Some(" "), &[], None).is_err());

        begin_run(fixture.output.path(), Some("run-a"), &[], None)?;
        assert!(record_candidate(fixture.output.path(), Some("run-b"), &fixture.names()).is_err());
        let mut duplicate = fixture.names();
        duplicate.push(duplicate[0].clone());
        assert!(record_candidate(fixture.output.path(), Some("run-a"), &duplicate).is_err());
        Ok(())
    }

    #[test]
    fn publication_gate_rejects_stale_and_mutated_artifacts() -> Result<()> {
        let fixture = Fixture::new()?;
        fixture.prepare("run-stale")?;
        fs::write(fixture.output.path().join("unexpected.parquet"), b"stale")?;
        let stale = validate(
            fixture.data.path(),
            fixture.output.path(),
            &fixture.lifecycle_path,
            "run-stale",
        )
        .expect_err("stale root metric must fail");
        assert!(stale.to_string().contains("stale Parquet"));
        fs::remove_file(fixture.output.path().join("unexpected.parquet"))?;

        let manifest = fixture.output.path().join("manifest.json");
        fs::write(manifest, b"{\"changed\":true}")?;
        let changed = validate(
            fixture.data.path(),
            fixture.output.path(),
            &fixture.lifecycle_path,
            "run-stale",
        )
        .expect_err("artifact mutation must fail");
        assert!(changed.to_string().contains("changed after merge"));
        Ok(())
    }

    #[test]
    fn contract_and_date_helpers_fail_closed() -> Result<()> {
        assert!(validate_date("2026-03-02", "week_start").is_ok());
        assert!(validate_date("2001", "period").is_ok());
        assert!(validate_date("2026-Q1", "period").is_ok());
        assert!(validate_date("0000", "period").is_err());
        assert!(validate_date("2026-Q5", "period").is_err());
        assert!(validate_date("2026-03", "year_month").is_ok());
        assert!(validate_date("bad", "year_month").is_err());
        assert_eq!(snapshot_month_index("2026-03")?, 2026 * 12 + 3);
        assert!(snapshot_month_index("2026-13").is_err());
        for (kind, dtype) in [
            (Kind::String, DataType::String),
            (Kind::I32, DataType::Int32),
            (Kind::I64, DataType::Int64),
            (Kind::U32, DataType::UInt32),
            (Kind::F64, DataType::Float64),
        ] {
            assert!(kind_matches(kind, &dtype));
        }
        assert!(!kind_matches(Kind::String, &DataType::Int64));

        let fixture = Fixture::new()?;
        let registry = load_lifecycle(&fixture.lifecycle_path)?;
        let invalid = DatasetContract {
            coverage: None,
            wikis: None,
            minimum_rows_per_wiki: 1,
        };
        assert!(expected_wikis(&registry, &invalid).is_err());

        let explicit = DatasetContract {
            coverage: None,
            wikis: Some(BTreeSet::from(["nlwiki".to_string()])),
            minimum_rows_per_wiki: 1,
        };
        assert_eq!(
            expected_wikis(&registry, &explicit)?,
            BTreeSet::from(["nlwiki".to_string()])
        );
        Ok(())
    }

    #[test]
    fn filesystem_and_schema_helpers_report_invalid_inputs() -> Result<()> {
        let temp = TestDir::new()?;
        let directory_target = temp.path().join("existing-dir");
        fs::create_dir(&directory_target)?;
        assert!(atomic_json(&directory_target, &json!({"ok": true})).is_err());
        assert!(artifact_record(&directory_target).is_err());

        let malformed = temp.path().join("malformed.json");
        fs::write(&malformed, b"{")?;
        assert!(read_json::<Value>(&malformed).is_err());
        assert!(artifact_record(&temp.path().join("missing")).is_err());

        let wrong_width = temp.path().join("wrong-width.parquet");
        write_single_i64(&wrong_width)?;
        assert!(validate_schema(&wrong_width, &METRICS[0]).is_err());

        let wrong_name = temp.path().join("wrong-name.parquet");
        let columns = vec![
            Column::new("cohort_year".into(), ["2026"]),
            Column::new("cohort_size".into(), [1_u32]),
            Column::new("reached_5".into(), [1_u32]),
            Column::new("reached_25".into(), [1_u32]),
            Column::new("reached_100".into(), [1_u32]),
            Column::new("not_wiki".into(), ["nlwiki"]),
        ];
        let mut frame = DataFrame::new(1, columns)?;
        ParquetWriter::new(File::create(&wrong_name)?).finish(&mut frame)?;
        assert!(validate_schema(&wrong_name, &METRICS[0]).is_err());

        let wrong_type = temp.path().join("wrong-type.parquet");
        let columns = vec![
            Column::new("cohort_year".into(), ["2026"]),
            Column::new("cohort_size".into(), [1_i64]),
            Column::new("reached_5".into(), [1_u32]),
            Column::new("reached_25".into(), [1_u32]),
            Column::new("reached_100".into(), [1_u32]),
            Column::new("wiki".into(), ["nlwiki"]),
        ];
        let mut frame = DataFrame::new_infer_height(columns)?;
        ParquetWriter::new(File::create(&wrong_type)?).finish(&mut frame)?;
        assert!(validate_schema(&wrong_type, &METRICS[0]).is_err());
        Ok(())
    }

    #[test]
    fn snapshot_validation_covers_paused_and_hidden_lifecycle_states() -> Result<()> {
        let registry = LifecycleRegistry {
            schema_version: 1,
            publication_contract: PublicationContract {
                datasets: BTreeMap::new(),
            },
            wikis: BTreeMap::from([
                (
                    "frwiki".to_string(),
                    LifecycleWiki {
                        publication: "published".to_string(),
                        refresh: "paused".to_string(),
                        imported_cutoff: Some("2026-03".to_string()),
                        freshness_sla_days: None,
                    },
                ),
                (
                    "hiddenwiki".to_string(),
                    LifecycleWiki {
                        publication: "hidden".to_string(),
                        refresh: "paused".to_string(),
                        imported_cutoff: None,
                        freshness_sla_days: None,
                    },
                ),
                (
                    "manualwiki".to_string(),
                    LifecycleWiki {
                        publication: "published".to_string(),
                        refresh: "manual".to_string(),
                        imported_cutoff: None,
                        freshness_sla_days: None,
                    },
                ),
            ]),
        };
        let context = RunContext {
            schema_version: 1,
            run_id: "paused".to_string(),
            started_at_unix: now_unix()?,
            refresh_wikis: BTreeSet::new(),
            requested_snapshot_version: None,
        };
        let cutoffs = BTreeMap::from([
            ("frwiki".to_string(), "2026-03".to_string()),
            ("manualwiki".to_string(), "2026-03".to_string()),
        ]);
        let data = TestDir::new()?;
        assert!(validate_snapshots(data.path(), &registry, &context, &cutoffs)?.is_empty());
        assert!(validate_snapshots(data.path(), &registry, &context, &BTreeMap::new()).is_err());
        Ok(())
    }

    #[test]
    fn gate_rejects_rows_labeled_as_another_wiki() -> Result<()> {
        let fixture = Fixture::new()?;
        let business = METRICS
            .iter()
            .find(|metric| metric.name == "business_funnel")
            .context("business metric")?;
        let path = fixture.output.path().join("nlwiki/business_funnel.parquet");
        assert_eq!(business.schema.len(), 6);
        let columns = vec![
            Column::new("cohort_year".into(), ["2026"]),
            Column::new("cohort_size".into(), [1_u32]),
            Column::new("reached_5".into(), [1_u32]),
            Column::new("reached_25".into(), [1_u32]),
            Column::new("reached_100".into(), [1_u32]),
            Column::new("wiki".into(), ["otherwiki"]),
        ];
        let mut frame = DataFrame::new(1, columns)?;
        ParquetWriter::new(File::create(path)?).finish(&mut frame)?;
        fixture.prepare("wrong-wiki")?;
        let error = validate(
            fixture.data.path(),
            fixture.output.path(),
            &fixture.lifecycle_path,
            "wrong-wiki",
        )
        .expect_err("wrong wiki labels must fail");
        assert!(error.to_string().contains("wrong wiki"));
        Ok(())
    }
}
