use anyhow::{Context, Result, ensure};
use polars::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{info, warn};

use crate::generation_lifecycle::GenerationState as GState;
use crate::{licensing, storage};

const RUN_CONTEXT_FILE: &str = ".publication-run.json";
const CANDIDATE_FILE: &str = ".publication-candidate.json";
pub const RECEIPT_FILE: &str = "publication-gate.json";
const VALIDATION_BATCH_ROWS: usize = 250_000;
const JSON_ARTIFACTS: [&str; 13] = [
    crate::browser_data::INDEX_FILENAME,
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
    license_spdx: String,
}

#[derive(Serialize, Deserialize)]
struct RunContext {
    schema_version: u8,
    run_id: String,
    started_at_unix: u64,
    refresh_wikis: BTreeSet<String>,
    requested_snapshot_version: Option<String>,
    #[serde(default)]
    requested_snapshot_versions: BTreeMap<String, String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq)]
struct PreparedArtifact {
    path: String,
    bytes: u64,
    rows: u64,
    sha256: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq)]
struct ReadyWikiCandidate {
    schema_version: u8,
    wiki: String,
    snapshot: String,
    run_id: String,
    ready_at_unix: u64,
    generating_commit: Option<String>,
    cutoff_date: String,
    #[serde(default)]
    workload_profile: Option<crate::workload_profile::WorkloadProfile>,
    artifacts: Vec<PreparedArtifact>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq)]
struct QualificationReceipt {
    schema_version: u8,
    publication_eligible: bool,
    wiki: String,
    snapshot: String,
    run_id: String,
    qualified_at_unix: u64,
    generating_commit: Option<String>,
    cutoff_date: String,
    workload_profile: crate::workload_profile::WorkloadProfile,
    artifacts: Vec<PreparedArtifact>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum WikiPreparationPlan {
    NoOp {
        ready_path: PathBuf,
    },
    Build {
        same_snapshot_candidate: bool,
        compute_reused: bool,
        patrol_reused: bool,
    },
}

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq)]
struct SelectionEntry {
    wiki: String,
    snapshot: String,
    candidate_relative: String,
    #[serde(default)]
    previous_candidate_relative: Option<String>,
    previous_snapshot: Option<String>,
    backup_relative: Option<String>,
    #[serde(default)]
    workload_profile: Option<crate::workload_profile::WorkloadProfile>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq)]
struct PublicationSelection {
    schema_version: u8,
    run_id: String,
    state: String,
    entries: Vec<SelectionEntry>,
}

#[derive(Clone, Copy)]
struct CandidateGeneration<'a> {
    output_dir: &'a Path,
    wiki: &'a str,
    snapshot: &'a str,
    run_id: &'a str,
}

impl<'a> CandidateGeneration<'a> {
    fn new(output_dir: &'a Path, wiki: &'a str, snapshot: &'a str, run_id: &'a str) -> Self {
        Self {
            output_dir,
            wiki,
            snapshot,
            run_id,
        }
    }

    fn adopt(
        self,
        state: GState,
        reason: &str,
    ) -> Result<crate::generation_lifecycle::GenerationRecord> {
        crate::generation_lifecycle::adopt(
            self.output_dir,
            self.wiki,
            self.snapshot,
            self.run_id,
            state,
            reason,
        )
    }

    fn transition(
        self,
        state: GState,
        reason: &str,
        publication_run_id: Option<&str>,
    ) -> Result<crate::generation_lifecycle::GenerationRecord> {
        crate::generation_lifecycle::transition(
            self.output_dir,
            self.wiki,
            self.snapshot,
            self.run_id,
            state,
            reason,
            publication_run_id,
        )
    }
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
    #[serde(default)]
    minimum_rows_by_wiki: BTreeMap<String, u64>,
}

impl DatasetContract {
    fn minimum_rows(&self, wiki: &str) -> u64 {
        self.minimum_rows_by_wiki
            .get(wiki)
            .copied()
            .unwrap_or(self.minimum_rows_per_wiki)
    }
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
struct BrowserDataReport {
    generation: String,
    partitions: usize,
    rows: u64,
    bytes: u64,
    largest_partition_bytes: u64,
}

#[derive(Serialize, Deserialize)]
struct GateReceipt {
    schema_version: u8,
    run_id: String,
    validated_at_unix: u64,
    license: licensing::LicensePolicy,
    attribution: String,
    independence_notice: String,
    source_datasets: Vec<licensing::SourceDataset>,
    trademark: licensing::TrademarkPolicy,
    privacy: licensing::PrivacyPolicy,
    toolforge_open_licensing: licensing::ToolforgePolicy,
    provenance: PublicationProvenance,
    selected_snapshot_versions: BTreeMap<String, String>,
    cutoff_dates: BTreeMap<String, String>,
    metrics: BTreeMap<String, MetricReport>,
    patrol_sources: BTreeMap<String, PatrolSourceReport>,
    browser_data: BrowserDataReport,
    artifacts: Vec<ArtifactRecord>,
}

#[derive(Serialize, Deserialize)]
struct PublicationProvenance {
    run_id: String,
    generating_commit: Option<String>,
    generated_at_unix: u64,
    selected_snapshot_versions: BTreeMap<String, String>,
    #[serde(default)]
    workload_profiles: BTreeMap<String, crate::workload_profile::WorkloadProfile>,
    #[serde(default)]
    determinism_contract: Option<crate::determinism::DeterminismContract>,
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
        license_spdx: licensing::ARTIFACT_LICENSE_SPDX.to_string(),
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
        requested_snapshot_versions: requested_snapshot_version
            .map(|snapshot| {
                refresh_wikis
                    .iter()
                    .map(|wiki| (wiki.clone(), snapshot.to_string()))
                    .collect()
            })
            .unwrap_or_default(),
    };
    atomic_json(&output_dir.join(RUN_CONTEXT_FILE), &context)
}

fn begin_selected_run(
    output_dir: &Path,
    run_id: &str,
    selected_snapshots: &BTreeMap<String, String>,
) -> Result<()> {
    ensure!(
        !run_id.trim().is_empty(),
        "publication run ID cannot be empty"
    );
    let context = RunContext {
        schema_version: 2,
        run_id: run_id.to_string(),
        started_at_unix: now_unix()?,
        refresh_wikis: selected_snapshots.keys().cloned().collect(),
        requested_snapshot_version: None,
        requested_snapshot_versions: selected_snapshots.clone(),
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

fn valid_component(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

pub(crate) fn wiki_candidate_dir(
    output_dir: &Path,
    wiki: &str,
    snapshot: &str,
    run_id: &str,
) -> Result<PathBuf> {
    ensure!(valid_component(wiki), "unsafe candidate wiki identifier");
    storage::validate_snapshot_version(snapshot)?;
    ensure!(valid_component(run_id), "unsafe candidate run identifier");
    Ok(output_dir
        .join("_candidates")
        .join(wiki)
        .join(snapshot)
        .join(run_id))
}

pub(crate) fn wiki_qualification_dir(
    output_dir: &Path,
    wiki: &str,
    snapshot: &str,
    run_id: &str,
) -> Result<PathBuf> {
    ensure!(
        valid_component(wiki),
        "unsafe qualification wiki identifier"
    );
    storage::validate_snapshot_version(snapshot)?;
    ensure!(
        valid_component(run_id),
        "unsafe qualification run identifier"
    );
    Ok(output_dir
        .join("_qualifications")
        .join(wiki)
        .join(snapshot)
        .join(run_id))
}

fn prepared_artifact(candidate_dir: &Path, path: &Path) -> Result<PreparedArtifact> {
    let relative = path
        .strip_prefix(candidate_dir)
        .context("candidate artifact is outside candidate directory")?
        .to_string_lossy()
        .into_owned();
    let mut reader = ParquetReader::new(File::open(path)?);
    let rows = u64::try_from(reader.num_rows()?)?;
    let (bytes, sha256) = storage::sha256_file(path)?;
    Ok(PreparedArtifact {
        path: relative,
        bytes,
        rows,
        sha256,
    })
}

fn validate_prepared_artifact(candidate_dir: &Path, artifact: &PreparedArtifact) -> Result<()> {
    let relative = Path::new(&artifact.path);
    ensure!(
        !relative.is_absolute()
            && relative
                .components()
                .all(|component| matches!(component, std::path::Component::Normal(_))),
        "candidate artifact has an unsafe path"
    );
    let path = candidate_dir.join(relative);
    let (bytes, sha256) = storage::sha256_file(&path)?;
    let mut reader = ParquetReader::new(File::open(&path)?);
    let rows = u64::try_from(reader.num_rows()?)?;
    ensure!(
        bytes == artifact.bytes && sha256 == artifact.sha256 && rows == artifact.rows,
        "prepared candidate artifact identity changed"
    );
    Ok(())
}

pub(crate) fn mark_wiki_candidate_ready(
    data_dir: &Path,
    output_dir: &Path,
    lifecycle_path: &Path,
    wiki: &str,
    snapshot: &str,
    run_id: &str,
) -> Result<PathBuf> {
    let candidate_dir = wiki_candidate_dir(output_dir, wiki, snapshot, run_id)?;
    let generation = CandidateGeneration::new(output_dir, wiki, snapshot, run_id);
    let reason = "recovered candidate preparation";
    let generation_state = generation.adopt(GState::Building, reason)?;
    storage::read_generation_manifest(data_dir, wiki, snapshot)?;
    let registry = load_lifecycle(lifecycle_path)?;
    let lifecycle = registry
        .wikis
        .get(wiki)
        .with_context(|| format!("candidate wiki {wiki} is not registered"))?;
    ensure!(
        lifecycle.publication == "published"
            && matches!(lifecycle.refresh.as_str(), "scheduled" | "manual"),
        "candidate wiki {wiki} is not managed for publication"
    );
    let mut artifacts = Vec::new();
    let mut cutoff_date = None;
    for spec in &METRICS {
        let contract = registry
            .publication_contract
            .datasets
            .get(spec.name)
            .context("missing candidate dataset contract")?;
        if !expected_wikis(&registry, contract)?.contains(wiki) {
            continue;
        }
        let path = candidate_dir
            .join(wiki)
            .join(format!("{}.parquet", spec.name));
        let rows = validate_schema(&path, spec)?;
        let minimum_rows = contract.minimum_rows(wiki);
        ensure!(
            rows >= minimum_rows,
            "{} candidate has {rows} rows for {wiki}; minimum is {minimum_rows}",
            spec.name
        );
        let summary = summarize(&path, spec)?;
        ensure!(
            summary.wiki_min == wiki && summary.wiki_max == wiki,
            "candidate metric contains rows for another wiki"
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
            let cutoff = summary
                .maximum_date
                .clone()
                .context("candidate GDP maximum date is missing")?;
            validate_snapshot_cutoff(wiki, snapshot, &cutoff)?;
            cutoff_date = Some(cutoff);
        }
        artifacts.push(prepared_artifact(&candidate_dir, &path)?);
    }
    let patrol_dir = data_dir.join("patrol").join(wiki);
    for name in ["patrol.parquet", "rights.parquet"] {
        let mut reader = ParquetReader::new(File::open(patrol_dir.join(name))?);
        ensure!(
            reader.num_rows()? > 0,
            "candidate patrol source {name} is empty"
        );
    }
    artifacts.sort_by(|left, right| left.path.cmp(&right.path));
    let workload_profile = crate::workload_profile::load(data_dir, wiki, snapshot)?;
    ensure!(
        workload_profile.is_some() || !crate::workload_profile::require_qualified()?,
        "qualified production candidate has no persisted workload profile"
    );
    let ready = ReadyWikiCandidate {
        schema_version: 1,
        wiki: wiki.to_string(),
        snapshot: snapshot.to_string(),
        run_id: run_id.to_string(),
        ready_at_unix: now_unix()?,
        generating_commit: licensing::generating_commit(),
        cutoff_date: cutoff_date.context("candidate GDP cutoff is missing")?,
        workload_profile,
        artifacts,
    };
    let generation_state =
        if generation_state.state == crate::generation_lifecycle::GenerationState::Building {
            let reason = "candidate artifacts passed semantic validation";
            generation.transition(GState::Validated, reason, None)?
        } else {
            generation_state
        };
    ensure!(
        matches!(
            generation_state.state,
            crate::generation_lifecycle::GenerationState::Validated
                | crate::generation_lifecycle::GenerationState::Ready
        ),
        "candidate cannot be marked ready from {:?}",
        generation_state.state
    );
    let ready_path = candidate_dir.join("ready.json");
    atomic_json(&ready_path, &ready)?;
    if generation_state.state == crate::generation_lifecycle::GenerationState::Validated {
        let reason = "ready receipt was durably published";
        generation.transition(GState::Ready, reason, None)?;
    }
    info!(wiki, snapshot, run_id, path = %ready_path.display(), "wiki candidate is ready");
    Ok(ready_path)
}

pub(crate) fn ensure_qualification_wiki(lifecycle_path: &Path, wiki: &str) -> Result<()> {
    let registry = load_lifecycle(lifecycle_path)?;
    let lifecycle = registry
        .wikis
        .get(wiki)
        .with_context(|| format!("qualification wiki {wiki} is not registered"))?;
    let hidden_qualification =
        lifecycle.publication == "hidden" && lifecycle.refresh == "qualification";
    let paused_publication = lifecycle.publication == "published" && lifecycle.refresh == "paused";
    ensure!(
        hidden_qualification || paused_publication,
        "qualification wiki {wiki} must be hidden/qualification or published/paused"
    );
    Ok(())
}

pub(crate) fn mark_wiki_qualification_ready(
    data_dir: &Path,
    output_dir: &Path,
    lifecycle_path: &Path,
    wiki: &str,
    snapshot: &str,
    run_id: &str,
) -> Result<PathBuf> {
    ensure_qualification_wiki(lifecycle_path, wiki)?;
    storage::read_generation_manifest(data_dir, wiki, snapshot)?;
    let qualification_dir = wiki_qualification_dir(output_dir, wiki, snapshot, run_id)?;
    let generation = CandidateGeneration::new(output_dir, wiki, snapshot, run_id);
    let generation_state = generation.adopt(GState::Building, "recovered qualification run")?;
    let mut artifacts = Vec::new();
    let mut cutoff_date = None;
    for spec in &METRICS {
        let path = qualification_dir
            .join(wiki)
            .join(format!("{}.parquet", spec.name));
        let rows = validate_schema(&path, spec)?;
        ensure!(
            rows > 0,
            "{} qualification output is empty for {wiki}",
            spec.name
        );
        let summary = summarize(&path, spec)?;
        ensure!(
            summary.wiki_min == wiki && summary.wiki_max == wiki,
            "qualification metric contains rows for another wiki"
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
            let cutoff = summary
                .maximum_date
                .clone()
                .context("qualification GDP maximum date is missing")?;
            validate_snapshot_cutoff(wiki, snapshot, &cutoff)?;
            cutoff_date = Some(cutoff);
        }
        artifacts.push(prepared_artifact(&qualification_dir, &path)?);
    }
    let patrol_dir = data_dir.join("patrol").join(wiki);
    for name in ["patrol.parquet", "rights.parquet"] {
        let mut reader = ParquetReader::new(File::open(patrol_dir.join(name))?);
        ensure!(
            reader.num_rows()? > 0,
            "qualification patrol source {name} is empty"
        );
    }
    artifacts.sort_by(|left, right| left.path.cmp(&right.path));
    let workload_profile = crate::workload_profile::load(data_dir, wiki, snapshot)?
        .context("qualification has no persisted workload profile")?;
    let receipt = QualificationReceipt {
        schema_version: 1,
        publication_eligible: false,
        wiki: wiki.to_string(),
        snapshot: snapshot.to_string(),
        run_id: run_id.to_string(),
        qualified_at_unix: now_unix()?,
        generating_commit: licensing::generating_commit(),
        cutoff_date: cutoff_date.context("qualification GDP cutoff is missing")?,
        workload_profile,
        artifacts,
    };
    let generation_state = if generation_state.state == GState::Building {
        generation.transition(
            GState::Validated,
            "qualification artifacts passed semantic validation",
            None,
        )?
    } else {
        generation_state
    };
    ensure!(
        matches!(generation_state.state, GState::Validated | GState::Ready),
        "qualification cannot be marked ready from {:?}",
        generation_state.state
    );
    let receipt_path = qualification_dir.join("qualification.json");
    atomic_json(&receipt_path, &receipt)?;
    if generation_state.state == GState::Validated {
        generation.transition(
            GState::Ready,
            "publication-ineligible qualification receipt was durably published",
            None,
        )?;
    }
    info!(wiki, snapshot, run_id, path = %receipt_path.display(), "wiki qualification is ready");
    Ok(receipt_path)
}

fn validate_ready_candidate_metadata(
    data_dir: &Path,
    candidate_dir: &Path,
    ready: &ReadyWikiCandidate,
) -> Result<()> {
    ensure!(
        ready.schema_version == 1,
        "unsupported ready candidate schema"
    );
    ensure!(
        candidate_dir.ends_with(
            Path::new(&ready.wiki)
                .join(&ready.snapshot)
                .join(&ready.run_id)
        ),
        "ready candidate path does not match its identity"
    );
    storage::read_generation_manifest(data_dir, &ready.wiki, &ready.snapshot)?;
    if let Some(profile) = &ready.workload_profile {
        profile.validate(&ready.wiki, &ready.snapshot)?;
        profile.ensure_compute_qualified()?;
    } else {
        ensure!(
            !crate::workload_profile::require_qualified()?,
            "qualified production ready candidate has no workload profile"
        );
    }
    ensure!(
        !ready.artifacts.is_empty(),
        "ready candidate has no artifacts"
    );
    validate_snapshot_cutoff(&ready.wiki, &ready.snapshot, &ready.cutoff_date)
}

fn validate_ready_candidate(
    data_dir: &Path,
    candidate_dir: &Path,
    ready: &ReadyWikiCandidate,
) -> Result<()> {
    validate_ready_candidate_metadata(data_dir, candidate_dir, ready)?;
    for artifact in &ready.artifacts {
        validate_prepared_artifact(candidate_dir, artifact)?;
    }
    Ok(())
}

fn reconcile_ready_generation_state(output_dir: &Path, ready: &ReadyWikiCandidate) -> Result<()> {
    let generation =
        CandidateGeneration::new(output_dir, &ready.wiki, &ready.snapshot, &ready.run_id);
    let reason = "adopted legacy validated ready receipt";
    let mut record = generation.adopt(GState::Ready, reason)?;
    if record.state == crate::generation_lifecycle::GenerationState::Building {
        let reason = "recovered candidate passed ready-receipt validation";
        record = generation.transition(GState::Validated, reason, None)?;
    }
    if record.state == crate::generation_lifecycle::GenerationState::Validated {
        let reason = "recovered durable ready receipt";
        generation.transition(GState::Ready, reason, None)?;
    }
    Ok(())
}

fn copy_candidate_files(
    source_candidate: &Path,
    target_candidate: &Path,
    files: &[PathBuf],
) -> Result<()> {
    for source in files {
        let relative = source
            .strip_prefix(source_candidate)
            .expect("verified reusable file must remain below its candidate");
        let target = target_candidate.join(relative);
        let parent = target
            .parent()
            .context("reusable candidate target has no parent")?;
        fs::create_dir_all(parent)?;
        let temporary = parent.join(format!(
            ".{}.{}.reuse.tmp",
            target
                .file_name()
                .context("reusable candidate target has no filename")?
                .to_string_lossy(),
            std::process::id()
        ));
        let copied = (|| -> Result<()> {
            fs::copy(source, &temporary)?;
            File::open(&temporary)?.sync_all()?;
            fs::rename(&temporary, &target)?;
            File::open(parent)?.sync_all()?;
            Ok(())
        })();
        if copied.is_err() {
            let _ = fs::remove_file(&temporary);
        }
        copied?;
    }
    Ok(())
}

pub(crate) fn plan_wiki_preparation(
    data_dir: &Path,
    output_dir: &Path,
    wiki: &str,
    snapshot: &str,
    run_id: &str,
) -> Result<WikiPreparationPlan> {
    ensure!(valid_component(wiki), "unsafe candidate wiki");
    storage::validate_snapshot_version(snapshot)?;
    ensure!(valid_component(run_id), "unsafe candidate run ID");
    let snapshot_root = output_dir.join("_candidates").join(wiki).join(snapshot);
    let target_candidate = wiki_candidate_dir(output_dir, wiki, snapshot, run_id)?;
    ensure!(
        !target_candidate.join("ready.json").exists(),
        "run ID already identifies a ready candidate"
    );
    if !snapshot_root.is_dir() {
        crate::generation_lifecycle::begin(output_dir, wiki, snapshot, run_id)?;
        return Ok(WikiPreparationPlan::Build {
            same_snapshot_candidate: false,
            compute_reused: false,
            patrol_reused: false,
        });
    }

    let mut candidates = Vec::new();
    for entry in fs::read_dir(&snapshot_root)? {
        let candidate_dir = entry?.path();
        let ready_path = candidate_dir.join("ready.json");
        if !ready_path.is_file() {
            continue;
        }
        let ready: ReadyWikiCandidate = read_json(&ready_path)?;
        ensure!(
            ready.wiki == wiki && ready.snapshot == snapshot,
            "ready candidate identity does not match preparation request"
        );
        validate_ready_candidate(data_dir, &candidate_dir, &ready)?;
        reconcile_ready_generation_state(output_dir, &ready)?;
        candidates.push((ready, candidate_dir));
    }
    candidates.sort_by(|(left, _), (right, _)| {
        (left.ready_at_unix, &left.run_id).cmp(&(right.ready_at_unix, &right.run_id))
    });

    let mut reusable_compute = None;
    let mut reusable_patrol = None;
    for (_, candidate_dir) in candidates.iter().rev() {
        let compute =
            crate::compute::reusable_candidate_files(wiki, snapshot, data_dir, candidate_dir)?;
        let patrol =
            crate::patrol::reusable_candidate_files(wiki, snapshot, data_dir, candidate_dir)?;
        if compute.is_some() && patrol.is_some() {
            let ready_path = candidate_dir.join("ready.json");
            info!(wiki, snapshot, path = %ready_path.display(), "snapshot candidate fingerprints are unchanged");
            return Ok(WikiPreparationPlan::NoOp { ready_path });
        }
        if reusable_compute.is_none()
            && let Some(files) = compute
        {
            reusable_compute = Some((candidate_dir.clone(), files));
        }
        if reusable_patrol.is_none()
            && let Some(files) = patrol
        {
            reusable_patrol = Some((candidate_dir.clone(), files));
        }
    }

    crate::generation_lifecycle::begin(output_dir, wiki, snapshot, run_id)?;

    reusable_compute
        .as_ref()
        .map_or(Ok(()), |(source, files)| {
            copy_candidate_files(source, &target_candidate, files)
        })?;
    reusable_patrol.as_ref().map_or(Ok(()), |(source, files)| {
        copy_candidate_files(source, &target_candidate, files)
    })?;
    let compute_reused = reusable_compute.is_some();
    let patrol_reused = reusable_patrol.is_some();
    info!(
        wiki,
        snapshot, compute_reused, patrol_reused, "planned invalidated candidate stages"
    );
    Ok(WikiPreparationPlan::Build {
        same_snapshot_candidate: !candidates.is_empty(),
        compute_reused,
        patrol_reused,
    })
}

fn latest_ready_candidates(
    data_dir: &Path,
    output_dir: &Path,
    lifecycle_path: &Path,
) -> Result<Vec<(ReadyWikiCandidate, PathBuf)>> {
    let registry = load_lifecycle(lifecycle_path)?;
    let mut selected = Vec::new();
    for (wiki, lifecycle) in registry.wikis {
        if lifecycle.publication != "published"
            || !matches!(lifecycle.refresh.as_str(), "scheduled" | "manual")
        {
            continue;
        }
        let root = output_dir.join("_candidates").join(&wiki);
        if !root.is_dir() {
            continue;
        }
        let mut candidates = Vec::new();
        for snapshot_entry in fs::read_dir(&root)? {
            let snapshot_dir = snapshot_entry?.path();
            if !snapshot_dir.is_dir() {
                continue;
            }
            for run_entry in fs::read_dir(&snapshot_dir)? {
                let candidate_dir = run_entry?.path();
                let ready_path = candidate_dir.join("ready.json");
                if !ready_path.is_file() {
                    continue;
                }
                let ready: ReadyWikiCandidate = read_json(&ready_path)?;
                ensure!(ready.wiki == wiki, "ready candidate wiki mismatch");
                validate_ready_candidate_metadata(data_dir, &candidate_dir, &ready)?;
                reconcile_ready_generation_state(output_dir, &ready)?;
                candidates.push((ready, candidate_dir));
            }
        }
        candidates.sort_by(|(left, _), (right, _)| {
            (&left.snapshot, left.ready_at_unix, &left.run_id).cmp(&(
                &right.snapshot,
                right.ready_at_unix,
                &right.run_id,
            ))
        });
        let Some(candidate) = candidates.pop() else {
            continue;
        };
        let current = storage::current_snapshot_version(data_dir, &wiki)?;
        ensure!(
            current
                .as_deref()
                .is_none_or(|value| candidate.0.snapshot.as_str() >= value),
            "ready candidate would downgrade {wiki}"
        );
        selected.push(candidate);
    }
    Ok(selected)
}

fn selection_path(output_dir: &Path, run_id: &str) -> Result<PathBuf> {
    ensure!(
        valid_component(run_id),
        "unsafe publication selection run ID"
    );
    Ok(output_dir
        .join("_publication_transactions")
        .join(run_id)
        .join("selection.json"))
}

fn active_candidate_target(output_dir: &Path, wiki: &str) -> Result<Option<PathBuf>> {
    let path = output_dir.join(wiki);
    match fs::read_link(&path) {
        Ok(target) => Ok(Some(target)),
        Err(error) if error.kind() == std::io::ErrorKind::InvalidInput => Ok(None),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn active_candidate_relative(output_dir: &Path, wiki: &str) -> Result<Option<String>> {
    let Some(target) = active_candidate_target(output_dir, wiki)? else {
        return Ok(None);
    };
    let absolute = if target.is_absolute() {
        target
    } else {
        output_dir.join(target)
    };
    let Some(candidate) = absolute.parent() else {
        return Ok(None);
    };
    let Ok(relative) = candidate.strip_prefix(output_dir) else {
        return Ok(None);
    };
    let components = relative
        .components()
        .map(|component| match component {
            std::path::Component::Normal(value) => value.to_str(),
            _ => None,
        })
        .collect::<Option<Vec<_>>>();
    let Some(components) = components else {
        return Ok(None);
    };
    if components.len() == 4
        && components[0] == "_candidates"
        && components[1] == wiki
        && crate::storage::validate_snapshot_version(components[2]).is_ok()
        && valid_component(components[3])
        && absolute.file_name().and_then(|name| name.to_str()) == Some(wiki)
    {
        Ok(Some(relative.to_string_lossy().into_owned()))
    } else {
        Ok(None)
    }
}

fn remove_selection_link(path: &Path) -> Result<()> {
    if path.is_symlink() {
        fs::remove_file(path)?;
    }
    Ok(())
}

fn move_active_to_backup(output_dir: &Path, active: &Path, backup: Option<&str>) -> Result<()> {
    let Some(backup) = backup else {
        return Ok(());
    };
    fs::rename(active, output_dir.join(backup))?;
    Ok(())
}

fn restore_backup(output_dir: &Path, active: &Path, backup: Option<&str>) -> Result<()> {
    let Some(backup) = backup else {
        return Ok(());
    };
    let backup = output_dir.join(backup);
    if backup.exists() || backup.is_symlink() {
        fs::rename(backup, active)?;
    }
    Ok(())
}

fn remove_committed_backup(output_dir: &Path, backup: Option<&str>) -> Result<()> {
    let Some(backup) = backup else {
        return Ok(());
    };
    let backup = output_dir.join(backup);
    if backup.is_dir() {
        fs::remove_dir_all(backup)?;
    } else if backup.is_symlink() {
        fs::remove_file(backup)?;
    }
    Ok(())
}

fn rollback_selection_files(
    data_dir: &Path,
    output_dir: &Path,
    selection: &PublicationSelection,
) -> Result<()> {
    for entry in selection.entries.iter().rev() {
        let active = output_dir.join(&entry.wiki);
        let expected_target = PathBuf::from(&entry.candidate_relative).join(&entry.wiki);
        if active_candidate_target(output_dir, &entry.wiki)?.as_ref() == Some(&expected_target) {
            remove_selection_link(&active)?;
        }
        restore_backup(output_dir, &active, entry.backup_relative.as_deref())?;
        storage::restore_current_snapshot(
            data_dir,
            &entry.wiki,
            entry.previous_snapshot.as_deref(),
        )?;
    }
    Ok(())
}

fn candidate_identity(candidate_dir: &Path) -> Option<(&str, &str)> {
    let run_id = candidate_dir.file_name()?.to_str()?;
    let snapshot = candidate_dir.parent()?.file_name()?.to_str()?;
    (valid_component(run_id) && crate::storage::validate_snapshot_version(snapshot).is_ok())
        .then_some((snapshot, run_id))
}

fn retire_superseded_candidates(
    output_dir: &Path,
    entry: &SelectionEntry,
    publication_run_id: &str,
) -> Result<usize> {
    let root = output_dir.join("_candidates").join(&entry.wiki);
    let retained = output_dir.join(&entry.candidate_relative);
    let rollback = entry
        .previous_candidate_relative
        .as_deref()
        .map(|relative| output_dir.join(relative));
    let mut removed = 0;
    if !root.is_dir() {
        return Ok(0);
    }
    for snapshot_entry in fs::read_dir(&root)? {
        let snapshot_dir = snapshot_entry?.path();
        if !snapshot_dir.is_dir() {
            continue;
        }
        for run_entry in fs::read_dir(&snapshot_dir)? {
            let candidate_dir = run_entry?.path();
            if !candidate_dir.is_dir()
                || candidate_dir == retained
                || rollback.as_ref() == Some(&candidate_dir)
                || !candidate_dir.join("ready.json").is_file()
            {
                continue;
            }
            let Some((snapshot, run_id)) = candidate_identity(&candidate_dir) else {
                continue;
            };
            let generation = CandidateGeneration::new(output_dir, &entry.wiki, snapshot, run_id);
            let reason = "adopted legacy ready candidate";
            let record = generation.adopt(GState::Ready, reason)?;
            if matches!(
                record.state,
                crate::generation_lifecycle::GenerationState::Ready
                    | crate::generation_lifecycle::GenerationState::Published
            ) {
                let reason = "a newer candidate passed publication validation";
                generation.transition(GState::Superseded, reason, Some(publication_run_id))?;
            }
            let reason = "superseded candidate is outside the rollback window";
            generation.transition(GState::Retired, reason, Some(publication_run_id))?;
            fs::remove_dir_all(&candidate_dir)?;
            removed += 1;
        }
        if fs::read_dir(&snapshot_dir)?.next().is_none() {
            fs::remove_dir(&snapshot_dir)?;
        }
    }
    Ok(removed)
}

fn activate_ready_candidates(
    data_dir: &Path,
    output_dir: &Path,
    lifecycle_path: &Path,
    run_id: &str,
) -> Result<PublicationSelection> {
    let candidates = latest_ready_candidates(data_dir, output_dir, lifecycle_path)?;
    let transaction_dir = selection_path(output_dir, run_id)?
        .parent()
        .context("selection path has no parent")?
        .to_path_buf();
    ensure!(
        !transaction_dir.exists(),
        "publication transaction already exists"
    );
    fs::create_dir_all(transaction_dir.join("backups"))?;
    let mut entries = Vec::new();
    for (ready, candidate_dir) in candidates {
        let candidate_relative = candidate_dir
            .strip_prefix(output_dir)
            .context("candidate is outside output directory")?
            .to_string_lossy()
            .into_owned();
        let expected_target = PathBuf::from(&candidate_relative).join(&ready.wiki);
        if active_candidate_target(output_dir, &ready.wiki)?.as_ref() == Some(&expected_target)
            && storage::current_snapshot_version(data_dir, &ready.wiki)?.as_deref()
                == Some(ready.snapshot.as_str())
        {
            continue;
        }
        validate_ready_candidate(data_dir, &candidate_dir, &ready)?;
        let active = output_dir.join(&ready.wiki);
        let backup = transaction_dir.join("backups").join(&ready.wiki);
        let backup_relative = (active.exists() || active.is_symlink()).then(|| {
            backup
                .strip_prefix(output_dir)
                .expect("transaction backup remains inside output")
                .to_string_lossy()
                .into_owned()
        });
        let previous_snapshot = storage::current_snapshot_version(data_dir, &ready.wiki)?;
        let previous_candidate_relative = active_candidate_relative(output_dir, &ready.wiki)?;
        entries.push(SelectionEntry {
            wiki: ready.wiki,
            snapshot: ready.snapshot,
            candidate_relative,
            previous_candidate_relative,
            previous_snapshot,
            backup_relative,
            workload_profile: ready.workload_profile,
        });
    }
    let mut selection = PublicationSelection {
        schema_version: 1,
        run_id: run_id.to_string(),
        state: "activating".to_string(),
        entries,
    };
    let path = selection_path(output_dir, run_id)?;
    atomic_json(&path, &selection)?;
    let activation = (|| -> Result<()> {
        for entry in &selection.entries {
            let active = output_dir.join(&entry.wiki);
            move_active_to_backup(output_dir, &active, entry.backup_relative.as_deref())?;
            let temporary = output_dir.join(format!(".{}.select.{run_id}.tmp", entry.wiki));
            remove_selection_link(&temporary)?;
            let link_result = std::os::unix::fs::symlink(
                PathBuf::from(&entry.candidate_relative).join(&entry.wiki),
                &temporary,
            );
            link_result?;
            fs::rename(&temporary, &active)?;
            storage::publish_current_snapshot(data_dir, &entry.wiki, &entry.snapshot)?;
        }
        Ok(())
    })();
    if let Err(error) = activation {
        for entry in &selection.entries {
            let temporary = output_dir.join(format!(".{}.select.{run_id}.tmp", entry.wiki));
            remove_selection_link(&temporary)?;
        }
        let rollback_result = rollback_selection_files(data_dir, output_dir, &selection);
        rollback_result?;
        selection.state = "rolled_back".to_string();
        let journal_result = atomic_json(&path, &selection);
        journal_result?;
        return Err(error).context("failed to activate ready candidates");
    }
    selection.state = "selected".to_string();
    atomic_json(&path, &selection)?;
    Ok(selection)
}

fn validate_active_ready_candidates(
    data_dir: &Path,
    output_dir: &Path,
    lifecycle_path: &Path,
) -> Result<()> {
    for (ready, candidate_dir) in latest_ready_candidates(data_dir, output_dir, lifecycle_path)? {
        let expected_target = candidate_dir
            .strip_prefix(output_dir)
            .context("candidate is outside output directory")?
            .join(&ready.wiki);
        ensure!(
            active_candidate_target(output_dir, &ready.wiki)?.as_ref() == Some(&expected_target)
                && storage::current_snapshot_version(data_dir, &ready.wiki)?.as_deref()
                    == Some(ready.snapshot.as_str()),
            "published candidate identity changed while planning repair"
        );
        validate_ready_candidate(data_dir, &candidate_dir, &ready)?;
    }
    Ok(())
}

pub(crate) fn prepare_ready_publication(
    data_dir: &Path,
    output_dir: &Path,
    lifecycle_path: &Path,
    run_id: &str,
) -> Result<()> {
    let mut selection = activate_ready_candidates(data_dir, output_dir, lifecycle_path, run_id)?;
    if selection.entries.is_empty() {
        let reusable = read_json::<Candidate>(&output_dir.join(CANDIDATE_FILE))
            .context("unchanged publication has no current artifact inventory")
            .and_then(|published| {
                validate_artifact_inventory(output_dir, &published)
                    .context("unchanged publication artifacts are not reusable")
            });
        match reusable {
            Ok(()) => {
                selection.state = "no_op".to_string();
                atomic_json(&selection_path(output_dir, run_id)?, &selection)?;
                crate::observability::record_stage_skipped("publication_prepare", None);
                info!(run_id, "ready-candidate publication is unchanged");
                return Ok(());
            }
            Err(error) => {
                warn!(
                    run_id,
                    error = %format!("{error:#}"),
                    "published artifact inventory requires transactional repair"
                );
                validate_active_ready_candidates(data_dir, output_dir, lifecycle_path)
                    .context("publication repair inputs are not reusable")?;
            }
        }
    }
    let snapshots = selection
        .entries
        .iter()
        .map(|entry| (entry.wiki.clone(), entry.snapshot.clone()))
        .collect::<BTreeMap<_, _>>();
    let result = (|| -> Result<()> {
        begin_selected_run(output_dir, run_id, &snapshots)?;
        crate::merge::merge_outputs(output_dir, Some(run_id))?;
        validate(data_dir, output_dir, lifecycle_path, run_id)
    })();
    if let Err(error) = result {
        rollback_selection_files(data_dir, output_dir, &selection)?;
        let mut rolled_back = selection;
        rolled_back.state = "rolled_back".to_string();
        atomic_json(&selection_path(output_dir, run_id)?, &rolled_back)?;
        return Err(error).context("ready-candidate publication preparation failed");
    }
    Ok(())
}

pub(crate) fn rollback_ready_publication(
    data_dir: &Path,
    output_dir: &Path,
    lifecycle_path: &Path,
    run_id: &str,
) -> Result<()> {
    let path = selection_path(output_dir, run_id)?;
    let mut selection: PublicationSelection = read_json(&path)?;
    ensure!(
        selection.schema_version == 1
            && selection.run_id == run_id
            && selection.state == "selected",
        "publication selection is not active"
    );
    rollback_selection_files(data_dir, output_dir, &selection)?;
    selection.state = "rolled_back".to_string();
    atomic_json(&path, &selection)?;
    begin_selected_run(output_dir, run_id, &BTreeMap::new())?;
    crate::merge::merge_outputs(output_dir, Some(run_id))?;
    validate(data_dir, output_dir, lifecycle_path, run_id)
}

pub(crate) fn commit_ready_publication(
    data_dir: &Path,
    output_dir: &Path,
    run_id: &str,
) -> Result<()> {
    let path = selection_path(output_dir, run_id)?;
    let mut selection: PublicationSelection = read_json(&path)?;
    ensure!(
        selection.schema_version == 1
            && selection.run_id == run_id
            && matches!(selection.state.as_str(), "selected" | "committing"),
        "publication selection is not active"
    );
    verify(output_dir, run_id)?;
    let receipt: GateReceipt = read_json(&output_dir.join(RECEIPT_FILE))?;
    ensure!(
        receipt.run_id == run_id,
        "publication receipt belongs to another transaction"
    );
    if selection.state == "selected" {
        selection.state = "committing".to_string();
        atomic_json(&path, &selection)?;
    }
    for entry in &selection.entries {
        ensure!(
            storage::current_snapshot_version(data_dir, &entry.wiki)?.as_deref()
                == Some(entry.snapshot.as_str()),
            "selected snapshot changed before publication commit"
        );
        let selected = output_dir.join(&entry.candidate_relative);
        let (selected_snapshot, selected_run_id) =
            candidate_identity(&selected).context("selected candidate identity is invalid")?;
        let generation =
            CandidateGeneration::new(output_dir, &entry.wiki, selected_snapshot, selected_run_id);
        let reason = "adopted legacy selected candidate";
        generation.adopt(GState::Ready, reason)?;
        let reason = "site publication passed validation";
        generation.transition(GState::Published, reason, Some(run_id))?;
        if let Some(previous) = entry.previous_candidate_relative.as_deref() {
            let previous = output_dir.join(previous);
            if let Some((snapshot, candidate_run_id)) = candidate_identity(&previous) {
                let previous =
                    CandidateGeneration::new(output_dir, &entry.wiki, snapshot, candidate_run_id);
                let reason = "adopted legacy published candidate";
                previous.adopt(GState::Published, reason)?;
                let reason = "replacement site publication passed validation";
                previous.transition(GState::Superseded, reason, Some(run_id))?;
            }
        }
        let retired_candidates = retire_superseded_candidates(output_dir, entry, run_id)?;
        storage::retire_inactive_snapshots(data_dir, &entry.wiki)?;
        remove_committed_backup(output_dir, entry.backup_relative.as_deref())?;
        info!(
            wiki = entry.wiki,
            retired_candidates, "retired superseded wiki candidates"
        );
    }
    selection.state = "committed".to_string();
    atomic_json(&path, &selection)
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
    for (wiki, lifecycle) in &registry.wikis {
        ensure!(
            matches!(
                lifecycle.publication.as_str(),
                "published" | "hidden" | "retired"
            ),
            "wiki lifecycle entry {wiki} has invalid publication state"
        );
        ensure!(
            matches!(
                lifecycle.refresh.as_str(),
                "scheduled" | "manual" | "paused" | "qualification"
            ),
            "wiki lifecycle entry {wiki} has invalid refresh state"
        );
        ensure!(
            lifecycle.refresh != "qualification" || lifecycle.publication == "hidden",
            "wiki lifecycle entry {wiki} must be hidden during qualification"
        );
        ensure!(
            lifecycle.publication != "retired" || lifecycle.refresh == "paused",
            "retired wiki lifecycle entry {wiki} must be paused"
        );
    }
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

fn summarize_batched(path: &Path, spec: &MetricSpec, batch_rows: usize) -> Result<FileSummary> {
    ensure!(
        batch_rows > 0,
        "publication validation batch size must be positive"
    );
    let mut columns = vec!["wiki".to_string()];
    if let Some(date) = spec.date_column {
        columns.push(date.to_string());
    }
    if let Some(total) = spec.conservation_column {
        columns.push(total.to_string());
    }
    columns.sort();
    columns.dedup();
    let mut reader = storage::SequentialParquetReader::new(path, Some(columns), batch_rows)?;
    let rows = reader.rows();
    ensure!(rows > 0, "{} is empty", path.display());

    let mut wiki_min: Option<String> = None;
    let mut wiki_max: Option<String> = None;
    let mut previous_wiki: Option<String> = None;
    let mut minimum_date: Option<String> = None;
    let mut maximum_date: Option<String> = None;
    let mut conservation_total = spec.conservation_column.map(|_| 0_i64);
    let mut observed_rows = 0_usize;

    while let Some(batch) = reader.next_batch()? {
        observed_rows = observed_rows
            .checked_add(batch.height())
            .context("publication validation row count overflow")?;
        let wikis = batch.column("wiki")?.str()?;
        for wiki in wikis.iter() {
            let wiki = wiki.with_context(|| format!("{} contains a null wiki", path.display()))?;
            ensure!(
                previous_wiki
                    .as_deref()
                    .is_none_or(|previous| previous <= wiki),
                "{} is not in deterministic wiki-major order",
                path.display()
            );
            update_string_range(wiki, &mut wiki_min, &mut wiki_max);
            if previous_wiki.as_deref() != Some(wiki) {
                previous_wiki = Some(wiki.to_owned());
            }
        }
        if let Some(date) = spec.date_column {
            for value in batch.column(date)?.str()?.iter() {
                let value =
                    value.with_context(|| format!("{} contains a null {date}", path.display()))?;
                update_string_range(value, &mut minimum_date, &mut maximum_date);
            }
        }
        if let Some(total_column) = spec.conservation_column {
            let batch_total = sum_conservation_column(&batch, total_column, path)?;
            let total = conservation_total
                .as_mut()
                .context("missing conservation state")?;
            *total = total
                .checked_add(batch_total)
                .with_context(|| format!("{} conservation total overflow", path.display()))?;
        }
    }
    ensure!(
        observed_rows == rows,
        "row conservation failed during publication validation"
    );

    Ok(FileSummary {
        wiki_min: wiki_min.context("aggregate wiki_min is null")?,
        wiki_max: wiki_max.context("aggregate wiki_max is null")?,
        minimum_date,
        maximum_date,
        conservation_total,
    })
}

fn update_string_range(value: &str, minimum: &mut Option<String>, maximum: &mut Option<String>) {
    if minimum.as_deref().is_none_or(|current| value < current) {
        *minimum = Some(value.to_owned());
    }
    if maximum.as_deref().is_none_or(|current| value > current) {
        *maximum = Some(value.to_owned());
    }
}

fn sum_conservation_column(frame: &DataFrame, name: &str, _path: &Path) -> Result<i64> {
    let column = frame.column(name)?;
    ensure!(
        column.null_count() == 0,
        "metric contains null conservation values in {name}"
    );
    match column.dtype() {
        DataType::UInt32 => column.u32()?.iter().try_fold(0_i64, |total, value| {
            total
                .checked_add(i64::from(value.context("validated non-null UInt32 value")?))
                .context("conservation batch total overflow")
        }),
        DataType::Int64 => column.i64()?.iter().try_fold(0_i64, |total, value| {
            total
                .checked_add(value.context("validated non-null Int64 value")?)
                .context("conservation batch total overflow")
        }),
        dtype => anyhow::bail!("conservation column {name} has type {dtype:?}"),
    }
}

fn summarize(path: &Path, spec: &MetricSpec) -> Result<FileSummary> {
    summarize_batched(path, spec, VALIDATION_BATCH_ROWS)
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

fn validate_snapshot_cutoff(wiki: &str, snapshot: &str, cutoff: &str) -> Result<()> {
    // MediaWiki history generations can contain the partial calendar month after
    // their version label. Permit that bounded lead, while still rejecting data
    // from a later generation or a cutoff more than two months behind.
    let lag = snapshot_month_index(snapshot)? - snapshot_month_index(cutoff)?;
    ensure!(
        (-1..=2).contains(&lag),
        "{wiki} cutoff {cutoff} is not plausible for snapshot {snapshot}"
    );
    Ok(())
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
        context.schema_version >= 2
            || context.refresh_wikis.is_empty()
            || scheduled.is_subset(&context.refresh_wikis),
        "full publication run must include every scheduled wiki"
    );
    for wiki in &context.refresh_wikis {
        let lifecycle = registry
            .wikis
            .get(wiki)
            .with_context(|| format!("refresh run contains unregistered wiki {wiki}"))?;
        ensure!(
            lifecycle.publication == "published",
            "refresh run contains non-published wiki {wiki}"
        );
        ensure!(
            lifecycle.refresh == "scheduled" || lifecycle.refresh == "manual",
            "refresh run contains {wiki}, whose lifecycle state is {}",
            lifecycle.refresh
        );
    }
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
        let refreshed_manual =
            lifecycle.refresh == "manual" && context.refresh_wikis.contains(wiki);
        if lifecycle.refresh == "scheduled" || refreshed_manual {
            let snapshot = storage::current_snapshot_version(data_dir, wiki)?
                .with_context(|| format!("managed wiki {wiki} has no selected snapshot"))?;
            if context.refresh_wikis.contains(wiki) {
                let requested = context
                    .requested_snapshot_versions
                    .get(wiki)
                    .map(String::as_str)
                    .or(context.requested_snapshot_version.as_deref());
                ensure!(
                    requested == Some(snapshot.as_str()),
                    "selected snapshot for {wiki} does not match requested snapshot"
                );
            }
            validate_snapshot_cutoff(wiki, &snapshot, cutoff)?;
            let pointer = storage::snapshot_pointer_path(data_dir, wiki);
            let age_days =
                now_unix()?.saturating_sub(artifact_record(&pointer)?.modified_secs) / 86_400;
            if lifecycle.refresh == "scheduled" || lifecycle.freshness_sla_days.is_some() {
                let sla = lifecycle
                    .freshness_sla_days
                    .context("scheduled wiki has no freshness SLA")?;
                ensure!(
                    age_days <= sla,
                    "selected snapshot pointer for {wiki} is {age_days} days old (SLA {sla})"
                );
            }
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
        matches!(context.schema_version, 1 | 2),
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
            let minimum_rows = contract.minimum_rows(&wiki);
            ensure!(
                rows >= minimum_rows,
                "{} has {rows} rows for {wiki}; minimum is {minimum_rows}",
                spec.name
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
    let published_wikis: BTreeSet<_> = registry
        .wikis
        .iter()
        .filter(|(_, lifecycle)| lifecycle.publication == "published")
        .map(|(wiki, _)| wiki.clone())
        .collect();
    crate::browser_data::validate(output_dir, Some(&published_wikis))?;
    let browser_index =
        crate::browser_data::read_index(&output_dir.join(crate::browser_data::INDEX_FILENAME))?;
    let browser_data = BrowserDataReport {
        generation: browser_index.generation,
        partitions: browser_index.entries.len(),
        rows: browser_index
            .entries
            .iter()
            .try_fold(0_u64, |total, entry| {
                total
                    .checked_add(entry.rows)
                    .context("browser row total overflow")
            })?,
        bytes: browser_index
            .entries
            .iter()
            .try_fold(0_u64, |total, entry| {
                total
                    .checked_add(entry.bytes)
                    .context("browser byte total overflow")
            })?,
        largest_partition_bytes: browser_index
            .entries
            .iter()
            .map(|entry| entry.bytes)
            .max()
            .unwrap_or(0),
    };
    let selected_snapshots = validate_snapshots(data_dir, &registry, &context, &cutoffs)?;
    let patrol_contract = registry
        .publication_contract
        .datasets
        .get("patrol")
        .context("missing patrol contract")?;
    let patrol_wikis = expected_wikis(&registry, patrol_contract)?;
    let mut patrol_sources = BTreeMap::new();
    for wiki in patrol_wikis.into_iter().filter(|wiki| {
        context.refresh_wikis.contains(wiki)
            || registry
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
    let policy = licensing::publication_policy()?;
    let validated_at_unix = now_unix()?;
    let workload_profiles = if selection_path(output_dir, run_id)?.is_file() {
        let selection: PublicationSelection = read_json(&selection_path(output_dir, run_id)?)?;
        selection
            .entries
            .into_iter()
            .filter_map(|entry| entry.workload_profile.map(|profile| (entry.wiki, profile)))
            .collect()
    } else {
        BTreeMap::new()
    };
    let receipt = GateReceipt {
        schema_version: 4,
        run_id: run_id.to_string(),
        validated_at_unix,
        license: policy.license,
        attribution: policy.attribution,
        independence_notice: policy.independence_notice,
        source_datasets: policy.source_datasets,
        trademark: policy.trademark,
        privacy: policy.privacy,
        toolforge_open_licensing: policy.toolforge,
        provenance: PublicationProvenance {
            run_id: run_id.to_string(),
            generating_commit: licensing::generating_commit(),
            generated_at_unix: validated_at_unix,
            selected_snapshot_versions: selected_snapshots.clone(),
            workload_profiles,
            determinism_contract: Some(crate::determinism::contract()?),
        },
        selected_snapshot_versions: selected_snapshots,
        cutoff_dates: cutoffs,
        metrics: reports,
        patrol_sources,
        browser_data,
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
    let policy = licensing::publication_policy()?;
    // A standalone site build (the on-demand wiki-econ-site Toolforge Job) runs
    // under its own fresh run ID, separate from whichever compute run last
    // validated the data. What must hold is that context/candidate/receipt all
    // agree with EACH OTHER, not that they match this invocation's run ID.
    ensure!(
        context.run_id == candidate.run_id && candidate.run_id == receipt.run_id,
        "publication receipt (run {}) does not match run context (run {})",
        receipt.run_id,
        context.run_id
    );
    // receipt.provenance.generating_commit records which build produced this
    // data; it is not compared against this process's own build commit here,
    // since an on-demand site build legitimately runs a newer (or older)
    // binary than whatever last validated the data.
    ensure!(
        matches!(receipt.schema_version, 3 | 4)
            && receipt.license == policy.license
            && receipt.attribution == policy.attribution
            && receipt.independence_notice == policy.independence_notice
            && receipt.source_datasets == policy.source_datasets
            && receipt.trademark == policy.trademark
            && receipt.privacy == policy.privacy
            && receipt.toolforge_open_licensing == policy.toolforge
            && receipt.provenance.run_id == receipt.run_id
            && receipt.provenance.generated_at_unix == receipt.validated_at_unix
            && receipt.provenance.selected_snapshot_versions == receipt.selected_snapshot_versions
            && (receipt.schema_version == 3
                || receipt.provenance.determinism_contract.as_ref()
                    == Some(&crate::determinism::contract()?))
            && receipt.artifacts == candidate.artifacts,
        "publication receipt does not match candidate artifacts"
    );
    for artifact in &receipt.artifacts {
        ensure!(
            &artifact_record(&output_dir.join(&artifact.name))? == artifact,
            "artifact {} changed after validation",
            artifact.name
        );
    }
    info!(run_id, receipt_run_id = %receipt.run_id, "publication receipt verified");
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
            storage::publish_test_snapshot_pointer(data.path(), "nlwiki", "2026-03")?;
            let patrol_dir = data.path().join("patrol/nlwiki");
            fs::create_dir_all(&patrol_dir)?;
            write_single_i64(&patrol_dir.join("patrol.parquet"))?;
            write_single_i64(&patrol_dir.join("rights.parquet"))?;
            fs::write(
                patrol_dir.join("autopatrol_groups.json"),
                b"{\"autopatrol_groups\":[]}",
            )
            .expect("autopatrol group fixture should write");

            let wiki_dir = output.path().join("nlwiki");
            fs::create_dir_all(&wiki_dir)?;
            for spec in &METRICS {
                let wiki_path = wiki_dir.join(format!("{}.parquet", spec.name));
                write_metric(&wiki_path, spec, "nlwiki")?;
                let root_path = output.path().join(format!("{}.parquet", spec.name));
                write_metric(&root_path, spec, "nlwiki")?;
            }
            crate::browser_data::materialize(
                output.path(),
                Some(&BTreeSet::from(["nlwiki".to_string()])),
            )
            .expect("publication fixture browser index is valid");
            for name in JSON_ARTIFACTS
                .into_iter()
                .filter(|name| *name != crate::browser_data::INDEX_FILENAME)
            {
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

        fn ready_candidate(&self, run_id: &str) -> Result<PathBuf> {
            let analytical =
                storage::snapshot_analytical_wiki_dir(self.data.path(), "nlwiki", "2026-03")?;
            write_single_i64(&analytical.join("template.parquet"))?;
            storage::write_test_generation_manifest_from_files(
                self.data.path(),
                "nlwiki",
                "2026-03",
            )
            .expect("candidate generation manifest should be writable");
            let snapshot = "2026-03";
            let data_dir = self.data.path();
            let (plan, _) =
                crate::snapshot_plan::SnapshotPlan::load_or_resolve(data_dir, "nlwiki", snapshot)?;
            let source_sizes = vec![Some(1); plan.sources.len()];
            crate::workload_profile::load_or_select(self.data.path(), &plan, &source_sizes)?;
            let candidate = wiki_candidate_dir(self.output.path(), "nlwiki", "2026-03", run_id)?;
            let candidate_wiki = candidate.join("nlwiki");
            fs::create_dir_all(&candidate_wiki)?;
            for spec in &METRICS {
                fs::copy(
                    self.output
                        .path()
                        .join("nlwiki")
                        .join(format!("{}.parquet", spec.name)),
                    candidate_wiki.join(format!("{}.parquet", spec.name)),
                )
                .expect("candidate metric should copy");
            }
            crate::compute::record_candidate_fingerprint_for_test(
                "nlwiki",
                "2026-03",
                self.data.path(),
                &candidate,
            )
            .expect("candidate compute fingerprint should record");
            crate::patrol::record_candidate_fingerprint_for_test(
                "nlwiki",
                "2026-03",
                self.data.path(),
                &candidate,
            )
            .expect("candidate patrol fingerprint should record");
            mark_wiki_candidate_ready(
                self.data.path(),
                self.output.path(),
                &self.lifecycle_path,
                "nlwiki",
                "2026-03",
                run_id,
            )
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
    fn publication_summary_reduces_projected_batches_deterministically() -> Result<()> {
        let directory = TestDir::new()?;
        let path = directory.path().join("weekly.parquet");
        let mut frame = DataFrame::new(
            3,
            vec![
                Column::new("wiki".into(), ["ptwiki", "frwiki", "nlwiki"]),
                Column::new(
                    "week_start".into(),
                    ["2026-07-13", "2026-07-27", "2026-07-20"],
                ),
                Column::new("edits".into(), [3_u32, 5_u32, 7_u32]),
                // This unrelated payload proves the validator projects only
                // the columns required by the publication contract.
                Column::new("unused".into(), ["large", "payload", "column"]),
            ],
        )
        .expect("publication summary fixture must be constructible");
        ParquetWriter::new(File::create(&path)?).finish(&mut frame)?;
        assert!(summarize_batched(&path, &METRICS[8], 1).is_err());

        let mut frame = DataFrame::new(
            3,
            vec![
                Column::new("wiki".into(), ["frwiki", "nlwiki", "ptwiki"]),
                Column::new(
                    "week_start".into(),
                    ["2026-07-27", "2026-07-20", "2026-07-13"],
                ),
                Column::new("edits".into(), [5_u32, 7_u32, 3_u32]),
                Column::new("unused".into(), ["payload", "column", "large"]),
            ],
        )
        .expect("sorted publication summary fixture must be constructible");
        ParquetWriter::new(File::create(&path)?).finish(&mut frame)?;

        let summary = summarize_batched(&path, &METRICS[8], 1)?;
        assert_eq!(summary.wiki_min, "frwiki");
        assert_eq!(summary.wiki_max, "ptwiki");
        assert_eq!(summary.minimum_date.as_deref(), Some("2026-07-13"));
        assert_eq!(summary.maximum_date.as_deref(), Some("2026-07-27"));
        assert_eq!(summary.conservation_total, Some(15));
        assert!(summarize_batched(&path, &METRICS[8], 0).is_err());

        let nullable = df!("edits" => &[Some(1_u32), None])?;
        assert!(sum_conservation_column(&nullable, "edits", &path).is_err());
        let signed = df!("edits" => &[2_i64, -1_i64])?;
        assert_eq!(sum_conservation_column(&signed, "edits", &path)?, 1);
        let unsupported = df!("edits" => &[1_f64])?;
        assert!(sum_conservation_column(&unsupported, "edits", &path).is_err());
        Ok(())
    }

    #[test]
    fn publication_gate_validates_and_rechecks_a_complete_run() -> Result<()> {
        let fixture = Fixture::new()?;
        fixture.prepare("run-good")?;

        validate(
            fixture.data.path(),
            fixture.output.path(),
            &fixture.lifecycle_path,
            "run-good",
        )
        .expect("complete publication fixture should validate");
        verify(fixture.output.path(), "run-good")?;

        let receipt: Value = read_json(&fixture.output.path().join(RECEIPT_FILE))?;
        assert_eq!(receipt["schema_version"], 4);
        assert_eq!(
            receipt["provenance"]["determinism_contract"]["contract_version"],
            "pipeline-byte-determinism-v1"
        );
        assert!(
            receipt["browser_data"]["partitions"]
                .as_u64()
                .is_some_and(|value| value > 0)
        );
        assert_eq!(receipt["run_id"], "run-good");
        assert_eq!(receipt["license"]["spdx_identifier"], "MIT");
        assert_eq!(receipt["provenance"]["run_id"], "run-good");
        assert_eq!(
            receipt["provenance"]["selected_snapshot_versions"]["nlwiki"],
            "2026-03"
        );
        assert_eq!(
            receipt["toolforge_open_licensing"]["open_data_license_spdx"],
            "MIT"
        );
        assert!(
            receipt["source_datasets"]
                .as_array()
                .is_some_and(|v| !v.is_empty())
        );
        assert!(receipt["artifacts"].as_array().is_some_and(|artifacts| {
            artifacts
                .iter()
                .all(|artifact| artifact["license_spdx"] == "MIT")
        }));
        assert_eq!(receipt["cutoff_dates"]["nlwiki"], "2026-03");
        assert_eq!(
            receipt["metrics"]["page_weekly_edits"]["conservation_total"],
            1
        );
        assert_eq!(receipt["patrol_sources"]["nlwiki"]["rights_events"], 1);

        let receipt_path = fixture.output.path().join(RECEIPT_FILE);
        let mut tampered_receipt = receipt.clone();
        tampered_receipt["attribution"] = Value::String("tampered".to_string());
        atomic_json(&receipt_path, &tampered_receipt)?;
        assert!(verify(fixture.output.path(), "run-good").is_err());
        atomic_json(&receipt_path, &receipt)?;
        verify(fixture.output.path(), "run-good")?;

        begin_run(fixture.output.path(), Some("run-merge-only"), &[], None)?;
        let merge_candidate = record_candidate(
            fixture.output.path(),
            Some("run-merge-only"),
            &fixture.names(),
        );
        merge_candidate?;
        let merge_validation = validate(
            fixture.data.path(),
            fixture.output.path(),
            &fixture.lifecycle_path,
            "run-merge-only",
        );
        merge_validation?;

        let registry = load_lifecycle(&fixture.lifecycle_path)?;
        let merge_only = RunContext {
            schema_version: 1,
            run_id: "merge-only".to_string(),
            started_at_unix: now_unix()?,
            refresh_wikis: BTreeSet::new(),
            requested_snapshot_version: None,
            requested_snapshot_versions: BTreeMap::new(),
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
    fn ready_candidate_selection_rolls_back_without_touching_the_live_site() -> Result<()> {
        let fixture = Fixture::new()?;
        let ready = fixture.ready_candidate("candidate-1")?;
        assert!(ready.is_file());
        let mut legacy_ready: ReadyWikiCandidate = read_json(&ready)?;
        legacy_ready.workload_profile = None;
        let candidate_dir = ready
            .parent()
            .context("ready receipt should have a parent")?;
        validate_ready_candidate(fixture.data.path(), candidate_dir, &legacy_ready)?;
        let original_wiki_dir = fixture.output.path().join("nlwiki");
        assert!(!original_wiki_dir.is_symlink());
        std::os::unix::fs::symlink(
            "stale-target",
            fixture.output.path().join(".nlwiki.select.publish-1.tmp"),
        )
        .expect("stale selection link fixture should be writable");

        prepare_ready_publication(
            fixture.data.path(),
            fixture.output.path(),
            &fixture.lifecycle_path,
            "publish-1",
        )
        .expect("ready publication should select candidate");
        assert!(original_wiki_dir.is_symlink());
        verify(fixture.output.path(), "publish-1")?;

        rollback_ready_publication(
            fixture.data.path(),
            fixture.output.path(),
            &fixture.lifecycle_path,
            "publish-1",
        )
        .expect("ready publication rollback should restore prior data");
        assert!(original_wiki_dir.is_dir());
        assert!(!original_wiki_dir.is_symlink());
        assert_eq!(
            storage::current_snapshot_version(fixture.data.path(), "nlwiki")?.as_deref(),
            Some("2026-03")
        );
        let selection: PublicationSelection =
            read_json(&selection_path(fixture.output.path(), "publish-1")?)?;
        assert_eq!(selection.state, "rolled_back");
        Ok(())
    }

    #[test]
    fn preparation_planner_noops_or_seeds_only_reusable_stages() -> Result<()> {
        let fixture = Fixture::new()?;
        assert_eq!(
            plan_wiki_preparation(
                fixture.data.path(),
                fixture.output.path(),
                "nlwiki",
                "2026-04",
                "new-snapshot",
            )
            .expect("new snapshot should require a build"),
            WikiPreparationPlan::Build {
                same_snapshot_candidate: false,
                compute_reused: false,
                patrol_reused: false,
            }
        );

        fs::create_dir_all(
            fixture
                .output
                .path()
                .join("_candidates/nlwiki/2026-03/incomplete"),
        )
        .expect("incomplete candidate fixture should write");
        let ready = fixture.ready_candidate("candidate-reusable")?;
        let state_path = crate::generation_lifecycle::state_path(
            fixture.output.path(),
            "nlwiki",
            "2026-03",
            "candidate-reusable",
        )
        .expect("candidate state path should resolve");
        fs::remove_file(state_path).expect("candidate state should be removable");
        crate::generation_lifecycle::begin(
            fixture.output.path(),
            "nlwiki",
            "2026-03",
            "candidate-reusable",
        )
        .expect("candidate lifecycle should restart");
        assert_eq!(
            plan_wiki_preparation(
                fixture.data.path(),
                fixture.output.path(),
                "nlwiki",
                "2026-03",
                "noop-check",
            )
            .expect("unchanged snapshot should be reusable"),
            WikiPreparationPlan::NoOp { ready_path: ready }
        );

        let source = wiki_candidate_dir(
            fixture.output.path(),
            "nlwiki",
            "2026-03",
            "candidate-reusable",
        )
        .expect("candidate source path should resolve");
        let compute_receipt = source.join("_stages/compute/nlwiki.json");
        let mut receipt: Value = read_json(&compute_receipt)?;
        receipt["algorithm_version"] = Value::String("superseded-algorithm".to_string());
        atomic_json(&compute_receipt, &receipt)?;
        let failed_target =
            wiki_candidate_dir(fixture.output.path(), "nlwiki", "2026-03", "failed-copy")?;
        fs::write(&failed_target, b"block candidate directory")?;
        assert!(
            plan_wiki_preparation(
                fixture.data.path(),
                fixture.output.path(),
                "nlwiki",
                "2026-03",
                "failed-copy",
            )
            .is_err()
        );
        fs::remove_file(failed_target)?;

        let plan = plan_wiki_preparation(
            fixture.data.path(),
            fixture.output.path(),
            "nlwiki",
            "2026-03",
            "partial-reuse",
        )
        .expect("patrol-only reuse should plan");
        assert_eq!(
            plan,
            WikiPreparationPlan::Build {
                same_snapshot_candidate: true,
                compute_reused: false,
                patrol_reused: true,
            }
        );
        let target =
            wiki_candidate_dir(fixture.output.path(), "nlwiki", "2026-03", "partial-reuse")?;
        assert!(target.join("nlwiki/patrol.parquet").is_file());
        assert!(target.join("_stages/patrol_compute/nlwiki.json").is_file());
        assert!(!target.join("nlwiki/gdp.parquet").exists());

        fixture.ready_candidate("candidate-compute")?;
        crate::patrol::record_candidate_fingerprint_for_test(
            "nlwiki",
            "2026-03",
            fixture.data.path(),
            &source,
        )
        .expect("reusable patrol fingerprint should follow the generation fixture");
        let compute_source = wiki_candidate_dir(
            fixture.output.path(),
            "nlwiki",
            "2026-03",
            "candidate-compute",
        )
        .expect("compute candidate path should resolve");
        let patrol_receipt = compute_source.join("_stages/patrol_compute/nlwiki.json");
        let mut patrol_value: Value = read_json(&patrol_receipt)?;
        patrol_value["algorithm_version"] = Value::String("superseded-patrol".to_string());
        atomic_json(&patrol_receipt, &patrol_value)?;
        assert_eq!(
            plan_wiki_preparation(
                fixture.data.path(),
                fixture.output.path(),
                "nlwiki",
                "2026-03",
                "combined-reuse",
            )
            .expect("independently reusable stages should combine"),
            WikiPreparationPlan::Build {
                same_snapshot_candidate: true,
                compute_reused: true,
                patrol_reused: true,
            }
        );
        let combined =
            wiki_candidate_dir(fixture.output.path(), "nlwiki", "2026-03", "combined-reuse")?;
        assert!(combined.join("nlwiki/gdp.parquet").is_file());
        assert!(combined.join("nlwiki/patrol.parquet").is_file());
        assert!(
            plan_wiki_preparation(
                fixture.data.path(),
                fixture.output.path(),
                "nlwiki",
                "2026-03",
                "candidate-reusable",
            )
            .is_err()
        );

        let copy_root = fixture.output.path().join("copy-error");
        fs::create_dir_all(&copy_root)?;
        assert!(
            copy_candidate_files(
                &copy_root,
                &fixture.output.path().join("copy-target"),
                &[copy_root.join("missing")],
            )
            .is_err()
        );
        Ok(())
    }

    #[test]
    fn ready_candidate_commit_retires_superseded_candidates_and_backup() -> Result<()> {
        let fixture = Fixture::new()?;
        fixture.ready_candidate("candidate-1")?;
        fixture.ready_candidate("candidate-2")?;

        prepare_ready_publication(
            fixture.data.path(),
            fixture.output.path(),
            &fixture.lifecycle_path,
            "publish-2",
        )
        .expect("candidate should prepare for commit");
        let selection_file = selection_path(fixture.output.path(), "publish-2")?;
        let mut interrupted: PublicationSelection = read_json(&selection_file)?;
        interrupted.state = "committing".to_string();
        interrupted.entries[0].previous_candidate_relative =
            Some("_candidates/nlwiki/invalid/run".to_string());
        atomic_json(&selection_file, &interrupted)?;
        commit_ready_publication(fixture.data.path(), fixture.output.path(), "publish-2")?;

        let active = fixture.output.path().join("nlwiki");
        assert!(active.is_symlink());
        assert!(
            fixture
                .output
                .path()
                .join("_candidates/nlwiki/2026-03/candidate-2")
                .is_dir()
        );
        assert!(
            !fixture
                .output
                .path()
                .join("_candidates/nlwiki/2026-03/candidate-1")
                .exists()
        );
        assert_eq!(
            crate::generation_lifecycle::load(
                fixture.output.path(),
                "nlwiki",
                "2026-03",
                "candidate-1",
            )
            .expect("retired candidate state should load")
            .context("retired candidate state should remain")?
            .state,
            crate::generation_lifecycle::GenerationState::Retired
        );
        let selection: PublicationSelection =
            read_json(&selection_path(fixture.output.path(), "publish-2")?)?;
        assert_eq!(selection.state, "committed");
        assert!(
            !fixture
                .output
                .path()
                .join("_publication_transactions/publish-2/backups/nlwiki")
                .exists()
        );

        let page_weekly = fixture.output.path().join("page_weekly_edits.parquet");
        let page_weekly_before = fs::read(&page_weekly)?;
        let candidate_before = fs::read(fixture.output.path().join(CANDIDATE_FILE))?;
        let receipt_before = fs::read(fixture.output.path().join(RECEIPT_FILE))?;

        prepare_ready_publication(
            fixture.data.path(),
            fixture.output.path(),
            &fixture.lifecycle_path,
            "publish-noop",
        )
        .expect("unchanged candidate publication should prepare");
        let no_op: PublicationSelection =
            read_json(&selection_path(fixture.output.path(), "publish-noop")?)?;
        assert!(no_op.entries.is_empty());
        assert_eq!(no_op.state, "no_op");
        assert_eq!(fs::read(&page_weekly)?, page_weekly_before);
        assert_eq!(
            fs::read(fixture.output.path().join(CANDIDATE_FILE))?,
            candidate_before
        );
        assert_eq!(
            fs::read(fixture.output.path().join(RECEIPT_FILE))?,
            receipt_before
        );
        assert!(
            commit_ready_publication(fixture.data.path(), fixture.output.path(), "publish-noop")
                .is_err()
        );

        fs::write(&page_weekly, b"interrupted publication debris")?;
        prepare_ready_publication(
            fixture.data.path(),
            fixture.output.path(),
            &fixture.lifecycle_path,
            "publish-repair",
        )
        .expect("damaged published inventory should rebuild transactionally");
        commit_ready_publication(fixture.data.path(), fixture.output.path(), "publish-repair")
            .expect("repaired publication should commit");
        assert_ne!(fs::read(&page_weekly)?, b"interrupted publication debris");

        let candidate_2 =
            wiki_candidate_dir(fixture.output.path(), "nlwiki", "2026-03", "candidate-2")?;
        let candidate_3 =
            wiki_candidate_dir(fixture.output.path(), "nlwiki", "2026-03", "candidate-3")?;
        fs::create_dir_all(candidate_3.join("nlwiki"))?;
        for spec in &METRICS {
            fs::copy(
                candidate_2
                    .join("nlwiki")
                    .join(format!("{}.parquet", spec.name)),
                candidate_3
                    .join("nlwiki")
                    .join(format!("{}.parquet", spec.name)),
            )
            .expect("new candidate metric should copy");
        }
        let mut cloned: ReadyWikiCandidate = read_json(&candidate_2.join("ready.json"))?;
        cloned.run_id = "candidate-3".to_string();
        cloned.ready_at_unix += 1;
        atomic_json(&candidate_3.join("ready.json"), &cloned)?;
        prepare_ready_publication(
            fixture.data.path(),
            fixture.output.path(),
            &fixture.lifecycle_path,
            "publish-symlink-backup",
        )
        .expect("symlink-backed candidate should prepare");
        commit_ready_publication(
            fixture.data.path(),
            fixture.output.path(),
            "publish-symlink-backup",
        )
        .expect("symlink-backed candidate should commit");
        assert_eq!(
            crate::generation_lifecycle::load(
                fixture.output.path(),
                "nlwiki",
                "2026-03",
                "candidate-2",
            )
            .expect("rollback candidate state should load")
            .context("rollback candidate state should remain")?
            .state,
            crate::generation_lifecycle::GenerationState::Superseded
        );
        assert!(candidate_2.is_dir());
        assert_eq!(
            crate::generation_lifecycle::load(
                fixture.output.path(),
                "nlwiki",
                "2026-03",
                "candidate-3",
            )
            .expect("published candidate state should load")
            .context("published candidate state should exist")?
            .state,
            crate::generation_lifecycle::GenerationState::Published
        );
        Ok(())
    }

    #[test]
    fn candidate_discovery_and_retirement_ignore_only_well_identified_entries() -> Result<()> {
        let fixture = Fixture::new()?;
        fixture.ready_candidate("candidate-1")?;
        let mut lifecycle: Value = read_json(&fixture.lifecycle_path)?;
        lifecycle["wikis"]["pausedwiki"] = json!({
            "publication": "published",
            "refresh": "paused",
            "imported_cutoff": "2026-03"
        });
        lifecycle["wikis"]["hiddenwiki"] = json!({
            "publication": "hidden",
            "refresh": "manual"
        });
        lifecycle["wikis"]["manualwiki"] = json!({
            "publication": "published",
            "refresh": "manual"
        });
        lifecycle["wikis"]["missingrootwiki"] = json!({
            "publication": "published",
            "refresh": "manual"
        });
        atomic_json(&fixture.lifecycle_path, &lifecycle)?;

        let wiki_root = fixture.output.path().join("_candidates/nlwiki");
        fs::write(wiki_root.join("not-a-snapshot-directory"), b"ignored")?;
        let empty_snapshot = wiki_root.join("2026-02");
        fs::create_dir_all(empty_snapshot.join("run-without-ready"))?;
        fs::write(empty_snapshot.join("not-a-run-directory"), b"ignored")?;
        fs::create_dir_all(
            fixture
                .output
                .path()
                .join("_candidates/manualwiki/2026-02/run-without-ready"),
        )
        .expect("manual candidate root fixture should be writable");
        assert_eq!(
            latest_ready_candidates(
                fixture.data.path(),
                fixture.output.path(),
                &fixture.lifecycle_path,
            )
            .expect("ready discovery should succeed")
            .len(),
            1
        );
        assert!(active_candidate_target(fixture.output.path(), &"x".repeat(10_000)).is_err());
        assert_eq!(
            active_candidate_relative(fixture.output.path(), "missingwiki")?,
            None
        );
        let invalid_active = fixture.output.path().join("invalid-active");
        std::os::unix::fs::symlink("outside", &invalid_active)?;
        assert_eq!(
            active_candidate_relative(fixture.output.path(), "invalid-active")?,
            None
        );
        fs::remove_file(&invalid_active)?;
        std::os::unix::fs::symlink(
            "_candidates/nlwiki/2026-03/../candidate-1/nlwiki",
            &invalid_active,
        )
        .expect("traversal symlink fixture should be writable");
        assert_eq!(
            active_candidate_relative(fixture.output.path(), "invalid-active")?,
            None
        );
        fs::remove_file(&invalid_active)?;
        std::os::unix::fs::symlink(Path::new("/"), &invalid_active)?;
        assert_eq!(
            active_candidate_relative(fixture.output.path(), "invalid-active")?,
            None
        );
        fs::remove_file(&invalid_active)?;
        std::os::unix::fs::symlink("/outside/invalid-active", &invalid_active)?;
        assert_eq!(
            active_candidate_relative(fixture.output.path(), "invalid-active")?,
            None
        );
        fs::remove_file(&invalid_active)?;
        let absolute_candidate = fixture
            .output
            .path()
            .join("_candidates/nlwiki/2026-03/candidate-1/nlwiki");
        std::os::unix::fs::symlink(&absolute_candidate, &invalid_active)?;
        assert_eq!(
            active_candidate_relative(fixture.output.path(), "invalid-active")?,
            None
        );
        fs::remove_file(&invalid_active)?;
        assert!(selection_path(fixture.output.path(), "unsafe/run").is_err());

        let active = fixture.output.path().join("nlwiki");
        move_active_to_backup(fixture.output.path(), &active, None)?;
        restore_backup(fixture.output.path(), &active, None)?;
        restore_backup(
            fixture.output.path(),
            &active,
            Some("_publication_transactions/missing-backup"),
        )
        .expect("missing backup should be a safe no-op");
        remove_committed_backup(fixture.output.path(), None)?;
        let regular_backup = fixture.output.path().join("regular-backup");
        fs::write(&regular_backup, b"not pipeline owned")?;
        remove_committed_backup(fixture.output.path(), Some("regular-backup"))?;
        assert!(regular_backup.is_file());
        rollback_selection_files(
            fixture.data.path(),
            fixture.output.path(),
            &PublicationSelection {
                schema_version: 1,
                run_id: "direct-rollback".to_string(),
                state: "selected".to_string(),
                entries: vec![SelectionEntry {
                    wiki: "nlwiki".to_string(),
                    snapshot: "2026-03".to_string(),
                    candidate_relative: "_candidates/nlwiki/2026-03/not-active".to_string(),
                    previous_candidate_relative: None,
                    previous_snapshot: Some("2026-03".to_string()),
                    backup_relative: None,
                    workload_profile: None,
                }],
            },
        )
        .expect("rollback without a backup should be a safe no-op");

        let pointer = storage::snapshot_pointer_path(fixture.data.path(), "nlwiki");
        let pointer_temp = pointer
            .parent()
            .expect("snapshot pointer always has a parent")
            .join(format!(".current-snapshot.json.{}.tmp", std::process::id()));
        fs::create_dir(&pointer_temp).expect("pointer failure fixture should be writable");
        let failed_restore = PublicationSelection {
            schema_version: 1,
            run_id: "failed-restore".to_string(),
            state: "selected".to_string(),
            entries: vec![SelectionEntry {
                wiki: "nlwiki".to_string(),
                snapshot: "2026-03".to_string(),
                candidate_relative: "_candidates/nlwiki/2026-03/not-active".to_string(),
                previous_candidate_relative: None,
                previous_snapshot: Some("2026-03".to_string()),
                backup_relative: None,
                workload_profile: None,
            }],
        };
        assert!(
            rollback_selection_files(fixture.data.path(), fixture.output.path(), &failed_restore,)
                .is_err()
        );
        fs::remove_dir(pointer_temp).expect("pointer failure fixture should clean up");

        let missing = SelectionEntry {
            wiki: "missingwiki".to_string(),
            snapshot: "2026-03".to_string(),
            candidate_relative: "_candidates/missingwiki/2026-03/run".to_string(),
            previous_candidate_relative: None,
            previous_snapshot: None,
            backup_relative: None,
            workload_profile: None,
        };
        assert_eq!(
            retire_superseded_candidates(fixture.output.path(), &missing, "publication")?,
            0
        );
        let retire_root = fixture.output.path().join("_candidates/retirewiki");
        fs::create_dir_all(retire_root.join("2026-01/empty"))?;
        fs::create_dir_all(retire_root.join("invalid/unsafe$run"))?;
        fs::write(retire_root.join("invalid/unsafe$run/ready.json"), b"ready")?;
        fs::write(retire_root.join("not-a-directory"), b"ignored")?;
        fs::write(
            retire_root.join("2026-01/empty/not-a-candidate"),
            b"ignored",
        )
        .expect("non-candidate retirement fixture should be writable");
        fs::write(retire_root.join("2026-01/empty/ready.json"), b"ready")?;
        let retained = retire_root.join("2026-02/keep");
        fs::create_dir_all(&retained)?;
        let retirement = SelectionEntry {
            wiki: "retirewiki".to_string(),
            snapshot: "2026-02".to_string(),
            candidate_relative: "_candidates/retirewiki/2026-02/keep".to_string(),
            previous_candidate_relative: None,
            previous_snapshot: None,
            backup_relative: None,
            workload_profile: None,
        };
        assert_eq!(
            retire_superseded_candidates(fixture.output.path(), &retirement, "publication")?,
            1
        );
        let superseded = retire_root.join("2026-01/superseded");
        fs::create_dir_all(&superseded)?;
        fs::write(superseded.join("ready.json"), b"ready")?;
        crate::generation_lifecycle::adopt(
            fixture.output.path(),
            "retirewiki",
            "2026-01",
            "superseded",
            GState::Superseded,
            "superseded fixture",
        )
        .expect("superseded lifecycle fixture should be adopted");
        assert_eq!(
            retire_superseded_candidates(fixture.output.path(), &retirement, "publication")?,
            1
        );
        Ok(())
    }

    #[test]
    fn candidate_marking_supports_metric_specific_coverage() -> Result<()> {
        let fixture = Fixture::new()?;
        fixture.ready_candidate("candidate-subset")?;
        let mut lifecycle: Value = read_json(&fixture.lifecycle_path)?;
        lifecycle["publication_contract"]["datasets"]["business_funnel"] =
            json!({"wikis": ["otherwiki"], "minimum_rows_per_wiki": 1});
        lifecycle["wikis"]["otherwiki"] = json!({
            "publication": "published",
            "refresh": "paused",
            "imported_cutoff": "2026-03"
        });
        atomic_json(&fixture.lifecycle_path, &lifecycle)?;

        let ready_path = mark_wiki_candidate_ready(
            fixture.data.path(),
            fixture.output.path(),
            &fixture.lifecycle_path,
            "nlwiki",
            "2026-03",
            "candidate-subset",
        )
        .expect("metric-specific ready candidate should validate");
        let ready: ReadyWikiCandidate = read_json(&ready_path)?;
        assert!(
            !ready
                .artifacts
                .iter()
                .any(|artifact| { artifact.path.ends_with("business_funnel.parquet") })
        );
        crate::generation_lifecycle::transition(
            fixture.output.path(),
            "nlwiki",
            "2026-03",
            "candidate-subset",
            GState::Published,
            "published fixture",
            Some("publication"),
        )
        .expect("published lifecycle fixture should transition");
        assert!(
            mark_wiki_candidate_ready(
                fixture.data.path(),
                fixture.output.path(),
                &fixture.lifecycle_path,
                "nlwiki",
                "2026-03",
                "candidate-subset",
            )
            .is_err()
        );
        Ok(())
    }

    fn prepare_hidden_qualification_fixture(fixture: &Fixture, run_id: &str) {
        let mut lifecycle: Value =
            read_json(&fixture.lifecycle_path).expect("fixture lifecycle should load");
        lifecycle["wikis"]["nlwiki"] = json!({
            "publication": "hidden",
            "refresh": "qualification"
        });
        atomic_json(&fixture.lifecycle_path, &lifecycle)
            .expect("hidden qualification lifecycle should persist");
        ensure_qualification_wiki(&fixture.lifecycle_path, "nlwiki")
            .expect("hidden qualification lifecycle should validate");

        let analytical =
            storage::snapshot_analytical_wiki_dir(fixture.data.path(), "nlwiki", "2026-03")
                .expect("analytical generation path should resolve");
        write_single_i64(&analytical.join("template.parquet"))
            .expect("analytical fixture should write");
        storage::write_test_generation_manifest_from_files(
            fixture.data.path(),
            "nlwiki",
            "2026-03",
        )
        .expect("generation manifest should write");
        let (plan, _) = crate::snapshot_plan::SnapshotPlan::load_or_resolve(
            fixture.data.path(),
            "nlwiki",
            "2026-03",
        )
        .expect("snapshot plan should resolve");
        crate::workload_profile::load_or_select(
            fixture.data.path(),
            &plan,
            &vec![Some(1); plan.sources.len()],
        )
        .expect("qualification workload profile should persist");
        let qualification =
            wiki_qualification_dir(fixture.output.path(), "nlwiki", "2026-03", run_id)
                .expect("qualification path should resolve");
        let qualification_wiki = qualification.join("nlwiki");
        fs::create_dir_all(&qualification_wiki)
            .expect("qualification output directory should exist");
        for spec in &METRICS {
            fs::copy(
                fixture
                    .output
                    .path()
                    .join("nlwiki")
                    .join(format!("{}.parquet", spec.name)),
                qualification_wiki.join(format!("{}.parquet", spec.name)),
            )
            .expect("qualification metric should copy");
        }
    }

    #[test]
    fn paused_published_wiki_can_qualify_without_hiding_its_imported_baseline() {
        let fixture = Fixture::new().expect("qualification fixture should build");
        let mut lifecycle: Value =
            read_json(&fixture.lifecycle_path).expect("fixture lifecycle should load");
        lifecycle["wikis"]["nlwiki"] = json!({
            "publication": "published",
            "refresh": "paused",
            "provenance": "local-import",
            "imported_cutoff": "2026-03"
        });
        atomic_json(&fixture.lifecycle_path, &lifecycle)
            .expect("paused published lifecycle should persist");

        ensure_qualification_wiki(&fixture.lifecycle_path, "nlwiki")
            .expect("paused published wiki should be eligible for isolated qualification");

        lifecycle["wikis"]["nlwiki"]["refresh"] = json!("scheduled");
        atomic_json(&fixture.lifecycle_path, &lifecycle)
            .expect("scheduled lifecycle should persist");
        assert!(ensure_qualification_wiki(&fixture.lifecycle_path, "nlwiki").is_err());
    }

    fn block_generation_state_write(fixture: &Fixture, run_id: &str) -> PathBuf {
        let state_path = crate::generation_lifecycle::state_path(
            fixture.output.path(),
            "nlwiki",
            "2026-03",
            run_id,
        )
        .expect("generation state path should resolve");
        let blocker = state_path
            .parent()
            .expect("generation state should have a parent")
            .join(format!(
                ".{}.{pid}.tmp",
                state_path
                    .file_name()
                    .expect("generation state should have a filename")
                    .to_string_lossy(),
                pid = std::process::id()
            ));
        fs::create_dir_all(&blocker).expect("state write blocker should be created");
        blocker
    }

    #[test]
    fn hidden_qualification_receipt_is_structurally_ineligible_for_publication() {
        let fixture = Fixture::new().expect("qualification fixture should build");
        prepare_hidden_qualification_fixture(&fixture, "qualification-1");

        let receipt_path = mark_wiki_qualification_ready(
            fixture.data.path(),
            fixture.output.path(),
            &fixture.lifecycle_path,
            "nlwiki",
            "2026-03",
            "qualification-1",
        )
        .expect("qualification should become ready");
        let receipt: QualificationReceipt =
            read_json(&receipt_path).expect("qualification receipt should load");
        assert!(!receipt.publication_eligible);
        assert_eq!(receipt.artifacts.len(), METRICS.len());
        assert_eq!(
            mark_wiki_qualification_ready(
                fixture.data.path(),
                fixture.output.path(),
                &fixture.lifecycle_path,
                "nlwiki",
                "2026-03",
                "qualification-1",
            )
            .expect("ready qualification retry should be idempotent"),
            receipt_path
        );
        assert!(!fixture.output.path().join("_candidates/nlwiki").exists());
        assert!(
            latest_ready_candidates(
                fixture.data.path(),
                fixture.output.path(),
                &fixture.lifecycle_path,
            )
            .expect("publisher candidate scan should succeed")
            .is_empty()
        );
    }

    #[test]
    fn qualification_state_write_failures_never_reach_ready() {
        for (run_id, fail_after_validation) in [
            ("qualification-building-failure", false),
            ("qualification-ready-failure", true),
        ] {
            let fixture = Fixture::new().expect("qualification fixture should build");
            prepare_hidden_qualification_fixture(&fixture, run_id);
            crate::generation_lifecycle::adopt(
                fixture.output.path(),
                "nlwiki",
                "2026-03",
                run_id,
                GState::Building,
                "qualification test started",
            )
            .expect("building state should persist");
            if fail_after_validation {
                crate::generation_lifecycle::transition(
                    fixture.output.path(),
                    "nlwiki",
                    "2026-03",
                    run_id,
                    GState::Validated,
                    "qualification test validated",
                    None,
                )
                .expect("validated state should persist");
            }
            let _blocker = block_generation_state_write(&fixture, run_id);

            let error = mark_wiki_qualification_ready(
                fixture.data.path(),
                fixture.output.path(),
                &fixture.lifecycle_path,
                "nlwiki",
                "2026-03",
                run_id,
            )
            .expect_err("blocked lifecycle transition must fail qualification");
            assert!(error.to_string().contains("directory"));
            let state = crate::generation_lifecycle::load(
                fixture.output.path(),
                "nlwiki",
                "2026-03",
                run_id,
            )
            .expect("generation state should remain readable")
            .expect("generation state should exist");
            assert_eq!(
                state.state,
                if fail_after_validation {
                    GState::Validated
                } else {
                    GState::Building
                }
            );
        }
    }

    #[test]
    fn initial_candidate_commit_has_no_previous_generation() {
        let fixture = Fixture::new().expect("initial publication fixture should build");
        fixture
            .ready_candidate("initial-candidate")
            .expect("initial candidate should become ready");
        prepare_ready_publication(
            fixture.data.path(),
            fixture.output.path(),
            &fixture.lifecycle_path,
            "initial-publication",
        )
        .expect("initial candidate should prepare for publication");

        commit_ready_publication(
            fixture.data.path(),
            fixture.output.path(),
            "initial-publication",
        )
        .expect("initial candidate should commit");

        let path = selection_path(fixture.output.path(), "initial-publication")
            .expect("initial selection path should resolve");
        let selection: PublicationSelection =
            read_json(&path).expect("initial selection should remain readable");
        assert!(selection.entries[0].previous_candidate_relative.is_none());
        assert_eq!(selection.state, "committed");
    }

    #[test]
    fn activation_and_merge_failures_restore_the_previous_selection() -> Result<()> {
        let activation_fixture = Fixture::new()?;
        activation_fixture.ready_candidate("candidate-activation")?;
        let selection_temp = activation_fixture
            .output
            .path()
            .join(".nlwiki.select.activation-failure.tmp");
        fs::create_dir(&selection_temp)?;
        let activation_error = prepare_ready_publication(
            activation_fixture.data.path(),
            activation_fixture.output.path(),
            &activation_fixture.lifecycle_path,
            "activation-failure",
        )
        .expect_err("pointer failure must abort activation");
        assert!(
            format!("{activation_error:#}").contains("failed to activate ready candidates"),
            "unexpected activation error: {activation_error:#}"
        );
        assert!(!activation_fixture.output.path().join("nlwiki").is_symlink());
        fs::remove_dir(selection_temp)?;

        let merge_fixture = Fixture::new()?;
        merge_fixture.ready_candidate("candidate-merge")?;
        let root_metric = merge_fixture.output.path().join("business_funnel.parquet");
        fs::remove_file(&root_metric)?;
        fs::create_dir(&root_metric)?;
        assert!(
            prepare_ready_publication(
                merge_fixture.data.path(),
                merge_fixture.output.path(),
                &merge_fixture.lifecycle_path,
                "merge-failure",
            )
            .is_err()
        );
        assert!(!merge_fixture.output.path().join("nlwiki").is_symlink());
        let journal = selection_path(merge_fixture.output.path(), "merge-failure")
            .expect("selection journal path should be safe");
        let selection: PublicationSelection =
            read_json(&journal).expect("rolled-back selection journal should remain readable");
        assert_eq!(selection.state, "rolled_back");
        Ok(())
    }

    #[test]
    fn publication_gate_verify_accepts_a_later_standalone_run_id() -> Result<()> {
        // The on-demand wiki-econ-site Toolforge Job builds the site under its
        // own fresh run ID, independent of whichever wiki-econ-compute run
        // last validated the data. verify() must accept that: it only needs
        // context/candidate/receipt to agree with each other, not with the
        // run ID of the process calling verify.
        let fixture = Fixture::new()?;
        fixture.prepare("run-good")?;
        let validation = validate(
            fixture.data.path(),
            fixture.output.path(),
            &fixture.lifecycle_path,
            "run-good",
        );
        validation?;
        verify(fixture.output.path(), "a-later-standalone-site-run")?;

        let receipt: Value = read_json(&fixture.output.path().join(RECEIPT_FILE))?;
        assert_eq!(receipt["run_id"], "run-good");

        let receipt_path = fixture.output.path().join(RECEIPT_FILE);
        let mut mismatched_receipt = receipt.clone();
        mismatched_receipt["run_id"] = Value::String("some-other-run".to_string());
        atomic_json(&receipt_path, &mismatched_receipt)?;
        assert!(verify(fixture.output.path(), "a-later-standalone-site-run").is_err());
        atomic_json(&receipt_path, &receipt)?;
        Ok(())
    }

    #[test]
    fn publication_gate_verify_accepts_a_receipt_built_by_an_older_commit() -> Result<()> {
        // A redeployed site binary is very often a newer build than whatever
        // commit last ran publication-validate. verify() must not require the
        // receipt's recorded generating_commit to equal this process's own
        // build commit; that provenance field is informational, not a gate.
        let fixture = Fixture::new()?;
        fixture.prepare("run-good")?;
        let validation = validate(
            fixture.data.path(),
            fixture.output.path(),
            &fixture.lifecycle_path,
            "run-good",
        );
        validation?;

        let receipt_path = fixture.output.path().join(RECEIPT_FILE);
        let mut receipt: Value = read_json(&receipt_path)?;
        receipt["provenance"]["generating_commit"] =
            Value::String("stale-compute-commit".to_string());
        atomic_json(&receipt_path, &receipt)?;

        verify(fixture.output.path(), "run-good")?;
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
        assert!(validate_snapshot_cutoff("nlwiki", "2026-07", "2026-08").is_ok());
        assert!(validate_snapshot_cutoff("nlwiki", "2026-07", "2026-09").is_err());
        assert!(validate_snapshot_cutoff("nlwiki", "2026-07", "2026-04").is_err());
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
            minimum_rows_by_wiki: BTreeMap::new(),
        };
        assert!(expected_wikis(&registry, &invalid).is_err());

        let explicit = DatasetContract {
            coverage: None,
            wikis: Some(BTreeSet::from(["nlwiki".to_string()])),
            minimum_rows_per_wiki: 1,
            minimum_rows_by_wiki: BTreeMap::from([("nlwiki".to_string(), 2)]),
        };
        assert_eq!(explicit.minimum_rows("nlwiki"), 2);
        assert_eq!(explicit.minimum_rows("frwiki"), 1);
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
            requested_snapshot_versions: BTreeMap::new(),
        };
        let cutoffs = BTreeMap::from([
            ("frwiki".to_string(), "2026-03".to_string()),
            ("manualwiki".to_string(), "2026-03".to_string()),
        ]);
        let data = TestDir::new()?;
        assert!(validate_snapshots(data.path(), &registry, &context, &cutoffs)?.is_empty());
        assert!(validate_snapshots(data.path(), &registry, &context, &BTreeMap::new()).is_err());

        let analytical =
            storage::snapshot_analytical_wiki_dir(data.path(), "manualwiki", "2026-03")?;
        let warehouse = storage::snapshot_warehouse_wiki_dir(data.path(), "manualwiki", "2026-03")?;
        fs::create_dir_all(analytical)?;
        fs::create_dir_all(warehouse)?;
        storage::publish_test_snapshot_pointer(data.path(), "manualwiki", "2026-03")?;
        let manual_context = RunContext {
            schema_version: 1,
            run_id: "manual".to_string(),
            started_at_unix: now_unix()?,
            refresh_wikis: BTreeSet::from(["manualwiki".to_string()]),
            requested_snapshot_version: Some("2026-03".to_string()),
            requested_snapshot_versions: BTreeMap::from([(
                "manualwiki".to_string(),
                "2026-03".to_string(),
            )]),
        };
        assert_eq!(
            validate_snapshots(data.path(), &registry, &manual_context, &cutoffs)?
                .get("manualwiki")
                .map(String::as_str),
            Some("2026-03")
        );

        let paused_context = RunContext {
            refresh_wikis: BTreeSet::from(["frwiki".to_string()]),
            ..manual_context
        };
        assert!(validate_snapshots(data.path(), &registry, &paused_context, &cutoffs).is_err());
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
