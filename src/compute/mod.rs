//! Computation planning and scan-fusion façade.
//!
//! This module selects invalidated metric families, opens each shared history
//! partition once, and routes the bounded frame to the owning family module.
//! Metric formulas, family checkpoints, and family-specific output assembly
//! belong in `monthly`, `activity`, `lifecycle`, and `weekly`.

pub mod activity;
pub mod gdp;
pub mod inequality;
pub mod labor;
pub mod lifecycle;
pub mod monthly;
pub mod weekly;

use anyhow::{Context, Result};
use polars::prelude::*;
use serde::Serialize;
use std::collections::BTreeMap;
#[cfg(test)]
use std::collections::HashMap;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tracing::info;

#[cfg(test)]
use crate::{
    determinism,
    resource_governor::{GovernorPaths, ResourceGovernor},
};
use crate::{fingerprint, metric_registry::MetricFamily, storage, workload_profile};

#[cfg(test)]
use activity::{
    ACTIVITY_TIER_OUTPUT_COLUMNS, ActivityPeriod, activity_tier_labels, finish_activity_year,
    gdp_activity_tiers_for_period,
};
use activity::{
    activity_tiers_all_periods, finish_activity_year_cached, gdp_editor_month_frame,
    write_activity_outputs,
};
use lifecycle::{
    LifecycleCheckpoint, RegisteredState, lifecycle_full_digest, lifecycle_prefix_digest,
    load_cached_lifecycle_outputs, load_latest_lifecycle_checkpoint, store_lifecycle_outputs,
    write_lifecycle_outputs,
};
#[cfg(test)]
use lifecycle::{finalize_funnel, finalize_labor_cohorts};
#[cfg(test)]
pub(crate) use monthly::EditorIdentityCoveragePeriod;
#[cfg(test)]
use monthly::write_editor_identity_coverage;
pub(crate) use monthly::{
    EDITOR_IDENTITY_REPORT, EditorIdentityCoverageReport, read_editor_identity_coverage,
};
use monthly::{
    MonthlyFrames, editor_identity_coverage_frame, editor_identity_report_path,
    finish_inequality_year_cached, gdp_monthly_frame, gdp_type_share_frame, labor_monthly_frame,
    write_monthly_outputs,
};
#[cfg(test)]
use weekly::*;
pub(crate) use weekly::{
    ResourcePeak, WeeklyAggregationConfig, WeeklyAggregationReport, benchmark_page_weekly_edits,
};
use weekly::{
    compute_page_weekly_edits_for_snapshot_cached, compute_page_weekly_external_qualification,
};

const EDITOR_ACTOR_COLUMN: &str = "editor_actor";
pub(crate) fn snapshot_contains_complete_month(snapshot: &str, event_month: &str) -> bool {
    event_month <= snapshot
}

impl MetricFamily {
    const NONWEEKLY: [Self; 3] = [Self::Monthly, Self::ActivityTiers, Self::Lifecycle];

    fn algorithm_version(self, weekly_config: &WeeklyAggregationConfig) -> String {
        match self {
            Self::PageWeek => weekly_config.algorithm_version(),
            _ => self.base_algorithm_version().to_string(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Invalidation {
    Reuse,
    Recompute,
}

impl Invalidation {
    fn must_compute(self) -> bool {
        self == Self::Recompute
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub(crate) struct ComputePlan {
    pub(crate) monthly: Invalidation,
    pub(crate) activity_tiers: Invalidation,
    pub(crate) lifecycle: Invalidation,
    pub(crate) page_week: Invalidation,
}

impl ComputePlan {
    fn all_recompute() -> Self {
        Self {
            monthly: Invalidation::Recompute,
            activity_tiers: Invalidation::Recompute,
            lifecycle: Invalidation::Recompute,
            page_week: Invalidation::Recompute,
        }
    }

    fn invalidation(self, family: MetricFamily) -> Invalidation {
        match family {
            MetricFamily::Monthly => self.monthly,
            MetricFamily::ActivityTiers => self.activity_tiers,
            MetricFamily::Lifecycle => self.lifecycle,
            MetricFamily::PageWeek => self.page_week,
            MetricFamily::Patrol => unreachable!("patrol is not a core compute family"),
        }
    }

    fn set_invalidation(&mut self, family: MetricFamily, invalidation: Invalidation) {
        match family {
            MetricFamily::Monthly => self.monthly = invalidation,
            MetricFamily::ActivityTiers => self.activity_tiers = invalidation,
            MetricFamily::Lifecycle => self.lifecycle = invalidation,
            MetricFamily::PageWeek => self.page_week = invalidation,
            MetricFamily::Patrol => unreachable!("patrol is not a core compute family"),
        }
    }

    fn all_reused(self) -> bool {
        MetricFamily::CORE
            .into_iter()
            .all(|family| self.invalidation(family) == Invalidation::Reuse)
    }

    fn any_nonweekly(self) -> bool {
        MetricFamily::NONWEEKLY
            .into_iter()
            .any(|family| self.invalidation(family).must_compute())
    }
}

fn analytical_lazyframe(wiki: &str, data_dir: &Path) -> Result<LazyFrame> {
    let layer =
        storage::active_compute_layer(data_dir, wiki, storage::GenerationLayer::Analytical)?;
    let parquet_dir = storage::active_layer_wiki_dir(data_dir, wiki, layer)?;
    if !parquet_dir.exists() {
        anyhow::bail!("No parquet data for {wiki}. Run `ingest` first.");
    }

    let files = storage::active_fragment_files(data_dir, wiki, layer)?;
    if files.is_empty() {
        anyhow::bail!(
            "No parquet files found for {wiki} in {}",
            parquet_dir.display()
        );
    }

    let args = ScanArgsParquet {
        cache: true,
        ..Default::default()
    };
    let file_names: Vec<String> = files
        .iter()
        .map(|file| file.to_string_lossy().to_string())
        .collect();
    let parquet_files = file_names.iter().map(|file| file.as_str().into()).collect();
    LazyFrame::scan_parquet_sources(ScanSources::Paths(parquet_files), args).map_err(Into::into)
}

fn analytical_projection(df: LazyFrame, schema: &Schema) -> Result<DataFrame> {
    let has_year_month = schema.get("year_month").is_some();
    let has_year = schema.get("year").is_some();
    let has_year_month_key = schema.get("year_month_key").is_some();
    let has_user_type = schema.get("user_type").is_some();
    let has_is_reverted = schema.get("is_reverted").is_some();
    let has_is_minor = schema.get("is_minor").is_some();

    let can_filter_revision_creates =
        schema.get("event_entity").is_some() && schema.get("event_type").is_some();
    let df = if can_filter_revision_creates {
        df.filter(
            col("event_entity")
                .eq(lit("revision"))
                .and(col("event_type").eq(lit("create"))),
        )
    } else {
        df
    };

    let event_user_is_anonymous = bool_flag_expr(
        "event_user_is_anonymous",
        schema.get("event_user_is_anonymous"),
    );
    let event_user_is_temporary = bool_flag_expr(
        "event_user_is_temporary",
        schema.get("event_user_is_temporary"),
    );

    let user_type = if has_user_type {
        col("user_type")
    } else {
        user_type_col(
            event_user_is_anonymous.clone(),
            event_user_is_temporary.clone(),
        )
    };
    let historical_actor = schema.get("event_user_text_historical").is_some();
    let current_actor = schema.get("event_user_text").is_some();
    let actor_text = match (historical_actor, current_actor) {
        (true, true) => when(
            col("event_user_text_historical")
                .is_not_null()
                .and(col("event_user_text_historical").neq(lit(""))),
        )
        .then(col("event_user_text_historical"))
        .otherwise(col("event_user_text")),
        (true, false) => col("event_user_text_historical"),
        (false, true) => col("event_user_text"),
        (false, false) => lit(NULL).cast(DataType::String),
    };
    let editor_actor = if historical_actor || current_actor {
        when(col("event_user_id").is_null())
            .then(actor_text)
            .otherwise(lit(NULL).cast(DataType::String))
    } else {
        lit(NULL).cast(DataType::String)
    }
    .alias(EDITOR_ACTOR_COLUMN);

    let projected = df
        .select([
            if has_year_month {
                col("year_month")
            } else {
                year_month_col()
            },
            if has_year { col("year") } else { year_col() },
            if has_year_month_key {
                col("year_month_key")
            } else {
                year_month_key_col()
            },
            user_type,
            col("event_user_id"),
            editor_actor,
            col("page_namespace"),
            col("revision_id"),
            col("revision_text_bytes_diff"),
            if has_is_reverted {
                col("is_reverted")
            } else {
                bool_flag_expr(
                    "revision_is_identity_reverted",
                    schema.get("revision_is_identity_reverted"),
                )
                .alias("is_reverted")
            },
            if has_is_minor {
                col("is_minor")
            } else {
                bool_flag_expr("revision_minor_edit", schema.get("revision_minor_edit"))
                    .alias("is_minor")
            },
        ])
        .collect()?;
    Ok(projected)
}

/// A MediaWiki editor identity is either a user-table ID (permanent,
/// temporary, and most bots) or the historical actor text when no user-table
/// row exists (legacy IP actors). The actor text remains an internal grouping
/// key and is never written to a metric output.
pub(super) fn editor_identity_expr() -> Expr {
    as_struct(vec![col("event_user_id"), col(EDITOR_ACTOR_COLUMN)])
}

pub(super) fn editor_identity_available_expr() -> Expr {
    col("event_user_id")
        .is_not_null()
        .or(col(EDITOR_ACTOR_COLUMN)
            .is_not_null()
            .and(col(EDITOR_ACTOR_COLUMN).neq(lit(""))))
}

fn unique_identified_editors_expr() -> Expr {
    editor_identity_expr()
        .filter(editor_identity_available_expr())
        .n_unique()
}

pub(super) fn ensure_editor_identity_inputs(frame: &DataFrame) -> Result<DataFrame> {
    if frame.column(EDITOR_ACTOR_COLUMN).is_ok() {
        return Ok(frame.clone());
    }
    let projected = frame
        .clone()
        .lazy()
        .with_column(lit(NULL).cast(DataType::String).alias(EDITOR_ACTOR_COLUMN))
        .collect()?;
    Ok(projected)
}

fn ensure_editor_identity_key(frame: &DataFrame) -> Result<DataFrame> {
    if frame.column("editor_identity").is_ok() {
        return Ok(frame.clone());
    }
    ensure_editor_identity_inputs(frame)?
        .lazy()
        .filter(editor_identity_available_expr())
        .with_column(editor_identity_expr().alias("editor_identity"))
        .collect()
        .map_err(Into::into)
}

/// Load the minimal base dataset for metric computation into memory once.
pub fn load_wiki(wiki: &str, data_dir: &Path) -> Result<DataFrame> {
    let df = analytical_lazyframe(wiki, data_dir)?;
    let schema = df.clone().collect_schema()?;

    let started = Instant::now();
    let df = analytical_projection(df, &schema)?;

    info!(
        wiki = wiki,
        rows = df.height(),
        columns = df.width(),
        elapsed_ms = started.elapsed().as_secs_f64() * 1_000.0,
        "loaded base dataset"
    );

    Ok(df)
}

fn load_partition(files: &[PathBuf]) -> Result<DataFrame> {
    let args = ScanArgsParquet {
        cache: true,
        ..Default::default()
    };
    let file_names: Vec<String> = files
        .iter()
        .map(|file| file.to_string_lossy().to_string())
        .collect();
    let parquet_files = file_names.iter().map(|file| file.as_str().into()).collect();
    let df = LazyFrame::scan_parquet_sources(ScanSources::Paths(parquet_files), args)?;
    let schema = df.clone().collect_schema()?;
    analytical_projection(df, &schema)
}

fn warehouse_lazyframe(files: &[PathBuf]) -> Result<LazyFrame> {
    let args = ScanArgsParquet {
        cache: true,
        ..Default::default()
    };
    let file_names: Vec<String> = files
        .iter()
        .map(|file| file.to_string_lossy().to_string())
        .collect();
    let parquet_files = file_names.iter().map(|file| file.as_str().into()).collect();
    let lf = LazyFrame::scan_parquet_sources(ScanSources::Paths(parquet_files), args)?.select([
        col("event_timestamp"),
        col("page_id"),
        col("page_title"),
        col("page_namespace"),
    ]);
    Ok(lf)
}

/// Extract year-month string from event_timestamp (format: YYYY-MM-DD HH:MM:SS.0)
pub fn year_month_col() -> Expr {
    col("event_timestamp")
        .str()
        .slice(lit(0), lit(7))
        .alias("year_month")
}

/// Extract year string
pub fn year_col() -> Expr {
    col("event_timestamp")
        .str()
        .slice(lit(0), lit(4))
        .cast(DataType::Int32)
        .alias("year")
}

/// Extract year-month key as YYYYMM integer.
pub fn year_month_key_col() -> Expr {
    (col("event_timestamp")
        .str()
        .slice(lit(0), lit(4))
        .cast(DataType::Int32)
        * lit(100_i32)
        + col("event_timestamp")
            .str()
            .slice(lit(5), lit(2))
            .cast(DataType::Int32))
    .alias("year_month_key")
}

/// Categorize user: "bot", "anonymous", "temporary", or "registered".
pub fn user_type_col(event_user_is_anonymous: Expr, event_user_is_temporary: Expr) -> Expr {
    when(
        col("event_user_is_bot_by")
            .is_not_null()
            .and(col("event_user_is_bot_by").neq(lit(""))),
    )
    .then(lit("bot"))
    .when(event_user_is_anonymous)
    .then(lit("anonymous"))
    .when(event_user_is_temporary)
    .then(lit("temporary"))
    .otherwise(lit("registered"))
    .alias("user_type")
}

/// Assign one deterministic classification to an identity within a period.
/// Bot status takes precedence because it is an account property that may be
/// observed on only part of an account's history; the remaining identity
/// classes are mutually exclusive in normal MediaWiki data.
pub(super) fn user_type_rank_expr() -> Expr {
    when(col("user_type").eq(lit("bot")))
        .then(lit(3_i32))
        .when(col("user_type").eq(lit("temporary")))
        .then(lit(2_i32))
        .when(col("user_type").eq(lit("anonymous")))
        .then(lit(1_i32))
        .otherwise(lit(0_i32))
}

pub(super) fn user_type_from_rank_expr() -> Expr {
    when(col("user_type_rank").eq(lit(3_i32)))
        .then(lit("bot"))
        .when(col("user_type_rank").eq(lit(2_i32)))
        .then(lit("temporary"))
        .when(col("user_type_rank").eq(lit(1_i32)))
        .then(lit("anonymous"))
        .otherwise(lit("registered"))
        .alias("user_type")
}

fn bool_flag_expr(column: &str, dtype: Option<&DataType>) -> Expr {
    match dtype {
        Some(DataType::Boolean) => col(column),
        _ => col(column).eq(lit("true")),
    }
}

fn concat_frames(mut frames: Vec<DataFrame>) -> Result<DataFrame> {
    let Some(mut first) = frames.pop() else {
        return Ok(DataFrame::empty());
    };
    for frame in frames {
        first.vstack_mut(&frame)?;
    }
    Ok(first)
}

fn add_wiki_column(df: &mut DataFrame, wiki: &str) -> Result<()> {
    df.with_column(Column::new("wiki".into(), vec![wiki; df.height()]))?;
    Ok(())
}

fn sort_frame<const N: usize>(df: DataFrame, columns: [&str; N]) -> Result<DataFrame> {
    df.sort(columns, SortMultipleOptions::default())
        .map_err(Into::into)
}

struct PendingOutput {
    final_path: PathBuf,
    temp_path: PathBuf,
    published: bool,
}

impl PendingOutput {
    fn new(final_path: PathBuf) -> Result<Self> {
        final_path.parent().map(fs::create_dir_all).transpose()?;
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let file_name = final_path
            .file_name()
            .and_then(|name| name.to_str())
            .context("output path has no UTF-8 file name")?;
        let temp_path =
            final_path.with_file_name(format!(".{file_name}.{}-{nonce}.tmp", std::process::id()));
        Ok(Self {
            final_path,
            temp_path,
            published: false,
        })
    }

    fn publish(mut self) -> Result<u64> {
        File::open(&self.temp_path)?.sync_all()?;
        let bytes = fs::metadata(&self.temp_path)?.len();
        fs::rename(&self.temp_path, &self.final_path)?;
        let parent = self
            .final_path
            .parent()
            .expect("pending output always has a validated parent directory");
        File::open(parent)?.sync_all()?;
        self.published = true;
        Ok(bytes)
    }
}

impl Drop for PendingOutput {
    fn drop(&mut self) {
        if !self.published {
            let _ = fs::remove_file(&self.temp_path);
        }
    }
}

/// Write a DataFrame to parquet in the output directory.
pub fn write_output(df: &mut DataFrame, wiki: &str, metric: &str, output_dir: &Path) -> Result<()> {
    let wiki_dir = output_dir.join(wiki);
    let path = wiki_dir.join(format!("{metric}.parquet"));
    let started = Instant::now();
    let mut semantics = crate::artifact_receipt::SemanticAccumulator::new(
        crate::artifact_receipt::SemanticSpec::for_identity(&format!("{metric}.parquet")),
    );
    semantics.observe(df)?;
    let pending = PendingOutput::new(path.clone())?;
    let mut file = File::create(&pending.temp_path)?;
    ParquetWriter::new(&mut file)
        .with_compression(ParquetCompression::Zstd(None))
        .finish(df)?;
    drop(file);
    let bytes = pending.publish()?;
    crate::artifact_receipt::write_semantic_draft(&path, semantics)?;
    info!(
        wiki = wiki,
        metric = metric,
        rows = df.height(),
        columns = df.width(),
        bytes = bytes,
        elapsed_ms = started.elapsed().as_secs_f64() * 1_000.0,
        path = %path.display(),
        "wrote metric output"
    );
    Ok(())
}

#[cfg(test)]
fn compute_all_incremental(
    wiki: &str,
    data_dir: &Path,
    output_dir: &Path,
    snapshot: Option<&str>,
    plan: ComputePlan,
) -> Result<usize> {
    compute_all_incremental_cached(wiki, data_dir, output_dir, snapshot, plan, None)
}

fn compute_all_incremental_cached(
    wiki: &str,
    data_dir: &Path,
    output_dir: &Path,
    snapshot: Option<&str>,
    plan: ComputePlan,
    cross_snapshot: Option<&crate::cross_snapshot::CrossSnapshotCache>,
) -> Result<usize> {
    if !plan.any_nonweekly() {
        return Ok(0);
    }
    let mut partitions = match snapshot {
        Some(snapshot) => {
            let layer_result = storage::snapshot_compute_layer(
                data_dir,
                wiki,
                snapshot,
                storage::GenerationLayer::Analytical,
            );
            let layer = layer_result?;
            let result = storage::snapshot_partition_specs(data_dir, wiki, snapshot, layer);
            result?
        }
        None => {
            let layer_result =
                storage::active_compute_layer(data_dir, wiki, storage::GenerationLayer::Analytical);
            let layer = layer_result?;
            storage::active_partition_specs(data_dir, wiki, layer)?
        }
    };
    if let Some(snapshot) = snapshot {
        partitions
            .retain(|partition| snapshot_contains_complete_month(snapshot, &partition.year_month));
    }
    if partitions.is_empty() {
        anyhow::ensure!(
            snapshot.is_none(),
            "snapshot generation contains no analytical partitions"
        );
        let base = load_wiki(wiki, data_dir)?;
        compute_nonweekly_flat(wiki, &base, output_dir, plan)?;
        return Ok(1);
    }

    let mut inequality_frames = Vec::new();
    let mut inequality_editor_month_frames = Vec::new();
    let mut inequality_month_digests = Vec::new();
    let mut inequality_year = None;
    let mut gdp_frames = Vec::new();
    let mut gdp_type_frames = Vec::new();
    let mut identity_coverage_frames = Vec::new();
    let mut gdp_tier_frames = Vec::new();
    let mut gdp_editor_month_frames = Vec::new();
    let mut gdp_activity_month_digests = Vec::new();
    let mut gdp_activity_year = None;
    let mut labor_monthly_frames = Vec::new();
    let lifecycle_input_digest = if plan.lifecycle.must_compute() {
        cross_snapshot
            .map(|cache| lifecycle_full_digest(cache, &partitions))
            .transpose()?
    } else {
        None
    };
    let mut cached_lifecycle_outputs = if let (Some(cache), Some(input_digest)) =
        (cross_snapshot, lifecycle_input_digest.as_deref())
    {
        load_cached_lifecycle_outputs(cache, input_digest)?
    } else {
        None
    };
    let lifecycle_checkpoint =
        if plan.lifecycle.must_compute() && cached_lifecycle_outputs.is_none() {
            cross_snapshot
                .map(|cache| load_latest_lifecycle_checkpoint(cache, &partitions))
                .transpose()?
                .flatten()
        } else {
            None
        };
    let lifecycle_resume_through = lifecycle_checkpoint
        .as_ref()
        .map(|checkpoint| checkpoint.through_month.clone());
    let mut registered_state =
        (plan.lifecycle.must_compute() && cached_lifecycle_outputs.is_none()).then(|| {
            lifecycle_checkpoint
                .map(LifecycleCheckpoint::into_state)
                .unwrap_or_else(RegisteredState::new)
        });
    let mut lifecycle_month_digests = Vec::new();

    let partition_count = partitions.len();
    for partition in partitions {
        if plan.monthly.must_compute()
            && let Some(current_year) = inequality_year
        {
            anyhow::ensure!(
                partition.year >= current_year,
                "analytical partitions are not ordered chronologically"
            );
            if partition.year != current_year {
                let finish_result = finish_inequality_year_cached(
                    &mut inequality_editor_month_frames,
                    &mut inequality_frames,
                    &mut inequality_month_digests,
                    cross_snapshot,
                );
                finish_result?;
            }
        }
        if plan.monthly.must_compute() {
            inequality_year = Some(partition.year);
        }
        if plan.activity_tiers.must_compute()
            && let Some(current_year) = gdp_activity_year
        {
            anyhow::ensure!(
                partition.year >= current_year,
                "analytical partitions are not ordered chronologically"
            );
            if partition.year != current_year {
                let finish_result = finish_activity_year_cached(
                    &mut gdp_editor_month_frames,
                    &mut gdp_tier_frames,
                    &mut gdp_activity_month_digests,
                    cross_snapshot,
                );
                finish_result?;
            }
        }
        if plan.activity_tiers.must_compute() {
            gdp_activity_year = Some(partition.year);
        }
        let base = load_partition(&partition.files)?;
        if plan.lifecycle.must_compute()
            && let Some(cache) = cross_snapshot
        {
            lifecycle_month_digests.push(cache.month_digest(&partition.year_month)?.to_string());
        }
        let year_month_key = partition
            .year_month
            .split_once('-')
            .map(|(year, month): (&str, &str)| {
                let year: i32 = year.parse().expect("partition year should be numeric");
                let month: i32 = month.parse().expect("partition month should be numeric");
                year * 100 + month
            })
            .context("invalid partition year_month format")?;

        if plan.monthly.must_compute() {
            let input_digest = cross_snapshot
                .map(|cache| cache.month_digest(&partition.year_month))
                .transpose()?;
            let inequality_editor_month = cached_or_compute(
                cross_snapshot,
                "inequality_editor_month",
                monthly::ALGORITHM_VERSION,
                input_digest,
                "editor_month",
                || inequality::editor_month_frame(&base),
            );
            inequality_editor_month_frames.push(inequality_editor_month?);
            if let Some(input_digest) = input_digest {
                inequality_month_digests.push(input_digest.to_string());
            }
            let gdp = cached_or_compute(
                cross_snapshot,
                "monthly",
                monthly::ALGORITHM_VERSION,
                input_digest,
                "gdp",
                || gdp_monthly_frame(&base),
            );
            gdp_frames.push(gdp?);
            let gdp_type = cached_or_compute(
                cross_snapshot,
                "monthly",
                monthly::ALGORITHM_VERSION,
                input_digest,
                "gdp_user_type_share",
                || gdp_type_share_frame(&base),
            );
            gdp_type_frames.push(gdp_type?);
            identity_coverage_frames.push(editor_identity_coverage_frame(&base)?);
            let labor_monthly = cached_or_compute(
                cross_snapshot,
                "monthly",
                monthly::ALGORITHM_VERSION,
                input_digest,
                "labor_monthly",
                || labor_monthly_frame(&base),
            );
            labor_monthly_frames.push(labor_monthly?);
        }
        if plan.activity_tiers.must_compute() {
            let input_digest = cross_snapshot
                .map(|cache| cache.month_digest(&partition.year_month))
                .transpose()?;
            let editor_month = cached_or_compute(
                cross_snapshot,
                "editor_month",
                activity::ALGORITHM_VERSION,
                input_digest,
                "editor_month",
                || gdp_editor_month_frame(&base),
            );
            gdp_editor_month_frames.push(editor_month?);
            if let Some(input_digest) = input_digest {
                gdp_activity_month_digests.push(input_digest.to_string());
            }
        }
        if let Some(state) = registered_state.as_mut()
            && lifecycle_resume_through
                .as_deref()
                .is_none_or(|through| partition.year_month.as_str() > through)
        {
            state.observe_partition(&base, partition.year, year_month_key)?;
        }
        if partition.year_month.ends_with("-12")
            && lifecycle_resume_through
                .as_deref()
                .is_none_or(|through| partition.year_month.as_str() > through)
            && let (Some(cache), Some(state)) = (cross_snapshot, registered_state.as_ref())
        {
            let prefix = lifecycle_prefix_digest(cache, &lifecycle_month_digests);
            let checkpoint = LifecycleCheckpoint::from_state(state, &partition.year_month, &prefix);
            let checkpoint_result = cache.store_json(
                "lifecycle_checkpoint",
                lifecycle::ALGORITHM_VERSION,
                &prefix,
                "state",
                &checkpoint,
            );
            checkpoint_result?;
        }
        for file in &partition.files {
            storage::discard_path_cache(file);
        }
    }
    if plan.monthly.must_compute() {
        let finish_result = finish_inequality_year_cached(
            &mut inequality_editor_month_frames,
            &mut inequality_frames,
            &mut inequality_month_digests,
            cross_snapshot,
        );
        finish_result?;
        let result = write_monthly_outputs(
            wiki,
            snapshot,
            output_dir,
            MonthlyFrames {
                inequality_frames,
                gdp_frames,
                gdp_type_frames,
                identity_coverage_frames,
                labor_monthly_frames,
            },
        );
        result.context("failed to write partitioned monthly-family outputs")?;
    }
    if plan.activity_tiers.must_compute() {
        let finish_result = finish_activity_year_cached(
            &mut gdp_editor_month_frames,
            &mut gdp_tier_frames,
            &mut gdp_activity_month_digests,
            cross_snapshot,
        );
        finish_result?;
        write_activity_outputs(wiki, output_dir, gdp_tier_frames)?;
    }
    if let Some(outputs) = cached_lifecycle_outputs.as_mut() {
        for (metric, frame) in outputs {
            write_output(frame, wiki, metric, output_dir)?;
        }
    } else if let Some(state) = registered_state {
        write_lifecycle_outputs(wiki, output_dir, state)?;
        if let (Some(cache), Some(input_digest)) =
            (cross_snapshot, lifecycle_input_digest.as_deref())
        {
            store_lifecycle_outputs(cache, input_digest, wiki, output_dir)?;
        }
    }

    Ok(partition_count)
}

fn cached_or_compute<F>(
    cache: Option<&crate::cross_snapshot::CrossSnapshotCache>,
    kind: &str,
    algorithm_version: &str,
    input_digest: Option<&str>,
    artifact: &str,
    compute: F,
) -> Result<DataFrame>
where
    F: FnOnce() -> Result<DataFrame>,
{
    let Some(cache) = cache else {
        return compute();
    };
    let input_digest = input_digest.context("cross-snapshot cache has no input digest")?;
    if let Some(frame) = cache.load(kind, algorithm_version, input_digest, artifact)? {
        return Ok(frame);
    }
    let mut frame = compute()?;
    cache.store(kind, algorithm_version, input_digest, artifact, &mut frame)?;
    Ok(frame)
}

fn compute_nonweekly_flat(
    wiki: &str,
    base: &DataFrame,
    output_dir: &Path,
    plan: ComputePlan,
) -> Result<()> {
    if plan.monthly.must_compute() {
        let result = write_monthly_outputs(
            wiki,
            None,
            output_dir,
            MonthlyFrames {
                inequality_frames: vec![inequality::compute_frame(base)?],
                gdp_frames: vec![gdp_monthly_frame(base)?],
                gdp_type_frames: vec![gdp_type_share_frame(base)?],
                identity_coverage_frames: vec![editor_identity_coverage_frame(base)?],
                labor_monthly_frames: vec![labor_monthly_frame(base)?],
            },
        );
        result.context("failed to write flat monthly-family outputs")?;
    }
    if plan.activity_tiers.must_compute() {
        let result = write_activity_outputs(
            wiki,
            output_dir,
            vec![activity_tiers_all_periods(base.clone())?],
        );
        result.context("failed to write flat activity-tier outputs")?;
    }
    if plan.lifecycle.must_compute() {
        let mut state = RegisteredState::new();
        state.observe_history(base)?;
        write_lifecycle_outputs(wiki, output_dir, state)?;
    }
    Ok(())
}

pub(crate) fn compute_stage_inputs(
    wiki: &str,
    data_dir: &Path,
    selected_snapshot: Option<&str>,
) -> Result<Vec<fingerprint::TrackedPath>> {
    let snapshot = match selected_snapshot {
        Some(snapshot) => Some(snapshot.to_string()),
        None => storage::current_snapshot_version(data_dir, wiki)?,
    };
    if let Some(snapshot) = snapshot {
        // Resolving the fragments validates (and, for a pre-manifest
        // generation, safely migrates) the authoritative allowlist.
        storage::ensure_generation_manifest(data_dir, wiki, &snapshot)?;
        let manifest = storage::generation_manifest_path(data_dir, wiki, &snapshot)?;
        let mut generation_outputs = vec![fingerprint::TrackedPath::new(
            "generation-manifest",
            manifest,
        )];
        let compaction = crate::compaction::manifest_path(data_dir, wiki, &snapshot)?;
        if compaction.is_file() {
            generation_outputs.push(fingerprint::TrackedPath::new(
                "compaction-manifest",
                compaction,
            ));
        }
        let receipt = fingerprint::data_stage_receipt_path(data_dir, wiki, &snapshot, "ingest");
        let spec = fingerprint::StageSpec {
            stage: "ingest",
            scope: wiki,
            selected_snapshot: Some(&snapshot),
            algorithm_version: crate::ingest::INGEST_ALGORITHM_VERSION,
        };
        let mut inputs = if fingerprint::outputs_reusable(&receipt, spec, &generation_outputs)? {
            vec![fingerprint::TrackedPath::new(
                format!("stage/ingest/{wiki}/{snapshot}"),
                receipt,
            )]
        } else {
            generation_outputs
        };
        inputs.sort_by(|left, right| left.identity.cmp(&right.identity));
        return Ok(inputs);
    }

    let analytical = storage::active_analytical_wiki_dir(data_dir, wiki)?;
    let warehouse = storage::active_warehouse_wiki_dir(data_dir, wiki)?;
    let mut generation_outputs = Vec::new();
    for (prefix, root) in [("analytical", &analytical), ("warehouse", &warehouse)] {
        for path in storage::collect_parquet_files(root)? {
            let relative = path.strip_prefix(root)?;
            generation_outputs.push(fingerprint::TrackedPath::new(
                format!("{prefix}/{}", relative.to_string_lossy()),
                path,
            ));
        }
    }
    let mut inputs = generation_outputs;
    inputs.sort_by(|left, right| left.identity.cmp(&right.identity));
    Ok(inputs)
}

fn family_inputs(
    family: MetricFamily,
    wiki: &str,
    data_dir: &Path,
    snapshot: Option<&str>,
) -> Result<Vec<fingerprint::TrackedPath>> {
    let mut inputs = compute_stage_inputs(wiki, data_dir, snapshot)?;
    if family == MetricFamily::PageWeek
        && let Some(snapshot) = snapshot
    {
        let profile = workload_profile::profile_path(data_dir, wiki, snapshot)?;
        if profile.is_file() {
            inputs.push(fingerprint::TrackedPath::new(
                format!("workload-profile/{wiki}/{snapshot}"),
                profile,
            ));
        }
    }
    inputs.sort_by(|left, right| left.identity.cmp(&right.identity));
    Ok(inputs)
}

fn legacy_compute_inputs(
    wiki: &str,
    data_dir: &Path,
    snapshot: Option<&str>,
) -> Result<Vec<fingerprint::TrackedPath>> {
    let mut inputs = compute_stage_inputs(wiki, data_dir, snapshot)?;
    if let Some(snapshot) = snapshot {
        let profile = workload_profile::profile_path(data_dir, wiki, snapshot)?;
        if profile.is_file() {
            inputs.push(fingerprint::TrackedPath::new(
                format!("workload-profile/{wiki}/{snapshot}"),
                profile,
            ));
        }
    }
    inputs.sort_by(|left, right| left.identity.cmp(&right.identity));
    Ok(inputs)
}

fn family_outputs(
    family: MetricFamily,
    wiki: &str,
    output_dir: &Path,
) -> Vec<fingerprint::TrackedPath> {
    let mut outputs = family
        .metrics()
        .iter()
        .map(|metric| {
            fingerprint::TrackedPath::new(
                format!("output/{wiki}/{metric}.parquet"),
                output_dir.join(wiki).join(format!("{metric}.parquet")),
            )
        })
        .collect::<Vec<_>>();
    if family == MetricFamily::Monthly {
        outputs.push(fingerprint::TrackedPath::new(
            format!("output/{wiki}/{EDITOR_IDENTITY_REPORT}"),
            editor_identity_report_path(output_dir, wiki),
        ));
    }
    outputs
}

fn compute_stage_outputs(wiki: &str, output_dir: &Path) -> Vec<fingerprint::TrackedPath> {
    MetricFamily::CORE
        .into_iter()
        .flat_map(|family| family_outputs(family, wiki, output_dir))
        .filter(|output| output.path.is_file())
        .collect()
}

fn legacy_compute_stage_receipt(output_dir: &Path, wiki: &str) -> PathBuf {
    output_dir
        .join("_stages")
        .join("compute")
        .join(format!("{wiki}.json"))
}

fn family_stage_receipt(output_dir: &Path, wiki: &str, family: MetricFamily) -> PathBuf {
    output_dir
        .join("_stages")
        .join("compute")
        .join(family.name())
        .join(format!("{wiki}.json"))
}

fn family_stage_spec<'a>(
    family: MetricFamily,
    wiki: &'a str,
    snapshot: Option<&'a str>,
    algorithm_version: &'a str,
) -> fingerprint::StageSpec<'a> {
    fingerprint::StageSpec {
        stage: match family {
            MetricFamily::Monthly => "compute_monthly",
            MetricFamily::ActivityTiers => "compute_activity_tiers",
            MetricFamily::Lifecycle => "compute_lifecycle",
            MetricFamily::PageWeek => "compute_page_week",
            MetricFamily::Patrol => unreachable!("patrol has its own compute stage"),
        },
        scope: wiki,
        selected_snapshot: snapshot,
        algorithm_version,
    }
}

fn migrate_legacy_compute_receipt(
    wiki: &str,
    snapshot: &str,
    data_dir: &Path,
    output_dir: &Path,
    weekly_config: &WeeklyAggregationConfig,
) -> Result<()> {
    let legacy_path = legacy_compute_stage_receipt(output_dir, wiki);
    if !legacy_path.is_file() {
        return Ok(());
    }
    let legacy_inputs = legacy_compute_inputs(wiki, data_dir, Some(snapshot))?;
    let legacy_outputs = compute_stage_outputs(wiki, output_dir);
    let legacy_algorithm = weekly_config.legacy_algorithm_version();
    let legacy_spec = fingerprint::StageSpec {
        stage: "compute",
        scope: wiki,
        selected_snapshot: Some(snapshot),
        algorithm_version: &legacy_algorithm,
    };
    if !fingerprint::reusable(&legacy_path, legacy_spec, &legacy_inputs, &legacy_outputs)? {
        return Ok(());
    }
    let source = fingerprint::read_receipt(&legacy_path)?;
    // Monthly outputs gained a total user-type tie-break order with the family
    // split, so they must be rebuilt once. The remaining families are byte-
    // compatible with the authenticated legacy core receipt.
    for family in [
        MetricFamily::ActivityTiers,
        MetricFamily::Lifecycle,
        MetricFamily::PageWeek,
    ] {
        let algorithm = family.algorithm_version(weekly_config);
        let receipt_path = family_stage_receipt(output_dir, wiki, family);
        let inputs = family_inputs(family, wiki, data_dir, Some(snapshot))?;
        let outputs = family_outputs(family, wiki, output_dir);
        let spec = family_stage_spec(family, wiki, Some(snapshot), &algorithm);
        if !fingerprint::reusable(&receipt_path, spec, &inputs, &outputs)? {
            fingerprint::record_from_verified_receipt(
                &receipt_path,
                spec,
                &inputs,
                &outputs,
                &source,
            )?;
        }
    }
    info!(
        wiki,
        snapshot, "split legacy core compute receipt into metric families"
    );
    Ok(())
}

fn family_is_reusable(
    family: MetricFamily,
    wiki: &str,
    snapshot: Option<&str>,
    data_dir: &Path,
    output_dir: &Path,
    weekly_config: &WeeklyAggregationConfig,
) -> Result<bool> {
    let algorithm = family.algorithm_version(weekly_config);
    let inputs = family_inputs(family, wiki, data_dir, snapshot)?;
    let outputs = family_outputs(family, wiki, output_dir);
    fingerprint::reusable(
        &family_stage_receipt(output_dir, wiki, family),
        family_stage_spec(family, wiki, snapshot, &algorithm),
        &inputs,
        &outputs,
    )
}

pub(crate) fn reusable_candidate_families(
    wiki: &str,
    snapshot: &str,
    data_dir: &Path,
    candidate_dir: &Path,
) -> Result<Vec<(MetricFamily, Vec<PathBuf>)>> {
    storage::validate_snapshot_version(snapshot)?;
    let profile_exists = workload_profile::load(data_dir, wiki, snapshot)?.is_some();
    if !profile_exists {
        info!(
            wiki,
            snapshot, "page-week candidate cannot be reused before workload profile selection"
        );
    }
    let weekly_config = if profile_exists {
        WeeklyAggregationConfig::for_snapshot(data_dir, wiki, Some(snapshot))?
    } else {
        WeeklyAggregationConfig::from_environment()?
    };
    if profile_exists {
        migrate_legacy_compute_receipt(wiki, snapshot, data_dir, candidate_dir, &weekly_config)?;
    }
    let mut reusable = Vec::new();
    for family in MetricFamily::CORE {
        if family == MetricFamily::PageWeek && !profile_exists {
            continue;
        }
        if !family_is_reusable(
            family,
            wiki,
            Some(snapshot),
            data_dir,
            candidate_dir,
            &weekly_config,
        )? {
            continue;
        }
        let mut files = family_outputs(family, wiki, candidate_dir)
            .into_iter()
            .flat_map(|output| {
                let receipt = (output.path.extension().and_then(|value| value.to_str())
                    == Some("parquet"))
                .then(|| crate::artifact_receipt::sidecar_path(&output.path).ok())
                .flatten();
                std::iter::once(output.path).chain(receipt)
            })
            .collect::<Vec<_>>();
        files.push(family_stage_receipt(candidate_dir, wiki, family));
        reusable.push((family, files));
    }
    Ok(reusable)
}

/// Authenticate same-snapshot outputs after redownloadable metric input has
/// been deliberately purged. The stage receipt still commits to the original
/// input identities; this path verifies its envelope, current algorithm, and
/// every concrete output without pretending the absent inputs remain reusable
/// for a new computation.
pub(crate) fn candidate_receipts_current_without_inputs(
    wiki: &str,
    snapshot: &str,
    candidate_dir: &Path,
    profile: Option<&workload_profile::WorkloadProfile>,
) -> Result<bool> {
    storage::validate_snapshot_version(snapshot)?;
    let Some(profile) = profile else {
        return Ok(false);
    };
    profile.validate(wiki, snapshot)?;
    let weekly_config = WeeklyAggregationConfig::from_workload_profile(profile)?;
    for family in MetricFamily::CORE {
        let algorithm = family.algorithm_version(&weekly_config);
        let outputs = family_outputs(family, wiki, candidate_dir);
        let outputs_reusable = fingerprint::outputs_reusable(
            &family_stage_receipt(candidate_dir, wiki, family),
            family_stage_spec(family, wiki, Some(snapshot), &algorithm),
            &outputs,
        );
        if !outputs_reusable? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn compute_plan(
    wiki: &str,
    snapshot: Option<&str>,
    data_dir: &Path,
    output_dir: &Path,
    weekly_config: &WeeklyAggregationConfig,
) -> Result<ComputePlan> {
    let mut plan = ComputePlan::all_recompute();
    for family in MetricFamily::CORE {
        let invalidation =
            if family_is_reusable(family, wiki, snapshot, data_dir, output_dir, weekly_config)? {
                Invalidation::Reuse
            } else {
                Invalidation::Recompute
            };
        plan.set_invalidation(family, invalidation);
    }
    Ok(plan)
}

pub(crate) fn family_receipt_identities(
    wiki: &str,
    snapshot: &str,
    data_dir: &Path,
    candidate_dir: &Path,
) -> Result<BTreeMap<String, String>> {
    let reusable = reusable_candidate_families(wiki, snapshot, data_dir, candidate_dir)?;
    let reusable_names = reusable
        .iter()
        .map(|(family, _)| family.name())
        .collect::<Vec<_>>();
    let missing_names = MetricFamily::CORE
        .into_iter()
        .filter(|family| !reusable_names.contains(&family.name()))
        .map(MetricFamily::name)
        .collect::<Vec<_>>();
    anyhow::ensure!(
        reusable.len() == MetricFamily::CORE.len(),
        "candidate does not have a complete reusable compute-family receipt set; missing {}",
        missing_names.join(", ")
    );
    MetricFamily::CORE
        .into_iter()
        .map(|family| {
            let receipt =
                fingerprint::read_receipt(&family_stage_receipt(candidate_dir, wiki, family))?;
            Ok((family.name().to_string(), receipt.fingerprint))
        })
        .collect()
}

pub(crate) fn family_receipt_algorithms(
    wiki: &str,
    snapshot: &str,
    data_dir: &Path,
    candidate_dir: &Path,
) -> Result<BTreeMap<String, String>> {
    family_receipt_identities(wiki, snapshot, data_dir, candidate_dir)?;
    MetricFamily::CORE
        .into_iter()
        .map(|family| {
            let receipt =
                fingerprint::read_receipt(&family_stage_receipt(candidate_dir, wiki, family))?;
            Ok((family.name().to_string(), receipt.algorithm_version))
        })
        .collect()
}

#[cfg(test)]
pub(crate) fn record_candidate_fingerprint_for_test(
    wiki: &str,
    snapshot: &str,
    data_dir: &Path,
    candidate_dir: &Path,
) -> Result<()> {
    let report_path = editor_identity_report_path(candidate_dir, wiki);
    if !report_path.is_file() {
        let gdp_share = ParquetReader::new(
            File::open(candidate_dir.join(wiki).join("gdp_user_type_share.parquet"))
                .expect("candidate GDP share fixture should exist"),
        )
        .finish()?;
        let coverage = gdp_share
            .lazy()
            .select([
                col("year_month"),
                col("user_type"),
                col("edits").alias("total_edits"),
                col("edits").alias("identified_edits"),
                lit(0_i32).cast(DataType::UInt32).alias("excluded_edits"),
            ])
            .collect()?;
        write_editor_identity_coverage(wiki, Some(snapshot), candidate_dir, vec![coverage])?;
    }
    let weekly_config = WeeklyAggregationConfig::for_snapshot(data_dir, wiki, Some(snapshot))?;
    for family in MetricFamily::CORE {
        let algorithm = family.algorithm_version(&weekly_config);
        let inputs = family_inputs(family, wiki, data_dir, Some(snapshot))?;
        let outputs = family_outputs(family, wiki, candidate_dir);
        fingerprint::record(
            &family_stage_receipt(candidate_dir, wiki, family),
            family_stage_spec(family, wiki, Some(snapshot), &algorithm),
            &inputs,
            &outputs,
        )?;
    }
    Ok(())
}

/// Run all metric families for a wiki.
pub fn compute_all(wiki: &str, data_dir: &Path, output_dir: &Path) -> Result<()> {
    let snapshot = storage::current_snapshot_version(data_dir, wiki)?;
    compute_all_selected(wiki, data_dir, output_dir, snapshot.as_deref())
}

pub(crate) fn compute_all_for_snapshot(
    wiki: &str,
    snapshot: &str,
    data_dir: &Path,
    output_dir: &Path,
) -> Result<()> {
    storage::validate_snapshot_version(snapshot)?;
    compute_all_selected(wiki, data_dir, output_dir, Some(snapshot))
}

pub(crate) fn compute_cross_snapshot_qualification_build(
    wiki: &str,
    snapshot: &str,
    data_dir: &Path,
    output_dir: &Path,
    use_cache: bool,
) -> Result<crate::cross_snapshot::CacheStats> {
    storage::validate_snapshot_version(snapshot)?;
    anyhow::ensure!(
        !output_dir.exists(),
        "cross-snapshot qualification output already exists: {}",
        output_dir.display()
    );
    fs::create_dir_all(output_dir)?;
    let weekly_config = WeeklyAggregationConfig::for_snapshot(data_dir, wiki, Some(snapshot))?;
    let cache = use_cache
        .then(|| crate::cross_snapshot::CrossSnapshotCache::new(data_dir, wiki, snapshot))
        .transpose()?;
    let compute_result = compute_all_incremental_cached(
        wiki,
        data_dir,
        output_dir,
        Some(snapshot),
        ComputePlan::all_recompute(),
        cache.as_ref(),
    );
    compute_result?;
    let weekly_result = compute_page_weekly_external_qualification(
        wiki,
        data_dir,
        output_dir,
        &weekly_config,
        Some(snapshot),
        cache.as_ref(),
    );
    weekly_result?;
    Ok(cache.as_ref().map_or_default(|cache| cache.stats()))
}

fn compute_all_selected(
    wiki: &str,
    data_dir: &Path,
    output_dir: &Path,
    snapshot: Option<&str>,
) -> Result<()> {
    anyhow::ensure!(
        !output_dir.join("ready.json").exists() && !output_dir.join("qualification.json").exists(),
        "refusing to modify an immutable ready candidate"
    );
    let weekly_config = WeeklyAggregationConfig::for_snapshot(data_dir, wiki, snapshot)?;
    if let Some(snapshot) = snapshot {
        migrate_legacy_compute_receipt(wiki, snapshot, data_dir, output_dir, &weekly_config)?;
    }
    let plan = compute_plan(wiki, snapshot, data_dir, output_dir, &weekly_config)?;
    if plan.all_reused() {
        crate::observability::record_stage_reused("compute", Some(wiki));
        info!(
            wiki,
            snapshot = snapshot.unwrap_or("legacy"),
            families = MetricFamily::CORE.len(),
            "reusing deterministic compute stage"
        );
        return Ok(());
    }

    info!(wiki = wiki, ?plan, "computing invalidated metric families");
    let started = Instant::now();
    let cross_snapshot = snapshot
        .map(|snapshot| crate::cross_snapshot::production_cache(data_dir, wiki, snapshot))
        .transpose()?
        .flatten();
    info!(
        wiki,
        snapshot = snapshot.unwrap_or("legacy"),
        enabled = cross_snapshot.is_some(),
        "selected production cross-snapshot cache policy"
    );

    let cross_snapshot_cache = cross_snapshot.as_ref();
    let compute_result = compute_all_incremental_cached(
        wiki,
        data_dir,
        output_dir,
        snapshot,
        plan,
        cross_snapshot_cache,
    );
    let analytical_partitions_scanned = compute_result?;
    if plan.page_week.must_compute() {
        compute_page_weekly_edits_for_snapshot_cached(
            wiki,
            data_dir,
            output_dir,
            &weekly_config,
            snapshot,
            cross_snapshot.as_ref(),
        )?;
    }
    for family in MetricFamily::CORE {
        if !plan.invalidation(family).must_compute() {
            crate::observability::record_stage_reused(
                &format!("compute_{}", family.name()),
                Some(wiki),
            );
            continue;
        }
        let algorithm = family.algorithm_version(&weekly_config);
        let inputs = family_inputs(family, wiki, data_dir, snapshot)?;
        let outputs = family_outputs(family, wiki, output_dir);
        if outputs.iter().any(|output| !output.path.is_file()) {
            info!(
                wiki,
                family = family.name(),
                "metric family produced no output for this input layout"
            );
            continue;
        }
        fingerprint::record(
            &family_stage_receipt(output_dir, wiki, family),
            family_stage_spec(family, wiki, snapshot, &algorithm),
            &inputs,
            &outputs,
        )
        .with_context(|| {
            format!(
                "failed to record {} compute-family receipt for {wiki}",
                family.name()
            )
        })?;
    }

    info!(
        wiki = wiki,
        analytical_partitions_scanned,
        cache = ?cross_snapshot.as_ref().map(crate::cross_snapshot::CrossSnapshotCache::stats),
        elapsed_ms = started.elapsed().as_secs_f64() * 1_000.0,
        "finished metric computation"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resource_governor::ResourceBudget;
    use crate::test_support::{TestDir, init_test_tracing};

    #[test]
    fn snapshot_excludes_partial_following_month() {
        assert!(snapshot_contains_complete_month("2026-07", "2026-07"));
        assert!(snapshot_contains_complete_month("2026-07", "2001-01"));
        assert!(!snapshot_contains_complete_month("2026-07", "2026-08"));
    }

    fn editor_months(edits: &[u32], month_keys: &[i32], user_ids: &[i64]) -> Result<DataFrame> {
        anyhow::ensure!(edits.len() == month_keys.len() && edits.len() == user_ids.len());
        DataFrame::new_infer_height(vec![
            Column::new(
                "year_month".into(),
                month_keys
                    .iter()
                    .map(|key| format!("{:04}-{:02}", key / 100, key % 100))
                    .collect::<Vec<_>>(),
            ),
            Column::new("year_month_key".into(), month_keys.to_vec()),
            Column::new("user_type".into(), vec!["registered"; edits.len()]),
            Column::new("user_type_rank".into(), vec![0_i32; edits.len()]),
            Column::new("event_user_id".into(), user_ids.to_vec()),
            Column::new("edits".into(), edits.to_vec()),
            Column::new(
                "net_bytes".into(),
                edits
                    .iter()
                    .map(|value| i64::from(*value))
                    .collect::<Vec<_>>(),
            ),
            Column::new(
                "gross_bytes".into(),
                edits
                    .iter()
                    .map(|value| i64::from(*value))
                    .collect::<Vec<_>>(),
            ),
        ])
        .map_err(Into::into)
    }

    fn single_activity_tier(period: ActivityPeriod, edits: u32) -> Result<(String, u32)> {
        let frame = editor_months(&[edits], &[202401], &[1])?;
        let output = gdp_activity_tiers_for_period(&frame, period)?;
        Ok((
            output
                .column("activity_tier")?
                .str()?
                .get(0)
                .context("activity tier should be present")?
                .to_string(),
            output
                .column("tier_rank")?
                .u32()?
                .get(0)
                .context("activity tier rank should be present")?,
        ))
    }

    #[test]
    fn activity_tiers_scale_monthly_rates_at_exact_period_boundaries() -> Result<()> {
        for (period, boundaries) in [
            (
                ActivityPeriod::Month,
                [
                    (1, "1 edit"),
                    (2, "2-4 edits"),
                    (5, "5-24 edits"),
                    (25, "25-99 edits"),
                    (100, "100+ edits"),
                ],
            ),
            (
                ActivityPeriod::Quarter,
                [
                    (3, "1-3 edits"),
                    (4, "4-14 edits"),
                    (15, "15-74 edits"),
                    (75, "75-299 edits"),
                    (300, "300+ edits"),
                ],
            ),
            (
                ActivityPeriod::Year,
                [
                    (12, "1-12 edits"),
                    (13, "13-59 edits"),
                    (60, "60-299 edits"),
                    (300, "300-1199 edits"),
                    (1200, "1200+ edits"),
                ],
            ),
        ] {
            for (rank, (edits, label)) in boundaries.into_iter().enumerate() {
                assert_eq!(
                    single_activity_tier(period, edits)?,
                    (label.to_string(), rank as u32)
                );
            }
        }
        assert_eq!(
            single_activity_tier(ActivityPeriod::Month, 99)?.0,
            "25-99 edits"
        );
        assert_eq!(
            single_activity_tier(ActivityPeriod::Quarter, 299)?.0,
            "75-299 edits"
        );
        assert_eq!(
            single_activity_tier(ActivityPeriod::Year, 1199)?.0,
            "300-1199 edits"
        );
        assert_eq!(activity_tier_labels(1)[0], "1 edit");
        assert_eq!(activity_tier_labels(3)[0], "1-3 edits");
        assert!(ActivityPeriod::Month.fields(202400).is_err());
        assert!(ActivityPeriod::Quarter.fields(20240).is_err());
        Ok(())
    }

    #[test]
    fn activity_period_assigns_each_identity_one_user_type() -> Result<()> {
        let base = df!(
            "year_month" => &["2024-01", "2024-01", "2024-02"],
            "year_month_key" => &[202401_i32, 202401, 202402],
            "user_type" => &["registered", "bot", "registered"],
            "event_user_id" => &[1_i64, 1, 2],
            "revision_id" => &[10_i64, 11, 12],
            "revision_text_bytes_diff" => &[1_i64, 1, 1],
        )
        .expect("activity identity fixture should be valid");
        let editor_months = gdp_editor_month_frame(&base)?;
        let annual = gdp_activity_tiers_for_period(&editor_months, ActivityPeriod::Year)?;
        assert_eq!(annual.column("editors")?.u32()?.sum(), Some(2));
        let bot = annual
            .lazy()
            .filter(col("user_type").eq(lit("bot")))
            .collect()?;
        assert_eq!(bot.column("editors")?.u32()?.sum(), Some(1));
        assert_eq!(bot.column("total_edits")?.u32()?.sum(), Some(2));
        Ok(())
    }

    #[test]
    fn activity_tiers_reclassify_editors_and_conserve_edits_for_each_period() -> Result<()> {
        let months = [202401, 202402, 202403];
        let edits = [100, 100, 100, 99, 99, 99];
        let month_keys = [
            months[0], months[1], months[2], months[0], months[1], months[2],
        ];
        let user_ids = [1, 1, 1, 2, 2, 2];
        let frame = editor_months(&edits, &month_keys, &user_ids)?;
        let quarterly = gdp_activity_tiers_for_period(&frame, ActivityPeriod::Quarter)?;
        assert_eq!(
            quarterly
                .get_column_names()
                .iter()
                .map(|name| name.as_str())
                .collect::<Vec<_>>(),
            ACTIVITY_TIER_OUTPUT_COLUMNS
        );
        assert_eq!(quarterly.column("total_edits")?.u32()?.sum(), Some(597));
        assert_eq!(quarterly.column("editors")?.u32()?.sum(), Some(2));
        assert_eq!(quarterly.column("period")?.str()?.get(0), Some("2024-Q1"));
        assert_eq!(
            quarterly.column("period_start")?.str()?.get(0),
            Some("2024-01")
        );
        assert_eq!(
            quarterly.column("period_end")?.str()?.get(0),
            Some("2024-03")
        );
        assert_eq!(quarterly.column("period_months")?.u32()?.get(0), Some(3));
        assert!(
            quarterly
                .column("activity_tier")?
                .str()?
                .iter()
                .flatten()
                .any(|tier| tier == "300+ edits")
        );

        let all_periods = activity_tiers_all_periods(registered_base_df(&[
            (Some(1), 2024, 202401, 1),
            (Some(1), 2024, 202402, 2),
        ])?)?;
        let period_types = all_periods
            .column("period_type")?
            .str()?
            .iter()
            .flatten()
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(
            period_types,
            std::collections::BTreeSet::from(["month", "quarter", "year"])
        );

        let mut empty = Vec::new();
        let mut output = Vec::new();
        finish_activity_year(&mut empty, &mut output)?;
        assert!(output.is_empty());
        Ok(())
    }

    #[test]
    fn period_workforce_deduplicates_namespaces_and_months() -> Result<()> {
        let columns = vec![
            Column::new(
                "year_month".into(),
                vec!["2024-01", "2024-01", "2024-02", "2024-02"],
            ),
            Column::new(
                "year_month_key".into(),
                vec![202401_i32, 202401, 202402, 202402],
            ),
            Column::new("user_type".into(), vec!["registered"; 4]),
            Column::new("event_user_id".into(), vec![1_i64, 1, 1, 2]),
            Column::new("page_namespace".into(), vec![0_i32, 1, 0, 0]),
            Column::new("revision_id".into(), vec![1_i64, 2, 3, 4]),
            Column::new("revision_text_bytes_diff".into(), vec![1_i64; 4]),
        ];
        let base = DataFrame::new_infer_height(columns)?;
        let editor_months = gdp_editor_month_frame(&base)?;

        let monthly = gdp_activity_tiers_for_period(&editor_months, ActivityPeriod::Month)?;
        let yearly = gdp_activity_tiers_for_period(&editor_months, ActivityPeriod::Year)?;

        assert_eq!(monthly.column("editors")?.u32()?.sum(), Some(3));
        assert_eq!(yearly.column("editors")?.u32()?.sum(), Some(2));
        assert_eq!(yearly.column("total_edits")?.u32()?.sum(), Some(4));
        Ok(())
    }

    #[test]
    fn activity_tier_incremental_compute_flushes_each_calendar_year() -> Result<()> {
        let data_dir = TestDir::new()?;
        let output_dir = TestDir::new()?;
        let wiki = "two-year-wiki";
        write_partitioned_base_parquet(&data_dir, wiki)?;

        let next_year_dir = storage::month_partition_dir(
            &storage::analytical_wiki_dir(data_dir.path(), wiki),
            2025,
            "2025-01",
        );
        fs::create_dir_all(&next_year_dir)?;
        let mut next_year = analytical_partition_df(AnalyticalPartitionRows {
            year_month: ["2025-01", "2025-01"],
            year_month_key: [202501, 202501],
            user_type: ["registered", "registered"],
            event_user_id: [1, 4],
            page_namespace: [0, 0],
            revision_id: [14, 15],
            revision_text_bytes_diff: [9, 11],
            is_reverted: [false, false],
            is_minor: [false, true],
        })?;
        let next_year_path = next_year_dir.join("part-000.parquet");
        let mut next_year_file = fs::File::create(next_year_path)?;
        ParquetWriter::new(&mut next_year_file).finish(&mut next_year)?;

        compute_all_incremental(
            wiki,
            data_dir.path(),
            output_dir.path(),
            None,
            ComputePlan::all_recompute(),
        )
        .expect("partitioned activity-tier fixture should compute");
        let tiers_path = output_dir
            .path()
            .join(wiki)
            .join("gdp_activity_tiers.parquet");
        let tiers = ParquetReader::new(File::open(tiers_path)?).finish()?;
        let mut expected_columns = ACTIVITY_TIER_OUTPUT_COLUMNS.to_vec();
        expected_columns.push("wiki");
        assert_eq!(
            tiers
                .get_column_names()
                .iter()
                .map(|name| name.as_str())
                .collect::<Vec<_>>(),
            expected_columns
        );
        assert!(
            tiers
                .column("period")?
                .str()?
                .iter()
                .flatten()
                .any(|period| period == "2025")
        );
        Ok(())
    }

    fn sample_input_df() -> Result<DataFrame> {
        DataFrame::new_infer_height(vec![
            Column::new(
                "event_entity".into(),
                vec![
                    "revision", "revision", "revision", "revision", "revision", "revision",
                ],
            ),
            Column::new(
                "event_type".into(),
                vec!["create", "create", "create", "create", "create", "create"],
            ),
            Column::new(
                "event_timestamp".into(),
                vec![
                    "2024-01-01 00:00:00.0",
                    "2024-01-03 00:00:00.0",
                    "2024-01-05 00:00:00.0",
                    "2024-02-01 00:00:00.0",
                    "2024-02-10 00:00:00.0",
                    "2025-01-10 00:00:00.0",
                ],
            ),
            Column::new("event_user_id".into(), vec![1_i64, 2, 4, 3, 1, 1]),
            Column::new(
                "event_user_is_bot_by".into(),
                vec![None::<&str>, None, None, None, Some("bot"), None],
            ),
            Column::new(
                "event_user_is_anonymous".into(),
                vec!["false", "false", "false", "false", "false", "false"],
            ),
            Column::new(
                "event_user_is_temporary".into(),
                vec!["false", "true", "true", "false", "false", "false"],
            ),
            Column::new("page_namespace".into(), vec![0_i32, 0, 0, 1, 0, 0]),
            Column::new("revision_id".into(), vec![10_i64, 11, 12, 13, 14, 15]),
            Column::new(
                "revision_text_bytes_diff".into(),
                vec![10_i64, 20, 15, -5, 7, 30],
            ),
            Column::new(
                "revision_is_identity_reverted".into(),
                vec!["false", "false", "false", "true", "false", "false"],
            ),
            Column::new(
                "revision_minor_edit".into(),
                vec!["false", "true", "false", "false", "false", "true"],
            ),
        ])
        .map_err(Into::into)
    }

    struct AnalyticalPartitionRows<'a> {
        year_month: [&'a str; 2],
        year_month_key: [i32; 2],
        user_type: [&'a str; 2],
        event_user_id: [i64; 2],
        page_namespace: [i32; 2],
        revision_id: [i64; 2],
        revision_text_bytes_diff: [i64; 2],
        is_reverted: [bool; 2],
        is_minor: [bool; 2],
    }

    fn analytical_partition_df(rows: AnalyticalPartitionRows<'_>) -> Result<DataFrame> {
        let columns = vec![
            Column::new("year_month".into(), rows.year_month.to_vec()),
            Column::new("year".into(), vec![2024_i32, 2024]),
            Column::new("year_month_key".into(), rows.year_month_key.to_vec()),
            Column::new("user_type".into(), rows.user_type.to_vec()),
            Column::new("event_user_id".into(), rows.event_user_id.to_vec()),
            Column::new("page_namespace".into(), rows.page_namespace.to_vec()),
            Column::new("revision_id".into(), rows.revision_id.to_vec()),
            Column::new(
                "revision_text_bytes_diff".into(),
                rows.revision_text_bytes_diff.to_vec(),
            ),
            Column::new("is_reverted".into(), rows.is_reverted.to_vec()),
            Column::new("is_minor".into(), rows.is_minor.to_vec()),
        ];
        DataFrame::new_infer_height(columns).map_err(Into::into)
    }

    fn write_input_parquet(temp_dir: &TestDir, wiki: &str) -> Result<()> {
        let parquet_dir = storage::analytical_wiki_dir(temp_dir.path(), wiki);
        fs::create_dir_all(&parquet_dir)?;
        let path = parquet_dir.join("part-000.parquet");
        let mut file = fs::File::create(path)?;
        let mut df = sample_input_df()?;
        ParquetWriter::new(&mut file).finish(&mut df)?;
        Ok(())
    }

    fn write_partitioned_base_parquet(temp_dir: &TestDir, wiki: &str) -> Result<()> {
        let jan_dir = storage::month_partition_dir(
            &storage::analytical_wiki_dir(temp_dir.path(), wiki),
            2024,
            "2024-01",
        );
        let feb_dir = storage::month_partition_dir(
            &storage::analytical_wiki_dir(temp_dir.path(), wiki),
            2024,
            "2024-02",
        );
        fs::create_dir_all(&jan_dir)?;
        fs::create_dir_all(&feb_dir)?;

        let mut jan = analytical_partition_df(AnalyticalPartitionRows {
            year_month: ["2024-01", "2024-01"],
            year_month_key: [202401, 202401],
            user_type: ["registered", "temporary"],
            event_user_id: [1, 2],
            page_namespace: [0, 0],
            revision_id: [10, 11],
            revision_text_bytes_diff: [15, 5],
            is_reverted: [false, false],
            is_minor: [true, false],
        })?;
        let mut feb = analytical_partition_df(AnalyticalPartitionRows {
            year_month: ["2024-02", "2024-02"],
            year_month_key: [202402, 202402],
            user_type: ["registered", "registered"],
            event_user_id: [1, 3],
            page_namespace: [0, 1],
            revision_id: [12, 13],
            revision_text_bytes_diff: [7, -3],
            is_reverted: [false, true],
            is_minor: [false, false],
        })?;

        ParquetWriter::new(&mut fs::File::create(jan_dir.join("part-000.parquet"))?)
            .finish(&mut jan)?;
        ParquetWriter::new(&mut fs::File::create(feb_dir.join("part-000.parquet"))?)
            .finish(&mut feb)?;
        Ok(())
    }

    fn write_partitioned_warehouse_parquet(temp_dir: &TestDir, wiki: &str) -> Result<()> {
        let jan_dir = storage::month_partition_dir(
            &storage::warehouse_wiki_dir(temp_dir.path(), wiki),
            2024,
            "2024-01",
        );
        let feb_dir = storage::month_partition_dir(
            &storage::warehouse_wiki_dir(temp_dir.path(), wiki),
            2024,
            "2024-02",
        );
        fs::create_dir_all(&jan_dir)?;
        fs::create_dir_all(&feb_dir)?;

        let mut jan = DataFrame::new_infer_height(vec![
            Column::new(
                "event_timestamp".into(),
                vec![
                    "2024-01-02 10:00:00.0",
                    "2024-01-03 11:00:00.0",
                    "2024-01-08 09:00:00.0",
                ],
            ),
            Column::new("page_id".into(), vec![10_i64, 10, 10]),
            Column::new("page_title".into(), vec!["Alpha", "Alpha", "Alpha"]),
            Column::new("page_namespace".into(), vec![0_i32, 0, 0]),
        ])
        .expect("valid January fixture");
        let mut feb = DataFrame::new_infer_height(vec![
            Column::new(
                "event_timestamp".into(),
                vec![
                    "2024-02-02 12:00:00.0",
                    "2024-02-06 08:00:00.0",
                    "2024-02-07 14:00:00.0",
                ],
            ),
            Column::new("page_id".into(), vec![10_i64, 10, 20]),
            Column::new("page_title".into(), vec!["Alpha", "Alpha", "Beta"]),
            Column::new("page_namespace".into(), vec![0_i32, 0, 0]),
        ])
        .expect("valid February fixture");

        ParquetWriter::new(&mut fs::File::create(jan_dir.join("part-000.parquet"))?)
            .finish(&mut jan)?;
        ParquetWriter::new(&mut fs::File::create(feb_dir.join("part-000.parquet"))?)
            .finish(&mut feb)?;
        Ok(())
    }

    fn write_null_page_warehouse_parquet(temp_dir: &TestDir, wiki: &str) -> Result<()> {
        let partition_dir = storage::month_partition_dir(
            &storage::warehouse_wiki_dir(temp_dir.path(), wiki),
            2002,
            "2002-06",
        );
        fs::create_dir_all(&partition_dir)?;

        let mut frame = DataFrame::new_infer_height(vec![
            Column::new(
                "event_timestamp".into(),
                vec!["2002-06-10 17:55:22.0", "2002-06-10 18:00:00.0"],
            ),
            Column::new("page_id".into(), vec![None, Some(10_i64)]),
            Column::new("page_title".into(), vec![None, Some("Known page")]),
            Column::new("page_namespace".into(), vec![None, Some(0_i32)]),
        ])
        .expect("valid null-page warehouse fixture");
        ParquetWriter::new(
            &mut fs::File::create(partition_dir.join("part-000.parquet"))
                .expect("null-page warehouse fixture should be writable"),
        )
        .finish(&mut frame)
        .expect("null-page warehouse fixture should serialize");
        Ok(())
    }

    fn write_partitioned_legacy_parquet(temp_dir: &TestDir, wiki: &str) -> Result<()> {
        let jan_dir = storage::month_partition_dir(
            &storage::analytical_wiki_dir(temp_dir.path(), wiki),
            2024,
            "2024-01",
        );
        let feb_dir = storage::month_partition_dir(
            &storage::analytical_wiki_dir(temp_dir.path(), wiki),
            2024,
            "2024-02",
        );
        fs::create_dir_all(&jan_dir)?;
        fs::create_dir_all(&feb_dir)?;

        let jan_columns = vec![
            Column::new("event_entity".into(), vec!["revision", "revision"]),
            Column::new("event_type".into(), vec!["create", "create"]),
            Column::new(
                "event_timestamp".into(),
                vec!["2024-01-01 00:00:00.0", "2024-01-03 00:00:00.0"],
            ),
            Column::new("event_user_id".into(), vec![1_i64, 2]),
            Column::new("event_user_is_bot_by".into(), vec![None::<&str>, None]),
            Column::new("event_user_is_anonymous".into(), vec![false, false]),
            Column::new("event_user_is_temporary".into(), vec![false, true]),
            Column::new("page_namespace".into(), vec![0_i32, 0]),
            Column::new("revision_id".into(), vec![10_i64, 11]),
            Column::new("revision_text_bytes_diff".into(), vec![15_i64, 5]),
            Column::new("revision_is_identity_reverted".into(), vec![false, false]),
            Column::new("revision_minor_edit".into(), vec![true, false]),
        ];
        let feb_columns = vec![
            Column::new("event_entity".into(), vec!["revision", "revision"]),
            Column::new("event_type".into(), vec!["create", "create"]),
            Column::new(
                "event_timestamp".into(),
                vec!["2024-02-01 00:00:00.0", "2024-02-10 00:00:00.0"],
            ),
            Column::new("event_user_id".into(), vec![1_i64, 3]),
            Column::new("event_user_is_bot_by".into(), vec![None::<&str>, None]),
            Column::new("event_user_is_anonymous".into(), vec![false, false]),
            Column::new("event_user_is_temporary".into(), vec![false, false]),
            Column::new("page_namespace".into(), vec![0_i32, 1]),
            Column::new("revision_id".into(), vec![12_i64, 13]),
            Column::new("revision_text_bytes_diff".into(), vec![7_i64, -3]),
            Column::new("revision_is_identity_reverted".into(), vec![false, true]),
            Column::new("revision_minor_edit".into(), vec![false, false]),
        ];

        let mut jan = DataFrame::new_infer_height(jan_columns)?;
        let mut feb = DataFrame::new_infer_height(feb_columns)?;
        ParquetWriter::new(&mut fs::File::create(jan_dir.join("part-000.parquet"))?)
            .finish(&mut jan)?;
        ParquetWriter::new(&mut fs::File::create(feb_dir.join("part-000.parquet"))?)
            .finish(&mut feb)?;
        Ok(())
    }

    fn write_partitioned_compatibility_parquet_without_filter_columns(
        temp_dir: &TestDir,
        wiki: &str,
    ) -> Result<()> {
        let dir = storage::month_partition_dir(
            &storage::analytical_wiki_dir(temp_dir.path(), wiki),
            2024,
            "2024-01",
        );
        fs::create_dir_all(&dir)?;
        let columns = vec![
            Column::new("year_month".into(), vec!["2024-01", "2024-01"]),
            Column::new("year".into(), vec![2024_i32, 2024]),
            Column::new("year_month_key".into(), vec![202401_i32, 202401]),
            Column::new("user_type".into(), vec!["registered", "temporary"]),
            Column::new("event_user_id".into(), vec![1_i64, 2]),
            Column::new("page_namespace".into(), vec![0_i32, 0]),
            Column::new("revision_id".into(), vec![10_i64, 11]),
            Column::new("revision_text_bytes_diff".into(), vec![15_i64, 5]),
            Column::new("revision_is_identity_reverted".into(), vec![false, false]),
            Column::new("revision_minor_edit".into(), vec![true, false]),
        ];
        let mut df = DataFrame::new_infer_height(columns)?;
        ParquetWriter::new(&mut fs::File::create(dir.join("part-000.parquet"))?).finish(&mut df)?;
        Ok(())
    }

    fn write_precomputed_parquet(
        temp_dir: &TestDir,
        wiki: &str,
        with_output_flags: bool,
    ) -> Result<()> {
        let parquet_dir = storage::analytical_wiki_dir(temp_dir.path(), wiki);
        fs::create_dir_all(&parquet_dir)?;
        let path = parquet_dir.join("part-000.parquet");
        let mut file = fs::File::create(path)?;

        let mut columns = vec![
            Column::new("event_entity".into(), vec!["revision", "revision"]),
            Column::new("event_type".into(), vec!["create", "create"]),
            Column::new("year_month".into(), vec!["2024-01", "2024-02"]),
            Column::new("year".into(), vec![2024_i32, 2024]),
            Column::new("year_month_key".into(), vec![202401_i32, 202402]),
            Column::new("user_type".into(), vec!["registered", "temporary"]),
            Column::new("event_user_id".into(), vec![1_i64, 2]),
            Column::new("page_namespace".into(), vec![0_i32, 1]),
            Column::new("revision_id".into(), vec![10_i64, 11]),
            Column::new("revision_text_bytes_diff".into(), vec![15_i64, -3]),
        ];

        if with_output_flags {
            columns.push(Column::new("is_reverted".into(), vec![false, true]));
            columns.push(Column::new("is_minor".into(), vec![true, false]));
        } else {
            columns.push(Column::new(
                "revision_is_identity_reverted".into(),
                vec![false, true],
            ));
            columns.push(Column::new("revision_minor_edit".into(), vec![true, false]));
        }

        let mut df = DataFrame::new_infer_height(columns)?;
        ParquetWriter::new(&mut file).finish(&mut df)?;
        Ok(())
    }

    fn write_compatibility_parquet_with_output_flags(temp_dir: &TestDir, wiki: &str) -> Result<()> {
        let parquet_dir = storage::analytical_wiki_dir(temp_dir.path(), wiki);
        fs::create_dir_all(&parquet_dir)?;
        let path = parquet_dir.join("part-000.parquet");
        let mut file = fs::File::create(path)?;
        let columns = vec![
            Column::new("event_entity".into(), vec!["revision", "revision"]),
            Column::new("event_type".into(), vec!["create", "create"]),
            Column::new(
                "event_timestamp".into(),
                vec!["2024-01-01 00:00:00.0", "2024-02-01 00:00:00.0"],
            ),
            Column::new("year_month".into(), vec!["2024-01", "2024-02"]),
            Column::new("year".into(), vec![2024_i32, 2024]),
            Column::new("user_type".into(), vec!["registered", "temporary"]),
            Column::new("event_user_id".into(), vec![1_i64, 2]),
            Column::new("page_namespace".into(), vec![0_i32, 1]),
            Column::new("revision_id".into(), vec![10_i64, 11]),
            Column::new("revision_text_bytes_diff".into(), vec![15_i64, -3]),
            Column::new("is_reverted".into(), vec![false, true]),
            Column::new("is_minor".into(), vec![true, false]),
        ];
        let mut df = DataFrame::new_infer_height(columns)?;
        ParquetWriter::new(&mut file).finish(&mut df)?;
        Ok(())
    }

    fn registered_base_df(rows: &[(Option<i64>, i32, i32, i64)]) -> Result<DataFrame> {
        DataFrame::new_infer_height(vec![
            Column::new(
                "year_month".into(),
                rows.iter()
                    .map(|(_, _, year_month_key, _)| {
                        format!("{:04}-{:02}", year_month_key / 100, year_month_key % 100)
                    })
                    .collect::<Vec<_>>(),
            ),
            Column::new(
                "year".into(),
                rows.iter().map(|(_, year, _, _)| *year).collect::<Vec<_>>(),
            ),
            Column::new(
                "year_month_key".into(),
                rows.iter()
                    .map(|(_, _, year_month_key, _)| *year_month_key)
                    .collect::<Vec<_>>(),
            ),
            Column::new("user_type".into(), vec!["registered"; rows.len()]),
            Column::new(
                "event_user_id".into(),
                rows.iter()
                    .map(|(user_id, _, _, _)| *user_id)
                    .collect::<Vec<_>>(),
            ),
            Column::new("page_namespace".into(), vec![0_i32; rows.len()]),
            Column::new(
                "revision_id".into(),
                rows.iter()
                    .map(|(_, _, _, revision_id)| *revision_id)
                    .collect::<Vec<_>>(),
            ),
            Column::new("revision_text_bytes_diff".into(), vec![1_i64; rows.len()]),
            Column::new("is_reverted".into(), vec![false; rows.len()]),
            Column::new("is_minor".into(), vec![false; rows.len()]),
        ])
        .map_err(Into::into)
    }

    fn anonymous_identity_input(include_historical_actor_text: bool) -> Result<DataFrame> {
        let mut columns = vec![
            Column::new(
                "event_timestamp".into(),
                vec![
                    "2025-05-01 00:00:00.0",
                    "2025-05-02 00:00:00.0",
                    "2025-05-03 00:00:00.0",
                ],
            ),
            Column::new("event_user_id".into(), vec![None::<i64>, None, None]),
            Column::new(
                "event_user_is_bot_by".into(),
                vec![None::<&str>, None, None],
            ),
            Column::new("event_user_is_anonymous".into(), vec![true, true, true]),
            Column::new("event_user_is_temporary".into(), vec![false, false, false]),
            Column::new("page_namespace".into(), vec![0_i32, 0, 0]),
            Column::new("revision_id".into(), vec![1_i64, 2, 3]),
            Column::new("revision_text_bytes_diff".into(), vec![1_i64, 1, 1]),
            Column::new("is_reverted".into(), vec![false, false, false]),
            Column::new("is_minor".into(), vec![false, false, false]),
        ];
        if include_historical_actor_text {
            columns.push(Column::new(
                "event_user_text_historical".into(),
                vec!["192.0.2.1", "192.0.2.1", "198.51.100.4"],
            ));
            columns.push(Column::new(
                "event_user_text".into(),
                vec![None::<&str>, None, None],
            ));
        }
        DataFrame::new_infer_height(columns).map_err(Into::into)
    }

    #[test]
    fn compute_all_writes_expected_outputs() -> Result<()> {
        init_test_tracing();
        let data_dir = TestDir::new()?;
        let output_dir = TestDir::new()?;
        let wiki = "testwiki";

        write_input_parquet(&data_dir, wiki)?;
        write_partitioned_warehouse_parquet(&data_dir, wiki)?;
        compute_all(wiki, data_dir.path(), output_dir.path())?;

        for metric in [
            "business_funnel",
            "gdp",
            "gdp_activity_tiers",
            "gdp_user_type_share",
            "inequality",
            "labor_churn",
            "labor_cohorts",
            "labor_monthly",
            "page_weekly_edits",
        ] {
            assert!(
                output_dir
                    .path()
                    .join(wiki)
                    .join(format!("{metric}.parquet"))
                    .exists()
            );
        }

        let inequality_path = output_dir.path().join(wiki).join("inequality.parquet");
        let inequality_path = inequality_path.to_string_lossy().to_string();
        let inequality =
            LazyFrame::scan_parquet(inequality_path.as_str().into(), Default::default())?
                .collect()?;
        let user_types: Vec<String> = inequality
            .column("user_type")?
            .str()?
            .iter()
            .flatten()
            .map(ToOwned::to_owned)
            .collect();
        assert!(user_types.iter().any(|user_type| user_type == "temporary"));

        Ok(())
    }

    #[test]
    fn historical_anonymous_actor_text_provides_distinct_editor_identity() -> Result<()> {
        let input = anonymous_identity_input(true)?;
        let schema = input.schema().clone();
        let projected = analytical_projection(input.lazy(), schema.as_ref())?;

        let type_share = gdp_type_share_frame(&projected)?;
        assert_eq!(type_share.column("edits")?.u32()?.get(0), Some(3));
        assert_eq!(type_share.column("editors")?.u32()?.get(0), Some(2));

        let inequality = inequality::compute_frame(&projected)?;
        assert_eq!(inequality.column("total_editors")?.u32()?.get(0), Some(2));
        assert_eq!(inequality.column("total_edits")?.u32()?.get(0), Some(3));
        Ok(())
    }

    #[test]
    fn suppressed_identity_rows_are_counted_but_not_collapsed() -> Result<()> {
        let input = anonymous_identity_input(false)?;
        let schema = input.schema().clone();
        let projected = analytical_projection(input.lazy(), schema.as_ref())?;
        let type_share = gdp_type_share_frame(&projected)?;
        assert_eq!(type_share.column("edits")?.u32()?.get(0), Some(3));
        assert_eq!(type_share.column("editors")?.u32()?.get(0), Some(0));
        assert_eq!(inequality::compute_frame(&projected)?.height(), 0);

        let output = TestDir::new()?;
        assert!(read_editor_identity_coverage(output.path(), "testwiki")?.is_none());
        write_editor_identity_coverage(
            "testwiki",
            Some("2025-05"),
            output.path(),
            vec![editor_identity_coverage_frame(&projected)?],
        )
        .expect("suppressed identity coverage should be writable");
        let report = read_editor_identity_coverage(output.path(), "testwiki")?
            .expect("identity coverage report should exist");
        assert_eq!(report.total_edits, 3);
        assert_eq!(report.identified_edits, 0);
        assert_eq!(report.excluded_edits, 3);
        assert_eq!(report.periods[0].excluded_edits, 3);
        Ok(())
    }

    #[test]
    fn analytical_projection_supports_each_actor_schema_generation() -> Result<()> {
        for (keep_historical, keep_current, expected) in [
            (true, false, Some("192.0.2.1")),
            (false, true, None),
            (false, false, None),
        ] {
            let mut input = anonymous_identity_input(true)?;
            if !keep_historical {
                input.drop_in_place("event_user_text_historical")?;
            }
            if !keep_current {
                input.drop_in_place("event_user_text")?;
            }
            let schema = input.schema().clone();
            let projected = analytical_projection(input.lazy(), schema.as_ref())?;
            assert_eq!(
                projected.column(EDITOR_ACTOR_COLUMN)?.str()?.get(0),
                expected
            );
        }
        Ok(())
    }

    #[test]
    fn compute_all_reuses_matching_stage_fingerprint() -> Result<()> {
        init_test_tracing();
        let data_dir = TestDir::new()?;
        let output_dir = TestDir::new()?;
        let wiki = "testwiki";
        write_input_parquet(&data_dir, wiki)?;
        write_partitioned_warehouse_parquet(&data_dir, wiki)?;

        compute_all(wiki, data_dir.path(), output_dir.path())?;
        let metric = output_dir.path().join(wiki).join("gdp.parquet");
        let before = fs::metadata(&metric)?.modified()?;
        let receipt = family_stage_receipt(output_dir.path(), wiki, MetricFamily::Monthly);
        let receipt_before = fs::read(&receipt)?;

        compute_all(wiki, data_dir.path(), output_dir.path())?;

        assert_eq!(fs::metadata(metric)?.modified()?, before);
        assert_eq!(fs::read(receipt)?, receipt_before);
        Ok(())
    }

    fn invalidate_family_receipt(
        output_dir: &Path,
        wiki: &str,
        family: MetricFamily,
    ) -> Result<()> {
        let path = family_stage_receipt(output_dir, wiki, family);
        let mut receipt = fingerprint::read_receipt(&path)?;
        receipt.algorithm_version = "test-invalidated-family".to_string();
        fs::write(path, serde_json::to_vec_pretty(&receipt)?)?;
        Ok(())
    }

    #[test]
    fn family_invalidation_does_not_touch_unrelated_metrics_or_patrol() -> Result<()> {
        let data_dir = TestDir::new()?;
        let output_dir = TestDir::new()?;
        let wiki = "family-wiki";
        write_input_parquet(&data_dir, wiki)?;
        write_partitioned_warehouse_parquet(&data_dir, wiki)?;
        compute_all(wiki, data_dir.path(), output_dir.path())?;

        let metric = |name: &str| output_dir.path().join(wiki).join(format!("{name}.parquet"));
        let gdp = metric("gdp");
        let activity = metric("gdp_activity_tiers");
        let lifecycle = metric("business_funnel");
        let page_week = metric("page_weekly_edits");
        let gdp_before = fs::metadata(&gdp)?.modified()?;
        let lifecycle_before = fs::metadata(&lifecycle)?.modified()?;
        let page_week_before = fs::metadata(&page_week)?.modified()?;
        let activity_before = fs::metadata(&activity)?.modified()?;
        let gdp_bytes = fs::read(&gdp)?;
        let activity_bytes = fs::read(&activity)?;
        let lifecycle_bytes = fs::read(&lifecycle)?;
        let page_week_bytes = fs::read(&page_week)?;

        std::thread::sleep(std::time::Duration::from_millis(10));
        invalidate_family_receipt(output_dir.path(), wiki, MetricFamily::ActivityTiers)?;
        compute_all(wiki, data_dir.path(), output_dir.path())?;
        assert_eq!(fs::metadata(&gdp)?.modified()?, gdp_before);
        assert_eq!(fs::metadata(&lifecycle)?.modified()?, lifecycle_before);
        assert_eq!(fs::metadata(&page_week)?.modified()?, page_week_before);
        assert!(fs::metadata(&activity)?.modified()? > activity_before);
        assert_eq!(fs::read(&activity)?, activity_bytes);

        let gdp_after_activity = fs::metadata(&gdp)?.modified()?;
        let lifecycle_after_activity = fs::metadata(&lifecycle)?.modified()?;
        let page_week_after_activity = fs::metadata(&page_week)?.modified()?;
        std::thread::sleep(std::time::Duration::from_millis(10));
        invalidate_family_receipt(output_dir.path(), wiki, MetricFamily::PageWeek)?;
        compute_all(wiki, data_dir.path(), output_dir.path())?;
        assert_eq!(fs::metadata(&gdp)?.modified()?, gdp_after_activity);
        assert_eq!(
            fs::metadata(&lifecycle)?.modified()?,
            lifecycle_after_activity
        );
        assert!(fs::metadata(&page_week)?.modified()? > page_week_after_activity);
        assert_eq!(fs::read(&page_week)?, page_week_bytes);

        let gdp_after_page_week = fs::metadata(&gdp)?.modified()?;
        let page_week_after_page_week = fs::metadata(&page_week)?.modified()?;
        let lifecycle_before_rebuild = fs::metadata(&lifecycle)?.modified()?;
        std::thread::sleep(std::time::Duration::from_millis(10));
        invalidate_family_receipt(output_dir.path(), wiki, MetricFamily::Lifecycle)?;
        compute_all(wiki, data_dir.path(), output_dir.path())?;
        assert_eq!(fs::metadata(&gdp)?.modified()?, gdp_after_page_week);
        assert_eq!(
            fs::metadata(&page_week)?.modified()?,
            page_week_after_page_week
        );
        assert!(fs::metadata(&lifecycle)?.modified()? > lifecycle_before_rebuild);
        assert_eq!(fs::read(&lifecycle)?, lifecycle_bytes);

        let lifecycle_after_rebuild = fs::metadata(&lifecycle)?.modified()?;
        let page_week_after_lifecycle = fs::metadata(&page_week)?.modified()?;
        let gdp_before_rebuild = fs::metadata(&gdp)?.modified()?;
        std::thread::sleep(std::time::Duration::from_millis(10));
        invalidate_family_receipt(output_dir.path(), wiki, MetricFamily::Monthly)?;
        compute_all(wiki, data_dir.path(), output_dir.path())?;
        assert!(fs::metadata(&gdp)?.modified()? > gdp_before_rebuild);
        assert_eq!(
            fs::metadata(&lifecycle)?.modified()?,
            lifecycle_after_rebuild
        );
        assert_eq!(
            fs::metadata(&page_week)?.modified()?,
            page_week_after_lifecycle
        );
        assert_eq!(fs::read(&gdp)?, gdp_bytes);

        let family_receipts_before = MetricFamily::CORE
            .into_iter()
            .map(|family| fs::read(family_stage_receipt(output_dir.path(), wiki, family)))
            .collect::<std::io::Result<Vec<_>>>()?;
        let patrol_dir = data_dir.path().join("patrol").join(wiki);
        fs::create_dir_all(&patrol_dir)?;
        fs::write(patrol_dir.join("parser-input.changed"), "patrol-only")?;
        compute_all(wiki, data_dir.path(), output_dir.path())?;
        let family_receipts_after = MetricFamily::CORE
            .into_iter()
            .map(|family| fs::read(family_stage_receipt(output_dir.path(), wiki, family)))
            .collect::<std::io::Result<Vec<_>>>()?;
        assert_eq!(family_receipts_after, family_receipts_before);
        Ok(())
    }

    #[test]
    fn complete_nonweekly_rebuild_scans_each_partition_once() -> Result<()> {
        let data_dir = TestDir::new()?;
        let output_dir = TestDir::new()?;
        let wiki = "scan-fusion-wiki";
        write_partitioned_base_parquet(&data_dir, wiki)?;
        let layer = storage::active_compute_layer(
            data_dir.path(),
            wiki,
            storage::GenerationLayer::Analytical,
        )
        .expect("test generation should expose an analytical layer");
        let expected = storage::active_partition_specs(data_dir.path(), wiki, layer)
            .expect("test generation should expose partition specs")
            .len();
        let scanned = compute_all_incremental(
            wiki,
            data_dir.path(),
            output_dir.path(),
            None,
            ComputePlan::all_recompute(),
        )
        .expect("complete nonweekly rebuild should succeed");
        assert_eq!(scanned, expected);
        Ok(())
    }

    #[test]
    fn partitioned_monthly_scan_flushes_at_a_year_boundary() -> Result<()> {
        let data_dir = TestDir::new()?;
        let output_dir = TestDir::new()?;
        let wiki = "multi-year-scan-wiki";
        write_partitioned_base_parquet(&data_dir, wiki)?;
        let next_year = storage::month_partition_dir(
            &storage::analytical_wiki_dir(data_dir.path(), wiki),
            2025,
            "2025-01",
        );
        fs::create_dir_all(&next_year)?;
        let mut january = analytical_partition_df(AnalyticalPartitionRows {
            year_month: ["2025-01", "2025-01"],
            year_month_key: [202501, 202501],
            user_type: ["registered", "registered"],
            event_user_id: [1, 4],
            page_namespace: [0, 0],
            revision_id: [14, 15],
            revision_text_bytes_diff: [3, 9],
            is_reverted: [false, false],
            is_minor: [false, false],
        })?;
        ParquetWriter::new(&mut fs::File::create(next_year.join("part-000.parquet"))?)
            .finish(&mut january)?;

        compute_all_incremental(
            wiki,
            data_dir.path(),
            output_dir.path(),
            None,
            only_family(MetricFamily::Monthly),
        )
        .expect("multi-year monthly compute should succeed");
        let inequality_path = output_dir.path().join(wiki).join("inequality.parquet");
        let inequality_file =
            fs::File::open(inequality_path).expect("multi-year inequality output should exist");
        let inequality = ParquetReader::new(inequality_file)
            .finish()
            .expect("multi-year inequality output should be readable");
        assert!(
            inequality
                .column("period")?
                .str()?
                .iter()
                .flatten()
                .any(|value| value == "2025")
        );
        Ok(())
    }

    fn only_family(family: MetricFamily) -> ComputePlan {
        let mut plan = ComputePlan {
            monthly: Invalidation::Reuse,
            activity_tiers: Invalidation::Reuse,
            lifecycle: Invalidation::Reuse,
            page_week: Invalidation::Reuse,
        };
        plan.set_invalidation(family, Invalidation::Recompute);
        plan
    }

    #[test]
    #[should_panic(expected = "patrol is not a core compute family")]
    fn core_plan_rejects_patrol_lookup() {
        ComputePlan::all_recompute().invalidation(MetricFamily::Patrol);
    }

    #[test]
    #[should_panic(expected = "patrol is not a core compute family")]
    fn core_plan_rejects_patrol_assignment() {
        let mut plan = ComputePlan::all_recompute();
        plan.set_invalidation(MetricFamily::Patrol, Invalidation::Reuse);
    }

    #[test]
    #[should_panic(expected = "patrol has its own compute stage")]
    fn core_stage_spec_rejects_patrol() {
        let _ = family_stage_spec(MetricFamily::Patrol, "testwiki", None, "v1");
    }

    #[test]
    #[should_panic(expected = "patrol is not a core compute family")]
    fn core_only_family_fixture_rejects_patrol() {
        let _ = only_family(MetricFamily::Patrol);
    }

    #[test]
    fn partitioned_scan_fusion_runs_only_the_requested_nonweekly_accumulators() -> Result<()> {
        let data_dir = TestDir::new()?;
        let wiki = "selective-scan-wiki";
        write_partitioned_base_parquet(&data_dir, wiki)?;

        let reused_output = TestDir::new()?;
        assert_eq!(
            compute_all_incremental(
                wiki,
                data_dir.path(),
                reused_output.path(),
                None,
                only_family(MetricFamily::PageWeek),
            )
            .expect("page-week-only plan should skip the nonweekly scan"),
            0
        );

        for (family, present, absent) in [
            (MetricFamily::Monthly, "gdp", "gdp_activity_tiers"),
            (
                MetricFamily::ActivityTiers,
                "gdp_activity_tiers",
                "business_funnel",
            ),
            (MetricFamily::Lifecycle, "business_funnel", "gdp"),
        ] {
            let output = TestDir::new()?;
            assert_eq!(
                compute_all_incremental(
                    wiki,
                    data_dir.path(),
                    output.path(),
                    None,
                    only_family(family),
                )
                .expect("selected nonweekly family should compute"),
                2
            );
            assert!(
                output
                    .path()
                    .join(wiki)
                    .join(format!("{present}.parquet"))
                    .is_file()
            );
            assert!(
                !output
                    .path()
                    .join(wiki)
                    .join(format!("{absent}.parquet"))
                    .exists()
            );
        }
        Ok(())
    }

    #[test]
    fn selective_compute_propagates_family_writer_weekly_and_receipt_failures() -> Result<()> {
        let base = analytical_partition_df(AnalyticalPartitionRows {
            year_month: ["2024-01", "2024-01"],
            year_month_key: [202401, 202401],
            user_type: ["registered", "registered"],
            event_user_id: [1, 2],
            page_namespace: [0, 0],
            revision_id: [1, 2],
            revision_text_bytes_diff: [10, 20],
            is_reverted: [false, false],
            is_minor: [false, true],
        })
        .expect("analytical failure fixture should be valid");
        let monthly_output = TestDir::new()?;
        fs::create_dir_all(
            monthly_output
                .path()
                .join("failure-wiki/inequality.parquet"),
        )
        .expect("monthly output conflict fixture should be created");
        assert!(
            compute_nonweekly_flat(
                "failure-wiki",
                &base,
                monthly_output.path(),
                only_family(MetricFamily::Monthly),
            )
            .is_err()
        );

        let malformed_monthly_output =
            TestDir::new().expect("malformed output fixture should be creatable");
        let malformed_labor =
            DataFrame::new_infer_height(vec![Column::new("unexpected".into(), [1_i64])])
                .expect("malformed labor fixture should still be a valid DataFrame");
        assert!(
            write_monthly_outputs(
                "malformed-monthly",
                None,
                malformed_monthly_output.path(),
                MonthlyFrames {
                    inequality_frames: vec![
                        inequality::compute_frame(&base)
                            .expect("inequality fixture should compute")
                    ],
                    gdp_frames: vec![gdp_monthly_frame(&base).expect("GDP fixture should compute")],
                    gdp_type_frames: vec![
                        gdp_type_share_frame(&base).expect("GDP share fixture should compute")
                    ],
                    identity_coverage_frames: vec![
                        editor_identity_coverage_frame(&base)
                            .expect("identity coverage fixture should compute")
                    ],
                    labor_monthly_frames: vec![malformed_labor],
                },
            )
            .is_err(),
            "missing labor sort keys must fail before publication"
        );

        let partitioned_data =
            TestDir::new().expect("partitioned input fixture should be creatable");
        let partitioned_output =
            TestDir::new().expect("partitioned output fixture should be creatable");
        write_partitioned_base_parquet(&partitioned_data, "partitioned-failure")
            .expect("partitioned failure fixture should be writable");
        fs::create_dir_all(
            partitioned_output
                .path()
                .join("partitioned-failure/inequality.parquet"),
        )
        .expect("partitioned output conflict fixture should be created");
        assert!(
            compute_all_incremental(
                "partitioned-failure",
                partitioned_data.path(),
                partitioned_output.path(),
                None,
                only_family(MetricFamily::Monthly),
            )
            .is_err(),
            "partitioned monthly writer failures must propagate"
        );

        let activity_output = TestDir::new()?;
        fs::create_dir_all(
            activity_output
                .path()
                .join("failure-wiki/gdp_activity_tiers.parquet"),
        )
        .expect("activity output conflict fixture should be created");
        assert!(
            compute_nonweekly_flat(
                "failure-wiki",
                &base,
                activity_output.path(),
                only_family(MetricFamily::ActivityTiers),
            )
            .is_err()
        );

        let invalid_weekly_data = TestDir::new()?;
        let invalid_weekly_output = TestDir::new()?;
        write_input_parquet(&invalid_weekly_data, "invalid-weekly")?;
        let invalid_partition = storage::month_partition_dir(
            &storage::warehouse_wiki_dir(invalid_weekly_data.path(), "invalid-weekly"),
            2024,
            "2024-01",
        );
        fs::create_dir_all(&invalid_partition)?;
        fs::write(invalid_partition.join("broken.parquet"), "not parquet")?;
        assert!(
            compute_all(
                "invalid-weekly",
                invalid_weekly_data.path(),
                invalid_weekly_output.path(),
            )
            .is_err()
        );

        let receipt_data = TestDir::new()?;
        let receipt_output = TestDir::new()?;
        write_input_parquet(&receipt_data, "receipt-failure")?;
        write_partitioned_warehouse_parquet(&receipt_data, "receipt-failure")?;
        fs::create_dir_all(family_stage_receipt(
            receipt_output.path(),
            "receipt-failure",
            MetricFamily::Monthly,
        ))
        .expect("receipt publication conflict fixture should be created");
        let error = compute_all(
            "receipt-failure",
            receipt_data.path(),
            receipt_output.path(),
        )
        .expect_err("receipt publication conflict should fail compute");
        assert!(error.to_string().contains("failed to record monthly"));
        Ok(())
    }

    #[test]
    fn versioned_compute_falls_back_when_ingest_receipt_is_missing() -> Result<()> {
        let data_dir = TestDir::new()?;
        let output_dir = TestDir::new()?;
        let wiki = "testwiki";
        write_input_parquet(&data_dir, wiki)?;
        write_partitioned_warehouse_parquet(&data_dir, wiki)?;
        assert!(
            !legacy_compute_inputs(wiki, data_dir.path(), None)
                .expect("legacy unversioned inputs should resolve")
                .is_empty()
        );
        let version = "2026-08";
        let analytical = storage::snapshot_analytical_wiki_dir(data_dir.path(), wiki, version)?;
        let warehouse = storage::snapshot_warehouse_wiki_dir(data_dir.path(), wiki, version)?;
        for (source, destination) in [
            (
                storage::analytical_wiki_dir(data_dir.path(), wiki),
                &analytical,
            ),
            (
                storage::warehouse_wiki_dir(data_dir.path(), wiki),
                &warehouse,
            ),
        ] {
            for path in storage::collect_parquet_files(&source)? {
                let relative = path.strip_prefix(&source)?;
                let target = if relative.components().count() == 1 {
                    destination
                        .join("year=2026/year_month=2026-01")
                        .join(relative)
                } else {
                    destination.join(relative)
                };
                target.parent().map(fs::create_dir_all).transpose()?;
                fs::copy(path, target)?;
            }
        }
        storage::write_test_generation_manifest_from_files(data_dir.path(), wiki, version)?;
        storage::publish_test_snapshot_pointer(data_dir.path(), wiki, version)?;

        let before_rows = load_wiki(wiki, data_dir.path())?.height();
        let listed = storage::active_fragment_files(
            data_dir.path(),
            wiki,
            storage::GenerationLayer::Analytical,
        )
        .expect("selected analytical fragments should resolve");
        let unlisted = listed[0].with_file_name("unlisted-but-valid.parquet");
        fs::copy(&listed[0], &unlisted)?;
        assert_eq!(load_wiki(wiki, data_dir.path())?.height(), before_rows);
        let active_again = storage::active_fragment_files(
            data_dir.path(),
            wiki,
            storage::GenerationLayer::Analytical,
        )
        .expect("manifest allowlist should remain readable");
        assert!(!active_again.contains(&unlisted));

        let inputs = compute_stage_inputs(wiki, data_dir.path(), None)?;
        assert_eq!(inputs.len(), 1);
        assert_eq!(inputs[0].identity, "generation-manifest");

        let receipt =
            fingerprint::data_stage_receipt_path(data_dir.path(), wiki, version, "ingest");
        fingerprint::record(
            &receipt,
            fingerprint::StageSpec {
                stage: "ingest",
                scope: wiki,
                selected_snapshot: Some(version),
                algorithm_version: crate::ingest::INGEST_ALGORITHM_VERSION,
            },
            &inputs,
            &inputs,
        )
        .expect("ingest receipt fixture should record");
        let reused = compute_stage_inputs(wiki, data_dir.path(), None)?;
        assert_eq!(reused.len(), 1);
        assert_eq!(reused[0].identity, "stage/ingest/testwiki/2026-08");

        storage::restore_current_snapshot(data_dir.path(), wiki, None)?;
        storage::restore_current_snapshot(data_dir.path(), wiki, None)?;
        compute_all_for_snapshot(wiki, version, data_dir.path(), output_dir.path())?;
        assert!(output_dir.path().join(wiki).join("gdp.parquet").is_file());
        assert!(
            compute_all_for_snapshot(wiki, "invalid", data_dir.path(), output_dir.path()).is_err()
        );
        Ok(())
    }

    #[test]
    fn legacy_core_receipt_migrates_to_authenticated_family_receipts_without_recompute()
    -> Result<()> {
        let data_dir = TestDir::new()?;
        let output_dir = TestDir::new()?;
        let wiki = "migrationwiki";
        let version = "2026-08";
        write_partitioned_base_parquet(&data_dir, wiki)?;
        write_partitioned_warehouse_parquet(&data_dir, wiki)?;

        for (source, destination) in [
            (
                storage::analytical_wiki_dir(data_dir.path(), wiki),
                storage::snapshot_analytical_wiki_dir(data_dir.path(), wiki, version)?,
            ),
            (
                storage::warehouse_wiki_dir(data_dir.path(), wiki),
                storage::snapshot_warehouse_wiki_dir(data_dir.path(), wiki, version)?,
            ),
        ] {
            for path in storage::collect_parquet_files(&source)? {
                let target = destination.join(path.strip_prefix(&source)?);
                target.parent().map(fs::create_dir_all).transpose()?;
                fs::copy(path, target)?;
            }
        }
        storage::write_test_generation_manifest_from_files(data_dir.path(), wiki, version)?;
        storage::publish_test_snapshot_pointer(data_dir.path(), wiki, version)?;
        let (snapshot_plan, _) =
            crate::snapshot_plan::SnapshotPlan::load_or_resolve(data_dir.path(), wiki, version)?;
        let source_sizes = vec![Some(1); snapshot_plan.sources.len()];
        workload_profile::load_or_select(data_dir.path(), &snapshot_plan, &source_sizes)?;
        compute_all_for_snapshot(wiki, version, data_dir.path(), output_dir.path())?;

        let weekly_config =
            WeeklyAggregationConfig::for_snapshot(data_dir.path(), wiki, Some(version))?;
        let legacy_algorithm = weekly_config.legacy_algorithm_version();
        let legacy_inputs = legacy_compute_inputs(wiki, data_dir.path(), Some(version))?;
        let legacy_outputs = compute_stage_outputs(wiki, output_dir.path());
        fingerprint::record(
            &legacy_compute_stage_receipt(output_dir.path(), wiki),
            fingerprint::StageSpec {
                stage: "compute",
                scope: wiki,
                selected_snapshot: Some(version),
                algorithm_version: &legacy_algorithm,
            },
            &legacy_inputs,
            &legacy_outputs,
        )
        .expect("legacy combined receipt should be recorded");
        fs::remove_dir_all(output_dir.path().join("_stages/compute/monthly"))?;
        fs::remove_dir_all(output_dir.path().join("_stages/compute/activity_tiers"))?;
        fs::remove_dir_all(output_dir.path().join("_stages/compute/lifecycle"))?;
        fs::remove_dir_all(output_dir.path().join("_stages/compute/page_week"))?;
        let artifacts_before = legacy_outputs
            .iter()
            .map(|output| fs::read(&output.path))
            .collect::<std::io::Result<Vec<_>>>()?;

        let blocked_receipt =
            family_stage_receipt(output_dir.path(), wiki, MetricFamily::ActivityTiers);
        fs::create_dir_all(&blocked_receipt)?;
        assert!(
            migrate_legacy_compute_receipt(
                wiki,
                version,
                data_dir.path(),
                output_dir.path(),
                &weekly_config,
            )
            .is_err()
        );
        fs::remove_dir(blocked_receipt)?;

        migrate_legacy_compute_receipt(
            wiki,
            version,
            data_dir.path(),
            output_dir.path(),
            &weekly_config,
        )
        .expect("compatible legacy family receipts should migrate");
        assert_eq!(
            legacy_outputs
                .iter()
                .map(|output| fs::read(&output.path))
                .collect::<std::io::Result<Vec<_>>>()?,
            artifacts_before
        );
        assert!(
            !family_stage_receipt(output_dir.path(), wiki, MetricFamily::Monthly).exists(),
            "monthly must rebuild once because its total ordering changed"
        );
        for family in [
            MetricFamily::ActivityTiers,
            MetricFamily::Lifecycle,
            MetricFamily::PageWeek,
        ] {
            assert!(family_stage_receipt(output_dir.path(), wiki, family).is_file());
        }
        let nonmonthly_before = [
            MetricFamily::ActivityTiers,
            MetricFamily::Lifecycle,
            MetricFamily::PageWeek,
        ]
        .into_iter()
        .flat_map(|family| family_outputs(family, wiki, output_dir.path()))
        .map(|output| fs::read(output.path))
        .collect::<std::io::Result<Vec<_>>>()?;

        compute_all_for_snapshot(wiki, version, data_dir.path(), output_dir.path())?;
        assert_eq!(
            [
                MetricFamily::ActivityTiers,
                MetricFamily::Lifecycle,
                MetricFamily::PageWeek,
            ]
            .into_iter()
            .flat_map(|family| family_outputs(family, wiki, output_dir.path()))
            .map(|output| fs::read(output.path))
            .collect::<std::io::Result<Vec<_>>>()?,
            nonmonthly_before
        );
        let receipts_before = MetricFamily::CORE
            .into_iter()
            .map(|family| fs::read(family_stage_receipt(output_dir.path(), wiki, family)))
            .collect::<std::io::Result<Vec<_>>>()?;
        for family in MetricFamily::CORE {
            assert!(
                family_is_reusable(
                    family,
                    wiki,
                    Some(version),
                    data_dir.path(),
                    output_dir.path(),
                    &weekly_config,
                )
                .expect("fresh family receipt should validate")
            );
        }

        migrate_legacy_compute_receipt(
            wiki,
            version,
            data_dir.path(),
            output_dir.path(),
            &weekly_config,
        )
        .expect("repeated legacy migration should be idempotent");
        assert_eq!(
            MetricFamily::CORE
                .into_iter()
                .map(|family| fs::read(family_stage_receipt(output_dir.path(), wiki, family)))
                .collect::<std::io::Result<Vec<_>>>()?,
            receipts_before
        );

        for family in MetricFamily::CORE {
            let receipt = family_stage_receipt(output_dir.path(), wiki, family);
            let parent = receipt
                .parent()
                .expect("family receipt should have a parent");
            fs::remove_dir_all(parent).expect("family receipt directory should be removable");
        }
        let legacy_path = legacy_compute_stage_receipt(output_dir.path(), wiki);
        let mut invalid_legacy = fingerprint::read_receipt(&legacy_path)?;
        invalid_legacy.algorithm_version = "obsolete-legacy-version".to_string();
        fs::write(&legacy_path, serde_json::to_vec_pretty(&invalid_legacy)?)?;
        migrate_legacy_compute_receipt(
            wiki,
            version,
            data_dir.path(),
            output_dir.path(),
            &weekly_config,
        )
        .expect("incompatible legacy receipt should remain a safe no-op");
        assert!(
            family_receipt_identities(wiki, version, data_dir.path(), output_dir.path(),).is_err()
        );

        fs::remove_file(&legacy_path).expect("invalid legacy receipt should be removable");

        fs::write(
            storage::generation_manifest_path(data_dir.path(), wiki, version)?,
            "corrupt generation manifest",
        )
        .expect("generation corruption fixture should be written");
        assert!(
            reusable_candidate_families(wiki, version, data_dir.path(), output_dir.path(),)
                .is_err()
        );
        Ok(())
    }

    #[test]
    fn page_weekly_edits_computes_week_over_week_variation() -> Result<()> {
        init_test_tracing();
        let data_dir = TestDir::new()?;
        let output_dir = TestDir::new()?;
        let wiki = "testwiki";

        write_partitioned_base_parquet(&data_dir, wiki)?;
        write_partitioned_warehouse_parquet(&data_dir, wiki)?;
        compute_all(wiki, data_dir.path(), output_dir.path())?;

        let weekly_path = output_dir
            .path()
            .join(wiki)
            .join("page_weekly_edits.parquet");
        let weekly_path = weekly_path.to_string_lossy().to_string();
        let weekly = LazyFrame::scan_parquet(weekly_path.as_str().into(), Default::default())?
            .filter(col("page_id").eq(lit(10_i64)))
            .sort(["week_start"], Default::default())
            .collect()?;

        let week_start: Vec<String> = weekly
            .column("week_start")?
            .str()?
            .iter()
            .flatten()
            .map(ToOwned::to_owned)
            .collect();
        let edits: Vec<u32> = weekly.column("edits")?.u32()?.iter().flatten().collect();
        let previous_week_edits: Vec<u32> = weekly
            .column("previous_week_edits")?
            .u32()?
            .iter()
            .flatten()
            .collect();
        let wow_change: Vec<i64> = weekly
            .column("wow_change")?
            .i64()?
            .iter()
            .flatten()
            .collect();
        let wow_rate: Vec<Option<f64>> = weekly.column("wow_rate")?.f64()?.iter().collect();

        assert_eq!(
            week_start,
            vec![
                "2024-01-01".to_string(),
                "2024-01-08".to_string(),
                "2024-01-29".to_string(),
                "2024-02-05".to_string(),
            ]
        );
        assert_eq!(edits, vec![2, 1, 1, 1]);
        assert_eq!(previous_week_edits, vec![0, 2, 0, 1]);
        assert_eq!(wow_change, vec![2, -1, 1, 0]);
        assert_eq!(wow_rate, vec![None, Some(-0.5), None, Some(0.0)]);

        Ok(())
    }

    #[test]
    fn page_weekly_edits_conserves_revisions_with_null_page_identity() -> Result<()> {
        let data_dir = TestDir::new()?;
        let output_dir = TestDir::new()?;
        let wiki = "testwiki";
        write_null_page_warehouse_parquet(&data_dir, wiki)?;
        let config = WeeklyAggregationConfig::new(DEFAULT_WEEKLY_BUCKET_COUNT, None)?;

        let report = compute_page_weekly_edits(wiki, data_dir.path(), output_dir.path(), &config)?
            .context("null-page fixture should produce a weekly report")?;
        assert_eq!(report.total_edits, 2);
        assert_eq!(report.output_rows, 2);

        let output = ParquetReader::new(
            File::open(
                output_dir
                    .path()
                    .join(wiki)
                    .join("page_weekly_edits.parquet"),
            )
            .expect("weekly null-page output should be readable"),
        )
        .finish()?;
        let null_page = output.lazy().filter(col("page_id").is_null()).collect()?;
        assert_eq!(null_page.height(), 1);
        assert_eq!(null_page.column("edits")?.u32()?.get(0), Some(1));
        assert_eq!(
            null_page.column("week_start")?.str()?.get(0),
            Some("2002-06-10")
        );
        Ok(())
    }

    #[test]
    fn page_weekly_output_is_byte_deterministic_and_cleans_runs() -> Result<()> {
        let data_dir = TestDir::new()?;
        let first_output = TestDir::new()?;
        let second_output = TestDir::new()?;
        let wiki = "testwiki";
        write_partitioned_warehouse_parquet(&data_dir, wiki)?;
        let config = WeeklyAggregationConfig::new(DEFAULT_WEEKLY_BUCKET_COUNT, None)?;

        compute_page_weekly_edits(wiki, data_dir.path(), first_output.path(), &config)?;
        compute_page_weekly_edits(wiki, data_dir.path(), second_output.path(), &config)?;
        let relative = Path::new(wiki).join("page_weekly_edits.parquet");
        assert_eq!(
            fs::read(first_output.path().join(&relative))?,
            fs::read(second_output.path().join(&relative))?
        );
        for output in [first_output.path(), second_output.path()] {
            assert!(fs::read_dir(output.join(wiki))?.all(|entry| {
                !entry
                    .expect("readable output entry")
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".page_weekly_edits-runs-")
            }));
        }
        Ok(())
    }

    #[test]
    fn supported_bucket_counts_produce_identical_weekly_results() -> Result<()> {
        let data_dir = TestDir::new()?;
        let output_root = TestDir::new()?;
        let scratch_root = TestDir::new()?;
        let wiki = "testwiki";
        write_partitioned_warehouse_parquet(&data_dir, wiki)?;
        let mut expected: Option<DataFrame> = None;

        for bucket_count in FLAT_BENCHMARK_BUCKET_COUNTS {
            let output = output_root.path().join(bucket_count.to_string());
            let config =
                WeeklyAggregationConfig::new(bucket_count, Some(scratch_root.path().to_path_buf()))
                    .expect("supported bucket count");
            let report = compute_page_weekly_edits(wiki, data_dir.path(), &output, &config)?
                .context("weekly fixture should produce a report")?;
            assert_eq!(report.bucket_count, bucket_count);
            assert_eq!(report.total_edits, 6);
            assert!(report.scratch_peak_bytes > 0);

            let path = output.join(wiki).join("page_weekly_edits.parquet");
            let frame = sort_frame(
                ParquetReader::new(File::open(path)?).finish()?,
                weekly_sort_keys(),
            )
            .expect("weekly output should sort");
            if let Some(expected) = &expected {
                assert!(expected.equals_missing(&frame));
            } else {
                expected = Some(frame);
            }
        }
        Ok(())
    }

    #[test]
    fn two_level_weekly_buckets_match_flat_output_and_conserve_every_level() -> Result<()> {
        let data_dir = TestDir::new()?;
        let output_root = TestDir::new()?;
        let scratch_root = TestDir::new()?;
        let wiki = "testwiki";
        write_partitioned_warehouse_parquet(&data_dir, wiki)?;

        let flat_output = output_root.path().join("flat");
        let flat = WeeklyAggregationConfig::new(256, Some(scratch_root.path().to_path_buf()))?;
        compute_page_weekly_edits(wiki, data_dir.path(), &flat_output, &flat)?;

        let two_level_output = output_root.path().join("two-level-first");
        let repeated_output = output_root.path().join("two-level-repeated");
        let two_level =
            WeeklyAggregationConfig::new_two_level(64, 16, Some(scratch_root.path().to_path_buf()))
                .expect("supported two-level layout");
        let report =
            compute_page_weekly_edits(wiki, data_dir.path(), &two_level_output, &two_level)?
                .context("two-level fixture should produce a report")?;

        assert_eq!(report.bucket_count, 1_024);
        assert_eq!(report.primary_bucket_count, 64);
        assert_eq!(report.secondary_bucket_count, 16);
        assert_eq!(report.primary_bucket_staged_rows.len(), 64);
        assert_eq!(report.bucket_staged_rows.len(), 1_024);
        assert_eq!(report.primary_bucket_staged_rows.iter().sum::<usize>(), 5);
        assert_eq!(report.bucket_staged_rows.iter().sum::<usize>(), 5);
        assert_eq!(report.total_edits, 6);

        let relative = Path::new(wiki).join("page_weekly_edits.parquet");
        compute_page_weekly_edits(wiki, data_dir.path(), &repeated_output, &two_level)?;
        assert_eq!(
            fs::read(two_level_output.join(&relative))?,
            fs::read(repeated_output.join(&relative))?
        );
        let flat_frame = sort_frame(
            ParquetReader::new(File::open(flat_output.join(&relative))?).finish()?,
            weekly_sort_keys(),
        )
        .expect("flat weekly output should sort");
        let two_level_frame = sort_frame(
            ParquetReader::new(File::open(two_level_output.join(&relative))?).finish()?,
            weekly_sort_keys(),
        )
        .expect("two-level weekly output should sort");
        assert!(flat_frame.equals_missing(&two_level_frame));
        assert!(fs::read_dir(scratch_root.path())?.all(|entry| {
            !entry
                .expect("readable scratch entry")
                .file_name()
                .to_string_lossy()
                .starts_with(".page_weekly_edits-runs-")
        }));
        Ok(())
    }

    #[test]
    fn checked_conservation_sums_fail_closed_on_overflow() {
        assert_eq!(checked_sum_i64(&[1, 2], "edits").unwrap(), 3);
        assert!(checked_sum_i64(&[i64::MAX, 1], "edits").is_err());
        assert_eq!(checked_sum_usize(&[1, 2], "rows").unwrap(), 3);
        assert!(checked_sum_usize(&[usize::MAX, 1], "rows").is_err());
    }

    #[test]
    fn two_level_routing_rejects_more_secondary_writers_than_budgeted() -> Result<()> {
        let output = TestDir::new()?;
        let runs = WeeklyRunDir::new(output.path(), "testwiki", None)?;
        let mut budget = ResourceBudget::from_environment()?;
        budget.max_active_parquet_writers = 16;
        let governor = ResourceGovernor::new(
            budget,
            GovernorPaths::new(output.path().to_path_buf(), None),
        );
        let config = WeeklyAggregationConfig::new_two_level(64, 32, None)?;
        let mut peak = ResourcePeak::default();
        let error = route_primary_to_secondary_buckets(
            &runs,
            &runs.primary_path(0),
            0,
            &config,
            BucketTotals { rows: 1, edits: 1 },
            &governor,
            &mut peak,
        )
        .expect_err("32 secondary writers must not exceed a 16-writer budget");
        assert!(error.to_string().contains("exceeds the governed"));
        Ok(())
    }

    type WeeklyTestRow<'a> = (
        Option<i64>,
        Option<i32>,
        Option<&'a str>,
        Option<i32>,
        Option<u32>,
    );

    fn weekly_batch_df(rows: &[WeeklyTestRow<'_>]) -> Result<DataFrame> {
        df!(
            "page_id" => rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            "page_namespace" => rows.iter().map(|r| r.1).collect::<Vec<_>>(),
            "page_title" => rows.iter().map(|r| r.2).collect::<Vec<_>>(),
            "week_start" => rows.iter().map(|r| r.3).collect::<Vec<_>>(),
            "edits" => rows.iter().map(|r| r.4).collect::<Vec<_>>(),
        )
        .and_then(|df| {
            df.lazy()
                .with_column(col("week_start").cast(DataType::Date))
                .collect()
        })
        .map_err(Into::into)
    }

    fn governed_worker_fixture(output: &Path, workers: usize) -> Result<Vec<u8>> {
        let runs = WeeklyRunDir::new(output, "testwiki", None)?;
        let fixture = [
            weekly_batch_df(&[
                (Some(1), Some(0), Some("Alpha"), Some(0), Some(3)),
                (Some(1), Some(0), Some("Alpha"), Some(7), Some(5)),
            ])?,
            weekly_batch_df(&[
                (Some(2), Some(0), Some("Beta"), Some(0), Some(7)),
                (Some(2), Some(0), Some("Beta"), Some(7), Some(11)),
            ])?,
        ];
        let mut prepared = Vec::new();
        for (logical_bucket, mut frame) in fixture.into_iter().enumerate() {
            let staged_path = runs.secondary_path(0, logical_bucket);
            ParquetWriter::new(File::create(&staged_path)?).finish(&mut frame)?;
            prepared.push(PreparedWeeklyBucket {
                logical_bucket,
                primary_bucket: 0,
                secondary_bucket: logical_bucket,
                staged_rows: 2,
                staged_path: Some(staged_path),
            });
        }
        let mut budget = ResourceBudget::from_environment()?;
        budget.memory_ceiling_bytes = u64::MAX;
        budget.memory_reserve_bytes = 0;
        budget.scratch_limit_bytes = u64::MAX;
        budget.max_open_files = usize::MAX;
        budget.thread_limit = workers;
        budget.weekly_worker_limit = workers;
        let governor = ResourceGovernor::new(
            budget,
            GovernorPaths::new(output.to_path_buf(), Some(runs.path().to_path_buf())),
        );
        let mut results = Vec::new();
        for batch in prepared.chunks(workers) {
            let reconciled =
                reconcile_weekly_bucket_batch(&runs, &[], batch.to_vec(), "testwiki", &governor);
            results.extend(reconciled?);
        }
        let final_path = output.join(format!("workers-{workers}.parquet"));
        let mut writer = None;
        let mut total_edits = 0;
        let mut output_rows = 0;
        let mut minimum = None;
        let mut maximum = None;
        let mut scratch_peak = 0;
        let mut working_peak = 0;
        let mut resource_peak = ResourcePeak::default();
        let append = append_weekly_bucket_results(
            &runs,
            results,
            &final_path,
            &mut writer,
            &mut total_edits,
            &mut output_rows,
            &mut minimum,
            &mut maximum,
            &mut scratch_peak,
            &mut working_peak,
            &mut resource_peak,
        );
        append?;
        writer
            .context("worker fixture produced no output")?
            .finish()?;
        assert_eq!(total_edits, 26);
        assert_eq!(output_rows, 4);
        assert_eq!(governor.sample()?.active_bucket_workers, 0);
        fs::read(final_path).map_err(Into::into)
    }

    #[test]
    fn weekly_output_bytes_are_identical_with_one_and_two_bucket_workers() -> Result<()> {
        let output = TestDir::new()?;
        let serial = governed_worker_fixture(output.path(), 1)?;
        let parallel = governed_worker_fixture(output.path(), 2)?;
        assert_eq!(serial, parallel);
        Ok(())
    }

    #[test]
    fn large_logical_topology_caps_writers_and_reclaims_scratch_per_unit() -> Result<()> {
        let output = TestDir::new()?;
        let runs = WeeklyRunDir::new(output.path(), "testwiki", None)?;
        let config = WeeklyAggregationConfig::new_two_level(64, 32, None)?;
        assert_eq!(config.logical_bucket_count(), 2_048);

        let mut page_ids = vec![None; config.secondary_bucket_count];
        for page_id in 0_i64..1_000_000 {
            let hash = determinism::stable_page_hash(Some(page_id));
            if hash as usize & (config.primary_bucket_count - 1) != 0 {
                continue;
            }
            let secondary = (hash >> config.primary_bucket_count.trailing_zeros()) as usize
                & (config.secondary_bucket_count - 1);
            page_ids[secondary].get_or_insert(page_id);
            if page_ids.iter().all(Option::is_some) {
                break;
            }
        }
        anyhow::ensure!(
            page_ids.iter().all(Option::is_some),
            "fixture could not cover every secondary bucket"
        );
        let rows = page_ids
            .iter()
            .map(|page_id| (*page_id, Some(0), Some("Page"), Some(19_723), Some(1)))
            .collect::<Vec<_>>();
        let mut frame = weekly_batch_df(&rows)?;
        let primary_path = runs.primary_path(0);
        ParquetWriter::new(File::create(&primary_path)?).finish(&mut frame)?;

        let mut budget = ResourceBudget::from_environment()?;
        budget.memory_ceiling_bytes = u64::MAX;
        budget.memory_reserve_bytes = 0;
        budget.scratch_limit_bytes = u64::MAX;
        budget.max_open_files = usize::MAX;
        budget.max_active_parquet_writers = 32;
        let governor = ResourceGovernor::new(
            budget,
            GovernorPaths::new(output.path().to_path_buf(), None),
        );
        let mut peak = ResourcePeak::default();
        let routing = route_primary_to_secondary_buckets(
            &runs,
            &primary_path,
            0,
            &config,
            BucketTotals {
                rows: config.secondary_bucket_count,
                edits: i64::try_from(config.secondary_bucket_count)?,
            },
            &governor,
            &mut peak,
        )
        .expect("bounded routing fixture must succeed");

        assert_eq!(routing.peak_active_writers, 32);
        assert_eq!(routing.paths.iter().flatten().count(), 32);
        assert!(config.logical_bucket_count() > routing.peak_active_writers * 32);
        let mut scratch_bytes = runs.size_bytes()?;
        for path in routing.paths.iter().flatten() {
            reclaim_completed_weekly_scratch(Some(path))?;
            assert!(!path.exists());
            let remaining = runs.size_bytes()?;
            assert!(remaining < scratch_bytes);
            scratch_bytes = remaining;
        }
        assert_eq!(scratch_bytes, fs::metadata(primary_path)?.len());
        Ok(())
    }

    #[test]
    fn weekly_bucket_assignment_is_stable_and_keeps_each_page_together() {
        assert!(stable_weekly_bucket(Some(1), 256) < 256);
        assert_eq!(
            stable_weekly_bucket(Some(42), 256),
            stable_weekly_bucket(Some(42), 256)
        );
        assert_eq!(
            stable_weekly_bucket(None, 256),
            stable_weekly_bucket(None, 256)
        );
        for page_id in [None, Some(-1), Some(0), Some(42), Some(i64::MAX)] {
            assert_eq!(
                stable_weekly_bucket(page_id, 256),
                stable_weekly_bucket(page_id, 512) & 255
            );
            assert_eq!(
                stable_weekly_bucket(page_id, 512),
                stable_weekly_bucket(page_id, 1024) & 511
            );
            assert!(stable_weekly_secondary_bucket(page_id, 64, 32) < 32);
            assert_eq!(
                stable_weekly_secondary_bucket(page_id, 64, 32),
                stable_weekly_secondary_bucket(page_id, 64, 32)
            );
        }
        assert_eq!(format_epoch_day(0).as_deref(), Some("1970-01-01"));
    }

    #[test]
    fn weekly_config_accepts_only_benchmark_bucket_counts() -> Result<()> {
        assert!(WeeklyAggregationConfig::new(256, None).is_ok());
        assert!(WeeklyAggregationConfig::new(512, None).is_ok());
        assert!(WeeklyAggregationConfig::new(1024, None).is_ok());
        assert!(WeeklyAggregationConfig::new(128, None).is_ok());
        assert!(WeeklyAggregationConfig::new(63, None).is_err());
        assert!(WeeklyAggregationConfig::new_two_level(64, 32, None).is_ok());
        assert!(WeeklyAggregationConfig::new_two_level(64, 8, None).is_ok());
        assert_eq!(
            WeeklyAggregationConfig::new_two_level(32, 8, None)?.logical_bucket_count(),
            256
        );
        let default_version = WeeklyAggregationConfig::new(256, None)?.algorithm_version();
        assert!(default_version.starts_with(weekly::ALGORITHM_VERSION));
        assert!(default_version.contains("splitmix64-finalizer-v1-seed0000000000000000"));
        assert!(default_version.ends_with("-primary256-secondary1"));
        assert!(
            WeeklyAggregationConfig::new(512, None)?
                .algorithm_version()
                .ends_with("-primary512-secondary1")
        );
        assert_eq!(
            WeeklyAggregationConfig::from_values(
                Some("1024".into()),
                None,
                None,
                Some("/capacity-scratch".into()),
            )
            .expect("supported configuration"),
            WeeklyAggregationConfig::new(1024, Some(PathBuf::from("/capacity-scratch")))
                .expect("supported configuration")
        );
        assert!(
            WeeklyAggregationConfig::from_values(Some("bad".into()), None, None, None).is_err()
        );
        assert!(
            WeeklyAggregationConfig::from_values(
                Some("256".into()),
                Some("64".into()),
                Some("16".into()),
                None,
            )
            .is_err()
        );
        assert_eq!(
            WeeklyAggregationConfig::from_values(None, Some("64".into()), Some("32".into()), None,)
                .expect("supported explicit environment values")
                .logical_bucket_count(),
            2048
        );
        #[cfg(unix)]
        {
            use std::os::unix::ffi::OsStringExt;
            assert!(
                WeeklyAggregationConfig::from_values(
                    Some(std::ffi::OsString::from_vec(vec![0xff])),
                    None,
                    None,
                    None,
                )
                .is_err()
            );
        }
        assert_eq!(max_option(Some(3), Some(7)), Some(7));
        assert_eq!(
            valid_weekly_run_id(Some("capacity_run-1".into())),
            "capacity_run-1"
        );
        assert_eq!(
            valid_weekly_run_id(Some("invalid/run".into())),
            "standalone"
        );
        Ok(())
    }

    #[test]
    fn capacity_benchmark_rejects_a_missing_warehouse() -> Result<()> {
        let data = TestDir::new()?;
        let output = TestDir::new()?;
        let config = WeeklyAggregationConfig::new(DEFAULT_WEEKLY_BUCKET_COUNT, None)?;
        let error = benchmark_page_weekly_edits("frwiki", data.path(), output.path(), &config)
            .expect_err("capacity benchmark requires warehouse partitions");
        assert!(error.to_string().contains("no warehouse partitions"));
        Ok(())
    }

    #[test]
    fn weekly_partition_staging_writes_predicate_pushdown_runs_and_cleans() -> Result<()> {
        let output = TestDir::new()?;
        let scratch = TestDir::new()?;
        let runs = WeeklyRunDir::new(output.path(), "testwiki", Some(scratch.path()))?;
        let run_path = runs.path().to_path_buf();
        assert!(run_path.starts_with(scratch.path()));
        let frame = weekly_batch_df(&[
            (Some(1), Some(0), Some("Alpha"), Some(0), Some(3)),
            (Some(2), Some(0), Some("Beta"), Some(7), Some(5)),
        ])?;
        let mut rows = vec![0usize; DEFAULT_WEEKLY_BUCKET_COUNT];
        let mut edits = vec![0i64; DEFAULT_WEEKLY_BUCKET_COUNT];
        let path = stage_weekly_partition(
            &runs,
            0,
            &mut rows,
            &mut edits,
            frame,
            DEFAULT_WEEKLY_BUCKET_COUNT,
        )
        .expect("weekly fixture should stage");

        assert_eq!(rows.iter().sum::<usize>(), 2);
        assert!(runs.size_bytes()? > 0);
        assert!(path.exists());
        let alpha_bucket = stable_weekly_bucket(Some(1), DEFAULT_WEEKLY_BUCKET_COUNT);
        let alpha = read_staged_weekly_bucket(std::slice::from_ref(&path), alpha_bucket)?;
        assert_eq!(alpha.height(), 1);
        assert!(alpha.column("_primary_bucket").is_err());
        assert!(
            read_staged_weekly_bucket(&[scratch.path().join("missing.parquet")], alpha_bucket)
                .is_err()
        );
        drop(runs);
        assert!(!run_path.exists());
        Ok(())
    }

    #[test]
    fn previous_week_scan_is_null_safe_and_rejects_duplicate_keys() -> Result<()> {
        let null_page = weekly_batch_df(&[
            (None, Some(0), None, Some(0), Some(3)),
            (None, Some(0), None, Some(7), Some(5)),
            (Some(1), Some(0), Some("Alpha"), Some(21), Some(2)),
        ])?;
        assert_eq!(previous_week_edits(&null_page)?, vec![0, 3, 0]);

        let duplicate = weekly_batch_df(&[
            (Some(1), Some(0), Some("Alpha"), Some(7), Some(2)),
            (Some(1), Some(0), Some("Alpha"), Some(7), Some(3)),
        ])?;
        assert!(
            previous_week_edits(&duplicate)
                .unwrap_err()
                .to_string()
                .contains("duplicate weekly key")
        );

        let null_edits = weekly_batch_df(&[
            (Some(1), Some(0), Some("Alpha"), Some(0), None),
            (Some(1), Some(0), Some("Alpha"), Some(7), Some(3)),
        ])?;
        assert!(
            previous_week_edits(&null_edits)
                .unwrap_err()
                .to_string()
                .contains("null edits")
        );
        Ok(())
    }

    #[test]
    fn pending_output_replaces_atomically_and_cleans_abandoned_temp() -> Result<()> {
        let output = TestDir::new()?;
        let path = output.path().join("metric.parquet");
        fs::write(&path, b"old")?;

        let pending = PendingOutput::new(path.clone())?;
        fs::write(&pending.temp_path, b"new")?;
        assert_eq!(pending.publish()?, 3);
        assert_eq!(fs::read(&path)?, b"new");

        let abandoned = PendingOutput::new(path.clone())?;
        let abandoned_path = abandoned.temp_path.clone();
        fs::write(&abandoned_path, b"partial")?;
        drop(abandoned);
        assert!(!abandoned_path.exists());
        assert_eq!(fs::read(path)?, b"new");
        Ok(())
    }

    #[test]
    fn concat_frames_returns_empty_for_no_input() -> Result<()> {
        let frame = concat_frames(Vec::new())?;
        assert_eq!(frame.height(), 0);
        assert_eq!(frame.width(), 0);
        Ok(())
    }

    #[test]
    fn cross_snapshot_helpers_fail_closed_and_clear_empty_periods() -> Result<()> {
        let root = TestDir::new()?;
        let identity = crate::canonical_month::MonthIdentity {
            schema_version: 1,
            wiki: "testwiki".to_string(),
            event_month: "2024-02".to_string(),
            logical_schema_version: crate::canonical_month::LOGICAL_SCHEMA_VERSION,
            encoding_version: crate::canonical_month::ENCODING_VERSION.to_string(),
            ordering_contract: "test".to_string(),
            digest: "ab".repeat(32),
            rows: 1,
            edits: 1,
        };
        let cache = crate::cross_snapshot::CrossSnapshotCache::for_test(
            root.path(),
            "testwiki",
            vec![identity.clone()],
        );

        let mut editor_month_frames = Vec::new();
        let mut output_frames = Vec::new();
        let mut month_digests = vec!["stale".to_string()];
        let empty_finish = finish_activity_year_cached(
            &mut editor_month_frames,
            &mut output_frames,
            &mut month_digests,
            Some(&cache),
        );
        empty_finish?;
        assert!(month_digests.is_empty());
        assert!(output_frames.is_empty());

        let mut inequality_inputs = Vec::new();
        let mut inequality_outputs = Vec::new();
        let mut inequality_digests = vec!["stale".to_string()];
        finish_inequality_year_cached(
            &mut inequality_inputs,
            &mut inequality_outputs,
            &mut inequality_digests,
            Some(&cache),
        )
        .expect("empty inequality period should be accepted");
        assert!(inequality_digests.is_empty());
        assert!(inequality_outputs.is_empty());

        let inequality_month = || {
            DataFrame::new_infer_height(vec![
                Column::new("year_month".into(), ["2024-02"]),
                Column::new("year_month_key".into(), [202402_i32]),
                Column::new("editor_identity".into(), ["id:1"]),
                Column::new("user_type_rank".into(), [0_i32]),
                Column::new("edits".into(), [1_u32]),
            ])
            .map_err(anyhow::Error::from)
        };
        inequality_inputs.push(inequality_month().expect("inequality fixture should be valid"));
        assert!(
            finish_inequality_year_cached(
                &mut inequality_inputs,
                &mut inequality_outputs,
                &mut inequality_digests,
                Some(&cache),
            )
            .is_err()
        );
        inequality_digests.push(identity.digest.clone());
        finish_inequality_year_cached(
            &mut inequality_inputs,
            &mut inequality_outputs,
            &mut inequality_digests,
            Some(&cache),
        )
        .expect("first inequality cache write should succeed");
        assert_eq!(inequality_outputs.len(), 1);

        inequality_inputs.push(inequality_month().expect("inequality fixture should be valid"));
        inequality_digests.push(identity.digest.clone());
        finish_inequality_year_cached(
            &mut inequality_inputs,
            &mut inequality_outputs,
            &mut inequality_digests,
            Some(&cache),
        )
        .expect("inequality cache reuse should succeed");
        assert_eq!(inequality_outputs.len(), 2);

        editor_month_frames.push(editor_months(&[1], &[202402], &[1])?);
        assert!(
            finish_activity_year_cached(
                &mut editor_month_frames,
                &mut output_frames,
                &mut month_digests,
                Some(&cache),
            )
            .is_err()
        );
        assert!(WeeklyContributionCursor::new(&root.path().join("missing.parquet")).is_err());

        let existing_output = root.path().join("existing-output");
        fs::create_dir(&existing_output)?;
        assert!(
            compute_cross_snapshot_qualification_build(
                "testwiki",
                "2026-08",
                root.path(),
                &existing_output,
                true,
            )
            .is_err()
        );
        Ok(())
    }

    #[test]
    fn registered_state_tracks_bounds_and_skips_null_users() -> Result<()> {
        let mut state = RegisteredState::new();
        let null_only = registered_base_df(&[(None, 2024, 202401, 1)])?;
        state.observe_partition(&null_only, 2024, 202401)?;
        assert!(state.funnel_stats.is_empty());
        state.observe_history(&null_only)?;
        assert!(state.funnel_stats.is_empty());

        let early = registered_base_df(&[(Some(1), 2024, 202401, 10)])?;
        let late = registered_base_df(&[(Some(1), 2025, 202502, 11)])?;
        let earlier = registered_base_df(&[(Some(1), 2023, 202312, 12)])?;
        state.observe_partition(&early, 2024, 202401)?;
        state.observe_partition(&late, 2025, 202502)?;
        state.observe_partition(&earlier, 2023, 202312)?;

        assert_eq!(state.funnel_stats.get(&1), Some(&(2023, 3)));
        assert_eq!(state.cohort_spans.get(&1), Some(&(2023, 2025)));
        Ok(())
    }

    #[test]
    fn lifecycle_checkpoint_roundtrip_replays_the_suffix_exactly() -> Result<()> {
        let mut continuous = RegisteredState::new();
        let first =
            registered_base_df(&[(Some(1), 2024, 202401, 10), (Some(2), 2024, 202412, 11)])?;
        continuous.observe_partition(&first, 2024, 202412)?;
        let checkpoint = LifecycleCheckpoint::from_state(&continuous, "2024-12", &"ab".repeat(32));
        checkpoint.validate("2024-12", &"ab".repeat(32))?;
        assert!(checkpoint.validate("2023-12", &"ab".repeat(32)).is_err());
        let serialized = serde_json::to_vec(&checkpoint)?;
        let restored: LifecycleCheckpoint = serde_json::from_slice(&serialized)?;
        let mut resumed = restored.into_state();

        let suffix =
            registered_base_df(&[(Some(1), 2025, 202501, 12), (Some(3), 2025, 202502, 13)])?;
        continuous.observe_partition(&suffix, 2025, 202502)?;
        resumed.observe_partition(&suffix, 2025, 202502)?;
        assert_eq!(resumed.funnel_stats, continuous.funnel_stats);
        assert_eq!(resumed.cohort_spans, continuous.cohort_spans);
        assert_eq!(resumed.churn_month.active, continuous.churn_month.active);
        assert_eq!(resumed.churn_month.spans, continuous.churn_month.spans);
        assert_eq!(
            resumed.churn_quarter.active,
            continuous.churn_quarter.active
        );
        assert_eq!(resumed.churn_quarter.spans, continuous.churn_quarter.spans);
        assert_eq!(resumed.churn_year.active, continuous.churn_year.active);
        assert_eq!(resumed.churn_year.spans, continuous.churn_year.spans);
        Ok(())
    }

    #[test]
    fn finalize_funnel_writes_threshold_counts() -> Result<()> {
        init_test_tracing();
        let output_dir = TestDir::new()?;
        let stats = HashMap::from([
            (1_i64, (2024, 1_u32)),
            (2, (2024, 5)),
            (3, (2024, 25)),
            (4, (2024, 100)),
        ]);

        finalize_funnel(stats, "testwiki", output_dir.path())?;

        let path = output_dir
            .path()
            .join("testwiki")
            .join("business_funnel.parquet")
            .to_string_lossy()
            .to_string();
        let df = LazyFrame::scan_parquet(path.as_str().into(), Default::default())?.collect()?;
        assert_eq!(df.column("cohort_size")?.u32()?.get(0), Some(4));
        assert_eq!(df.column("reached_5")?.u32()?.get(0), Some(3));
        assert_eq!(df.column("reached_25")?.u32()?.get(0), Some(2));
        assert_eq!(df.column("reached_100")?.u32()?.get(0), Some(1));
        Ok(())
    }

    #[test]
    fn finalize_labor_cohorts_writes_output() -> Result<()> {
        init_test_tracing();
        let output_dir = TestDir::new()?;
        let spans = HashMap::from([(1_i64, (2024, 2025)), (2, (2024, 2024))]);

        finalize_labor_cohorts(spans, "testwiki", output_dir.path())?;

        let path = output_dir
            .path()
            .join("testwiki")
            .join("labor_cohorts.parquet")
            .to_string_lossy()
            .to_string();
        let df = LazyFrame::scan_parquet(path.as_str().into(), Default::default())?.collect()?;
        assert!(df.height() > 0);
        assert_eq!(df.column("wiki")?.str()?.get(0), Some("testwiki"));
        Ok(())
    }

    #[test]
    fn compute_all_uses_partitioned_incremental_path() -> Result<()> {
        init_test_tracing();
        let data_dir = TestDir::new()?;
        let output_dir = TestDir::new()?;
        let wiki = "partitionedwiki";

        write_partitioned_base_parquet(&data_dir, wiki)?;
        compute_all(wiki, data_dir.path(), output_dir.path())?;

        let gdp_path = output_dir.path().join(wiki).join("gdp.parquet");
        let gdp_path = gdp_path.to_string_lossy().to_string();
        let gdp =
            LazyFrame::scan_parquet(gdp_path.as_str().into(), Default::default())?.collect()?;
        assert_eq!(gdp.height(), 4);

        let churn_path = output_dir.path().join(wiki).join("labor_churn.parquet");
        let churn_path = churn_path.to_string_lossy().to_string();
        let churn =
            LazyFrame::scan_parquet(churn_path.as_str().into(), Default::default())?.collect()?;
        assert!(
            churn
                .column("period_type")?
                .str()?
                .iter()
                .flatten()
                .any(|value| value == "quarter")
        );
        Ok(())
    }

    #[test]
    fn load_partition_uses_existing_analytical_projection_without_filter_columns() -> Result<()> {
        init_test_tracing();
        let data_dir = TestDir::new()?;
        let wiki = "projection-partitionedwiki";

        write_partitioned_base_parquet(&data_dir, wiki)?;
        let jan_dir = storage::month_partition_dir(
            &storage::analytical_wiki_dir(data_dir.path(), wiki),
            2024,
            "2024-01",
        );
        let loaded = load_partition(&storage::collect_parquet_files(&jan_dir)?)?;

        assert_eq!(loaded.height(), 2);
        assert_eq!(loaded.width(), crate::schema::ANALYTICAL_COLUMNS.len() + 1);
        assert_eq!(loaded.column("year_month")?.str()?.get(0), Some("2024-01"));
        Ok(())
    }

    #[test]
    fn load_partition_compatibility_projection_skips_revision_filter_when_columns_are_absent()
    -> Result<()> {
        init_test_tracing();
        let data_dir = TestDir::new()?;
        let wiki = "compatibility-partitionedwiki";

        write_partitioned_compatibility_parquet_without_filter_columns(&data_dir, wiki)?;
        let jan_dir = storage::month_partition_dir(
            &storage::analytical_wiki_dir(data_dir.path(), wiki),
            2024,
            "2024-01",
        );
        let loaded = load_partition(&storage::collect_parquet_files(&jan_dir)?)?;

        assert_eq!(loaded.height(), 2);
        assert_eq!(loaded.column("is_minor")?.bool()?.get(0), Some(true));
        Ok(())
    }

    #[test]
    fn compute_all_supports_partitioned_legacy_parquet_layouts() -> Result<()> {
        init_test_tracing();
        let data_dir = TestDir::new()?;
        let output_dir = TestDir::new()?;
        let wiki = "legacy-partitionedwiki";

        write_partitioned_legacy_parquet(&data_dir, wiki)?;
        compute_all(wiki, data_dir.path(), output_dir.path())?;

        let gdp_path = output_dir
            .path()
            .join(wiki)
            .join("gdp.parquet")
            .to_string_lossy()
            .to_string();
        let gdp =
            LazyFrame::scan_parquet(gdp_path.as_str().into(), Default::default())?.collect()?;
        assert_eq!(gdp.height(), 4);

        let labor_path = output_dir
            .path()
            .join(wiki)
            .join("labor_monthly.parquet")
            .to_string_lossy()
            .to_string();
        let labor =
            LazyFrame::scan_parquet(labor_path.as_str().into(), Default::default())?.collect()?;
        assert!(
            labor
                .column("user_type")?
                .str()?
                .iter()
                .flatten()
                .any(|value| value == "temporary")
        );
        Ok(())
    }

    #[test]
    fn load_wiki_filters_and_enriches_rows() -> Result<()> {
        init_test_tracing();
        let data_dir = TestDir::new()?;
        let wiki = "testwiki";

        write_input_parquet(&data_dir, wiki)?;
        let loaded = load_wiki(wiki, data_dir.path())?;

        assert_eq!(loaded.height(), 6);
        assert_eq!(loaded.width(), 11);

        let user_types: Vec<String> = loaded
            .column("user_type")?
            .str()?
            .iter()
            .flatten()
            .map(ToOwned::to_owned)
            .collect();
        assert!(user_types.iter().any(|user_type| user_type == "bot"));
        assert!(user_types.iter().any(|user_type| user_type == "temporary"));

        Ok(())
    }

    #[test]
    fn load_wiki_errors_when_parquet_directory_is_missing() {
        init_test_tracing();
        let data_dir = TestDir::new().expect("temp dir");
        let err =
            load_wiki("missingwiki", data_dir.path()).expect_err("missing parquet should fail");
        assert!(err.to_string().contains("Run `ingest` first"));
    }

    #[test]
    fn load_wiki_errors_when_no_parquet_files_exist() -> Result<()> {
        init_test_tracing();
        let data_dir = TestDir::new()?;
        let wiki = "emptywiki";
        fs::create_dir_all(storage::analytical_wiki_dir(data_dir.path(), wiki))?;

        let err = load_wiki(wiki, data_dir.path()).expect_err("empty parquet dir should fail");
        assert!(err.to_string().contains("No parquet files found"));
        Ok(())
    }

    #[test]
    fn load_wiki_uses_precomputed_columns_when_available() -> Result<()> {
        init_test_tracing();
        let data_dir = TestDir::new()?;
        let wiki = "precomputedwiki";

        write_precomputed_parquet(&data_dir, wiki, true)?;
        let loaded = load_wiki(wiki, data_dir.path())?;

        assert_eq!(loaded.column("year_month_key")?.i32()?.get(0), Some(202401));
        assert_eq!(loaded.column("user_type")?.str()?.get(1), Some("temporary"));
        assert_eq!(loaded.column("is_reverted")?.bool()?.get(1), Some(true));
        assert_eq!(loaded.column("is_minor")?.bool()?.get(0), Some(true));
        Ok(())
    }

    #[test]
    fn load_wiki_uses_boolean_source_flags_when_output_flags_are_missing() -> Result<()> {
        init_test_tracing();
        let data_dir = TestDir::new()?;
        let wiki = "boolean-fallback-wiki";

        write_precomputed_parquet(&data_dir, wiki, false)?;
        let loaded = load_wiki(wiki, data_dir.path())?;

        assert_eq!(loaded.column("is_reverted")?.bool()?.get(1), Some(true));
        assert_eq!(loaded.column("is_minor")?.bool()?.get(0), Some(true));
        Ok(())
    }

    #[test]
    fn load_wiki_uses_existing_output_flags_in_compatibility_projection() -> Result<()> {
        init_test_tracing();
        let data_dir = TestDir::new()?;
        let wiki = "compatibility-output-flags-wiki";

        write_compatibility_parquet_with_output_flags(&data_dir, wiki)?;
        let loaded = load_wiki(wiki, data_dir.path())?;

        assert_eq!(loaded.column("is_reverted")?.bool()?.get(1), Some(true));
        assert_eq!(loaded.column("is_minor")?.bool()?.get(0), Some(true));
        assert_eq!(loaded.column("year_month_key")?.i32()?.get(0), Some(202401));
        Ok(())
    }
}
