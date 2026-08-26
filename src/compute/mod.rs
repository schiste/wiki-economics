pub mod activity;
pub mod gdp;
pub mod inequality;
pub mod labor;
pub mod lifecycle;
pub mod monthly;
pub mod weekly;

use anyhow::{Context, Result};
use chrono::{Duration, NaiveDate};
use polars::io::parquet::write::BatchedWriter;
use polars::prelude::*;
use serde::Serialize;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::env;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tracing::info;

use crate::{
    determinism, fingerprint,
    observability::MemorySnapshot,
    resource_governor::{GovernorPaths, ResourceGovernor},
    schema, storage, workload_profile,
};

const LEGACY_COMPUTE_ALGORITHM_VERSION: &str = "core-metrics-v8-period-aware-activity-tiers";
const DEFAULT_WEEKLY_BUCKET_COUNT: usize = 256;
const DEFAULT_SECONDARY_BUCKET_COUNT: usize = 1;
const WEEKLY_ROUTING_BATCH_ROWS: usize = 250_000;
const SUPPORTED_PRIMARY_BUCKET_COUNTS: [usize; 6] = [32, 64, 128, 256, 512, 1024];
#[cfg(test)]
const FLAT_BENCHMARK_BUCKET_COUNTS: [usize; 3] = [256, 512, 1024];
const SUPPORTED_SECONDARY_BUCKET_COUNTS: [usize; 4] = [1, 8, 16, 32];
const WEEKLY_BUCKET_COUNT_ENV: &str = "WIKI_ECON_WEEKLY_BUCKET_COUNT";
const WEEKLY_PRIMARY_BUCKET_COUNT_ENV: &str = "WIKI_ECON_WEEKLY_PRIMARY_BUCKET_COUNT";
const WEEKLY_SECONDARY_BUCKET_COUNT_ENV: &str = "WIKI_ECON_WEEKLY_SECONDARY_BUCKET_COUNT";
const SCRATCH_DIR_ENV: &str = "WIKI_ECON_SCRATCH_DIR";

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum MetricFamily {
    Monthly,
    ActivityTiers,
    Lifecycle,
    PageWeek,
}

impl MetricFamily {
    pub(crate) const ALL: [Self; 4] = [
        Self::Monthly,
        Self::ActivityTiers,
        Self::Lifecycle,
        Self::PageWeek,
    ];

    const NONWEEKLY: [Self; 3] = [Self::Monthly, Self::ActivityTiers, Self::Lifecycle];

    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::Monthly => "monthly",
            Self::ActivityTiers => "activity_tiers",
            Self::Lifecycle => "lifecycle",
            Self::PageWeek => "page_week",
        }
    }

    pub(crate) fn metrics(self) -> &'static [&'static str] {
        match self {
            Self::Monthly => &monthly::METRICS,
            Self::ActivityTiers => &activity::METRICS,
            Self::Lifecycle => &lifecycle::METRICS,
            Self::PageWeek => &weekly::METRICS,
        }
    }

    pub(crate) fn for_metric(metric: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|family| family.metrics().contains(&metric))
    }

    fn algorithm_version(self, weekly_config: &WeeklyAggregationConfig) -> String {
        match self {
            Self::Monthly => monthly::ALGORITHM_VERSION.to_string(),
            Self::ActivityTiers => activity::ALGORITHM_VERSION.to_string(),
            Self::Lifecycle => lifecycle::ALGORITHM_VERSION.to_string(),
            Self::PageWeek => weekly_config.algorithm_version(),
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
        }
    }

    fn all_reused(self) -> bool {
        MetricFamily::ALL
            .into_iter()
            .all(|family| self.invalidation(family) == Invalidation::Reuse)
    }

    fn any_nonweekly(self) -> bool {
        MetricFamily::NONWEEKLY
            .into_iter()
            .any(|family| self.invalidation(family).must_compute())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct WeeklyAggregationConfig {
    primary_bucket_count: usize,
    secondary_bucket_count: usize,
    scratch_root: Option<PathBuf>,
    workload_algorithm_version: Option<String>,
}

impl WeeklyAggregationConfig {
    #[cfg(test)]
    pub(crate) fn new(bucket_count: usize, scratch_root: Option<PathBuf>) -> Result<Self> {
        Self::new_two_level(bucket_count, DEFAULT_SECONDARY_BUCKET_COUNT, scratch_root)
    }

    pub(crate) fn new_two_level(
        primary_bucket_count: usize,
        secondary_bucket_count: usize,
        scratch_root: Option<PathBuf>,
    ) -> Result<Self> {
        anyhow::ensure!(
            SUPPORTED_PRIMARY_BUCKET_COUNTS.contains(&primary_bucket_count),
            "weekly primary bucket count must be one of 32, 64, 128, 256, 512, or 1024"
        );
        anyhow::ensure!(
            SUPPORTED_SECONDARY_BUCKET_COUNTS.contains(&secondary_bucket_count),
            "weekly secondary bucket count must be one of 1, 8, 16, or 32"
        );
        primary_bucket_count
            .checked_mul(secondary_bucket_count)
            .context("weekly logical bucket count overflow")?;
        Ok(Self {
            primary_bucket_count,
            secondary_bucket_count,
            scratch_root,
            workload_algorithm_version: None,
        })
    }

    fn for_snapshot(data_dir: &Path, wiki: &str, snapshot: Option<&str>) -> Result<Self> {
        let selected_snapshot = match snapshot {
            Some(snapshot) => Some(snapshot.to_string()),
            None => storage::current_snapshot_version(data_dir, wiki)?,
        };
        if let Some(selected_snapshot) = selected_snapshot
            && let Some(profile) = workload_profile::load(data_dir, wiki, &selected_snapshot)?
        {
            profile.ensure_compute_qualified()?;
            let scratch_root = env::var_os(SCRATCH_DIR_ENV).map(PathBuf::from);
            let primary = profile.parameters.primary_buckets;
            let secondary = profile.parameters.secondary_buckets;
            let mut config = Self::new_two_level(primary, secondary, scratch_root)?;
            config.workload_algorithm_version = Some(profile.algorithm_version()?);
            return Ok(config);
        }
        anyhow::ensure!(
            !workload_profile::require_qualified()?,
            "qualified production compute requires a persisted workload profile for {wiki}"
        );
        Self::from_environment()
    }

    fn from_environment() -> Result<Self> {
        Self::from_values(
            env::var_os(WEEKLY_BUCKET_COUNT_ENV),
            env::var_os(WEEKLY_PRIMARY_BUCKET_COUNT_ENV),
            env::var_os(WEEKLY_SECONDARY_BUCKET_COUNT_ENV),
            env::var_os(SCRATCH_DIR_ENV),
        )
    }

    fn from_values(
        legacy_bucket_count: Option<std::ffi::OsString>,
        primary_bucket_count: Option<std::ffi::OsString>,
        secondary_bucket_count: Option<std::ffi::OsString>,
        scratch_root: Option<std::ffi::OsString>,
    ) -> Result<Self> {
        anyhow::ensure!(
            legacy_bucket_count.is_none()
                || (primary_bucket_count.is_none() && secondary_bucket_count.is_none()),
            "{WEEKLY_BUCKET_COUNT_ENV} cannot be combined with primary/secondary bucket settings"
        );
        let primary_name = if primary_bucket_count.is_some() {
            WEEKLY_PRIMARY_BUCKET_COUNT_ENV
        } else {
            WEEKLY_BUCKET_COUNT_ENV
        };
        let primary_bucket_count =
            parse_bucket_env(primary_bucket_count.or(legacy_bucket_count), primary_name)?
                .unwrap_or(DEFAULT_WEEKLY_BUCKET_COUNT);
        let secondary_bucket_count =
            parse_bucket_env(secondary_bucket_count, WEEKLY_SECONDARY_BUCKET_COUNT_ENV)?
                .unwrap_or(DEFAULT_SECONDARY_BUCKET_COUNT);
        let scratch_root = scratch_root.map(PathBuf::from);
        Self::new_two_level(primary_bucket_count, secondary_bucket_count, scratch_root)
    }

    pub(crate) fn algorithm_version(&self) -> String {
        let selection = self
            .workload_algorithm_version
            .as_deref()
            .unwrap_or("explicit-qualification-configuration");
        let partition = determinism::partition_algorithm_version(
            self.primary_bucket_count,
            self.secondary_bucket_count,
        );
        format!("{}-{selection}-{partition}", weekly::ALGORITHM_VERSION)
    }

    fn legacy_algorithm_version(&self) -> String {
        let selection = self
            .workload_algorithm_version
            .as_deref()
            .unwrap_or("explicit-qualification-configuration");
        let partition = determinism::partition_algorithm_version(
            self.primary_bucket_count,
            self.secondary_bucket_count,
        );
        format!("{LEGACY_COMPUTE_ALGORITHM_VERSION}-{selection}-{partition}")
    }

    fn logical_bucket_count(&self) -> usize {
        self.primary_bucket_count * self.secondary_bucket_count
    }
}

fn parse_bucket_env(value: Option<std::ffi::OsString>, name: &str) -> Result<Option<usize>> {
    value
        .map(|value| {
            let value = value
                .into_string()
                .map_err(|value| anyhow::anyhow!("invalid {name} value {value:?}"))?;
            value
                .parse::<usize>()
                .with_context(|| format!("invalid {name} value {value:?}"))
        })
        .transpose()
}

#[derive(Clone, Copy, Debug, Default, Serialize)]
pub(crate) struct ResourcePeak {
    pub rss_bytes: Option<u64>,
    pub cgroup_current_bytes: Option<u64>,
    pub scratch_bytes: Option<u64>,
}

impl ResourcePeak {
    fn observe(&mut self, snapshot: MemorySnapshot, scratch_bytes: Option<u64>) {
        self.rss_bytes = max_option(self.rss_bytes, snapshot.rss_bytes);
        self.cgroup_current_bytes =
            max_option(self.cgroup_current_bytes, snapshot.cgroup_current_bytes);
        self.scratch_bytes = max_option(self.scratch_bytes, scratch_bytes);
    }
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct WeeklyAggregationReport {
    pub wiki: String,
    pub bucket_count: usize,
    pub primary_bucket_count: usize,
    pub secondary_bucket_count: usize,
    pub partitions: usize,
    pub staged_rows: usize,
    pub output_rows: usize,
    pub total_edits: i64,
    pub minimum_week_start: Option<String>,
    pub maximum_week_start: Option<String>,
    pub bucket_staged_rows: Vec<usize>,
    pub primary_bucket_staged_rows: Vec<usize>,
    pub largest_bucket_staged_rows: usize,
    pub output_bytes: u64,
    pub scratch_peak_bytes: u64,
    pub working_storage_peak_bytes: u64,
    pub reduction_peak: ResourcePeak,
    pub reconciliation_peak: ResourcePeak,
    pub final_memory: MemorySnapshot,
    pub reduction_elapsed_ms: u64,
    pub reconciliation_elapsed_ms: u64,
    pub elapsed_ms: u64,
}

fn max_option(left: Option<u64>, right: Option<u64>) -> Option<u64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (left, right) => left.or(right),
    }
}

pub(super) struct ChurnAccumulator {
    period_type: &'static str,
    seen: HashSet<(i64, i32)>,
    active: BTreeMap<i32, u32>,
    spans: HashMap<i64, (i32, i32)>,
}

impl ChurnAccumulator {
    pub(super) fn new(period_type: &'static str) -> Self {
        Self {
            period_type,
            seen: HashSet::new(),
            active: BTreeMap::new(),
            spans: HashMap::new(),
        }
    }

    pub(super) fn observe(&mut self, user_id: i64, period_key: i32) {
        if !self.seen.insert((user_id, period_key)) {
            return;
        }

        *self.active.entry(period_key).or_insert(0) += 1;
        self.spans
            .entry(user_id)
            .and_modify(|(first, last)| {
                if period_key < *first {
                    *first = period_key;
                }
                if period_key > *last {
                    *last = period_key;
                }
            })
            .or_insert((period_key, period_key));
    }

    pub(super) fn finish(self) -> Result<DataFrame> {
        let mut arrivals: HashMap<i32, u32> = HashMap::new();
        let mut departures: HashMap<i32, u32> = HashMap::new();
        for (first, last) in self.spans.into_values() {
            *arrivals.entry(first).or_insert(0) += 1;
            *departures.entry(last).or_insert(0) += 1;
        }

        let period_keys: Vec<i32> = self.active.keys().copied().collect();
        let periods: Vec<String> = period_keys
            .iter()
            .map(|period_key| labor::format_period_key(*period_key, self.period_type))
            .collect();
        let active_editors: Vec<u32> = period_keys
            .iter()
            .map(|period_key| self.active[period_key])
            .collect();
        let arrivals_out: Vec<u32> = period_keys
            .iter()
            .map(|period_key| arrivals.get(period_key).copied().unwrap_or(0))
            .collect();
        let departures_out: Vec<u32> = period_keys
            .iter()
            .map(|period_key| departures.get(period_key).copied().unwrap_or(0))
            .collect();
        let arrival_rate: Vec<f64> = arrivals_out
            .iter()
            .zip(&active_editors)
            .map(|(&arrivals_count, &active_count)| arrivals_count as f64 / active_count as f64)
            .collect();
        let departure_rate: Vec<f64> = departures_out
            .iter()
            .zip(&active_editors)
            .map(|(&departures_count, &active_count)| departures_count as f64 / active_count as f64)
            .collect();

        DataFrame::new_infer_height(vec![
            Column::new("period".into(), periods),
            Column::new("active_editors".into(), active_editors),
            Column::new("arrivals".into(), arrivals_out),
            Column::new("departures".into(), departures_out),
            Column::new(
                "period_type".into(),
                vec![self.period_type; self.active.len()],
            ),
            Column::new("arrival_rate".into(), arrival_rate),
            Column::new("departure_rate".into(), departure_rate),
        ])
        .map_err(Into::into)
    }
}

struct RegisteredState {
    funnel_stats: HashMap<i64, (i32, u32)>,
    cohort_spans: HashMap<i64, (i32, i32)>,
    churn_month: ChurnAccumulator,
    churn_quarter: ChurnAccumulator,
    churn_year: ChurnAccumulator,
}

impl RegisteredState {
    fn new() -> Self {
        Self {
            funnel_stats: HashMap::new(),
            cohort_spans: HashMap::new(),
            churn_month: ChurnAccumulator::new("month"),
            churn_quarter: ChurnAccumulator::new("quarter"),
            churn_year: ChurnAccumulator::new("year"),
        }
    }

    fn observe_partition(
        &mut self,
        base: &DataFrame,
        year: i32,
        year_month_key: i32,
    ) -> Result<()> {
        let partial = registered_editor_totals(base)?;
        let user_ids = partial.column("event_user_id")?.i64()?;
        let total_edits = partial.column("total_edits")?.u32()?;
        let cohort_years = partial.column("cohort_year")?.i32()?;

        for idx in 0..partial.height() {
            let (Some(user_id), Some(user_total_edits), Some(cohort_year)) = (
                user_ids.get(idx),
                total_edits.get(idx),
                cohort_years.get(idx),
            ) else {
                continue;
            };

            self.funnel_stats
                .entry(user_id)
                .and_modify(|(existing_cohort_year, edits)| {
                    if cohort_year < *existing_cohort_year {
                        *existing_cohort_year = cohort_year;
                    }
                    *edits += user_total_edits;
                })
                .or_insert((cohort_year, user_total_edits));

            self.cohort_spans
                .entry(user_id)
                .and_modify(|(first_year, last_year)| {
                    if year < *first_year {
                        *first_year = year;
                    }
                    if year > *last_year {
                        *last_year = year;
                    }
                })
                .or_insert((year, year));

            self.churn_month.observe(user_id, year_month_key);
            self.churn_quarter.observe(
                user_id,
                labor::normalize_period_key(year_month_key, "quarter")?,
            );
            self.churn_year.observe(user_id, year);
        }

        Ok(())
    }

    fn observe_history(&mut self, base: &DataFrame) -> Result<()> {
        let registered = base
            .clone()
            .lazy()
            .filter(col("user_type").eq(lit("registered")))
            .select([col("event_user_id"), col("year"), col("year_month_key")])
            .collect()?;
        let user_ids = registered.column("event_user_id")?.i64()?;
        let years = registered.column("year")?.i32()?;
        let months = registered.column("year_month_key")?.i32()?;
        for row in 0..registered.height() {
            let (Some(user_id), Some(year), Some(year_month_key)) =
                (user_ids.get(row), years.get(row), months.get(row))
            else {
                continue;
            };
            self.funnel_stats
                .entry(user_id)
                .and_modify(|(first_year, edits)| {
                    *first_year = (*first_year).min(year);
                    *edits += 1;
                })
                .or_insert((year, 1));
            self.cohort_spans
                .entry(user_id)
                .and_modify(|(first_year, last_year)| {
                    *first_year = (*first_year).min(year);
                    *last_year = (*last_year).max(year);
                })
                .or_insert((year, year));
            self.churn_month.observe(user_id, year_month_key);
            self.churn_quarter.observe(
                user_id,
                labor::normalize_period_key(year_month_key, "quarter")?,
            );
            self.churn_year.observe(user_id, year);
        }
        Ok(())
    }
}

fn analytical_select_exprs() -> Vec<Expr> {
    schema::ANALYTICAL_COLUMNS
        .iter()
        .map(|column| col(*column))
        .collect()
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
    let has_analytical_projection = schema::ANALYTICAL_COLUMNS
        .iter()
        .all(|column| schema.get(column).is_some());

    if has_analytical_projection {
        return df
            .select(analytical_select_exprs())
            .collect()
            .map_err(Into::into);
    }

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

    df.select([
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
        if has_user_type {
            col("user_type")
        } else {
            user_type_col(
                event_user_is_anonymous.clone(),
                event_user_is_temporary.clone(),
            )
        },
        col("event_user_id"),
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

fn gdp_monthly_frame(base: &DataFrame) -> Result<DataFrame> {
    base.clone()
        .lazy()
        .group_by([col("year_month"), col("page_namespace"), col("user_type")])
        .agg([
            col("revision_text_bytes_diff")
                .filter(col("revision_text_bytes_diff").gt(lit(0i64)))
                .sum()
                .alias("gross_bytes_added"),
            col("revision_text_bytes_diff").sum().alias("net_bytes"),
            col("revision_id").count().alias("total_edits"),
            col("is_reverted")
                .not()
                .cast(DataType::UInt32)
                .sum()
                .alias("productive_edits"),
            col("is_reverted")
                .cast(DataType::UInt32)
                .sum()
                .alias("reverted_edits"),
            col("event_user_id").n_unique().alias("unique_editors"),
            col("is_minor")
                .cast(DataType::UInt32)
                .sum()
                .alias("minor_edits"),
        ])
        .with_columns([
            (col("net_bytes").cast(DataType::Float64) / col("total_edits").cast(DataType::Float64))
                .alias("bytes_per_edit"),
            (col("net_bytes").cast(DataType::Float64)
                / col("unique_editors").cast(DataType::Float64))
            .alias("bytes_per_editor"),
            (col("reverted_edits").cast(DataType::Float64)
                / col("total_edits").cast(DataType::Float64))
            .alias("revert_rate"),
        ])
        .collect()
        .map_err(Into::into)
}

fn gdp_type_share_frame(base: &DataFrame) -> Result<DataFrame> {
    base.clone()
        .lazy()
        .group_by([col("year_month"), col("user_type")])
        .agg([
            col("revision_id").count().alias("edits"),
            col("revision_text_bytes_diff").sum().alias("net_bytes"),
            col("event_user_id").n_unique().alias("editors"),
        ])
        .collect()
        .map_err(Into::into)
}

fn gdp_editor_month_frame(base: &DataFrame) -> Result<DataFrame> {
    base.clone()
        .lazy()
        .group_by([
            col("year_month"),
            col("year_month_key"),
            col("user_type"),
            col("event_user_id"),
        ])
        .agg([
            col("revision_id").count().alias("edits"),
            col("revision_text_bytes_diff").sum().alias("net_bytes"),
            col("revision_text_bytes_diff")
                .filter(col("revision_text_bytes_diff").gt(lit(0i64)))
                .sum()
                .alias("gross_bytes"),
        ])
        .collect()
        .map_err(Into::into)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ActivityPeriod {
    Month,
    Quarter,
    Year,
}

impl ActivityPeriod {
    fn name(self) -> &'static str {
        match self {
            Self::Month => "month",
            Self::Quarter => "quarter",
            Self::Year => "year",
        }
    }

    fn months(self) -> u32 {
        match self {
            Self::Month => 1,
            Self::Quarter => 3,
            Self::Year => 12,
        }
    }

    fn key_expr(self) -> Expr {
        match self {
            Self::Month => col("year_month_key"),
            Self::Quarter => {
                let year = col("year_month_key") / lit(100_i32);
                let month = col("year_month_key") % lit(100_i32);
                year * lit(10_i32) + ((month - lit(1_i32)) / lit(3_i32) + lit(1_i32))
            }
            Self::Year => col("year_month_key") / lit(100_i32),
        }
    }

    fn fields(self, key: i32) -> Result<(String, String, String)> {
        match self {
            Self::Month => {
                let year = key / 100;
                let month = key % 100;
                anyhow::ensure!(
                    (1..=12).contains(&month),
                    "invalid activity month key {key}"
                );
                let period = format!("{year:04}-{month:02}");
                Ok((period.clone(), period.clone(), period))
            }
            Self::Quarter => {
                let year = key / 10;
                let quarter = key % 10;
                anyhow::ensure!(
                    (1..=4).contains(&quarter),
                    "invalid activity quarter key {key}"
                );
                let first_month = (quarter - 1) * 3 + 1;
                Ok((
                    format!("{year:04}-Q{quarter}"),
                    format!("{year:04}-{first_month:02}"),
                    format!("{year:04}-{:02}", first_month + 2),
                ))
            }
            Self::Year => Ok((
                format!("{key:04}"),
                format!("{key:04}-01"),
                format!("{key:04}-12"),
            )),
        }
    }
}

fn activity_tier_labels(months: u32) -> [String; 5] {
    let first = if months == 1 {
        "1 edit".to_string()
    } else {
        format!("1-{months} edits")
    };
    [
        first,
        format!("{}-{} edits", months + 1, 5 * months - 1),
        format!("{}-{} edits", 5 * months, 25 * months - 1),
        format!("{}-{} edits", 25 * months, 100 * months - 1),
        format!("{}+ edits", 100 * months),
    ]
}

fn gdp_activity_tiers_for_period(
    editor_months: &DataFrame,
    period: ActivityPeriod,
) -> Result<DataFrame> {
    let months = period.months();
    let labels = activity_tier_labels(months);
    let input_edits = editor_months
        .column("edits")?
        .cast(&DataType::Int64)?
        .i64()?
        .sum()
        .unwrap_or(0);
    let mut frame = editor_months
        .clone()
        .lazy()
        .with_column(period.key_expr().alias("period_key"))
        .group_by([col("period_key"), col("user_type"), col("event_user_id")])
        .agg([
            col("edits").sum().alias("edits"),
            col("net_bytes").sum().alias("net_bytes"),
            col("gross_bytes").sum().alias("gross_bytes"),
        ])
        .with_columns([
            when(col("edits").lt_eq(lit(months)))
                .then(lit(labels[0].clone()))
                .when(col("edits").lt(lit(5 * months)))
                .then(lit(labels[1].clone()))
                .when(col("edits").lt(lit(25 * months)))
                .then(lit(labels[2].clone()))
                .when(col("edits").lt(lit(100 * months)))
                .then(lit(labels[3].clone()))
                .otherwise(lit(labels[4].clone()))
                .alias("activity_tier"),
            when(col("edits").lt_eq(lit(months)))
                .then(lit(0_u32))
                .when(col("edits").lt(lit(5 * months)))
                .then(lit(1_u32))
                .when(col("edits").lt(lit(25 * months)))
                .then(lit(2_u32))
                .when(col("edits").lt(lit(100 * months)))
                .then(lit(3_u32))
                .otherwise(lit(4_u32))
                .cast(DataType::UInt32)
                .alias("tier_rank"),
        ])
        .group_by([
            col("period_key"),
            col("user_type"),
            col("tier_rank"),
            col("activity_tier"),
        ])
        .agg([
            col("event_user_id").n_unique().alias("editors"),
            col("edits").sum().alias("total_edits"),
            col("net_bytes").sum().alias("net_bytes"),
            col("gross_bytes").sum().alias("gross_bytes"),
        ])
        .collect()?;

    let output_edits = frame
        .column("total_edits")?
        .cast(&DataType::Int64)?
        .i64()?
        .sum()
        .unwrap_or(0);
    let period_name = period.name();
    anyhow::ensure!(
        input_edits == output_edits,
        "{} activity-tier edit conservation failed: input={input_edits}, output={output_edits}",
        period_name
    );

    let keys = frame.column("period_key")?.i32()?;
    let fields = keys
        .into_no_null_iter()
        .map(|key| period.fields(key))
        .collect::<Result<Vec<_>>>()?;
    let height = frame.height();
    for column in [
        Column::new(
            "period".into(),
            fields
                .iter()
                .map(|(value, _, _)| value.as_str())
                .collect::<Vec<_>>(),
        ),
        Column::new(
            "period_start".into(),
            fields
                .iter()
                .map(|(_, value, _)| value.as_str())
                .collect::<Vec<_>>(),
        ),
        Column::new(
            "year_month".into(),
            fields
                .iter()
                .map(|(_, value, _)| value.as_str())
                .collect::<Vec<_>>(),
        ),
        Column::new(
            "period_end".into(),
            fields
                .iter()
                .map(|(_, _, value)| value.as_str())
                .collect::<Vec<_>>(),
        ),
        Column::new("period_type".into(), vec![period.name(); height]),
        Column::new("period_months".into(), vec![months; height]),
    ] {
        frame.with_column(column)?;
    }
    frame.drop_in_place("period_key")?;
    sort_frame(frame, ["period", "user_type", "tier_rank"])
}

fn activity_tiers_all_periods(base: DataFrame) -> Result<DataFrame> {
    let editor_months = gdp_editor_month_frame(&base)?;
    concat_frames(vec![
        gdp_activity_tiers_for_period(&editor_months, ActivityPeriod::Month)?,
        gdp_activity_tiers_for_period(&editor_months, ActivityPeriod::Quarter)?,
        gdp_activity_tiers_for_period(&editor_months, ActivityPeriod::Year)?,
    ])
}

fn finish_activity_year(
    editor_month_frames: &mut Vec<DataFrame>,
    output_frames: &mut Vec<DataFrame>,
) -> Result<()> {
    if editor_month_frames.is_empty() {
        return Ok(());
    }
    let editor_months = concat_frames(std::mem::take(editor_month_frames))?;
    let monthly = gdp_activity_tiers_for_period(&editor_months, ActivityPeriod::Month)?;
    let quarterly = gdp_activity_tiers_for_period(&editor_months, ActivityPeriod::Quarter)?;
    let yearly = gdp_activity_tiers_for_period(&editor_months, ActivityPeriod::Year)?;
    output_frames.extend([monthly, quarterly, yearly]);
    Ok(())
}

fn labor_monthly_frame(base: &DataFrame) -> Result<DataFrame> {
    base.clone()
        .lazy()
        .group_by([col("year_month"), col("page_namespace"), col("user_type")])
        .agg([
            col("event_user_id").n_unique().alias("unique_editors"),
            col("revision_id").count().alias("total_edits"),
            col("revision_text_bytes_diff").sum().alias("net_bytes"),
            col("is_reverted")
                .cast(DataType::UInt32)
                .sum()
                .alias("reverted_edits"),
        ])
        .collect()
        .map_err(Into::into)
}

fn registered_editor_totals(base: &DataFrame) -> Result<DataFrame> {
    base.clone()
        .lazy()
        .filter(col("user_type").eq(lit("registered")))
        .group_by([col("event_user_id")])
        .agg([
            col("revision_id").count().alias("total_edits"),
            col("year").min().cast(DataType::Int32).alias("cohort_year"),
        ])
        .collect()
        .map_err(Into::into)
}

fn finalize_funnel(stats: HashMap<i64, (i32, u32)>, wiki: &str, output_dir: &Path) -> Result<()> {
    let mut by_cohort: BTreeMap<i32, (u32, u32, u32, u32)> = BTreeMap::new();
    for (_, (cohort_year, total_edits)) in stats {
        let entry = by_cohort.entry(cohort_year).or_insert((0, 0, 0, 0));
        entry.0 += 1;
        if total_edits >= 5 {
            entry.1 += 1;
        }
        if total_edits >= 25 {
            entry.2 += 1;
        }
        if total_edits >= 100 {
            entry.3 += 1;
        }
    }

    let funnel_columns = vec![
        Column::new(
            "cohort_year".into(),
            by_cohort
                .keys()
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
        ),
        Column::new(
            "cohort_size".into(),
            by_cohort.values().map(|entry| entry.0).collect::<Vec<_>>(),
        ),
        Column::new(
            "reached_5".into(),
            by_cohort.values().map(|entry| entry.1).collect::<Vec<_>>(),
        ),
        Column::new(
            "reached_25".into(),
            by_cohort.values().map(|entry| entry.2).collect::<Vec<_>>(),
        ),
        Column::new(
            "reached_100".into(),
            by_cohort.values().map(|entry| entry.3).collect::<Vec<_>>(),
        ),
    ];
    let mut funnel = DataFrame::new_infer_height(funnel_columns)?;
    add_wiki_column(&mut funnel, wiki)?;
    write_output(&mut funnel, wiki, "business_funnel", output_dir)
}

fn finalize_labor_cohorts(
    spans: HashMap<i64, (i32, i32)>,
    wiki: &str,
    output_dir: &Path,
) -> Result<()> {
    let mut rows: Vec<(i32, i32)> = spans.into_values().collect();
    rows.sort();
    let all_years: Vec<i32> = rows
        .iter()
        .flat_map(|(first, last)| [*first, *last])
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();
    let editor_span_columns = vec![
        Column::new(
            "cohort_year".into(),
            rows.iter()
                .map(|(first, _)| Some(*first))
                .collect::<Vec<_>>(),
        ),
        Column::new(
            "last_year".into(),
            rows.iter().map(|(_, last)| Some(*last)).collect::<Vec<_>>(),
        ),
    ];
    let editor_spans = DataFrame::new_infer_height(editor_span_columns)?;
    let mut cohort_out = labor::build_cohort_output(&editor_spans, &all_years)?;
    add_wiki_column(&mut cohort_out, wiki)?;
    write_output(&mut cohort_out, wiki, "labor_cohorts", output_dir)
}

fn compute_page_weekly_edits(
    wiki: &str,
    data_dir: &Path,
    output_dir: &Path,
    config: &WeeklyAggregationConfig,
) -> Result<Option<WeeklyAggregationReport>> {
    compute_page_weekly_edits_for_snapshot(wiki, data_dir, output_dir, config, None)
}

fn compute_page_weekly_edits_for_snapshot(
    wiki: &str,
    data_dir: &Path,
    output_dir: &Path,
    config: &WeeklyAggregationConfig,
    snapshot: Option<&str>,
) -> Result<Option<WeeklyAggregationReport>> {
    let aggregation_started = Instant::now();
    let governed_scratch_root = config
        .scratch_root
        .clone()
        .or_else(|| Some(output_dir.join(wiki)));
    let governor_paths = GovernorPaths::new(data_dir.to_path_buf(), governed_scratch_root);
    let governor = ResourceGovernor::from_environment(governor_paths)?;
    let partitions = match snapshot {
        Some(snapshot) => {
            let layer_result = storage::snapshot_compute_layer(
                data_dir,
                wiki,
                snapshot,
                storage::GenerationLayer::Warehouse,
            );
            let layer = layer_result?;
            let result = storage::snapshot_partition_specs(data_dir, wiki, snapshot, layer);
            result?
        }
        None => {
            let layer =
                storage::active_compute_layer(data_dir, wiki, storage::GenerationLayer::Warehouse)?;
            storage::active_partition_specs(data_dir, wiki, layer)?
        }
    };
    if partitions.is_empty() {
        info!(
            wiki = wiki,
            "skipping page_weekly_edits: no warehouse partitions found"
        );
        return Ok(None);
    }
    for partition in &partitions {
        let bytes = partition.files.iter().try_fold(0_u64, |total, path| {
            total
                .checked_add(path.metadata()?.len())
                .context("logical partition byte count overflow")
        })?;
        governor.validate_logical_partition(&partition.year_month, bytes)?;
    }
    governor.checkpoint("page_weekly_edits_preflight")?;

    let event_date_options = StrptimeOptions {
        format: Some("%Y-%m-%d".into()),
        strict: true,
        exact: true,
        cache: true,
    };

    // Reduce one calendar-month partition at a time, then route its weekly
    // rows into stable primary page buckets on disk. Large configurations
    // subdivide one primary bucket at a time before reconciliation, so Polars
    // never needs to group or sort more than one logical secondary bucket.
    // All rows for a page use the same logical bucket, which keeps previous-
    // week calculation local while Rust controls the bounded traversal.
    let runs = WeeklyRunDir::new(output_dir, wiki, config.scratch_root.as_deref())?;
    let mut staged_paths = Vec::with_capacity(partitions.len());
    let mut primary_bucket_rows = vec![0usize; config.primary_bucket_count];
    let mut primary_bucket_edits = vec![0i64; config.primary_bucket_count];
    let mut total_edits_before = 0i64;
    let mut reduction_peak = ResourcePeak::default();
    let mut reconciliation_peak = ResourcePeak::default();
    let reduction_started = Instant::now();
    info!(
        wiki = wiki,
        partitions = partitions.len(),
        primary_buckets = config.primary_bucket_count,
        secondary_buckets = config.secondary_bucket_count,
        logical_buckets = config.logical_bucket_count(),
        max_active_parquet_writers = governor.budget().max_active_parquet_writers,
        active_parquet_writers = 1,
        run_dir = %runs.path().display(),
        "page_weekly_edits: starting disk-backed bucket reduction"
    );
    let mut running_rows: usize = 0;
    for (idx, partition) in partitions.iter().enumerate() {
        let started = Instant::now();
        let partition_weekly = warehouse_lazyframe(&partition.files)?
            .with_column(
                col("event_timestamp")
                    .str()
                    .slice(lit(0), lit(10))
                    .str()
                    .to_date(event_date_options.clone())
                    .dt()
                    .truncate(lit("1w"))
                    .alias("week_start"),
            )
            .group_by(weekly_group_keys())
            // Some historical revisions have a valid timestamp/revision ID
            // but no recoverable page identity (for example a suppressed or
            // deleted page). The weekly pipeline intentionally supports a
            // null page key, so count rows rather than non-null page IDs.
            .agg([len().alias("edits")])
            .collect()?;
        running_rows += partition_weekly.height();
        let partition_edits = sum_edits_column(std::slice::from_ref(&partition_weekly))?;
        let source_rows = parquet_paths_row_count(&partition.files)?;
        anyhow::ensure!(
            u64::try_from(partition_edits)? == source_rows,
            "page_weekly_edits partition {} lost edits during monthly reduction: {source_rows} source rows, {partition_edits} reduced edits",
            partition.year_month
        );
        total_edits_before = total_edits_before
            .checked_add(partition_edits)
            .context("page_weekly_edits input edit count overflow")?;
        let stage_result = runs.stage(
            idx,
            &mut primary_bucket_rows,
            &mut primary_bucket_edits,
            partition_weekly,
            config.primary_bucket_count,
        );
        staged_paths.push(stage_result?);
        for file in &partition.files {
            storage::discard_path_cache(file);
        }
        let memory = MemorySnapshot::capture();
        reduction_peak.observe(memory, None);
        governor.checkpoint("page_weekly_edits_reduce_partition")?;
        info!(
            wiki = wiki,
            partition = idx + 1,
            total_partitions = partitions.len(),
            year_month = partition.year_month.as_str(),
            running_rows = running_rows,
            elapsed_ms = started.elapsed().as_secs_f64() * 1_000.0,
            rss_bytes = ?memory.rss_bytes,
            cgroup_current_bytes = ?memory.cgroup_current_bytes,
            cgroup_peak_bytes = ?memory.cgroup_peak_bytes,
            cgroup_limit_bytes = ?memory.cgroup_limit_bytes,
            "page_weekly_edits: reduced and staged partition"
        );
    }
    anyhow::ensure!(
        checked_sum_i64(&primary_bucket_edits, "primary routing edit count")? == total_edits_before,
        "page_weekly_edits primary routing lost or duplicated edits"
    );
    let mut scratch_peak_bytes = runs.size_bytes()?;
    let primary_paths = if config.secondary_bucket_count > 1 {
        let compaction = compact_weekly_primary_buckets(
            &runs,
            &staged_paths,
            PrimaryBucketTotals {
                rows: &primary_bucket_rows,
                edits: &primary_bucket_edits,
            },
            governor.budget().max_active_parquet_writers,
            &governor,
            &mut scratch_peak_bytes,
            &mut reduction_peak,
        );
        let paths = compaction?;
        for path in &staged_paths {
            fs::remove_file(path)?;
        }
        staged_paths.clear();
        paths
    } else {
        vec![None; config.primary_bucket_count]
    };
    let reduction_elapsed_ms = reduction_started.elapsed().as_millis() as u64;
    let mut bucket_rows = if config.secondary_bucket_count == 1 {
        primary_bucket_rows.clone()
    } else {
        vec![0usize; config.logical_bucket_count()]
    };
    let mut working_storage_peak_bytes = scratch_peak_bytes;
    reduction_peak.scratch_bytes = Some(scratch_peak_bytes);
    reconciliation_peak.scratch_bytes = Some(scratch_peak_bytes);

    let final_path = output_dir.join(wiki).join("page_weekly_edits.parquet");
    let mut output: Option<AtomicBatchedParquetWriter> = None;
    let mut total_edits_after = 0i64;
    let mut output_rows = 0usize;
    let mut min_week_start: Option<i32> = None;
    let mut max_week_start: Option<i32> = None;
    let reconciliation_started = Instant::now();
    info!(
        wiki = wiki,
        staged_rows = running_rows,
        nonempty_buckets = bucket_rows.iter().filter(|&&rows| rows > 0).count(),
        "page_weekly_edits: reconciling staged buckets"
    );
    for primary_bucket in 0..config.primary_bucket_count {
        if primary_bucket_rows[primary_bucket] == 0 {
            continue;
        }
        let secondary_paths = if config.secondary_bucket_count > 1 {
            let primary_path = primary_paths[primary_bucket]
                .as_ref()
                .with_context(|| format!("missing non-empty primary bucket {primary_bucket}"))?;
            let routing = route_primary_to_secondary_buckets(
                &runs,
                primary_path,
                primary_bucket,
                config,
                BucketTotals {
                    rows: primary_bucket_rows[primary_bucket],
                    edits: primary_bucket_edits[primary_bucket],
                },
                &governor,
                &mut reconciliation_peak,
            );
            let routed = routing?;
            anyhow::ensure!(
                routed.peak_active_writers <= governor.budget().max_active_parquet_writers,
                "secondary routing reported a writer peak above its governed limit"
            );
            scratch_peak_bytes = scratch_peak_bytes.max(runs.size_bytes()?);
            working_storage_peak_bytes = working_storage_peak_bytes.max(scratch_peak_bytes);
            fs::remove_file(primary_path)?;
            for (secondary, &rows) in routed.rows.iter().enumerate() {
                bucket_rows[primary_bucket * config.secondary_bucket_count + secondary] = rows;
            }
            Some(routed.paths)
        } else {
            None
        };

        for secondary_bucket in 0..config.secondary_bucket_count {
            let bucket = primary_bucket * config.secondary_bucket_count + secondary_bucket;
            let staged_rows = bucket_rows[bucket];
            if staged_rows == 0 {
                continue;
            }
            let started = Instant::now();
            let staged_path = match &secondary_paths {
                Some(paths) => Some(
                    paths[secondary_bucket]
                        .clone()
                        .context("missing non-empty secondary bucket")?,
                ),
                None => None,
            };
            let staged = match staged_path.as_deref() {
                Some(path) => ParquetReader::new(File::open(path)?).finish()?,
                None => read_staged_weekly_bucket(&staged_paths, primary_bucket)?,
            };
            let actual_staged_rows = staged.height();
            anyhow::ensure!(
                actual_staged_rows == staged_rows,
                "page_weekly_edits bucket {primary_bucket}/{secondary_bucket} row count changed: expected {staged_rows}, read {actual_staged_rows}"
            );
            let bucket_edits_before = sum_edits_column(std::slice::from_ref(&staged))?;
            let merged = staged
                .lazy()
                .group_by(weekly_group_keys())
                .agg([col("edits").sum()])
                .collect()?;
            let merged = sort_frame(merged, weekly_sort_keys())?;
            let weeks = merged.column("week_start")?.date()?.physical();
            min_week_start = min_week_start.into_iter().chain(weeks.min()).min();
            max_week_start = max_week_start.into_iter().chain(weeks.max()).max();
            let bucket_edits_after = sum_edits_column(std::slice::from_ref(&merged))?;
            anyhow::ensure!(
                bucket_edits_before == bucket_edits_after,
                "page_weekly_edits bucket {primary_bucket}/{secondary_bucket} lost or duplicated edits: {bucket_edits_before} before, {bucket_edits_after} after"
            );
            let mut result = add_weekly_change_columns(merged, wiki)?;
            if output.is_none() {
                let schema = result.schema();
                let writer = AtomicBatchedParquetWriter::new(final_path.clone(), schema)?;
                output = Some(writer);
            }
            output
                .as_mut()
                .context("page_weekly_edits output writer was not initialized")?
                .write_batch(&mut result)?;
            reclaim_completed_weekly_scratch(staged_path.as_deref())?;
            output_rows += result.height();
            total_edits_after = total_edits_after
                .checked_add(bucket_edits_after)
                .context("page_weekly_edits output edit count overflow")?;
            let working_bytes = runs
                .size_bytes()?
                .checked_add(
                    output
                        .as_ref()
                        .context("page_weekly_edits output writer was not initialized")?
                        .current_bytes()?,
                )
                .context("page_weekly_edits working storage byte count overflow")?;
            working_storage_peak_bytes = working_storage_peak_bytes.max(working_bytes);
            let memory = MemorySnapshot::capture();
            reconciliation_peak.observe(memory, None);
            governor.checkpoint("page_weekly_edits_reconcile_bucket")?;
            info!(
                wiki = wiki,
                primary_bucket,
                secondary_bucket,
                logical_bucket = bucket,
                total_buckets = config.logical_bucket_count(),
                staged_rows = staged_rows,
                merged_rows = result.height(),
                output_rows = output_rows,
                elapsed_ms = started.elapsed().as_secs_f64() * 1_000.0,
                rss_bytes = ?memory.rss_bytes,
                cgroup_current_bytes = ?memory.cgroup_current_bytes,
                cgroup_peak_bytes = ?memory.cgroup_peak_bytes,
                cgroup_limit_bytes = ?memory.cgroup_limit_bytes,
                "page_weekly_edits: reconciled and wrote bucket"
            );
        }
    }

    anyhow::ensure!(
        total_edits_before == total_edits_after,
        "page_weekly_edits lost or duplicated data for {wiki}: {total_edits_before} edits before merge, {total_edits_after} after"
    );
    let output = output.context("page_weekly_edits produced no rows from non-empty partitions")?;
    let bytes = output.finish()?;
    working_storage_peak_bytes = working_storage_peak_bytes.max(bytes);
    let reconciliation_elapsed_ms = reconciliation_started.elapsed().as_millis() as u64;
    let memory = MemorySnapshot::capture();
    reconciliation_peak.scratch_bytes = Some(scratch_peak_bytes);
    let minimum_week_start = min_week_start.and_then(format_epoch_day);
    let maximum_week_start = max_week_start.and_then(format_epoch_day);
    info!(
        wiki = wiki,
        metric = "page_weekly_edits",
        rows = output_rows,
        columns = 11,
        total_edits = total_edits_after,
        min_week_start = ?minimum_week_start,
        max_week_start = ?maximum_week_start,
        largest_bucket_staged_rows = bucket_rows.iter().copied().max().unwrap_or(0),
        bytes = bytes,
        path = %final_path.display(),
        rss_bytes = ?memory.rss_bytes,
        cgroup_current_bytes = ?memory.cgroup_current_bytes,
        cgroup_peak_bytes = ?memory.cgroup_peak_bytes,
        cgroup_limit_bytes = ?memory.cgroup_limit_bytes,
        "page_weekly_edits: published disk-backed result"
    );
    Ok(Some(WeeklyAggregationReport {
        wiki: wiki.to_string(),
        bucket_count: config.logical_bucket_count(),
        primary_bucket_count: config.primary_bucket_count,
        secondary_bucket_count: config.secondary_bucket_count,
        partitions: partitions.len(),
        staged_rows: running_rows,
        output_rows,
        total_edits: total_edits_after,
        minimum_week_start,
        maximum_week_start,
        bucket_staged_rows: bucket_rows.clone(),
        primary_bucket_staged_rows: primary_bucket_rows,
        largest_bucket_staged_rows: bucket_rows.iter().copied().max().unwrap_or(0),
        output_bytes: bytes,
        scratch_peak_bytes,
        working_storage_peak_bytes,
        reduction_peak,
        reconciliation_peak,
        final_memory: memory,
        reduction_elapsed_ms,
        reconciliation_elapsed_ms,
        elapsed_ms: aggregation_started.elapsed().as_millis() as u64,
    }))
}

fn weekly_group_keys() -> [Expr; 4] {
    [
        col("page_id"),
        col("page_namespace"),
        col("page_title"),
        col("week_start"),
    ]
}

fn weekly_sort_keys() -> [&'static str; 4] {
    ["page_id", "page_namespace", "page_title", "week_start"]
}

fn stable_weekly_bucket(page_id: Option<i64>, bucket_count: usize) -> usize {
    determinism::stable_page_hash(page_id) as usize & (bucket_count - 1)
}

fn stable_weekly_secondary_bucket(
    page_id: Option<i64>,
    primary_bucket_count: usize,
    secondary_bucket_count: usize,
) -> usize {
    (determinism::stable_page_hash(page_id) >> primary_bucket_count.trailing_zeros()) as usize
        & (secondary_bucket_count - 1)
}

fn format_epoch_day(day: i32) -> Option<String> {
    NaiveDate::from_ymd_opt(1970, 1, 1)?
        .checked_add_signed(Duration::days(i64::from(day)))
        .map(|date| date.format("%Y-%m-%d").to_string())
}

fn stage_weekly_partition(
    runs: &WeeklyRunDir,
    partition_index: usize,
    bucket_rows: &mut [usize],
    bucket_edits: &mut [i64],
    partition: DataFrame,
    bucket_count: usize,
) -> Result<PathBuf> {
    let page_ids = partition.column("page_id")?.i64()?;
    let mut row_indices: Vec<Vec<IdxSize>> = (0..bucket_count).map(|_| Vec::new()).collect();
    for row in 0..partition.height() {
        row_indices[stable_weekly_bucket(page_ids.get(row), bucket_count)].push(row as IdxSize);
    }

    let path = runs.partition_path(partition_index);
    let mut writer: Option<BatchedWriter<File>> = None;
    for (bucket, indices) in row_indices.into_iter().enumerate() {
        if indices.is_empty() {
            continue;
        }
        let take = IdxCa::from_vec("weekly_bucket_rows".into(), indices);
        let mut bucket_frame = partition.take(&take)?;
        let primary_bucket_column = Column::new(
            "_primary_bucket".into(),
            vec![u32::try_from(bucket)?; bucket_frame.height()],
        );
        bucket_frame.with_column(primary_bucket_column)?;
        bucket_frame.rechunk_mut();
        if writer.is_none() {
            writer = Some(
                ParquetWriter::new(File::create(&path)?)
                    .with_compression(ParquetCompression::Zstd(None))
                    .batched(bucket_frame.schema())?,
            );
        }
        writer
            .as_mut()
            .context("weekly partition writer was not initialized")?
            .write_batch(&bucket_frame)?;
        bucket_rows[bucket] += bucket_frame.height();
        bucket_edits[bucket] = bucket_edits[bucket]
            .checked_add(sum_edits_column(std::slice::from_ref(&bucket_frame))?)
            .context("primary bucket edit count overflow")?;
    }
    writer
        .context("weekly partition produced no stable buckets")?
        .finish()?;
    Ok(path)
}

fn read_staged_weekly_bucket(paths: &[PathBuf], bucket: usize) -> Result<DataFrame> {
    let parquet_paths = paths
        .iter()
        .map(|path| path.to_string_lossy().to_string())
        .collect::<Vec<_>>();
    let sources = parquet_paths
        .iter()
        .map(|path| path.as_str().into())
        .collect();
    let scan_args = ScanArgsParquet {
        cache: false,
        ..Default::default()
    };
    let staged = LazyFrame::scan_parquet_sources(ScanSources::Paths(sources), scan_args)?;
    let bucket = u32::try_from(bucket)?;
    Ok(staged
        .filter(col("_primary_bucket").eq(lit(bucket)))
        .drop(cols(["_primary_bucket"]))
        .collect()?)
}

fn read_staged_primary_range(path: &Path, start: usize, end: usize) -> Result<DataFrame> {
    let path_string = path.to_string_lossy().to_string();
    let sources = vec![path_string.as_str().into()];
    let scan_args = ScanArgsParquet {
        cache: false,
        ..Default::default()
    };
    let staged = LazyFrame::scan_parquet_sources(ScanSources::Paths(sources.into()), scan_args)?;
    Ok(staged
        .filter(
            col("_primary_bucket")
                .gt_eq(lit(u32::try_from(start)?))
                .and(col("_primary_bucket").lt(lit(u32::try_from(end)?))),
        )
        .collect()?)
}

struct PrimaryBucketTotals<'a> {
    rows: &'a [usize],
    edits: &'a [i64],
}

#[derive(Clone, Copy)]
struct BucketTotals {
    rows: usize,
    edits: i64,
}

fn compact_weekly_primary_buckets(
    runs: &WeeklyRunDir,
    staged_paths: &[PathBuf],
    expected: PrimaryBucketTotals<'_>,
    writer_limit: usize,
    governor: &ResourceGovernor,
    scratch_peak_bytes: &mut u64,
    reduction_peak: &mut ResourcePeak,
) -> Result<Vec<Option<PathBuf>>> {
    anyhow::ensure!(
        writer_limit > 0,
        "primary compaction writer limit must be positive"
    );
    anyhow::ensure!(
        expected.rows.len() == expected.edits.len(),
        "primary compaction row/edit bucket counts differ"
    );
    let primary_bucket_count = expected.rows.len();
    let mut paths = vec![None; primary_bucket_count];
    let mut actual_rows = vec![0usize; primary_bucket_count];
    let mut actual_edits = vec![0i64; primary_bucket_count];

    for start in (0..primary_bucket_count).step_by(writer_limit) {
        let end = (start + writer_limit).min(primary_bucket_count);
        let mut writers: BTreeMap<usize, BatchedWriter<File>> = BTreeMap::new();
        for staged_path in staged_paths {
            let staged = read_staged_primary_range(staged_path, start, end)?;
            if staged.height() == 0 {
                continue;
            }
            let input_edits = sum_edits_column(std::slice::from_ref(&staged))?;
            let primary_buckets = staged.column("_primary_bucket")?.u32()?;
            let mut row_indices: Vec<Vec<IdxSize>> = (start..end).map(|_| Vec::new()).collect();
            for row in 0..staged.height() {
                let bucket = primary_buckets
                    .get(row)
                    .context("primary staging row has no bucket")?;
                let bucket = usize::try_from(bucket)?;
                anyhow::ensure!(
                    (start..end).contains(&bucket),
                    "primary staging row escaped its compaction range"
                );
                row_indices[bucket - start].push(row as IdxSize);
            }
            let mut routed_edits = 0i64;
            for (offset, indices) in row_indices.into_iter().enumerate() {
                if indices.is_empty() {
                    continue;
                }
                let bucket = start + offset;
                let take = IdxCa::from_vec("primary_compaction_rows".into(), indices);
                let mut frame = staged.take(&take)?;
                frame.drop_in_place("_primary_bucket")?;
                frame.rechunk_mut();
                let edits = sum_edits_column(std::slice::from_ref(&frame))?;
                routed_edits = routed_edits
                    .checked_add(edits)
                    .context("primary compaction edit count overflow")?;
                actual_rows[bucket] = actual_rows[bucket]
                    .checked_add(frame.height())
                    .context("primary compaction row count overflow")?;
                actual_edits[bucket] = actual_edits[bucket]
                    .checked_add(edits)
                    .context("primary compaction edit count overflow")?;
                let path = runs.primary_path(bucket);
                let writer = match writers.entry(bucket) {
                    std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        paths[bucket] = Some(path.clone());
                        entry.insert(
                            ParquetWriter::new(File::create(path)?)
                                .with_compression(ParquetCompression::Zstd(None))
                                .batched(frame.schema())?,
                        )
                    }
                };
                writer.write_batch(&frame)?;
            }
            anyhow::ensure!(
                input_edits == routed_edits,
                "primary compaction range {start}..{end} lost or duplicated edits"
            );
        }
        for writer in writers.into_values() {
            writer.finish()?;
        }
        *scratch_peak_bytes = (*scratch_peak_bytes).max(runs.size_bytes()?);
        reduction_peak.observe(MemorySnapshot::capture(), Some(*scratch_peak_bytes));
        governor.checkpoint("page_weekly_edits_compact_primary")?;
    }

    for bucket in 0..primary_bucket_count {
        anyhow::ensure!(
            actual_rows[bucket] == expected.rows[bucket],
            "primary bucket {bucket} row count changed during compaction"
        );
        anyhow::ensure!(
            actual_edits[bucket] == expected.edits[bucket],
            "primary bucket {bucket} edit count changed during compaction"
        );
    }
    Ok(paths)
}

#[derive(Debug)]
struct SecondaryRouting {
    paths: Vec<Option<PathBuf>>,
    rows: Vec<usize>,
    peak_active_writers: usize,
}

fn parquet_paths_row_count(paths: &[PathBuf]) -> Result<u64> {
    paths.iter().try_fold(0u64, |total, path| {
        let rows = ParquetReader::new(File::open(path)?).num_rows()?;
        total
            .checked_add(u64::try_from(rows)?)
            .context("Parquet row count overflow")
    })
}

fn route_primary_to_secondary_buckets(
    runs: &WeeklyRunDir,
    primary_path: &Path,
    primary_bucket: usize,
    config: &WeeklyAggregationConfig,
    expected: BucketTotals,
    governor: &ResourceGovernor,
    reconciliation_peak: &mut ResourcePeak,
) -> Result<SecondaryRouting> {
    let primary_bucket_count = config.primary_bucket_count;
    let secondary_bucket_count = config.secondary_bucket_count;
    anyhow::ensure!(
        secondary_bucket_count <= governor.budget().max_active_parquet_writers,
        "secondary bucket count {secondary_bucket_count} exceeds the governed Parquet writer limit {}",
        governor.budget().max_active_parquet_writers
    );
    let mut paths = vec![None; secondary_bucket_count];
    let mut rows = vec![0usize; secondary_bucket_count];
    let mut edits = vec![0i64; secondary_bucket_count];
    let mut writers: BTreeMap<usize, BatchedWriter<File>> = BTreeMap::new();
    let mut peak_active_writers = 0usize;
    let mut reader =
        storage::SequentialParquetReader::new(primary_path, None, WEEKLY_ROUTING_BATCH_ROWS)?;
    anyhow::ensure!(
        reader.rows() == expected.rows,
        "primary bucket {primary_bucket} footer row count changed before secondary routing"
    );

    while let Some(batch) = reader.next_batch()? {
        let input_edits = sum_edits_column(std::slice::from_ref(&batch))?;
        let page_ids = batch.column("page_id")?.i64()?;
        let mut row_indices: Vec<Vec<IdxSize>> =
            (0..secondary_bucket_count).map(|_| Vec::new()).collect();
        for row in 0..batch.height() {
            let secondary = stable_weekly_secondary_bucket(
                page_ids.get(row),
                primary_bucket_count,
                secondary_bucket_count,
            );
            row_indices[secondary].push(row as IdxSize);
        }
        let mut routed_edits = 0i64;
        for (secondary, indices) in row_indices.into_iter().enumerate() {
            if indices.is_empty() {
                continue;
            }
            let take = IdxCa::from_vec("secondary_routing_rows".into(), indices);
            let mut frame = batch.take(&take)?;
            frame.rechunk_mut();
            let frame_edits = sum_edits_column(std::slice::from_ref(&frame))?;
            routed_edits = routed_edits
                .checked_add(frame_edits)
                .context("secondary routing edit count overflow")?;
            rows[secondary] = rows[secondary]
                .checked_add(frame.height())
                .context("secondary routing row count overflow")?;
            edits[secondary] = edits[secondary]
                .checked_add(frame_edits)
                .context("secondary routing edit count overflow")?;
            let path = runs.secondary_path(primary_bucket, secondary);
            let writer = match writers.entry(secondary) {
                std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
                std::collections::btree_map::Entry::Vacant(entry) => {
                    paths[secondary] = Some(path.clone());
                    entry.insert(
                        ParquetWriter::new(File::create(path)?)
                            .with_compression(ParquetCompression::Zstd(None))
                            .batched(frame.schema())?,
                    )
                }
            };
            writer.write_batch(&frame)?;
            peak_active_writers = peak_active_writers.max(writers.len());
            anyhow::ensure!(
                peak_active_writers <= governor.budget().max_active_parquet_writers,
                "secondary routing exceeded the governed Parquet writer limit"
            );
        }
        anyhow::ensure!(
            input_edits == routed_edits,
            "primary bucket {primary_bucket} lost or duplicated edits during secondary routing"
        );
        reconciliation_peak.observe(MemorySnapshot::capture(), None);
        governor.checkpoint("page_weekly_edits_route_secondary")?;
    }
    for writer in writers.into_values() {
        writer.finish()?;
    }
    anyhow::ensure!(
        checked_sum_usize(&rows, "secondary routing row count")? == expected.rows,
        "primary bucket {primary_bucket} row count changed during secondary routing"
    );
    anyhow::ensure!(
        checked_sum_i64(&edits, "secondary routing edit count")? == expected.edits,
        "primary bucket {primary_bucket} edit count changed during secondary routing"
    );
    Ok(SecondaryRouting {
        paths,
        rows,
        peak_active_writers,
    })
}

fn reclaim_completed_weekly_scratch(path: Option<&Path>) -> Result<()> {
    if let Some(path) = path {
        fs::remove_file(path)?;
    }
    Ok(())
}

fn add_weekly_change_columns(mut weekly: DataFrame, wiki: &str) -> Result<DataFrame> {
    let previous_week_edits = previous_week_edits(&weekly)?;
    let previous_column = Column::new("previous_week_edits".into(), previous_week_edits);
    weekly.with_column(previous_column)?;
    weekly
        .lazy()
        .with_column(
            (col("edits").cast(DataType::Int64) - col("previous_week_edits").cast(DataType::Int64))
                .alias("wow_change"),
        )
        .with_column(
            when(col("previous_week_edits").eq(lit(0u32)))
                .then(lit(NULL))
                .otherwise(
                    col("wow_change").cast(DataType::Float64)
                        / col("previous_week_edits").cast(DataType::Float64),
                )
                .alias("wow_rate"),
        )
        .with_columns([
            col("week_start").dt().iso_year().alias("iso_year"),
            col("week_start")
                .dt()
                .week()
                .cast(DataType::Int32)
                .alias("iso_week"),
            col("week_start")
                .dt()
                .to_string("%Y-%m-%d")
                .alias("week_start"),
        ])
        .select([
            col("week_start"),
            col("iso_year"),
            col("iso_week"),
            col("page_id"),
            col("page_title"),
            col("page_namespace"),
            col("edits"),
            col("previous_week_edits"),
            col("wow_change"),
            col("wow_rate"),
            lit(wiki).alias("wiki"),
        ])
        .collect()
        .map_err(Into::into)
}

fn previous_week_edits(weekly: &DataFrame) -> Result<Vec<u32>> {
    let page_ids = weekly.column("page_id")?.i64()?;
    let namespaces = weekly.column("page_namespace")?.i32()?;
    let titles = weekly.column("page_title")?.str()?;
    let weeks = weekly.column("week_start")?.date()?.physical();
    let edits = weekly.column("edits")?.u32()?;
    let mut previous = Vec::with_capacity(weekly.height());

    for row in 0..weekly.height() {
        if row == 0 {
            previous.push(0);
            continue;
        }
        let prior = row - 1;
        let same_page = page_ids.get(row) == page_ids.get(prior)
            && namespaces.get(row) == namespaces.get(prior)
            && titles.get(row) == titles.get(prior);
        let current_week = weeks.get(row);
        let prior_week = weeks.get(prior);
        anyhow::ensure!(
            !(same_page && current_week == prior_week),
            "page_weekly_edits contains a duplicate weekly key at row {row}"
        );
        let is_continuation = same_page
            && current_week
                .zip(prior_week)
                .is_some_and(|(current, prior)| current.checked_sub(prior) == Some(7));
        previous.push(if is_continuation {
            edits
                .get(prior)
                .context("page_weekly_edits contains a null edits value")?
        } else {
            0
        });
    }
    Ok(previous)
}

struct WeeklyRunDir {
    path: PathBuf,
}

impl WeeklyRunDir {
    fn new(output_dir: &Path, wiki: &str, scratch_root: Option<&Path>) -> Result<Self> {
        let parent = scratch_root
            .map(|root| root.join(wiki))
            .unwrap_or_else(|| output_dir.join(wiki));
        fs::create_dir_all(&parent)?;
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let run_id = valid_weekly_run_id(env::var("WIKI_ECON_RUN_ID").ok());
        let path = parent.join(format!(
            ".page_weekly_edits-runs-{run_id}-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir(&path)?;
        Ok(Self { path })
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn partition_path(&self, partition: usize) -> PathBuf {
        self.path.join(format!("partition-{partition:06}.parquet"))
    }

    fn primary_path(&self, primary: usize) -> PathBuf {
        self.path.join(format!("primary-{primary:04}.parquet"))
    }

    fn secondary_path(&self, primary: usize, secondary: usize) -> PathBuf {
        self.path.join(format!(
            "primary-{primary:04}-secondary-{secondary:04}.parquet"
        ))
    }

    fn size_bytes(&self) -> Result<u64> {
        fs::read_dir(&self.path)?.try_fold(0_u64, |total, entry| {
            let bytes = entry?.metadata()?.len();
            total
                .checked_add(bytes)
                .context("weekly scratch byte count overflow")
        })
    }

    fn stage(
        &self,
        partition_index: usize,
        bucket_rows: &mut [usize],
        bucket_edits: &mut [i64],
        partition: DataFrame,
        bucket_count: usize,
    ) -> Result<PathBuf> {
        stage_weekly_partition(
            self,
            partition_index,
            bucket_rows,
            bucket_edits,
            partition,
            bucket_count,
        )
    }
}

fn valid_weekly_run_id(value: Option<String>) -> String {
    value
        .filter(|value| {
            !value.is_empty()
                && value
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        })
        .unwrap_or_else(|| "standalone".to_string())
}

impl Drop for WeeklyRunDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
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

struct AtomicBatchedParquetWriter {
    pending: PendingOutput,
    writer: Option<BatchedWriter<File>>,
    semantics: crate::artifact_receipt::SemanticAccumulator,
}

impl AtomicBatchedParquetWriter {
    fn new(final_path: PathBuf, schema: &Schema) -> Result<Self> {
        let identity = final_path
            .file_name()
            .and_then(|name| name.to_str())
            .context("page_weekly_edits output has no UTF-8 filename")?
            .to_string();
        let pending = PendingOutput::new(final_path)?;
        let file = File::create(&pending.temp_path)?;
        let writer = ParquetWriter::new(file)
            .with_compression(ParquetCompression::Zstd(None))
            .batched(schema)?;
        Ok(Self {
            pending,
            writer: Some(writer),
            semantics: crate::artifact_receipt::SemanticAccumulator::new(
                crate::artifact_receipt::SemanticSpec::for_identity(&identity),
            ),
        })
    }

    fn write_batch(&mut self, df: &mut DataFrame) -> Result<()> {
        df.rechunk_mut();
        self.semantics.observe(df)?;
        self.writer
            .as_mut()
            .context("page_weekly_edits output writer was already finished")?
            .write_batch(df)?;
        Ok(())
    }

    fn current_bytes(&self) -> Result<u64> {
        Ok(fs::metadata(&self.pending.temp_path)?.len())
    }

    fn finish(mut self) -> Result<u64> {
        self.writer
            .take()
            .context("page_weekly_edits output writer was already finished")?
            .finish()?;
        let final_path = self.pending.final_path.clone();
        let bytes = self.pending.publish()?;
        crate::artifact_receipt::write_semantic_draft(&final_path, self.semantics)?;
        Ok(bytes)
    }
}

fn sum_edits_column(frames: &[DataFrame]) -> Result<i64> {
    frames.iter().try_fold(0i64, |acc, frame| {
        let sum = frame
            .column("edits")?
            .cast(&DataType::Int64)?
            .i64()?
            .sum()
            .unwrap_or(0);
        acc.checked_add(sum).context("edit column sum overflow")
    })
}

fn checked_sum_i64(values: &[i64], label: &str) -> Result<i64> {
    values.iter().try_fold(0i64, |total, &value| {
        total
            .checked_add(value)
            .with_context(|| format!("{label} overflow"))
    })
}

fn checked_sum_usize(values: &[usize], label: &str) -> Result<usize> {
    values.iter().try_fold(0usize, |total, &value| {
        total
            .checked_add(value)
            .with_context(|| format!("{label} overflow"))
    })
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

fn compute_all_incremental(
    wiki: &str,
    data_dir: &Path,
    output_dir: &Path,
    snapshot: Option<&str>,
    plan: ComputePlan,
) -> Result<usize> {
    if !plan.any_nonweekly() {
        return Ok(0);
    }
    let partitions = match snapshot {
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
    let mut gdp_frames = Vec::new();
    let mut gdp_type_frames = Vec::new();
    let mut gdp_tier_frames = Vec::new();
    let mut gdp_editor_month_frames = Vec::new();
    let mut gdp_activity_year = None;
    let mut labor_monthly_frames = Vec::new();
    let mut registered_state = plan.lifecycle.must_compute().then(RegisteredState::new);

    let partition_count = partitions.len();
    for partition in partitions {
        if plan.activity_tiers.must_compute()
            && let Some(current_year) = gdp_activity_year
        {
            anyhow::ensure!(
                partition.year >= current_year,
                "analytical partitions are not ordered chronologically"
            );
            if partition.year != current_year {
                finish_activity_year(&mut gdp_editor_month_frames, &mut gdp_tier_frames)?;
            }
        }
        if plan.activity_tiers.must_compute() {
            gdp_activity_year = Some(partition.year);
        }
        let base = load_partition(&partition.files)?;
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
            inequality_frames.push(inequality::compute_frame(&base)?);
            gdp_frames.push(gdp_monthly_frame(&base)?);
            gdp_type_frames.push(gdp_type_share_frame(&base)?);
            labor_monthly_frames.push(labor_monthly_frame(&base)?);
        }
        if plan.activity_tiers.must_compute() {
            gdp_editor_month_frames.push(gdp_editor_month_frame(&base)?);
        }
        if let Some(state) = registered_state.as_mut() {
            state.observe_partition(&base, partition.year, year_month_key)?;
        }
        for file in &partition.files {
            storage::discard_path_cache(file);
        }
    }
    if plan.monthly.must_compute() {
        let result = write_monthly_outputs(
            wiki,
            output_dir,
            inequality_frames,
            gdp_frames,
            gdp_type_frames,
            labor_monthly_frames,
        );
        result.context("failed to write partitioned monthly-family outputs")?;
    }
    if plan.activity_tiers.must_compute() {
        finish_activity_year(&mut gdp_editor_month_frames, &mut gdp_tier_frames)?;
        write_activity_outputs(wiki, output_dir, gdp_tier_frames)?;
    }
    if let Some(state) = registered_state {
        write_lifecycle_outputs(wiki, output_dir, state)?;
    }

    Ok(partition_count)
}

fn write_monthly_outputs(
    wiki: &str,
    output_dir: &Path,
    inequality_frames: Vec<DataFrame>,
    gdp_frames: Vec<DataFrame>,
    gdp_type_frames: Vec<DataFrame>,
    labor_monthly_frames: Vec<DataFrame>,
) -> Result<()> {
    let mut inequality_out = concat_frames(inequality_frames)?;
    inequality_out =
        inequality_out.sort(["year_month", "user_type"], SortMultipleOptions::default())?;
    add_wiki_column(&mut inequality_out, wiki)?;
    write_output(&mut inequality_out, wiki, "inequality", output_dir)?;

    let mut gdp_out = concat_frames(gdp_frames)?;
    gdp_out = sort_frame(gdp_out, ["year_month", "page_namespace", "user_type"])?;
    add_wiki_column(&mut gdp_out, wiki)?;
    write_output(&mut gdp_out, wiki, "gdp", output_dir)?;

    let mut gdp_type_out = concat_frames(gdp_type_frames)?;
    gdp_type_out =
        gdp_type_out.sort(["year_month", "user_type"], SortMultipleOptions::default())?;
    add_wiki_column(&mut gdp_type_out, wiki)?;
    write_output(&mut gdp_type_out, wiki, "gdp_user_type_share", output_dir)?;

    let mut labor_monthly_out = concat_frames(labor_monthly_frames)?;
    labor_monthly_out = sort_frame(
        labor_monthly_out,
        ["year_month", "page_namespace", "user_type"],
    )?;
    add_wiki_column(&mut labor_monthly_out, wiki)?;
    write_output(&mut labor_monthly_out, wiki, "labor_monthly", output_dir)
}

fn write_activity_outputs(wiki: &str, output_dir: &Path, frames: Vec<DataFrame>) -> Result<()> {
    let mut output = concat_frames(frames)?;
    output = sort_frame(output, ["period", "user_type", "tier_rank"])?;
    add_wiki_column(&mut output, wiki)?;
    write_output(&mut output, wiki, "gdp_activity_tiers", output_dir)
}

fn write_lifecycle_outputs(wiki: &str, output_dir: &Path, state: RegisteredState) -> Result<()> {
    finalize_funnel(state.funnel_stats, wiki, output_dir)?;
    finalize_labor_cohorts(state.cohort_spans, wiki, output_dir)?;
    let churn_frames = vec![
        state.churn_month.finish()?,
        state.churn_quarter.finish()?,
        state.churn_year.finish()?,
    ];
    let mut churn = concat_frames(churn_frames)?;
    add_wiki_column(&mut churn, wiki)?;
    write_output(&mut churn, wiki, "labor_churn", output_dir)
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
            output_dir,
            vec![inequality::compute_frame(base)?],
            vec![gdp_monthly_frame(base)?],
            vec![gdp_type_share_frame(base)?],
            vec![labor_monthly_frame(base)?],
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
    family
        .metrics()
        .iter()
        .map(|metric| {
            fingerprint::TrackedPath::new(
                format!("output/{wiki}/{metric}.parquet"),
                output_dir.join(wiki).join(format!("{metric}.parquet")),
            )
        })
        .collect()
}

fn compute_stage_outputs(wiki: &str, output_dir: &Path) -> Vec<fingerprint::TrackedPath> {
    MetricFamily::ALL
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
    for family in MetricFamily::ALL {
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
                let receipt = crate::artifact_receipt::sidecar_path(&output.path).ok();
                std::iter::once(output.path).chain(receipt)
            })
            .collect::<Vec<_>>();
        files.push(family_stage_receipt(candidate_dir, wiki, family));
        reusable.push((family, files));
    }
    Ok(reusable)
}

fn compute_plan(
    wiki: &str,
    snapshot: Option<&str>,
    data_dir: &Path,
    output_dir: &Path,
    weekly_config: &WeeklyAggregationConfig,
) -> Result<ComputePlan> {
    let mut plan = ComputePlan::all_recompute();
    for family in MetricFamily::ALL {
        let invalidation =
            if family_is_reusable(family, wiki, snapshot, data_dir, output_dir, weekly_config)? {
                Invalidation::Reuse
            } else {
                Invalidation::Recompute
            };
        match family {
            MetricFamily::Monthly => plan.monthly = invalidation,
            MetricFamily::ActivityTiers => plan.activity_tiers = invalidation,
            MetricFamily::Lifecycle => plan.lifecycle = invalidation,
            MetricFamily::PageWeek => plan.page_week = invalidation,
        }
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
    let missing_names = MetricFamily::ALL
        .into_iter()
        .filter(|family| !reusable_names.contains(&family.name()))
        .map(MetricFamily::name)
        .collect::<Vec<_>>();
    anyhow::ensure!(
        reusable.len() == MetricFamily::ALL.len(),
        "candidate does not have a complete reusable compute-family receipt set; missing {}",
        missing_names.join(", ")
    );
    MetricFamily::ALL
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
    MetricFamily::ALL
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
    let weekly_config = WeeklyAggregationConfig::for_snapshot(data_dir, wiki, Some(snapshot))?;
    for family in MetricFamily::ALL {
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
            families = MetricFamily::ALL.len(),
            "reusing deterministic compute stage"
        );
        return Ok(());
    }

    info!(wiki = wiki, ?plan, "computing invalidated metric families");
    let started = Instant::now();

    let analytical_partitions_scanned =
        compute_all_incremental(wiki, data_dir, output_dir, snapshot, plan)?;
    if plan.page_week.must_compute() {
        compute_page_weekly_edits_for_snapshot(
            wiki,
            data_dir,
            output_dir,
            &weekly_config,
            snapshot,
        )?;
    }
    for family in MetricFamily::ALL {
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
        elapsed_ms = started.elapsed().as_secs_f64() * 1_000.0,
        "finished metric computation"
    );
    Ok(())
}

pub(crate) fn benchmark_page_weekly_edits(
    wiki: &str,
    data_dir: &Path,
    output_dir: &Path,
    config: &WeeklyAggregationConfig,
) -> Result<WeeklyAggregationReport> {
    compute_page_weekly_edits(wiki, data_dir, output_dir, config)?.with_context(|| {
        format!("cannot benchmark page_weekly_edits: no warehouse partitions for {wiki}")
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resource_governor::ResourceBudget;
    use crate::test_support::{TestDir, init_test_tracing};

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
    fn activity_tiers_reclassify_editors_and_conserve_edits_for_each_period() -> Result<()> {
        let months = [202401, 202402, 202403];
        let edits = [100, 100, 100, 99, 99, 99];
        let month_keys = [
            months[0], months[1], months[2], months[0], months[1], months[2],
        ];
        let user_ids = [1, 1, 1, 2, 2, 2];
        let frame = editor_months(&edits, &month_keys, &user_ids)?;
        let quarterly = gdp_activity_tiers_for_period(&frame, ActivityPeriod::Quarter)?;
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

        let family_receipts_before = MetricFamily::ALL
            .into_iter()
            .map(|family| fs::read(family_stage_receipt(output_dir.path(), wiki, family)))
            .collect::<std::io::Result<Vec<_>>>()?;
        let patrol_dir = data_dir.path().join("patrol").join(wiki);
        fs::create_dir_all(&patrol_dir)?;
        fs::write(patrol_dir.join("parser-input.changed"), "patrol-only")?;
        compute_all(wiki, data_dir.path(), output_dir.path())?;
        let family_receipts_after = MetricFamily::ALL
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

    fn only_family(family: MetricFamily) -> ComputePlan {
        let mut plan = ComputePlan {
            monthly: Invalidation::Reuse,
            activity_tiers: Invalidation::Reuse,
            lifecycle: Invalidation::Reuse,
            page_week: Invalidation::Reuse,
        };
        match family {
            MetricFamily::Monthly => plan.monthly = Invalidation::Recompute,
            MetricFamily::ActivityTiers => plan.activity_tiers = Invalidation::Recompute,
            MetricFamily::Lifecycle => plan.lifecycle = Invalidation::Recompute,
            MetricFamily::PageWeek => plan.page_week = Invalidation::Recompute,
        }
        plan
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
                malformed_monthly_output.path(),
                vec![inequality::compute_frame(&base).expect("inequality fixture should compute")],
                vec![gdp_monthly_frame(&base).expect("GDP fixture should compute")],
                vec![gdp_type_share_frame(&base).expect("GDP share fixture should compute")],
                vec![malformed_labor],
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
        let receipts_before = MetricFamily::ALL
            .into_iter()
            .map(|family| fs::read(family_stage_receipt(output_dir.path(), wiki, family)))
            .collect::<std::io::Result<Vec<_>>>()?;
        for family in MetricFamily::ALL {
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
            MetricFamily::ALL
                .into_iter()
                .map(|family| fs::read(family_stage_receipt(output_dir.path(), wiki, family)))
                .collect::<std::io::Result<Vec<_>>>()?,
            receipts_before
        );

        for family in MetricFamily::ALL {
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
        assert_eq!(loaded.width(), schema::ANALYTICAL_COLUMNS.len());
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
        assert_eq!(loaded.width(), 10);

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
