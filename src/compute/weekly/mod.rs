//! Bounded page-week reduction, disk routing, reconciliation, and reporting.

/// Semantic version for page-week reduction, reconciliation, and lag values.
/// The selected deterministic bucket topology is appended to this value.
pub(crate) const ALGORITHM_VERSION: &str = "page-week-v2-governed-parallel-buckets";
pub(crate) const CONTRIBUTION_ALGORITHM_VERSION: &str =
    "page-week-month-contribution-v1-monday-boundary";

use super::{PendingOutput, cached_or_compute, sort_frame, warehouse_lazyframe};
use crate::{
    determinism,
    observability::MemorySnapshot,
    resource_governor::{
        BucketPermit, GovernorPaths, ResourceBudget, ResourceGovernor, ResourceObservation,
    },
    storage, workload_profile,
};
use anyhow::{Context, Result};
use chrono::{Datelike, Duration, NaiveDate};
use polars::io::parquet::write::BatchedWriter;
use polars::prelude::*;
use serde::Serialize;
use std::collections::BTreeMap;
use std::env;
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tracing::info;

pub(super) const LEGACY_COMPUTE_ALGORITHM_VERSION: &str =
    "core-metrics-v8-period-aware-activity-tiers";
pub(super) const DEFAULT_WEEKLY_BUCKET_COUNT: usize = 256;
pub(super) const DEFAULT_SECONDARY_BUCKET_COUNT: usize = 1;
pub(super) const WEEKLY_ROUTING_BATCH_ROWS: usize = 250_000;
pub(super) const WEEKLY_BUCKET_MIN_MEMORY_BYTES: u64 = 64 * 1024 * 1024;
pub(super) const WEEKLY_BUCKET_ESTIMATED_BYTES_PER_ROW: u64 = 256;
pub(super) const WEEKLY_RESULT_ESTIMATED_BYTES_PER_ROW: u64 = 128;
const WEEKLY_OUTPUT_CACHE_EVICT_BYTES: u64 = 64 * 1024 * 1024;
pub(super) const SUPPORTED_PRIMARY_BUCKET_COUNTS: [usize; 6] = [32, 64, 128, 256, 512, 1024];
#[cfg(test)]
pub(super) const FLAT_BENCHMARK_BUCKET_COUNTS: [usize; 3] = [256, 512, 1024];
pub(super) const SUPPORTED_SECONDARY_BUCKET_COUNTS: [usize; 4] = [1, 8, 16, 32];
pub(super) const WEEKLY_BUCKET_COUNT_ENV: &str = "WIKI_ECON_WEEKLY_BUCKET_COUNT";
pub(super) const WEEKLY_PRIMARY_BUCKET_COUNT_ENV: &str = "WIKI_ECON_WEEKLY_PRIMARY_BUCKET_COUNT";
pub(super) const WEEKLY_SECONDARY_BUCKET_COUNT_ENV: &str =
    "WIKI_ECON_WEEKLY_SECONDARY_BUCKET_COUNT";
pub(super) const SCRATCH_DIR_ENV: &str = "WIKI_ECON_SCRATCH_DIR";

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct WeeklyAggregationConfig {
    pub(super) primary_bucket_count: usize,
    pub(super) secondary_bucket_count: usize,
    pub(super) scratch_root: Option<PathBuf>,
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

    pub(super) fn for_snapshot(
        data_dir: &Path,
        wiki: &str,
        snapshot: Option<&str>,
    ) -> Result<Self> {
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

    pub(super) fn from_workload_profile(
        profile: &workload_profile::WorkloadProfile,
    ) -> Result<Self> {
        profile.ensure_compute_qualified()?;
        let mut config = Self::new_two_level(
            profile.parameters.primary_buckets,
            profile.parameters.secondary_buckets,
            None,
        )
        .expect("a validated workload profile always has a supported bucket topology");
        config.workload_algorithm_version = Some(profile.algorithm_version()?);
        Ok(config)
    }

    pub(super) fn from_environment() -> Result<Self> {
        Self::from_values(
            env::var_os(WEEKLY_BUCKET_COUNT_ENV),
            env::var_os(WEEKLY_PRIMARY_BUCKET_COUNT_ENV),
            env::var_os(WEEKLY_SECONDARY_BUCKET_COUNT_ENV),
            env::var_os(SCRATCH_DIR_ENV),
        )
    }

    pub(super) fn from_values(
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
        format!("{}-{selection}-{partition}", ALGORITHM_VERSION)
    }

    pub(super) fn legacy_algorithm_version(&self) -> String {
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

    pub(super) fn logical_bucket_count(&self) -> usize {
        self.primary_bucket_count * self.secondary_bucket_count
    }
}

pub(super) fn parse_bucket_env(
    value: Option<std::ffi::OsString>,
    name: &str,
) -> Result<Option<usize>> {
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
    pub(super) fn observe(&mut self, snapshot: MemorySnapshot, scratch_bytes: Option<u64>) {
        self.rss_bytes = max_option(self.rss_bytes, snapshot.rss_bytes);
        self.cgroup_current_bytes =
            max_option(self.cgroup_current_bytes, snapshot.cgroup_current_bytes);
        self.scratch_bytes = max_option(self.scratch_bytes, scratch_bytes);
    }

    pub(super) fn merge(&mut self, other: Self) {
        self.rss_bytes = max_option(self.rss_bytes, other.rss_bytes);
        self.cgroup_current_bytes =
            max_option(self.cgroup_current_bytes, other.cgroup_current_bytes);
        self.scratch_bytes = max_option(self.scratch_bytes, other.scratch_bytes);
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
    pub resources: ResourceObservation,
}

pub(super) fn max_option(left: Option<u64>, right: Option<u64>) -> Option<u64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (left, right) => left.or(right),
    }
}

pub(super) fn compute_page_weekly_edits(
    wiki: &str,
    data_dir: &Path,
    output_dir: &Path,
    config: &WeeklyAggregationConfig,
) -> Result<Option<WeeklyAggregationReport>> {
    compute_page_weekly_edits_for_snapshot(wiki, data_dir, output_dir, config, None)
}

pub(super) fn compute_page_weekly_edits_for_snapshot(
    wiki: &str,
    data_dir: &Path,
    output_dir: &Path,
    config: &WeeklyAggregationConfig,
    snapshot: Option<&str>,
) -> Result<Option<WeeklyAggregationReport>> {
    compute_page_weekly_edits_for_snapshot_cached(
        wiki, data_dir, output_dir, config, snapshot, None,
    )
}

pub(super) fn compute_page_weekly_edits_for_snapshot_cached(
    wiki: &str,
    data_dir: &Path,
    output_dir: &Path,
    config: &WeeklyAggregationConfig,
    snapshot: Option<&str>,
    cross_snapshot: Option<&crate::cross_snapshot::CrossSnapshotCache>,
) -> Result<Option<WeeklyAggregationReport>> {
    let aggregation_started = Instant::now();
    let governed_scratch_root = config
        .scratch_root
        .clone()
        .or_else(|| Some(output_dir.join(wiki)));
    let governor_paths = GovernorPaths::new(data_dir.to_path_buf(), governed_scratch_root);
    let governor = ResourceGovernor::from_environment(governor_paths)?;
    compute_page_weekly_edits_for_snapshot_with_governor(
        wiki,
        data_dir,
        output_dir,
        config,
        snapshot,
        cross_snapshot,
        &governor,
        aggregation_started,
    )
}

#[allow(clippy::too_many_arguments)]
fn compute_page_weekly_edits_for_snapshot_with_governor(
    wiki: &str,
    data_dir: &Path,
    output_dir: &Path,
    config: &WeeklyAggregationConfig,
    snapshot: Option<&str>,
    cross_snapshot: Option<&crate::cross_snapshot::CrossSnapshotCache>,
    governor: &ResourceGovernor,
    aggregation_started: Instant,
) -> Result<Option<WeeklyAggregationReport>> {
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
        let input_digest = cross_snapshot
            .map(|cache| cache.month_digest(&partition.year_month))
            .transpose()?;
        let partition_weekly = cached_or_compute(
            cross_snapshot,
            "page_week_contribution",
            CONTRIBUTION_ALGORITHM_VERSION,
            input_digest,
            "weekly_contribution",
            || {
                let reduced = warehouse_lazyframe(&partition.files)?
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
                sort_frame(reduced, weekly_sort_keys())
            },
        )?;
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
            governor,
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
    if config.secondary_bucket_count == 1 {
        for primary_start in
            (0..config.primary_bucket_count).step_by(governor.budget().weekly_worker_limit)
        {
            let primary_end = (primary_start + governor.budget().weekly_worker_limit)
                .min(config.primary_bucket_count);
            let prepared = (primary_start..primary_end)
                .filter(|&primary_bucket| primary_bucket_rows[primary_bucket] > 0)
                .map(|primary_bucket| PreparedWeeklyBucket {
                    logical_bucket: primary_bucket,
                    primary_bucket,
                    secondary_bucket: 0,
                    staged_rows: primary_bucket_rows[primary_bucket],
                    staged_path: None,
                })
                .collect::<Vec<_>>();
            let results =
                reconcile_weekly_bucket_batch(&runs, &staged_paths, prepared, wiki, governor)?;
            let append = append_weekly_bucket_results(
                &runs,
                results,
                &final_path,
                &mut output,
                &mut total_edits_after,
                &mut output_rows,
                &mut min_week_start,
                &mut max_week_start,
                &mut scratch_peak_bytes,
                &mut working_storage_peak_bytes,
                &mut reconciliation_peak,
            );
            append?;
        }
    } else {
        route_and_reconcile_weekly_buckets(
            &runs,
            &primary_paths,
            &primary_bucket_rows,
            &primary_bucket_edits,
            config,
            wiki,
            governor,
            &mut bucket_rows,
            &final_path,
            &mut output,
            &mut total_edits_after,
            &mut output_rows,
            &mut min_week_start,
            &mut max_week_start,
            &mut scratch_peak_bytes,
            &mut working_storage_peak_bytes,
            &mut reconciliation_peak,
        )?;
    }

    anyhow::ensure!(
        total_edits_before == total_edits_after,
        "page_weekly_edits lost or duplicated data for {wiki}: {total_edits_before} edits before merge, {total_edits_after} after"
    );
    let output = output.context("page_weekly_edits produced no rows from non-empty partitions")?;
    info!(
        path = %final_path.display(),
        "page_weekly_edits: finalizing output"
    );
    let bytes = output.finish()?;
    working_storage_peak_bytes = working_storage_peak_bytes.max(bytes);
    let reconciliation_elapsed_ms = reconciliation_started.elapsed().as_millis() as u64;
    let memory = MemorySnapshot::capture();
    governor.checkpoint("page_weekly_edits_complete")?;
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
        resources: governor.observation(),
    }))
}

pub(super) fn weekly_group_keys() -> [Expr; 4] {
    [
        col("page_id"),
        col("page_namespace"),
        col("page_title"),
        col("week_start"),
    ]
}

#[cfg(not(test))]
pub(super) const WEEKLY_EXTERNAL_BATCH_ROWS: usize = 100_000;
#[cfg(test)]
pub(super) const WEEKLY_EXTERNAL_BATCH_ROWS: usize = 2;
pub(super) const WEEKLY_EXTERNAL_READ_ROWS: usize = 4_096;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) struct WeeklyContributionRow {
    page_id: Option<i64>,
    page_namespace: Option<i32>,
    // Rows are cloned for cursor-order checks and output batching; share the title bytes.
    page_title: Option<Arc<str>>,
    week_start: i32,
    edits: u32,
}

impl WeeklyContributionRow {
    pub(super) fn same_key(&self, other: &Self) -> bool {
        self.page_id == other.page_id
            && self.page_namespace == other.page_namespace
            && self.page_title == other.page_title
            && self.week_start == other.week_start
    }
}

pub(super) struct WeeklyContributionCursor {
    reader: storage::SequentialParquetReader,
    batch: Option<DataFrame>,
    row: usize,
    previous: Option<WeeklyContributionRow>,
}

impl WeeklyContributionCursor {
    pub(super) fn new(path: &Path) -> Result<Self> {
        let projection = [
            "page_id",
            "page_namespace",
            "page_title",
            "week_start",
            "edits",
        ]
        .into_iter()
        .map(str::to_string)
        .collect();
        let reader_result = storage::SequentialParquetReader::new(
            path,
            Some(projection),
            WEEKLY_EXTERNAL_READ_ROWS,
        );
        Ok(Self {
            reader: reader_result?,
            batch: None,
            row: 0,
            previous: None,
        })
    }

    pub(super) fn next_row(&mut self) -> Result<Option<WeeklyContributionRow>> {
        loop {
            if let Some(batch) = &self.batch
                && self.row < batch.height()
            {
                let row = WeeklyContributionRow {
                    page_id: batch.column("page_id")?.i64()?.get(self.row),
                    page_namespace: batch.column("page_namespace")?.i32()?.get(self.row),
                    page_title: batch
                        .column("page_title")?
                        .str()?
                        .get(self.row)
                        .map(Arc::<str>::from),
                    week_start: batch
                        .column("week_start")?
                        .date()?
                        .physical()
                        .get(self.row)
                        .context("weekly contribution has no week")?,
                    edits: batch
                        .column("edits")?
                        .u32()?
                        .get(self.row)
                        .context("weekly contribution has no edit count")?,
                };
                self.row += 1;
                if let Some(previous) = &self.previous {
                    anyhow::ensure!(
                        previous <= &row,
                        "weekly contribution run violates its logical order"
                    );
                }
                self.previous = Some(row.clone());
                return Ok(Some(row));
            }
            self.batch = self.reader.next_batch()?;
            self.row = 0;
            if self.batch.is_none() {
                return Ok(None);
            }
        }
    }
}

#[derive(Default)]
pub(super) struct WeeklyFinalBatch {
    week_start: Vec<String>,
    iso_year: Vec<i32>,
    iso_week: Vec<i32>,
    page_id: Vec<Option<i64>>,
    page_title: Vec<Option<Arc<str>>>,
    page_namespace: Vec<Option<i32>>,
    edits: Vec<u32>,
    previous_week_edits: Vec<u32>,
    wow_change: Vec<i64>,
    wow_rate: Vec<Option<f64>>,
    previous: Option<WeeklyContributionRow>,
}

impl WeeklyFinalBatch {
    pub(super) fn len(&self) -> usize {
        self.edits.len()
    }

    pub(super) fn push(&mut self, row: WeeklyContributionRow) -> Result<()> {
        let date = format_epoch_day(row.week_start).context("weekly contribution date overflow")?;
        let parsed = NaiveDate::parse_from_str(&date, "%Y-%m-%d")?;
        let previous = self.previous.as_ref().map_or(0, |previous| {
            if previous.page_id == row.page_id
                && previous.page_namespace == row.page_namespace
                && previous.page_title == row.page_title
                && row.week_start.checked_sub(previous.week_start) == Some(7)
            {
                previous.edits
            } else {
                0
            }
        });
        self.week_start.push(date);
        self.iso_year.push(parsed.iso_week().year());
        self.iso_week.push(i32::try_from(parsed.iso_week().week())?);
        self.page_id.push(row.page_id);
        self.page_title.push(row.page_title.clone());
        self.page_namespace.push(row.page_namespace);
        self.edits.push(row.edits);
        self.previous_week_edits.push(previous);
        let change = i64::from(row.edits) - i64::from(previous);
        self.wow_change.push(change);
        self.wow_rate
            .push((previous != 0).then_some(change as f64 / f64::from(previous)));
        self.previous = Some(row);
        Ok(())
    }

    pub(super) fn take_frame(&mut self, wiki: &str) -> Result<DataFrame> {
        let rows = self.len();
        let previous = self.previous.clone();
        let taken = std::mem::take(self);
        self.previous = previous;
        let mut page_titles = StringChunkedBuilder::new("page_title".into(), rows);
        for title in taken.page_title {
            page_titles.append_option(title.as_deref());
        }
        DataFrame::new_infer_height(vec![
            Column::new("week_start".into(), taken.week_start),
            Column::new("iso_year".into(), taken.iso_year),
            Column::new("iso_week".into(), taken.iso_week),
            Column::new("page_id".into(), taken.page_id),
            page_titles.finish().into_column(),
            Column::new("page_namespace".into(), taken.page_namespace),
            Column::new("edits".into(), taken.edits),
            Column::new("previous_week_edits".into(), taken.previous_week_edits),
            Column::new("wow_change".into(), taken.wow_change),
            Column::new("wow_rate".into(), taken.wow_rate),
            Column::new("wiki".into(), vec![wiki; rows]),
        ])
        .map_err(Into::into)
    }
}

pub(super) fn write_weekly_contribution_run(path: &Path, frame: &mut DataFrame) -> Result<()> {
    let mut file = File::create(path)?;
    ParquetWriter::new(&mut file)
        .with_compression(ParquetCompression::Zstd(None))
        .with_row_group_size(Some(WEEKLY_EXTERNAL_READ_ROWS))
        .set_parallel(false)
        .finish(frame)?;
    file.sync_all()?;
    Ok(())
}

pub(super) fn compute_page_weekly_external_qualification(
    wiki: &str,
    data_dir: &Path,
    output_dir: &Path,
    config: &WeeklyAggregationConfig,
    snapshot: Option<&str>,
    cross_snapshot: Option<&crate::cross_snapshot::CrossSnapshotCache>,
) -> Result<Option<WeeklyAggregationReport>> {
    let started = Instant::now();
    let snapshot = snapshot.context("external weekly qualification requires a snapshot")?;
    let layer_result = storage::snapshot_compute_layer(
        data_dir,
        wiki,
        snapshot,
        storage::GenerationLayer::Warehouse,
    );
    let layer = layer_result?;
    let partitions = storage::snapshot_partition_specs(data_dir, wiki, snapshot, layer)?;
    anyhow::ensure!(
        !partitions.is_empty(),
        "external weekly qualification requires at least one input partition"
    );
    let runs = WeeklyRunDir::new(output_dir, wiki, config.scratch_root.as_deref())?;
    let event_date_options = StrptimeOptions {
        format: Some("%Y-%m-%d".into()),
        strict: true,
        exact: true,
        cache: true,
    };
    let reduction_started = Instant::now();
    let mut contribution_paths = Vec::with_capacity(partitions.len());
    let mut staged_rows = 0usize;
    let mut total_edits_before = 0i64;
    for (index, partition) in partitions.iter().enumerate() {
        let partition_started = Instant::now();
        let input_digest = cross_snapshot
            .map(|cache| cache.month_digest(&partition.year_month))
            .transpose()?;
        let contribution_result = cached_or_compute(
            cross_snapshot,
            "page_week_contribution",
            CONTRIBUTION_ALGORITHM_VERSION,
            input_digest,
            "weekly_contribution",
            || {
                let reduced = warehouse_lazyframe(&partition.files)?
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
                    .agg([len().alias("edits")])
                    .collect()?;
                sort_frame(reduced, weekly_sort_keys())
            },
        );
        let mut contribution = contribution_result?;
        let edits = sum_edits_column(std::slice::from_ref(&contribution))?;
        let source_rows = parquet_paths_row_count(&partition.files)?;
        anyhow::ensure!(
            u64::try_from(edits)? == source_rows,
            "external weekly month {} lost edits",
            partition.year_month
        );
        total_edits_before = total_edits_before
            .checked_add(edits)
            .context("external weekly input edit overflow")?;
        staged_rows = staged_rows
            .checked_add(contribution.height())
            .context("external weekly contribution row overflow")?;
        let path = runs.partition_path(index);
        write_weekly_contribution_run(&path, &mut contribution)?;
        // These runs are consumed only after all source months have been reduced.
        // Drop their clean page-cache pages now so 309 writes do not accumulate
        // in the job's cgroup before reconciliation begins.
        storage::discard_path_cache(&path);
        contribution_paths.push(path);
        drop(contribution);
        for file in &partition.files {
            storage::discard_path_cache(file);
        }
        let memory = MemorySnapshot::capture();
        info!(
            wiki,
            partition = index + 1,
            total_partitions = partitions.len(),
            year_month = partition.year_month.as_str(),
            staged_rows,
            elapsed_ms = partition_started.elapsed().as_secs_f64() * 1_000.0,
            rss_bytes = ?memory.rss_bytes,
            cgroup_current_bytes = ?memory.cgroup_current_bytes,
            cgroup_peak_bytes = ?memory.cgroup_peak_bytes,
            cgroup_limit_bytes = ?memory.cgroup_limit_bytes,
            "page_weekly_edits: reduced and staged external contribution"
        );
    }
    let reduction_elapsed_ms = reduction_started.elapsed().as_millis() as u64;
    let scratch_peak_bytes = runs.size_bytes()?;
    let mut cursors = contribution_paths
        .iter()
        .map(|path| WeeklyContributionCursor::new(path))
        .collect::<Result<Vec<_>>>()?;
    let mut heap = std::collections::BinaryHeap::new();
    for (run, cursor) in cursors.iter_mut().enumerate() {
        if let Some(row) = cursor.next_row()? {
            heap.push(std::cmp::Reverse((row, run)));
        }
    }
    let reconciliation_started = Instant::now();
    let final_path = output_dir.join(wiki).join("page_weekly_edits.parquet");
    let mut writer: Option<AtomicBatchedParquetWriter> = None;
    let mut batch = WeeklyFinalBatch::default();
    let mut current: Option<WeeklyContributionRow> = None;
    let mut output_rows = 0usize;
    let mut total_edits_after = 0i64;
    let mut minimum_week = None;
    let mut maximum_week = None;

    let flush = |batch: &mut WeeklyFinalBatch,
                 writer: &mut Option<AtomicBatchedParquetWriter>|
     -> Result<()> {
        let mut frame = batch.take_frame(wiki)?;
        if writer.is_none() {
            let writer_result = AtomicBatchedParquetWriter::new(final_path.clone(), frame.schema());
            *writer = Some(writer_result?);
        }
        writer
            .as_mut()
            .context("external weekly writer was not initialized")?
            .write_batch(&mut frame)
    };

    while let Some(std::cmp::Reverse((row, run))) = heap.pop() {
        if let Some(next) = cursors[run].next_row()? {
            heap.push(std::cmp::Reverse((next, run)));
        }
        if let Some(active) = current.as_mut()
            && active.same_key(&row)
        {
            active.edits = active
                .edits
                .checked_add(row.edits)
                .context("external weekly boundary edit overflow")?;
            continue;
        }
        if let Some(completed) = current.replace(row) {
            minimum_week = minimum_week.into_iter().chain([completed.week_start]).min();
            maximum_week = maximum_week.into_iter().chain([completed.week_start]).max();
            total_edits_after = total_edits_after
                .checked_add(i64::from(completed.edits))
                .context("external weekly output edit overflow")?;
            output_rows += 1;
            batch.push(completed)?;
            if batch.len() >= WEEKLY_EXTERNAL_BATCH_ROWS {
                flush(&mut batch, &mut writer)?;
            }
        }
    }
    let completed = current.context("external weekly merge produced no rows")?;
    minimum_week = minimum_week.into_iter().chain([completed.week_start]).min();
    maximum_week = maximum_week.into_iter().chain([completed.week_start]).max();
    total_edits_after = total_edits_after
        .checked_add(i64::from(completed.edits))
        .context("external weekly output edit overflow")?;
    output_rows += 1;
    let push_result = batch.push(completed);
    push_result?;
    flush(&mut batch, &mut writer)?;
    anyhow::ensure!(
        total_edits_before == total_edits_after,
        "external weekly merge lost or duplicated edits"
    );
    let writer = writer.context("external weekly merge produced no output")?;
    let output_bytes = writer.finish()?;
    let reconciliation_elapsed_ms = reconciliation_started.elapsed().as_millis() as u64;
    let memory = MemorySnapshot::capture();
    let minimum_week_start = minimum_week.and_then(format_epoch_day);
    let maximum_week_start = maximum_week.and_then(format_epoch_day);
    Ok(Some(WeeklyAggregationReport {
        wiki: wiki.to_string(),
        bucket_count: config.logical_bucket_count(),
        primary_bucket_count: config.primary_bucket_count,
        secondary_bucket_count: config.secondary_bucket_count,
        partitions: partitions.len(),
        staged_rows,
        output_rows,
        total_edits: total_edits_after,
        minimum_week_start,
        maximum_week_start,
        bucket_staged_rows: Vec::new(),
        primary_bucket_staged_rows: Vec::new(),
        largest_bucket_staged_rows: 0,
        output_bytes,
        scratch_peak_bytes,
        working_storage_peak_bytes: scratch_peak_bytes.saturating_add(output_bytes),
        reduction_peak: ResourcePeak::default(),
        reconciliation_peak: ResourcePeak::default(),
        final_memory: memory,
        reduction_elapsed_ms,
        reconciliation_elapsed_ms,
        elapsed_ms: started.elapsed().as_millis() as u64,
        resources: ResourceObservation::default(),
    }))
}

pub(super) fn weekly_sort_keys() -> [&'static str; 4] {
    ["page_id", "page_namespace", "page_title", "week_start"]
}

pub(super) fn stable_weekly_bucket(page_id: Option<i64>, bucket_count: usize) -> usize {
    determinism::stable_page_hash(page_id) as usize & (bucket_count - 1)
}

pub(super) fn stable_weekly_secondary_bucket(
    page_id: Option<i64>,
    primary_bucket_count: usize,
    secondary_bucket_count: usize,
) -> usize {
    (determinism::stable_page_hash(page_id) >> primary_bucket_count.trailing_zeros()) as usize
        & (secondary_bucket_count - 1)
}

fn sync_and_discard_path_cache(path: &Path) -> Result<()> {
    let file = File::options().read(true).write(true).open(path)?;
    file.sync_all()?;
    storage::discard_file_cache(&file, 0, file.metadata()?.len());
    Ok(())
}

pub(super) fn format_epoch_day(day: i32) -> Option<String> {
    NaiveDate::from_ymd_opt(1970, 1, 1)?
        .checked_add_signed(Duration::days(i64::from(day)))
        .map(|date| date.format("%Y-%m-%d").to_string())
}

pub(super) fn stage_weekly_partition(
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
    // The monthly partitions are all staged before compaction starts. Flush
    // each completed file and advise Linux to evict it now so the scratch
    // output cache does not grow with all 309 months in the job's cgroup.
    sync_and_discard_path_cache(&path)?;
    Ok(path)
}

pub(super) fn read_staged_weekly_bucket(paths: &[PathBuf], bucket: usize) -> Result<DataFrame> {
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

pub(super) fn read_staged_primary_range(
    path: &Path,
    start: usize,
    end: usize,
) -> Result<DataFrame> {
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

pub(super) struct PrimaryBucketTotals<'a> {
    rows: &'a [usize],
    edits: &'a [i64],
}

#[derive(Clone, Copy)]
pub(super) struct BucketTotals {
    pub(super) rows: usize,
    pub(super) edits: i64,
}

pub(super) fn compact_weekly_primary_buckets(
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
        for (bucket, writer) in writers {
            writer.finish()?;
            let path = paths[bucket]
                .as_deref()
                .context("finished primary bucket has no output path")?;
            sync_and_discard_path_cache(path)?;
        }
        // The staged partitions are read completely during compaction and
        // removed below. Drop their reclaimable pages before the next
        // resource checkpoint rather than charging them to the next stage.
        for path in staged_paths {
            storage::discard_path_cache(path);
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
pub(super) struct SecondaryRouting {
    pub(super) paths: Vec<Option<PathBuf>>,
    rows: Vec<usize>,
    pub(super) peak_active_writers: usize,
}

struct WeeklyPrimaryRouteJob {
    primary_bucket: usize,
    primary_path: PathBuf,
    expected: BucketTotals,
    permit: BucketPermit,
}

fn weekly_route_memory_estimate(writer_limit: usize) -> Result<u64> {
    let batch_bytes = u64::try_from(WEEKLY_ROUTING_BATCH_ROWS)?
        .checked_mul(WEEKLY_BUCKET_ESTIMATED_BYTES_PER_ROW)
        .context("weekly route batch memory estimate overflow")?;
    let writer_bytes = u64::try_from(writer_limit)?
        .checked_mul(1024 * 1024)
        .context("weekly route writer memory estimate overflow")?;
    batch_bytes
        .checked_add(writer_bytes)
        .context("weekly route memory estimate overflow")
}

fn weekly_route_worker_limit(governor: &ResourceGovernor, writer_limit: usize) -> Result<usize> {
    let sample = governor.sample()?;
    weekly_route_worker_limit_from_sample(
        governor.budget(),
        writer_limit,
        sample.open_file_descriptors,
    )
}

fn weekly_route_worker_limit_from_sample(
    budget: &ResourceBudget,
    writer_limit: usize,
    open_file_descriptors: Option<usize>,
) -> Result<usize> {
    // One routing worker can hold a batch reader, every writer in its current
    // writer batch, and a few temporary files. Include all of those handles
    // before sizing the concurrent route wave; the governor then reserves its
    // own per-worker descriptor allowance as a second gate.
    let descriptors_per_worker = writer_limit
        .checked_add(6)
        .context("weekly routing descriptor estimate overflow")?;
    let descriptor_workers = match open_file_descriptors {
        Some(open) => budget
            .max_open_files
            .saturating_sub(open)
            .checked_div(descriptors_per_worker)
            .unwrap_or(0),
        None => budget.weekly_worker_limit,
    };
    anyhow::ensure!(
        descriptor_workers > 0,
        "weekly routing cannot admit one worker within the {}-descriptor limit",
        budget.max_open_files
    );
    Ok(budget.weekly_worker_limit.min(descriptor_workers))
}

fn join_weekly_worker_results<'scope, T: Send + 'scope>(
    handles: Vec<std::thread::ScopedJoinHandle<'scope, Result<T>>>,
    panic_message: &'static str,
) -> Result<Vec<T>> {
    let mut results = Vec::with_capacity(handles.len());
    let mut failure = None;
    for handle in handles {
        match handle.join() {
            Ok(Ok(result)) => results.push(result),
            Ok(Err(error)) => {
                if failure.is_none() {
                    failure = Some(error);
                }
            }
            Err(_) => {
                if failure.is_none() {
                    failure = Some(anyhow::anyhow!(panic_message));
                }
            }
        }
    }
    failure.map_or(Ok(results), Err)
}

fn required_secondary_bucket_path(
    rows: usize,
    path: Option<PathBuf>,
    primary_bucket: usize,
    secondary_bucket: usize,
) -> Result<Option<PathBuf>> {
    if rows == 0 {
        return Ok(None);
    }
    path.with_context(|| {
        format!("missing non-empty secondary bucket {primary_bucket}/{secondary_bucket}")
    })
    .map(Some)
}

#[allow(clippy::too_many_arguments)]
fn route_and_reconcile_weekly_buckets(
    runs: &WeeklyRunDir,
    primary_paths: &[Option<PathBuf>],
    primary_bucket_rows: &[usize],
    primary_bucket_edits: &[i64],
    config: &WeeklyAggregationConfig,
    wiki: &str,
    governor: &ResourceGovernor,
    bucket_rows: &mut [usize],
    final_path: &Path,
    output: &mut Option<AtomicBatchedParquetWriter>,
    total_edits_after: &mut i64,
    output_rows: &mut usize,
    min_week_start: &mut Option<i32>,
    max_week_start: &mut Option<i32>,
    scratch_peak_bytes: &mut u64,
    working_storage_peak_bytes: &mut u64,
    reconciliation_peak: &mut ResourcePeak,
) -> Result<()> {
    anyhow::ensure!(
        primary_paths.len() == primary_bucket_rows.len()
            && primary_bucket_rows.len() == primary_bucket_edits.len(),
        "weekly primary bucket inventory changed before routing"
    );
    anyhow::ensure!(
        bucket_rows.len() == config.logical_bucket_count(),
        "weekly logical bucket inventory changed before routing"
    );
    let writer_limit = governor.budget().max_active_parquet_writers;
    anyhow::ensure!(
        writer_limit > 0,
        "weekly routing writer limit must be positive"
    );
    let worker_limit = weekly_route_worker_limit(governor, writer_limit)?;
    let estimated_memory_bytes = weekly_route_memory_estimate(writer_limit)?;
    let candidates = (0..config.primary_bucket_count)
        .filter(|&primary| primary_bucket_rows[primary] > 0)
        .map(|primary| {
            let path = primary_paths[primary]
                .clone()
                .with_context(|| format!("missing non-empty primary bucket {primary}"))?;
            Ok((
                primary,
                path,
                BucketTotals {
                    rows: primary_bucket_rows[primary],
                    edits: primary_bucket_edits[primary],
                },
            ))
        })
        .collect::<Result<Vec<_>>>()?;

    let mut next = 0;
    while next < candidates.len() {
        let mut width = worker_limit.min(candidates.len() - next);
        let (jobs, width) = loop {
            let mut jobs = Vec::with_capacity(width);
            let mut admission_error = None;
            for (primary_bucket, primary_path, expected) in &candidates[next..next + width] {
                let estimated_scratch_bytes = primary_path.metadata()?.len();
                match governor.admit_bucket(estimated_memory_bytes, estimated_scratch_bytes) {
                    Ok(permit) => jobs.push(WeeklyPrimaryRouteJob {
                        primary_bucket: *primary_bucket,
                        primary_path: primary_path.clone(),
                        expected: *expected,
                        permit,
                    }),
                    Err(error) => {
                        admission_error = Some(error);
                        break;
                    }
                }
            }
            if let Some(error) = admission_error {
                drop(jobs);
                if width == 1 {
                    return Err(
                        error.context("resource governor could not admit one weekly route worker")
                    );
                }
                width -= 1;
            } else {
                break (jobs, width);
            }
        };

        let routed = std::thread::scope(|scope| -> Result<Vec<_>> {
            let handles = jobs
                .into_iter()
                .map(|job| {
                    scope.spawn(move || {
                        let WeeklyPrimaryRouteJob {
                            primary_bucket,
                            primary_path,
                            expected,
                            permit,
                        } = job;
                        let _permit = permit;
                        let mut peak = ResourcePeak::default();
                        let routing = route_primary_to_secondary_buckets(
                            runs,
                            &primary_path,
                            primary_bucket,
                            config,
                            expected,
                            governor,
                            &mut peak,
                        )?;
                        anyhow::ensure!(
                            routing.peak_active_writers <= writer_limit,
                            "secondary routing reported a writer peak above its governed limit"
                        );
                        Ok((primary_bucket, primary_path, routing, peak))
                    })
                })
                .collect::<Vec<_>>();
            join_weekly_worker_results(handles, "weekly primary routing worker panicked")
        })?;

        // Account for the transient overlap of immutable primary inputs and
        // routed secondary files before removing the primary files.
        *scratch_peak_bytes = (*scratch_peak_bytes).max(runs.size_bytes()?);
        *working_storage_peak_bytes = (*working_storage_peak_bytes).max(*scratch_peak_bytes);
        let mut routed = routed;
        routed.sort_by_key(|(primary, _, _, _)| *primary);
        let mut prepared = Vec::new();
        for (primary_bucket, primary_path, routing, route_peak) in routed {
            reconciliation_peak.merge(route_peak);
            for (secondary_bucket, (rows, path)) in
                routing.rows.into_iter().zip(routing.paths).enumerate()
            {
                let logical_bucket =
                    primary_bucket * config.secondary_bucket_count + secondary_bucket;
                bucket_rows[logical_bucket] = rows;
                let Some(staged_path) =
                    required_secondary_bucket_path(rows, path, primary_bucket, secondary_bucket)?
                else {
                    continue;
                };
                prepared.push(PreparedWeeklyBucket {
                    logical_bucket,
                    primary_bucket,
                    secondary_bucket,
                    staged_rows: rows,
                    staged_path: Some(staged_path),
                });
            }
            fs::remove_file(primary_path)?;
        }
        prepared.sort_by_key(|bucket| bucket.logical_bucket);
        for batch in prepared.chunks(governor.budget().weekly_worker_limit) {
            let results = reconcile_weekly_bucket_batch(runs, &[], batch.to_vec(), wiki, governor)?;
            append_weekly_bucket_results(
                runs,
                results,
                final_path,
                output,
                total_edits_after,
                output_rows,
                min_week_start,
                max_week_start,
                scratch_peak_bytes,
                working_storage_peak_bytes,
                reconciliation_peak,
            )?;
        }
        next += width;
    }
    Ok(())
}

pub(super) fn parquet_paths_row_count(paths: &[PathBuf]) -> Result<u64> {
    paths.iter().try_fold(0u64, |total, path| {
        let rows = ParquetReader::new(File::open(path)?).num_rows()?;
        total
            .checked_add(u64::try_from(rows)?)
            .context("Parquet row count overflow")
    })
}

pub(super) fn route_primary_to_secondary_buckets(
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
    let writer_limit = governor.budget().max_active_parquet_writers;
    anyhow::ensure!(
        writer_limit > 0,
        "secondary routing requires a positive governed Parquet writer limit"
    );
    let mut paths = vec![None; secondary_bucket_count];
    let mut rows = vec![0usize; secondary_bucket_count];
    let mut edits = vec![0i64; secondary_bucket_count];
    let mut peak_active_writers = 0usize;
    let mut routed_rows = 0usize;
    let mut routed_edits = 0i64;

    // A large qualified topology can intentionally contain more logical
    // secondary buckets than the process can keep open at once. Route those
    // buckets in deterministic writer-sized batches. Each batch rereads the
    // immutable primary file, so memory and file descriptors stay bounded by
    // the governed writer limit while the logical topology is preserved.
    for secondary_start in (0..secondary_bucket_count).step_by(writer_limit) {
        let secondary_end = (secondary_start + writer_limit).min(secondary_bucket_count);
        info!(
            primary_bucket,
            secondary_start,
            secondary_end,
            secondary_bucket_count,
            writer_limit,
            "page_weekly_edits: routing secondary writer batch"
        );
        let mut writers: BTreeMap<usize, BatchedWriter<File>> = BTreeMap::new();
        let mut reader =
            storage::SequentialParquetReader::new(primary_path, None, WEEKLY_ROUTING_BATCH_ROWS)?;
        anyhow::ensure!(
            reader.rows() == expected.rows,
            "primary bucket {primary_bucket} footer row count changed before secondary routing"
        );

        while let Some(batch) = reader.next_batch()? {
            let page_ids = batch.column("page_id")?.i64()?;
            let mut row_indices: Vec<Vec<IdxSize>> = (secondary_start..secondary_end)
                .map(|_| Vec::new())
                .collect();
            for row in 0..batch.height() {
                let secondary = stable_weekly_secondary_bucket(
                    page_ids.get(row),
                    primary_bucket_count,
                    secondary_bucket_count,
                );
                if (secondary_start..secondary_end).contains(&secondary) {
                    row_indices[secondary - secondary_start].push(row as IdxSize);
                }
            }
            for (offset, indices) in row_indices.into_iter().enumerate() {
                if indices.is_empty() {
                    continue;
                }
                let secondary = secondary_start + offset;
                let take = IdxCa::from_vec("secondary_routing_rows".into(), indices);
                let mut frame = batch.take(&take)?;
                frame.rechunk_mut();
                let frame_edits = sum_edits_column(std::slice::from_ref(&frame))?;
                routed_rows = routed_rows
                    .checked_add(frame.height())
                    .context("secondary routing row count overflow")?;
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
            }
            reconciliation_peak.observe(MemorySnapshot::capture(), None);
            governor.checkpoint("page_weekly_edits_route_secondary")?;
        }
        info!(
            primary_bucket,
            secondary_start,
            secondary_end,
            active_writers = writers.len(),
            peak_active_writers,
            "page_weekly_edits: finished secondary writer batch"
        );
        for (secondary, writer) in writers {
            writer.finish()?;
            let path = paths[secondary]
                .as_deref()
                .context("finished secondary bucket has no output path")?;
            sync_and_discard_path_cache(path)?;
        }
        // This primary bucket is reread once per writer-sized batch. Its
        // cache is no longer useful after the final batch has closed.
        if secondary_end == secondary_bucket_count {
            storage::discard_path_cache(primary_path);
        }
    }
    anyhow::ensure!(
        checked_sum_usize(&rows, "secondary routing row count")? == expected.rows
            && routed_rows == expected.rows,
        "primary bucket {primary_bucket} row count changed during secondary routing"
    );
    anyhow::ensure!(
        routed_edits == expected.edits,
        "primary bucket {primary_bucket} edit count changed during secondary routing"
    );
    Ok(SecondaryRouting {
        paths,
        rows,
        peak_active_writers,
    })
}

pub(super) fn reclaim_completed_weekly_scratch(path: Option<&Path>) -> Result<()> {
    if let Some(path) = path {
        storage::discard_path_cache(path);
        fs::remove_file(path)?;
    }
    Ok(())
}

#[derive(Clone, Debug)]
pub(super) struct PreparedWeeklyBucket {
    pub(super) logical_bucket: usize,
    pub(super) primary_bucket: usize,
    pub(super) secondary_bucket: usize,
    pub(super) staged_rows: usize,
    pub(super) staged_path: Option<PathBuf>,
}

#[derive(Debug)]
pub(super) struct WeeklyBucketResult {
    logical_bucket: usize,
    primary_bucket: usize,
    secondary_bucket: usize,
    staged_rows: usize,
    output_rows: usize,
    edits: i64,
    minimum_week_start: Option<i32>,
    maximum_week_start: Option<i32>,
    result_path: PathBuf,
    elapsed_ms: u64,
    memory: MemorySnapshot,
}

pub(super) fn reconcile_weekly_bucket_batch(
    runs: &WeeklyRunDir,
    staged_paths: &[PathBuf],
    prepared: Vec<PreparedWeeklyBucket>,
    wiki: &str,
    governor: &ResourceGovernor,
) -> Result<Vec<WeeklyBucketResult>> {
    if prepared.is_empty() {
        return Ok(Vec::new());
    }
    anyhow::ensure!(
        prepared.len() <= governor.budget().weekly_worker_limit,
        "weekly bucket batch exceeds the governed worker limit"
    );
    let mut offset = 0;
    let mut results = Vec::with_capacity(prepared.len());
    while offset < prepared.len() {
        let mut width = (prepared.len() - offset).min(governor.budget().weekly_worker_limit);
        let (admitted, width) = loop {
            let mut admitted = Vec::with_capacity(width);
            let mut admission_error = None;
            for bucket in &prepared[offset..offset + width] {
                let (memory_bytes, scratch_bytes) = weekly_bucket_resource_estimates(bucket)?;
                match governor.admit_bucket(memory_bytes, scratch_bytes) {
                    Ok(permit) => admitted.push((bucket.clone(), permit)),
                    Err(error) => {
                        admission_error = Some(error);
                        break;
                    }
                }
            }
            if let Some(error) = admission_error {
                drop(admitted);
                if width == 1 {
                    return Err(
                        error.context("resource governor could not admit one weekly bucket")
                    );
                }
                width -= 1;
            } else {
                break (admitted, width);
            }
        };
        let wave = std::thread::scope(|scope| -> Result<Vec<WeeklyBucketResult>> {
            let handles = admitted
                .into_iter()
                .map(|(bucket, permit)| {
                    scope.spawn(move || {
                        reconcile_weekly_bucket(runs, staged_paths, bucket, wiki, governor, permit)
                    })
                })
                .collect::<Vec<_>>();
            join_weekly_worker_results(handles, "weekly bucket worker panicked")
        })?;
        results.extend(wave);
        offset += width;
    }
    results.sort_by_key(|result| result.logical_bucket);
    Ok(results)
}

fn weekly_bucket_resource_estimates(bucket: &PreparedWeeklyBucket) -> Result<(u64, u64)> {
    let estimated_rows = u64::try_from(bucket.staged_rows)?;
    let estimated_memory_bytes = estimated_rows
        .checked_mul(WEEKLY_BUCKET_ESTIMATED_BYTES_PER_ROW)
        .context("weekly bucket memory estimate overflow")?
        .max(WEEKLY_BUCKET_MIN_MEMORY_BYTES);
    let estimated_scratch_bytes = estimated_rows
        .checked_mul(WEEKLY_RESULT_ESTIMATED_BYTES_PER_ROW)
        .context("weekly bucket scratch estimate overflow")?;
    Ok((estimated_memory_bytes, estimated_scratch_bytes))
}

pub(super) fn reconcile_weekly_bucket(
    runs: &WeeklyRunDir,
    staged_paths: &[PathBuf],
    bucket: PreparedWeeklyBucket,
    wiki: &str,
    governor: &ResourceGovernor,
    _permit: BucketPermit,
) -> Result<WeeklyBucketResult> {
    let started = Instant::now();
    let staged = match bucket.staged_path.as_deref() {
        Some(path) => ParquetReader::new(File::open(path)?).finish()?,
        None => read_staged_weekly_bucket(staged_paths, bucket.primary_bucket)?,
    };
    anyhow::ensure!(
        staged.height() == bucket.staged_rows,
        "page_weekly_edits bucket {}/{} row count changed: expected {}",
        bucket.primary_bucket,
        bucket.secondary_bucket,
        bucket.staged_rows
    );
    let edits_before = sum_edits_column(std::slice::from_ref(&staged))?;
    let merged = staged
        .lazy()
        .group_by(weekly_group_keys())
        .agg([col("edits").sum()])
        .collect()?;
    let merged = sort_frame(merged, weekly_sort_keys())?;
    let weeks = merged.column("week_start")?.date()?.physical();
    let minimum_week_start = weeks.min();
    let maximum_week_start = weeks.max();
    let edits_after = sum_edits_column(std::slice::from_ref(&merged))?;
    anyhow::ensure!(
        edits_before == edits_after,
        "page_weekly_edits bucket {}/{} lost or duplicated edits: {edits_before} before, {edits_after} after",
        bucket.primary_bucket,
        bucket.secondary_bucket
    );
    let mut result = add_weekly_change_columns(merged, wiki)?;
    let output_rows = result.height();
    let result_path = runs.result_path(bucket.logical_bucket);
    let pending = PendingOutput::new(result_path.clone())?;
    let mut file = File::create(&pending.temp_path)?;
    ParquetWriter::new(&mut file)
        .with_compression(ParquetCompression::Zstd(None))
        .finish(&mut result)?;
    drop(file);
    pending.publish()?;
    reclaim_completed_weekly_scratch(bucket.staged_path.as_deref())?;
    let memory = MemorySnapshot::capture();
    governor.checkpoint("page_weekly_edits_reconcile_bucket")?;
    Ok(WeeklyBucketResult {
        logical_bucket: bucket.logical_bucket,
        primary_bucket: bucket.primary_bucket,
        secondary_bucket: bucket.secondary_bucket,
        staged_rows: bucket.staged_rows,
        output_rows,
        edits: edits_after,
        minimum_week_start,
        maximum_week_start,
        result_path,
        elapsed_ms: started.elapsed().as_millis() as u64,
        memory,
    })
}

#[allow(clippy::too_many_arguments)]
pub(super) fn append_weekly_bucket_results(
    runs: &WeeklyRunDir,
    results: Vec<WeeklyBucketResult>,
    final_path: &Path,
    output: &mut Option<AtomicBatchedParquetWriter>,
    total_edits_after: &mut i64,
    output_rows: &mut usize,
    min_week_start: &mut Option<i32>,
    max_week_start: &mut Option<i32>,
    scratch_peak_bytes: &mut u64,
    working_storage_peak_bytes: &mut u64,
    reconciliation_peak: &mut ResourcePeak,
) -> Result<()> {
    info!(
        scratch_dir = %runs.path().display(),
        final_output = %final_path.display(),
        result_count = results.len(),
        "page_weekly_edits: appending reconciled bucket batch"
    );
    *scratch_peak_bytes = (*scratch_peak_bytes).max(runs.size_bytes()?);
    for result in results {
        info!(
            logical_bucket = result.logical_bucket,
            primary_bucket = result.primary_bucket,
            secondary_bucket = result.secondary_bucket,
            result_path = %result.result_path.display(),
            final_output = %final_path.display(),
            "page_weekly_edits: opening reconciled bucket"
        );
        let mut frame = ParquetReader::new(File::open(&result.result_path)?).finish()?;
        anyhow::ensure!(
            frame.height() == result.output_rows,
            "weekly bucket result row count changed before final merge"
        );
        if output.is_none() {
            let writer = AtomicBatchedParquetWriter::new(final_path.to_path_buf(), frame.schema());
            *output = Some(writer?);
        }
        output
            .as_mut()
            .context("page_weekly_edits output writer was not initialized")?
            .write_batch(&mut frame)?;
        storage::discard_path_cache(&result.result_path);
        fs::remove_file(&result.result_path)?;
        *output_rows = output_rows
            .checked_add(result.output_rows)
            .context("page_weekly_edits output row count overflow")?;
        *total_edits_after = total_edits_after
            .checked_add(result.edits)
            .context("page_weekly_edits output edit count overflow")?;
        *min_week_start = (*min_week_start)
            .into_iter()
            .chain(result.minimum_week_start)
            .min();
        *max_week_start = (*max_week_start)
            .into_iter()
            .chain(result.maximum_week_start)
            .max();
        let working_bytes = runs
            .size_bytes()?
            .checked_add(
                output
                    .as_ref()
                    .context("page_weekly_edits output writer was not initialized")?
                    .current_bytes()?,
            )
            .context("page_weekly_edits working storage byte count overflow")?;
        *working_storage_peak_bytes = (*working_storage_peak_bytes).max(working_bytes);
        reconciliation_peak.observe(result.memory, Some(*scratch_peak_bytes));
        info!(
            primary_bucket = result.primary_bucket,
            secondary_bucket = result.secondary_bucket,
            logical_bucket = result.logical_bucket,
            staged_rows = result.staged_rows,
            merged_rows = result.output_rows,
            output_rows = *output_rows,
            elapsed_ms = result.elapsed_ms,
            rss_bytes = ?result.memory.rss_bytes,
            cgroup_current_bytes = ?result.memory.cgroup_current_bytes,
            "page_weekly_edits: merged reconciled bucket in deterministic order"
        );
    }
    Ok(())
}

pub(super) fn add_weekly_change_columns(mut weekly: DataFrame, wiki: &str) -> Result<DataFrame> {
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

pub(super) fn previous_week_edits(weekly: &DataFrame) -> Result<Vec<u32>> {
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

pub(super) struct WeeklyRunDir {
    path: PathBuf,
}

impl WeeklyRunDir {
    pub(super) fn new(output_dir: &Path, wiki: &str, scratch_root: Option<&Path>) -> Result<Self> {
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

    pub(super) fn path(&self) -> &Path {
        &self.path
    }

    pub(super) fn partition_path(&self, partition: usize) -> PathBuf {
        self.path.join(format!("partition-{partition:06}.parquet"))
    }

    pub(super) fn primary_path(&self, primary: usize) -> PathBuf {
        self.path.join(format!("primary-{primary:04}.parquet"))
    }

    pub(super) fn secondary_path(&self, primary: usize, secondary: usize) -> PathBuf {
        self.path.join(format!(
            "primary-{primary:04}-secondary-{secondary:04}.parquet"
        ))
    }

    pub(super) fn result_path(&self, logical_bucket: usize) -> PathBuf {
        self.path
            .join(format!("result-{logical_bucket:06}.parquet"))
    }

    pub(super) fn size_bytes(&self) -> Result<u64> {
        fs::read_dir(&self.path)?.try_fold(0_u64, |total, entry| {
            let bytes = entry?.metadata()?.len();
            total
                .checked_add(bytes)
                .context("weekly scratch byte count overflow")
        })
    }

    pub(super) fn stage(
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

pub(super) fn valid_weekly_run_id(value: Option<String>) -> String {
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

/// Write the final page-week Parquet in durable chunks so completed output
/// pages can leave the Toolforge job's cgroup page cache as the file grows.
struct CacheEvictingOutputFile {
    file: File,
    bytes_since_eviction: u64,
    eviction_interval_bytes: u64,
}

impl CacheEvictingOutputFile {
    fn new(file: File) -> Self {
        Self::with_eviction_interval(file, WEEKLY_OUTPUT_CACHE_EVICT_BYTES)
    }

    fn with_eviction_interval(file: File, eviction_interval_bytes: u64) -> Self {
        Self {
            file,
            bytes_since_eviction: 0,
            eviction_interval_bytes,
        }
    }
}

impl Write for CacheEvictingOutputFile {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        let written = self.file.write(buffer)?;
        self.bytes_since_eviction = self
            .bytes_since_eviction
            .saturating_add(u64::try_from(written).unwrap_or(u64::MAX));
        if self.bytes_since_eviction >= self.eviction_interval_bytes {
            // The advice is best-effort. Sync first so completed file pages
            // are clean and eligible for eviction while later batches append.
            if self.file.sync_data().is_ok()
                && let Ok(metadata) = self.file.metadata()
            {
                storage::discard_file_cache(&self.file, 0, metadata.len());
            }
            self.bytes_since_eviction = 0;
        }
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

pub(super) struct AtomicBatchedParquetWriter {
    pending: PendingOutput,
    writer: Option<BatchedWriter<CacheEvictingOutputFile>>,
    semantics: crate::artifact_receipt::SemanticAccumulator,
}

impl AtomicBatchedParquetWriter {
    pub(super) fn new(final_path: PathBuf, schema: &Schema) -> Result<Self> {
        let identity = final_path
            .file_name()
            .and_then(|name| name.to_str())
            .context("page_weekly_edits output has no UTF-8 filename")?
            .to_string();
        let pending = PendingOutput::new(final_path)?;
        let file = CacheEvictingOutputFile::new(File::create(&pending.temp_path)?);
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

    pub(super) fn write_batch(&mut self, df: &mut DataFrame) -> Result<()> {
        df.rechunk_mut();
        self.semantics.observe(df)?;
        self.writer
            .as_mut()
            .context("page_weekly_edits output writer was already finished")?
            .write_batch(df)?;
        Ok(())
    }

    pub(super) fn current_bytes(&self) -> Result<u64> {
        Ok(fs::metadata(&self.pending.temp_path)?.len())
    }

    pub(super) fn finish(mut self) -> Result<u64> {
        self.writer
            .take()
            .context("page_weekly_edits output writer was already finished")?
            .finish()?;
        let final_path = self.pending.final_path.clone();
        let bytes = self.pending.publish()?;
        crate::artifact_receipt::write_semantic_draft(&final_path, self.semantics)?;
        storage::discard_path_cache(&final_path);
        Ok(bytes)
    }
}

pub(super) fn sum_edits_column(frames: &[DataFrame]) -> Result<i64> {
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

pub(super) fn checked_sum_i64(values: &[i64], label: &str) -> Result<i64> {
    values.iter().try_fold(0i64, |total, &value| {
        total
            .checked_add(value)
            .with_context(|| format!("{label} overflow"))
    })
}

pub(super) fn checked_sum_usize(values: &[usize], label: &str) -> Result<usize> {
    values.iter().try_fold(0usize, |total, &value| {
        total
            .checked_add(value)
            .with_context(|| format!("{label} overflow"))
    })
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
mod governed_routing_tests {
    use super::*;
    use crate::resource_governor::GovernorPaths;
    use crate::test_support::TestDir;

    fn budget() -> ResourceBudget {
        ResourceBudget {
            memory_ceiling_bytes: u64::MAX,
            memory_reserve_bytes: 0,
            persistent_storage_reserve_bytes: 0,
            bounded_scratch_reserve_bytes: 0,
            rollback_generation_reserve_bytes: 0,
            scratch_limit_bytes: u64::MAX,
            max_open_files: 512,
            source_worker_limit: 2,
            thread_limit: 2,
            max_logical_partition_bytes: u64::MAX,
            max_active_parquet_writers: 2,
            weekly_worker_limit: 2,
        }
    }

    fn page_id_for_primary(primary_bucket: usize, bucket_count: usize) -> i64 {
        (0_i64..1_000_000)
            .find(|page_id| stable_weekly_bucket(Some(*page_id), bucket_count) == primary_bucket)
            .expect("fixture should find a page ID for every primary bucket")
    }

    fn primary_bucket_frame(page_id: i64) -> DataFrame {
        df!(
            "page_id" => [Some(page_id)],
            "page_namespace" => [Some(0_i32)],
            "page_title" => [Some(format!("Page-{page_id}"))],
            "week_start" => [Some(19_723_i32)],
            "edits" => [Some(1_u32)],
        )
        .expect("weekly bucket fixture should have a valid schema")
        .lazy()
        .with_column(col("week_start").cast(DataType::Date))
        .collect()
        .expect("weekly bucket fixture should collect")
    }

    fn write_primary_buckets(
        runs: &WeeklyRunDir,
        primary_bucket_count: usize,
        buckets: &[usize],
    ) -> (Vec<Option<PathBuf>>, Vec<usize>, Vec<i64>) {
        let mut paths = vec![None; primary_bucket_count];
        let mut rows = vec![0; primary_bucket_count];
        let mut edits = vec![0; primary_bucket_count];
        for primary_bucket in buckets.iter().copied() {
            let page_id = page_id_for_primary(primary_bucket, primary_bucket_count);
            let path = runs.primary_path(primary_bucket);
            let mut frame = primary_bucket_frame(page_id);
            ParquetWriter::new(File::create(&path).expect("primary fixture should be writable"))
                .finish(&mut frame)
                .expect("primary fixture should serialize");
            paths[primary_bucket] = Some(path);
            rows[primary_bucket] = 1;
            edits[primary_bucket] = 1;
        }
        (paths, rows, edits)
    }

    #[allow(clippy::too_many_arguments)]
    fn route_fixture(
        runs: &WeeklyRunDir,
        primary_paths: &[Option<PathBuf>],
        primary_bucket_rows: &[usize],
        primary_bucket_edits: &[i64],
        config: &WeeklyAggregationConfig,
        wiki: &str,
        governor: &ResourceGovernor,
        final_path: &Path,
    ) -> Result<(Vec<usize>, i64, usize, bool)> {
        let mut bucket_rows = vec![0; config.logical_bucket_count()];
        let mut output = None;
        let mut total_edits_after = 0;
        let mut output_rows = 0;
        let mut min_week_start = None;
        let mut max_week_start = None;
        let mut scratch_peak_bytes = 0;
        let mut working_storage_peak_bytes = 0;
        let mut reconciliation_peak = ResourcePeak::default();
        route_and_reconcile_weekly_buckets(
            runs,
            primary_paths,
            primary_bucket_rows,
            primary_bucket_edits,
            config,
            wiki,
            governor,
            &mut bucket_rows,
            final_path,
            &mut output,
            &mut total_edits_after,
            &mut output_rows,
            &mut min_week_start,
            &mut max_week_start,
            &mut scratch_peak_bytes,
            &mut working_storage_peak_bytes,
            &mut reconciliation_peak,
        )
        .map(|()| {
            (
                bucket_rows,
                total_edits_after,
                output_rows,
                output.is_some(),
            )
        })
    }

    fn write_single_warehouse_partition(data_dir: &TestDir, wiki: &str) {
        let wiki_dir = crate::storage::warehouse_wiki_dir(data_dir.path(), wiki);
        let partition_dir = crate::storage::month_partition_dir(&wiki_dir, 2024, "2024-01");
        fs::create_dir_all(&partition_dir).expect("warehouse fixture directory should be writable");
        let mut frame = DataFrame::new_infer_height(vec![
            Column::new("event_timestamp".into(), vec!["2024-01-02 10:00:00.0"]),
            Column::new("page_id".into(), vec![10_i64]),
            Column::new("page_title".into(), vec!["Alpha"]),
            Column::new("page_namespace".into(), vec![0_i32]),
        ])
        .expect("warehouse fixture should have a valid schema");
        ParquetWriter::new(
            &mut File::create(partition_dir.join("part-000.parquet"))
                .expect("warehouse fixture should be writable"),
        )
        .finish(&mut frame)
        .expect("warehouse fixture should serialize");
    }

    #[test]
    fn worker_limit_uses_configured_capacity_when_fd_telemetry_is_missing() -> Result<()> {
        let budget = budget();
        assert_eq!(
            weekly_route_worker_limit_from_sample(&budget, 2, None)?,
            budget.weekly_worker_limit
        );
        assert!(weekly_route_worker_limit_from_sample(&budget, 2, Some(512)).is_err());
        Ok(())
    }

    #[test]
    fn route_admission_retries_a_smaller_wave_and_reconciles_in_order() {
        let output_dir = TestDir::new().expect("route fixture directory should be writable");
        let wiki = "testwiki";
        let runs = WeeklyRunDir::new(output_dir.path(), wiki, None)
            .expect("weekly run directory should be writable");
        let config = WeeklyAggregationConfig::new_two_level(32, 8, None)
            .expect("two-level fixture layout should be valid");
        let governor = ResourceGovernor::new(
            budget(),
            GovernorPaths::new(output_dir.path().to_path_buf(), None),
        )
        .with_persistent_available_sequence([u64::MAX, u64::MAX, 0]);
        let (paths, rows, edits) = write_primary_buckets(&runs, 32, &[0, 1]);
        let final_path = output_dir
            .path()
            .join(wiki)
            .join("page_weekly_edits.parquet");

        let (bucket_rows, total_edits_after, output_rows, output_initialized) = route_fixture(
            &runs,
            &paths,
            &rows,
            &edits,
            &config,
            wiki,
            &governor,
            &final_path,
        )
        .expect("route should shrink its wave and finish both buckets");
        assert_eq!(total_edits_after, 2);
        assert_eq!(output_rows, 2);
        assert_eq!(bucket_rows.iter().sum::<usize>(), 2);
        assert!(output_initialized);
    }

    #[test]
    fn route_admission_rejects_an_unavailable_single_worker() {
        let output_dir = TestDir::new().expect("route fixture directory should be writable");
        let wiki = "testwiki";
        let runs = WeeklyRunDir::new(output_dir.path(), wiki, None)
            .expect("weekly run directory should be writable");
        let config = WeeklyAggregationConfig::new_two_level(32, 8, None)
            .expect("two-level fixture layout should be valid");
        let governor = ResourceGovernor::new(
            budget(),
            GovernorPaths::new(output_dir.path().to_path_buf(), None),
        )
        .with_persistent_available_sequence([u64::MAX, 0]);
        let (paths, rows, edits) = write_primary_buckets(&runs, 32, &[0]);
        let final_path = output_dir
            .path()
            .join(wiki)
            .join("page_weekly_edits.parquet");

        let error = route_fixture(
            &runs,
            &paths,
            &rows,
            &edits,
            &config,
            wiki,
            &governor,
            &final_path,
        )
        .expect_err("one worker must be rejected when scratch space is unavailable");
        assert!(
            error
                .to_string()
                .contains("could not admit one weekly route worker")
        );
    }

    #[test]
    fn route_worker_parquet_error_is_returned() {
        let output_dir = TestDir::new().expect("route fixture directory should be writable");
        let wiki = "testwiki";
        let runs = WeeklyRunDir::new(output_dir.path(), wiki, None)
            .expect("weekly run directory should be writable");
        let config = WeeklyAggregationConfig::new_two_level(32, 8, None)
            .expect("two-level fixture layout should be valid");
        let governor = ResourceGovernor::new(
            budget(),
            GovernorPaths::new(output_dir.path().to_path_buf(), None),
        )
        .with_persistent_available_sequence([u64::MAX; 8]);
        let mut paths = vec![None; config.primary_bucket_count];
        let mut rows = vec![0; config.primary_bucket_count];
        let mut edits = vec![0; config.primary_bucket_count];
        let invalid_path = runs.primary_path(0);
        fs::write(&invalid_path, b"not a parquet file")
            .expect("invalid parquet fixture should be writable");
        paths[0] = Some(invalid_path);
        rows[0] = 1;
        edits[0] = 1;
        let final_path = output_dir
            .path()
            .join(wiki)
            .join("page_weekly_edits.parquet");

        let error = route_fixture(
            &runs,
            &paths,
            &rows,
            &edits,
            &config,
            wiki,
            &governor,
            &final_path,
        )
        .expect_err("route worker must return its Parquet read error");
        assert!(!error.to_string().is_empty());
    }

    #[test]
    fn append_failure_is_returned_from_route_batch() {
        let output_dir = TestDir::new().expect("route fixture directory should be writable");
        let wiki = "testwiki";
        let runs = WeeklyRunDir::new(output_dir.path(), wiki, None)
            .expect("weekly run directory should be writable");
        let config = WeeklyAggregationConfig::new_two_level(32, 8, None)
            .expect("two-level fixture layout should be valid");
        let governor = ResourceGovernor::new(
            budget(),
            GovernorPaths::new(output_dir.path().to_path_buf(), None),
        )
        .with_persistent_available_sequence([u64::MAX; 32]);
        let (paths, rows, edits) = write_primary_buckets(&runs, 32, &[0]);
        let blocked_parent = output_dir.path().join("not-a-directory");
        fs::write(&blocked_parent, b"file blocks output directory creation")
            .expect("blocked output path should be writable");
        let final_path = blocked_parent.join("page_weekly_edits.parquet");

        let error = route_fixture(
            &runs,
            &paths,
            &rows,
            &edits,
            &config,
            wiki,
            &governor,
            &final_path,
        )
        .expect_err("route batch must propagate the final-output creation error");
        assert!(!error.to_string().is_empty());
    }

    #[test]
    fn reconciliation_admission_retries_a_smaller_wave() -> Result<()> {
        let output_dir = TestDir::new()?;
        let wiki = "testwiki";
        let runs = WeeklyRunDir::new(output_dir.path(), wiki, None)?;
        let governor = ResourceGovernor::new(
            budget(),
            GovernorPaths::new(output_dir.path().to_path_buf(), None),
        )
        .with_persistent_available_sequence([u64::MAX, 0, u64::MAX]);
        let mut prepared = Vec::new();

        for logical_bucket in 0..2 {
            let page_id = page_id_for_primary(logical_bucket, 32);
            let staged_path = runs.secondary_path(0, logical_bucket);
            let mut frame = primary_bucket_frame(page_id);
            ParquetWriter::new(File::create(&staged_path)?).finish(&mut frame)?;
            prepared.push(PreparedWeeklyBucket {
                logical_bucket,
                primary_bucket: 0,
                secondary_bucket: logical_bucket,
                staged_rows: 1,
                staged_path: Some(staged_path),
            });
        }

        let results = reconcile_weekly_bucket_batch(&runs, &[], prepared, wiki, &governor)?;
        assert_eq!(
            results
                .iter()
                .map(|result| result.logical_bucket)
                .collect::<Vec<_>>(),
            vec![0, 1]
        );
        assert_eq!(results.iter().map(|result| result.edits).sum::<i64>(), 2);
        Ok(())
    }

    #[test]
    fn reconciliation_admission_rejects_an_unavailable_single_bucket() {
        let output_dir =
            TestDir::new().expect("reconciliation fixture directory should be writable");
        let wiki = "testwiki";
        let runs = WeeklyRunDir::new(output_dir.path(), wiki, None)
            .expect("weekly run directory should be writable");
        let governor = ResourceGovernor::new(
            budget(),
            GovernorPaths::new(output_dir.path().to_path_buf(), None),
        )
        .with_persistent_available_sequence([0]);
        let staged_path = runs.secondary_path(0, 0);
        let mut frame = primary_bucket_frame(page_id_for_primary(0, 32));
        ParquetWriter::new(File::create(&staged_path).expect("staged fixture should be writable"))
            .finish(&mut frame)
            .expect("staged fixture should serialize");
        let prepared = vec![PreparedWeeklyBucket {
            logical_bucket: 0,
            primary_bucket: 0,
            secondary_bucket: 0,
            staged_rows: 1,
            staged_path: Some(staged_path),
        }];

        let error = reconcile_weekly_bucket_batch(&runs, &[], prepared, wiki, &governor)
            .expect_err("one bucket must be rejected when scratch space is unavailable");
        assert!(
            error
                .to_string()
                .contains("could not admit one weekly bucket")
        );
    }

    #[test]
    fn two_level_compute_propagates_unavailable_route_worker() {
        let data_dir = TestDir::new().expect("warehouse fixture directory should be writable");
        let output_dir = TestDir::new().expect("weekly output directory should be writable");
        let wiki = "testwiki";
        write_single_warehouse_partition(&data_dir, wiki);
        let config = WeeklyAggregationConfig::new_two_level(32, 8, None)
            .expect("two-level fixture layout should be valid");
        let scratch_root = output_dir.path().join(wiki);
        fs::create_dir_all(&scratch_root).expect("weekly scratch directory should be writable");
        let mut capacity = budget();
        capacity.max_active_parquet_writers = 32;
        let governor = ResourceGovernor::new(
            capacity,
            GovernorPaths::new(data_dir.path().to_path_buf(), Some(scratch_root)),
        )
        .with_persistent_available_sequence([u64::MAX, u64::MAX, u64::MAX, u64::MAX, 0]);

        let error = compute_page_weekly_edits_for_snapshot_with_governor(
            wiki,
            data_dir.path(),
            output_dir.path(),
            &config,
            None,
            None,
            &governor,
            std::time::Instant::now(),
        )
        .expect_err("top-level compute must propagate the route admission failure");
        assert!(
            error
                .to_string()
                .contains("could not admit one weekly route worker")
        );
    }

    #[test]
    fn secondary_bucket_path_requires_files_only_for_nonempty_buckets() -> Result<()> {
        assert_eq!(required_secondary_bucket_path(0, None, 3, 4)?, None);
        let error = required_secondary_bucket_path(1, None, 3, 4)
            .expect_err("non-empty secondary bucket requires a staged file");
        assert!(
            error
                .to_string()
                .contains("missing non-empty secondary bucket 3/4")
        );
        let path = PathBuf::from("secondary.parquet");
        assert_eq!(
            required_secondary_bucket_path(1, Some(path.clone()), 3, 4)?,
            Some(path)
        );
        Ok(())
    }

    #[test]
    fn worker_join_preserves_errors_and_converts_panics() {
        let worker_error = std::thread::scope(|scope| {
            let handles = vec![
                scope.spawn(|| -> Result<usize> { anyhow::bail!("injected weekly worker error") }),
            ];
            join_weekly_worker_results(handles, "injected weekly worker panic")
        })
        .expect_err("worker errors must fail the batch");
        assert!(
            worker_error
                .to_string()
                .contains("injected weekly worker error")
        );

        let worker_panic = std::thread::scope(|scope| {
            let handles =
                vec![scope.spawn(|| -> Result<usize> { panic!("injected scoped worker panic") })];
            join_weekly_worker_results(handles, "injected weekly worker panic")
        })
        .expect_err("worker panics must fail the batch");
        assert!(
            worker_panic
                .to_string()
                .contains("injected weekly worker panic")
        );
    }
}

#[cfg(test)]
mod output_cache_tests {
    use super::{CacheEvictingOutputFile, File};
    use crate::test_support::TestDir;
    use std::io::Write;

    #[test]
    fn syncs_and_evicts_completed_output_chunks() -> anyhow::Result<()> {
        let directory = TestDir::new()?;
        let path = directory.path().join("page-week.tmp");
        let file = File::create(&path)?;
        let mut output = CacheEvictingOutputFile::with_eviction_interval(file, 5);

        output.write_all(b"first")?;
        assert_eq!(output.bytes_since_eviction, 0);
        output.write_all(b"next")?;
        assert_eq!(output.bytes_since_eviction, 4);
        output.flush()?;
        assert_eq!(std::fs::read(path)?, b"firstnext");

        Ok(())
    }
}
