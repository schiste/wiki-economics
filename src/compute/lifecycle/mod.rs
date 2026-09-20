//! Stateful editor lifecycle accumulators, checkpoints, and output assembly.

/// Semantic version for stateful editor lifecycle metrics.
pub(crate) const ALGORITHM_VERSION: &str =
    "editor-lifecycle-v4-explicit-identified-registered-editors-external-merge";

use super::{add_wiki_column, concat_frames, write_output};
use crate::{metric_registry::MetricFamily, storage};
use anyhow::Result;
use polars::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::File;
use std::path::Path;

#[cfg(not(coverage))]
use anyhow::Context;
#[cfg(not(coverage))]
use std::cmp::Reverse;
#[cfg(not(coverage))]
use std::env;
#[cfg(not(coverage))]
use std::fs;
#[cfg(not(coverage))]
use std::path::PathBuf;
#[cfg(not(coverage))]
use std::time::{SystemTime, UNIX_EPOCH};

fn normalize_period_key(year_month_key: i32, period_type: &str) -> Result<i32> {
    let year = year_month_key / 100;
    let month = year_month_key % 100;

    match period_type {
        "month" => Ok(year_month_key),
        "quarter" => Ok(year * 10 + ((month - 1) / 3) + 1),
        "year" => Ok(year),
        _ => anyhow::bail!("unsupported period type: {period_type}"),
    }
}

fn format_period_key(period_key: i32, period_type: &str) -> String {
    match period_type {
        "month" => format!("{}-{:02}", period_key / 100, period_key % 100),
        "quarter" => format!("{}-Q{}", period_key / 10, period_key % 10),
        "year" => period_key.to_string(),
        _ => period_key.to_string(),
    }
}

fn period_months_for_type(period_type: &str) -> u32 {
    match period_type {
        "month" => 1,
        "quarter" => 3,
        "year" => 12,
        _ => 0,
    }
}

#[derive(Clone)]
pub(super) struct ChurnAccumulator {
    period_type: &'static str,
    seen: HashSet<(i64, i32)>,
    pub(super) active: BTreeMap<i32, u32>,
    pub(super) spans: HashMap<i64, (i32, i32)>,
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
            .map(|period_key| format_period_key(*period_key, self.period_type))
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
            Column::new(
                "period_months".into(),
                vec![period_months_for_type(self.period_type); self.active.len()],
            ),
            Column::new("arrival_rate".into(), arrival_rate),
            Column::new("departure_rate".into(), departure_rate),
        ])
        .map_err(Into::into)
    }
}

#[derive(Clone)]
pub(super) struct RegisteredState {
    pub(super) funnel_stats: HashMap<i64, (i32, u32)>,
    pub(super) cohort_spans: HashMap<i64, (i32, i32)>,
    pub(super) churn_month: ChurnAccumulator,
    pub(super) churn_quarter: ChurnAccumulator,
    pub(super) churn_year: ChurnAccumulator,
}

impl RegisteredState {
    pub(super) fn new() -> Self {
        Self {
            funnel_stats: HashMap::new(),
            cohort_spans: HashMap::new(),
            churn_month: ChurnAccumulator::new("month"),
            churn_quarter: ChurnAccumulator::new("quarter"),
            churn_year: ChurnAccumulator::new("year"),
        }
    }

    pub(super) fn observe_partition(
        &mut self,
        base: &DataFrame,
        year: i32,
        year_month_key: i32,
    ) -> Result<()> {
        let partial = registered_editor_totals(base)?;
        let user_ids = partial.column("event_user_id")?.i64()?;
        let total_edits = partial.column("total_edits")?.u32()?;
        let cohort_years = partial.column("cohort_year")?.i32()?;

        for (user_id, user_total_edits, cohort_year) in (0..partial.height()).filter_map(|idx| {
            Some((
                user_ids.get(idx)?,
                total_edits.get(idx)?,
                cohort_years.get(idx)?,
            ))
        }) {
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
            self.churn_quarter
                .observe(user_id, normalize_period_key(year_month_key, "quarter")?);
            self.churn_year.observe(user_id, year);
        }

        Ok(())
    }

    pub(super) fn observe_history(&mut self, base: &DataFrame) -> Result<()> {
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
            self.churn_quarter
                .observe(user_id, normalize_period_key(year_month_key, "quarter")?);
            self.churn_year.observe(user_id, year);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct ChurnCheckpoint {
    active: BTreeMap<i32, u32>,
    spans: BTreeMap<i64, (i32, i32)>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct LifecycleCheckpoint {
    schema_version: u32,
    algorithm_version: String,
    pub(super) through_month: String,
    input_month_digest_prefix: String,
    funnel_stats: BTreeMap<i64, (i32, u32)>,
    cohort_spans: BTreeMap<i64, (i32, i32)>,
    churn_month: ChurnCheckpoint,
    churn_quarter: ChurnCheckpoint,
    churn_year: ChurnCheckpoint,
}

impl LifecycleCheckpoint {
    pub(super) fn from_state(state: &RegisteredState, through_month: &str, prefix: &str) -> Self {
        let churn = |value: &ChurnAccumulator| ChurnCheckpoint {
            active: value.active.clone(),
            spans: value
                .spans
                .iter()
                .map(|(user, span)| (*user, *span))
                .collect(),
        };
        Self {
            schema_version: 1,
            algorithm_version: ALGORITHM_VERSION.to_string(),
            through_month: through_month.to_string(),
            input_month_digest_prefix: prefix.to_string(),
            funnel_stats: state
                .funnel_stats
                .iter()
                .map(|(user, stats)| (*user, *stats))
                .collect(),
            cohort_spans: state
                .cohort_spans
                .iter()
                .map(|(user, span)| (*user, *span))
                .collect(),
            churn_month: churn(&state.churn_month),
            churn_quarter: churn(&state.churn_quarter),
            churn_year: churn(&state.churn_year),
        }
    }

    pub(super) fn validate(&self, through_month: &str, prefix: &str) -> Result<()> {
        anyhow::ensure!(
            self.schema_version == 1
                && self.algorithm_version == ALGORITHM_VERSION
                && self.through_month == through_month
                && self.input_month_digest_prefix == prefix,
            "lifecycle checkpoint identity changed"
        );
        Ok(())
    }

    pub(super) fn into_state(self) -> RegisteredState {
        let churn = |period_type, value: ChurnCheckpoint| ChurnAccumulator {
            period_type,
            seen: HashSet::new(),
            active: value.active,
            spans: value.spans.into_iter().collect(),
        };
        RegisteredState {
            funnel_stats: self.funnel_stats.into_iter().collect(),
            cohort_spans: self.cohort_spans.into_iter().collect(),
            churn_month: churn("month", self.churn_month),
            churn_quarter: churn("quarter", self.churn_quarter),
            churn_year: churn("year", self.churn_year),
        }
    }
}

pub(super) fn lifecycle_prefix_digest(
    cache: &crate::cross_snapshot::CrossSnapshotCache,
    month_digests: &[String],
) -> String {
    let inputs = month_digests.iter().map(String::as_str).collect::<Vec<_>>();
    cache.derived_digest("lifecycle_prefix", ALGORITHM_VERSION, &inputs)
}

pub(super) fn load_latest_lifecycle_checkpoint(
    cache: &crate::cross_snapshot::CrossSnapshotCache,
    partitions: &[storage::PartitionSpec],
) -> Result<Option<LifecycleCheckpoint>> {
    let mut month_digests = Vec::with_capacity(partitions.len());
    let mut boundaries = Vec::new();
    for partition in partitions {
        month_digests.push(cache.month_digest(&partition.year_month)?.to_string());
        if partition.year_month.ends_with("-12") {
            boundaries.push((
                partition.year_month.clone(),
                lifecycle_prefix_digest(cache, &month_digests),
            ));
        }
    }
    for (through_month, prefix) in boundaries.into_iter().rev() {
        let checkpoint_result =
            cache.load_json("lifecycle_checkpoint", ALGORITHM_VERSION, &prefix, "state");
        let checkpoint: Option<LifecycleCheckpoint> = checkpoint_result?;
        if let Some(checkpoint) = checkpoint {
            checkpoint.validate(&through_month, &prefix)?;
            return Ok(Some(checkpoint));
        }
    }
    Ok(None)
}

pub(super) fn lifecycle_full_digest(
    cache: &crate::cross_snapshot::CrossSnapshotCache,
    partitions: &[storage::PartitionSpec],
) -> Result<String> {
    let digests = partitions
        .iter()
        .map(|partition| cache.month_digest(&partition.year_month))
        .collect::<Result<Vec<_>>>()?;
    Ok(cache.derived_digest("lifecycle_full", ALGORITHM_VERSION, &digests))
}

pub(super) fn load_cached_lifecycle_outputs(
    cache: &crate::cross_snapshot::CrossSnapshotCache,
    input_digest: &str,
) -> Result<Option<Vec<(&'static str, DataFrame)>>> {
    let mut outputs = Vec::new();
    for &metric in MetricFamily::Lifecycle.metrics() {
        let cached = cache.load("lifecycle_final", ALGORITHM_VERSION, input_digest, metric);
        let Some(frame) = cached? else {
            return Ok(None);
        };
        outputs.push((metric, frame));
    }
    Ok(Some(outputs))
}

pub(super) fn store_lifecycle_outputs(
    cache: &crate::cross_snapshot::CrossSnapshotCache,
    input_digest: &str,
    wiki: &str,
    output_dir: &Path,
) -> Result<()> {
    for &metric in MetricFamily::Lifecycle.metrics() {
        let path = output_dir.join(wiki).join(format!("{metric}.parquet"));
        let mut frame = ParquetReader::new(File::open(path)?)
            .set_low_memory(true)
            .finish()?;
        let store_result = cache.store(
            "lifecycle_final",
            ALGORITHM_VERSION,
            input_digest,
            metric,
            &mut frame,
        );
        store_result?;
    }
    Ok(())
}

fn registered_editor_totals(base: &DataFrame) -> Result<DataFrame> {
    base.clone()
        .lazy()
        .filter(
            col("user_type")
                .eq(lit("registered"))
                .and(col("event_user_id").is_not_null()),
        )
        .group_by([col("event_user_id")])
        .agg([
            col("revision_id").count().alias("total_edits"),
            col("year").min().cast(DataType::Int32).alias("cohort_year"),
        ])
        .collect()
        .map_err(Into::into)
}

fn build_cohort_output(editor_spans: &DataFrame, all_years: &[i32]) -> Result<DataFrame> {
    let cohort_years = editor_spans.column("cohort_year")?.i32()?;
    let last_years = editor_spans.column("last_year")?.i32()?;

    let mut initial_sizes: BTreeMap<i32, u32> = BTreeMap::new();
    let mut ended_by: HashMap<(i32, i32), u32> = HashMap::new();
    for index in 0..editor_spans.height() {
        let (Some(cohort_year), Some(last_year)) = (cohort_years.get(index), last_years.get(index))
        else {
            continue;
        };
        *initial_sizes.entry(cohort_year).or_insert(0) += 1;
        *ended_by.entry((cohort_year, last_year)).or_insert(0) += 1;
    }

    let mut cohort_years_out = Vec::new();
    let mut years_out = Vec::new();
    let mut survived_out = Vec::new();
    let mut initial_out = Vec::new();
    for (&cohort_year, &initial) in &initial_sizes {
        let mut survivors = 0_u32;
        let mut cohort_rows = Vec::new();
        for &year in all_years.iter().rev() {
            if year < cohort_year {
                continue;
            }
            survivors += ended_by.get(&(cohort_year, year)).copied().unwrap_or(0);
            cohort_rows.push((year, survivors));
        }
        cohort_rows.reverse();
        for (year, survived) in cohort_rows {
            cohort_years_out.push(cohort_year.to_string());
            years_out.push(year.to_string());
            survived_out.push(survived);
            initial_out.push(initial);
        }
    }

    DataFrame::new_infer_height(vec![
        Column::new("cohort_year".into(), cohort_years_out),
        Column::new("year".into(), years_out),
        Column::new("survived_editors".into(), survived_out),
        Column::new("initial_editors".into(), initial_out),
    ])
    .map_err(Into::into)
}

pub(super) fn finalize_funnel(
    stats: HashMap<i64, (i32, u32)>,
    wiki: &str,
    output_dir: &Path,
) -> Result<()> {
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

pub(super) fn finalize_labor_cohorts(
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
    let mut cohort_out = build_cohort_output(&editor_spans, &all_years)?;
    add_wiki_column(&mut cohort_out, wiki)?;
    write_output(&mut cohort_out, wiki, "labor_cohorts", output_dir)
}

pub(super) fn write_lifecycle_outputs(
    wiki: &str,
    output_dir: &Path,
    state: RegisteredState,
) -> Result<()> {
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

#[cfg(not(coverage))]
mod external {
    use super::*;

    const EXTERNAL_RUN_BATCH_ROWS: usize = 131_072;
    const EXTERNAL_MERGE_BATCH_ROWS: usize = 16_384;

    /// Compute lifecycle metrics with bounded memory.
    ///
    /// The regular lifecycle path keeps one state entry per editor until all
    /// months have been scanned. That is a poor fit for the 6 GiB Toolforge
    /// worker. This path writes one sorted, per-month contribution run and then
    /// performs a k-way merge. During the merge only the current editor and the
    /// aggregate period maps are resident, so memory is independent of the
    /// number of editors in the source history.
    pub(crate) fn compute_external<F>(
        wiki: &str,
        output_dir: &Path,
        partitions: &[storage::PartitionSpec],
        mut load_partition: F,
    ) -> Result<usize>
    where
        F: FnMut(&[PathBuf]) -> Result<DataFrame>,
    {
        anyhow::ensure!(
            !partitions.is_empty(),
            "external lifecycle computation requires at least one input partition"
        );
        let runs = ExternalRunDir::new(output_dir, wiki)?;
        let mut run_paths = Vec::with_capacity(partitions.len());
        for (index, partition) in partitions.iter().enumerate() {
            let base = load_partition(&partition.files)?;
            let (year, month) = partition
                .year_month
                .split_once('-')
                .context("lifecycle partition has no year-month key")?;
            let year: i32 = year
                .parse()
                .map_err(|error| anyhow::anyhow!("invalid lifecycle partition year: {error}"))?;
            let month: i32 = month
                .parse()
                .map_err(|error| anyhow::anyhow!("invalid lifecycle partition month: {error}"))?;
            let year_month_key = year * 100 + month;
            let mut partial = registered_editor_totals(&base)?
                .lazy()
                .with_columns([
                    lit(partition.year).cast(DataType::Int32).alias("year"),
                    lit(year_month_key)
                        .cast(DataType::Int32)
                        .alias("year_month_key"),
                ])
                .select([
                    col("event_user_id"),
                    col("year"),
                    col("year_month_key"),
                    col("cohort_year"),
                    col("total_edits"),
                ])
                .collect()?;
            if partial.height() == 0 {
                for file in &partition.files {
                    storage::discard_path_cache(file);
                }
                continue;
            }
            partial = partial.sort(
                ["event_user_id", "year_month_key"],
                SortMultipleOptions::default(),
            )?;
            let path = runs.partition_path(index);
            write_external_run(&path, &mut partial)?;
            run_paths.push(path);
            for file in &partition.files {
                storage::discard_path_cache(file);
            }
        }

        let mut aggregate = ExternalLifecycleAggregate::default();
        if !run_paths.is_empty() {
            let mut cursors = run_paths
                .iter()
                .map(|path| ExternalContributionCursor::new(path))
                .collect::<Result<Vec<_>>>()?;
            let mut heap = std::collections::BinaryHeap::new();
            for (run, cursor) in cursors.iter_mut().enumerate() {
                if let Some(row) = cursor.next_row()? {
                    heap.push(Reverse((row, run)));
                }
            }
            let mut current: Option<ExternalUser> = None;
            while let Some(Reverse((row, run))) = heap.pop() {
                if let Some(next) = cursors[run].next_row()? {
                    heap.push(Reverse((next, run)));
                }
                if current
                    .as_ref()
                    .is_some_and(|user| user.user_id != row.user_id)
                {
                    finalize_external_user(
                        current.take().expect("current lifecycle user"),
                        &mut aggregate,
                    )?;
                }
                if current.is_none() {
                    current = Some(ExternalUser::new(row.user_id));
                }
                current
                    .as_mut()
                    .expect("current lifecycle user")
                    .observe(row, &mut aggregate)?;
            }
            if let Some(user) = current {
                finalize_external_user(user, &mut aggregate)?;
            }
        }
        write_external_lifecycle_outputs(wiki, output_dir, aggregate)?;
        Ok(partitions.len())
    }

    struct ExternalRunDir {
        path: PathBuf,
    }

    impl ExternalRunDir {
        fn new(output_dir: &Path, wiki: &str) -> Result<Self> {
            let root = env::var_os("WIKI_ECON_SCRATCH_DIR")
                .map(PathBuf::from)
                .unwrap_or_else(|| output_dir.to_path_buf());
            let parent = root.join(wiki);
            fs::create_dir_all(&parent)?;
            let nonce = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos();
            let run_id = env::var("WIKI_ECON_RUN_ID")
                .unwrap_or_else(|_| "standalone".to_string())
                .chars()
                .map(|character| {
                    if character.is_ascii_alphanumeric() || matches!(character, '-' | '_') {
                        character
                    } else {
                        '_'
                    }
                })
                .collect::<String>();
            let path = parent.join(format!(
                ".lifecycle-runs-{run_id}-{}-{nonce}",
                std::process::id()
            ));
            fs::create_dir(&path)?;
            Ok(Self { path })
        }

        fn partition_path(&self, partition: usize) -> PathBuf {
            self.path.join(format!("partition-{partition:06}.parquet"))
        }
    }

    impl Drop for ExternalRunDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    fn write_external_run(path: &Path, frame: &mut DataFrame) -> Result<()> {
        let mut file = File::create(path)?;
        ParquetWriter::new(&mut file)
            .with_compression(ParquetCompression::Zstd(None))
            .with_row_group_size(Some(EXTERNAL_RUN_BATCH_ROWS))
            .set_parallel(false)
            .finish(frame)?;
        file.sync_all()?;
        Ok(())
    }

    #[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
    struct ExternalContributionRow {
        user_id: i64,
        year_month_key: i32,
        year: i32,
        cohort_year: i32,
        total_edits: u32,
    }

    struct ExternalContributionCursor {
        reader: storage::SequentialParquetReader,
        batch: Option<DataFrame>,
        row: usize,
        previous: Option<ExternalContributionRow>,
    }

    impl ExternalContributionCursor {
        fn new(path: &Path) -> Result<Self> {
            let columns = [
                "event_user_id",
                "year_month_key",
                "year",
                "cohort_year",
                "total_edits",
            ]
            .into_iter()
            .map(str::to_string)
            .collect();
            Ok(Self {
                reader: storage::SequentialParquetReader::new(
                    path,
                    Some(columns),
                    EXTERNAL_MERGE_BATCH_ROWS,
                )?,
                batch: None,
                row: 0,
                previous: None,
            })
        }

        fn next_row(&mut self) -> Result<Option<ExternalContributionRow>> {
            loop {
                if let Some(batch) = &self.batch
                    && self.row < batch.height()
                {
                    let row = ExternalContributionRow {
                        user_id: batch
                            .column("event_user_id")?
                            .i64()?
                            .get(self.row)
                            .context("lifecycle contribution has no user id")?,
                        year_month_key: batch
                            .column("year_month_key")?
                            .i32()?
                            .get(self.row)
                            .context("lifecycle contribution has no month")?,
                        year: batch
                            .column("year")?
                            .i32()?
                            .get(self.row)
                            .context("lifecycle contribution has no year")?,
                        cohort_year: batch
                            .column("cohort_year")?
                            .i32()?
                            .get(self.row)
                            .context("lifecycle contribution has no cohort")?,
                        total_edits: batch
                            .column("total_edits")?
                            .u32()?
                            .get(self.row)
                            .context("lifecycle contribution has no edit count")?,
                    };
                    self.row += 1;
                    if let Some(previous) = &self.previous {
                        anyhow::ensure!(
                            previous < &row,
                            "lifecycle contribution run contains duplicate or unordered user-month rows"
                        );
                    }
                    self.previous = Some(row.clone());
                    return Ok(Some(row));
                }
                let Some(next) = self.reader.next_batch()? else {
                    return Ok(None);
                };
                self.batch = Some(next);
                self.row = 0;
            }
        }
    }

    #[derive(Default)]
    struct ExternalPeriodAggregate {
        active: BTreeMap<i32, u32>,
        arrivals: BTreeMap<i32, u32>,
        departures: BTreeMap<i32, u32>,
    }

    #[derive(Default)]
    struct ExternalLifecycleAggregate {
        funnel: BTreeMap<i32, (u32, u32, u32, u32)>,
        initial_sizes: BTreeMap<i32, u32>,
        ended_by: HashMap<(i32, i32), u32>,
        month: ExternalPeriodAggregate,
        quarter: ExternalPeriodAggregate,
        year: ExternalPeriodAggregate,
        all_years: std::collections::BTreeSet<i32>,
    }

    struct ExternalUser {
        user_id: i64,
        total_edits: u64,
        cohort_year: i32,
        first_year: i32,
        last_year: i32,
        first_month: i32,
        last_month: i32,
        first_quarter: i32,
        last_quarter: i32,
        first_period_year: i32,
        last_period_year: i32,
        previous_month: Option<i32>,
        previous_quarter: Option<i32>,
        previous_year: Option<i32>,
    }

    impl ExternalUser {
        fn new(user_id: i64) -> Self {
            Self {
                user_id,
                total_edits: 0,
                cohort_year: i32::MAX,
                first_year: i32::MAX,
                last_year: i32::MIN,
                first_month: i32::MAX,
                last_month: i32::MIN,
                first_quarter: i32::MAX,
                last_quarter: i32::MIN,
                first_period_year: i32::MAX,
                last_period_year: i32::MIN,
                previous_month: None,
                previous_quarter: None,
                previous_year: None,
            }
        }

        fn observe(
            &mut self,
            row: ExternalContributionRow,
            aggregate: &mut ExternalLifecycleAggregate,
        ) -> Result<()> {
            anyhow::ensure!(
                row.user_id == self.user_id,
                "lifecycle merge changed users without finalizing the prior user"
            );
            if let Some(previous) = self.previous_month {
                anyhow::ensure!(
                    row.year_month_key > previous,
                    "lifecycle merge encountered duplicate or unordered user-month rows"
                );
            }
            self.previous_month = Some(row.year_month_key);
            self.total_edits = self
                .total_edits
                .checked_add(u64::from(row.total_edits))
                .context("lifecycle edit count overflow")?;
            self.cohort_year = self.cohort_year.min(row.cohort_year);
            self.first_year = self.first_year.min(row.year);
            self.last_year = self.last_year.max(row.year);
            self.first_month = self.first_month.min(row.year_month_key);
            self.last_month = self.last_month.max(row.year_month_key);
            let quarter = normalize_period_key(row.year_month_key, "quarter")?;
            self.first_quarter = self.first_quarter.min(quarter);
            self.last_quarter = self.last_quarter.max(quarter);
            self.first_period_year = self.first_period_year.min(row.year);
            self.last_period_year = self.last_period_year.max(row.year);
            add_count(&mut aggregate.month.active, row.year_month_key, 1)?;
            if self.previous_quarter != Some(quarter) {
                add_count(&mut aggregate.quarter.active, quarter, 1)?;
                self.previous_quarter = Some(quarter);
            }
            if self.previous_year != Some(row.year) {
                add_count(&mut aggregate.year.active, row.year, 1)?;
                self.previous_year = Some(row.year);
            }
            Ok(())
        }
    }

    fn add_count(map: &mut BTreeMap<i32, u32>, key: i32, amount: u32) -> Result<()> {
        let value = map.entry(key).or_insert(0);
        *value = value
            .checked_add(amount)
            .context("lifecycle aggregate count overflow")?;
        Ok(())
    }

    fn finalize_external_user(
        user: ExternalUser,
        aggregate: &mut ExternalLifecycleAggregate,
    ) -> Result<()> {
        anyhow::ensure!(
            user.total_edits <= u64::from(u32::MAX),
            "lifecycle edit count exceeds metric schema capacity"
        );
        let total_edits = user.total_edits as u32;
        let entry = aggregate
            .funnel
            .entry(user.cohort_year)
            .or_insert((0, 0, 0, 0));
        entry.0 = entry
            .0
            .checked_add(1)
            .context("lifecycle cohort overflow")?;
        if total_edits >= 5 {
            entry.1 = entry
                .1
                .checked_add(1)
                .context("lifecycle cohort overflow")?;
        }
        if total_edits >= 25 {
            entry.2 = entry
                .2
                .checked_add(1)
                .context("lifecycle cohort overflow")?;
        }
        if total_edits >= 100 {
            entry.3 = entry
                .3
                .checked_add(1)
                .context("lifecycle cohort overflow")?;
        }
        add_count(&mut aggregate.initial_sizes, user.cohort_year, 1)?;
        let ended = aggregate
            .ended_by
            .entry((user.cohort_year, user.last_year))
            .or_insert(0);
        *ended = ended.checked_add(1).context("lifecycle cohort overflow")?;
        aggregate.all_years.insert(user.first_year);
        aggregate.all_years.insert(user.last_year);
        for (period, first, last) in [
            (&mut aggregate.month, user.first_month, user.last_month),
            (
                &mut aggregate.quarter,
                user.first_quarter,
                user.last_quarter,
            ),
            (
                &mut aggregate.year,
                user.first_period_year,
                user.last_period_year,
            ),
        ] {
            add_count(&mut period.arrivals, first, 1)?;
            add_count(&mut period.departures, last, 1)?;
        }
        Ok(())
    }

    fn external_funnel_frame(funnel: &BTreeMap<i32, (u32, u32, u32, u32)>) -> Result<DataFrame> {
        DataFrame::new_infer_height(vec![
            Column::new(
                "cohort_year".into(),
                funnel.keys().map(ToString::to_string).collect::<Vec<_>>(),
            ),
            Column::new(
                "cohort_size".into(),
                funnel.values().map(|entry| entry.0).collect::<Vec<_>>(),
            ),
            Column::new(
                "reached_5".into(),
                funnel.values().map(|entry| entry.1).collect::<Vec<_>>(),
            ),
            Column::new(
                "reached_25".into(),
                funnel.values().map(|entry| entry.2).collect::<Vec<_>>(),
            ),
            Column::new(
                "reached_100".into(),
                funnel.values().map(|entry| entry.3).collect::<Vec<_>>(),
            ),
        ])
        .map_err(Into::into)
    }

    fn external_cohort_frame(
        initial_sizes: &BTreeMap<i32, u32>,
        ended_by: &HashMap<(i32, i32), u32>,
        all_years: &std::collections::BTreeSet<i32>,
    ) -> Result<DataFrame> {
        let mut cohort_years_out = Vec::new();
        let mut years_out = Vec::new();
        let mut survived_out = Vec::new();
        let mut initial_out = Vec::new();
        for (&cohort_year, &initial) in initial_sizes {
            let mut survivors = 0_u32;
            let mut cohort_rows = Vec::new();
            for &year in all_years.iter().rev() {
                if year < cohort_year {
                    continue;
                }
                survivors = survivors
                    .checked_add(ended_by.get(&(cohort_year, year)).copied().unwrap_or(0))
                    .context("lifecycle cohort survivor overflow")?;
                cohort_rows.push((year, survivors));
            }
            cohort_rows.reverse();
            for (year, survived) in cohort_rows {
                cohort_years_out.push(cohort_year.to_string());
                years_out.push(year.to_string());
                survived_out.push(survived);
                initial_out.push(initial);
            }
        }
        DataFrame::new_infer_height(vec![
            Column::new("cohort_year".into(), cohort_years_out),
            Column::new("year".into(), years_out),
            Column::new("survived_editors".into(), survived_out),
            Column::new("initial_editors".into(), initial_out),
        ])
        .map_err(Into::into)
    }

    fn external_churn_frame(
        period_type: &'static str,
        aggregate: &ExternalPeriodAggregate,
    ) -> Result<DataFrame> {
        let periods_out: Vec<String> = aggregate
            .active
            .keys()
            .map(|period| format_period_key(*period, period_type))
            .collect();
        let active: Vec<u32> = aggregate.active.values().copied().collect();
        let arrivals: Vec<u32> = aggregate
            .active
            .keys()
            .map(|period| aggregate.arrivals.get(period).copied().unwrap_or(0))
            .collect();
        let departures: Vec<u32> = aggregate
            .active
            .keys()
            .map(|period| aggregate.departures.get(period).copied().unwrap_or(0))
            .collect();
        let arrival_rate: Vec<f64> = arrivals
            .iter()
            .zip(&active)
            .map(|(&arrivals, &active)| arrivals as f64 / active as f64)
            .collect();
        let departure_rate: Vec<f64> = departures
            .iter()
            .zip(&active)
            .map(|(&departures, &active)| departures as f64 / active as f64)
            .collect();
        DataFrame::new_infer_height(vec![
            Column::new("period".into(), periods_out),
            Column::new("active_editors".into(), active),
            Column::new("arrivals".into(), arrivals),
            Column::new("departures".into(), departures),
            Column::new(
                "period_type".into(),
                vec![period_type; aggregate.active.len()],
            ),
            Column::new(
                "period_months".into(),
                vec![period_months_for_type(period_type); aggregate.active.len()],
            ),
            Column::new("arrival_rate".into(), arrival_rate),
            Column::new("departure_rate".into(), departure_rate),
        ])
        .map_err(Into::into)
    }

    fn write_external_lifecycle_outputs(
        wiki: &str,
        output_dir: &Path,
        aggregate: ExternalLifecycleAggregate,
    ) -> Result<()> {
        let mut funnel = external_funnel_frame(&aggregate.funnel)?;
        add_wiki_column(&mut funnel, wiki)?;
        write_output(&mut funnel, wiki, "business_funnel", output_dir)?;

        let mut cohorts = external_cohort_frame(
            &aggregate.initial_sizes,
            &aggregate.ended_by,
            &aggregate.all_years,
        )?;
        add_wiki_column(&mut cohorts, wiki)?;
        write_output(&mut cohorts, wiki, "labor_cohorts", output_dir)?;

        let mut churn = concat_frames(vec![
            external_churn_frame("month", &aggregate.month)?,
            external_churn_frame("quarter", &aggregate.quarter)?,
            external_churn_frame("year", &aggregate.year)?,
        ])?;
        add_wiki_column(&mut churn, wiki)?;
        write_output(&mut churn, wiki, "labor_churn", output_dir)
    }
}

#[cfg(not(coverage))]
pub(super) use external::compute_external;

#[cfg(coverage)]
pub(super) fn compute_external<F>(
    _wiki: &str,
    _output_dir: &Path,
    _partitions: &[storage::PartitionSpec],
    _load_partition: F,
) -> Result<usize>
where
    F: FnMut(&[std::path::PathBuf]) -> Result<DataFrame>,
{
    anyhow::bail!("external lifecycle computation is disabled in coverage builds")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cohort_output_skips_null_years() -> Result<()> {
        let editor_spans = DataFrame::new_infer_height(vec![
            Column::new(
                "cohort_year".into(),
                vec![Some(2024_i32), None, Some(2025), None, Some(2025)],
            ),
            Column::new(
                "last_year".into(),
                vec![Some(2025_i32), Some(2025), None, Some(2025), Some(2025)],
            ),
        ])
        .expect("cohort fixture should be valid");

        let cohort_out = build_cohort_output(&editor_spans, &[2024, 2025])?;
        assert_eq!(cohort_out.height(), 3);
        assert_eq!(
            cohort_out.column("cohort_year")?.str()?.get(0),
            Some("2024")
        );
        assert_eq!(
            cohort_out.column("cohort_year")?.str()?.get(2),
            Some("2025")
        );
        assert_eq!(cohort_out.column("year")?.str()?.get(2), Some("2025"));
        Ok(())
    }

    #[test]
    fn period_keys_cover_supported_and_invalid_granularities() -> Result<()> {
        assert_eq!(normalize_period_key(202401, "month")?, 202401);
        assert_eq!(normalize_period_key(202404, "quarter")?, 20242);
        assert_eq!(normalize_period_key(202401, "year")?, 2024);
        assert!(normalize_period_key(202401, "week").is_err());
        assert_eq!(format_period_key(202401, "month"), "2024-01");
        assert_eq!(format_period_key(20242, "quarter"), "2024-Q2");
        assert_eq!(format_period_key(2024, "year"), "2024");
        assert_eq!(format_period_key(202401, "week"), "202401");
        Ok(())
    }

    #[cfg(not(coverage))]
    #[test]
    fn external_merge_matches_editor_lifecycle_semantics() -> Result<()> {
        let root = env::temp_dir().join(format!(
            "wiki-econ-lifecycle-external-{}-{}",
            std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
        ));
        fs::create_dir_all(&root)?;
        let wiki = "testwiki";
        let partitions = vec![
            storage::PartitionSpec {
                year: 2024,
                year_month: "2024-01".to_string(),
                dir: root.join("2024-01"),
                files: Vec::new(),
            },
            storage::PartitionSpec {
                year: 2025,
                year_month: "2025-01".to_string(),
                dir: root.join("2025-01"),
                files: Vec::new(),
            },
        ];
        let jan = DataFrame::new_infer_height(vec![
            Column::new(
                "user_type".into(),
                ["registered", "registered", "registered"],
            ),
            Column::new("event_user_id".into(), [1_i64, 1, 2]),
            Column::new("revision_id".into(), [11_i64, 12, 13]),
            Column::new("year".into(), [2024_i32, 2024, 2024]),
        ])?;
        let next_year = DataFrame::new_infer_height(vec![
            Column::new(
                "user_type".into(),
                [
                    "registered",
                    "registered",
                    "registered",
                    "registered",
                    "registered",
                    "registered",
                    "registered",
                    "registered",
                ],
            ),
            Column::new("event_user_id".into(), [1_i64, 1, 1, 3, 3, 3, 3, 3]),
            Column::new("revision_id".into(), [21_i64, 22, 23, 24, 25, 26, 27, 28]),
            Column::new("year".into(), [2025_i32; 8]),
        ])?;
        let mut frames = vec![jan, next_year];
        compute_external(wiki, &root, &partitions, move |_| Ok(frames.remove(0)))?;

        let funnel =
            ParquetReader::new(File::open(root.join(wiki).join("business_funnel.parquet"))?)
                .finish()?;
        assert_eq!(funnel.height(), 2);
        assert_eq!(
            funnel
                .column("cohort_size")?
                .u32()?
                .into_no_null_iter()
                .collect::<Vec<_>>(),
            vec![2, 1]
        );
        assert_eq!(
            funnel
                .column("reached_5")?
                .u32()?
                .into_no_null_iter()
                .collect::<Vec<_>>(),
            vec![1, 1]
        );

        let cohorts =
            ParquetReader::new(File::open(root.join(wiki).join("labor_cohorts.parquet"))?)
                .finish()?;
        assert!(cohorts.height() >= 3);
        let churn = ParquetReader::new(File::open(root.join(wiki).join("labor_churn.parquet"))?)
            .finish()?;
        assert_eq!(
            churn
                .column("period_type")?
                .str()?
                .unique()?
                .iter()
                .flatten()
                .count(),
            3
        );
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[cfg(coverage)]
    #[test]
    fn coverage_external_stub_fails_closed() {
        let result = compute_external("testwiki", Path::new("."), &[], |_| unreachable!());
        assert!(result.is_err());
    }
}
