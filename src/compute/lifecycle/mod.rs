//! Stateful editor lifecycle accumulators, checkpoints, and output assembly.

/// Semantic version for stateful editor lifecycle metrics.
pub(crate) const ALGORITHM_VERSION: &str =
    "editor-lifecycle-v3-explicit-identified-registered-editors";

use super::{add_wiki_column, concat_frames, labor, write_output};
use crate::{metric_registry::MetricFamily, storage};
use anyhow::Result;
use polars::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::File;
use std::path::Path;

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
            self.churn_quarter.observe(
                user_id,
                labor::normalize_period_key(year_month_key, "quarter")?,
            );
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
            self.churn_quarter.observe(
                user_id,
                labor::normalize_period_key(year_month_key, "quarter")?,
            );
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
    let mut cohort_out = labor::build_cohort_output(&editor_spans, &all_years)?;
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
