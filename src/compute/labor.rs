use anyhow::Result;
use polars::prelude::*;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;
use tracing::debug;

use super::write_output;

pub(crate) fn normalize_period_key(year_month_key: i32, period_type: &str) -> Result<i32> {
    let year = year_month_key / 100;
    let month = year_month_key % 100;

    match period_type {
        "month" => Ok(year_month_key),
        "quarter" => Ok(year * 10 + ((month - 1) / 3) + 1),
        "year" => Ok(year),
        _ => anyhow::bail!("unsupported period type: {period_type}"),
    }
}

pub(crate) fn format_period_key(period_key: i32, period_type: &str) -> String {
    match period_type {
        "month" => format!("{}-{:02}", period_key / 100, period_key % 100),
        "quarter" => format!("{}-Q{}", period_key / 10, period_key % 10),
        "year" => period_key.to_string(),
        _ => period_key.to_string(),
    }
}

pub(crate) fn build_cohort_output(
    editor_spans: &DataFrame,
    all_years: &[i32],
) -> Result<DataFrame> {
    let cohort_years = editor_spans.column("cohort_year")?.i32()?;
    let last_years = editor_spans.column("last_year")?.i32()?;

    let mut initial_sizes: BTreeMap<i32, u32> = BTreeMap::new();
    let mut ended_by: HashMap<(i32, i32), u32> = HashMap::new();

    for index in 0..editor_spans.height() {
        let Some(cohort_year) = cohort_years.get(index) else {
            continue;
        };
        let Some(last_year) = last_years.get(index) else {
            continue;
        };

        *initial_sizes.entry(cohort_year).or_insert(0) += 1;
        *ended_by.entry((cohort_year, last_year)).or_insert(0) += 1;
    }

    let mut cohort_years_out: Vec<String> = Vec::new();
    let mut years_out: Vec<String> = Vec::new();
    let mut survived_out: Vec<u32> = Vec::new();
    let mut initial_out: Vec<u32> = Vec::new();

    for (&cohort_year, &initial) in &initial_sizes {
        let mut survivors = 0_u32;
        let mut cohort_rows: Vec<(i32, u32)> = Vec::new();

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

/// Compute churn (arrivals, departures, rates) for a given time granularity.
/// `editor_months` must have columns: event_user_id, year_month_key, edits.
fn churn_for_granularity(editor_months: &DataFrame, period_type: &str) -> Result<DataFrame> {
    let user_ids = editor_months.column("event_user_id")?.i64()?;
    let year_month_keys = editor_months.column("year_month_key")?.i32()?;

    let mut seen: HashSet<(i64, i32)> = HashSet::new();
    let mut active: BTreeMap<i32, u32> = BTreeMap::new();
    let mut spans: HashMap<i64, (i32, i32)> = HashMap::new();

    for index in 0..editor_months.height() {
        let (Some(user_id), Some(year_month_key)) =
            (user_ids.get(index), year_month_keys.get(index))
        else {
            continue;
        };

        let period_key = normalize_period_key(year_month_key, period_type)?;
        if !seen.insert((user_id, period_key)) {
            continue;
        }

        *active.entry(period_key).or_insert(0) += 1;

        spans
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

    let mut arrivals: HashMap<i32, u32> = HashMap::new();
    let mut departures: HashMap<i32, u32> = HashMap::new();
    for (first, last) in spans.into_values() {
        *arrivals.entry(first).or_insert(0) += 1;
        *departures.entry(last).or_insert(0) += 1;
    }

    let period_keys: Vec<i32> = active.keys().copied().collect();
    let periods: Vec<String> = period_keys
        .iter()
        .map(|period_key| format_period_key(*period_key, period_type))
        .collect();
    let active_editors: Vec<u32> = period_keys
        .iter()
        .map(|period_key| active[period_key])
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
    let period_count = periods.len();

    DataFrame::new_infer_height(vec![
        Column::new("period".into(), periods),
        Column::new("active_editors".into(), active_editors),
        Column::new("arrivals".into(), arrivals_out),
        Column::new("departures".into(), departures_out),
        Column::new("period_type".into(), vec![period_type; period_count]),
        Column::new("arrival_rate".into(), arrival_rate),
        Column::new("departure_rate".into(), departure_rate),
    ])
    .map_err(Into::into)
}

/// Compute labor market metrics: participation, churn, cohort survival.
pub fn compute(wiki: &str, base: &DataFrame, output_dir: &Path) -> Result<()> {
    debug!(wiki = wiki, "computing labor metrics");

    // --- 1. Monthly workforce stats ---
    let monthly = base
        .clone()
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
        .sort(
            ["year_month", "page_namespace"],
            SortMultipleOptions::default(),
        )
        .collect()?;

    let mut monthly_out = monthly.clone();
    let wiki_col = Column::new("wiki".into(), vec![wiki; monthly_out.height()]);
    monthly_out.with_column(wiki_col)?;
    write_output(&mut monthly_out, wiki, "labor_monthly", output_dir)?;

    // --- 2. Cohort survival (yearly) ---
    let editor_years = base
        .clone()
        .lazy()
        .filter(col("user_type").eq(lit("registered")))
        .group_by([col("event_user_id"), col("year")])
        .agg([col("revision_id").count().alias("edits")])
        .collect()?;

    let editor_spans = editor_years
        .clone()
        .lazy()
        .group_by([col("event_user_id")])
        .agg([
            col("year").min().alias("cohort_year"),
            col("year").max().alias("last_year"),
        ])
        .collect()?;

    let all_years: Vec<i32> = editor_years
        .column("year")?
        .unique()?
        .sort(Default::default())?
        .i32()?
        .into_iter()
        .flatten()
        .collect();

    let mut cohort_out = build_cohort_output(&editor_spans, &all_years)?;
    let wiki_col = Column::new("wiki".into(), vec![wiki; cohort_out.height()]);
    cohort_out.with_column(wiki_col)?;
    write_output(&mut cohort_out, wiki, "labor_cohorts", output_dir)?;

    // --- 3. Churn at multiple granularities (registered editors only) ---
    let editor_months = base
        .clone()
        .lazy()
        .filter(col("user_type").eq(lit("registered")))
        .group_by([col("event_user_id"), col("year_month_key")])
        .agg([col("revision_id").count().alias("edits")])
        .collect()?;

    let churn_monthly = churn_for_granularity(&editor_months, "month")?;
    let churn_quarterly = churn_for_granularity(&editor_months, "quarter")?;
    let churn_yearly = churn_for_granularity(&editor_months, "year")?;

    let churn_frames = [
        churn_monthly.lazy(),
        churn_quarterly.lazy(),
        churn_yearly.lazy(),
    ];
    let mut churn = concat(churn_frames, Default::default())?.collect()?;

    let wiki_col = Column::new("wiki".into(), vec![wiki; churn.height()]);
    churn.with_column(wiki_col)?;
    write_output(&mut churn, wiki, "labor_churn", output_dir)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{build_cohort_output, churn_for_granularity, format_period_key};
    use polars::prelude::*;

    #[test]
    fn churn_deduplicates_editor_periods() -> anyhow::Result<()> {
        let columns = vec![
            Column::new("event_user_id".into(), vec![1_i64, 1, 2]),
            Column::new("year_month_key".into(), vec![202401_i32, 202402, 202402]),
            Column::new("edits".into(), vec![2_u32, 3, 1]),
        ];
        let editor_months = DataFrame::new_infer_height(columns)?;

        let churn = churn_for_granularity(&editor_months, "quarter")?;
        let arrivals = churn.column("arrivals")?.u32()?.get(0);
        let active = churn.column("active_editors")?.u32()?.get(0);

        assert_eq!(arrivals, Some(2));
        assert_eq!(active, Some(2));
        Ok(())
    }

    #[test]
    fn churn_handles_month_and_year_with_null_rows() -> anyhow::Result<()> {
        let columns = vec![
            Column::new(
                "event_user_id".into(),
                vec![Some(1_i64), Some(1), Some(2), Some(2), None],
            ),
            Column::new(
                "year_month_key".into(),
                vec![
                    Some(202403_i32),
                    Some(202401),
                    Some(202502),
                    Some(202504),
                    Some(202404),
                ],
            ),
            Column::new("edits".into(), vec![1_u32, 1, 1, 1, 1]),
        ];
        let editor_months = DataFrame::new_infer_height(columns)?;

        let monthly = churn_for_granularity(&editor_months, "month")?;
        let yearly = churn_for_granularity(&editor_months, "year")?;

        assert_eq!(monthly.height(), 4);
        assert_eq!(yearly.height(), 2);
        assert_eq!(yearly.column("period")?.str()?.get(0), Some("2024"));
        assert_eq!(yearly.column("period")?.str()?.get(1), Some("2025"));
        Ok(())
    }

    #[test]
    fn churn_rejects_unknown_period_type() -> anyhow::Result<()> {
        let columns = vec![
            Column::new("event_user_id".into(), vec![1_i64]),
            Column::new("year_month_key".into(), vec![202401_i32]),
            Column::new("edits".into(), vec![1_u32]),
        ];
        let editor_months = DataFrame::new_infer_height(columns)?;

        let err = churn_for_granularity(&editor_months, "week").expect_err("invalid period type");
        assert!(err.to_string().contains("unsupported period type"));
        Ok(())
    }

    #[test]
    fn cohort_output_skips_null_years() -> anyhow::Result<()> {
        let columns = vec![
            Column::new(
                "cohort_year".into(),
                vec![Some(2024_i32), None, Some(2025), None, Some(2025)],
            ),
            Column::new(
                "last_year".into(),
                vec![Some(2025_i32), Some(2025), None, Some(2025), Some(2025)],
            ),
        ];
        let editor_spans = DataFrame::new_infer_height(columns)?;

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
    fn format_period_key_falls_back_for_unknown_type() {
        assert_eq!(format_period_key(202401, "week"), "202401");
    }
}
