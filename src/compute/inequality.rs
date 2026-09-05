use anyhow::{Context, Result};
use polars::prelude::*;
use std::collections::BTreeMap;
use std::path::Path;
use tracing::debug;

use super::{
    activity::ActivityPeriod, editor_identity_available_expr, editor_identity_expr,
    ensure_editor_identity_inputs, user_type_from_rank_expr, user_type_rank_expr, write_output,
};

type InequalityRow = (
    String,
    String,
    String,
    String,
    String,
    u32,
    String,
    f64,
    f64,
    f64,
    u32,
    u32,
    u32,
);

/// Compute Gini coefficient from a sorted array of values.
fn gini_from_sorted(values: &[f64]) -> f64 {
    let n = values.len();
    if n < 2 {
        return 0.0;
    }
    let total: f64 = values.iter().sum();
    if total == 0.0 {
        return 0.0;
    }
    let weighted_sum: f64 = values
        .iter()
        .enumerate()
        .map(|(i, v)| (i as f64 + 1.0) * v)
        .sum();
    (2.0 * weighted_sum) / (n as f64 * total) - (n as f64 + 1.0) / n as f64
}

/// Compute Theil T index (GE(1)) — decomposable inequality measure.
fn theil_from_values(values: &[f64]) -> f64 {
    let n = values.len() as f64;
    if n < 2.0 {
        return 0.0;
    }
    let mean = values.iter().sum::<f64>() / n;
    if mean == 0.0 {
        return 0.0;
    }
    values
        .iter()
        .filter(|&&v| v > 0.0)
        .map(|&v| {
            let ratio = v / mean;
            ratio * ratio.ln()
        })
        .sum::<f64>()
        / n
}

/// Compute Palma ratio: share of top 10% / share of bottom 40%.
fn palma_from_sorted(values: &[f64]) -> f64 {
    let n = values.len();
    if n < 10 {
        return 0.0;
    }
    let total: f64 = values.iter().sum();
    if total == 0.0 {
        return 0.0;
    }
    let bottom_40_end = (n as f64 * 0.4) as usize;
    let top_10_start = n - (n as f64 * 0.1) as usize;
    let bottom_40: f64 = values[..bottom_40_end].iter().sum();
    let top_10: f64 = values[top_10_start..].iter().sum();
    if bottom_40 == 0.0 {
        return f64::INFINITY;
    }
    top_10 / bottom_40
}

/// Minimum number of editors to reach 50% of edits (fragility index).
fn min_editors_50pct(sorted_desc: &[f64]) -> usize {
    let total: f64 = sorted_desc.iter().sum();
    if total == 0.0 {
        return 0;
    }
    let mut cumsum = 0.0;
    for (i, &v) in sorted_desc.iter().enumerate() {
        cumsum += v;
        if cumsum >= total * 0.5 {
            return i + 1;
        }
    }
    sorted_desc.len()
}

pub(super) fn editor_month_frame(base: &DataFrame) -> Result<DataFrame> {
    let month_key = if base.column("year_month_key").is_ok() {
        col("year_month_key")
    } else {
        col("year_month")
            .str()
            .slice(lit(0), lit(4))
            .cast(DataType::Int32)
            * lit(100_i32)
            + col("year_month")
                .str()
                .slice(lit(5), lit(2))
                .cast(DataType::Int32)
    };
    ensure_editor_identity_inputs(base)?
        .lazy()
        .filter(
            editor_identity_available_expr()
                .and(col("year_month").is_not_null())
                .and(col("user_type").is_not_null()),
        )
        .with_column(month_key.alias("year_month_key"))
        .group_by([
            col("year_month"),
            col("year_month_key"),
            editor_identity_expr().alias("editor_identity"),
        ])
        .agg([
            user_type_rank_expr().max().alias("user_type_rank"),
            col("revision_id").count().alias("edits"),
        ])
        .with_column(user_type_from_rank_expr())
        .collect()
        .map_err(Into::into)
}

fn compute_period_frame(editor_monthly: &DataFrame, period: ActivityPeriod) -> Result<DataFrame> {
    let period_name = period.name();
    let input_edits = editor_monthly
        .column("edits")?
        .cast(&DataType::UInt64)?
        .u64()?
        .sum()
        .unwrap_or(0);
    let editor_period = editor_monthly
        .clone()
        .lazy()
        .with_column(period.key_expr().alias("period_key"))
        .group_by([col("period_key"), col("editor_identity")])
        .agg([
            col("user_type_rank").max().alias("user_type_rank"),
            col("edits").sum().alias("edits"),
        ])
        .with_column(user_type_from_rank_expr())
        .collect()?;

    let result_rows = rows_from_editor_period(&editor_period, period)?;

    let output_edits = result_rows.iter().map(|row| u64::from(row.12)).sum::<u64>();
    anyhow::ensure!(
        input_edits == output_edits,
        "{period_name} inequality edit conservation failed: input={input_edits}, output={output_edits}"
    );

    rows_to_frame(&result_rows)
}

fn rows_from_editor_period(
    editor_period: &DataFrame,
    period: ActivityPeriod,
) -> Result<Vec<InequalityRow>> {
    let period_name = period.name();
    let period_months = period.months();
    let period_keys = editor_period.column("period_key")?.i32()?;
    let user_types = editor_period.column("user_type")?.str()?;
    let edits = editor_period
        .column("edits")?
        .cast(&DataType::UInt64)?
        .u64()?
        .clone();
    let mut grouped: BTreeMap<(i32, String), Vec<f64>> = BTreeMap::new();

    for idx in 0..editor_period.height() {
        let (Some(period_key), Some(user_type), Some(edit_count)) =
            (period_keys.get(idx), user_types.get(idx), edits.get(idx))
        else {
            continue;
        };

        grouped
            .entry((period_key, user_type.to_string()))
            .or_default()
            .push(edit_count as f64);
    }

    let mut result_rows = Vec::new();
    for ((period_key, user_type), mut values) in grouped {
        values.sort_by(|a, b| a.partial_cmp(b).unwrap());

        let gini = gini_from_sorted(&values);
        let theil = theil_from_values(&values);
        let palma = palma_from_sorted(&values);

        values.reverse();
        let fragility = min_editors_50pct(&values);
        let total_editors = values.len();
        let total_edits: f64 = values.iter().sum();
        let (period_value, period_start, period_end) = period.fields(period_key)?;

        result_rows.push((
            period_start.clone(),
            period_value,
            period_start,
            period_end,
            period_name.to_string(),
            period_months,
            user_type,
            gini,
            theil,
            palma,
            u32::try_from(fragility)?,
            u32::try_from(total_editors)?,
            u32::try_from(total_edits as u64)
                .with_context(|| format!("{period_name} inequality edit total exceeds u32"))?,
        ));
    }

    Ok(result_rows)
}

fn rows_to_frame(result_rows: &[InequalityRow]) -> Result<DataFrame> {
    let columns = vec![
        Column::new(
            "year_month".into(),
            result_rows
                .iter()
                .map(|row| row.0.as_str())
                .collect::<Vec<_>>(),
        ),
        Column::new(
            "period".into(),
            result_rows
                .iter()
                .map(|row| row.1.as_str())
                .collect::<Vec<_>>(),
        ),
        Column::new(
            "period_start".into(),
            result_rows
                .iter()
                .map(|row| row.2.as_str())
                .collect::<Vec<_>>(),
        ),
        Column::new(
            "period_end".into(),
            result_rows
                .iter()
                .map(|row| row.3.as_str())
                .collect::<Vec<_>>(),
        ),
        Column::new(
            "period_type".into(),
            result_rows
                .iter()
                .map(|row| row.4.as_str())
                .collect::<Vec<_>>(),
        ),
        Column::new(
            "period_months".into(),
            result_rows.iter().map(|row| row.5).collect::<Vec<_>>(),
        ),
        Column::new(
            "user_type".into(),
            result_rows
                .iter()
                .map(|row| row.6.as_str())
                .collect::<Vec<_>>(),
        ),
        Column::new(
            "gini".into(),
            result_rows.iter().map(|row| row.7).collect::<Vec<_>>(),
        ),
        Column::new(
            "theil".into(),
            result_rows.iter().map(|row| row.8).collect::<Vec<_>>(),
        ),
        Column::new(
            "palma".into(),
            result_rows.iter().map(|row| row.9).collect::<Vec<_>>(),
        ),
        Column::new(
            "min_editors_50pct".into(),
            result_rows.iter().map(|row| row.10).collect::<Vec<_>>(),
        ),
        Column::new(
            "total_editors".into(),
            result_rows.iter().map(|row| row.11).collect::<Vec<_>>(),
        ),
        Column::new(
            "total_edits".into(),
            result_rows.iter().map(|row| row.12).collect::<Vec<_>>(),
        ),
    ];
    DataFrame::new_infer_height(columns).map_err(Into::into)
}

pub(super) fn compute_periods(editor_monthly: &DataFrame) -> Result<DataFrame> {
    let frames = vec![
        compute_period_frame(editor_monthly, ActivityPeriod::Month)?,
        compute_period_frame(editor_monthly, ActivityPeriod::Quarter)?,
        compute_period_frame(editor_monthly, ActivityPeriod::Year)?,
    ];
    let mut result = super::concat_frames(frames)?;
    result = result.sort(["period", "period_type", "user_type"], Default::default())?;
    Ok(result)
}

pub fn compute_frame(base: &DataFrame) -> Result<DataFrame> {
    compute_periods(&editor_month_frame(base)?)
}

/// Compute exact editor distributions for calendar months, quarters, and years.
/// All namespaces are combined before each period-level distribution is ranked.
pub fn compute(wiki: &str, base: &DataFrame, output_dir: &Path) -> Result<()> {
    debug!(wiki = wiki, "computing inequality metrics");

    let mut result = compute_frame(base)?;

    let wiki_col = Column::new("wiki".into(), vec![wiki; result.height()]);
    result.with_column(wiki_col)?;

    write_output(&mut result, wiki, "inequality", output_dir)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::TestDir;

    #[test]
    fn gini_handles_simple_distribution() {
        let values = vec![1.0, 1.0, 3.0];
        let gini = gini_from_sorted(&values);
        assert!(gini > 0.2 && gini < 0.3);
    }

    #[test]
    fn palma_uses_tail_shares() {
        let values = vec![1.0; 10];
        assert_eq!(palma_from_sorted(&values), 0.25);
    }

    #[test]
    fn inequality_helpers_cover_edge_cases() {
        assert_eq!(gini_from_sorted(&[]), 0.0);
        assert_eq!(gini_from_sorted(&[0.0, 0.0]), 0.0);
        assert_eq!(theil_from_values(&[1.0]), 0.0);
        assert_eq!(theil_from_values(&[0.0, 0.0]), 0.0);
        assert_eq!(palma_from_sorted(&[0.0; 10]), 0.0);
        assert!(
            palma_from_sorted(&[0.0, 0.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0]).is_infinite()
        );
        assert_eq!(min_editors_50pct(&[0.0, 0.0]), 0);
        assert_eq!(min_editors_50pct(&[f64::NAN]), 1);
    }

    #[test]
    fn compute_skips_null_groups_and_writes_output() -> Result<()> {
        let output_dir = TestDir::new()?;
        let columns = vec![
            Column::new(
                "year_month".into(),
                vec![Some("2024-01"), Some("2024-01"), None],
            ),
            Column::new(
                "user_type".into(),
                vec![Some("registered"), Some("registered"), Some("registered")],
            ),
            Column::new(
                "event_user_id".into(),
                vec![Some(1_i64), Some(2_i64), Some(3_i64)],
            ),
            Column::new("revision_id".into(), vec![10_i64, 11, 12]),
        ];
        let base = DataFrame::new_infer_height(columns)?;

        compute("testwiki", &base, output_dir.path())?;

        let result_path = output_dir
            .path()
            .join("testwiki")
            .join("inequality.parquet");
        let result_path = result_path.to_string_lossy().to_string();
        let result =
            LazyFrame::scan_parquet(result_path.as_str().into(), Default::default())?.collect()?;

        assert_eq!(result.height(), 3);
        let monthly = result
            .lazy()
            .filter(col("period_type").eq(lit("month")))
            .collect()?;
        assert_eq!(monthly.column("year_month")?.str()?.get(0), Some("2024-01"));
        assert_eq!(monthly.column("total_editors")?.u32()?.get(0), Some(2));
        Ok(())
    }

    #[test]
    fn period_reduction_skips_null_group_keys_and_counts() -> Result<()> {
        let editor_period = DataFrame::new_infer_height(vec![
            Column::new("period_key".into(), [Some(202401_i32), None, Some(202401)]),
            Column::new(
                "user_type".into(),
                [Some("registered"), Some("registered"), None],
            ),
            Column::new("edits".into(), [Some(2_u32), Some(3), Some(4)]),
        ])
        .expect("null-key inequality fixture should be valid");
        let result = rows_from_editor_period(&editor_period, ActivityPeriod::Month)?;
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].11, 1);
        assert_eq!(result[0].12, 2);
        Ok(())
    }

    #[test]
    fn quarter_and_year_count_repeated_editors_once() -> Result<()> {
        let base = df!(
            "year_month" => &["2024-01", "2024-01", "2024-02", "2024-02"],
            "user_type" => &["registered", "registered", "registered", "registered"],
            "event_user_id" => &[1_i64, 1, 1, 2],
            "revision_id" => &[10_i64, 11, 12, 13],
        )
        .expect("repeated-editor inequality fixture should be valid");

        let result = compute_frame(&base)?;
        for period_type in ["quarter", "year"] {
            let period = result
                .clone()
                .lazy()
                .filter(col("period_type").eq(lit(period_type)))
                .collect()?;
            assert_eq!(period.height(), 1);
            assert_eq!(period.column("total_editors")?.u32()?.get(0), Some(2));
            assert_eq!(period.column("total_edits")?.u32()?.get(0), Some(4));
            assert!((period.column("gini")?.f64()?.get(0).unwrap() - 0.25).abs() < 1e-12);
        }
        Ok(())
    }

    #[test]
    fn period_user_types_are_disjoint_when_bot_status_changes() -> Result<()> {
        let base = df!(
            "year_month" => &["2024-01", "2024-01", "2024-01"],
            "user_type" => &["registered", "bot", "registered"],
            "event_user_id" => &[1_i64, 1, 2],
            "revision_id" => &[10_i64, 11, 12],
        )
        .expect("user-type inequality fixture should be valid");
        let monthly = compute_frame(&base)?
            .lazy()
            .filter(col("period_type").eq(lit("month")))
            .collect()?;
        assert_eq!(monthly.column("total_editors")?.u32()?.sum(), Some(2));
        let bot = monthly
            .lazy()
            .filter(col("user_type").eq(lit("bot")))
            .collect()?;
        assert_eq!(bot.column("total_editors")?.u32()?.get(0), Some(1));
        assert_eq!(bot.column("total_edits")?.u32()?.get(0), Some(2));
        Ok(())
    }
}
