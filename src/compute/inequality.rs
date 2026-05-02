use anyhow::Result;
use polars::prelude::*;
use std::collections::BTreeMap;
use std::path::Path;
use tracing::debug;

use super::write_output;

type InequalityRow = (String, String, f64, f64, f64, usize, usize, usize);

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

pub fn compute_frame(base: &DataFrame) -> Result<DataFrame> {
    let editor_monthly = base
        .clone()
        .lazy()
        .group_by([col("year_month"), col("user_type"), col("event_user_id")])
        .agg([col("revision_id").count().alias("edits")])
        .collect()?;

    let mut result_rows: Vec<InequalityRow> = Vec::new();
    let year_months = editor_monthly.column("year_month")?.str()?;
    let user_types = editor_monthly.column("user_type")?.str()?;
    let edits = editor_monthly.column("edits")?.u32()?;
    let mut grouped: BTreeMap<(String, String), Vec<f64>> = BTreeMap::new();

    for idx in 0..editor_monthly.height() {
        let (Some(month), Some(user_type), Some(edit_count)) =
            (year_months.get(idx), user_types.get(idx), edits.get(idx))
        else {
            continue;
        };

        grouped
            .entry((month.to_string(), user_type.to_string()))
            .or_default()
            .push(edit_count as f64);
    }

    for ((month, user_type), mut values) in grouped {
        if values.len() < 2 {
            continue;
        }

        values.sort_by(|a, b| a.partial_cmp(b).unwrap());

        let gini = gini_from_sorted(&values);
        let theil = theil_from_values(&values);
        let palma = palma_from_sorted(&values);

        values.reverse();
        let fragility = min_editors_50pct(&values);
        let total_editors = values.len();
        let total_edits: f64 = values.iter().sum();

        result_rows.push((
            month,
            user_type,
            gini,
            theil,
            palma,
            fragility,
            total_editors,
            total_edits as usize,
        ));
    }

    let columns = vec![
        Column::new(
            "year_month".into(),
            result_rows
                .iter()
                .map(|row| row.0.as_str())
                .collect::<Vec<_>>(),
        ),
        Column::new(
            "user_type".into(),
            result_rows
                .iter()
                .map(|row| row.1.as_str())
                .collect::<Vec<_>>(),
        ),
        Column::new(
            "gini".into(),
            result_rows.iter().map(|row| row.2).collect::<Vec<_>>(),
        ),
        Column::new(
            "theil".into(),
            result_rows.iter().map(|row| row.3).collect::<Vec<_>>(),
        ),
        Column::new(
            "palma".into(),
            result_rows.iter().map(|row| row.4).collect::<Vec<_>>(),
        ),
        Column::new(
            "min_editors_50pct".into(),
            result_rows
                .iter()
                .map(|row| row.5 as u32)
                .collect::<Vec<_>>(),
        ),
        Column::new(
            "total_editors".into(),
            result_rows
                .iter()
                .map(|row| row.6 as u32)
                .collect::<Vec<_>>(),
        ),
        Column::new(
            "total_edits".into(),
            result_rows
                .iter()
                .map(|row| row.7 as u32)
                .collect::<Vec<_>>(),
        ),
    ];
    DataFrame::new_infer_height(columns).map_err(Into::into)
}

/// Compute all inequality metrics, grouped by year-month.
/// We aggregate across all namespaces per month to keep the output manageable,
/// with a separate per-namespace breakdown.
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

        assert_eq!(result.height(), 1);
        assert_eq!(result.column("year_month")?.str()?.get(0), Some("2024-01"));
        assert_eq!(result.column("total_editors")?.u32()?.get(0), Some(2));
        Ok(())
    }
}
