use anyhow::Result;
use polars::prelude::*;
use std::path::Path;
use tracing::debug;

use super::{
    editor_identity_available_expr, editor_identity_expr, ensure_editor_identity_inputs,
    write_output,
};

/// Compute GDP-style metrics: output, productivity, sectoral breakdown.
pub fn compute(wiki: &str, base: &DataFrame, output_dir: &Path) -> Result<()> {
    debug!(wiki = wiki, "computing gdp metrics");

    let base = ensure_editor_identity_inputs(base)?.lazy();

    // --- 1. Monthly GDP by namespace (sector) ---
    let monthly_gdp = base
        .clone()
        .group_by([col("year_month"), col("page_namespace"), col("user_type")])
        .agg([
            // Gross output: total bytes added (positive diffs only)
            col("revision_text_bytes_diff")
                .filter(col("revision_text_bytes_diff").gt(lit(0i64)))
                .sum()
                .alias("gross_bytes_added"),
            // Total bytes diff (net — includes removals)
            col("revision_text_bytes_diff").sum().alias("net_bytes"),
            // Total edits
            col("revision_id").count().alias("total_edits"),
            // Non-reverted edits
            col("is_reverted")
                .not()
                .cast(DataType::UInt32)
                .sum()
                .alias("productive_edits"),
            // Reverted edits
            col("is_reverted")
                .cast(DataType::UInt32)
                .sum()
                .alias("reverted_edits"),
            // Unique editors
            editor_identity_expr()
                .filter(editor_identity_available_expr())
                .n_unique()
                .alias("unique_editors"),
            // Minor edits
            col("is_minor")
                .cast(DataType::UInt32)
                .sum()
                .alias("minor_edits"),
        ])
        .with_columns([
            // Productivity: net bytes per edit
            (col("net_bytes").cast(DataType::Float64) / col("total_edits").cast(DataType::Float64))
                .alias("bytes_per_edit"),
            // GDP per capita: net bytes per editor
            (col("net_bytes").cast(DataType::Float64)
                / col("unique_editors").cast(DataType::Float64))
            .alias("bytes_per_editor"),
            // Revert rate
            (col("reverted_edits").cast(DataType::Float64)
                / col("total_edits").cast(DataType::Float64))
            .alias("revert_rate"),
        ])
        .sort(
            ["year_month", "page_namespace"],
            SortMultipleOptions::default(),
        )
        .collect()?;

    let mut gdp_out = monthly_gdp;
    let wiki_col = Column::new("wiki".into(), vec![wiki; gdp_out.height()]);
    gdp_out.with_column(wiki_col)?;
    write_output(&mut gdp_out, wiki, "gdp", output_dir)?;

    // --- 2. User type share of economy ---
    let type_share = base
        .clone()
        .group_by([col("year_month"), col("user_type")])
        .agg([
            col("revision_id").count().alias("edits"),
            col("revision_text_bytes_diff").sum().alias("net_bytes"),
            editor_identity_expr()
                .filter(editor_identity_available_expr())
                .n_unique()
                .alias("editors"),
        ])
        .sort(["year_month", "user_type"], SortMultipleOptions::default())
        .collect()?;

    let mut type_out = type_share;
    let wiki_col = Column::new("wiki".into(), vec![wiki; type_out.height()]);
    type_out.with_column(wiki_col)?;
    write_output(&mut type_out, wiki, "gdp_user_type_share", output_dir)?;

    // --- 3. Activity tier breakdown ---
    // Classification is recomputed at each supported period length. A power
    // editor therefore means 100+ edits/month, 300+/quarter, or 1200+/year.
    let mut tier_out = super::activity_tiers_all_periods(base.clone().collect()?)?;
    let wiki_col = Column::new("wiki".into(), vec![wiki; tier_out.height()]);
    tier_out.with_column(wiki_col)?;
    write_output(&mut tier_out, wiki, "gdp_activity_tiers", output_dir)?;

    // --- 4. Acquisition funnel: cumulative milestones per cohort year ---
    // For each registered editor, compute total lifetime edits and first-edit year.
    // Then aggregate by cohort year: what fraction reached 5+, 25+, 100+ total edits.
    let funnel = base
        .clone()
        .filter(col("user_type").eq(lit("registered")))
        .group_by([col("event_user_id")])
        .agg([
            col("revision_id").count().alias("total_edits"),
            col("year")
                .min()
                .cast(DataType::String)
                .alias("cohort_year"),
        ])
        .group_by([col("cohort_year")])
        .agg([
            col("event_user_id").count().alias("cohort_size"),
            col("total_edits")
                .gt_eq(lit(5))
                .cast(DataType::UInt32)
                .sum()
                .alias("reached_5"),
            col("total_edits")
                .gt_eq(lit(25))
                .cast(DataType::UInt32)
                .sum()
                .alias("reached_25"),
            col("total_edits")
                .gt_eq(lit(100))
                .cast(DataType::UInt32)
                .sum()
                .alias("reached_100"),
        ])
        .sort(["cohort_year"], SortMultipleOptions::default())
        .collect()?;

    let mut funnel_out = funnel;
    let wiki_col = Column::new("wiki".into(), vec![wiki; funnel_out.height()]);
    funnel_out.with_column(wiki_col)?;
    write_output(&mut funnel_out, wiki, "business_funnel", output_dir)?;

    Ok(())
}
