use anyhow::Result;
use polars::prelude::*;
use std::path::Path;
use tracing::debug;

use super::write_output;

/// Compute GDP-style metrics: output, productivity, sectoral breakdown.
pub fn compute(wiki: &str, base: &DataFrame, output_dir: &Path) -> Result<()> {
    debug!(wiki = wiki, "computing gdp metrics");

    let base = base.clone().lazy();

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
            col("event_user_id").n_unique().alias("unique_editors"),
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
            col("event_user_id").n_unique().alias("editors"),
        ])
        .sort(["year_month", "user_type"], SortMultipleOptions::default())
        .collect()?;

    let mut type_out = type_share;
    let wiki_col = Column::new("wiki".into(), vec![wiki; type_out.height()]);
    type_out.with_column(wiki_col)?;
    write_output(&mut type_out, wiki, "gdp_user_type_share", output_dir)?;

    // --- 3. Activity tier breakdown ---
    // Per editor per month: count edits, classify into tier, then aggregate
    let tiers = base
        .clone()
        .group_by([col("year_month"), col("user_type"), col("event_user_id")])
        .agg([
            col("revision_id").count().alias("edits"),
            col("revision_text_bytes_diff").sum().alias("net_bytes"),
            col("revision_text_bytes_diff")
                .filter(col("revision_text_bytes_diff").gt(lit(0i64)))
                .sum()
                .alias("gross_bytes"),
        ])
        .with_column(
            when(col("edits").eq(lit(1)))
                .then(lit("1 edit"))
                .when(col("edits").lt(lit(5)))
                .then(lit("2-4 edits"))
                .when(col("edits").lt(lit(25)))
                .then(lit("5-24 edits"))
                .when(col("edits").lt(lit(100)))
                .then(lit("25-99 edits"))
                .otherwise(lit("100+ edits"))
                .alias("activity_tier"),
        )
        .group_by([col("year_month"), col("user_type"), col("activity_tier")])
        .agg([
            col("event_user_id").n_unique().alias("editors"),
            col("edits").sum().alias("total_edits"),
            col("net_bytes").sum().alias("net_bytes"),
            col("gross_bytes").sum().alias("gross_bytes"),
        ])
        .sort(
            ["year_month", "user_type", "activity_tier"],
            SortMultipleOptions::default(),
        )
        .collect()?;

    let mut tier_out = tiers;
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
