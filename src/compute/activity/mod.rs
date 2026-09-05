/// Semantic version for month-, quarter-, and year-scaled activity tiers.
pub(crate) const ALGORITHM_VERSION: &str = "activity-tiers-v5-exclusive-period-user-type";

use super::{
    add_wiki_column, concat_frames, editor_identity_available_expr, editor_identity_expr,
    ensure_editor_identity_inputs, ensure_editor_identity_key, sort_frame,
    user_type_from_rank_expr, user_type_rank_expr, write_output,
};
use anyhow::Result;
use polars::prelude::*;
use std::path::Path;

pub(super) fn gdp_editor_month_frame(base: &DataFrame) -> Result<DataFrame> {
    ensure_editor_identity_inputs(base)?
        .lazy()
        .filter(editor_identity_available_expr())
        .group_by([
            col("year_month"),
            col("year_month_key"),
            editor_identity_expr().alias("editor_identity"),
        ])
        .agg([
            user_type_rank_expr().max().alias("user_type_rank"),
            col("revision_id").count().alias("edits"),
            col("revision_text_bytes_diff").sum().alias("net_bytes"),
            col("revision_text_bytes_diff")
                .filter(col("revision_text_bytes_diff").gt(lit(0i64)))
                .sum()
                .alias("gross_bytes"),
        ])
        .with_column(user_type_from_rank_expr())
        .collect()
        .map_err(Into::into)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ActivityPeriod {
    Month,
    Quarter,
    Year,
}

impl ActivityPeriod {
    pub(super) fn name(self) -> &'static str {
        match self {
            Self::Month => "month",
            Self::Quarter => "quarter",
            Self::Year => "year",
        }
    }

    pub(super) fn months(self) -> u32 {
        match self {
            Self::Month => 1,
            Self::Quarter => 3,
            Self::Year => 12,
        }
    }

    pub(super) fn key_expr(self) -> Expr {
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

    pub(super) fn fields(self, key: i32) -> Result<(String, String, String)> {
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

pub(super) fn activity_tier_labels(months: u32) -> [String; 5] {
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

pub(super) const ACTIVITY_TIER_OUTPUT_COLUMNS: [&str; 13] = [
    "year_month",
    "period",
    "period_start",
    "period_end",
    "period_type",
    "period_months",
    "user_type",
    "activity_tier",
    "tier_rank",
    "editors",
    "total_edits",
    "net_bytes",
    "gross_bytes",
];

fn canonicalize_activity_tier_columns(frame: &DataFrame) -> Result<DataFrame> {
    frame
        .select(ACTIVITY_TIER_OUTPUT_COLUMNS.iter().copied())
        .map_err(Into::into)
}

pub(super) fn gdp_activity_tiers_for_period(
    editor_months: &DataFrame,
    period: ActivityPeriod,
) -> Result<DataFrame> {
    let editor_months = ensure_editor_identity_key(editor_months)?;
    let months = period.months();
    let labels = activity_tier_labels(months);
    let input_edits = editor_months
        .column("edits")?
        .cast(&DataType::Int64)?
        .i64()?
        .sum()
        .unwrap_or(0);
    let mut frame = editor_months
        .lazy()
        .with_column(period.key_expr().alias("period_key"))
        .group_by([col("period_key"), col("editor_identity")])
        .agg([
            col("user_type_rank").max().alias("user_type_rank"),
            col("edits").sum().alias("edits"),
            col("net_bytes").sum().alias("net_bytes"),
            col("gross_bytes").sum().alias("gross_bytes"),
        ])
        .with_columns([
            user_type_from_rank_expr(),
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
            col("editor_identity").n_unique().alias("editors"),
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
    let frame = sort_frame(frame, ["period", "user_type", "tier_rank"])?;
    canonicalize_activity_tier_columns(&frame)
}

pub(super) fn activity_tiers_all_periods(base: DataFrame) -> Result<DataFrame> {
    let editor_months = gdp_editor_month_frame(&base)?;
    concat_frames(vec![
        gdp_activity_tiers_for_period(&editor_months, ActivityPeriod::Month)?,
        gdp_activity_tiers_for_period(&editor_months, ActivityPeriod::Quarter)?,
        gdp_activity_tiers_for_period(&editor_months, ActivityPeriod::Year)?,
    ])
}

pub(super) fn finish_activity_year(
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

pub(super) fn finish_activity_year_cached(
    editor_month_frames: &mut Vec<DataFrame>,
    output_frames: &mut Vec<DataFrame>,
    month_digests: &mut Vec<String>,
    cache: Option<&crate::cross_snapshot::CrossSnapshotCache>,
) -> Result<()> {
    if editor_month_frames.is_empty() {
        month_digests.clear();
        return Ok(());
    }
    if let Some(cache) = cache {
        anyhow::ensure!(
            editor_month_frames.len() == month_digests.len(),
            "activity cache month inputs and identities disagree"
        );
        let digest_refs = month_digests.iter().map(String::as_str).collect::<Vec<_>>();
        let input_digest = cache.derived_digest("activity_year", ALGORITHM_VERSION, &digest_refs);
        let cached = cache.load(
            "activity_year",
            ALGORITHM_VERSION,
            &input_digest,
            "gdp_activity_tiers",
        );
        if let Some(frame) = cached? {
            editor_month_frames.clear();
            month_digests.clear();
            output_frames.push(frame);
            return Ok(());
        }
        let mut frames = Vec::new();
        finish_activity_year(editor_month_frames, &mut frames)?;
        let mut frame = concat_frames(frames)?;
        let store_result = cache.store(
            "activity_year",
            ALGORITHM_VERSION,
            &input_digest,
            "gdp_activity_tiers",
            &mut frame,
        );
        store_result?;
        month_digests.clear();
        output_frames.push(frame);
        return Ok(());
    }
    finish_activity_year(editor_month_frames, output_frames)
}

pub(super) fn write_activity_outputs(
    wiki: &str,
    output_dir: &Path,
    frames: Vec<DataFrame>,
) -> Result<()> {
    let mut output = concat_frames(frames)?;
    output = sort_frame(output, ["period", "user_type", "tier_rank"])?;
    output = canonicalize_activity_tier_columns(&output)?;
    add_wiki_column(&mut output, wiki)?;
    write_output(&mut output, wiki, "gdp_activity_tiers", output_dir)
}
