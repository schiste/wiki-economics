/// Semantic version for monthly stateless aggregates.
///
/// Increment this when GDP, GDP user-type share, inequality, or monthly labor
/// semantics change. Physical scan scheduling alone does not require a bump.
pub(crate) const ALGORITHM_VERSION: &str = "monthly-stateless-v5-exact-period-inequality";

use super::{
    PendingOutput, add_wiki_column, concat_frames, editor_identity_available_expr,
    ensure_editor_identity_inputs, sort_frame, unique_identified_editors_expr, write_output,
};
use crate::compute::inequality;
use anyhow::{Context, Result};
use polars::prelude::*;
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use tracing::info;

pub(crate) const EDITOR_IDENTITY_REPORT: &str = "editor_identity_coverage.json";

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct EditorIdentityCoveragePeriod {
    pub(crate) year_month: String,
    pub(crate) user_type: String,
    pub(crate) total_edits: u64,
    pub(crate) identified_edits: u64,
    pub(crate) excluded_edits: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct EditorIdentityCoverageReport {
    pub(crate) schema_version: u32,
    pub(crate) wiki: String,
    pub(crate) snapshot: Option<String>,
    pub(crate) algorithm_version: String,
    pub(crate) total_edits: u64,
    pub(crate) identified_edits: u64,
    pub(crate) excluded_edits: u64,
    pub(crate) periods: Vec<EditorIdentityCoveragePeriod>,
}

pub(super) fn gdp_monthly_frame(base: &DataFrame) -> Result<DataFrame> {
    ensure_editor_identity_inputs(base)?
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
            unique_identified_editors_expr().alias("unique_editors"),
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

pub(super) fn gdp_type_share_frame(base: &DataFrame) -> Result<DataFrame> {
    ensure_editor_identity_inputs(base)?
        .lazy()
        .group_by([col("year_month"), col("user_type")])
        .agg([
            col("revision_id").count().alias("edits"),
            col("revision_text_bytes_diff").sum().alias("net_bytes"),
            unique_identified_editors_expr().alias("editors"),
        ])
        .collect()
        .map_err(Into::into)
}

pub(super) fn editor_identity_coverage_frame(base: &DataFrame) -> Result<DataFrame> {
    ensure_editor_identity_inputs(base)?
        .lazy()
        .group_by([col("year_month"), col("user_type")])
        .agg([
            col("revision_id").count().alias("total_edits"),
            col("revision_id")
                .filter(editor_identity_available_expr())
                .count()
                .alias("identified_edits"),
            col("revision_id")
                .filter(editor_identity_available_expr().not())
                .count()
                .alias("excluded_edits"),
        ])
        .collect()
        .map_err(Into::into)
}

pub(super) fn editor_identity_report_path(output_dir: &Path, wiki: &str) -> PathBuf {
    output_dir.join(wiki).join(EDITOR_IDENTITY_REPORT)
}

pub(super) fn write_editor_identity_coverage(
    wiki: &str,
    snapshot: Option<&str>,
    output_dir: &Path,
    frames: Vec<DataFrame>,
) -> Result<()> {
    let frame =
        concat_frames(frames)?.sort(["year_month", "user_type"], SortMultipleOptions::default())?;
    let months = frame.column("year_month")?.str()?;
    let user_types = frame.column("user_type")?.str()?;
    let totals = frame.column("total_edits")?.u32()?;
    let identified = frame.column("identified_edits")?.u32()?;
    let excluded = frame.column("excluded_edits")?.u32()?;
    let mut periods = Vec::with_capacity(frame.height());
    let mut total_edits = 0_u64;
    let mut identified_edits = 0_u64;
    let mut excluded_edits = 0_u64;
    for row in 0..frame.height() {
        let period = EditorIdentityCoveragePeriod {
            year_month: months
                .get(row)
                .context("identity coverage month is null")?
                .to_string(),
            user_type: user_types
                .get(row)
                .context("identity coverage user type is null")?
                .to_string(),
            total_edits: u64::from(totals.get(row).context("identity coverage total is null")?),
            identified_edits: u64::from(
                identified
                    .get(row)
                    .context("identity coverage identified total is null")?,
            ),
            excluded_edits: u64::from(
                excluded
                    .get(row)
                    .context("identity coverage excluded total is null")?,
            ),
        };
        anyhow::ensure!(
            period.total_edits == period.identified_edits + period.excluded_edits,
            "editor identity coverage does not conserve edits"
        );
        total_edits = total_edits
            .checked_add(period.total_edits)
            .context("identity coverage total overflow")?;
        identified_edits = identified_edits
            .checked_add(period.identified_edits)
            .context("identity coverage identified total overflow")?;
        excluded_edits = excluded_edits
            .checked_add(period.excluded_edits)
            .context("identity coverage excluded total overflow")?;
        periods.push(period);
    }
    anyhow::ensure!(
        total_edits == identified_edits + excluded_edits,
        "editor identity report does not conserve edits"
    );
    let report = EditorIdentityCoverageReport {
        schema_version: 1,
        wiki: wiki.to_string(),
        snapshot: snapshot.map(str::to_string),
        algorithm_version: ALGORITHM_VERSION.to_string(),
        total_edits,
        identified_edits,
        excluded_edits,
        periods,
    };
    let path = editor_identity_report_path(output_dir, wiki);
    let pending = PendingOutput::new(path)?;
    let mut file = File::create(&pending.temp_path)?;
    serde_json::to_writer_pretty(&mut file, &report)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    drop(file);
    pending.publish()?;
    info!(
        wiki,
        total_edits, identified_edits, excluded_edits, "recorded editor identity coverage"
    );
    Ok(())
}

pub(crate) fn read_editor_identity_coverage(
    output_dir: &Path,
    wiki: &str,
) -> Result<Option<EditorIdentityCoverageReport>> {
    let path = editor_identity_report_path(output_dir, wiki);
    if !path.is_file() {
        return Ok(None);
    }
    let report: EditorIdentityCoverageReport = serde_json::from_slice(&fs::read(&path)?)?;
    let mut period_total = 0_u64;
    let mut period_identified = 0_u64;
    let mut period_excluded = 0_u64;
    let mut previous: Option<(&str, &str)> = None;
    for period in &report.periods {
        let key = (period.year_month.as_str(), period.user_type.as_str());
        anyhow::ensure!(
            previous.is_none_or(|prior| prior < key)
                && period.total_edits == period.identified_edits + period.excluded_edits,
            "invalid editor identity coverage period for {wiki}"
        );
        previous = Some(key);
        period_total = period_total
            .checked_add(period.total_edits)
            .context("identity coverage period total overflow")?;
        period_identified = period_identified
            .checked_add(period.identified_edits)
            .context("identity coverage period identified total overflow")?;
        period_excluded = period_excluded
            .checked_add(period.excluded_edits)
            .context("identity coverage period excluded total overflow")?;
    }
    anyhow::ensure!(
        report.schema_version == 1
            && report.wiki == wiki
            && report.algorithm_version == ALGORITHM_VERSION
            && report.total_edits == report.identified_edits + report.excluded_edits
            && report.total_edits == period_total
            && report.identified_edits == period_identified
            && report.excluded_edits == period_excluded,
        "invalid editor identity coverage report for {wiki}"
    );
    Ok(Some(report))
}

pub(super) fn labor_monthly_frame(base: &DataFrame) -> Result<DataFrame> {
    ensure_editor_identity_inputs(base)?
        .lazy()
        .group_by([col("year_month"), col("page_namespace"), col("user_type")])
        .agg([
            unique_identified_editors_expr().alias("unique_editors"),
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

pub(super) fn finish_inequality_year_cached(
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
            "inequality cache month inputs and identities disagree"
        );
        let digest_refs = month_digests.iter().map(String::as_str).collect::<Vec<_>>();
        let input_digest = cache.derived_digest("inequality_year", ALGORITHM_VERSION, &digest_refs);
        let cached = cache.load(
            "inequality_year",
            ALGORITHM_VERSION,
            &input_digest,
            "inequality",
        );
        if let Some(frame) = cached? {
            editor_month_frames.clear();
            month_digests.clear();
            output_frames.push(frame);
            return Ok(());
        }
        let editor_months = concat_frames(std::mem::take(editor_month_frames))?;
        let mut frame = inequality::compute_periods(&editor_months)?;
        let store_result = cache.store(
            "inequality_year",
            ALGORITHM_VERSION,
            &input_digest,
            "inequality",
            &mut frame,
        );
        store_result?;
        month_digests.clear();
        output_frames.push(frame);
        return Ok(());
    }
    let editor_months = concat_frames(std::mem::take(editor_month_frames))?;
    output_frames.push(inequality::compute_periods(&editor_months)?);
    Ok(())
}

pub(super) struct MonthlyFrames {
    pub(super) inequality_frames: Vec<DataFrame>,
    pub(super) gdp_frames: Vec<DataFrame>,
    pub(super) gdp_type_frames: Vec<DataFrame>,
    pub(super) identity_coverage_frames: Vec<DataFrame>,
    pub(super) labor_monthly_frames: Vec<DataFrame>,
}

pub(super) fn write_monthly_outputs(
    wiki: &str,
    snapshot: Option<&str>,
    output_dir: &Path,
    frames: MonthlyFrames,
) -> Result<()> {
    let mut inequality_out = concat_frames(frames.inequality_frames)?;
    let inequality_sort = inequality_out.sort(
        ["period", "period_type", "user_type"],
        SortMultipleOptions::default(),
    );
    inequality_out = inequality_sort?;
    add_wiki_column(&mut inequality_out, wiki)?;
    write_output(&mut inequality_out, wiki, "inequality", output_dir)?;

    let mut gdp_out = concat_frames(frames.gdp_frames)?;
    gdp_out = sort_frame(gdp_out, ["year_month", "page_namespace", "user_type"])?;
    add_wiki_column(&mut gdp_out, wiki)?;
    write_output(&mut gdp_out, wiki, "gdp", output_dir)?;

    let mut gdp_type_out = concat_frames(frames.gdp_type_frames)?;
    gdp_type_out =
        gdp_type_out.sort(["year_month", "user_type"], SortMultipleOptions::default())?;
    add_wiki_column(&mut gdp_type_out, wiki)?;
    write_output(&mut gdp_type_out, wiki, "gdp_user_type_share", output_dir)?;
    write_editor_identity_coverage(wiki, snapshot, output_dir, frames.identity_coverage_frames)?;

    let mut labor_monthly_out = concat_frames(frames.labor_monthly_frames)?;
    labor_monthly_out = sort_frame(
        labor_monthly_out,
        ["year_month", "page_namespace", "user_type"],
    )?;
    add_wiki_column(&mut labor_monthly_out, wiki)?;
    write_output(&mut labor_monthly_out, wiki, "labor_monthly", output_dir)
}
