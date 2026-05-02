pub mod gdp;
pub mod inequality;
pub mod labor;

use anyhow::{Context, Result};
use polars::prelude::*;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::path::Path;
use std::time::Instant;
use tracing::info;

use crate::{schema, storage};

pub(super) struct ChurnAccumulator {
    period_type: &'static str,
    seen: HashSet<(i64, i32)>,
    active: BTreeMap<i32, u32>,
    spans: HashMap<i64, (i32, i32)>,
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

struct RegisteredState {
    funnel_stats: HashMap<i64, (i32, u32)>,
    cohort_spans: HashMap<i64, (i32, i32)>,
    churn_month: ChurnAccumulator,
    churn_quarter: ChurnAccumulator,
    churn_year: ChurnAccumulator,
}

impl RegisteredState {
    fn new() -> Self {
        Self {
            funnel_stats: HashMap::new(),
            cohort_spans: HashMap::new(),
            churn_month: ChurnAccumulator::new("month"),
            churn_quarter: ChurnAccumulator::new("quarter"),
            churn_year: ChurnAccumulator::new("year"),
        }
    }

    fn observe_partition(
        &mut self,
        base: &DataFrame,
        year: i32,
        year_month_key: i32,
    ) -> Result<()> {
        let partial = registered_editor_totals(base)?;
        let user_ids = partial.column("event_user_id")?.i64()?;
        let total_edits = partial.column("total_edits")?.u32()?;
        let cohort_years = partial.column("cohort_year")?.i32()?;

        for idx in 0..partial.height() {
            let (Some(user_id), Some(user_total_edits), Some(cohort_year)) = (
                user_ids.get(idx),
                total_edits.get(idx),
                cohort_years.get(idx),
            ) else {
                continue;
            };

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
}

fn analytical_select_exprs() -> Vec<Expr> {
    schema::ANALYTICAL_COLUMNS
        .iter()
        .map(|column| col(*column))
        .collect()
}

fn analytical_lazyframe(wiki: &str, data_dir: &Path) -> Result<LazyFrame> {
    let parquet_dir = storage::analytical_wiki_dir(data_dir, wiki);
    if !parquet_dir.exists() {
        anyhow::bail!("No parquet data for {wiki}. Run `ingest` first.");
    }

    let files = storage::collect_parquet_files(&parquet_dir)?;
    if files.is_empty() {
        anyhow::bail!(
            "No parquet files found for {wiki} in {}",
            parquet_dir.display()
        );
    }

    let args = ScanArgsParquet {
        cache: true,
        ..Default::default()
    };
    let file_names: Vec<String> = files
        .iter()
        .map(|file| file.to_string_lossy().to_string())
        .collect();
    let parquet_files = file_names.iter().map(|file| file.as_str().into()).collect();
    LazyFrame::scan_parquet_sources(ScanSources::Paths(parquet_files), args).map_err(Into::into)
}

fn analytical_projection(df: LazyFrame, schema: &Schema) -> Result<DataFrame> {
    let has_analytical_projection = schema::ANALYTICAL_COLUMNS
        .iter()
        .all(|column| schema.get(column).is_some());

    if has_analytical_projection {
        return df
            .select(analytical_select_exprs())
            .collect()
            .map_err(Into::into);
    }

    let has_year_month = schema.get("year_month").is_some();
    let has_year = schema.get("year").is_some();
    let has_year_month_key = schema.get("year_month_key").is_some();
    let has_user_type = schema.get("user_type").is_some();
    let has_is_reverted = schema.get("is_reverted").is_some();
    let has_is_minor = schema.get("is_minor").is_some();

    let can_filter_revision_creates =
        schema.get("event_entity").is_some() && schema.get("event_type").is_some();
    let df = if can_filter_revision_creates {
        df.filter(
            col("event_entity")
                .eq(lit("revision"))
                .and(col("event_type").eq(lit("create"))),
        )
    } else {
        df
    };

    let event_user_is_anonymous = bool_flag_expr(
        "event_user_is_anonymous",
        schema.get("event_user_is_anonymous"),
    );
    let event_user_is_temporary = bool_flag_expr(
        "event_user_is_temporary",
        schema.get("event_user_is_temporary"),
    );

    df.select([
        if has_year_month {
            col("year_month")
        } else {
            year_month_col()
        },
        if has_year { col("year") } else { year_col() },
        if has_year_month_key {
            col("year_month_key")
        } else {
            year_month_key_col()
        },
        if has_user_type {
            col("user_type")
        } else {
            user_type_col(
                event_user_is_anonymous.clone(),
                event_user_is_temporary.clone(),
            )
        },
        col("event_user_id"),
        col("page_namespace"),
        col("revision_id"),
        col("revision_text_bytes_diff"),
        if has_is_reverted {
            col("is_reverted")
        } else {
            bool_flag_expr(
                "revision_is_identity_reverted",
                schema.get("revision_is_identity_reverted"),
            )
            .alias("is_reverted")
        },
        if has_is_minor {
            col("is_minor")
        } else {
            bool_flag_expr("revision_minor_edit", schema.get("revision_minor_edit"))
                .alias("is_minor")
        },
    ])
    .collect()
    .map_err(Into::into)
}

/// Load the minimal base dataset for metric computation into memory once.
pub fn load_wiki(wiki: &str, data_dir: &Path) -> Result<DataFrame> {
    let df = analytical_lazyframe(wiki, data_dir)?;
    let schema = df.clone().collect_schema()?;

    let started = Instant::now();
    let df = analytical_projection(df, &schema)?;

    info!(
        wiki = wiki,
        rows = df.height(),
        columns = df.width(),
        elapsed_ms = started.elapsed().as_secs_f64() * 1_000.0,
        "loaded base dataset"
    );

    Ok(df)
}

fn load_partition(dir: &Path) -> Result<DataFrame> {
    let files = storage::collect_parquet_files(dir)?;
    let args = ScanArgsParquet {
        cache: true,
        ..Default::default()
    };
    let file_names: Vec<String> = files
        .iter()
        .map(|file| file.to_string_lossy().to_string())
        .collect();
    let parquet_files = file_names.iter().map(|file| file.as_str().into()).collect();
    let df = LazyFrame::scan_parquet_sources(ScanSources::Paths(parquet_files), args)?;
    let schema = df.clone().collect_schema()?;
    analytical_projection(df, &schema)
}

/// Extract year-month string from event_timestamp (format: YYYY-MM-DD HH:MM:SS.0)
pub fn year_month_col() -> Expr {
    col("event_timestamp")
        .str()
        .slice(lit(0), lit(7))
        .alias("year_month")
}

/// Extract year string
pub fn year_col() -> Expr {
    col("event_timestamp")
        .str()
        .slice(lit(0), lit(4))
        .cast(DataType::Int32)
        .alias("year")
}

/// Extract year-month key as YYYYMM integer.
pub fn year_month_key_col() -> Expr {
    (col("event_timestamp")
        .str()
        .slice(lit(0), lit(4))
        .cast(DataType::Int32)
        * lit(100_i32)
        + col("event_timestamp")
            .str()
            .slice(lit(5), lit(2))
            .cast(DataType::Int32))
    .alias("year_month_key")
}

/// Categorize user: "bot", "anonymous", "temporary", or "registered".
pub fn user_type_col(event_user_is_anonymous: Expr, event_user_is_temporary: Expr) -> Expr {
    when(
        col("event_user_is_bot_by")
            .is_not_null()
            .and(col("event_user_is_bot_by").neq(lit(""))),
    )
    .then(lit("bot"))
    .when(event_user_is_anonymous)
    .then(lit("anonymous"))
    .when(event_user_is_temporary)
    .then(lit("temporary"))
    .otherwise(lit("registered"))
    .alias("user_type")
}

fn bool_flag_expr(column: &str, dtype: Option<&DataType>) -> Expr {
    match dtype {
        Some(DataType::Boolean) => col(column),
        _ => col(column).eq(lit("true")),
    }
}

fn concat_frames(mut frames: Vec<DataFrame>) -> Result<DataFrame> {
    let Some(mut first) = frames.pop() else {
        return Ok(DataFrame::empty());
    };
    for frame in frames {
        first.vstack_mut(&frame)?;
    }
    Ok(first)
}

fn add_wiki_column(df: &mut DataFrame, wiki: &str) -> Result<()> {
    df.with_column(Column::new("wiki".into(), vec![wiki; df.height()]))?;
    Ok(())
}

fn sort_frame<const N: usize>(df: DataFrame, columns: [&str; N]) -> Result<DataFrame> {
    df.sort(columns, SortMultipleOptions::default())
        .map_err(Into::into)
}

fn gdp_monthly_frame(base: &DataFrame) -> Result<DataFrame> {
    base.clone()
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
            col("event_user_id").n_unique().alias("unique_editors"),
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

fn gdp_type_share_frame(base: &DataFrame) -> Result<DataFrame> {
    base.clone()
        .lazy()
        .group_by([col("year_month"), col("user_type")])
        .agg([
            col("revision_id").count().alias("edits"),
            col("revision_text_bytes_diff").sum().alias("net_bytes"),
            col("event_user_id").n_unique().alias("editors"),
        ])
        .collect()
        .map_err(Into::into)
}

fn gdp_activity_tiers_frame(base: &DataFrame) -> Result<DataFrame> {
    base.clone()
        .lazy()
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
        .collect()
        .map_err(Into::into)
}

fn labor_monthly_frame(base: &DataFrame) -> Result<DataFrame> {
    base.clone()
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
        .collect()
        .map_err(Into::into)
}

fn registered_editor_totals(base: &DataFrame) -> Result<DataFrame> {
    base.clone()
        .lazy()
        .filter(col("user_type").eq(lit("registered")))
        .group_by([col("event_user_id")])
        .agg([
            col("revision_id").count().alias("total_edits"),
            col("year").min().cast(DataType::Int32).alias("cohort_year"),
        ])
        .collect()
        .map_err(Into::into)
}

fn finalize_funnel(stats: HashMap<i64, (i32, u32)>, wiki: &str, output_dir: &Path) -> Result<()> {
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

fn finalize_labor_cohorts(
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

/// Write a DataFrame to parquet in the output directory.
pub fn write_output(df: &mut DataFrame, wiki: &str, metric: &str, output_dir: &Path) -> Result<()> {
    let wiki_dir = output_dir.join(wiki);
    fs::create_dir_all(&wiki_dir)?;
    let path = wiki_dir.join(format!("{metric}.parquet"));
    let started = Instant::now();
    let mut file = fs::File::create(&path)?;
    ParquetWriter::new(&mut file)
        .with_compression(ParquetCompression::Zstd(None))
        .finish(df)?;
    let bytes = fs::metadata(&path)?.len();
    info!(
        wiki = wiki,
        metric = metric,
        rows = df.height(),
        columns = df.width(),
        bytes = bytes,
        elapsed_ms = started.elapsed().as_secs_f64() * 1_000.0,
        path = %path.display(),
        "wrote metric output"
    );
    Ok(())
}

fn compute_all_incremental(wiki: &str, data_dir: &Path, output_dir: &Path) -> Result<()> {
    let analytical_dir = storage::analytical_wiki_dir(data_dir, wiki);
    let partitions = storage::collect_partition_specs(&analytical_dir)?;
    if partitions.is_empty() {
        let base = load_wiki(wiki, data_dir)?;
        inequality::compute(wiki, &base, output_dir)?;
        labor::compute(wiki, &base, output_dir)?;
        gdp::compute(wiki, &base, output_dir)?;
        return Ok(());
    }

    let mut inequality_frames = Vec::new();
    let mut gdp_frames = Vec::new();
    let mut gdp_type_frames = Vec::new();
    let mut gdp_tier_frames = Vec::new();
    let mut labor_monthly_frames = Vec::new();
    let mut registered_state = RegisteredState::new();

    for partition in partitions {
        let base = load_partition(&partition.dir)?;
        let year_month_key = partition
            .year_month
            .split_once('-')
            .map(|(year, month): (&str, &str)| {
                let year: i32 = year.parse().expect("partition year should be numeric");
                let month: i32 = month.parse().expect("partition month should be numeric");
                year * 100 + month
            })
            .context("invalid partition year_month format")?;

        inequality_frames.push(inequality::compute_frame(&base)?);
        gdp_frames.push(gdp_monthly_frame(&base)?);
        gdp_type_frames.push(gdp_type_share_frame(&base)?);
        gdp_tier_frames.push(gdp_activity_tiers_frame(&base)?);
        labor_monthly_frames.push(labor_monthly_frame(&base)?);
        registered_state.observe_partition(&base, partition.year, year_month_key)?;
    }

    let mut inequality_out = concat_frames(inequality_frames)?;
    inequality_out =
        inequality_out.sort(["year_month", "user_type"], SortMultipleOptions::default())?;
    add_wiki_column(&mut inequality_out, wiki)?;
    write_output(&mut inequality_out, wiki, "inequality", output_dir)?;

    let mut gdp_out = concat_frames(gdp_frames)?;
    gdp_out = sort_frame(gdp_out, ["year_month", "page_namespace"])?;
    add_wiki_column(&mut gdp_out, wiki)?;
    write_output(&mut gdp_out, wiki, "gdp", output_dir)?;

    let mut gdp_type_out = concat_frames(gdp_type_frames)?;
    gdp_type_out =
        gdp_type_out.sort(["year_month", "user_type"], SortMultipleOptions::default())?;
    add_wiki_column(&mut gdp_type_out, wiki)?;
    write_output(&mut gdp_type_out, wiki, "gdp_user_type_share", output_dir)?;

    let mut gdp_tier_out = concat_frames(gdp_tier_frames)?;
    gdp_tier_out = sort_frame(gdp_tier_out, ["year_month", "user_type", "activity_tier"])?;
    add_wiki_column(&mut gdp_tier_out, wiki)?;
    write_output(&mut gdp_tier_out, wiki, "gdp_activity_tiers", output_dir)?;

    finalize_funnel(registered_state.funnel_stats, wiki, output_dir)?;

    let mut labor_monthly_out = concat_frames(labor_monthly_frames)?;
    labor_monthly_out = sort_frame(labor_monthly_out, ["year_month", "page_namespace"])?;
    add_wiki_column(&mut labor_monthly_out, wiki)?;
    write_output(&mut labor_monthly_out, wiki, "labor_monthly", output_dir)?;

    finalize_labor_cohorts(registered_state.cohort_spans, wiki, output_dir)?;

    let churn_frames = vec![
        registered_state.churn_month.finish()?,
        registered_state.churn_quarter.finish()?,
        registered_state.churn_year.finish()?,
    ];
    let mut churn = concat_frames(churn_frames)?;
    add_wiki_column(&mut churn, wiki)?;
    write_output(&mut churn, wiki, "labor_churn", output_dir)?;

    Ok(())
}

/// Run all metric families for a wiki.
pub fn compute_all(wiki: &str, data_dir: &Path, output_dir: &Path) -> Result<()> {
    info!(wiki = wiki, "computing metrics");
    let started = Instant::now();

    compute_all_incremental(wiki, data_dir, output_dir)?;

    info!(
        wiki = wiki,
        elapsed_ms = started.elapsed().as_secs_f64() * 1_000.0,
        "finished metric computation"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{TestDir, init_test_tracing};

    fn sample_input_df() -> Result<DataFrame> {
        DataFrame::new_infer_height(vec![
            Column::new(
                "event_entity".into(),
                vec![
                    "revision", "revision", "revision", "revision", "revision", "revision",
                ],
            ),
            Column::new(
                "event_type".into(),
                vec!["create", "create", "create", "create", "create", "create"],
            ),
            Column::new(
                "event_timestamp".into(),
                vec![
                    "2024-01-01 00:00:00.0",
                    "2024-01-03 00:00:00.0",
                    "2024-01-05 00:00:00.0",
                    "2024-02-01 00:00:00.0",
                    "2024-02-10 00:00:00.0",
                    "2025-01-10 00:00:00.0",
                ],
            ),
            Column::new("event_user_id".into(), vec![1_i64, 2, 4, 3, 1, 1]),
            Column::new(
                "event_user_is_bot_by".into(),
                vec![None::<&str>, None, None, None, Some("bot"), None],
            ),
            Column::new(
                "event_user_is_anonymous".into(),
                vec!["false", "false", "false", "false", "false", "false"],
            ),
            Column::new(
                "event_user_is_temporary".into(),
                vec!["false", "true", "true", "false", "false", "false"],
            ),
            Column::new("page_namespace".into(), vec![0_i32, 0, 0, 1, 0, 0]),
            Column::new("revision_id".into(), vec![10_i64, 11, 12, 13, 14, 15]),
            Column::new(
                "revision_text_bytes_diff".into(),
                vec![10_i64, 20, 15, -5, 7, 30],
            ),
            Column::new(
                "revision_is_identity_reverted".into(),
                vec!["false", "false", "false", "true", "false", "false"],
            ),
            Column::new(
                "revision_minor_edit".into(),
                vec!["false", "true", "false", "false", "false", "true"],
            ),
        ])
        .map_err(Into::into)
    }

    struct AnalyticalPartitionRows<'a> {
        year_month: [&'a str; 2],
        year_month_key: [i32; 2],
        user_type: [&'a str; 2],
        event_user_id: [i64; 2],
        page_namespace: [i32; 2],
        revision_id: [i64; 2],
        revision_text_bytes_diff: [i64; 2],
        is_reverted: [bool; 2],
        is_minor: [bool; 2],
    }

    fn analytical_partition_df(rows: AnalyticalPartitionRows<'_>) -> Result<DataFrame> {
        let columns = vec![
            Column::new("year_month".into(), rows.year_month.to_vec()),
            Column::new("year".into(), vec![2024_i32, 2024]),
            Column::new("year_month_key".into(), rows.year_month_key.to_vec()),
            Column::new("user_type".into(), rows.user_type.to_vec()),
            Column::new("event_user_id".into(), rows.event_user_id.to_vec()),
            Column::new("page_namespace".into(), rows.page_namespace.to_vec()),
            Column::new("revision_id".into(), rows.revision_id.to_vec()),
            Column::new(
                "revision_text_bytes_diff".into(),
                rows.revision_text_bytes_diff.to_vec(),
            ),
            Column::new("is_reverted".into(), rows.is_reverted.to_vec()),
            Column::new("is_minor".into(), rows.is_minor.to_vec()),
        ];
        DataFrame::new_infer_height(columns).map_err(Into::into)
    }

    fn write_input_parquet(temp_dir: &TestDir, wiki: &str) -> Result<()> {
        let parquet_dir = storage::analytical_wiki_dir(temp_dir.path(), wiki);
        fs::create_dir_all(&parquet_dir)?;
        let path = parquet_dir.join("part-000.parquet");
        let mut file = fs::File::create(path)?;
        let mut df = sample_input_df()?;
        ParquetWriter::new(&mut file).finish(&mut df)?;
        Ok(())
    }

    fn write_partitioned_base_parquet(temp_dir: &TestDir, wiki: &str) -> Result<()> {
        let jan_dir = storage::month_partition_dir(
            &storage::analytical_wiki_dir(temp_dir.path(), wiki),
            2024,
            "2024-01",
        );
        let feb_dir = storage::month_partition_dir(
            &storage::analytical_wiki_dir(temp_dir.path(), wiki),
            2024,
            "2024-02",
        );
        fs::create_dir_all(&jan_dir)?;
        fs::create_dir_all(&feb_dir)?;

        let mut jan = analytical_partition_df(AnalyticalPartitionRows {
            year_month: ["2024-01", "2024-01"],
            year_month_key: [202401, 202401],
            user_type: ["registered", "temporary"],
            event_user_id: [1, 2],
            page_namespace: [0, 0],
            revision_id: [10, 11],
            revision_text_bytes_diff: [15, 5],
            is_reverted: [false, false],
            is_minor: [true, false],
        })?;
        let mut feb = analytical_partition_df(AnalyticalPartitionRows {
            year_month: ["2024-02", "2024-02"],
            year_month_key: [202402, 202402],
            user_type: ["registered", "registered"],
            event_user_id: [1, 3],
            page_namespace: [0, 1],
            revision_id: [12, 13],
            revision_text_bytes_diff: [7, -3],
            is_reverted: [false, true],
            is_minor: [false, false],
        })?;

        ParquetWriter::new(&mut fs::File::create(jan_dir.join("part-000.parquet"))?)
            .finish(&mut jan)?;
        ParquetWriter::new(&mut fs::File::create(feb_dir.join("part-000.parquet"))?)
            .finish(&mut feb)?;
        Ok(())
    }

    fn write_partitioned_legacy_parquet(temp_dir: &TestDir, wiki: &str) -> Result<()> {
        let jan_dir = storage::month_partition_dir(
            &storage::analytical_wiki_dir(temp_dir.path(), wiki),
            2024,
            "2024-01",
        );
        let feb_dir = storage::month_partition_dir(
            &storage::analytical_wiki_dir(temp_dir.path(), wiki),
            2024,
            "2024-02",
        );
        fs::create_dir_all(&jan_dir)?;
        fs::create_dir_all(&feb_dir)?;

        let jan_columns = vec![
            Column::new("event_entity".into(), vec!["revision", "revision"]),
            Column::new("event_type".into(), vec!["create", "create"]),
            Column::new(
                "event_timestamp".into(),
                vec!["2024-01-01 00:00:00.0", "2024-01-03 00:00:00.0"],
            ),
            Column::new("event_user_id".into(), vec![1_i64, 2]),
            Column::new("event_user_is_bot_by".into(), vec![None::<&str>, None]),
            Column::new("event_user_is_anonymous".into(), vec![false, false]),
            Column::new("event_user_is_temporary".into(), vec![false, true]),
            Column::new("page_namespace".into(), vec![0_i32, 0]),
            Column::new("revision_id".into(), vec![10_i64, 11]),
            Column::new("revision_text_bytes_diff".into(), vec![15_i64, 5]),
            Column::new("revision_is_identity_reverted".into(), vec![false, false]),
            Column::new("revision_minor_edit".into(), vec![true, false]),
        ];
        let feb_columns = vec![
            Column::new("event_entity".into(), vec!["revision", "revision"]),
            Column::new("event_type".into(), vec!["create", "create"]),
            Column::new(
                "event_timestamp".into(),
                vec!["2024-02-01 00:00:00.0", "2024-02-10 00:00:00.0"],
            ),
            Column::new("event_user_id".into(), vec![1_i64, 3]),
            Column::new("event_user_is_bot_by".into(), vec![None::<&str>, None]),
            Column::new("event_user_is_anonymous".into(), vec![false, false]),
            Column::new("event_user_is_temporary".into(), vec![false, false]),
            Column::new("page_namespace".into(), vec![0_i32, 1]),
            Column::new("revision_id".into(), vec![12_i64, 13]),
            Column::new("revision_text_bytes_diff".into(), vec![7_i64, -3]),
            Column::new("revision_is_identity_reverted".into(), vec![false, true]),
            Column::new("revision_minor_edit".into(), vec![false, false]),
        ];

        let mut jan = DataFrame::new_infer_height(jan_columns)?;
        let mut feb = DataFrame::new_infer_height(feb_columns)?;
        ParquetWriter::new(&mut fs::File::create(jan_dir.join("part-000.parquet"))?)
            .finish(&mut jan)?;
        ParquetWriter::new(&mut fs::File::create(feb_dir.join("part-000.parquet"))?)
            .finish(&mut feb)?;
        Ok(())
    }

    fn write_partitioned_compatibility_parquet_without_filter_columns(
        temp_dir: &TestDir,
        wiki: &str,
    ) -> Result<()> {
        let dir = storage::month_partition_dir(
            &storage::analytical_wiki_dir(temp_dir.path(), wiki),
            2024,
            "2024-01",
        );
        fs::create_dir_all(&dir)?;
        let columns = vec![
            Column::new("year_month".into(), vec!["2024-01", "2024-01"]),
            Column::new("year".into(), vec![2024_i32, 2024]),
            Column::new("year_month_key".into(), vec![202401_i32, 202401]),
            Column::new("user_type".into(), vec!["registered", "temporary"]),
            Column::new("event_user_id".into(), vec![1_i64, 2]),
            Column::new("page_namespace".into(), vec![0_i32, 0]),
            Column::new("revision_id".into(), vec![10_i64, 11]),
            Column::new("revision_text_bytes_diff".into(), vec![15_i64, 5]),
            Column::new("revision_is_identity_reverted".into(), vec![false, false]),
            Column::new("revision_minor_edit".into(), vec![true, false]),
        ];
        let mut df = DataFrame::new_infer_height(columns)?;
        ParquetWriter::new(&mut fs::File::create(dir.join("part-000.parquet"))?).finish(&mut df)?;
        Ok(())
    }

    fn write_precomputed_parquet(
        temp_dir: &TestDir,
        wiki: &str,
        with_output_flags: bool,
    ) -> Result<()> {
        let parquet_dir = storage::analytical_wiki_dir(temp_dir.path(), wiki);
        fs::create_dir_all(&parquet_dir)?;
        let path = parquet_dir.join("part-000.parquet");
        let mut file = fs::File::create(path)?;

        let mut columns = vec![
            Column::new("event_entity".into(), vec!["revision", "revision"]),
            Column::new("event_type".into(), vec!["create", "create"]),
            Column::new("year_month".into(), vec!["2024-01", "2024-02"]),
            Column::new("year".into(), vec![2024_i32, 2024]),
            Column::new("year_month_key".into(), vec![202401_i32, 202402]),
            Column::new("user_type".into(), vec!["registered", "temporary"]),
            Column::new("event_user_id".into(), vec![1_i64, 2]),
            Column::new("page_namespace".into(), vec![0_i32, 1]),
            Column::new("revision_id".into(), vec![10_i64, 11]),
            Column::new("revision_text_bytes_diff".into(), vec![15_i64, -3]),
        ];

        if with_output_flags {
            columns.push(Column::new("is_reverted".into(), vec![false, true]));
            columns.push(Column::new("is_minor".into(), vec![true, false]));
        } else {
            columns.push(Column::new(
                "revision_is_identity_reverted".into(),
                vec![false, true],
            ));
            columns.push(Column::new("revision_minor_edit".into(), vec![true, false]));
        }

        let mut df = DataFrame::new_infer_height(columns)?;
        ParquetWriter::new(&mut file).finish(&mut df)?;
        Ok(())
    }

    fn write_compatibility_parquet_with_output_flags(temp_dir: &TestDir, wiki: &str) -> Result<()> {
        let parquet_dir = storage::analytical_wiki_dir(temp_dir.path(), wiki);
        fs::create_dir_all(&parquet_dir)?;
        let path = parquet_dir.join("part-000.parquet");
        let mut file = fs::File::create(path)?;
        let columns = vec![
            Column::new("event_entity".into(), vec!["revision", "revision"]),
            Column::new("event_type".into(), vec!["create", "create"]),
            Column::new(
                "event_timestamp".into(),
                vec!["2024-01-01 00:00:00.0", "2024-02-01 00:00:00.0"],
            ),
            Column::new("year_month".into(), vec!["2024-01", "2024-02"]),
            Column::new("year".into(), vec![2024_i32, 2024]),
            Column::new("user_type".into(), vec!["registered", "temporary"]),
            Column::new("event_user_id".into(), vec![1_i64, 2]),
            Column::new("page_namespace".into(), vec![0_i32, 1]),
            Column::new("revision_id".into(), vec![10_i64, 11]),
            Column::new("revision_text_bytes_diff".into(), vec![15_i64, -3]),
            Column::new("is_reverted".into(), vec![false, true]),
            Column::new("is_minor".into(), vec![true, false]),
        ];
        let mut df = DataFrame::new_infer_height(columns)?;
        ParquetWriter::new(&mut file).finish(&mut df)?;
        Ok(())
    }

    fn registered_base_df(rows: &[(Option<i64>, i32, i32, i64)]) -> Result<DataFrame> {
        DataFrame::new_infer_height(vec![
            Column::new(
                "year_month".into(),
                rows.iter()
                    .map(|(_, _, year_month_key, _)| {
                        format!("{:04}-{:02}", year_month_key / 100, year_month_key % 100)
                    })
                    .collect::<Vec<_>>(),
            ),
            Column::new(
                "year".into(),
                rows.iter().map(|(_, year, _, _)| *year).collect::<Vec<_>>(),
            ),
            Column::new(
                "year_month_key".into(),
                rows.iter()
                    .map(|(_, _, year_month_key, _)| *year_month_key)
                    .collect::<Vec<_>>(),
            ),
            Column::new("user_type".into(), vec!["registered"; rows.len()]),
            Column::new(
                "event_user_id".into(),
                rows.iter()
                    .map(|(user_id, _, _, _)| *user_id)
                    .collect::<Vec<_>>(),
            ),
            Column::new("page_namespace".into(), vec![0_i32; rows.len()]),
            Column::new(
                "revision_id".into(),
                rows.iter()
                    .map(|(_, _, _, revision_id)| *revision_id)
                    .collect::<Vec<_>>(),
            ),
            Column::new("revision_text_bytes_diff".into(), vec![1_i64; rows.len()]),
            Column::new("is_reverted".into(), vec![false; rows.len()]),
            Column::new("is_minor".into(), vec![false; rows.len()]),
        ])
        .map_err(Into::into)
    }

    #[test]
    fn compute_all_writes_expected_outputs() -> Result<()> {
        init_test_tracing();
        let data_dir = TestDir::new()?;
        let output_dir = TestDir::new()?;
        let wiki = "testwiki";

        write_input_parquet(&data_dir, wiki)?;
        compute_all(wiki, data_dir.path(), output_dir.path())?;

        for metric in [
            "business_funnel",
            "gdp",
            "gdp_activity_tiers",
            "gdp_user_type_share",
            "inequality",
            "labor_churn",
            "labor_cohorts",
            "labor_monthly",
        ] {
            assert!(
                output_dir
                    .path()
                    .join(wiki)
                    .join(format!("{metric}.parquet"))
                    .exists()
            );
        }

        let inequality_path = output_dir.path().join(wiki).join("inequality.parquet");
        let inequality_path = inequality_path.to_string_lossy().to_string();
        let inequality =
            LazyFrame::scan_parquet(inequality_path.as_str().into(), Default::default())?
                .collect()?;
        let user_types: Vec<String> = inequality
            .column("user_type")?
            .str()?
            .into_iter()
            .flatten()
            .map(ToOwned::to_owned)
            .collect();
        assert!(user_types.iter().any(|user_type| user_type == "temporary"));

        Ok(())
    }

    #[test]
    fn concat_frames_returns_empty_for_no_input() -> Result<()> {
        let frame = concat_frames(Vec::new())?;
        assert_eq!(frame.height(), 0);
        assert_eq!(frame.width(), 0);
        Ok(())
    }

    #[test]
    fn registered_state_tracks_bounds_and_skips_null_users() -> Result<()> {
        let mut state = RegisteredState::new();
        let null_only = registered_base_df(&[(None, 2024, 202401, 1)])?;
        state.observe_partition(&null_only, 2024, 202401)?;
        assert!(state.funnel_stats.is_empty());

        let early = registered_base_df(&[(Some(1), 2024, 202401, 10)])?;
        let late = registered_base_df(&[(Some(1), 2025, 202502, 11)])?;
        let earlier = registered_base_df(&[(Some(1), 2023, 202312, 12)])?;
        state.observe_partition(&early, 2024, 202401)?;
        state.observe_partition(&late, 2025, 202502)?;
        state.observe_partition(&earlier, 2023, 202312)?;

        assert_eq!(state.funnel_stats.get(&1), Some(&(2023, 3)));
        assert_eq!(state.cohort_spans.get(&1), Some(&(2023, 2025)));
        Ok(())
    }

    #[test]
    fn finalize_funnel_writes_threshold_counts() -> Result<()> {
        init_test_tracing();
        let output_dir = TestDir::new()?;
        let stats = HashMap::from([
            (1_i64, (2024, 1_u32)),
            (2, (2024, 5)),
            (3, (2024, 25)),
            (4, (2024, 100)),
        ]);

        finalize_funnel(stats, "testwiki", output_dir.path())?;

        let path = output_dir
            .path()
            .join("testwiki")
            .join("business_funnel.parquet")
            .to_string_lossy()
            .to_string();
        let df = LazyFrame::scan_parquet(path.as_str().into(), Default::default())?.collect()?;
        assert_eq!(df.column("cohort_size")?.u32()?.get(0), Some(4));
        assert_eq!(df.column("reached_5")?.u32()?.get(0), Some(3));
        assert_eq!(df.column("reached_25")?.u32()?.get(0), Some(2));
        assert_eq!(df.column("reached_100")?.u32()?.get(0), Some(1));
        Ok(())
    }

    #[test]
    fn finalize_labor_cohorts_writes_output() -> Result<()> {
        init_test_tracing();
        let output_dir = TestDir::new()?;
        let spans = HashMap::from([(1_i64, (2024, 2025)), (2, (2024, 2024))]);

        finalize_labor_cohorts(spans, "testwiki", output_dir.path())?;

        let path = output_dir
            .path()
            .join("testwiki")
            .join("labor_cohorts.parquet")
            .to_string_lossy()
            .to_string();
        let df = LazyFrame::scan_parquet(path.as_str().into(), Default::default())?.collect()?;
        assert!(df.height() > 0);
        assert_eq!(df.column("wiki")?.str()?.get(0), Some("testwiki"));
        Ok(())
    }

    #[test]
    fn compute_all_uses_partitioned_incremental_path() -> Result<()> {
        init_test_tracing();
        let data_dir = TestDir::new()?;
        let output_dir = TestDir::new()?;
        let wiki = "partitionedwiki";

        write_partitioned_base_parquet(&data_dir, wiki)?;
        compute_all(wiki, data_dir.path(), output_dir.path())?;

        let gdp_path = output_dir.path().join(wiki).join("gdp.parquet");
        let gdp_path = gdp_path.to_string_lossy().to_string();
        let gdp =
            LazyFrame::scan_parquet(gdp_path.as_str().into(), Default::default())?.collect()?;
        assert_eq!(gdp.height(), 4);

        let churn_path = output_dir.path().join(wiki).join("labor_churn.parquet");
        let churn_path = churn_path.to_string_lossy().to_string();
        let churn =
            LazyFrame::scan_parquet(churn_path.as_str().into(), Default::default())?.collect()?;
        assert!(
            churn
                .column("period_type")?
                .str()?
                .into_iter()
                .flatten()
                .any(|value| value == "quarter")
        );
        Ok(())
    }

    #[test]
    fn load_partition_uses_existing_analytical_projection_without_filter_columns() -> Result<()> {
        init_test_tracing();
        let data_dir = TestDir::new()?;
        let wiki = "projection-partitionedwiki";

        write_partitioned_base_parquet(&data_dir, wiki)?;
        let jan_dir = storage::month_partition_dir(
            &storage::analytical_wiki_dir(data_dir.path(), wiki),
            2024,
            "2024-01",
        );
        let loaded = load_partition(&jan_dir)?;

        assert_eq!(loaded.height(), 2);
        assert_eq!(loaded.width(), schema::ANALYTICAL_COLUMNS.len());
        assert_eq!(loaded.column("year_month")?.str()?.get(0), Some("2024-01"));
        Ok(())
    }

    #[test]
    fn load_partition_compatibility_projection_skips_revision_filter_when_columns_are_absent()
    -> Result<()> {
        init_test_tracing();
        let data_dir = TestDir::new()?;
        let wiki = "compatibility-partitionedwiki";

        write_partitioned_compatibility_parquet_without_filter_columns(&data_dir, wiki)?;
        let jan_dir = storage::month_partition_dir(
            &storage::analytical_wiki_dir(data_dir.path(), wiki),
            2024,
            "2024-01",
        );
        let loaded = load_partition(&jan_dir)?;

        assert_eq!(loaded.height(), 2);
        assert_eq!(loaded.column("is_minor")?.bool()?.get(0), Some(true));
        Ok(())
    }

    #[test]
    fn compute_all_supports_partitioned_legacy_parquet_layouts() -> Result<()> {
        init_test_tracing();
        let data_dir = TestDir::new()?;
        let output_dir = TestDir::new()?;
        let wiki = "legacy-partitionedwiki";

        write_partitioned_legacy_parquet(&data_dir, wiki)?;
        compute_all(wiki, data_dir.path(), output_dir.path())?;

        let gdp_path = output_dir
            .path()
            .join(wiki)
            .join("gdp.parquet")
            .to_string_lossy()
            .to_string();
        let gdp =
            LazyFrame::scan_parquet(gdp_path.as_str().into(), Default::default())?.collect()?;
        assert_eq!(gdp.height(), 4);

        let labor_path = output_dir
            .path()
            .join(wiki)
            .join("labor_monthly.parquet")
            .to_string_lossy()
            .to_string();
        let labor =
            LazyFrame::scan_parquet(labor_path.as_str().into(), Default::default())?.collect()?;
        assert!(
            labor
                .column("user_type")?
                .str()?
                .into_iter()
                .flatten()
                .any(|value| value == "temporary")
        );
        Ok(())
    }

    #[test]
    fn load_wiki_filters_and_enriches_rows() -> Result<()> {
        init_test_tracing();
        let data_dir = TestDir::new()?;
        let wiki = "testwiki";

        write_input_parquet(&data_dir, wiki)?;
        let loaded = load_wiki(wiki, data_dir.path())?;

        assert_eq!(loaded.height(), 6);
        assert_eq!(loaded.width(), 10);

        let user_types: Vec<String> = loaded
            .column("user_type")?
            .str()?
            .into_iter()
            .flatten()
            .map(ToOwned::to_owned)
            .collect();
        assert!(user_types.iter().any(|user_type| user_type == "bot"));
        assert!(user_types.iter().any(|user_type| user_type == "temporary"));

        Ok(())
    }

    #[test]
    fn load_wiki_errors_when_parquet_directory_is_missing() {
        init_test_tracing();
        let data_dir = TestDir::new().expect("temp dir");
        let err =
            load_wiki("missingwiki", data_dir.path()).expect_err("missing parquet should fail");
        assert!(err.to_string().contains("Run `ingest` first"));
    }

    #[test]
    fn load_wiki_errors_when_no_parquet_files_exist() -> Result<()> {
        init_test_tracing();
        let data_dir = TestDir::new()?;
        let wiki = "emptywiki";
        fs::create_dir_all(storage::analytical_wiki_dir(data_dir.path(), wiki))?;

        let err = load_wiki(wiki, data_dir.path()).expect_err("empty parquet dir should fail");
        assert!(err.to_string().contains("No parquet files found"));
        Ok(())
    }

    #[test]
    fn load_wiki_uses_precomputed_columns_when_available() -> Result<()> {
        init_test_tracing();
        let data_dir = TestDir::new()?;
        let wiki = "precomputedwiki";

        write_precomputed_parquet(&data_dir, wiki, true)?;
        let loaded = load_wiki(wiki, data_dir.path())?;

        assert_eq!(loaded.column("year_month_key")?.i32()?.get(0), Some(202401));
        assert_eq!(loaded.column("user_type")?.str()?.get(1), Some("temporary"));
        assert_eq!(loaded.column("is_reverted")?.bool()?.get(1), Some(true));
        assert_eq!(loaded.column("is_minor")?.bool()?.get(0), Some(true));
        Ok(())
    }

    #[test]
    fn load_wiki_uses_boolean_source_flags_when_output_flags_are_missing() -> Result<()> {
        init_test_tracing();
        let data_dir = TestDir::new()?;
        let wiki = "boolean-fallback-wiki";

        write_precomputed_parquet(&data_dir, wiki, false)?;
        let loaded = load_wiki(wiki, data_dir.path())?;

        assert_eq!(loaded.column("is_reverted")?.bool()?.get(1), Some(true));
        assert_eq!(loaded.column("is_minor")?.bool()?.get(0), Some(true));
        Ok(())
    }

    #[test]
    fn load_wiki_uses_existing_output_flags_in_compatibility_projection() -> Result<()> {
        init_test_tracing();
        let data_dir = TestDir::new()?;
        let wiki = "compatibility-output-flags-wiki";

        write_compatibility_parquet_with_output_flags(&data_dir, wiki)?;
        let loaded = load_wiki(wiki, data_dir.path())?;

        assert_eq!(loaded.column("is_reverted")?.bool()?.get(1), Some(true));
        assert_eq!(loaded.column("is_minor")?.bool()?.get(0), Some(true));
        assert_eq!(loaded.column("year_month_key")?.i32()?.get(0), Some(202401));
        Ok(())
    }
}
