use anyhow::{Context, Result, ensure};
use polars::prelude::*;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::{
    artifact_receipt, licensing,
    metric_registry::{self, BrowserPartitioning, MetricDefinition, MetricId},
};

pub const INDEX_FILENAME: &str = "browser-data-index.json";
pub const INDEX_SCHEMA_VERSION: u32 = 3;
pub const CACHE_SCHEMA_VERSION: u32 = 3;
const GLOBAL_WIKI: &str = "all";
const GLOBAL_ROOT: &str = "_browser-global";
const GLOBAL_AGGREGATION_VERSION: &str = "global-browser-aggregate-v2-semantic-composition";

pub(crate) fn browser_metrics() -> impl Iterator<Item = &'static MetricDefinition> {
    metric_registry::definitions().filter(|definition| {
        definition.browser_partitioning == BrowserPartitioning::PerWikiAndGlobalYearShards
    })
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BrowserDataEntry {
    pub metric: String,
    pub wiki: String,
    pub minimum_date: String,
    pub maximum_date: String,
    pub file: String,
    pub rows: u64,
    pub bytes: u64,
    pub sha256: String,
    pub artifact_receipt_sha256: String,
    pub scope: String,
    pub shard: Option<String>,
    pub aggregation_version: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BrowserDataIndex {
    pub schema_version: u32,
    pub cache_schema_version: u32,
    pub generation: String,
    pub license_spdx: String,
    pub entries: Vec<BrowserDataEntry>,
}

#[derive(Serialize)]
struct IndexSeed<'a> {
    schema_version: u32,
    cache_schema_version: u32,
    entries: &'a [BrowserDataEntry],
}

fn published_wikis(output_dir: &Path, allowlist: Option<&BTreeSet<String>>) -> Result<Vec<String>> {
    let mut wikis = Vec::new();
    for entry in fs::read_dir(output_dir)? {
        let entry = entry?;
        // Publication selects immutable candidates with per-wiki symlinks.
        // `Path::is_dir` follows those links while still rejecting files.
        if !entry.path().is_dir() {
            continue;
        }
        let wiki = entry.file_name().to_string_lossy().into_owned();
        if wiki.starts_with('_') || allowlist.is_some_and(|allowed| !allowed.contains(&wiki)) {
            continue;
        }
        if browser_metrics().any(|metric| entry.path().join(metric.id.parquet_name()).is_file()) {
            wikis.push(wiki);
        }
    }
    wikis.sort();
    Ok(wikis)
}

fn build_index(
    output_dir: &Path,
    allowlist: Option<&BTreeSet<String>>,
) -> Result<BrowserDataIndex> {
    build_index_with_previous(output_dir, allowlist, None).map(|(index, _, _)| index)
}

fn build_index_with_previous(
    output_dir: &Path,
    allowlist: Option<&BTreeSet<String>>,
    previous: Option<&BrowserDataIndex>,
) -> Result<(BrowserDataIndex, usize, usize)> {
    materialize_global_partitions(output_dir, allowlist)?;
    let wikis = published_wikis(output_dir, allowlist)?;
    ensure!(
        !wikis.is_empty(),
        "no published wiki outputs found for browser data"
    );
    let previous = previous
        .into_iter()
        .flat_map(|index| &index.entries)
        .map(|entry| (entry.file.as_str(), entry))
        .collect::<BTreeMap<_, _>>();
    let mut entries = Vec::new();
    let mut reused = 0_usize;
    let mut rebuilt = 0_usize;
    for wiki in wikis {
        for definition in browser_metrics() {
            let metric = definition.name;
            let date_column = definition
                .date_column
                .expect("browser-partitioned metrics have a date column");
            let source = output_dir.join(&wiki).join(format!("{metric}.parquet"));
            if !source.is_file() {
                continue;
            }
            let identity = format!("{wiki}/{metric}.parquet");
            let document = if artifact_receipt::sidecar_path(&source)?.is_file() {
                artifact_receipt::read(&source)?
            } else {
                artifact_receipt::scan_and_write(
                    &source,
                    &identity,
                    "legacy-browser-index-migration-v1",
                    "legacy-unreceipted-input",
                )
                .or_else(|_| {
                    artifact_receipt::scan_and_write_with_spec(
                        &source,
                        &identity,
                        "legacy-browser-index-migration-v1",
                        "legacy-unreceipted-input",
                        artifact_receipt::SemanticSpec {
                            date_column: Some(date_column.to_string()),
                            conservation_columns: Vec::new(),
                            ordering_contract: "wiki-major/v1".to_string(),
                            page_week_consistency: false,
                        },
                    )
                })?
            };
            let document = artifact_receipt::verify(
                &source,
                &document.receipt.identity,
                Some(&document.receipt_sha256),
                artifact_receipt::VerificationMode::Fast,
            )?;
            let receipt_sha256 = document.receipt_sha256;
            let receipt = document.receipt;
            ensure!(
                receipt.rows > 0 && receipt.minimum_wiki == wiki && receipt.maximum_wiki == wiki,
                "browser partition receipt is empty or contains another wiki"
            );
            let minimum_date = receipt
                .minimum_date
                .context("browser partition date minimum is null")?;
            let maximum_date = receipt
                .maximum_date
                .context("browser partition date maximum is null")?;
            ensure!(
                receipt
                    .parquet_schema
                    .iter()
                    .any(|field| field.name == date_column),
                "browser partition receipt is missing {date_column}"
            );
            let expected = BrowserDataEntry {
                metric: metric.to_string(),
                wiki: wiki.clone(),
                minimum_date,
                maximum_date,
                file: format!("browser-data/{metric}/{wiki}.parquet"),
                rows: receipt.rows,
                bytes: receipt.bytes,
                sha256: receipt.artifact_sha256,
                artifact_receipt_sha256: receipt_sha256,
                scope: "wiki".to_string(),
                shard: None,
                aggregation_version: None,
            };
            if let Some(existing) = previous.get(expected.file.as_str())
                && **existing == expected
            {
                entries.push((*existing).clone());
                reused += 1;
            } else {
                entries.push(expected);
                rebuilt += 1;
            }
        }
    }
    for definition in browser_metrics() {
        let metric = definition.name;
        let date_column = definition
            .date_column
            .expect("browser-partitioned metrics have a date column");
        let metric_root = output_dir.join(GLOBAL_ROOT).join(metric);
        if !metric_root.is_dir() {
            continue;
        }
        let mut shards = fs::read_dir(&metric_root)?
            .filter_map(|entry| entry.ok().map(|entry| entry.path()))
            .filter(|path| path.extension().and_then(|value| value.to_str()) == Some("parquet"))
            .collect::<Vec<_>>();
        shards.sort();
        for source in shards {
            let shard = source
                .file_stem()
                .and_then(|value| value.to_str())
                .context("global browser shard has no UTF-8 name")?
                .to_string();
            let identity = format!("{GLOBAL_ROOT}/{metric}/{shard}.parquet");
            let verification = artifact_receipt::verify(
                &source,
                &identity,
                None,
                artifact_receipt::VerificationMode::Fast,
            );
            let document = verification?;
            let receipt_sha256 = document.receipt_sha256;
            let receipt = document.receipt;
            ensure!(
                receipt.rows > 0
                    && receipt.minimum_wiki == GLOBAL_WIKI
                    && receipt.maximum_wiki == GLOBAL_WIKI
                    && receipt
                        .parquet_schema
                        .iter()
                        .any(|field| field.name == date_column),
                "global browser partition is empty or invalid"
            );
            let expected = BrowserDataEntry {
                metric: metric.to_string(),
                wiki: GLOBAL_WIKI.to_string(),
                minimum_date: receipt
                    .minimum_date
                    .context("global browser partition date minimum is null")?,
                maximum_date: receipt
                    .maximum_date
                    .context("global browser partition date maximum is null")?,
                file: format!("browser-data/{metric}/all-{shard}.parquet"),
                rows: receipt.rows,
                bytes: receipt.bytes,
                sha256: receipt.artifact_sha256,
                artifact_receipt_sha256: receipt_sha256,
                scope: "global".to_string(),
                shard: Some(shard),
                aggregation_version: Some(GLOBAL_AGGREGATION_VERSION.to_string()),
            };
            if let Some(existing) = previous.get(expected.file.as_str())
                && **existing == expected
            {
                entries.push((*existing).clone());
                reused += 1;
            } else {
                entries.push(expected);
                rebuilt += 1;
            }
        }
    }
    entries.sort_by(|left, right| {
        (&left.metric, &left.wiki, &left.minimum_date, &left.file).cmp(&(
            &right.metric,
            &right.wiki,
            &right.minimum_date,
            &right.file,
        ))
    });
    for definition in browser_metrics() {
        let metric = definition.name;
        ensure!(
            entries.iter().any(|entry| entry.metric == metric),
            "browser data index has no partition for metric {metric}"
        );
    }
    let seed = IndexSeed {
        schema_version: INDEX_SCHEMA_VERSION,
        cache_schema_version: CACHE_SCHEMA_VERSION,
        entries: &entries,
    };
    let generation = hex::encode(Sha256::digest(serde_json::to_vec(&seed)?));
    Ok((
        BrowserDataIndex {
            schema_version: INDEX_SCHEMA_VERSION,
            cache_schema_version: CACHE_SCHEMA_VERSION,
            generation,
            license_spdx: licensing::ARTIFACT_LICENSE_SPDX.to_string(),
            entries,
        },
        reused,
        rebuilt,
    ))
}

fn materialize_global_partitions(
    output_dir: &Path,
    allowlist: Option<&BTreeSet<String>>,
) -> Result<()> {
    let staging = output_dir.join(format!(".{GLOBAL_ROOT}.{}.tmp", std::process::id()));
    if staging.exists() {
        fs::remove_dir_all(&staging)?;
    }
    fs::create_dir_all(&staging)?;
    let result = (|| -> Result<()> {
        for definition in browser_metrics() {
            let metric = definition.name;
            let date_column = definition
                .date_column
                .expect("browser-partitioned metrics have a date column");
            let source = output_dir.join(format!("{metric}.parquet"));
            if !source.is_file() {
                continue;
            }
            let frame = ParquetReader::new(File::open(&source)?)
                .set_low_memory(true)
                .read_parallel(ParallelStrategy::None)
                .finish()?;
            let frame = if let Some(allowed) = allowlist {
                let allowed = allowed.iter().cloned().collect::<Vec<_>>();
                frame
                    .lazy()
                    .filter(col("wiki").is_in(
                        lit(Series::new("allowed".into(), allowed)).implode(false),
                        false,
                    ))
                    .collect()?
            } else {
                frame
            };
            ensure!(frame.height() > 0, "global {metric} input is empty");
            let aggregate = aggregate_global_metric(metric, frame)?;
            write_year_shards(&staging, metric, date_column, &aggregate)?;
        }
        Ok(())
    })();
    if let Err(error) = result {
        let _ = fs::remove_dir_all(&staging);
        return Err(error);
    }
    let destination = output_dir.join(GLOBAL_ROOT);
    let backup = output_dir.join(format!(".{GLOBAL_ROOT}.{}.backup", std::process::id()));
    publish_global_directory(&staging, &destination, &backup, |from, to| {
        fs::rename(from, to)
    })?;
    File::open(output_dir)?.sync_all()?;
    Ok(())
}

fn publish_global_directory(
    staging: &Path,
    destination: &Path,
    backup: &Path,
    mut rename: impl FnMut(&Path, &Path) -> std::io::Result<()>,
) -> Result<()> {
    if backup.exists() {
        fs::remove_dir_all(backup)?;
    }
    if destination.exists() {
        rename(destination, backup)?;
    }
    if let Err(error) = rename(staging, destination) {
        if backup.exists() {
            let _ = rename(backup, destination);
        }
        return Err(error).context("failed to publish global browser partitions");
    }
    if backup.exists() {
        fs::remove_dir_all(backup)?;
    }
    Ok(())
}

fn aggregate_global_metric(metric: &str, frame: DataFrame) -> Result<DataFrame> {
    const FUNNEL_SUMS: &[&str] = &["cohort_size", "reached_5", "reached_25", "reached_100"];
    const GDP_KEYS: &[&str] = &["year_month", "page_namespace", "user_type"];
    const GDP_SUMS: &[&str] = &[
        "gross_bytes_added",
        "net_bytes",
        "total_edits",
        "productive_edits",
        "reverted_edits",
        "unique_editors",
        "minor_edits",
    ];
    const ACTIVITY_KEYS: &[&str] = &[
        "year_month",
        "period",
        "period_start",
        "period_end",
        "period_type",
        "period_months",
        "user_type",
        "activity_tier",
        "tier_rank",
    ];
    const ACTIVITY_SUMS: &[&str] = &["editors", "total_edits", "net_bytes", "gross_bytes"];
    const SHARE_KEYS: &[&str] = &["year_month", "user_type"];
    const SHARE_SUMS: &[&str] = &["edits", "net_bytes", "editors"];
    const CHURN_KEYS: &[&str] = &["period", "period_type"];
    const CHURN_SUMS: &[&str] = &["active_editors", "arrivals", "departures"];
    const COHORT_KEYS: &[&str] = &["cohort_year", "year"];
    const COHORT_SUMS: &[&str] = &["survived_editors", "initial_editors"];
    const LABOR_SUMS: &[&str] = &[
        "unique_editors",
        "total_edits",
        "net_bytes",
        "reverted_edits",
    ];
    let metric_id = metric.parse::<MetricId>()?;
    let result = match metric_id {
        MetricId::BusinessFunnel => aggregate_sums(frame, &["cohort_year"], FUNNEL_SUMS)?,
        MetricId::Gdp => aggregate_sums(frame, GDP_KEYS, GDP_SUMS)?
            .lazy()
            .with_columns([
                safe_ratio("net_bytes", "total_edits", "bytes_per_edit"),
                safe_ratio("net_bytes", "unique_editors", "bytes_per_editor"),
                safe_ratio("reverted_edits", "total_edits", "revert_rate"),
            ])
            .collect()?,
        MetricId::GdpActivityTiers => aggregate_sums(frame, ACTIVITY_KEYS, ACTIVITY_SUMS)?,
        MetricId::GdpUserTypeShare => aggregate_sums(frame, SHARE_KEYS, SHARE_SUMS)?,
        MetricId::LaborChurn => aggregate_sums(frame, CHURN_KEYS, CHURN_SUMS)?
            .lazy()
            .with_columns([
                safe_ratio("arrivals", "active_editors", "arrival_rate"),
                safe_ratio("departures", "active_editors", "departure_rate"),
            ])
            .collect()?,
        MetricId::LaborCohorts => aggregate_sums(frame, COHORT_KEYS, COHORT_SUMS)?,
        MetricId::LaborMonthly => aggregate_sums(frame, GDP_KEYS, LABOR_SUMS)?,
        MetricId::Inequality => aggregate_global_inequality(frame)?,
        MetricId::Patrol => aggregate_global_patrol(frame)?,
        MetricId::PageWeeklyEdits => {
            anyhow::bail!("page-week data is not globally browser-composable")
        }
    };
    let sort_columns = result
        .get_column_names()
        .iter()
        .filter(|name| name.as_str() != "wiki")
        .map(|name| col(name.as_str()))
        .collect::<Vec<_>>();
    result
        .lazy()
        .sort_by_exprs(sort_columns, SortMultipleOptions::default())
        .collect()
        .map_err(anyhow::Error::from)
}

fn aggregate_sums(frame: DataFrame, keys: &[&str], sums: &[&str]) -> Result<DataFrame> {
    frame
        .lazy()
        .group_by(keys.iter().map(|name| col(*name)).collect::<Vec<_>>())
        .agg(
            sums.iter()
                .map(|name| col(*name).cast(DataType::Int64).sum().alias(*name))
                .collect::<Vec<_>>(),
        )
        .with_columns([lit(GLOBAL_WIKI).alias("wiki")])
        .collect()
        .map_err(anyhow::Error::from)
}

fn safe_ratio(numerator: &'static str, denominator: &'static str, output: &'static str) -> Expr {
    when(col(denominator).neq(lit(0_i64)))
        .then(col(numerator).cast(DataType::Float64) / col(denominator).cast(DataType::Float64))
        .otherwise(lit(0.0_f64))
        .alias(output)
}

fn aggregate_global_inequality(frame: DataFrame) -> Result<DataFrame> {
    let edits = col("total_edits").cast(DataType::Float64);
    let editors = col("total_editors").cast(DataType::Float64);
    frame
        .lazy()
        .with_columns([
            (edits.clone() * col("theil")).alias("_within_theil"),
            (edits.clone() * (edits.clone() / editors.clone()).log(lit(std::f64::consts::E)))
                .alias("_mean_log"),
        ])
        .group_by([
            col("year_month"),
            col("period"),
            col("period_start"),
            col("period_end"),
            col("period_type"),
            col("period_months"),
            col("user_type"),
        ])
        .agg([
            col("total_editors").cast(DataType::Int64).sum(),
            col("total_edits").cast(DataType::Int64).sum(),
            col("_within_theil").sum(),
            col("_mean_log").sum(),
        ])
        .with_columns([
            lit(NULL).cast(DataType::Float64).alias("gini"),
            ((col("_within_theil") + col("_mean_log")
                - col("total_edits").cast(DataType::Float64)
                    * (col("total_edits").cast(DataType::Float64)
                        / col("total_editors").cast(DataType::Float64))
                    .log(lit(std::f64::consts::E)))
                / col("total_edits").cast(DataType::Float64))
            .alias("theil"),
            lit(NULL).cast(DataType::Float64).alias("palma"),
            lit(NULL).cast(DataType::Int64).alias("min_editors_50pct"),
            lit(GLOBAL_WIKI).alias("wiki"),
        ])
        .select([
            col("year_month"),
            col("period"),
            col("period_start"),
            col("period_end"),
            col("period_type"),
            col("period_months"),
            col("user_type"),
            col("gini"),
            col("theil"),
            col("palma"),
            col("min_editors_50pct"),
            col("total_editors"),
            col("total_edits"),
            col("wiki"),
        ])
        .collect()
        .map_err(anyhow::Error::from)
}

fn aggregate_global_patrol(frame: DataFrame) -> Result<DataFrame> {
    const PATROL_KEYS: &[&str] = &["year_month", "page_namespace", "user_type"];
    const PATROL_SUMS: &[&str] = &[
        "total_patrols",
        "patrol_new_pages",
        "patrol_diffs",
        "patrolled_revisions",
        "autopatrolled_revisions",
        "total_revisions",
    ];
    let aggregate_result = aggregate_sums(frame, PATROL_KEYS, PATROL_SUMS);
    let aggregate = aggregate_result?;
    aggregate
        .lazy()
        .with_columns([
            lit(NULL).cast(DataType::Int64).alias("unique_patrollers"),
            lit(NULL)
                .cast(DataType::Float64)
                .alias("median_latency_hours"),
            lit(NULL).cast(DataType::Float64).alias("p90_latency_hours"),
            safe_ratio("patrolled_revisions", "total_revisions", "_patrol_fraction"),
            safe_ratio(
                "autopatrolled_revisions",
                "total_revisions",
                "_autopatrol_fraction",
            ),
            lit(NULL).cast(DataType::Float64).alias("top1_pct"),
            lit(NULL)
                .cast(DataType::Int64)
                .alias("min_patrollers_50pct"),
        ])
        .with_columns([
            (col("_patrol_fraction") * lit(100.0_f64)).alias("patrol_coverage_pct"),
            ((col("_patrol_fraction") + col("_autopatrol_fraction")) * lit(100.0_f64))
                .alias("adjusted_coverage_pct"),
        ])
        .select([
            col("year_month"),
            col("wiki"),
            col("page_namespace"),
            col("user_type"),
            col("total_patrols"),
            col("unique_patrollers"),
            col("patrol_new_pages"),
            col("patrol_diffs"),
            col("median_latency_hours"),
            col("p90_latency_hours"),
            col("patrolled_revisions"),
            col("autopatrolled_revisions"),
            col("total_revisions"),
            col("patrol_coverage_pct"),
            col("adjusted_coverage_pct"),
            col("top1_pct"),
            col("min_patrollers_50pct"),
        ])
        .collect()
        .map_err(anyhow::Error::from)
}

fn write_year_shards(
    staging: &Path,
    metric: &str,
    date_column: &str,
    frame: &DataFrame,
) -> Result<Vec<PathBuf>> {
    let mut years = BTreeSet::new();
    let dates = frame.column(date_column)?;
    for row in 0..frame.height() {
        let value = dates.get(row)?;
        let value = match value {
            AnyValue::String(value) => value,
            AnyValue::StringOwned(ref value) => value.as_str(),
            AnyValue::Null => continue,
            value => anyhow::bail!("global {metric} date is not a string: {value:?}"),
        };
        ensure!(value.len() >= 4, "global {metric} date is too short");
        years.insert(value[..4].to_string());
    }
    ensure!(!years.is_empty(), "global {metric} has no dated rows");
    let directory = staging.join(metric);
    fs::create_dir_all(&directory)?;
    let mut paths = Vec::new();
    for year in years {
        let mask = BooleanChunked::from_iter((0..frame.height()).map(|row| {
            frame
                .column(date_column)
                .ok()
                .and_then(|column| column.get(row).ok())
                .and_then(|value| match value {
                    AnyValue::String(value) => Some(value.starts_with(&year)),
                    AnyValue::StringOwned(value) => Some(value.as_str().starts_with(&year)),
                    _ => None,
                })
                .unwrap_or(false)
        }));
        let mut shard = frame.filter(&mask)?;
        ensure!(shard.height() > 0, "global {metric}/{year} shard is empty");
        let path = directory.join(format!("{year}.parquet"));
        ParquetWriter::new(File::create(&path)?)
            .set_parallel(false)
            .finish(&mut shard)?;
        let identity = format!("{GLOBAL_ROOT}/{metric}/{year}.parquet");
        let receipt = artifact_receipt::scan_and_write_with_spec(
            &path,
            &identity,
            GLOBAL_AGGREGATION_VERSION,
            "publication-ready-receipts",
            artifact_receipt::SemanticSpec {
                date_column: Some(date_column.to_string()),
                conservation_columns: Vec::new(),
                ordering_contract: "global-time-shard/v1".to_string(),
                page_week_consistency: false,
            },
        );
        receipt?;
        paths.push(path);
    }
    Ok(paths)
}

pub fn materialize(output_dir: &Path, allowlist: Option<&BTreeSet<String>>) -> Result<()> {
    let destination = output_dir.join(INDEX_FILENAME);
    let previous = if destination.is_file() {
        match read_index(&destination) {
            Ok(index) => Some(index),
            Err(error) => {
                tracing::warn!(path = %destination.display(), error = %error, "rebuilding an obsolete or invalid derived browser index");
                None
            }
        }
    } else {
        None
    };
    let (index, reused, rebuilt) =
        build_index_with_previous(output_dir, allowlist, previous.as_ref())?;
    let temporary = output_dir.join(format!(".{INDEX_FILENAME}.{}.tmp", std::process::id()));
    let result = (|| -> Result<()> {
        let mut file = File::create(&temporary)?;
        serde_json::to_writer_pretty(&mut file, &index)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        fs::rename(&temporary, &destination)?;
        File::open(output_dir)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result.with_context(|| format!("failed to publish {}", destination.display()))?;
    tracing::info!(reused, rebuilt, "published incremental browser data index");
    Ok(())
}

pub fn read_index(path: &Path) -> Result<BrowserDataIndex> {
    let index: BrowserDataIndex = serde_json::from_slice(&fs::read(path)?)?;
    ensure!(
        index.schema_version == INDEX_SCHEMA_VERSION
            && index.cache_schema_version == CACHE_SCHEMA_VERSION
            && index.license_spdx == licensing::ARTIFACT_LICENSE_SPDX
            && !index.entries.is_empty(),
        "invalid browser data index"
    );
    let seed = IndexSeed {
        schema_version: index.schema_version,
        cache_schema_version: index.cache_schema_version,
        entries: &index.entries,
    };
    ensure!(
        index.generation == hex::encode(Sha256::digest(serde_json::to_vec(&seed)?)),
        "browser data index generation does not match its entries"
    );
    Ok(index)
}

pub fn validate(output_dir: &Path, allowlist: Option<&BTreeSet<String>>) -> Result<()> {
    let recorded = read_index(&output_dir.join(INDEX_FILENAME))?;
    let expected = build_index(output_dir, allowlist)?;
    ensure!(
        recorded == expected,
        "browser data index does not match current per-wiki outputs"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::TestDir;

    fn write_metric(root: &Path, wiki: &str, metric: &str, date_column: &str) -> Result<()> {
        let directory = root.join(wiki);
        fs::create_dir_all(&directory)?;
        let mut frame = df!(
            "wiki" => &[wiki, wiki],
            date_column => &["2025-12", "2026-01"],
            "value" => &[1_i64, 2],
        )
        .expect("browser metric fixture columns have equal lengths");
        let mut file = File::create(directory.join(format!("{metric}.parquet")))?;
        ParquetWriter::new(&mut file)
            .set_parallel(false)
            .finish(&mut frame)?;
        Ok(())
    }

    fn write_complete_wiki(root: &Path, wiki: &str) -> Result<()> {
        for definition in browser_metrics() {
            write_metric(
                root,
                wiki,
                definition.name,
                definition
                    .date_column
                    .expect("browser metric should have a date column"),
            )?;
        }
        Ok(())
    }

    fn global_source(metric: &str) -> Result<DataFrame> {
        let wiki = &["nlwiki", "ptwiki"];
        let frame = match metric {
            "business_funnel" => df!(
                "wiki" => wiki,
                "cohort_year" => &["2025", "2026"],
                "cohort_size" => &[10_i64, 20],
                "reached_5" => &[8_i64, 16],
                "reached_25" => &[4_i64, 8],
                "reached_100" => &[2_i64, 4],
            ),
            "gdp" => df!(
                "wiki" => wiki,
                "year_month" => &["2025-12", "2026-01"],
                "page_namespace" => &[0_i32, 0],
                "user_type" => &["registered", "registered"],
                "gross_bytes_added" => &[100_i64, 200],
                "net_bytes" => &[50_i64, 100],
                "total_edits" => &[10_i64, 0],
                "productive_edits" => &[8_i64, 16],
                "reverted_edits" => &[2_i64, 0],
                "unique_editors" => &[5_i64, 0],
                "minor_edits" => &[1_i64, 2],
            ),
            "gdp_activity_tiers" => df!(
                "wiki" => wiki,
                "year_month" => &["2025-12", "2026-01"],
                "period" => &["2025", "2026"],
                "period_start" => &["2025-01", "2026-01"],
                "period_end" => &["2025-12", "2026-12"],
                "period_type" => &["year", "year"],
                "period_months" => &[12_i64, 12],
                "user_type" => &["registered", "registered"],
                "activity_tier" => &["1-1200 edits", "1201+ edits"],
                "tier_rank" => &[1_i64, 2],
                "editors" => &[5_i64, 6],
                "total_edits" => &[20_i64, 30],
                "net_bytes" => &[40_i64, 50],
                "gross_bytes" => &[60_i64, 70],
            ),
            "gdp_user_type_share" => df!(
                "wiki" => wiki,
                "year_month" => &["2025-12", "2026-01"],
                "user_type" => &["registered", "registered"],
                "edits" => &[10_i64, 20],
                "net_bytes" => &[30_i64, 40],
                "editors" => &[5_i64, 6],
            ),
            "inequality" => df!(
                "wiki" => wiki,
                "year_month" => &["2025-12", "2026-01"],
                "period" => &["2025", "2026"],
                "period_start" => &["2025-01", "2026-01"],
                "period_end" => &["2025-12", "2026-12"],
                "period_type" => &["year", "year"],
                "period_months" => &[12_i64, 12],
                "user_type" => &["registered", "registered"],
                "gini" => &[0.3_f64, 0.4],
                "theil" => &[0.2_f64, 0.25],
                "palma" => &[1.1_f64, 1.2],
                "min_editors_50pct" => &[2_i64, 3],
                "total_editors" => &[10_i64, 20],
                "total_edits" => &[100_i64, 200],
            ),
            "labor_churn" => df!(
                "wiki" => wiki,
                "period" => &["2025-12", "2026-01"],
                "period_type" => &["month", "month"],
                "active_editors" => &[10_i64, 0],
                "arrivals" => &[2_i64, 0],
                "departures" => &[1_i64, 0],
            ),
            "labor_cohorts" => df!(
                "wiki" => wiki,
                "cohort_year" => &["2025", "2026"],
                "year" => &["2025", "2026"],
                "survived_editors" => &[8_i64, 16],
                "initial_editors" => &[10_i64, 20],
            ),
            "labor_monthly" => df!(
                "wiki" => wiki,
                "year_month" => &["2025-12", "2026-01"],
                "page_namespace" => &[0_i32, 0],
                "user_type" => &["registered", "registered"],
                "unique_editors" => &[10_i64, 20],
                "total_edits" => &[100_i64, 200],
                "net_bytes" => &[50_i64, 100],
                "reverted_edits" => &[2_i64, 4],
            ),
            "patrol" => df!(
                "wiki" => wiki,
                "year_month" => &["2025-12", "2026-01"],
                "page_namespace" => &[0_i32, 0],
                "user_type" => &["registered", "registered"],
                "total_patrols" => &[10_i64, 20],
                "unique_patrollers" => &[2_i64, 4],
                "patrol_new_pages" => &[3_i64, 6],
                "patrol_diffs" => &[7_i64, 14],
                "patrolled_revisions" => &[8_i64, 0],
                "autopatrolled_revisions" => &[1_i64, 0],
                "total_revisions" => &[10_i64, 0],
            ),
            _ => return Err(anyhow::anyhow!("unsupported fixture metric {metric}")),
        };
        frame.map_err(anyhow::Error::from)
    }

    fn write_global_sources(root: &Path) -> Result<()> {
        for definition in browser_metrics() {
            let mut frame = global_source(definition.name)?;
            ParquetWriter::new(File::create(root.join(definition.id.parquet_name()))?)
                .set_parallel(false)
                .finish(&mut frame)?;
        }
        Ok(())
    }

    #[test]
    fn index_is_deterministic_partitioned_and_excludes_weekly_data() -> Result<()> {
        let first = TestDir::new()?;
        let second = TestDir::new()?;
        for root in [first.path(), second.path()] {
            write_complete_wiki(root, "nlwiki")?;
            write_complete_wiki(root, "ptwiki")?;
            write_metric(root, "nlwiki", "page_weekly_edits", "week_start")?;
            materialize(root, None)?;
        }
        assert_eq!(
            fs::read(first.path().join(INDEX_FILENAME))?,
            fs::read(second.path().join(INDEX_FILENAME))?
        );
        let index = read_index(&first.path().join(INDEX_FILENAME))?;
        assert_eq!(index.entries.len(), browser_metrics().count() * 2);
        assert!(
            index
                .entries
                .iter()
                .all(|entry| entry.wiki == "nlwiki" || entry.wiki == "ptwiki")
        );
        assert!(
            index
                .entries
                .iter()
                .all(|entry| entry.metric != "page_weekly_edits")
        );
        assert!(
            index
                .entries
                .iter()
                .all(|entry| entry.file.contains(&entry.wiki))
        );
        Ok(())
    }

    #[test]
    fn index_reuses_only_entries_with_the_same_artifact_receipt() -> Result<()> {
        let output = TestDir::new()?;
        write_complete_wiki(output.path(), "nlwiki")?;
        let allowlist = BTreeSet::from(["nlwiki".to_string()]);
        materialize(output.path(), Some(&allowlist))?;
        let previous = read_index(&output.path().join(INDEX_FILENAME))?;
        let (_, reused, rebuilt) =
            build_index_with_previous(output.path(), Some(&allowlist), Some(&previous))?;
        assert_eq!((reused, rebuilt), (browser_metrics().count(), 0));

        let changed = output.path().join("nlwiki/gdp.parquet");
        fs::remove_file(artifact_receipt::sidecar_path(&changed)?)?;
        let mut changed_frame = df!(
            "wiki" => &["nlwiki", "nlwiki", "nlwiki"],
            "year_month" => &["2025-12", "2026-01", "2026-02"],
            "value" => &[1_i64, 2, 3],
        )
        .expect("changed browser fixture should be valid");
        ParquetWriter::new(File::create(&changed)?)
            .set_parallel(false)
            .finish(&mut changed_frame)?;
        let (_, reused, rebuilt) =
            build_index_with_previous(output.path(), Some(&allowlist), Some(&previous))?;
        assert_eq!((reused, rebuilt), (browser_metrics().count() - 1, 1));

        fs::write(
            output.path().join(INDEX_FILENAME),
            b"{\"schema_version\":1}",
        )
        .expect("obsolete browser index should be writable");
        materialize(output.path(), Some(&allowlist))?;
        let rebuilt = read_index(&output.path().join(INDEX_FILENAME))?;
        assert_eq!(rebuilt.schema_version, INDEX_SCHEMA_VERSION);
        assert_eq!(rebuilt.entries.len(), browser_metrics().count());
        Ok(())
    }

    #[test]
    fn index_respects_allowlist_and_rejects_mixed_wiki_partitions() -> Result<()> {
        let output = TestDir::new()?;
        write_complete_wiki(output.path(), "nlwiki")?;
        write_complete_wiki(output.path(), "ptwiki")?;
        materialize(output.path(), Some(&BTreeSet::from(["ptwiki".to_string()])))?;
        let index = read_index(&output.path().join(INDEX_FILENAME))?;
        assert!(index.entries.iter().all(|entry| entry.wiki == "ptwiki"));

        write_metric(output.path(), "nlwiki", "gdp", "year_month")?;
        let path = output.path().join("nlwiki/gdp.parquet");
        let mut mixed = df!(
            "wiki" => &["nlwiki", "ptwiki"],
            "year_month" => &["2025-12", "2026-01"],
            "value" => &[1_i64, 2],
        )
        .expect("mixed-wiki fixture columns have equal lengths");
        let mut file = File::create(&path)?;
        ParquetWriter::new(&mut file)
            .set_parallel(false)
            .finish(&mut mixed)?;
        assert!(materialize(output.path(), Some(&BTreeSet::from(["nlwiki".to_string()]))).is_err());

        let mut null_date = df!(
            "wiki" => &["nlwiki"],
            "year_month" => &[None::<&str>],
            "value" => &[1_i64],
        )
        .expect("null-date fixture columns have equal lengths");
        ParquetWriter::new(File::create(&path)?)
            .set_parallel(false)
            .finish(&mut null_date)?;
        assert!(materialize(output.path(), Some(&BTreeSet::from(["nlwiki".to_string()]))).is_err());

        let mut null_wiki = df!(
            "wiki" => &[None::<&str>],
            "year_month" => &["2026-01"],
            "value" => &[1_i64],
        )
        .expect("null-wiki fixture columns have equal lengths");
        ParquetWriter::new(File::create(&path)?)
            .set_parallel(false)
            .finish(&mut null_wiki)?;
        assert!(materialize(output.path(), Some(&BTreeSet::from(["nlwiki".to_string()]))).is_err());
        Ok(())
    }

    #[test]
    fn index_allows_metric_specific_wiki_coverage() -> Result<()> {
        let output = TestDir::new()?;
        write_complete_wiki(output.path(), "nlwiki")?;
        write_complete_wiki(output.path(), "svwiki")?;
        fs::remove_file(output.path().join("svwiki/patrol.parquet"))?;
        materialize(output.path(), None)?;
        let index = read_index(&output.path().join(INDEX_FILENAME))?;
        assert!(
            index
                .entries
                .iter()
                .any(|entry| entry.metric == "patrol" && entry.wiki == "nlwiki")
        );
        assert!(
            !index
                .entries
                .iter()
                .any(|entry| entry.metric == "patrol" && entry.wiki == "svwiki")
        );
        assert!(
            index
                .entries
                .iter()
                .any(|entry| entry.metric == "gdp" && entry.wiki == "svwiki")
        );
        Ok(())
    }

    #[test]
    fn empty_partition_and_interrupted_index_publication_fail_closed() -> Result<()> {
        let empty = TestDir::new()?;
        write_complete_wiki(empty.path(), "nlwiki")?;
        let mut empty_frame = df!(
            "wiki" => Vec::<String>::new(),
            "year_month" => Vec::<String>::new(),
            "value" => Vec::<i64>::new(),
        )
        .expect("empty browser fixture has a valid schema");
        ParquetWriter::new(File::create(empty.path().join("nlwiki/gdp.parquet"))?)
            .finish(&mut empty_frame)?;
        assert!(materialize(empty.path(), None).is_err());

        let interrupted = TestDir::new()?;
        write_complete_wiki(interrupted.path(), "nlwiki")?;
        fs::create_dir(interrupted.path().join(INDEX_FILENAME))?;
        assert!(materialize(interrupted.path(), None).is_err());
        assert!(
            !interrupted
                .path()
                .join(format!(".{INDEX_FILENAME}.{}.tmp", std::process::id()))
                .exists()
        );
        Ok(())
    }

    #[test]
    fn global_gdp_productivity_uses_net_bytes() -> Result<()> {
        let source = df!(
            "wiki" => &["nlwiki", "ptwiki"],
            "year_month" => &["2026-01", "2026-01"],
            "page_namespace" => &[0_i32, 0],
            "user_type" => &["registered", "registered"],
            "gross_bytes_added" => &[100_i64, 200],
            "net_bytes" => &[40_i64, 20],
            "total_edits" => &[2_i64, 1],
            "productive_edits" => &[2_i64, 1],
            "reverted_edits" => &[0_i64, 0],
            "unique_editors" => &[1_i64, 1],
            "minor_edits" => &[0_i64, 0],
        )?;
        let result = aggregate_global_metric("gdp", source)?;
        assert_eq!(result.height(), 1);
        assert_eq!(result.column("bytes_per_edit")?.f64()?.get(0), Some(20.0));
        assert_eq!(result.column("bytes_per_editor")?.f64()?.get(0), Some(30.0));
        Ok(())
    }

    #[test]
    fn global_patrol_keeps_only_additive_statistics() -> Result<()> {
        let source = df!(
            "wiki" => &["nlwiki", "ptwiki"],
            "year_month" => &["2026-01", "2026-01"],
            "page_namespace" => &[0_i32, 0],
            "user_type" => &["registered", "registered"],
            "total_patrols" => &[10_i64, 20],
            "unique_patrollers" => &[2_i64, 4],
            "patrol_new_pages" => &[3_i64, 6],
            "patrol_diffs" => &[7_i64, 14],
            "patrolled_revisions" => &[8_i64, 4],
            "autopatrolled_revisions" => &[1_i64, 2],
            "total_revisions" => &[10_i64, 10],
        )?;
        let result = aggregate_global_metric("patrol", source)?;
        assert_eq!(result.height(), 1);
        assert_eq!(result.column("total_patrols")?.i64()?.get(0), Some(30));
        assert_eq!(result.column("unique_patrollers")?.i64()?.get(0), None);
        assert_eq!(
            result.column("patrol_coverage_pct")?.f64()?.get(0),
            Some(60.0)
        );
        assert_eq!(
            result.column("adjusted_coverage_pct")?.f64()?.get(0),
            Some(75.0)
        );
        Ok(())
    }

    #[test]
    fn global_aggregates_are_sharded_reused_and_fail_closed() -> Result<()> {
        let output = TestDir::new()?;
        write_complete_wiki(output.path(), "nlwiki")?;
        write_complete_wiki(output.path(), "ptwiki")?;
        write_global_sources(output.path())?;

        let stale_staging = output
            .path()
            .join(format!(".{GLOBAL_ROOT}.{}.tmp", std::process::id()));
        let stale_backup = output
            .path()
            .join(format!(".{GLOBAL_ROOT}.{}.backup", std::process::id()));
        fs::create_dir(&stale_staging)?;
        fs::create_dir(&stale_backup)?;
        materialize(output.path(), None)?;
        assert!(!stale_staging.exists());
        assert!(!stale_backup.exists());

        let first = read_index(&output.path().join(INDEX_FILENAME))?;
        let global = first
            .entries
            .iter()
            .filter(|entry| entry.scope == "global")
            .collect::<Vec<_>>();
        assert_eq!(global.len(), browser_metrics().count() * 2);
        assert!(global.iter().all(|entry| {
            entry.wiki == GLOBAL_WIKI
                && entry.shard.is_some()
                && entry.aggregation_version.as_deref() == Some(GLOBAL_AGGREGATION_VERSION)
        }));

        materialize(output.path(), None)?;
        assert_eq!(first, read_index(&output.path().join(INDEX_FILENAME))?);
        assert!(aggregate_global_metric("unknown", DataFrame::empty()).is_err());
        assert!(global_source("unknown").is_err());

        let invalid = TestDir::new()?;
        let mut broken = df!("wiki" => &["nlwiki"], "year_month" => &["2026-01"])?;
        ParquetWriter::new(File::create(invalid.path().join("gdp.parquet"))?)
            .finish(&mut broken)?;
        assert!(materialize_global_partitions(invalid.path(), None).is_err());
        assert!(
            !invalid
                .path()
                .join(format!(".{GLOBAL_ROOT}.{}.tmp", std::process::id()))
                .exists()
        );

        let transaction = TestDir::new()?;
        let staging = transaction.path().join("staging");
        let destination = transaction.path().join("destination");
        let backup = transaction.path().join("backup");
        fs::create_dir(&staging)?;
        fs::create_dir(&destination)?;
        fs::write(destination.join("live"), b"live")?;
        let error = publish_global_directory(&staging, &destination, &backup, |from, to| {
            if from == staging {
                Err(std::io::Error::other("injected publish failure"))
            } else {
                fs::rename(from, to)
            }
        })
        .expect_err("an interrupted global switch must fail");
        assert!(error.to_string().contains("failed to publish"));
        assert!(destination.join("live").is_file());
        assert!(!backup.exists());

        let allowlisted = TestDir::new()?;
        write_complete_wiki(allowlisted.path(), "nlwiki")?;
        write_complete_wiki(allowlisted.path(), "ptwiki")?;
        write_global_sources(allowlisted.path())?;
        materialize(
            allowlisted.path(),
            Some(&BTreeSet::from(["ptwiki".to_string()])),
        )
        .expect("allowlisted global materialization should succeed");
        let global_gdp = ParquetReader::new(
            File::open(
                allowlisted
                    .path()
                    .join(format!("{GLOBAL_ROOT}/gdp/2026.parquet")),
            )
            .expect("allowlisted global GDP shard should exist"),
        )
        .finish()?;
        assert_eq!(global_gdp.height(), 1);
        assert_eq!(
            global_gdp.column("gross_bytes_added")?.i64()?.get(0),
            Some(200)
        );
        assert_eq!(
            global_gdp.column("bytes_per_edit")?.f64()?.get(0),
            Some(0.0)
        );
        assert_eq!(
            global_gdp.column("bytes_per_editor")?.f64()?.get(0),
            Some(0.0)
        );
        assert_eq!(global_gdp.column("wiki")?.str()?.get(0), Some(GLOBAL_WIKI));
        Ok(())
    }

    #[test]
    fn global_year_shards_reject_invalid_dates_and_skip_nulls() -> Result<()> {
        let root = TestDir::new()?;
        let with_null = df!(
            "year_month" => &[Some("2026-01"), None],
            "wiki" => &[GLOBAL_WIKI, GLOBAL_WIKI],
            "value" => &[1_i64, 2],
        )
        .expect("nullable date fixture is valid");
        let paths = write_year_shards(root.path(), "fixture", "year_month", &with_null)?;
        assert_eq!(paths.len(), 1);

        let wrong_type = df!("year_month" => &[2026_i64], "wiki" => &[GLOBAL_WIKI])?;
        assert!(write_year_shards(root.path(), "bad-type", "year_month", &wrong_type).is_err());
        let too_short = df!("year_month" => &["x"], "wiki" => &[GLOBAL_WIKI])?;
        assert!(write_year_shards(root.path(), "short", "year_month", &too_short).is_err());
        let undated = df!(
            "year_month" => &[None::<&str>],
            "wiki" => &[GLOBAL_WIKI],
        )
        .expect("undated fixture is valid");
        assert!(write_year_shards(root.path(), "undated", "year_month", &undated).is_err());
        Ok(())
    }
}
