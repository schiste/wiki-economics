use anyhow::{Context, Result, ensure};
use polars::prelude::*;
use serde::Deserialize;
use serde_json::{Map, Value, json};
use std::collections::BTreeSet;
use std::fs::{self, File};
use std::path::{Path, PathBuf};

use crate::test_support::{TestDir, init_test_tracing};
use crate::{compute, merge, metric_registry::MetricFamily, patrol};

const FIXTURE_JSON: &str = include_str!("../tests/fixtures/adversarial-metric-parity.json");

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Fixture {
    schema_version: u32,
    history: Vec<HistoryRow>,
    patrol_events: Vec<PatrolEvent>,
    patrol_coverage: Vec<PatrolCoverage>,
    custom_inputs: Value,
    expected: Value,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct HistoryRow {
    wiki: String,
    timestamp: String,
    user_id: Option<i64>,
    historical_name: String,
    current_name: String,
    bot: bool,
    anonymous: bool,
    temporary: bool,
    indefinitely_blocked: bool,
    page_id: i64,
    page_title: String,
    namespace: i32,
    revision_id: i64,
    bytes_diff: i64,
    reverted: bool,
    minor: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PatrolEvent {
    wiki: String,
    year_month: String,
    namespace: i32,
    user_type: String,
    patroller: String,
    latency_hours: f64,
    new_page: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PatrolCoverage {
    wiki: String,
    year_month: String,
    namespace: i32,
    user_type: String,
    patrolled_revisions: i64,
    autopatrolled_revisions: i64,
    total_revisions: i64,
}

fn read_parquet(path: &Path) -> Result<DataFrame> {
    ParquetReader::new(File::open(path)?)
        .set_low_memory(true)
        .read_parallel(ParallelStrategy::None)
        .finish()
        .map_err(Into::into)
}

fn write_parquet(path: &Path, mut frame: DataFrame) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    ParquetWriter::new(File::create(path)?)
        .with_compression(ParquetCompression::Zstd(None))
        .set_parallel(false)
        .finish(&mut frame)?;
    Ok(())
}

fn history_frame(fixture: &Fixture, wiki: &str) -> Result<DataFrame> {
    let rows = fixture
        .history
        .iter()
        .filter(|row| row.wiki == wiki)
        .collect::<Vec<_>>();
    ensure!(!rows.is_empty(), "parity fixture has no history for {wiki}");
    let year_month = rows
        .iter()
        .map(|row| row.timestamp[..7].to_string())
        .collect::<Vec<_>>();
    let year = year_month
        .iter()
        .map(|month| month[..4].parse::<i32>())
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let year_month_key = year_month
        .iter()
        .map(|month| Ok(month[..4].parse::<i32>()? * 100 + month[5..].parse::<i32>()?))
        .collect::<Result<Vec<_>>>()?;
    let user_type = rows
        .iter()
        .map(|row| {
            if row.bot {
                "bot"
            } else if row.anonymous {
                "anonymous"
            } else if row.temporary {
                "temporary"
            } else {
                "registered"
            }
        })
        .collect::<Vec<_>>();
    DataFrame::new_infer_height(vec![
        Column::new(
            "event_timestamp".into(),
            rows.iter()
                .map(|row| row.timestamp.as_str())
                .collect::<Vec<_>>(),
        ),
        Column::new(
            "event_user_id".into(),
            rows.iter().map(|row| row.user_id).collect::<Vec<_>>(),
        ),
        Column::new(
            "event_user_text_historical".into(),
            rows.iter()
                .map(|row| row.historical_name.as_str())
                .collect::<Vec<_>>(),
        ),
        Column::new(
            "event_user_text".into(),
            rows.iter()
                .map(|row| row.current_name.as_str())
                .collect::<Vec<_>>(),
        ),
        Column::new(
            "event_user_is_bot_by".into(),
            rows.iter()
                .map(|row| row.bot.then_some("fixture-bot"))
                .collect::<Vec<_>>(),
        ),
        Column::new(
            "event_user_is_anonymous".into(),
            rows.iter().map(|row| row.anonymous).collect::<Vec<_>>(),
        ),
        Column::new(
            "event_user_is_temporary".into(),
            rows.iter().map(|row| row.temporary).collect::<Vec<_>>(),
        ),
        Column::new(
            "is_indefinitely_blocked".into(),
            rows.iter()
                .map(|row| row.indefinitely_blocked)
                .collect::<Vec<_>>(),
        ),
        Column::new(
            "page_id".into(),
            rows.iter().map(|row| row.page_id).collect::<Vec<_>>(),
        ),
        Column::new(
            "page_title".into(),
            rows.iter()
                .map(|row| row.page_title.as_str())
                .collect::<Vec<_>>(),
        ),
        Column::new(
            "page_namespace".into(),
            rows.iter().map(|row| row.namespace).collect::<Vec<_>>(),
        ),
        Column::new(
            "revision_id".into(),
            rows.iter().map(|row| row.revision_id).collect::<Vec<_>>(),
        ),
        Column::new(
            "revision_text_bytes_diff".into(),
            rows.iter().map(|row| row.bytes_diff).collect::<Vec<_>>(),
        ),
        Column::new(
            "is_reverted".into(),
            rows.iter().map(|row| row.reverted).collect::<Vec<_>>(),
        ),
        Column::new(
            "is_minor".into(),
            rows.iter().map(|row| row.minor).collect::<Vec<_>>(),
        ),
        Column::new("year_month".into(), year_month),
        Column::new("year".into(), year),
        Column::new("year_month_key".into(), year_month_key),
        Column::new("user_type".into(), user_type),
    ])
    .map_err(Into::into)
}

fn patrol_event_frame(fixture: &Fixture) -> Result<DataFrame> {
    DataFrame::new_infer_height(vec![
        Column::new(
            "wiki".into(),
            fixture
                .patrol_events
                .iter()
                .map(|row| row.wiki.as_str())
                .collect::<Vec<_>>(),
        ),
        Column::new(
            "year_month".into(),
            fixture
                .patrol_events
                .iter()
                .map(|row| row.year_month.as_str())
                .collect::<Vec<_>>(),
        ),
        Column::new(
            "page_namespace".into(),
            fixture
                .patrol_events
                .iter()
                .map(|row| row.namespace)
                .collect::<Vec<_>>(),
        ),
        Column::new(
            "user_type".into(),
            fixture
                .patrol_events
                .iter()
                .map(|row| row.user_type.as_str())
                .collect::<Vec<_>>(),
        ),
        Column::new(
            "patroller".into(),
            fixture
                .patrol_events
                .iter()
                .map(|row| row.patroller.as_str())
                .collect::<Vec<_>>(),
        ),
        Column::new(
            "latency_hours".into(),
            fixture
                .patrol_events
                .iter()
                .map(|row| row.latency_hours)
                .collect::<Vec<_>>(),
        ),
        Column::new(
            "new_page".into(),
            fixture
                .patrol_events
                .iter()
                .map(|row| row.new_page)
                .collect::<Vec<_>>(),
        ),
    ])
    .map_err(Into::into)
}

fn patrol_coverage_frame(fixture: &Fixture) -> Result<DataFrame> {
    DataFrame::new_infer_height(vec![
        Column::new(
            "wiki".into(),
            fixture
                .patrol_coverage
                .iter()
                .map(|row| row.wiki.as_str())
                .collect::<Vec<_>>(),
        ),
        Column::new(
            "year_month".into(),
            fixture
                .patrol_coverage
                .iter()
                .map(|row| row.year_month.as_str())
                .collect::<Vec<_>>(),
        ),
        Column::new(
            "page_namespace".into(),
            fixture
                .patrol_coverage
                .iter()
                .map(|row| row.namespace)
                .collect::<Vec<_>>(),
        ),
        Column::new(
            "user_type".into(),
            fixture
                .patrol_coverage
                .iter()
                .map(|row| row.user_type.as_str())
                .collect::<Vec<_>>(),
        ),
        Column::new(
            "patrolled_revisions".into(),
            fixture
                .patrol_coverage
                .iter()
                .map(|row| row.patrolled_revisions)
                .collect::<Vec<_>>(),
        ),
        Column::new(
            "autopatrolled_revisions".into(),
            fixture
                .patrol_coverage
                .iter()
                .map(|row| row.autopatrolled_revisions)
                .collect::<Vec<_>>(),
        ),
        Column::new(
            "total_revisions".into(),
            fixture
                .patrol_coverage
                .iter()
                .map(|row| row.total_revisions)
                .collect::<Vec<_>>(),
        ),
    ])
    .map_err(Into::into)
}

fn number(frame: &DataFrame, column: &str, row: usize) -> Result<Option<f64>> {
    Ok(frame
        .column(column)?
        .cast(&DataType::Float64)?
        .f64()?
        .get(row))
}

fn sum(frame: &DataFrame, column: &str) -> Result<f64> {
    Ok(frame
        .column(column)?
        .cast(&DataType::Float64)?
        .f64()?
        .sum()
        .unwrap_or_default())
}

fn selected(frame: &DataFrame, filter: Expr) -> Result<DataFrame> {
    frame
        .clone()
        .lazy()
        .filter(filter)
        .collect()
        .map_err(Into::into)
}

fn layer_summary(
    gdp: &DataFrame,
    activity: &DataFrame,
    inequality: &DataFrame,
    patrol: &DataFrame,
) -> Result<Value> {
    let gdp = selected(
        gdp,
        col("year_month")
            .str()
            .starts_with(lit("2024"))
            .and(col("page_namespace").eq(lit(0_i32)))
            .and(col("user_type").eq(lit("registered"))),
    )?;
    let net_bytes = sum(&gdp, "net_bytes")?;
    let total_edits = sum(&gdp, "total_edits")?;

    let activity = selected(
        activity,
        col("period")
            .eq(lit("2024"))
            .and(col("period_type").eq(lit("year")))
            .and(col("user_type").eq(lit("registered"))),
    )?;

    let inequality = selected(
        inequality,
        col("period")
            .eq(lit("2024"))
            .and(col("period_type").eq(lit("year")))
            .and(col("user_type").eq(lit("registered"))),
    )?;
    let inequality_editors = sum(&inequality, "total_editors")?;
    let inequality_edits = sum(&inequality, "total_edits")?;
    let mut within_theil = 0.0;
    let mut edit_mean_log = 0.0;
    for row in 0..inequality.height() {
        let editors = number(&inequality, "total_editors", row)?.unwrap_or_default();
        let edits = number(&inequality, "total_edits", row)?.unwrap_or_default();
        if editors > 0.0 && edits > 0.0 {
            within_theil += edits * number(&inequality, "theil", row)?.unwrap_or_default();
            edit_mean_log += edits * (edits / editors).ln();
        }
    }
    let composed_theil = (inequality_edits > 0.0 && inequality_editors > 0.0).then(|| {
        (within_theil + edit_mean_log
            - inequality_edits * (inequality_edits / inequality_editors).ln())
            / inequality_edits
    });
    let one_inequality = inequality.height() == 1;

    let patrol = selected(
        patrol,
        col("year_month")
            .str()
            .starts_with(lit("2024"))
            .and(col("page_namespace").eq(lit(0_i32)))
            .and(col("user_type").eq(lit("registered"))),
    )?;
    let patrolled_revisions = sum(&patrol, "patrolled_revisions")?;
    let autopatrolled_revisions = sum(&patrol, "autopatrolled_revisions")?;
    let total_revisions = sum(&patrol, "total_revisions")?;
    let one_patrol = patrol.height() == 1;

    Ok(json!({
        "gdp": {
            "period": "2024",
            "gross_bytes_added": sum(&gdp, "gross_bytes_added")?,
            "net_bytes": net_bytes,
            "total_edits": total_edits,
            "productive_edits": sum(&gdp, "productive_edits")?,
            "reverted_edits": sum(&gdp, "reverted_edits")?,
            "bytes_per_edit": net_bytes / total_edits,
        },
        "activity": {
            "period": "2024",
            "unique_editors": sum(&activity, "editors")?,
            "total_edits": sum(&activity, "total_edits")?,
            "net_bytes": sum(&activity, "net_bytes")?,
            "gross_bytes": sum(&activity, "gross_bytes")?,
        },
        "inequality": {
            "period": "2024",
            "total_editors": inequality_editors,
            "total_edits": inequality_edits,
            "min_editors_50pct": one_inequality.then(|| number(&inequality, "min_editors_50pct", 0)).transpose()?.flatten(),
            "gini": one_inequality.then(|| number(&inequality, "gini", 0)).transpose()?.flatten(),
            "theil": composed_theil,
            "palma": one_inequality.then(|| number(&inequality, "palma", 0)).transpose()?.flatten(),
        },
        "patrol": {
            "period": "2024",
            "total_patrols": sum(&patrol, "total_patrols")?,
            "unique_patrollers": one_patrol.then(|| number(&patrol, "unique_patrollers", 0)).transpose()?.flatten(),
            "patrol_new_pages": sum(&patrol, "patrol_new_pages")?,
            "patrol_diffs": sum(&patrol, "patrol_diffs")?,
            "median_latency_hours": one_patrol.then(|| number(&patrol, "median_latency_hours", 0)).transpose()?.flatten(),
            "p90_latency_hours": one_patrol.then(|| number(&patrol, "p90_latency_hours", 0)).transpose()?.flatten(),
            "patrolled_revisions": patrolled_revisions,
            "autopatrolled_revisions": autopatrolled_revisions,
            "total_revisions": total_revisions,
            "patrol_coverage_pct": patrolled_revisions / total_revisions * 100.0,
            "adjusted_coverage_pct": (patrolled_revisions + autopatrolled_revisions) / total_revisions * 100.0,
            "top1_pct": one_patrol.then(|| number(&patrol, "top1_pct", 0)).transpose()?.flatten(),
            "min_patrollers_50pct": one_patrol.then(|| number(&patrol, "min_patrollers_50pct", 0)).transpose()?.flatten(),
        }
    }))
}

fn concat_paths(paths: &[PathBuf]) -> Result<DataFrame> {
    let mut frames = paths
        .iter()
        .map(|path| read_parquet(path))
        .collect::<Result<Vec<_>>>()?;
    let mut result = frames.pop().context("no parity frames")?;
    for frame in frames {
        result.vstack_mut(&frame)?;
    }
    Ok(result)
}

fn assert_json_close(actual: &Value, expected: &Value, path: &str) {
    match (actual, expected) {
        (Value::Number(left), Value::Number(right)) => {
            let left = left.as_f64().expect("actual number must fit f64");
            let right = right.as_f64().expect("expected number must fit f64");
            assert!(
                (left - right).abs() <= 1e-10,
                "parity mismatch at {path}: {left} != {right}"
            );
        }
        (Value::Object(left), Value::Object(right)) => {
            assert_eq!(left.len(), right.len(), "object width differs at {path}");
            for (key, expected) in right {
                assert_json_close(
                    left.get(key)
                        .unwrap_or_else(|| panic!("missing {path}.{key}")),
                    expected,
                    &format!("{path}.{key}"),
                );
            }
        }
        (Value::Array(left), Value::Array(right)) => {
            assert_eq!(left.len(), right.len(), "array length differs at {path}");
            for (index, (left, right)) in left.iter().zip(right).enumerate() {
                assert_json_close(left, right, &format!("{path}[{index}]"));
            }
        }
        _ => assert_eq!(actual, expected, "parity mismatch at {path}"),
    }
}

fn defaults_summary(output_dir: &Path) -> Result<Value> {
    let read = |name: &str| -> Result<Value> {
        serde_json::from_slice(&fs::read(output_dir.join(name))?).map_err(Into::into)
    };
    let gdp = read("defaults_gdp.json")?;
    let inequality = read("defaults_inequality.json")?;
    let patrol = read("defaults_patrol.json")?;
    let output = gdp["output"].as_array().context("missing GDP defaults")?;
    let output = output
        .iter()
        .find(|row| row["period"] == "2024")
        .context("missing 2024 GDP default")?;
    let tier_rows = gdp["tiers"].as_array().context("missing tier defaults")?;
    let tier_rows = tier_rows
        .iter()
        .filter(|row| row["period"] == "2024")
        .collect::<Vec<_>>();
    let tier_sum = |field: &str| {
        tier_rows
            .iter()
            .map(|row| row[field].as_f64().unwrap_or_default())
            .sum::<f64>()
    };
    let inequality = inequality["data"]
        .as_array()
        .context("missing inequality defaults")?
        .iter()
        .find(|row| row["period"] == "2024")
        .context("missing 2024 inequality default")?;
    let patrol = patrol["patrol"]
        .as_array()
        .context("missing patrol defaults")?
        .iter()
        .find(|row| row["period"] == "2024")
        .context("missing 2024 patrol default")?;
    let copy_fields = |source: &Value, fields: &[&str]| {
        let mut result = Map::new();
        for field in fields {
            result.insert((*field).to_string(), source[*field].clone());
        }
        Value::Object(result)
    };
    let net = output["net_bytes"].as_f64().context("GDP net bytes")?;
    let edits = output["total_edits"].as_f64().context("GDP edits")?;
    Ok(json!({
        "gdp": {
            "period": "2024",
            "gross_bytes_added": output["gross_bytes_added"],
            "net_bytes": output["net_bytes"],
            "total_edits": output["total_edits"],
            "productive_edits": output["productive_edits"],
            "reverted_edits": output["reverted_edits"],
            "bytes_per_edit": net / edits,
        },
        "activity": {
            "period": "2024",
            "unique_editors": tier_sum("editors"),
            "total_edits": tier_sum("total_edits"),
            "net_bytes": tier_sum("net_bytes"),
            "gross_bytes": tier_sum("gross_bytes"),
        },
        "inequality": copy_fields(inequality, &["period", "total_editors", "total_edits", "min_editors_50pct", "gini", "theil", "palma"]),
        "patrol": copy_fields(patrol, &["period", "total_patrols", "unique_patrollers", "patrol_new_pages", "patrol_diffs", "median_latency_hours", "p90_latency_hours", "patrolled_revisions", "autopatrolled_revisions", "total_revisions", "patrol_coverage_pct", "adjusted_coverage_pct", "top1_pct", "min_patrollers_50pct"]),
    }))
}

fn assert_adversarial_coverage(fixture: &Fixture) {
    let wikis = fixture
        .history
        .iter()
        .map(|row| row.wiki.as_str())
        .collect::<BTreeSet<_>>();
    assert!(wikis.len() > 1, "fixture must cover multiple wikis");
    let renamed = fixture
        .history
        .iter()
        .filter(|row| row.wiki == "alphawiki" && row.user_id == Some(10));
    assert!(
        renamed
            .clone()
            .map(|row| row.historical_name.as_str())
            .collect::<BTreeSet<_>>()
            .len()
            > 1,
        "fixture must cover a renamed stable identity"
    );
    assert!(
        renamed
            .clone()
            .map(|row| &row.timestamp[..7])
            .collect::<BTreeSet<_>>()
            .len()
            > 1
            && renamed
                .map(|row| row.namespace)
                .collect::<BTreeSet<_>>()
                .len()
                > 1,
        "fixture must repeat an editor across months and namespaces"
    );
    assert!(fixture.history.iter().any(|row| row.bot));
    assert!(fixture.history.iter().any(|row| row.anonymous));
    assert!(fixture.history.iter().any(|row| row.temporary));
    assert!(fixture.history.iter().any(|row| row.indefinitely_blocked));
    let recurring_patroller = fixture
        .patrol_events
        .iter()
        .filter(|row| row.wiki == "alphawiki" && row.patroller == "Patroller A")
        .map(|row| row.year_month.as_str())
        .collect::<BTreeSet<_>>();
    assert!(
        recurring_patroller.len() > 1,
        "fixture must repeat a patroller across months"
    );
}

fn assert_base_parquet_identity_cases(frame: &DataFrame, wiki: &str) -> Result<()> {
    let types = frame
        .column("user_type")?
        .str()?
        .iter()
        .flatten()
        .collect::<BTreeSet<_>>();
    if wiki == "alphawiki" {
        assert_eq!(
            types,
            BTreeSet::from(["anonymous", "bot", "registered", "temporary"]),
            "base Parquet lost an adversarial user type"
        );
        assert!(
            frame
                .column("is_indefinitely_blocked")?
                .bool()?
                .iter()
                .flatten()
                .any(|blocked| blocked),
            "base Parquet lost block-state evidence"
        );
        let renamed = selected(frame, col("event_user_id").eq(lit(10_i64)))?;
        let distinct = |column: &str| -> Result<usize> { Ok(renamed.column(column)?.n_unique()?) };
        assert!(distinct("event_user_text_historical")? > 1);
        assert!(distinct("year_month")? > 1);
        assert!(distinct("page_namespace")? > 1);
    }
    Ok(())
}

#[test]
fn adversarial_metric_semantics_match_every_publication_layer() -> Result<()> {
    init_test_tracing();
    let fixture: Fixture = serde_json::from_str(FIXTURE_JSON)?;
    ensure!(
        fixture.schema_version == 1,
        "unsupported parity fixture schema"
    );
    ensure!(
        fixture.custom_inputs.is_object(),
        "missing JS parity inputs"
    );
    assert_adversarial_coverage(&fixture);

    let temp = TestDir::new()?;
    let output_dir = temp.path().join("output");
    let data_dir = temp.path().join("data");
    let base_dir = data_dir.join("parquet");
    let events = patrol_event_frame(&fixture)?;
    let coverage = patrol_coverage_frame(&fixture)?;
    let wikis = fixture
        .history
        .iter()
        .map(|row| row.wiki.clone())
        .collect::<BTreeSet<_>>();
    for wiki in &wikis {
        let base_path = base_dir.join(wiki).join("part-00000.parquet");
        write_parquet(&base_path, history_frame(&fixture, wiki)?)?;
        let base = read_parquet(&base_path)?;
        assert_base_parquet_identity_cases(&base, wiki)?;
        for family in [
            MetricFamily::Monthly,
            MetricFamily::ActivityTiers,
            MetricFamily::Lifecycle,
        ] {
            compute::execute_family(wiki, &data_dir, &output_dir, family)?;
        }
        let mut patrol = patrol::parity_fixture_metrics(wiki, &events, &coverage)?;
        compute::write_output(&mut patrol, wiki, "patrol", &output_dir)?;
        let mut weekly = df!(
            "week_start" => &["2024-01-01"],
            "iso_year" => &[2024_i32],
            "iso_week" => &[1_i32],
            "page_id" => &[1_i64],
            "page_title" => &[wiki.as_str()],
            "page_namespace" => &[0_i32],
            "edits" => &[1_u32],
            "previous_week_edits" => &[0_u32],
            "wow_change" => &[1_i64],
            "wow_rate" => &[None::<f64>],
            "wiki" => &[wiki.as_str()],
        )?;
        compute::write_output(&mut weekly, wiki, "page_weekly_edits", &output_dir)?;
    }

    let metric = |wiki: &str, name: &str| output_dir.join(wiki).join(format!("{name}.parquet"));
    let base_summary = layer_summary(
        &concat_paths(
            &wikis
                .iter()
                .map(|wiki| metric(wiki, "gdp"))
                .collect::<Vec<_>>(),
        )?,
        &concat_paths(
            &wikis
                .iter()
                .map(|wiki| metric(wiki, "gdp_activity_tiers"))
                .collect::<Vec<_>>(),
        )?,
        &concat_paths(
            &wikis
                .iter()
                .map(|wiki| metric(wiki, "inequality"))
                .collect::<Vec<_>>(),
        )?,
        &concat_paths(
            &wikis
                .iter()
                .map(|wiki| metric(wiki, "patrol"))
                .collect::<Vec<_>>(),
        )?,
    )?;
    assert_json_close(&base_summary, &fixture.expected, "base_parquet");

    merge::merge_outputs(&output_dir, None)?;
    let merged_summary = layer_summary(
        &read_parquet(&output_dir.join("gdp.parquet"))?,
        &read_parquet(&output_dir.join("gdp_activity_tiers.parquet"))?,
        &read_parquet(&output_dir.join("inequality.parquet"))?,
        &read_parquet(&output_dir.join("patrol.parquet"))?,
    )?;
    assert_json_close(&merged_summary, &fixture.expected, "merged_publication");

    let global = output_dir.join("_browser-global");
    let global_summary = layer_summary(
        &read_parquet(&global.join("gdp/2024.parquet"))?,
        &read_parquet(&global.join("gdp_activity_tiers/2024.parquet"))?,
        &read_parquet(&global.join("inequality/2024.parquet"))?,
        &read_parquet(&global.join("patrol/2024.parquet"))?,
    )?;
    assert_json_close(&global_summary, &fixture.expected, "global_shards");

    assert_json_close(
        &defaults_summary(&output_dir)?,
        &fixture.expected,
        "rust_defaults",
    );
    Ok(())
}
