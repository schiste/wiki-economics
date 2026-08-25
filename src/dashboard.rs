use anyhow::{Context, Result, bail, ensure};
use chrono::{Duration, NaiveDate};
use polars::prelude::*;
use serde_json::{Value, json};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;

use crate::{licensing, storage};

const LARGE_METRIC_BATCH_ROWS: usize = 250_000;
const ALL_WIKIS_SCOPE: &str = "all";

pub const ARTIFACTS: [&str; 12] = [
    "defaults_business.json",
    "defaults_edit_variation.json",
    "defaults_gdp.json",
    "defaults_inequality.json",
    "defaults_labor.json",
    "defaults_overview.json",
    "defaults_patrol.json",
    "meta_business.json",
    "meta_gdp.json",
    "meta_inequality.json",
    "meta_labor.json",
    "meta_patrol.json",
];

struct Frames {
    business_funnel: DataFrame,
    gdp: DataFrame,
    tiers: DataFrame,
    type_share: DataFrame,
    inequality: DataFrame,
    churn: DataFrame,
    cohorts: DataFrame,
    labor: DataFrame,
    patrol: DataFrame,
}

#[derive(Clone)]
struct CommonMeta {
    default_wiki: String,
    max_month: String,
    wikis: Vec<Value>,
    namespaces: Vec<Value>,
    ranges: Vec<Value>,
}

#[derive(Default)]
struct Average {
    sum: f64,
    weight: f64,
}

impl Average {
    #[cfg(test)]
    fn add(&mut self, value: Option<f64>) {
        self.add_weighted(value, 1.0);
    }

    fn add_weighted(&mut self, value: Option<f64>, weight: f64) {
        if let Some(value) = value
            && weight > 0.0
        {
            self.sum += value * weight;
            self.weight += weight;
        }
    }

    fn value(&self) -> Value {
        if self.weight == 0.0 {
            Value::Null
        } else {
            number(self.sum / self.weight)
        }
    }
}

pub fn materialize(output_dir: &Path) -> Result<()> {
    let frames = Frames::read(output_dir)?;
    let dashboard_wikis = wiki_set(&frames.gdp)?;
    let default_wiki = env::var("DEFAULT_WIKI")
        .ok()
        .or_else(|| dashboard_wikis.first().cloned())
        .context("dashboard input contains no wiki")?;
    ensure!(
        dashboard_wikis.contains(&default_wiki),
        "DEFAULT_WIKI {default_wiki} is absent from dashboard inputs"
    );
    let mut artifacts = BTreeMap::new();
    let (defaults_gdp, meta_gdp) = gdp_artifacts(&frames)?;
    let (defaults_labor, meta_labor) = labor_artifacts(&frames)?;
    let (defaults_business, meta_business) = business_artifacts(&frames)?;
    let (defaults_inequality, meta_inequality) = inequality_artifacts(&frames)?;
    let (defaults_patrol, meta_patrol) = patrol_artifacts(&frames)?;
    let defaults_overview = overview_artifacts(&frames)?;

    artifacts.insert("defaults_business.json", defaults_business);
    artifacts.insert(
        "defaults_edit_variation.json",
        edit_variation_artifact(output_dir, &default_wiki)?,
    );
    artifacts.insert("defaults_gdp.json", defaults_gdp);
    artifacts.insert("defaults_inequality.json", defaults_inequality);
    artifacts.insert("defaults_labor.json", defaults_labor);
    artifacts.insert("defaults_overview.json", defaults_overview);
    artifacts.insert("defaults_patrol.json", defaults_patrol);
    artifacts.insert("meta_business.json", meta_business);
    artifacts.insert("meta_gdp.json", meta_gdp);
    artifacts.insert("meta_inequality.json", meta_inequality);
    artifacts.insert("meta_labor.json", meta_labor);
    artifacts.insert("meta_patrol.json", meta_patrol);

    ensure!(
        artifacts.len() == ARTIFACTS.len()
            && ARTIFACTS.iter().all(|name| artifacts.contains_key(name)),
        "Rust dashboard generator did not produce the complete artifact set"
    );
    publish_json_set(output_dir, &artifacts)
}

impl Frames {
    fn read(output_dir: &Path) -> Result<Self> {
        Ok(Self {
            business_funnel: read_parquet(output_dir, "business_funnel")?,
            gdp: read_parquet(output_dir, "gdp")?,
            tiers: read_parquet(output_dir, "gdp_activity_tiers")?,
            type_share: read_parquet(output_dir, "gdp_user_type_share")?,
            inequality: read_parquet(output_dir, "inequality")?,
            churn: read_parquet(output_dir, "labor_churn")?,
            cohorts: read_parquet(output_dir, "labor_cohorts")?,
            labor: read_parquet(output_dir, "labor_monthly")?,
            patrol: read_parquet(output_dir, "patrol")?,
        })
    }
}

fn read_parquet(output_dir: &Path, metric: &str) -> Result<DataFrame> {
    let path = output_dir.join(format!("{metric}.parquet"));
    let frame = ParquetReader::new(
        File::open(&path).with_context(|| format!("missing merged dashboard input {path:?}"))?,
    )
    .set_low_memory(true)
    .read_parallel(ParallelStrategy::None)
    .finish()
    .map_err(anyhow::Error::from);
    frame.with_context(|| format!("failed to read merged dashboard input {path:?}"))
}

fn publish_json_set(output_dir: &Path, artifacts: &BTreeMap<&str, Value>) -> Result<()> {
    fs::create_dir_all(output_dir)?;
    let mut staged = Vec::with_capacity(artifacts.len());
    for (name, value) in artifacts {
        let bytes = serde_json::to_vec(value)?;
        serde_json::from_slice::<Value>(&bytes).context("generated dashboard JSON is invalid")?;
        let destination = output_dir.join(name);
        let temporary = output_dir.join(format!(".{name}.{}.tmp", std::process::id()));
        let mut file = File::create(&temporary)
            .with_context(|| format!("failed to stage dashboard artifact {name}"))?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        staged.push((temporary, destination));
    }
    for (temporary, destination) in &staged {
        if let Err(error) = fs::rename(temporary, destination) {
            for (remaining, _) in &staged {
                let _ = fs::remove_file(remaining);
            }
            return Err(error).with_context(|| {
                format!(
                    "failed to publish dashboard artifact {}",
                    destination.display()
                )
            });
        }
    }
    File::open(output_dir)?.sync_all()?;
    Ok(())
}

fn string(df: &DataFrame, column: &str, row: usize) -> Result<Option<String>> {
    match df.column(column)?.get(row)? {
        AnyValue::Null => Ok(None),
        AnyValue::String(value) => Ok(Some(value.to_string())),
        AnyValue::StringOwned(value) => Ok(Some(value.to_string())),
        value => bail!("expected string in {column}, found {value:?}"),
    }
}

fn integer(df: &DataFrame, column: &str, row: usize) -> Result<Option<i64>> {
    let value = match df.column(column)?.get(row)? {
        AnyValue::Null => return Ok(None),
        AnyValue::UInt32(value) => i64::from(value),
        AnyValue::Int32(value) => i64::from(value),
        AnyValue::Int64(value) => value,
        value => bail!("expected integer in {column}, found {value:?}"),
    };
    Ok(Some(value))
}

fn float(df: &DataFrame, column: &str, row: usize) -> Result<Option<f64>> {
    let value = match df.column(column)?.get(row)? {
        AnyValue::Null => return Ok(None),
        AnyValue::Float64(value) => value,
        AnyValue::UInt32(value) => f64::from(value),
        AnyValue::Int32(value) => f64::from(value),
        AnyValue::Int64(value) => value as f64,
        value => bail!("expected number in {column}, found {value:?}"),
    };
    Ok(Some(value))
}

fn number(value: f64) -> Value {
    if !value.is_finite() {
        Value::Null
    } else if value.fract() == 0.0 && value >= i64::MIN as f64 && value <= i64::MAX as f64 {
        json!(value as i64)
    } else {
        serde_json::Number::from_f64(value)
            .map(Value::Number)
            .unwrap_or(Value::Null)
    }
}

fn wiki_set(df: &DataFrame) -> Result<BTreeSet<String>> {
    let mut values = BTreeSet::new();
    for row in 0..df.height() {
        if let Some(wiki) = string(df, "wiki", row)? {
            values.insert(wiki);
        }
    }
    Ok(values)
}

fn common_meta(range_frame: &DataFrame, namespace_frame: Option<&DataFrame>) -> Result<CommonMeta> {
    common_meta_with_overrides(
        range_frame,
        namespace_frame,
        Some(ALL_WIKIS_SCOPE.to_string()),
        env::var("MAX_MONTH").ok(),
    )
}

fn common_meta_with_overrides(
    range_frame: &DataFrame,
    namespace_frame: Option<&DataFrame>,
    default_wiki: Option<String>,
    max_month: Option<String>,
) -> Result<CommonMeta> {
    let wiki_names = wiki_set(range_frame)?;
    ensure!(!wiki_names.is_empty(), "dashboard input contains no wiki");
    let default_wiki = default_wiki.unwrap_or_else(|| ALL_WIKIS_SCOPE.to_string());
    ensure!(
        default_wiki == ALL_WIKIS_SCOPE || wiki_names.contains(&default_wiki),
        "DEFAULT_WIKI {default_wiki} is absent from dashboard inputs"
    );
    let observed_max = (0..range_frame.height())
        .filter_map(|row| string(range_frame, "year_month", row).transpose())
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .max()
        .context("dashboard input contains no year_month")?;
    let max_month = max_month.unwrap_or(observed_max);
    ensure!(
        max_month.len() == 7 && max_month.as_bytes()[4] == b'-',
        "MAX_MONTH must use YYYY-MM"
    );

    let mut ranges: BTreeMap<String, (String, String)> = BTreeMap::new();
    for row in 0..range_frame.height() {
        let (Some(wiki), Some(month)) = (
            string(range_frame, "wiki", row)?,
            string(range_frame, "year_month", row)?,
        ) else {
            continue;
        };
        if month > max_month {
            continue;
        }
        ranges
            .entry(wiki)
            .and_modify(|range| {
                if month < range.0 {
                    range.0 = month.clone();
                }
                if month > range.1 {
                    range.1 = month.clone();
                }
            })
            .or_insert_with(|| (month.clone(), month));
    }

    let mut namespaces: BTreeMap<String, (BTreeSet<i64>, bool)> = BTreeMap::new();
    if let Some(frame) = namespace_frame {
        for row in 0..frame.height() {
            if let Some(wiki) = string(frame, "wiki", row)? {
                let entry = namespaces.entry(wiki).or_default();
                if let Some(namespace) = integer(frame, "page_namespace", row)? {
                    entry.0.insert(namespace);
                } else {
                    entry.1 = true;
                }
            }
        }
    }

    let all_namespaces = namespaces
        .values()
        .flat_map(|(values, _)| values.iter().copied())
        .collect::<BTreeSet<_>>();
    let all_has_null = namespaces.values().any(|(_, has_null)| *has_null);
    let mut namespaces = namespaces
        .into_iter()
        .flat_map(|(wiki, (values, has_null))| {
            let mut rows = values
                .into_iter()
                .map(|page_namespace| json!({"wiki": wiki, "page_namespace": page_namespace}))
                .collect::<Vec<_>>();
            if has_null {
                rows.push(json!({"wiki": wiki, "page_namespace": Value::Null}));
            }
            rows
        })
        .collect::<Vec<_>>();
    namespaces.extend(
        all_namespaces.into_iter().map(
            |page_namespace| json!({"wiki": ALL_WIKIS_SCOPE, "page_namespace": page_namespace}),
        ),
    );
    if all_has_null {
        namespaces.push(json!({"wiki": ALL_WIKIS_SCOPE, "page_namespace": Value::Null}));
    }

    let all_min = ranges.values().map(|range| range.0.clone()).min();
    let all_max = ranges.values().map(|range| range.1.clone()).max();
    let mut ranges = ranges
        .into_iter()
        .map(|(wiki, (mn, mx))| json!({"wiki": wiki, "mn": mn, "mx": mx}))
        .collect::<Vec<_>>();
    if let (Some(mn), Some(mx)) = (all_min, all_max) {
        ranges.push(json!({"wiki": ALL_WIKIS_SCOPE, "mn": mn, "mx": mx}));
    }

    Ok(CommonMeta {
        default_wiki,
        max_month,
        wikis: wiki_names
            .into_iter()
            .map(|wiki| json!({"wiki": wiki}))
            .collect(),
        namespaces,
        ranges,
    })
}

fn meta_json(meta: &CommonMeta, namespaces: bool) -> Value {
    let mut value = json!({
        "defaultWiki": meta.default_wiki,
        "maxMonth": meta.max_month,
        "wikis": meta.wikis,
        "rangeByWiki": meta.ranges,
    });
    if namespaces {
        value["nsByWiki"] = json!(meta.namespaces);
    }
    value
}

fn year(month: &str) -> Result<String> {
    ensure!(month.len() >= 4, "invalid year_month {month}");
    Ok(month[..4].to_string())
}

fn quarter(month: &str) -> Result<String> {
    ensure!(
        month.len() == 7 && month.as_bytes()[4] == b'-',
        "invalid year_month {month}"
    );
    let month_number: u8 = month[5..].parse()?;
    ensure!(
        (1..=12).contains(&month_number),
        "invalid year_month {month}"
    );
    Ok(format!("{}-Q{}", &month[..4], (month_number - 1) / 3 + 1))
}

fn selected_month_row(df: &DataFrame, row: usize, wiki: &str, max_month: &str) -> Result<bool> {
    Ok(
        (wiki == ALL_WIKIS_SCOPE || string(df, "wiki", row)?.as_deref() == Some(wiki))
            && string(df, "year_month", row)?.is_some_and(|month| month.as_str() <= max_month),
    )
}

fn selected_wiki_row(df: &DataFrame, row: usize, wiki: &str) -> Result<bool> {
    Ok(wiki == ALL_WIKIS_SCOPE || string(df, "wiki", row)?.as_deref() == Some(wiki))
}

/// Global landing-page snapshot: content-production totals summed across every
/// published wiki, plus a per-wiki breakdown for deep-linking into `/gdp`.
fn overview_artifacts(frames: &Frames) -> Result<Value> {
    let meta = common_meta(&frames.gdp, None)?;
    overview_from_gdp(&frames.gdp, &meta)
}

fn overview_row_in_range(df: &DataFrame, row: usize, max_month: &str) -> Result<bool> {
    Ok(string(df, "wiki", row)?.is_some()
        && string(df, "year_month", row)?.is_some_and(|month| month.as_str() <= max_month))
}

fn overview_from_gdp(gdp: &DataFrame, meta: &CommonMeta) -> Result<Value> {
    let mut trend: BTreeMap<String, [f64; 5]> = BTreeMap::new();
    let mut by_wiki_month: BTreeMap<(String, String), [f64; 5]> = BTreeMap::new();

    for row in 0..gdp.height() {
        if !overview_row_in_range(gdp, row, &meta.max_month)? {
            continue;
        }
        let wiki = string(gdp, "wiki", row)?.context("gdp wiki is null")?;
        let month = string(gdp, "year_month", row)?.context("gdp month is null")?;
        let Some(namespace) = integer(gdp, "page_namespace", row)? else {
            continue;
        };
        let user_type = string(gdp, "user_type", row)?.context("gdp user type is null")?;
        if namespace != 0 || user_type != "registered" {
            continue;
        }

        let period = year(&month)?;
        let trend_entry = trend.entry(period).or_default();
        let wiki_entry = by_wiki_month.entry((wiki, month)).or_default();
        for (index, column) in [
            "gross_bytes_added",
            "net_bytes",
            "total_edits",
            "reverted_edits",
            "unique_editors",
        ]
        .iter()
        .enumerate()
        {
            let value = float(gdp, column, row)?.unwrap_or_default();
            trend_entry[index] += value;
            wiki_entry[index] += value;
        }
    }

    // Wikis currently reach the dashboard on different schedules (see
    // config/wiki-lifecycle.json): some refresh live, others are frozen at an
    // import cutoff. Rather than collapsing that into one global "as of"
    // month -- which would either hide stale wikis behind a fresher headline
    // or drag the headline down to the stalest wiki -- each row below reports
    // the wiki's own latest available month, so the gap stays visible.
    let mut latest_by_wiki: BTreeMap<String, String> = BTreeMap::new();
    for (wiki, month) in by_wiki_month.keys() {
        latest_by_wiki
            .entry(wiki.clone())
            .and_modify(|current: &mut String| {
                if *month > *current {
                    *current = month.clone();
                }
            })
            .or_insert_with(|| month.clone());
    }

    let by_wiki: Vec<Value> = latest_by_wiki
        .iter()
        .map(|(wiki, month)| {
            let values = by_wiki_month
                .get(&(wiki.clone(), month.clone()))
                .copied()
                .unwrap_or_default();
            json!({
                "wiki": wiki,
                "latestMonth": month,
                "gross_bytes_added": number(values[0]),
                "net_bytes": number(values[1]),
                "total_edits": number(values[2]),
                "reverted_edits": number(values[3]),
                "unique_editors": number(values[4]),
            })
        })
        .collect();

    Ok(json!({
        "defaultWiki": meta.default_wiki,
        "maxMonth": meta.max_month,
        "wikis": meta.wikis,
        "rangeByWiki": meta.ranges,
        "trend": trend.into_iter().map(|(period, values)| json!({
            "period": period,
            "gross_bytes_added": number(values[0]),
            "net_bytes": number(values[1]),
            "total_edits": number(values[2]),
            "reverted_edits": number(values[3]),
            "unique_editors": number(values[4]),
        })).collect::<Vec<_>>(),
        "byWiki": by_wiki,
    }))
}

fn gdp_artifacts(frames: &Frames) -> Result<(Value, Value)> {
    let meta = common_meta(&frames.gdp, Some(&frames.gdp))?;
    gdp_artifacts_with_meta(frames, meta)
}

fn gdp_artifacts_with_meta(frames: &Frames, meta: CommonMeta) -> Result<(Value, Value)> {
    let mut output: BTreeMap<String, [f64; 6]> = BTreeMap::new();
    let mut by_type: BTreeMap<(String, String), [f64; 5]> = BTreeMap::new();
    let mut by_namespace: BTreeMap<(String, i64), [f64; 3]> = BTreeMap::new();

    for row in 0..frames.gdp.height() {
        if !selected_month_row(&frames.gdp, row, &meta.default_wiki, &meta.max_month)? {
            continue;
        }
        let month = string(&frames.gdp, "year_month", row)?.context("gdp month is null")?;
        let Some(namespace) = integer(&frames.gdp, "page_namespace", row)? else {
            continue;
        };
        let user_type = string(&frames.gdp, "user_type", row)?.context("gdp user type is null")?;
        if namespace == 0 {
            let entry = by_type
                .entry((month.clone(), user_type.clone()))
                .or_default();
            for (index, column) in [
                "gross_bytes_added",
                "net_bytes",
                "total_edits",
                "reverted_edits",
                "unique_editors",
            ]
            .iter()
            .enumerate()
            {
                entry[index] += float(&frames.gdp, column, row)?.unwrap_or_default();
            }
        }
        if user_type == "registered" && namespace == 0 {
            let entry = output.entry(year(&month)?).or_default();
            for (index, column) in [
                "gross_bytes_added",
                "net_bytes",
                "total_edits",
                "productive_edits",
                "reverted_edits",
                "unique_editors",
            ]
            .iter()
            .enumerate()
            {
                entry[index] += float(&frames.gdp, column, row)?.unwrap_or_default();
            }
            let entry = by_namespace.entry((month, namespace)).or_default();
            for (index, column) in ["total_edits", "gross_bytes_added", "net_bytes"]
                .iter()
                .enumerate()
            {
                entry[index] += float(&frames.gdp, column, row)?.unwrap_or_default();
            }
        }
    }

    let mut tiers: BTreeMap<(String, String), [f64; 4]> = BTreeMap::new();
    for row in 0..frames.tiers.height() {
        if !selected_month_row(&frames.tiers, row, &meta.default_wiki, &meta.max_month)?
            || string(&frames.tiers, "user_type", row)?.as_deref() != Some("registered")
        {
            continue;
        }
        let key = (
            string(&frames.tiers, "year_month", row)?.context("tier month is null")?,
            string(&frames.tiers, "activity_tier", row)?.context("activity tier is null")?,
        );
        let entry = tiers.entry(key).or_default();
        for (index, column) in ["editors", "total_edits", "gross_bytes", "net_bytes"]
            .iter()
            .enumerate()
        {
            entry[index] += float(&frames.tiers, column, row)?.unwrap_or_default();
        }
    }

    let mut type_share: BTreeMap<(String, String), f64> = BTreeMap::new();
    for row in 0..frames.type_share.height() {
        if !selected_month_row(&frames.type_share, row, &meta.default_wiki, &meta.max_month)? {
            continue;
        }
        let key = (
            string(&frames.type_share, "year_month", row)?.context("share month is null")?,
            string(&frames.type_share, "user_type", row)?.context("share user type is null")?,
        );
        *type_share.entry(key).or_default() +=
            float(&frames.type_share, "edits", row)?.unwrap_or_default();
    }

    let defaults = json!({
        "defaultWiki": meta.default_wiki,
        "maxMonth": meta.max_month,
        "wikis": meta.wikis,
        "nsByWiki": meta.namespaces,
        "rangeByWiki": meta.ranges,
        "output": output.into_iter().map(|(period, values)| json!({
            "period": period,
            "gross_bytes_added": number(values[0]),
            "net_bytes": number(values[1]),
            "total_edits": number(values[2]),
            "productive_edits": number(values[3]),
            "reverted_edits": number(values[4]),
            "unique_editors": number(values[5]),
        })).collect::<Vec<_>>(),
        "byType": by_type.into_iter().map(|((period, user_type), values)| json!({
            "period": period,
            "user_type": user_type,
            "gross_bytes_added": number(values[0]),
            "net_bytes": number(values[1]),
            "total_edits": number(values[2]),
            "reverted_edits": number(values[3]),
            "unique_editors": number(values[4]),
        })).collect::<Vec<_>>(),
        "byNamespace": by_namespace.into_iter().map(|((period, page_namespace), values)| json!({
            "period": period,
            "page_namespace": page_namespace,
            "edits": number(values[0]),
            "gross_bytes": number(values[1]),
            "net_bytes": number(values[2]),
        })).collect::<Vec<_>>(),
        "tiers": tiers.into_iter().map(|((period, activity_tier), values)| json!({
            "period": period,
            "activity_tier": activity_tier,
            "editors": number(values[0]),
            "total_edits": number(values[1]),
            "gross_bytes": number(values[2]),
            "net_bytes": number(values[3]),
        })).collect::<Vec<_>>(),
        "typeShare": type_share.into_iter().map(|((period, user_type), edits)| json!({
            "period": period,
            "user_type": user_type,
            "edits": number(edits),
        })).collect::<Vec<_>>(),
    });
    Ok((defaults, meta_json(&meta, true)))
}

fn labor_artifacts(frames: &Frames) -> Result<(Value, Value)> {
    let meta = common_meta(&frames.labor, Some(&frames.labor))?;
    labor_artifacts_with_meta(frames, meta)
}

fn labor_artifacts_with_meta(frames: &Frames, meta: CommonMeta) -> Result<(Value, Value)> {
    let mut workforce: BTreeMap<String, [f64; 4]> = BTreeMap::new();
    let mut by_type: BTreeMap<(String, String), f64> = BTreeMap::new();

    for row in 0..frames.labor.height() {
        if !selected_month_row(&frames.labor, row, &meta.default_wiki, &meta.max_month)? {
            continue;
        }
        let month = string(&frames.labor, "year_month", row)?.context("labor month is null")?;
        let Some(namespace) = integer(&frames.labor, "page_namespace", row)? else {
            continue;
        };
        let user_type =
            string(&frames.labor, "user_type", row)?.context("labor user type is null")?;
        if namespace == 0 {
            *by_type
                .entry((month.clone(), user_type.clone()))
                .or_default() += float(&frames.labor, "unique_editors", row)?.unwrap_or_default();
        }
        if namespace == 0 && user_type == "registered" {
            let entry = workforce.entry(year(&month)?).or_default();
            for (index, column) in [
                "unique_editors",
                "total_edits",
                "net_bytes",
                "reverted_edits",
            ]
            .iter()
            .enumerate()
            {
                entry[index] += float(&frames.labor, column, row)?.unwrap_or_default();
            }
        }
    }

    let churn = churn_rows(&frames.churn, &meta.default_wiki, "month", &meta.max_month)?;
    let cohorts = cohort_rows(frames, &meta.default_wiki)?;

    let defaults = json!({
        "defaultWiki": meta.default_wiki,
        "maxMonth": meta.max_month,
        "wikis": meta.wikis,
        "nsByWiki": meta.namespaces,
        "rangeByWiki": meta.ranges,
        "workforce": workforce.into_iter().map(|(period, values)| json!({
            "period": period,
            "unique_editors": number(values[0]),
            "total_edits": number(values[1]),
            "net_bytes": number(values[2]),
            "reverted_edits": number(values[3]),
        })).collect::<Vec<_>>(),
        "byType": by_type.into_iter().map(|((period, user_type), editors)| json!({
            "period": period,
            "user_type": user_type,
            "editors": number(editors),
        })).collect::<Vec<_>>(),
        "churn": churn,
        "cohorts": cohorts,
    });
    Ok((defaults, meta_json(&meta, true)))
}

fn cohort_rows(frames: &Frames, wiki: &str) -> Result<Vec<Value>> {
    let mut grouped: BTreeMap<(String, String), [i64; 2]> = BTreeMap::new();
    for row in 0..frames.cohorts.height() {
        if !selected_wiki_row(&frames.cohorts, row, wiki)? {
            continue;
        }
        let key = (
            string(&frames.cohorts, "cohort_year", row)?.context("cohort year is null")?,
            string(&frames.cohorts, "year", row)?.context("cohort activity year is null")?,
        );
        let entry = grouped.entry(key).or_default();
        entry[0] += integer(&frames.cohorts, "initial_editors", row)?.unwrap_or_default();
        entry[1] += integer(&frames.cohorts, "survived_editors", row)?.unwrap_or_default();
    }
    Ok(grouped
        .into_iter()
        .map(|((cohort_year, year), values)| {
            json!({
                "cohort_year": cohort_year,
                "year": year,
                "initial_editors": values[0],
                "survived_editors": values[1],
            })
        })
        .collect())
}

#[derive(Default)]
struct ChurnPeriod {
    active_editors: i64,
    arrivals: i64,
    departures: i64,
}

fn churn_rows(
    frame: &DataFrame,
    wiki: &str,
    period_type: &str,
    max_period: &str,
) -> Result<Vec<Value>> {
    let mut grouped: BTreeMap<String, ChurnPeriod> = BTreeMap::new();
    for row in 0..frame.height() {
        if !selected_wiki_row(frame, row, wiki)?
            || string(frame, "period_type", row)?.as_deref() != Some(period_type)
        {
            continue;
        }
        let period = string(frame, "period", row)?.context("churn period is null")?;
        if period.as_str() > max_period {
            continue;
        }
        let entry = grouped.entry(period).or_default();
        entry.active_editors += integer(frame, "active_editors", row)?.unwrap_or_default();
        entry.arrivals += integer(frame, "arrivals", row)?.unwrap_or_default();
        entry.departures += integer(frame, "departures", row)?.unwrap_or_default();
    }
    Ok(grouped
        .into_iter()
        .map(|(period, entry)| {
            let active = entry.active_editors as f64;
            json!({
                "period": period,
                "period_type": period_type,
                "active_editors": entry.active_editors,
                "arrivals": entry.arrivals,
                "departures": entry.departures,
                "arrival_rate": number(if active > 0.0 { entry.arrivals as f64 / active } else { 0.0 }),
                "departure_rate": number(if active > 0.0 { entry.departures as f64 / active } else { 0.0 }),
            })
        })
        .collect())
}

fn string_field<'a>(value: &'a Value, field: &str) -> &'a str {
    value.get(field).and_then(Value::as_str).unwrap_or("")
}

#[cfg(test)]
fn sort_two_strings(values: &mut [Value], first: &str, second: &str) {
    values.sort_by(|left, right| {
        string_field(left, first)
            .cmp(string_field(right, first))
            .then_with(|| string_field(left, second).cmp(string_field(right, second)))
    });
}

fn compare_optional_i64(left: Option<i64>, right: Option<i64>) -> Ordering {
    match (left, right) {
        (Some(left), Some(right)) => left.cmp(&right),
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => Ordering::Equal,
    }
}

fn business_artifacts(frames: &Frames) -> Result<(Value, Value)> {
    let meta = common_meta(&frames.labor, Some(&frames.gdp))?;
    let max_quarter = quarter(&meta.max_month)?;
    let churn = churn_rows(&frames.churn, &meta.default_wiki, "quarter", &max_quarter)?;

    let mut tiers: BTreeMap<(String, String), [f64; 4]> = BTreeMap::new();
    for row in 0..frames.tiers.height() {
        if !selected_month_row(&frames.tiers, row, &meta.default_wiki, &meta.max_month)?
            || string(&frames.tiers, "user_type", row)?.as_deref() != Some("registered")
        {
            continue;
        }
        let key = (
            quarter(&string(&frames.tiers, "year_month", row)?.context("tier month is null")?)?,
            string(&frames.tiers, "activity_tier", row)?.context("activity tier is null")?,
        );
        let entry = tiers.entry(key).or_default();
        for (index, column) in ["editors", "total_edits", "net_bytes", "gross_bytes"]
            .iter()
            .enumerate()
        {
            entry[index] += float(&frames.tiers, column, row)?.unwrap_or_default();
        }
    }

    let mut survival: BTreeMap<String, [f64; 2]> = BTreeMap::new();
    let mut equilibrium: BTreeMap<(String, Option<i64>), [f64; 2]> = BTreeMap::new();
    let mut yearly_bytes: BTreeMap<String, [f64; 2]> = BTreeMap::new();
    for row in 0..frames.gdp.height() {
        if !selected_month_row(&frames.gdp, row, &meta.default_wiki, &meta.max_month)?
            || string(&frames.gdp, "user_type", row)?.as_deref() != Some("registered")
        {
            continue;
        }
        let month = string(&frames.gdp, "year_month", row)?.context("gdp month is null")?;
        let namespace = integer(&frames.gdp, "page_namespace", row)?;
        let quarterly = quarter(&month)?;
        let equilibrium_entry = equilibrium
            .entry((quarterly.clone(), namespace))
            .or_default();
        equilibrium_entry[0] += float(&frames.gdp, "total_edits", row)?.unwrap_or_default();
        equilibrium_entry[1] += float(&frames.gdp, "reverted_edits", row)?.unwrap_or_default();
        if namespace == Some(0) {
            let survival_entry = survival.entry(quarterly).or_default();
            survival_entry[0] += float(&frames.gdp, "total_edits", row)?.unwrap_or_default();
            survival_entry[1] += float(&frames.gdp, "reverted_edits", row)?.unwrap_or_default();
            let yearly_entry = yearly_bytes.entry(year(&month)?).or_default();
            yearly_entry[0] += float(&frames.gdp, "net_bytes", row)?.unwrap_or_default();
            yearly_entry[1] += float(&frames.gdp, "unique_editors", row)?.unwrap_or_default();
        }
    }

    let cohorts = cohort_rows(frames, &meta.default_wiki)?;
    let funnel = funnel_rows(&frames.business_funnel, &meta.default_wiki)?;

    let mut equilibrium = equilibrium
        .into_iter()
        .map(|((period, page_namespace), values)| {
            json!({
                "period": period,
                "page_namespace": page_namespace,
                "total_edits": number(values[0]),
                "reverted_edits": number(values[1]),
            })
        })
        .collect::<Vec<_>>();
    equilibrium.sort_by(|left, right| {
        string_field(left, "period")
            .cmp(string_field(right, "period"))
            .then_with(|| {
                compare_optional_i64(
                    left["page_namespace"].as_i64(),
                    right["page_namespace"].as_i64(),
                )
            })
    });

    let defaults = json!({
        "defaultWiki": meta.default_wiki,
        "maxMonth": meta.max_month,
        "wikis": meta.wikis,
        "nsByWiki": meta.namespaces,
        "rangeByWiki": meta.ranges,
        "churn": churn,
        "tiers": tiers.into_iter().map(|((period, tier), values)| json!({
            "period": period,
            "tier": tier,
            "editors": number(values[0]),
            "edits": number(values[1]),
            "net_bytes": number(values[2]),
            "gross_bytes": number(values[3]),
        })).collect::<Vec<_>>(),
        "survival": survival.into_iter().map(|(period, values)| json!({
            "period": period,
            "total_edits": number(values[0]),
            "reverted_edits": number(values[1]),
        })).collect::<Vec<_>>(),
        "equilibrium": equilibrium,
        "cohorts": cohorts,
        "yearlyBytesPerEditor": yearly_bytes.into_iter().map(|(year, values)| json!({
            "year": year,
            "net_bytes": number(values[0]),
            "unique_editors": number(values[1]),
        })).collect::<Vec<_>>(),
        "funnel": funnel,
    });
    Ok((defaults, meta_json(&meta, true)))
}

fn funnel_rows(frame: &DataFrame, wiki: &str) -> Result<Vec<Value>> {
    let mut grouped: BTreeMap<String, [i64; 4]> = BTreeMap::new();
    for row in 0..frame.height() {
        if !selected_wiki_row(frame, row, wiki)? {
            continue;
        }
        let cohort_year =
            string(frame, "cohort_year", row)?.context("funnel cohort year is null")?;
        let entry = grouped.entry(cohort_year).or_default();
        for (index, column) in ["cohort_size", "reached_5", "reached_25", "reached_100"]
            .iter()
            .enumerate()
        {
            entry[index] += integer(frame, column, row)?.unwrap_or_default();
        }
    }
    Ok(grouped
        .into_iter()
        .map(|(cohort_year, values)| {
            json!({
                "cohort_year": cohort_year,
                "cohort_size": values[0],
                "reached_5": values[1],
                "reached_25": values[2],
                "reached_100": values[3],
            })
        })
        .collect())
}

#[derive(Default)]
struct InequalityYear {
    total_editors: f64,
    total_edits: f64,
    min_editors_50pct: f64,
    gini: Average,
    theil: Average,
    palma: Average,
}

fn inequality_artifacts(frames: &Frames) -> Result<(Value, Value)> {
    let meta = common_meta(&frames.inequality, None)?;
    let mut yearly: BTreeMap<String, InequalityYear> = BTreeMap::new();
    for row in 0..frames.inequality.height() {
        if !selected_month_row(&frames.inequality, row, &meta.default_wiki, &meta.max_month)?
            || string(&frames.inequality, "user_type", row)?.as_deref() != Some("registered")
        {
            continue;
        }
        let month =
            string(&frames.inequality, "year_month", row)?.context("inequality month is null")?;
        let entry = yearly.entry(year(&month)?).or_default();
        let editors = float(&frames.inequality, "total_editors", row)?.unwrap_or_default();
        entry.total_editors += editors;
        entry.total_edits += float(&frames.inequality, "total_edits", row)?.unwrap_or_default();
        entry.min_editors_50pct +=
            float(&frames.inequality, "min_editors_50pct", row)?.unwrap_or_default();
        entry
            .gini
            .add_weighted(float(&frames.inequality, "gini", row)?, editors);
        entry
            .theil
            .add_weighted(float(&frames.inequality, "theil", row)?, editors);
        entry
            .palma
            .add_weighted(float(&frames.inequality, "palma", row)?, editors);
    }
    let data = yearly
        .into_iter()
        .map(|(period, entry)| {
            json!({
                "period": period,
                "total_editors": number(entry.total_editors),
                "total_edits": number(entry.total_edits),
                "min_editors_50pct": number(entry.min_editors_50pct),
                "gini": entry.gini.value(),
                "theil": entry.theil.value(),
                "palma": entry.palma.value(),
            })
        })
        .collect::<Vec<_>>();
    let defaults = json!({
        "defaultWiki": meta.default_wiki,
        "maxMonth": meta.max_month,
        "wikis": meta.wikis,
        "rangeByWiki": meta.ranges,
        "data": data,
    });
    Ok((defaults, meta_json(&meta, false)))
}

#[derive(Default)]
struct PatrolYear {
    total_patrols: f64,
    unique_patrollers: f64,
    patrol_new_pages: f64,
    patrol_diffs: f64,
    median_latency_hours: Average,
    p90_latency_hours: Average,
    patrolled_revisions: f64,
    autopatrolled_revisions: f64,
    total_revisions: f64,
    top1_pct: Average,
    min_patrollers_50pct: f64,
}

fn patrol_artifacts(frames: &Frames) -> Result<(Value, Value)> {
    let meta = common_meta(&frames.patrol, Some(&frames.patrol))?;
    let mut yearly: BTreeMap<String, PatrolYear> = BTreeMap::new();
    for row in 0..frames.patrol.height() {
        if !selected_month_row(&frames.patrol, row, &meta.default_wiki, &meta.max_month)?
            || integer(&frames.patrol, "page_namespace", row)? != Some(0)
            || string(&frames.patrol, "user_type", row)?.as_deref() != Some("registered")
        {
            continue;
        }
        let month = string(&frames.patrol, "year_month", row)?.context("patrol month is null")?;
        let entry = yearly.entry(year(&month)?).or_default();
        let patrols = float(&frames.patrol, "total_patrols", row)?.unwrap_or_default();
        entry.total_patrols += patrols;
        entry.unique_patrollers +=
            float(&frames.patrol, "unique_patrollers", row)?.unwrap_or_default();
        entry.patrol_new_pages +=
            float(&frames.patrol, "patrol_new_pages", row)?.unwrap_or_default();
        entry.patrol_diffs += float(&frames.patrol, "patrol_diffs", row)?.unwrap_or_default();
        entry
            .median_latency_hours
            .add_weighted(float(&frames.patrol, "median_latency_hours", row)?, patrols);
        entry
            .p90_latency_hours
            .add_weighted(float(&frames.patrol, "p90_latency_hours", row)?, patrols);
        entry.patrolled_revisions +=
            float(&frames.patrol, "patrolled_revisions", row)?.unwrap_or_default();
        entry.autopatrolled_revisions +=
            float(&frames.patrol, "autopatrolled_revisions", row)?.unwrap_or_default();
        entry.total_revisions += float(&frames.patrol, "total_revisions", row)?.unwrap_or_default();
        entry
            .top1_pct
            .add_weighted(float(&frames.patrol, "top1_pct", row)?, patrols);
        entry.min_patrollers_50pct +=
            float(&frames.patrol, "min_patrollers_50pct", row)?.unwrap_or_default();
    }
    let patrol = yearly
        .into_iter()
        .map(|(period, entry)| {
            let patrol_coverage_pct = if entry.total_revisions > 0.0 {
                entry.patrolled_revisions / entry.total_revisions * 100.0
            } else {
                0.0
            };
            let adjusted_coverage_pct = if entry.total_revisions > 0.0 {
                (entry.patrolled_revisions + entry.autopatrolled_revisions) / entry.total_revisions
                    * 100.0
            } else {
                0.0
            };
            json!({
                "period": period,
                "total_patrols": number(entry.total_patrols),
                "unique_patrollers": number(entry.unique_patrollers),
                "patrol_new_pages": number(entry.patrol_new_pages),
                "patrol_diffs": number(entry.patrol_diffs),
                "median_latency_hours": entry.median_latency_hours.value(),
                "p90_latency_hours": entry.p90_latency_hours.value(),
                "patrolled_revisions": number(entry.patrolled_revisions),
                "autopatrolled_revisions": number(entry.autopatrolled_revisions),
                "total_revisions": number(entry.total_revisions),
                "patrol_coverage_pct": number(patrol_coverage_pct),
                "adjusted_coverage_pct": number(adjusted_coverage_pct),
                "top1_pct": entry.top1_pct.value(),
                "min_patrollers_50pct": number(entry.min_patrollers_50pct),
            })
        })
        .collect::<Vec<_>>();
    let defaults = json!({
        "defaultWiki": meta.default_wiki,
        "maxMonth": meta.max_month,
        "wikis": meta.wikis,
        "nsByWiki": meta.namespaces,
        "rangeByWiki": meta.ranges,
        "patrol": patrol,
    });
    Ok((defaults, meta_json(&meta, true)))
}

#[derive(Debug)]
struct VariationRow {
    week: String,
    title: String,
    previous_week_edits: i64,
    edits: Option<i64>,
    wow_change: Option<i64>,
    wow_rate: Option<f64>,
}

fn variation_order(left: &VariationRow, right: &VariationRow) -> Ordering {
    right
        .wow_change
        .cmp(&left.wow_change)
        .then_with(|| right.edits.cmp(&left.edits))
        .then_with(|| left.title.cmp(&right.title))
        .then_with(|| left.week.cmp(&right.week))
}

fn retain_top_variation(rows: &mut Vec<VariationRow>, candidate: VariationRow) {
    if rows.len() < 20 {
        rows.push(candidate);
        return;
    }
    let worst = rows
        .iter()
        .enumerate()
        .max_by(|(_, left), (_, right)| variation_order(left, right))
        .map(|(index, _)| index)
        .expect("twenty retained variation rows cannot be empty");
    if variation_order(&candidate, &rows[worst]) == Ordering::Less {
        rows[worst] = candidate;
    }
}

fn edit_variation_artifact(output_dir: &Path, default_wiki: &str) -> Result<Value> {
    let path = output_dir
        .join(default_wiki)
        .join("page_weekly_edits.parquet");
    let columns = Some(
        [
            "wiki",
            "page_namespace",
            "week_start",
            "page_title",
            "previous_week_edits",
            "edits",
            "wow_change",
            "wow_rate",
        ]
        .into_iter()
        .map(str::to_string)
        .collect(),
    );
    let mut reader =
        storage::SequentialParquetReader::new(&path, columns, LARGE_METRIC_BATCH_ROWS)?;
    let rows = reader.rows();
    ensure!(rows > 0, "page_weekly_edits.parquet is empty");
    let mut matching_rows = 0_i64;
    let mut min_week: Option<String> = None;
    let mut max_week: Option<String> = None;
    let mut top = Vec::with_capacity(20);
    let mut observed_rows = 0_usize;
    while let Some(batch) = reader.next_batch()? {
        observed_rows = observed_rows
            .checked_add(batch.height())
            .context("page-week dashboard row count overflow")?;
        for row in 0..batch.height() {
            let wiki = string(&batch, "wiki", row)?.context("variation wiki is null")?;
            ensure!(
                wiki == default_wiki,
                "{} contains rows for unexpected wiki {wiki}",
                path.display()
            );
            if integer(&batch, "page_namespace", row)? != Some(0) {
                continue;
            }
            matching_rows = matching_rows
                .checked_add(1)
                .context("page-week dashboard matching row count overflow")?;
            let week = string(&batch, "week_start", row)?.context("variation week is null")?;
            if min_week
                .as_deref()
                .is_none_or(|minimum| week.as_str() < minimum)
            {
                min_week = Some(week.clone());
            }
            if max_week
                .as_deref()
                .is_none_or(|maximum| week.as_str() > maximum)
            {
                max_week = Some(week.clone());
            }
            let previous_week_edits =
                integer(&batch, "previous_week_edits", row)?.unwrap_or_default();
            if previous_week_edits <= 0 {
                continue;
            }
            retain_top_variation(
                &mut top,
                VariationRow {
                    week,
                    title: string(&batch, "page_title", row)?.context("variation title is null")?,
                    previous_week_edits,
                    edits: integer(&batch, "edits", row)?,
                    wow_change: integer(&batch, "wow_change", row)?,
                    wow_rate: float(&batch, "wow_rate", row)?,
                },
            );
        }
    }
    ensure!(
        observed_rows == rows,
        "page-week dashboard row conservation failed: footer {rows}, scanned {observed_rows}"
    );
    top.sort_by(variation_order);
    let mut best = Vec::with_capacity(top.len());
    for row in top {
        let parsed_week = NaiveDate::parse_from_str(&row.week, "%Y-%m-%d")?;
        best.push(json!({
            "week_start": row.week,
            "week_end": (parsed_week + Duration::days(6)).to_string(),
            "page_title": row.title,
            "previous_week_edits": row.previous_week_edits,
            "edits": row.edits.unwrap_or_default(),
            "wow_change": row.wow_change.unwrap_or_default(),
            "wow_rate": row.wow_rate.map(number).unwrap_or(Value::Null),
        }));
    }

    Ok(json!({
        "defaultWiki": default_wiki,
        "summary": [{
            "rows": matching_rows,
            "min_week": min_week,
            "max_week": max_week,
        }],
        "topVariation": best,
    }))
}

fn write_parquet(output_dir: &Path, name: &str, mut frame: DataFrame) -> Result<()> {
    let mut file = File::create(output_dir.join(format!("{name}.parquet")))?;
    ParquetWriter::new(&mut file)
        .with_compression(ParquetCompression::Zstd(None))
        .set_parallel(false)
        .finish(&mut frame)?;
    Ok(())
}

pub fn write_site_fixture(output_dir: &Path) -> Result<()> {
    fs::create_dir_all(output_dir)?;
    let fixtures = [
        (
        "business_funnel",
        df!(
            "cohort_year" => &["2025", "2024", "2025"],
            "cohort_size" => &[10_u32, 4, 8],
            "reached_5" => &[7_u32, 3, 6],
            "reached_25" => &[3_u32, 1, 2],
            "reached_100" => &[1_u32, 0, 1],
            "wiki" => &["nlwiki", "nlwiki", "ptwiki"],
        )
        .expect("static fixture columns have equal lengths")),
        (
        "gdp",
        df!(
            "year_month" => &["2025-12", "2026-01", "2026-01", "2026-01", "2026-01"],
            "page_namespace" => &[Some(0_i32), Some(0), Some(0), None, Some(1)],
            "user_type" => &["registered", "registered", "registered", "registered", "registered"],
            "gross_bytes_added" => &[100_i64, 120, 80, 0, 2],
            "net_bytes" => &[80_i64, 90, 60, 0, 1],
            "total_edits" => &[10_u32, 12, 8, 0, 1],
            "productive_edits" => &[8_u32, 10, 6, 0, 1],
            "reverted_edits" => &[2_u32, 2, 2, 0, 0],
            "unique_editors" => &[5_u32, 6, 4, 0, 1],
            "minor_edits" => &[1_u32, 1, 1, 0, 0],
            "bytes_per_edit" => &[10.0_f64, 10.0, 10.0, 0.0, 2.0],
            "bytes_per_editor" => &[20.0_f64, 20.0, 20.0, 0.0, 2.0],
            "revert_rate" => &[0.2_f64, 1.0 / 6.0, 0.25, 0.0, 0.0],
            "wiki" => &["nlwiki", "nlwiki", "ptwiki", "nlwiki", "nlwiki"],
        )
        .expect("static fixture columns have equal lengths")),
        (
        "gdp_activity_tiers",
        df!(
            "year_month" => &["2026-01", "2026-01"],
            "user_type" => &["registered", "registered"],
            "activity_tier" => &["active", "active"],
            "editors" => &[6_u32, 4],
            "total_edits" => &[12_u32, 8],
            "net_bytes" => &[90_i64, 60],
            "gross_bytes" => &[120_i64, 80],
            "wiki" => &["nlwiki", "ptwiki"],
        )
        .expect("static fixture columns have equal lengths")),
        (
        "gdp_user_type_share",
        df!(
            "year_month" => &["2026-01", "2026-01"],
            "user_type" => &["registered", "registered"],
            "edits" => &[12_u32, 8],
            "net_bytes" => &[90_i64, 60],
            "editors" => &[6_u32, 4],
            "wiki" => &["nlwiki", "ptwiki"],
        )
        .expect("static fixture columns have equal lengths")),
        (
        "inequality",
        df!(
            "year_month" => &["2026-01", "2026-01"],
            "user_type" => &["registered", "registered"],
            "gini" => &[0.4_f64, 0.5],
            "theil" => &[0.3_f64, 0.4],
            "palma" => &[1.2_f64, 1.3],
            "min_editors_50pct" => &[2_u32, 2],
            "total_editors" => &[6_u32, 4],
            "total_edits" => &[12_u32, 8],
            "wiki" => &["nlwiki", "ptwiki"],
        )
        .expect("static fixture columns have equal lengths")),
        (
        "labor_churn",
        df!(
            "period" => &["2026-01", "2026-Q1", "2025-12", "2025-Q4", "2026-01", "2026-Q1"],
            "active_editors" => &[6_u32, 6, 5, 5, 4, 4],
            "arrivals" => &[2_u32, 2, 1, 1, 1, 1],
            "departures" => &[1_u32, 1, 1, 1, 1, 1],
            "period_type" => &["month", "quarter", "month", "quarter", "month", "quarter"],
            "arrival_rate" => &[0.3_f64, 0.3, 0.2, 0.2, 0.25, 0.25],
            "departure_rate" => &[0.1_f64, 0.1, 0.2, 0.2, 0.25, 0.25],
            "wiki" => &["nlwiki", "nlwiki", "nlwiki", "nlwiki", "ptwiki", "ptwiki"],
        )
        .expect("static fixture columns have equal lengths")),
        (
        "labor_cohorts",
        df!(
            "cohort_year" => &["2025", "2024", "2025"],
            "year" => &["2026", "2025", "2026"],
            "survived_editors" => &[5_u32, 2, 3],
            "initial_editors" => &[10_u32, 4, 8],
            "wiki" => &["nlwiki", "nlwiki", "ptwiki"],
        )
        .expect("static fixture columns have equal lengths")),
        (
        "labor_monthly",
        df!(
            "year_month" => &["2025-12", "2026-01", "2026-01", "2026-01", "2026-01", "2026-01"],
            "page_namespace" => &[Some(0_i32), Some(0), Some(0), None, Some(0), Some(1)],
            "user_type" => &["registered", "registered", "registered", "registered", "anonymous", "registered"],
            "unique_editors" => &[5_u32, 6, 4, 0, 1, 1],
            "total_edits" => &[10_u32, 12, 8, 0, 1, 1],
            "net_bytes" => &[80_i64, 90, 60, 0, 1, 1],
            "reverted_edits" => &[2_u32, 2, 2, 0, 0, 0],
            "wiki" => &["nlwiki", "nlwiki", "ptwiki", "nlwiki", "nlwiki", "nlwiki"],
        )
        .expect("static fixture columns have equal lengths")),
        (
        "page_weekly_edits",
        df!(
            "week_start" => &["2026-01-05", "2026-01-12", "2026-01-05"],
            "iso_year" => &[2026_i32, 2026, 2026],
            "iso_week" => &[2_i32, 3, 2],
            "page_id" => &[1_i64, 1, 2],
            "page_title" => &["Alpha", "Alpha", "Zulu"],
            "page_namespace" => &[0_i32, 0, 0],
            "edits" => &[5_u32, 9, 4],
            "previous_week_edits" => &[0_u32, 5, 0],
            "wow_change" => &[5_i64, 4, 4],
            "wow_rate" => &[None, Some(0.8_f64), None],
            "wiki" => &["nlwiki", "nlwiki", "ptwiki"],
        )
        .expect("static fixture columns have equal lengths")),
        (
        "patrol",
        df!(
            "year_month" => &["2026-01", "2026-01"],
            "wiki" => &["nlwiki", "ptwiki"],
            "page_namespace" => &[0_i32, 0],
            "user_type" => &["registered", "registered"],
            "total_patrols" => &[10_i64, 8],
            "unique_patrollers" => &[3_i32, 2],
            "patrol_new_pages" => &[4_i64, 3],
            "patrol_diffs" => &[6_i64, 5],
            "median_latency_hours" => &[1.5_f64, 2.0],
            "p90_latency_hours" => &[4.0_f64, 5.0],
            "patrolled_revisions" => &[8_i64, 6],
            "autopatrolled_revisions" => &[2_i64, 2],
            "total_revisions" => &[12_i64, 10],
            "patrol_coverage_pct" => &[80.0_f64, 75.0],
            "adjusted_coverage_pct" => &[85.0_f64, 80.0],
            "top1_pct" => &[40.0_f64, 45.0],
            "min_patrollers_50pct" => &[2_i32, 1],
        )
        .expect("static fixture columns have equal lengths")),
    ];
    for (name, source_frame) in &fixtures {
        let frwiki = source_frame
            .clone()
            .lazy()
            .filter(col("wiki").eq(lit("nlwiki")))
            .with_columns([lit("frwiki").alias("wiki")])
            .collect()?;
        let mut frame = source_frame.clone();
        frame.vstack_mut(&frwiki)?;
        for wiki in wiki_set(&frame)? {
            let wiki_dir = output_dir.join(&wiki);
            fs::create_dir_all(&wiki_dir)?;
            let partition = frame
                .clone()
                .lazy()
                .filter(col("wiki").eq(lit(wiki.as_str())))
                .collect()?;
            write_parquet(&wiki_dir, name, partition)?;
        }
        write_parquet(output_dir, name, frame)?;
    }
    crate::browser_data::materialize(output_dir, None)?;
    materialize(output_dir)?;
    let browser_index =
        crate::browser_data::read_index(&output_dir.join(crate::browser_data::INDEX_FILENAME))?;
    let downloadable_artifacts: Vec<Value> = fixtures
        .iter()
        .map(|(name, _)| {
            json!({
                "name": format!("{name}.parquet"),
                "license_spdx": licensing::ARTIFACT_LICENSE_SPDX,
                "media_type": "application/vnd.apache.parquet",
            })
        })
        .chain(ARTIFACTS.map(|name| {
            json!({
                "name": name,
                "license_spdx": licensing::ARTIFACT_LICENSE_SPDX,
                "media_type": "application/json",
            })
        }))
        .chain(std::iter::once(json!({
            "name": crate::browser_data::INDEX_FILENAME,
            "license_spdx": licensing::ARTIFACT_LICENSE_SPDX,
            "media_type": "application/json",
        })))
        .chain(browser_index.entries.iter().map(|entry| {
            json!({
                "name": entry.file,
                "license_spdx": licensing::ARTIFACT_LICENSE_SPDX,
                "media_type": "application/vnd.apache.parquet",
                "rows": entry.rows,
                "bytes": entry.bytes,
                "sha256": entry.sha256,
            })
        }))
        .collect();
    let policy = licensing::publication_policy()?;
    let root_package: Value = serde_json::from_str(include_str!("../package.json"))?;
    let site_package: Value = serde_json::from_str(include_str!("../site/package.json"))?;
    let browser_closure: Value =
        serde_json::from_str(include_str!("../config/site-dependency-closure.json"))?;
    publish_json_set(
        output_dir,
        &BTreeMap::from([(
            "manifest.json",
            json!({
                "schema_version": 3,
                "generated_at": "2026-01-31T00:00:00Z",
                "license": policy.license,
                "attribution": policy.attribution,
                "independence_notice": policy.independence_notice,
                "source_datasets": policy.source_datasets,
                "trademark": policy.trademark,
                "privacy": policy.privacy,
                "toolforge_open_licensing": policy.toolforge,
                "provenance": {
                    "run_id": "site-fixture",
                    "generating_commit": licensing::generating_commit(),
                    "generated_at": "2026-01-31T00:00:00Z",
                    "selected_snapshot_versions": {"frwiki": "2026-01", "nlwiki": "2026-01", "ptwiki": "2026-01"},
                    "release_environment": {
                        "schema_version": 1,
                        "source": "deterministic-site-fixture",
                        "runtime": {
                            "node": root_package["engines"]["node"],
                            "npm": root_package["engines"]["npm"],
                            "rust": env!("CARGO_PKG_RUST_VERSION"),
                        },
                        "browser_packages": {
                            "direct": site_package["dependencies"],
                            "generated": browser_closure["generated_packages"],
                        },
                        "system": {"status": "not-applicable-to-deterministic-fixture"},
                    },
                },
                "data_dir": "fixture",
                "output_dir": "fixture",
                "lifecycle": {"wikis": {}},
                "wikis": {"frwiki": {"status": "complete"}, "nlwiki": {"status": "complete"}, "ptwiki": {"status": "complete"}},
                "merged": [],
                "browser_data": browser_index,
                "downloadable_artifacts": downloadable_artifacts,
            }),
        )]),
    )
}

pub fn write_browser_performance_fixture(output_dir: &Path) -> Result<()> {
    write_site_fixture(output_dir)?;
    for (wiki, target_rows) in [
        ("nlwiki", 6_000_usize),
        ("ptwiki", 3_000),
        ("frwiki", 21_000),
    ] {
        for (metric, _) in crate::browser_data::BROWSER_METRICS {
            let path = output_dir.join(wiki).join(format!("{metric}.parquet"));
            let frame = ParquetReader::new(File::open(&path)?)
                .set_low_memory(true)
                .read_parallel(ParallelStrategy::None)
                .finish()?;
            ensure!(frame.height() > 0, "performance fixture source is empty");
            let indices = (0..target_rows)
                .map(|index| (index % frame.height()) as IdxSize)
                .collect();
            let expanded = frame.take(&IdxCa::from_vec("fixture_rows".into(), indices))?;
            write_parquet(&output_dir.join(wiki), metric, expanded)?;
        }
    }
    crate::browser_data::materialize(output_dir, None)?;
    refresh_fixture_browser_manifest(output_dir)
}

fn refresh_fixture_browser_manifest(output_dir: &Path) -> Result<()> {
    let index =
        crate::browser_data::read_index(&output_dir.join(crate::browser_data::INDEX_FILENAME))?;
    let manifest_path = output_dir.join("manifest.json");
    let mut manifest: Value = serde_json::from_slice(&fs::read(&manifest_path)?)?;
    let downloads = manifest
        .get_mut("downloadable_artifacts")
        .and_then(Value::as_array_mut)
        .context("fixture manifest has no downloadable artifact list")?;
    downloads.retain(|artifact| {
        artifact["name"].as_str().is_none_or(|name| {
            name != crate::browser_data::INDEX_FILENAME && !name.starts_with("browser-data/")
        })
    });
    downloads.push(json!({
        "name": crate::browser_data::INDEX_FILENAME,
        "license_spdx": licensing::ARTIFACT_LICENSE_SPDX,
        "media_type": "application/json",
    }));
    downloads.extend(index.entries.iter().map(|entry| {
        json!({
            "name": entry.file,
            "license_spdx": licensing::ARTIFACT_LICENSE_SPDX,
            "media_type": "application/vnd.apache.parquet",
            "rows": entry.rows,
            "bytes": entry.bytes,
            "sha256": entry.sha256,
        })
    }));
    downloads.sort_by(|left, right| left["name"].as_str().cmp(&right["name"].as_str()));
    manifest["browser_data"] = serde_json::to_value(index)?;
    publish_json_set(output_dir, &BTreeMap::from([("manifest.json", manifest)]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::TestDir;

    fn read_json(path: &Path) -> Result<Value> {
        Ok(serde_json::from_slice(&fs::read(path)?)?)
    }

    #[test]
    fn fixture_generates_complete_deterministic_dashboard_contract() -> Result<()> {
        let first = TestDir::new()?;
        let second = TestDir::new()?;

        write_site_fixture(first.path())?;
        write_site_fixture(second.path())?;

        for artifact in ARTIFACTS {
            let first_bytes = fs::read(first.path().join(artifact))?;
            let second_bytes = fs::read(second.path().join(artifact))?;
            assert_eq!(first_bytes, second_bytes, "{artifact} is not deterministic");
            serde_json::from_slice::<Value>(&first_bytes)?;
        }
        let gdp = read_json(&first.path().join("defaults_gdp.json"))?;
        assert_eq!(gdp["defaultWiki"], ALL_WIKIS_SCOPE);
        assert_eq!(gdp["maxMonth"], "2026-01");
        assert_eq!(gdp["output"][1]["total_edits"], 32);
        assert_eq!(
            gdp["rangeByWiki"]
                .as_array()
                .and_then(|ranges| ranges.iter().find(|range| range["wiki"] == ALL_WIKIS_SCOPE)),
            Some(&json!({"wiki": ALL_WIKIS_SCOPE, "mn": "2025-12", "mx": "2026-01"}))
        );

        let inequality = read_json(&first.path().join("defaults_inequality.json"))?;
        assert_eq!(inequality["data"][0]["total_editors"], 16);
        assert_eq!(inequality["data"][0]["gini"], json!(0.425));

        let labor = read_json(&first.path().join("defaults_labor.json"))?;
        assert_eq!(labor["churn"][1]["active_editors"], 16);
        assert_eq!(labor["churn"][1]["arrivals"], 5);

        let variation = read_json(&first.path().join("defaults_edit_variation.json"))?;
        assert_eq!(variation["summary"][0]["rows"], 2);
        assert_eq!(variation["topVariation"][0]["week_end"], "2026-01-18");
        assert_eq!(variation["topVariation"][0]["page_title"], "Alpha");

        let manifest = read_json(&first.path().join("manifest.json"))?;
        assert_eq!(manifest["generated_at"], "2026-01-31T00:00:00Z");
        assert_eq!(manifest["license"]["spdx_identifier"], "MIT");
        assert_eq!(manifest["provenance"]["run_id"], "site-fixture");
        assert_eq!(manifest["browser_data"]["schema_version"], 1);
        assert_eq!(manifest["downloadable_artifacts"][0]["license_spdx"], "MIT");
        assert_eq!(
            manifest["toolforge_open_licensing"]["open_data_license_spdx"],
            "MIT"
        );
        Ok(())
    }

    #[test]
    fn scoped_dashboard_reducers_enforce_bounds_and_zero_denominators() -> Result<()> {
        let output = TestDir::new()?;
        write_site_fixture(output.path())?;
        let mut frames = Frames::read(output.path())?;

        let gdp_meta = common_meta_with_overrides(
            &frames.gdp,
            Some(&frames.gdp),
            Some("nlwiki".to_string()),
            Some("2025-12".to_string()),
        )
        .expect("bounded GDP metadata is valid");
        let (gdp, _) = gdp_artifacts_with_meta(&frames, gdp_meta)?;
        assert_eq!(gdp["output"].as_array().map(Vec::len), Some(1));
        assert_eq!(gdp["typeShare"].as_array().map(Vec::len), Some(0));

        let labor_meta = common_meta_with_overrides(
            &frames.labor,
            Some(&frames.labor),
            Some("nlwiki".to_string()),
            Some("2025-12".to_string()),
        )
        .expect("bounded labor metadata is valid");
        let (labor, _) = labor_artifacts_with_meta(&frames, labor_meta)?;
        assert_eq!(labor["workforce"].as_array().map(Vec::len), Some(1));

        assert_eq!(cohort_rows(&frames, "nlwiki")?.len(), 2);
        assert_eq!(funnel_rows(&frames.business_funnel, "nlwiki")?.len(), 2);
        assert_eq!(
            churn_rows(&frames.churn, "nlwiki", "month", "2025-12")?.len(),
            1
        );

        frames
            .patrol
            .replace(
                "total_revisions",
                Column::new(
                    "total_revisions".into(),
                    vec![0_i64; frames.patrol.height()],
                ),
            )
            .expect("fixture patrol schema accepts a zero revision denominator");
        let (patrol, _) = patrol_artifacts(&frames)?;
        assert_eq!(patrol["patrol"][0]["patrol_coverage_pct"], 0);
        assert_eq!(patrol["patrol"][0]["adjusted_coverage_pct"], 0);
        Ok(())
    }

    #[test]
    fn browser_performance_fixture_has_deterministic_nl_pt_and_fr_profiles() -> Result<()> {
        let first = TestDir::new()?;
        let second = TestDir::new()?;
        write_browser_performance_fixture(first.path())?;
        write_browser_performance_fixture(second.path())?;
        let first_index = crate::browser_data::read_index(
            &first.path().join(crate::browser_data::INDEX_FILENAME),
        )
        .expect("first performance fixture index is valid");
        let second_index = crate::browser_data::read_index(
            &second.path().join(crate::browser_data::INDEX_FILENAME),
        )
        .expect("second performance fixture index is valid");
        assert_eq!(first_index, second_index);
        for (wiki, expected) in [("nlwiki", 6_000), ("ptwiki", 3_000), ("frwiki", 21_000)] {
            assert!(
                first_index
                    .entries
                    .iter()
                    .filter(|entry| entry.wiki == wiki)
                    .all(|entry| entry.rows == expected)
            );
        }
        Ok(())
    }

    #[test]
    fn empty_page_weekly_metric_fails_closed() -> Result<()> {
        let output = TestDir::new()?;
        write_site_fixture(output.path())?;
        let default_weekly = output.path().join("frwiki/page_weekly_edits.parquet");
        let empty = ParquetReader::new(File::open(&default_weekly)?)
            .finish()?
            .head(Some(0));
        write_parquet(
            default_weekly
                .parent()
                .expect("default weekly fixture has a parent"),
            "page_weekly_edits",
            empty,
        )
        .expect("the empty fixture is written to a valid temporary directory");

        let previous = fs::read(output.path().join("defaults_edit_variation.json"))?;
        let error = materialize(output.path()).expect_err("an empty metric must fail publication");
        assert!(
            error
                .to_string()
                .contains("page_weekly_edits.parquet is empty")
        );
        assert_eq!(
            fs::read(output.path().join("defaults_edit_variation.json"))?,
            previous
        );
        Ok(())
    }

    #[test]
    fn default_weekly_partition_rejects_rows_for_another_wiki() -> Result<()> {
        let output = TestDir::new()?;
        write_site_fixture(output.path())?;
        let default_dir = output.path().join("frwiki");
        let mut mislabeled =
            ParquetReader::new(File::open(default_dir.join("page_weekly_edits.parquet"))?)
                .finish()?;
        let wiki = Column::new("wiki".into(), vec!["nlwiki"; mislabeled.height()]);
        mislabeled.replace("wiki", wiki)?;
        write_parquet(&default_dir, "page_weekly_edits", mislabeled)?;

        let error = materialize(output.path())
            .expect_err("a per-wiki partition carrying another wiki must fail");
        assert!(error.to_string().contains("unexpected wiki nlwiki"));
        Ok(())
    }

    #[test]
    fn scalar_conversion_helpers_cover_supported_and_invalid_types() -> Result<()> {
        let frame = DataFrame::new(
            1,
            vec![
                Column::new("null".into(), &[None::<i64>]),
                Column::new("string".into(), &["value"]),
                Column::new("u32".into(), &[7_u32]),
                Column::new("u64".into(), &[11_u64]),
                Column::new("i32".into(), &[-3_i32]),
                Column::new("i64".into(), &[-9_i64]),
                Column::new("f64".into(), &[1.5_f64]),
            ],
        )
        .expect("scalar fixture columns have equal lengths");

        assert_eq!(string(&frame, "null", 0)?, None);
        assert!(string(&frame, "u32", 0).is_err());
        assert_eq!(integer(&frame, "null", 0)?, None);
        assert_eq!(integer(&frame, "u32", 0)?, Some(7));
        assert_eq!(integer(&frame, "i32", 0)?, Some(-3));
        assert_eq!(integer(&frame, "i64", 0)?, Some(-9));
        assert!(integer(&frame, "string", 0).is_err());
        assert!(integer(&frame, "u64", 0).is_err());
        assert_eq!(float(&frame, "null", 0)?, None);
        assert_eq!(float(&frame, "u32", 0)?, Some(7.0));
        assert_eq!(float(&frame, "i32", 0)?, Some(-3.0));
        assert_eq!(float(&frame, "i64", 0)?, Some(-9.0));
        assert_eq!(float(&frame, "f64", 0)?, Some(1.5));
        assert!(float(&frame, "string", 0).is_err());
        assert!(float(&frame, "u64", 0).is_err());

        assert_eq!(number(f64::NAN), Value::Null);
        assert_eq!(number(4.0), json!(4));
        assert_eq!(number(1.25), json!(1.25));
        let mut average = Average::default();
        assert_eq!(average.value(), Value::Null);
        average.add(None);
        average.add(Some(3.0));
        assert_eq!(average.value(), json!(3));
        Ok(())
    }

    #[test]
    fn metadata_date_and_sort_helpers_cover_edge_cases() -> Result<()> {
        let range = df!(
            "wiki" => &[Some("awiki"), Some("awiki"), Some("awiki"), None, Some("awiki")],
            "year_month" => &[Some("2026-02"), Some("2026-01"), Some("2026-03"), Some("2026-01"), None],
            "page_namespace" => &[Some(0_i32), None, Some(1), Some(2), Some(3)],
        )
        .expect("metadata fixture columns have equal lengths");
        let meta = common_meta(&range, Some(&range))?;
        assert_eq!(meta.default_wiki, ALL_WIKIS_SCOPE);
        assert_eq!(
            meta.ranges,
            vec![
                json!({"wiki":"awiki", "mn":"2026-01", "mx":"2026-03"}),
                json!({"wiki":ALL_WIKIS_SCOPE, "mn":"2026-01", "mx":"2026-03"}),
            ]
        );
        assert!(
            meta.namespaces
                .contains(&json!({"wiki":"awiki", "page_namespace": Value::Null}))
        );
        let bounded = common_meta_with_overrides(
            &range,
            None,
            Some("awiki".to_string()),
            Some("2026-02".to_string()),
        )
        .expect("the fixture contains a valid bounded range");
        assert_eq!(bounded.ranges[0]["mx"], "2026-02");
        assert!(
            common_meta_with_overrides(&range, None, None, Some("invalid".to_string())).is_err()
        );
        let empty = df!(
            "wiki" => &[None::<&str>],
            "year_month" => &[None::<&str>],
        )
        .expect("empty metadata fixture columns have equal lengths");
        assert!(common_meta_with_overrides(&empty, None, None, None).is_err());

        assert!(year("20").is_err());
        assert!(quarter("2026").is_err());
        assert!(quarter("2026-00").is_err());
        assert_eq!(quarter("2026-12")?, "2026-Q4");
        assert_eq!(compare_optional_i64(Some(1), Some(2)), Ordering::Less);
        assert_eq!(compare_optional_i64(Some(1), None), Ordering::Less);
        assert_eq!(compare_optional_i64(None, Some(1)), Ordering::Greater);
        assert_eq!(compare_optional_i64(None, None), Ordering::Equal);

        let mut values = vec![json!({"a":"z", "b":"a"}), json!({"a":"a"})];
        sort_two_strings(&mut values, "a", "b");
        assert_eq!(string_field(&values[0], "missing"), "");
        assert_eq!(values[0]["a"], "a");
        Ok(())
    }

    #[test]
    fn variation_selector_retains_only_the_deterministic_top_twenty() {
        let row = |change: i64, edits: i64, title: &str, week: &str| VariationRow {
            week: week.to_string(),
            title: title.to_string(),
            previous_week_edits: 1,
            edits: Some(edits),
            wow_change: Some(change),
            wow_rate: None,
        };
        let mut rows = Vec::new();
        for change in 0..20 {
            retain_top_variation(&mut rows, row(change, 1, "Same", "2026-01-01"));
        }
        retain_top_variation(&mut rows, row(-1, 99, "Ignored", "2026-01-01"));
        retain_top_variation(&mut rows, row(100, 1, "Best", "2026-01-01"));
        assert_eq!(rows.len(), 20);
        rows.sort_by(variation_order);
        assert_eq!(rows[0].wow_change, Some(100));
        assert_eq!(rows.last().and_then(|row| row.wow_change), Some(1));

        assert_eq!(
            variation_order(
                &row(5, 2, "Zulu", "2026-01-02"),
                &row(5, 1, "Alpha", "2026-01-01")
            ),
            Ordering::Less
        );
        assert_eq!(
            variation_order(
                &row(5, 1, "Alpha", "2026-01-02"),
                &row(5, 1, "Alpha", "2026-01-01")
            ),
            Ordering::Greater
        );
    }

    #[test]
    fn atomic_json_publication_cleans_staged_files_on_rename_failure() -> Result<()> {
        let output = TestDir::new()?;
        fs::create_dir(output.path().join("blocked.json"))?;
        let artifacts = BTreeMap::from([
            ("blocked.json", json!({"blocked": true})),
            ("later.json", json!({"later": true})),
        ]);

        let error = publish_json_set(output.path(), &artifacts)
            .expect_err("a directory cannot be replaced by a staged JSON file");
        assert!(
            error
                .to_string()
                .contains("failed to publish dashboard artifact")
        );
        assert!(
            !output
                .path()
                .join(format!(".later.json.{}.tmp", std::process::id()))
                .exists()
        );
        Ok(())
    }

    #[test]
    fn artifact_queries_propagate_malformed_input_errors() -> Result<()> {
        let output = TestDir::new()?;
        write_site_fixture(output.path())?;

        let mut labor_frames = Frames::read(output.path())?;
        labor_frames.churn.drop_in_place("period")?;
        assert!(labor_artifacts(&labor_frames).is_err());

        let mut business_churn_frames = Frames::read(output.path())?;
        business_churn_frames.churn.drop_in_place("period")?;
        assert!(business_artifacts(&business_churn_frames).is_err());

        let mut business_funnel_frames = Frames::read(output.path())?;
        business_funnel_frames
            .business_funnel
            .drop_in_place("cohort_size")?;
        assert!(business_artifacts(&business_funnel_frames).is_err());
        Ok(())
    }

    #[test]
    fn fixture_writer_reports_parquet_publication_failures() -> Result<()> {
        let output = TestDir::new()?;
        fs::create_dir(output.path().join("business_funnel.parquet"))?;
        assert!(write_site_fixture(output.path()).is_err());
        Ok(())
    }

    #[test]
    fn overview_from_gdp_skips_null_and_future_rows() -> Result<()> {
        let gdp = df!(
            "wiki" => &[None, Some("xwiki"), Some("xwiki"), Some("xwiki")],
            "year_month" => &[Some("2026-01"), None, Some("2026-02"), Some("2026-01")],
            "page_namespace" => &[Some(0_i32), Some(0), Some(0), Some(0)],
            "user_type" => &["registered", "registered", "registered", "registered"],
            "gross_bytes_added" => &[100_i64, 100, 100, 40],
            "net_bytes" => &[80_i64, 80, 80, 30],
            "total_edits" => &[10_u32, 10, 10, 4],
            "reverted_edits" => &[2_u32, 2, 2, 1],
            "unique_editors" => &[5_u32, 5, 5, 2],
        )
        .expect("overview fixture columns have equal lengths");
        let meta = CommonMeta {
            default_wiki: "xwiki".to_string(),
            max_month: "2026-01".to_string(),
            wikis: vec![json!("xwiki")],
            namespaces: vec![],
            ranges: vec![],
        };

        let overview = overview_from_gdp(&gdp, &meta)?;
        assert_eq!(overview["maxMonth"], "2026-01");
        assert_eq!(overview["trend"].as_array().map(Vec::len), Some(1));
        assert_eq!(overview["trend"][0]["total_edits"], 4);
        assert_eq!(overview["byWiki"].as_array().map(Vec::len), Some(1));
        assert_eq!(overview["byWiki"][0]["wiki"], "xwiki");
        assert_eq!(overview["byWiki"][0]["latestMonth"], "2026-01");
        assert_eq!(overview["byWiki"][0]["total_edits"], 4);
        Ok(())
    }
}
