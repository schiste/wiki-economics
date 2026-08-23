use anyhow::{Context, Result, bail, ensure};
use chrono::{Duration, NaiveDate};
use polars::prelude::*;
use serde_json::{Map, Value, json};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;

pub const ARTIFACTS: [&str; 11] = [
    "defaults_business.json",
    "defaults_edit_variation.json",
    "defaults_gdp.json",
    "defaults_inequality.json",
    "defaults_labor.json",
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
    count: u64,
}

impl Average {
    fn add(&mut self, value: Option<f64>) {
        if let Some(value) = value {
            self.sum += value;
            self.count += 1;
        }
    }

    fn value(&self) -> Value {
        if self.count == 0 {
            Value::Null
        } else {
            number(self.sum / self.count as f64)
        }
    }
}

pub fn materialize(output_dir: &Path) -> Result<()> {
    let frames = Frames::read(output_dir)?;
    let mut artifacts = BTreeMap::new();
    let (defaults_gdp, meta_gdp) = gdp_artifacts(&frames)?;
    let (defaults_labor, meta_labor) = labor_artifacts(&frames)?;
    let (defaults_business, meta_business) = business_artifacts(&frames)?;
    let (defaults_inequality, meta_inequality) = inequality_artifacts(&frames)?;
    let (defaults_patrol, meta_patrol) = patrol_artifacts(&frames)?;

    artifacts.insert("defaults_business.json", defaults_business);
    artifacts.insert(
        "defaults_edit_variation.json",
        edit_variation_artifact(output_dir)?,
    );
    artifacts.insert("defaults_gdp.json", defaults_gdp);
    artifacts.insert("defaults_inequality.json", defaults_inequality);
    artifacts.insert("defaults_labor.json", defaults_labor);
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

fn any_json(df: &DataFrame, column: &str, row: usize) -> Result<Value> {
    match df.column(column)?.get(row)? {
        AnyValue::Null => Ok(Value::Null),
        AnyValue::Boolean(value) => Ok(json!(value)),
        AnyValue::String(value) => Ok(json!(value)),
        AnyValue::StringOwned(value) => Ok(json!(value.as_str())),
        AnyValue::UInt32(value) => Ok(json!(value)),
        AnyValue::Int32(value) => Ok(json!(value)),
        AnyValue::Int64(value) => Ok(json!(value)),
        AnyValue::Float64(value) => Ok(number(value)),
        value => bail!("unsupported dashboard JSON value {value:?} in {column}"),
    }
}

fn row_json(df: &DataFrame, row: usize, columns: &[&str]) -> Result<Value> {
    let mut output = Map::new();
    for column in columns {
        output.insert((*column).to_string(), any_json(df, column, row)?);
    }
    Ok(Value::Object(output))
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
        env::var("DEFAULT_WIKI").ok(),
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
    let default_wiki = default_wiki
        .or_else(|| wiki_names.first().cloned())
        .context("dashboard input contains no wiki")?;
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

    let namespaces = namespaces
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
        .collect();

    Ok(CommonMeta {
        default_wiki,
        max_month,
        wikis: wiki_names
            .into_iter()
            .map(|wiki| json!({"wiki": wiki}))
            .collect(),
        namespaces,
        ranges: ranges
            .into_iter()
            .map(|(wiki, (mn, mx))| json!({"wiki": wiki, "mn": mn, "mx": mx}))
            .collect(),
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
    Ok(string(df, "wiki", row)?.as_deref() == Some(wiki)
        && string(df, "year_month", row)?.is_some_and(|month| month.as_str() <= max_month))
}

fn direct_rows<F>(df: &DataFrame, columns: &[&str], mut keep: F) -> Result<Vec<Value>>
where
    F: FnMut(usize) -> Result<bool>,
{
    let mut rows = Vec::new();
    for row in 0..df.height() {
        if keep(row)? {
            rows.push(row_json(df, row, columns)?);
        }
    }
    Ok(rows)
}

fn gdp_artifacts(frames: &Frames) -> Result<(Value, Value)> {
    let meta = common_meta(&frames.gdp, Some(&frames.gdp))?;
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

    let mut churn = direct_rows(
        &frames.churn,
        &[
            "period",
            "period_type",
            "active_editors",
            "arrivals",
            "departures",
            "arrival_rate",
            "departure_rate",
        ],
        |row| {
            Ok(
                string(&frames.churn, "wiki", row)?.as_deref() == Some(&meta.default_wiki)
                    && string(&frames.churn, "period_type", row)?.as_deref() == Some("month")
                    && string(&frames.churn, "period", row)?
                        .is_some_and(|period| period <= meta.max_month),
            )
        },
    )?;
    churn.sort_by(|left, right| string_field(left, "period").cmp(string_field(right, "period")));
    let mut cohorts = cohort_rows(frames, &meta.default_wiki)?;
    sort_two_strings(&mut cohorts, "cohort_year", "year");

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
    direct_rows(
        &frames.cohorts,
        &["cohort_year", "year", "initial_editors", "survived_editors"],
        |row| Ok(string(&frames.cohorts, "wiki", row)?.as_deref() == Some(wiki)),
    )
}

fn string_field<'a>(value: &'a Value, field: &str) -> &'a str {
    value.get(field).and_then(Value::as_str).unwrap_or("")
}

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
    let mut churn = direct_rows(
        &frames.churn,
        &[
            "period",
            "period_type",
            "active_editors",
            "arrivals",
            "departures",
            "arrival_rate",
            "departure_rate",
        ],
        |row| {
            Ok(
                string(&frames.churn, "wiki", row)?.as_deref() == Some(&meta.default_wiki)
                    && string(&frames.churn, "period_type", row)?.as_deref() == Some("quarter")
                    && string(&frames.churn, "period", row)?
                        .is_some_and(|period| period.as_str() <= max_quarter.as_str()),
            )
        },
    )?;
    churn.sort_by(|left, right| string_field(left, "period").cmp(string_field(right, "period")));

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

    let mut cohorts = cohort_rows(frames, &meta.default_wiki)?;
    sort_two_strings(&mut cohorts, "cohort_year", "year");
    let mut funnel =
        direct_rows(
            &frames.business_funnel,
            &[
                "cohort_year",
                "cohort_size",
                "reached_5",
                "reached_25",
                "reached_100",
                "wiki",
            ],
            |row| {
                Ok(string(&frames.business_funnel, "wiki", row)?.as_deref()
                    == Some(&meta.default_wiki))
            },
        )?;
    funnel.sort_by(|left, right| {
        string_field(left, "cohort_year").cmp(string_field(right, "cohort_year"))
    });

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
        entry.total_editors += float(&frames.inequality, "total_editors", row)?.unwrap_or_default();
        entry.total_edits += float(&frames.inequality, "total_edits", row)?.unwrap_or_default();
        entry.min_editors_50pct +=
            float(&frames.inequality, "min_editors_50pct", row)?.unwrap_or_default();
        entry.gini.add(float(&frames.inequality, "gini", row)?);
        entry.theil.add(float(&frames.inequality, "theil", row)?);
        entry.palma.add(float(&frames.inequality, "palma", row)?);
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
    patrol_coverage_pct: Average,
    adjusted_coverage_pct: Average,
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
        entry.total_patrols += float(&frames.patrol, "total_patrols", row)?.unwrap_or_default();
        entry.unique_patrollers +=
            float(&frames.patrol, "unique_patrollers", row)?.unwrap_or_default();
        entry.patrol_new_pages +=
            float(&frames.patrol, "patrol_new_pages", row)?.unwrap_or_default();
        entry.patrol_diffs += float(&frames.patrol, "patrol_diffs", row)?.unwrap_or_default();
        entry
            .median_latency_hours
            .add(float(&frames.patrol, "median_latency_hours", row)?);
        entry
            .p90_latency_hours
            .add(float(&frames.patrol, "p90_latency_hours", row)?);
        entry.patrolled_revisions +=
            float(&frames.patrol, "patrolled_revisions", row)?.unwrap_or_default();
        entry.autopatrolled_revisions +=
            float(&frames.patrol, "autopatrolled_revisions", row)?.unwrap_or_default();
        entry.total_revisions += float(&frames.patrol, "total_revisions", row)?.unwrap_or_default();
        entry
            .patrol_coverage_pct
            .add(float(&frames.patrol, "patrol_coverage_pct", row)?);
        entry
            .adjusted_coverage_pct
            .add(float(&frames.patrol, "adjusted_coverage_pct", row)?);
        entry.top1_pct.add(float(&frames.patrol, "top1_pct", row)?);
        entry.min_patrollers_50pct +=
            float(&frames.patrol, "min_patrollers_50pct", row)?.unwrap_or_default();
    }
    let patrol = yearly
        .into_iter()
        .map(|(period, entry)| {
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
                "patrol_coverage_pct": entry.patrol_coverage_pct.value(),
                "adjusted_coverage_pct": entry.adjusted_coverage_pct.value(),
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

fn edit_variation_artifact(output_dir: &Path) -> Result<Value> {
    let path = output_dir.join("page_weekly_edits.parquet");
    let mut metadata_reader = ParquetReader::new(File::open(&path)?);
    let rows = metadata_reader.num_rows()?;
    ensure!(rows > 0, "page_weekly_edits.parquet is empty");
    let first = ParquetReader::new(File::open(&path)?)
        .with_columns(Some(vec!["wiki".to_string()]))
        .with_slice(Some((0, 1)))
        .finish()?;
    let default_wiki = env::var("DEFAULT_WIKI")
        .ok()
        .or_else(|| string(&first, "wiki", 0).ok().flatten())
        .context("page_weekly_edits.parquet has no wiki")?;
    let path_string = path.to_string_lossy().into_owned();
    let filtered = LazyFrame::scan_parquet(path_string.as_str().into(), Default::default())?
        .filter(col("wiki").eq(lit(default_wiki.clone())))
        .filter(col("page_namespace").eq(lit(0_i32)));
    let summary = filtered
        .clone()
        .select([
            len().alias("rows"),
            col("week_start").min().alias("min_week"),
            col("week_start").max().alias("max_week"),
        ])
        .collect_with_engine(Engine::Streaming)?
        .unwrap_single();
    let top = filtered
        .filter(col("previous_week_edits").gt(lit(0_u32)))
        .sort(
            ["wow_change", "edits", "page_title", "week_start"],
            SortMultipleOptions::default()
                .with_order_descending_multi([true, true, false, false])
                .with_nulls_last(true),
        )
        .limit(20)
        .select([
            col("week_start"),
            col("page_title"),
            col("previous_week_edits"),
            col("edits"),
            col("wow_change"),
            col("wow_rate"),
        ])
        .collect_with_engine(Engine::Streaming)?
        .unwrap_single();

    let matching_rows = integer(&summary, "rows", 0)?.unwrap_or_default();
    let min_week = string(&summary, "min_week", 0)?;
    let max_week = string(&summary, "max_week", 0)?;
    let mut best = Vec::with_capacity(top.height());
    for row in 0..top.height() {
        let week = string(&top, "week_start", row)?.context("variation week is null")?;
        let parsed_week = NaiveDate::parse_from_str(&week, "%Y-%m-%d")?;
        best.push(json!({
            "week_start": week,
            "week_end": (parsed_week + Duration::days(6)).to_string(),
            "page_title": string(&top, "page_title", row)?.context("variation title is null")?,
            "previous_week_edits": integer(&top, "previous_week_edits", row)?.unwrap_or_default(),
            "edits": integer(&top, "edits", row)?.unwrap_or_default(),
            "wow_change": integer(&top, "wow_change", row)?.unwrap_or_default(),
            "wow_rate": float(&top, "wow_rate", row)?.map(number).unwrap_or(Value::Null),
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
            "wiki" => &["awiki", "awiki", "zwiki"],
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
            "wiki" => &["awiki", "awiki", "zwiki", "awiki", "awiki"],
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
            "wiki" => &["awiki", "zwiki"],
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
            "wiki" => &["awiki", "zwiki"],
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
            "wiki" => &["awiki", "zwiki"],
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
            "wiki" => &["awiki", "awiki", "awiki", "awiki", "zwiki", "zwiki"],
        )
        .expect("static fixture columns have equal lengths")),
        (
        "labor_cohorts",
        df!(
            "cohort_year" => &["2025", "2024", "2025"],
            "year" => &["2026", "2025", "2026"],
            "survived_editors" => &[5_u32, 2, 3],
            "initial_editors" => &[10_u32, 4, 8],
            "wiki" => &["awiki", "awiki", "zwiki"],
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
            "wiki" => &["awiki", "awiki", "zwiki", "awiki", "awiki", "awiki"],
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
            "wiki" => &["awiki", "awiki", "zwiki"],
        )
        .expect("static fixture columns have equal lengths")),
        (
        "patrol",
        df!(
            "year_month" => &["2026-01", "2026-01"],
            "wiki" => &["awiki", "zwiki"],
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
    for (name, frame) in fixtures {
        write_parquet(output_dir, name, frame)?;
    }
    materialize(output_dir)?;
    publish_json_set(
        output_dir,
        &BTreeMap::from([(
            "manifest.json",
            json!({
                "schema_version": 2,
                "generated_at": "2026-01-31T00:00:00Z",
                "data_dir": "fixture",
                "output_dir": "fixture",
                "lifecycle": {"wikis": {}},
                "wikis": {"awiki": {"status": "complete"}, "zwiki": {"status": "complete"}},
                "merged": [],
            }),
        )]),
    )
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
        assert_eq!(gdp["defaultWiki"], "awiki");
        assert_eq!(gdp["maxMonth"], "2026-01");
        assert_eq!(gdp["output"][1]["total_edits"], 12);

        let variation = read_json(&first.path().join("defaults_edit_variation.json"))?;
        assert_eq!(variation["summary"][0]["rows"], 2);
        assert_eq!(variation["topVariation"][0]["week_end"], "2026-01-18");
        assert_eq!(variation["topVariation"][0]["page_title"], "Alpha");

        let manifest = read_json(&first.path().join("manifest.json"))?;
        assert_eq!(manifest["generated_at"], "2026-01-31T00:00:00Z");
        Ok(())
    }

    #[test]
    fn empty_page_weekly_metric_fails_closed() -> Result<()> {
        let output = TestDir::new()?;
        write_site_fixture(output.path())?;
        write_parquet(
            output.path(),
            "page_weekly_edits",
            DataFrame::new(0, vec![Column::new_empty("wiki".into(), &DataType::String)])
                .expect("empty fixture schema is valid"),
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
    fn scalar_conversion_helpers_cover_supported_and_invalid_types() -> Result<()> {
        let frame = DataFrame::new(
            1,
            vec![
                Column::new("null".into(), &[None::<i64>]),
                Column::new("boolean".into(), &[true]),
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

        assert_eq!(any_json(&frame, "null", 0)?, Value::Null);
        assert_eq!(any_json(&frame, "boolean", 0)?, json!(true));
        assert_eq!(any_json(&frame, "string", 0)?, json!("value"));
        assert_eq!(any_json(&frame, "u32", 0)?, json!(7));
        assert_eq!(any_json(&frame, "i32", 0)?, json!(-3));
        assert_eq!(any_json(&frame, "i64", 0)?, json!(-9));
        assert_eq!(any_json(&frame, "f64", 0)?, json!(1.5));
        assert!(any_json(&frame, "u64", 0).is_err());

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
        assert_eq!(meta.default_wiki, "awiki");
        assert_eq!(
            meta.ranges,
            vec![json!({"wiki":"awiki", "mn":"2026-01", "mx":"2026-03"})]
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
}
