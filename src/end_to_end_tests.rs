//! End-to-end pipeline integration test.
//!
//! Exercises ingest -> compute -> merge with a tiny in-process bz2 fixture.
//! Acts as the safety net for any refactor that touches per-metric numeric
//! outputs (in particular, the churn-accumulator consolidation in PR 4 of
//! the audit remediation plan). Snapshots are intentionally integer-only so
//! floating-point rate drift is excluded from the assertion surface; the
//! deterministic counts are what we care about.

use anyhow::Result;
use bzip2::Compression;
use bzip2::write::BzEncoder;
use polars::prelude::*;
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;

use crate::test_support::{TestDir, init_test_tracing};
use crate::{compute, ingest, merge, schema, storage};

fn fixture_row(timestamp: &str, user_id: &str, revision_id: &str) -> String {
    let mut row = vec![String::new(); schema::COLUMNS.len()];
    for (name, value) in [
        ("wiki_db", "tinywiki"),
        ("event_entity", "revision"),
        ("event_type", "create"),
        ("event_timestamp", timestamp),
        ("event_user_id", user_id),
        ("event_user_text", "TestUser"),
        ("event_user_is_anonymous", "false"),
        ("event_user_is_temporary", "false"),
        ("event_user_registration_timestamp", "2023-01-01 00:00:00.0"),
        ("event_user_first_edit_timestamp", timestamp),
        ("page_id", "10"),
        ("page_title", "ExamplePage"),
        ("page_namespace", "0"),
        ("page_namespace_is_content", "true"),
        ("page_is_redirect", "false"),
        ("revision_id", revision_id),
        ("revision_parent_id", "0"),
        ("revision_minor_edit", "false"),
        ("revision_text_bytes", "1200"),
        ("revision_text_bytes_diff", "100"),
        ("revision_is_identity_reverted", "false"),
        ("revision_is_identity_revert", "false"),
    ] {
        let idx = schema::COLUMNS
            .iter()
            .position(|column| column == &name)
            .expect("fixture column should exist");
        row[idx] = value.to_string();
    }
    row.join("\t")
}

fn write_bz2(path: &Path, rows: &[String]) -> Result<()> {
    let file = File::create(path)?;
    let mut encoder = BzEncoder::new(file, Compression::best());
    for row in rows {
        encoder.write_all(row.as_bytes())?;
        encoder.write_all(b"\n")?;
    }
    encoder.finish()?;
    Ok(())
}

fn read_parquet(path: &Path) -> Result<DataFrame> {
    let path_string = path.to_string_lossy().to_string();
    LazyFrame::scan_parquet(path_string.as_str().into(), Default::default())?
        .collect()
        .map_err(Into::into)
}

#[test]
fn pipeline_ingests_computes_and_merges_a_tinywiki_fixture() -> Result<()> {
    init_test_tracing();
    let temp = TestDir::new()?;
    let data_dir = temp.path().join("data");
    let output_dir = temp.path().join("output");
    let raw_dir = data_dir.join("raw").join("tinywiki");
    fs::create_dir_all(&raw_dir)?;

    // Fixture: 4 revisions across 3 months, 3 distinct users.
    //   user 1: 2024-01 + 2024-02   (arrival 2024-01, departure 2024-02)
    //   user 2: 2024-01             (arrival 2024-01, departure 2024-01)
    //   user 3: 2024-03             (arrival 2024-03, departure 2024-03)
    let rows = vec![
        fixture_row("2024-01-15 12:00:00.0", "1", "100"),
        fixture_row("2024-01-20 09:30:00.0", "2", "101"),
        fixture_row("2024-02-15 18:00:00.0", "1", "102"),
        fixture_row("2024-03-10 06:00:00.0", "3", "103"),
    ];
    write_bz2(&raw_dir.join("tinywiki.tsv.bz2"), &rows)?;

    let analytical_paths = ingest::ingest_wiki("tinywiki", &data_dir)?;
    assert!(
        !analytical_paths.is_empty(),
        "ingest should produce at least one analytical parquet partition"
    );

    let marker_dir = storage::analytical_wiki_dir(&data_dir, "tinywiki").join("_markers");
    let markers: Vec<_> = fs::read_dir(&marker_dir)?
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.path()
                .extension()
                .and_then(|ext| ext.to_str())
                .is_some_and(|ext| ext == "done")
        })
        .collect();
    assert_eq!(markers.len(), 1, "exactly one ingest marker should exist");

    compute::compute_all("tinywiki", &data_dir, &output_dir)?;

    let wiki_output_dir = output_dir.join("tinywiki");
    for metric in [
        "labor_monthly",
        "labor_cohorts",
        "labor_churn",
        "gdp",
        "gdp_user_type_share",
        "gdp_activity_tiers",
        "business_funnel",
        "inequality",
    ] {
        let path = wiki_output_dir.join(format!("{metric}.parquet"));
        assert!(path.exists(), "expected {} to exist", path.display());
        let df = read_parquet(&path)?;
        assert!(df.height() > 0, "metric {metric} should have rows");
        assert!(
            df.get_column_names()
                .iter()
                .any(|name| name.as_str() == "wiki"),
            "metric {metric} should carry a wiki column"
        );
    }

    // Snapshot integer-only counts that lock the numeric contract for the
    // pipeline. Refactors that drift any of these mean the new code is no
    // longer numerically equivalent to the old code.
    let labor_monthly = read_parquet(&wiki_output_dir.join("labor_monthly.parquet"))?;
    let registered_rows = labor_monthly
        .clone()
        .lazy()
        .filter(col("user_type").eq(lit("registered")))
        .filter(col("page_namespace").eq(lit(0_i32)))
        .sort(["year_month"], Default::default())
        .collect()?;
    let year_months: Vec<String> = registered_rows
        .column("year_month")?
        .str()?
        .into_iter()
        .map(|opt| opt.unwrap_or("").to_string())
        .collect();
    let unique_editors: Vec<u32> = registered_rows
        .column("unique_editors")?
        .u32()?
        .into_iter()
        .map(|opt| opt.unwrap_or(0))
        .collect();
    let editors_by_month: HashMap<String, u32> =
        year_months.into_iter().zip(unique_editors).collect();
    assert_eq!(editors_by_month.get("2024-01").copied(), Some(2));
    assert_eq!(editors_by_month.get("2024-02").copied(), Some(1));
    assert_eq!(editors_by_month.get("2024-03").copied(), Some(1));

    let labor_churn = read_parquet(&wiki_output_dir.join("labor_churn.parquet"))?;
    let monthly_churn = labor_churn
        .clone()
        .lazy()
        .filter(col("period_type").eq(lit("month")))
        .sort(["period"], Default::default())
        .collect()?;
    let periods: Vec<String> = monthly_churn
        .column("period")?
        .str()?
        .into_iter()
        .map(|opt| opt.unwrap_or("").to_string())
        .collect();
    let active: Vec<u32> = monthly_churn
        .column("active_editors")?
        .u32()?
        .into_iter()
        .map(|opt| opt.unwrap_or(0))
        .collect();
    let arrivals: Vec<u32> = monthly_churn
        .column("arrivals")?
        .u32()?
        .into_iter()
        .map(|opt| opt.unwrap_or(0))
        .collect();
    let departures: Vec<u32> = monthly_churn
        .column("departures")?
        .u32()?
        .into_iter()
        .map(|opt| opt.unwrap_or(0))
        .collect();
    let churn_by_month: HashMap<String, (u32, u32, u32)> = periods
        .into_iter()
        .zip(active)
        .zip(arrivals)
        .zip(departures)
        .map(|(((m, a), ar), d)| (m, (a, ar, d)))
        .collect();
    assert_eq!(churn_by_month.get("2024-01").copied(), Some((2, 2, 1)));
    assert_eq!(churn_by_month.get("2024-02").copied(), Some((1, 0, 1)));
    assert_eq!(churn_by_month.get("2024-03").copied(), Some((1, 1, 1)));

    // Merge step: combines the per-wiki outputs to root-level files and
    // (best-effort) materializes dashboard JSON artifacts. We assert only
    // on the combined parquet outputs, since the artifact generators are
    // shell scripts whose presence depends on the working directory.
    merge::merge_outputs(&output_dir)?;
    for metric in [
        "labor_monthly",
        "labor_churn",
        "gdp",
        "inequality",
        "business_funnel",
    ] {
        let merged = output_dir.join(format!("{metric}.parquet"));
        assert!(merged.exists(), "merged {metric} should exist");
        let df = read_parquet(&merged)?;
        let wiki_values: Vec<String> = df
            .column("wiki")?
            .str()?
            .into_iter()
            .map(|opt| opt.unwrap_or("").to_string())
            .collect();
        assert!(
            wiki_values.iter().any(|w| w == "tinywiki"),
            "merged {metric} should contain tinywiki rows"
        );
    }

    Ok(())
}
