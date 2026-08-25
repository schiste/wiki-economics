use anyhow::{Context, Result, ensure};
use polars::prelude::*;
use serde::Serialize;
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use crate::{observability::MemorySnapshot, schema, storage};

const REPORT_SCHEMA_VERSION: u32 = 1;
const BATCH_ROWS: usize = 250_000;

#[derive(Debug, Serialize)]
pub(crate) struct WikiSchemaBenchmark {
    wiki: String,
    selected_snapshot: String,
    fragments: usize,
    rows: u64,
    warehouse_bytes: u64,
    analytical_bytes: u64,
    current_two_layer_bytes: u64,
    projected_metric_input_bytes: u64,
    projected_savings_bytes: u64,
    projected_savings_percent: f64,
    largest_temporary_fragment_bytes: u64,
    elapsed_ms: u64,
    memory: MemorySnapshot,
}

#[derive(Debug, Serialize)]
pub(crate) struct SchemaBenchmarkReport {
    schema_version: u32,
    metric_input_schema: String,
    columns: &'static [&'static str],
    generated_at_unix: u64,
    source_commit: Option<String>,
    run_id: Option<String>,
    wikis: Vec<WikiSchemaBenchmark>,
}

fn directory_bytes(path: &Path) -> Result<u64> {
    if !path.exists() {
        return Ok(0);
    }
    let mut total = 0_u64;
    let mut pending = vec![path.to_path_buf()];
    while let Some(directory) = pending.pop() {
        for entry in fs::read_dir(directory)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                pending.push(entry.path());
            } else if entry.file_type()?.is_file() {
                total = total
                    .checked_add(entry.metadata()?.len())
                    .context("schema benchmark byte count overflow")?;
            }
        }
    }
    Ok(total)
}

fn project_fragment(source: &Path, destination: &Path) -> Result<(u64, u64)> {
    let projection = schema::METRIC_INPUT_COLUMNS
        .iter()
        .map(|column| (*column).to_string())
        .collect::<Vec<_>>();
    let mut reader = storage::SequentialParquetReader::new(source, Some(projection), BATCH_ROWS)?;
    let expected_rows = u64::try_from(reader.rows())?;
    let schema_frame = reader.schema_frame().with_context(|| {
        format!(
            "{} does not satisfy the qualified metric-input schema",
            source.display()
        )
    })?;
    ensure!(
        schema_frame
            .get_column_names()
            .iter()
            .map(|name| name.as_str())
            .eq(schema::METRIC_INPUT_COLUMNS.iter().copied()),
        "{} projected columns are not in the qualified schema order",
        source.display()
    );
    let output = File::create(destination)?;
    let writer = ParquetWriter::new(output)
        .with_compression(ParquetCompression::Zstd(None))
        .with_row_group_size(Some(BATCH_ROWS))
        .set_parallel(false);
    let mut writer = writer.batched(schema_frame.schema())?;
    let mut rows = 0_u64;
    while let Some(batch) = reader.next_batch()? {
        rows = rows
            .checked_add(u64::try_from(batch.height())?)
            .context("schema benchmark row count overflow")?;
        writer.write_batch(&batch)?;
    }
    let bytes = writer.finish()?;
    drop(writer);
    File::open(destination)?.sync_all()?;
    ensure!(
        rows == expected_rows,
        "metric-input projection lost rows for {}: expected {expected_rows}, wrote {rows}",
        source.display()
    );
    let footer_rows = ParquetReader::new(File::open(destination)?).num_rows()?;
    ensure!(
        u64::try_from(footer_rows)? == rows,
        "metric-input projection footer row count mismatch"
    );
    storage::discard_path_cache(destination);
    Ok((rows, bytes))
}

fn benchmark_wiki(data_dir: &Path, scratch: &Path, wiki: &str) -> Result<WikiSchemaBenchmark> {
    let started = Instant::now();
    let selected_snapshot = storage::current_snapshot_version(data_dir, wiki)?
        .with_context(|| format!("schema benchmark requires an active snapshot for {wiki}"))?;
    let warehouse_root = storage::active_warehouse_wiki_dir(data_dir, wiki)?;
    let analytical_root = storage::active_analytical_wiki_dir(data_dir, wiki)?;
    let sources =
        storage::active_fragment_files(data_dir, wiki, storage::GenerationLayer::Warehouse)?;
    ensure!(
        !sources.is_empty(),
        "no warehouse fragments found for {wiki}"
    );
    let warehouse_bytes = directory_bytes(&warehouse_root)?;
    let analytical_bytes = directory_bytes(&analytical_root)?;
    let current_two_layer_bytes = warehouse_bytes
        .checked_add(analytical_bytes)
        .context("schema benchmark current byte count overflow")?;
    let wiki_scratch = scratch.join(wiki);
    fs::create_dir(&wiki_scratch)?;
    let result = (|| -> Result<(u64, u64, u64)> {
        let mut rows = 0_u64;
        let mut projected_bytes = 0_u64;
        let mut largest = 0_u64;
        for (index, source) in sources.iter().enumerate() {
            let destination = wiki_scratch.join(format!("fragment-{index:06}.parquet"));
            let (fragment_rows, fragment_bytes) = project_fragment(source, &destination)?;
            rows = rows
                .checked_add(fragment_rows)
                .context("schema benchmark total row count overflow")?;
            projected_bytes = projected_bytes
                .checked_add(fragment_bytes)
                .context("schema benchmark projected byte count overflow")?;
            largest = largest.max(fragment_bytes);
            fs::remove_file(&destination)?;
        }
        Ok((rows, projected_bytes, largest))
    })();
    let cleanup = fs::remove_dir(&wiki_scratch);
    let (rows, projected_metric_input_bytes, largest_temporary_fragment_bytes) = result?;
    cleanup?;
    let projected_savings_bytes = current_two_layer_bytes
        .checked_sub(projected_metric_input_bytes)
        .context("qualified metric-input layer is larger than the current two layers")?;
    let projected_savings_percent =
        projected_savings_bytes as f64 * 100.0 / current_two_layer_bytes as f64;
    Ok(WikiSchemaBenchmark {
        wiki: wiki.to_string(),
        selected_snapshot,
        fragments: sources.len(),
        rows,
        warehouse_bytes,
        analytical_bytes,
        current_two_layer_bytes,
        projected_metric_input_bytes,
        projected_savings_bytes,
        projected_savings_percent,
        largest_temporary_fragment_bytes,
        elapsed_ms: u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
        memory: MemorySnapshot::capture(),
    })
}

fn atomic_json(path: &Path, report: &SchemaBenchmarkReport) -> Result<()> {
    let parent = path
        .parent()
        .context("schema benchmark report has no parent")?;
    fs::create_dir_all(parent)?;
    let temporary = parent.join(format!(
        ".{}.{}.tmp",
        path.file_name()
            .and_then(|name| name.to_str())
            .context("schema benchmark report has no valid filename")?,
        std::process::id()
    ));
    let result = (|| -> Result<()> {
        let mut file = File::create(&temporary)?;
        serde_json::to_writer_pretty(&mut file, report)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        fs::rename(&temporary, path)?;
        File::open(parent)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

pub(crate) fn run(
    data_dir: &Path,
    scratch_root: &Path,
    report_path: &Path,
    wikis: &[String],
) -> Result<SchemaBenchmarkReport> {
    ensure!(
        !wikis.is_empty(),
        "schema benchmark requires at least one wiki"
    );
    fs::create_dir_all(scratch_root)?;
    let run_scratch = scratch_root.join(format!("metric-input-benchmark-{}", std::process::id()));
    ensure!(
        !run_scratch.exists(),
        "schema benchmark scratch already exists: {}",
        run_scratch.display()
    );
    fs::create_dir(&run_scratch)?;
    let result = (|| -> Result<Vec<WikiSchemaBenchmark>> {
        wikis
            .iter()
            .map(|wiki| benchmark_wiki(data_dir, &run_scratch, wiki))
            .collect()
    })();
    let cleanup = fs::remove_dir_all(&run_scratch);
    let reports = result?;
    cleanup?;
    let report = SchemaBenchmarkReport {
        schema_version: REPORT_SCHEMA_VERSION,
        metric_input_schema: "qualified-metric-input-v1".to_string(),
        columns: schema::METRIC_INPUT_COLUMNS,
        generated_at_unix: SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
        source_commit: std::env::var("WIKI_ECON_SOURCE_COMMIT").ok(),
        run_id: std::env::var("WIKI_ECON_RUN_ID").ok(),
        wikis: reports,
    };
    atomic_json(report_path, &report)?;
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::TestDir;

    #[test]
    fn projection_is_row_conserving_and_bounded_to_one_fragment() -> Result<()> {
        let root = TestDir::new()?;
        let source = root.path().join("warehouse.parquet");
        let destination = root.path().join("metric-input.parquet");
        let mut columns = Vec::new();
        for name in schema::WAREHOUSE_COLUMNS {
            let column = match *name {
                "event_timestamp"
                | "event_user_text"
                | "event_user_is_bot_by"
                | "page_title"
                | "year_month"
                | "user_type" => Column::new((*name).into(), ["value", "value"]),
                "event_user_is_anonymous"
                | "event_user_is_temporary"
                | "is_reverted"
                | "is_minor"
                | "page_namespace_is_content"
                | "page_is_redirect"
                | "revision_minor_edit"
                | "revision_is_identity_reverted"
                | "revision_is_identity_revert" => Column::new((*name).into(), [false, true]),
                "page_namespace" | "year" | "year_month_key" => {
                    Column::new((*name).into(), [0_i32, 1])
                }
                _ => Column::new((*name).into(), [1_i64, 2]),
            };
            columns.push(column);
        }
        let mut frame = DataFrame::new(2, columns)?;
        ParquetWriter::new(File::create(&source)?).finish(&mut frame)?;

        let (rows, bytes) = project_fragment(&source, &destination)?;
        assert_eq!(rows, 2);
        assert_eq!(bytes, fs::metadata(&destination)?.len());
        let projected = ParquetReader::new(File::open(destination)?).finish()?;
        assert!(
            projected
                .get_column_names()
                .iter()
                .map(|name| name.as_str())
                .eq(schema::METRIC_INPUT_COLUMNS.iter().copied())
        );
        Ok(())
    }

    #[test]
    fn run_rejects_empty_wiki_sets_without_leaving_scratch() {
        let data = TestDir::new().expect("data fixture");
        let scratch = TestDir::new().expect("scratch fixture");
        let report = data.path().join("report.json");
        assert!(run(data.path(), scratch.path(), &report, &[]).is_err());
        assert!(!report.exists());
    }
}
