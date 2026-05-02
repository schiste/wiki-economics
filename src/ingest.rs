use anyhow::{Context, Result};
use bzip2::read::BzDecoder;
use polars::prelude::*;
use rayon::prelude::*;
use std::collections::BTreeMap;
use std::fs::{self, File};
#[cfg(test)]
use std::io::Write;
use std::io::{BufRead, BufReader, Cursor};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, info, warn};

use crate::{schema, storage};

const INGEST_CHUNK_BYTES: usize = 32 * 1024 * 1024;

fn warehouse_select_exprs() -> Vec<Expr> {
    schema::WAREHOUSE_COLUMNS
        .iter()
        .map(|column| col(*column))
        .collect()
}

#[cfg(test)]
fn cleanup_temp_file(temp_path: &Path) {
    if let Err(err) = fs::remove_file(temp_path)
        && err.kind() != std::io::ErrorKind::NotFound
    {
        warn!(path = %temp_path.display(), error = %err, "failed to remove temporary TSV");
    }
}

fn cleanup_written_paths(paths: &[PathBuf]) {
    for path in paths {
        if let Err(err) = fs::remove_file(path)
            && err.kind() != std::io::ErrorKind::NotFound
        {
            warn!(path = %path.display(), error = %err, "failed to remove partial parquet output");
        }
    }
}

fn ingest_source_id(src: &Path) -> Result<String> {
    let source_id = src
        .file_stem()
        .and_then(|stem| stem.to_str())
        .context("source path has no valid file stem")?
        .replace(".tsv", "");
    Ok(source_id)
}

fn csv_read_options() -> CsvReadOptions {
    let ingest_cols: Arc<[PlSmallStr]> = schema::INGEST_COLUMNS.iter().map(|&s| s.into()).collect();
    CsvReadOptions::default()
        .with_has_header(false)
        .with_schema(Some(Arc::new(schema::dump_schema())))
        .with_columns(Some(ingest_cols))
        .with_rechunk(true)
        .map_parse_options(|options| {
            options
                .with_separator(b'\t')
                .with_quote_char(None)
                .with_null_values(Some(NullValues::AllColumnsSingle("".into())))
        })
}

fn parse_ingest_chunk(bytes: Vec<u8>) -> Result<DataFrame> {
    CsvReader::new(Cursor::new(bytes))
        .with_options(csv_read_options())
        .finish()
        .map_err(Into::into)
}

fn normalize_revision_chunk(df: DataFrame) -> Result<DataFrame> {
    df.lazy()
        .filter(
            col("event_entity")
                .eq(lit("revision"))
                .and(col("event_type").eq(lit("create"))),
        )
        .with_columns([
            col("event_user_id").cast(DataType::Int64),
            col("page_id").cast(DataType::Int64),
            col("page_namespace").cast(DataType::Int32),
            col("revision_id").cast(DataType::Int64),
            col("revision_parent_id").cast(DataType::Int64),
            col("revision_text_bytes").cast(DataType::Int64),
            col("revision_text_bytes_diff").cast(DataType::Int64),
            col("event_user_is_anonymous")
                .eq(lit("true"))
                .alias("event_user_is_anonymous"),
            col("event_user_is_temporary")
                .eq(lit("true"))
                .alias("event_user_is_temporary"),
            col("revision_minor_edit")
                .eq(lit("true"))
                .alias("revision_minor_edit"),
            col("revision_is_identity_reverted")
                .eq(lit("true"))
                .alias("revision_is_identity_reverted"),
        ])
        .with_columns([
            col("event_timestamp")
                .str()
                .slice(lit(0), lit(7))
                .alias("year_month"),
            col("event_timestamp")
                .str()
                .slice(lit(0), lit(4))
                .cast(DataType::Int32)
                .alias("year"),
            (col("event_timestamp")
                .str()
                .slice(lit(0), lit(4))
                .cast(DataType::Int32)
                * lit(100_i32)
                + col("event_timestamp")
                    .str()
                    .slice(lit(5), lit(2))
                    .cast(DataType::Int32))
            .alias("year_month_key"),
            when(
                col("event_user_is_bot_by")
                    .is_not_null()
                    .and(col("event_user_is_bot_by").neq(lit(""))),
            )
            .then(lit("bot"))
            .when(col("event_user_is_anonymous"))
            .then(lit("anonymous"))
            .when(col("event_user_is_temporary"))
            .then(lit("temporary"))
            .otherwise(lit("registered"))
            .alias("user_type"),
            col("revision_is_identity_reverted").alias("is_reverted"),
            col("revision_minor_edit").alias("is_minor"),
        ])
        .select(warehouse_select_exprs())
        .collect()
        .map_err(Into::into)
}

fn build_partition_index(df: &DataFrame) -> Result<BTreeMap<(i32, String), Vec<u32>>> {
    let years = df.column("year")?.i32()?;
    let year_months = df.column("year_month")?.str()?;

    let mut index: BTreeMap<(i32, String), Vec<u32>> = BTreeMap::new();
    for row_idx in 0..df.height() {
        let year = years
            .get(row_idx)
            .context("normalized chunk is missing year")?;
        let year_month = year_months
            .get(row_idx)
            .context("normalized chunk is missing year_month")?;
        index
            .entry((year, year_month.to_string()))
            .or_default()
            .push(row_idx as u32);
    }

    Ok(index)
}

fn write_parquet(df: &mut DataFrame, dest: &Path) -> Result<()> {
    dest.parent().map(fs::create_dir_all).transpose()?;

    let mut file = File::create(dest)?;
    ParquetWriter::new(&mut file)
        .with_compression(ParquetCompression::Zstd(None))
        .finish(df)?;
    Ok(())
}

fn write_partitioned_frames(
    normalized: &DataFrame,
    data_dir: &Path,
    wiki: &str,
    source_id: &str,
    chunk_idx: usize,
) -> Result<(Vec<PathBuf>, Vec<PathBuf>)> {
    let partition_index = build_partition_index(normalized)?;
    let analytical_root = storage::analytical_wiki_dir(data_dir, wiki);
    let warehouse_root = storage::warehouse_wiki_dir(data_dir, wiki);

    let mut analytical_paths = Vec::new();
    let mut warehouse_paths = Vec::new();

    for ((year, year_month), row_indices) in partition_index {
        let take_idx = UInt32Chunked::from_vec("idx".into(), row_indices);
        let partition_df = normalized.take(&take_idx)?;

        let partition_dir = storage::month_partition_dir(&warehouse_root, year, &year_month);
        let warehouse_path = partition_dir.join(format!("{source_id}.part-{chunk_idx:05}.parquet"));
        let mut warehouse_df = partition_df.clone();
        write_parquet(&mut warehouse_df, &warehouse_path)?;
        warehouse_paths.push(warehouse_path);

        let partition_dir = storage::month_partition_dir(&analytical_root, year, &year_month);
        let analytical_path =
            partition_dir.join(format!("{source_id}.part-{chunk_idx:05}.parquet"));
        let mut analytical_df = partition_df.select(schema::ANALYTICAL_COLUMNS.iter().copied())?;
        write_parquet(&mut analytical_df, &analytical_path)?;
        analytical_paths.push(analytical_path);
    }

    Ok((analytical_paths, warehouse_paths))
}

fn flush_chunk(
    chunk_bytes: &mut Vec<u8>,
    data_dir: &Path,
    wiki: &str,
    source_id: &str,
    chunk_idx: usize,
) -> Result<(usize, Vec<PathBuf>, Vec<PathBuf>)> {
    if chunk_bytes.is_empty() {
        return Ok((0, Vec::new(), Vec::new()));
    }

    let bytes = std::mem::take(chunk_bytes);
    let parsed = parse_ingest_chunk(bytes)?;
    let normalized = normalize_revision_chunk(parsed)?;
    let rows = normalized.height();
    if rows == 0 {
        return Ok((0, Vec::new(), Vec::new()));
    }

    let (analytical_paths, warehouse_paths) =
        write_partitioned_frames(&normalized, data_dir, wiki, source_id, chunk_idx)?;
    Ok((rows, analytical_paths, warehouse_paths))
}

/// Convert a single TSV.bz2 dump file into partitioned Parquet layers.
fn convert_file(src: &Path, wiki: &str, data_dir: &Path) -> Result<Vec<PathBuf>> {
    convert_file_with_chunk_limit(src, wiki, data_dir, INGEST_CHUNK_BYTES)
}

fn convert_file_with_chunk_limit(
    src: &Path,
    wiki: &str,
    data_dir: &Path,
    chunk_limit: usize,
) -> Result<Vec<PathBuf>> {
    let source_id = ingest_source_id(src)?;
    let marker = storage::marker_path(data_dir, wiki, &source_id);
    if storage::marker_manifest_is_valid(data_dir, wiki, &source_id)? {
        debug!(
            source = %src.display(),
            marker = %marker.display(),
            "skipping already ingested source"
        );
        return storage::collect_parquet_files(&storage::analytical_wiki_dir(data_dir, wiki));
    }

    let started = Instant::now();
    info!(source = %src.display(), wiki = wiki, "converting dump file");

    let file = File::open(src).context(format!("Cannot open {}", src.display()))?;
    let decoder = BzDecoder::new(BufReader::with_capacity(8 * 1024 * 1024, file));
    let mut reader = BufReader::with_capacity(8 * 1024 * 1024, decoder);

    let mut line = Vec::new();
    let mut chunk_bytes = Vec::with_capacity(INGEST_CHUNK_BYTES);
    let mut chunk_idx = 0usize;
    let mut total_rows = 0usize;
    let mut analytical_paths = Vec::new();
    let mut warehouse_paths = Vec::new();

    let conversion = (|| -> Result<()> {
        loop {
            line.clear();
            let bytes_read = reader.read_until(b'\n', &mut line)?;
            if bytes_read == 0 {
                break;
            }

            chunk_bytes.extend_from_slice(&line);
            if chunk_bytes.len() >= chunk_limit {
                let (rows, analytical, warehouse) =
                    flush_chunk(&mut chunk_bytes, data_dir, wiki, &source_id, chunk_idx)?;
                total_rows += rows;
                analytical_paths.extend(analytical);
                warehouse_paths.extend(warehouse);
                chunk_idx += 1;
            }
        }

        if !chunk_bytes.is_empty() {
            let (rows, analytical, warehouse) =
                flush_chunk(&mut chunk_bytes, data_dir, wiki, &source_id, chunk_idx)?;
            total_rows += rows;
            analytical_paths.extend(analytical);
            warehouse_paths.extend(warehouse);
        }

        let manifest = storage::MarkerManifest {
            source: Some(src.display().to_string()),
            rows: total_rows,
            analytical_paths: analytical_paths.clone(),
            warehouse_paths: warehouse_paths.clone(),
        };
        storage::write_marker_manifest(data_dir, wiki, &source_id, &manifest)?;
        Ok(())
    })();

    if let Err(err) = conversion {
        cleanup_written_paths(&analytical_paths);
        cleanup_written_paths(&warehouse_paths);
        let _ = fs::remove_file(&marker);
        return Err(err);
    }

    let analytical_bytes: u64 = analytical_paths
        .iter()
        .filter_map(|path| fs::metadata(path).ok().map(|metadata| metadata.len()))
        .sum();
    let warehouse_bytes: u64 = warehouse_paths
        .iter()
        .filter_map(|path| fs::metadata(path).ok().map(|metadata| metadata.len()))
        .sum();

    info!(
        source = %src.display(),
        wiki = wiki,
        rows = total_rows,
        analytical_parts = analytical_paths.len(),
        analytical_mb = analytical_bytes as f64 / 1_048_576.0,
        warehouse_parts = warehouse_paths.len(),
        warehouse_mb = warehouse_bytes as f64 / 1_048_576.0,
        elapsed_ms = started.elapsed().as_secs_f64() * 1_000.0,
        "converted dump file"
    );

    Ok(analytical_paths)
}

/// Ingest all raw dump files for a wiki into partitioned Parquet.
pub fn ingest_wiki(wiki: &str, data_dir: &Path) -> Result<Vec<PathBuf>> {
    let raw_dir = data_dir.join("raw").join(wiki);
    let analytical_dir = storage::analytical_wiki_dir(data_dir, wiki);
    let warehouse_dir = storage::warehouse_wiki_dir(data_dir, wiki);
    fs::create_dir_all(&analytical_dir)?;
    fs::create_dir_all(&warehouse_dir)?;

    if !raw_dir.exists() {
        anyhow::bail!("No raw data for {wiki}. Run `fetch` first.");
    }

    let mut src_files: Vec<PathBuf> = fs::read_dir(&raw_dir)?
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| path.extension().is_some_and(|ext| ext == "bz2"))
        .collect();
    src_files.sort();

    info!(
        wiki = wiki,
        files = src_files.len(),
        "ingesting raw dump files"
    );

    src_files
        .par_iter()
        .try_for_each(|src| convert_file(src, wiki, data_dir).map(|_| ()))?;

    let analytical_paths = storage::collect_parquet_files(&analytical_dir)?;
    info!(
        wiki = wiki,
        files = analytical_paths.len(),
        analytical_dir = %analytical_dir.display(),
        warehouse_dir = %warehouse_dir.display(),
        "finished ingest"
    );
    Ok(analytical_paths)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{TestDir, init_test_tracing};
    use bzip2::Compression;
    use bzip2::write::BzEncoder;

    fn sample_row(
        timestamp: &str,
        user_id: &str,
        revision_id: &str,
        entity: &str,
        event_type: &str,
    ) -> String {
        let mut row = vec![String::new(); schema::COLUMNS.len()];
        for (name, value) in [
            ("wiki_db", "testwiki"),
            ("event_entity", entity),
            ("event_type", event_type),
            ("event_timestamp", timestamp),
            ("event_user_id", user_id),
            ("event_user_text", "ExampleUser"),
            ("event_user_is_anonymous", "false"),
            ("event_user_is_temporary", "false"),
            ("event_user_registration_timestamp", "2023-01-01 00:00:00.0"),
            ("event_user_first_edit_timestamp", timestamp),
            ("page_id", "10"),
            ("page_title", "Example"),
            ("page_namespace", "0"),
            ("page_namespace_is_content", "true"),
            ("page_is_redirect", "false"),
            ("revision_id", revision_id),
            ("revision_parent_id", "99"),
            ("revision_minor_edit", "false"),
            ("revision_text_bytes", "1200"),
            ("revision_text_bytes_diff", "25"),
            ("revision_is_identity_reverted", "false"),
            ("revision_is_identity_revert", "false"),
        ] {
            let idx = schema::COLUMNS
                .iter()
                .position(|column| column == &name)
                .expect("column should exist");
            row[idx] = value.to_string();
        }
        row.join("\t")
    }

    fn write_bz2_dump(path: &Path, rows: &[String]) -> Result<()> {
        let file = File::create(path)?;
        let mut encoder = BzEncoder::new(file, Compression::best());
        for row in rows {
            encoder.write_all(row.as_bytes())?;
            encoder.write_all(b"\n")?;
        }
        encoder.finish()?;
        Ok(())
    }

    #[test]
    fn convert_file_skips_existing_output() -> Result<()> {
        init_test_tracing();
        let temp_dir = TestDir::new()?;
        let wiki = "skipwiki";
        let src = temp_dir.path().join("source.tsv.bz2");
        let rows = [sample_row(
            "2024-01-01 00:00:00.0",
            "42",
            "100",
            "revision",
            "create",
        )];
        write_bz2_dump(&src, &rows)?;
        let outputs = convert_file(&src, wiki, temp_dir.path())?;
        let rerun = convert_file(&src, wiki, temp_dir.path())?;

        assert_eq!(rerun, outputs);
        Ok(())
    }

    #[test]
    fn convert_file_rebuilds_missing_warehouse_outputs_even_with_marker() -> Result<()> {
        init_test_tracing();
        let temp_dir = TestDir::new()?;
        let wiki = "repairwiki";
        let src = temp_dir.path().join("source.tsv.bz2");
        let rows = [sample_row(
            "2024-01-01 00:00:00.0",
            "42",
            "100",
            "revision",
            "create",
        )];
        write_bz2_dump(&src, &rows)?;
        convert_file(&src, wiki, temp_dir.path())?;

        let warehouse_files =
            storage::collect_parquet_files(&storage::warehouse_wiki_dir(temp_dir.path(), wiki))?;
        assert_eq!(warehouse_files.len(), 1);
        fs::remove_file(&warehouse_files[0])?;

        let rerun = convert_file(&src, wiki, temp_dir.path())?;
        let repaired =
            storage::collect_parquet_files(&storage::warehouse_wiki_dir(temp_dir.path(), wiki))?;

        assert_eq!(rerun.len(), 1);
        assert_eq!(repaired.len(), 1);
        Ok(())
    }

    #[test]
    fn cleanup_written_paths_ignores_missing_and_logs_directory_errors() -> Result<()> {
        init_test_tracing();
        let temp_dir = TestDir::new()?;
        let dir_path = temp_dir.path().join("not-a-file");
        fs::create_dir_all(&dir_path)?;

        cleanup_written_paths(&[dir_path.clone(), temp_dir.path().join("missing.parquet")]);

        assert!(dir_path.exists());
        Ok(())
    }

    #[test]
    fn write_parquet_creates_parent_directories() -> Result<()> {
        init_test_tracing();
        let temp_dir = TestDir::new()?;
        let dest = temp_dir
            .path()
            .join("nested")
            .join("path")
            .join("frame.parquet");
        let mut df = DataFrame::new_infer_height(vec![Column::new("value".into(), vec![1_i32])])?;

        write_parquet(&mut df, &dest)?;

        assert!(dest.exists());
        Ok(())
    }

    #[test]
    fn flush_chunk_returns_zero_for_empty_and_filtered_chunks() -> Result<()> {
        init_test_tracing();
        let temp_dir = TestDir::new()?;

        let empty = flush_chunk(&mut Vec::new(), temp_dir.path(), "testwiki", "source", 0)?;
        assert_eq!(empty.0, 0);
        assert!(empty.1.is_empty());
        assert!(empty.2.is_empty());

        let filtered_row = sample_row("2024-01-01 00:00:00.0", "42", "100", "page", "create");
        let mut filtered_bytes = filtered_row.into_bytes();
        let root = temp_dir.path();
        let filtered = flush_chunk(&mut filtered_bytes, root, "testwiki", "source", 1)?;
        assert_eq!(filtered.0, 0);
        assert!(filtered.1.is_empty());
        assert!(filtered.2.is_empty());
        Ok(())
    }

    #[test]
    fn convert_file_writes_partitioned_outputs() -> Result<()> {
        init_test_tracing();
        let temp_dir = TestDir::new()?;
        let wiki = "testwiki";
        let src = temp_dir.path().join("source.tsv.bz2");
        let rows = [
            sample_row("2024-01-01 00:00:00.0", "42", "100", "revision", "create"),
            sample_row("2024-02-01 00:00:00.0", "42", "101", "revision", "create"),
            sample_row("2024-02-01 00:00:00.0", "99", "102", "page", "create"),
        ];
        write_bz2_dump(&src, &rows)?;

        let outputs = convert_file(&src, wiki, temp_dir.path())?;

        assert_eq!(outputs.len(), 2);
        assert!(storage::marker_path(temp_dir.path(), wiki, "source").exists());

        let analytical_files =
            storage::collect_parquet_files(&storage::analytical_wiki_dir(temp_dir.path(), wiki))?;
        let warehouse_files =
            storage::collect_parquet_files(&storage::warehouse_wiki_dir(temp_dir.path(), wiki))?;
        assert_eq!(analytical_files.len(), 2);
        assert_eq!(warehouse_files.len(), 2);

        let analytical_path = analytical_files[0].to_string_lossy().to_string();
        let df = LazyFrame::scan_parquet(analytical_path.as_str().into(), Default::default())?
            .collect()?;
        assert_eq!(df.width(), schema::ANALYTICAL_COLUMNS.len());
        assert_eq!(df.column("revision_id")?.i64()?.get(0), Some(100));
        Ok(())
    }

    #[test]
    fn convert_file_flushes_multiple_chunks_when_limit_is_small() -> Result<()> {
        init_test_tracing();
        let temp_dir = TestDir::new()?;
        let wiki = "chunkwiki";
        let src = temp_dir.path().join("source.tsv.bz2");
        let rows = [
            sample_row("2024-01-01 00:00:00.0", "42", "100", "revision", "create"),
            sample_row("2024-01-02 00:00:00.0", "43", "101", "revision", "create"),
        ];
        write_bz2_dump(&src, &rows)?;

        let outputs = convert_file_with_chunk_limit(&src, wiki, temp_dir.path(), 128)?;

        assert_eq!(outputs.len(), 2);
        assert!(storage::marker_path(temp_dir.path(), wiki, "source").exists());
        Ok(())
    }

    #[test]
    fn convert_file_cleans_up_temp_file_on_failure() -> Result<()> {
        init_test_tracing();
        let temp_dir = TestDir::new()?;
        let wiki = "testwiki";
        let src = temp_dir.path().join("source.tsv.bz2");
        let rows = [sample_row("bad", "42", "100", "revision", "create")];
        write_bz2_dump(&src, &rows)?;

        let err = convert_file(&src, wiki, temp_dir.path()).expect_err("invalid row should fail");
        assert!(!err.to_string().is_empty());
        assert!(
            storage::collect_parquet_files(&storage::analytical_wiki_dir(temp_dir.path(), wiki))?
                .is_empty()
        );
        assert!(
            storage::collect_parquet_files(&storage::warehouse_wiki_dir(temp_dir.path(), wiki))?
                .is_empty()
        );
        Ok(())
    }

    #[test]
    fn ingest_wiki_converts_available_bz2_files() -> Result<()> {
        init_test_tracing();
        let temp_dir = TestDir::new()?;
        let raw_dir = temp_dir.path().join("raw").join("testwiki");
        fs::create_dir_all(&raw_dir)?;
        let part1_rows = [sample_row(
            "2024-01-01 00:00:00.0",
            "42",
            "100",
            "revision",
            "create",
        )];
        let part2_rows = [sample_row(
            "2024-02-01 00:00:00.0",
            "43",
            "101",
            "revision",
            "create",
        )];
        write_bz2_dump(&raw_dir.join("part1.tsv.bz2"), &part1_rows)?;
        write_bz2_dump(&raw_dir.join("part2.tsv.bz2"), &part2_rows)?;

        let outputs = ingest_wiki("testwiki", temp_dir.path())?;
        assert_eq!(outputs.len(), 2);
        Ok(())
    }

    #[test]
    fn ingest_wiki_errors_when_raw_dir_is_missing() -> Result<()> {
        init_test_tracing();
        let temp_dir = TestDir::new()?;

        let err = ingest_wiki("missingwiki", temp_dir.path()).expect_err("missing raw dir");
        assert!(err.to_string().contains("Run `fetch` first"));
        Ok(())
    }

    #[test]
    fn cleanup_temp_file_logs_and_ignores_non_file_paths() -> Result<()> {
        init_test_tracing();
        let temp_dir = TestDir::new()?;
        let dir_path = temp_dir.path().join("not-a-file");
        fs::create_dir_all(&dir_path)?;

        cleanup_temp_file(&dir_path);
        cleanup_temp_file(&temp_dir.path().join("missing.tsv"));

        assert!(dir_path.exists());
        Ok(())
    }
}
