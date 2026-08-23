use anyhow::{Context, Result, ensure};
use bzip2::read::BzDecoder;
use polars::prelude::*;
use rayon::prelude::*;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
#[cfg(test)]
use std::io::Write;
use std::io::{BufRead, BufReader, Cursor, Read};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, info, warn};

use crate::fingerprint::{self, StageSpec, TrackedPath};
use crate::{fetch, schema, storage};

const INGEST_CHUNK_BYTES: usize = 32 * 1024 * 1024;
pub(crate) const INGEST_ALGORITHM_VERSION: &str =
    "history-tsv-to-generation-parquet-v3-strict-markers";

#[derive(Clone, Debug)]
struct IngestRoots {
    analytical: PathBuf,
    warehouse: PathBuf,
    snapshot_version: Option<String>,
}

struct SourceIdentityReader {
    inner: File,
    hasher: Sha256,
    bytes: u64,
}

impl SourceIdentityReader {
    fn new(inner: File) -> Self {
        storage::prepare_sequential_read(&inner);
        Self {
            inner,
            hasher: Sha256::new(),
            bytes: 0,
        }
    }

    fn finish(self) -> (u64, String) {
        (self.bytes, hex::encode(self.hasher.finalize()))
    }
}

impl Read for SourceIdentityReader {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        let read = self.inner.read(buffer)?;
        self.hasher.update(&buffer[..read]);
        storage::discard_file_cache(&self.inner, self.bytes, read as u64);
        let read = u64::try_from(read).map_err(std::io::Error::other)?;
        self.bytes = self
            .bytes
            .checked_add(read)
            .ok_or_else(|| std::io::Error::other("source size overflow"))?;
        usize::try_from(read).map_err(std::io::Error::other)
    }
}

impl IngestRoots {
    fn legacy(data_dir: &Path, wiki: &str) -> Self {
        Self {
            analytical: storage::analytical_wiki_dir(data_dir, wiki),
            warehouse: storage::warehouse_wiki_dir(data_dir, wiki),
            snapshot_version: None,
        }
    }

    fn snapshot(data_dir: &Path, wiki: &str, snapshot_version: &str) -> Result<Self> {
        Ok(Self {
            analytical: storage::snapshot_analytical_wiki_dir(data_dir, wiki, snapshot_version)?,
            warehouse: storage::snapshot_warehouse_wiki_dir(data_dir, wiki, snapshot_version)?,
            snapshot_version: Some(snapshot_version.to_string()),
        })
    }
}

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

pub(crate) fn ingest_source_id(src: &Path) -> Result<String> {
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
    file.sync_data()?;
    storage::discard_file_cache(&file, 0, file.metadata()?.len());
    Ok(())
}

fn write_partitioned_frames(
    normalized: &DataFrame,
    roots: &IngestRoots,
    source_id: &str,
    chunk_idx: usize,
) -> Result<(Vec<PathBuf>, Vec<PathBuf>)> {
    let partition_index = build_partition_index(normalized)?;

    let mut analytical_paths = Vec::new();
    let mut warehouse_paths = Vec::new();

    for ((year, year_month), row_indices) in partition_index {
        let take_idx = UInt32Chunked::from_vec("idx".into(), row_indices);
        let partition_df = normalized.take(&take_idx)?;

        let partition_dir = storage::month_partition_dir(&roots.warehouse, year, &year_month);
        let warehouse_path = partition_dir.join(format!("{source_id}.part-{chunk_idx:05}.parquet"));
        let mut warehouse_df = partition_df.clone();
        write_parquet(&mut warehouse_df, &warehouse_path)?;
        warehouse_paths.push(warehouse_path);

        let partition_dir = storage::month_partition_dir(&roots.analytical, year, &year_month);
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
    roots: &IngestRoots,
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
        write_partitioned_frames(&normalized, roots, source_id, chunk_idx)?;
    Ok((rows, analytical_paths, warehouse_paths))
}

/// Convert a single TSV.bz2 dump file into partitioned Parquet layers.
#[cfg(test)]
fn convert_file(src: &Path, wiki: &str, data_dir: &Path) -> Result<Vec<PathBuf>> {
    convert_file_with_chunk_limit(
        src,
        wiki,
        data_dir,
        &IngestRoots::legacy(data_dir, wiki),
        INGEST_CHUNK_BYTES,
    )
}

fn convert_file_with_chunk_limit(
    src: &Path,
    wiki: &str,
    data_dir: &Path,
    roots: &IngestRoots,
    chunk_limit: usize,
) -> Result<Vec<PathBuf>> {
    let source_id = ingest_source_id(src)?;
    let marker = storage::marker_path_in(&roots.analytical, &source_id);
    if storage::marker_manifest_is_valid_in(data_dir, &roots.analytical, &source_id)? {
        debug!(
            source = %src.display(),
            marker = %marker.display(),
            "skipping already ingested source"
        );
        return storage::collect_parquet_files(&roots.analytical);
    }

    let started = Instant::now();
    info!(source = %src.display(), wiki = wiki, "converting dump file");

    let file = File::open(src).context(format!("Cannot open {}", src.display()))?;
    let source_reader = SourceIdentityReader::new(file);
    let decoder = BzDecoder::new(BufReader::with_capacity(8 * 1024 * 1024, source_reader));
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
                    flush_chunk(&mut chunk_bytes, roots, &source_id, chunk_idx)?;
                total_rows += rows;
                analytical_paths.extend(analytical);
                warehouse_paths.extend(warehouse);
                chunk_idx += 1;
            }
        }

        if !chunk_bytes.is_empty() {
            let (rows, analytical, warehouse) =
                flush_chunk(&mut chunk_bytes, roots, &source_id, chunk_idx)?;
            total_rows += rows;
            analytical_paths.extend(analytical);
            warehouse_paths.extend(warehouse);
        }

        Ok(())
    })();

    if let Err(err) = conversion {
        cleanup_written_paths(&analytical_paths);
        cleanup_written_paths(&warehouse_paths);
        let _ = fs::remove_file(&marker);
        return Err(err);
    }

    let receipt = (|| -> Result<()> {
        let decoder = reader.into_inner();
        let buffered_source = decoder.into_inner();
        let mut source_reader = buffered_source.into_inner();
        std::io::copy(&mut source_reader, &mut std::io::sink())?;
        let (source_size_bytes, source_sha256) = source_reader.finish();
        let manifest = storage::MarkerManifest {
            snapshot_version: roots.snapshot_version.clone(),
            source: src.to_path_buf(),
            source_size_bytes,
            source_sha256,
            rows: total_rows,
            allow_empty: false,
            analytical_paths: analytical_paths.clone(),
            warehouse_paths: warehouse_paths.clone(),
        };
        storage::write_marker_manifest_in(data_dir, &roots.analytical, &source_id, &manifest)?;
        Ok(())
    })();
    if let Err(err) = receipt {
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

pub(crate) fn snapshot_version_from_filename<'a>(filename: &'a str, wiki: &str) -> Option<&'a str> {
    let snapshot_version = filename.get(..7)?;
    let separator = filename.get(7..)?.strip_prefix('.')?;
    separator.strip_prefix(wiki)?.strip_prefix('.')?;
    storage::validate_snapshot_version(snapshot_version)
        .ok()
        .map(|_| snapshot_version)
}

fn select_ingest_sources(
    wiki: &str,
    src_files: Vec<PathBuf>,
    requested_snapshot: Option<&str>,
) -> Result<(Option<String>, Vec<PathBuf>)> {
    let discovered: BTreeSet<String> = src_files
        .iter()
        .filter_map(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .and_then(|name| snapshot_version_from_filename(name, wiki))
                .map(str::to_string)
        })
        .collect();
    let snapshot_version = match requested_snapshot {
        Some(version) => {
            storage::validate_snapshot_version(version)?;
            Some(version.to_string())
        }
        None if discovered.is_empty() => None,
        None => {
            ensure!(
                discovered.len() == 1,
                "raw dump directory for {wiki} contains multiple snapshots: {discovered:?}"
            );
            discovered.into_iter().next()
        }
    };

    let Some(snapshot_version) = snapshot_version else {
        return Ok((None, src_files));
    };
    let expected = fetch::build_file_list(wiki, &snapshot_version)?;
    let by_name: BTreeMap<String, PathBuf> = src_files
        .into_iter()
        .filter_map(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .map(|name| (name.to_string(), path.clone()))
        })
        .collect();
    let missing: Vec<&str> = expected
        .iter()
        .map(String::as_str)
        .filter(|filename| !by_name.contains_key(*filename))
        .collect();
    ensure!(
        missing.is_empty(),
        "snapshot {snapshot_version} for {wiki} is incomplete; missing {} source file(s): {}",
        missing.len(),
        missing.join(", ")
    );
    let selected = expected
        .into_iter()
        .filter_map(|filename| by_name.get(&filename).cloned())
        .collect();
    Ok((Some(snapshot_version), selected))
}

fn ingest_wiki_for_snapshot(
    wiki: &str,
    data_dir: &Path,
    requested_snapshot: Option<&str>,
) -> Result<Vec<PathBuf>> {
    let raw_dir = data_dir.join("raw").join(wiki);
    if let Some(version) = requested_snapshot {
        storage::validate_snapshot_version(version)?;
        let roots = IngestRoots::snapshot(data_dir, wiki, version)?;
        let outputs = ingest_stage_outputs(data_dir, &roots)?;
        let receipt_path = fingerprint::data_stage_receipt_path(data_dir, wiki, version, "ingest");
        let spec = StageSpec {
            stage: "ingest",
            scope: wiki,
            selected_snapshot: Some(version),
            algorithm_version: INGEST_ALGORITHM_VERSION,
        };
        if !outputs.is_empty() && fingerprint::outputs_reusable(&receipt_path, spec, &outputs)? {
            storage::publish_current_snapshot(data_dir, wiki, version)?;
            crate::observability::record_stage_reused("ingest", Some(wiki));
            info!(
                wiki,
                snapshot_version = version,
                receipt = %receipt_path.display(),
                "reusing deterministic ingest stage"
            );
            return storage::collect_parquet_files(&roots.analytical);
        }
    }
    let mut src_files: Vec<PathBuf> = if raw_dir.exists() {
        fs::read_dir(&raw_dir)?
            .filter_map(|entry| entry.ok().map(|entry| entry.path()))
            .filter(|path| path.extension().is_some_and(|ext| ext == "bz2"))
            .collect()
    } else {
        Vec::new()
    };
    src_files.sort();

    let (snapshot_version, src_files) = match requested_snapshot {
        Some(version) => {
            storage::validate_snapshot_version(version)?;
            let roots = IngestRoots::snapshot(data_dir, wiki, version)?;
            let expected = fetch::build_file_list(wiki, version)?;
            let by_name: BTreeMap<String, PathBuf> = src_files
                .into_iter()
                .filter_map(|path| {
                    path.file_name()
                        .and_then(|name| name.to_str())
                        .map(|name| (name.to_string(), path.clone()))
                })
                .collect();
            let mut selected = Vec::new();
            let mut missing = Vec::new();
            for filename in expected {
                if let Some(path) = by_name.get(&filename) {
                    selected.push(path.clone());
                    continue;
                }
                let source_id = ingest_source_id(Path::new(&filename))?;
                if !storage::marker_manifest_is_valid_in(data_dir, &roots.analytical, &source_id)? {
                    missing.push(filename);
                }
            }
            ensure!(
                missing.is_empty(),
                "snapshot {version} for {wiki} is incomplete; missing {} source file(s): {}",
                missing.len(),
                missing.join(", ")
            );
            (Some(version.to_string()), selected)
        }
        None => {
            ensure!(
                raw_dir.exists(),
                "No raw data for {wiki}. Run `fetch` first."
            );
            ensure!(
                !src_files.is_empty(),
                "No raw .bz2 files for {wiki}. Run `fetch` first."
            );
            select_ingest_sources(wiki, src_files, None)?
        }
    };
    let roots = match snapshot_version.as_deref() {
        Some(version) => IngestRoots::snapshot(data_dir, wiki, version)?,
        None => IngestRoots::legacy(data_dir, wiki),
    };
    fs::create_dir_all(&roots.analytical)?;
    fs::create_dir_all(&roots.warehouse)?;

    info!(
        wiki = wiki,
        snapshot_version = snapshot_version.as_deref().unwrap_or("legacy"),
        files = src_files.len(),
        "ingesting raw dump files"
    );

    src_files.par_iter().try_for_each(|src| {
        convert_file_with_chunk_limit(src, wiki, data_dir, &roots, INGEST_CHUNK_BYTES).map(|_| ())
    })?;

    let sources_to_validate = match snapshot_version.as_deref() {
        Some(version) => fetch::build_file_list(wiki, version)?
            .into_iter()
            .map(PathBuf::from)
            .collect(),
        None => src_files.clone(),
    };
    for source in &sources_to_validate {
        let source_id = ingest_source_id(source)?;
        ensure!(
            storage::marker_manifest_is_valid_in(data_dir, &roots.analytical, &source_id)?,
            "snapshot source {source_id} did not produce a valid ingest marker"
        );
    }

    if let Some(snapshot_version) = snapshot_version.as_deref() {
        let inputs = ingest_stage_inputs(data_dir, &roots, &src_files)?;
        let outputs = ingest_stage_outputs(data_dir, &roots)?;
        fingerprint::record(
            &fingerprint::data_stage_receipt_path(data_dir, wiki, snapshot_version, "ingest"),
            StageSpec {
                stage: "ingest",
                scope: wiki,
                selected_snapshot: Some(snapshot_version),
                algorithm_version: INGEST_ALGORITHM_VERSION,
            },
            &inputs,
            &outputs,
        )?;
        storage::publish_current_snapshot(data_dir, wiki, snapshot_version)?;
    }

    let analytical_paths = storage::collect_parquet_files(&roots.analytical)?;
    info!(
        wiki = wiki,
        snapshot_version = snapshot_version.as_deref().unwrap_or("legacy"),
        files = analytical_paths.len(),
        analytical_dir = %roots.analytical.display(),
        warehouse_dir = %roots.warehouse.display(),
        "finished ingest"
    );
    Ok(analytical_paths)
}

fn ingest_stage_outputs(_data_dir: &Path, roots: &IngestRoots) -> Result<Vec<TrackedPath>> {
    let mut outputs = Vec::new();
    for (prefix, root) in [
        ("analytical", &roots.analytical),
        ("warehouse", &roots.warehouse),
    ] {
        for path in storage::collect_parquet_files(root)? {
            let relative = path.strip_prefix(root)?;
            outputs.push(TrackedPath::new(
                format!("{prefix}/{}", relative.to_string_lossy()),
                path,
            ));
        }
    }
    if outputs.is_empty() {
        outputs = fingerprint::collect_tracked_files(
            &roots.analytical.join("_markers"),
            "ingest-marker",
        )?;
    }
    outputs.sort_by(|left, right| left.identity.cmp(&right.identity));
    Ok(outputs)
}

fn ingest_stage_inputs(
    _data_dir: &Path,
    roots: &IngestRoots,
    raw_sources: &[PathBuf],
) -> Result<Vec<TrackedPath>> {
    let mut inputs = Vec::new();
    for source in raw_sources {
        let source_id = ingest_source_id(source)?;
        inputs.push(TrackedPath::new(format!("raw/{source_id}"), source));
    }
    if inputs.is_empty() {
        let marker_root = roots.analytical.join("_markers");
        inputs = fingerprint::collect_tracked_files(&marker_root, "ingest-marker")?;
    }
    Ok(inputs)
}

/// Ingest all raw dump files for a wiki into partitioned Parquet. Standard
/// Wikimedia snapshot filenames are isolated into an immutable generation;
/// legacy/test filenames retain the historical direct-directory layout.
pub fn ingest_wiki(wiki: &str, data_dir: &Path) -> Result<Vec<PathBuf>> {
    ingest_wiki_for_snapshot(wiki, data_dir, None)
}

pub fn ingest_wiki_snapshot(
    wiki: &str,
    snapshot_version: &str,
    data_dir: &Path,
) -> Result<Vec<PathBuf>> {
    ingest_wiki_for_snapshot(wiki, data_dir, Some(snapshot_version))
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
        let roots = IngestRoots::legacy(temp_dir.path(), "testwiki");

        let empty = flush_chunk(&mut Vec::new(), &roots, "source", 0)?;
        assert_eq!(empty.0, 0);
        assert!(empty.1.is_empty());
        assert!(empty.2.is_empty());

        let filtered_row = sample_row("2024-01-01 00:00:00.0", "42", "100", "page", "create");
        let mut filtered_bytes = filtered_row.into_bytes();
        let filtered = flush_chunk(&mut filtered_bytes, &roots, "source", 1)?;
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

        let roots = IngestRoots::legacy(temp_dir.path(), wiki);
        let outputs = convert_file_with_chunk_limit(&src, wiki, temp_dir.path(), &roots, 128)?;

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
    fn convert_file_cleans_outputs_when_marker_commit_is_interrupted() -> Result<()> {
        let temp_dir = TestDir::new()?;
        let wiki = "testwiki";
        let src = temp_dir.path().join("source.tsv.bz2");
        write_bz2_dump(
            &src,
            &[sample_row(
                "2024-01-01 00:00:00.0",
                "42",
                "100",
                "revision",
                "create",
            )],
        )
        .expect("valid interrupted-marker fixture should compress");
        let marker = storage::marker_path(temp_dir.path(), wiki, "source");
        let staging = marker
            .parent()
            .context("marker parent")?
            .join(format!(".source.done.{}.tmp", std::process::id()));
        fs::create_dir_all(&staging)?;

        assert!(convert_file(&src, wiki, temp_dir.path()).is_err());
        assert!(
            storage::collect_parquet_files(&storage::analytical_wiki_dir(temp_dir.path(), wiki))?
                .is_empty()
        );
        assert!(
            storage::collect_parquet_files(&storage::warehouse_wiki_dir(temp_dir.path(), wiki))?
                .is_empty()
        );
        assert!(!marker.exists());
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
    fn source_selection_infers_one_snapshot_and_ignores_stale_files_when_requested() -> Result<()> {
        let july = PathBuf::from("2026-07.testwiki.all-time.tsv.bz2");
        let august = PathBuf::from("2026-08.testwiki.all-time.tsv.bz2");
        let unversioned = PathBuf::from("notes.tsv.bz2");

        let (version, selected) = select_ingest_sources(
            "testwiki",
            vec![july.clone(), august.clone(), unversioned],
            Some("2026-08"),
        )
        .expect("requested snapshot should select its exact source");
        assert_eq!(version.as_deref(), Some("2026-08"));
        assert_eq!(selected, vec![august]);

        let (inferred, inferred_sources) =
            select_ingest_sources("testwiki", vec![selected[0].clone()], None)?;
        assert_eq!(inferred.as_deref(), Some("2026-08"));
        assert_eq!(inferred_sources, selected);

        let error = select_ingest_sources("testwiki", vec![july, selected[0].clone()], None)
            .expect_err("mixed snapshots require an explicit selection");
        assert!(error.to_string().contains("multiple snapshots"));
        assert_eq!(
            snapshot_version_from_filename("notes.tsv.bz2", "testwiki"),
            None
        );
        assert_eq!(
            snapshot_version_from_filename("2026-08.otherwiki.all-time.tsv.bz2", "testwiki"),
            None
        );
        Ok(())
    }

    #[test]
    fn source_selection_rejects_invalid_or_incomplete_requested_snapshot() {
        assert!(
            select_ingest_sources("testwiki", Vec::new(), Some("invalid"))
                .unwrap_err()
                .to_string()
                .contains("invalid snapshot")
        );
        let error = select_ingest_sources(
            "frwiki",
            vec![PathBuf::from("2026-08.frwiki.2001.tsv.bz2")],
            Some("2026-08"),
        )
        .expect_err("yearly snapshot must contain every expected source");
        assert!(error.to_string().contains("missing 25 source file"));
    }

    #[test]
    fn source_selection_preserves_legacy_files() -> Result<()> {
        let files = vec![
            PathBuf::from("part1.tsv.bz2"),
            PathBuf::from("part2.tsv.bz2"),
        ];
        let (version, selected) = select_ingest_sources("testwiki", files.clone(), None)?;
        assert_eq!(version, None);
        assert_eq!(selected, files);
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
    fn ingest_wiki_errors_when_raw_dir_is_empty() -> Result<()> {
        init_test_tracing();
        let temp_dir = TestDir::new()?;
        fs::create_dir_all(temp_dir.path().join("raw").join("emptywiki"))?;

        let err = ingest_wiki("emptywiki", temp_dir.path()).expect_err("empty raw dir");
        assert!(err.to_string().contains("No raw .bz2 files"));
        Ok(())
    }

    #[test]
    fn explicit_snapshot_reuses_complete_markers_without_raw_files() -> Result<()> {
        init_test_tracing();
        let temp_dir = TestDir::new()?;
        let wiki = "testwiki";
        let version = "2026-08";
        let analytical = storage::snapshot_analytical_wiki_dir(temp_dir.path(), wiki, version)?;
        let warehouse = storage::snapshot_warehouse_wiki_dir(temp_dir.path(), wiki, version)?;
        fs::create_dir_all(&warehouse)?;
        let filename = fetch::build_file_list(wiki, version)?
            .pop()
            .expect("all-time source");
        storage::write_test_marker_in(
            temp_dir.path(),
            &analytical,
            &ingest_source_id(Path::new(&filename))?,
        )
        .expect("zero-row marker should be written");

        let paths = ingest_wiki_snapshot(wiki, version, temp_dir.path())?;
        let receipt =
            fingerprint::data_stage_receipt_path(temp_dir.path(), wiki, version, "ingest");
        let receipt_before = fs::read(&receipt)?;
        let reused = ingest_wiki_snapshot(wiki, version, temp_dir.path())?;

        assert_eq!(paths.len(), 1);
        assert_eq!(reused, paths);
        assert_eq!(fs::read(receipt)?, receipt_before);
        assert_eq!(
            storage::current_snapshot_version(temp_dir.path(), wiki)?.as_deref(),
            Some(version)
        );
        Ok(())
    }

    #[test]
    fn explicit_snapshot_rejects_sources_missing_from_raw_and_markers() -> Result<()> {
        let temp_dir = TestDir::new()?;
        let error = ingest_wiki_snapshot("testwiki", "2026-08", temp_dir.path())
            .expect_err("an uncovered source must require fetching");
        assert!(error.to_string().contains("missing 1 source file"));
        Ok(())
    }

    #[test]
    fn ingest_stage_fails_when_marker_inventory_cannot_be_read() -> Result<()> {
        let temp_dir = TestDir::new()?;
        let roots = IngestRoots::snapshot(temp_dir.path(), "testwiki", "2026-08")?;
        fs::create_dir_all(&roots.analytical)?;
        fs::write(roots.analytical.join("_markers"), "not-a-directory")?;
        assert!(ingest_stage_outputs(temp_dir.path(), &roots).is_err());
        Ok(())
    }

    #[test]
    fn versioned_ingest_propagates_stage_receipt_publication_failure() -> Result<()> {
        let temp_dir = TestDir::new()?;
        let wiki = "testwiki";
        let version = "2026-08";
        let raw = temp_dir.path().join("raw").join(wiki);
        fs::create_dir_all(&raw)?;
        let filename = fetch::build_file_list(wiki, version)?
            .pop()
            .expect("all-time source");
        write_bz2_dump(
            &raw.join(filename),
            &[sample_row(
                "2024-01-01 00:00:00.0",
                "42",
                "100",
                "revision",
                "create",
            )],
        )
        .expect("versioned raw fixture should be written");
        let receipt =
            fingerprint::data_stage_receipt_path(temp_dir.path(), wiki, version, "ingest");
        fs::create_dir_all(&receipt)?;

        let error = ingest_wiki_snapshot(wiki, version, temp_dir.path())
            .expect_err("receipt rename failure must fail the ingest stage");
        assert!(!error.to_string().is_empty());
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
