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

#[cfg(test)]
use crate::fetch;
use crate::fingerprint::{self, StageSpec, TrackedPath};
use crate::snapshot_plan::{SnapshotPlan, SourceSpec};
use crate::{schema, storage};

const INGEST_CHUNK_BYTES: usize = 32 * 1024 * 1024;
pub(crate) const INGEST_ALGORITHM_VERSION: &str =
    "history-tsv-to-qualified-metric-input-v7-generation-schema-v2";

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SourceIngestCommit {
    pub(crate) source_id: String,
    pub(crate) rows: usize,
    pub(crate) reused: bool,
}

#[derive(Clone, Debug)]
struct IngestRoots {
    analytical: PathBuf,
    warehouse: PathBuf,
    metric_input: Option<PathBuf>,
    snapshot_version: Option<String>,
}

#[derive(Default)]
struct LayerOutputs {
    analytical: Vec<PathBuf>,
    warehouse: Vec<PathBuf>,
    metric_input: Vec<PathBuf>,
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
            metric_input: None,
            snapshot_version: None,
        }
    }

    fn snapshot(data_dir: &Path, wiki: &str, snapshot_version: &str) -> Result<Self> {
        let metric_input =
            storage::snapshot_metric_input_wiki_dir(data_dir, wiki, snapshot_version)?;
        Ok(Self {
            analytical: storage::snapshot_analytical_wiki_dir(data_dir, wiki, snapshot_version)?,
            warehouse: storage::snapshot_warehouse_wiki_dir(data_dir, wiki, snapshot_version)?,
            metric_input: Some(metric_input),
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

fn next_year_month(value: &str) -> Result<String> {
    let year = value[..4].parse::<i32>()?;
    let month = value[5..].parse::<u8>()?;
    Ok(if month == 12 {
        format!("{:04}-01", year + 1)
    } else {
        format!("{year:04}-{:02}", month + 1)
    })
}

fn cleanup_planned_source_outputs(roots: &IngestRoots, source: &SourceSpec) -> Result<usize> {
    let mut removed = 0_usize;
    let mut year_month = source.event_range.start.clone();
    loop {
        let year = year_month[..4].parse::<i32>()?;
        let mut layer_roots = vec![&roots.analytical, &roots.warehouse];
        if let Some(metric_input) = roots.metric_input.as_ref() {
            layer_roots.push(metric_input);
        }
        for root in layer_roots {
            let partition = storage::month_partition_dir(root, year, &year_month);
            if !partition.is_dir() {
                continue;
            }
            let temporary_prefix = format!(".{}.part-", source.source_id);
            for entry in fs::read_dir(&partition)? {
                let entry = entry?;
                if !entry.file_type()?.is_file() {
                    continue;
                }
                let path = entry.path();
                let name = entry.file_name();
                let is_temporary = name.to_string_lossy().starts_with(&temporary_prefix)
                    && name.to_string_lossy().contains(".parquet.")
                    && name.to_string_lossy().ends_with(".tmp");
                if storage::is_source_output(&path, &source.source_id) || is_temporary {
                    fs::remove_file(path)?;
                    removed += 1;
                }
            }
            File::open(&partition)?.sync_all()?;
        }
        if year_month == source.event_range.end {
            break;
        }
        year_month = next_year_month(&year_month)?;
    }
    Ok(removed)
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

fn write_parquet(df: &mut DataFrame, dest: &Path, transaction_id: Option<&str>) -> Result<()> {
    dest.parent().map(fs::create_dir_all).transpose()?;
    let target = match transaction_id {
        Some(transaction_id) => {
            let filename = dest
                .file_name()
                .and_then(|name| name.to_str())
                .context("Parquet output has no valid filename")?;
            dest.with_file_name(format!(".{filename}.{transaction_id}.tmp"))
        }
        None => dest.to_path_buf(),
    };
    let write_result = (|| -> Result<()> {
        let mut file = File::create(&target)?;
        ParquetWriter::new(&mut file)
            .with_compression(ParquetCompression::Zstd(None))
            .finish(df)?;
        file.sync_all()?;
        let rows = ParquetReader::new(File::open(&target)?).num_rows()?;
        ensure!(rows == df.height(), "Parquet row validation failed");
        storage::discard_file_cache(&file, 0, file.metadata()?.len());
        if target != dest {
            fs::rename(&target, dest)?;
            File::open(dest.parent().context("Parquet output has no parent")?)?.sync_all()?;
        }
        Ok(())
    })();
    if write_result.is_err() && target != dest {
        let _ = fs::remove_file(&target);
    }
    write_result
}

fn write_partitioned_frames(
    normalized: &DataFrame,
    roots: &IngestRoots,
    source_id: &str,
    chunk_idx: usize,
    transaction_id: Option<&str>,
) -> Result<LayerOutputs> {
    let partition_index = build_partition_index(normalized)?;
    let mut outputs = LayerOutputs::default();

    for ((year, year_month), row_indices) in partition_index {
        let take_idx = UInt32Chunked::from_vec("idx".into(), row_indices);
        let partition_df = normalized.take(&take_idx)?;

        if let Some(metric_input_root) = roots.metric_input.as_ref() {
            let partition_dir = storage::month_partition_dir(metric_input_root, year, &year_month);
            let metric_input_path =
                partition_dir.join(format!("{source_id}.part-{chunk_idx:05}.parquet"));
            let mut metric_input_df =
                partition_df.select(schema::METRIC_INPUT_COLUMNS.iter().copied())?;
            write_parquet(&mut metric_input_df, &metric_input_path, transaction_id)?;
            outputs.metric_input.push(metric_input_path);
        } else {
            let partition_dir = storage::month_partition_dir(&roots.warehouse, year, &year_month);
            let warehouse_path =
                partition_dir.join(format!("{source_id}.part-{chunk_idx:05}.parquet"));
            let mut warehouse_df = partition_df.clone();
            write_parquet(&mut warehouse_df, &warehouse_path, transaction_id)?;
            outputs.warehouse.push(warehouse_path);

            let partition_dir = storage::month_partition_dir(&roots.analytical, year, &year_month);
            let analytical_path =
                partition_dir.join(format!("{source_id}.part-{chunk_idx:05}.parquet"));
            let mut analytical_df =
                partition_df.select(schema::ANALYTICAL_COLUMNS.iter().copied())?;
            write_parquet(&mut analytical_df, &analytical_path, transaction_id)?;
            outputs.analytical.push(analytical_path);
        }
    }

    Ok(outputs)
}

fn flush_chunk(
    chunk_bytes: &mut Vec<u8>,
    roots: &IngestRoots,
    source_id: &str,
    chunk_idx: usize,
    transaction_id: Option<&str>,
) -> Result<(usize, LayerOutputs)> {
    if chunk_bytes.is_empty() {
        return Ok((0, LayerOutputs::default()));
    }

    let bytes = std::mem::take(chunk_bytes);
    let parsed = parse_ingest_chunk(bytes)?;
    let normalized = normalize_revision_chunk(parsed)?;
    let rows = normalized.height();
    if rows == 0 {
        return Ok((0, LayerOutputs::default()));
    }

    let outputs =
        write_partitioned_frames(&normalized, roots, source_id, chunk_idx, transaction_id)?;
    Ok((rows, outputs))
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
        None,
    )
}

fn convert_file_with_chunk_limit(
    src: &Path,
    wiki: &str,
    data_dir: &Path,
    roots: &IngestRoots,
    chunk_limit: usize,
    transaction_id: Option<&str>,
) -> Result<Vec<PathBuf>> {
    let source_id = ingest_source_id(src)?;
    let marker = storage::marker_path_in(&roots.analytical, &source_id);
    if storage::marker_manifest_is_valid_in(data_dir, &roots.analytical, &source_id)? {
        debug!(
            source = %src.display(),
            marker = %marker.display(),
            "skipping already ingested source"
        );
        return storage::collect_parquet_files(
            roots.metric_input.as_deref().unwrap_or(&roots.analytical),
        );
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
    let mut metric_input_paths = Vec::new();

    let conversion = (|| -> Result<()> {
        loop {
            line.clear();
            let bytes_read = reader.read_until(b'\n', &mut line)?;
            if bytes_read == 0 {
                break;
            }

            chunk_bytes.extend_from_slice(&line);
            if chunk_bytes.len() >= chunk_limit {
                let (rows, outputs) = flush_chunk(
                    &mut chunk_bytes,
                    roots,
                    &source_id,
                    chunk_idx,
                    transaction_id,
                )
                .context("failed to flush a bounded ingest chunk")?;
                total_rows += rows;
                analytical_paths.extend(outputs.analytical);
                warehouse_paths.extend(outputs.warehouse);
                metric_input_paths.extend(outputs.metric_input);
                chunk_idx += 1;
            }
        }

        if !chunk_bytes.is_empty() {
            let (rows, outputs) = flush_chunk(
                &mut chunk_bytes,
                roots,
                &source_id,
                chunk_idx,
                transaction_id,
            )?;
            total_rows += rows;
            analytical_paths.extend(outputs.analytical);
            warehouse_paths.extend(outputs.warehouse);
            metric_input_paths.extend(outputs.metric_input);
        }

        Ok(())
    })();

    if let Err(err) = conversion {
        cleanup_written_paths(&analytical_paths);
        cleanup_written_paths(&warehouse_paths);
        cleanup_written_paths(&metric_input_paths);
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
            metric_input_paths: metric_input_paths.clone(),
        };
        if transaction_id.is_some() {
            storage::write_precleaned_marker_manifest_in(
                data_dir,
                &roots.analytical,
                &source_id,
                &manifest,
            )?;
        } else {
            storage::write_marker_manifest_in(data_dir, &roots.analytical, &source_id, &manifest)?;
        }
        Ok(())
    })();
    if let Err(err) = receipt {
        cleanup_written_paths(&analytical_paths);
        cleanup_written_paths(&warehouse_paths);
        cleanup_written_paths(&metric_input_paths);
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
    let metric_input_bytes: u64 = metric_input_paths
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
        metric_input_parts = metric_input_paths.len(),
        metric_input_mb = metric_input_bytes as f64 / 1_048_576.0,
        elapsed_ms = started.elapsed().as_secs_f64() * 1_000.0,
        "converted dump file"
    );

    Ok(if metric_input_paths.is_empty() {
        analytical_paths
    } else {
        metric_input_paths
    })
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
    let plan = SnapshotPlan::resolve(wiki, &snapshot_version)?;
    let expected = plan.filenames()?;
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
        SnapshotPlan::load_or_resolve(data_dir, wiki, version)?;
        let roots = IngestRoots::snapshot(data_dir, wiki, version)?;
        let outputs = ingest_stage_outputs(data_dir, wiki, &roots)?;
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
            let layer_result = storage::snapshot_compute_layer(
                data_dir,
                wiki,
                version,
                storage::GenerationLayer::Analytical,
            );
            let layer = layer_result?;
            return storage::snapshot_fragment_files(data_dir, wiki, version, layer);
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
            let (plan, _) = SnapshotPlan::load_or_resolve(data_dir, wiki, version)?;
            let expected = plan.filenames()?;
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
    if let Some(version) = snapshot_version.as_deref() {
        SnapshotPlan::load_or_resolve(data_dir, wiki, version)?;
    }
    let roots = match snapshot_version.as_deref() {
        Some(version) => IngestRoots::snapshot(data_dir, wiki, version)?,
        None => IngestRoots::legacy(data_dir, wiki),
    };
    fs::create_dir_all(&roots.analytical)?;
    if let Some(metric_input) = roots.metric_input.as_ref() {
        fs::create_dir_all(metric_input)?;
    } else {
        fs::create_dir_all(&roots.warehouse)?;
    }

    info!(
        wiki = wiki,
        snapshot_version = snapshot_version.as_deref().unwrap_or("legacy"),
        files = src_files.len(),
        "ingesting raw dump files"
    );

    src_files.par_iter().try_for_each(|src| {
        convert_file_with_chunk_limit(src, wiki, data_dir, &roots, INGEST_CHUNK_BYTES, None)
            .map(|_| ())
    })?;

    let sources_to_validate = match snapshot_version.as_deref() {
        Some(version) => SnapshotPlan::load_or_resolve(data_dir, wiki, version)?
            .0
            .filenames()?
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
        storage::write_generation_manifest(data_dir, wiki, snapshot_version)?;
        let inputs = ingest_stage_inputs(data_dir, wiki, &roots, &src_files)?;
        let outputs = ingest_stage_outputs(data_dir, wiki, &roots)?;
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

    let output_root = roots.metric_input.as_deref().unwrap_or(&roots.analytical);
    let output_paths = storage::collect_parquet_files(output_root)?;
    info!(
        wiki = wiki,
        snapshot_version = snapshot_version.as_deref().unwrap_or("legacy"),
        files = output_paths.len(),
        output_dir = %output_root.display(),
        "finished ingest"
    );
    if let Some(snapshot) = snapshot_version.as_deref() {
        let layer_result = storage::snapshot_compute_layer(
            data_dir,
            wiki,
            snapshot,
            storage::GenerationLayer::Analytical,
        );
        let layer = layer_result?;
        storage::snapshot_fragment_files(data_dir, wiki, snapshot, layer)
    } else {
        Ok(output_paths)
    }
}

fn ingest_stage_outputs(
    data_dir: &Path,
    wiki: &str,
    roots: &IngestRoots,
) -> Result<Vec<TrackedPath>> {
    if let Some(snapshot_version) = roots.snapshot_version.as_deref() {
        let manifest = storage::generation_manifest_path(data_dir, wiki, snapshot_version)?;
        if manifest.is_file() {
            return Ok(vec![TrackedPath::new("generation-manifest", manifest)]);
        }
    }
    let mut outputs = Vec::new();
    let mut layer_roots = vec![
        ("analytical", &roots.analytical),
        ("warehouse", &roots.warehouse),
    ];
    if let Some(metric_input) = roots.metric_input.as_ref() {
        layer_roots.push(("metric-input", metric_input));
    }
    for (prefix, root) in layer_roots {
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
    data_dir: &Path,
    wiki: &str,
    roots: &IngestRoots,
    raw_sources: &[PathBuf],
) -> Result<Vec<TrackedPath>> {
    let mut inputs = Vec::new();
    if let Some(snapshot) = roots.snapshot_version.as_deref() {
        let (_, plan_path) = SnapshotPlan::load_or_resolve(data_dir, wiki, snapshot)?;
        inputs.push(TrackedPath::new("snapshot-plan", plan_path));
    }
    for source in raw_sources {
        let source_id = ingest_source_id(source)?;
        inputs.push(TrackedPath::new(format!("raw/{source_id}"), source));
    }
    Ok(inputs)
}

fn snapshot_marker_inputs(
    data_dir: &Path,
    wiki: &str,
    snapshot_version: &str,
) -> Result<Vec<TrackedPath>> {
    let (plan, plan_path) = SnapshotPlan::load_or_resolve(data_dir, wiki, snapshot_version)?;
    let analytical_root = storage::snapshot_analytical_wiki_dir(data_dir, wiki, snapshot_version)?;
    let mut inputs = vec![TrackedPath::new("snapshot-plan", plan_path)];
    for source in plan.sources {
        let marker = storage::marker_path_in(&analytical_root, &source.source_id);
        ensure!(
            storage::marker_manifest_is_valid_in(data_dir, &analytical_root, &source.source_id,)?,
            "snapshot source {} has no valid ingest marker",
            source.source_id
        );
        inputs.push(TrackedPath::new(
            format!("ingest-marker/{}", source.source_id),
            marker,
        ));
    }
    Ok(inputs)
}

/// Convert and commit one planned source, then release its compressed input.
/// The marker is synced and revalidated against the exact source before raw
/// deletion, making the marker the restart boundary for this transaction.
pub(crate) fn ingest_snapshot_source(
    wiki: &str,
    snapshot_version: &str,
    data_dir: &Path,
    source: &Path,
    run_id: &str,
) -> Result<SourceIngestCommit> {
    storage::validate_snapshot_version(snapshot_version)?;
    ensure!(
        !run_id.is_empty()
            && run_id.len() <= 160
            && run_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.')),
        "invalid source transaction run ID {run_id:?}"
    );
    let (plan, _) = SnapshotPlan::load_or_resolve(data_dir, wiki, snapshot_version)?;
    let filename = source
        .file_name()
        .and_then(|name| name.to_str())
        .context("snapshot source has no valid filename")?;
    let source_id = ingest_source_id(source)?;
    let planned_source = plan
        .sources
        .iter()
        .find(|planned| {
            planned.source_id == source_id && planned.filename().ok() == Some(filename)
        })
        .context(format!(
            "source {filename} is not part of the persisted snapshot plan for {wiki} {snapshot_version}"
        ))?;
    ensure!(
        source.is_file(),
        "snapshot source is missing: {}",
        source.display()
    );

    let roots = IngestRoots::snapshot(data_dir, wiki, snapshot_version)?;
    fs::create_dir_all(&roots.analytical)?;
    let metric_input_root = roots
        .metric_input
        .as_ref()
        .context("snapshot ingest has no metric-input root")?;
    fs::create_dir_all(metric_input_root)?;
    let reused = storage::marker_manifest_is_valid_in(data_dir, &roots.analytical, &source_id)?;
    if !reused {
        ensure!(
            storage::current_snapshot_version(data_dir, wiki)?.as_deref() != Some(snapshot_version),
            "selected generation is immutable; rebuild this source in a new candidate generation"
        );
        let stale_outputs = cleanup_planned_source_outputs(&roots, planned_source)?;
        if stale_outputs > 0 {
            info!(
                wiki,
                snapshot_version,
                source = source_id,
                stale_outputs,
                "removed abandoned source outputs"
            );
        }
        convert_file_with_chunk_limit(
            source,
            wiki,
            data_dir,
            &roots,
            INGEST_CHUNK_BYTES,
            Some(run_id),
        )
        .context("source-window decode failed")?;
    }
    ensure!(
        storage::marker_manifest_covers_source_in(data_dir, &roots.analytical, &source_id, source,),
        "strict ingest marker for {source_id} does not cover its staged source"
    );
    let manifest = storage::read_marker_manifest_in(data_dir, &roots.analytical, &source_id)?
        .context("validated ingest marker disappeared before source commit")?;
    fs::remove_file(source).context("failed to release committed compressed source")?;
    let parent = source.parent().context("snapshot source has no parent")?;
    File::open(parent)?.sync_all()?;
    info!(
        wiki,
        snapshot_version,
        source = source_id,
        rows = manifest.rows,
        reused,
        "committed ingest source and released compressed input"
    );
    Ok(SourceIngestCommit {
        source_id,
        rows: manifest.rows,
        reused,
    })
}

/// Validate the exact marker/output inventory, record a deterministic ingest
/// receipt using those durable source commits, and only then select the new
/// generation for downstream compute.
pub(crate) fn finalize_snapshot_ingest(
    wiki: &str,
    snapshot_version: &str,
    data_dir: &Path,
) -> Result<Vec<PathBuf>> {
    finalize_snapshot_ingest_with_selection(wiki, snapshot_version, data_dir, true)
}

pub(crate) fn finalize_snapshot_ingest_candidate(
    wiki: &str,
    snapshot_version: &str,
    data_dir: &Path,
) -> Result<Vec<PathBuf>> {
    finalize_snapshot_ingest_with_selection(wiki, snapshot_version, data_dir, false)
}

fn finalize_snapshot_ingest_with_selection(
    wiki: &str,
    snapshot_version: &str,
    data_dir: &Path,
    select_generation: bool,
) -> Result<Vec<PathBuf>> {
    let roots = IngestRoots::snapshot(data_dir, wiki, snapshot_version)?;
    storage::write_generation_manifest(data_dir, wiki, snapshot_version)?;
    let inputs = snapshot_marker_inputs(data_dir, wiki, snapshot_version)?;
    let outputs = ingest_stage_outputs(data_dir, wiki, &roots)?;
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
    if select_generation {
        storage::publish_current_snapshot(data_dir, wiki, snapshot_version)?;
    }
    let layer_result = storage::snapshot_compute_layer(
        data_dir,
        wiki,
        snapshot_version,
        storage::GenerationLayer::Analytical,
    );
    let layer = layer_result?;
    storage::snapshot_fragment_files(data_dir, wiki, snapshot_version, layer)
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

    fn build_concurrency_qualification_generation(
        data_dir: &Path,
        worker_count: usize,
    ) -> Result<()> {
        let wiki = "enwiki";
        let snapshot = "2001-02";
        let (plan, _) = SnapshotPlan::load_or_resolve(data_dir, wiki, snapshot)?;
        let staging = data_dir.join("qualification-inputs");
        fs::create_dir_all(&staging)?;
        let mut sources = Vec::new();
        for (index, source) in plan.sources.iter().enumerate() {
            let path = staging.join(source.filename()?);
            let timestamp = format!("{}-15 12:00:00.0", source.event_range.start);
            let row = sample_row(
                &timestamp,
                &(index + 1).to_string(),
                &(100 + index).to_string(),
                "revision",
                "create",
            )
            .replacen("testwiki", wiki, 1);
            write_bz2_dump(&path, &[row])?;
            sources.push(path);
        }
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(worker_count)
            .build()?;
        pool.install(|| {
            sources.par_iter().try_for_each(|source| {
                ingest_snapshot_source(
                    wiki,
                    snapshot,
                    data_dir,
                    source,
                    &format!("qualification-workers-{worker_count}"),
                )
                .map(|_| ())
            })
        })?;
        finalize_snapshot_ingest_candidate(wiki, snapshot, data_dir)?;
        Ok(())
    }

    fn write_snapshot_source_fixture(
        data_dir: &Path,
    ) -> Result<(SnapshotPlan, SourceSpec, PathBuf)> {
        let (plan, _) = SnapshotPlan::load_or_resolve(data_dir, "testwiki", "2026-08")?;
        let source_spec = plan.sources[0].clone();
        let staging = data_dir.join("source-fixture");
        fs::create_dir_all(&staging)?;
        let source = staging.join(source_spec.filename()?);
        write_bz2_dump(
            &source,
            &[sample_row(
                "2026-08-15 12:00:00.0",
                "42",
                "100",
                "revision",
                "create",
            )],
        )
        .expect("snapshot source fixture must compress");
        Ok((plan, source_spec, source))
    }

    #[test]
    fn source_concurrency_does_not_change_fragment_bytes() -> Result<()> {
        let baseline = TestDir::new()?;
        let candidate = TestDir::new()?;
        let reports = TestDir::new()?;
        build_concurrency_qualification_generation(baseline.path(), 1)?;
        build_concurrency_qualification_generation(candidate.path(), 2)?;
        let wiki = "enwiki";
        let snapshot = "2001-02";
        let expected_sources = SnapshotPlan::load_or_resolve(baseline.path(), wiki, snapshot)?
            .0
            .sources
            .len();
        let (layer, baseline_root, candidate_root) = (
            "metric-input",
            storage::snapshot_metric_input_wiki_dir(baseline.path(), wiki, snapshot)?,
            storage::snapshot_metric_input_wiki_dir(candidate.path(), wiki, snapshot)?,
        );
        let report = crate::determinism::qualify_concurrency(
            &baseline_root,
            &candidate_root,
            "parquet",
            1,
            2,
            INGEST_ALGORITHM_VERSION,
            &reports.path().join(format!("{layer}.json")),
        )
        .expect("worker-independent ingest fragments must qualify");
        assert_eq!(report.artifact_count, expected_sources);
        Ok(())
    }

    #[test]
    fn source_restart_recovers_parquets_renamed_before_marker_publication() -> Result<()> {
        let data_dir = TestDir::new()?;
        let (_plan, source_spec, source) = write_snapshot_source_fixture(data_dir.path())?;
        let roots = IngestRoots::snapshot(data_dir.path(), "testwiki", "2026-08")?;
        let outputs = convert_file_with_chunk_limit(
            &source,
            "testwiki",
            data_dir.path(),
            &roots,
            INGEST_CHUNK_BYTES,
            Some("killed-after-rename"),
        )
        .expect("crash fixture must reach marker publication");
        let marker = storage::marker_path_in(&roots.analytical, &source_spec.source_id);
        fs::remove_file(&marker)?;
        assert!(outputs.iter().all(|path| path.is_file()));

        let commit = ingest_snapshot_source(
            "testwiki",
            "2026-08",
            data_dir.path(),
            &source,
            "restart-after-rename",
        )
        .expect("restart must rebuild uncommitted renamed Parquets");

        assert!(!commit.reused);
        assert!(!source.exists());
        let marker_valid = storage::marker_manifest_is_valid_in(
            data_dir.path(),
            &roots.analytical,
            &source_spec.source_id,
        )
        .expect("rebuilt marker must be readable");
        assert!(marker_valid);
        assert!(storage::collect_parquet_files(&roots.analytical)?.is_empty());
        assert!(storage::collect_parquet_files(&roots.warehouse)?.is_empty());
        let metric_input_root = roots.metric_input.as_ref().expect("metric input root");
        assert_eq!(storage::collect_parquet_files(metric_input_root)?.len(), 1);
        Ok(())
    }

    #[test]
    fn source_restart_removes_parquet_temporaries_left_before_rename() -> Result<()> {
        let data_dir = TestDir::new()?;
        let (_plan, source_spec, source) = write_snapshot_source_fixture(data_dir.path())?;
        let roots = IngestRoots::snapshot(data_dir.path(), "testwiki", "2026-08")?;
        let partition = storage::month_partition_dir(&roots.analytical, 2026, "2026-08");
        let warehouse_partition = storage::month_partition_dir(&roots.warehouse, 2026, "2026-08");
        let metric_input_partition = storage::month_partition_dir(
            roots.metric_input.as_ref().expect("metric input root"),
            2026,
            "2026-08",
        );
        fs::create_dir_all(&partition)?;
        fs::create_dir_all(&warehouse_partition)?;
        fs::create_dir_all(&metric_input_partition)?;
        let temporary_name = format!(
            ".{}.part-00000.parquet.killed-before-rename.tmp",
            source_spec.source_id
        );
        let analytical_temporary = partition.join(&temporary_name);
        let warehouse_temporary = warehouse_partition.join(&temporary_name);
        let metric_input_temporary = metric_input_partition.join(&temporary_name);
        fs::write(&analytical_temporary, b"partial")?;
        fs::write(&warehouse_temporary, b"partial")?;
        fs::write(&metric_input_temporary, b"partial")?;

        let commit = ingest_snapshot_source(
            "testwiki",
            "2026-08",
            data_dir.path(),
            &source,
            "restart-before-rename",
        )
        .expect("restart must replace abandoned Parquet temporaries");

        assert!(!commit.reused);
        assert!(!analytical_temporary.exists());
        assert!(!warehouse_temporary.exists());
        assert!(!metric_input_temporary.exists());
        assert!(storage::collect_parquet_files(&roots.analytical)?.is_empty());
        assert!(storage::collect_parquet_files(&roots.warehouse)?.is_empty());
        let metric_input_root = roots.metric_input.as_ref().expect("metric input root");
        assert_eq!(storage::collect_parquet_files(metric_input_root)?.len(), 1);
        let marker_valid = storage::marker_manifest_is_valid_in(
            data_dir.path(),
            &roots.analytical,
            &source_spec.source_id,
        )
        .expect("marker after temporary recovery must be readable");
        assert!(marker_valid);
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

        write_parquet(&mut df, &dest, None)?;

        assert!(dest.exists());
        Ok(())
    }

    #[test]
    fn transactional_parquet_write_cleans_temp_when_commit_fails() -> Result<()> {
        let temp_dir = TestDir::new()?;
        let dest = temp_dir.path().join("blocked.parquet");
        fs::create_dir(&dest)?;
        let mut frame = DataFrame::new_infer_height(vec![Column::new("value".into(), [1_i64])])?;
        let temporary = temp_dir.path().join(".blocked.parquet.test-run.tmp");

        assert!(write_parquet(&mut frame, &dest, Some("test-run")).is_err());
        assert!(!temporary.exists());
        Ok(())
    }

    #[test]
    fn flush_chunk_returns_zero_for_empty_and_filtered_chunks() -> Result<()> {
        init_test_tracing();
        let temp_dir = TestDir::new()?;
        let roots = IngestRoots::legacy(temp_dir.path(), "testwiki");

        let empty = flush_chunk(&mut Vec::new(), &roots, "source", 0, None)?;
        assert_eq!(empty.0, 0);
        assert!(empty.1.analytical.is_empty());
        assert!(empty.1.warehouse.is_empty());
        assert!(empty.1.metric_input.is_empty());

        let filtered_row = sample_row("2024-01-01 00:00:00.0", "42", "100", "page", "create");
        let mut filtered_bytes = filtered_row.into_bytes();
        let filtered = flush_chunk(&mut filtered_bytes, &roots, "source", 1, None)?;
        assert_eq!(filtered.0, 0);
        assert!(filtered.1.analytical.is_empty());
        assert!(filtered.1.warehouse.is_empty());
        assert!(filtered.1.metric_input.is_empty());
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
        let outputs = convert_file_with_chunk_limit(
            &src,
            wiki,
            temp_dir.path(),
            &roots,
            128,
            Some("threshold-run"),
        )
        .expect("transactional threshold fixture should ingest");

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

        assert!(
            convert_file_with_chunk_limit(
                &src,
                wiki,
                temp_dir.path(),
                &IngestRoots::legacy(temp_dir.path(), wiki),
                INGEST_CHUNK_BYTES,
                Some("interrupted-run"),
            )
            .is_err()
        );
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
        let fragment = paths[0].clone();
        let fragment_bytes = fs::read(&fragment)?;
        fs::write(&fragment, vec![0_u8; fragment_bytes.len()])?;
        let reused = ingest_wiki_snapshot(wiki, version, temp_dir.path())?;

        assert_eq!(paths.len(), 1);
        assert_eq!(reused, paths);
        assert_eq!(fs::read(receipt)?, receipt_before);
        assert!(storage::read_generation_manifest(temp_dir.path(), wiki, version).is_err());
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
    fn ingest_stage_inputs_rejects_a_corrupt_snapshot_plan() -> Result<()> {
        let data_dir = TestDir::new()?;
        let wiki = "testwiki";
        let version = "2026-08";
        let roots = IngestRoots::snapshot(data_dir.path(), wiki, version)?;
        let plan_path = crate::snapshot_plan::plan_path(data_dir.path(), wiki, version)?;
        fs::create_dir_all(plan_path.parent().expect("plan parent"))?;
        fs::write(&plan_path, b"{truncated")?;

        assert!(ingest_stage_inputs(data_dir.path(), wiki, &roots, &[]).is_err());

        let legacy_source = data_dir.path().join("legacy.tsv.bz2");
        fs::write(&legacy_source, b"BZhfixture")?;
        let legacy = IngestRoots::legacy(data_dir.path(), wiki);
        let inputs = ingest_stage_inputs(data_dir.path(), wiki, &legacy, &[legacy_source])?;
        assert_eq!(inputs.len(), 1);
        assert_eq!(inputs[0].identity, "raw/legacy");
        Ok(())
    }

    #[test]
    fn ingest_stage_fails_when_marker_inventory_cannot_be_read() -> Result<()> {
        let temp_dir = TestDir::new()?;
        let roots = IngestRoots::snapshot(temp_dir.path(), "testwiki", "2026-08")?;
        fs::create_dir_all(&roots.analytical)?;
        fs::create_dir_all(&roots.warehouse)?;
        assert!(ingest_stage_outputs(temp_dir.path(), "testwiki", &roots)?.is_empty());
        let legacy = IngestRoots::legacy(temp_dir.path(), "legacywiki");
        assert!(ingest_stage_outputs(temp_dir.path(), "legacywiki", &legacy)?.is_empty());
        fs::write(roots.analytical.join("_markers"), "not-a-directory")?;
        assert!(ingest_stage_outputs(temp_dir.path(), "testwiki", &roots).is_err());
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
        let planned = SnapshotPlan::resolve("testwiki", "2026-08")?
            .sources
            .remove(0);
        assert_eq!(
            cleanup_planned_source_outputs(
                &IngestRoots::snapshot(temp_dir.path(), "testwiki", "2026-08")?,
                &planned,
            )
            .expect("missing candidate roots should require no cleanup"),
            0
        );
        Ok(())
    }

    #[test]
    fn snapshot_source_transaction_is_resumable_and_reclaims_raw_input() -> Result<()> {
        init_test_tracing();
        let data_dir = TestDir::new()?;
        let wiki = "testwiki";
        let version = "2026-08";
        let (plan, _) = SnapshotPlan::load_or_resolve(data_dir.path(), wiki, version)?;
        let planned = &plan.sources[0];
        let staging = data_dir
            .path()
            .join("raw")
            .join(wiki)
            .join(".source-window");
        fs::create_dir_all(&staging)?;
        let source = staging.join(planned.filename()?);
        write_bz2_dump(
            &source,
            &[sample_row(
                "2026-08-01 00:00:00.0",
                "42",
                "100",
                "revision",
                "create",
            )],
        )
        .expect("source transaction fixture should compress");
        let compressed = fs::read(&source)?;

        assert_eq!(
            fetch::pending_snapshot_sources(wiki, version, data_dir.path())?.len(),
            1
        );
        assert!(fetch::finalize_snapshot_fetch(wiki, version, data_dir.path()).is_err());
        let first = ingest_snapshot_source(wiki, version, data_dir.path(), &source, "first-run")?;
        assert_eq!(first.source_id, planned.source_id);
        assert_eq!(first.rows, 1);
        assert!(!first.reused);
        assert!(!source.exists());
        assert_eq!(
            storage::current_snapshot_version(data_dir.path(), wiki)?,
            None
        );
        assert!(fetch::pending_snapshot_sources(wiki, version, data_dir.path())?.is_empty());

        let analytical = storage::snapshot_analytical_wiki_dir(data_dir.path(), wiki, version)?;
        let marker = storage::marker_path_in(&analytical, &planned.source_id);
        let marker_bytes = fs::read(&marker)?;
        fs::write(&source, &compressed)?;
        let reused = ingest_snapshot_source(wiki, version, data_dir.path(), &source, "retry-run")?;
        assert!(reused.reused);
        assert_eq!(fs::read(&marker)?, marker_bytes);
        assert!(!source.exists());

        let stale_output = analytical
            .join("year=2026/year_month=2026-08")
            .join(format!(
                ".{}.part-99999.parquet.dead-run.tmp",
                planned.source_id
            ));
        stale_output.parent().map(fs::create_dir_all).transpose()?;
        fs::write(&stale_output, b"partial parquet")?;
        let unrelated_file = stale_output
            .parent()
            .context("stale output parent")?
            .join("keep.txt");
        let unrelated_dir = stale_output
            .parent()
            .context("stale output parent")?
            .join("keep-dir");
        fs::write(&unrelated_file, b"keep")?;
        fs::create_dir(&unrelated_dir)?;
        fs::write(&marker, b"{truncated")?;
        fs::write(&source, &compressed)?;
        let rebuilt =
            ingest_snapshot_source(wiki, version, data_dir.path(), &source, "rebuild-run")?;
        assert!(!rebuilt.reused);
        assert!(!stale_output.exists());
        assert!(unrelated_file.exists());
        assert!(unrelated_dir.exists());
        assert!(
            storage::marker_manifest_is_valid_in(data_dir.path(), &analytical, &planned.source_id,)
                .expect("rebuilt marker should be readable")
        );

        fetch::finalize_snapshot_fetch(wiki, version, data_dir.path())?;
        let outputs = finalize_snapshot_ingest(wiki, version, data_dir.path())?;
        assert_eq!(outputs.len(), 1);
        assert_eq!(
            storage::current_snapshot_version(data_dir.path(), wiki)?.as_deref(),
            Some(version)
        );
        assert!(
            fingerprint::data_stage_receipt_path(data_dir.path(), wiki, version, "fetch").is_file()
        );
        assert!(
            fingerprint::data_stage_receipt_path(data_dir.path(), wiki, version, "ingest")
                .is_file()
        );
        fs::write(&marker, b"{truncated")?;
        fs::write(&source, &compressed)?;
        let immutable =
            ingest_snapshot_source(wiki, version, data_dir.path(), &source, "selected-rebuild")
                .expect_err("selected generation fragments must not be rebuilt in place");
        assert!(immutable.to_string().contains("immutable"));
        assert!(source.is_file());
        Ok(())
    }

    #[test]
    fn source_transaction_rejects_unplanned_or_missing_inputs() -> Result<()> {
        let data_dir = TestDir::new()?;
        SnapshotPlan::load_or_resolve(data_dir.path(), "testwiki", "2026-08")?;
        let unplanned = data_dir.path().join("raw/testwiki/unplanned.tsv.bz2");
        fs::create_dir_all(unplanned.parent().context("unplanned source parent")?)?;
        fs::write(&unplanned, b"BZhfixture")?;
        assert!(
            ingest_snapshot_source(
                "testwiki",
                "2026-08",
                data_dir.path(),
                &unplanned,
                "test-run",
            )
            .is_err()
        );

        let planned = data_dir
            .path()
            .join("raw/testwiki/2026-08.testwiki.all-time.tsv.bz2");
        assert!(
            ingest_snapshot_source(
                "testwiki",
                "2026-08",
                data_dir.path(),
                &planned,
                "../unsafe",
            )
            .unwrap_err()
            .to_string()
            .contains("invalid source transaction run ID")
        );
        fs::write(&planned, b"BZhnot-a-valid-bzip-stream")?;
        let decode_error = ingest_snapshot_source(
            "testwiki",
            "2026-08",
            data_dir.path(),
            &planned,
            "decode-run",
        )
        .expect_err("corrupt compressed input must fail its source transaction");
        assert!(
            decode_error
                .to_string()
                .contains("source-window decode failed")
        );
        fs::remove_file(&planned)?;
        let error =
            ingest_snapshot_source("testwiki", "2026-08", data_dir.path(), &planned, "test-run")
                .expect_err("missing planned source must fail");
        assert!(error.to_string().contains("source is missing"));
        Ok(())
    }

    #[test]
    fn committed_source_window_cleanup_removes_only_marker_backed_inputs() -> Result<()> {
        let data_dir = TestDir::new()?;
        let wiki = "testwiki";
        let version = "2026-08";
        let (plan, _) = SnapshotPlan::load_or_resolve(data_dir.path(), wiki, version)?;
        let planned = &plan.sources[0];
        let staging = data_dir
            .path()
            .join("raw")
            .join(wiki)
            .join(".source-window");
        fs::create_dir_all(&staging)?;
        let source = staging.join(planned.filename()?);
        write_bz2_dump(
            &source,
            &[sample_row(
                "2026-08-01 00:00:00.0",
                "42",
                "100",
                "revision",
                "create",
            )],
        )
        .expect("cleanup fixture should compress");
        let compressed = fs::read(&source)?;
        ingest_snapshot_source(wiki, version, data_dir.path(), &source, "cleanup-run")?;

        fs::write(&source, &compressed)?;
        let abandoned = staging.join(format!(".{}.old-run.download", planned.source_id));
        fs::write(&abandoned, &compressed[..3])?;
        let unrelated = staging.join("operator-note");
        fs::write(&unrelated, b"keep")?;
        let unrelated_dir = staging.join("operator-directory");
        fs::create_dir(&unrelated_dir)?;

        assert_eq!(
            fetch::cleanup_committed_source_window_inputs(wiki, version, data_dir.path())?,
            2
        );
        assert!(!source.exists());
        assert!(!abandoned.exists());
        assert!(unrelated.exists());
        assert!(unrelated_dir.exists());
        Ok(())
    }

    #[test]
    fn source_transaction_rejects_a_valid_marker_for_another_staged_path() -> Result<()> {
        let data_dir = TestDir::new()?;
        let wiki = "testwiki";
        let version = "2026-08";
        let (plan, _) = SnapshotPlan::load_or_resolve(data_dir.path(), wiki, version)?;
        let filename = plan.sources[0].filename()?;
        let staged = data_dir
            .path()
            .join("raw/testwiki/.source-window")
            .join(filename);
        staged.parent().map(fs::create_dir_all).transpose()?;
        write_bz2_dump(
            &staged,
            &[sample_row(
                "2026-08-01 00:00:00.0",
                "42",
                "100",
                "revision",
                "create",
            )],
        )
        .expect("alternate-path fixture should compress");
        let compressed = fs::read(&staged)?;
        ingest_snapshot_source(wiki, version, data_dir.path(), &staged, "first-run")?;

        let other = data_dir.path().join("raw/testwiki").join(filename);
        fs::write(&other, compressed)?;
        let error = ingest_snapshot_source(wiki, version, data_dir.path(), &other, "second-run")
            .expect_err("a marker for another staged path must not cover this input");
        assert!(error.to_string().contains("does not cover"));
        Ok(())
    }

    #[test]
    fn snapshot_pointer_stays_unpublished_when_ingest_receipt_commit_fails() -> Result<()> {
        let data_dir = TestDir::new()?;
        let wiki = "testwiki";
        let version = "2026-08";
        let (plan, _) = SnapshotPlan::load_or_resolve(data_dir.path(), wiki, version)?;
        let staged = data_dir
            .path()
            .join("raw/testwiki/.source-window")
            .join(plan.sources[0].filename()?);
        staged.parent().map(fs::create_dir_all).transpose()?;
        write_bz2_dump(
            &staged,
            &[sample_row(
                "2026-08-01 00:00:00.0",
                "42",
                "100",
                "revision",
                "create",
            )],
        )
        .expect("receipt failure fixture should compress");
        ingest_snapshot_source(wiki, version, data_dir.path(), &staged, "receipt-run")?;
        let receipt =
            fingerprint::data_stage_receipt_path(data_dir.path(), wiki, version, "ingest");
        fs::create_dir_all(&receipt)?;

        assert!(finalize_snapshot_ingest(wiki, version, data_dir.path()).is_err());
        assert_eq!(
            storage::current_snapshot_version(data_dir.path(), wiki)?,
            None
        );
        Ok(())
    }
}
