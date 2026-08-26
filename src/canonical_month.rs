use anyhow::{Context, Result, ensure};
use polars::prelude::{DataFrame, DataType};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::{schema, storage};

pub(crate) const LOGICAL_SCHEMA_VERSION: u32 = 2;
pub(crate) const ENCODING_VERSION: &str = "metric-input-canonical-row-v1";
const RECEIPT_SCHEMA_VERSION: u32 = 1;
const BATCH_ROWS: usize = 100_000;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct MonthIdentity {
    pub(crate) schema_version: u32,
    pub(crate) wiki: String,
    pub(crate) event_month: String,
    pub(crate) logical_schema_version: u32,
    pub(crate) encoding_version: String,
    pub(crate) ordering_contract: String,
    pub(crate) digest: String,
    pub(crate) rows: u64,
    pub(crate) edits: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct MonthInventory {
    pub(crate) schema_version: u32,
    pub(crate) wiki: String,
    pub(crate) snapshot: String,
    pub(crate) generation_manifest_sha256: String,
    pub(crate) identities: Vec<MonthIdentity>,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct CanonicalRow {
    event_timestamp: Option<String>,
    event_user_id: Option<i64>,
    event_user_text: Option<String>,
    event_user_is_bot_by: Option<String>,
    event_user_is_anonymous: Option<bool>,
    event_user_is_temporary: Option<bool>,
    page_id: Option<i64>,
    page_title: Option<String>,
    page_namespace: Option<i32>,
    revision_id: Option<i64>,
    revision_text_bytes_diff: Option<i64>,
    is_reverted: Option<bool>,
    is_minor: Option<bool>,
}

impl CanonicalRow {
    fn from_frame(frame: &DataFrame, row: usize) -> Result<Self> {
        Ok(Self {
            event_timestamp: string_at(frame, "event_timestamp", row)?,
            event_user_id: i64_at(frame, "event_user_id", row)?,
            event_user_text: string_at(frame, "event_user_text", row)?,
            event_user_is_bot_by: string_at(frame, "event_user_is_bot_by", row)?,
            event_user_is_anonymous: bool_at(frame, "event_user_is_anonymous", row)?,
            event_user_is_temporary: bool_at(frame, "event_user_is_temporary", row)?,
            page_id: i64_at(frame, "page_id", row)?,
            page_title: string_at(frame, "page_title", row)?,
            page_namespace: i32_at(frame, "page_namespace", row)?,
            revision_id: i64_at(frame, "revision_id", row)?,
            revision_text_bytes_diff: i64_at(frame, "revision_text_bytes_diff", row)?,
            is_reverted: bool_at(frame, "is_reverted", row)?,
            is_minor: bool_at(frame, "is_minor", row)?,
        })
    }

    fn update_digest(&self, digest: &mut Sha256) {
        encode_string(digest, self.event_timestamp.as_deref());
        encode_i64(digest, self.event_user_id);
        encode_string(digest, self.event_user_text.as_deref());
        encode_string(digest, self.event_user_is_bot_by.as_deref());
        encode_bool(digest, self.event_user_is_anonymous);
        encode_bool(digest, self.event_user_is_temporary);
        encode_i64(digest, self.page_id);
        encode_string(digest, self.page_title.as_deref());
        encode_i32(digest, self.page_namespace);
        encode_i64(digest, self.revision_id);
        encode_i64(digest, self.revision_text_bytes_diff);
        encode_bool(digest, self.is_reverted);
        encode_bool(digest, self.is_minor);
    }
}

fn string_at(frame: &DataFrame, name: &str, row: usize) -> Result<Option<String>> {
    Ok(frame
        .column(name)
        .with_context(|| format!("canonical month input is missing {name}"))?
        .str()
        .with_context(|| format!("canonical month column {name} is not String"))?
        .get(row)
        .map(str::to_owned))
}

fn i64_at(frame: &DataFrame, name: &str, row: usize) -> Result<Option<i64>> {
    Ok(frame
        .column(name)
        .with_context(|| format!("canonical month input is missing {name}"))?
        .i64()
        .with_context(|| format!("canonical month column {name} is not Int64"))?
        .get(row))
}

fn i32_at(frame: &DataFrame, name: &str, row: usize) -> Result<Option<i32>> {
    Ok(frame
        .column(name)
        .with_context(|| format!("canonical month input is missing {name}"))?
        .i32()
        .with_context(|| format!("canonical month column {name} is not Int32"))?
        .get(row))
}

fn bool_at(frame: &DataFrame, name: &str, row: usize) -> Result<Option<bool>> {
    Ok(frame
        .column(name)
        .with_context(|| format!("canonical month input is missing {name}"))?
        .bool()
        .with_context(|| format!("canonical month column {name} is not Boolean"))?
        .get(row))
}

fn encode_marker(digest: &mut Sha256, present: bool) {
    digest.update([u8::from(present)]);
}

fn encode_string(digest: &mut Sha256, value: Option<&str>) {
    encode_marker(digest, value.is_some());
    if let Some(value) = value {
        digest.update((value.len() as u64).to_be_bytes());
        digest.update(value.as_bytes());
    }
}

fn encode_i64(digest: &mut Sha256, value: Option<i64>) {
    encode_marker(digest, value.is_some());
    if let Some(value) = value {
        digest.update(value.to_be_bytes());
    }
}

fn encode_i32(digest: &mut Sha256, value: Option<i32>) {
    encode_marker(digest, value.is_some());
    if let Some(value) = value {
        digest.update(value.to_be_bytes());
    }
}

fn encode_bool(digest: &mut Sha256, value: Option<bool>) {
    encode_marker(digest, value.is_some());
    if let Some(value) = value {
        digest.update([u8::from(value)]);
    }
}

struct SortedRun {
    reader: storage::SequentialParquetReader,
    batch: Option<DataFrame>,
    row: usize,
    previous: Option<CanonicalRow>,
}

impl SortedRun {
    fn new(path: &Path) -> Result<Self> {
        let reader = storage::SequentialParquetReader::new(
            path,
            Some(
                schema::METRIC_INPUT_COLUMNS
                    .iter()
                    .map(|column| (*column).to_string())
                    .collect(),
            ),
            BATCH_ROWS,
        )?;
        validate_schema(&reader.schema_frame()?)?;
        Ok(Self {
            reader,
            batch: None,
            row: 0,
            previous: None,
        })
    }

    fn next_row(&mut self) -> Result<Option<CanonicalRow>> {
        loop {
            if let Some(batch) = &self.batch
                && self.row < batch.height()
            {
                let row = CanonicalRow::from_frame(batch, self.row)?;
                self.row += 1;
                if let Some(previous) = &self.previous {
                    ensure!(
                        previous <= &row,
                        "canonical month input fragment violates the logical row order"
                    );
                }
                self.previous = Some(row.clone());
                return Ok(Some(row));
            }
            self.batch = self.reader.next_batch()?;
            self.row = 0;
            if self.batch.is_none() {
                return Ok(None);
            }
        }
    }
}

fn validate_schema(frame: &DataFrame) -> Result<()> {
    let expected = [
        ("event_timestamp", DataType::String),
        ("event_user_id", DataType::Int64),
        ("event_user_text", DataType::String),
        ("event_user_is_bot_by", DataType::String),
        ("event_user_is_anonymous", DataType::Boolean),
        ("event_user_is_temporary", DataType::Boolean),
        ("page_id", DataType::Int64),
        ("page_title", DataType::String),
        ("page_namespace", DataType::Int32),
        ("revision_id", DataType::Int64),
        ("revision_text_bytes_diff", DataType::Int64),
        ("is_reverted", DataType::Boolean),
        ("is_minor", DataType::Boolean),
    ];
    ensure!(
        frame.width() == expected.len(),
        "canonical month schema width changed"
    );
    for (name, data_type) in expected {
        let column = frame
            .column(name)
            .with_context(|| format!("canonical month schema is missing {name}"))?;
        ensure!(
            column.dtype() == &data_type,
            "canonical month column {name} has {:?}, expected {data_type:?}",
            column.dtype()
        );
    }
    Ok(())
}

fn digest_header(digest: &mut Sha256) {
    digest.update(b"wiki-economics\0canonical-month\0");
    digest.update(LOGICAL_SCHEMA_VERSION.to_be_bytes());
    digest.update((ENCODING_VERSION.len() as u64).to_be_bytes());
    digest.update(ENCODING_VERSION.as_bytes());
    for column in schema::METRIC_INPUT_COLUMNS {
        digest.update((column.len() as u64).to_be_bytes());
        digest.update(column.as_bytes());
    }
}

pub(crate) fn compute(
    wiki: &str,
    event_month: &str,
    mut files: Vec<PathBuf>,
) -> Result<MonthIdentity> {
    ensure!(
        event_month.len() == 7
            && event_month.as_bytes()[4] == b'-'
            && event_month
                .bytes()
                .enumerate()
                .all(|(index, byte)| index == 4 || byte.is_ascii_digit()),
        "invalid canonical event month {event_month:?}"
    );
    ensure!(!files.is_empty(), "canonical month has no fragments");
    files.sort();
    files.dedup();
    let mut runs = files
        .iter()
        .map(|path| SortedRun::new(path))
        .collect::<Result<Vec<_>>>()?;
    let mut heap = BinaryHeap::new();
    for (run, reader) in runs.iter_mut().enumerate() {
        if let Some(row) = reader.next_row()? {
            heap.push(Reverse((row, run)));
        }
    }
    let mut digest = Sha256::new();
    digest_header(&mut digest);
    let mut rows = 0_u64;
    while let Some(Reverse((row, run))) = heap.pop() {
        let timestamp = row
            .event_timestamp
            .as_deref()
            .context("canonical month row has no event timestamp")?;
        ensure!(
            timestamp.starts_with(event_month),
            "canonical month row escaped event-month partition"
        );
        row.update_digest(&mut digest);
        rows = rows
            .checked_add(1)
            .context("canonical month row overflow")?;
        if let Some(next) = runs[run].next_row()? {
            heap.push(Reverse((next, run)));
        }
    }
    ensure!(rows > 0, "canonical month cannot be empty");
    Ok(MonthIdentity {
        schema_version: RECEIPT_SCHEMA_VERSION,
        wiki: wiki.to_string(),
        event_month: event_month.to_string(),
        logical_schema_version: LOGICAL_SCHEMA_VERSION,
        encoding_version: ENCODING_VERSION.to_string(),
        ordering_contract: "null-first-lexicographic-all-13-columns-v1".to_string(),
        digest: hex::encode(digest.finalize()),
        rows,
        edits: rows,
    })
}

pub(crate) fn receipt_path(
    data_dir: &Path,
    wiki: &str,
    snapshot: &str,
    event_month: &str,
) -> Result<PathBuf> {
    storage::validate_snapshot_version(snapshot)?;
    ensure!(
        event_month
            .bytes()
            .all(|byte| byte.is_ascii_digit() || byte == b'-'),
        "unsafe canonical event month"
    );
    Ok(data_dir
        .join("incremental")
        .join("month-identities")
        .join(wiki)
        .join(snapshot)
        .join(format!("{event_month}.json")))
}

pub(crate) fn inventory_path(data_dir: &Path, wiki: &str, snapshot: &str) -> Result<PathBuf> {
    storage::validate_snapshot_version(snapshot)?;
    Ok(data_dir
        .join("incremental")
        .join("month-identities")
        .join(wiki)
        .join(snapshot)
        .join("inventory.json"))
}

pub(crate) fn write_receipt(path: &Path, identity: &MonthIdentity) -> Result<()> {
    let parent = path.parent().context("month identity has no parent")?;
    fs::create_dir_all(parent)?;
    let name = path
        .file_name()
        .context("month identity has no filename")?
        .to_string_lossy();
    let temporary = parent.join(format!(".{name}.{}.tmp", std::process::id()));
    let mut bytes = serde_json::to_vec_pretty(identity)?;
    bytes.push(b'\n');
    let result = (|| -> Result<()> {
        let mut file = File::create(&temporary)?;
        file.write_all(&bytes)?;
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

pub(crate) fn read_receipt(path: &Path) -> Result<MonthIdentity> {
    let identity: MonthIdentity = serde_json::from_slice(&fs::read(path)?)
        .with_context(|| format!("invalid canonical month receipt {}", path.display()))?;
    ensure!(
        identity.schema_version == RECEIPT_SCHEMA_VERSION
            && identity.logical_schema_version == LOGICAL_SCHEMA_VERSION
            && identity.encoding_version == ENCODING_VERSION
            && identity.digest.len() == 64
            && identity.digest.bytes().all(|byte| byte.is_ascii_hexdigit())
            && identity.rows > 0
            && identity.edits == identity.rows,
        "canonical month receipt violates its contract"
    );
    Ok(identity)
}

fn validate_inventory(
    data_dir: &Path,
    wiki: &str,
    snapshot: &str,
    generation_manifest_sha256: &str,
    inventory: &MonthInventory,
) -> Result<()> {
    ensure!(
        inventory.schema_version == RECEIPT_SCHEMA_VERSION
            && inventory.wiki == wiki
            && inventory.snapshot == snapshot
            && inventory.generation_manifest_sha256 == generation_manifest_sha256
            && !inventory.identities.is_empty(),
        "canonical month inventory identity changed"
    );
    let mut previous = None;
    for identity in &inventory.identities {
        ensure!(
            identity.wiki == wiki
                && previous
                    .as_deref()
                    .is_none_or(|month| month < identity.event_month.as_str()),
            "canonical month inventory is duplicated or unordered"
        );
        let path = receipt_path(data_dir, wiki, snapshot, &identity.event_month)?;
        ensure!(
            read_receipt(&path)? == *identity,
            "canonical month receipt and inventory disagree"
        );
        previous = Some(identity.event_month.clone());
    }
    Ok(())
}

pub(crate) fn ensure_snapshot_inventory(
    data_dir: &Path,
    wiki: &str,
    snapshot: &str,
) -> Result<MonthInventory> {
    storage::validate_snapshot_version(snapshot)?;
    let manifest = storage::read_generation_manifest(data_dir, wiki, snapshot)?;
    ensure!(
        manifest.schema_version == 3,
        "cross-snapshot identities require a compacted schema-v3 generation"
    );
    let manifest_path = storage::generation_manifest_path(data_dir, wiki, snapshot)?;
    let (_, generation_manifest_sha256) = storage::sha256_file(&manifest_path)?;
    let inventory_path = inventory_path(data_dir, wiki, snapshot)?;
    if inventory_path.is_file() {
        let inventory: MonthInventory = serde_json::from_slice(&fs::read(&inventory_path)?)
            .with_context(|| {
                format!(
                    "invalid canonical month inventory {}",
                    inventory_path.display()
                )
            })?;
        validate_inventory(
            data_dir,
            wiki,
            snapshot,
            &generation_manifest_sha256,
            &inventory,
        )?;
        return Ok(inventory);
    }

    let partitions = storage::snapshot_partition_specs(
        data_dir,
        wiki,
        snapshot,
        storage::GenerationLayer::MetricInput,
    )?;
    ensure!(
        !partitions.is_empty(),
        "compacted generation has no metric-input months"
    );
    let mut identities = Vec::with_capacity(partitions.len());
    for partition in partitions {
        let identity = compute(wiki, &partition.year_month, partition.files)?;
        let path = receipt_path(data_dir, wiki, snapshot, &identity.event_month)?;
        write_receipt(&path, &identity)?;
        identities.push(identity);
    }
    let inventory = MonthInventory {
        schema_version: RECEIPT_SCHEMA_VERSION,
        wiki: wiki.to_string(),
        snapshot: snapshot.to_string(),
        generation_manifest_sha256,
        identities,
    };
    validate_inventory(
        data_dir,
        wiki,
        snapshot,
        &inventory.generation_manifest_sha256,
        &inventory,
    )?;
    write_receipt_json(&inventory_path, &inventory)?;
    Ok(inventory)
}

fn write_receipt_json<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    let parent = path.parent().context("canonical receipt has no parent")?;
    fs::create_dir_all(parent)?;
    let name = path
        .file_name()
        .context("canonical receipt has no filename")?
        .to_string_lossy();
    let temporary = parent.join(format!(".{name}.{}.tmp", std::process::id()));
    let mut bytes = serde_json::to_vec_pretty(value)?;
    bytes.push(b'\n');
    let result = (|| -> Result<()> {
        let mut file = File::create(&temporary)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        fs::rename(&temporary, path)?;
        File::open(parent)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(temporary);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::TestDir;
    use anyhow::Result;
    use polars::prelude::{Column, ParquetCompression, ParquetWriter};

    fn frame(revisions: &[i64]) -> Result<DataFrame> {
        let len = revisions.len();
        DataFrame::new_infer_height(vec![
            Column::new("event_timestamp".into(), vec!["2024-02-01 00:00:00.0"; len]),
            Column::new(
                "event_user_id".into(),
                revisions.iter().map(|id| Some(*id)).collect::<Vec<_>>(),
            ),
            Column::new(
                "event_user_text".into(),
                revisions
                    .iter()
                    .map(|id| Some(format!("u{id}")))
                    .collect::<Vec<_>>(),
            ),
            Column::new("event_user_is_bot_by".into(), vec![None::<String>; len]),
            Column::new("event_user_is_anonymous".into(), vec![Some(false); len]),
            Column::new("event_user_is_temporary".into(), vec![Some(false); len]),
            Column::new(
                "page_id".into(),
                revisions.iter().map(|id| Some(*id)).collect::<Vec<_>>(),
            ),
            Column::new(
                "page_title".into(),
                revisions
                    .iter()
                    .map(|id| Some(format!("p{id}")))
                    .collect::<Vec<_>>(),
            ),
            Column::new("page_namespace".into(), vec![Some(0_i32); len]),
            Column::new(
                "revision_id".into(),
                revisions.iter().map(|id| Some(*id)).collect::<Vec<_>>(),
            ),
            Column::new("revision_text_bytes_diff".into(), vec![Some(1_i64); len]),
            Column::new("is_reverted".into(), vec![Some(false); len]),
            Column::new("is_minor".into(), vec![Some(false); len]),
        ])
        .map_err(Into::into)
    }

    fn parquet(path: &Path, revisions: &[i64]) -> Result<()> {
        path.parent().map(fs::create_dir_all).transpose()?;
        let mut frame = frame(revisions)?;
        let mut file = File::create(path)?;
        ParquetWriter::new(&mut file)
            .with_compression(ParquetCompression::Zstd(None))
            .finish(&mut frame)?;
        Ok(())
    }

    #[test]
    fn identity_ignores_filenames_and_fragment_boundaries_but_counts_duplicates() -> Result<()> {
        let root = TestDir::new()?;
        let first = root.path().join("first");
        parquet(&first.join("a.parquet"), &[1, 3])?;
        parquet(&first.join("b.parquet"), &[2, 4])?;
        let second = root.path().join("second");
        parquet(&second.join("renamed.parquet"), &[1, 2, 3, 4])?;
        let left = compute(
            "testwiki",
            "2024-02",
            vec![first.join("b.parquet"), first.join("a.parquet")],
        )?;
        let right = compute("testwiki", "2024-02", vec![second.join("renamed.parquet")])?;
        assert_eq!(left.digest, right.digest);
        assert_eq!(left.rows, 4);

        parquet(&second.join("duplicate.parquet"), &[4])?;
        let duplicate = compute(
            "testwiki",
            "2024-02",
            vec![
                second.join("renamed.parquet"),
                second.join("duplicate.parquet"),
            ],
        )?;
        assert_ne!(duplicate.digest, left.digest);
        assert_eq!(duplicate.rows, 5);
        Ok(())
    }

    #[test]
    fn rejects_unsorted_or_wrong_month_input() -> Result<()> {
        let root = TestDir::new()?;
        let unsorted = root.path().join("unsorted.parquet");
        parquet(&unsorted, &[2, 1])?;
        assert!(compute("testwiki", "2024-02", vec![unsorted]).is_err());
        let ordered = root.path().join("ordered.parquet");
        parquet(&ordered, &[1])?;
        assert!(compute("testwiki", "2024-03", vec![ordered]).is_err());
        Ok(())
    }

    #[test]
    fn receipt_is_atomic_and_strict() -> Result<()> {
        let root = TestDir::new()?;
        let artifact = root.path().join("month.parquet");
        parquet(&artifact, &[1])?;
        let identity = compute("testwiki", "2024-02", vec![artifact])?;
        let path = receipt_path(root.path(), "testwiki", "2026-08", "2024-02")?;
        write_receipt(&path, &identity)?;
        assert_eq!(read_receipt(&path)?, identity);
        fs::write(&path, b"{\"schema_version\":1}")?;
        assert!(read_receipt(&path).is_err());
        Ok(())
    }
}
