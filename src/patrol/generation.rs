use super::*;
use serde::de::DeserializeOwned;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fs::OpenOptions;
use std::io::{self, BufRead, BufWriter};
use std::marker::PhantomData;

const GENERATION_SCHEMA_VERSION: u32 = 2;
const ARTIFACT_ORDERING: &str = "timestamp-logical-fields-v1";
#[cfg(not(test))]
const EXTERNAL_SORT_BATCH_ROWS: usize = PARQUET_BATCH_ROWS;
#[cfg(test)]
const EXTERNAL_SORT_BATCH_ROWS: usize = 2;
#[cfg(not(test))]
const EXTERNAL_SORT_FAN_IN: usize = 16;
#[cfg(test)]
const EXTERNAL_SORT_FAN_IN: usize = 2;
const NFS_DIRECTORY_REMOVE_ATTEMPTS: usize = 6;

#[derive(Debug, Serialize)]
struct CurrentPatrolGeneration {
    schema_version: u32,
    wiki: String,
    snapshot: String,
    parser_version: String,
    manifest_relative_path: String,
    manifest_sha256: String,
    manifest_file_sha256: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct MonthArtifact {
    pub(super) event_month: String,
    pub(super) relative_path: String,
    pub(super) artifact_sha256: String,
    pub(super) bytes: u64,
    pub(super) rows: u64,
    pub(super) observed_modified_unix_nanos: u128,
    pub(super) ordering_contract: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PatrolGeneration {
    schema_version: u32,
    pub(super) wiki: String,
    pub(super) snapshot: String,
    pub(super) parser_version: String,
    pub(super) source: LoggingSourceIdentity,
    pub(super) stats: LoggingParseStats,
    pub(super) autopatrol_groups: Vec<String>,
    pub(super) patrol_months: Vec<MonthArtifact>,
    pub(super) rights_months: Vec<MonthArtifact>,
    pub(super) rights_timeline_digest: String,
    pub(super) manifest_sha256: String,
}

impl PatrolGeneration {
    fn canonical_hash(&self) -> Result<String> {
        let mut canonical = self.clone();
        canonical.manifest_sha256.clear();
        Ok(hex::encode(Sha256::digest(serde_json::to_vec(&canonical)?)))
    }
}

struct ActiveMonthSpool {
    month: String,
    writer: BufWriter<File>,
}

struct MonthlySpool<T> {
    root: PathBuf,
    kind: &'static str,
    active: Option<ActiveMonthSpool>,
    months: BTreeSet<String>,
    row_type: PhantomData<T>,
}

pub(super) struct MonthlyPatrolWriter {
    spool: MonthlySpool<PatrolRow>,
}

pub(super) struct MonthlyRightsWriter {
    spool: MonthlySpool<RightsRow>,
}

impl<T: Serialize> MonthlySpool<T> {
    fn new(root: &Path, kind: &'static str) -> Self {
        Self {
            root: root.to_path_buf(),
            kind,
            active: None,
            months: BTreeSet::new(),
            row_type: PhantomData,
        }
    }

    fn add(&mut self, month: &str, row: &T) -> Result<()> {
        if self
            .active
            .as_ref()
            .is_some_and(|active| active.month == month)
        {
            return self.write(row);
        }
        self.finish_active()?;
        let path = spool_path(&self.root, self.kind, month)?;
        ensure_parent_dir(&path)?;
        let writer = BufWriter::new(OpenOptions::new().create(true).append(true).open(&path)?);
        self.months.insert(month.to_string());
        self.active = Some(ActiveMonthSpool {
            month: month.to_string(),
            writer,
        });
        self.write(row)
    }

    fn write(&mut self, row: &T) -> Result<()> {
        let writer = &mut self
            .active
            .as_mut()
            .context("monthly spool was not initialized")?
            .writer;
        serde_json::to_writer(&mut *writer, row)?;
        writer.write_all(b"\n")?;
        Ok(())
    }

    fn finish_active(&mut self) -> Result<()> {
        let Some(mut active) = self.active.take() else {
            return Ok(());
        };
        active.writer.flush()?;
        active.writer.get_ref().sync_all()?;
        let bytes = active.writer.get_ref().metadata()?.len();
        storage::discard_file_cache(active.writer.get_ref(), 0, bytes);
        Ok(())
    }

    fn finish(mut self) -> Result<Vec<(String, PathBuf)>> {
        self.finish_active()?;
        self.months
            .into_iter()
            .map(|month| {
                let path = spool_path(&self.root, self.kind, &month)?;
                Ok((month, path))
            })
            .collect()
    }
}

impl MonthlyPatrolWriter {
    fn new(root: &Path) -> Self {
        Self {
            spool: MonthlySpool::new(root, "patrol"),
        }
    }

    fn finish(self) -> Result<Vec<PathBuf>> {
        let root = self.spool.root.clone();
        let months = self.spool.finish()?;
        let mut completed = Vec::with_capacity(months.len());
        for (month, spool) in months {
            let final_path = month_path(&root, "patrol", &month)?;
            ensure_parent_dir(&final_path)?;
            let temporary = final_path.with_extension("parquet.tmp");
            let _ = fs::remove_file(&temporary);
            let mut writer = PatrolWriter::new(&temporary)?;
            external_sort_spool::<PatrolRow, _>(&spool, |row| writer.add(row))?;
            writer.finish()?;
            commit_month_artifact(&temporary, &final_path)?;
            storage::discard_path_cache(&final_path);
            completed.push(final_path);
        }
        remove_spool_tree(&root, "patrol")?;
        Ok(completed)
    }
}

impl PatrolSink for MonthlyPatrolWriter {
    fn add_patrol(&mut self, row: PatrolRow) -> Result<()> {
        let month = event_month(&row.timestamp)?;
        self.spool.add(&month, &row)
    }
}

impl MonthlyRightsWriter {
    fn new(root: &Path) -> Self {
        Self {
            spool: MonthlySpool::new(root, "rights"),
        }
    }

    fn finish(self) -> Result<Vec<PathBuf>> {
        let root = self.spool.root.clone();
        let months = self.spool.finish()?;
        let mut completed = Vec::with_capacity(months.len());
        for (month, spool) in months {
            let final_path = month_path(&root, "rights", &month)?;
            ensure_parent_dir(&final_path)?;
            let temporary = final_path.with_extension("parquet.tmp");
            let _ = fs::remove_file(&temporary);
            let mut writer = RightsWriter::new(&temporary)?;
            external_sort_spool::<RightsRow, _>(&spool, |row| writer.add(row))?;
            writer.finish()?;
            commit_month_artifact(&temporary, &final_path)?;
            storage::discard_path_cache(&final_path);
            completed.push(final_path);
        }
        remove_spool_tree(&root, "rights")?;
        Ok(completed)
    }
}

impl RightsSink for MonthlyRightsWriter {
    fn add_rights(&mut self, row: RightsRow) -> Result<()> {
        let month = event_month(&row.timestamp)?;
        self.spool.add(&month, &row)
    }
}

fn spool_path(root: &Path, kind: &str, month: &str) -> Result<PathBuf> {
    month_path(&root.join(".spool"), kind, month).map(|path| path.with_extension("jsonl"))
}

fn remove_spool_tree(root: &Path, kind: &str) -> Result<()> {
    let path = root.join(".spool").join(kind);
    if path.exists() {
        remove_directory_tree(&path)?;
    }
    let spool_root = root.join(".spool");
    if spool_root.is_dir() && fs::read_dir(&spool_root)?.next().is_none() {
        remove_directory_tree(&spool_root)?;
    }
    Ok(())
}

fn remove_directory_tree(path: &Path) -> Result<()> {
    let context = format!(
        "failed to remove committed patrol scratch directory {}",
        path.display()
    );
    retry_nfs_directory_remove(|| fs::remove_dir_all(path)).context(context)
}

fn retry_nfs_directory_remove<F>(mut remove: F) -> io::Result<()>
where
    F: FnMut() -> io::Result<()>,
{
    let mut attempt = 0_usize;
    loop {
        match remove() {
            Ok(()) => return Ok(()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(error)
                if error.kind() == io::ErrorKind::DirectoryNotEmpty
                    && attempt + 1 < NFS_DIRECTORY_REMOVE_ATTEMPTS =>
            {
                #[cfg(not(test))]
                std::thread::sleep(std::time::Duration::from_millis(25_u64 << attempt.min(4)));
                #[cfg(test)]
                std::thread::yield_now();
                attempt += 1;
            }
            Err(error) => return Err(error),
        }
    }
}

fn commit_month_artifact(temporary: &Path, final_path: &Path) -> Result<()> {
    File::open(temporary)?.sync_all()?;
    fs::rename(temporary, final_path)?;
    let parent = final_path
        .parent()
        .expect("constructed patrol month always has a parent");
    File::open(parent)?.sync_all()?;
    Ok(())
}

fn external_sort_spool<T, F>(spool: &Path, mut emit: F) -> Result<()>
where
    T: DeserializeOwned + Ord + Serialize,
    F: FnMut(T) -> Result<()>,
{
    let work = spool.with_extension("sort");
    let _ = fs::remove_dir_all(&work);
    fs::create_dir(&work)?;
    let result = (|| {
        let mut runs = create_sorted_runs::<T>(spool, &work)?;
        storage::discard_path_cache(spool);
        let mut round = 0_usize;
        while runs.len() > EXTERNAL_SORT_FAN_IN {
            let round_root = work.join(format!("round-{round:04}"));
            fs::create_dir(&round_root)?;
            let mut next = Vec::new();
            for (index, group) in runs.chunks(EXTERNAL_SORT_FAN_IN).enumerate() {
                let path = round_root.join(format!("run-{index:08}.jsonl"));
                merge_runs_to_json::<T>(group, &path)?;
                next.push(path);
            }
            for path in runs {
                storage::discard_path_cache(&path);
                fs::remove_file(path)?;
            }
            runs = next;
            round += 1;
        }
        merge_sorted_runs::<T, _>(&runs, &mut emit)?;
        for path in &runs {
            storage::discard_path_cache(path);
        }
        fs::remove_file(spool)?;
        Ok::<_, anyhow::Error>(())
    })();
    let cleanup = remove_directory_tree(&work);
    result?;
    cleanup?;
    Ok(())
}

fn create_sorted_runs<T>(spool: &Path, work: &Path) -> Result<Vec<PathBuf>>
where
    T: DeserializeOwned + Ord + Serialize,
{
    let mut reader = BufReader::new(File::open(spool)?);
    let mut line = String::new();
    let mut rows: Vec<T> = Vec::with_capacity(EXTERNAL_SORT_BATCH_ROWS);
    let mut runs = Vec::new();
    loop {
        line.clear();
        let read = reader.read_line(&mut line)?;
        if read == 0 {
            break;
        }
        rows.push(serde_json::from_str(line.trim_end())?);
        if rows.len() == EXTERNAL_SORT_BATCH_ROWS {
            write_sorted_run(&mut rows, work, runs.len(), &mut runs)?;
        }
    }
    if !rows.is_empty() {
        write_sorted_run(&mut rows, work, runs.len(), &mut runs)?;
    }
    anyhow::ensure!(!runs.is_empty(), "monthly patrol spool is empty");
    Ok(runs)
}

fn write_sorted_run<T: Ord + Serialize>(
    rows: &mut Vec<T>,
    work: &Path,
    index: usize,
    runs: &mut Vec<PathBuf>,
) -> Result<()> {
    rows.sort();
    let path = work.join(format!("run-{index:08}.jsonl"));
    let mut writer = BufWriter::new(File::create(&path)?);
    for row in rows.drain(..) {
        serde_json::to_writer(&mut writer, &row)?;
        writer.write_all(b"\n")?;
    }
    writer.flush()?;
    writer.get_ref().sync_all()?;
    let bytes = writer.get_ref().metadata()?.len();
    storage::discard_file_cache(writer.get_ref(), 0, bytes);
    runs.push(path);
    Ok(())
}

fn merge_runs_to_json<T>(runs: &[PathBuf], output: &Path) -> Result<()>
where
    T: DeserializeOwned + Ord + Serialize,
{
    let mut writer = BufWriter::new(File::create(output)?);
    merge_sorted_runs::<T, _>(runs, |row| {
        serde_json::to_writer(&mut writer, &row)?;
        writer.write_all(b"\n")?;
        Ok(())
    })?;
    writer.flush()?;
    writer.get_ref().sync_all()?;
    let bytes = writer.get_ref().metadata()?.len();
    storage::discard_file_cache(writer.get_ref(), 0, bytes);
    Ok(())
}

fn merge_sorted_runs<T, F>(runs: &[PathBuf], mut emit: F) -> Result<()>
where
    T: DeserializeOwned + Ord,
    F: FnMut(T) -> Result<()>,
{
    let mut readers = runs
        .iter()
        .map(|path| File::open(path).map(BufReader::new))
        .collect::<std::io::Result<Vec<_>>>()?;
    let mut heap = BinaryHeap::new();
    for (index, reader) in readers.iter_mut().enumerate() {
        if let Some(row) = read_spooled_row(reader)? {
            heap.push(Reverse((row, index)));
        }
    }
    while let Some(Reverse((row, index))) = heap.pop() {
        emit(row)?;
        if let Some(next) = read_spooled_row(&mut readers[index])? {
            heap.push(Reverse((next, index)));
        }
    }
    Ok(())
}

fn read_spooled_row<T: DeserializeOwned>(reader: &mut BufReader<File>) -> Result<Option<T>> {
    let mut line = String::new();
    if reader.read_line(&mut line)? == 0 {
        return Ok(None);
    }
    Ok(Some(serde_json::from_str(line.trim_end())?))
}

pub(super) fn generation_dir(data_dir: &Path, wiki: &str, snapshot: &str) -> Result<PathBuf> {
    storage::validate_snapshot_version(snapshot)?;
    anyhow::ensure!(
        !wiki.is_empty()
            && wiki
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_')),
        "unsafe patrol generation wiki"
    );
    Ok(data_dir
        .join("patrol")
        .join(wiki)
        .join("generations")
        .join(snapshot)
        .join(hex::encode(Sha256::digest(
            PATROL_PARSER_VERSION.as_bytes(),
        ))))
}

pub(super) fn manifest_path(data_dir: &Path, wiki: &str, snapshot: &str) -> Result<PathBuf> {
    Ok(generation_dir(data_dir, wiki, snapshot)?.join("generation.json"))
}

pub(super) fn exists(data_dir: &Path, wiki: &str, snapshot: &str) -> bool {
    manifest_path(data_dir, wiki, snapshot).is_ok_and(|path| path.is_file())
}

pub(super) fn load(data_dir: &Path, wiki: &str, snapshot: &str) -> Result<PatrolGeneration> {
    let root = generation_dir(data_dir, wiki, snapshot)?;
    let path = root.join("generation.json");
    let generation: PatrolGeneration = serde_json::from_slice(&fs::read(&path)?)
        .with_context(|| format!("invalid patrol generation manifest {}", path.display()))?;
    validate(&root, wiki, snapshot, &generation)?;
    Ok(generation)
}

pub(super) fn artifact_path(root: &Path, artifact: &MonthArtifact) -> Result<PathBuf> {
    checked_artifact_path(root, &artifact.relative_path)
}

pub(super) fn tracked_inputs(
    data_dir: &Path,
    wiki: &str,
    snapshot: &str,
) -> Result<Vec<fingerprint::TrackedPath>> {
    let root = generation_dir(data_dir, wiki, snapshot)?;
    load(data_dir, wiki, snapshot)?;
    Ok(vec![fingerprint::TrackedPath::new(
        format!("patrol-generation/{wiki}/{snapshot}/manifest"),
        root.join("generation.json"),
    )])
}

pub(super) fn fetch<T: PatrolTransport + ?Sized>(
    transport: &T,
    wiki: &str,
    snapshot: &str,
    data_dir: &Path,
) -> Result<PatrolGeneration> {
    let final_root = generation_dir(data_dir, wiki, snapshot)?;
    if final_root.join("generation.json").is_file() {
        let generation = load(data_dir, wiki, snapshot)?;
        publish_current_pointer(data_dir, wiki, &generation)?;
        return Ok(generation);
    }
    anyhow::ensure!(
        !final_root.exists(),
        "incomplete patrol generation already exists: {}",
        final_root.display()
    );
    let generations = final_root
        .parent()
        .context("patrol generation has no generations root")?;
    fs::create_dir_all(generations)?;
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_nanos();
    let staging = generations.join(format!(".{snapshot}.{}.{}.tmp", std::process::id(), nonce));
    fs::create_dir(&staging)?;
    let result = build_generation(transport, wiki, snapshot, data_dir, &staging);
    let generation = match result {
        Ok(generation) => generation,
        Err(error) => {
            let _ = remove_directory_tree(&staging);
            return Err(error);
        }
    };
    let publish_context = format!(
        "failed to publish patrol generation {} as {}",
        staging.display(),
        final_root.display()
    );
    fs::rename(&staging, &final_root).context(publish_context)?;
    File::open(generations)?.sync_all()?;
    validate(&final_root, wiki, snapshot, &generation)?;
    publish_current_pointer(data_dir, wiki, &generation)?;
    Ok(generation)
}

fn publish_current_pointer(
    data_dir: &Path,
    wiki: &str,
    generation: &PatrolGeneration,
) -> Result<()> {
    let parser_identity = hex::encode(Sha256::digest(PATROL_PARSER_VERSION.as_bytes()));
    let manifest_path =
        generation_dir(data_dir, wiki, &generation.snapshot)?.join("generation.json");
    let (_, manifest_file_sha256) = storage::sha256_file(&manifest_path)?;
    let pointer = CurrentPatrolGeneration {
        schema_version: 1,
        wiki: wiki.to_string(),
        snapshot: generation.snapshot.clone(),
        parser_version: generation.parser_version.clone(),
        manifest_relative_path: format!(
            "generations/{}/{parser_identity}/generation.json",
            generation.snapshot
        ),
        manifest_sha256: generation.manifest_sha256.clone(),
        manifest_file_sha256,
    };
    atomic_json(
        &data_dir
            .join("patrol")
            .join(wiki)
            .join("current-generation.json"),
        &pointer,
    )
}

fn build_generation<T: PatrolTransport + ?Sized>(
    transport: &T,
    wiki: &str,
    snapshot: &str,
    data_dir: &Path,
    staging: &Path,
) -> Result<PatrolGeneration> {
    let source_path = staging.join("source.xml.gz");
    let source = download_logging_dump(transport, wiki, &source_path)?;
    let legacy_meta = data_dir
        .join("patrol")
        .join(wiki)
        .join("autopatrol_groups.json");
    let mut autopatrol_groups = fetch_autopatrol_groups(transport, wiki)?;
    if autopatrol_groups.is_empty() {
        autopatrol_groups = load_cached_autopatrol_groups(&legacy_meta)?;
    }
    autopatrol_groups.sort();
    autopatrol_groups.dedup();

    let mut patrol_writer = MonthlyPatrolWriter::new(staging);
    let mut rights_writer = MonthlyRightsWriter::new(staging);
    let stats = parse_logging_events(&source_path, &mut patrol_writer, &mut rights_writer)?;
    validate_logging_parse(&source_path, stats)?;
    let patrol_files = patrol_writer.finish()?;
    let rights_files = rights_writer.finish()?;
    anyhow::ensure!(
        stats.patrol_events == 0 || !patrol_files.is_empty(),
        "patrol events were parsed without monthly artifacts"
    );
    anyhow::ensure!(
        stats.rights_events == 0 || !rights_files.is_empty(),
        "rights events were parsed without monthly artifacts"
    );
    let patrol_months = artifact_receipts(staging, &patrol_files)?;
    let rights_months = artifact_receipts(staging, &rights_files)?;
    let rights_timeline_digest = timeline_digest(&rights_months);
    let mut generation = PatrolGeneration {
        schema_version: GENERATION_SCHEMA_VERSION,
        wiki: wiki.to_string(),
        snapshot: snapshot.to_string(),
        parser_version: PATROL_PARSER_VERSION.to_string(),
        source,
        stats,
        autopatrol_groups,
        patrol_months,
        rights_months,
        rights_timeline_digest,
        manifest_sha256: String::new(),
    };
    generation.manifest_sha256 = generation.canonical_hash()?;
    validate(staging, wiki, snapshot, &generation)?;
    atomic_json(&staging.join("generation.json"), &generation)?;
    fs::remove_file(&source_path).context("failed to release committed patrol logging source")?;
    File::open(staging)?.sync_all()?;
    Ok(generation)
}

fn validate(root: &Path, wiki: &str, snapshot: &str, generation: &PatrolGeneration) -> Result<()> {
    anyhow::ensure!(
        generation.schema_version == GENERATION_SCHEMA_VERSION
            && generation.wiki == wiki
            && generation.snapshot == snapshot
            && generation.parser_version == PATROL_PARSER_VERSION
            && generation.stats.total_log_items
                == generation.stats.patrol_events
                    + generation.stats.rights_events
                    + generation.stats.skipped_events
            && generation.source.downloaded_sha256.len() == 64
            && generation
                .source
                .downloaded_sha256
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
            && generation.manifest_sha256 == generation.canonical_hash()?,
        "patrol generation identity changed"
    );
    validate_artifacts(root, &generation.patrol_months)?;
    validate_artifacts(root, &generation.rights_months)?;
    anyhow::ensure!(
        generation.rights_timeline_digest == timeline_digest(&generation.rights_months),
        "patrol rights timeline identity changed"
    );
    Ok(())
}

fn validate_artifacts(root: &Path, artifacts: &[MonthArtifact]) -> Result<()> {
    let mut previous = None;
    for artifact in artifacts {
        anyhow::ensure!(
            artifact.ordering_contract == ARTIFACT_ORDERING
                && artifact.rows > 0
                && previous
                    .as_deref()
                    .is_none_or(|month| month < artifact.event_month.as_str()),
            "patrol monthly artifact inventory is invalid"
        );
        let path = checked_artifact_path(root, &artifact.relative_path)?;
        let metadata = fs::metadata(&path)?;
        anyhow::ensure!(
            metadata.is_file() && metadata.len() == artifact.bytes,
            "patrol monthly artifact size changed"
        );
        if modified_nanos(&metadata)? != artifact.observed_modified_unix_nanos {
            let (_, sha256) = storage::sha256_file(&path)?;
            anyhow::ensure!(
                sha256 == artifact.artifact_sha256,
                "patrol monthly artifact identity changed"
            );
            let rows = ParquetReader::new(File::open(&path)?).num_rows()?;
            anyhow::ensure!(
                u64::try_from(rows)? == artifact.rows,
                "patrol monthly row count changed"
            );
        }
        previous = Some(artifact.event_month.clone());
    }
    Ok(())
}

fn artifact_receipts(root: &Path, paths: &[PathBuf]) -> Result<Vec<MonthArtifact>> {
    let mut artifacts = Vec::with_capacity(paths.len());
    for path in paths {
        let event_month = path
            .parent()
            .and_then(Path::file_name)
            .and_then(|value| value.to_str())
            .and_then(|value| value.strip_prefix("month="))
            .context("monthly patrol artifact has no month partition")?
            .to_string();
        let relative_path = path
            .strip_prefix(root)?
            .to_str()
            .context("monthly patrol artifact path is not UTF-8")?
            .to_string();
        let metadata = fs::metadata(path)?;
        let (bytes, artifact_sha256) = storage::sha256_file(path)?;
        let rows = ParquetReader::new(File::open(path)?).num_rows()?;
        artifacts.push(MonthArtifact {
            event_month,
            relative_path,
            artifact_sha256,
            bytes,
            rows: u64::try_from(rows)?,
            observed_modified_unix_nanos: modified_nanos(&metadata)?,
            ordering_contract: ARTIFACT_ORDERING.to_string(),
        });
    }
    artifacts.sort_by(|left, right| left.event_month.cmp(&right.event_month));
    Ok(artifacts)
}

fn checked_artifact_path(root: &Path, relative: &str) -> Result<PathBuf> {
    let path = Path::new(relative);
    anyhow::ensure!(
        !relative.is_empty()
            && path
                .components()
                .all(|component| matches!(component, std::path::Component::Normal(_))),
        "unsafe patrol generation artifact path"
    );
    Ok(root.join(path))
}

fn timeline_digest(artifacts: &[MonthArtifact]) -> String {
    let mut digest = Sha256::new();
    digest.update(b"wiki-economics\0patrol-rights-timeline\0v1\0");
    for artifact in artifacts {
        update_string(&mut digest, &artifact.event_month);
        update_string(&mut digest, &artifact.artifact_sha256);
        digest.update(artifact.rows.to_be_bytes());
    }
    hex::encode(digest.finalize())
}

fn update_string(digest: &mut Sha256, value: &str) {
    digest.update((value.len() as u64).to_be_bytes());
    digest.update(value.as_bytes());
}

fn modified_nanos(metadata: &fs::Metadata) -> Result<u128> {
    Ok(metadata
        .modified()?
        .duration_since(std::time::UNIX_EPOCH)
        .context("patrol monthly artifact has a pre-epoch modification time")?
        .as_nanos())
}

fn month_path(root: &Path, kind: &str, month: &str) -> Result<PathBuf> {
    let (year, month_number) = month
        .split_once('-')
        .context("event month is not YYYY-MM")?;
    anyhow::ensure!(
        year.len() == 4
            && month_number.len() == 2
            && year.bytes().all(|byte| byte.is_ascii_digit())
            && month_number.bytes().all(|byte| byte.is_ascii_digit())
            && (1..=12).contains(&month_number.parse::<u8>()?),
        "event month is not YYYY-MM"
    );
    Ok(root
        .join(kind)
        .join(format!("year={year}"))
        .join(format!("month={month}"))
        .join("part-00000.parquet"))
}

fn event_month(timestamp: &str) -> Result<String> {
    let month = timestamp
        .get(..7)
        .context("logging event timestamp has no event month")?;
    month_path(Path::new("."), "validate", month)?;
    Ok(month.to_string())
}

pub(super) fn atomic_json<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    let parent = path
        .parent()
        .context("patrol generation receipt has no parent")?;
    fs::create_dir_all(parent)?;
    let temporary = path.with_extension(format!("json.{}.tmp", std::process::id()));
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
mod directory_remove_tests {
    use super::*;
    use std::cell::Cell;

    #[test]
    fn nfs_directory_remove_retries_only_transient_nonempty_errors() {
        let attempts = Cell::new(0_usize);
        retry_nfs_directory_remove(|| {
            let attempt = attempts.get();
            attempts.set(attempt + 1);
            if attempt < 2 {
                Err(io::Error::from(io::ErrorKind::DirectoryNotEmpty))
            } else {
                Ok(())
            }
        })
        .expect("a transient NFS directory state should be retried");
        assert_eq!(attempts.get(), 3);

        retry_nfs_directory_remove(|| Err(io::Error::from(io::ErrorKind::NotFound)))
            .expect("an already removed scratch directory is successful");

        let other = retry_nfs_directory_remove(|| {
            Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "not retryable",
            ))
        })
        .expect_err("unrelated filesystem failures must remain fail-closed");
        assert_eq!(other.kind(), io::ErrorKind::PermissionDenied);

        let persistent = Cell::new(0_usize);
        let error = retry_nfs_directory_remove(|| {
            persistent.set(persistent.get() + 1);
            Err(io::Error::from(io::ErrorKind::DirectoryNotEmpty))
        })
        .expect_err("persistent non-empty scratch must not be hidden");
        assert_eq!(error.kind(), io::ErrorKind::DirectoryNotEmpty);
        assert_eq!(persistent.get(), NFS_DIRECTORY_REMOVE_ATTEMPTS);
    }

    #[test]
    fn patrol_scratch_tree_removal_is_idempotent() -> Result<()> {
        let root = crate::test_support::TestDir::new()?;
        let scratch = root.path().join("nested/scratch");
        fs::create_dir_all(scratch.join("child"))?;
        fs::write(scratch.join("child/part"), b"scratch")?;

        remove_directory_tree(&scratch)?;
        assert!(!scratch.exists());
        remove_directory_tree(&scratch)?;
        Ok(())
    }
}
