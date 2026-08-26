use super::*;

const GENERATION_SCHEMA_VERSION: u32 = 1;
const ARTIFACT_ORDERING: &str = "timestamp-logical-fields-v1";

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct MonthArtifact {
    pub(super) event_month: String,
    pub(super) relative_path: String,
    pub(super) artifact_sha256: String,
    pub(super) bytes: u64,
    pub(super) rows: u64,
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
}

struct ActivePatrolMonth {
    month: String,
    temporary: PathBuf,
    final_path: PathBuf,
    writer: PatrolWriter,
}

struct ActiveRightsMonth {
    month: String,
    temporary: PathBuf,
    final_path: PathBuf,
    writer: RightsWriter,
}

pub(super) struct MonthlyPatrolWriter {
    root: PathBuf,
    active: Option<ActivePatrolMonth>,
    completed: Vec<PathBuf>,
}

pub(super) struct MonthlyRightsWriter {
    root: PathBuf,
    active: Option<ActiveRightsMonth>,
    completed: Vec<PathBuf>,
}

impl MonthlyPatrolWriter {
    fn new(root: &Path) -> Self {
        Self {
            root: root.to_path_buf(),
            active: None,
            completed: Vec::new(),
        }
    }

    fn rotate(&mut self, month: &str) -> Result<()> {
        if self
            .active
            .as_ref()
            .is_some_and(|active| active.month == month)
        {
            return Ok(());
        }
        if let Some(active) = self.active.as_ref() {
            anyhow::ensure!(
                active.month.as_str() < month,
                "patrol logging source is not ordered by event month"
            );
        }
        self.finish_active()?;
        let final_path = month_path(&self.root, "patrol", month)?;
        ensure_parent_dir(&final_path)?;
        let temporary = final_path.with_extension("parquet.tmp");
        let _ = fs::remove_file(&temporary);
        let writer = PatrolWriter::new(&temporary)?;
        self.active = Some(ActivePatrolMonth {
            month: month.to_string(),
            temporary,
            final_path,
            writer,
        });
        Ok(())
    }

    fn finish_active(&mut self) -> Result<()> {
        let Some(active) = self.active.take() else {
            return Ok(());
        };
        active.writer.finish()?;
        File::open(&active.temporary)?.sync_all()?;
        fs::rename(&active.temporary, &active.final_path)?;
        File::open(
            active
                .final_path
                .parent()
                .context("patrol month has no parent")?,
        )?
        .sync_all()?;
        self.completed.push(active.final_path);
        Ok(())
    }

    fn finish(mut self) -> Result<Vec<PathBuf>> {
        self.finish_active()?;
        Ok(self.completed)
    }
}

impl PatrolSink for MonthlyPatrolWriter {
    fn add_patrol(&mut self, row: PatrolRow) -> Result<()> {
        let month = event_month(&row.timestamp)?;
        self.rotate(&month)?;
        self.active
            .as_mut()
            .context("patrol month writer was not initialized")?
            .writer
            .add(row)
    }
}

impl MonthlyRightsWriter {
    fn new(root: &Path) -> Self {
        Self {
            root: root.to_path_buf(),
            active: None,
            completed: Vec::new(),
        }
    }

    fn rotate(&mut self, month: &str) -> Result<()> {
        if self
            .active
            .as_ref()
            .is_some_and(|active| active.month == month)
        {
            return Ok(());
        }
        if let Some(active) = self.active.as_ref() {
            anyhow::ensure!(
                active.month.as_str() < month,
                "rights logging source is not ordered by event month"
            );
        }
        self.finish_active()?;
        let final_path = month_path(&self.root, "rights", month)?;
        ensure_parent_dir(&final_path)?;
        let temporary = final_path.with_extension("parquet.tmp");
        let _ = fs::remove_file(&temporary);
        let writer = RightsWriter::new(&temporary)?;
        self.active = Some(ActiveRightsMonth {
            month: month.to_string(),
            temporary,
            final_path,
            writer,
        });
        Ok(())
    }

    fn finish_active(&mut self) -> Result<()> {
        let Some(active) = self.active.take() else {
            return Ok(());
        };
        active.writer.finish()?;
        File::open(&active.temporary)?.sync_all()?;
        fs::rename(&active.temporary, &active.final_path)?;
        File::open(
            active
                .final_path
                .parent()
                .context("rights month has no parent")?,
        )?
        .sync_all()?;
        self.completed.push(active.final_path);
        Ok(())
    }

    fn finish(mut self) -> Result<Vec<PathBuf>> {
        self.finish_active()?;
        Ok(self.completed)
    }
}

impl RightsSink for MonthlyRightsWriter {
    fn add_rights(&mut self, row: RightsRow) -> Result<()> {
        let month = event_month(&row.timestamp)?;
        self.rotate(&month)?;
        self.active
            .as_mut()
            .context("rights month writer was not initialized")?
            .writer
            .add(row)
    }
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
        .join(snapshot))
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

pub(super) fn fetch<T: PatrolTransport + ?Sized>(
    transport: &T,
    wiki: &str,
    snapshot: &str,
    data_dir: &Path,
) -> Result<PatrolGeneration> {
    let final_root = generation_dir(data_dir, wiki, snapshot)?;
    if final_root.join("generation.json").is_file() {
        return load(data_dir, wiki, snapshot);
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
    let result = build_generation(transport, wiki, snapshot, &staging);
    let generation = match result {
        Ok(generation) => generation,
        Err(error) => {
            let _ = fs::remove_dir_all(&staging);
            return Err(error);
        }
    };
    fs::rename(&staging, &final_root)?;
    File::open(generations)?.sync_all()?;
    validate(&final_root, wiki, snapshot, &generation)?;
    Ok(generation)
}

fn build_generation<T: PatrolTransport + ?Sized>(
    transport: &T,
    wiki: &str,
    snapshot: &str,
    staging: &Path,
) -> Result<PatrolGeneration> {
    let source_path = staging.join("source.xml.gz");
    let source = download_logging_dump(transport, wiki, &source_path)?;
    let legacy_meta = staging
        .parent()
        .and_then(Path::parent)
        .context("patrol staging path has no wiki root")?
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
    let generation = PatrolGeneration {
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
    };
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
                .all(|byte| byte.is_ascii_hexdigit()),
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
        let (bytes, sha256) = storage::sha256_file(&path)?;
        anyhow::ensure!(
            bytes == artifact.bytes && sha256 == artifact.artifact_sha256,
            "patrol monthly artifact identity changed"
        );
        let rows = ParquetReader::new(File::open(&path)?).num_rows()?;
        anyhow::ensure!(
            u64::try_from(rows)? == artifact.rows,
            "patrol monthly row count changed"
        );
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
        let (bytes, artifact_sha256) = storage::sha256_file(path)?;
        let rows = ParquetReader::new(File::open(path)?).num_rows()?;
        artifacts.push(MonthArtifact {
            event_month,
            relative_path,
            artifact_sha256,
            bytes,
            rows: u64::try_from(rows)?,
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

fn atomic_json<T: Serialize>(path: &Path, value: &T) -> Result<()> {
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
