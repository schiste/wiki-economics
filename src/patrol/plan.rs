use super::{PATROL_DUMP_BASE, PatrolTransport};
use anyhow::{Context, Result, ensure};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use url::Url;

const PLAN_SCHEMA_VERSION: u32 = 1;
const STATUS_SCHEMA_VERSION: u32 = 1;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum PatrolSourceLayout {
    Recombined,
    Split,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PatrolSourceSpec {
    pub(super) source_id: String,
    pub(super) url: Url,
    pub(super) expected_size: u64,
    pub(super) md5: String,
    pub(super) sha1: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PatrolSourcePlan {
    pub(super) schema_version: u32,
    pub(super) wiki: String,
    pub(super) history_snapshot: String,
    pub(super) logging_dump_date: String,
    pub(super) coverage_through: String,
    pub(super) layout: PatrolSourceLayout,
    pub(super) dump_status_url: Url,
    pub(super) inventory_updated: String,
    pub(super) sources: Vec<PatrolSourceSpec>,
    pub(super) plan_sha256: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PatrolPlanStatus {
    pub(super) schema_version: u32,
    pub(super) wiki: String,
    pub(super) history_snapshot: String,
    pub(super) logging_dump_date: String,
    pub(super) state: String,
    pub(super) recombined_status: String,
    pub(super) split_status: String,
    pub(super) checked_at: String,
    pub(super) dump_status_url: String,
    pub(super) message: String,
}

#[derive(Debug)]
pub(super) struct UpstreamWaiting {
    pub(super) wiki: String,
    pub(super) history_snapshot: String,
    pub(super) logging_dump_date: String,
    pub(super) recombined_status: String,
    pub(super) split_status: String,
}

impl fmt::Display for UpstreamWaiting {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "UPSTREAM_WAITING: Wikimedia logging dump {} for {}/{} is not complete (recombined={}, split={}); validated history transactions remain reusable",
            self.logging_dump_date,
            self.wiki,
            self.history_snapshot,
            self.recombined_status,
            self.split_status
        )
    }
}

impl Error for UpstreamWaiting {}

impl PatrolSourcePlan {
    pub(super) fn load_or_resolve<T: PatrolTransport + ?Sized>(
        transport: &T,
        wiki: &str,
        snapshot: &str,
        data_dir: &Path,
    ) -> Result<Self> {
        validate_identity(wiki, snapshot)?;
        let path = plan_path(data_dir, wiki, snapshot)?;
        if path.is_file() {
            let plan = Self::load(&path, wiki, snapshot)?;
            clear_waiting_status(data_dir, wiki, snapshot)?;
            return Ok(plan);
        }
        let resolved = Self::resolve(transport, wiki, snapshot, data_dir)?;
        atomic_json(&path, &resolved)?;
        clear_waiting_status(data_dir, wiki, snapshot)?;
        Ok(resolved)
    }

    fn resolve<T: PatrolTransport + ?Sized>(
        transport: &T,
        wiki: &str,
        snapshot: &str,
        data_dir: &Path,
    ) -> Result<Self> {
        let logging_dump_date = required_logging_dump_date(snapshot)?;
        let status_url = Url::parse(&format!(
            "{PATROL_DUMP_BASE}/{wiki}/{logging_dump_date}/dumpstatus.json"
        ))?;
        let inventory = transport
            .get_json(status_url.as_str())
            .with_context(|| format!("unable to inspect patrol inventory {status_url}"))?;
        let jobs = inventory
            .get("jobs")
            .and_then(Value::as_object)
            .context("patrol dump status has no jobs object")?;
        let recombined = jobs.get("xmlpagelogsdumprecombine");
        let split = jobs.get("xmlpagelogsdump");
        let recombined_status = job_status(recombined);
        let split_status = job_status(split);

        let selected = if recombined_status == "done" {
            Some((
                PatrolSourceLayout::Recombined,
                parse_job_sources(recombined, wiki, &logging_dump_date)?,
            ))
        } else if split_status == "done" {
            let mut sources = parse_job_sources(split, wiki, &logging_dump_date)?;
            sources.sort_by_key(|source| {
                split_source_index(&source.source_id, wiki, &logging_dump_date)
                    .unwrap_or(usize::MAX)
            });
            Some((PatrolSourceLayout::Split, sources))
        } else {
            None
        };
        let Some((layout, sources)) = selected else {
            let waiting = UpstreamWaiting {
                wiki: wiki.to_string(),
                history_snapshot: snapshot.to_string(),
                logging_dump_date: logging_dump_date.clone(),
                recombined_status: recombined_status.clone(),
                split_status: split_status.clone(),
            };
            let status = PatrolPlanStatus {
                schema_version: STATUS_SCHEMA_VERSION,
                wiki: wiki.to_string(),
                history_snapshot: snapshot.to_string(),
                logging_dump_date,
                state: "waiting_upstream".to_string(),
                recombined_status,
                split_status,
                checked_at: Utc::now().to_rfc3339(),
                dump_status_url: status_url.to_string(),
                message: waiting.to_string(),
            };
            atomic_json(&status_path(data_dir, wiki, snapshot)?, &status)?;
            return Err(waiting.into());
        };

        ensure!(
            !sources.is_empty(),
            "completed patrol job has no source files"
        );
        let inventory_updated = match layout {
            PatrolSourceLayout::Recombined => job_updated(recombined),
            PatrolSourceLayout::Split => job_updated(split),
        };
        let mut plan = Self {
            schema_version: PLAN_SCHEMA_VERSION,
            wiki: wiki.to_string(),
            history_snapshot: snapshot.to_string(),
            logging_dump_date,
            coverage_through: snapshot.to_string(),
            layout,
            dump_status_url: status_url,
            inventory_updated,
            sources,
            plan_sha256: String::new(),
        };
        plan.plan_sha256 = plan.canonical_hash()?;
        plan.validate(wiki, snapshot)?;
        Ok(plan)
    }

    fn load(path: &Path, wiki: &str, snapshot: &str) -> Result<Self> {
        let plan: Self = serde_json::from_slice(&fs::read(path)?)
            .with_context(|| format!("invalid patrol source plan {}", path.display()))?;
        plan.validate(wiki, snapshot)?;
        Ok(plan)
    }

    pub(super) fn validate(&self, wiki: &str, snapshot: &str) -> Result<()> {
        validate_identity(wiki, snapshot)?;
        ensure!(
            self.schema_version == PLAN_SCHEMA_VERSION
                && self.wiki == wiki
                && self.history_snapshot == snapshot
                && self.logging_dump_date == required_logging_dump_date(snapshot)?
                && self.coverage_through == snapshot
                && self.plan_sha256 == self.canonical_hash()?,
            "patrol source plan identity changed"
        );
        ensure!(!self.sources.is_empty(), "patrol source plan is empty");
        for source in &self.sources {
            ensure!(source.expected_size > 0, "patrol source has a zero size");
            ensure!(
                source.md5.len() == 32 && source.md5.bytes().all(|byte| byte.is_ascii_hexdigit()),
                "patrol source has an invalid MD5"
            );
            ensure!(
                source.sha1.len() == 40 && source.sha1.bytes().all(|byte| byte.is_ascii_hexdigit()),
                "patrol source has an invalid SHA-1"
            );
            ensure!(
                source.url.scheme() == "https"
                    && source.url.host_str() == Some("dumps.wikimedia.org"),
                "patrol source is outside dumps.wikimedia.org"
            );
            ensure!(
                source
                    .url
                    .path()
                    .contains(&format!("/{wiki}/{}/", self.logging_dump_date)),
                "patrol source URL does not match the pinned dump"
            );
        }
        match self.layout {
            PatrolSourceLayout::Recombined => ensure!(
                self.sources.len() == 1
                    && self.sources[0].source_id
                        == format!("{wiki}-{}-pages-logging.xml.gz", self.logging_dump_date),
                "recombined patrol plan does not contain its one canonical source"
            ),
            PatrolSourceLayout::Split => {
                for (index, source) in self.sources.iter().enumerate() {
                    ensure!(
                        source.source_id
                            == format!(
                                "{wiki}-{}-pages-logging{}.xml.gz",
                                self.logging_dump_date,
                                index + 1
                            ),
                        "split patrol plan has a missing, duplicate, or non-contiguous source"
                    );
                }
            }
        }
        Ok(())
    }

    fn canonical_hash(&self) -> Result<String> {
        let mut canonical = self.clone();
        canonical.plan_sha256.clear();
        Ok(hex::encode(Sha256::digest(serde_json::to_vec(&canonical)?)))
    }
}

pub(super) fn is_upstream_waiting(error: &anyhow::Error) -> bool {
    error
        .chain()
        .any(|cause| cause.downcast_ref::<UpstreamWaiting>().is_some())
}

fn parse_job_sources(
    job: Option<&Value>,
    wiki: &str,
    dump_date: &str,
) -> Result<Vec<PatrolSourceSpec>> {
    let files = job
        .and_then(|job| job.get("files"))
        .and_then(Value::as_object)
        .context("completed patrol job has no files object")?;
    let mut sources = BTreeMap::new();
    for (name, value) in files {
        let size = value
            .get("size")
            .and_then(Value::as_u64)
            .context("completed patrol source has no size")?;
        let path = value
            .get("url")
            .and_then(Value::as_str)
            .context("completed patrol source has no URL")?;
        let md5 = value
            .get("md5")
            .and_then(Value::as_str)
            .context("completed patrol source has no MD5")?;
        let sha1 = value
            .get("sha1")
            .and_then(Value::as_str)
            .context("completed patrol source has no SHA-1")?;
        ensure!(
            path.ends_with(name),
            "patrol source filename does not match its URL"
        );
        ensure!(
            name.starts_with(&format!("{wiki}-{dump_date}-pages-logging"))
                && name.ends_with(".xml.gz"),
            "unexpected patrol source filename {name}"
        );
        let url = Url::parse(&format!("{PATROL_DUMP_BASE}{path}"))?;
        ensure!(
            sources
                .insert(
                    name.clone(),
                    PatrolSourceSpec {
                        source_id: name.clone(),
                        url,
                        expected_size: size,
                        md5: md5.to_string(),
                        sha1: sha1.to_string()
                    }
                )
                .is_none(),
            "duplicate patrol source {name}"
        );
    }
    Ok(sources.into_values().collect())
}

fn split_source_index(name: &str, wiki: &str, dump_date: &str) -> Option<usize> {
    let prefix = format!("{wiki}-{dump_date}-pages-logging");
    let suffix = name.strip_prefix(&prefix)?.strip_suffix(".xml.gz")?;
    let index = suffix.parse::<usize>().ok()?;
    (index > 0).then_some(index)
}

fn job_status(job: Option<&Value>) -> String {
    job.and_then(|job| job.get("status"))
        .and_then(Value::as_str)
        .unwrap_or("missing")
        .to_string()
}

fn job_updated(job: Option<&Value>) -> String {
    job.and_then(|job| job.get("updated"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string()
}

fn required_logging_dump_date(snapshot: &str) -> Result<String> {
    crate::storage::validate_snapshot_version(snapshot)?;
    let year: u32 = snapshot[..4].parse()?;
    let month: u32 = snapshot[5..].parse()?;
    let (year, month) = if month == 12 {
        (year + 1, 1)
    } else {
        (year, month + 1)
    };
    Ok(format!("{year:04}{month:02}01"))
}

fn validate_identity(wiki: &str, snapshot: &str) -> Result<()> {
    crate::snapshot_plan::WikiId::new(wiki)?;
    crate::storage::validate_snapshot_version(snapshot)
}

pub(super) fn plan_path(data_dir: &Path, wiki: &str, snapshot: &str) -> Result<PathBuf> {
    validate_identity(wiki, snapshot)?;
    Ok(data_dir
        .join("snapshots")
        .join(wiki)
        .join(snapshot)
        .join("patrol-source-plan.json"))
}

pub(super) fn status_path(data_dir: &Path, wiki: &str, snapshot: &str) -> Result<PathBuf> {
    validate_identity(wiki, snapshot)?;
    Ok(data_dir
        .join("snapshots")
        .join(wiki)
        .join(snapshot)
        .join("patrol-source-status.json"))
}

fn clear_waiting_status(data_dir: &Path, wiki: &str, snapshot: &str) -> Result<()> {
    let path = status_path(data_dir, wiki, snapshot)?;
    match fs::remove_file(&path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error)
            .with_context(|| format!("failed to clear patrol wait status {}", path.display())),
    }
}

fn atomic_json<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    let parent = path.parent().context("patrol source plan has no parent")?;
    fs::create_dir_all(parent)?;
    let temporary = path.with_extension(format!("json.{}.tmp", std::process::id()));
    let result = (|| -> Result<()> {
        let mut file = File::create(&temporary)?;
        serde_json::to_writer_pretty(&mut file, value)?;
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
