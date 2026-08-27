use anyhow::{Context, Result, ensure};
use clap::ValueEnum;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::snapshot_plan::{SnapshotPlan, SourceLayout};
use crate::workload_profile::{self, WorkloadProfileName};

const TASK_SCHEMA_VERSION: u32 = 1;
const LEASE_SCHEMA_VERSION: u32 = 1;
const NOTIFICATION_SCHEMA_VERSION: u32 = 1;
const QUEUE_ALGORITHM_VERSION: &str = "fleet-queue-v1";
const DEFAULT_MAX_ATTEMPTS: u32 = 3;
const MEDIUM_MEMORY_THRESHOLD_BYTES: u64 = 3 * 512 * 1024 * 1024;
const MEDIUM_SCRATCH_THRESHOLD_BYTES: u64 = 4 * 1024 * 1024 * 1024;
const MEDIUM_FRAGMENT_THRESHOLD: u64 = 2_048;
const MAX_ERROR_BYTES: usize = 500;

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ResourceClass {
    Small,
    MediumLarge,
    Isolated,
}

impl ResourceClass {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Small => "small",
            Self::MediumLarge => "medium_large",
            Self::Isolated => "isolated",
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SchedulingSignals {
    pub(crate) source_layout: SourceLayout,
    pub(crate) source_count: usize,
    pub(crate) compressed_source_bytes: Option<u64>,
    pub(crate) prior_rows: Option<u64>,
    pub(crate) fragment_count: Option<u64>,
    pub(crate) historical_memory_peak_bytes: Option<u64>,
    pub(crate) historical_scratch_peak_bytes: Option<u64>,
    pub(crate) observed_throughput_rows_per_second: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FleetTask {
    pub(crate) schema_version: u32,
    pub(crate) queue_algorithm_version: String,
    pub(crate) task_id: String,
    pub(crate) wiki: String,
    pub(crate) snapshot: String,
    pub(crate) resource_class: ResourceClass,
    pub(crate) signals: SchedulingSignals,
    pub(crate) attempt: u32,
    pub(crate) not_before_unix: u64,
    pub(crate) discovered_at_unix: u64,
    pub(crate) controller_run_id: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FleetClaim {
    pub(crate) schema_version: u32,
    pub(crate) task: FleetTask,
    pub(crate) worker_id: String,
    pub(crate) lease_id: String,
    pub(crate) claimed_at_unix: u64,
    pub(crate) heartbeat_at_unix: u64,
    pub(crate) lease_timeout_secs: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct ReadyNotification {
    schema_version: u32,
    task_id: String,
    wiki: String,
    snapshot: String,
    worker_id: String,
    ready_receipt: String,
    completed_at_unix: u64,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DiscoveryReport {
    pub(crate) scheduled: usize,
    pub(crate) unchanged: usize,
    pub(crate) leased: usize,
    pub(crate) replaced_obsolete: usize,
    pub(crate) recovered_stale: usize,
    pub(crate) quarantined: usize,
    pub(crate) by_resource_class: BTreeMap<String, usize>,
}

impl DiscoveryReport {
    pub(crate) fn merge(&mut self, other: Self) {
        self.scheduled += other.scheduled;
        self.unchanged += other.unchanged;
        self.leased += other.leased;
        self.replaced_obsolete += other.replaced_obsolete;
        self.recovered_stale += other.recovered_stale;
        self.quarantined += other.quarantined;
        for (class, count) in other.by_resource_class {
            *self.by_resource_class.entry(class).or_default() += count;
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
struct LifecycleRegistry {
    schema_version: u32,
    wikis: BTreeMap<String, LifecycleWiki>,
}

#[derive(Clone, Debug, Deserialize)]
struct LifecycleWiki {
    publication: String,
    refresh: String,
    #[serde(default)]
    fleet_resource_class: Option<ResourceClass>,
}

pub(crate) fn scheduled_wikis(path: &Path) -> Result<Vec<String>> {
    let registry: LifecycleRegistry = serde_json::from_slice(
        &fs::read(path).with_context(|| format!("failed to read {}", path.display()))?,
    )
    .with_context(|| format!("invalid lifecycle registry {}", path.display()))?;
    ensure!(
        registry.schema_version == 1,
        "unsupported lifecycle schema {}",
        registry.schema_version
    );
    let mut wikis = Vec::new();
    for (wiki, entry) in registry.wikis {
        validate_component(&wiki, "wiki")?;
        ensure!(
            matches!(
                entry.publication.as_str(),
                "published" | "hidden" | "retired"
            ),
            "invalid publication state for {wiki}"
        );
        ensure!(
            matches!(
                entry.refresh.as_str(),
                "scheduled" | "manual" | "paused" | "qualification"
            ),
            "invalid refresh state for {wiki}"
        );
        if entry.refresh == "scheduled" {
            ensure!(
                entry.publication == "published",
                "scheduled fleet wiki {wiki} is not published"
            );
            wikis.push(wiki);
        }
    }
    wikis.sort();
    Ok(wikis)
}

pub(crate) fn lifecycle_resource_overrides(path: &Path) -> Result<BTreeMap<String, ResourceClass>> {
    let registry: LifecycleRegistry = serde_json::from_slice(&fs::read(path)?)?;
    ensure!(registry.schema_version == 1, "unsupported lifecycle schema");
    Ok(registry
        .wikis
        .into_iter()
        .filter_map(|(wiki, entry)| entry.fleet_resource_class.map(|class| (wiki, class)))
        .collect())
}

pub(crate) fn classify(
    data_dir: &Path,
    output_dir: &Path,
    plan: &SnapshotPlan,
    explicit: Option<ResourceClass>,
) -> Result<(ResourceClass, SchedulingSignals)> {
    let profile = workload_profile::load(data_dir, plan.wiki.as_str(), plan.snapshot.as_str())?;
    let compressed_source_bytes = profile
        .as_ref()
        .map(|profile| profile.signals.total_compressed_bytes);
    let prior_rows = profile
        .as_ref()
        .and_then(|profile| profile.signals.prior_measured_rows);
    let (historical_memory_peak_bytes, historical_scratch_peak_bytes, throughput) =
        historical_signals(output_dir, plan.wiki.as_str(), prior_rows)?;
    let fragment_count = generation_fragment_count(data_dir, plan.wiki.as_str())?;
    workload_profile::record_observations(
        data_dir,
        plan.wiki.as_str(),
        workload_profile::WorkloadObservations {
            schema_version: 1,
            fragment_count,
            peak_memory_bytes: historical_memory_peak_bytes,
            peak_scratch_bytes: historical_scratch_peak_bytes,
            throughput_rows_per_second: throughput,
        },
    )?;
    let signals = SchedulingSignals {
        source_layout: plan.layout,
        source_count: plan.sources.len(),
        compressed_source_bytes,
        prior_rows,
        fragment_count,
        historical_memory_peak_bytes,
        historical_scratch_peak_bytes,
        observed_throughput_rows_per_second: throughput,
    };
    if plan.layout == SourceLayout::Monthly {
        ensure!(
            explicit.is_none_or(|class| class == ResourceClass::Isolated),
            "monthly source layouts require the isolated fleet resource class"
        );
    }
    let class = explicit.unwrap_or_else(|| {
        if plan.layout == SourceLayout::Monthly {
            ResourceClass::Isolated
        } else if profile
            .as_ref()
            .is_some_and(|profile| profile.profile == WorkloadProfileName::Large)
            || historical_memory_peak_bytes
                .is_some_and(|peak| peak >= MEDIUM_MEMORY_THRESHOLD_BYTES)
            || historical_scratch_peak_bytes
                .is_some_and(|peak| peak >= MEDIUM_SCRATCH_THRESHOLD_BYTES)
            || fragment_count.is_some_and(|count| count >= MEDIUM_FRAGMENT_THRESHOLD)
        {
            ResourceClass::MediumLarge
        } else {
            ResourceClass::Small
        }
    });
    Ok((class, signals))
}

pub(crate) fn enqueue(
    queue_root: &Path,
    wiki: &str,
    snapshot: &str,
    resource_class: ResourceClass,
    signals: SchedulingSignals,
    controller_run_id: &str,
) -> Result<DiscoveryReport> {
    enqueue_at(
        queue_root,
        wiki,
        snapshot,
        resource_class,
        signals,
        controller_run_id,
        now_unix()?,
    )
}

fn enqueue_at(
    queue_root: &Path,
    wiki: &str,
    snapshot: &str,
    resource_class: ResourceClass,
    signals: SchedulingSignals,
    controller_run_id: &str,
    now: u64,
) -> Result<DiscoveryReport> {
    validate_component(wiki, "wiki")?;
    crate::storage::validate_snapshot_version(snapshot)?;
    validate_component(controller_run_id, "controller run ID")?;
    initialize(queue_root)?;
    let mut report = DiscoveryReport {
        recovered_stale: recover_stale_at(queue_root, now, DEFAULT_MAX_ATTEMPTS)?,
        ..DiscoveryReport::default()
    };
    let lease = lease_dir(queue_root, wiki);
    if lease.is_dir() {
        report.leased = 1;
        return Ok(report);
    }
    let task = FleetTask {
        schema_version: TASK_SCHEMA_VERSION,
        queue_algorithm_version: QUEUE_ALGORITHM_VERSION.to_string(),
        task_id: task_identity(wiki, snapshot, resource_class, &signals)?,
        wiki: wiki.to_string(),
        snapshot: snapshot.to_string(),
        resource_class,
        signals,
        attempt: 0,
        not_before_unix: now,
        discovered_at_unix: now,
        controller_run_id: controller_run_id.to_string(),
    };
    task.validate()?;
    if completed_evidence_valid(queue_root, &task)? {
        report.unchanged = 1;
        return Ok(report);
    }
    let pending = pending_path(queue_root, wiki);
    if pending.is_file() {
        let existing: FleetTask = read_json(&pending)?;
        existing.validate()?;
        if existing.task_id == task.task_id {
            report.unchanged = 1;
            return Ok(report);
        }
        atomic_write_json(
            &queue_root
                .join("superseded")
                .join(format!("{}-{}.json", existing.wiki, existing.task_id)),
            &existing,
        )?;
        report.replaced_obsolete = 1;
    }
    atomic_write_json(&pending, &task)?;
    report.scheduled = 1;
    *report
        .by_resource_class
        .entry(resource_class.as_str().to_string())
        .or_default() += 1;
    Ok(report)
}

pub(crate) fn claim(
    queue_root: &Path,
    resource_class: ResourceClass,
    worker_id: &str,
    lease_timeout_secs: u64,
) -> Result<Option<FleetClaim>> {
    claim_at(
        queue_root,
        resource_class,
        worker_id,
        lease_timeout_secs,
        now_unix()?,
    )
}

pub(crate) fn write_claim_receipt(path: &Path, claim: &FleetClaim) -> Result<()> {
    claim.validate()?;
    atomic_write_json(path, claim)
}

fn claim_at(
    queue_root: &Path,
    resource_class: ResourceClass,
    worker_id: &str,
    lease_timeout_secs: u64,
    now: u64,
) -> Result<Option<FleetClaim>> {
    validate_component(worker_id, "worker ID")?;
    ensure!(lease_timeout_secs > 0, "lease timeout must be positive");
    initialize(queue_root)?;
    recover_stale_at(queue_root, now, DEFAULT_MAX_ATTEMPTS)?;
    let mut tasks = pending_tasks(queue_root)?;
    tasks.sort_by(|left, right| {
        (
            left.attempt,
            left.not_before_unix,
            &left.wiki,
            &left.task_id,
        )
            .cmp(&(
                right.attempt,
                right.not_before_unix,
                &right.wiki,
                &right.task_id,
            ))
    });
    for task in tasks {
        if task.resource_class != resource_class || task.not_before_unix > now {
            continue;
        }
        let directory = lease_dir(queue_root, &task.wiki);
        match fs::create_dir(&directory) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error).context("failed to acquire fleet lease"),
        }
        let lease_id = hex::encode(Sha256::digest(
            format!(
                "{}\0{}\0{}\0{now}",
                task.task_id,
                worker_id,
                std::process::id()
            )
            .as_bytes(),
        ));
        let claim = FleetClaim {
            schema_version: LEASE_SCHEMA_VERSION,
            task,
            worker_id: worker_id.to_string(),
            lease_id,
            claimed_at_unix: now,
            heartbeat_at_unix: now,
            lease_timeout_secs,
        };
        let write = atomic_write_json(&directory.join("owner.json"), &claim);
        if let Err(error) = write {
            let _ = fs::remove_dir_all(&directory);
            return Err(error).context("failed to publish fleet lease owner");
        }
        sync_dir(&queue_root.join("leases"))?;
        return Ok(Some(claim));
    }
    Ok(None)
}

pub(crate) fn heartbeat(queue_root: &Path, claim_path: &Path) -> Result<FleetClaim> {
    let mut claim: FleetClaim = read_json(claim_path)?;
    claim.validate()?;
    let live = read_live_claim(queue_root, &claim.task.wiki)?;
    ensure!(
        live.lease_id == claim.lease_id && live.worker_id == claim.worker_id,
        "fleet heartbeat does not own the live lease"
    );
    claim.heartbeat_at_unix = now_unix()?;
    atomic_write_json(
        &lease_dir(queue_root, &claim.task.wiki).join("owner.json"),
        &claim,
    )?;
    atomic_write_json(claim_path, &claim)?;
    Ok(claim)
}

pub(crate) fn complete(queue_root: &Path, claim_path: &Path, output_dir: &Path) -> Result<PathBuf> {
    let claim: FleetClaim = read_json(claim_path)?;
    claim.validate()?;
    ensure_live_owner(queue_root, &claim)?;
    let ready_index = output_dir
        .join("_ready-index")
        .join(format!("{}.json", claim.task.wiki));
    let ready: Value = serde_json::from_slice(
        &fs::read(&ready_index)
            .with_context(|| format!("fleet completion is missing {}", ready_index.display()))?,
    )?;
    ensure!(
        ready.get("schema_version").and_then(Value::as_u64) == Some(1)
            && ready.get("wiki").and_then(Value::as_str) == Some(claim.task.wiki.as_str())
            && ready
                .pointer("/newest_valid_ready/snapshot")
                .and_then(Value::as_str)
                == Some(claim.task.snapshot.as_str()),
        "fleet completion ready index does not match the claimed wiki and snapshot"
    );
    let notification = ReadyNotification {
        schema_version: NOTIFICATION_SCHEMA_VERSION,
        task_id: claim.task.task_id.clone(),
        wiki: claim.task.wiki.clone(),
        snapshot: claim.task.snapshot.clone(),
        worker_id: claim.worker_id.clone(),
        ready_receipt: ready_index.to_string_lossy().into_owned(),
        completed_at_unix: now_unix()?,
    };
    let notification_path = queue_root
        .join("notifications")
        .join("ready")
        .join(format!("{}-{}.json", claim.task.wiki, claim.task.task_id));
    atomic_write_json(&notification_path, &notification)?;
    atomic_write_json(
        &queue_root
            .join("completed")
            .join(format!("{}-{}.json", claim.task.wiki, claim.task.task_id)),
        &claim.task,
    )?;
    fs::remove_file(pending_path(queue_root, &claim.task.wiki))?;
    fs::remove_dir_all(lease_dir(queue_root, &claim.task.wiki))?;
    sync_dir(queue_root)?;
    Ok(notification_path)
}

pub(crate) fn fail(
    queue_root: &Path,
    claim_path: &Path,
    error: &str,
    max_attempts: u32,
) -> Result<bool> {
    ensure!(max_attempts > 0, "maximum attempts must be positive");
    let claim: FleetClaim = read_json(claim_path)?;
    claim.validate()?;
    ensure_live_owner(queue_root, &claim)?;
    let now = now_unix()?;
    fail_claim_at(queue_root, &claim, error, max_attempts, now)
}

fn fail_claim_at(
    queue_root: &Path,
    claim: &FleetClaim,
    error: &str,
    max_attempts: u32,
    now: u64,
) -> Result<bool> {
    let next_attempt = claim
        .task
        .attempt
        .checked_add(1)
        .context("fleet attempt overflow")?;
    let concise = concise_error(error);
    let quarantined = next_attempt >= max_attempts;
    if quarantined {
        atomic_write_json(
            &queue_root.join("quarantine").join(format!(
                "{}-{}-attempt-{next_attempt}.json",
                claim.task.wiki, claim.task.task_id
            )),
            &serde_json::json!({
                "schema_version": 1,
                "reason": "retry_limit_exhausted",
                "error": concise,
                "failed_at_unix": now,
                "claim": claim,
            }),
        )?;
        fs::remove_file(pending_path(queue_root, &claim.task.wiki))?;
    } else {
        let mut task = claim.task.clone();
        task.attempt = next_attempt;
        task.not_before_unix = now.saturating_add(retry_delay_secs(next_attempt));
        atomic_write_json(&pending_path(queue_root, &task.wiki), &task)?;
        atomic_write_json(
            &queue_root.join("failures").join(format!(
                "{}-{}-attempt-{next_attempt}.json",
                task.wiki, task.task_id
            )),
            &serde_json::json!({
                "schema_version": 1,
                "error": concise,
                "failed_at_unix": now,
                "worker_id": claim.worker_id,
                "task": task,
            }),
        )?;
    }
    fs::remove_dir_all(lease_dir(queue_root, &claim.task.wiki))?;
    sync_dir(queue_root)?;
    Ok(quarantined)
}

pub(crate) fn recover_stale(queue_root: &Path, max_attempts: u32) -> Result<usize> {
    recover_stale_at(queue_root, now_unix()?, max_attempts)
}

fn recover_stale_at(queue_root: &Path, now: u64, max_attempts: u32) -> Result<usize> {
    initialize(queue_root)?;
    let leases = queue_root.join("leases");
    let mut recovered = 0;
    for entry in fs::read_dir(&leases)? {
        let entry = entry?;
        let wiki = entry.file_name().to_string_lossy().into_owned();
        if !entry.path().is_dir() {
            quarantine_unknown(
                queue_root,
                &entry.path(),
                "lease entry is not a directory",
                now,
            )?;
            continue;
        }
        let owner_path = entry.path().join("owner.json");
        let claim = read_json::<FleetClaim>(&owner_path);
        let Ok(claim) = claim else {
            quarantine_unknown(
                queue_root,
                &entry.path(),
                "lease owner is missing or invalid",
                now,
            )?;
            continue;
        };
        if claim.validate().is_err() || claim.task.wiki != wiki {
            quarantine_unknown(queue_root, &entry.path(), "lease identity is invalid", now)?;
            continue;
        }
        if now.saturating_sub(claim.heartbeat_at_unix) <= claim.lease_timeout_secs {
            continue;
        }
        fail_claim_at(
            queue_root,
            &claim,
            "worker lease heartbeat expired",
            max_attempts,
            now,
        )?;
        recovered += 1;
    }
    Ok(recovered)
}

impl FleetTask {
    fn validate(&self) -> Result<()> {
        ensure!(
            self.schema_version == TASK_SCHEMA_VERSION
                && self.queue_algorithm_version == QUEUE_ALGORITHM_VERSION,
            "unsupported fleet task schema"
        );
        validate_component(&self.wiki, "task wiki")?;
        crate::storage::validate_snapshot_version(&self.snapshot)?;
        validate_component(&self.controller_run_id, "controller run ID")?;
        ensure!(
            self.task_id
                == task_identity(
                    &self.wiki,
                    &self.snapshot,
                    self.resource_class,
                    &self.signals
                )?,
            "fleet task identity mismatch"
        );
        ensure!(self.signals.source_count > 0, "fleet task has no sources");
        Ok(())
    }
}

impl FleetClaim {
    fn validate(&self) -> Result<()> {
        ensure!(
            self.schema_version == LEASE_SCHEMA_VERSION,
            "unsupported fleet lease schema"
        );
        self.task.validate()?;
        validate_component(&self.worker_id, "worker ID")?;
        ensure!(
            self.lease_id.len() == 64 && self.lease_id.bytes().all(|byte| byte.is_ascii_hexdigit()),
            "invalid fleet lease ID"
        );
        ensure!(self.lease_timeout_secs > 0, "invalid fleet lease timeout");
        ensure!(
            self.heartbeat_at_unix >= self.claimed_at_unix,
            "fleet heartbeat predates claim"
        );
        Ok(())
    }
}

fn initialize(root: &Path) -> Result<()> {
    for directory in [
        "pending",
        "leases",
        "completed",
        "failures",
        "superseded",
        "quarantine",
        "notifications/ready",
    ] {
        fs::create_dir_all(root.join(directory))?;
    }
    Ok(())
}

fn pending_tasks(root: &Path) -> Result<Vec<FleetTask>> {
    let mut tasks = Vec::new();
    for entry in fs::read_dir(root.join("pending"))? {
        let entry = entry?;
        if !entry.path().is_file()
            || entry.path().extension().and_then(|value| value.to_str()) != Some("json")
        {
            continue;
        }
        let task: FleetTask = read_json(&entry.path())?;
        task.validate()?;
        ensure!(
            entry.file_name().to_string_lossy() == format!("{}.json", task.wiki),
            "fleet pending filename does not match task wiki"
        );
        tasks.push(task);
    }
    Ok(tasks)
}

fn task_identity(
    wiki: &str,
    snapshot: &str,
    resource_class: ResourceClass,
    signals: &SchedulingSignals,
) -> Result<String> {
    #[derive(Serialize)]
    struct Seed<'a> {
        algorithm: &'static str,
        wiki: &'a str,
        snapshot: &'a str,
        resource_class: ResourceClass,
        signals: &'a SchedulingSignals,
    }
    Ok(hex::encode(Sha256::digest(serde_json::to_vec(&Seed {
        algorithm: QUEUE_ALGORITHM_VERSION,
        wiki,
        snapshot,
        resource_class,
        signals,
    })?)))
}

fn generation_fragment_count(data_dir: &Path, wiki: &str) -> Result<Option<u64>> {
    let Some(snapshot) = crate::storage::current_snapshot_version(data_dir, wiki)? else {
        return Ok(None);
    };
    let path = crate::storage::generation_manifest_path(data_dir, wiki, &snapshot)?;
    if !path.is_file() {
        return Ok(None);
    }
    let value: Value = serde_json::from_slice(&fs::read(path)?)?;
    let count = value
        .get("fragments")
        .and_then(Value::as_array)
        .map(|fragments| fragments.len() as u64);
    Ok(count)
}

fn historical_signals(
    output_dir: &Path,
    wiki: &str,
    prior_rows: Option<u64>,
) -> Result<(Option<u64>, Option<u64>, Option<u64>)> {
    let path = output_dir
        .join("_candidate-status")
        .join(format!("{wiki}.history.jsonl"));
    if !path.is_file() {
        return Ok((None, None, None));
    }
    let mut memory_peak = None;
    let mut scratch_peak = None;
    let mut throughput = None;
    for line in fs::read_to_string(path)?
        .lines()
        .filter(|line| !line.trim().is_empty())
    {
        let value: Value = serde_json::from_str(line)?;
        memory_peak = maximum(
            memory_peak,
            value.get("memoryPeakBytes").and_then(Value::as_u64),
        );
        scratch_peak = maximum(
            scratch_peak,
            value.get("scratchPeakBytes").and_then(Value::as_u64),
        );
        let recorded = value.get("throughputRowsPerSecond").and_then(Value::as_u64);
        let derived = prior_rows.and_then(|rows| {
            value
                .pointer("/stageDurationsMs/compute")
                .and_then(Value::as_u64)
                .filter(|duration_ms| *duration_ms > 0)
                .map(|duration_ms| {
                    u64::try_from(
                        (u128::from(rows) * 1_000 / u128::from(duration_ms))
                            .min(u128::from(u64::MAX)),
                    )
                    .unwrap_or(u64::MAX)
                })
        });
        throughput = minimum_nonzero(throughput, recorded.or(derived));
    }
    Ok((memory_peak, scratch_peak, throughput))
}

fn maximum(left: Option<u64>, right: Option<u64>) -> Option<u64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (left, right) => left.or(right),
    }
}

fn minimum_nonzero(left: Option<u64>, right: Option<u64>) -> Option<u64> {
    match (
        left.filter(|value| *value > 0),
        right.filter(|value| *value > 0),
    ) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (left, right) => left.or(right),
    }
}

fn ensure_live_owner(root: &Path, claim: &FleetClaim) -> Result<()> {
    let live = read_live_claim(root, &claim.task.wiki)?;
    ensure!(
        live.lease_id == claim.lease_id
            && live.worker_id == claim.worker_id
            && live.task.task_id == claim.task.task_id,
        "fleet completion does not own the live lease"
    );
    Ok(())
}

fn read_live_claim(root: &Path, wiki: &str) -> Result<FleetClaim> {
    let claim: FleetClaim = read_json(&lease_dir(root, wiki).join("owner.json"))?;
    claim.validate()?;
    Ok(claim)
}

fn pending_path(root: &Path, wiki: &str) -> PathBuf {
    root.join("pending").join(format!("{wiki}.json"))
}

fn completed_evidence_valid(root: &Path, task: &FleetTask) -> Result<bool> {
    let completed_path = root
        .join("completed")
        .join(format!("{}-{}.json", task.wiki, task.task_id));
    let notification_path = root
        .join("notifications/ready")
        .join(format!("{}-{}.json", task.wiki, task.task_id));
    if !completed_path.is_file() || !notification_path.is_file() {
        return Ok(false);
    }
    let completed: FleetTask = read_json(&completed_path)?;
    completed.validate()?;
    ensure!(
        completed.task_id == task.task_id
            && completed.wiki == task.wiki
            && completed.snapshot == task.snapshot
            && completed.resource_class == task.resource_class
            && completed.signals == task.signals,
        "completed fleet task evidence drifted"
    );
    let notification: ReadyNotification = read_json(&notification_path)?;
    ensure!(
        notification.schema_version == NOTIFICATION_SCHEMA_VERSION
            && notification.task_id == task.task_id
            && notification.wiki == task.wiki
            && notification.snapshot == task.snapshot
            && notification.completed_at_unix > 0,
        "fleet ready notification does not match its completed task"
    );
    let ready_path = Path::new(&notification.ready_receipt);
    if !ready_path.is_file() {
        return Ok(false);
    }
    let ready: Value = serde_json::from_slice(&fs::read(ready_path)?)?;
    Ok(
        ready.get("schema_version").and_then(Value::as_u64) == Some(1)
            && ready.get("wiki").and_then(Value::as_str) == Some(task.wiki.as_str())
            && ready
                .pointer("/newest_valid_ready/snapshot")
                .and_then(Value::as_str)
                == Some(task.snapshot.as_str()),
    )
}

fn lease_dir(root: &Path, wiki: &str) -> PathBuf {
    root.join("leases").join(wiki)
}

fn retry_delay_secs(attempt: u32) -> u64 {
    300_u64.saturating_mul(1_u64 << attempt.min(6))
}

fn concise_error(error: &str) -> String {
    error
        .replace(['\r', '\n'], " ")
        .trim()
        .chars()
        .take(MAX_ERROR_BYTES)
        .collect()
}

fn quarantine_unknown(root: &Path, path: &Path, reason: &str, now: u64) -> Result<()> {
    let name = path
        .file_name()
        .and_then(|value| value.to_str())
        .context("fleet quarantine entry has no UTF-8 name")?;
    let destination = root
        .join("quarantine")
        .join(format!("ambiguous-{name}-{now}"));
    fs::rename(path, &destination)?;
    atomic_write_json(
        &destination.with_extension("receipt.json"),
        &serde_json::json!({
            "schema_version": 1,
            "reason": reason,
            "quarantined_at_unix": now,
            "artifact": destination,
        }),
    )
}

fn validate_component(value: &str, label: &str) -> Result<()> {
    ensure!(
        !value.is_empty()
            && value.len() <= 160
            && value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_')),
        "unsafe {label} {value:?}"
    );
    Ok(())
}

fn read_json<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T> {
    serde_json::from_slice(
        &fs::read(path).with_context(|| format!("failed to read {}", path.display()))?,
    )
    .with_context(|| format!("invalid JSON in {}", path.display()))
}

fn atomic_write_json(path: &Path, value: &impl Serialize) -> Result<()> {
    let parent = path.parent().context("fleet document has no parent")?;
    fs::create_dir_all(parent)?;
    let temporary = parent.join(format!(
        ".{}.{}-{}.tmp",
        path.file_name()
            .and_then(|value| value.to_str())
            .context("fleet document has no UTF-8 filename")?,
        std::process::id(),
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
    ));
    let result = (|| -> Result<()> {
        let mut file = File::create(&temporary)?;
        serde_json::to_writer_pretty(&mut file, value)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        fs::rename(&temporary, path)?;
        sync_dir(parent)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

fn sync_dir(path: &Path) -> Result<()> {
    File::open(path)?.sync_all()?;
    Ok(())
}

fn now_unix() -> Result<u64> {
    Ok(SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::TestDir;

    fn signals(layout: SourceLayout) -> SchedulingSignals {
        SchedulingSignals {
            source_layout: layout,
            source_count: 2,
            compressed_source_bytes: Some(42),
            prior_rows: Some(100),
            fragment_count: Some(3),
            historical_memory_peak_bytes: Some(100),
            historical_scratch_peak_bytes: Some(50),
            observed_throughput_rows_per_second: Some(10),
        }
    }

    #[test]
    fn queue_claims_independent_wikis_and_retries_then_quarantines() -> Result<()> {
        let root = TestDir::new()?;
        enqueue_at(
            root.path(),
            "nlwiki",
            "2026-08",
            ResourceClass::Small,
            signals(SourceLayout::Yearly),
            "controller-1",
            1_000,
        )?;
        enqueue_at(
            root.path(),
            "ptwiki",
            "2026-08",
            ResourceClass::Small,
            signals(SourceLayout::Yearly),
            "controller-1",
            1_000,
        )?;
        let first = claim_at(root.path(), ResourceClass::Small, "worker-1", 60, 1_001)?
            .context("first task should be claimable")?;
        let second = claim_at(root.path(), ResourceClass::Small, "worker-2", 60, 1_001)?
            .context("second task should be claimable")?;
        assert_ne!(first.task.wiki, second.task.wiki);
        assert!(claim_at(root.path(), ResourceClass::Small, "worker-3", 60, 1_001)?.is_none());

        assert!(!fail_claim_at(
            root.path(),
            &first,
            "temporary\nfailure",
            2,
            1_010
        )?);
        assert!(claim_at(root.path(), ResourceClass::Small, "worker-3", 60, 1_100)?.is_none());
        let retry = claim_at(root.path(), ResourceClass::Small, "worker-3", 60, 1_700)?
            .context("retry should become eligible after backoff")?;
        assert!(fail_claim_at(root.path(), &retry, "permanent", 2, 1_701)?);
        assert!(!pending_path(root.path(), &retry.task.wiki).exists());
        assert!(
            fs::read_dir(root.path().join("quarantine"))?
                .next()
                .is_some()
        );
        Ok(())
    }

    #[test]
    fn stale_lease_recovery_is_bounded_and_idempotent() -> Result<()> {
        let root = TestDir::new()?;
        enqueue_at(
            root.path(),
            "nlwiki",
            "2026-08",
            ResourceClass::Small,
            signals(SourceLayout::Yearly),
            "controller-1",
            100,
        )?;
        let claim = claim_at(root.path(), ResourceClass::Small, "worker-1", 10, 101)?
            .context("task should be claimable")?;
        assert_eq!(recover_stale_at(root.path(), 111, 3)?, 0);
        assert_eq!(recover_stale_at(root.path(), 112, 3)?, 1);
        assert_eq!(recover_stale_at(root.path(), 112, 3)?, 0);
        let retried: FleetTask = read_json(&pending_path(root.path(), "nlwiki"))?;
        assert_eq!(retried.attempt, 1);
        assert!(!lease_dir(root.path(), &claim.task.wiki).exists());
        Ok(())
    }

    #[test]
    fn completion_requires_matching_ready_index_and_publishes_notification() -> Result<()> {
        let queue = TestDir::new()?;
        let output = TestDir::new()?;
        enqueue_at(
            queue.path(),
            "nlwiki",
            "2026-08",
            ResourceClass::Small,
            signals(SourceLayout::Yearly),
            "controller-1",
            100,
        )?;
        let claim = claim_at(queue.path(), ResourceClass::Small, "worker-1", 60, 101)?
            .context("task should be claimable")?;
        let claim_path = queue.path().join("claim.json");
        atomic_write_json(&claim_path, &claim)?;
        fs::create_dir_all(output.path().join("_ready-index"))?;
        fs::write(
            output.path().join("_ready-index/nlwiki.json"),
            br#"{"schema_version":1,"wiki":"nlwiki","newest_valid_ready":{"snapshot":"2026-07"}}"#,
        )?;
        assert!(complete(queue.path(), &claim_path, output.path()).is_err());
        fs::write(
            output.path().join("_ready-index/nlwiki.json"),
            br#"{"schema_version":1,"wiki":"nlwiki","newest_valid_ready":{"snapshot":"2026-08"}}"#,
        )?;
        let notification = complete(queue.path(), &claim_path, output.path())?;
        assert!(notification.is_file());
        assert!(!pending_path(queue.path(), "nlwiki").exists());
        assert!(!lease_dir(queue.path(), "nlwiki").exists());
        let unchanged = enqueue_at(
            queue.path(),
            "nlwiki",
            "2026-08",
            ResourceClass::Small,
            signals(SourceLayout::Yearly),
            "controller-2",
            200,
        )?;
        assert_eq!(unchanged.unchanged, 1);
        fs::remove_file(output.path().join("_ready-index/nlwiki.json"))?;
        let repaired = enqueue_at(
            queue.path(),
            "nlwiki",
            "2026-08",
            ResourceClass::Small,
            signals(SourceLayout::Yearly),
            "controller-3",
            201,
        )?;
        assert_eq!(repaired.scheduled, 1);
        Ok(())
    }

    #[test]
    fn discovery_deduplicates_and_replaces_only_unleased_work() -> Result<()> {
        let root = TestDir::new()?;
        let first = enqueue_at(
            root.path(),
            "nlwiki",
            "2026-07",
            ResourceClass::Small,
            signals(SourceLayout::Yearly),
            "controller-1",
            100,
        )?;
        assert_eq!(first.scheduled, 1);
        let unchanged = enqueue_at(
            root.path(),
            "nlwiki",
            "2026-07",
            ResourceClass::Small,
            signals(SourceLayout::Yearly),
            "controller-2",
            101,
        )?;
        assert_eq!(unchanged.unchanged, 1);
        let replaced = enqueue_at(
            root.path(),
            "nlwiki",
            "2026-08",
            ResourceClass::Small,
            signals(SourceLayout::Yearly),
            "controller-3",
            102,
        )?;
        assert_eq!(replaced.replaced_obsolete, 1);
        let _claim = claim_at(root.path(), ResourceClass::Small, "worker-1", 60, 103)?;
        let leased = enqueue_at(
            root.path(),
            "nlwiki",
            "2026-09",
            ResourceClass::Small,
            signals(SourceLayout::Yearly),
            "controller-4",
            104,
        )?;
        assert_eq!(leased.leased, 1);
        Ok(())
    }

    #[test]
    fn lifecycle_and_signal_classification_have_no_wiki_name_branch() -> Result<()> {
        let root = TestDir::new()?;
        let lifecycle = root.path().join("lifecycle.json");
        fs::write(
            &lifecycle,
            br#"{"schema_version":1,"wikis":{"smallwiki":{"publication":"published","refresh":"scheduled"},"manualwiki":{"publication":"published","refresh":"manual"},"monthlywiki":{"publication":"hidden","refresh":"qualification","fleet_resource_class":"isolated"}}}"#,
        )?;
        assert_eq!(scheduled_wikis(&lifecycle)?, vec!["smallwiki"]);
        assert_eq!(
            lifecycle_resource_overrides(&lifecycle)?.get("monthlywiki"),
            Some(&ResourceClass::Isolated)
        );

        let yearly = SnapshotPlan::resolve("frwiki", "2026-08")?;
        let monthly = SnapshotPlan::resolve("enwiki", "2026-08")?;
        let (yearly_class, _) = classify(root.path(), root.path(), &yearly, None)?;
        let (monthly_class, _) = classify(root.path(), root.path(), &monthly, None)?;
        assert_eq!(yearly_class, ResourceClass::Small);
        assert_eq!(monthly_class, ResourceClass::Isolated);
        assert!(
            classify(
                root.path(),
                root.path(),
                &monthly,
                Some(ResourceClass::Small)
            )
            .is_err()
        );
        Ok(())
    }

    #[test]
    fn fixed_queue_represents_hundreds_of_wikis_without_job_definitions() -> Result<()> {
        let root = TestDir::new()?;
        for index in 0..300 {
            let wiki = format!("w{index:03}wiki");
            enqueue_at(
                root.path(),
                &wiki,
                "2026-08",
                ResourceClass::Small,
                signals(SourceLayout::Yearly),
                "controller-1",
                100,
            )?;
        }
        assert_eq!(fs::read_dir(root.path().join("pending"))?.count(), 300);
        let claim = claim_at(root.path(), ResourceClass::Small, "worker-1", 60, 101)?
            .context("a large fleet should remain claimable")?;
        assert_eq!(claim.task.wiki, "w000wiki");
        Ok(())
    }

    #[test]
    fn malformed_queue_evidence_fails_closed_or_is_quarantined() -> Result<()> {
        let root = TestDir::new()?;
        initialize(root.path())?;
        fs::write(root.path().join("pending/badwiki.json"), b"{")?;
        assert!(pending_tasks(root.path()).is_err());
        fs::remove_file(root.path().join("pending/badwiki.json"))?;
        fs::create_dir(root.path().join("leases/badwiki"))?;
        assert_eq!(recover_stale_at(root.path(), 100, 3)?, 0);
        assert!(!root.path().join("leases/badwiki").exists());
        assert!(
            fs::read_dir(root.path().join("quarantine"))?
                .next()
                .is_some()
        );
        Ok(())
    }

    #[test]
    fn helpers_validate_identity_backoff_and_error_bounds() -> Result<()> {
        assert!(validate_component("bad/wiki", "test").is_err());
        assert_eq!(retry_delay_secs(1), 600);
        assert_eq!(concise_error(&"x".repeat(600)).len(), MAX_ERROR_BYTES);
        assert_eq!(concise_error("a\nb"), "a b");
        assert_eq!(maximum(Some(1), Some(2)), Some(2));
        assert_eq!(maximum(None, Some(2)), Some(2));
        assert_eq!(minimum_nonzero(Some(3), Some(2)), Some(2));
        assert_eq!(minimum_nonzero(Some(0), Some(2)), Some(2));
        Ok(())
    }
}
