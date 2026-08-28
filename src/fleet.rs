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

use crate::publication::READY_INDEX_SCHEMA_VERSION;
use crate::snapshot_plan::{SnapshotPlan, SourceLayout};
use crate::workload_profile::{self, WorkloadProfileName};

const TASK_SCHEMA_VERSION: u32 = 1;
const LEASE_SCHEMA_VERSION: u32 = 1;
const NOTIFICATION_SCHEMA_VERSION: u32 = 1;
const QUEUE_ALGORITHM_VERSION: &str = "fleet-queue-v2";
const LEGACY_QUEUE_ALGORITHM_VERSION: &str = "fleet-queue-v1";
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
    let observation_write = workload_profile::record_observations(
        data_dir,
        plan.wiki.as_str(),
        workload_profile::WorkloadObservations {
            schema_version: 1,
            fragment_count,
            peak_memory_bytes: historical_memory_peak_bytes,
            peak_scratch_bytes: historical_scratch_peak_bytes,
            throughput_rows_per_second: throughput,
        },
    );
    observation_write?;
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
        task_id: task_identity(wiki, snapshot)?,
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
            // Resource classification and observations are scheduling metadata.
            // Refresh them without turning the same immutable snapshot into new
            // semantic work or changing its stable task identity.
            atomic_write_json(&pending, &task)?;
            report.unchanged = 1;
            return Ok(report);
        }
        let superseded_write = atomic_write_json(
            &queue_root
                .join("superseded")
                .join(format!("{}-{}.json", existing.wiki, existing.task_id)),
            &existing,
        );
        superseded_write?;
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
    claim_at_with(
        queue_root,
        resource_class,
        worker_id,
        lease_timeout_secs,
        now,
        acquire_lease,
        publish_claim,
    )
}

fn acquire_lease(path: &Path) -> std::io::Result<()> {
    fs::create_dir(path)
}

fn publish_claim(path: &Path, claim: &FleetClaim) -> Result<()> {
    atomic_write_json(path, claim)
}

fn claim_at_with(
    queue_root: &Path,
    resource_class: ResourceClass,
    worker_id: &str,
    lease_timeout_secs: u64,
    now: u64,
    mut acquire: impl FnMut(&Path) -> std::io::Result<()>,
    mut publish: impl FnMut(&Path, &FleetClaim) -> Result<()>,
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
        match acquire(&directory) {
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
        let write = publish(&directory.join("owner.json"), &claim);
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
    let live_write = atomic_write_json(
        &lease_dir(queue_root, &claim.task.wiki).join("owner.json"),
        &claim,
    );
    live_write?;
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
    let ready_bytes = fs::read(&ready_index)
        .with_context(|| format!("fleet completion is missing {}", ready_index.display()))?;
    let ready: Value = serde_json::from_slice(&ready_bytes)?;
    ensure!(
        ready.get("schema_version").and_then(Value::as_u64)
            == Some(u64::from(READY_INDEX_SCHEMA_VERSION))
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
    let completed_write = atomic_write_json(
        &queue_root
            .join("completed")
            .join(format!("{}-{}.json", claim.task.wiki, claim.task.task_id)),
        &claim.task,
    );
    completed_write?;
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
        let quarantine_write = atomic_write_json(
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
        );
        quarantine_write?;
        fs::remove_file(pending_path(queue_root, &claim.task.wiki))?;
    } else {
        let mut task = claim.task.clone();
        task.attempt = next_attempt;
        task.not_before_unix = now.saturating_add(retry_delay_secs(next_attempt));
        atomic_write_json(&pending_path(queue_root, &task.wiki), &task)?;
        let failure_write = atomic_write_json(
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
        );
        failure_write?;
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
            let quarantined = quarantine_unknown(
                queue_root,
                &entry.path(),
                "lease entry is not a directory",
                now,
            );
            quarantined?;
            continue;
        }
        let owner_path = entry.path().join("owner.json");
        let claim = read_json::<FleetClaim>(&owner_path);
        let Ok(claim) = claim else {
            let quarantined = quarantine_unknown(
                queue_root,
                &entry.path(),
                "lease owner is missing or invalid",
                now,
            );
            quarantined?;
            continue;
        };
        if claim.validate().is_err() || claim.task.wiki != wiki {
            quarantine_unknown(queue_root, &entry.path(), "lease identity is invalid", now)?;
            continue;
        }
        if now.saturating_sub(claim.heartbeat_at_unix) <= claim.lease_timeout_secs {
            continue;
        }
        let failed = fail_claim_at(
            queue_root,
            &claim,
            "worker lease heartbeat expired",
            max_attempts,
            now,
        );
        failed?;
        recovered += 1;
    }
    Ok(recovered)
}

impl FleetTask {
    fn validate(&self) -> Result<()> {
        ensure!(
            self.schema_version == TASK_SCHEMA_VERSION
                && matches!(
                    self.queue_algorithm_version.as_str(),
                    QUEUE_ALGORITHM_VERSION | LEGACY_QUEUE_ALGORITHM_VERSION
                ),
            "unsupported fleet task schema"
        );
        validate_component(&self.wiki, "task wiki")?;
        crate::storage::validate_snapshot_version(&self.snapshot)?;
        validate_component(&self.controller_run_id, "controller run ID")?;
        let identity = if self.queue_algorithm_version == LEGACY_QUEUE_ALGORITHM_VERSION {
            legacy_task_identity(
                &self.wiki,
                &self.snapshot,
                self.resource_class,
                &self.signals,
            )
        } else {
            task_identity(&self.wiki, &self.snapshot)
        };
        let expected_identity = identity?;
        ensure!(
            self.task_id == expected_identity,
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

fn task_identity(wiki: &str, snapshot: &str) -> Result<String> {
    #[derive(Serialize)]
    struct Seed<'a> {
        algorithm: &'static str,
        wiki: &'a str,
        snapshot: &'a str,
    }
    Ok(hex::encode(Sha256::digest(serde_json::to_vec(&Seed {
        algorithm: QUEUE_ALGORITHM_VERSION,
        wiki,
        snapshot,
    })?)))
}

fn legacy_task_identity(
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
        algorithm: LEGACY_QUEUE_ALGORITHM_VERSION,
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
            && completed.snapshot == task.snapshot,
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
    Ok(ready.get("schema_version").and_then(Value::as_u64)
        == Some(u64::from(READY_INDEX_SCHEMA_VERSION))
        && ready.get("wiki").and_then(Value::as_str) == Some(task.wiki.as_str())
        && ready
            .pointer("/newest_valid_ready/snapshot")
            .and_then(Value::as_str)
            == Some(task.snapshot.as_str()))
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

    fn enqueue_fixture(
        root: &Path,
        wiki: &str,
        snapshot: &str,
        controller: &str,
        now: u64,
    ) -> Result<DiscoveryReport> {
        enqueue_at(
            root,
            wiki,
            snapshot,
            ResourceClass::Small,
            signals(SourceLayout::Yearly),
            controller,
            now,
        )
    }

    #[test]
    fn queue_claims_independent_wikis_and_retries_then_quarantines() -> Result<()> {
        let root = TestDir::new()?;
        enqueue_fixture(root.path(), "nlwiki", "2026-08", "controller-1", 1_000)?;
        enqueue_fixture(root.path(), "ptwiki", "2026-08", "controller-1", 1_000)?;
        let first = claim_at(root.path(), ResourceClass::Small, "worker-1", 60, 1_001)?
            .context("first task should be claimable")?;
        let second = claim_at(root.path(), ResourceClass::Small, "worker-2", 60, 1_001)?
            .context("second task should be claimable")?;
        assert_ne!(first.task.wiki, second.task.wiki);
        assert!(claim_at(root.path(), ResourceClass::Small, "worker-3", 60, 1_001)?.is_none());

        let first_failure = fail_claim_at(root.path(), &first, "temporary\nfailure", 2, 1_010)?;
        assert!(!first_failure);
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
        enqueue_fixture(root.path(), "nlwiki", "2026-08", "controller-1", 100)?;
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
        enqueue_fixture(queue.path(), "nlwiki", "2026-08", "controller-1", 100)?;
        let claim = claim_at(queue.path(), ResourceClass::Small, "worker-1", 60, 101)?
            .context("task should be claimable")?;
        let claim_path = queue.path().join("claim.json");
        atomic_write_json(&claim_path, &claim)?;
        fs::create_dir_all(output.path().join("_ready-index"))?;
        const STALE_READY: &[u8] =
            br#"{"schema_version":2,"wiki":"nlwiki","newest_valid_ready":{"snapshot":"2026-07"}}"#;
        let ready_index = output.path().join("_ready-index/nlwiki.json");
        fs::write(&ready_index, STALE_READY)?;
        assert!(complete(queue.path(), &claim_path, output.path()).is_err());
        const OBSOLETE_READY: &[u8] =
            br#"{"schema_version":1,"wiki":"nlwiki","newest_valid_ready":{"snapshot":"2026-08"}}"#;
        fs::write(&ready_index, OBSOLETE_READY)?;
        assert!(complete(queue.path(), &claim_path, output.path()).is_err());
        const CURRENT_READY: &[u8] =
            br#"{"schema_version":2,"wiki":"nlwiki","newest_valid_ready":{"snapshot":"2026-08"}}"#;
        fs::write(&ready_index, CURRENT_READY)?;
        let notification = complete(queue.path(), &claim_path, output.path())?;
        assert!(notification.is_file());
        assert!(!pending_path(queue.path(), "nlwiki").exists());
        assert!(!lease_dir(queue.path(), "nlwiki").exists());
        let mut changed_signals = signals(SourceLayout::Yearly);
        changed_signals.historical_memory_peak_bytes = Some(4_000_000_000);
        changed_signals.observed_throughput_rows_per_second = Some(99_999);
        let unchanged = enqueue_at(
            queue.path(),
            "nlwiki",
            "2026-08",
            ResourceClass::MediumLarge,
            changed_signals,
            "controller-2",
            200,
        );
        let unchanged = unchanged.context("changed scheduling metadata should remain a no-op")?;
        assert_eq!(unchanged.unchanged, 1);
        fs::remove_file(output.path().join("_ready-index/nlwiki.json"))?;
        let repaired = enqueue_fixture(queue.path(), "nlwiki", "2026-08", "controller-3", 201)?;
        assert_eq!(repaired.scheduled, 1);
        Ok(())
    }

    #[test]
    fn discovery_deduplicates_and_replaces_only_unleased_work() -> Result<()> {
        let root = TestDir::new()?;
        let first = enqueue_fixture(root.path(), "nlwiki", "2026-07", "controller-1", 100)?;
        assert_eq!(first.scheduled, 1);
        let unchanged = enqueue_fixture(root.path(), "nlwiki", "2026-07", "controller-2", 101)?;
        assert_eq!(unchanged.unchanged, 1);
        let replaced = enqueue_fixture(root.path(), "nlwiki", "2026-08", "controller-3", 102)?;
        assert_eq!(replaced.replaced_obsolete, 1);
        let _claim = claim_at(root.path(), ResourceClass::Small, "worker-1", 60, 103)?;
        let leased = enqueue_fixture(root.path(), "nlwiki", "2026-09", "controller-4", 104)?;
        assert_eq!(leased.leased, 1);
        Ok(())
    }

    #[test]
    fn legacy_tasks_remain_valid_during_queue_v2_rollout() -> Result<()> {
        let task_signals = signals(SourceLayout::Yearly);
        let task_id =
            legacy_task_identity("nlwiki", "2026-08", ResourceClass::Small, &task_signals);
        let task_id = task_id.context("legacy task identity should remain computable")?;
        let task = FleetTask {
            schema_version: TASK_SCHEMA_VERSION,
            queue_algorithm_version: LEGACY_QUEUE_ALGORITHM_VERSION.to_string(),
            task_id,
            wiki: "nlwiki".to_string(),
            snapshot: "2026-08".to_string(),
            resource_class: ResourceClass::Small,
            signals: task_signals,
            attempt: 0,
            not_before_unix: 100,
            discovered_at_unix: 100,
            controller_run_id: "controller-v1".to_string(),
        };
        task.validate()
    }

    #[test]
    fn lifecycle_and_signal_classification_have_no_wiki_name_branch() -> Result<()> {
        let root = TestDir::new()?;
        let lifecycle = root.path().join("lifecycle.json");
        const LIFECYCLE: &[u8] = br#"{"schema_version":1,"wikis":{"smallwiki":{"publication":"published","refresh":"scheduled"},"manualwiki":{"publication":"published","refresh":"manual"},"monthlywiki":{"publication":"hidden","refresh":"qualification","fleet_resource_class":"isolated"}}}"#;
        fs::write(&lifecycle, LIFECYCLE)?;
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
            enqueue_fixture(root.path(), &wiki, "2026-08", "controller-1", 100)?;
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
        assert_eq!(ResourceClass::Small.as_str(), "small");
        assert_eq!(ResourceClass::MediumLarge.as_str(), "medium_large");
        assert_eq!(ResourceClass::Isolated.as_str(), "isolated");
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

    #[test]
    fn public_queue_wrappers_heartbeat_and_validation_fail_closed() -> Result<()> {
        let queue = TestDir::new()?;
        let output = TestDir::new()?;
        let report_result = enqueue(
            queue.path(),
            "nlwiki",
            "2026-08",
            ResourceClass::Small,
            signals(SourceLayout::Yearly),
            "controller-wrapper",
        );
        let report = report_result?;
        assert_eq!(report.scheduled, 1);
        assert!(
            enqueue(
                queue.path(),
                "bad/wiki",
                "2026-08",
                ResourceClass::Small,
                signals(SourceLayout::Yearly),
                "controller-wrapper",
            )
            .is_err()
        );
        assert!(claim(queue.path(), ResourceClass::Small, "worker", 0).is_err());

        let claim = claim(queue.path(), ResourceClass::Small, "worker", 60)?
            .context("wrapper task should be claimable")?;
        let receipt = queue.path().join("claim.json");
        write_claim_receipt(&receipt, &claim)?;
        let refreshed = heartbeat(queue.path(), &receipt)?;
        assert_eq!(refreshed.lease_id, claim.lease_id);
        assert!(complete(queue.path(), &receipt, output.path()).is_err());

        let mut impostor = refreshed.clone();
        impostor.lease_id = "0".repeat(64);
        atomic_write_json(&receipt, &impostor)?;
        assert!(heartbeat(queue.path(), &receipt).is_err());
        assert!(fail(queue.path(), &receipt, "lost ownership", 3).is_err());
        assert!(fail(queue.path(), &receipt, "invalid attempts", 0).is_err());
        assert_eq!(recover_stale(queue.path(), 3)?, 0);

        let mut invalid_task = claim.task.clone();
        invalid_task.schema_version = 99;
        assert!(invalid_task.validate().is_err());
        invalid_task = claim.task.clone();
        invalid_task.task_id = "0".repeat(64);
        assert!(invalid_task.validate().is_err());
        invalid_task = claim.task.clone();
        invalid_task.signals.source_count = 0;
        let invalid_identity = task_identity(&invalid_task.wiki, &invalid_task.snapshot);
        invalid_task.task_id = invalid_identity?;
        assert!(invalid_task.validate().is_err());

        for invalid in [
            FleetClaim {
                schema_version: 99,
                ..claim.clone()
            },
            FleetClaim {
                lease_id: "bad".to_string(),
                ..claim.clone()
            },
            FleetClaim {
                lease_timeout_secs: 0,
                ..claim.clone()
            },
            FleetClaim {
                claimed_at_unix: claim.heartbeat_at_unix.saturating_add(1),
                ..claim.clone()
            },
        ] {
            assert!(invalid.validate().is_err());
            assert!(write_claim_receipt(&queue.path().join("invalid.json"), &invalid).is_err());
        }

        let acquire_failure = TestDir::new()?;
        let acquire_root = acquire_failure.path();
        enqueue_fixture(acquire_root, "nlwiki", "2026-08", "controller", 100)?;
        let acquisition = claim_at_with(
            acquire_failure.path(),
            ResourceClass::Small,
            "worker",
            60,
            101,
            |_| {
                Err(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "injected",
                ))
            },
            publish_claim,
        );
        assert!(acquisition.is_err());

        let publish_failure = TestDir::new()?;
        let publish_root = publish_failure.path();
        enqueue_fixture(publish_root, "nlwiki", "2026-08", "controller", 100)?;
        let publication = claim_at_with(
            publish_failure.path(),
            ResourceClass::Small,
            "worker",
            60,
            101,
            acquire_lease,
            |_, _| Err(anyhow::anyhow!("injected owner publication failure")),
        );
        assert!(publication.is_err());
        assert!(!lease_dir(publish_failure.path(), "nlwiki").exists());
        Ok(())
    }

    #[test]
    fn measured_history_and_generation_fragments_drive_classification() -> Result<()> {
        let data = TestDir::new()?;
        let output = TestDir::new()?;
        let pointer = crate::storage::write_current_snapshot_pointer_for_test(
            data.path(),
            "testwiki",
            "2026-08",
        );
        pointer?;
        let manifest =
            crate::storage::generation_manifest_path(data.path(), "testwiki", "2026-08")?;
        fs::create_dir_all(manifest.parent().context("manifest has a parent")?)?;
        fs::write(&manifest, br#"{"fragments":[{}, {}, {}]}"#)?;

        let status = output
            .path()
            .join("_candidate-status/testwiki.history.jsonl");
        fs::create_dir_all(status.parent().context("history has a parent")?)?;
        let history_json = concat!(
            "\n",
            "{\"memoryPeakBytes\":1610612736,\"scratchPeakBytes\":100,",
            "\"throughputRowsPerSecond\":400}\n",
            "{\"memoryPeakBytes\":2000000000,\"scratchPeakBytes\":200,",
            "\"stageDurationsMs\":{\"compute\":1000}}\n"
        );
        fs::write(&status, history_json)?;
        assert_eq!(generation_fragment_count(data.path(), "testwiki")?, Some(3));
        let history = historical_signals(output.path(), "testwiki", Some(1_000))?;
        assert_eq!(history, (Some(2_000_000_000), Some(200), Some(400)));

        let plan = SnapshotPlan::resolve("testwiki", "2026-08")?;
        let (class, classified) = classify(data.path(), output.path(), &plan, None)?;
        assert_eq!(class, ResourceClass::MediumLarge);
        assert_eq!(classified.fragment_count, Some(3));

        fs::remove_file(&manifest)?;
        assert_eq!(generation_fragment_count(data.path(), "testwiki")?, None);
        assert_eq!(generation_fragment_count(data.path(), "missingwiki")?, None);
        Ok(())
    }

    #[test]
    fn lifecycle_reports_and_queue_evidence_cover_recovery_edges() -> Result<()> {
        let root = TestDir::new()?;
        let lifecycle = root.path().join("lifecycle.json");
        for invalid in [
            br#"{"schema_version":2,"wikis":{}}"#.as_slice(),
            br#"{"schema_version":1,"wikis":{"xwiki":{"publication":"bad","refresh":"manual"}}}"#.as_slice(),
            br#"{"schema_version":1,"wikis":{"xwiki":{"publication":"published","refresh":"bad"}}}"#.as_slice(),
            br#"{"schema_version":1,"wikis":{"xwiki":{"publication":"hidden","refresh":"scheduled"}}}"#.as_slice(),
        ] {
            fs::write(&lifecycle, invalid)?;
            assert!(scheduled_wikis(&lifecycle).is_err());
        }

        let mut combined = DiscoveryReport::default();
        combined.merge(DiscoveryReport {
            scheduled: 1,
            unchanged: 2,
            leased: 3,
            replaced_obsolete: 4,
            recovered_stale: 5,
            quarantined: 6,
            by_resource_class: BTreeMap::from([("small".to_string(), 7)]),
        });
        combined.merge(DiscoveryReport {
            by_resource_class: BTreeMap::from([("small".to_string(), 1)]),
            ..DiscoveryReport::default()
        });
        assert_eq!(combined.by_resource_class["small"], 8);
        assert_eq!(combined.quarantined, 6);

        let queue = root.path().join("queue");
        initialize(&queue)?;
        fs::create_dir(queue.join("pending/not-json"))?;
        fs::write(queue.join("pending/ignored.txt"), b"ignored")?;
        assert!(pending_tasks(&queue)?.is_empty());

        let report = enqueue_fixture(&queue, "nlwiki", "2026-08", "controller", 100)?;
        assert_eq!(report.scheduled, 1);
        let pending = queue.join("pending/nlwiki.json");
        let wrong_pending = queue.join("pending/wrongwiki.json");
        fs::rename(&pending, &wrong_pending)?;
        assert!(pending_tasks(&queue).is_err());
        fs::rename(&wrong_pending, &pending)?;

        fs::write(queue.join("leases/ambiguouswiki"), b"not a directory")?;
        assert_eq!(recover_stale_at(&queue, 200, 3)?, 0);
        assert!(!queue.join("leases/ambiguouswiki").exists());

        let _lease = claim_at(&queue, ResourceClass::Small, "worker", 10, 201)?
            .context("recovery fixture should be claimable")?;
        let wrong_lease = lease_dir(&queue, "wrongwiki");
        fs::rename(lease_dir(&queue, "nlwiki"), &wrong_lease)?;
        assert_eq!(recover_stale_at(&queue, 300, 3)?, 0);
        assert!(!wrong_lease.exists());

        let atomic_target = root.path().join("atomic-target");
        fs::create_dir(&atomic_target)?;
        assert!(atomic_write_json(&atomic_target, &serde_json::json!({"ok": true})).is_err());
        assert!(
            fs::read_dir(root.path())?
                .filter_map(std::result::Result::ok)
                .all(|entry| !entry.file_name().to_string_lossy().ends_with(".tmp"))
        );
        Ok(())
    }
}
