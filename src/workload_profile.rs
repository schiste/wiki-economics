use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::env;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::snapshot_plan::SnapshotPlan;
use crate::storage::{self, GenerationLayer};

const PROFILE_SCHEMA_VERSION: u32 = 2;
const LEGACY_PROFILE_SCHEMA_VERSION: u32 = 1;
const SELECTION_ALGORITHM_VERSION: &str = "adaptive-workload-profile-v2-measured";
const LEGACY_SELECTION_ALGORITHM_VERSION: &str = "adaptive-workload-profile-v1";
const PROFILE_OVERRIDE_ENV: &str = "WIKI_ECON_WORKLOAD_PROFILE";
const REQUIRE_QUALIFIED_ENV: &str = "WIKI_ECON_REQUIRE_QUALIFIED_PROFILE";
const SMALL_MAX_COMPRESSED_BYTES: u64 = 64 * 1024 * 1024 * 1024;
const SMALL_MAX_SOURCE_COUNT: usize = 64;
const SMALL_MAX_PRIOR_ROWS: u64 = 5_000_000_000;
const SMALL_MAX_FRAGMENT_COUNT: u64 = 2_048;
const SMALL_MAX_HISTORICAL_MEMORY_BYTES: u64 = 4_500_000_000;
const SMALL_MAX_HISTORICAL_SCRATCH_BYTES: u64 = 8 * 1024 * 1024 * 1024;
const SMALL_MAX_ESTIMATED_RUNTIME_SECS: u64 = 6 * 60 * 60;
const CAPACITY_POLICY: &str = include_str!("../config/capacity-qualification.json");
const OBSERVATIONS_SCHEMA_VERSION: u32 = 1;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum WorkloadProfileName {
    Small,
    Large,
}

impl WorkloadProfileName {
    fn parse(value: &str) -> Result<Self> {
        match value {
            "small" => Ok(Self::Small),
            "large" => Ok(Self::Large),
            _ => anyhow::bail!("unsupported workload profile {value:?}; expected small or large"),
        }
    }

    fn parameters(self) -> WorkloadParameters {
        match self {
            Self::Small => WorkloadParameters {
                source_workers: 2,
                primary_buckets: 32,
                secondary_buckets: 8,
            },
            Self::Large => WorkloadParameters {
                source_workers: 3,
                primary_buckets: 64,
                secondary_buckets: 32,
            },
        }
    }

    fn production_eligible(self) -> bool {
        matches!(self, Self::Small)
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Small => "small",
            Self::Large => "large",
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ProfileSelectionMode {
    Automatic,
    ManualQualificationOverride,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct WorkloadSignals {
    pub(crate) total_compressed_bytes: u64,
    pub(crate) source_count: usize,
    pub(crate) prior_measured_rows: Option<u64>,
    #[serde(default)]
    pub(crate) prior_fragment_count: Option<u64>,
    #[serde(default)]
    pub(crate) historical_peak_memory_bytes: Option<u64>,
    #[serde(default)]
    pub(crate) historical_peak_scratch_bytes: Option<u64>,
    #[serde(default)]
    pub(crate) observed_throughput_rows_per_second: Option<u64>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct WorkloadObservations {
    pub(crate) schema_version: u32,
    pub(crate) fragment_count: Option<u64>,
    pub(crate) peak_memory_bytes: Option<u64>,
    pub(crate) peak_scratch_bytes: Option<u64>,
    pub(crate) throughput_rows_per_second: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct WorkloadParameters {
    pub(crate) source_workers: usize,
    pub(crate) primary_buckets: usize,
    pub(crate) secondary_buckets: usize,
}

impl WorkloadParameters {
    pub(crate) fn logical_buckets(&self) -> Result<usize> {
        self.primary_buckets
            .checked_mul(self.secondary_buckets)
            .context("workload logical bucket count overflow")
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct WorkloadProfile {
    pub(crate) schema_version: u32,
    pub(crate) selection_algorithm_version: String,
    pub(crate) wiki: String,
    pub(crate) snapshot: String,
    pub(crate) profile: WorkloadProfileName,
    pub(crate) selection_mode: ProfileSelectionMode,
    pub(crate) signals: WorkloadSignals,
    pub(crate) parameters: WorkloadParameters,
}

#[derive(Deserialize)]
struct CapacityPolicy {
    wikis: BTreeMap<String, WikiCapacityPolicy>,
}

#[derive(Deserialize)]
struct WikiCapacityPolicy {
    required_bucket_counts: Vec<usize>,
    maximum_source_workers: usize,
}

pub(crate) fn profile_path(data_dir: &Path, wiki: &str, snapshot: &str) -> Result<PathBuf> {
    storage::validate_snapshot_version(snapshot)?;
    ensure!(
        !wiki.is_empty()
            && wiki
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_')),
        "unsafe workload-profile wiki identifier"
    );
    Ok(data_dir
        .join("snapshots")
        .join(wiki)
        .join(snapshot)
        .join("workload-profile.json"))
}

pub(crate) fn load(data_dir: &Path, wiki: &str, snapshot: &str) -> Result<Option<WorkloadProfile>> {
    let path = profile_path(data_dir, wiki, snapshot)?;
    if !path.is_file() {
        return Ok(None);
    }
    let profile: WorkloadProfile = serde_json::from_slice(
        &fs::read(&path).with_context(|| format!("failed to read {}", path.display()))?,
    )
    .with_context(|| format!("invalid workload profile JSON in {}", path.display()))?;
    profile.validate(wiki, snapshot)?;
    Ok(Some(profile))
}

pub(crate) fn load_or_select(
    data_dir: &Path,
    plan: &SnapshotPlan,
    source_sizes: &[Option<u64>],
) -> Result<WorkloadProfile> {
    if let Some(profile) = load(data_dir, plan.wiki.as_str(), plan.snapshot.as_str())? {
        return Ok(profile);
    }
    ensure!(
        source_sizes.len() == plan.sources.len(),
        "workload sizing source inventory does not match the snapshot plan"
    );
    let total_compressed_bytes = source_sizes.iter().try_fold(0_u64, |total, size| {
        let size = size.context("workload sizing requires every compressed source size")?;
        ensure!(size > 0, "workload sizing found a zero-byte source");
        total
            .checked_add(size)
            .context("workload compressed byte total overflow")
    })?;
    let wiki = plan.wiki.as_str();
    let snapshot = plan.snapshot.as_str();
    let prior_measured_rows = prior_measured_rows(data_dir, wiki, snapshot)?;
    let observations = load_observations(data_dir, wiki)?;
    let signals = WorkloadSignals {
        total_compressed_bytes,
        source_count: plan.sources.len(),
        prior_measured_rows,
        prior_fragment_count: observations.fragment_count,
        historical_peak_memory_bytes: observations.peak_memory_bytes,
        historical_peak_scratch_bytes: observations.peak_scratch_bytes,
        observed_throughput_rows_per_second: observations.throughput_rows_per_second,
    };
    let override_name = env::var(PROFILE_OVERRIDE_ENV).ok();
    let (profile, selection_mode) = select_with_override(&signals, override_name.as_deref())?;
    let selected = WorkloadProfile {
        schema_version: PROFILE_SCHEMA_VERSION,
        selection_algorithm_version: SELECTION_ALGORITHM_VERSION.to_string(),
        wiki: wiki.to_string(),
        snapshot: snapshot.to_string(),
        profile,
        selection_mode,
        signals,
        parameters: profile.parameters(),
    };
    selected.validate(wiki, snapshot)?;
    let path = profile_path(data_dir, wiki, snapshot)?;
    write_atomic(&path, &selected)?;
    Ok(selected)
}

fn select_with_override(
    signals: &WorkloadSignals,
    override_name: Option<&str>,
) -> Result<(WorkloadProfileName, ProfileSelectionMode)> {
    Ok(match override_name {
        Some(value) => (
            WorkloadProfileName::parse(value)?,
            ProfileSelectionMode::ManualQualificationOverride,
        ),
        None => (select_automatic(signals), ProfileSelectionMode::Automatic),
    })
}

fn select_automatic(signals: &WorkloadSignals) -> WorkloadProfileName {
    let prior_rows_fit = signals
        .prior_measured_rows
        .is_none_or(|rows| rows <= SMALL_MAX_PRIOR_ROWS);
    let estimated_runtime_fits = match (
        signals.prior_measured_rows,
        signals.observed_throughput_rows_per_second,
    ) {
        (Some(rows), Some(throughput)) if throughput > 0 => {
            rows.div_ceil(throughput) <= SMALL_MAX_ESTIMATED_RUNTIME_SECS
        }
        _ => true,
    };
    if signals.total_compressed_bytes <= SMALL_MAX_COMPRESSED_BYTES
        && signals.source_count <= SMALL_MAX_SOURCE_COUNT
        && prior_rows_fit
        && estimated_runtime_fits
        && signals
            .prior_fragment_count
            .is_none_or(|count| count <= SMALL_MAX_FRAGMENT_COUNT)
        && signals
            .historical_peak_memory_bytes
            .is_none_or(|bytes| bytes <= SMALL_MAX_HISTORICAL_MEMORY_BYTES)
        && signals
            .historical_peak_scratch_bytes
            .is_none_or(|bytes| bytes <= SMALL_MAX_HISTORICAL_SCRATCH_BYTES)
    {
        WorkloadProfileName::Small
    } else {
        WorkloadProfileName::Large
    }
}

impl WorkloadProfile {
    pub(crate) fn validate(&self, wiki: &str, snapshot: &str) -> Result<()> {
        let supported_schema = self.schema_version == PROFILE_SCHEMA_VERSION
            || self.schema_version == LEGACY_PROFILE_SCHEMA_VERSION;
        ensure!(
            supported_schema,
            "unsupported workload profile schema {}",
            self.schema_version
        );
        ensure!(
            (self.schema_version == PROFILE_SCHEMA_VERSION
                && self.selection_algorithm_version == SELECTION_ALGORITHM_VERSION)
                || (self.schema_version == LEGACY_PROFILE_SCHEMA_VERSION
                    && self.selection_algorithm_version == LEGACY_SELECTION_ALGORITHM_VERSION),
            "unsupported workload profile selection algorithm"
        );
        ensure!(
            self.wiki == wiki && self.snapshot == snapshot,
            "workload profile identity does not match {wiki}/{snapshot}"
        );
        ensure!(
            self.signals.source_count > 0,
            "workload source count is zero"
        );
        ensure!(
            self.signals.total_compressed_bytes > 0,
            "workload compressed byte total is zero"
        );
        ensure!(
            self.parameters == self.profile.parameters(),
            "workload profile parameters do not match the named profile"
        );
        if self.selection_mode == ProfileSelectionMode::Automatic
            && self.schema_version == PROFILE_SCHEMA_VERSION
        {
            ensure!(
                self.profile == select_automatic(&self.signals),
                "automatic workload profile does not match its recorded sizing signals"
            );
        }
        self.parameters.logical_buckets()?;
        Ok(())
    }

    pub(crate) fn ensure_compute_qualified(&self) -> Result<()> {
        self.ensure_compute_qualified_with(require_qualified()?)
    }

    fn ensure_compute_qualified_with(&self, required: bool) -> Result<()> {
        if !required {
            return Ok(());
        }
        ensure!(
            self.selection_mode == ProfileSelectionMode::Automatic,
            "production rejects a manually overridden workload profile"
        );
        ensure!(
            self.profile.production_eligible(),
            "workload profile {:?} has not completed production qualification",
            self.profile
        );
        let policy: CapacityPolicy = serde_json::from_str(CAPACITY_POLICY)?;
        let wiki = policy
            .wikis
            .get(&self.wiki)
            .with_context(|| format!("{} has no production capacity qualification", self.wiki))?;
        let logical_buckets = self.parameters.logical_buckets()?;
        ensure!(
            wiki.required_bucket_counts.contains(&logical_buckets),
            "workload layout {}x{} ({logical_buckets} logical buckets) is not qualified for {}",
            self.parameters.primary_buckets,
            self.parameters.secondary_buckets,
            self.wiki
        );
        Ok(())
    }

    pub(crate) fn ensure_source_qualified(&self, effective_source_workers: usize) -> Result<()> {
        self.ensure_source_qualified_with(effective_source_workers, require_qualified()?)
    }

    fn ensure_source_qualified_with(
        &self,
        effective_source_workers: usize,
        required: bool,
    ) -> Result<()> {
        self.ensure_compute_qualified_with(required)?;
        if !required {
            return Ok(());
        }
        let policy: CapacityPolicy = serde_json::from_str(CAPACITY_POLICY)?;
        let wiki = policy
            .wikis
            .get(&self.wiki)
            .context("production capacity qualification disappeared")?;
        ensure!(
            effective_source_workers <= wiki.maximum_source_workers,
            "effective source concurrency {effective_source_workers} exceeds the qualified maximum of {} for {}",
            wiki.maximum_source_workers,
            self.wiki
        );
        Ok(())
    }

    pub(crate) fn algorithm_version(&self) -> Result<String> {
        Ok(format!(
            "{}-{}",
            self.selection_algorithm_version,
            self.profile.as_str()
        ))
    }
}

pub(crate) fn record_observations(
    data_dir: &Path,
    wiki: &str,
    incoming: WorkloadObservations,
) -> Result<()> {
    ensure!(
        incoming.schema_version == OBSERVATIONS_SCHEMA_VERSION,
        "unsupported workload observation schema"
    );
    let mut current = load_observations(data_dir, wiki)?;
    current.fragment_count = maximum(current.fragment_count, incoming.fragment_count);
    current.peak_memory_bytes = maximum(current.peak_memory_bytes, incoming.peak_memory_bytes);
    current.peak_scratch_bytes = maximum(current.peak_scratch_bytes, incoming.peak_scratch_bytes);
    current.throughput_rows_per_second = minimum_nonzero(
        current.throughput_rows_per_second,
        incoming.throughput_rows_per_second,
    );
    write_atomic_value(&observations_path(data_dir, wiki)?, &current)
}

fn observations_path(data_dir: &Path, wiki: &str) -> Result<PathBuf> {
    ensure!(
        !wiki.is_empty()
            && wiki
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_')),
        "unsafe workload observation wiki"
    );
    Ok(data_dir
        .join("workload-observations")
        .join(format!("{wiki}.json")))
}

fn load_observations(data_dir: &Path, wiki: &str) -> Result<WorkloadObservations> {
    let path = observations_path(data_dir, wiki)?;
    if !path.is_file() {
        return Ok(WorkloadObservations {
            schema_version: OBSERVATIONS_SCHEMA_VERSION,
            ..WorkloadObservations::default()
        });
    }
    let observations: WorkloadObservations = serde_json::from_slice(&fs::read(&path)?)?;
    ensure!(
        observations.schema_version == OBSERVATIONS_SCHEMA_VERSION,
        "unsupported workload observation schema"
    );
    Ok(observations)
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

pub(crate) fn require_qualified() -> Result<bool> {
    let value = env::var(REQUIRE_QUALIFIED_ENV).ok();
    require_qualified_from(value.as_deref())
}

fn require_qualified_from(value: Option<&str>) -> Result<bool> {
    match value {
        None | Some("0" | "false") => Ok(false),
        Some("1" | "true") => Ok(true),
        Some(value) => anyhow::bail!(
            "invalid {REQUIRE_QUALIFIED_ENV} value {value:?}; expected 0, 1, false, or true"
        ),
    }
}

fn prior_measured_rows(data_dir: &Path, wiki: &str, snapshot: &str) -> Result<Option<u64>> {
    let selected_manifest = storage::generation_manifest_path(data_dir, wiki, snapshot)?;
    let version = storage::current_snapshot_version(data_dir, wiki)?
        .or_else(|| selected_manifest.is_file().then(|| snapshot.to_string()));
    let Some(version) = version else {
        return Ok(None);
    };
    let manifest = match storage::ensure_generation_manifest(data_dir, wiki, &version) {
        Ok(manifest) => manifest,
        Err(error) => {
            let retention_path = crate::retention::receipt_path(data_dir, wiki, &version)?;
            if !retention_path.is_file() {
                return Err(error);
            }
            crate::retention::validate_purged_snapshot(data_dir, wiki, &version).with_context(
                || {
                    format!(
                        "current snapshot {version} for {wiki} is unavailable and its retention proof is invalid"
                    )
                },
            )?;
            tracing::info!(
                wiki,
                current_snapshot = version,
                requested_snapshot = snapshot,
                "ignoring deliberately purged prior generation while sizing new work"
            );
            return Ok(None);
        }
    };
    manifest
        .fragments
        .iter()
        .filter(|fragment| fragment.layer == GenerationLayer::Warehouse)
        .try_fold(0_u64, |total, fragment| {
            total
                .checked_add(fragment.rows)
                .context("prior measured row total overflow")
        })
        .map(Some)
}

fn write_atomic(path: &Path, profile: &WorkloadProfile) -> Result<()> {
    write_atomic_value(path, profile)
}

fn write_atomic_value(path: &Path, value: &impl Serialize) -> Result<()> {
    let parent = path.parent().context("workload profile has no parent")?;
    fs::create_dir_all(parent)?;
    let temporary = parent.join(format!(".workload-profile.{}.tmp", std::process::id()));
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::TestDir;

    fn signals(bytes: u64, sources: usize, rows: Option<u64>) -> WorkloadSignals {
        WorkloadSignals {
            total_compressed_bytes: bytes,
            source_count: sources,
            prior_measured_rows: rows,
            prior_fragment_count: None,
            historical_peak_memory_bytes: None,
            historical_peak_scratch_bytes: None,
            observed_throughput_rows_per_second: None,
        }
    }

    fn profile(
        wiki: &str,
        name: WorkloadProfileName,
        mode: ProfileSelectionMode,
    ) -> WorkloadProfile {
        WorkloadProfile {
            schema_version: PROFILE_SCHEMA_VERSION,
            selection_algorithm_version: SELECTION_ALGORITHM_VERSION.to_string(),
            wiki: wiki.to_string(),
            snapshot: "2026-08".to_string(),
            profile: name,
            selection_mode: mode,
            signals: signals(1, 1, None),
            parameters: name.parameters(),
        }
    }

    #[test]
    fn automatic_selection_uses_every_sizing_signal() {
        assert_eq!(
            select_automatic(&signals(
                SMALL_MAX_COMPRESSED_BYTES,
                64,
                Some(5_000_000_000)
            )),
            WorkloadProfileName::Small
        );
        assert_eq!(
            select_automatic(&signals(SMALL_MAX_COMPRESSED_BYTES + 1, 1, None)),
            WorkloadProfileName::Large
        );
        assert_eq!(
            select_automatic(&signals(1, SMALL_MAX_SOURCE_COUNT + 1, None)),
            WorkloadProfileName::Large
        );
        assert_eq!(
            select_automatic(&signals(1, 1, Some(SMALL_MAX_PRIOR_ROWS + 1))),
            WorkloadProfileName::Large
        );
        let mut measured = signals(1, 1, Some(1));
        measured.prior_fragment_count = Some(SMALL_MAX_FRAGMENT_COUNT + 1);
        assert_eq!(select_automatic(&measured), WorkloadProfileName::Large);
        measured.prior_fragment_count = Some(1);
        measured.historical_peak_memory_bytes = Some(SMALL_MAX_HISTORICAL_MEMORY_BYTES + 1);
        assert_eq!(select_automatic(&measured), WorkloadProfileName::Large);
        measured.historical_peak_memory_bytes = Some(1);
        measured.historical_peak_scratch_bytes = Some(SMALL_MAX_HISTORICAL_SCRATCH_BYTES + 1);
        assert_eq!(select_automatic(&measured), WorkloadProfileName::Large);
        measured.historical_peak_scratch_bytes = Some(1);
        measured.prior_measured_rows = Some(1_000_000_000);
        measured.observed_throughput_rows_per_second = Some(1_000);
        assert_eq!(select_automatic(&measured), WorkloadProfileName::Large);
    }

    #[test]
    fn measured_observations_merge_monotonically_and_seed_new_profiles() -> Result<()> {
        let root = TestDir::new()?;
        let initial = WorkloadObservations {
            schema_version: OBSERVATIONS_SCHEMA_VERSION,
            fragment_count: Some(100),
            peak_memory_bytes: Some(200),
            peak_scratch_bytes: Some(300),
            throughput_rows_per_second: Some(400),
        };
        record_observations(root.path(), "testwiki", initial)?;
        let update = WorkloadObservations {
            schema_version: OBSERVATIONS_SCHEMA_VERSION,
            fragment_count: Some(50),
            peak_memory_bytes: Some(250),
            peak_scratch_bytes: None,
            throughput_rows_per_second: Some(350),
        };
        record_observations(root.path(), "testwiki", update)?;
        let observed = load_observations(root.path(), "testwiki")?;
        assert_eq!(observed.fragment_count, Some(100));
        assert_eq!(observed.peak_memory_bytes, Some(250));
        assert_eq!(observed.peak_scratch_bytes, Some(300));
        assert_eq!(observed.throughput_rows_per_second, Some(350));

        let plan = SnapshotPlan::resolve("testwiki", "2026-08")?;
        let profile = load_or_select(root.path(), &plan, &[Some(42)])?;
        assert_eq!(profile.signals.prior_fragment_count, Some(100));
        assert_eq!(profile.signals.historical_peak_memory_bytes, Some(250));
        Ok(())
    }

    #[test]
    fn legacy_profiles_remain_readable_during_schema_two_rollout() -> Result<()> {
        let root = TestDir::new()?;
        let mut legacy = profile(
            "testwiki",
            WorkloadProfileName::Small,
            ProfileSelectionMode::Automatic,
        );
        legacy.schema_version = LEGACY_PROFILE_SCHEMA_VERSION;
        legacy.selection_algorithm_version = LEGACY_SELECTION_ALGORITHM_VERSION.to_string();
        legacy.signals.prior_fragment_count = None;
        legacy.signals.historical_peak_memory_bytes = None;
        legacy.signals.historical_peak_scratch_bytes = None;
        legacy.signals.observed_throughput_rows_per_second = None;
        let path = profile_path(root.path(), "testwiki", "2026-08")?;
        write_atomic(&path, &legacy)?;
        assert_eq!(load(root.path(), "testwiki", "2026-08")?, Some(legacy));
        Ok(())
    }

    #[test]
    fn persisted_profile_is_immutable_and_strict() -> Result<()> {
        let root = TestDir::new()?;
        let plan = SnapshotPlan::resolve("testwiki", "2026-08")?;
        let first = load_or_select(root.path(), &plan, &[Some(42)])?;
        assert_eq!(first.profile, WorkloadProfileName::Small);
        let second = load_or_select(root.path(), &plan, &[Some(u64::MAX)])?;
        assert_eq!(second, first);

        let path = profile_path(root.path(), "testwiki", "2026-08")?;
        let mut value: serde_json::Value = serde_json::from_slice(&fs::read(&path)?)?;
        value["parameters"]["primary_buckets"] = serde_json::json!(64);
        fs::write(&path, serde_json::to_vec(&value)?)?;
        assert!(load(root.path(), "testwiki", "2026-08").is_err());
        Ok(())
    }

    #[test]
    fn prior_rows_prefer_the_published_generation_over_a_partial_candidate() -> Result<()> {
        let root = TestDir::new()?;
        let wiki = "testwiki";
        let current = "2026-07";
        let (plan, _) = SnapshotPlan::load_or_resolve(root.path(), wiki, current)?;
        let analytical = storage::snapshot_analytical_wiki_dir(root.path(), wiki, current)?;
        storage::write_test_marker_in(root.path(), &analytical, &plan.sources[0].source_id)?;
        storage::write_generation_manifest(root.path(), wiki, current)?;
        storage::publish_current_snapshot(root.path(), wiki, current)?;

        let candidate = "2026-08";
        let candidate_manifest = storage::generation_manifest_path(root.path(), wiki, candidate)?;
        let candidate_parent = candidate_manifest
            .parent()
            .expect("candidate manifest parent");
        fs::create_dir_all(candidate_parent).expect("create candidate manifest parent");
        fs::write(candidate_manifest, b"{partial")?;

        assert_eq!(prior_measured_rows(root.path(), wiki, candidate)?, Some(1));
        Ok(())
    }

    #[test]
    fn prior_rows_strictly_migrate_a_selected_pre_manifest_generation() -> Result<()> {
        let root = TestDir::new()?;
        let wiki = "testwiki";
        let current = "2026-07";
        let (plan, _) = SnapshotPlan::load_or_resolve(root.path(), wiki, current)?;
        let analytical = storage::snapshot_analytical_wiki_dir(root.path(), wiki, current)?;
        storage::write_test_marker_in(root.path(), &analytical, &plan.sources[0].source_id)?;
        let manifest = storage::write_generation_manifest(root.path(), wiki, current)?;
        storage::publish_current_snapshot(root.path(), wiki, current)?;
        fs::remove_file(&manifest)?;

        assert_eq!(prior_measured_rows(root.path(), wiki, current)?, Some(1));
        assert!(manifest.is_file());
        storage::read_generation_manifest(root.path(), wiki, current)?;
        Ok(())
    }

    #[test]
    fn prior_rows_treat_authorized_purged_generation_as_an_optional_hint() -> Result<()> {
        let root = TestDir::new()?;
        let wiki = "testwiki";
        let current = "2026-07";
        let candidate = "2026-08";
        let (plan, _) = SnapshotPlan::load_or_resolve(root.path(), wiki, current)?;
        let analytical = storage::snapshot_analytical_wiki_dir(root.path(), wiki, current)?;
        storage::write_test_marker_in(root.path(), &analytical, &plan.sources[0].source_id)?;
        storage::write_generation_manifest(root.path(), wiki, current)?;
        storage::publish_current_snapshot(root.path(), wiki, current)?;
        let (_, source_plan_sha256) = storage::sha256_file(&crate::snapshot_plan::plan_path(
            root.path(),
            wiki,
            current,
        )?)?;
        crate::retention::audit_or_apply(
            root.path(),
            crate::retention::RetentionAuthorization {
                wiki: wiki.to_string(),
                snapshot: current.to_string(),
                ready_sha256: "a".repeat(64),
                source_plan_sha256,
                policy: crate::retention::RetentionPolicy {
                    source_recoverability: crate::retention::SourceRecoverability::Redownloadable,
                    history_input: crate::retention::InputRetention::PurgeAfterReady,
                    patrol_source: crate::retention::InputRetention::Retain,
                    computed_rollback_generations: 1,
                },
            },
            true,
        )?;

        assert_eq!(
            storage::current_snapshot_version(root.path(), wiki)?.as_deref(),
            Some(current)
        );
        assert_eq!(prior_measured_rows(root.path(), wiki, candidate)?, None);

        let retention_path = crate::retention::receipt_path(root.path(), wiki, current)?;
        fs::write(&retention_path, b"{truncated")?;
        assert!(prior_measured_rows(root.path(), wiki, candidate).is_err());
        Ok(())
    }

    #[test]
    fn invalid_profile_inputs_fail_closed() -> Result<()> {
        let root = TestDir::new()?;
        let plan = SnapshotPlan::resolve("testwiki", "2026-08")?;
        assert!(load_or_select(root.path(), &plan, &[]).is_err());
        assert!(load_or_select(root.path(), &plan, &[None]).is_err());
        assert!(load_or_select(root.path(), &plan, &[Some(0)]).is_err());
        let monthly = SnapshotPlan::resolve("enwiki", "2001-02")?;
        assert!(load_or_select(root.path(), &monthly, &[Some(u64::MAX), Some(1)]).is_err());
        assert!(profile_path(root.path(), "../bad", "2026-08").is_err());
        Ok(())
    }

    #[test]
    fn production_qualification_checks_capacity_registry() -> Result<()> {
        let selected = profile(
            "nlwiki",
            WorkloadProfileName::Small,
            ProfileSelectionMode::Automatic,
        );
        selected.validate("nlwiki", "2026-08")?;
        assert!(selected.parameters.logical_buckets()? == 256);
        assert!(
            selected
                .algorithm_version()?
                .contains("adaptive-workload-profile-v2-measured-small")
        );
        selected.ensure_compute_qualified_with(false)?;
        selected.ensure_compute_qualified_with(true)?;
        selected.ensure_source_qualified_with(1, true)?;
        assert!(selected.ensure_source_qualified_with(2, true).is_err());

        let manual = profile(
            "nlwiki",
            WorkloadProfileName::Small,
            ProfileSelectionMode::ManualQualificationOverride,
        );
        manual.validate("nlwiki", "2026-08")?;
        assert!(manual.ensure_compute_qualified_with(true).is_err());
        let large = profile(
            "nlwiki",
            WorkloadProfileName::Large,
            ProfileSelectionMode::Automatic,
        );
        assert!(large.ensure_compute_qualified_with(true).is_err());
        let unknown = profile(
            "testwiki",
            WorkloadProfileName::Small,
            ProfileSelectionMode::Automatic,
        );
        assert!(unknown.ensure_compute_qualified_with(true).is_err());
        let mut unqualified_layout = selected.clone();
        unqualified_layout.parameters.primary_buckets = 64;
        assert!(
            unqualified_layout
                .ensure_compute_qualified_with(true)
                .is_err()
        );
        Ok(())
    }

    #[test]
    fn manual_selection_and_boolean_policy_parsing_are_strict() -> Result<()> {
        let sample = signals(1, 1, None);
        assert_eq!(
            select_with_override(&sample, Some("small"))?,
            (
                WorkloadProfileName::Small,
                ProfileSelectionMode::ManualQualificationOverride
            )
        );
        assert_eq!(
            select_with_override(&sample, Some("large"))?,
            (
                WorkloadProfileName::Large,
                ProfileSelectionMode::ManualQualificationOverride
            )
        );
        assert!(select_with_override(&sample, Some("unknown")).is_err());
        for value in [None, Some("0"), Some("false")] {
            assert!(!require_qualified_from(value)?);
        }
        for value in [Some("1"), Some("true")] {
            assert!(require_qualified_from(value)?);
        }
        assert!(require_qualified_from(Some("yes")).is_err());
        assert_eq!(WorkloadProfileName::Large.as_str(), "large");
        Ok(())
    }

    #[test]
    fn validation_and_atomic_publication_fail_closed() -> Result<()> {
        let mut automatic = profile(
            "nlwiki",
            WorkloadProfileName::Small,
            ProfileSelectionMode::Automatic,
        );
        automatic.profile = WorkloadProfileName::Large;
        automatic.parameters = WorkloadProfileName::Large.parameters();
        assert!(automatic.validate("nlwiki", "2026-08").is_err());

        let root = TestDir::new()?;
        let target = root.path().join("profile.json");
        fs::create_dir(&target)?;
        fs::write(target.join("keep"), b"not replaceable")?;
        assert!(write_atomic(&target, &automatic).is_err());
        assert!(
            !root
                .path()
                .join(format!(".workload-profile.{}.tmp", std::process::id()))
                .exists()
        );
        Ok(())
    }
}
