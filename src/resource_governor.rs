use anyhow::{Context, Result};
use serde::Serialize;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tracing::info;

use crate::observability::MemorySnapshot;

pub(crate) const MEMORY_CEILING_ENV: &str = "WIKI_ECON_MEMORY_CEILING_BYTES";
pub(crate) const MEMORY_RESERVE_ENV: &str = "WIKI_ECON_MEMORY_RESERVE_BYTES";
pub(crate) const PERSISTENT_RESERVE_ENV: &str = "WIKI_ECON_PERSISTENT_STORAGE_RESERVE_BYTES";
pub(crate) const BOUNDED_SCRATCH_RESERVE_ENV: &str = "WIKI_ECON_BOUNDED_SCRATCH_RESERVE_BYTES";
pub(crate) const ROLLBACK_GENERATION_RESERVE_ENV: &str =
    "WIKI_ECON_ROLLBACK_GENERATION_RESERVE_BYTES";
pub(crate) const SCRATCH_LIMIT_ENV: &str = "WIKI_ECON_SCRATCH_LIMIT_BYTES";
pub(crate) const MAX_OPEN_FILES_ENV: &str = "WIKI_ECON_MAX_OPEN_FILES";
pub(crate) const SOURCE_WORKERS_ENV: &str = "WIKI_ECON_SOURCE_WORKERS";
pub(crate) const THREAD_LIMIT_ENV: &str = "WIKI_ECON_THREAD_LIMIT";
pub(crate) const MAX_LOGICAL_PARTITION_ENV: &str = "WIKI_ECON_MAX_LOGICAL_PARTITION_BYTES";
pub(crate) const MAX_PARQUET_WRITERS_ENV: &str = "WIKI_ECON_MAX_ACTIVE_PARQUET_WRITERS";

const GIB: u64 = 1024 * 1024 * 1024;
const DEFAULT_MEMORY_CEILING_BYTES: u64 = 16 * GIB;
const DEFAULT_SCRATCH_LIMIT_BYTES: u64 = 64 * GIB;
const DEFAULT_MAX_OPEN_FILES: usize = 512;
const DEFAULT_MAX_LOGICAL_PARTITION_BYTES: u64 = 8 * GIB;
const DEFAULT_MAX_ACTIVE_PARQUET_WRITERS: usize = 16;
const SOURCE_FD_ALLOWANCE: usize = 8;

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub(crate) struct ResourceBudget {
    pub(crate) memory_ceiling_bytes: u64,
    pub(crate) memory_reserve_bytes: u64,
    pub(crate) persistent_storage_reserve_bytes: u64,
    pub(crate) bounded_scratch_reserve_bytes: u64,
    pub(crate) rollback_generation_reserve_bytes: u64,
    pub(crate) scratch_limit_bytes: u64,
    pub(crate) max_open_files: usize,
    pub(crate) source_worker_limit: usize,
    pub(crate) thread_limit: usize,
    pub(crate) max_logical_partition_bytes: u64,
    pub(crate) max_active_parquet_writers: usize,
}

impl ResourceBudget {
    pub(crate) fn from_environment() -> Result<Self> {
        let detected_limit = MemorySnapshot::capture().cgroup_limit_bytes;
        let memory_ceiling_bytes = parse_u64_env(MEMORY_CEILING_ENV)?
            .or(detected_limit)
            .unwrap_or(DEFAULT_MEMORY_CEILING_BYTES);
        let memory_reserve_bytes =
            parse_u64_env(MEMORY_RESERVE_ENV)?.unwrap_or(memory_ceiling_bytes / 4);
        let thread_limit = parse_usize_env(THREAD_LIMIT_ENV)?
            .or(parse_usize_env("RAYON_NUM_THREADS")?)
            .or(parse_usize_env("POLARS_MAX_THREADS")?)
            .unwrap_or(1);
        let budget = Self {
            memory_ceiling_bytes,
            memory_reserve_bytes,
            persistent_storage_reserve_bytes: parse_u64_env(PERSISTENT_RESERVE_ENV)?.unwrap_or(0),
            bounded_scratch_reserve_bytes: parse_u64_env(BOUNDED_SCRATCH_RESERVE_ENV)?.unwrap_or(0),
            rollback_generation_reserve_bytes: parse_u64_env(ROLLBACK_GENERATION_RESERVE_ENV)?
                .unwrap_or(0),
            scratch_limit_bytes: parse_u64_env(SCRATCH_LIMIT_ENV)?
                .unwrap_or(DEFAULT_SCRATCH_LIMIT_BYTES),
            max_open_files: parse_usize_env(MAX_OPEN_FILES_ENV)?.unwrap_or(DEFAULT_MAX_OPEN_FILES),
            source_worker_limit: parse_usize_env(SOURCE_WORKERS_ENV)?.unwrap_or(1),
            thread_limit,
            max_logical_partition_bytes: parse_u64_env(MAX_LOGICAL_PARTITION_ENV)?
                .unwrap_or(DEFAULT_MAX_LOGICAL_PARTITION_BYTES),
            max_active_parquet_writers: parse_usize_env(MAX_PARQUET_WRITERS_ENV)?
                .unwrap_or(DEFAULT_MAX_ACTIVE_PARQUET_WRITERS),
        };
        budget.validate()?;
        budget.validate_external_thread_pools()?;
        Ok(budget)
    }

    fn validate(&self) -> Result<()> {
        anyhow::ensure!(
            self.memory_ceiling_bytes > 0,
            "memory ceiling must be positive"
        );
        anyhow::ensure!(
            self.memory_reserve_bytes < self.memory_ceiling_bytes,
            "memory reserve must be smaller than the memory ceiling"
        );
        anyhow::ensure!(
            self.scratch_limit_bytes > 0,
            "scratch limit must be positive"
        );
        anyhow::ensure!(
            self.max_open_files > SOURCE_FD_ALLOWANCE,
            "maximum open files must exceed the per-source allowance of {SOURCE_FD_ALLOWANCE}"
        );
        anyhow::ensure!(
            self.source_worker_limit > 0,
            "source worker limit must be positive"
        );
        anyhow::ensure!(self.thread_limit > 0, "thread limit must be positive");
        anyhow::ensure!(
            self.max_logical_partition_bytes > 0,
            "maximum logical partition size must be positive"
        );
        anyhow::ensure!(
            self.max_active_parquet_writers > 0,
            "maximum active Parquet writers must be positive"
        );
        Ok(())
    }

    fn validate_external_thread_pools(&self) -> Result<()> {
        self.validate_external_thread_pool_values([
            ("RAYON_NUM_THREADS", parse_usize_env("RAYON_NUM_THREADS")?),
            ("POLARS_MAX_THREADS", parse_usize_env("POLARS_MAX_THREADS")?),
        ])
    }

    fn validate_external_thread_pool_values(
        &self,
        values: [(&str, Option<usize>); 2],
    ) -> Result<()> {
        for (name, configured) in values {
            if let Some(configured) = configured {
                anyhow::ensure!(
                    configured <= self.thread_limit,
                    "{name}={configured} exceeds resource-governor thread limit {}",
                    self.thread_limit
                );
            }
        }
        Ok(())
    }

    pub(crate) fn memory_admission_bytes(&self) -> u64 {
        self.memory_ceiling_bytes - self.memory_reserve_bytes
    }

    fn persistent_admission_reserve_bytes(&self) -> Result<u64> {
        self.persistent_storage_reserve_bytes
            .checked_add(self.bounded_scratch_reserve_bytes)
            .and_then(|value| value.checked_add(self.rollback_generation_reserve_bytes))
            .context("persistent storage reserve overflow")
    }
}

#[derive(Clone, Debug)]
pub(crate) struct GovernorPaths {
    persistent_root: PathBuf,
    scratch_root: Option<PathBuf>,
}

impl GovernorPaths {
    pub(crate) fn new(persistent_root: PathBuf, scratch_root: Option<PathBuf>) -> Self {
        Self {
            persistent_root,
            scratch_root,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
pub(crate) struct CpuSnapshot {
    pub(crate) usage_usec: Option<u64>,
    pub(crate) throttled_usec: Option<u64>,
    pub(crate) nr_throttled: Option<u64>,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct ResourceSample {
    pub(crate) sampled_at_epoch_ms: u128,
    pub(crate) memory: MemorySnapshot,
    pub(crate) cpu: CpuSnapshot,
    pub(crate) scratch_bytes: u64,
    pub(crate) persistent_filesystem_used_bytes: Option<u64>,
    pub(crate) persistent_available_bytes: Option<u64>,
    pub(crate) open_file_descriptors: Option<usize>,
    pub(crate) active_source_workers: usize,
    pub(crate) reserved_persistent_bytes: u64,
    pub(crate) downloaded_bytes: u64,
    pub(crate) ingested_rows: u64,
    pub(crate) download_elapsed_ms: u64,
    pub(crate) ingest_elapsed_ms: u64,
    pub(crate) download_bytes_per_second: Option<u64>,
    pub(crate) ingest_rows_per_second: Option<u64>,
}

#[derive(Debug, Default)]
struct GovernorState {
    active_source_workers: usize,
    reserved_persistent_bytes: u64,
    downloaded_bytes: u64,
    ingested_rows: u64,
    download_elapsed_ms: u64,
    ingest_elapsed_ms: u64,
}

#[derive(Clone, Debug)]
pub(crate) struct ResourceGovernor {
    budget: ResourceBudget,
    paths: GovernorPaths,
    state: Arc<Mutex<GovernorState>>,
}

impl ResourceGovernor {
    pub(crate) fn from_environment(paths: GovernorPaths) -> Result<Self> {
        Ok(Self::new(ResourceBudget::from_environment()?, paths))
    }

    pub(crate) fn new(budget: ResourceBudget, paths: GovernorPaths) -> Self {
        Self {
            budget,
            paths,
            state: Arc::new(Mutex::new(GovernorState::default())),
        }
    }

    pub(crate) fn budget(&self) -> &ResourceBudget {
        &self.budget
    }

    pub(crate) fn preflight_snapshot(
        &self,
        source_bytes: &[Option<u64>],
        maximum_window_sources: usize,
    ) -> Result<ResourceSample> {
        let unknown = source_bytes.iter().filter(|value| value.is_none()).count();
        anyhow::ensure!(
            unknown == 0,
            "resource preflight cannot prove snapshot storage fit: {unknown} source sizes are unknown"
        );
        let total_source_bytes = checked_sum(source_bytes.iter().flatten().copied())?;
        let mut ordered = source_bytes.iter().flatten().copied().collect::<Vec<_>>();
        ordered.sort_unstable_by(|left, right| right.cmp(left));
        let window_limit = maximum_window_sources.min(self.budget.source_worker_limit);
        let window_bytes = checked_sum(ordered.into_iter().take(window_limit))?;
        let estimated_additional = total_source_bytes
            .checked_add(window_bytes)
            .context("snapshot storage estimate overflow")?;
        let sample = self.sample()?;
        let available = sample
            .persistent_available_bytes
            .context("resource preflight requires persistent filesystem availability")?;
        let fixed_reserve = self.budget.persistent_admission_reserve_bytes()?;
        let required = fixed_reserve
            .checked_add(estimated_additional)
            .context("snapshot storage requirement overflow")?;
        anyhow::ensure!(
            available >= required,
            "resource preflight rejected snapshot: {available} persistent bytes available, {required} required (including {fixed_reserve} safety/scratch/rollback reserve and {estimated_additional} estimated raw-window/candidate bytes)"
        );
        self.validate_sample(&sample, 0)?;
        info!(
            budget = %serde_json::to_string(&self.budget)?,
            sample = %serde_json::to_string(&sample)?,
            total_source_bytes,
            window_bytes,
            estimated_additional,
            "resource governor accepted snapshot preflight"
        );
        Ok(sample)
    }

    pub(crate) fn validate_logical_partition(&self, identity: &str, bytes: u64) -> Result<()> {
        anyhow::ensure!(
            bytes <= self.budget.max_logical_partition_bytes,
            "logical partition {identity} is {bytes} bytes, above the governed maximum of {} bytes",
            self.budget.max_logical_partition_bytes
        );
        Ok(())
    }

    pub(crate) fn admit_source(&self, expected_raw_bytes: u64) -> Result<SourcePermit> {
        let mut state = self.state.lock().expect("resource governor mutex poisoned");
        anyhow::ensure!(
            state.active_source_workers < self.budget.source_worker_limit,
            "resource governor source-worker limit reached"
        );
        let sample = self.sample_with_state(&state)?;
        let additional_bytes = state
            .reserved_persistent_bytes
            .checked_add(expected_raw_bytes)
            .context("source storage reservation overflow")?;
        self.validate_sample(&sample, additional_bytes)?;
        state.active_source_workers += 1;
        state.reserved_persistent_bytes = additional_bytes;
        info!(sample = %serde_json::to_string(&sample)?, "resource governor admitted source");
        Ok(SourcePermit {
            governor: self.clone(),
            reserved_persistent_bytes: expected_raw_bytes,
            started: Instant::now(),
            completed: false,
        })
    }

    pub(crate) fn sample(&self) -> Result<ResourceSample> {
        let state = self.state.lock().expect("resource governor mutex poisoned");
        self.sample_with_state(&state)
    }

    pub(crate) fn checkpoint(&self, stage: &str) -> Result<ResourceSample> {
        let sample = self.sample()?;
        self.validate_sample(&sample, 0)?;
        info!(
            stage,
            sample = %serde_json::to_string(&sample)?,
            "resource governor checkpoint"
        );
        Ok(sample)
    }

    fn sample_with_state(&self, state: &GovernorState) -> Result<ResourceSample> {
        let persistent_available_bytes = fs4::available_space(&self.paths.persistent_root).ok();
        let persistent_filesystem_used_bytes = fs4::total_space(&self.paths.persistent_root)
            .ok()
            .zip(persistent_available_bytes)
            .map(|(total, available)| total.saturating_sub(available));
        let scratch_bytes = self
            .paths
            .scratch_root
            .as_deref()
            .map(directory_bytes)
            .transpose()?
            .unwrap_or(0);
        Ok(ResourceSample {
            sampled_at_epoch_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis(),
            memory: MemorySnapshot::capture(),
            cpu: capture_cpu(),
            scratch_bytes,
            persistent_filesystem_used_bytes,
            persistent_available_bytes,
            open_file_descriptors: open_file_descriptors(),
            active_source_workers: state.active_source_workers,
            reserved_persistent_bytes: state.reserved_persistent_bytes,
            downloaded_bytes: state.downloaded_bytes,
            ingested_rows: state.ingested_rows,
            download_elapsed_ms: state.download_elapsed_ms,
            ingest_elapsed_ms: state.ingest_elapsed_ms,
            download_bytes_per_second: throughput(
                state.downloaded_bytes,
                state.download_elapsed_ms,
            ),
            ingest_rows_per_second: throughput(state.ingested_rows, state.ingest_elapsed_ms),
        })
    }

    fn validate_sample(&self, sample: &ResourceSample, additional_bytes: u64) -> Result<()> {
        let observed_memory = sample
            .memory
            .cgroup_current_bytes
            .into_iter()
            .chain(sample.memory.cgroup_peak_bytes)
            .chain(sample.memory.rss_bytes)
            .max();
        #[cfg(target_os = "linux")]
        let observed_memory =
            observed_memory.context("resource governor requires memory telemetry")?;
        #[cfg(not(target_os = "linux"))]
        let observed_memory = observed_memory.unwrap_or(0);
        anyhow::ensure!(
            observed_memory <= self.budget.memory_admission_bytes(),
            "resource governor memory gate closed at {observed_memory} bytes; admission ceiling is {} bytes",
            self.budget.memory_admission_bytes()
        );
        anyhow::ensure!(
            sample.scratch_bytes <= self.budget.scratch_limit_bytes,
            "resource governor scratch gate closed at {} bytes; limit is {} bytes",
            sample.scratch_bytes,
            self.budget.scratch_limit_bytes
        );
        if let Some(open) = sample.open_file_descriptors {
            anyhow::ensure!(
                open.saturating_add(SOURCE_FD_ALLOWANCE) <= self.budget.max_open_files,
                "resource governor file-descriptor gate closed at {open} open files; limit is {}",
                self.budget.max_open_files
            );
        }
        let available = sample
            .persistent_available_bytes
            .context("resource governor requires persistent filesystem availability")?;
        let required = self
            .budget
            .persistent_admission_reserve_bytes()?
            .checked_add(additional_bytes)
            .context("resource admission storage requirement overflow")?;
        anyhow::ensure!(
            available >= required,
            "resource governor storage gate closed: {available} bytes available, {required} required"
        );
        Ok(())
    }

    pub(crate) fn record_source_progress(
        &self,
        downloaded_bytes: u64,
        download_elapsed_ms: u64,
        ingested_rows: u64,
        ingest_elapsed_ms: u64,
    ) -> Result<ResourceSample> {
        let mut state = self.state.lock().expect("resource governor mutex poisoned");
        state.downloaded_bytes = state
            .downloaded_bytes
            .checked_add(downloaded_bytes)
            .context("download byte counter overflow")?;
        state.ingested_rows = state
            .ingested_rows
            .checked_add(ingested_rows)
            .context("ingest row counter overflow")?;
        state.download_elapsed_ms = state
            .download_elapsed_ms
            .checked_add(download_elapsed_ms)
            .context("download duration counter overflow")?;
        state.ingest_elapsed_ms = state
            .ingest_elapsed_ms
            .checked_add(ingest_elapsed_ms)
            .context("ingest duration counter overflow")?;
        let sample = self.sample_with_state(&state)?;
        info!(sample = %serde_json::to_string(&sample)?, "resource governor source progress");
        Ok(sample)
    }
}

pub(crate) struct SourcePermit {
    governor: ResourceGovernor,
    reserved_persistent_bytes: u64,
    started: Instant,
    completed: bool,
}

impl SourcePermit {
    pub(crate) fn complete(mut self) {
        self.completed = true;
    }
}

impl Drop for SourcePermit {
    fn drop(&mut self) {
        let mut state = self
            .governor
            .state
            .lock()
            .expect("resource governor mutex poisoned");
        state.active_source_workers = state.active_source_workers.saturating_sub(1);
        state.reserved_persistent_bytes = state
            .reserved_persistent_bytes
            .saturating_sub(self.reserved_persistent_bytes);
        info!(
            elapsed_ms = self.started.elapsed().as_millis() as u64,
            completed = self.completed,
            active_source_workers = state.active_source_workers,
            reserved_persistent_bytes = state.reserved_persistent_bytes,
            "resource governor released source"
        );
    }
}

fn parse_u64_env(name: &str) -> Result<Option<u64>> {
    parse_env(name)
}

fn parse_usize_env(name: &str) -> Result<Option<usize>> {
    parse_env(name)
}

fn parse_env<T>(name: &str) -> Result<Option<T>>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    let value = env::var(name).ok();
    parse_optional_value(name, value.as_deref())
}

fn parse_optional_value<T>(name: &str, value: Option<&str>) -> Result<Option<T>>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    value
        .map(|value| {
            value
                .parse::<T>()
                .map_err(|error| anyhow::anyhow!("invalid {name} value {value:?}: {error}"))
        })
        .transpose()
}

fn checked_sum(values: impl IntoIterator<Item = u64>) -> Result<u64> {
    values.into_iter().try_fold(0_u64, |total, value| {
        total
            .checked_add(value)
            .context("resource byte sum overflow")
    })
}

fn throughput(units: u64, elapsed_ms: u64) -> Option<u64> {
    (elapsed_ms > 0).then(|| units.saturating_mul(1_000) / elapsed_ms)
}

fn capture_cpu() -> CpuSnapshot {
    fs::read_to_string("/sys/fs/cgroup/cpu.stat")
        .ok()
        .map(|value| parse_cpu_stat(&value))
        .unwrap_or_default()
}

fn parse_cpu_stat(value: &str) -> CpuSnapshot {
    let read = |key: &str| {
        value.lines().find_map(|line| {
            let mut fields = line.split_whitespace();
            (fields.next()? == key)
                .then(|| fields.next()?.parse::<u64>().ok())
                .flatten()
        })
    };
    CpuSnapshot {
        usage_usec: read("usage_usec"),
        throttled_usec: read("throttled_usec"),
        nr_throttled: read("nr_throttled"),
    }
}

fn open_file_descriptors() -> Option<usize> {
    fs::read_dir("/proc/self/fd").ok().map(Iterator::count)
}

fn directory_bytes(path: &Path) -> Result<u64> {
    if !path.exists() {
        return Ok(0);
    }
    let mut total = 0_u64;
    let mut pending = vec![path.to_path_buf()];
    while let Some(directory) = pending.pop() {
        for entry in fs::read_dir(&directory)? {
            let entry = entry?;
            let file_type = entry.file_type()?;
            if file_type.is_dir() {
                pending.push(entry.path());
                continue;
            }
            if file_type.is_file() {
                let bytes = entry.metadata()?.len();
                total = total
                    .checked_add(bytes)
                    .context("resource directory byte count overflow")?;
            }
        }
    }
    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::TestDir;

    fn budget() -> ResourceBudget {
        ResourceBudget {
            memory_ceiling_bytes: 1_000_000_000,
            memory_reserve_bytes: 250_000_000,
            persistent_storage_reserve_bytes: 0,
            bounded_scratch_reserve_bytes: 0,
            rollback_generation_reserve_bytes: 0,
            scratch_limit_bytes: 1_000_000,
            max_open_files: 128,
            source_worker_limit: 2,
            thread_limit: 4,
            max_logical_partition_bytes: 100,
            max_active_parquet_writers: 16,
        }
    }

    #[test]
    fn validates_budget_invariants_and_partition_limit() -> Result<()> {
        let root = TestDir::new()?;
        let governor = ResourceGovernor::new(
            budget(),
            GovernorPaths::new(root.path().to_path_buf(), None),
        );
        governor.validate_logical_partition("2026-01", 100)?;
        assert!(governor.validate_logical_partition("2026-02", 101).is_err());
        let mut invalid = budget();
        invalid.memory_reserve_bytes = invalid.memory_ceiling_bytes;
        assert!(invalid.validate().is_err());
        let mut overflowing = budget();
        overflowing.persistent_storage_reserve_bytes = u64::MAX;
        overflowing.bounded_scratch_reserve_bytes = 1;
        assert!(overflowing.persistent_admission_reserve_bytes().is_err());
        assert!(
            budget()
                .validate_external_thread_pool_values([
                    ("RAYON_NUM_THREADS", Some(5)),
                    ("POLARS_MAX_THREADS", None),
                ])
                .is_err()
        );
        budget().validate_external_thread_pool_values([
            ("RAYON_NUM_THREADS", Some(4)),
            ("POLARS_MAX_THREADS", Some(3)),
        ])?;
        Ok(())
    }

    #[test]
    fn snapshot_preflight_fails_closed_on_unknown_sizes() -> Result<()> {
        let root = TestDir::new()?;
        let governor = ResourceGovernor::new(
            budget(),
            GovernorPaths::new(root.path().to_path_buf(), None),
        );
        assert!(governor.preflight_snapshot(&[Some(10), None], 2).is_err());
        assert!(
            governor
                .preflight_snapshot(&[Some(u64::MAX), Some(1)], 2)
                .is_err()
        );
        Ok(())
    }

    #[test]
    fn snapshot_preflight_and_progress_emit_complete_runtime_samples() -> Result<()> {
        let root = TestDir::new()?;
        let scratch = root.path().join("scratch");
        fs::create_dir(&scratch)?;
        fs::write(scratch.join("part"), b"1234")?;
        let mut permissive = budget();
        permissive.memory_ceiling_bytes = u64::MAX;
        permissive.memory_reserve_bytes = 0;
        let governor = ResourceGovernor::new(
            permissive,
            GovernorPaths::new(root.path().to_path_buf(), Some(scratch)),
        );
        let preflight = governor.preflight_snapshot(&[Some(10), Some(20)], 2)?;
        assert_eq!(preflight.scratch_bytes, 4);
        governor.checkpoint("test")?;
        let progress = governor.record_source_progress(2_000, 500, 300, 100)?;
        assert_eq!(progress.download_bytes_per_second, Some(4_000));
        assert_eq!(progress.ingest_rows_per_second, Some(3_000));
        Ok(())
    }

    #[test]
    fn parsing_and_checked_arithmetic_fail_closed() {
        assert_eq!(parse_optional_value("test", Some("42")).unwrap(), Some(42));
        assert!(parse_optional_value::<usize>("test", Some("invalid")).is_err());
        assert_eq!(parse_optional_value::<u64>("test", None).unwrap(), None);
        assert!(checked_sum([u64::MAX, 1]).is_err());
    }

    #[test]
    fn parses_cpu_throttling_and_rates() {
        assert_eq!(
            parse_cpu_stat("usage_usec 42\nnr_throttled 3\nthrottled_usec 7\n"),
            CpuSnapshot {
                usage_usec: Some(42),
                throttled_usec: Some(7),
                nr_throttled: Some(3),
            }
        );
        assert_eq!(throughput(1_000, 250), Some(4_000));
        assert_eq!(throughput(10, 0), None);
    }

    #[test]
    fn directory_bytes_counts_nested_files() -> Result<()> {
        let root = TestDir::new()?;
        fs::create_dir(root.path().join("nested"))?;
        fs::write(root.path().join("one"), b"123")?;
        fs::write(root.path().join("nested/two"), b"4567")?;
        #[cfg(unix)]
        std::os::unix::fs::symlink(root.path().join("one"), root.path().join("ignored-link"))?;
        assert_eq!(directory_bytes(root.path())?, 7);
        Ok(())
    }

    #[test]
    fn source_permits_reserve_storage_and_enforce_single_flight_limit() -> Result<()> {
        let root = TestDir::new()?;
        let mut permissive = budget();
        permissive.memory_ceiling_bytes = u64::MAX;
        permissive.memory_reserve_bytes = 0;
        let governor = ResourceGovernor::new(
            permissive,
            GovernorPaths::new(root.path().to_path_buf(), None),
        );
        let first = governor.admit_source(10)?;
        let second = governor.admit_source(20)?;
        let sample = governor.sample()?;
        assert_eq!(sample.active_source_workers, 2);
        assert_eq!(sample.reserved_persistent_bytes, 30);
        assert!(governor.admit_source(1).is_err());
        drop(first);
        let third = governor.admit_source(1)?;
        drop(second);
        third.complete();
        let sample = governor.sample()?;
        assert_eq!(sample.active_source_workers, 0);
        assert_eq!(sample.reserved_persistent_bytes, 0);
        Ok(())
    }

    #[test]
    fn runtime_sample_closes_memory_scratch_fd_and_storage_gates() -> Result<()> {
        let root = TestDir::new()?;
        let governor = ResourceGovernor::new(
            budget(),
            GovernorPaths::new(root.path().to_path_buf(), None),
        );
        let sample = ResourceSample {
            sampled_at_epoch_ms: 0,
            memory: MemorySnapshot {
                rss_bytes: Some(750_000_001),
                cgroup_current_bytes: None,
                cgroup_peak_bytes: None,
                cgroup_limit_bytes: Some(1_000_000_000),
            },
            cpu: CpuSnapshot::default(),
            scratch_bytes: 0,
            persistent_filesystem_used_bytes: Some(0),
            persistent_available_bytes: Some(1_000_000),
            open_file_descriptors: Some(1),
            active_source_workers: 0,
            reserved_persistent_bytes: 0,
            downloaded_bytes: 0,
            ingested_rows: 0,
            download_elapsed_ms: 0,
            ingest_elapsed_ms: 0,
            download_bytes_per_second: None,
            ingest_rows_per_second: None,
        };
        assert!(governor.validate_sample(&sample, 0).is_err());

        let mut scratch = sample.clone();
        scratch.memory.rss_bytes = Some(1);
        scratch.scratch_bytes = 1_000_001;
        assert!(governor.validate_sample(&scratch, 0).is_err());

        let mut files = sample.clone();
        files.memory.rss_bytes = Some(1);
        files.open_file_descriptors = Some(121);
        assert!(governor.validate_sample(&files, 0).is_err());

        let mut storage = sample.clone();
        storage.memory.rss_bytes = Some(1);
        storage.persistent_available_bytes = Some(9);
        assert!(governor.validate_sample(&storage, 10).is_err());

        let mut unmetered_fds = sample;
        unmetered_fds.memory.rss_bytes = Some(1);
        unmetered_fds.open_file_descriptors = None;
        governor.validate_sample(&unmetered_fds, 0)?;
        Ok(())
    }
}
