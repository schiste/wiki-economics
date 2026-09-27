use anyhow::{Context, Result};
use serde::Serialize;
use std::ffi::OsStr;
use std::path::Path;
use std::time::Instant;
use tracing::info;

use crate::resource_governor::{GovernorPaths, ResourceGovernor, SourcePermit};
use crate::snapshot_plan::{SnapshotPlan, SourceSpec};
use crate::{fetch, ingest, workload_profile};

pub(crate) const SOURCE_WINDOW_SIZE_ENV: &str = "WIKI_ECON_SOURCE_WINDOW_SIZE";
pub(crate) const DEFAULT_SOURCE_WINDOW_SIZE: usize = 1;
pub(crate) const MAX_SOURCE_WINDOW_SIZE: usize = 4;

#[derive(Clone, Copy)]
struct ExecutionMode {
    window_size: usize,
    select_generation: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub(crate) struct SourceWindowSummary {
    pub(crate) wiki: String,
    pub(crate) snapshot: String,
    pub(crate) window_size: usize,
    pub(crate) source_worker_limit: usize,
    pub(crate) planned_sources: usize,
    pub(crate) reused_sources: usize,
    pub(crate) ingested_sources: usize,
    pub(crate) ingested_rows: u64,
}

trait SourceTransactionOps: Sync {
    fn planned_sources(&self, wiki: &str, snapshot: &str, data_dir: &Path) -> Result<usize>;
    fn cleanup_committed(&self, wiki: &str, snapshot: &str, data_dir: &Path) -> Result<usize>;
    fn pending_sources(
        &self,
        wiki: &str,
        snapshot: &str,
        data_dir: &Path,
    ) -> Result<Vec<SourceSpec>>;
    fn source_sizes(
        &self,
        data_dir: &Path,
        wiki: &str,
        snapshot: &str,
        sources: &[SourceSpec],
    ) -> Result<Vec<Option<u64>>>;
    fn fetch_source(
        &self,
        wiki: &str,
        snapshot: &str,
        data_dir: &Path,
        run_id: &str,
        source: &SourceSpec,
    ) -> Result<std::path::PathBuf>;
    fn ingest_source(
        &self,
        wiki: &str,
        snapshot: &str,
        data_dir: &Path,
        source: &Path,
        run_id: &str,
    ) -> Result<ingest::SourceIngestCommit>;
    fn finalize(
        &self,
        wiki: &str,
        snapshot: &str,
        data_dir: &Path,
        select_generation: bool,
    ) -> Result<()>;
}

struct RealSourceTransactionOps;

impl SourceTransactionOps for RealSourceTransactionOps {
    fn planned_sources(&self, wiki: &str, snapshot: &str, data_dir: &Path) -> Result<usize> {
        Ok(SnapshotPlan::load_or_resolve(data_dir, wiki, snapshot)?
            .0
            .sources
            .len())
    }

    fn cleanup_committed(&self, wiki: &str, snapshot: &str, data_dir: &Path) -> Result<usize> {
        fetch::cleanup_committed_source_window_inputs(wiki, snapshot, data_dir)
    }

    fn pending_sources(
        &self,
        wiki: &str,
        snapshot: &str,
        data_dir: &Path,
    ) -> Result<Vec<SourceSpec>> {
        fetch::pending_snapshot_sources(wiki, snapshot, data_dir)
    }

    fn source_sizes(
        &self,
        data_dir: &Path,
        wiki: &str,
        snapshot: &str,
        sources: &[SourceSpec],
    ) -> Result<Vec<Option<u64>>> {
        fetch::snapshot_source_sizes(data_dir, wiki, snapshot, sources)
    }

    fn fetch_source(
        &self,
        wiki: &str,
        snapshot: &str,
        data_dir: &Path,
        run_id: &str,
        source: &SourceSpec,
    ) -> Result<std::path::PathBuf> {
        single_source_path(fetch::fetch_snapshot_source_window(
            wiki,
            snapshot,
            data_dir,
            run_id,
            std::slice::from_ref(source),
        )?)
    }

    fn ingest_source(
        &self,
        wiki: &str,
        snapshot: &str,
        data_dir: &Path,
        source: &Path,
        run_id: &str,
    ) -> Result<ingest::SourceIngestCommit> {
        ingest::ingest_snapshot_source(wiki, snapshot, data_dir, source, run_id)
    }

    fn finalize(
        &self,
        wiki: &str,
        snapshot: &str,
        data_dir: &Path,
        select_generation: bool,
    ) -> Result<()> {
        fetch::finalize_snapshot_fetch(wiki, snapshot, data_dir)?;
        if select_generation {
            ingest::finalize_snapshot_ingest(wiki, snapshot, data_dir)?;
        } else {
            ingest::finalize_snapshot_ingest_candidate(wiki, snapshot, data_dir)?;
        }
        fetch::cleanup_committed_source_window_inputs(wiki, snapshot, data_dir)?;
        Ok(())
    }
}

fn single_source_path(paths: Vec<std::path::PathBuf>) -> Result<std::path::PathBuf> {
    anyhow::ensure!(
        paths.len() == 1,
        "single-source fetch returned an incomplete path set"
    );
    paths
        .into_iter()
        .next()
        .context("single-source fetch returned no path")
}

pub(crate) fn configured_window_size(cli_value: Option<usize>) -> Result<usize> {
    configured_window_size_from(
        cli_value,
        std::env::var_os(SOURCE_WINDOW_SIZE_ENV).as_deref(),
    )
}

fn configured_window_size_from(
    cli_value: Option<usize>,
    env_value: Option<&OsStr>,
) -> Result<usize> {
    let value = match cli_value {
        Some(value) => value,
        None => env_value
            .map(|value| {
                value
                    .to_str()
                    .context("source-window size environment value is not UTF-8")?
                    .parse::<usize>()
                    .context("source-window size environment value is not an integer")
            })
            .transpose()?
            .unwrap_or(DEFAULT_SOURCE_WINDOW_SIZE),
    };
    anyhow::ensure!(
        (1..=MAX_SOURCE_WINDOW_SIZE).contains(&value),
        "source-window size must be between 1 and {MAX_SOURCE_WINDOW_SIZE}, got {value}"
    );
    Ok(value)
}

fn execute_bounded<T, P, A, F>(
    items: &[T],
    worker_limit: usize,
    mut admit: A,
    process: F,
) -> Result<(usize, u64)>
where
    T: Sync,
    P: Send,
    A: FnMut(usize, &T) -> Result<P>,
    F: Fn(usize, &T, P) -> Result<u64> + Sync,
{
    anyhow::ensure!(
        (1..=MAX_SOURCE_WINDOW_SIZE).contains(&worker_limit),
        "source-worker limit must be between 1 and {MAX_SOURCE_WINDOW_SIZE}, got {worker_limit}"
    );

    std::thread::scope(|scope| {
        let (completed_tx, completed_rx) = std::sync::mpsc::channel::<(usize, Result<u64>)>();
        let mut next_item = 0;
        let mut active_workers = 0;
        let mut completed_items = 0_usize;
        let mut ingested_rows = 0_u64;

        // Refill a worker slot as soon as any source finishes. Fixed-size
        // waves leave idle tails while the slowest source in each wave ingests.
        while next_item < items.len() || active_workers > 0 {
            while next_item < items.len() && active_workers < worker_limit {
                let index = next_item;
                let permit = match admit(index, &items[index]) {
                    Ok(permit) => permit,
                    Err(_error) if active_workers > 0 => {
                        // A concurrent task may release the headroom this
                        // source needs. Wait for one completion, then retry.
                        break;
                    }
                    Err(error) => {
                        return Err(
                            error.context("resource governor could not admit a source worker")
                        );
                    }
                };
                let item = &items[index];
                let process = &process;
                let completed_tx = completed_tx.clone();
                scope.spawn(move || {
                    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        process(index, item, permit)
                    }))
                    .unwrap_or_else(|_| {
                        Err(anyhow::anyhow!(
                            "source prefetch worker panicked during fetch or ingest"
                        ))
                    });
                    let _ = completed_tx.send((index, outcome));
                });
                next_item += 1;
                active_workers += 1;
            }

            let (index, outcome) = completed_rx
                .recv()
                .context("source worker completion channel closed unexpectedly")?;
            active_workers -= 1;
            match outcome {
                Ok(rows) => {
                    completed_items = completed_items
                        .checked_add(1)
                        .context("completed source count overflow")?;
                    ingested_rows = ingested_rows
                        .checked_add(rows)
                        .context("snapshot ingest row count overflow")?;
                }
                Err(error) => {
                    // Returning exits dispatch immediately. Scoped threads are
                    // still joined, dropping their permits before this error
                    // reaches the pipeline.
                    return Err(error.context(format!("source worker failed for item {index}")));
                }
            }
        }

        Ok((completed_items, ingested_rows))
    })
}

fn ensure_pending_sources_completed(completed: usize, expected: usize) -> Result<()> {
    anyhow::ensure!(
        completed == expected,
        "bounded source executor completed {completed} of {expected} pending sources"
    );
    Ok(())
}

struct SourceExecution<'a> {
    wiki: &'a str,
    snapshot: &'a str,
    data_dir: &'a Path,
    run_id: &'a str,
    governor: Option<&'a ResourceGovernor>,
}

fn process_source<O: SourceTransactionOps>(
    ops: &O,
    execution: &SourceExecution<'_>,
    source: &SourceSpec,
    permit: Option<SourcePermit>,
) -> Result<u64> {
    let fetched = fetch_source_transaction(ops, execution, source, permit)?;
    ingest_fetched_source(ops, execution, fetched)
}

struct FetchedSource {
    source: SourceSpec,
    path: std::path::PathBuf,
    downloaded_bytes: u64,
    download_elapsed_ms: u64,
    permit: Option<SourcePermit>,
}

fn fetch_source_transaction<O: SourceTransactionOps>(
    ops: &O,
    execution: &SourceExecution<'_>,
    source: &SourceSpec,
    permit: Option<SourcePermit>,
) -> Result<FetchedSource> {
    let download_started = Instant::now();
    let path = ops.fetch_source(
        execution.wiki,
        execution.snapshot,
        execution.data_dir,
        execution.run_id,
        source,
    )?;
    let download_elapsed_ms = download_started.elapsed().as_millis() as u64;
    anyhow::ensure!(
        path.file_name().and_then(|name| name.to_str()) == Some(source.filename()?),
        "source-window fetch returned the wrong path for {}",
        source.source_id
    );
    let downloaded_bytes = path.metadata()?.len();
    Ok(FetchedSource {
        source: source.clone(),
        path,
        downloaded_bytes,
        download_elapsed_ms,
        permit,
    })
}

fn ingest_fetched_source<O: SourceTransactionOps>(
    ops: &O,
    execution: &SourceExecution<'_>,
    fetched: FetchedSource,
) -> Result<u64> {
    let FetchedSource {
        source: expected_source,
        path,
        downloaded_bytes,
        download_elapsed_ms,
        permit,
    } = fetched;
    let ingest_started = Instant::now();
    let commit = ops.ingest_source(
        execution.wiki,
        execution.snapshot,
        execution.data_dir,
        &path,
        execution.run_id,
    )?;
    let ingest_elapsed_ms = ingest_started.elapsed().as_millis() as u64;
    anyhow::ensure!(
        commit.source_id == expected_source.source_id,
        "ingest committed the wrong source for {}",
        expected_source.source_id
    );
    let rows = u64::try_from(commit.rows)?;
    if let Some(governor) = execution.governor {
        governor.record_source_progress(
            downloaded_bytes,
            download_elapsed_ms,
            rows,
            ingest_elapsed_ms,
        )?;
    }
    if let Some(permit) = permit {
        permit.complete();
    }
    Ok(rows)
}

/// Execute a snapshot as bounded, independently committed source
/// transactions. The candidate generation is selected only after the exact
/// canonical source inventory and all Parquet outputs validate.
pub(crate) fn prepare_snapshot(
    wiki: &str,
    snapshot: &str,
    data_dir: &Path,
    run_id: &str,
    window_size: usize,
) -> Result<SourceWindowSummary> {
    let governor = governed_snapshot(data_dir, wiki, snapshot, window_size)?;
    prepare_snapshot_with_ops(
        &RealSourceTransactionOps,
        wiki,
        snapshot,
        data_dir,
        run_id,
        ExecutionMode {
            window_size,
            select_generation: true,
        },
        Some(&governor),
    )
}

pub(crate) fn prepare_candidate_snapshot(
    wiki: &str,
    snapshot: &str,
    data_dir: &Path,
    run_id: &str,
    window_size: usize,
) -> Result<SourceWindowSummary> {
    let governor = governed_snapshot(data_dir, wiki, snapshot, window_size)?;
    prepare_snapshot_with_ops(
        &RealSourceTransactionOps,
        wiki,
        snapshot,
        data_dir,
        run_id,
        ExecutionMode {
            window_size,
            select_generation: false,
        },
        Some(&governor),
    )
}

fn governed_snapshot(
    data_dir: &Path,
    wiki: &str,
    snapshot: &str,
    window_size: usize,
) -> Result<ResourceGovernor> {
    governed_snapshot_with_sizes(data_dir, wiki, snapshot, window_size, |sources| {
        fetch::snapshot_source_sizes(data_dir, wiki, snapshot, sources)
    })
}

fn governed_snapshot_with_sizes<F>(
    data_dir: &Path,
    wiki: &str,
    snapshot: &str,
    window_size: usize,
    resolve_sizes: F,
) -> Result<ResourceGovernor>
where
    F: FnOnce(&[SourceSpec]) -> Result<Vec<Option<u64>>>,
{
    let (plan, _) = SnapshotPlan::load_or_resolve(data_dir, wiki, snapshot)?;
    let analytical = crate::storage::snapshot_analytical_wiki_dir(data_dir, wiki, snapshot)?;
    let mut source_sizes = vec![None; plan.sources.len()];
    let mut unresolved = Vec::new();
    let mut unresolved_indices = Vec::new();
    for (index, source) in plan.sources.iter().enumerate() {
        if crate::compaction::source_is_represented(data_dir, wiki, snapshot, &source.source_id)?
            && let Some(marker) =
                crate::storage::read_marker_manifest_in(data_dir, &analytical, &source.source_id)?
        {
            source_sizes[index] = Some(marker.source_size_bytes);
        } else {
            unresolved.push(source.clone());
            unresolved_indices.push(index);
        }
    }
    let resolved = resolve_sizes(&unresolved)?;
    anyhow::ensure!(
        resolved.len() == unresolved_indices.len(),
        "workload sizing returned an incomplete source-size inventory"
    );
    for (index, size) in unresolved_indices.into_iter().zip(resolved) {
        source_sizes[index] = size;
    }
    let profile = workload_profile::load_or_select(data_dir, &plan, &source_sizes)?;
    let scratch_root = std::env::var_os("WIKI_ECON_SCRATCH_DIR").map(Into::into);
    let paths = GovernorPaths::new(data_dir.to_path_buf(), scratch_root);
    let source_workers = profile.parameters.source_workers;
    let governor = ResourceGovernor::from_environment_with_source_workers(paths, source_workers)?;
    let effective_source_workers = governor.budget().source_worker_limit.min(window_size);
    profile.ensure_source_qualified(effective_source_workers)?;
    info!(
        wiki,
        snapshot,
        selected_profile = ?profile.profile,
        selection_mode = ?profile.selection_mode,
        total_compressed_bytes = profile.signals.total_compressed_bytes,
        source_count = profile.signals.source_count,
        prior_measured_rows = profile.signals.prior_measured_rows,
        requested_source_workers = profile.parameters.source_workers,
        effective_source_workers,
        primary_buckets = profile.parameters.primary_buckets,
        secondary_buckets = profile.parameters.secondary_buckets,
        "selected adaptive workload profile"
    );
    Ok(governor)
}

fn prepare_snapshot_with_ops<O: SourceTransactionOps>(
    ops: &O,
    wiki: &str,
    snapshot: &str,
    data_dir: &Path,
    run_id: &str,
    execution_mode: ExecutionMode,
    governor: Option<&ResourceGovernor>,
) -> Result<SourceWindowSummary> {
    let ExecutionMode {
        window_size,
        select_generation,
    } = execution_mode;
    let planned_sources = ops.planned_sources(wiki, snapshot, data_dir)?;
    let recovered_inputs = ops.cleanup_committed(wiki, snapshot, data_dir)?;
    let pending = ops.pending_sources(wiki, snapshot, data_dir)?;
    let source_sizes = ops.source_sizes(data_dir, wiki, snapshot, &pending)?;
    anyhow::ensure!(
        source_sizes.len() == pending.len(),
        "resource preflight returned an incomplete source-size inventory"
    );
    let source_worker_limit = governor
        .map(|governor| governor.budget().source_worker_limit)
        .unwrap_or(1)
        .min(window_size);
    if let Some(governor) = governor {
        governor.preflight_snapshot(&source_sizes, source_worker_limit)?;
    }
    let reused_sources = planned_sources
        .checked_sub(pending.len())
        .context("pending source inventory exceeds snapshot plan")?;
    let pending_bytes = source_sizes.iter().try_fold(0_u64, |total, bytes| {
        total
            .checked_add(bytes.unwrap_or_default())
            .context("pending source byte total overflow")
    })?;
    let planned_bytes = workload_profile::load(data_dir, wiki, snapshot)?
        .map(|profile| profile.signals.total_compressed_bytes);
    let reused_bytes = planned_bytes.map(|total| total.saturating_sub(pending_bytes));
    info!(
        wiki,
        snapshot,
        run_id,
        window_size,
        planned_sources,
        reused_sources,
        pending_sources = pending.len(),
        recovered_inputs,
        planned_bytes = planned_bytes.unwrap_or_default(),
        reused_bytes = reused_bytes.unwrap_or_default(),
        "starting bounded source-window execution"
    );

    let execution = SourceExecution {
        wiki,
        snapshot,
        data_dir,
        run_id,
        governor,
    };
    let (ingested_sources, ingested_rows) = execute_bounded(
        &pending,
        source_worker_limit,
        |index, _source| {
            let expected_size =
                source_sizes[index].context("source size became unknown after preflight")?;
            governor
                .map(|governor| governor.admit_source(expected_size))
                .transpose()
        },
        |_, source, permit| process_source(ops, &execution, source, permit),
    )?;
    ensure_pending_sources_completed(ingested_sources, pending.len())?;

    ops.finalize(wiki, snapshot, data_dir, select_generation)?;
    if let Some(governor) = governor {
        let observation = governor.observation();
        let fragment_count = crate::storage::read_generation_manifest(data_dir, wiki, snapshot)
            .ok()
            .and_then(|manifest| u64::try_from(manifest.fragments.len()).ok());
        let observations = workload_profile::WorkloadObservations {
            schema_version: 1,
            fragment_count,
            peak_memory_bytes: observation
                .cgroup_reported_peak_bytes
                .into_iter()
                .chain(observation.cgroup_current_peak_bytes)
                .chain(observation.rss_peak_bytes)
                .max(),
            peak_scratch_bytes: Some(observation.scratch_peak_bytes),
            throughput_rows_per_second: observation.ingest_rows_per_second,
        };
        workload_profile::record_observations(data_dir, wiki, observations)?;
        info!(
            wiki,
            snapshot,
            observation = %serde_json::to_string(&observation)?,
            "persisted completed source-window resource observation"
        );
    }
    let summary = SourceWindowSummary {
        wiki: wiki.to_string(),
        snapshot: snapshot.to_string(),
        window_size,
        source_worker_limit,
        planned_sources,
        reused_sources,
        ingested_sources,
        ingested_rows,
    };
    info!(
        summary = %serde_json::to_string(&summary)?,
        "completed bounded source-window execution"
    );
    Ok(summary)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resource_governor::{GovernorPaths, ResourceBudget};
    use crate::test_support::TestDir;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex, mpsc};

    #[derive(Default)]
    struct FakeOps {
        planned: usize,
        pending: Vec<SourceSpec>,
        windows: Mutex<Vec<Vec<String>>>,
        ingested: Mutex<Vec<String>>,
        finalized: AtomicBool,
        selected_generation: AtomicBool,
        path_count_delta: isize,
        wrong_path: bool,
        wrong_commit: bool,
        ingest_error: bool,
        panic_fetch: bool,
    }

    impl SourceTransactionOps for FakeOps {
        fn planned_sources(&self, _wiki: &str, _snapshot: &str, _data_dir: &Path) -> Result<usize> {
            Ok(self.planned)
        }

        fn cleanup_committed(
            &self,
            _wiki: &str,
            _snapshot: &str,
            _data_dir: &Path,
        ) -> Result<usize> {
            Ok(1)
        }

        fn pending_sources(
            &self,
            _wiki: &str,
            _snapshot: &str,
            _data_dir: &Path,
        ) -> Result<Vec<SourceSpec>> {
            Ok(self.pending.clone())
        }

        fn source_sizes(
            &self,
            _data_dir: &Path,
            _wiki: &str,
            _snapshot: &str,
            sources: &[SourceSpec],
        ) -> Result<Vec<Option<u64>>> {
            Ok(vec![Some(1); sources.len()])
        }

        fn fetch_source(
            &self,
            _wiki: &str,
            _snapshot: &str,
            data_dir: &Path,
            _run_id: &str,
            source: &SourceSpec,
        ) -> Result<std::path::PathBuf> {
            assert!(!self.panic_fetch, "injected source prefetch panic");
            self.windows
                .lock()
                .expect("fake windows mutex poisoned")
                .push(vec![source.source_id.clone()]);
            if self.path_count_delta < 0 {
                anyhow::bail!("source-window fetch returned an incomplete path set");
            }
            if self.wrong_path {
                return Ok(std::path::PathBuf::from("unexpected.tsv.bz2"));
            }
            let path = data_dir.join(source.filename()?);
            std::fs::write(&path, b"fixture")?;
            Ok(path)
        }

        fn ingest_source(
            &self,
            _wiki: &str,
            _snapshot: &str,
            _data_dir: &Path,
            source: &Path,
            _run_id: &str,
        ) -> Result<ingest::SourceIngestCommit> {
            anyhow::ensure!(!self.ingest_error, "injected ingest failure");
            let source_id = ingest::ingest_source_id(source)?;
            self.ingested
                .lock()
                .expect("fake ingested mutex poisoned")
                .push(source_id.clone());
            Ok(ingest::SourceIngestCommit {
                source_id: if self.wrong_commit {
                    "wrong-source".to_string()
                } else {
                    source_id
                },
                rows: 10,
                reused: false,
            })
        }

        fn finalize(
            &self,
            _wiki: &str,
            _snapshot: &str,
            _data_dir: &Path,
            select_generation: bool,
        ) -> Result<()> {
            self.finalized.store(true, Ordering::Relaxed);
            self.selected_generation
                .store(select_generation, Ordering::Relaxed);
            Ok(())
        }
    }

    #[test]
    fn source_window_size_defaults_and_accepts_explicit_values() -> Result<()> {
        assert_eq!(
            configured_window_size_from(None, None)?,
            DEFAULT_SOURCE_WINDOW_SIZE
        );
        assert_eq!(
            configured_window_size_from(Some(4), Some(OsStr::new("bad")))?,
            4
        );
        assert_eq!(configured_window_size_from(None, Some(OsStr::new("2")))?, 2);
        Ok(())
    }

    #[test]
    fn source_window_size_rejects_invalid_values() {
        for value in ["0", "5", "not-a-number"] {
            assert!(configured_window_size_from(None, Some(OsStr::new(value))).is_err());
        }
        assert!(configured_window_size_from(Some(0), None).is_err());
    }

    #[test]
    fn bounded_execution_never_exceeds_worker_limit_and_keeps_commits() -> Result<()> {
        let items = [1_u8, 2, 3, 4, 5, 6, 7];
        let active = Arc::new(AtomicUsize::new(0));
        let maximum_active = Arc::new(AtomicUsize::new(0));
        let (completed, rows) = execute_bounded(
            &items,
            3,
            |_, _| Ok(()),
            |_, item, ()| {
                let current = active.fetch_add(1, Ordering::AcqRel) + 1;
                maximum_active.fetch_max(current, Ordering::Relaxed);
                std::thread::sleep(std::time::Duration::from_millis(10));
                active.fetch_sub(1, Ordering::AcqRel);
                Ok(u64::from(*item))
            },
        )
        .expect("bounded worker execution should complete");

        assert_eq!(completed, items.len());
        assert_eq!(rows, items.iter().map(|item| u64::from(*item)).sum::<u64>());
        assert!(maximum_active.load(Ordering::Relaxed) > 1);
        assert!(maximum_active.load(Ordering::Relaxed) <= 3);
        Ok(())
    }

    #[test]
    fn source_completion_count_must_match_pending_inventory() {
        assert!(ensure_pending_sources_completed(3, 3).is_ok());
        let error = ensure_pending_sources_completed(2, 3)
            .expect_err("an incomplete source inventory must fail closed");
        assert!(
            error
                .to_string()
                .contains("bounded source executor completed 2 of 3 pending sources")
        );
    }

    #[test]
    fn bounded_execution_retries_temporary_and_surfaces_persistent_admission_rejection() {
        let items = [1_u8, 2];
        let run = |persistent_rejection: bool| -> Result<(usize, u64, usize)> {
            let second_item_attempts = AtomicUsize::new(0);
            let (completed, rows) = execute_bounded(
                &items,
                2,
                |index, _| {
                    if index == 1 {
                        let attempt = second_item_attempts.fetch_add(1, Ordering::AcqRel);
                        if persistent_rejection || attempt == 0 {
                            anyhow::bail!("injected source admission rejection");
                        }
                    }
                    Ok(())
                },
                |_, item, ()| Ok(u64::from(*item)),
            )?;
            Ok((
                completed,
                rows,
                second_item_attempts.load(Ordering::Relaxed),
            ))
        };

        let (completed, rows, attempts) = run(false)
            .expect("temporary admission rejection must be retried after a worker completes");
        assert_eq!(completed, items.len());
        assert_eq!(rows, 3);
        assert_eq!(attempts, 2);

        let error = run(true)
            .expect_err("persistent admission rejection must propagate after active work joins");
        assert!(error.chain().any(|cause| {
            cause
                .to_string()
                .contains("resource governor could not admit a source worker")
        }));
    }

    #[test]
    fn bounded_execution_refills_a_slot_before_the_slowest_source_finishes() -> Result<()> {
        let items = [0_u8, 1, 2];
        let slow_source_active = Arc::new(AtomicBool::new(false));
        let (release_tx, release_rx_original) = mpsc::channel::<()>();
        let release_rx = Arc::new(Mutex::new(release_rx_original));
        let (slow_started_tx, slow_started_rx) = mpsc::channel();
        let (refill_observation_tx, refill_observation_rx) = mpsc::channel();
        let active_flag = Arc::clone(&slow_source_active);
        let execution = std::thread::spawn(move || {
            execute_bounded(
                &items,
                2,
                |_, _| Ok(()),
                |_, item, ()| match *item {
                    0 => {
                        active_flag.store(true, Ordering::Release);
                        let _ = slow_started_tx.send(());
                        release_rx
                            .lock()
                            .expect("release receiver mutex poisoned")
                            .recv()
                            .context("slow source release channel closed")?;
                        active_flag.store(false, Ordering::Release);
                        Ok(1)
                    }
                    1 => Ok(1),
                    _ => {
                        let slow_source_was_active = active_flag.load(Ordering::Acquire);
                        let _ = refill_observation_tx.send(slow_source_was_active);
                        Ok(1)
                    }
                },
            )
        });

        let slow_started = slow_started_rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .is_ok();
        let refilled_while_slow_active = refill_observation_rx
            .recv_timeout(std::time::Duration::from_millis(500))
            .unwrap_or(false);
        let _ = release_tx.send(());
        let result = execution.join().expect("bounded executor should not panic");

        assert!(slow_started, "the first source should start");
        assert!(
            refilled_while_slow_active,
            "the next source should start before the slow source finishes"
        );
        assert_eq!(result?, (3, 3));
        Ok(())
    }

    #[test]
    fn bounded_execution_stops_dispatching_after_a_worker_error() {
        let items = [1_u8, 2, 3, 4, 5];
        let error = execute_bounded(
            &items,
            2,
            |_, _| Ok(()),
            |_, item, ()| {
                anyhow::ensure!(*item != 3, "injected worker failure");
                Ok(1)
            },
        )
        .expect_err("the worker failure must stop source execution");
        assert!(
            error
                .chain()
                .any(|cause| cause.to_string().contains("injected worker failure")),
            "expected injected worker failure in chain, got: {error:#}"
        );
    }

    #[test]
    fn bounded_execution_rejects_an_invalid_window() {
        assert!(execute_bounded(&[1_u8], 0, |_, _| Ok(()), |_, _, ()| Ok(1)).is_err());
    }

    #[test]
    fn snapshot_preparation_processes_pending_sources_with_bounded_workers_and_reports_reuse()
    -> Result<()> {
        let data_dir = TestDir::new()?;
        let pending = SnapshotPlan::resolve("enwiki", "2001-03")?
            .sources
            .into_iter()
            .take(3)
            .collect::<Vec<_>>();
        let ops = FakeOps {
            planned: 5,
            pending,
            ..FakeOps::default()
        };

        let summary = prepare_snapshot_with_ops(
            &ops,
            "enwiki",
            "2001-03",
            data_dir.path(),
            "run-1",
            ExecutionMode {
                window_size: 2,
                select_generation: true,
            },
            None,
        )
        .expect("ungoverned fixture should complete");

        assert_eq!(summary.planned_sources, 5);
        assert_eq!(summary.reused_sources, 2);
        assert_eq!(summary.ingested_sources, 3);
        assert_eq!(summary.ingested_rows, 30);
        assert_eq!(
            ops.windows
                .lock()
                .expect("fake windows mutex poisoned")
                .iter()
                .map(Vec::len)
                .collect::<Vec<_>>(),
            vec![1, 1, 1]
        );
        assert_eq!(
            ops.ingested
                .lock()
                .expect("fake ingested mutex poisoned")
                .len(),
            3
        );
        assert!(ops.finalized.load(Ordering::Relaxed));
        assert!(ops.selected_generation.load(Ordering::Relaxed));
        Ok(())
    }

    #[test]
    fn candidate_preparation_finalizes_without_selecting_the_generation() -> Result<()> {
        let data_dir = TestDir::new()?;
        let ops = FakeOps {
            planned: 1,
            pending: SnapshotPlan::resolve("testwiki", "2026-08")?.sources,
            ..FakeOps::default()
        };

        prepare_snapshot_with_ops(
            &ops,
            "testwiki",
            "2026-08",
            data_dir.path(),
            "candidate-run",
            ExecutionMode {
                window_size: 1,
                select_generation: false,
            },
            None,
        )
        .expect("candidate execution should finalize");

        assert!(ops.finalized.load(Ordering::Relaxed));
        assert!(!ops.selected_generation.load(Ordering::Relaxed));
        Ok(())
    }

    #[test]
    fn governed_snapshot_preparation_records_progress_and_worker_budget() -> Result<()> {
        let data_dir = TestDir::new()?;
        let pending = SnapshotPlan::resolve("enwiki", "2001-01")?
            .sources
            .into_iter()
            .take(2)
            .collect::<Vec<_>>();
        let ops = FakeOps {
            planned: 2,
            pending,
            ..FakeOps::default()
        };
        let governor = ResourceGovernor::new(
            ResourceBudget {
                memory_ceiling_bytes: u64::MAX,
                memory_reserve_bytes: 0,
                persistent_storage_reserve_bytes: 0,
                bounded_scratch_reserve_bytes: 0,
                rollback_generation_reserve_bytes: 0,
                scratch_limit_bytes: u64::MAX,
                max_open_files: 512,
                source_worker_limit: 2,
                thread_limit: 2,
                max_logical_partition_bytes: u64::MAX,
                max_active_parquet_writers: 16,
                weekly_worker_limit: 1,
            },
            GovernorPaths::new(data_dir.path().to_path_buf(), None),
        );
        let summary = prepare_snapshot_with_ops(
            &ops,
            "enwiki",
            "2001-01",
            data_dir.path(),
            "governed-run",
            ExecutionMode {
                window_size: 2,
                select_generation: true,
            },
            Some(&governor),
        )
        .expect("governed fixture should complete");
        assert_eq!(summary.source_worker_limit, 2);
        assert_eq!(governor.sample()?.ingested_rows, 20);

        let overflow_governor = ResourceGovernor::new(
            governor.budget().clone(),
            GovernorPaths::new(data_dir.path().to_path_buf(), None),
        );
        overflow_governor.record_source_progress(u64::MAX, 0, 0, 0)?;
        let overflow_ops = FakeOps {
            planned: 1,
            pending: SnapshotPlan::resolve("testwiki", "2026-08")?.sources,
            ..FakeOps::default()
        };
        assert!(
            prepare_snapshot_with_ops(
                &overflow_ops,
                "testwiki",
                "2026-08",
                data_dir.path(),
                "overflow-run",
                ExecutionMode {
                    window_size: 1,
                    select_generation: true,
                },
                Some(&overflow_governor),
            )
            .is_err()
        );
        Ok(())
    }

    #[test]
    fn source_scheduler_retries_admission_after_in_flight_work_releases_resources() -> Result<()> {
        let data_dir = TestDir::new()?;
        let pending = SnapshotPlan::resolve("enwiki", "2001-02")?
            .sources
            .into_iter()
            .take(2)
            .collect::<Vec<_>>();
        let ops = FakeOps {
            planned: 2,
            pending,
            ..FakeOps::default()
        };
        let governor = ResourceGovernor::new(
            ResourceBudget {
                memory_ceiling_bytes: u64::MAX,
                memory_reserve_bytes: 0,
                persistent_storage_reserve_bytes: 0,
                bounded_scratch_reserve_bytes: 0,
                rollback_generation_reserve_bytes: 0,
                scratch_limit_bytes: u64::MAX,
                max_open_files: 512,
                source_worker_limit: 2,
                thread_limit: 2,
                max_logical_partition_bytes: u64::MAX,
                max_active_parquet_writers: 16,
                weekly_worker_limit: 1,
            },
            GovernorPaths::new(data_dir.path().to_path_buf(), None),
        )
        .with_persistent_available_sequence([100, 100, 0]);

        let summary = prepare_snapshot_with_ops(
            &ops,
            "enwiki",
            "2001-02",
            data_dir.path(),
            "admission-shrink-run",
            ExecutionMode {
                window_size: 2,
                select_generation: false,
            },
            Some(&governor),
        )
        .expect("the source window should shrink and process every source");

        assert_eq!(summary.source_worker_limit, 2);
        assert_eq!(summary.ingested_sources, 2);
        assert_eq!(summary.ingested_rows, 20);
        assert_eq!(
            ops.ingested
                .lock()
                .expect("fake ingested mutex poisoned")
                .len(),
            2
        );
        assert!(ops.finalized.load(Ordering::Relaxed));
        assert!(!ops.selected_generation.load(Ordering::Relaxed));
        Ok(())
    }

    #[test]
    fn disk_reserve_exhaustion_stops_after_committed_source_without_finalizing() -> Result<()> {
        let data_dir = TestDir::new()?;
        let pending = SnapshotPlan::resolve("enwiki", "2001-02")?
            .sources
            .into_iter()
            .take(2)
            .collect::<Vec<_>>();
        let ops = FakeOps {
            planned: 2,
            pending,
            ..FakeOps::default()
        };
        let governor = ResourceGovernor::new(
            ResourceBudget {
                memory_ceiling_bytes: u64::MAX,
                memory_reserve_bytes: 0,
                persistent_storage_reserve_bytes: 0,
                bounded_scratch_reserve_bytes: 0,
                rollback_generation_reserve_bytes: 0,
                scratch_limit_bytes: u64::MAX,
                max_open_files: 512,
                source_worker_limit: 1,
                thread_limit: 1,
                max_logical_partition_bytes: u64::MAX,
                max_active_parquet_writers: 16,
                weekly_worker_limit: 1,
            },
            GovernorPaths::new(data_dir.path().to_path_buf(), None),
        )
        .with_persistent_available_sequence([100, 100, 100, 0]);

        let error = prepare_snapshot_with_ops(
            &ops,
            "enwiki",
            "2001-02",
            data_dir.path(),
            "disk-exhaustion-run",
            ExecutionMode {
                window_size: 1,
                select_generation: false,
            },
            Some(&governor),
        )
        .expect_err("the second source admission must observe exhausted disk reserve");

        assert!(
            error
                .chain()
                .any(|cause| cause.to_string().contains("storage gate closed")),
            "expected storage exhaustion in error chain, got: {error:#}",
        );
        assert_eq!(
            ops.ingested
                .lock()
                .expect("fake ingested mutex poisoned")
                .len(),
            1
        );
        assert!(!ops.finalized.load(Ordering::Relaxed));
        assert_eq!(governor.sample()?.ingested_rows, 10);
        Ok(())
    }

    #[test]
    fn snapshot_preparation_rejects_fetch_and_commit_mismatches() -> Result<()> {
        let data_dir = TestDir::new()?;
        let pending = SnapshotPlan::resolve("testwiki", "2026-08")?.sources;
        let missing_path = FakeOps {
            planned: 1,
            pending: pending.clone(),
            path_count_delta: -1,
            ..FakeOps::default()
        };
        assert!(
            prepare_snapshot_with_ops(
                &missing_path,
                "testwiki",
                "2026-08",
                data_dir.path(),
                "run-1",
                ExecutionMode {
                    window_size: 1,
                    select_generation: true,
                },
                None,
            )
            .unwrap_err()
            .chain()
            .any(|cause| cause.to_string().contains("incomplete path set"))
        );

        let wrong_commit = FakeOps {
            planned: 1,
            pending,
            wrong_commit: true,
            ..FakeOps::default()
        };
        assert!(
            prepare_snapshot_with_ops(
                &wrong_commit,
                "testwiki",
                "2026-08",
                data_dir.path(),
                "run-2",
                ExecutionMode {
                    window_size: 1,
                    select_generation: true,
                },
                None,
            )
            .unwrap_err()
            .chain()
            .any(|cause| cause.to_string().contains("wrong source"))
        );

        let wrong_path = FakeOps {
            planned: 1,
            pending: SnapshotPlan::resolve("testwiki", "2026-08")?.sources,
            wrong_path: true,
            ..FakeOps::default()
        };
        assert!(
            prepare_snapshot_with_ops(
                &wrong_path,
                "testwiki",
                "2026-08",
                data_dir.path(),
                "run-3",
                ExecutionMode {
                    window_size: 1,
                    select_generation: true,
                },
                None,
            )
            .unwrap_err()
            .chain()
            .any(|cause| cause.to_string().contains("wrong path"))
        );
        let ingest_error = FakeOps {
            planned: 1,
            pending: SnapshotPlan::resolve("testwiki", "2026-08")?.sources,
            ingest_error: true,
            ..FakeOps::default()
        };
        assert!(
            prepare_snapshot_with_ops(
                &ingest_error,
                "testwiki",
                "2026-08",
                data_dir.path(),
                "run-4",
                ExecutionMode {
                    window_size: 1,
                    select_generation: true,
                },
                None,
            )
            .unwrap_err()
            .chain()
            .any(|cause| cause.to_string().contains("injected ingest failure"))
        );
        Ok(())
    }

    #[test]
    fn pipelined_source_failures_stop_without_finalizing() -> Result<()> {
        let run = |ops: &FakeOps, root: &Path, run_id: &str| {
            prepare_snapshot_with_ops(
                ops,
                "enwiki",
                "2001-02",
                root,
                run_id,
                ExecutionMode {
                    window_size: 2,
                    select_generation: false,
                },
                None,
            )
        };
        let pending = SnapshotPlan::resolve("enwiki", "2001-02")?
            .sources
            .into_iter()
            .take(2)
            .collect::<Vec<_>>();

        let fetch_root = TestDir::new()?;
        let fetch_error = FakeOps {
            planned: 2,
            pending: pending.clone(),
            path_count_delta: -1,
            ..FakeOps::default()
        };
        assert!(run(&fetch_error, fetch_root.path(), "fetch-error").is_err());
        assert!(!fetch_error.finalized.load(Ordering::Relaxed));

        let ingest_root = TestDir::new()?;
        let ingest_error = FakeOps {
            planned: 2,
            pending: pending.clone(),
            ingest_error: true,
            ..FakeOps::default()
        };
        assert!(run(&ingest_error, ingest_root.path(), "ingest-error").is_err());
        assert!(!ingest_error.finalized.load(Ordering::Relaxed));

        let panic_root = TestDir::new()?;
        let panic = FakeOps {
            planned: 2,
            pending,
            panic_fetch: true,
            ..FakeOps::default()
        };
        let error = run(&panic, panic_root.path(), "panic")
            .expect_err("a producer panic must become a normal pipeline error");
        assert!(error.chain().any(|cause| {
            cause
                .to_string()
                .contains("source prefetch worker panicked")
        }));
        assert!(!panic.finalized.load(Ordering::Relaxed));
        Ok(())
    }

    #[test]
    fn real_source_transaction_boundaries_fail_before_network_for_invalid_inputs() -> Result<()> {
        let data_dir = TestDir::new()?;
        let ops = RealSourceTransactionOps;
        assert_eq!(
            ops.planned_sources("testwiki", "2026-08", data_dir.path())?,
            1
        );
        assert!(
            ops.planned_sources("../bad", "2026-08", data_dir.path())
                .is_err()
        );
        assert_eq!(
            ops.cleanup_committed("testwiki", "2026-08", data_dir.path())?,
            0
        );
        let mut pinned = SnapshotPlan::resolve("testwiki", "2026-08")?.sources;
        pinned[0].expected_size = Some(9);
        assert_eq!(
            ops.source_sizes(data_dir.path(), "testwiki", "2026-08", &pinned)?,
            vec![Some(9)]
        );
        assert_eq!(
            single_source_path(vec![std::path::PathBuf::from("only")])?,
            std::path::PathBuf::from("only")
        );
        assert!(single_source_path(Vec::new()).is_err());
        assert!(
            ops.pending_sources("../bad", "2026-08", data_dir.path())
                .is_err()
        );
        assert!(
            ops.fetch_source(
                "testwiki",
                "2026-08",
                data_dir.path(),
                "../bad",
                &SnapshotPlan::resolve("testwiki", "2026-08")?.sources[0],
            )
            .is_err()
        );
        let missing = data_dir.path().join("2026-08.testwiki.all-time.tsv.bz2");
        assert!(
            ops.ingest_source("testwiki", "invalid", data_dir.path(), &missing, "run",)
                .is_err()
        );
        assert!(
            ops.finalize("../bad", "2026-08", data_dir.path(), true)
                .is_err()
        );
        assert!(prepare_snapshot("../bad", "2026-08", data_dir.path(), "run", 1,).is_err());
        Ok(())
    }

    #[test]
    fn real_source_transaction_finalize_commits_receipts_and_pointer() -> Result<()> {
        let data_dir = TestDir::new()?;
        let wiki = "testwiki";
        let snapshot = "2026-08";
        let (plan, _) = SnapshotPlan::load_or_resolve(data_dir.path(), wiki, snapshot)?;
        let analytical =
            crate::storage::snapshot_analytical_wiki_dir(data_dir.path(), wiki, snapshot)?;
        let warehouse =
            crate::storage::snapshot_warehouse_wiki_dir(data_dir.path(), wiki, snapshot)?;
        std::fs::create_dir_all(&warehouse)?;
        crate::storage::write_test_marker_in(
            data_dir.path(),
            &analytical,
            &plan.sources[0].source_id,
        )
        .expect("strict source marker fixture should be written");

        RealSourceTransactionOps.finalize(wiki, snapshot, data_dir.path(), true)?;

        assert_eq!(
            crate::storage::current_snapshot_version(data_dir.path(), wiki)?.as_deref(),
            Some(snapshot)
        );
        prepare_snapshot(wiki, snapshot, data_dir.path(), "governed-finished", 1)?;
        Ok(())
    }

    #[test]
    fn real_candidate_finalize_commits_receipts_without_switching_pointer() -> Result<()> {
        let data_dir = TestDir::new()?;
        let wiki = "testwiki";
        let snapshot = "2026-08";
        let (plan, _) = SnapshotPlan::load_or_resolve(data_dir.path(), wiki, snapshot)?;
        let analytical =
            crate::storage::snapshot_analytical_wiki_dir(data_dir.path(), wiki, snapshot)?;
        let warehouse =
            crate::storage::snapshot_warehouse_wiki_dir(data_dir.path(), wiki, snapshot)?;
        std::fs::create_dir_all(&warehouse)?;
        crate::storage::write_test_marker_in(
            data_dir.path(),
            &analytical,
            &plan.sources[0].source_id,
        )
        .expect("candidate marker fixture should be writable");

        RealSourceTransactionOps.finalize(wiki, snapshot, data_dir.path(), false)?;

        assert!(
            crate::storage::generation_manifest_path(data_dir.path(), wiki, snapshot)?.is_file()
        );
        assert_eq!(
            crate::storage::current_snapshot_version(data_dir.path(), wiki)?,
            None
        );
        prepare_candidate_snapshot(wiki, snapshot, data_dir.path(), "governed-candidate", 1)?;
        Ok(())
    }

    #[test]
    fn candidate_validation_failure_preserves_current_generation_pointer() -> Result<()> {
        let data_dir = TestDir::new()?;
        let wiki = "testwiki";
        let current = "2026-07";
        let candidate = "2026-08";
        let (current_plan, _) = SnapshotPlan::load_or_resolve(data_dir.path(), wiki, current)?;
        let current_analytical =
            crate::storage::snapshot_analytical_wiki_dir(data_dir.path(), wiki, current)?;
        let current_warehouse =
            crate::storage::snapshot_warehouse_wiki_dir(data_dir.path(), wiki, current)?;
        std::fs::create_dir_all(&current_warehouse)?;
        crate::storage::write_test_marker_in(
            data_dir.path(),
            &current_analytical,
            &current_plan.sources[0].source_id,
        )
        .expect("current generation marker fixture must be valid");
        RealSourceTransactionOps.finalize(wiki, current, data_dir.path(), true)?;
        assert_eq!(
            crate::storage::current_snapshot_version(data_dir.path(), wiki)?.as_deref(),
            Some(current)
        );

        SnapshotPlan::load_or_resolve(data_dir.path(), wiki, candidate)?;
        let error = RealSourceTransactionOps
            .finalize(wiki, candidate, data_dir.path(), false)
            .expect_err("candidate without an ingest generation must fail validation");

        assert!(
            error
                .to_string()
                .contains("no committed ingest or compaction proof"),
            "unexpected candidate validation error: {error:#}"
        );
        assert_eq!(
            crate::storage::current_snapshot_version(data_dir.path(), wiki)?.as_deref(),
            Some(current)
        );
        assert!(
            !crate::storage::generation_manifest_path(data_dir.path(), wiki, candidate)?.exists()
        );
        Ok(())
    }

    #[test]
    fn governed_snapshot_profiles_a_pending_pinned_source_without_network() -> Result<()> {
        let data_dir = TestDir::new()?;
        SnapshotPlan::load_or_resolve(data_dir.path(), "testwiki", "2026-08")?;

        let governor =
            governed_snapshot_with_sizes(data_dir.path(), "testwiki", "2026-08", 2, |sources| {
                assert_eq!(sources.len(), 1);
                Ok(vec![Some(42)])
            })?;
        assert!((1..=2).contains(&governor.budget().source_worker_limit));
        let profile = crate::workload_profile::load(data_dir.path(), "testwiki", "2026-08")?
            .context("governed snapshot should persist its profile")?;
        assert_eq!(profile.signals.total_compressed_bytes, 42);
        Ok(())
    }
}
