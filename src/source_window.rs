use anyhow::{Context, Result};
use rayon::prelude::*;
use serde::Serialize;
use std::ffi::OsStr;
use std::path::Path;
use std::time::Instant;
use tracing::info;

use crate::resource_governor::{GovernorPaths, ResourceGovernor};
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
    fn source_sizes(&self, sources: &[SourceSpec]) -> Result<Vec<Option<u64>>>;
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

    fn source_sizes(&self, sources: &[SourceSpec]) -> Result<Vec<Option<u64>>> {
        fetch::snapshot_source_sizes(sources)
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

fn execute_bounded<T, F>(items: &[T], window_size: usize, mut process: F) -> Result<usize>
where
    F: FnMut(&[T]) -> Result<usize>,
{
    anyhow::ensure!(
        (1..=MAX_SOURCE_WINDOW_SIZE).contains(&window_size),
        "source-window size must be between 1 and {MAX_SOURCE_WINDOW_SIZE}, got {window_size}"
    );
    let mut completed = 0_usize;
    for window in items.chunks(window_size) {
        completed = completed
            .checked_add(process(window)?)
            .context("completed source count overflow")?;
    }
    Ok(completed)
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
    expected_bytes: u64,
) -> Result<u64> {
    let permit = execution
        .governor
        .map(|governor| governor.admit_source(expected_bytes))
        .transpose()?;
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
        commit.source_id == source.source_id,
        "ingest committed the wrong source for {}",
        source.source_id
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
        fetch::snapshot_source_sizes(sources)
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
        if crate::storage::marker_manifest_is_valid_in(data_dir, &analytical, &source.source_id)?
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
    let source_sizes = ops.source_sizes(&pending)?;
    anyhow::ensure!(
        source_sizes.len() == pending.len(),
        "resource preflight returned an incomplete source-size inventory"
    );
    if let Some(governor) = governor {
        governor.preflight_snapshot(&source_sizes, window_size)?;
    }
    let reused_sources = planned_sources
        .checked_sub(pending.len())
        .context("pending source inventory exceeds snapshot plan")?;
    info!(
        wiki,
        snapshot,
        run_id,
        window_size,
        planned_sources,
        reused_sources,
        pending_sources = pending.len(),
        recovered_inputs,
        "starting bounded source-window execution"
    );

    let source_worker_limit = governor
        .map(|governor| governor.budget().source_worker_limit)
        .unwrap_or(1)
        .min(window_size);
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(source_worker_limit)
        .thread_name(|index| format!("source-worker-{index}"))
        .build()
        .context("failed to create governed source worker pool")?;
    let execution = SourceExecution {
        wiki,
        snapshot,
        data_dir,
        run_id,
        governor,
    };
    let mut ingested_rows = 0_u64;
    let ingested_sources = execute_bounded(&pending, window_size, |sources| {
        let offset = pending
            .iter()
            .position(|candidate| candidate.source_id == sources[0].source_id)
            .context("source window is not part of pending inventory")?;
        let rows = pool.install(|| {
            sources
                .par_iter()
                .enumerate()
                .map(|(index, source)| {
                    let expected_bytes = source_sizes[offset + index]
                        .context("source size became unknown after preflight")?;
                    process_source(ops, &execution, source, expected_bytes)
                })
                .collect::<Result<Vec<_>>>()
        })?;
        let rows = rows.into_iter().try_fold(0_u64, |total, rows| {
            total
                .checked_add(rows)
                .context("source-window row count overflow")
        })?;
        ingested_rows = ingested_rows
            .checked_add(rows)
            .context("snapshot ingest row count overflow")?;
        Ok(sources.len())
    })?;

    ops.finalize(wiki, snapshot, data_dir, select_generation)?;
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
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, Ordering};

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

        fn source_sizes(&self, sources: &[SourceSpec]) -> Result<Vec<Option<u64>>> {
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
    fn bounded_execution_never_exceeds_the_window_and_keeps_commits() -> Result<()> {
        let items = [1_u8, 2, 3, 4, 5, 6, 7];
        let mut windows = Vec::new();
        let completed = execute_bounded(&items, 3, |window| {
            windows.push(window.to_vec());
            Ok(window.len())
        })?;
        assert_eq!(completed, items.len());
        assert_eq!(windows, vec![vec![1, 2, 3], vec![4, 5, 6], vec![7]]);
        Ok(())
    }

    #[test]
    fn bounded_execution_stops_at_the_failed_window() {
        let items = [1_u8, 2, 3, 4, 5];
        let mut windows = Vec::new();
        let error = execute_bounded(&items, 2, |window| {
            windows.push(window.to_vec());
            anyhow::ensure!(window[0] != 3, "interrupted");
            Ok(window.len())
        })
        .expect_err("second window must fail");
        assert!(error.to_string().contains("interrupted"));
        assert_eq!(windows, vec![vec![1, 2], vec![3, 4]]);
    }

    #[test]
    fn bounded_execution_rejects_an_invalid_window() {
        assert!(execute_bounded(&[1_u8], 0, |_| Ok(1)).is_err());
    }

    #[test]
    fn snapshot_preparation_batches_pending_sources_and_reports_reuse() -> Result<()> {
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
            .to_string()
            .contains("incomplete path set")
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
            .to_string()
            .contains("wrong source")
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
            .to_string()
            .contains("wrong path")
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
            .to_string()
            .contains("injected ingest failure")
        );
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
        assert_eq!(ops.source_sizes(&pinned)?, vec![Some(9)]);
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
    fn governed_snapshot_profiles_a_pending_pinned_source_without_network() -> Result<()> {
        let data_dir = TestDir::new()?;
        SnapshotPlan::load_or_resolve(data_dir.path(), "testwiki", "2026-08")?;

        let governor =
            governed_snapshot_with_sizes(data_dir.path(), "testwiki", "2026-08", 2, |sources| {
                assert_eq!(sources.len(), 1);
                Ok(vec![Some(42)])
            })?;
        assert_eq!(governor.budget().source_worker_limit, 2);
        let profile = crate::workload_profile::load(data_dir.path(), "testwiki", "2026-08")?
            .context("governed snapshot should persist its profile")?;
        assert_eq!(profile.signals.total_compressed_bytes, 42);
        Ok(())
    }
}
