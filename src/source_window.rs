use anyhow::{Context, Result};
use serde::Serialize;
use std::ffi::OsStr;
use std::path::Path;
use tracing::info;

use crate::snapshot_plan::{SnapshotPlan, SourceSpec};
use crate::{fetch, ingest};

pub(crate) const SOURCE_WINDOW_SIZE_ENV: &str = "WIKI_ECON_SOURCE_WINDOW_SIZE";
pub(crate) const DEFAULT_SOURCE_WINDOW_SIZE: usize = 1;
pub(crate) const MAX_SOURCE_WINDOW_SIZE: usize = 4;

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub(crate) struct SourceWindowSummary {
    pub(crate) wiki: String,
    pub(crate) snapshot: String,
    pub(crate) window_size: usize,
    pub(crate) planned_sources: usize,
    pub(crate) reused_sources: usize,
    pub(crate) ingested_sources: usize,
    pub(crate) ingested_rows: u64,
}

trait SourceTransactionOps {
    fn planned_sources(&self, wiki: &str, snapshot: &str, data_dir: &Path) -> Result<usize>;
    fn cleanup_committed(&self, wiki: &str, snapshot: &str, data_dir: &Path) -> Result<usize>;
    fn pending_sources(
        &self,
        wiki: &str,
        snapshot: &str,
        data_dir: &Path,
    ) -> Result<Vec<SourceSpec>>;
    fn fetch_window(
        &self,
        wiki: &str,
        snapshot: &str,
        data_dir: &Path,
        run_id: &str,
        sources: &[SourceSpec],
    ) -> Result<Vec<std::path::PathBuf>>;
    fn ingest_source(
        &self,
        wiki: &str,
        snapshot: &str,
        data_dir: &Path,
        source: &Path,
        run_id: &str,
    ) -> Result<ingest::SourceIngestCommit>;
    fn finalize(&self, wiki: &str, snapshot: &str, data_dir: &Path) -> Result<()>;
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

    fn fetch_window(
        &self,
        wiki: &str,
        snapshot: &str,
        data_dir: &Path,
        run_id: &str,
        sources: &[SourceSpec],
    ) -> Result<Vec<std::path::PathBuf>> {
        fetch::fetch_snapshot_source_window(wiki, snapshot, data_dir, run_id, sources)
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

    fn finalize(&self, wiki: &str, snapshot: &str, data_dir: &Path) -> Result<()> {
        fetch::finalize_snapshot_fetch(wiki, snapshot, data_dir)?;
        ingest::finalize_snapshot_ingest(wiki, snapshot, data_dir)?;
        fetch::cleanup_committed_source_window_inputs(wiki, snapshot, data_dir)?;
        Ok(())
    }
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

fn process_source_window<O: SourceTransactionOps>(
    ops: &O,
    wiki: &str,
    snapshot: &str,
    data_dir: &Path,
    run_id: &str,
    sources: &[SourceSpec],
) -> Result<(usize, u64)> {
    let paths = ops.fetch_window(wiki, snapshot, data_dir, run_id, sources)?;
    anyhow::ensure!(
        paths.len() == sources.len(),
        "source-window fetch returned an incomplete path set"
    );
    let mut rows = 0_u64;
    for (source, path) in sources.iter().zip(paths) {
        anyhow::ensure!(
            path.file_name().and_then(|name| name.to_str()) == Some(source.filename()?),
            "source-window fetch returned the wrong path for {}",
            source.source_id
        );
        let commit = ops.ingest_source(wiki, snapshot, data_dir, &path, run_id)?;
        anyhow::ensure!(
            commit.source_id == source.source_id,
            "ingest committed the wrong source for {}",
            source.source_id
        );
        rows = rows
            .checked_add(u64::try_from(commit.rows)?)
            .context("source-window row count overflow")?;
    }
    Ok((sources.len(), rows))
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
    prepare_snapshot_with_ops(
        &RealSourceTransactionOps,
        wiki,
        snapshot,
        data_dir,
        run_id,
        window_size,
    )
}

fn prepare_snapshot_with_ops<O: SourceTransactionOps>(
    ops: &O,
    wiki: &str,
    snapshot: &str,
    data_dir: &Path,
    run_id: &str,
    window_size: usize,
) -> Result<SourceWindowSummary> {
    let planned_sources = ops.planned_sources(wiki, snapshot, data_dir)?;
    let recovered_inputs = ops.cleanup_committed(wiki, snapshot, data_dir)?;
    let pending = ops.pending_sources(wiki, snapshot, data_dir)?;
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

    let mut ingested_rows = 0_u64;
    let ingested_sources = execute_bounded(&pending, window_size, |sources| {
        let (completed, rows) =
            process_source_window(ops, wiki, snapshot, data_dir, run_id, sources)?;
        ingested_rows = ingested_rows
            .checked_add(rows)
            .context("snapshot ingest row count overflow")?;
        Ok(completed)
    })?;

    ops.finalize(wiki, snapshot, data_dir)?;
    let summary = SourceWindowSummary {
        wiki: wiki.to_string(),
        snapshot: snapshot.to_string(),
        window_size,
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
    use crate::test_support::TestDir;
    use std::cell::{Cell, RefCell};

    #[derive(Default)]
    struct FakeOps {
        planned: usize,
        pending: Vec<SourceSpec>,
        windows: RefCell<Vec<Vec<String>>>,
        ingested: RefCell<Vec<String>>,
        finalized: Cell<bool>,
        path_count_delta: isize,
        wrong_path: bool,
        wrong_commit: bool,
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

        fn fetch_window(
            &self,
            _wiki: &str,
            _snapshot: &str,
            _data_dir: &Path,
            _run_id: &str,
            sources: &[SourceSpec],
        ) -> Result<Vec<std::path::PathBuf>> {
            self.windows.borrow_mut().push(
                sources
                    .iter()
                    .map(|source| source.source_id.clone())
                    .collect(),
            );
            let mut paths = sources
                .iter()
                .map(|source| source.filename().map(std::path::PathBuf::from))
                .collect::<Result<Vec<_>>>()?;
            if self.path_count_delta < 0 {
                paths.pop();
            }
            if self.wrong_path && !paths.is_empty() {
                paths[0] = std::path::PathBuf::from("unexpected.tsv.bz2");
            }
            Ok(paths)
        }

        fn ingest_source(
            &self,
            _wiki: &str,
            _snapshot: &str,
            _data_dir: &Path,
            source: &Path,
            _run_id: &str,
        ) -> Result<ingest::SourceIngestCommit> {
            let source_id = ingest::ingest_source_id(source)?;
            self.ingested.borrow_mut().push(source_id.clone());
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

        fn finalize(&self, _wiki: &str, _snapshot: &str, _data_dir: &Path) -> Result<()> {
            self.finalized.set(true);
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

        let summary =
            prepare_snapshot_with_ops(&ops, "enwiki", "2001-03", data_dir.path(), "run-1", 2)?;

        assert_eq!(summary.planned_sources, 5);
        assert_eq!(summary.reused_sources, 2);
        assert_eq!(summary.ingested_sources, 3);
        assert_eq!(summary.ingested_rows, 30);
        assert_eq!(
            ops.windows
                .borrow()
                .iter()
                .map(Vec::len)
                .collect::<Vec<_>>(),
            vec![2, 1]
        );
        assert_eq!(ops.ingested.borrow().len(), 3);
        assert!(ops.finalized.get());
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
                1,
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
                1,
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
                1,
            )
            .unwrap_err()
            .to_string()
            .contains("wrong path")
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
        assert!(
            ops.pending_sources("../bad", "2026-08", data_dir.path())
                .is_err()
        );
        assert!(
            ops.fetch_window("testwiki", "2026-08", data_dir.path(), "../bad", &[],)
                .is_err()
        );
        let missing = data_dir.path().join("2026-08.testwiki.all-time.tsv.bz2");
        assert!(
            ops.ingest_source("testwiki", "invalid", data_dir.path(), &missing, "run",)
                .is_err()
        );
        assert!(ops.finalize("../bad", "2026-08", data_dir.path()).is_err());
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

        RealSourceTransactionOps.finalize(wiki, snapshot, data_dir.path())?;

        assert_eq!(
            crate::storage::current_snapshot_version(data_dir.path(), wiki)?.as_deref(),
            Some(snapshot)
        );
        Ok(())
    }
}
