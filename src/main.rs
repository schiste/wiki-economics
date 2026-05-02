#![forbid(unsafe_code)]

mod bench;
mod compute;
mod fetch;
mod ingest;
mod merge;
mod patrol;
mod schema;
mod storage;
#[cfg(test)]
mod end_to_end_tests;
#[cfg(test)]
mod test_support;

use anyhow::Result;
use chrono::{DateTime, Datelike, Utc};
use clap::{Parser, Subcommand};
use std::path::PathBuf;
use std::time::Instant;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(name = "wiki-econ", about = "Wikipedia economic analysis toolkit")]
struct Cli {
    /// Base data directory
    #[arg(long, default_value = "data")]
    data_dir: PathBuf,

    /// Output directory for computed metrics
    #[arg(long, default_value = "output")]
    output_dir: PathBuf,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Download dump files from Wikimedia
    Fetch {
        /// Wiki database names (e.g., nlwiki frwiki dewiki)
        wikis: Vec<String>,

        /// Dump snapshot version (YYYY-MM)
        #[arg(long)]
        version: Option<String>,
    },

    /// Convert raw TSV.bz2 dumps to Parquet
    Ingest {
        /// Wiki database names
        wikis: Vec<String>,
    },

    /// Compute economic metrics from Parquet data
    Compute {
        /// Wiki database names
        wikis: Vec<String>,
    },

    /// Merge per-wiki outputs into combined parquet files
    Merge,

    /// Download and parse patrol logging data
    PatrolFetch {
        /// Wiki database names
        wikis: Vec<String>,
    },

    /// Compute patrol metrics only
    PatrolCompute {
        /// Wiki database names
        wikis: Vec<String>,

        /// Recompute all patrol months from scratch
        #[arg(long, default_value_t = false)]
        rebuild: bool,

        /// Limit computation to the first N pending months
        #[arg(long)]
        limit_months: Option<usize>,
    },

    /// Benchmark compute performance on existing parquet data
    Bench {
        /// Wiki database names
        wikis: Vec<String>,

        /// Warmup iterations before timing
        #[arg(long, default_value_t = 1)]
        warmup: usize,

        /// Measured iterations
        #[arg(long, default_value_t = 5)]
        iterations: usize,

        /// Keep per-iteration outputs under --output-dir/bench
        #[arg(long, default_value_t = false)]
        keep_outputs: bool,
    },

    /// Run the full pipeline: fetch → ingest → compute → merge
    Run {
        /// Wiki database names
        wikis: Vec<String>,

        /// Dump snapshot version (YYYY-MM)
        #[arg(long)]
        version: Option<String>,
    },
}

trait Ops {
    fn fetch_wiki(&self, wiki: &str, version: &str, data_dir: &std::path::Path) -> Result<()>;
    fn fetch_patrol(&self, wiki: &str, data_dir: &std::path::Path) -> Result<()>;
    fn ingest_wiki(&self, wiki: &str, data_dir: &std::path::Path) -> Result<()>;
    fn compute_all(
        &self,
        wiki: &str,
        data_dir: &std::path::Path,
        output_dir: &std::path::Path,
    ) -> Result<()>;
    fn compute_patrol(
        &self,
        wiki: &str,
        data_dir: &std::path::Path,
        output_dir: &std::path::Path,
        rebuild: bool,
        limit_months: Option<usize>,
    ) -> Result<()>;
    fn benchmark(
        &self,
        wikis: &[String],
        data_dir: &std::path::Path,
        output_dir: &std::path::Path,
        warmup: usize,
        iterations: usize,
        keep_outputs: bool,
    ) -> Result<()>;
    fn merge_outputs(&self, output_dir: &std::path::Path) -> Result<()>;
}

struct RealOps;

impl Ops for RealOps {
    fn fetch_wiki(&self, wiki: &str, version: &str, data_dir: &std::path::Path) -> Result<()> {
        fetch::fetch_wiki(wiki, version, data_dir).map(|_| ())
    }

    fn fetch_patrol(&self, wiki: &str, data_dir: &std::path::Path) -> Result<()> {
        patrol::fetch_patrol(wiki, data_dir)
    }

    fn ingest_wiki(&self, wiki: &str, data_dir: &std::path::Path) -> Result<()> {
        ingest::ingest_wiki(wiki, data_dir).map(|_| ())
    }

    fn compute_all(
        &self,
        wiki: &str,
        data_dir: &std::path::Path,
        output_dir: &std::path::Path,
    ) -> Result<()> {
        compute::compute_all(wiki, data_dir, output_dir)
    }

    fn compute_patrol(
        &self,
        wiki: &str,
        data_dir: &std::path::Path,
        output_dir: &std::path::Path,
        rebuild: bool,
        limit_months: Option<usize>,
    ) -> Result<()> {
        patrol::compute_patrol(wiki, data_dir, output_dir, rebuild, limit_months)
    }

    fn benchmark(
        &self,
        wikis: &[String],
        data_dir: &std::path::Path,
        output_dir: &std::path::Path,
        warmup: usize,
        iterations: usize,
        keep_outputs: bool,
    ) -> Result<()> {
        bench::run(
            wikis,
            data_dir,
            output_dir,
            warmup,
            iterations,
            keep_outputs,
        )
    }

    fn merge_outputs(&self, output_dir: &std::path::Path) -> Result<()> {
        merge::merge_outputs(output_dir)
    }
}

fn run_timed_stage<T>(
    stage: &str,
    wiki: Option<&str>,
    action: impl FnOnce() -> Result<T>,
) -> Result<T> {
    let started = Instant::now();
    info!(stage = stage, wiki = wiki.unwrap_or("-"), "starting stage");
    let result = action();
    if result.is_ok() {
        info!(
            stage = stage,
            wiki = wiki.unwrap_or("-"),
            elapsed_ms = started.elapsed().as_secs_f64() * 1_000.0,
            "completed stage"
        );
    }
    result
}

fn snapshot_version_for(now: DateTime<Utc>) -> String {
    let current_month = now.month();
    let (year, month) = if current_month == 1 {
        (now.year() - 1, 12)
    } else {
        (now.year(), current_month - 1)
    };
    format!("{year:04}-{month:02}")
}

fn default_snapshot_version() -> String {
    snapshot_version_for(Utc::now())
}

fn run_with_ops(cli: Cli, ops: &impl Ops) -> Result<()> {
    let data_dir = cli.data_dir;
    let output_dir = cli.output_dir;

    match cli.command {
        Commands::Fetch { wikis, version } => {
            let version = version.unwrap_or_else(default_snapshot_version);
            for wiki in &wikis {
                run_timed_stage("fetch", Some(wiki), || {
                    ops.fetch_wiki(wiki, &version, &data_dir)
                })?;
                run_timed_stage("patrol_fetch", Some(wiki), || {
                    ops.fetch_patrol(wiki, &data_dir)
                })?;
            }
        }

        Commands::Ingest { wikis } => {
            for wiki in &wikis {
                run_timed_stage("ingest", Some(wiki), || ops.ingest_wiki(wiki, &data_dir))?;
            }
        }

        Commands::Compute { wikis } => {
            for wiki in &wikis {
                run_timed_stage("compute", Some(wiki), || {
                    ops.compute_all(wiki, &data_dir, &output_dir)
                })?;
                run_timed_stage("patrol_compute", Some(wiki), || {
                    ops.compute_patrol(wiki, &data_dir, &output_dir, false, None)
                })?;
            }
            run_timed_stage("merge", None, || ops.merge_outputs(&output_dir))?;
        }

        Commands::Merge => {
            run_timed_stage("merge", None, || ops.merge_outputs(&output_dir))?;
        }

        Commands::PatrolFetch { wikis } => {
            for wiki in &wikis {
                run_timed_stage("patrol_fetch", Some(wiki), || {
                    ops.fetch_patrol(wiki, &data_dir)
                })?;
            }
        }

        Commands::PatrolCompute {
            wikis,
            rebuild,
            limit_months,
        } => {
            for wiki in &wikis {
                run_timed_stage("patrol_compute", Some(wiki), || {
                    ops.compute_patrol(wiki, &data_dir, &output_dir, rebuild, limit_months)
                })?;
            }
        }

        Commands::Bench {
            wikis,
            warmup,
            iterations,
            keep_outputs,
        } => {
            run_timed_stage("bench", None, || {
                ops.benchmark(
                    &wikis,
                    &data_dir,
                    &output_dir,
                    warmup,
                    iterations,
                    keep_outputs,
                )
            })?;
        }

        Commands::Run { wikis, version } => {
            let version = version.unwrap_or_else(default_snapshot_version);
            for wiki in &wikis {
                info!(wiki = wiki, "running full pipeline");
                run_timed_stage("fetch", Some(wiki), || {
                    ops.fetch_wiki(wiki, &version, &data_dir)
                })?;
                run_timed_stage("patrol_fetch", Some(wiki), || {
                    ops.fetch_patrol(wiki, &data_dir)
                })?;
                run_timed_stage("ingest", Some(wiki), || ops.ingest_wiki(wiki, &data_dir))?;
                run_timed_stage("compute", Some(wiki), || {
                    ops.compute_all(wiki, &data_dir, &output_dir)
                })?;
                run_timed_stage("patrol_compute", Some(wiki), || {
                    ops.compute_patrol(wiki, &data_dir, &output_dir, false, None)
                })?;
            }
            run_timed_stage("merge", None, || ops.merge_outputs(&output_dir))?;
        }
    }

    Ok(())
}

fn main() -> Result<()> {
    init_tracing();
    run_with_ops(Cli::parse(), &RealOps)
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .without_time()
        .compact()
        .try_init();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{TestDir, init_test_tracing};
    use bzip2::Compression;
    use bzip2::write::BzEncoder;
    use flate2::Compression as GzipCompression;
    use flate2::write::GzEncoder;
    use polars::prelude::*;
    use serde_json::{Value, json};
    use std::cell::RefCell;
    use std::collections::VecDeque;
    use std::fs;
    use std::io::Write;
    use std::path::Path;
    use std::sync::{Arc, Mutex};

    #[derive(Default)]
    struct RecordingOps {
        calls: RefCell<Vec<String>>,
    }

    impl RecordingOps {
        fn record(&self, entry: String) {
            self.calls.borrow_mut().push(entry);
        }
    }

    struct FailingOps {
        fail_stage: &'static str,
    }

    struct FakePatrolTransport {
        bodies: Mutex<VecDeque<Vec<u8>>>,
        json_values: Mutex<VecDeque<Value>>,
    }

    impl FakePatrolTransport {
        fn new(bodies: Vec<Vec<u8>>, json_values: Vec<Value>) -> Self {
            Self {
                bodies: Mutex::new(bodies.into()),
                json_values: Mutex::new(json_values.into()),
            }
        }
    }

    impl crate::patrol::PatrolTransport for FakePatrolTransport {
        fn get(
            &self,
            _url: &str,
            _range_start: Option<u64>,
        ) -> Result<crate::patrol::PatrolTransportResponse> {
            let body = self
                .bodies
                .lock()
                .expect("test transport bodies lock should not be poisoned")
                .pop_front()
                .expect("test transport should have a queued body");
            Ok(crate::patrol::PatrolTransportResponse::from_bytes(body))
        }

        fn get_json(&self, _url: &str) -> Result<Value> {
            self.json_values
                .lock()
                .expect("test transport json values lock should not be poisoned")
                .pop_front()
                .ok_or_else(|| anyhow::anyhow!("test transport should have a queued JSON response"))
        }
    }

    fn sample_dump_row() -> String {
        let mut row = vec![String::new(); crate::schema::COLUMNS.len()];
        for (name, value) in [
            ("wiki_db", "testwiki"),
            ("event_entity", "revision"),
            ("event_type", "create"),
            ("event_timestamp", "2024-01-01 00:00:00.0"),
            ("event_user_id", "42"),
            ("event_user_text", "ExampleUser"),
            ("event_user_is_anonymous", "false"),
            ("event_user_is_temporary", "false"),
            ("event_user_registration_timestamp", "2023-01-01 00:00:00.0"),
            ("event_user_first_edit_timestamp", "2024-01-01 00:00:00.0"),
            ("page_id", "10"),
            ("page_title", "Example"),
            ("page_namespace", "0"),
            ("page_namespace_is_content", "true"),
            ("page_is_redirect", "false"),
            ("revision_id", "100"),
            ("revision_parent_id", "99"),
            ("revision_minor_edit", "false"),
            ("revision_text_bytes", "1200"),
            ("revision_text_bytes_diff", "25"),
            ("revision_is_identity_reverted", "false"),
            ("revision_is_identity_revert", "false"),
        ] {
            let idx = crate::schema::COLUMNS
                .iter()
                .position(|column| column == &name)
                .expect("column should exist");
            row[idx] = value.to_string();
        }
        row.join("\t")
    }

    fn write_bz2_dump(path: &Path) -> Result<()> {
        let file = fs::File::create(path)?;
        let mut encoder = BzEncoder::new(file, Compression::best());
        encoder.write_all(sample_dump_row().as_bytes())?;
        encoder.write_all(b"\n")?;
        encoder.finish()?;
        Ok(())
    }

    fn gzip_bytes(content: &str) -> Result<Vec<u8>> {
        let mut encoder = GzEncoder::new(Vec::new(), GzipCompression::default());
        encoder.write_all(content.as_bytes())?;
        encoder.finish().map_err(Into::into)
    }

    fn write_compute_input(data_dir: &Path, wiki: &str) -> Result<()> {
        let parquet_dir = data_dir.join("parquet").join(wiki);
        fs::create_dir_all(&parquet_dir)?;
        let path = parquet_dir.join("part-000.parquet");
        let mut file = fs::File::create(path)?;
        let columns = vec![
            Column::new("event_entity".into(), vec!["revision", "revision"]),
            Column::new("event_type".into(), vec!["create", "create"]),
            Column::new(
                "event_timestamp".into(),
                vec!["2024-01-01 00:00:00.0", "2024-02-01 00:00:00.0"],
            ),
            Column::new("event_user_id".into(), vec![1_i64, 2]),
            Column::new("event_user_is_bot_by".into(), vec![None::<&str>, None]),
            Column::new("event_user_is_anonymous".into(), vec!["false", "false"]),
            Column::new("event_user_is_temporary".into(), vec!["false", "false"]),
            Column::new("page_namespace".into(), vec![0_i32, 0]),
            Column::new("revision_id".into(), vec![10_i64, 11]),
            Column::new("revision_text_bytes_diff".into(), vec![10_i64, 20]),
            Column::new(
                "revision_is_identity_reverted".into(),
                vec!["false", "false"],
            ),
            Column::new("revision_minor_edit".into(), vec!["false", "false"]),
        ];
        let mut df = DataFrame::new_infer_height(columns)?;
        ParquetWriter::new(&mut file).finish(&mut df)?;
        Ok(())
    }

    fn write_patrol_compute_input(data_dir: &Path, wiki: &str) -> Result<()> {
        let patrol_dir = data_dir.join("patrol").join(wiki);
        fs::create_dir_all(&patrol_dir)?;
        let autopatrol_groups =
            serde_json::to_vec(&json!({ "autopatrol_groups": ["autopatrolled"] }))?;
        fs::write(patrol_dir.join("autopatrol_groups.json"), autopatrol_groups)?;

        let patrol_columns = vec![
            Column::new("timestamp".into(), vec!["2026-01-05 12:00:00"]),
            Column::new("current_revision_id".into(), vec![101_i64]),
            Column::new("prev_revision_id".into(), vec![100_i64]),
            Column::new("user".into(), vec![Some("PatrollerA")]),
        ];
        let mut patrol_df = DataFrame::new_infer_height(patrol_columns)?;
        let mut patrol_file = fs::File::create(patrol_dir.join("patrol.parquet"))?;
        ParquetWriter::new(&mut patrol_file).finish(&mut patrol_df)?;

        let rights_columns = vec![
            Column::new("timestamp".into(), vec!["2026-01-01 00:00:00"]),
            Column::new("target_user".into(), vec![Some("EditorA")]),
            Column::new("old_groups".into(), vec![Some("")]),
            Column::new("new_groups".into(), vec![Some("autopatrolled")]),
        ];
        let mut rights_df = DataFrame::new_infer_height(rights_columns)?;
        let mut rights_file = fs::File::create(patrol_dir.join("rights.parquet"))?;
        ParquetWriter::new(&mut rights_file).finish(&mut rights_df)?;

        let warehouse_dir = crate::storage::warehouse_wiki_dir(data_dir, wiki);
        let partition_dir = crate::storage::month_partition_dir(&warehouse_dir, 2026, "2026-01");
        fs::create_dir_all(&partition_dir)?;
        let revision_columns = vec![
            Column::new("revision_id".into(), vec![101_i64]),
            Column::new("event_timestamp".into(), vec![Some("2026-01-05 10:00:00")]),
            Column::new("event_user_id".into(), vec![Some(1_i64)]),
            Column::new("event_user_text".into(), vec![Some("EditorA")]),
            Column::new("page_namespace".into(), vec![Some(0_i32)]),
            Column::new("event_user_is_bot_by".into(), vec![None::<&str>]),
            Column::new("event_user_is_anonymous".into(), vec![false]),
            Column::new("event_user_is_temporary".into(), vec![false]),
        ];
        let mut revision_df = DataFrame::new_infer_height(revision_columns)?;
        let mut revision_file = fs::File::create(partition_dir.join("part-00000.parquet"))?;
        ParquetWriter::new(&mut revision_file).finish(&mut revision_df)?;
        Ok(())
    }

    impl Ops for RecordingOps {
        fn fetch_wiki(&self, wiki: &str, version: &str, data_dir: &Path) -> Result<()> {
            self.record(format!("fetch:{wiki}:{version}:{}", data_dir.display()));
            Ok(())
        }

        fn fetch_patrol(&self, wiki: &str, data_dir: &Path) -> Result<()> {
            self.record(format!("fetch_patrol:{wiki}:{}", data_dir.display()));
            Ok(())
        }

        fn ingest_wiki(&self, wiki: &str, data_dir: &Path) -> Result<()> {
            self.record(format!("ingest:{wiki}:{}", data_dir.display()));
            Ok(())
        }

        fn compute_all(&self, wiki: &str, data_dir: &Path, output_dir: &Path) -> Result<()> {
            self.record(format!(
                "compute:{wiki}:{}:{}",
                data_dir.display(),
                output_dir.display()
            ));
            Ok(())
        }

        fn compute_patrol(
            &self,
            wiki: &str,
            data_dir: &Path,
            output_dir: &Path,
            rebuild: bool,
            limit_months: Option<usize>,
        ) -> Result<()> {
            let limit_str = limit_months
                .map(|n| n.to_string())
                .unwrap_or_else(|| "_".to_string());
            self.record(format!(
                "compute_patrol:{wiki}:{}:{}:{rebuild}:{limit_str}",
                data_dir.display(),
                output_dir.display()
            ));
            Ok(())
        }

        fn benchmark(
            &self,
            wikis: &[String],
            data_dir: &Path,
            output_dir: &Path,
            warmup: usize,
            iterations: usize,
            keep_outputs: bool,
        ) -> Result<()> {
            self.record(format!(
                "bench:{}:{}:{}:{warmup}:{iterations}:{keep_outputs}",
                wikis.join(","),
                data_dir.display(),
                output_dir.display(),
            ));
            Ok(())
        }

        fn merge_outputs(&self, output_dir: &Path) -> Result<()> {
            self.record(format!("merge:{}", output_dir.display()));
            Ok(())
        }
    }

    impl Ops for FailingOps {
        fn fetch_wiki(&self, _wiki: &str, _version: &str, _data_dir: &Path) -> Result<()> {
            if self.fail_stage == "fetch" {
                anyhow::bail!("fetch failed");
            }
            Ok(())
        }

        fn fetch_patrol(&self, _wiki: &str, _data_dir: &Path) -> Result<()> {
            if self.fail_stage == "fetch_patrol" {
                anyhow::bail!("fetch patrol failed");
            }
            Ok(())
        }

        fn ingest_wiki(&self, _wiki: &str, _data_dir: &Path) -> Result<()> {
            if self.fail_stage == "ingest" {
                anyhow::bail!("ingest failed");
            }
            Ok(())
        }

        fn compute_all(&self, _wiki: &str, _data_dir: &Path, _output_dir: &Path) -> Result<()> {
            if self.fail_stage == "compute" {
                anyhow::bail!("compute failed");
            }
            Ok(())
        }

        fn compute_patrol(
            &self,
            _wiki: &str,
            _data_dir: &Path,
            _output_dir: &Path,
            _rebuild: bool,
            _limit_months: Option<usize>,
        ) -> Result<()> {
            if self.fail_stage == "compute_patrol" {
                anyhow::bail!("compute patrol failed");
            }
            Ok(())
        }

        fn benchmark(
            &self,
            _wikis: &[String],
            _data_dir: &Path,
            _output_dir: &Path,
            _warmup: usize,
            _iterations: usize,
            _keep_outputs: bool,
        ) -> Result<()> {
            if self.fail_stage == "bench" {
                anyhow::bail!("bench failed");
            }
            Ok(())
        }

        fn merge_outputs(&self, _output_dir: &Path) -> Result<()> {
            if self.fail_stage == "merge" {
                anyhow::bail!("merge failed");
            }
            Ok(())
        }
    }

    #[test]
    fn run_with_ops_dispatches_fetch() -> Result<()> {
        init_test_tracing();
        let cli = Cli::try_parse_from([
            "wiki-econ",
            "--data-dir",
            "fixtures/data",
            "fetch",
            "frwiki",
            "dewiki",
            "--version",
            "2026-02",
        ])?;
        let ops = RecordingOps::default();

        run_with_ops(cli, &ops)?;

        assert_eq!(
            ops.calls.into_inner(),
            vec![
                "fetch:frwiki:2026-02:fixtures/data",
                "fetch_patrol:frwiki:fixtures/data",
                "fetch:dewiki:2026-02:fixtures/data",
                "fetch_patrol:dewiki:fixtures/data",
            ]
        );
        Ok(())
    }

    #[test]
    fn run_with_ops_dispatches_ingest() -> Result<()> {
        init_test_tracing();
        let cli = Cli::try_parse_from(["wiki-econ", "ingest", "frwiki"])?;
        let ops = RecordingOps::default();

        run_with_ops(cli, &ops)?;

        assert_eq!(ops.calls.into_inner(), vec!["ingest:frwiki:data"]);
        Ok(())
    }

    #[test]
    fn run_with_ops_dispatches_compute_then_merge() -> Result<()> {
        init_test_tracing();
        let cli = Cli::try_parse_from([
            "wiki-econ",
            "--data-dir",
            "d",
            "--output-dir",
            "o",
            "compute",
            "frwiki",
            "dewiki",
        ])?;
        let ops = RecordingOps::default();

        run_with_ops(cli, &ops)?;

        assert_eq!(
            ops.calls.into_inner(),
            vec![
                "compute:frwiki:d:o",
                "compute_patrol:frwiki:d:o:false:_",
                "compute:dewiki:d:o",
                "compute_patrol:dewiki:d:o:false:_",
                "merge:o",
            ]
        );
        Ok(())
    }

    #[test]
    fn run_with_ops_dispatches_merge() -> Result<()> {
        init_test_tracing();
        let cli = Cli::try_parse_from(["wiki-econ", "--output-dir", "combined", "merge"])?;
        let ops = RecordingOps::default();

        run_with_ops(cli, &ops)?;

        assert_eq!(ops.calls.into_inner(), vec!["merge:combined"]);
        Ok(())
    }

    #[test]
    fn run_with_ops_dispatches_patrol_fetch() -> Result<()> {
        init_test_tracing();
        let cli = Cli::try_parse_from(["wiki-econ", "--data-dir", "d", "patrol-fetch", "frwiki"])?;
        let ops = RecordingOps::default();

        run_with_ops(cli, &ops)?;

        assert_eq!(ops.calls.into_inner(), vec!["fetch_patrol:frwiki:d"]);
        Ok(())
    }

    #[test]
    fn run_with_ops_dispatches_patrol_compute() -> Result<()> {
        init_test_tracing();
        let cli = Cli::try_parse_from([
            "wiki-econ",
            "--data-dir",
            "d",
            "--output-dir",
            "o",
            "patrol-compute",
            "frwiki",
        ])?;
        let ops = RecordingOps::default();

        run_with_ops(cli, &ops)?;

        assert_eq!(
            ops.calls.into_inner(),
            vec!["compute_patrol:frwiki:d:o:false:_"]
        );
        Ok(())
    }

    #[test]
    fn run_with_ops_dispatches_patrol_compute_with_rebuild_flag() -> Result<()> {
        init_test_tracing();
        let data_dir = TestDir::new()?;
        let output_dir = TestDir::new()?;
        // Path content is hashed for the recording assertion only; actual
        // parquet output is asserted in the RealOps integration further down.
        let data_path = data_dir.path().to_str().expect("utf-8 path").to_string();
        let output_path = output_dir.path().to_str().expect("utf-8 path").to_string();
        let cli = Cli::try_parse_from([
            "wiki-econ",
            "--data-dir",
            &data_path,
            "--output-dir",
            &output_path,
            "patrol-compute",
            "testwiki",
            "--rebuild",
            "--limit-months",
            "3",
        ])?;
        let ops = RecordingOps::default();

        run_with_ops(cli, &ops)?;

        assert_eq!(
            ops.calls.into_inner(),
            vec![format!(
                "compute_patrol:testwiki:{data_path}:{output_path}:true:3"
            )]
        );
        Ok(())
    }

    #[test]
    fn run_with_ops_dispatches_bench() -> Result<()> {
        init_test_tracing();
        let cli = Cli::try_parse_from([
            "wiki-econ",
            "--data-dir",
            "dataset",
            "--output-dir",
            "bench-out",
            "bench",
            "frwiki",
            "dewiki",
            "--warmup",
            "2",
            "--iterations",
            "4",
            "--keep-outputs",
        ])?;
        let ops = RecordingOps::default();

        run_with_ops(cli, &ops)?;

        assert_eq!(
            ops.calls.into_inner(),
            vec!["bench:frwiki,dewiki:dataset:bench-out:2:4:true"]
        );
        Ok(())
    }

    #[test]
    fn run_with_ops_dispatches_full_pipeline() -> Result<()> {
        init_test_tracing();
        let cli = Cli::try_parse_from([
            "wiki-econ",
            "--data-dir",
            "dataset",
            "--output-dir",
            "results",
            "run",
            "frwiki",
            "--version",
            "2025-12",
        ])?;
        let ops = RecordingOps::default();

        run_with_ops(cli, &ops)?;

        assert_eq!(
            ops.calls.into_inner(),
            vec![
                "fetch:frwiki:2025-12:dataset",
                "fetch_patrol:frwiki:dataset",
                "ingest:frwiki:dataset",
                "compute:frwiki:dataset:results",
                "compute_patrol:frwiki:dataset:results:false:_",
                "merge:results",
            ]
        );
        Ok(())
    }

    #[test]
    fn real_ops_execute_local_paths() -> Result<()> {
        init_test_tracing();
        let ops = RealOps;
        let data_dir = TestDir::new()?;
        let output_dir = TestDir::new()?;

        let raw_ingest_dir = data_dir.path().join("raw").join("ingestwiki");
        fs::create_dir_all(&raw_ingest_dir)?;
        write_bz2_dump(&raw_ingest_dir.join("2026-02.ingestwiki.all-time.tsv.bz2"))?;
        ops.ingest_wiki("ingestwiki", data_dir.path())?;
        assert!(
            !crate::storage::collect_parquet_files(&crate::storage::analytical_wiki_dir(
                data_dir.path(),
                "ingestwiki"
            ))?
            .is_empty()
        );

        let fetch_err = ops
            .fetch_wiki("enwiki", "2026-02", data_dir.path())
            .expect_err("monthly fetch should fail before network work");
        assert!(fetch_err.to_string().contains("not yet supported"));

        write_compute_input(data_dir.path(), "computewiki")?;
        ops.compute_all("computewiki", data_dir.path(), output_dir.path())?;
        assert!(
            output_dir
                .path()
                .join("computewiki")
                .join("gdp.parquet")
                .exists()
        );

        let bench_cli = Cli::try_parse_from([
            "wiki-econ",
            "--data-dir",
            data_dir.path().to_str().expect("utf-8 path"),
            "--output-dir",
            output_dir.path().to_str().expect("utf-8 path"),
            "bench",
            "computewiki",
            "--warmup",
            "0",
            "--iterations",
            "1",
        ])?;
        run_with_ops(bench_cli, &ops)?;

        let patrol_xml = r#"<mediawiki xmlns="http://www.mediawiki.org/xml/export-0.11/">
  <logitem>
    <id>1</id>
    <timestamp>2026-01-05T12:00:00Z</timestamp>
    <contributor><username>Patroller</username><id>10</id></contributor>
    <type>patrol</type>
    <logtitle>Page</logtitle>
    <params>101
100
0</params>
  </logitem>
</mediawiki>"#;
        let fake_transport = Arc::new(FakePatrolTransport::new(
            vec![gzip_bytes(patrol_xml)?],
            vec![json!({
                "query": {
                    "usergroups": [
                        { "name": "autopatrolled", "rights": ["autopatrol"] }
                    ]
                }
            })],
        ));
        let _guard = crate::patrol::install_test_transport(fake_transport);
        ops.fetch_patrol("patrolwiki", data_dir.path())?;
        assert!(
            data_dir
                .path()
                .join("patrol")
                .join("patrolwiki")
                .join("patrol.parquet")
                .exists()
        );

        write_patrol_compute_input(data_dir.path(), "patrolcomputewiki")?;
        let result =
            ops.compute_patrol("patrolcomputewiki", data_dir.path(), output_dir.path(), false, None);
        result?;
        assert!(
            output_dir
                .path()
                .join("patrolcomputewiki")
                .join("patrol.parquet")
                .exists()
        );

        // Re-run with rebuild=true after pre-creating the parts dir; this
        // exercises the branch in patrol::compute_patrol that wipes
        // _patrol_parts before recomputing month parts. Ensures the rebuild
        // arg threads through `Ops::compute_patrol` to the real pipeline.
        let parts_dir = output_dir
            .path()
            .join("patrolcomputewiki")
            .join("_patrol_parts");
        fs::create_dir_all(&parts_dir)?;
        fs::write(parts_dir.join("stale.parquet"), b"stale-bytes")?;
        let rebuild_result =
            ops.compute_patrol("patrolcomputewiki", data_dir.path(), output_dir.path(), true, None);
        rebuild_result?;
        let stale_gone = !parts_dir.join("stale.parquet").exists();
        assert!(stale_gone);
        Ok(())
    }

    #[test]
    fn real_ops_merge_outputs_delegates_to_merge_pipeline() -> Result<()> {
        init_test_tracing();
        let ops = RealOps;
        let output_dir = TestDir::new()?;
        let wiki_dir = output_dir.path().join("testwiki");
        fs::create_dir_all(&wiki_dir)?;
        let path = wiki_dir.join("metric.parquet");
        let mut file = fs::File::create(path)?;
        let columns = vec![
            Column::new("wiki".into(), vec!["testwiki"]),
            Column::new("value".into(), vec![1_i64]),
        ];
        let mut df = DataFrame::new_infer_height(columns)?;
        ParquetWriter::new(&mut file).finish(&mut df)?;

        ops.merge_outputs(output_dir.path())?;

        assert!(output_dir.path().join("metric.parquet").exists());
        Ok(())
    }

    #[test]
    fn tracing_helpers_initialize_and_time_stages() -> Result<()> {
        init_test_tracing();
        init_tracing();

        let value = run_timed_stage("unit", None, || Ok::<_, anyhow::Error>(7_u8))?;

        assert_eq!(value, 7);
        Ok(())
    }

    #[test]
    fn run_timed_stage_propagates_errors() {
        init_test_tracing();
        let err = run_timed_stage("unit", None, || -> Result<()> { anyhow::bail!("boom") })
            .expect_err("timed stage should propagate errors");
        assert!(err.to_string().contains("boom"));
    }

    #[test]
    fn snapshot_version_for_uses_previous_month() {
        let may = chrono::TimeZone::with_ymd_and_hms(&chrono::Utc, 2026, 5, 2, 12, 0, 0)
            .single()
            .expect("valid timestamp");
        assert_eq!(snapshot_version_for(may), "2026-04");

        let january = chrono::TimeZone::with_ymd_and_hms(&chrono::Utc, 2026, 1, 3, 8, 0, 0)
            .single()
            .expect("valid timestamp");
        assert_eq!(snapshot_version_for(january), "2025-12");
    }

    #[test]
    fn run_with_ops_propagates_fetch_errors() -> Result<()> {
        init_test_tracing();
        let cli = Cli::try_parse_from(["wiki-econ", "fetch", "frwiki"])?;
        let err = run_with_ops(
            cli,
            &FailingOps {
                fail_stage: "fetch",
            },
        )
        .expect_err("fetch failure should propagate");
        assert!(err.to_string().contains("fetch failed"));
        Ok(())
    }

    #[test]
    fn run_with_ops_propagates_patrol_fetch_errors() -> Result<()> {
        init_test_tracing();
        let cli = Cli::try_parse_from(["wiki-econ", "fetch", "frwiki"])?;
        let err = run_with_ops(
            cli,
            &FailingOps {
                fail_stage: "fetch_patrol",
            },
        )
        .expect_err("patrol fetch failure should propagate");
        assert!(err.to_string().contains("fetch patrol failed"));
        Ok(())
    }

    #[test]
    fn run_with_ops_propagates_ingest_errors() -> Result<()> {
        init_test_tracing();
        let cli = Cli::try_parse_from(["wiki-econ", "ingest", "frwiki"])?;
        let err = run_with_ops(
            cli,
            &FailingOps {
                fail_stage: "ingest",
            },
        )
        .expect_err("ingest failure should propagate");
        assert!(err.to_string().contains("ingest failed"));
        Ok(())
    }

    #[test]
    fn run_with_ops_propagates_compute_errors() -> Result<()> {
        init_test_tracing();
        let cli = Cli::try_parse_from(["wiki-econ", "compute", "frwiki"])?;
        let err = run_with_ops(
            cli,
            &FailingOps {
                fail_stage: "compute",
            },
        )
        .expect_err("compute failure should propagate");
        assert!(err.to_string().contains("compute failed"));
        Ok(())
    }

    #[test]
    fn run_with_ops_propagates_patrol_compute_errors() -> Result<()> {
        init_test_tracing();
        let cli = Cli::try_parse_from(["wiki-econ", "compute", "frwiki"])?;
        let err = run_with_ops(
            cli,
            &FailingOps {
                fail_stage: "compute_patrol",
            },
        )
        .expect_err("patrol compute failure should propagate");
        assert!(err.to_string().contains("compute patrol failed"));
        Ok(())
    }

    #[test]
    fn run_with_ops_compute_propagates_merge_errors() -> Result<()> {
        init_test_tracing();
        let cli = Cli::try_parse_from(["wiki-econ", "compute", "frwiki"])?;
        let err = run_with_ops(
            cli,
            &FailingOps {
                fail_stage: "merge",
            },
        )
        .expect_err("compute merge failure should propagate");
        assert!(err.to_string().contains("merge failed"));
        Ok(())
    }

    #[test]
    fn run_with_ops_propagates_merge_errors() -> Result<()> {
        init_test_tracing();
        let cli = Cli::try_parse_from(["wiki-econ", "merge"])?;
        let err = run_with_ops(
            cli,
            &FailingOps {
                fail_stage: "merge",
            },
        )
        .expect_err("merge failure should propagate");
        assert!(err.to_string().contains("merge failed"));
        Ok(())
    }

    #[test]
    fn run_with_ops_propagates_bench_errors() -> Result<()> {
        init_test_tracing();
        let cli = Cli::try_parse_from(["wiki-econ", "bench", "frwiki", "--warmup", "0"])?;
        let err = run_with_ops(
            cli,
            &FailingOps {
                fail_stage: "bench",
            },
        )
        .expect_err("bench failure should propagate");
        assert!(err.to_string().contains("bench failed"));
        Ok(())
    }

    #[test]
    fn failing_ops_succeeds_for_non_matching_stages() -> Result<()> {
        let ops = FailingOps { fail_stage: "none" };
        let data_dir = Path::new("data");
        let output_dir = Path::new("output");
        let wikis = vec!["frwiki".to_string()];

        ops.fetch_wiki("frwiki", "2026-02", data_dir)?;
        ops.fetch_patrol("frwiki", data_dir)?;
        ops.ingest_wiki("frwiki", data_dir)?;
        ops.compute_all("frwiki", data_dir, output_dir)?;
        ops.compute_patrol("frwiki", data_dir, output_dir, false, None)?;
        ops.benchmark(&wikis, data_dir, output_dir, 0, 1, false)?;
        ops.merge_outputs(output_dir)?;
        Ok(())
    }

    #[test]
    fn run_command_propagates_fetch_errors() -> Result<()> {
        init_test_tracing();
        let cli = Cli::try_parse_from(["wiki-econ", "run", "frwiki"])?;
        let err = run_with_ops(
            cli,
            &FailingOps {
                fail_stage: "fetch",
            },
        )
        .expect_err("run fetch failure should propagate");
        assert!(err.to_string().contains("fetch failed"));
        Ok(())
    }

    #[test]
    fn run_command_propagates_patrol_fetch_errors() -> Result<()> {
        init_test_tracing();
        let cli = Cli::try_parse_from(["wiki-econ", "run", "frwiki"])?;
        let err = run_with_ops(
            cli,
            &FailingOps {
                fail_stage: "fetch_patrol",
            },
        )
        .expect_err("run patrol fetch failure should propagate");
        assert!(err.to_string().contains("fetch patrol failed"));
        Ok(())
    }

    #[test]
    fn run_command_propagates_ingest_errors() -> Result<()> {
        init_test_tracing();
        let cli = Cli::try_parse_from(["wiki-econ", "run", "frwiki"])?;
        let err = run_with_ops(
            cli,
            &FailingOps {
                fail_stage: "ingest",
            },
        )
        .expect_err("run ingest failure should propagate");
        assert!(err.to_string().contains("ingest failed"));
        Ok(())
    }

    #[test]
    fn run_command_propagates_compute_errors() -> Result<()> {
        init_test_tracing();
        let cli = Cli::try_parse_from(["wiki-econ", "run", "frwiki"])?;
        let err = run_with_ops(
            cli,
            &FailingOps {
                fail_stage: "compute",
            },
        )
        .expect_err("run compute failure should propagate");
        assert!(err.to_string().contains("compute failed"));
        Ok(())
    }

    #[test]
    fn run_command_propagates_patrol_compute_errors() -> Result<()> {
        init_test_tracing();
        let cli = Cli::try_parse_from(["wiki-econ", "run", "frwiki"])?;
        let err = run_with_ops(
            cli,
            &FailingOps {
                fail_stage: "compute_patrol",
            },
        )
        .expect_err("run patrol compute failure should propagate");
        assert!(err.to_string().contains("compute patrol failed"));
        Ok(())
    }

    #[test]
    fn run_command_propagates_merge_errors() -> Result<()> {
        init_test_tracing();
        let cli = Cli::try_parse_from(["wiki-econ", "run", "frwiki"])?;
        let err = run_with_ops(
            cli,
            &FailingOps {
                fail_stage: "merge",
            },
        )
        .expect_err("run merge failure should propagate");
        assert!(err.to_string().contains("merge failed"));
        Ok(())
    }
}
