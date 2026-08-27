#![forbid(unsafe_code)]

mod artifact_receipt;
mod bench;
mod browser_data;
mod canonical_month;
mod capacity;
mod cleanup;
mod compaction;
mod compute;
mod cpu_qualification;
mod cross_snapshot;
mod dashboard;
mod determinism;
#[cfg(test)]
mod end_to_end_tests;
mod fetch;
mod fingerprint;
mod fleet;
mod generation_lifecycle;
mod ingest;
mod licensing;
mod merge;
mod observability;
mod patrol;
mod publication;
mod resource_governor;
mod schema;
mod schema_benchmark;
mod snapshot_plan;
mod source_window;
mod storage;
#[cfg(test)]
mod test_support;
mod wiki_lifecycle;
mod workload_profile;

use anyhow::{Context, Result, ensure};
use chrono::{DateTime, Datelike, Utc};
use clap::{Parser, Subcommand, ValueEnum};
use std::env;
use std::fmt;
use std::path::{Path, PathBuf};
use std::time::Duration;
use std::time::Instant;
use tracing::{Event, Subscriber, info};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::FmtContext;
use tracing_subscriber::fmt::format::{
    Compact, DefaultFields, Format, FormatEvent, FormatFields, Writer,
};
use tracing_subscriber::registry::LookupSpan;

#[derive(Parser)]
#[command(name = "wiki-econ", about = "Wikipedia economic analysis toolkit")]
struct Cli {
    /// Base data directory
    #[arg(long, default_value = "data")]
    data_dir: PathBuf,

    /// Output directory for computed metrics
    #[arg(long, default_value = "output")]
    output_dir: PathBuf,

    /// Unique publication run identifier used by the fail-closed gate
    #[arg(long)]
    run_id: Option<String>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Regenerate dashboard JSON from already-merged Parquet inputs
    #[command(hide = true)]
    DashboardMaterialize,

    /// Write the deterministic minimal data fixture used by real site CI
    #[command(hide = true)]
    SiteFixture,

    /// Write deterministic nlwiki/ptwiki/frwiki browser scalability fixtures
    #[command(hide = true)]
    BrowserPerformanceFixture,

    /// Compare exact artifact bytes produced with two concurrency settings
    DeterminismVerify {
        /// Artifact root produced by the lower-concurrency qualification build
        #[arg(long)]
        baseline_root: PathBuf,

        /// Artifact root produced by the second qualification build
        #[arg(long)]
        candidate_root: PathBuf,

        /// Artifact extension included in the exact comparison
        #[arg(long, default_value = "parquet")]
        artifact_extension: String,

        /// Worker count used for the baseline build
        #[arg(long)]
        baseline_workers: usize,

        /// Worker count used for the candidate build
        #[arg(long)]
        candidate_workers: usize,

        /// Exact computation and partition-topology version under test
        #[arg(long)]
        algorithm_version: String,

        /// Atomic deterministic qualification report path
        #[arg(long)]
        report: PathBuf,
    },

    /// Compare a content-reusing cross-snapshot build with a clean rebuild
    CrossSnapshotQualify {
        /// Wiki database name with two immutable schema-v3 generations
        wiki: String,

        /// Older generation used to seed content-addressed metric partitions
        #[arg(long)]
        baseline_version: String,

        /// Newer generation built both incrementally and from empty
        #[arg(long)]
        candidate_version: String,

        /// New, publication-invisible workspace retained as qualification evidence
        #[arg(long)]
        work_dir: PathBuf,

        /// Atomic qualification report path
        #[arg(long)]
        report: PathBuf,
    },

    /// Remove only expired, pipeline-owned staging artifacts
    #[command(hide = true)]
    CleanupStale {
        /// Published site distribution symlink
        #[arg(long, default_value = "site/dist")]
        site_dist_dir: PathBuf,

        /// Minimum artifact age before removal
        #[arg(long, default_value_t = 21_600)]
        minimum_age_secs: u64,

        /// Optional root containing pipeline-owned weekly aggregation scratch
        #[arg(long)]
        scratch_dir: Option<PathBuf>,

        /// Optional root containing isolated capacity benchmark staging
        #[arg(long)]
        capacity_dir: Option<PathBuf>,

        /// Wiki database names whose abandoned snapshot generations may be retired
        wikis: Vec<String>,
    },

    /// Resolve the latest complete dump snapshot shared by every wiki
    SnapshotResolve {
        /// Wiki database names
        wikis: Vec<String>,
    },

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

        /// Dump snapshot version (YYYY-MM); inferred from filenames when omitted
        #[arg(long)]
        version: Option<String>,
    },

    /// Compute economic metrics from Parquet data
    Compute {
        /// Wiki database names
        wikis: Vec<String>,
    },

    /// Prepare one immutable wiki candidate without changing the published site
    PrepareWiki {
        /// Wiki database name
        wiki: String,

        /// Dump snapshot version (YYYY-MM)
        #[arg(long)]
        version: Option<String>,

        /// Maximum number of compressed sources retained before ingest
        #[arg(long)]
        source_window_size: Option<usize>,

        /// Wiki lifecycle and publication contract
        #[arg(long, default_value = "config/wiki-lifecycle.json")]
        lifecycle: PathBuf,
    },

    /// Discover scheduled wikis and atomically enqueue independent preparation work
    FleetDiscover {
        /// Wiki lifecycle registry that defines the scheduled fleet
        #[arg(long, default_value = "config/wiki-lifecycle.json")]
        lifecycle: PathBuf,

        /// NFS-backed fleet control-plane root
        #[arg(long)]
        queue_dir: PathBuf,

        /// Deterministic snapshot override used by qualification and fixtures
        #[arg(long)]
        snapshot: Option<String>,
    },

    /// Claim one fleet task for a fixed resource-class worker
    FleetClaim {
        #[arg(long)]
        queue_dir: PathBuf,

        #[arg(long, value_enum)]
        resource_class: fleet::ResourceClass,

        #[arg(long)]
        worker_id: String,

        #[arg(long, default_value_t = 900)]
        lease_timeout_secs: u64,

        /// Atomic claim receipt consumed by heartbeats and completion
        #[arg(long)]
        receipt: PathBuf,
    },

    /// Refresh the heartbeat for a fleet task still owned by this worker
    FleetHeartbeat {
        #[arg(long)]
        queue_dir: PathBuf,

        #[arg(long)]
        receipt: PathBuf,
    },

    /// Mark a claimed wiki ready after authenticating its ready-candidate index
    FleetComplete {
        #[arg(long)]
        queue_dir: PathBuf,

        #[arg(long)]
        receipt: PathBuf,
    },

    /// Retry or quarantine one failed fleet task
    FleetFail {
        #[arg(long)]
        queue_dir: PathBuf,

        #[arg(long)]
        receipt: PathBuf,

        #[arg(long)]
        error: String,

        #[arg(long, default_value_t = 3)]
        max_attempts: u32,
    },

    /// Recover expired fleet leases without claiming new work
    FleetRecover {
        #[arg(long)]
        queue_dir: PathBuf,

        #[arg(long, default_value_t = 3)]
        max_attempts: u32,
    },

    /// Prepare and validate an isolated, permanently publication-ineligible wiki
    QualifyWiki {
        /// Hidden wiki registered with refresh=qualification
        wiki: String,

        /// Dump snapshot version (YYYY-MM)
        #[arg(long)]
        version: Option<String>,

        /// Maximum number of compressed sources retained before ingest
        #[arg(long)]
        source_window_size: Option<usize>,

        /// Wiki lifecycle containing the hidden qualification entry
        #[arg(long, default_value = "config/wiki-lifecycle.json")]
        lifecycle: PathBuf,
    },

    /// Select ready wiki candidates, merge, and issue a publication receipt
    PublicationPrepareReady {
        /// Wiki lifecycle and publication contract
        #[arg(long, default_value = "config/wiki-lifecycle.json")]
        lifecycle: PathBuf,
    },

    /// Commit a successfully built ready-candidate publication
    PublicationCommitReady,

    /// Roll back a selected candidate set after publication failure
    PublicationRollbackReady {
        /// Wiki lifecycle and publication contract
        #[arg(long, default_value = "config/wiki-lifecycle.json")]
        lifecycle: PathBuf,
    },

    /// Audit interrupted publication transactions without changing them
    PublicationRecoveryAudit {
        /// Published site distribution whose receipt must match the data gate
        #[arg(long, default_value = "site/dist")]
        site_dist_dir: PathBuf,

        /// Restrict the audit to one transaction
        #[arg(long = "run-id")]
        transaction_run_id: Option<String>,
    },

    /// Repair one or all interrupted publication transactions
    PublicationRecover {
        /// Wiki lifecycle and publication contract
        #[arg(long, default_value = "config/wiki-lifecycle.json")]
        lifecycle: PathBuf,

        /// Published site distribution whose receipt must match the data gate
        #[arg(long, default_value = "site/dist")]
        site_dist_dir: PathBuf,

        /// Interrupted transaction to repair
        #[arg(long = "run-id", conflicts_with = "all")]
        transaction_run_id: Option<String>,

        /// Repair every non-terminal transaction; used at publisher startup
        #[arg(long, conflicts_with = "transaction_run_id")]
        all: bool,

        /// Optional atomic JSON report for orchestration
        #[arg(long = "report")]
        report_path: Option<PathBuf>,
    },

    /// Merge per-wiki outputs into combined parquet files
    Merge,

    /// Validate a merged artifact set and issue its publication receipt
    PublicationValidate,

    /// Verify that the publication receipt still matches every artifact
    PublicationVerify,

    /// Independently rehash every published Parquet and verify its receipt
    ArtifactScrub {
        /// Optional atomic JSON report retained by operations
        #[arg(long)]
        report: Option<PathBuf>,
    },

    /// Retire non-current snapshot generations after successful publication
    SnapshotFinalize {
        /// Wiki database names
        wikis: Vec<String>,
    },

    /// Repair a missing/corrupt generation pointer after strict marker validation
    SnapshotRepair {
        /// Wiki database name
        wiki: String,

        /// Existing immutable generation to validate and select
        #[arg(long)]
        version: String,
    },

    /// Exit successfully only when the published site stage is reusable
    SiteFingerprintCheck {
        /// Observable project directory
        #[arg(long, default_value = "site")]
        site_dir: PathBuf,

        /// Published site distribution directory
        #[arg(long, default_value = "site/dist")]
        dist_dir: PathBuf,
    },

    /// Record the successfully published site stage
    SiteFingerprintRecord {
        /// Observable project directory
        #[arg(long, default_value = "site")]
        site_dir: PathBuf,

        /// Published site distribution directory
        #[arg(long, default_value = "site/dist")]
        dist_dir: PathBuf,
    },

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

    /// Benchmark full-history weekly aggregation under an explicit capacity gate
    CapacityBench {
        /// Wiki database name; lifecycle state is not changed by this command
        wiki: String,

        /// Stable disk-bucket count to benchmark
        #[arg(long, value_parser = clap::value_parser!(usize))]
        weekly_buckets: usize,

        /// Stable second-level buckets within each primary bucket
        #[arg(long, default_value_t = 1, value_parser = clap::value_parser!(usize))]
        weekly_secondary_buckets: usize,

        /// Dedicated scratch root for disk-backed aggregation runs
        #[arg(long)]
        scratch_dir: PathBuf,

        /// Atomic JSON report path
        #[arg(long)]
        report: Option<PathBuf>,

        /// Estimated raw transient requirement during a snapshot rollover
        #[arg(long, default_value_t = 33_285_996_544_u64)]
        raw_transient_bytes: u64,

        /// Optional tool-specific NFS quota when the platform enforces one
        #[arg(long)]
        nfs_quota_bytes: Option<u64>,

        /// Free capacity retained after the estimated rollover requirement
        #[arg(long, default_value_t = 53_687_091_200_u64)]
        storage_reserve_bytes: u64,

        /// Root whose current usage is charged against the confirmed quota
        #[arg(long)]
        quota_root: Option<PathBuf>,

        /// Required cgroup memory headroom at the observed peak
        #[arg(long, default_value_t = 25_u8)]
        minimum_memory_headroom_percent: u8,

        /// CPU quota requested for this independently scheduled matrix cell
        #[arg(long, default_value_t = 1)]
        requested_cpu: usize,
    },

    /// Validate the complete CPU/resource qualification matrix from capacity receipts
    CpuQualify {
        /// Capacity benchmark receipt paths, one per required matrix cell
        #[arg(long = "capacity-report", required = true)]
        capacity_reports: Vec<PathBuf>,

        /// Atomic qualification report path
        #[arg(long)]
        report: PathBuf,
    },

    /// Qualify the single metric-input schema against active warehouse data
    SchemaBenchmark {
        /// Wiki database names to qualify
        wikis: Vec<String>,

        /// Scratch root; only one projected fragment is retained at a time
        #[arg(long)]
        scratch_dir: PathBuf,

        /// Atomic JSON qualification report path
        #[arg(long)]
        report: PathBuf,
    },

    /// Run the full pipeline: fetch → ingest → compute → merge
    Run {
        /// Wiki database names
        wikis: Vec<String>,

        /// Dump snapshot version (YYYY-MM)
        #[arg(long)]
        version: Option<String>,

        /// Maximum number of compressed sources retained before ingest
        #[arg(long)]
        source_window_size: Option<usize>,

        /// Which portion of the pipeline to execute
        #[arg(long, value_enum, default_value_t = RunStage::All)]
        stage: RunStage,
    },
}

/// Portion of the fetch → ingest → compute → merge pipeline a `Run` invocation executes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum RunStage {
    /// Ingest and compute, back to back (today's behavior).
    All,
    /// Fetch, ingest, and patrol-fetch only.
    Ingest,
    /// Compute, patrol-compute, and the trailing merge only.
    Compute,
}

impl RunStage {
    fn runs_ingest(self) -> bool {
        matches!(self, RunStage::All | RunStage::Ingest)
    }

    fn runs_compute(self) -> bool {
        matches!(self, RunStage::All | RunStage::Compute)
    }
}

trait Ops {
    fn resolve_snapshot(
        &self,
        _wikis: &[String],
        now: DateTime<Utc>,
        _data_dir: &Path,
    ) -> Result<String> {
        Ok(snapshot_version_for(now))
    }
    fn persist_snapshot_plans(
        &self,
        _wikis: &[String],
        _version: &str,
        _data_dir: &Path,
    ) -> Result<()> {
        Ok(())
    }
    fn fetch_wiki(&self, wiki: &str, version: &str, data_dir: &std::path::Path) -> Result<()>;
    fn fetch_patrol(&self, wiki: &str, data_dir: &std::path::Path) -> Result<()>;
    fn fetch_patrol_for_snapshot(
        &self,
        wiki: &str,
        _version: &str,
        data_dir: &std::path::Path,
    ) -> Result<()> {
        self.fetch_patrol(wiki, data_dir)
    }
    fn ingest_wiki(
        &self,
        wiki: &str,
        version: Option<&str>,
        data_dir: &std::path::Path,
    ) -> Result<()>;
    fn prepare_wiki_snapshot(
        &self,
        wiki: &str,
        version: &str,
        data_dir: &Path,
        _run_id: &str,
        _window_size: usize,
    ) -> Result<()> {
        self.fetch_wiki(wiki, version, data_dir)?;
        self.ingest_wiki(wiki, Some(version), data_dir)?;
        self.cleanup_raw_dump(wiki, data_dir)
    }
    fn cleanup_raw_dump(&self, wiki: &str, data_dir: &std::path::Path) -> Result<()>;
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
    #[allow(clippy::too_many_arguments)]
    fn capacity_benchmark(
        &self,
        wiki: &str,
        data_dir: &std::path::Path,
        output_dir: &std::path::Path,
        scratch_dir: &std::path::Path,
        report_path: &std::path::Path,
        weekly_buckets: usize,
        weekly_secondary_buckets: usize,
        raw_transient_bytes: u64,
        nfs_quota_bytes: Option<u64>,
        storage_reserve_bytes: u64,
        quota_root: &std::path::Path,
        minimum_memory_headroom_percent: u8,
        requested_cpu: usize,
    ) -> Result<()>;
    fn cpu_qualification(&self, capacity_reports: &[PathBuf], report: &Path) -> Result<()> {
        cpu_qualification::run(capacity_reports, report).map(drop)
    }
    fn schema_benchmark(
        &self,
        data_dir: &Path,
        scratch_dir: &Path,
        report_path: &Path,
        wikis: &[String],
        run_id: Option<&str>,
    ) -> Result<()>;
    fn merge_outputs(&self, output_dir: &std::path::Path, run_id: Option<&str>) -> Result<()>;
    fn finalize_snapshot(&self, wiki: &str, data_dir: &std::path::Path) -> Result<()>;
    fn prepare_candidate_snapshot(
        &self,
        wiki: &str,
        version: &str,
        data_dir: &Path,
        run_id: &str,
        window_size: usize,
    ) -> Result<()> {
        self.prepare_wiki_snapshot(wiki, version, data_dir, run_id, window_size)
    }
    fn plan_candidate_preparation(
        &self,
        _wiki: &str,
        _version: &str,
        _data_dir: &Path,
        _output_dir: &Path,
        _run_id: &str,
    ) -> Result<publication::WikiPreparationPlan> {
        Ok(publication::WikiPreparationPlan::Build {
            same_snapshot_candidate: false,
            compute_reused: false,
            patrol_reused: false,
        })
    }
    fn cached_patrol_sources_available(&self, _wiki: &str, _data_dir: &Path) -> bool {
        false
    }
    fn cached_patrol_generation_available(
        &self,
        wiki: &str,
        _version: &str,
        data_dir: &Path,
    ) -> bool {
        self.cached_patrol_sources_available(wiki, data_dir)
    }
    fn compute_candidate(
        &self,
        wiki: &str,
        _version: &str,
        data_dir: &Path,
        candidate_dir: &Path,
    ) -> Result<()> {
        self.compute_all(wiki, data_dir, candidate_dir)?;
        Ok(())
    }
    fn compute_candidate_patrol(
        &self,
        wiki: &str,
        _version: &str,
        data_dir: &Path,
        candidate_dir: &Path,
    ) -> Result<()> {
        self.compute_patrol(wiki, data_dir, candidate_dir, false, None)?;
        Ok(())
    }
    fn mark_candidate_ready(
        &self,
        data_dir: &Path,
        output_dir: &Path,
        lifecycle: &Path,
        wiki: &str,
        version: &str,
        run_id: &str,
    ) -> Result<PathBuf> {
        publication::mark_wiki_candidate_ready(
            data_dir, output_dir, lifecycle, wiki, version, run_id,
        )
    }
    fn ensure_qualification_wiki(&self, lifecycle: &Path, wiki: &str) -> Result<()> {
        publication::ensure_qualification_wiki(lifecycle, wiki)
    }
    fn mark_qualification_ready(
        &self,
        data_dir: &Path,
        output_dir: &Path,
        lifecycle: &Path,
        wiki: &str,
        version: &str,
        run_id: &str,
    ) -> Result<PathBuf> {
        publication::mark_wiki_qualification_ready(
            data_dir, output_dir, lifecycle, wiki, version, run_id,
        )
    }
    fn prepare_ready_publication(
        &self,
        data_dir: &Path,
        output_dir: &Path,
        lifecycle: &Path,
        run_id: &str,
    ) -> Result<()> {
        publication::prepare_ready_publication(data_dir, output_dir, lifecycle, run_id)
    }
    fn commit_ready_publication(
        &self,
        data_dir: &Path,
        output_dir: &Path,
        run_id: &str,
    ) -> Result<()> {
        publication::commit_ready_publication(data_dir, output_dir, run_id)
    }
    fn rollback_ready_publication(
        &self,
        data_dir: &Path,
        output_dir: &Path,
        lifecycle: &Path,
        run_id: &str,
    ) -> Result<()> {
        publication::rollback_ready_publication(data_dir, output_dir, lifecycle, run_id)
    }
}

struct RealOps;

impl Ops for RealOps {
    fn resolve_snapshot(
        &self,
        wikis: &[String],
        now: DateTime<Utc>,
        data_dir: &Path,
    ) -> Result<String> {
        fetch::resolve_latest_completed_snapshot(data_dir, wikis, now)
    }

    fn persist_snapshot_plans(
        &self,
        wikis: &[String],
        version: &str,
        data_dir: &Path,
    ) -> Result<()> {
        for wiki in wikis {
            snapshot_plan::SnapshotPlan::load_or_resolve(data_dir, wiki, version)?;
        }
        Ok(())
    }

    fn fetch_wiki(&self, wiki: &str, version: &str, data_dir: &std::path::Path) -> Result<()> {
        fetch::fetch_wiki(wiki, version, data_dir).map(|_| ())
    }

    fn fetch_patrol(&self, wiki: &str, data_dir: &std::path::Path) -> Result<()> {
        patrol::fetch_patrol(wiki, data_dir)
    }

    fn fetch_patrol_for_snapshot(
        &self,
        wiki: &str,
        version: &str,
        data_dir: &std::path::Path,
    ) -> Result<()> {
        patrol::fetch_patrol_for_snapshot(wiki, version, data_dir)
    }

    fn ingest_wiki(
        &self,
        wiki: &str,
        version: Option<&str>,
        data_dir: &std::path::Path,
    ) -> Result<()> {
        match version {
            Some(version) => ingest::ingest_wiki_snapshot(wiki, version, data_dir).map(|_| ()),
            None => ingest::ingest_wiki(wiki, data_dir).map(|_| ()),
        }
    }

    fn cleanup_raw_dump(&self, wiki: &str, data_dir: &std::path::Path) -> Result<()> {
        fetch::cleanup_raw_dump(wiki, data_dir)
    }

    fn prepare_wiki_snapshot(
        &self,
        wiki: &str,
        version: &str,
        data_dir: &Path,
        run_id: &str,
        window_size: usize,
    ) -> Result<()> {
        source_window::prepare_snapshot(wiki, version, data_dir, run_id, window_size).map(|_| ())
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

    fn capacity_benchmark(
        &self,
        wiki: &str,
        data_dir: &std::path::Path,
        output_dir: &std::path::Path,
        scratch_dir: &std::path::Path,
        report_path: &std::path::Path,
        weekly_buckets: usize,
        weekly_secondary_buckets: usize,
        raw_transient_bytes: u64,
        nfs_quota_bytes: Option<u64>,
        storage_reserve_bytes: u64,
        quota_root: &std::path::Path,
        minimum_memory_headroom_percent: u8,
        requested_cpu: usize,
    ) -> Result<()> {
        execute_capacity_benchmark(capacity::CapacityBenchmarkOptions {
            wiki,
            data_dir,
            output_dir,
            scratch_root: scratch_dir,
            quota_root,
            report_path,
            bucket_count: weekly_buckets,
            secondary_bucket_count: weekly_secondary_buckets,
            raw_transient_requirement_bytes: raw_transient_bytes,
            nfs_quota_bytes,
            storage_reserve_bytes,
            minimum_memory_headroom_percent,
            requested_cpu,
            telemetry_override: None,
        })
    }

    fn schema_benchmark(
        &self,
        data_dir: &Path,
        scratch_dir: &Path,
        report_path: &Path,
        wikis: &[String],
        run_id: Option<&str>,
    ) -> Result<()> {
        let benchmark = schema_benchmark::run(
            data_dir,
            scratch_dir,
            report_path,
            wikis,
            std::env::var("WIKI_ECON_SOURCE_COMMIT").ok().as_deref(),
            run_id,
        );
        let result = benchmark?;
        println!("{}", serde_json::to_string(&result)?);
        Ok(())
    }

    fn merge_outputs(&self, output_dir: &std::path::Path, run_id: Option<&str>) -> Result<()> {
        merge::merge_outputs(output_dir, run_id)
    }

    fn finalize_snapshot(&self, wiki: &str, data_dir: &std::path::Path) -> Result<()> {
        let removed = storage::retire_inactive_snapshots(data_dir, wiki)?;
        info!(
            wiki = wiki,
            removed, "retired inactive snapshot generations"
        );
        Ok(())
    }

    fn prepare_candidate_snapshot(
        &self,
        wiki: &str,
        version: &str,
        data_dir: &Path,
        run_id: &str,
        window_size: usize,
    ) -> Result<()> {
        source_window::prepare_candidate_snapshot(wiki, version, data_dir, run_id, window_size)
            .map(|_| ())
    }

    fn plan_candidate_preparation(
        &self,
        wiki: &str,
        version: &str,
        data_dir: &Path,
        output_dir: &Path,
        run_id: &str,
    ) -> Result<publication::WikiPreparationPlan> {
        publication::plan_wiki_preparation(data_dir, output_dir, wiki, version, run_id)
    }

    fn cached_patrol_sources_available(&self, wiki: &str, data_dir: &Path) -> bool {
        patrol::cached_sources_available(data_dir, wiki)
    }

    fn cached_patrol_generation_available(
        &self,
        wiki: &str,
        version: &str,
        data_dir: &Path,
    ) -> bool {
        patrol::cached_sources_available_for_snapshot(data_dir, wiki, version)
    }

    fn compute_candidate(
        &self,
        wiki: &str,
        version: &str,
        data_dir: &Path,
        candidate_dir: &Path,
    ) -> Result<()> {
        compute::compute_all_for_snapshot(wiki, version, data_dir, candidate_dir)
    }

    fn compute_candidate_patrol(
        &self,
        wiki: &str,
        version: &str,
        data_dir: &Path,
        candidate_dir: &Path,
    ) -> Result<()> {
        patrol::compute_patrol_for_snapshot(wiki, version, data_dir, candidate_dir, false, None)
    }
}

fn execute_capacity_benchmark(options: capacity::CapacityBenchmarkOptions<'_>) -> Result<()> {
    let result = capacity::run(options)?;
    println!("{}", serde_json::to_string(&result)?);
    Ok(())
}

fn run_timed_stage<T>(
    stage: &str,
    wiki: Option<&str>,
    action: impl FnOnce() -> Result<T>,
) -> Result<T> {
    let started = Instant::now();
    observability::record_stage_started(stage, wiki);
    info!(stage = stage, wiki = wiki.unwrap_or("-"), "starting stage");
    let result = action();
    let duration = started.elapsed();
    let duration_ms = u64::try_from(duration.as_millis()).unwrap_or(u64::MAX);
    match &result {
        Ok(_) => {
            observability::record_stage_completed(stage, wiki, duration_ms);
            info!(
                stage = stage,
                wiki = wiki.unwrap_or("-"),
                elapsed_ms = duration.as_secs_f64() * 1_000.0,
                "completed stage"
            );
        }
        Err(error) => observability::record_stage_failed(stage, wiki, duration_ms, error),
    }
    result
}

fn record_reused_stage(stage: &str, wiki: Option<&str>) {
    observability::record_stage_started(stage, wiki);
    observability::record_stage_reused(stage, wiki);
    observability::record_stage_completed(stage, wiki, 0);
    info!(
        stage,
        wiki = wiki.unwrap_or("-"),
        "reused stage without execution"
    );
}

fn record_skipped_stage(stage: &str, wiki: Option<&str>) {
    observability::record_stage_started(stage, wiki);
    observability::record_stage_skipped(stage, wiki);
    observability::record_stage_completed(stage, wiki, 0);
    info!(stage, wiki = wiki.unwrap_or("-"), "skipped unneeded stage");
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

fn run_with_ops(cli: Cli, ops: &impl Ops) -> Result<()> {
    let data_dir = cli.data_dir;
    let output_dir = cli.output_dir;
    let run_id = cli.run_id;

    match cli.command {
        Commands::DashboardMaterialize => dashboard::materialize(&output_dir)?,
        Commands::SiteFixture => dashboard::write_site_fixture(&output_dir)?,
        Commands::BrowserPerformanceFixture => {
            dashboard::write_browser_performance_fixture(&output_dir)?
        }
        Commands::DeterminismVerify {
            baseline_root,
            candidate_root,
            artifact_extension,
            baseline_workers,
            candidate_workers,
            algorithm_version,
            report,
        } => {
            let qualification = determinism::qualify_concurrency(
                &baseline_root,
                &candidate_root,
                &artifact_extension,
                baseline_workers,
                candidate_workers,
                &algorithm_version,
                &report,
            )?;
            println!("{}", serde_json::to_string(&qualification)?);
        }
        Commands::CrossSnapshotQualify {
            wiki,
            baseline_version,
            candidate_version,
            work_dir,
            report,
        } => {
            let qualification_result = cross_snapshot::qualify(
                &data_dir,
                &wiki,
                &baseline_version,
                &candidate_version,
                &work_dir,
                &report,
            );
            let qualification = qualification_result?;
            println!("{}", serde_json::to_string(&qualification)?);
        }
        Commands::CleanupStale {
            site_dist_dir,
            minimum_age_secs,
            scratch_dir,
            capacity_dir,
            wikis,
        } => {
            let report = cleanup::clean_abandoned(
                &data_dir,
                &output_dir,
                &site_dist_dir,
                cleanup::CleanupStagingRoots {
                    weekly: scratch_dir.as_deref(),
                    capacity: capacity_dir.as_deref(),
                },
                &wikis,
                run_id.as_deref(),
                Duration::from_secs(minimum_age_secs),
            )?;
            println!("{}", serde_json::to_string(&report)?);
        }

        Commands::SnapshotResolve { wikis } => {
            let version = run_timed_stage("snapshot_resolve", None, || {
                ops.resolve_snapshot(&wikis, Utc::now(), &data_dir)
            })?;
            ops.persist_snapshot_plans(&wikis, &version, &data_dir)?;
            println!("{version}");
        }

        Commands::Fetch { wikis, version } => {
            let version = match version {
                Some(version) => version,
                None => run_timed_stage("snapshot_resolve", None, || {
                    ops.resolve_snapshot(&wikis, Utc::now(), &data_dir)
                })?,
            };
            ops.persist_snapshot_plans(&wikis, &version, &data_dir)?;
            for wiki in &wikis {
                run_timed_stage("fetch", Some(wiki), || {
                    ops.fetch_wiki(wiki, &version, &data_dir)
                })?;
                run_timed_stage("patrol_fetch", Some(wiki), || {
                    ops.fetch_patrol_for_snapshot(wiki, &version, &data_dir)
                })?;
            }
        }

        Commands::Ingest { wikis, version } => {
            for wiki in &wikis {
                run_timed_stage("ingest", Some(wiki), || {
                    ops.ingest_wiki(wiki, version.as_deref(), &data_dir)
                })?;
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
            run_timed_stage("merge", None, || {
                ops.merge_outputs(&output_dir, run_id.as_deref())
            })?;
        }

        Commands::PrepareWiki {
            wiki,
            version,
            source_window_size,
            lifecycle,
        } => {
            let run_id = run_id
                .as_deref()
                .context("candidate preparation requires --run-id")?;
            let version = match version {
                Some(version) => version,
                None => run_timed_stage("snapshot_resolve", Some(&wiki), || {
                    ops.resolve_snapshot(std::slice::from_ref(&wiki), Utc::now(), &data_dir)
                })?,
            };
            let source_window_size = source_window::configured_window_size(source_window_size)?;
            ops.persist_snapshot_plans(std::slice::from_ref(&wiki), &version, &data_dir)?;
            let candidate_dir =
                publication::wiki_candidate_dir(&output_dir, &wiki, &version, run_id)?;
            let preparation = run_timed_stage("candidate_discovery", Some(&wiki), || {
                let plan = ops.plan_candidate_preparation(
                    &wiki,
                    &version,
                    &data_dir,
                    &output_dir,
                    run_id,
                )?;
                if matches!(plan, publication::WikiPreparationPlan::NoOp { .. }) {
                    observability::record_stage_reused("candidate_discovery", Some(&wiki));
                }
                Ok(plan)
            })?;

            match preparation {
                publication::WikiPreparationPlan::NoOp { ready_path } => {
                    info!(wiki, version, path = %ready_path.display(), "candidate preparation is a recorded no-op");
                    println!("{}", ready_path.display());
                }
                publication::WikiPreparationPlan::Build {
                    same_snapshot_candidate,
                    compute_reused,
                    patrol_reused,
                } => {
                    run_timed_stage("source_window", Some(&wiki), || {
                        ops.prepare_candidate_snapshot(
                            &wiki,
                            &version,
                            &data_dir,
                            run_id,
                            source_window_size,
                        )
                    })?;
                    if same_snapshot_candidate
                        && ops.cached_patrol_generation_available(&wiki, &version, &data_dir)
                    {
                        record_skipped_stage("patrol_fetch", Some(&wiki));
                    } else {
                        run_timed_stage("patrol_fetch", Some(&wiki), || {
                            ops.fetch_patrol_for_snapshot(&wiki, &version, &data_dir)
                        })?;
                    }
                    if compute_reused {
                        record_reused_stage("compute", Some(&wiki));
                    } else {
                        run_timed_stage("compute", Some(&wiki), || {
                            ops.compute_candidate(&wiki, &version, &data_dir, &candidate_dir)
                        })?;
                    }
                    if patrol_reused {
                        record_reused_stage("patrol_compute", Some(&wiki));
                    } else {
                        run_timed_stage("patrol_compute", Some(&wiki), || {
                            ops.compute_candidate_patrol(&wiki, &version, &data_dir, &candidate_dir)
                        })?;
                    }
                    let ready = run_timed_stage("candidate_validate", Some(&wiki), || {
                        ops.mark_candidate_ready(
                            &data_dir,
                            &output_dir,
                            &lifecycle,
                            &wiki,
                            &version,
                            run_id,
                        )
                    })?;
                    println!("{}", ready.display());
                }
            }
        }

        Commands::FleetDiscover {
            lifecycle,
            queue_dir,
            snapshot,
        } => {
            let controller_run_id = run_id
                .as_deref()
                .context("fleet discovery requires --run-id")?;
            let wikis = fleet::scheduled_wikis(&lifecycle)?;
            let overrides = fleet::lifecycle_resource_overrides(&lifecycle)?;
            let mut report = fleet::DiscoveryReport::default();
            for wiki in wikis {
                let version = match snapshot.as_deref() {
                    Some(version) => version.to_string(),
                    None => run_timed_stage("fleet_snapshot_resolve", Some(&wiki), || {
                        ops.resolve_snapshot(std::slice::from_ref(&wiki), Utc::now(), &data_dir)
                    })?,
                };
                ops.persist_snapshot_plans(std::slice::from_ref(&wiki), &version, &data_dir)?;
                let (plan, _) =
                    snapshot_plan::SnapshotPlan::load_or_resolve(&data_dir, &wiki, &version)?;
                let (resource_class, signals) =
                    fleet::classify(&data_dir, &output_dir, &plan, overrides.get(&wiki).copied())?;
                report.merge(fleet::enqueue(
                    &queue_dir,
                    &wiki,
                    &version,
                    resource_class,
                    signals,
                    controller_run_id,
                )?);
            }
            println!("{}", serde_json::to_string(&report)?);
        }

        Commands::FleetClaim {
            queue_dir,
            resource_class,
            worker_id,
            lease_timeout_secs,
            receipt,
        } => match fleet::claim(&queue_dir, resource_class, &worker_id, lease_timeout_secs)? {
            Some(claim) => {
                fleet::write_claim_receipt(&receipt, &claim)?;
                println!("{}", serde_json::to_string(&claim)?);
            }
            None => println!("{{\"claimed\":false}}"),
        },

        Commands::FleetHeartbeat { queue_dir, receipt } => {
            let claim = fleet::heartbeat(&queue_dir, &receipt)?;
            println!("{}", serde_json::to_string(&claim)?);
        }

        Commands::FleetComplete { queue_dir, receipt } => {
            let notification = fleet::complete(&queue_dir, &receipt, &output_dir)?;
            println!("{}", notification.display());
        }

        Commands::FleetFail {
            queue_dir,
            receipt,
            error,
            max_attempts,
        } => {
            let quarantined = fleet::fail(&queue_dir, &receipt, &error, max_attempts)?;
            println!("{{\"quarantined\":{quarantined}}}");
        }

        Commands::FleetRecover {
            queue_dir,
            max_attempts,
        } => {
            let recovered = fleet::recover_stale(&queue_dir, max_attempts)?;
            println!("{{\"recovered\":{recovered}}}");
        }

        Commands::QualifyWiki {
            wiki,
            version,
            source_window_size,
            lifecycle,
        } => {
            let run_id = run_id
                .as_deref()
                .context("wiki qualification requires --run-id")?;
            ops.ensure_qualification_wiki(&lifecycle, &wiki)?;
            let version = match version {
                Some(version) => version,
                None => run_timed_stage("snapshot_resolve", Some(&wiki), || {
                    ops.resolve_snapshot(std::slice::from_ref(&wiki), Utc::now(), &data_dir)
                })?,
            };
            let source_window_size = source_window::configured_window_size(source_window_size)?;
            ops.persist_snapshot_plans(std::slice::from_ref(&wiki), &version, &data_dir)?;
            let qualification_dir =
                publication::wiki_qualification_dir(&output_dir, &wiki, &version, run_id)?;
            run_timed_stage("source_window", Some(&wiki), || {
                ops.prepare_candidate_snapshot(
                    &wiki,
                    &version,
                    &data_dir,
                    run_id,
                    source_window_size,
                )
            })?;
            run_timed_stage("patrol_fetch", Some(&wiki), || {
                ops.fetch_patrol_for_snapshot(&wiki, &version, &data_dir)
            })?;
            run_timed_stage("compute", Some(&wiki), || {
                ops.compute_candidate(&wiki, &version, &data_dir, &qualification_dir)
            })?;
            run_timed_stage("patrol_compute", Some(&wiki), || {
                ops.compute_candidate_patrol(&wiki, &version, &data_dir, &qualification_dir)
            })?;
            let receipt = run_timed_stage("qualification_validate", Some(&wiki), || {
                ops.mark_qualification_ready(
                    &data_dir,
                    &output_dir,
                    &lifecycle,
                    &wiki,
                    &version,
                    run_id,
                )
            })?;
            println!("{}", receipt.display());
        }

        Commands::PublicationPrepareReady { lifecycle } => {
            let run_id = run_id
                .as_deref()
                .context("ready publication preparation requires --run-id")?;
            run_timed_stage("publication_prepare", None, || {
                ops.prepare_ready_publication(&data_dir, &output_dir, &lifecycle, run_id)
            })?;
        }

        Commands::PublicationCommitReady => {
            let run_id = run_id
                .as_deref()
                .context("ready publication commit requires --run-id")?;
            run_timed_stage("publication_commit", None, || {
                ops.commit_ready_publication(&data_dir, &output_dir, run_id)
            })?;
        }

        Commands::PublicationRollbackReady { lifecycle } => {
            let run_id = run_id
                .as_deref()
                .context("ready publication rollback requires --run-id")?;
            run_timed_stage("publication_rollback", None, || {
                ops.rollback_ready_publication(&data_dir, &output_dir, &lifecycle, run_id)
            })?;
        }

        Commands::PublicationRecoveryAudit {
            site_dist_dir,
            transaction_run_id,
        } => {
            let report = publication::audit_publication_recovery(
                &data_dir,
                &output_dir,
                &site_dist_dir,
                transaction_run_id.as_deref(),
            );
            println!("{}", serde_json::to_string_pretty(&report)?);
        }

        Commands::PublicationRecover {
            lifecycle,
            site_dist_dir,
            transaction_run_id,
            all,
            report_path,
        } => {
            ensure!(
                all || transaction_run_id.is_some(),
                "publication recovery requires --run-id or --all"
            );
            let recovery_run_id = run_id.as_deref().map(str::to_string).unwrap_or_else(|| {
                format!(
                    "recovery-{}",
                    transaction_run_id.as_deref().unwrap_or("all")
                )
            });
            let report = publication::recover_publication_transactions(
                &data_dir,
                &output_dir,
                &lifecycle,
                &site_dist_dir,
                transaction_run_id.as_deref(),
                &recovery_run_id,
            )?;
            if let Some(path) = report_path.as_ref() {
                publication::write_publication_recovery_report(path, &report)?;
            }
            println!("{}", serde_json::to_string_pretty(&report)?);
        }

        Commands::Merge => {
            publication::begin_run(&output_dir, run_id.as_deref(), &[], None)?;
            run_timed_stage("merge", None, || {
                ops.merge_outputs(&output_dir, run_id.as_deref())
            })?;
        }

        Commands::PublicationValidate => {
            let run_id = run_id
                .as_deref()
                .context("publication validation requires --run-id")?;
            let lifecycle = env::var_os("WIKI_ECON_WIKI_LIFECYCLE_FILE")
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("config/wiki-lifecycle.json"));
            run_timed_stage("publication_validate", None, || {
                publication::validate(&data_dir, &output_dir, &lifecycle, run_id)
            })?;
        }

        Commands::PublicationVerify => {
            let run_id = run_id
                .as_deref()
                .context("publication verification requires --run-id")?;
            run_timed_stage("publication_verify", None, || {
                publication::verify(&output_dir, run_id)
            })?;
        }

        Commands::ArtifactScrub { report } => {
            let scrub_run_id = run_id.as_deref().unwrap_or("manual-artifact-scrub");
            let scrub_result = run_timed_stage("artifact_scrub", None, || {
                artifact_receipt::scrub_published(&output_dir)
            });
            let scrub = match scrub_result {
                Ok(scrub) => scrub,
                Err(error) => {
                    artifact_receipt::record_scrub_failure(&output_dir, scrub_run_id, &error)?;
                    return Err(error);
                }
            };
            let publication = (|| -> Result<()> {
                if let Some(path) = report {
                    artifact_receipt::write_scrub_report(&path, &scrub)?;
                }
                artifact_receipt::record_scrub_success(&output_dir, scrub_run_id, &scrub)
            })();
            if let Err(error) = publication {
                artifact_receipt::record_scrub_failure(&output_dir, scrub_run_id, &error)?;
                return Err(error);
            }
            println!("{}", serde_json::to_string_pretty(&scrub)?);
        }

        Commands::SnapshotFinalize { wikis } => {
            for wiki in &wikis {
                run_timed_stage("snapshot_finalize", Some(wiki), || {
                    ops.finalize_snapshot(wiki, &data_dir)
                })?;
            }
        }

        Commands::SnapshotRepair { wiki, version } => {
            run_timed_stage("snapshot_repair", Some(&wiki), || {
                let markers = storage::repair_current_snapshot(&data_dir, &wiki, &version)?;
                info!(wiki, version, markers, "repaired current snapshot pointer");
                Ok(())
            })?;
        }

        Commands::SiteFingerprintCheck { site_dir, dist_dir } => {
            anyhow::ensure!(
                fingerprint::site_is_reusable(&output_dir, &site_dir, &dist_dir)?,
                "site stage fingerprint changed"
            );
            observability::record_stage_reused("site", None);
            info!(path = %dist_dir.display(), "reusing deterministic site stage");
        }

        Commands::SiteFingerprintRecord { site_dir, dist_dir } => {
            fingerprint::record_site(&output_dir, &site_dir, &dist_dir)?;
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

        Commands::CapacityBench {
            wiki,
            weekly_buckets,
            weekly_secondary_buckets,
            scratch_dir,
            report,
            raw_transient_bytes,
            nfs_quota_bytes,
            storage_reserve_bytes,
            quota_root,
            minimum_memory_headroom_percent,
            requested_cpu,
        } => {
            let bucket_label = if weekly_secondary_buckets == 1 {
                format!("weekly-buckets-{weekly_buckets}")
            } else {
                format!("weekly-buckets-{weekly_buckets}x{weekly_secondary_buckets}")
            };
            let report_path = report.unwrap_or_else(|| {
                output_dir
                    .join("capacity")
                    .join(&wiki)
                    .join(format!("{bucket_label}.json"))
            });
            let benchmark_output = output_dir.join("capacity").join(&wiki).join(&bucket_label);
            let quota_root = quota_root.unwrap_or_else(|| {
                data_dir
                    .parent()
                    .map(Path::to_path_buf)
                    .unwrap_or_else(|| data_dir.clone())
            });
            run_timed_stage("capacity_benchmark", Some(&wiki), || {
                ops.capacity_benchmark(
                    &wiki,
                    &data_dir,
                    &benchmark_output,
                    &scratch_dir,
                    &report_path,
                    weekly_buckets,
                    weekly_secondary_buckets,
                    raw_transient_bytes,
                    nfs_quota_bytes,
                    storage_reserve_bytes,
                    &quota_root,
                    minimum_memory_headroom_percent,
                    requested_cpu,
                )
            })?;
        }

        Commands::CpuQualify {
            capacity_reports,
            report,
        } => {
            run_timed_stage("cpu_qualification", None, || {
                ops.cpu_qualification(&capacity_reports, &report)
            })?;
        }

        Commands::SchemaBenchmark {
            wikis,
            scratch_dir,
            report,
        } => {
            run_timed_stage("schema_benchmark", None, || {
                ops.schema_benchmark(&data_dir, &scratch_dir, &report, &wikis, run_id.as_deref())
            })?;
        }

        Commands::Run {
            wikis,
            version,
            source_window_size,
            stage,
        } => {
            let version = match version {
                Some(version) => version,
                None => run_timed_stage("snapshot_resolve", None, || {
                    ops.resolve_snapshot(&wikis, Utc::now(), &data_dir)
                })?,
            };
            let source_window_size = source_window::configured_window_size(source_window_size)?;
            let source_window_run_id = run_id
                .clone()
                .unwrap_or_else(|| format!("manual-{}", std::process::id()));
            ops.persist_snapshot_plans(&wikis, &version, &data_dir)?;
            publication::begin_run(&output_dir, run_id.as_deref(), &wikis, Some(&version))?;
            for wiki in &wikis {
                info!(wiki = wiki, stage = ?stage, "running pipeline stage");
                if stage.runs_ingest() {
                    run_timed_stage("source_window", Some(wiki), || {
                        ops.prepare_wiki_snapshot(
                            wiki,
                            &version,
                            &data_dir,
                            &source_window_run_id,
                            source_window_size,
                        )
                    })?;
                    run_timed_stage("patrol_fetch", Some(wiki), || {
                        ops.fetch_patrol_for_snapshot(wiki, &version, &data_dir)
                    })?;
                }
                if stage.runs_compute() {
                    run_timed_stage("compute", Some(wiki), || {
                        ops.compute_all(wiki, &data_dir, &output_dir)
                    })?;
                    run_timed_stage("patrol_compute", Some(wiki), || {
                        ops.compute_patrol(wiki, &data_dir, &output_dir, false, None)
                    })?;
                }
            }
            if stage.runs_compute() {
                run_timed_stage("merge", None, || {
                    ops.merge_outputs(&output_dir, run_id.as_deref())
                })?;
            }
        }
    }

    Ok(())
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let run_id = logging_run_id(&cli);
    init_tracing(&run_id);
    run_with_ops(cli, &RealOps)
}

#[derive(Clone, Debug)]
struct RunIdEventFormat {
    run_id: String,
    inner: Format<Compact, ()>,
}

impl RunIdEventFormat {
    fn new(run_id: &str, ansi: bool) -> Self {
        Self {
            run_id: run_id
                .chars()
                .map(|character| {
                    if character.is_ascii_alphanumeric() || "._-".contains(character) {
                        character
                    } else {
                        '_'
                    }
                })
                .collect(),
            inner: tracing_subscriber::fmt::format()
                .without_time()
                .with_target(false)
                .with_ansi(ansi)
                .compact(),
        }
    }
}

impl<S, N> FormatEvent<S, N> for RunIdEventFormat
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
    N: for<'writer> FormatFields<'writer> + 'static,
{
    fn format_event(
        &self,
        context: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        write!(writer, "run_id={} ", self.run_id)?;
        self.inner.format_event(context, writer, event)
    }
}

fn init_tracing(run_id: &str) {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .fmt_fields(DefaultFields::new())
        .event_format(RunIdEventFormat::new(run_id, log_ansi_enabled()))
        .try_init();
}

fn logging_run_id(cli: &Cli) -> String {
    let environment_run_id = env::var("WIKI_ECON_RUN_ID").ok();
    resolve_logging_run_id(
        cli.run_id.as_deref(),
        environment_run_id.as_deref(),
        std::process::id(),
    )
}

fn log_ansi_enabled() -> bool {
    let configured = env::var("WIKI_ECON_LOG_ANSI").ok();
    log_ansi_enabled_from(configured.as_deref())
}

fn resolve_logging_run_id(cli: Option<&str>, environment: Option<&str>, pid: u32) -> String {
    cli.filter(|value| !value.is_empty())
        .or_else(|| environment.filter(|value| !value.is_empty()))
        .map(ToString::to_string)
        .unwrap_or_else(|| format!("standalone-{pid}"))
}

fn log_ansi_enabled_from(value: Option<&str>) -> bool {
    !value.is_some_and(|value| matches!(value, "0" | "false" | "no"))
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
        preparation_plans: RefCell<VecDeque<publication::WikiPreparationPlan>>,
        preparation_error: bool,
        cached_patrol_sources: bool,
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
        fn resolve_snapshot(
            &self,
            wikis: &[String],
            _now: DateTime<Utc>,
            _data_dir: &Path,
        ) -> Result<String> {
            self.record(format!("resolve_snapshot:{}", wikis.join(",")));
            Ok("2026-07".to_string())
        }

        fn fetch_wiki(&self, wiki: &str, version: &str, data_dir: &Path) -> Result<()> {
            self.record(format!("fetch:{wiki}:{version}:{}", data_dir.display()));
            Ok(())
        }

        fn fetch_patrol(&self, wiki: &str, data_dir: &Path) -> Result<()> {
            self.record(format!("fetch_patrol:{wiki}:{}", data_dir.display()));
            Ok(())
        }

        fn ingest_wiki(&self, wiki: &str, version: Option<&str>, data_dir: &Path) -> Result<()> {
            self.record(format!(
                "ingest:{wiki}:{}:{}",
                version.unwrap_or("_"),
                data_dir.display()
            ));
            Ok(())
        }

        fn prepare_wiki_snapshot(
            &self,
            wiki: &str,
            version: &str,
            data_dir: &Path,
            _run_id: &str,
            window_size: usize,
        ) -> Result<()> {
            self.record(format!(
                "source_window:{wiki}:{version}:{}:{window_size}",
                data_dir.display()
            ));
            Ok(())
        }

        fn plan_candidate_preparation(
            &self,
            _wiki: &str,
            _version: &str,
            _data_dir: &Path,
            _output_dir: &Path,
            _run_id: &str,
        ) -> Result<publication::WikiPreparationPlan> {
            if self.preparation_error {
                anyhow::bail!("candidate plan failed");
            }
            Ok(self.preparation_plans.borrow_mut().pop_front().unwrap_or(
                publication::WikiPreparationPlan::Build {
                    same_snapshot_candidate: false,
                    compute_reused: false,
                    patrol_reused: false,
                },
            ))
        }

        fn cached_patrol_sources_available(&self, _wiki: &str, _data_dir: &Path) -> bool {
            self.cached_patrol_sources
        }

        fn cleanup_raw_dump(&self, wiki: &str, data_dir: &Path) -> Result<()> {
            self.record(format!("cleanup_raw:{wiki}:{}", data_dir.display()));
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

        fn capacity_benchmark(
            &self,
            wiki: &str,
            data_dir: &Path,
            output_dir: &Path,
            scratch_dir: &Path,
            report_path: &Path,
            weekly_buckets: usize,
            weekly_secondary_buckets: usize,
            raw_transient_bytes: u64,
            nfs_quota_bytes: Option<u64>,
            storage_reserve_bytes: u64,
            quota_root: &Path,
            minimum_memory_headroom_percent: u8,
            requested_cpu: usize,
        ) -> Result<()> {
            self.record(format!(
                "capacity:{wiki}:{}:{}:{}:{}:{weekly_buckets}x{weekly_secondary_buckets}:{raw_transient_bytes}:{}:{storage_reserve_bytes}:{}:{minimum_memory_headroom_percent}:{requested_cpu}",
                data_dir.display(),
                output_dir.display(),
                scratch_dir.display(),
                report_path.display(),
                nfs_quota_bytes
                    .map(|quota| quota.to_string())
                    .unwrap_or_else(|| "shared".to_string()),
                quota_root.display(),
            ));
            Ok(())
        }

        fn cpu_qualification(&self, capacity_reports: &[PathBuf], report: &Path) -> Result<()> {
            self.record(format!(
                "cpu-qualification:{}:{}",
                capacity_reports
                    .iter()
                    .map(|path| path.display().to_string())
                    .collect::<Vec<_>>()
                    .join(","),
                report.display()
            ));
            Ok(())
        }

        fn schema_benchmark(
            &self,
            data_dir: &Path,
            scratch_dir: &Path,
            report_path: &Path,
            wikis: &[String],
            _run_id: Option<&str>,
        ) -> Result<()> {
            self.record(format!(
                "schema_benchmark:{}:{}:{}",
                wikis.join(","),
                data_dir.display(),
                scratch_dir.display()
            ));
            anyhow::ensure!(
                report_path == Path::new("reports/schema.json"),
                "unexpected schema report path"
            );
            Ok(())
        }

        fn merge_outputs(&self, output_dir: &Path, _run_id: Option<&str>) -> Result<()> {
            self.record(format!("merge:{}", output_dir.display()));
            Ok(())
        }

        fn finalize_snapshot(&self, wiki: &str, data_dir: &Path) -> Result<()> {
            self.record(format!("snapshot_finalize:{wiki}:{}", data_dir.display()));
            Ok(())
        }

        fn mark_candidate_ready(
            &self,
            _data_dir: &Path,
            output_dir: &Path,
            _lifecycle: &Path,
            wiki: &str,
            version: &str,
            run_id: &str,
        ) -> Result<PathBuf> {
            self.record(format!("candidate_ready:{wiki}:{version}:{run_id}"));
            Ok(output_dir.join("ready.json"))
        }

        fn ensure_qualification_wiki(&self, _lifecycle: &Path, wiki: &str) -> Result<()> {
            self.record(format!("qualification_lifecycle:{wiki}"));
            Ok(())
        }

        fn mark_qualification_ready(
            &self,
            _data_dir: &Path,
            output_dir: &Path,
            _lifecycle: &Path,
            wiki: &str,
            version: &str,
            run_id: &str,
        ) -> Result<PathBuf> {
            self.record(format!("qualification_ready:{wiki}:{version}:{run_id}"));
            Ok(output_dir.join("qualification.json"))
        }

        fn prepare_ready_publication(
            &self,
            _data_dir: &Path,
            _output_dir: &Path,
            _lifecycle: &Path,
            run_id: &str,
        ) -> Result<()> {
            self.record(format!("publication_prepare:{run_id}"));
            Ok(())
        }

        fn commit_ready_publication(
            &self,
            _data_dir: &Path,
            _output_dir: &Path,
            run_id: &str,
        ) -> Result<()> {
            self.record(format!("publication_commit:{run_id}"));
            Ok(())
        }

        fn rollback_ready_publication(
            &self,
            _data_dir: &Path,
            _output_dir: &Path,
            _lifecycle: &Path,
            run_id: &str,
        ) -> Result<()> {
            self.record(format!("publication_rollback:{run_id}"));
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

        fn ingest_wiki(&self, _wiki: &str, _version: Option<&str>, _data_dir: &Path) -> Result<()> {
            if self.fail_stage == "ingest" {
                anyhow::bail!("ingest failed");
            }
            Ok(())
        }

        fn cleanup_raw_dump(&self, _wiki: &str, _data_dir: &Path) -> Result<()> {
            if self.fail_stage == "cleanup_raw" {
                anyhow::bail!("cleanup raw failed");
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

        fn capacity_benchmark(
            &self,
            _wiki: &str,
            _data_dir: &Path,
            _output_dir: &Path,
            _scratch_dir: &Path,
            _report_path: &Path,
            _weekly_buckets: usize,
            _weekly_secondary_buckets: usize,
            _raw_transient_bytes: u64,
            _nfs_quota_bytes: Option<u64>,
            _storage_reserve_bytes: u64,
            _quota_root: &Path,
            _minimum_memory_headroom_percent: u8,
            _requested_cpu: usize,
        ) -> Result<()> {
            if self.fail_stage == "capacity" {
                anyhow::bail!("capacity benchmark failed");
            }
            Ok(())
        }

        fn schema_benchmark(
            &self,
            _data_dir: &Path,
            _scratch_dir: &Path,
            _report_path: &Path,
            _wikis: &[String],
            _run_id: Option<&str>,
        ) -> Result<()> {
            if self.fail_stage == "schema_benchmark" {
                anyhow::bail!("schema benchmark failed");
            }
            Ok(())
        }

        fn merge_outputs(&self, _output_dir: &Path, _run_id: Option<&str>) -> Result<()> {
            if self.fail_stage == "merge" {
                anyhow::bail!("merge failed");
            }
            Ok(())
        }

        fn finalize_snapshot(&self, _wiki: &str, _data_dir: &Path) -> Result<()> {
            if self.fail_stage == "snapshot_finalize" {
                anyhow::bail!("snapshot finalize failed");
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
    fn run_with_ops_dispatches_snapshot_resolution() -> Result<()> {
        init_test_tracing();
        let cli = Cli::try_parse_from(["wiki-econ", "snapshot-resolve", "frwiki", "dewiki"])?;
        let ops = RecordingOps::default();

        run_with_ops(cli, &ops)?;

        assert_eq!(
            ops.calls.into_inner(),
            vec!["resolve_snapshot:frwiki,dewiki"]
        );
        Ok(())
    }

    #[test]
    fn run_with_ops_dispatches_ingest() -> Result<()> {
        init_test_tracing();
        let cli = Cli::try_parse_from(["wiki-econ", "ingest", "frwiki"])?;
        let ops = RecordingOps::default();

        run_with_ops(cli, &ops)?;

        assert_eq!(ops.calls.into_inner(), vec!["ingest:frwiki:_:data"]);
        Ok(())
    }

    #[test]
    fn run_with_ops_dispatches_versioned_ingest_and_snapshot_finalize() -> Result<()> {
        init_test_tracing();
        let ingest_cli = Cli::try_parse_from([
            "wiki-econ",
            "--data-dir",
            "dataset",
            "ingest",
            "nlwiki",
            "--version",
            "2026-07",
        ])?;
        let finalize_cli = Cli::try_parse_from([
            "wiki-econ",
            "--data-dir",
            "dataset",
            "snapshot-finalize",
            "nlwiki",
        ])?;
        let ops = RecordingOps::default();

        run_with_ops(ingest_cli, &ops)?;
        run_with_ops(finalize_cli, &ops)?;

        assert_eq!(
            ops.calls.into_inner(),
            vec![
                "ingest:nlwiki:2026-07:dataset",
                "snapshot_finalize:nlwiki:dataset",
            ]
        );
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
    fn run_with_ops_prepares_one_isolated_wiki_candidate() -> Result<()> {
        init_test_tracing();
        let cli = Cli::try_parse_from([
            "wiki-econ",
            "--data-dir",
            "fixtures/data",
            "--output-dir",
            "fixtures/output",
            "--run-id",
            "run-7",
            "prepare-wiki",
            "nlwiki",
            "--version",
            "2026-07",
            "--source-window-size",
            "2",
        ])?;
        let ops = RecordingOps::default();

        run_with_ops(cli, &ops)?;

        assert_eq!(
            ops.calls.into_inner(),
            vec![
                "source_window:nlwiki:2026-07:fixtures/data:2",
                "fetch_patrol:nlwiki:fixtures/data",
                "compute:nlwiki:fixtures/data:fixtures/output/_candidates/nlwiki/2026-07/run-7",
                "compute_patrol:nlwiki:fixtures/data:fixtures/output/_candidates/nlwiki/2026-07/run-7:false:_",
                "candidate_ready:nlwiki:2026-07:run-7",
            ]
        );
        Ok(())
    }

    #[test]
    fn run_with_ops_qualifies_one_publication_invisible_wiki() -> Result<()> {
        init_test_tracing();
        let cli = Cli::try_parse_from([
            "wiki-econ",
            "--data-dir",
            "qualification/data",
            "--output-dir",
            "qualification/output",
            "--run-id",
            "qualify-7",
            "qualify-wiki",
            "itwiki",
            "--version",
            "2026-07",
            "--source-window-size",
            "1",
        ])?;
        let ops = RecordingOps::default();

        run_with_ops(cli, &ops)?;

        assert_eq!(
            ops.calls.into_inner(),
            vec![
                "qualification_lifecycle:itwiki",
                "source_window:itwiki:2026-07:qualification/data:1",
                "fetch_patrol:itwiki:qualification/data",
                "compute:itwiki:qualification/data:qualification/output/_qualifications/itwiki/2026-07/qualify-7",
                "compute_patrol:itwiki:qualification/data:qualification/output/_qualifications/itwiki/2026-07/qualify-7:false:_",
                "qualification_ready:itwiki:2026-07:qualify-7",
            ]
        );
        Ok(())
    }

    #[test]
    fn wiki_qualification_resolves_an_unpinned_snapshot() -> Result<()> {
        let cli = Cli::try_parse_from([
            "wiki-econ",
            "--run-id",
            "qualification-resolved-run",
            "qualify-wiki",
            "itwiki",
        ])?;
        let ops = RecordingOps::default();

        run_with_ops(cli, &ops)?;

        assert_eq!(ops.calls.borrow()[1], "resolve_snapshot:itwiki");
        assert!(ops.calls.borrow().iter().any(|call| {
            call == "qualification_ready:itwiki:2026-07:qualification-resolved-run"
        }));
        Ok(())
    }

    #[test]
    fn candidate_preparation_resolves_an_unpinned_snapshot() -> Result<()> {
        let cli = Cli::try_parse_from([
            "wiki-econ",
            "--run-id",
            "resolved-run",
            "prepare-wiki",
            "nlwiki",
        ])?;
        let ops = RecordingOps::default();

        run_with_ops(cli, &ops)?;

        assert_eq!(ops.calls.borrow()[0], "resolve_snapshot:nlwiki");
        assert!(
            ops.calls
                .borrow()
                .iter()
                .any(|call| call == "candidate_ready:nlwiki:2026-07:resolved-run")
        );
        Ok(())
    }

    #[test]
    fn unchanged_candidate_preparation_is_a_recorded_noop() -> Result<()> {
        let cli = Cli::try_parse_from([
            "wiki-econ",
            "--run-id",
            "noop-run",
            "prepare-wiki",
            "nlwiki",
            "--version",
            "2026-07",
        ])?;
        let ops = RecordingOps {
            preparation_plans: RefCell::new(VecDeque::from([
                publication::WikiPreparationPlan::NoOp {
                    ready_path: PathBuf::from("existing/ready.json"),
                },
            ])),
            ..RecordingOps::default()
        };

        run_with_ops(cli, &ops)?;

        assert!(ops.calls.into_inner().is_empty());
        Ok(())
    }

    #[test]
    fn preparation_runs_only_invalidated_candidate_stages() -> Result<()> {
        let cli = Cli::try_parse_from([
            "wiki-econ",
            "--data-dir",
            "fixtures/data",
            "--output-dir",
            "fixtures/output",
            "--run-id",
            "partial-run",
            "prepare-wiki",
            "nlwiki",
            "--version",
            "2026-07",
        ])?;
        let ops = RecordingOps {
            preparation_plans: RefCell::new(VecDeque::from([
                publication::WikiPreparationPlan::Build {
                    same_snapshot_candidate: true,
                    compute_reused: true,
                    patrol_reused: true,
                },
            ])),
            cached_patrol_sources: true,
            ..RecordingOps::default()
        };

        run_with_ops(cli, &ops)?;

        assert_eq!(
            ops.calls.into_inner(),
            vec![
                "source_window:nlwiki:2026-07:fixtures/data:1",
                "candidate_ready:nlwiki:2026-07:partial-run",
            ]
        );
        Ok(())
    }

    #[test]
    fn ops_default_publication_methods_delegate_and_fail_closed() -> Result<()> {
        let root = TestDir::new()?;
        let data = root.path().join("data");
        let output = root.path().join("output");
        let lifecycle = root.path().join("missing-lifecycle.json");
        fs::create_dir_all(&output)?;
        let ops = FailingOps { fail_stage: "none" };

        assert!(
            ops.mark_candidate_ready(&data, &output, &lifecycle, "nlwiki", "2026-07", "candidate",)
                .is_err()
        );
        assert!(ops.ensure_qualification_wiki(&lifecycle, "itwiki").is_err());
        assert!(
            ops.mark_qualification_ready(
                &data,
                &output,
                &lifecycle,
                "itwiki",
                "2026-07",
                "qualification",
            )
            .is_err()
        );
        assert!(
            ops.prepare_ready_publication(&data, &output, &lifecycle, "prepare")
                .is_err()
        );
        assert!(
            ops.commit_ready_publication(&data, &output, "commit")
                .is_err()
        );
        assert!(
            ops.rollback_ready_publication(&data, &output, &lifecycle, "rollback")
                .is_err()
        );
        assert_eq!(
            ops.plan_candidate_preparation("nlwiki", "2026-07", &data, &output, "candidate",)?,
            publication::WikiPreparationPlan::Build {
                same_snapshot_candidate: false,
                compute_reused: false,
                patrol_reused: false,
            }
        );
        assert!(!ops.cached_patrol_sources_available("nlwiki", &data));
        Ok(())
    }

    #[test]
    fn run_with_ops_dispatches_short_publication_transaction_commands() -> Result<()> {
        init_test_tracing();
        let ops = RecordingOps::default();
        for command in [
            "publication-prepare-ready",
            "publication-commit-ready",
            "publication-rollback-ready",
        ] {
            let cli = Cli::try_parse_from(["wiki-econ", "--run-id", "publish-9", command])?;
            run_with_ops(cli, &ops)?;
        }

        assert_eq!(
            ops.calls.into_inner(),
            vec![
                "publication_prepare:publish-9",
                "publication_commit:publish-9",
                "publication_rollback:publish-9",
            ]
        );
        Ok(())
    }

    #[test]
    fn publication_recovery_cli_audits_and_repairs_empty_transaction_sets() -> Result<()> {
        let root = TestDir::new()?;
        let data = root.path().join("data");
        let output = root.path().join("output");
        let site = root.path().join("site-dist");
        let report = root.path().join("recovery.json");
        fs::create_dir_all(&data)?;
        fs::create_dir_all(&output)?;
        fs::create_dir_all(&site)?;

        let audit = Cli::try_parse_from([
            "wiki-econ",
            "--data-dir",
            data.to_str().context("data path")?,
            "--output-dir",
            output.to_str().context("output path")?,
            "publication-recovery-audit",
            "--site-dist-dir",
            site.to_str().context("site path")?,
        ])
        .expect("recovery audit CLI should parse");
        run_with_ops(audit, &RecordingOps::default())?;

        let recover = Cli::try_parse_from([
            "wiki-econ",
            "--data-dir",
            data.to_str().context("data path")?,
            "--output-dir",
            output.to_str().context("output path")?,
            "publication-recover",
            "--all",
            "--site-dist-dir",
            site.to_str().context("site path")?,
            "--report",
            report.to_str().context("report path")?,
        ])
        .expect("recovery repair CLI should parse");
        run_with_ops(recover, &RecordingOps::default())?;
        assert!(report.is_file());

        let missing_selector = Cli::try_parse_from([
            "wiki-econ",
            "--data-dir",
            data.to_str().context("data path")?,
            "--output-dir",
            output.to_str().context("output path")?,
            "publication-recover",
            "--site-dist-dir",
            site.to_str().context("site path")?,
        ])
        .expect("recovery CLI without a selector should parse before validation");
        assert!(run_with_ops(missing_selector, &RecordingOps::default()).is_err());

        let broken_transaction = output.join("_publication_transactions/broken");
        fs::create_dir_all(&broken_transaction)?;
        fs::write(broken_transaction.join("selection.json"), "invalid")?;
        let ambiguous = Cli::try_parse_from([
            "wiki-econ",
            "--data-dir",
            data.to_str().context("data path")?,
            "--output-dir",
            output.to_str().context("output path")?,
            "publication-recover",
            "--run-id",
            "broken",
            "--site-dist-dir",
            site.to_str().context("site path")?,
        ])
        .expect("ambiguous recovery CLI should parse");
        assert!(run_with_ops(ambiguous, &RecordingOps::default()).is_err());

        fs::remove_dir_all(output.join("_publication_transactions"))?;
        let blocked_report = root.path().join("blocked-report");
        fs::create_dir(&blocked_report)?;
        let report_failure = Cli::try_parse_from([
            "wiki-econ",
            "--data-dir",
            data.to_str().context("data path")?,
            "--output-dir",
            output.to_str().context("output path")?,
            "publication-recover",
            "--all",
            "--site-dist-dir",
            site.to_str().context("site path")?,
            "--report",
            blocked_report.to_str().context("blocked report path")?,
        ])
        .expect("blocked report recovery CLI should parse");
        assert!(run_with_ops(report_failure, &RecordingOps::default()).is_err());

        let no_report = Cli::try_parse_from([
            "wiki-econ",
            "--data-dir",
            data.to_str().context("data path")?,
            "--output-dir",
            output.to_str().context("output path")?,
            "publication-recover",
            "--all",
            "--site-dist-dir",
            site.to_str().context("site path")?,
        ])
        .expect("no-report recovery CLI should parse");
        run_with_ops(no_report, &RecordingOps::default())
            .expect("no-report recovery should succeed");
        Ok(())
    }

    #[test]
    fn isolated_candidate_and_publication_commands_require_run_ids() -> Result<()> {
        for args in [
            vec!["wiki-econ", "prepare-wiki", "nlwiki"],
            vec!["wiki-econ", "qualify-wiki", "itwiki"],
            vec!["wiki-econ", "publication-prepare-ready"],
            vec!["wiki-econ", "publication-commit-ready"],
            vec!["wiki-econ", "publication-rollback-ready"],
        ] {
            let error = run_with_ops(Cli::try_parse_from(args)?, &RecordingOps::default())
                .expect_err("transactional commands require a stable run ID");
            assert!(error.to_string().contains("requires --run-id"));
        }
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
    fn publication_commands_require_run_ids_and_reach_their_validators() -> Result<()> {
        let ops = RecordingOps::default();
        for command in ["publication-validate", "publication-verify"] {
            let cli = Cli::try_parse_from(["wiki-econ", command])?;
            let error = run_with_ops(cli, &ops).expect_err("run ID is mandatory");
            assert!(error.to_string().contains("requires --run-id"));

            let temp = TestDir::new()?;
            let data = temp.path().join("data");
            let output = temp.path().join("output");
            fs::create_dir_all(&data)?;
            fs::create_dir_all(&output)?;
            let cli = Cli::try_parse_from([
                "wiki-econ",
                "--data-dir",
                data.to_str().context("UTF-8 data path")?,
                "--output-dir",
                output.to_str().context("UTF-8 output path")?,
                "--run-id",
                "test-run",
                command,
            ])
            .expect("publication command should parse");
            let error = run_with_ops(cli, &ops).expect_err("missing publication state must fail");
            assert!(error.to_string().contains("failed to read"));
        }
        Ok(())
    }

    #[test]
    fn run_id_initializes_context_for_merge_and_full_run() -> Result<()> {
        let ops = RecordingOps::default();
        for command in [vec!["merge"], vec!["run", "nlwiki", "--version", "2026-03"]] {
            let output = TestDir::new()?;
            let mut args = vec![
                "wiki-econ",
                "--output-dir",
                output.path().to_str().context("UTF-8 output path")?,
                "--run-id",
                "test-run",
            ];
            args.extend(command);
            run_with_ops(Cli::try_parse_from(args)?, &ops)?;
            let context: Value =
                serde_json::from_slice(&fs::read(output.path().join(".publication-run.json"))?)?;
            assert_eq!(context["run_id"], "test-run");
        }
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
    fn determinism_verify_cli_compares_distinct_worker_builds() -> Result<()> {
        let root = TestDir::new()?;
        let baseline = root.path().join("baseline");
        let candidate = root.path().join("candidate");
        fs::create_dir(&baseline)?;
        fs::create_dir(&candidate)?;
        fs::write(baseline.join("metric.parquet"), b"same")?;
        fs::write(candidate.join("metric.parquet"), b"same")?;
        let report = root.path().join("determinism.json");
        run_with_ops(
            Cli {
                data_dir: root.path().join("data"),
                output_dir: root.path().join("output"),
                run_id: None,
                command: Commands::DeterminismVerify {
                    baseline_root: baseline.clone(),
                    candidate_root: candidate.clone(),
                    artifact_extension: "parquet".to_string(),
                    baseline_workers: 1,
                    candidate_workers: 2,
                    algorithm_version: "fixture-primary32-secondary8".to_string(),
                    report: report.clone(),
                },
            },
            &RecordingOps::default(),
        )
        .expect("identical artifacts from distinct worker counts must qualify");
        let value: Value = serde_json::from_slice(&fs::read(report)?)?;
        assert_eq!(value["baseline_workers"], 1);
        assert_eq!(value["candidate_workers"], 2);
        let rejected = run_with_ops(
            Cli {
                data_dir: root.path().join("data"),
                output_dir: root.path().join("output"),
                run_id: None,
                command: Commands::DeterminismVerify {
                    baseline_root: baseline,
                    candidate_root: candidate,
                    artifact_extension: "parquet".to_string(),
                    baseline_workers: 2,
                    candidate_workers: 2,
                    algorithm_version: "fixture-primary32-secondary8".to_string(),
                    report: root.path().join("rejected.json"),
                },
            },
            &RecordingOps::default(),
        );
        assert!(rejected.is_err());
        Ok(())
    }

    #[test]
    fn capacity_bench_cli_supports_optional_platform_quota() -> Result<()> {
        let cli = Cli::try_parse_from([
            "wiki-econ",
            "capacity-bench",
            "frwiki",
            "--weekly-buckets",
            "512",
            "--weekly-secondary-buckets",
            "32",
            "--scratch-dir",
            "/scratch",
            "--report",
            "/reports/frwiki-512.json",
            "--nfs-quota-bytes",
            "100000000000",
            "--quota-root",
            "/tool-root",
        ])?;
        assert!(matches!(
            cli.command,
            Commands::CapacityBench {
                wiki,
                weekly_buckets: 512,
                weekly_secondary_buckets: 32,
                scratch_dir,
                report,
                raw_transient_bytes: 33_285_996_544,
                nfs_quota_bytes: Some(100_000_000_000),
                storage_reserve_bytes: 53_687_091_200,
                quota_root,
                minimum_memory_headroom_percent: 25,
                requested_cpu: 1,
            } if wiki == "frwiki"
                && scratch_dir == Path::new("/scratch")
                && report.as_deref() == Some(Path::new("/reports/frwiki-512.json"))
                && quota_root.as_deref() == Some(Path::new("/tool-root"))
        ));

        let unquotaed = Cli::try_parse_from([
            "wiki-econ",
            "capacity-bench",
            "frwiki",
            "--weekly-buckets",
            "256",
            "--scratch-dir",
            "/scratch",
        ])?;
        assert!(matches!(
            unquotaed.command,
            Commands::CapacityBench {
                nfs_quota_bytes: None,
                ..
            }
        ));
        Ok(())
    }

    #[test]
    fn cpu_qualification_cli_requires_explicit_receipts_and_output() -> Result<()> {
        let cli = Cli::try_parse_from([
            "wiki-econ",
            "cpu-qualify",
            "--capacity-report",
            "/reports/nl.json",
            "--capacity-report",
            "/reports/pt.json",
            "--report",
            "/reports/qualification.json",
        ])?;
        assert!(matches!(
            cli.command,
            Commands::CpuQualify {
                capacity_reports,
                report,
            } if capacity_reports
                == vec![PathBuf::from("/reports/nl.json"), PathBuf::from("/reports/pt.json")]
                && report == Path::new("/reports/qualification.json")
        ));
        assert!(
            Cli::try_parse_from([
                "wiki-econ",
                "cpu-qualify",
                "--report",
                "/reports/qualification.json",
            ])
            .is_err()
        );
        Ok(())
    }

    #[test]
    fn snapshot_repair_cli_requires_an_explicit_generation() -> Result<()> {
        let cli = Cli::try_parse_from([
            "wiki-econ",
            "snapshot-repair",
            "nlwiki",
            "--version",
            "2026-07",
        ])?;
        assert!(matches!(
            cli.command,
            Commands::SnapshotRepair { wiki, version }
                if wiki == "nlwiki" && version == "2026-07"
        ));
        assert!(Cli::try_parse_from(["wiki-econ", "snapshot-repair", "nlwiki"]).is_err());
        Ok(())
    }

    #[test]
    fn snapshot_repair_command_validates_and_selects_the_generation() -> Result<()> {
        let data = TestDir::new()?;
        let output = TestDir::new()?;
        let wiki = "repairwiki";
        let version = "2026-07";
        let analytical = crate::storage::snapshot_analytical_wiki_dir(data.path(), wiki, version)?;
        crate::storage::write_test_marker_in(
            data.path(),
            &analytical,
            "2026-07.repairwiki.all-time",
        )
        .expect("repair marker should be writable");
        run_with_ops(
            Cli {
                data_dir: data.path().to_path_buf(),
                output_dir: output.path().to_path_buf(),
                run_id: None,
                command: Commands::SnapshotRepair {
                    wiki: wiki.to_string(),
                    version: version.to_string(),
                },
            },
            &RecordingOps::default(),
        )
        .expect("snapshot repair command should succeed");
        assert_eq!(
            crate::storage::current_snapshot_version(data.path(), wiki)?.as_deref(),
            Some(version)
        );
        Ok(())
    }

    #[test]
    fn real_capacity_benchmark_serializes_isolated_evidence() -> Result<()> {
        let data = TestDir::new()?;
        let output = TestDir::new()?;
        let scratch = TestDir::new()?;
        let reports = TestDir::new()?;
        let snapshot = "2026-01";
        let warehouse =
            crate::storage::snapshot_warehouse_wiki_dir(data.path(), "frwiki", snapshot)?;
        let partition = crate::storage::month_partition_dir(&warehouse, 2026, "2026-01");
        fs::create_dir_all(&partition)?;
        let analytical =
            crate::storage::snapshot_analytical_wiki_dir(data.path(), "frwiki", snapshot)
                .expect("capacity analytical generation should resolve");
        fs::create_dir_all(analytical).expect("capacity analytical generation should be created");
        let mut frame = DataFrame::new_infer_height(vec![
            Column::new("event_timestamp".into(), ["2026-01-05 00:00:00.0"]),
            Column::new("page_id".into(), [42_i64]),
            Column::new("page_namespace".into(), [0_i32]),
            Column::new("page_title".into(), ["Capacity"]),
        ])
        .expect("capacity command fixture");
        ParquetWriter::new(fs::File::create(partition.join("part.parquet"))?).finish(&mut frame)?;
        crate::storage::write_test_generation_manifest_from_files(data.path(), "frwiki", snapshot)?;
        crate::storage::publish_test_snapshot_pointer(data.path(), "frwiki", snapshot)?;
        let report_path = reports.path().join("frwiki.json");

        execute_capacity_benchmark(capacity::CapacityBenchmarkOptions {
            wiki: "frwiki",
            data_dir: data.path(),
            output_dir: output.path(),
            scratch_root: scratch.path(),
            quota_root: data.path(),
            report_path: &report_path,
            bucket_count: 256,
            secondary_bucket_count: 1,
            raw_transient_requirement_bytes: 0,
            nfs_quota_bytes: Some(1_000_000_000),
            storage_reserve_bytes: 0,
            minimum_memory_headroom_percent: 25,
            requested_cpu: 1,
            telemetry_override: Some(observability::MemorySnapshot {
                rss_bytes: Some(25),
                cgroup_current_bytes: Some(50),
                cgroup_peak_bytes: Some(50),
                cgroup_limit_bytes: Some(100),
            }),
        })?;
        assert!(report_path.is_file());

        let native_output = output.path().join("native");
        let native_report = reports.path().join("native.json");
        let _ = RealOps.capacity_benchmark(
            "frwiki",
            data.path(),
            &native_output,
            scratch.path(),
            &native_report,
            256,
            1,
            0,
            Some(1_000_000_000),
            0,
            data.path(),
            25,
            1,
        );
        Ok(())
    }

    #[test]
    fn run_with_ops_dispatches_capacity_benchmark() -> Result<()> {
        init_test_tracing();
        let cli = Cli::try_parse_from([
            "wiki-econ",
            "--data-dir",
            "dataset",
            "--output-dir",
            "capacity-out",
            "capacity-bench",
            "frwiki",
            "--weekly-buckets",
            "1024",
            "--weekly-secondary-buckets",
            "32",
            "--scratch-dir",
            "scratch",
            "--raw-transient-bytes",
            "31000000000",
            "--nfs-quota-bytes",
            "100000000000",
            "--storage-reserve-bytes",
            "40000000000",
            "--quota-root",
            "tool-root",
            "--minimum-memory-headroom-percent",
            "30",
        ])?;
        let ops = RecordingOps::default();

        run_with_ops(cli, &ops)?;

        assert_eq!(
            ops.calls.into_inner(),
            vec![
                "capacity:frwiki:dataset:capacity-out/capacity/frwiki/weekly-buckets-1024x32:scratch:capacity-out/capacity/frwiki/weekly-buckets-1024x32.json:1024x32:31000000000:100000000000:40000000000:tool-root:30:1"
            ]
        );
        Ok(())
    }

    #[test]
    fn run_with_ops_dispatches_cpu_qualification() -> Result<()> {
        let cli = Cli::try_parse_from([
            "wiki-econ",
            "cpu-qualify",
            "--capacity-report",
            "reports/nlwiki.json",
            "--capacity-report",
            "reports/ptwiki.json",
            "--report",
            "reports/cpu-qualification.json",
        ])?;
        let ops = RecordingOps::default();

        run_with_ops(cli, &ops)?;

        assert_eq!(
            ops.calls.into_inner(),
            vec![
                "cpu-qualification:reports/nlwiki.json,reports/ptwiki.json:reports/cpu-qualification.json"
            ]
        );
        Ok(())
    }

    #[test]
    fn run_with_ops_dispatches_schema_benchmark() -> Result<()> {
        let cli = Cli::try_parse_from([
            "wiki-econ",
            "--data-dir",
            "dataset",
            "schema-benchmark",
            "nlwiki",
            "ptwiki",
            "frwiki",
            "--scratch-dir",
            "scratch",
            "--report",
            "reports/schema.json",
        ])?;
        let ops = RecordingOps::default();

        run_with_ops(cli, &ops)?;

        assert_eq!(
            ops.calls.into_inner(),
            vec!["schema_benchmark:nlwiki,ptwiki,frwiki:dataset:scratch"]
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
            "--source-window-size",
            "3",
        ])?;
        let ops = RecordingOps::default();

        run_with_ops(cli, &ops)?;

        assert_eq!(
            ops.calls.into_inner(),
            vec![
                "source_window:frwiki:2025-12:dataset:3",
                "fetch_patrol:frwiki:dataset",
                "compute:frwiki:dataset:results",
                "compute_patrol:frwiki:dataset:results:false:_",
                "merge:results",
            ]
        );
        Ok(())
    }

    #[test]
    fn run_with_ops_stage_ingest_skips_compute_and_merge() -> Result<()> {
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
            "--source-window-size",
            "3",
            "--stage",
            "ingest",
        ])?;
        let ops = RecordingOps::default();

        run_with_ops(cli, &ops)?;

        assert_eq!(
            ops.calls.into_inner(),
            vec![
                "source_window:frwiki:2025-12:dataset:3",
                "fetch_patrol:frwiki:dataset",
            ]
        );
        Ok(())
    }

    #[test]
    fn run_with_ops_stage_compute_skips_fetch_ingest_and_cleanup() -> Result<()> {
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
            "--stage",
            "compute",
        ])?;
        let ops = RecordingOps::default();

        run_with_ops(cli, &ops)?;

        assert_eq!(
            ops.calls.into_inner(),
            vec![
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

        let now = chrono::TimeZone::with_ymd_and_hms(&chrono::Utc, 2026, 8, 22, 0, 0, 0).unwrap();
        assert!(ops.resolve_snapshot(&[], now, data_dir.path()).is_err());
        ops.persist_snapshot_plans(&["simplewiki".to_string()], "2026-07", data_dir.path())?;
        assert!(
            crate::snapshot_plan::plan_path(data_dir.path(), "simplewiki", "2026-07")?.is_file()
        );

        let raw_ingest_dir = data_dir.path().join("raw").join("ingestwiki");
        fs::create_dir_all(&raw_ingest_dir)?;
        write_bz2_dump(&raw_ingest_dir.join("2026-02.ingestwiki.all-time.tsv.bz2"))?;
        ops.ingest_wiki("ingestwiki", Some("2026-02"), data_dir.path())?;
        let active_fragments = crate::storage::active_fragment_files(
            data_dir.path(),
            "ingestwiki",
            crate::storage::GenerationLayer::MetricInput,
        )
        .expect("compacted ingest fragments should resolve");
        assert!(!active_fragments.is_empty());
        ops.finalize_snapshot("ingestwiki", data_dir.path())?;

        let schema_scratch = output_dir.path().join("schema-scratch");
        let schema_report = output_dir.path().join("schema-benchmark.json");
        let benchmark_wikis = ["ingestwiki".to_string()];
        let benchmark_result = ops.schema_benchmark(
            data_dir.path(),
            &schema_scratch,
            &schema_report,
            &benchmark_wikis,
            Some("schema-e2e"),
        );
        benchmark_result?;
        assert!(schema_report.is_file());

        let raw_legacy_dir = data_dir.path().join("raw").join("legacywiki");
        fs::create_dir_all(&raw_legacy_dir)?;
        write_bz2_dump(&raw_legacy_dir.join("legacy.tsv.bz2"))?;
        ops.ingest_wiki("legacywiki", None, data_dir.path())?;

        ops.cleanup_raw_dump("ingestwiki", data_dir.path())?;
        assert!(
            !raw_ingest_dir
                .join("2026-02.ingestwiki.all-time.tsv.bz2")
                .exists()
        );

        let fetch_err = ops
            .fetch_wiki("../enwiki", "2026-02", data_dir.path())
            .expect_err("unsafe wiki should fail before network work");
        assert!(fetch_err.to_string().contains("invalid wiki database name"));
        assert!(
            ops.prepare_wiki_snapshot("../enwiki", "2026-02", data_dir.path(), "test-run", 1)
                .is_err()
        );
        assert!(
            ops.prepare_candidate_snapshot(
                "../enwiki",
                "2026-02",
                data_dir.path(),
                "candidate-run",
                1,
            )
            .is_err()
        );
        assert!(
            ops.plan_candidate_preparation(
                "../enwiki",
                "2026-02",
                data_dir.path(),
                output_dir.path(),
                "candidate-run",
            )
            .is_err()
        );
        assert!(!ops.cached_patrol_sources_available("missingwiki", data_dir.path()));
        assert!(
            ops.compute_candidate("computewiki", "invalid", data_dir.path(), output_dir.path(),)
                .is_err()
        );

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
        let guard = crate::patrol::install_test_transport(fake_transport);
        ops.fetch_patrol("patrolwiki", data_dir.path())?;
        assert!(
            data_dir
                .path()
                .join("patrol")
                .join("patrolwiki")
                .join("patrol.parquet")
                .exists()
        );
        drop(guard);

        write_patrol_compute_input(data_dir.path(), "patrolcomputewiki")?;
        let result = ops.compute_patrol(
            "patrolcomputewiki",
            data_dir.path(),
            output_dir.path(),
            false,
            None,
        );
        result?;
        assert!(
            output_dir
                .path()
                .join("patrolcomputewiki")
                .join("patrol.parquet")
                .exists()
        );

        let patrol_snapshot = "2026-01";
        let legacy_warehouse =
            crate::storage::warehouse_wiki_dir(data_dir.path(), "patrolcomputewiki");
        let snapshot_warehouse = crate::storage::snapshot_warehouse_wiki_dir(
            data_dir.path(),
            "patrolcomputewiki",
            patrol_snapshot,
        )
        .expect("snapshot warehouse fixture should resolve");
        for path in crate::storage::collect_parquet_files(&legacy_warehouse)? {
            let destination = snapshot_warehouse.join(path.strip_prefix(&legacy_warehouse)?);
            destination.parent().map(fs::create_dir_all).transpose()?;
            fs::copy(path, destination)?;
        }
        crate::storage::write_test_generation_manifest_from_files(
            data_dir.path(),
            "patrolcomputewiki",
            patrol_snapshot,
        )
        .expect("patrol generation manifest should be writable");
        let snapshot_transport = Arc::new(FakePatrolTransport::new(
            vec![gzip_bytes(patrol_xml)?],
            vec![json!({"query": {"usergroups": []}})],
        ));
        let snapshot_guard = crate::patrol::install_test_transport(snapshot_transport);
        ops.fetch_patrol_for_snapshot("patrolsnapshotwiki", patrol_snapshot, data_dir.path())?;
        assert!(ops.cached_patrol_generation_available(
            "patrolsnapshotwiki",
            patrol_snapshot,
            data_dir.path()
        ));
        crate::storage::write_current_snapshot_pointer_for_test(
            data_dir.path(),
            "patrolsnapshotwiki",
            patrol_snapshot,
        )
        .expect("test snapshot pointer should be writable");
        ops.fetch_patrol("patrolsnapshotwiki", data_dir.path())?;
        drop(snapshot_guard);
        let patrol_candidate = output_dir.path().join("patrol-candidate");
        ops.compute_candidate_patrol(
            "patrolcomputewiki",
            patrol_snapshot,
            data_dir.path(),
            &patrol_candidate,
        )
        .expect("explicit patrol candidate should compute");
        assert!(
            patrol_candidate
                .join("patrolcomputewiki/patrol.parquet")
                .is_file()
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
        let rebuild_result = ops.compute_patrol(
            "patrolcomputewiki",
            data_dir.path(),
            output_dir.path(),
            true,
            None,
        );
        rebuild_result?;
        let stale_gone = !parts_dir.join("stale.parquet").exists();
        assert!(stale_gone);
        Ok(())
    }

    #[test]
    fn recording_ops_covers_compatibility_raw_cleanup() -> Result<()> {
        let ops = RecordingOps::default();
        ops.cleanup_raw_dump("testwiki", Path::new("dataset"))?;
        assert_eq!(ops.calls.into_inner(), vec!["cleanup_raw:testwiki:dataset"]);
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

        let error = ops
            .merge_outputs(output_dir.path(), None)
            .expect_err("real generators reject an incomplete metric fixture");

        assert!(output_dir.path().join("metric.parquet").exists());
        assert!(!error.to_string().is_empty());
        Ok(())
    }

    #[test]
    fn tracing_helpers_initialize_and_time_stages() -> Result<()> {
        init_test_tracing();
        init_tracing("unit-test");

        let value = run_timed_stage("unit", None, || Ok::<_, anyhow::Error>(7_u8))?;

        assert_eq!(value, 7);
        Ok(())
    }

    #[test]
    fn logging_context_prefers_explicit_run_ids_and_controls_ansi() {
        assert_eq!(
            resolve_logging_run_id(Some("cli-run"), Some("env-run"), 42),
            "cli-run"
        );
        assert_eq!(resolve_logging_run_id(None, Some("env-run"), 42), "env-run");
        assert_eq!(resolve_logging_run_id(None, None, 42), "standalone-42");
        assert_eq!(
            RunIdEventFormat::new("run id\n42", false).run_id,
            "run_id_42"
        );
        assert!(log_ansi_enabled_from(None));
        assert!(log_ansi_enabled_from(Some("1")));
        assert!(!log_ansi_enabled_from(Some("0")));
        assert!(!log_ansi_enabled_from(Some("false")));
        assert!(!log_ansi_enabled_from(Some("no")));
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
    fn run_with_ops_records_checks_and_invalidates_site_fingerprint() -> Result<()> {
        let data_dir = TestDir::new()?;
        let output_dir = TestDir::new()?;
        let workspace_dir = TestDir::new()?;
        let site_dir = workspace_dir.path().join("site");
        let dist_dir = TestDir::new()?;
        fs::create_dir_all(site_dir.join("src"))?;
        fs::create_dir_all(site_dir.join("data-build"))?;
        fs::write(output_dir.path().join("metric.json"), "{}")?;
        fs::write(
            output_dir.path().join(".publication-candidate.json"),
            r#"{"artifacts":[{"name":"metric.json"}]}"#,
        )
        .expect("candidate fixture should be written");
        fs::write(
            output_dir.path().join(publication::RECEIPT_FILE),
            r#"{"selected_snapshot_versions":{"nlwiki":"2026-07"}}"#,
        )
        .expect("gate fixture should be written");
        fs::write(site_dir.join("src/index.md"), "# Site")?;
        fs::write(site_dir.join("data-build/manifest.sh"), "true")?;
        fs::write(site_dir.join("observablehq.config.js"), "export default {}")
            .expect("site config fixture should be written");
        fs::write(site_dir.join("package.json"), "{}")?;
        fs::write(workspace_dir.path().join("package.json"), "{}")?;
        fs::write(workspace_dir.path().join("package-lock.json"), "{}")?;
        fs::write(dist_dir.path().join("index.html"), "published")?;
        let command = |command| Cli {
            data_dir: data_dir.path().to_path_buf(),
            output_dir: output_dir.path().to_path_buf(),
            run_id: None,
            command,
        };

        run_with_ops(
            command(Commands::SiteFingerprintRecord {
                site_dir: site_dir.clone(),
                dist_dir: dist_dir.path().to_path_buf(),
            }),
            &RecordingOps::default(),
        )
        .expect("site fingerprint should record");
        run_with_ops(
            command(Commands::SiteFingerprintCheck {
                site_dir: site_dir.clone(),
                dist_dir: dist_dir.path().to_path_buf(),
            }),
            &RecordingOps::default(),
        )
        .expect("unchanged site should be reusable");

        fs::write(site_dir.join("src/index.md"), "# Changed")?;
        assert!(
            run_with_ops(
                command(Commands::SiteFingerprintCheck {
                    site_dir,
                    dist_dir: dist_dir.path().to_path_buf(),
                }),
                &RecordingOps::default(),
            )
            .is_err()
        );
        Ok(())
    }

    #[test]
    fn artifact_scrub_cli_rehashes_and_records_published_artifacts() -> Result<()> {
        let output = TestDir::new()?;
        let artifact = output.path().join("gdp.parquet");
        let mut frame = df!(
            "year_month" => &["2026-07"],
            "total_edits" => &[3_u32],
            "wiki" => &["nlwiki"],
        )
        .expect("valid scrub CLI fixture");
        ParquetWriter::new(fs::File::create(&artifact)?).finish(&mut frame)?;
        artifact_receipt::scan_and_write(&artifact, "gdp.parquet", "test-v1", "input")?;
        let report = output.path().join("reports/scrub.json");
        let cli = Cli::try_parse_from([
            "wiki-econ",
            "--output-dir",
            output.path().to_str().context("UTF-8 output fixture")?,
            "artifact-scrub",
            "--report",
            report.to_str().context("UTF-8 report fixture")?,
        ])
        .expect("valid artifact-scrub CLI");
        run_with_ops(cli, &RecordingOps::default())?;
        let value: Value = serde_json::from_slice(&fs::read(report)?)?;
        assert_eq!(value["artifacts"].as_array().map(Vec::len), Some(1));

        let stdout_only_cli = Cli::try_parse_from([
            "wiki-econ",
            "--output-dir",
            output.path().to_str().context("UTF-8 output fixture")?,
            "artifact-scrub",
        ])
        .expect("valid stdout-only artifact-scrub CLI");
        run_with_ops(stdout_only_cli, &RecordingOps::default())?;

        let invalid_report_cli = Cli::try_parse_from([
            "wiki-econ",
            "--output-dir",
            output.path().to_str().context("UTF-8 output fixture")?,
            "artifact-scrub",
            "--report",
            output.path().to_str().context("UTF-8 report fixture")?,
        ])
        .expect("valid artifact-scrub failure CLI");
        assert!(run_with_ops(invalid_report_cli, &RecordingOps::default()).is_err());

        let empty = TestDir::new().expect("empty scrub fixture");
        let empty_cli = Cli::try_parse_from([
            "wiki-econ",
            "--output-dir",
            empty.path().to_str().expect("UTF-8 empty output"),
            "--run-id",
            "scrub-empty",
            "artifact-scrub",
        ])
        .expect("valid failing artifact-scrub CLI");
        assert!(run_with_ops(empty_cli, &RecordingOps::default()).is_err());
        let status: Value = serde_json::from_slice(
            &fs::read(empty.path().join("_scrubs/status.json"))
                .expect("failed scrub status should exist"),
        )
        .expect("failed scrub status should be valid JSON");
        assert_eq!(status["state"], "failed");
        assert_eq!(status["run_id"], "scrub-empty");
        Ok(())
    }

    #[test]
    fn run_with_ops_materializes_site_fixture_and_dashboard_json() -> Result<()> {
        let output_dir = TestDir::new()?;
        let command = |command| Cli {
            data_dir: PathBuf::from("unused"),
            output_dir: output_dir.path().to_path_buf(),
            run_id: None,
            command,
        };

        run_with_ops(command(Commands::SiteFixture), &RecordingOps::default())?;
        let first = fs::read(output_dir.path().join("defaults_gdp.json"))?;
        run_with_ops(
            command(Commands::DashboardMaterialize),
            &RecordingOps::default(),
        )
        .expect("dashboard fixture should rematerialize");
        assert_eq!(
            fs::read(output_dir.path().join("defaults_gdp.json"))?,
            first
        );
        run_with_ops(
            command(Commands::BrowserPerformanceFixture),
            &RecordingOps::default(),
        )
        .expect("browser performance fixture should materialize");
        let browser_index =
            browser_data::read_index(&output_dir.path().join(browser_data::INDEX_FILENAME))?;
        assert!(
            browser_index
                .entries
                .iter()
                .any(|entry| entry.wiki == "frwiki" && entry.rows == 21_000)
        );
        Ok(())
    }

    #[test]
    fn run_with_ops_dispatches_safe_stale_cleanup() -> Result<()> {
        let root = TestDir::new()?;
        let output = root.path().join("output");
        let site_dist = root.path().join("site/dist");
        fs::create_dir_all(&output)?;
        fs::create_dir_all(site_dist.parent().context("site parent")?)?;
        let abandoned = output.join(".metric.parquet.merge.dead-run.tmp");
        fs::write(&abandoned, b"partial")?;

        run_with_ops(
            Cli {
                data_dir: root.path().join("data"),
                output_dir: output,
                run_id: Some("current-run".to_string()),
                command: Commands::CleanupStale {
                    site_dist_dir: site_dist,
                    minimum_age_secs: 0,
                    scratch_dir: None,
                    capacity_dir: None,
                    wikis: Vec::new(),
                },
            },
            &RecordingOps::default(),
        )
        .expect("safe stale cleanup command should succeed");

        assert!(!abandoned.exists());
        Ok(())
    }

    #[test]
    fn run_with_ops_propagates_stale_cleanup_errors() -> Result<()> {
        let root = TestDir::new()?;
        let output = root.path().join("output");
        let site_dist = root.path().join("site/dist");
        let pointer = storage::snapshot_pointer_path(root.path(), "nlwiki");
        fs::create_dir_all(&output)?;
        fs::create_dir_all(site_dist.parent().context("site parent")?)?;
        pointer.parent().map(fs::create_dir_all).transpose()?;
        fs::write(pointer, b"invalid")?;

        let result = run_with_ops(
            Cli {
                data_dir: root.path().to_path_buf(),
                output_dir: output,
                run_id: Some("current-run".to_string()),
                command: Commands::CleanupStale {
                    site_dist_dir: site_dist,
                    minimum_age_secs: 0,
                    scratch_dir: None,
                    capacity_dir: None,
                    wikis: vec!["nlwiki".to_string()],
                },
            },
            &RecordingOps::default(),
        );

        assert!(result.is_err());
        Ok(())
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
    fn run_with_ops_propagates_candidate_discovery_errors() -> Result<()> {
        let cli = Cli::try_parse_from([
            "wiki-econ",
            "--run-id",
            "candidate-error",
            "prepare-wiki",
            "nlwiki",
            "--version",
            "2026-07",
        ])?;
        let ops = RecordingOps {
            preparation_error: true,
            ..RecordingOps::default()
        };
        let error =
            run_with_ops(cli, &ops).expect_err("candidate discovery failure should propagate");
        assert!(error.to_string().contains("candidate plan failed"));
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
    fn run_with_ops_propagates_capacity_benchmark_errors() -> Result<()> {
        init_test_tracing();
        let cli = Cli::try_parse_from([
            "wiki-econ",
            "capacity-bench",
            "frwiki",
            "--weekly-buckets",
            "256",
            "--scratch-dir",
            "scratch",
            "--nfs-quota-bytes",
            "100000000000",
        ])?;
        let error = run_with_ops(
            cli,
            &FailingOps {
                fail_stage: "capacity",
            },
        )
        .expect_err("capacity failure should propagate");
        assert!(error.to_string().contains("capacity benchmark failed"));
        Ok(())
    }

    #[test]
    fn run_with_ops_propagates_schema_benchmark_errors() -> Result<()> {
        init_test_tracing();
        let cli = Cli::try_parse_from([
            "wiki-econ",
            "schema-benchmark",
            "frwiki",
            "--scratch-dir",
            "scratch",
            "--report",
            "schema.json",
        ])?;
        let error = run_with_ops(
            cli,
            &FailingOps {
                fail_stage: "schema_benchmark",
            },
        )
        .expect_err("schema benchmark failure should propagate");
        assert!(error.to_string().contains("schema benchmark failed"));
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
        let qualification = TestDir::new()?;
        let data_dir = Path::new("data");
        let output_dir = Path::new("output");
        let wikis = vec!["frwiki".to_string()];

        ops.fetch_wiki("frwiki", "2026-02", data_dir)?;
        ops.fetch_patrol("frwiki", data_dir)?;
        ops.ingest_wiki("frwiki", None, data_dir)?;
        ops.cleanup_raw_dump("frwiki", data_dir)?;
        ops.compute_all("frwiki", data_dir, output_dir)?;
        ops.compute_patrol("frwiki", data_dir, output_dir, false, None)?;
        ops.benchmark(&wikis, data_dir, output_dir, 0, 1, false)?;
        ops.capacity_benchmark(
            "frwiki",
            data_dir,
            output_dir,
            Path::new("scratch"),
            Path::new("report.json"),
            256,
            1,
            0,
            Some(1),
            0,
            data_dir,
            25,
            1,
        )
        .expect("non-matching failing ops stage");
        let qualification_report = qualification.path().join("cpu.json");
        assert!(
            ops.cpu_qualification(&[], &qualification_report).is_err(),
            "an incomplete qualification matrix must fail closed"
        );
        assert!(qualification_report.is_file());
        let schema_result = ops.schema_benchmark(
            data_dir,
            Path::new("scratch"),
            Path::new("schema.json"),
            &wikis,
            Some("schema-test"),
        );
        schema_result?;
        ops.merge_outputs(output_dir, None)?;
        ops.finalize_snapshot("frwiki", data_dir)?;
        Ok(())
    }

    #[test]
    fn run_with_ops_propagates_snapshot_finalize_errors() -> Result<()> {
        init_test_tracing();
        let cli = Cli::try_parse_from(["wiki-econ", "snapshot-finalize", "frwiki"])?;
        let err = run_with_ops(
            cli,
            &FailingOps {
                fail_stage: "snapshot_finalize",
            },
        )
        .expect_err("snapshot finalization failure should propagate");
        assert!(err.to_string().contains("snapshot finalize failed"));
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
    fn run_command_propagates_cleanup_raw_errors() -> Result<()> {
        init_test_tracing();
        let cli = Cli::try_parse_from(["wiki-econ", "run", "frwiki"])?;
        let err = run_with_ops(
            cli,
            &FailingOps {
                fail_stage: "cleanup_raw",
            },
        )
        .expect_err("run cleanup_raw failure should propagate");
        assert!(err.to_string().contains("cleanup raw failed"));
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
