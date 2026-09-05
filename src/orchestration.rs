//! Typed orchestration boundaries for the CLI workflows.
//!
//! Domain modules own the work. These traits describe only the capabilities
//! that multi-stage workflows need, keeping snapshot and publication safety
//! requirements explicit at compile time.

use anyhow::{Context, Result};
#[cfg(any(test, coverage))]
use chrono::Datelike;
use chrono::{DateTime, Utc};
use std::path::{Path, PathBuf};
use std::time::Instant;
use tracing::info;

use crate::{fleet, observability, publication, snapshot_plan, source_window};

#[derive(Clone, Copy, Debug)]
pub(crate) struct AppPaths<'a> {
    pub(crate) data: &'a Path,
    pub(crate) output: &'a Path,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct RunContext<'a> {
    pub(crate) paths: AppPaths<'a>,
    pub(crate) run_id: Option<&'a str>,
    pub(crate) now: DateTime<Utc>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PreparationMode {
    Candidate,
    Qualification,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct PrepareWikiRequest<'a> {
    pub(crate) wiki: &'a str,
    pub(crate) version: Option<&'a str>,
    pub(crate) source_window_size: Option<usize>,
    pub(crate) lifecycle: &'a Path,
    pub(crate) mode: PreparationMode,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct PipelineRunRequest<'a> {
    pub(crate) wikis: &'a [String],
    pub(crate) version: Option<&'a str>,
    pub(crate) source_window_size: Option<usize>,
    pub(crate) stage: crate::RunStage,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct BenchmarkRequest<'a> {
    pub(crate) wikis: &'a [String],
    pub(crate) paths: AppPaths<'a>,
    pub(crate) warmup: usize,
    pub(crate) iterations: usize,
    pub(crate) keep_outputs: bool,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct CapacityBenchmarkRequest<'a> {
    pub(crate) wiki: &'a str,
    pub(crate) data_dir: &'a Path,
    pub(crate) output_dir: &'a Path,
    pub(crate) scratch_dir: &'a Path,
    pub(crate) report_path: &'a Path,
    pub(crate) weekly_buckets: usize,
    pub(crate) weekly_secondary_buckets: usize,
    pub(crate) raw_transient_bytes: u64,
    pub(crate) nfs_quota_bytes: Option<u64>,
    pub(crate) storage_reserve_bytes: u64,
    pub(crate) quota_root: &'a Path,
    pub(crate) minimum_memory_headroom_percent: u8,
    pub(crate) requested_cpu: usize,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct SchemaBenchmarkRequest<'a> {
    pub(crate) data_dir: &'a Path,
    pub(crate) scratch_dir: &'a Path,
    pub(crate) report_path: &'a Path,
    pub(crate) wikis: &'a [String],
    pub(crate) run_id: Option<&'a str>,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct PatrolComputeRequest<'a> {
    pub(crate) wikis: &'a [String],
    pub(crate) rebuild: bool,
    pub(crate) limit_months: Option<usize>,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct AccountCreationRequest<'a> {
    pub(crate) wiki: &'a str,
    pub(crate) version: Option<&'a str>,
    pub(crate) destination: &'a Path,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct FleetDiscoveryRequest<'a> {
    pub(crate) lifecycle: &'a Path,
    pub(crate) queue_dir: &'a Path,
    pub(crate) snapshot: Option<&'a str>,
}

#[derive(Clone, Copy, Debug)]
struct CandidateSourceRequest<'a> {
    wiki: &'a str,
    version: &'a str,
    run_id: &'a str,
    window_size: usize,
}

#[derive(Clone, Copy, Debug)]
struct CandidateMetricRequest<'a> {
    wiki: &'a str,
    version: &'a str,
    destination: &'a Path,
    same_snapshot_candidate: bool,
    compute_reused: bool,
    patrol_reused: bool,
}

pub(crate) trait SnapshotOps {
    fn resolve_snapshot(
        &self,
        wikis: &[String],
        now: DateTime<Utc>,
        data_dir: &Path,
    ) -> Result<String>;

    fn persist_snapshot_plans(
        &self,
        wikis: &[String],
        version: &str,
        data_dir: &Path,
    ) -> Result<()>;

    fn validate_completed_snapshot(&self, wiki: &str, version: &str, data_dir: &Path)
    -> Result<()>;

    fn finalize_snapshot(&self, wiki: &str, data_dir: &Path) -> Result<()>;

    fn reset_obsolete_qualification_generation(
        &self,
        wiki: &str,
        version: &str,
        data_dir: &Path,
    ) -> Result<bool>;
}

pub(crate) trait HistoryInputOps {
    fn fetch_wiki(&self, wiki: &str, version: &str, data_dir: &Path) -> Result<()>;

    fn ingest_wiki(&self, wiki: &str, version: Option<&str>, data_dir: &Path) -> Result<()>;

    fn prepare_wiki_snapshot(
        &self,
        wiki: &str,
        version: &str,
        data_dir: &Path,
        run_id: &str,
        window_size: usize,
    ) -> Result<()>;

    fn prepare_candidate_snapshot(
        &self,
        wiki: &str,
        version: &str,
        data_dir: &Path,
        run_id: &str,
        window_size: usize,
    ) -> Result<()>;
}

pub(crate) trait PatrolOps {
    fn fetch_patrol(&self, wiki: &str, data_dir: &Path) -> Result<()>;

    fn fetch_patrol_for_snapshot(&self, wiki: &str, version: &str, data_dir: &Path) -> Result<()>;

    fn preflight_patrol_for_snapshot(
        &self,
        wiki: &str,
        version: &str,
        data_dir: &Path,
    ) -> Result<()>;

    fn cached_patrol_generation_available(
        &self,
        wiki: &str,
        version: &str,
        data_dir: &Path,
    ) -> bool;

    fn compute_patrol(
        &self,
        wiki: &str,
        data_dir: &Path,
        output_dir: &Path,
        rebuild: bool,
        limit_months: Option<usize>,
    ) -> Result<()>;

    fn compute_candidate_patrol(
        &self,
        wiki: &str,
        version: &str,
        data_dir: &Path,
        candidate_dir: &Path,
    ) -> Result<()>;

    fn build_account_creation_staging_report(
        &self,
        wiki: &str,
        version: &str,
        data_dir: &Path,
        destination: &Path,
    ) -> Result<()>;
}

pub(crate) trait MetricComputeOps {
    fn compute_all(&self, wiki: &str, data_dir: &Path, output_dir: &Path) -> Result<()>;

    fn compute_candidate(
        &self,
        wiki: &str,
        version: &str,
        data_dir: &Path,
        candidate_dir: &Path,
    ) -> Result<()>;
}

pub(crate) trait CandidateOps {
    fn plan_candidate_preparation(
        &self,
        wiki: &str,
        version: &str,
        data_dir: &Path,
        output_dir: &Path,
        run_id: &str,
    ) -> Result<publication::WikiPreparationPlan>;

    fn plan_qualification_preparation(
        &self,
        wiki: &str,
        version: &str,
        data_dir: &Path,
        output_dir: &Path,
        run_id: &str,
    ) -> Result<publication::WikiPreparationPlan>;

    fn mark_candidate_ready(
        &self,
        data_dir: &Path,
        output_dir: &Path,
        lifecycle: &Path,
        wiki: &str,
        version: &str,
        run_id: &str,
    ) -> Result<PathBuf>;

    fn ensure_qualification_wiki(&self, lifecycle: &Path, wiki: &str) -> Result<()>;

    fn mark_qualification_ready(
        &self,
        data_dir: &Path,
        output_dir: &Path,
        lifecycle: &Path,
        wiki: &str,
        version: &str,
        run_id: &str,
    ) -> Result<PathBuf>;
}

pub(crate) trait PublicationOps {
    fn merge_outputs(&self, output_dir: &Path, run_id: Option<&str>) -> Result<()>;

    fn prepare_ready_publication(
        &self,
        data_dir: &Path,
        output_dir: &Path,
        lifecycle: &Path,
        run_id: &str,
    ) -> Result<()>;

    fn commit_ready_publication(
        &self,
        data_dir: &Path,
        output_dir: &Path,
        run_id: &str,
    ) -> Result<()>;

    fn rollback_ready_publication(
        &self,
        data_dir: &Path,
        output_dir: &Path,
        lifecycle: &Path,
        run_id: &str,
    ) -> Result<()>;
}

pub(crate) trait QualificationOps {
    fn benchmark(&self, request: BenchmarkRequest<'_>) -> Result<()>;

    fn capacity_benchmark(&self, request: CapacityBenchmarkRequest<'_>) -> Result<()>;

    fn cpu_qualification(&self, capacity_reports: &[PathBuf], report: &Path) -> Result<()>;

    fn schema_benchmark(&self, request: SchemaBenchmarkRequest<'_>) -> Result<()>;
}

pub(crate) trait ApplicationOps:
    SnapshotOps
    + HistoryInputOps
    + PatrolOps
    + MetricComputeOps
    + CandidateOps
    + PublicationOps
    + QualificationOps
{
}

impl<T> ApplicationOps for T where
    T: SnapshotOps
        + HistoryInputOps
        + PatrolOps
        + MetricComputeOps
        + CandidateOps
        + PublicationOps
        + QualificationOps
{
}

pub(crate) fn timed_stage<T>(
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

pub(crate) fn record_reused_stage(stage: &str, wiki: Option<&str>) {
    observability::record_stage_started(stage, wiki);
    observability::record_stage_reused(stage, wiki);
    observability::record_stage_completed(stage, wiki, 0);
    info!(
        stage,
        wiki = wiki.unwrap_or("-"),
        "reused stage without execution"
    );
}

pub(crate) fn record_skipped_stage(stage: &str, wiki: Option<&str>) {
    observability::record_stage_started(stage, wiki);
    observability::record_stage_skipped(stage, wiki);
    observability::record_stage_completed(stage, wiki, 0);
    info!(stage, wiki = wiki.unwrap_or("-"), "skipped unneeded stage");
}

#[cfg(any(test, coverage))]
pub(crate) fn previous_month_snapshot(now: DateTime<Utc>) -> String {
    let current_month = now.month();
    let (year, month) = if current_month == 1 {
        (now.year() - 1, 12)
    } else {
        (now.year(), current_month - 1)
    };
    format!("{year:04}-{month:02}")
}

pub(crate) fn handle_snapshot_resolve(
    context: RunContext<'_>,
    ops: &impl SnapshotOps,
    wikis: &[String],
) -> Result<String> {
    let version = timed_stage("snapshot_resolve", None, || {
        ops.resolve_snapshot(wikis, context.now, context.paths.data)
    })?;
    ops.persist_snapshot_plans(wikis, &version, context.paths.data)?;
    Ok(version)
}

pub(crate) fn handle_fleet_discovery(
    context: RunContext<'_>,
    ops: &impl SnapshotOps,
    request: FleetDiscoveryRequest<'_>,
) -> Result<fleet::DiscoveryReport> {
    let controller_run_id = context
        .run_id
        .context("fleet discovery requires --run-id")?;
    let wikis = fleet::scheduled_wikis(request.lifecycle)?;
    let overrides = fleet::lifecycle_resource_overrides(request.lifecycle)?;
    let mut report = fleet::DiscoveryReport::default();
    for wiki in wikis {
        let version = match request.snapshot {
            Some(version) => version.to_string(),
            None => timed_stage("fleet_snapshot_resolve", Some(&wiki), || {
                ops.resolve_snapshot(std::slice::from_ref(&wiki), context.now, context.paths.data)
            })?,
        };
        ops.persist_snapshot_plans(std::slice::from_ref(&wiki), &version, context.paths.data)?;
        let (plan, _) =
            snapshot_plan::SnapshotPlan::load_or_resolve(context.paths.data, &wiki, &version)?;
        let classification = fleet::classify(
            context.paths.data,
            context.paths.output,
            &plan,
            overrides.get(&wiki).copied(),
        );
        let (resource_class, signals) = classification?;
        let enqueue = fleet::enqueue(
            request.queue_dir,
            &wiki,
            &version,
            resource_class,
            signals,
            controller_run_id,
        );
        report.merge(enqueue?);
    }
    Ok(report)
}

pub(crate) fn handle_fetch(
    context: RunContext<'_>,
    ops: &(impl SnapshotOps + HistoryInputOps + PatrolOps),
    wikis: &[String],
    requested_version: Option<&str>,
) -> Result<()> {
    let version = resolve_requested_snapshot(context, ops, wikis, requested_version)?;
    ops.persist_snapshot_plans(wikis, &version, context.paths.data)?;
    for wiki in wikis {
        timed_stage("patrol_preflight", Some(wiki), || {
            ops.preflight_patrol_for_snapshot(wiki, &version, context.paths.data)
        })?;
        timed_stage("fetch", Some(wiki), || {
            ops.fetch_wiki(wiki, &version, context.paths.data)
        })?;
        timed_stage("patrol_fetch", Some(wiki), || {
            ops.fetch_patrol_for_snapshot(wiki, &version, context.paths.data)
        })?;
    }
    Ok(())
}

pub(crate) fn handle_ingest(
    context: RunContext<'_>,
    ops: &impl HistoryInputOps,
    wikis: &[String],
    version: Option<&str>,
) -> Result<()> {
    for wiki in wikis {
        timed_stage("ingest", Some(wiki), || {
            ops.ingest_wiki(wiki, version, context.paths.data)
        })?;
    }
    Ok(())
}

pub(crate) fn handle_compute(
    context: RunContext<'_>,
    ops: &(impl MetricComputeOps + PatrolOps + PublicationOps),
    wikis: &[String],
) -> Result<()> {
    for wiki in wikis {
        timed_stage("compute", Some(wiki), || {
            ops.compute_all(wiki, context.paths.data, context.paths.output)
        })?;
        timed_stage("patrol_compute", Some(wiki), || {
            ops.compute_patrol(wiki, context.paths.data, context.paths.output, false, None)
        })?;
    }
    timed_stage("merge", None, || {
        ops.merge_outputs(context.paths.output, context.run_id)
    })
}

pub(crate) fn handle_prepare_wiki(
    context: RunContext<'_>,
    ops: &(impl SnapshotOps + HistoryInputOps + PatrolOps + MetricComputeOps + CandidateOps),
    request: PrepareWikiRequest<'_>,
) -> Result<PathBuf> {
    let run_id = context.run_id.with_context(|| match request.mode {
        PreparationMode::Candidate => "candidate preparation requires --run-id",
        PreparationMode::Qualification => "wiki qualification requires --run-id",
    })?;
    if request.mode == PreparationMode::Qualification {
        ops.ensure_qualification_wiki(request.lifecycle, request.wiki)?;
    }
    let wikis = [request.wiki.to_string()];
    let version = match request.version {
        Some(version) => {
            timed_stage("snapshot_validate", Some(request.wiki), || {
                ops.validate_completed_snapshot(request.wiki, version, context.paths.data)
            })?;
            version.to_string()
        }
        None => timed_stage("snapshot_resolve", Some(request.wiki), || {
            ops.resolve_snapshot(&wikis, context.now, context.paths.data)
        })?,
    };
    let source_window_size = source_window::configured_window_size(request.source_window_size)?;
    ops.persist_snapshot_plans(&wikis, &version, context.paths.data)?;

    let retired_obsolete_input = if request.mode == PreparationMode::Qualification {
        let reset =
            ops.reset_obsolete_qualification_generation(request.wiki, &version, context.paths.data);
        reset?
    } else {
        false
    };
    if retired_obsolete_input {
        record_skipped_stage("obsolete_input_retired", Some(request.wiki));
    }

    let destination = match request.mode {
        PreparationMode::Candidate => {
            publication::wiki_candidate_dir(context.paths.output, request.wiki, &version, run_id)?
        }
        PreparationMode::Qualification => {
            let qualification = publication::wiki_qualification_dir(
                context.paths.output,
                request.wiki,
                &version,
                run_id,
            );
            qualification?
        }
    };

    if request.mode == PreparationMode::Qualification {
        let source = CandidateSourceRequest {
            wiki: request.wiki,
            version: &version,
            run_id,
            window_size: source_window_size,
        };
        prepare_candidate_source(context, ops, source)?;
    }

    let discovery_stage = match request.mode {
        PreparationMode::Candidate => "candidate_discovery",
        PreparationMode::Qualification => "qualification_discovery",
    };
    let preparation = timed_stage(discovery_stage, Some(request.wiki), || {
        let plan = match request.mode {
            PreparationMode::Candidate => ops.plan_candidate_preparation(
                request.wiki,
                &version,
                context.paths.data,
                context.paths.output,
                run_id,
            ),
            PreparationMode::Qualification => ops.plan_qualification_preparation(
                request.wiki,
                &version,
                context.paths.data,
                context.paths.output,
                run_id,
            ),
        }?;
        if matches!(plan, publication::WikiPreparationPlan::NoOp { .. }) {
            observability::record_stage_reused(discovery_stage, Some(request.wiki));
        }
        Ok(plan)
    })?;

    let (same_snapshot_candidate, compute_reused, patrol_reused) = match preparation {
        publication::WikiPreparationPlan::Build {
            same_snapshot_candidate,
            compute_reused,
            patrol_reused,
        } => (same_snapshot_candidate, compute_reused, patrol_reused),
        publication::WikiPreparationPlan::NoOp { ready_path } => {
            if request.mode == PreparationMode::Qualification {
                anyhow::bail!(
                    "qualification preparation unexpectedly resolved as a publication no-op"
                );
            }
            info!(wiki = request.wiki, version, path = %ready_path.display(), "candidate preparation is a recorded no-op");
            return Ok(ready_path);
        }
    };

    if request.mode == PreparationMode::Candidate {
        let source = CandidateSourceRequest {
            wiki: request.wiki,
            version: &version,
            run_id,
            window_size: source_window_size,
        };
        prepare_candidate_source(context, ops, source)?;
    }
    let metrics = CandidateMetricRequest {
        wiki: request.wiki,
        version: &version,
        destination: &destination,
        same_snapshot_candidate,
        compute_reused,
        patrol_reused,
    };
    execute_candidate_metrics(context, ops, metrics)?;

    let validation_stage = match request.mode {
        PreparationMode::Candidate => "candidate_validate",
        PreparationMode::Qualification => "qualification_validate",
    };
    timed_stage(validation_stage, Some(request.wiki), || {
        match request.mode {
            PreparationMode::Candidate => ops.mark_candidate_ready(
                context.paths.data,
                context.paths.output,
                request.lifecycle,
                request.wiki,
                &version,
                run_id,
            ),
            PreparationMode::Qualification => ops.mark_qualification_ready(
                context.paths.data,
                context.paths.output,
                request.lifecycle,
                request.wiki,
                &version,
                run_id,
            ),
        }
    })
}

fn prepare_candidate_source(
    context: RunContext<'_>,
    ops: &impl HistoryInputOps,
    request: CandidateSourceRequest<'_>,
) -> Result<()> {
    timed_stage("source_window", Some(request.wiki), || {
        ops.prepare_candidate_snapshot(
            request.wiki,
            request.version,
            context.paths.data,
            request.run_id,
            request.window_size,
        )
    })
}

fn execute_candidate_metrics(
    context: RunContext<'_>,
    ops: &(impl PatrolOps + MetricComputeOps),
    request: CandidateMetricRequest<'_>,
) -> Result<()> {
    if request.compute_reused {
        record_reused_stage("compute", Some(request.wiki));
    } else {
        timed_stage("compute", Some(request.wiki), || {
            ops.compute_candidate(
                request.wiki,
                request.version,
                context.paths.data,
                request.destination,
            )
        })?;
    }
    if request.patrol_reused {
        record_reused_stage("patrol_preflight", Some(request.wiki));
        record_reused_stage("patrol_fetch", Some(request.wiki));
        record_reused_stage("patrol_compute", Some(request.wiki));
        return Ok(());
    }

    let patrol_cached =
        ops.cached_patrol_generation_available(request.wiki, request.version, context.paths.data);
    if patrol_cached {
        record_reused_stage("patrol_preflight", Some(request.wiki));
    } else {
        timed_stage("patrol_preflight", Some(request.wiki), || {
            ops.preflight_patrol_for_snapshot(request.wiki, request.version, context.paths.data)
        })?;
    }
    if request.same_snapshot_candidate && patrol_cached {
        record_skipped_stage("patrol_fetch", Some(request.wiki));
    } else {
        timed_stage("patrol_fetch", Some(request.wiki), || {
            ops.fetch_patrol_for_snapshot(request.wiki, request.version, context.paths.data)
        })?;
    }
    timed_stage("patrol_compute", Some(request.wiki), || {
        ops.compute_candidate_patrol(
            request.wiki,
            request.version,
            context.paths.data,
            request.destination,
        )
    })
}

fn resolve_requested_snapshot(
    context: RunContext<'_>,
    ops: &impl SnapshotOps,
    wikis: &[String],
    requested_version: Option<&str>,
) -> Result<String> {
    match requested_version {
        Some(version) => {
            for wiki in wikis {
                timed_stage("snapshot_validate", Some(wiki), || {
                    ops.validate_completed_snapshot(wiki, version, context.paths.data)
                })?;
            }
            Ok(version.to_string())
        }
        None => timed_stage("snapshot_resolve", None, || {
            ops.resolve_snapshot(wikis, context.now, context.paths.data)
        }),
    }
}

pub(crate) fn handle_pipeline_run(
    context: RunContext<'_>,
    ops: &(impl SnapshotOps + HistoryInputOps + PatrolOps + MetricComputeOps + PublicationOps),
    request: PipelineRunRequest<'_>,
) -> Result<()> {
    let version = resolve_requested_snapshot(context, ops, request.wikis, request.version)?;
    let source_window_size = source_window::configured_window_size(request.source_window_size)?;
    let source_window_run_id = context
        .run_id
        .map(str::to_string)
        .unwrap_or_else(|| format!("manual-{}", std::process::id()));
    ops.persist_snapshot_plans(request.wikis, &version, context.paths.data)?;
    let run = publication::begin_run(
        context.paths.output,
        context.run_id,
        request.wikis,
        Some(&version),
    );
    run?;
    for wiki in request.wikis {
        info!(wiki, stage = ?request.stage, "running pipeline stage");
        if request.stage.runs_ingest() {
            if ops.cached_patrol_generation_available(wiki, &version, context.paths.data) {
                record_reused_stage("patrol_preflight", Some(wiki));
            } else {
                timed_stage("patrol_preflight", Some(wiki), || {
                    ops.preflight_patrol_for_snapshot(wiki, &version, context.paths.data)
                })?;
            }
            timed_stage("source_window", Some(wiki), || {
                ops.prepare_wiki_snapshot(
                    wiki,
                    &version,
                    context.paths.data,
                    &source_window_run_id,
                    source_window_size,
                )
            })?;
            timed_stage("patrol_fetch", Some(wiki), || {
                ops.fetch_patrol_for_snapshot(wiki, &version, context.paths.data)
            })?;
        }
        if request.stage.runs_compute() {
            timed_stage("compute", Some(wiki), || {
                ops.compute_all(wiki, context.paths.data, context.paths.output)
            })?;
            timed_stage("patrol_compute", Some(wiki), || {
                ops.compute_patrol(wiki, context.paths.data, context.paths.output, false, None)
            })?;
        }
    }
    if request.stage.runs_compute() {
        timed_stage("merge", None, || {
            ops.merge_outputs(context.paths.output, context.run_id)
        })?;
    }
    Ok(())
}

pub(crate) fn handle_publication_prepare(
    context: RunContext<'_>,
    ops: &impl PublicationOps,
    lifecycle: &Path,
) -> Result<()> {
    let run_id = context
        .run_id
        .context("ready publication preparation requires --run-id")?;
    timed_stage("publication_prepare", None, || {
        ops.prepare_ready_publication(context.paths.data, context.paths.output, lifecycle, run_id)
    })
}

pub(crate) fn handle_publication_commit(
    context: RunContext<'_>,
    ops: &impl PublicationOps,
) -> Result<()> {
    let run_id = context
        .run_id
        .context("ready publication commit requires --run-id")?;
    timed_stage("publication_commit", None, || {
        ops.commit_ready_publication(context.paths.data, context.paths.output, run_id)
    })
}

pub(crate) fn handle_publication_rollback(
    context: RunContext<'_>,
    ops: &impl PublicationOps,
    lifecycle: &Path,
) -> Result<()> {
    let run_id = context
        .run_id
        .context("ready publication rollback requires --run-id")?;
    timed_stage("publication_rollback", None, || {
        ops.rollback_ready_publication(context.paths.data, context.paths.output, lifecycle, run_id)
    })
}

pub(crate) fn handle_merge(context: RunContext<'_>, ops: &impl PublicationOps) -> Result<()> {
    publication::begin_run(context.paths.output, context.run_id, &[], None)?;
    timed_stage("merge", None, || {
        ops.merge_outputs(context.paths.output, context.run_id)
    })
}

pub(crate) fn handle_snapshot_finalize(
    context: RunContext<'_>,
    ops: &impl SnapshotOps,
    wikis: &[String],
) -> Result<()> {
    for wiki in wikis {
        timed_stage("snapshot_finalize", Some(wiki), || {
            ops.finalize_snapshot(wiki, context.paths.data)
        })?;
    }
    Ok(())
}

pub(crate) fn handle_patrol_fetch(
    context: RunContext<'_>,
    ops: &impl PatrolOps,
    wikis: &[String],
) -> Result<()> {
    for wiki in wikis {
        timed_stage("patrol_fetch", Some(wiki), || {
            ops.fetch_patrol(wiki, context.paths.data)
        })?;
    }
    Ok(())
}

pub(crate) fn handle_patrol_refresh(
    context: RunContext<'_>,
    ops: &impl PatrolOps,
    request: PatrolComputeRequest<'_>,
) -> Result<()> {
    for wiki in request.wikis {
        timed_stage("patrol_fetch", Some(wiki), || {
            ops.fetch_patrol(wiki, context.paths.data)
        })?;
        compute_patrol(context, ops, wiki, request.rebuild, request.limit_months)?;
    }
    Ok(())
}

pub(crate) fn handle_patrol_compute(
    context: RunContext<'_>,
    ops: &impl PatrolOps,
    request: PatrolComputeRequest<'_>,
) -> Result<()> {
    for wiki in request.wikis {
        compute_patrol(context, ops, wiki, request.rebuild, request.limit_months)?;
    }
    Ok(())
}

fn compute_patrol(
    context: RunContext<'_>,
    ops: &impl PatrolOps,
    wiki: &str,
    rebuild: bool,
    limit_months: Option<usize>,
) -> Result<()> {
    timed_stage("patrol_compute", Some(wiki), || {
        ops.compute_patrol(
            wiki,
            context.paths.data,
            context.paths.output,
            rebuild,
            limit_months,
        )
    })
}

pub(crate) fn handle_account_creation(
    context: RunContext<'_>,
    ops: &impl PatrolOps,
    request: AccountCreationRequest<'_>,
) -> Result<()> {
    let snapshot = match request.version {
        Some(version) => version.to_string(),
        None => crate::storage::current_snapshot_version(context.paths.data, request.wiki)?
            .with_context(|| {
                format!(
                    "account creation requires a selected snapshot for {}",
                    request.wiki
                )
            })?,
    };
    timed_stage("account_creation_extract", Some(request.wiki), || {
        ops.build_account_creation_staging_report(
            request.wiki,
            &snapshot,
            context.paths.data,
            request.destination,
        )
    })
}

pub(crate) fn handle_benchmark(
    ops: &impl QualificationOps,
    request: BenchmarkRequest<'_>,
) -> Result<()> {
    timed_stage("bench", None, || ops.benchmark(request))
}

pub(crate) fn handle_capacity_benchmark(
    ops: &impl QualificationOps,
    request: CapacityBenchmarkRequest<'_>,
) -> Result<()> {
    timed_stage("capacity_benchmark", Some(request.wiki), || {
        ops.capacity_benchmark(request)
    })
}

pub(crate) fn handle_cpu_qualification(
    ops: &impl QualificationOps,
    capacity_reports: &[PathBuf],
    report: &Path,
) -> Result<()> {
    timed_stage("cpu_qualification", None, || {
        ops.cpu_qualification(capacity_reports, report)
    })
}

pub(crate) fn handle_schema_benchmark(
    ops: &impl QualificationOps,
    request: SchemaBenchmarkRequest<'_>,
) -> Result<()> {
    timed_stage("schema_benchmark", None, || ops.schema_benchmark(request))
}
