"use strict";

const assert = require("node:assert/strict");
const test = require("node:test");
const {classifyError, stripAnsi, summarizeOperationLog} = require("./admin-operation-status.cjs");

test("operation summaries expose bounded source-window progress in human terms", () => {
  const log = [
    'run_id=test \u001b[32m INFO\u001b[0m selected completed Wikimedia snapshot version="2026-07" lag_months=2',
    'run_id=test INFO starting stage stage="source_window" wiki="dewiki"',
    'run_id=test INFO starting bounded source-window execution wiki="dewiki" snapshot="2026-07" planned_sources=26 reused_sources=2 pending_sources=24',
    'run_id=test INFO committed ingest source wiki="dewiki" source="2026-07.dewiki.2001" rows=2679',
    'run_id=test INFO committed ingest source wiki="dewiki" source="2026-07.dewiki.2002" rows=72193',
    'run_id=test INFO resource governor source progress sample={"downloaded_bytes":4853113,"ingested_rows":74872}',
    'run_id=test INFO starting source-window download wiki="dewiki" source="2026-07.dewiki.2003"',
  ].join("\n");

  const summary = summarizeOperationLog({}, log);
  assert.equal(summary.stage, "source_window");
  assert.equal(summary.stageLabel, "Downloading and ingesting history");
  assert.equal(summary.selectedSnapshot, "2026-07");
  assert.equal(summary.progress.totalSources, 26);
  assert.equal(summary.progress.completedSources, 4);
  assert.equal(summary.progress.percent, 15);
  assert.equal(summary.progress.currentSource, "2026-07.dewiki.2003");
  assert.equal(summary.progress.downloadedBytes, 4853113);
  assert.equal(summary.progress.ingestedRows, 74872);
  assert.equal(summary.recovery.resumable, true);
  assert.equal(summary.recovery.preservesValidatedTransactions, true);
  assert.doesNotMatch(stripAnsi(log), /\u001b/);
});

test("operation summaries preserve progress across bounded log tails", () => {
  const previous = {
    stage: "source_window",
    progress: {
      totalSources: 26,
      reusedSources: 0,
      completedSources: 18,
      completedSourceIds: ["source-2018"],
      downloadedBytes: 100,
      ingestedRows: 200,
    },
  };
  const tail = [
    'run_id=test INFO committed ingest source source="source-2019" rows=50',
    'run_id=test INFO resource governor source progress sample={"downloaded_bytes":150,"ingested_rows":250}',
  ].join("\n");
  const summary = summarizeOperationLog(previous, tail);
  assert.equal(summary.progress.totalSources, 26);
  assert.equal(summary.progress.completedSources, 18);
  assert.deepEqual(summary.progress.completedSourceIds, ["source-2018", "source-2019"]);
  assert.equal(summary.progress.downloadedBytes, 150);
  assert.equal(summary.progress.ingestedRows, 250);
});

test("operation summaries turn permanent identity failures into actionable explanations", () => {
  const summary = summarizeOperationLog({}, [
    'run_id=test INFO starting stage stage="compute" wiki="dewiki"',
    "Error: editor identity is unavailable: rows without event_user_id require event_user_text; rebuild the snapshot with the qualified metric-input schema",
  ].join("\n"));
  assert.equal(summary.stage, "compute");
  assert.match(summary.errorSummary, /Retrying unchanged inputs will fail again/);
  assert.match(summary.errorSummary, /compatible identity policy/);
  assert.equal(summary.retryable, false);
  assert.equal(summary.remediationCode, "editor_identity_unavailable");
  assert.match(summary.remediation, /explicitly acknowledge/);
});

test("operation summaries retain the requested snapshot when the bounded log has no resolver line", () => {
  const summary = summarizeOperationLog(
    {snapshot: "2026-07"},
    'run_id=test INFO starting stage stage=compute wiki="dewiki"',
  );

  assert.equal(summary.selectedSnapshot, "2026-07");
  assert.equal(summary.stage, "compute");
});

test("operation summaries use byte-weighted progress for uneven source files", () => {
  const summary = summarizeOperationLog({}, [
    'run_id=test INFO starting stage stage="source_window" wiki="dewiki"',
    'run_id=test INFO starting bounded source-window execution planned_sources=10 reused_sources=2 planned_bytes=1000 reused_bytes=100 pending_sources=8',
    'run_id=test INFO resource governor source progress sample={"memory":{"cgroup_current_bytes":200,"cgroup_peak_bytes":250},"scratch_bytes":30,"persistent_available_bytes":8000,"downloaded_bytes":400,"ingested_rows":12,"download_bytes_per_second":50,"ingest_rows_per_second":2}',
  ].join("\n"));
  assert.equal(summary.progress.percent, 50);
  assert.equal(summary.progress.plannedBytes, 1000);
  assert.equal(summary.progress.reusedBytes, 100);
  assert.equal(summary.progress.completedBytes, 500);
  assert.equal(summary.progress.downloadBytesPerSecond, 50);
  assert.equal(summary.progress.ingestRowsPerSecond, 2);
  assert.equal(summary.progress.etaSeconds, 10);
  assert.equal(summary.progress.memoryCurrentBytes, 200);
  assert.equal(summary.progress.memoryPeakBytes, 250);
  assert.equal(summary.progress.scratchBytes, 30);
  assert.equal(summary.progress.persistentAvailableBytes, 8000);
});

test("operation summaries include an in-flight source in byte progress and ETA", () => {
  const summary = summarizeOperationLog({}, [
    'run_id=test INFO starting stage stage="source_window" wiki="frwiki"',
    'run_id=test INFO starting bounded source-window execution planned_sources=4 reused_sources=0 planned_bytes=1000 reused_bytes=0 pending_sources=4',
    'run_id=test INFO resource governor source progress sample={"downloaded_bytes":200,"ingested_rows":12,"download_bytes_per_second":20}',
    'run_id=test INFO starting source-window download source="source-2"',
    'run_id=test INFO source download progress path="source-2" downloaded_bytes=300 expected_bytes=500 bytes_per_second=50',
  ].join("\n"));

  assert.equal(summary.progress.completedBytes, 500);
  assert.equal(summary.progress.percent, 50);
  assert.equal(summary.progress.currentSourceDownloadedBytes, 300);
  assert.equal(summary.progress.currentSourceExpectedBytes, 500);
  assert.equal(summary.progress.currentSourceBytesPerSecond, 50);
  assert.equal(summary.progress.etaSeconds, 10);
});

test("operation summaries distinguish incomplete logging dumps from defects", () => {
  const summary = summarizeOperationLog({}, [
    'run_id=test INFO starting stage stage="patrol_preflight" wiki="dewiki"',
    'Error: UPSTREAM_WAITING: Wikimedia logging dump 20260901 for dewiki/2026-08 is not complete (recombined=waiting, split=waiting); validated history transactions remain reusable',
  ].join("\n"));
  assert.equal(summary.stage, "patrol_preflight");
  assert.equal(summary.retryable, true);
  assert.equal(summary.remediationCode, "upstream_logging_waiting");
  assert.match(summary.errorSummary, /will not be downloaded again/);
});

test("a successful terminal receipt clears stale errors from earlier attempts", () => {
  const summary = summarizeOperationLog({
    state: "succeeded",
    exitCode: 0,
    error: "UPSTREAM_WAITING: Wikimedia logging dump 20260901 is not complete",
  }, [
    'run_id=test INFO starting stage stage="qualification_validate" wiki="dewiki"',
    "[finished state=succeeded exit=0]",
  ].join("\n"));

  assert.equal(summary.errorSummary, null);
  assert.equal(summary.rawError, null);
  assert.equal(summary.retryable, null);
  assert.equal(summary.remediationCode, null);
});

test("operation summaries stop retry loops for compute without patrol sources", () => {
  const summary = summarizeOperationLog({}, [
    'run_id=test INFO starting stage stage="patrol_compute" wiki="dewiki"',
    "Error: No patrol data for dewiki. Run `patrol-fetch` first.",
  ].join("\n"));
  assert.equal(summary.stage, "patrol_compute");
  assert.equal(summary.retryable, false);
  assert.equal(summary.remediationCode, "patrol_source_missing");
  assert.match(summary.remediation, /Patrol refresh/);
});

test("failure diagnoses prescribe only evidence-backed recovery paths", () => {
  assert.deepEqual(classifyError("workload profile Large has not completed production qualification"), {
    errorSummary: "The selected workload profile has not passed production qualification for this workload.",
    retryable: false,
    remediationCode: "workload_profile_unqualified",
    remediation: "Run the profile qualification with measured memory, scratch, duration, and deterministic-output evidence before retrying this project.",
  });
  assert.deepEqual(classifyError("Error: publication preflight is blocked"), {
    errorSummary: "Publication preflight rejected the current candidate set. The existing public generation remains unchanged.",
    retryable: false,
    remediationCode: "publication_preflight_blocked",
    remediation: "Open the publication workbench for the grouped blockers and change plan. Correct those candidate-level incompatibilities before running preflight again.",
  });
  assert.equal(classifyError("publication receipt and site generation disagree").remediationCode, "publication_evidence_mismatch");
  assert.equal(classifyError("worker lease heartbeat expired").remediationCode, "fleet_lease_stale");
  assert.equal(classifyError("automatic retries exhausted").remediationCode, "fleet_task_quarantined");
  assert.equal(classifyError("No space left on device").remediationCode, "storage_reserve_exhausted");
});
