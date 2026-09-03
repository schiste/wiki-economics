"use strict";

const assert = require("node:assert/strict");
const test = require("node:test");
const {stripAnsi, summarizeOperationLog} = require("./admin-operation-status.cjs");

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
    'run_id=test INFO resource governor source progress sample={"downloaded_bytes":400,"ingested_rows":12}',
  ].join("\n"));
  assert.equal(summary.progress.percent, 50);
  assert.equal(summary.progress.plannedBytes, 1000);
  assert.equal(summary.progress.reusedBytes, 100);
  assert.equal(summary.progress.completedBytes, 500);
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
