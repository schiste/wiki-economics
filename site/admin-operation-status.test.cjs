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
});

test("operation summaries retain the requested snapshot when the bounded log has no resolver line", () => {
  const summary = summarizeOperationLog(
    {snapshot: "2026-07"},
    'run_id=test INFO starting stage stage=compute wiki="dewiki"',
  );

  assert.equal(summary.selectedSnapshot, "2026-07");
  assert.equal(summary.stage, "compute");
});
