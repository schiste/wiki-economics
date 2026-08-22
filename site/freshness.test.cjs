"use strict";

const assert = require("node:assert/strict");
const {test} = require("node:test");
const {DAY_MS, evaluateFreshness, successfulRuns} = require("./freshness.cjs");

const lifecycle = {
  wikis: {
    nlwiki: {publication: "published", refresh: "scheduled", freshness_sla_days: 10},
    frwiki: {publication: "published", refresh: "paused", freshness_sla_days: 10},
  },
};

function success(overrides = {}) {
  return {
    schemaVersion: 2,
    state: "succeeded",
    exitCode: 0,
    runId: "run-current",
    startedAt: "2026-08-21T02:00:00Z",
    finishedAt: "2026-08-21T03:00:00Z",
    selectedSnapshot: "2026-07",
    memoryPeakBytes: 2 * 1024 ** 3,
    memoryLimitBytes: 6 * 1024 ** 3,
    diskFreeBytes: 100 * 1024 ** 3,
    publication: {
      selectedSnapshots: {nlwiki: "2026-07"},
      cutoffDates: {nlwiki: "2026-08"},
      metrics: {patrol: {rows: 100}},
      patrolSources: {nlwiki: {patrol_events: 1000, rights_events: 10}},
    },
    ...overrides,
  };
}

test("a recent, advancing, resource-safe publication is healthy", () => {
  const record = success();
  const result = evaluateFreshness({last: record, history: [record], lifecycle, now: Date.parse("2026-08-22T03:00:00Z")});
  assert.equal(result.status, "healthy");
  assert.deepEqual(result.alerts, []);
  assert.equal(result.summary.publishedSnapshots.nlwiki, "2026-07");
  assert.equal(successfulRuns(record, [record, {...record, state: "failed", exitCode: 1}]).length, 1);
});

test("semantic publication and resource regressions become actionable alerts", () => {
  const previous = success({
    runId: "run-previous",
    finishedAt: "2026-07-01T03:00:00Z",
    selectedSnapshot: "2026-06",
    publication: {...success().publication, selectedSnapshots: {nlwiki: "2026-06"}, cutoffDates: {nlwiki: "2026-08"}},
  });
  const current = success({
    memoryPeakBytes: 5 * 1024 ** 3,
    diskFreeBytes: 10 * 1024 ** 3,
    publication: {
      selectedSnapshots: {nlwiki: "2026-07"},
      cutoffDates: {nlwiki: "2026-08"},
      metrics: {patrol: {rows: 0}},
      patrolSources: {nlwiki: {patrol_events: 0, rights_events: 0}},
    },
  });
  const result = evaluateFreshness({last: current, history: [previous, current], lifecycle, now: Date.parse("2026-09-05T03:00:00Z")});
  assert.equal(result.status, "critical");
  const codes = new Set(result.alerts.map((alert) => alert.code));
  for (const code of ["refresh_success_old", "output_cutoff_stalled", "patrol_output_zero", "patrol_source_zero", "memory_pressure", "disk_headroom_low"]) {
    assert.ok(codes.has(code), `missing ${code}`);
  }
});

test("live records detect stalled heartbeats, stages, and unpublished selections", () => {
  const previous = success();
  const running = {
    state: "running",
    runId: "run-next",
    selectedSnapshot: "2026-08",
    currentStage: "fetch",
    currentWiki: "nlwiki",
    heartbeatAt: "2026-08-22T02:00:00Z",
    stages: [{stage: "fetch", wiki: "nlwiki", state: "running", startedAt: "2026-08-22T01:00:00Z"}],
  };
  const result = evaluateFreshness({
    last: running,
    history: [previous],
    lifecycle,
    now: Date.parse("2026-08-22T03:00:00Z"),
    thresholds: {heartbeatStaleMs: 5 * 60 * 1000, stageLimitsMs: {fetch: 30 * 60 * 1000}},
  });
  assert.equal(result.status, "critical");
  assert.deepEqual(new Set(result.alerts.map((alert) => alert.code)), new Set([
    "heartbeat_stalled", "stage_runtime_exceeded", "selected_dump_unpublished",
  ]));
});

test("memory between 75 and 80 percent is a warning", () => {
  const record = success({memoryPeakBytes: 4.6 * 1024 ** 3});
  const result = evaluateFreshness({last: record, history: [], lifecycle, now: Date.parse(record.finishedAt) + DAY_MS});
  assert.equal(result.status, "warning");
  assert.equal(result.alerts[0].code, "memory_pressure");
  assert.equal(result.alerts[0].severity, "warning");
});
