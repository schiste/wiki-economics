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
      browserData: {generation: "a".repeat(64), partitions: 9, rows: 1000, bytes: 1000, largestPartitionBytes: 500},
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
  assert.equal(result.summary.lastPublicationRunId, "run-current");
  assert.equal(successfulRuns(record, [record, {...record, state: "failed", exitCode: 1}]).length, 1);
});

test("a failed or malformed deep scrub is a publication-blocking alert", () => {
  const record = success();
  const failed = evaluateFreshness({
    last: record,
    history: [record],
    lifecycle,
    scrubStatus: {
      schema_version: 1,
      state: "failed",
      run_id: "scrub-20260826",
      updated_at_unix: 1_787_700_000,
      report_sha256: null,
      error: "semantic mismatch",
    },
    now: Date.parse("2026-08-22T03:00:00Z"),
  });
  assert.equal(failed.status, "critical");
  assert.equal(failed.alerts[0].code, "artifact_scrub_failed");
  assert.equal(failed.summary.artifactScrub.run_id, "scrub-20260826");

  const malformed = evaluateFreshness({
    last: record,
    history: [record],
    lifecycle,
    scrubStatus: {invalid: true},
    now: Date.parse("2026-08-22T03:00:00Z"),
  });
  assert.equal(malformed.alerts[0].code, "artifact_scrub_status_invalid");
});

test("a later site-only success retains the latest validated publication", () => {
  const publication = success({runId: "publish-1"});
  const siteOnly = success({
    runId: "site-2",
    startedAt: "2026-08-22T01:00:00Z",
    finishedAt: "2026-08-22T01:02:00Z",
    memoryPeakBytes: 100 * 1024 ** 2,
    memoryLimitBytes: 2 * 1024 ** 3,
    publication: null,
  });
  const result = evaluateFreshness({
    last: siteOnly,
    history: [publication, siteOnly],
    lifecycle,
    now: Date.parse("2026-08-22T03:00:00Z"),
  });
  assert.equal(result.status, "healthy");
  assert.deepEqual(result.alerts, []);
  assert.equal(result.summary.currentRunId, "site-2");
  assert.equal(result.summary.lastSuccessfulRunId, "site-2");
  assert.equal(result.summary.lastPublicationRunId, "publish-1");
  assert.equal(result.summary.lastPublicationAt, publication.finishedAt);
  assert.deepEqual(result.summary.publishedSnapshots, {nlwiki: "2026-07"});
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

test("incremental publication has a three-minute SLO", () => {
  const record = success({
    stageDurationsMs: {publication_prepare: 180_001},
    publication: {
      ...success().publication,
      changePlan: {
        changed: [{wiki: "nlwiki", family: "monthly"}],
        reused: [{wiki: "nlwiki", family: "page_week"}],
      },
    },
  });
  const result = evaluateFreshness({
    last: record,
    history: [record],
    lifecycle,
    now: Date.parse(record.finishedAt) + DAY_MS,
  });
  assert.equal(result.alerts[0].code, "incremental_publication_slow");
  assert.equal(result.alerts[0].changedFamilies, 1);
});

test("full baseline publication is not classified as incremental", () => {
  const record = success({
    stageDurationsMs: {publication_prepare: 1_109_480},
    publication: {
      ...success().publication,
      changePlan: {
        changed: [
          {wiki: "nlwiki", family: "monthly"},
          {wiki: "nlwiki", family: "page_week"},
        ],
        reused: [],
      },
    },
  });
  const result = evaluateFreshness({
    last: record,
    history: [record],
    lifecycle,
    now: Date.parse(record.finishedAt) + DAY_MS,
  });
  assert.ok(!result.alerts.some((alert) => alert.code === "incremental_publication_slow"));
});

test("browser publication size evidence is fail-closed and budgeted", () => {
  const missing = success({publication: {...success().publication, browserData: null}});
  let result = evaluateFreshness({last: missing, history: [], lifecycle, now: Date.parse(missing.finishedAt) + DAY_MS});
  assert.ok(result.alerts.some((alert) => alert.code === "browser_artifact_evidence_missing"));

  const oversized = success({publication: {...success().publication,
    browserData: {bytes: 201, largestPartitionBytes: 101}}});
  result = evaluateFreshness({last: oversized, history: [], lifecycle,
    now: Date.parse(oversized.finishedAt) + DAY_MS,
    thresholds: {maximumBrowserBytes: 200, maximumBrowserPartitionBytes: 100}});
  assert.deepEqual(new Set(result.alerts.map((alert) => alert.code)), new Set([
    "browser_artifact_total_exceeded", "browser_artifact_partition_exceeded",
  ]));
});
