"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const test = require("node:test");

const {
  buildOperationalTruth,
  completedSnapshots,
  expectedMetricsForWiki,
} = require("./admin-operational-truth.cjs");

function writeJson(file, value) {
  fs.mkdirSync(path.dirname(file), {recursive: true});
  fs.writeFileSync(file, `${JSON.stringify(value)}\n`, "utf8");
}

function fixture(t) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "wiki-econ-admin-truth-"));
  t.after(() => fs.rmSync(root, {recursive: true, force: true}));
  const dataDir = path.join(root, "data");
  const outputDir = path.join(root, "output");
  fs.mkdirSync(outputDir, {recursive: true});
  writeJson(path.join(root, "config", "generated", "metric-catalog.json"), {
    schema_version: 1,
    metrics: [
      {id: "gdp", family: "monthly", algorithm_version: "monthly-v1"},
      {id: "patrol", family: "patrol", algorithm_version: "patrol-v1"},
    ],
  });
  writeJson(path.join(root, "config", "toolforge-capacity.json"), {
    schema_version: 1,
    namespace_memory_limit_bytes: 8 * 1024 ** 3,
    resident_service_memory_bytes: 512 * 1024 ** 2,
    minimum_schedulable_job_bytes: 2 * 1024 ** 3,
    resource_requests: {
      small: 2 * 1024 ** 3,
      medium_large: 6 * 1024 ** 3,
      admin_dispatcher: 6 * 1024 ** 3,
      publisher: 6 * 1024 ** 3,
    },
    source: "test",
    verified_at: "2026-09-06",
  });
  writeJson(path.join(root, "config", "quality-policy.json"), {
    schema_version: 1,
    scrub_max_age_days: 14,
    default: {
      minimum_baseline: 100,
      decrease_warning_fraction: 0.01,
      decrease_critical_fraction: 0.1,
      increase_warning_fraction_per_month: 0.25,
      increase_critical_fraction_per_month: 1,
    },
    signals: {},
  });
  return {root, dataDir, outputDir};
}

function lifecycle() {
  return {
    schema_version: 1,
    publication_contract: {
      datasets: {
        gdp: {coverage: "all_published"},
        patrol: {wikis: ["nlwiki"]},
      },
    },
    wikis: {
      nlwiki: {publication: "published", refresh: "scheduled"},
      dewiki: {publication: "hidden", refresh: "qualification"},
    },
  };
}

function completedSnapshot(dataDir, wiki, snapshot) {
  const directory = path.join(dataDir, "snapshots", wiki, snapshot);
  writeJson(path.join(directory, "source-plan.json"), {schema_version: 1, wiki, snapshot, sources: []});
  writeJson(path.join(directory, "remote-inventory.json"), {schema_version: 1, wiki, snapshot, sources: []});
}

function readyIndex(outputDir, wiki, snapshot) {
  const reference = {
    candidate_relative: `_candidates/${wiki}/${snapshot}/run`,
    snapshot,
    run_id: "ready-run",
    core_family_receipt_identities: {monthly: "a".repeat(64)},
    patrol_receipt_identity: "b".repeat(64),
    ready_receipt_sha256: "c".repeat(64),
  };
  writeJson(path.join(outputDir, "_ready-index", `${wiki}.json`), {
    schema_version: 2,
    wiki,
    newest_valid_ready: reference,
    active_published: {...reference, snapshot: "2026-07"},
  });
}

test("operational truth separates a healthy publication from a failed newer candidate and quota pressure", (t) => {
  const {root, dataDir, outputDir} = fixture(t);
  const policy = lifecycle();
  completedSnapshot(dataDir, "nlwiki", "2026-07");
  completedSnapshot(dataDir, "nlwiki", "2026-08");
  // A plan without a completed remote inventory must not become "available".
  writeJson(path.join(dataDir, "snapshots", "nlwiki", "2026-09", "source-plan.json"), {
    schema_version: 1, wiki: "nlwiki", snapshot: "2026-09", sources: [],
  });
  readyIndex(outputDir, "nlwiki", "2026-08");
  writeJson(path.join(outputDir, "_candidate-status", "nlwiki.json"), {
    schemaVersion: 2,
    state: "failed",
    runId: "fleet-nlwiki-august",
    wikis: ["nlwiki"],
    selectedSnapshot: "2026-08",
    failingStage: "source_window",
    error: "workload profile Large has not completed production qualification",
    exitCode: 1,
    stages: [{stage: "snapshot_validate", state: "succeeded", durationMs: 93}],
    memoryPeakBytes: 1024,
    cpu: {usageUsec: 5000, throttledUsec: 0},
  });
  writeJson(path.join(outputDir, "publication-gate.json"), {
    schema_version: 8,
    run_id: "publish-july",
    selected_snapshot_versions: {nlwiki: "2026-07"},
    cutoff_dates: {nlwiki: "2026-08"},
    metrics: {
      gdp: {wikis: {nlwiki: {rows: 12, minimum_date: "2001-01", maximum_date: "2026-08"}}},
      patrol: {wikis: {nlwiki: {rows: 5, minimum_date: "2001-01", maximum_date: "2026-08"}}},
    },
  });
  writeJson(path.join(outputDir, "_scrubs", "status.json"), {
    schema_version: 1, state: "succeeded", run_id: "scrub-1", updated_at_unix: 1,
  });

  const result = buildOperationalTruth({
    root,
    dataDir,
    outputDir,
    lifecycle: policy,
    freshness: {status: "healthy", alerts: [], summary: {}},
    fleet: {work: [{wiki: "arwiki", state: "running", resourceClass: "medium_large"}]},
    adminOperations: {counts: {running: 0}},
    scheduledRefresh: {last: {state: "failed"}},
  });

  assert.equal(result.public.status, "healthy");
  assert.equal(result.public.gateValid, true);
  assert.equal(result.public.scrub.state, "succeeded");
  assert.equal(result.pipeline.status, "degraded");
  assert.match(result.pipeline.issues[0].message, /profile Large/);
  assert.equal(result.infrastructure.status, "constrained");
  assert.equal(result.infrastructure.availableRequestedBytes, 1536 * 1024 ** 2);
  assert.deepEqual(result.wikis.nlwiki.snapshots, {
    latestAvailable: "2026-08",
    candidate: "2026-08",
    ready: "2026-08",
    qualification: null,
    published: "2026-07",
    cutoff: "2026-08",
  });
  assert.equal(result.wikis.nlwiki.metrics.published.complete, true);
  assert.equal(result.wikis.nlwiki.metrics.candidate.complete, true);
  assert.equal(result.wikis.nlwiki.candidate.stages[0].durationMs, 93);
  assert.equal(result.wikis.nlwiki.candidate.remediationCode, "workload_profile_unqualified");
  assert.deepEqual(result.wikis.nlwiki.allowedActions, []);
});

test("metric completeness is exact and names every missing registered metric", (t) => {
  const {root, dataDir, outputDir} = fixture(t);
  const policy = lifecycle();
  completedSnapshot(dataDir, "nlwiki", "2026-07");
  writeJson(path.join(outputDir, "publication-gate.json"), {
    schema_version: 8,
    run_id: "publish-incomplete",
    selected_snapshot_versions: {nlwiki: "2026-07"},
    cutoff_dates: {nlwiki: "2026-08"},
    metrics: {
      gdp: {wikis: {nlwiki: {rows: 12, minimum_date: "2001-01", maximum_date: "2026-08"}}},
    },
  });

  const result = buildOperationalTruth({
    root, dataDir, outputDir, lifecycle: policy,
    freshness: {status: "critical", alerts: [], summary: {}},
    fleet: {work: []}, adminOperations: {counts: {}}, scheduledRefresh: {last: null},
  });

  assert.deepEqual(result.wikis.nlwiki.metrics.published.present, ["gdp"]);
  assert.deepEqual(result.wikis.nlwiki.metrics.published.missing, ["patrol"]);
  assert.equal(result.wikis.nlwiki.metrics.published.complete, false);
  assert.ok(result.pipeline.issues.some((issue) => issue.code === "published_metrics_incomplete"));
});

test("qualification projects expect the complete metric registry", () => {
  const definitions = [
    {id: "gdp", family: "monthly", algorithm_version: "v1"},
    {id: "page_weekly_edits", family: "page_week", algorithm_version: "v1"},
  ];
  assert.deepEqual(expectedMetricsForWiki(definitions, lifecycle(), "dewiki"), ["gdp", "page_weekly_edits"]);
});

test("a validated hidden qualification is first-class pipeline evidence", (t) => {
  const {root, dataDir, outputDir} = fixture(t);
  const result = buildOperationalTruth({
    root, dataDir, outputDir, lifecycle: lifecycle(),
    freshness: {status: "healthy", alerts: [], summary: {}},
    fleet: {work: []}, adminOperations: {counts: {}}, scheduledRefresh: {last: null},
    qualifications: {
      dewiki: [{
        wiki: "dewiki",
        snapshot: "2026-08",
        runId: "qualified-dewiki",
        qualifiedAtUnix: 1_788_526_634,
        artifactCount: 2,
        artifactBytes: 700_000_000,
        artifactRows: 118_000_000,
        metricIds: ["gdp", "patrol"],
        structurallyValid: true,
      }],
    },
  });

  assert.equal(result.wikis.dewiki.qualification.runId, "qualified-dewiki");
  assert.equal(result.wikis.dewiki.snapshots.qualification, "2026-08");
  assert.equal(result.wikis.dewiki.snapshots.candidate, "2026-08");
  assert.equal(result.wikis.dewiki.snapshots.latestAvailable, "2026-08");
  assert.equal(result.wikis.dewiki.metrics.candidate.complete, true);
  assert.equal(result.pipeline.counts.qualificationsReady, 1);
});

test("quarantined projects with the same root failure form one actionable blocker", (t) => {
  const {root, dataDir, outputDir} = fixture(t);
  const policy = lifecycle();
  for (const wiki of ["nlwiki", "dewiki"]) {
    writeJson(path.join(outputDir, "_candidate-status", `${wiki}.json`), {
      schemaVersion: 2,
      state: "failed",
      runId: `failed-${wiki}`,
      wikis: [wiki],
      selectedSnapshot: "2026-08",
      failingStage: "source_window",
      error: "workload profile Large has not completed production qualification",
      exitCode: 1,
    });
  }
  const fleet = {work: ["nlwiki", "dewiki"].map((wiki) => ({
    wiki, state: "quarantined", taskId: `${wiki}-task`, error: "retry_limit_exhausted",
  }))};
  const result = buildOperationalTruth({
    root, dataDir, outputDir, lifecycle: policy,
    freshness: {status: "healthy", alerts: [], summary: {}},
    fleet, adminOperations: {counts: {}}, scheduledRefresh: {last: null},
  });

  assert.equal(result.pipeline.blockerGroups.length, 1);
  assert.deepEqual(result.pipeline.blockerGroups[0].affectedWikis, ["dewiki", "nlwiki"]);
  assert.equal(result.pipeline.blockerGroups[0].code, "workload_profile_unqualified");
  assert.equal(result.pipeline.blockerGroups[0].retryable, false);
  assert.deepEqual(result.wikis.nlwiki.allowedActions, []);
});

test("latest completed snapshot requires both the plan and remote inventory", (t) => {
  const {dataDir} = fixture(t);
  completedSnapshot(dataDir, "nlwiki", "2026-07");
  writeJson(path.join(dataDir, "snapshots", "nlwiki", "2026-08", "source-plan.json"), {
    wiki: "nlwiki", snapshot: "2026-08",
  });
  assert.deepEqual(completedSnapshots(dataDir, "nlwiki"), ["2026-07"]);
});

test("publication preflight is current only while it names the indexed candidate", (t) => {
  const {root, dataDir, outputDir} = fixture(t);
  const policy = lifecycle();
  completedSnapshot(dataDir, "nlwiki", "2026-08");
  readyIndex(outputDir, "nlwiki", "2026-08");
  writeJson(path.join(outputDir, "publication-gate.json"), {
    schema_version: 8,
    run_id: "publish-july",
    selected_snapshot_versions: {nlwiki: "2026-07"},
    cutoff_dates: {nlwiki: "2026-08"},
    metrics: {
      gdp: {wikis: {nlwiki: {rows: 12, minimum_date: "2001-01", maximum_date: "2026-08"}}},
      patrol: {wikis: {nlwiki: {rows: 5, minimum_date: "2001-01", maximum_date: "2026-08"}}},
    },
  });
  writeJson(path.join(outputDir, "_admin", "publication-preflight.json"), {
    schema_version: 1,
    generated_at_unix: 1,
    eligible: true,
    wikis: [{wiki: "nlwiki", candidate_run_id: "ready-run"}],
    changed: [{wiki: "nlwiki", family: "monthly"}],
    reused: [],
  });
  const current = buildOperationalTruth({
    root, dataDir, outputDir, lifecycle: policy,
    freshness: {status: "healthy", alerts: [], summary: {}},
    fleet: {work: []}, adminOperations: {counts: {}}, scheduledRefresh: {last: null},
  });
  assert.equal(current.public.publication.preflight.current, true);
  current.wikis.nlwiki.ready.run_id = "different";

  writeJson(path.join(outputDir, "_ready-index", "nlwiki.json"), {
    ...readJsonFixture(path.join(outputDir, "_ready-index", "nlwiki.json")),
    newest_valid_ready: {
      ...readJsonFixture(path.join(outputDir, "_ready-index", "nlwiki.json")).newest_valid_ready,
      run_id: "different",
    },
  });
  const stale = buildOperationalTruth({
    root, dataDir, outputDir, lifecycle: policy,
    freshness: {status: "healthy", alerts: [], summary: {}},
    fleet: {work: []}, adminOperations: {counts: {}}, scheduledRefresh: {last: null},
  });
  assert.equal(stale.public.publication.preflight.current, false);
});

function readJsonFixture(file) {
  return JSON.parse(fs.readFileSync(file, "utf8"));
}
