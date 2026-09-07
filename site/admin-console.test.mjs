import assert from "node:assert/strict";
import test from "node:test";

import {
  applyAdminView,
  createAdaptivePoll,
  deriveProjectPipelineStages,
  hasActiveAdminWork,
  normalizeAdminView,
  persistOperationReceipts,
  readOperationReceipts,
  reconcileOperationReceipts,
  summarizeOperatorStatus,
  summarizePipelineIssues,
  summarizePublicationBlockers,
  upsertOperationReceipt
} from "./src/components/admin-console.js";

function memoryStorage(initial = {}) {
  const values = new Map(Object.entries(initial));
  return {
    getItem: (key) => values.get(key) ?? null,
    setItem: (key, value) => values.set(key, value),
    value: (key) => values.get(key)
  };
}

test("operation receipts persist, survive reload, and reconcile with server truth", () => {
  const storage = memoryStorage();
  const key = "receipts";
  const queued = upsertOperationReceipt([], {
    id: "request:admin-1",
    requestId: "admin-1",
    action: "qualify",
    wiki: "dewiki",
    state: "queued",
    title: "Qualification queued",
    recordedAt: "2026-09-06T10:00:00Z",
    updatedAt: "2026-09-06T10:00:00Z"
  });
  assert.equal(persistOperationReceipts(queued, storage, key), true);
  assert.deepEqual(readOperationReceipts(storage, key), queued);

  const completed = reconcileOperationReceipts(queued, {
    adminOperations: {
      running: [],
      queued: [],
      recent: [{
        requestId: "admin-1",
        action: "qualify",
        wiki: "dewiki",
        state: "succeeded",
        requestedAt: "2026-09-06T10:00:00Z",
        finishedAt: "2026-09-06T10:42:00Z",
        updatedAt: "2026-09-06T10:42:00Z",
        stageLabel: "Candidate ready"
      }]
    }
  });
  assert.equal(completed.length, 1);
  assert.equal(completed[0].state, "succeeded");
  assert.equal(completed[0].recordedAt, "2026-09-06T10:00:00Z");
  assert.equal(completed[0].detail, "Candidate ready");
});

test("malformed storage is ignored and unavailable storage does not break operations", () => {
  const corrupt = memoryStorage({receipts: "not json"});
  assert.deepEqual(readOperationReceipts(corrupt, "receipts"), []);
  assert.equal(persistOperationReceipts([], {setItem() { throw new Error("quota"); }}, "receipts"), false);
});

test("focused views reveal only their own sections and reject unknown route values", () => {
  function element(view) {
    return {
      dataset: view ? {adminView: view} : {adminViewTab: ""},
      hidden: false,
      tabIndex: 0,
      attributes: {},
      setAttribute(name, value) { this.attributes[name] = value; }
    };
  }
  const sections = [element("overview"), element("wikis"), element("runs"), element("quality")];
  const tabs = ["overview", "wikis", "runs", "quality"].map((view) => {
    const tab = element(null);
    tab.dataset.adminViewTab = view;
    return tab;
  });
  const root = {querySelectorAll: (selector) => selector === "[data-admin-view]" ? sections : tabs};

  assert.equal(normalizeAdminView("unknown"), "overview");
  assert.equal(applyAdminView(root, "quality"), "quality");
  assert.deepEqual(sections.map((section) => section.hidden), [true, true, true, false]);
  assert.deepEqual(tabs.map((tab) => tab.attributes["aria-selected"]), ["false", "false", "false", "true"]);
  assert.deepEqual(tabs.map((tab) => tab.tabIndex), [-1, -1, -1, 0]);
});

test("one adaptive poll serializes refreshes and changes cadence with workload and visibility", async () => {
  const timers = [];
  let active = true;
  let visible = true;
  let inFlight = 0;
  let maximumInFlight = 0;
  let release;
  const poller = createAdaptivePoll({
    poll: () => {
      inFlight += 1;
      maximumInFlight = Math.max(maximumInFlight, inFlight);
      return new Promise((resolve) => {
        release = () => {
          inFlight -= 1;
          resolve();
        };
      });
    },
    isActive: () => active,
    isVisible: () => visible,
    setTimer: (callback, delay) => {
      const timer = {callback, delay, cleared: false};
      timers.push(timer);
      return timer;
    },
    clearTimer: (timer) => { timer.cleared = true; },
    intervals: {active: 1000, idle: 5000, hidden: 30000, errorMaximum: 60000}
  });

  const first = poller.start();
  await Promise.resolve();
  const duplicate = poller.refresh();
  assert.equal(first, duplicate);
  assert.equal(inFlight, 1);
  release();
  await first;
  assert.equal(maximumInFlight, 1);
  assert.equal(timers.at(-1).delay, 0, "an explicit refresh during a request runs immediately afterward");

  active = false;
  assert.equal(poller.state().nextDelay, 5000);
  visible = false;
  assert.equal(poller.state().nextDelay, 30000);
  poller.stop();
});

test("active-work classification covers direct, queued, and fleet execution", () => {
  assert.equal(hasActiveAdminWork({adminOperations: {counts: {queued: 1}}}), true);
  assert.equal(hasActiveAdminWork({fleet: {counts: {running: 1}}}), true);
  assert.equal(hasActiveAdminWork({job: {running: true}}), true);
  assert.equal(hasActiveAdminWork({adminOperations: {counts: {queued: 0, running: 0}}}), false);
});

test("operator summary counts shared causes and qualification decisions instead of alarming per wiki", () => {
  const summary = summarizeOperatorStatus({
    adminOperations: {
      counts: {queued: 2},
      running: [{requestId: "request-1", wiki: "dewiki"}],
      queued: [{state: "waiting_upstream", wiki: "nlwiki"}],
      recent: [],
    },
    fleet: {
      work: [
        {taskId: "same-run", wiki: "dewiki", state: "running"},
        {taskId: "fleet-2", wiki: "frwiki", state: "running"},
      ],
    },
    operationalTruth: {
      public: {status: "healthy"},
      pipeline: {
        status: "degraded",
        blockerGroups: [{code: "profile", affectedWikis: ["nlwiki", "frwiki"], summary: "One profile is unqualified"}],
      },
      infrastructure: {status: "available"},
      wikis: {
        dewiki: {
          lifecycle: {publication: "hidden", refresh: "qualification"},
          qualification: {structurallyValid: true, snapshot: "2026-08", runId: "qualified-dewiki", artifactCount: 10},
        },
      },
    },
  });

  assert.equal(summary.publicStatus, "healthy");
  assert.equal(summary.activeCount, 3);
  assert.equal(summary.queuedCount, 2);
  assert.equal(summary.waitingUpstreamCount, 1);
  assert.equal(summary.decisionCount, 2, "one shared blocker and one qualification are two decisions");
  assert.deepEqual(summary.blockedWikis, ["frwiki", "nlwiki"]);
  assert.deepEqual(summary.qualificationReady, [{
    wiki: "dewiki", snapshot: "2026-08", runId: "qualified-dewiki", artifactCount: 10,
  }]);
});

test("stage ledger keeps an older healthy publication separate from a blocked update", () => {
  const stages = deriveProjectPipelineStages({
    lifecycle: {publication: "published", refresh: "scheduled"},
    candidate: {
      state: "failed",
      selectedSnapshot: "2026-08",
      failingStage: "source_window",
      retryable: false,
      remediation: "Qualify the Large profile first.",
      stages: [
        {stage: "snapshot_validate", state: "succeeded", durationMs: 91},
        {stage: "candidate_discovery", state: "succeeded", durationMs: 12},
        {stage: "source_window", state: "failed", durationMs: 13},
      ],
    },
    truth: {
      snapshots: {candidate: "2026-08", published: "2026-07"},
      metrics: {candidate: {complete: false}, published: {complete: true}},
    },
    manifestWiki: {raw: {files: 0}, parquet: {done: 0, total: 0}, patrol: {}},
  });

  assert.deepEqual(stages.map((stage) => [stage.id, stage.status]), [
    ["snapshot", "complete"],
    ["source", "blocked"],
    ["ingest", "waiting"],
    ["metrics", "waiting"],
    ["patrol_source", "waiting"],
    ["patrol_metrics", "waiting"],
    ["validation", "waiting"],
    ["publication", "waiting"],
  ]);
  assert.equal(stages[1].actionAllowed, false);
  assert.match(stages[1].blockedReason, /Qualify the Large profile/);
  assert.equal(stages[7].publishedSnapshot, "2026-07");
});

test("stage ledger makes promotion the next safe action for a validated private qualification", () => {
  const stages = deriveProjectPipelineStages({
    lifecycle: {publication: "hidden", refresh: "qualification"},
    candidate: {
      state: "succeeded",
      selectedSnapshot: "2026-08",
      stages: [
        {stage: "source_window", state: "succeeded", durationMs: 100},
        {stage: "compute", state: "succeeded", durationMs: 200},
        {stage: "patrol_fetch", state: "succeeded", durationMs: 30},
        {stage: "patrol_compute", state: "succeeded", durationMs: 40},
        {stage: "qualification_validate", state: "succeeded", durationMs: 5},
      ],
    },
    truth: {
      snapshots: {qualification: "2026-08", published: null},
      qualification: {structurallyValid: true},
      metrics: {candidate: {complete: true}, published: {complete: false}},
    },
    manifestWiki: {snapshot: {ready: true}, ingest: {ready: true}, patrol: {source_ready: 1, metric_ready: 1}},
  });

  assert.equal(stages.slice(0, 7).every((stage) => stage.status === "complete"), true);
  assert.equal(stages[7].status, "ready");
  assert.equal(stages[7].actionAllowed, true);
  assert.equal(stages[7].blockedReason, null);
});

test("publication blocker summary groups repeated schema mismatches", () => {
  const summary = summarizePublicationBlockers([
    "gdp candidates have incompatible merge schemas or algorithm versions between afwiki and frwiki",
    "inequality candidates have incompatible merge schemas or algorithm versions between afwiki and frwiki",
    "publication recovery audit is not clean",
  ]);
  assert.equal(summary.length, 2);
  assert.deepEqual(summary[0].metrics, ["gdp", "inequality"]);
  assert.match(summary[0].detail, /2 metrics differ/);
  assert.equal(summary[1].detail, "publication recovery audit is not clean");
});

test("overview issue summary collapses symptoms and omits snapshot noise covered by a blocker", () => {
  const summary = summarizePipelineIssues([
    {wiki: "frwiki", code: "candidate_failed", severity: "critical", message: "frwiki failed"},
    {wiki: "frwiki", code: "snapshot_pending", severity: "warning", message: "frwiki is pending"},
    {wiki: "afwiki", code: "candidate_ready_not_published", severity: "warning", message: "afwiki ready"},
    {wiki: "eswiki", code: "candidate_ready_not_published", severity: "warning", message: "eswiki ready"},
    {wiki: "afwiki", code: "unexpected_rows_change", severity: "warning", message: "afwiki rows rose"},
    {wiki: "eswiki", code: "artifact_scrub_stale", severity: "warning", message: "eswiki scrub stale"},
  ], [{affectedWikis: ["frwiki"]}]);

  assert.deepEqual(summary.map((group) => [group.code, group.count, group.affectedWikis]), [
    ["candidate_ready_not_published", 2, ["afwiki", "eswiki"]],
    ["data_quality_findings", 2, ["afwiki", "eswiki"]],
  ]);
  assert.match(summary[1].detail, /not separate pipeline failures/);
});
