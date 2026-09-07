import assert from "node:assert/strict";
import test from "node:test";

import {
  applyAdminView,
  createAdaptivePoll,
  hasActiveAdminWork,
  normalizeAdminView,
  persistOperationReceipts,
  readOperationReceipts,
  reconcileOperationReceipts,
  summarizeOperatorStatus,
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
