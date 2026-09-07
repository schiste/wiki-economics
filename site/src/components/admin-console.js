const RECEIPT_SCHEMA_VERSION = 1;
const DEFAULT_RECEIPT_KEY = "wiki-economics.admin.operation-receipts.v1";

export const ADMIN_VIEWS = Object.freeze([
  {id: "overview", label: "Control room", description: "What is happening and what needs a decision"},
  {id: "wikis", label: "Projects", description: "Start work and manage project lifecycle"},
  {id: "runs", label: "Runs & logs", description: "Active work, run evidence, and operator audit"},
  {id: "quality", label: "Data quality", description: "Metric receipts, anomalies, and merged outputs"}
]);

export function summarizeOperatorStatus(status = {}) {
  const operations = status.adminOperations || {};
  const fleetWork = status.fleet?.work || [];
  const truth = status.operationalTruth || {};
  const wikis = truth.wikis || {};
  const blockerGroups = truth.pipeline?.blockerGroups || [];
  const activeIds = new Set([
    ...(operations.running || []).map((operation) => `operator:${operation.runId || operation.requestId}`),
    ...fleetWork.filter((work) => work.state === "running")
      .map((work) => `fleet:${work.taskId || work.wiki}`),
    ...(status.job?.running ? [`direct:${status.job.runId || status.job.wiki || "global"}`] : [])
  ]);
  const qualificationReady = Object.entries(wikis)
    .filter(([, wiki]) => wiki.lifecycle?.publication === "hidden"
      && wiki.lifecycle?.refresh === "qualification"
      && wiki.qualification?.structurallyValid)
    .map(([wiki, value]) => ({
      wiki,
      snapshot: value.qualification.snapshot || value.snapshots?.qualification || null,
      runId: value.qualification.runId || null,
      artifactCount: Number(value.qualification.artifactCount || 0)
    }))
    .sort((left, right) => left.wiki.localeCompare(right.wiki));
  const blockedWikis = Array.from(new Set(blockerGroups.flatMap((group) => group.affectedWikis || []))).sort();
  const queued = Number(operations.counts?.queued ?? (operations.queued || []).length);
  const waitingUpstream = [...(operations.queued || []), ...(operations.recent || [])]
    .filter((operation) => operation.state === "waiting_upstream").length;

  return {
    publicStatus: truth.public?.status || status.freshness?.status || "unknown",
    pipelineStatus: truth.pipeline?.status || "unknown",
    infrastructureStatus: truth.infrastructure?.status || "unknown",
    activeCount: activeIds.size,
    queuedCount: queued,
    waitingUpstreamCount: waitingUpstream,
    blockerGroups,
    blockedWikis,
    qualificationReady,
    decisionCount: blockerGroups.length + qualificationReady.length,
  };
}

export function normalizeAdminView(value) {
  return ADMIN_VIEWS.some((view) => view.id === value) ? value : ADMIN_VIEWS[0].id;
}

export function applyAdminView(root, value) {
  const view = normalizeAdminView(value);
  for (const section of root.querySelectorAll("[data-admin-view]")) {
    const views = (section.dataset.adminView || "").split(/\s+/).filter(Boolean);
    section.hidden = !views.includes(view);
  }
  for (const tab of root.querySelectorAll("[data-admin-view-tab]")) {
    const selected = tab.dataset.adminViewTab === view;
    tab.setAttribute("aria-selected", String(selected));
    tab.tabIndex = selected ? 0 : -1;
  }
  return view;
}

export function createAdminViewNavigation({root = document, location = globalThis.location,
  history = globalThis.history, initialView = null, onChange = null} = {}) {
  const requested = initialView || new URL(location?.href || "http://localhost/admin").searchParams.get("view");
  let current = normalizeAdminView(requested);
  const navigation = root.createElement("nav");
  navigation.className = "admin-view-navigation";
  navigation.setAttribute("aria-label", "Admin views");
  navigation.setAttribute("role", "tablist");

  function select(value, {focus = false, updateUrl = true} = {}) {
    current = applyAdminView(root, value);
    if (updateUrl && history && location) {
      const url = new URL(location.href);
      if (current === ADMIN_VIEWS[0].id) url.searchParams.delete("view");
      else url.searchParams.set("view", current);
      history.replaceState(null, "", url);
    }
    const selected = navigation.querySelector(`[data-admin-view-tab="${current}"]`);
    if (focus) selected?.focus();
    onChange?.(current);
    return current;
  }

  for (const view of ADMIN_VIEWS) {
    const button = root.createElement("button");
    button.type = "button";
    button.id = `admin-view-tab-${view.id}`;
    button.dataset.adminViewTab = view.id;
    button.setAttribute("role", "tab");
    button.setAttribute("aria-controls", `admin-view-${view.id}`);
    button.title = view.description;
    button.textContent = view.label;
    button.addEventListener("click", () => select(view.id));
    button.addEventListener("keydown", (event) => {
      const index = ADMIN_VIEWS.findIndex((candidate) => candidate.id === current);
      let next = null;
      if (event.key === "ArrowRight") next = (index + 1) % ADMIN_VIEWS.length;
      if (event.key === "ArrowLeft") next = (index - 1 + ADMIN_VIEWS.length) % ADMIN_VIEWS.length;
      if (event.key === "Home") next = 0;
      if (event.key === "End") next = ADMIN_VIEWS.length - 1;
      if (next == null) return;
      event.preventDefault();
      select(ADMIN_VIEWS[next].id, {focus: true});
    });
    navigation.append(button);
  }

  const viewTargetsReady = () => ADMIN_VIEWS.every((view) => root.getElementById?.(`admin-view-${view.id}`));
  const observer = typeof MutationObserver === "function"
    ? new MutationObserver(() => {
        applyAdminView(root, current);
        if (viewTargetsReady()) observer.disconnect();
      })
    : null;
  observer?.observe(root.body || root, {childList: true, subtree: true});
  queueMicrotask(() => {
    applyAdminView(root, current);
    if (viewTargetsReady()) observer?.disconnect();
  });

  return {element: navigation, select, current: () => current, dispose: () => observer?.disconnect()};
}

function validReceipt(receipt) {
  return receipt && receipt.schemaVersion === RECEIPT_SCHEMA_VERSION
    && typeof receipt.id === "string" && receipt.id.length > 0
    && typeof receipt.recordedAt === "string"
    && typeof receipt.state === "string";
}

export function readOperationReceipts(storage = null, key = DEFAULT_RECEIPT_KEY) {
  try {
    const target = storage ?? globalThis.localStorage;
    const decoded = JSON.parse(target?.getItem(key) || "[]");
    return Array.isArray(decoded) ? decoded.filter(validReceipt) : [];
  } catch {
    return [];
  }
}

export function persistOperationReceipts(receipts, storage = null,
  key = DEFAULT_RECEIPT_KEY) {
  try {
    const target = storage ?? globalThis.localStorage;
    target?.setItem(key, JSON.stringify(receipts));
    return true;
  } catch {
    return false;
  }
}

export function upsertOperationReceipt(receipts, receipt, {now = () => new Date().toISOString(), limit = 40} = {}) {
  const normalized = {
    schemaVersion: RECEIPT_SCHEMA_VERSION,
    id: receipt.id,
    requestId: receipt.requestId || null,
    action: receipt.action || "operation",
    wiki: receipt.wiki || null,
    state: receipt.state,
    title: receipt.title || "Operator action",
    detail: receipt.detail || null,
    recordedAt: receipt.recordedAt || now(),
    updatedAt: receipt.updatedAt || now()
  };
  if (!validReceipt(normalized)) throw new Error("operation receipt requires an id and state");
  const existing = receipts.find((candidate) => candidate.id === normalized.id);
  const combined = existing ? {...existing, ...normalized, recordedAt: existing.recordedAt} : normalized;
  return [combined, ...receipts.filter((candidate) => candidate.id !== normalized.id)]
    .sort((left, right) => Date.parse(right.updatedAt) - Date.parse(left.updatedAt))
    .slice(0, limit);
}

export function operationReceiptsFromStatus(status) {
  const operations = status?.adminOperations;
  if (!operations) return [];
  return [
    ...(operations.running || []),
    ...(operations.queued || []),
    ...(operations.recent || [])
  ].filter((operation) => operation.requestId).map((operation) => ({
    id: `request:${operation.requestId}`,
    requestId: operation.requestId,
    action: operation.action,
    wiki: operation.wiki || null,
    state: operation.state || (operation.exitCode === 0 ? "succeeded" : "failed"),
    title: `${(operation.action || "operation").replaceAll("-", " ").replace(/^./, (character) => character.toUpperCase())}${operation.wiki ? ` · ${operation.wiki}` : ""}`,
    detail: operation.errorSummary || operation.error || operation.stageLabel || operation.stage || null,
    recordedAt: operation.requestedAt || operation.startedAt || operation.updatedAt,
    updatedAt: operation.updatedAt || operation.finishedAt || operation.startedAt
  }));
}

export function reconcileOperationReceipts(receipts, status, options = {}) {
  return operationReceiptsFromStatus(status).reduce(
    (current, receipt) => upsertOperationReceipt(current, receipt, options),
    receipts
  );
}

export function hasActiveAdminWork(status) {
  return Boolean(status?.job?.running
    || Number(status?.adminOperations?.counts?.running || 0) > 0
    || Number(status?.adminOperations?.counts?.queued || 0) > 0
    || Number(status?.fleet?.counts?.running || 0) > 0);
}

export function createAdaptivePoll({poll, isActive = () => false,
  isVisible = () => globalThis.document?.visibilityState !== "hidden",
  setTimer = globalThis.setTimeout, clearTimer = globalThis.clearTimeout,
  intervals = {active: 1000, idle: 5000, hidden: 30000, errorMaximum: 60000},
  onError = () => {}}) {
  let stopped = true;
  let timer = null;
  let pending = null;
  let refreshRequested = false;
  let failures = 0;

  function delay() {
    if (!isVisible()) return intervals.hidden;
    if (failures) return Math.min(intervals.idle * (2 ** failures), intervals.errorMaximum);
    return isActive() ? intervals.active : intervals.idle;
  }

  function schedule(wait = delay()) {
    if (stopped) return;
    if (timer != null) clearTimer(timer);
    timer = setTimer(() => {
      timer = null;
      void refresh();
    }, wait);
  }

  function refresh() {
    if (stopped) return Promise.resolve(null);
    if (timer != null) {
      clearTimer(timer);
      timer = null;
    }
    if (pending) {
      refreshRequested = true;
      return pending;
    }
    pending = Promise.resolve().then(poll).then((result) => {
      failures = 0;
      return result;
    }).catch((error) => {
      failures += 1;
      onError(error);
      return null;
    }).finally(() => {
      pending = null;
      const rerun = refreshRequested;
      refreshRequested = false;
      schedule(rerun ? 0 : delay());
    });
    return pending;
  }

  function start() {
    if (!stopped) return pending || Promise.resolve(null);
    stopped = false;
    return refresh();
  }

  function stop() {
    stopped = true;
    refreshRequested = false;
    if (timer != null) clearTimer(timer);
    timer = null;
  }

  return {start, stop, refresh, state: () => ({running: !stopped, pending: Boolean(pending), failures, nextDelay: delay()})};
}
