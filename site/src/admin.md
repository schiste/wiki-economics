---
title: Admin
---

# Operations

<div class="page-intro">

See what is running, what needs attention, and what is ready to publish. Durable operator, fleet, snapshot, and publication evidence is reconciled into four human milestones for every wiki.

</div>

```js
import {
  createAdaptivePoll,
  createAdminViewNavigation,
  hasActiveAdminWork,
  persistOperationReceipts,
  readOperationReceipts,
  reconcileOperationReceipts,
  upsertOperationReceipt
} from "./components/admin-console.js"
```

```js
const initialManifest = await FileAttachment("data/manifest.json").json()

function emptyWikiStatus(name) {
  return {
    name,
    tracked: false,
    raw: {version: null, files: 0, size: "0 B", details: []},
    parquet: {done: 0, total: 0, in_progress: 0, missing: [], size: "0 B"},
    patrol: {xml: 0, events: 0, rights: 0, groups: 0, source_ready: 0, metric_ready: 0},
    metrics: [],
    dashboard: [],
    status: "needs_fetch"
  }
}
```

```js
const API = globalThis.__wikiEconAdminApiBase || "http://127.0.0.1:3001/api"
const apiAvailable = Mutable(false)
const jobStatus = Mutable(null)
const authState = Mutable({enabled: false, authenticated: true, loginUrl: null, logoutUrl: null, user: null})
const liveManifest = Mutable(initialManifest)
const selectedWikiState = Mutable(null)
const initialRunId = typeof location !== "undefined" ? new URL(location.href).searchParams.get("run") : null
const selectedRunState = Mutable(initialRunId)
const operationReceiptState = Mutable(readOperationReceipts())
function setSelectedWiki(value, userInitiated = true) {
  if (userInitiated) adminUiState.selectedWikiUser = true
  selectedWikiState.value = value
}
function setSelectedRun(value) {
  selectedRunState.value = value || null
  if (typeof history !== "undefined" && typeof location !== "undefined") {
    const url = new URL(location.href)
    if (value) url.searchParams.set("run", value)
    else url.searchParams.delete("run")
    history.replaceState(null, "", url)
  }
}
const adminUiState = globalThis.__wikiEconAdminState ??= {
  showRunningLog: false,
  showJobLog: false,
  onboardingWiki: null,
  snapshotVersion: "",
  lastKnownRunner: null,
  onboardingMode: "qualification",
  onboardingResourceClass: "medium_large",
  notice: null,
  receiptSequence: 0
}
adminUiState.selectedWikiUser ??= false
let statusPoller = null
const SNAPSHOT_VERSION_RE = /^\d{4}-\d{2}$/
const languageNames = typeof Intl !== "undefined" && Intl.DisplayNames
  ? new Intl.DisplayNames(["en"], {type: "language"})
  : null

function cliFlags(manifest = initialManifest || {}) {
  const dataDir = manifest?.data_dir || "data"
  const outputDir = manifest?.output_dir || "output"
  return `--data-dir ${dataDir} --output-dir ${outputDir}`
}

// The server reports its actual runner (compiled binary on Toolforge via
// WIKI_ECON_BIN, `cargo run --release --` in local dev) via the /status
// payload, but that hint is shown precisely when the API is unreachable —
// so we cache the last successfully-observed runner label client-side and
// fall back to the cargo default only if we've never heard from the server.
function runnerCommand() {
  return adminUiState.lastKnownRunner?.label || "cargo run --release --"
}

function normalizeSnapshotVersion(value) {
  const trimmed = typeof value === "string" ? value.trim() : ""
  return trimmed || null
}

function validSnapshotVersion(value) {
  const normalized = normalizeSnapshotVersion(value)
  return normalized && SNAPSHOT_VERSION_RE.test(normalized) ? normalized : null
}

function wikipediaProjectLabel(wiki) {
  if (!wiki) return "Unknown project"
  const code = wiki.endsWith("wiki") ? wiki.slice(0, -4) : wiki
  // Intl.DisplayNames.of throws RangeError on inputs that aren't valid
  // BCP-47 tags (e.g. "simple", "bat_smg", "zh_classical", "nrm"); also
  // returns the input unchanged if it doesn't recognize the tag. Try the
  // raw code first, then a hyphen-normalized variant for codes that use
  // MediaWiki's underscore convention. Fall through to the bare wiki name
  // on any failure so the picker label can never crash.
  const variants = [code, code.replace(/_/g, "-")]
  for (const candidate of variants) {
    let language = null
    try { language = languageNames?.of(candidate) ?? null } catch { language = null }
    if (language && language.toLowerCase() !== candidate.toLowerCase()) {
      return `${language} Wikipedia (${wiki})`
    }
  }
  return `${wiki} (Wikipedia)`
}

function wikipediaProjectSearchText(wiki) {
  return `${wiki} ${wikipediaProjectLabel(wiki)}`.toLowerCase()
}

function preferredSnapshotVersion() {
  // A version is a strict operator pin, never a calendar-derived default.
  // Leaving it blank delegates selection to Rust's completed-dump resolver.
  return validSnapshotVersion(adminUiState.snapshotVersion) ?? null
}

function setLogButtonLabel(button, expanded) {
  const expandLabel = button.dataset.expandLabel || "Show output"
  const collapseLabel = button.dataset.collapseLabel || "Hide output"
  const lines = button.dataset.lines || "0"
  button.textContent = `${expanded ? collapseLabel : expandLabel} (${lines} lines)`
}

function toggleLogSection(event, key) {
  const button = event.currentTarget
  adminUiState[key] = !adminUiState[key]
  const expanded = adminUiState[key]
  const section = button.closest(".admin-log-section")
  const output = section?.querySelector(".admin-job-log")
  if (output) output.hidden = !expanded
  setLogButtonLabel(button, expanded)
}

async function copyTextToClipboard(text, button, successLabel = "Copied") {
  try {
    await navigator.clipboard.writeText(text)
  } catch {
    const textarea = document.createElement("textarea")
    textarea.value = text
    textarea.setAttribute("readonly", "")
    textarea.style.position = "absolute"
    textarea.style.left = "-9999px"
    document.body.appendChild(textarea)
    textarea.select()
    document.execCommand("copy")
    textarea.remove()
  }

  if (!button) return
  const originalLabel = button.dataset.originalLabel || ""
  button.dataset.originalLabel = originalLabel || button.innerHTML
  button.innerHTML = successLabel
  clearTimeout(button._copyResetTimer)
  button._copyResetTimer = setTimeout(() => {
    button.innerHTML = button.dataset.originalLabel || originalLabel
  }, 1500)
}

function copyIconButton(getText, label = "Copy output") {
  return html`<button
    class="admin-icon-btn admin-copy-btn"
    title=${label}
    aria-label=${label}
    onclick=${(event) => copyTextToClipboard(getText(), event.currentTarget)}
  >
    <svg viewBox="0 0 16 16" aria-hidden="true" focusable="false">
      <path d="M5 2.5A1.5 1.5 0 0 1 6.5 1h5A1.5 1.5 0 0 1 13 2.5v7A1.5 1.5 0 0 1 11.5 11h-5A1.5 1.5 0 0 1 5 9.5z"></path>
      <path d="M3.5 5A1.5 1.5 0 0 0 2 6.5v6A1.5 1.5 0 0 0 3.5 14h5A1.5 1.5 0 0 0 10 12.5V12H6.5A2.5 2.5 0 0 1 4 9.5V5z"></path>
    </svg>
  </button>`
}

// diskHeadroom/rawCleanup come from admin-server.cjs's trackStageFromChunk,
// which sniffs them out of the Rust CLI's fetch/ingest log lines (there's
// no structured event channel) — either can be absent (older job, or the
// stage hasn't happened yet), so render nothing rather than a false badge.
function pipelineBadges(diskHeadroom, rawCleanup) {
  const badges = []
  if (diskHeadroom) {
    badges.push(html`<span class="admin-badge ${diskHeadroom.ok ? "ok" : "fail"}" title=${diskHeadroom.message || ""}>
      ${diskHeadroom.ok ? "Disk headroom OK" : "Disk headroom check failed"}
    </span>`)
  }
  if (rawCleanup?.done) {
    badges.push(html`<span class="admin-badge ok" title=${rawCleanup.message || ""}>Raw dump cleaned up</span>`)
  }
  return badges.length ? html`<div class="admin-pipeline-badges">${badges}</div>` : ""
}

function adminConnectionHelp() {
  const auth = authState.value || {}
  if (auth.enabled && auth.authenticated === false) {
    const loginUrl = auth.loginUrl || "/admin/login"
    return html`<span style="color:#c62828">authentication required</span> — <a href=${loginUrl}>sign in</a>`
  }
  return html`<span style="color:#c62828">API offline</span> — run <code>scripts/dev.sh</code> or <code>WIKI_ECON_ADMIN_ENABLED=1 node site/admin-server.cjs</code>`
}

function adminConnectionWarning() {
  const auth = authState.value || {}
  if (auth.enabled && auth.authenticated === false) {
    const loginUrl = auth.loginUrl || "/admin/login"
    return html`<div class="warning">Admin authentication required. <a href=${loginUrl}>Sign in</a> to continue.</div>`
  }
  return html`<div class="warning">Start the dev/operator admin server to enable commands: <code>scripts/dev.sh</code> or <code>WIKI_ECON_ADMIN_ENABLED=1 node site/admin-server.cjs</code></div>`
}

function recordOperationReceipt(receipt) {
  const now = new Date().toISOString()
  const id = receipt.id || `local:${Date.now()}:${adminUiState.receiptSequence++}`
  const next = upsertOperationReceipt(operationReceiptState.value, {
    ...receipt,
    id,
    updatedAt: receipt.updatedAt || now,
    recordedAt: receipt.recordedAt || now
  })
  operationReceiptState.value = next
  persistOperationReceipts(next)
  return id
}

function syncOperationReceipts(status) {
  const next = reconcileOperationReceipts(operationReceiptState.value, status)
  if (JSON.stringify(next) !== JSON.stringify(operationReceiptState.value)) {
    operationReceiptState.value = next
    persistOperationReceipts(next)
  }
}

async function checkApi() {
  try {
    const r = await fetch(`${API}/status`, {credentials: "same-origin"})
    const data = await r.json().catch(() => null)
    if (r.status === 401) {
      apiAvailable.value = false
      jobStatus.value = data
      authState.value = data?.auth || {enabled: true, authenticated: false, loginUrl: "/admin/login", logoutUrl: null, user: null}
      return
    }
    if (r.ok) {
      apiAvailable.value = true
      jobStatus.value = data
      authState.value = data?.auth || {enabled: false, authenticated: true, loginUrl: null, logoutUrl: null, user: null}
      if (data.manifest?.wikis) {
        liveManifest.value = data.manifest
      }
      if (data.runner?.label) {
        adminUiState.lastKnownRunner = data.runner
      }
      syncOperationReceipts(data)
    } else {
      apiAvailable.value = false
      jobStatus.value = data
    }
  } catch (error) {
    apiAvailable.value = false
    jobStatus.value = null
    throw error
  }
}

async function runCommand(action, wikiOrOptions = null) {
  try {
    const options = typeof wikiOrOptions === "string"
      ? {wiki: wikiOrOptions}
      : (wikiOrOptions ?? {})
    const requestedVersion = normalizeSnapshotVersion(options.version)
    if (requestedVersion && !SNAPSHOT_VERSION_RE.test(requestedVersion)) {
      recordOperationReceipt({
        action,
        wiki: options.wiki,
        state: "failed",
        title: `${actionLabel(action)} was not started`,
        detail: "Invalid snapshot version. Use YYYY-MM."
      })
      return null
    }
    const body = JSON.stringify({
      ...(options.wiki ? {wiki: options.wiki} : {}),
      ...(requestedVersion ? {version: requestedVersion} : {}),
      ...(options.requestId ? {requestId: options.requestId} : {}),
      ...(options.taskId ? {taskId: options.taskId} : {}),
      ...(options.mode ? {mode: options.mode} : {}),
      ...(options.resourceClass ? {resourceClass: options.resourceClass} : {}),
      ...(options.operation ? {operation: options.operation} : {}),
      ...(options.refresh ? {refresh: options.refresh} : {}),
      ...(options.freshnessSlaDays != null ? {freshnessSlaDays: options.freshnessSlaDays} : {}),
      ...(options.lifecycleRevision ? {lifecycleRevision: options.lifecycleRevision} : {}),
      ...(options.qualificationRunId ? {qualificationRunId: options.qualificationRunId} : {}),
      ...(options.candidateRunId ? {candidateRunId: options.candidateRunId} : {}),
      ...(options.acknowledgeBlockedRetry ? {acknowledgeBlockedRetry: true} : {})
    })
    const r = await fetch(`${API}/${action}`, {
      method: "POST",
      headers: {"Content-Type": "application/json"},
      body,
      credentials: "same-origin"
    })
    const data = await r.json()
    if (r.status === 401) {
      authState.value = data?.auth || {enabled: true, authenticated: false, loginUrl: "/admin/login", logoutUrl: null, user: null}
      recordOperationReceipt({
        action,
        wiki: options.wiki,
        state: "failed",
        title: `${actionLabel(action)} was not started`,
        detail: "Admin authentication is required. Sign in again to continue."
      })
      return null
    }
    if (data.error) {
      recordOperationReceipt({
        action,
        wiki: options.wiki,
        state: "failed",
        title: `${actionLabel(action)} was not started`,
        detail: data.error
      })
      return null
    }
    adminUiState.notice = data.queued
      ? `${actionLabel(action)} queued as ${data.requestId}`
      : `${actionLabel(action)} accepted`
    recordOperationReceipt({
      id: data.requestId ? `request:${data.requestId}` : undefined,
      requestId: data.requestId,
      action,
      wiki: options.wiki,
      state: data.queued ? "queued" : "accepted",
      title: `${actionLabel(action)} ${data.queued ? "queued" : "accepted"}`,
      detail: data.requestId ? `Request ${data.requestId}` : "The admin server accepted the operation."
    })
    adminUiState.showRunningLog = false
    adminUiState.showJobLog = false
    await statusPoller?.refresh()
    return data
  } catch (e) {
    recordOperationReceipt({
      action,
      wiki: typeof wikiOrOptions === "string" ? wikiOrOptions : wikiOrOptions?.wiki,
      state: "failed",
      title: `${actionLabel(action)} was not started`,
      detail: "Admin server not reachable. Run scripts/dev.sh or start the Toolforge admin service."
    })
    return null
  }
}

async function registerWiki(wiki, mode, resourceClass, {start = false, version = null} = {}) {
  const result = await runCommand(start ? "onboard-wiki" : "register-wiki", {
    wiki, mode, resourceClass, version, lifecycleRevision: jobStatus.value?.lifecycleRevision || null
  })
  if (!result?.registered) return
  setSelectedWiki(wiki)
  if (start && !result.queued && result.nextAction) {
    await runCommand(result.nextAction, {wiki, version})
  }
}

function actionLabel(action) {
  switch (action) {
    case "fetch": return "fetch missing"
    case "patrol-fetch": return "fetch patrol"
    case "ingest": return "ingest"
    case "compute": return "compute"
    case "patrol-compute": return "refresh patrol"
    case "patrol-rebuild": return "refetch and rebuild patrol"
    case "merge": return "merge data"
    case "publish": return "publish candidates"
    case "site": return "rebuild site"
    case "fleet-recover": return "recover fleet"
    case "quarantine-retry": return "retry quarantined task"
    case "publication-recovery-audit": return "audit publication recovery"
    case "publication-preflight": return "run publication preflight"
    case "artifact-scrub": return "scrub published artifacts"
    case "recover-admin": return "recover operator queue"
    case "qualify": return "qualify project"
    case "register-wiki": return "add project"
    case "onboard-wiki": return "add and start project"
    case "update-lifecycle": return "update lifecycle policy"
    case "promote-qualification": return "promote qualification"
    case "retire-candidate": return "retire candidate"
    case "rebuild-candidate": return "rebuild candidate"
    case "cleanup": return "cleanup"
    case "cancel": return "cancel"
    case "run": return "prepare update"
    default: return action
  }
}

function actionTooltip(action) {
  switch (action) {
    case "fetch":
      return "Download only the missing history dump files for this wiki; existing dump files are skipped."
    case "patrol-fetch":
      return "Download or refresh the patrol logging sources needed to compute patrol metrics."
    case "ingest":
      return "Convert the available raw history dumps into parquet partitions, skipping already completed sources."
    case "compute":
      return "Compute the core economic metrics from the ingested parquet data for this wiki."
    case "patrol-compute":
      return "Check upstream readiness, fetch any missing patrol sources, then resume patrol computation from existing month shards."
    case "patrol-rebuild":
      return "Check upstream readiness, fetch any missing patrol sources, then rebuild only this wiki's patrol metrics."
    case "merge":
      return "Regenerate merged publication data without rebuilding the website."
    case "publish":
      return "Select valid ready candidates, validate the publication, and atomically switch the live generation."
    case "site":
      return "Rebuild and validate only the website against the currently published data."
    case "fleet-recover":
      return "Reclaim stale fleet leases and requeue recoverable work without touching healthy tasks."
    case "quarantine-retry":
      return "Requeue one exact authenticated fleet task after its failure cause has been corrected."
    case "publication-recovery-audit":
      return "Audit interrupted publication transactions without changing production state."
    case "publication-preflight":
      return "Authenticate ready candidates and show the exact wiki-by-family change plan without publishing."
    case "artifact-scrub":
      return "Sequentially rehash and semantically verify every published artifact."
    case "recover-admin":
      return "Requeue an operator operation only after its dedicated worker heartbeat has been stale for ten minutes."
    case "qualify":
      return "Run fetch, ingest, compute, patrol, and validation as a publication-invisible qualification."
    case "register-wiki":
      return "Persist this project in the lifecycle registry before any data is downloaded."
    case "update-lifecycle":
      return "Atomically change scheduling, resource class, or freshness SLA and record the operator action."
    case "promote-qualification":
      return "Copy one exact validated qualification into a managed candidate, then update lifecycle policy only after that succeeds."
    case "retire-candidate":
      return "Retire one exact unpublished ready candidate. The active publication and rollback generation are protected."
    case "rebuild-candidate":
      return "Build a new immutable candidate for the exact snapshot without modifying the existing candidate or live publication."
    case "cleanup":
      return "Remove temporary files and invalid ingest markers for this wiki."
    case "cancel":
      return "Stop the currently running pipeline job."
    case "run":
      return "Prepare and validate a new immutable candidate for this wiki without publishing it directly."
    default:
      return ""
  }
}

function actionTooltipWithApi(action, enabled = true) {
  const base = actionTooltip(action)
  return enabled ? base : `${base} Admin API offline.`
}

statusPoller = createAdaptivePoll({
  poll: checkApi,
  isActive: () => hasActiveAdminWork(jobStatus.value),
  onError: () => {}
})
void statusPoller.start()
// Stop the sole adaptive status loop on hot reload.
invalidation.then(() => {
  statusPoller?.stop()
})
```

```js
const apiStatus = apiAvailable
const job = jobStatus
const auth = authState
const currentManifest = liveManifest || initialManifest || {generated_at: "unknown", wikis: {}, merged: []}
const currentWikis = currentManifest.wikis || {}
const lifecycleStates = job?.wikiStates || currentManifest.lifecycle?.wikis || {}
const lifecycleRevision = job?.lifecycleRevision || null
const lifecycleAudit = job?.lifecycleAudit || {events: [], invalid: []}
const qualifications = job?.qualifications || {}
const refreshWikis = job?.refreshWikis || Object.entries(lifecycleStates).filter(([, state]) => state.refresh === "scheduled").map(([wiki]) => wiki)
const publishedWikis = job?.publishedWikis || Object.entries(lifecycleStates).filter(([, state]) => state.publication === "published").map(([wiki]) => wiki)
const wikiJobMap = job?.wikiJobs || {}
const wikiJobHistory = job?.wikiJobHistory || {}
const globalJob = job?.globalJob || null
const fleet = job?.fleet || {counts: {}, work: [], quarantine: [], recentFailures: []}
const fleetByWiki = new Map((fleet.work || []).map((entry) => [entry.wiki, entry]))
const adminOperations = job?.adminOperations || {executionMode: "direct", counts: {}, queued: [], running: [], recent: []}
const operatorOperations = [
  ...(adminOperations.running || []),
  ...(adminOperations.queued || []),
  ...(adminOperations.recent || [])
]
const operatorOperationByWiki = new Map()
for (const operation of operatorOperations) {
  if (operation.wiki && !operatorOperationByWiki.has(operation.wiki)) operatorOperationByWiki.set(operation.wiki, operation)
}
const adminRuns = job?.adminRuns || {active: null, recent: []}
const freshness = job?.freshness || {status: "unknown", alerts: [], summary: {}}
const snapshotPlans = job?.snapshotPlans || []
const latestPlanByWiki = new Map()
for (const plan of snapshotPlans) {
  const current = latestPlanByWiki.get(plan.wiki)
  if (!current || plan.snapshot > current.snapshot) latestPlanByWiki.set(plan.wiki, plan)
}
const operationalTruth = job?.operationalTruth || {
  public: {status: freshness.status || "unknown", alerts: freshness.alerts || [], scrub: {state: "missing"}},
  pipeline: {status: "unknown", issues: []},
  infrastructure: {status: "unknown", issues: [], activeRequests: []},
  wikis: {}
}
const operationalWikiTruth = operationalTruth.wikis || {}
const supportedWikis = Array.from(new Set(job?.supportedWikis || [])).sort((a, b) => a.localeCompare(b))
```

<p class="filter-desc">Last scanned: ${currentManifest.generated_at}${apiStatus ? html` · <span style="color:#2e7d32">API connected</span>` : html` · ${adminConnectionHelp()}`}</p>

```js
const adminViewController = createAdminViewNavigation()
invalidation.then(() => adminViewController.dispose())
display(adminViewController.element)
```

```js
const operationReceipts = operationReceiptState
const visibleOperationReceipts = operationReceipts.slice(0, 8)

function receiptTone(state) {
  if (["failed", "interrupted", "quarantined", "stalled"].includes(state)) return "danger"
  if (["running", "cancelling"].includes(state)) return "active"
  if (["queued", "waiting_upstream", "accepted"].includes(state)) return "waiting"
  if (state === "succeeded") return "success"
  return "neutral"
}

function receiptStateLabel(state) {
  return state ? state.replaceAll("_", " ").replace(/^./, (character) => character.toUpperCase()) : "Unknown"
}

function receiptTime(value) {
  if (!value) return "time unknown"
  const delta = Date.now() - Date.parse(value)
  if (!Number.isFinite(delta)) return value
  if (Math.abs(delta) < 60_000) return "just now"
  if (Math.abs(delta) < 3_600_000) return `${Math.floor(Math.abs(delta) / 60_000)}m ago`
  return `${Math.floor(Math.abs(delta) / 3_600_000)}h ago`
}

function clearResolvedOperationReceipts() {
  const retained = operationReceiptState.value.filter((receipt) =>
    !["succeeded", "failed", "cancelled", "accepted"].includes(receipt.state))
  operationReceiptState.value = retained
  persistOperationReceipts(retained)
}

display(visibleOperationReceipts.length ? html`<section class="admin-operation-receipts" aria-labelledby="operation-receipts-title">
  <header>
    <div>
      <h2 id="operation-receipts-title">Operation receipts</h2>
      <span>Durable outcomes from this browser and authenticated server records.</span>
    </div>
    <button class="admin-btn small" onclick=${clearResolvedOperationReceipts}>Clear resolved</button>
  </header>
  <div class="admin-operation-receipt-list" role="log" aria-live="polite" aria-relevant="additions text">
    ${visibleOperationReceipts.map((receipt) => html`<article class=${`admin-operation-receipt ${receiptTone(receipt.state)}`}>
      <span class="admin-operation-receipt-state">${receiptStateLabel(receipt.state)}</span>
      <div><strong>${receipt.title}</strong>${receipt.detail ? html`<span>${receipt.detail}</span>` : ""}</div>
      <time datetime=${receipt.updatedAt}>${receiptTime(receipt.updatedAt)}</time>
    </article>`)}
  </div>
</section>` : html`<div class="admin-operation-receipts empty" role="status" aria-live="polite">
  <strong>No operator action receipts yet.</strong>
  <span>Queued, completed, and failed actions will remain here across page reloads.</span>
</div>`)
```

<!-- ── Job output panel ───────────────────────────────────── -->

```js
const trackedWikiEntries = Object.entries(currentWikis)
const trackedWikiNames = trackedWikiEntries.map(([name]) => name)
```

```js
const effectiveJob = job?.job || null
const runningWiki = effectiveJob?.running ? effectiveJob.wiki ?? null : null
const allWikiNames = Array.from(new Set([
  ...trackedWikiNames,
  ...Object.keys(lifecycleStates),
  ...Object.keys(wikiJobMap),
  ...Object.keys(wikiJobHistory),
  ...(fleet.work || []).map((entry) => entry.wiki),
  ...operatorOperations.map((entry) => entry.wiki),
  ...snapshotPlans.map((entry) => entry.wiki),
  ...(runningWiki ? [runningWiki] : [])
])).filter(Boolean)
const inlineRunningJob = effectiveJob?.running && runningWiki ? effectiveJob : null
const topLevelJob = effectiveJob && !effectiveJob.wiki ? effectiveJob : globalJob
function normalizeWikiStatus(name, value = {}) {
  const fallback = emptyWikiStatus(name)
  return {
    ...fallback,
    ...value,
    tracked: true,
    raw: {...fallback.raw, ...(value.raw || {})},
    parquet: {...fallback.parquet, ...(value.parquet || {})},
    patrol: {...fallback.patrol, ...(value.patrol || {})},
    metrics: Array.isArray(value.metrics) ? value.metrics : [],
    dashboard: Array.isArray(value.dashboard) ? value.dashboard : []
  }
}
const wikiMap = new Map(trackedWikiEntries.map(([name, value]) => [name, normalizeWikiStatus(name, value)]))
for (const name of allWikiNames) {
  if (!wikiMap.has(name)) wikiMap.set(name, emptyWikiStatus(name))
}

function latestWikiJob(name) {
  if (name === runningWiki && inlineRunningJob) return inlineRunningJob
  const candidates = [
    operatorOperationByWiki.get(name),
    wikiJobMap[name],
    wikiJobHistory[name]?.[0],
    operationalWikiTruth[name]?.candidate
  ].filter(Boolean)
  return candidates.sort((left, right) => Date.parse(operationTimestamp(right) || 0) - Date.parse(operationTimestamp(left) || 0))[0] || null
}

function operationalState(name, wiki) {
  const direct = latestWikiJob(name)
  const fleetWork = fleetByWiki.get(name)
  if (direct?.running || direct?.state === "running" || direct?.state === "cancelling") return direct.state || "running"
  if (direct?.state === "queued") return "queued"
  if (direct?.state === "waiting_upstream") return "waiting_upstream"
  if (fleetWork?.state) return fleetWork.state
  if (direct?.interrupted) return "interrupted"
  if (direct?.cancelled) return "cancelled"
  if (direct && direct.exitCode !== 0 && direct.exitCode != null) return "failed"
  if (!wiki.tracked && latestPlanByWiki.has(name)) return "planned"
  return wiki.status || "needs_fetch"
}

const operationalPriority = {
  stalled: 0,
  quarantined: 1,
  interrupted: 2,
  failed: 3,
  waiting_upstream: 4,
  running: 5,
  cancelling: 6,
  queued: 7,
  planned: 7,
  needs_fetch: 10,
  needs_patrol_fetch: 11,
  needs_ingest: 12,
  needs_compute: 13,
  needs_patrol_compute: 14,
  needs_merge: 15,
  cancelled: 16,
  complete: 30
}

const wikiEntries = Array.from(wikiMap.entries()).sort(([leftName, left], [rightName, right]) => {
  const leftState = operationalState(leftName, left)
  const rightState = operationalState(rightName, right)
  return (operationalPriority[leftState] ?? 20) - (operationalPriority[rightState] ?? 20)
    || leftName.localeCompare(rightName)
})
const wikiNames = wikiEntries.map(([name]) => name)
const activeOperatorWiki = adminOperations.running?.[0]?.wiki || adminOperations.queued?.[0]?.wiki || null
const selectedWikiCandidate = (adminUiState.selectedWikiUser && wikiNames.includes(selectedWikiState) ? selectedWikiState : null)
  || runningWiki
  || activeOperatorWiki
  || wikiNames[0]
  || "—"
if (selectedWikiCandidate !== "—" && selectedWikiCandidate !== selectedWikiState) {
  setSelectedWiki(selectedWikiCandidate, false)
}
const selectedWiki = selectedWikiCandidate.trim().toLowerCase()
const hasSelectedWiki = selectedWiki !== "—"
```

```js
const attentionStates = new Set(["stalled", "quarantined", "interrupted", "failed"])
const attentionCount = wikiEntries.filter(([name, wiki]) => attentionStates.has(operationalState(name, wiki))).length
const publicIssueCount = (operationalTruth.public?.alerts || []).length
const pipelineIssueCount = (operationalTruth.pipeline?.issues || []).length
const infrastructureIssueCount = (operationalTruth.infrastructure?.issues || []).length
const operatorIssueCount = publicIssueCount + Math.max(attentionCount, pipelineIssueCount) + infrastructureIssueCount
const activeCount = Number(Boolean(inlineRunningJob))
  + Number(fleet.counts?.running || 0)
  + Number(adminOperations.counts?.running || 0)
  + Number(adminOperations.counts?.queued || 0)
display(html`<div class="admin-command-header">
  <div class="admin-command-health ${operatorIssueCount > 0 ? "attention" : "clear"}">
    <span class="admin-command-kicker">Operator status</span>
    <strong>${operatorIssueCount > 0 ? `${operatorIssueCount} ${operatorIssueCount === 1 ? "item needs" : "items need"} attention` : "No blocking issues"}</strong>
    <span>${activeCount > 0 ? `${activeCount} active ${activeCount === 1 ? "run" : "runs"}` : "Pipeline idle"}</span>
  </div>
  <dl class="admin-command-facts">
    <div><dt>Public data</dt><dd class=${operationalTruth.public?.status === "healthy" ? "ok" : operationalTruth.public?.status === "critical" ? "bad" : ""}>${operationalTruth.public?.status || "unknown"}</dd></div>
    <div><dt>Update pipeline</dt><dd class=${operationalTruth.pipeline?.status === "healthy" ? "ok" : operationalTruth.pipeline?.status === "degraded" ? "bad" : ""}>${operationalTruth.pipeline?.status || "unknown"}</dd></div>
    <div><dt>Infrastructure</dt><dd class=${operationalTruth.infrastructure?.status === "available" ? "ok" : ""}>${operationalTruth.infrastructure?.status || "unknown"}</dd></div>
    <div><dt>Coverage</dt><dd>${publishedWikis.length} published · ${refreshWikis.length} scheduled</dd></div>
  </dl>
  <div class="admin-command-session">
    <span>${auth?.user?.email || (auth?.enabled ? "Sign-in required" : "Local operator")}</span>
    ${auth?.logoutUrl ? html`<a href=${auth.logoutUrl}>Sign out</a>` : ""}
  </div>
</div>`)
```

```js
topLevelJob
  ? html`<div data-admin-view="runs" class="admin-job-panel ${topLevelJob.running ? "running" : topLevelJob.cancelled ? "failed" : topLevelJob.exitCode === 0 ? "success" : "failed"}">
      <div class="admin-job-header">
        <strong>${topLevelJob.running ? "Running..." : topLevelJob.cancelled ? "Cancelled" : topLevelJob.exitCode === 0 ? "Completed" : "Failed"}</strong>
        <code>${topLevelJob.command || ""}</code>
      </div>
      ${topLevelJob.running && job.progress ? html`<div class="admin-progress">
        <div class="admin-progress-info">
          <span class="admin-progress-stage">${job.progress.stage || "starting"}</span>
          <span class="admin-progress-detail">${job.progress.detail}</span>
          <span class="admin-progress-pct">${job.progress.pct}%</span>
        </div>
        <div class="admin-progress-track">
          <div class="admin-progress-fill" style=${"width:" + job.progress.pct + "%"}></div>
        </div>
      </div>` : ""}
      ${topLevelJob.running
        ? pipelineBadges(job.progress?.diskHeadroom, job.progress?.rawCleanup)
        : pipelineBadges(topLevelJob.diskHeadroom, topLevelJob.rawCleanup)}
      ${topLevelJob.running
        ? html`<div class="admin-log-section">
            <div class="admin-log-bar">
              <button
                class="admin-log-toggle admin-log-button"
                data-expand-label="Show log output"
                data-collapse-label="Hide log output"
                data-lines=${String((topLevelJob.log || []).length)}
                onclick=${(event) => toggleLogSection(event, "showRunningLog")}
              >
                ${adminUiState.showRunningLog ? "Hide log output" : "Show log output"} (${(topLevelJob.log || []).length} lines)
              </button>
              ${copyIconButton(() => (topLevelJob.log || []).join(""), "Copy full log")}
            </div>
            <pre class="admin-job-log" ?hidden=${!adminUiState.showRunningLog}>${(topLevelJob.log || []).join("")}</pre>
          </div>`
        : html`<div class="admin-log-section">
            <div class="admin-log-bar">
              <button
                class="admin-log-toggle admin-log-button"
                data-expand-label="Show log output"
                data-collapse-label="Hide log output"
                data-lines=${String((topLevelJob.log || []).length)}
                onclick=${(event) => toggleLogSection(event, "showJobLog")}
              >
                ${adminUiState.showJobLog ? "Hide log output" : "Show log output"} (${(topLevelJob.log || []).length} lines)
              </button>
              ${copyIconButton(() => (topLevelJob.log || []).join(""), "Copy full log")}
            </div>
            <pre class="admin-job-log admin-job-log-full" ?hidden=${!adminUiState.showJobLog}>${(topLevelJob.log || []).join("")}</pre>
          </div>`
      }
    </div>`
  : html`<span></span>`
```

<div id="admin-view-runs" class="chart-section admin-activity-section" data-admin-view="runs">

## Activity

```js
function operationTimestamp(operation) {
  return operation.updatedAt || operation.finishedAt || operation.startedAt || null
}

function operationLabel(state) {
  return ({
    running: "Running",
    cancelling: "Cancelling",
    queued: "Queued",
    waiting_upstream: "Waiting upstream",
    stalled: "Stalled",
    quarantined: "Quarantined",
    succeeded: "Succeeded",
    failed: "Failed",
    interrupted: "Interrupted",
    cancelled: "Cancelled"
  })[state] || state || "Unknown"
}

function operationTone(state) {
  if (["failed", "interrupted", "quarantined", "stalled"].includes(state)) return "danger"
  if (["running", "cancelling"].includes(state)) return "active"
  if (["queued", "waiting_upstream"].includes(state)) return "waiting"
  if (state === "succeeded") return "success"
  return "neutral"
}

function relativeTime(value) {
  if (!value) return "time unknown"
  const delta = Date.now() - Date.parse(value)
  if (!Number.isFinite(delta)) return formatRefreshTimestamp(value)
  const absolute = Math.abs(delta)
  if (absolute < 60_000) return "just now"
  if (absolute < 3_600_000) return `${Math.floor(absolute / 60_000)}m ago`
  if (absolute < 86_400_000) return `${Math.floor(absolute / 3_600_000)}h ago`
  return `${Math.floor(absolute / 86_400_000)}d ago`
}

const activityRows = [
  ...(adminRuns.active ? [{...adminRuns.active, source: "operator"}] : []),
  ...(fleet.work || []).map((entry) => ({
    ...entry,
    action: "prepare",
    source: "fleet",
    stage: entry.state === "queued" ? "waiting for worker" : "candidate preparation"
  })),
  ...(adminRuns.recent || []).map((entry) => ({...entry, source: "operator"})),
  ...Object.values(operationalWikiTruth)
    .filter((entry) => entry?.candidate)
    .map((entry) => ({...entry.candidate, wiki: entry.wiki, source: "candidate receipt"}))
]
  .map((entry) => {
    const durable = entry.runId
      ? Object.values(operationalWikiTruth).find((truth) => truth?.candidate?.runId === entry.runId)?.candidate
      : null
    return durable ? {...entry, ...durable, log: entry.log || durable.log || []} : entry
  })
  .filter((entry, index, rows) => {
    const identity = entry.runId
      ? `run:${entry.runId}`
      : entry.taskId
        ? `task:${entry.taskId}`
        : `fallback:${entry.source}:${entry.wiki || "global"}:${operationTimestamp(entry) || "unknown"}:${entry.state || "unknown"}`
    return rows.findIndex((candidate) => {
      const candidateIdentity = candidate.runId
        ? `run:${candidate.runId}`
        : candidate.taskId
          ? `task:${candidate.taskId}`
          : `fallback:${candidate.source}:${candidate.wiki || "global"}:${operationTimestamp(candidate) || "unknown"}:${candidate.state || "unknown"}`
      return candidateIdentity === identity
    }) === index
  })
  .sort((left, right) => {
    const priority = {stalled: 0, quarantined: 1, interrupted: 2, failed: 3, running: 4, cancelling: 5, queued: 6}
    return (priority[left.state] ?? 20) - (priority[right.state] ?? 20)
      || Date.parse(operationTimestamp(right) || 0) - Date.parse(operationTimestamp(left) || 0)
  })
  .slice(0, 12)
```

```js
display(activityRows.length
  ? html`<div class="admin-activity-ledger">
      ${activityRows.map((operation) => html`<button
        class="admin-activity-row ${operationTone(operation.state)}"
        onclick=${() => {
          if (operation.wiki) setSelectedWiki(operation.wiki)
          setSelectedRun(operation.runId || operation.taskId || null)
        }}
      >
        <span class="admin-activity-state">${operationLabel(operation.state)}</span>
        <span class="admin-activity-main">
          <strong>${operation.wiki || "Global publication"}</strong>
          <span>${operation.stage || operation.action || "pipeline"}${operation.snapshot ? ` · ${operation.snapshot}` : ""}</span>
        </span>
        <span class="admin-activity-source">${operation.source}</span>
        <time datetime=${operationTimestamp(operation) || ""}>${relativeTime(operationTimestamp(operation))}</time>
      </button>`)}
    </div>`
  : html`<div class="admin-empty-state"><strong>No recent operator activity.</strong><span>Scheduled and manual runs will appear here as soon as they are queued.</span></div>`)
```

```js
const selectedRunId = selectedRunState
const selectedRun = activityRows.find((entry) => (entry.runId || entry.taskId) === selectedRunId) || null
const selectedRunTruth = selectedRun?.wiki ? operationalWikiTruth[selectedRun.wiki] || null : null

function bytesLabel(value) {
  if (!Number.isFinite(Number(value))) return "—"
  const bytes = Number(value)
  if (bytes >= 1024 ** 3) return `${(bytes / 1024 ** 3).toFixed(2)} GiB`
  if (bytes >= 1024 ** 2) return `${(bytes / 1024 ** 2).toFixed(1)} MiB`
  return `${bytes.toLocaleString()} B`
}

function durationLabel(value) {
  if (!Number.isFinite(Number(value))) return "—"
  const seconds = Number(value) / 1000
  if (seconds < 60) return `${seconds.toFixed(seconds < 10 ? 1 : 0)}s`
  const minutes = Math.floor(seconds / 60)
  return `${minutes}m ${Math.round(seconds % 60)}s`
}

function runStages(run) {
  if (Array.isArray(run?.stages) && run.stages.length) return run.stages
  return Object.entries(run?.stageDurationsMs || {}).map(([stage, durationMs]) => ({
    stage, durationMs, state: stage === run?.failingStage ? "failed" : "succeeded"
  }))
}

async function executeAllowedAction(action) {
  if (action.confirmation && !confirm(action.confirmation)) return
  await runCommand(action.id, {
    wiki: action.wiki || selectedRun?.wiki || null,
    taskId: action.taskId || selectedRun?.taskId || null,
    acknowledgeBlockedRetry: Boolean(action.acknowledgeBlockedRetry)
  })
}
```

```js
selectedRun ? display(html`<section class="admin-run-sheet" aria-label="Run details">
  <header class="admin-run-sheet-header">
    <div>
      <span class="admin-run-sheet-state ${operationTone(selectedRun.state)}">${operationLabel(selectedRun.state)}</span>
      <h3>${selectedRun.wiki ? wikipediaProjectLabel(selectedRun.wiki) : "Global operation"}</h3>
      <code>${selectedRun.runId || selectedRun.taskId}</code>
    </div>
    <button class="admin-btn small" onclick=${() => setSelectedRun(null)}>Close details</button>
  </header>
  <div class="admin-run-facts">
    <div><span>Snapshot</span><strong>${selectedRun.selectedSnapshot || selectedRun.snapshot || "—"}</strong></div>
    <div><span>Elapsed</span><strong>${selectedRun.durationSecs != null ? durationLabel(selectedRun.durationSecs * 1000) : "—"}</strong></div>
    <div><span>Memory peak</span><strong>${bytesLabel(selectedRun.memoryPeakBytes)}</strong><small>${selectedRun.memoryLimitBytes ? ` of ${bytesLabel(selectedRun.memoryLimitBytes)}` : ""}</small></div>
    <div><span>Disk free</span><strong>${bytesLabel(selectedRun.disk?.freeBytes)}</strong></div>
    <div><span>CPU time</span><strong>${selectedRun.cpu?.usageUsec ? durationLabel(selectedRun.cpu.usageUsec / 1000) : "—"}</strong><small>${selectedRun.cpu?.throttledUsec ? ` throttled ${durationLabel(selectedRun.cpu.throttledUsec / 1000)}` : ""}</small></div>
    <div><span>Heartbeat</span><strong>${relativeTime(selectedRun.heartbeatAt || selectedRun.updatedAt)}</strong></div>
  </div>
  <div class="admin-run-timeline">
    ${runStages(selectedRun).length ? runStages(selectedRun).map((stage) => html`<div class="admin-run-stage ${stage.state || "unknown"}">
      <i aria-hidden="true"></i>
      <div><strong>${stage.stage?.replaceAll("_", " ") || "stage"}</strong><span>${stage.reused ? "Reused" : stage.skipped ? "Skipped" : operationLabel(stage.state)}</span></div>
      <time>${durationLabel(stage.durationMs)}</time>
      ${stage.error ? html`<p>${stage.error}</p>` : ""}
    </div>`) : html`<div class="admin-empty-state"><strong>No stage receipt was recorded.</strong><span>The operation log remains available below.</span></div>`}
  </div>
  ${selectedRun.errorSummary || selectedRun.error ? html`<div class="admin-run-diagnosis">
    <span>Diagnosis</span>
    <strong>${selectedRun.errorSummary || selectedRun.error}</strong>
    <p>${selectedRun.remediation || "Review the recorded evidence before retrying."}</p>
    <div class="admin-dossier-actions">
      ${(selectedRunTruth?.allowedActions || []).length
        ? selectedRunTruth.allowedActions.map((action) => html`<button class="admin-btn ${action.id === "quarantine-retry" ? "danger" : ""}" ?disabled=${!apiStatus} onclick=${() => executeAllowedAction(action)}>${action.label}</button>`)
        : html`<span class="admin-action-blocked">No automated action is safe for this diagnosis.</span>`}
    </div>
  </div>` : ""}
  ${selectedRun.provenance ? html`<details class="admin-run-provenance"><summary>Build provenance</summary><pre>${JSON.stringify(selectedRun.provenance, null, 2)}</pre></details>` : ""}
  ${(selectedRun.log || []).length ? html`<details class="admin-run-provenance"><summary>Operation log</summary><pre>${(selectedRun.log || []).join("")}</pre></details>` : ""}
</section>`) : display(html`<span></span>`)
```

</div>

<!-- ── Scheduled refresh panel ─────────────────────────────── -->

<div id="admin-view-overview" class="chart-section" data-admin-view="overview">

## Publication health

<div class="note">Fail-closed freshness evaluation from the durable publisher run record, artifact scrub, lifecycle SLAs, memory, disk, and browser-size evidence.</div>

```js
// `last` is the live run record. Starting/running records carry a heartbeat
// and current stage; terminal records also carry exit status, stage timings,
// provenance, resources, publication aggregates, and site generation.
// (no run has ever written a status file yet — e.g. fresh deploy, or local
// dev where WIKI_ECON_OUTPUT_DIR isn't the Toolforge NFS mount).
// `scheduleCron` is the WIKI_ECON_REFRESH_SCHEDULE string (display-only —
// there's no way to confirm it against Toolforge's actual cron
// registration from here) — may be null if unset.
//
// This is a judgment call: how much slack should there be between the
// schedule and "no successful run" before it's worth flagging loudly
// instead of just showing the raw last-run timestamp? A weekly schedule
// that's 2 days late might be nothing; 3 missed weeks in a row is very
// different from 1 run that happened to fail once.
//
// Return {status: "healthy"|"stale"|"failed"|"unknown", message}.
function classifyRefreshHealth(last, scheduleCron) {
  if (!last) return {status: "unknown", message: "No refresh has reported in yet."}
  if (["starting", "running"].includes(last.state)) {
    const heartbeatAge = Date.now() - new Date(last.heartbeatAt || last.startedAt).getTime()
    if (!Number.isFinite(heartbeatAge) || heartbeatAge > 5 * 60 * 1000) {
      return {status: "stale", message: `Run heartbeat is stale${last.currentStage ? ` at ${last.currentStage}` : ""}.`}
    }
    return {status: "healthy", message: `Running${last.currentStage ? `: ${last.currentStage}` : ""}.`}
  }
  if (last.state === "failed" || last.exitCode !== 0) {
    return {status: "failed", message: `Last run failed${last.failingStage ? ` at ${last.failingStage}` : ""}.`}
  }
  return {status: "healthy", message: "Last run succeeded."}
}

const refreshHealthColors = {
  healthy: "#2e7d32",
  stale: "#f57f17",
  failed: "#c62828",
  unknown: "var(--theme-foreground-muted)"
}

function formatRefreshTimestamp(iso) {
  if (!iso) return "—"
  try { return new Date(iso).toLocaleString() } catch { return iso }
}

function formatRefreshDuration(secs) {
  if (secs == null) return "—"
  const m = Math.floor(secs / 60)
  const s = secs % 60
  return m > 0 ? `${m}m ${s}s` : `${s}s`
}

function formatRefreshBytes(bytes) {
  if (bytes == null) return "—"
  const gib = bytes / (1024 ** 3)
  return `${gib.toFixed(gib >= 10 ? 1 : 2)} GiB`
}
```

```js
const scheduledRefresh = job?.scheduledRefresh || {schedule: null, last: null, history: []}
const refreshHealth = classifyRefreshHealth(scheduledRefresh.last, scheduledRefresh.schedule)
const refreshHistoryNewestFirst = [...(scheduledRefresh.history || [])].reverse()
const operationalAlerts = [
  ...(operationalTruth.public?.alerts || []).map((alert) => ({...alert, domain: "Public data"})),
  ...(operationalTruth.pipeline?.issues || []).map((alert) => ({...alert, domain: "Pipeline"})),
  ...(operationalTruth.infrastructure?.issues || []).map((alert) => ({...alert, domain: "Infrastructure"}))
].sort((left, right) => (left.severity === "critical" ? 0 : 1) - (right.severity === "critical" ? 0 : 1))
const publicationOps = operationalTruth.public?.publication || {}
const publicationPreflight = publicationOps.preflight || null
const preflightCanPublish = Boolean(publicationPreflight?.eligible && publicationPreflight?.current)
const displayedChangePlan = publicationPreflight?.current
  ? {changed: publicationPreflight.changed || [], reused: publicationPreflight.reused || [], source: "Current preflight"}
  : publicationOps.currentChangePlan
    ? {...publicationOps.currentChangePlan, source: "Last publication"}
    : null
```

```js
display(html`<div class="admin-refresh-panel">
  <div class="admin-control-strip">
    <div class="admin-control-chip">
      <span class="admin-control-label">Public data</span>
      <strong style=${"color:" + (operationalTruth.public?.status === "healthy" ? "#2e7d32" : "#c62828")}>${operationalTruth.public?.status || "unknown"}</strong>
    </div>
    <div class="admin-control-chip">
      <span class="admin-control-label">Update pipeline</span>
      <strong style=${"color:" + (operationalTruth.pipeline?.status === "healthy" ? "#2e7d32" : operationalTruth.pipeline?.status === "working" ? "#1565c0" : operationalTruth.pipeline?.status === "attention" ? "#b26a00" : "#c62828")}>${operationalTruth.pipeline?.status || "unknown"}</strong>
    </div>
    <div class="admin-control-chip">
      <span class="admin-control-label">Infrastructure</span>
      <strong style=${"color:" + (operationalTruth.infrastructure?.status === "available" ? "#2e7d32" : operationalTruth.infrastructure?.status === "constrained" ? "#b26a00" : "#c62828")}>${operationalTruth.infrastructure?.status || "unknown"}</strong>
      <small>${operationalTruth.infrastructure?.namespaceMemoryLimitBytes ? `${formatRefreshBytes(operationalTruth.infrastructure.activeJobRequestedBytes + operationalTruth.infrastructure.residentServiceMemoryBytes)} / ${formatRefreshBytes(operationalTruth.infrastructure.namespaceMemoryLimitBytes)} requested` : "No capacity evidence"}</small>
    </div>
    <div class="admin-control-chip">
      <span class="admin-control-label">Last publication</span>
      <strong>${formatRefreshTimestamp(freshness.summary?.lastPublicationAt)}</strong>
    </div>
    <div class="admin-control-chip">
      <span class="admin-control-label">Artifact scrub</span>
      <strong>${operationalTruth.public?.scrub?.state || "missing"}</strong>
    </div>
    <div class="admin-control-chip">
      <span class="admin-control-label">Peak memory</span>
      <strong>${formatRefreshBytes(scheduledRefresh.last?.memoryPeakBytes)} / ${formatRefreshBytes(scheduledRefresh.last?.memoryLimitBytes)}</strong>
    </div>
  </div>
  ${operationalAlerts.length ? html`<div class="admin-health-alerts">
    ${operationalAlerts.slice(0, 10).map((alert) => html`<div class=${alert.severity || "warning"}><strong>${alert.domain} · ${alert.code.replaceAll("_", " ")}</strong><span>${alert.message}</span></div>`)}
    ${operationalAlerts.length > 10 ? html`<span>${operationalAlerts.length - 10} more operational alerts. Filter the project list to inspect each one.</span>` : ""}
  </div>` : html`<div class="admin-health-clear">Published data, update pipeline, and configured infrastructure checks pass.</div>`}
  <div class="admin-publication-actions">
    <button class="admin-btn" ?disabled=${!apiStatus} title=${actionTooltipWithApi("publication-preflight", apiStatus)} onclick=${() => runCommand("publication-preflight")}>Run publication preflight</button>
    <button class="admin-btn primary" ?disabled=${!apiStatus || !preflightCanPublish} title=${preflightCanPublish ? actionTooltipWithApi("publish", apiStatus) : "Run a current, passing publication preflight first."} onclick=${() => {
      if (confirm("Publish every validated ready candidate and atomically switch the live site?")) runCommand("publish")
    }}>Publish ready candidates</button>
    <button class="admin-btn" ?disabled=${!apiStatus} title=${actionTooltipWithApi("site", apiStatus)} onclick=${() => {
      if (confirm("Rebuild and validate only the website against the current publication?")) runCommand("site")
    }}>Rebuild site only</button>
    <details class="admin-inline-advanced"><summary>Recovery and verification</summary><div class="admin-recovery-actions">
      <button class="admin-btn" ?disabled=${!apiStatus} title=${actionTooltipWithApi("publication-recovery-audit", apiStatus)} onclick=${() => runCommand("publication-recovery-audit")}>Audit publication recovery</button>
      <button class="admin-btn" ?disabled=${!apiStatus} title=${actionTooltipWithApi("fleet-recover", apiStatus)} onclick=${() => { if (confirm("Authenticate stale fleet leases and requeue recoverable work?")) runCommand("fleet-recover") }}>Recover stale fleet leases</button>
      <button class="admin-btn" ?disabled=${!apiStatus} title=${actionTooltipWithApi("artifact-scrub", apiStatus)} onclick=${() => { if (confirm("Sequentially rehash and semantically verify every published artifact?")) runCommand("artifact-scrub") }}>Scrub published artifacts</button>
      <button class="admin-btn" ?disabled=${!apiStatus} title=${actionTooltipWithApi("merge", apiStatus)} onclick=${() => runCommand("merge")}>Regenerate merged artifacts only</button>
    </div></details>
  </div>
  <section class="admin-preflight ${publicationPreflight?.eligible ? "eligible" : publicationPreflight ? "blocked" : "missing"}">
    <header><div><span>Publication preflight</span><strong>${publicationPreflight ? publicationPreflight.current ? publicationPreflight.eligible ? "Ready to publish" : "Blocked" : "Out of date" : "Not run"}</strong></div>${publicationPreflight ? html`<time>${relativeTime(new Date(publicationPreflight.generated_at_unix * 1000).toISOString())}</time>` : ""}</header>
    ${publicationPreflight?.blockers?.length ? html`<ul>${publicationPreflight.blockers.map((blocker) => html`<li>${blocker}</li>`)}</ul>` : publicationPreflight?.eligible ? html`<p>Ready candidates, recovery state, scrub state, and current publication evidence passed.</p>` : html`<p>Run preflight to authenticate the candidate set and unlock publication.</p>`}
    ${displayedChangePlan ? html`<details open><summary>${displayedChangePlan.source} change plan · ${displayedChangePlan.changed?.length || 0} changed · ${displayedChangePlan.reused?.length || 0} reused</summary>
      <div class="admin-change-plan">
        <div><strong>Will rebuild</strong>${displayedChangePlan.changed?.length ? html`<ul>${displayedChangePlan.changed.map((item) => html`<li><code>${item.wiki}</code><span>${item.family}</span></li>`)}</ul>` : html`<p>No metric family changes.</p>`}</div>
        <div><strong>Will reuse</strong>${displayedChangePlan.reused?.length ? html`<ul>${displayedChangePlan.reused.map((item) => html`<li><code>${item.wiki}</code><span>${item.family}</span></li>`)}</ul>` : html`<p>No reusable families reported.</p>`}</div>
      </div>
    </details>` : ""}
  </section>
  ${refreshHistoryNewestFirst.length ? html`<details class="admin-history-details"><summary>Publication history (${refreshHistoryNewestFirst.length})</summary><table class="admin-refresh-history">
    <thead><tr><th>Started</th><th>Finished</th><th>Result</th><th>Duration</th><th>Peak memory</th><th>Wikis</th></tr></thead>
    <tbody>
      ${refreshHistoryNewestFirst.map(run => html`<tr>
        <td>${formatRefreshTimestamp(run.startedAt)}</td>
        <td>${formatRefreshTimestamp(run.finishedAt)}</td>
        <td style=${"color:" + (run.state === "succeeded" || run.exitCode === 0 ? "#2e7d32" : "#c62828")}>${run.state === "succeeded" || run.exitCode === 0 ? "Success" : `Failed (${run.exitCode})`}</td>
        <td>${formatRefreshDuration(run.durationSecs)}</td>
        <td>${formatRefreshBytes(run.memoryPeakBytes)} / ${formatRefreshBytes(run.memoryLimitBytes)}</td>
        <td>${(run.wikis || []).join(", ") || "—"}</td>
      </tr>`)}
    </tbody>
  </table></details>` : html`<p class="filter-desc">No publisher runs recorded yet.</p>`}
</div>`)
```

</div>

<!-- ── Data quality ledger ───────────────────────────────── -->

<div id="admin-view-quality" class="chart-section" data-admin-view="quality">

## Data quality

<div class="note">Candidate receipts are compared with the currently published receipts without reopening large Parquet files. Warnings account for the number of elapsed snapshot months; algorithm changes are shown but are not treated as comparable trends.</div>

```js
function qualityInteger(value) {
  return Number.isFinite(Number(value)) ? Number(value).toLocaleString() : "—"
}

function qualityPercent(value) {
  return Number.isFinite(Number(value)) ? `${Number(value) >= 0 ? "+" : ""}${(Number(value) * 100).toFixed(1)}%` : "—"
}

function qualityHash(value) {
  return typeof value === "string" && value.length >= 12 ? `${value.slice(0, 12)}…` : "—"
}

function qualityAge(seconds) {
  if (!Number.isFinite(Number(seconds))) return "Not scrubbed"
  const days = Number(seconds) / 86400
  return days < 1 ? `${Math.max(0, Math.floor(Number(seconds) / 3600))}h ago` : `${Math.floor(days)}d ago`
}

function qualityTotals(evidence) {
  const totals = Object.entries(evidence?.totals || {})
  return totals.length ? totals.map(([name, value]) => `${name.replaceAll("_", " ")} ${qualityInteger(value)}`).join(" · ") : "No conservation total"
}

function qualityEvidence(scope, evidence) {
  if (!evidence) return html`<div class="admin-quality-empty">No ${scope} artifact</div>`
  if (!evidence.valid) return html`<div class="admin-quality-invalid">Receipt invalid</div>`
  return html`<div class="admin-quality-evidence">
    <div><strong>${qualityInteger(evidence.rows)}</strong><span>rows · ${bytesLabel(evidence.bytes)}</span></div>
    <span>${evidence.minimumDate || "—"} → ${evidence.maximumDate || "—"}</span>
    <span>${qualityTotals(evidence)}</span>
    <span class=${evidence.schemaMatches ? "pass" : "fail"}>Schema ${evidence.schema?.length || 0} fields · ${evidence.schemaMatches ? "matches" : "mismatch"}</span>
    <span class=${evidence.algorithmMatches ? "pass" : "fail"} title=${evidence.algorithmVersion || ""}>Algorithm ${evidence.algorithmMatches ? "current" : "mismatch"}</span>
    <code title=${evidence.artifactSha256 || ""}>data ${qualityHash(evidence.artifactSha256)}</code>
    <code title=${evidence.receiptSha256 || ""}>receipt ${qualityHash(evidence.receiptSha256)}</code>
    ${scope === "published" ? html`<span class=${evidence.scrubAgeSeconds == null ? "muted" : "pass"}>${qualityAge(evidence.scrubAgeSeconds)}</span>` : ""}
  </div>`
}

const qualityEntries = Object.values(operationalWikiTruth)
  .filter((wiki) => wiki?.quality?.metrics?.length)
  .sort((left, right) => {
    const severity = (entry) => entry.quality.anomalies.some((item) => item.severity === "critical") ? 0
      : entry.quality.anomalies.length ? 1 : 2
    return severity(left) - severity(right) || left.wiki.localeCompare(right.wiki)
  })
const qualityMetricCount = qualityEntries.reduce((sum, wiki) => sum + wiki.quality.metrics.length, 0)
const qualityAnomalies = qualityEntries.flatMap((wiki) => wiki.quality.anomalies.map((anomaly) => ({...anomaly, wiki: wiki.wiki})))
```

```js
display(qualityEntries.length ? html`<div class="admin-quality-ledger">
  <div class="admin-quality-summary">
    <div><strong>${qualityEntries.length}</strong><span>wikis evidenced</span></div>
    <div><strong>${qualityMetricCount}</strong><span>metric contracts</span></div>
    <div class=${qualityAnomalies.length ? "warning" : "healthy"}><strong>${qualityAnomalies.length}</strong><span>anomalies</span></div>
    <div><strong>${qualityEntries.filter((wiki) => wiki.quality.scrub?.stale).length}</strong><span>stale scrubs</span></div>
  </div>
  ${qualityEntries.map((wiki) => {
    const quality = wiki.quality
    const critical = quality.anomalies.filter((item) => item.severity === "critical").length
    const warning = quality.anomalies.length - critical
    return html`<details class="admin-quality-wiki" ?open=${critical > 0 || selectedWiki === wiki.wiki}>
      <summary onclick=${() => setSelectedWiki(wiki.wiki)}>
        <span class="admin-quality-wiki-name"><strong>${wiki.wiki}</strong><small>${quality.publishedSnapshot || "—"} published → ${quality.candidateSnapshot || "—"} candidate</small></span>
        <span class="admin-quality-counts">
          ${critical ? html`<b class="critical">${critical} critical</b>` : ""}
          ${warning ? html`<b class="warning">${warning} warning</b>` : ""}
          ${!quality.anomalies.length ? html`<b class="healthy">Checks pass</b>` : ""}
          <small>scrub ${qualityAge(quality.scrub?.ageSeconds)}</small>
        </span>
      </summary>
      ${quality.anomalies.length ? html`<div class="admin-quality-alerts">${quality.anomalies.map((anomaly) => html`<div class=${anomaly.severity}><strong>${anomaly.metric || anomaly.signal?.replaceAll("_", " ") || "Quality"}</strong><span>${anomaly.message}</span></div>`)}</div>` : ""}
      <div class="admin-quality-signal-strip">
        ${quality.signals.map((signal) => html`<div class=${signal.anomaly?.severity || "neutral"}>
          <span>${signal.label}</span>
          <strong>${qualityInteger(signal.published)} → ${qualityInteger(signal.candidate)}</strong>
          <small>${signal.anomaly ? qualityPercent(signal.anomaly.fraction) : signal.published == null || signal.candidate == null ? "Not yet recorded" : "Within policy"}</small>
        </div>`)}
      </div>
      <div class="admin-quality-table-wrap"><table class="admin-quality-table">
        <thead><tr><th>Metric contract</th><th>Published evidence</th><th>Candidate evidence</th><th>Change</th></tr></thead>
        <tbody>${quality.metrics.map((metric) => html`<tr class=${metric.status}>
          <th><strong>${metric.id}</strong><span>${metric.family}</span><small title=${metric.expectedAlgorithmVersion}>${metric.expectedSchema.length} expected fields</small></th>
          <td data-label="Published">${qualityEvidence("published", metric.published)}</td>
          <td data-label="Candidate">${qualityEvidence("candidate", metric.candidate)}</td>
          <td data-label="Change"><div class="admin-quality-change">
            <strong>${metric.comparison?.rows?.delta == null ? "Not comparable" : `${metric.comparison.rows.delta >= 0 ? "+" : ""}${qualityInteger(metric.comparison.rows.delta)} rows`}</strong>
            <span>${metric.comparison?.changed == null ? "Evidence incomplete" : metric.comparison.changed ? "Content changed" : "Identical bytes"}</span>
            ${metric.comparison?.algorithmComparable === false ? html`<small>Algorithm changed; trend alarms suppressed</small>` : ""}
          </div></td>
        </tr>`)}</tbody>
      </table></div>
    </details>`
  })}
</div>` : html`<div class="admin-empty-state"><strong>No authenticated metric receipts are available.</strong><span>Quality evidence appears after the admin API reads a ready candidate or publication gate.</span></div>`)
```

</div>

<!-- ── Pipeline status matrix ─────────────────────────────── -->

<div id="admin-view-wikis" class="chart-section" data-admin-view="wikis">

## Pipeline Status

<div class="note">Ordered by operator urgency: stalled and failed work first, then active and incomplete projects, with healthy published wikis last. Select a row for evidence and controls.</div>

```js
const statusColors = {
  complete: "#2e7d32",
  needs_fetch: "#c62828",
  needs_patrol_fetch: "#6a1b9a",
  needs_ingest: "#e65100",
  needs_compute: "#f57f17",
  needs_patrol_compute: "#8e24aa",
  needs_merge: "#1565c0",
  running: "#1565c0",
  queued: "#5c6bc0",
  waiting_upstream: "#7e6ab0",
  planned: "#607d8b",
  stalled: "#c62828",
  quarantined: "#c62828",
  interrupted: "#c62828",
  failed: "#c62828",
  cancelled: "#795548"
}
const statusLabels = {
  complete: "Published",
  needs_fetch: "Not prepared",
  needs_patrol_fetch: "Patrol source needed",
  needs_ingest: "Data conversion needed",
  needs_compute: "Metrics needed",
  needs_patrol_compute: "Patrol metrics needed",
  needs_merge: "Ready to publish",
  running: "Working",
  queued: "Waiting for worker",
  waiting_upstream: "Waiting for Wikimedia",
  planned: "Not managed",
  stalled: "Worker stopped reporting",
  quarantined: "Needs intervention",
  interrupted: "Interrupted",
  failed: "Needs attention",
  cancelled: "Cancelled"
}

const pipelineSteps = [
  {key: "source", label: "Data secured"},
  {key: "metrics", label: "Metrics built"},
  {key: "validation", label: "Checks passed"},
  {key: "publication", label: "Published"},
]

const technicalStageOrder = {
  snapshot_resolve: 0,
  patrol_preflight: 0,
  fetch: 0,
  source_window: 0,
  ingest: 0,
  patrol_fetch: 0,
  compute: 1,
  patrol_compute: 1,
  candidate_validate: 2,
  candidate_ready: 2,
  merge: 3,
  publication_prepare: 3,
  publication_verify: 3,
  site: 3,
  publication_commit: 3,
}

function summarizeStatuses(entries) {
  return entries.reduce((acc, [, wiki]) => {
    const key = wiki.status || "needs_fetch"
    acc[key] = (acc[key] || 0) + 1
    return acc
  }, {})
}

function metricCompletenessForMilestone(name, lifecycle) {
  const truth = operationalWikiTruth[name]
  if (!truth) return null
  const candidateIsNewer = truth.snapshots?.candidate && (!truth.snapshots?.published || truth.snapshots.candidate > truth.snapshots.published)
  return lifecycle?.publication === "hidden" || candidateIsNewer ? truth.metrics?.candidate : truth.metrics?.published
}

function milestoneState(name, wiki, milestoneKey, lifecycle, direct, state) {
  const truth = operationalWikiTruth[name]
  const publishedComplete = truth?.metrics?.published?.complete === true
  const candidateIsNewer = truth?.snapshots?.candidate && (!truth.snapshots?.published || truth.snapshots.candidate > truth.snapshots.published)
  const hasLivePublication = lifecycle?.publication === "published" && publishedComplete
  const hiddenQualification = lifecycle?.publication === "hidden" && lifecycle?.refresh === "qualification"
  if (milestoneKey === "publication" && hasLivePublication && !candidateIsNewer) return "done"
  if (hasLivePublication && state === "complete" && !candidateIsNewer) return "done"
  if (milestoneKey === "publication" && hiddenQualification) return "not-applicable"

  const stage = direct?.progress?.stage || direct?.stage || null
  const activeIndex = technicalStageOrder[stage]
  const milestoneIndex = pipelineSteps.findIndex((step) => step.key === milestoneKey)
  const failed = ["failed", "interrupted", "quarantined", "stalled"].includes(state)
  if (Number.isInteger(activeIndex)) {
    if (milestoneIndex < activeIndex) return "done"
    if (milestoneIndex === activeIndex) return failed ? "issue" : ["running", "cancelling"].includes(state) ? "active" : "issue"
  }

  if (milestoneKey === "source") {
    if ((direct?.progress?.completedSources || 0) >= (direct?.progress?.totalSources || Number.MAX_SAFE_INTEGER)) return "done"
    if (wiki.snapshot?.ready || wiki.ingest?.ready || wiki.raw?.files > 0) return "done"
  }
  if (milestoneKey === "metrics" && metricCompletenessForMilestone(name, lifecycle)?.complete) return "done"
  if (milestoneKey === "validation" && truth?.ready?.snapshot === truth?.snapshots?.candidate
      && metricCompletenessForMilestone(name, lifecycle)?.complete) return "done"
  return "future"
}

function milestoneCaption(name, wiki, milestoneKey, lifecycle, direct, state) {
  const truth = operationalWikiTruth[name]
  const selectedSnapshot = direct?.selectedSnapshot || truth?.snapshots?.candidate || wiki.snapshot?.version || null
  const milestoneStateValue = milestoneState(name, wiki, milestoneKey, lifecycle, direct, state)
  if (milestoneStateValue === "active") return direct?.progress?.detail || direct?.stageLabel || "In progress"
  if (milestoneStateValue === "issue") return `Stopped at ${direct?.stageLabel || "this step"}`
  if (milestoneStateValue === "not-applicable") return "Hidden qualification"
  if (milestoneKey === "source") {
    if (wiki.snapshot?.mode === "retained-publication" && wiki.status === "complete") return "Validated; inputs retired"
    return selectedSnapshot ? `Snapshot ${selectedSnapshot}` : milestoneStateValue === "done" ? "Ready" : "Not started"
  }
  if (milestoneKey === "metrics") {
    const completeness = metricCompletenessForMilestone(name, lifecycle)
    return completeness ? `${completeness.present.length}/${completeness.expected.length} required` : "No receipt evidence"
  }
  if (milestoneKey === "validation") return milestoneStateValue === "done" ? "Candidate accepted" : "Not run"
  if (milestoneKey === "publication") return milestoneStateValue === "done"
    ? `Live · ${truth?.snapshots?.published || selectedSnapshot || "unknown"}`
    : truth?.snapshots?.published ? `Current live · ${truth.snapshots.published}` : "Not public"
  return ""
}

function stateExplanation(name, wiki, state, lifecycle, direct, fleetWork) {
  if (!lifecycle) return `${name} is supported by the Rust source resolver, but has no lifecycle policy. Processing is intentionally blocked until an operator registers it.`
  if (state === "queued") {
    const position = direct?.queuePosition ? `Queue position ${direct.queuePosition}. ` : ""
    const pickup = direct?.waitingForActiveOperation
      ? "It will start after the active operator operation finishes."
      : direct?.waitingForEarlierRequest
        ? "It will start after the earlier queued operator requests finish."
        : direct?.earliestDispatchAt
          ? `Earliest scheduled pickup is ${formatRefreshTimestamp(direct.earliestDispatchAt)} (${relativeTime(direct.earliestDispatchAt)}).`
          : "It is waiting for the scheduled operator worker."
    return `${position}${pickup} The request is durable, so it is safe to close this page.`
  }
  if (state === "waiting_upstream") {
    const cause = direct?.errorSummary || fleetWork?.error || "Wikimedia has not finished the logging dump required by this snapshot."
    const retry = direct?.earliestDispatchAt
      ? ` The admin will check again ${relativeTime(direct.earliestDispatchAt)} (${formatRefreshTimestamp(direct.earliestDispatchAt)}).`
      : " The worker will check again automatically."
    return `${cause}${retry}`
  }
  if (state === "running") return `${direct?.stageLabel || "Pipeline work"} is in progress${direct?.progress?.detail ? `: ${direct.progress.detail}` : ""}. The worker heartbeat is current.`
  if (state === "stalled") return `The worker lease exists but its heartbeat is overdue. Recover the fleet lease before submitting duplicate work.`
  if (state === "quarantined") return `Automatic retries were exhausted. Review the final log excerpt, correct the cause, then explicitly retry.`
  if (["failed", "interrupted"].includes(state)) return direct?.errorSummary || `The last operation did not finish. Completed source transactions remain reusable; retrying resumes from validated receipts rather than starting blindly from zero.`
  if (state === "needs_fetch") return `No validated history source or selected snapshot is available yet. Fetch is the first unblocked stage.`
  if (state === "needs_patrol_fetch") return `Core history is present, but the independent patrol source generation is incomplete.`
  if (state === "needs_ingest") return `History sources exist but have not all been converted into validated metric-input fragments.`
  if (state === "needs_compute") return `The warehouse generation is ready, but one or more core metric families are missing or invalid.`
  if (state === "needs_patrol_compute") return `Patrol sources are ready, but the derived patrol metric has not been validated.`
  if (state === "needs_merge") return `Per-wiki metrics are ready. They have not yet been incorporated into the public publication generation.`
  if (state === "complete") {
    if (lifecycle.publication === "published") return `${name} is live with a complete published artifact set. Redownloadable build inputs may be intentionally retired after validation to save storage.`
    if (lifecycle.refresh === "paused") return `${name} is a retained imported dataset. It remains public, but automatic refresh is paused.`
    return `${name} has a complete artifact set.`
  }
  if (fleetWork) return `Fleet state ${fleetWork.state} was inferred from the durable work item and lease evidence.`
  return `The state was inferred from lifecycle policy, snapshot receipts, source markers, metric artifacts, and publication files.`
}

function evidenceItems(name, wiki, lifecycle, direct, fleetWork, plan) {
  const truth = operationalWikiTruth[name]
  const metricTruth = metricCompletenessForMilestone(name, lifecycle)
  const sourceProgress = direct?.progress
  const sourceEvidence = sourceProgress?.totalSources
    ? `${sourceProgress.completedSources || 0}/${sourceProgress.totalSources} source files · ${formatRefreshBytes(sourceProgress.downloadedBytes)}`
    : wiki.snapshot?.ready
      ? `${wiki.ingest?.rows || 0} validated rows`
      : wiki.retention?.valid && wiki.retention?.history_input === "purge_after_ready" && wiki.status === "complete"
        ? "validated, then retired by policy"
        : `${wiki.raw?.files || 0} raw files · ${wiki.parquet?.done || 0}/${wiki.parquet?.total || 0} ingested`
  return [
    ["Lifecycle", lifecycle ? `${lifecycle.publication} / ${lifecycle.refresh}` : "not registered"],
    ["Latest available", truth?.snapshots?.latestAvailable || plan?.snapshot || "not discovered"],
    ["Candidate", truth?.snapshots?.candidate || "none"],
    ["Published", truth?.snapshots?.published ? `${truth.snapshots.published} · cutoff ${truth.snapshots.cutoff || "unknown"}` : "not published"],
    ["Source data", sourceEvidence],
    ["Metrics", metricTruth ? `${metricTruth.present.length}/${metricTruth.expected.length} required${metricTruth.missing.length ? ` · missing ${metricTruth.missing.join(", ")}` : ""}` : "no receipt evidence"],
    ["Last activity", direct ? `${operationLabel(direct.state || (direct.exitCode === 0 ? "succeeded" : "failed"))} · ${relativeTime(operationTimestamp(direct))}` : "no operator run recorded"],
    ["Worker", fleetWork ? `${fleetWork.workerId || fleetWork.resourceClass || "unclaimed"} · ${fleetWork.heartbeatAt ? `heartbeat ${relativeTime(fleetWork.heartbeatAt)}` : "no heartbeat"}` : "no fleet lease"]
  ]
}

function typedOperatorConfirmation(summary, token) {
  const entered = prompt(`${summary}\n\nType ${token} to continue.`)
  return entered === token
}

function lifecycleControls(name, lifecycle, direct) {
  if (!lifecycle) return ""
  const truth = operationalWikiTruth[name] || {}
  const operationActive = ["queued", "waiting_upstream", "running", "cancelling"].includes(direct?.state) || direct?.running
  const registryBusy = Boolean(job?.running || adminOperations.counts?.running || adminOperations.counts?.queued)
  const controlsDisabled = !apiStatus || operationActive || registryBusy || !lifecycleRevision
  const resourceSelect = html`<select class="admin-lifecycle-select" aria-label=${`${name} resource class`}>
    ${[
      ["Small", "small"],
      ["Medium / large", "medium_large"],
      ["Isolated qualification", "isolated"]
    ].map(([label, value]) => html`<option value=${value} selected=${(lifecycle.fleet_resource_class || "medium_large") === value}>${label}</option>`)}
  </select>`
  const slaInput = html`<input class="admin-lifecycle-number" type="number" min="1" max="365" step="1" value=${lifecycle.freshness_sla_days || 10} aria-label=${`${name} freshness SLA in days`}>`

  if (lifecycle.publication === "hidden" && lifecycle.refresh === "qualification") {
    const qualification = (qualifications[name] || []).find((entry) => entry.structurallyValid)
    const refreshSelect = html`<select class="admin-lifecycle-select" aria-label=${`${name} schedule after promotion`}>
      <option value="manual" selected>Manual updates</option>
      <option value="scheduled">Scheduled updates</option>
    </select>`
    return html`<section class="admin-lifecycle-console qualification">
      <header><div><span>Lifecycle decision</span><strong>Qualification remains invisible</strong></div><code>${lifecycleRevision?.slice(0, 12) || "no revision"}</code></header>
      ${qualification ? html`
        <div class="admin-lifecycle-evidence">
          <div><span>Qualified snapshot</span><strong>${qualification.snapshot}</strong></div>
          <div><span>Candidate identity</span><code>${qualification.runId}</code></div>
          <div><span>Artifacts</span><strong>${qualification.artifactCount}</strong></div>
          <div><span>Cutoff</span><strong>${qualification.cutoffDate || "not reported"}</strong></div>
        </div>
        <div class="admin-lifecycle-policy">${refreshSelect}${resourceSelect}${slaInput}</div>
        <div class="admin-lifecycle-actions">
          <button class="admin-btn primary" title=${actionTooltipWithApi("promote-qualification", apiStatus)} ?disabled=${controlsDisabled} onclick=${() => {
            const token = `promote ${name}`
            if (!typedOperatorConfirmation(`Promote qualification ${qualification.runId} at ${qualification.snapshot}. Lifecycle changes only after the immutable candidate is validated.`, token)) return
            runCommand("promote-qualification", {
              wiki: name,
              version: qualification.snapshot,
              qualificationRunId: qualification.runId,
              lifecycleRevision,
              refresh: refreshSelect.value,
              resourceClass: resourceSelect.value,
              freshnessSlaDays: Number(slaInput.value)
            })
          }}>Promote exact qualification</button>
        </div>` : html`<p class="admin-lifecycle-empty">No structurally valid qualification receipt is available. Complete or resume qualification before promotion.</p>`}
    </section>`
  }

  const ready = truth.ready || null
  const active = truth.activePublished || null
  const readyIsUnpublished = Boolean(ready?.run_id && ready.run_id !== active?.run_id)
  const exactSnapshot = ready?.snapshot || truth.snapshots?.candidate || truth.snapshots?.published || null
  const resumeSelect = html`<select class="admin-lifecycle-select" aria-label=${`${name} resume schedule`}>
    <option value="scheduled" selected>Scheduled updates</option>
    <option value="manual">Manual updates</option>
  </select>`
  return html`<section class="admin-lifecycle-console">
    <header><div><span>Lifecycle policy</span><strong>${lifecycle.refresh === "paused" ? "Paused safely" : lifecycle.refresh === "scheduled" ? "Scheduled and managed" : "Operator-triggered"}</strong></div><code>${lifecycleRevision?.slice(0, 12) || "no revision"}</code></header>
    <div class="admin-lifecycle-policy">
      ${lifecycle.refresh === "paused" ? resumeSelect : html`<div class="admin-lifecycle-readonly"><span>Refresh</span><strong>${lifecycle.refresh}</strong></div>`}
      ${resourceSelect}
      ${slaInput}
    </div>
    <div class="admin-lifecycle-actions">
      ${lifecycle.refresh === "paused" ? html`<button class="admin-btn primary" ?disabled=${controlsDisabled} onclick=${() => runCommand("update-lifecycle", {
        wiki: name, operation: "resume", refresh: resumeSelect.value,
        freshnessSlaDays: Number(slaInput.value), lifecycleRevision
      })}>Resume scheduling</button>` : html`<button class="admin-btn" ?disabled=${controlsDisabled} onclick=${() => {
        if (confirm(`Pause ${name} scheduling? Published data remains live and operator-triggered rebuilds remain available.`)) {
          runCommand("update-lifecycle", {wiki: name, operation: "pause", lifecycleRevision})
        }
      }}>Pause scheduling</button>`}
      <button class="admin-btn" ?disabled=${controlsDisabled} onclick=${() => runCommand("update-lifecycle", {
        wiki: name, operation: "configure", resourceClass: resourceSelect.value,
        freshnessSlaDays: Number(slaInput.value), lifecycleRevision
      })}>Save resource &amp; SLA policy</button>
      <button class="admin-btn" title=${actionTooltipWithApi("rebuild-candidate", apiStatus)} ?disabled=${!apiStatus || operationActive || !exactSnapshot} onclick=${() => {
        const token = `rebuild ${name}`
        if (!typedOperatorConfirmation(`Start a clean immutable rebuild of ${name} snapshot ${exactSnapshot}. The live and existing ready candidates will not be changed.`, token)) return
        runCommand("rebuild-candidate", {wiki: name, version: exactSnapshot, candidateRunId: ready?.run_id || null})
      }}>Rebuild exact snapshot</button>
      ${readyIsUnpublished ? html`<button class="admin-btn danger" title=${actionTooltipWithApi("retire-candidate", apiStatus)} ?disabled=${!apiStatus || operationActive} onclick=${() => {
        const token = `retire ${name}/${ready.run_id}`
        if (!typedOperatorConfirmation(`Retire unpublished candidate ${ready.run_id} at ${ready.snapshot}. This cannot target the live or rollback generation.`, token)) return
        runCommand("retire-candidate", {wiki: name, version: ready.snapshot, candidateRunId: ready.run_id})
      }}>Retire unpublished candidate</button>` : ""}
    </div>
    <p class="admin-lifecycle-footnote">Policy writes use revision locking. Candidate actions use exact snapshot and run identities. Every request and result is retained in the operator audit ledger.</p>
  </section>`
}

function pipelineDossier(name, wiki, state, lifecycle, direct, fleetWork, plan) {
  const canRun = Boolean(apiStatus && lifecycle)
  const isQualification = lifecycle?.publication === "hidden" && lifecycle?.refresh === "qualification"
  const operationActive = ["queued", "waiting_upstream", "running", "cancelling"].includes(direct?.state) || direct?.running
  const log = (direct?.log || []).join("")
  const progress = direct?.progress || null
  const progressPercent = Number.isFinite(progress?.percent) ? progress.percent : null
  const stoppedWithExplanation = direct?.errorSummary && ["failed", "interrupted", "quarantined", "waiting_upstream"].includes(state)
  const failureState = ["failed", "interrupted", "quarantined", "stalled"].includes(state)
  const allowedActions = operationalWikiTruth[name]?.allowedActions || []
  return html`<section class="admin-pipeline-dossier" aria-label=${`${name} pipeline details`}>
    <div class="admin-dossier-lead ${operationTone(state)}">
      <span class="admin-command-kicker">${statusLabels[state] || operationLabel(state)}</span>
      <strong>${state === "running" ? direct?.stageLabel || "Pipeline running" : state === "failed" ? `Stopped during ${direct?.stageLabel || "pipeline work"}` : state === "waiting_upstream" ? "Waiting for upstream data" : wikipediaProjectLabel(name)}</strong>
      <p>${state === "waiting_upstream"
        ? stateExplanation(name, wiki, state, lifecycle, direct, fleetWork)
        : stoppedWithExplanation
        ? "No candidate was published. Validated source transactions remain reusable; the specific cause is shown below."
        : stateExplanation(name, wiki, state, lifecycle, direct, fleetWork)}</p>
      ${progressPercent != null && operationActive ? html`<div class="admin-human-progress" aria-label=${`${progressPercent}% of source files complete`}>
        <div><span>${progress?.detail || "Working"}</span><strong>${progressPercent}%</strong></div>
        <div class="admin-human-progress-track"><i style=${`width:${progressPercent}%`}></i></div>
        <small>${progress?.ingestedRows ? `${progress.ingestedRows.toLocaleString()} rows ingested` : ""}${progress?.downloadedBytes ? ` · ${formatRefreshBytes(progress.downloadedBytes)} transferred` : ""}</small>
      </div>` : ""}
      ${stoppedWithExplanation ? html`<div class="admin-human-error"><strong>Why it stopped</strong><span>${direct.errorSummary}</span>${direct.remediation ? html`<small>${direct.remediation}</small>` : ""}</div>` : ""}
    </div>
    <div class="admin-milestone-line" aria-label="Project lifecycle">
      ${pipelineSteps.map((step) => {
        const stepState = milestoneState(name, wiki, step.key, lifecycle, direct, state)
        return html`<div class="admin-milestone ${stepState}">
          <i aria-hidden="true"></i><span>${step.label}</span><strong>${milestoneCaption(name, wiki, step.key, lifecycle, direct, state)}</strong>
        </div>`
      })}
    </div>
    <dl class="admin-dossier-facts">${evidenceItems(name, wiki, lifecycle, direct, fleetWork, plan).map(([label, value]) => html`<div><dt>${label}</dt><dd>${value}</dd></div>`)}</dl>
    <div class="admin-dossier-actions">
      ${!lifecycle ? html`<button class="admin-btn primary" ?disabled=${!apiStatus} onclick=${() => {
        if (confirm(`Add ${name} as a publication-invisible qualification project?`)) registerWiki(name, "qualification", "medium_large")
      }}>Add as qualification</button>` : failureState ? html`
        ${allowedActions.length ? allowedActions.map((action) => html`<button class="admin-btn ${action.id === "quarantine-retry" ? "danger" : ""}" ?disabled=${!apiStatus} onclick=${() => executeAllowedAction({...action, wiki: action.wiki || name, taskId: action.taskId || fleetWork?.taskId})}>${action.label}</button>`) : html`<span class="admin-action-blocked">No automated retry is safe. Complete the stated remediation first.</span>`}
        ${direct?.requestId && state === "stalled" ? html`<button class="admin-btn" ?disabled=${!apiStatus} onclick=${() => runCommand("recover-admin")}>Recover operator queue</button>` : ""}
      ` : html`
        <button class="admin-btn primary" ?disabled=${!apiStatus || operationActive}
          onclick=${() => {
            const blocked = direct?.state === "failed" && direct?.retryable === false
            if (blocked && !confirm(`${direct.errorSummary}\n\n${direct.remediation}\n\nConfirm only after that remediation has been completed.`)) return
            runCommand(isQualification ? "qualify" : "run", {
              wiki: name,
              version: preferredSnapshotVersion(),
              acknowledgeBlockedRetry: blocked
            })
          }}>
          ${direct?.state === "failed" && direct?.retryable === false ? "Retry after remediation" : isQualification ? "Run full qualification" : "Prepare full update"}
        </button>
        ${isQualification
          ? html`<button class="admin-btn" ?disabled=${!apiStatus || operationActive} onclick=${() => runCommand("qualify", {wiki: name, version: preferredSnapshotVersion()})}>Resume when patrol is ready</button>`
          : html`<button class="admin-btn" ?disabled=${!apiStatus || operationActive} onclick=${() => runCommand("patrol-rebuild", name)}>Refetch and rebuild patrol</button>`}
        <button class="admin-btn" ?disabled=${!apiStatus} onclick=${() => runCommand("cleanup", name)}>Clean stale staging</button>`}
      ${operationActive ? html`<button class="admin-btn danger" ?disabled=${!apiStatus} onclick=${() => runCommand("cancel", {requestId: direct?.requestId, wiki: name})}>Cancel operation</button>` : ""}
    </div>
    ${lifecycleControls(name, lifecycle, direct)}
    ${lifecycle ? html`<details class="admin-advanced-actions"><summary>Advanced stage controls</summary><div>
      <button class="admin-btn" ?disabled=${!canRun || operationActive} onclick=${() => runCommand("fetch", {wiki: name, version: preferredSnapshotVersion()})}>Fetch history</button>
      <button class="admin-btn" ?disabled=${!canRun || operationActive} onclick=${() => runCommand("ingest", name)}>Ingest</button>
      <button class="admin-btn" ?disabled=${!canRun || operationActive} onclick=${() => runCommand("compute", name)}>Compute metrics</button>
      ${!isQualification ? html`
        <button class="admin-btn" ?disabled=${!canRun || operationActive} onclick=${() => runCommand("patrol-fetch", name)}>Fetch patrol</button>
        <button class="admin-btn" ?disabled=${!canRun || operationActive} onclick=${() => runCommand("patrol-compute", name)}>Refresh patrol</button>
      ` : ""}
      <button class="admin-btn" ?disabled=${!canRun || operationActive} onclick=${() => runCommand("cleanup", name)}>Clean staging</button>
    </div></details>` : ""}
    ${log ? html`<details class="admin-dossier-log"><summary>Latest output (${(direct.log || []).length} chunks)</summary><pre class="admin-job-log">${log}</pre></details>` : ""}
  </section>`
}

function projectRowDetail(name, wiki, state, lifecycle, direct, fleetWork) {
  if (state === "running") return direct?.progress?.detail || direct?.stageLabel || "Pipeline work is in progress"
  if (state === "queued") return "Saved safely; waiting for the next available worker"
  if (["failed", "interrupted", "quarantined", "stalled", "waiting_upstream"].includes(state)) {
    return direct?.errorSummary || stateExplanation(name, wiki, state, lifecycle, direct, fleetWork)
  }
  if (state === "complete" && lifecycle?.refresh === "paused") return "Published imported data · automatic updates paused"
  if (state === "complete") return `Snapshot ${direct?.selectedSnapshot || wiki.snapshot?.version || "published"} is live and usable`
  return stateExplanation(name, wiki, state, lifecycle, direct, fleetWork)
}

function snapshotProgressLabel(name) {
  const snapshots = operationalWikiTruth[name]?.snapshots
  if (!snapshots) return "Snapshot evidence unavailable"
  return `Published ${snapshots.published || "—"} · candidate ${snapshots.candidate || "—"} · latest ${snapshots.latestAvailable || "—"}`
}

```

```js
const statusSummary = summarizeStatuses(wikiEntries)
const pipelineFilterInput = Inputs.radio(
  ["All", "Attention", "Active", "Incomplete"],
  {label: "Show", value: "All"}
)
const pipelineFilter = view(pipelineFilterInput)
```

```js
const visibleWikiEntries = wikiEntries.filter(([name, wiki]) => {
  const state = operationalState(name, wiki)
  if (pipelineFilter === "Attention") return attentionStates.has(state)
  if (pipelineFilter === "Active") return ["running", "queued", "stalled"].includes(state)
  if (pipelineFilter === "Incomplete") return state !== "complete"
  return true
})

display(html`<div class="admin-pipeline-board">
  <div class="admin-pipeline-summary concise">
    <div><strong>${wikiEntries.length}</strong><span>known wikis</span></div>
    <div><strong>${activeCount}</strong><span>running</span></div>
    <div class=${attentionCount ? "danger" : ""}><strong>${attentionCount}</strong><span>need attention</span></div>
    <div><strong>${statusSummary.complete || 0}</strong><span>complete</span></div>
  </div>
  <div class="admin-pipeline-toolbar">${pipelineFilterInput}<span>${visibleWikiEntries.length} shown</span></div>
  ${visibleWikiEntries.length === 0
    ? html`<div class="admin-empty-state"><strong>Nothing matches this view.</strong><span>Choose another filter to inspect the full inventory.</span></div>`
    : html`<div class="admin-pipeline-list" role="list">
        ${visibleWikiEntries.map(([name, wiki]) => {
          const lifecycle = lifecycleStates[name] || null
          const direct = latestWikiJob(name)
          const fleetWork = fleetByWiki.get(name)
          const plan = latestPlanByWiki.get(name)
          const state = operationalState(name, wiki)
          const isRunning = ["running", "cancelling"].includes(state)
          const stageDetail = isRunning
            ? `${direct?.stageLabel || direct?.stage || "Starting"}${direct?.progress?.percent != null ? ` · ${direct.progress.percent}%` : ""}`
            : snapshotProgressLabel(name)
          const expanded = selectedWiki === name
          return html`<div class="admin-pipeline-entry" role="listitem">
          <button
            class="admin-pipeline-row ${expanded ? "selected" : ""} state-${state}"
            aria-expanded=${String(expanded)}
            onclick=${() => { setSelectedWiki(name) }}>
            <span class="admin-pipeline-identity">
              <strong>${name}</strong>
              <small>${wikipediaProjectLabel(name).replace(` (${name})`, "")}</small>
            </span>
            <span class="admin-pipeline-state">
              <i style=${`--state-color:${statusColors[state] || "#607d8b"}`}></i>
              <span><strong>${statusLabels[state] || operationLabel(state)}</strong><small>${stageDetail}</small></span>
            </span>
            <span class="admin-pipeline-message">${projectRowDetail(name, wiki, state, lifecycle, direct, fleetWork)}</span>
            <span class="admin-stage-rail" aria-label="Project lifecycle">
              ${pipelineSteps.map((step) => {
                const stepState = milestoneState(name, wiki, step.key, lifecycle, direct, state)
                return html`<i class=${stepState} title=${`${step.label}: ${milestoneCaption(name, wiki, step.key, lifecycle, direct, state)}`}><span>${step.label}</span></i>`
              })}
            </span>
            <span class="admin-row-chevron" aria-hidden="true">${expanded ? "⌄" : "›"}</span>
          </button>
          ${expanded ? pipelineDossier(name, wiki, state, lifecycle, direct, fleetWork, plan) : ""}
          </div>`
        })}
      </div>`}
</div>`)
```

</div>

<!-- ── Fetch a new wiki ───────────────────────────────────── -->

<div class="chart-section" data-admin-view="wikis">

## Start or inspect a project

<div class="note">Only lifecycle-registered projects should be processed. An unregistered project can be inspected, but must first be added as a publication-invisible qualification project before downloading or computing data.</div>

```js
// Searchable project picker. It starts empty by default, opens the full
// project list when the field is clicked, and filters in place as the
// operator types either a wiki code or a language name.
const onboardingWikiOptions = supportedWikis
const onboardingWikiOptionsSet = new Set(onboardingWikiOptions)
const onboardingWikiInitial = onboardingWikiOptionsSet.has(adminUiState.onboardingWiki)
  ? adminUiState.onboardingWiki
  : ""
adminUiState.onboardingWiki = onboardingWikiInitial

const onboardingWikiInput = Inputs.text({
  label: `Project (${onboardingWikiOptions.length} Wikipedias)`,
  value: onboardingWikiInitial,
  placeholder: "Type a Wikipedia project name or code…",
  submit: false
})
const onboardingWikiInputElement = onboardingWikiInput.querySelector("input[type='text']")
if (onboardingWikiInputElement) {
  onboardingWikiInputElement.setAttribute("autocomplete", "off")
  onboardingWikiInputElement.setAttribute("spellcheck", "false")
  onboardingWikiInputElement.classList.add("admin-wiki-combobox")
}
const onboardingWikiPicker = html`<div class="admin-project-picker"></div>`
const onboardingWikiTip = html`<div class="admin-project-picker-tip">Tip: click the field to browse every supported project, or type to filter by language name or wiki code.</div>`
const onboardingWikiMenu = html`<div class="admin-project-picker-menu" hidden></div>`
onboardingWikiPicker.append(onboardingWikiInput, onboardingWikiTip, onboardingWikiMenu)
onboardingWikiPicker.value = onboardingWikiInitial

function setOnboardingWikiValue(value, {closeMenu = false} = {}) {
  const nextValue = typeof value === "string" ? value : ""
  if (onboardingWikiInputElement && onboardingWikiInputElement.value !== nextValue) {
    onboardingWikiInputElement.value = nextValue
  }
  adminUiState.onboardingWiki = nextValue
  onboardingWikiPicker.value = nextValue
  onboardingWikiPicker.dispatchEvent(new Event("input", {bubbles: true}))
  if (closeMenu) hideOnboardingWikiMenu()
}

function onboardingWikiMatches(wiki, query) {
  const normalized = query.trim().toLowerCase()
  if (!normalized) return true
  return wikipediaProjectSearchText(wiki).includes(normalized)
}

function showOnboardingWikiMenu() {
  onboardingWikiPicker.dataset.open = "true"
  onboardingWikiMenu.hidden = false
  renderOnboardingWikiMenu()
}

function hideOnboardingWikiMenu() {
  onboardingWikiPicker.dataset.open = "false"
  onboardingWikiMenu.hidden = true
}

function renderOnboardingWikiMenu() {
  const query = onboardingWikiInputElement?.value || ""
  const matches = onboardingWikiOptions.filter((wiki) => onboardingWikiMatches(wiki, query))
  onboardingWikiMenu.replaceChildren(
    ...(matches.length > 0
      ? matches.map((wiki) => {
          const option = html`<button type="button" class="admin-project-picker-option">
            <span class="admin-project-picker-option-label">${wikipediaProjectLabel(wiki)}</span>
            <code class="admin-project-picker-option-code">${wiki}</code>
          </button>`
          option.addEventListener("click", () => {
            setOnboardingWikiValue(wiki, {closeMenu: true})
          })
          return option
        })
      : [html`<div class="admin-project-picker-empty">No supported project matches <code>${query.trim() || "that search"}</code>.</div>`])
  )
}

if (onboardingWikiInputElement) {
  onboardingWikiInputElement.addEventListener("focus", showOnboardingWikiMenu)
  onboardingWikiInputElement.addEventListener("click", showOnboardingWikiMenu)
  onboardingWikiInputElement.addEventListener("input", () => {
    setOnboardingWikiValue(onboardingWikiInputElement.value)
    renderOnboardingWikiMenu()
  })
  onboardingWikiInputElement.addEventListener("keydown", (event) => {
    if (event.key === "Escape") {
      hideOnboardingWikiMenu()
      return
    }
    if (event.key === "Enter") {
      const typed = (onboardingWikiInputElement.value || "").trim()
      if (onboardingWikiOptionsSet.has(typed)) {
        event.preventDefault()
        setOnboardingWikiValue(typed, {closeMenu: true})
      }
    }
  })
}
onboardingWikiMenu.addEventListener("mousedown", (event) => event.preventDefault())
const closeOnboardingWikiPicker = (event) => {
  if (!onboardingWikiPicker.contains(event.target)) {
    hideOnboardingWikiMenu()
  }
}
if (typeof document !== "undefined") {
  document.addEventListener("pointerdown", closeOnboardingWikiPicker)
  invalidation.then(() => {
    document.removeEventListener("pointerdown", closeOnboardingWikiPicker)
  })
}

const onboardingWikiRaw = view(onboardingWikiPicker)
```

```js
// Trim and normalize the raw text into either a known wiki code or null;
// downstream cells (the Run/Fetch buttons) treat null as "no valid pick".
const onboardingWikiTrimmed = (onboardingWikiRaw || "").trim()
const onboardingWiki = onboardingWikiOptionsSet.has(onboardingWikiTrimmed)
  ? onboardingWikiTrimmed
  : null
const onboardingWikiUnknown = onboardingWikiTrimmed.length > 0 && onboardingWiki === null
const onboardingLifecycle = onboardingWiki ? lifecycleStates[onboardingWiki] || null : null
const onboardingCanRun = Boolean(onboardingLifecycle && ["scheduled", "manual", "qualification"].includes(onboardingLifecycle.refresh))
```

```js
const onboardingModeInput = Inputs.select(new Map([
  ["Qualification — hidden until explicitly promoted", "qualification"],
  ["Managed manually — published, operator-triggered", "manual"],
  ["Managed automatically — published and scheduled", "scheduled"]
]), {label: "Lifecycle", value: adminUiState.onboardingMode})
onboardingModeInput.addEventListener("input", () => { adminUiState.onboardingMode = onboardingModeInput.value })
const onboardingMode = view(onboardingModeInput)

const onboardingResourceInput = Inputs.select(new Map([
  ["Small worker", "small"],
  ["Medium / large worker", "medium_large"],
  ["Isolated qualification only", "isolated"]
]), {label: "Workload class", value: adminUiState.onboardingResourceClass})
onboardingResourceInput.addEventListener("input", () => { adminUiState.onboardingResourceClass = onboardingResourceInput.value })
const onboardingResourceClass = view(onboardingResourceInput)
```

```js
const snapshotVersionInput = Inputs.text({
  label: "Exact snapshot (optional)",
  value: adminUiState.snapshotVersion,
  placeholder: "Latest completed snapshot",
  submit: false
})
snapshotVersionInput.addEventListener("input", () => {
  adminUiState.snapshotVersion = snapshotVersionInput.value
})
const snapshotVersion = view(snapshotVersionInput)
```

<div class="admin-field-help">Leave this blank to resolve and pin the latest completed Wikimedia dump. Enter <code>YYYY-MM</code> only when intentionally reproducing an exact snapshot; unavailable or incomplete pins fail before download.</div>

```js
html`<div class="admin-onboarding-console">
  ${!apiStatus ? adminConnectionWarning() : ""}
  ${onboardingWikiOptions.length === 0 ? html`<div class="warning">No supported onboarding projects were reported by the admin API yet.</div>` : ""}
  ${onboardingWikiUnknown ? html`<div class="warning">No project matches <code>${onboardingWikiTrimmed}</code>. Click the field to reopen the full project list, or keep typing to narrow it down.</div>` : ""}
  ${onboardingWiki && !onboardingLifecycle ? html`<div class="admin-registration-callout"><strong>${onboardingWiki} is ready to be registered.</strong><span>No data is downloaded until you choose a lifecycle and start it. Qualification is the safest default: it remains invisible to publication.</span></div>` : ""}
  ${onboardingWiki && onboardingLifecycle ? html`<div class="admin-registration-callout registered"><strong>${onboardingWiki} is already registered.</strong><span>${onboardingLifecycle.publication} / ${onboardingLifecycle.refresh} · ${onboardingLifecycle.fleet_resource_class || "default"} worker</span></div>` : ""}
  ${!onboardingLifecycle ? html`<div class="admin-registration-policy">${onboardingModeInput}${onboardingResourceInput}</div>` : ""}
  <div class="admin-fetch-actions">
  ${onboardingWiki && !onboardingLifecycle ? html`
    <button class="admin-btn" ?disabled=${!apiStatus} onclick=${() => registerWiki(onboardingWiki, onboardingMode, onboardingResourceClass)}>Add project</button>
    <button class="admin-btn primary" ?disabled=${!apiStatus} onclick=${() => {
      const version = normalizeSnapshotVersion(snapshotVersion)
      if (confirm(`Add ${onboardingWiki} and start its ${onboardingMode} pipeline${version ? ` for exact snapshot ${version}` : " using the latest completed snapshot"}?`)) {
        registerWiki(onboardingWiki, onboardingMode, onboardingResourceClass, {start: true, version})
      }
    }}>Add & start ${onboardingMode === "qualification" ? "qualification" : "preparation"}</button>
  ` : html`<button class="admin-btn primary" ?disabled=${!apiStatus || !onboardingCanRun} onclick=${() => {
        const w = onboardingWiki
        const version = normalizeSnapshotVersion(snapshotVersion)
        if (!w) {
          recordOperationReceipt({state: "failed", action: "run", title: "Preparation was not started", detail: "Pick a supported Wikipedia project first."})
          return
        }
        const action = onboardingLifecycle?.refresh === "qualification" ? "qualify" : "run"
        if (confirm(`${action === "qualify" ? "Qualify" : "Prepare"} ${w}${version ? ` at exact snapshot ${version}` : " using the latest completed snapshot"}?`)) {
          runCommand(action, {wiki: w, version})
        }
      }}>${onboardingLifecycle?.refresh === "qualification" ? "Run full qualification" : "Prepare project data"}</button>`}
  <button class="admin-btn" ?disabled=${!apiStatus || !onboardingCanRun} title=${actionTooltipWithApi("fetch", apiStatus)} onclick=${() => {
        const w = onboardingWiki
        const version = normalizeSnapshotVersion(snapshotVersion)
        if (!w) {
          recordOperationReceipt({state: "failed", action: "fetch", title: "Fetch was not started", detail: "Pick a supported Wikipedia project first."})
          return
        }
        runCommand("fetch", {wiki: w, version})
      }}>Fetch missing</button>
  </div>
  ${!apiStatus ? html`
      <pre class="admin-cmd">cd ${currentManifest.data_dir}/.. && WIKI_ECON_ADMIN_ENABLED=1 node site/admin-server.cjs</pre>
      <pre class="admin-cmd">cd ${currentManifest.data_dir}/.. && ${runnerCommand()} ${cliFlags(currentManifest)} run ${onboardingWiki || "frwiki"}${normalizeSnapshotVersion(snapshotVersion) ? ` --version ${normalizeSnapshotVersion(snapshotVersion)}` : ""}</pre>`
    : ""}
</div>`
```

</div>

<!-- ── Immutable lifecycle audit ──────────────────────────── -->

<div class="chart-section" data-admin-view="runs">

## Operator audit

<div class="note">Append-only, content-addressed evidence for lifecycle decisions and destructive candidate operations. Invalid or modified records are reported separately and never treated as valid history.</div>

```js
const lifecycleAuditEvents = lifecycleAudit.events || []
const lifecycleAuditInvalid = lifecycleAudit.invalid || []
```

```js
html`<div class="admin-audit-ledger">
  <header>
    <div><span>Authenticated events</span><strong>${lifecycleAuditEvents.length}</strong></div>
    <div><span>Invalid records</span><strong class=${lifecycleAuditInvalid.length ? "danger" : "success"}>${lifecycleAuditInvalid.length}</strong></div>
    <div><span>Registry revision</span><code>${lifecycleRevision || "unavailable"}</code></div>
  </header>
  ${lifecycleAuditInvalid.length ? html`<div class="admin-audit-warning"><strong>Audit integrity needs attention</strong><span>${lifecycleAuditInvalid.map((entry) => `${entry.file}: ${entry.error}`).join(" · ")}</span></div>` : ""}
  ${lifecycleAuditEvents.length ? html`<div class="admin-audit-table-wrap"><table class="admin-audit-table">
    <thead><tr><th>Recorded</th><th>Operator</th><th>Project</th><th>Action</th><th>Phase</th><th>Evidence</th></tr></thead>
    <tbody>${lifecycleAuditEvents.map((event) => html`<tr>
      <td data-label="Recorded"><time datetime=${event.recordedAt || ""}>${event.recordedAt ? new Date(event.recordedAt).toLocaleString() : "unknown"}</time></td>
      <td data-label="Operator">${event.operator || "system"}</td>
      <td data-label="Project"><code>${event.wiki || "global"}</code></td>
      <td data-label="Action">${actionLabel(event.action || "recorded")}</td>
      <td data-label="Phase"><span class=${`admin-audit-phase ${event.phase || "recorded"}`}>${event.phase || "recorded"}</span></td>
      <td data-label="Evidence"><code title=${event.eventSha256}>${event.eventSha256?.slice(0, 12) || "—"}</code>${event.afterRevision ? html`<small title=${event.afterRevision}>registry ${event.afterRevision.slice(0, 12)}</small>` : ""}</td>
    </tr>`)}</tbody>
  </table></div>` : html`<div class="admin-audit-empty">No lifecycle decisions have been recorded through this admin deployment yet.</div>`}
</div>`
```

</div>

<!-- ── Wiki details ───────────────────────────────────────── -->

<div class="chart-section" data-admin-view="wikis">

## Artifact inventory

<div class="note">Low-level files for the project selected above. Status, diagnosis, logs, and recovery actions live in its expanded project row.</div>

```js
const w = hasSelectedWiki ? wikiMap.get(selectedWiki) || emptyWikiStatus(selectedWiki) : emptyWikiStatus("—")
const selectedLifecycle = lifecycleStates[selectedWiki] || null
const selectedFleetWork = fleetByWiki.get(selectedWiki) || null
const selectedPlan = latestPlanByWiki.get(selectedWiki) || null
const selectedTruth = operationalWikiTruth[selectedWiki] || null
```

```js
!hasSelectedWiki
  ? html`<span></span>`
  : html`<div class="admin-wiki-focus">
      <div><span>Project</span><strong>${wikipediaProjectLabel(selectedWiki)}</strong></div>
      <div><span>Operational state</span><strong>${statusLabels[operationalState(selectedWiki, w)] || operationLabel(operationalState(selectedWiki, w))}</strong></div>
      <div><span>Lifecycle</span><strong>${selectedLifecycle?.refresh || "Not registered"}</strong></div>
      <div><span>Latest available</span><strong>${selectedTruth?.snapshots?.latestAvailable || selectedPlan?.snapshot || "—"}</strong></div>
      <div><span>Candidate</span><strong>${selectedTruth?.snapshots?.candidate || "—"}</strong></div>
      <div><span>Published / cutoff</span><strong>${selectedTruth?.snapshots?.published || "—"} / ${selectedTruth?.snapshots?.cutoff || "—"}</strong></div>
    </div>
    ${!selectedLifecycle ? html`<div class="warning"><strong>Processing is blocked.</strong> ${selectedWiki} has a source plan but is not registered in the lifecycle. Add it as a publication-invisible qualification project before continuing.</div>` : ""}
    ${selectedFleetWork ? html`<div class="admin-run-evidence">
      <strong>Fleet ${operationLabel(selectedFleetWork.state)}</strong>
      <span>${selectedFleetWork.workerId || selectedFleetWork.resourceClass || "waiting for worker"} · snapshot ${selectedFleetWork.snapshot || "unknown"}${selectedFleetWork.heartbeatAt ? ` · heartbeat ${relativeTime(selectedFleetWork.heartbeatAt)}` : ""}</span>
    </div>` : ""}`
```

### Raw Dumps

```js
!hasSelectedWiki
  ? html`<div class="warning">No wiki is available yet. Start a pipeline run to populate this section.</div>`
  : w.raw.files > 0
  ? html`<p><strong>${w.raw.files}</strong> dump files, <strong>${w.raw.size}</strong> total · dump version <code>${w.raw.version}</code>
    ${apiStatus && selectedLifecycle ? html` · <button class="admin-btn refetch small" title=${actionTooltipWithApi("fetch", apiStatus)} onclick=${() => { if(confirm("Fetch missing dump files for " + selectedWiki + "? Existing files will be skipped.")) runCommand("fetch", {wiki: selectedWiki, version: preferredSnapshotVersion()}) }}>fetch missing</button>` : ""}
    </p>
    ${Inputs.table(w.raw.details.map(d => ({file: d.name, size: d.size, downloaded: d.date})), {
      header: {file: "File", size: "Size", downloaded: "Downloaded"},
      sort: "file", rows: 15
    })}`
  : w.snapshot?.ready
  ? html`<p>The raw transport files were cleaned after validating snapshot <code>${w.snapshot.version}</code>.
      The immutable ingest generation remains ready with <strong>${w.ingest?.rows || 0}</strong> rows.</p>`
  : html`<div class="warning">No raw dumps or validated snapshot found for <strong>${selectedWiki}</strong>.</div>
    ${apiStatus && selectedLifecycle
      ? html`<button class="admin-btn primary" title=${actionTooltipWithApi("fetch", apiStatus)} onclick=${() => runCommand("fetch", {wiki: selectedWiki, version: preferredSnapshotVersion()})}>Fetch missing</button>`
      : html`<pre class="admin-cmd">cd ${currentManifest.data_dir}/.. && ${runnerCommand()} ${cliFlags(currentManifest)} fetch ${selectedWiki}</pre>`
    }`
```

### Patrol Data

```js
!hasSelectedWiki
  ? html`<span></span>`
  : (w.patrol?.xml || w.patrol?.events || w.patrol?.rights || w.patrol?.groups || w.patrol?.metric_ready)
  ? html`<p><strong>${Number(w.patrol?.xml || 0) + Number(w.patrol?.events || 0) + Number(w.patrol?.rights || 0) + Number(w.patrol?.groups || 0)}/4</strong> patrol source artifacts ready${
      w.patrol?.metric_ready ? html` — <span style="color:#2e7d32">patrol metric computed</span>` : html` — <span style="color:#8e24aa">patrol metric missing</span>`
    }</p>
    <ul>
      <li>logging XML: ${w.patrol?.xml ? "ready" : "missing"}</li>
      <li>patrol events parquet: ${w.patrol?.events ? "ready" : "missing"}</li>
      <li>rights parquet: ${w.patrol?.rights ? "ready" : "missing"}</li>
      <li>autopatrol groups: ${w.patrol?.groups ? "ready" : "missing"}</li>
    </ul>`
  : html`<div class="warning">No patrol data found for <strong>${selectedWiki}</strong>.</div>
    ${apiStatus && selectedLifecycle
      ? html`<button class="admin-btn" title=${actionTooltipWithApi("patrol-fetch", apiStatus)} onclick=${() => runCommand("patrol-fetch", selectedWiki)}>Fetch patrol data</button>`
      : html`<pre class="admin-cmd">cd ${currentManifest.data_dir}/.. && ${runnerCommand()} ${cliFlags(currentManifest)} patrol-fetch ${selectedWiki}</pre>`
    }`
```

### Parquet Ingestion

```js
!hasSelectedWiki
  ? html`<span></span>`
  : w.parquet.done > 0
  ? html`<p><strong>${w.parquet.done}/${w.parquet.total}</strong> files converted, <strong>${w.parquet.size}</strong> total${
      w.parquet.in_progress > 0 ? html` — <span style="color:orange">ingesting (${w.parquet.in_progress} in progress)</span>` : ""
    }${w.parquet.missing.length > 0 ? html` — <span style="color:tomato">${w.parquet.missing.length} missing</span>` : ""
    }${apiStatus && selectedLifecycle && w.parquet.missing.length > 0 ? html` · <button class="admin-btn small" title=${actionTooltipWithApi("ingest", apiStatus)} onclick=${() => runCommand("ingest", selectedWiki)}>ingest missing</button>` : ""}
    </p>
    ${w.parquet.missing.length > 0
      ? html`<details><summary>Missing files</summary><ul>${w.parquet.missing.map(f => html`<li><code>${f}</code></li>`)}</ul></details>`
      : ""
    }`
  : html`<div class="warning">No ingested data for <strong>${selectedWiki}</strong>.</div>
    ${apiStatus && selectedLifecycle
      ? html`<button class="admin-btn" title=${actionTooltipWithApi("ingest", apiStatus)} onclick=${() => runCommand("ingest", selectedWiki)}>Ingest ${selectedWiki}</button>`
      : html`<pre class="admin-cmd">cd ${currentManifest.data_dir}/.. && ${runnerCommand()} ${cliFlags(currentManifest)} ingest ${selectedWiki}</pre>`
    }`
```

### Computed Metrics

```js
!hasSelectedWiki
  ? html`<span></span>`
  : selectedTruth?.metrics?.details?.length
  ? html`${Inputs.table(selectedTruth.metrics.details.map(metric => ({
      metric: metric.id,
      family: metric.family,
      candidate: metric.candidateReady ? "Ready" : "Missing",
      published: metric.publishedReady ? "Verified" : "Missing",
      rows: metric.publishedRows == null ? "—" : metric.publishedRows.toLocaleString(),
      range: metric.minimumDate && metric.maximumDate ? `${metric.minimumDate} – ${metric.maximumDate}` : "—",
      algorithm: metric.algorithmVersion
    })), {
      header: {metric: "Metric", family: "Family", candidate: "Candidate", published: "Published", rows: "Rows", range: "Date range", algorithm: "Algorithm"},
      sort: "metric"
    })}
    ${apiStatus && selectedLifecycle ? html`<button class="admin-btn small" title=${actionTooltipWithApi("compute", apiStatus)} onclick=${() => runCommand("compute", selectedWiki)}>recompute</button>` : ""}`
  : html`<div class="warning">No metrics computed for <strong>${selectedWiki}</strong>.</div>
    ${apiStatus && selectedLifecycle
      ? html`<button class="admin-btn" title=${actionTooltipWithApi("compute", apiStatus)} onclick=${() => runCommand("compute", selectedWiki)}>Compute ${selectedWiki}</button>`
      : html`<pre class="admin-cmd">cd ${currentManifest.data_dir}/.. && ${runnerCommand()} ${cliFlags(currentManifest)} compute ${selectedWiki}</pre>`
    }`
```

### Site Data Files

```js
!hasSelectedWiki
  ? html`<span></span>`
  : w.dashboard.length > 0
  ? html`${Inputs.table(w.dashboard.map(m => ({metric: m.name, size: m.size_kb + " KB"})), {
      header: {metric: "Metric file", size: "Size"}, sort: "metric"
    })}
    ${apiStatus ? html`<button class="admin-btn small" title=${actionTooltipWithApi("merge", apiStatus)} onclick=${() => runCommand("merge")}>regenerate merged artifacts</button>` : ""}`
  : html`<div class="warning">No site data found for <strong>${selectedWiki}</strong>.</div>
    ${apiStatus
      ? html`<button class="admin-btn" title=${actionTooltipWithApi("merge", apiStatus)} onclick=${() => runCommand("merge")}>Regenerate merged artifacts</button>`
      : html`<pre class="admin-cmd">cd ${currentManifest.data_dir}/.. && ${runnerCommand()} ${cliFlags(currentManifest)} merge</pre>`
    }`
```

</div>

<!-- ── Merged site data files ─────────────────────────────── -->

<div class="chart-section" data-admin-view="quality">

## Merged Site Data Files

<div class="note">Combined parquet files served to the browser. These are the final site data files the frontend reads.</div>

```js
currentManifest.merged.length > 0
  ? Inputs.table(currentManifest.merged.map(f => ({metric: f.name, size: f.size_kb + " KB"})), {
      header: {metric: "Metric", size: "Size"}, sort: "metric"
    })
  : html`<div class="warning">No merged files.</div>`
```

</div>

<style>
.admin-view-navigation {
  position: sticky;
  top: 0;
  z-index: 8;
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  margin: 1.1rem 0 0.8rem;
  padding: 0.24rem;
  border: 1px solid var(--theme-foreground-faintest);
  border-radius: 0.55rem;
  background: color-mix(in srgb, var(--theme-background) 94%, #315b8a 6%);
  box-shadow: 0 0.35rem 1rem color-mix(in srgb, var(--theme-foreground) 8%, transparent);
}
.admin-view-navigation button {
  min-height: 2.55rem;
  border: 0;
  border-radius: 0.35rem;
  color: var(--theme-foreground-muted);
  background: transparent;
  font: inherit;
  font-size: 0.82rem;
  font-weight: 700;
  cursor: pointer;
}
.admin-view-navigation button[aria-selected="true"] {
  color: var(--theme-foreground);
  background: var(--theme-background);
  box-shadow: 0 1px 0.35rem color-mix(in srgb, var(--theme-foreground) 10%, transparent);
}
.admin-view-navigation button:focus-visible,
.admin-operation-receipts button:focus-visible {
  outline: 3px solid color-mix(in srgb, #315b8a 70%, white 30%);
  outline-offset: 2px;
}
[data-admin-view][hidden] { display: none !important; }
.admin-operation-receipts {
  margin: 0 0 1rem;
  border: 1px solid var(--theme-foreground-faintest);
  border-left: 4px solid #315b8a;
  background: var(--theme-background);
}
.admin-operation-receipts > header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 1rem;
  padding: 0.7rem 0.85rem;
  border-bottom: 1px solid var(--theme-foreground-faintest);
}
.admin-operation-receipts h2 { margin: 0; font-size: 0.88rem; }
.admin-operation-receipts header span,
.admin-operation-receipts.empty span { display: block; color: var(--theme-foreground-muted); font-size: 0.7rem; }
.admin-operation-receipts.empty { display: flex; gap: 0.45rem; padding: 0.62rem 0.8rem; border-left-color: var(--theme-foreground-faintest); font-size: 0.72rem; }
.admin-operation-receipt-list { max-height: 15rem; overflow-y: auto; }
.admin-operation-receipt {
  display: grid;
  grid-template-columns: 6.25rem minmax(0, 1fr) auto;
  gap: 0.75rem;
  align-items: start;
  padding: 0.62rem 0.8rem;
  border-top: 1px solid var(--theme-foreground-faintest);
  font-size: 0.72rem;
}
.admin-operation-receipt:first-child { border-top: 0; }
.admin-operation-receipt-state { font-weight: 750; }
.admin-operation-receipt.active .admin-operation-receipt-state { color: #315b8a; }
.admin-operation-receipt.waiting .admin-operation-receipt-state { color: #6956a1; }
.admin-operation-receipt.success .admin-operation-receipt-state { color: #2e7d32; }
.admin-operation-receipt.danger .admin-operation-receipt-state { color: #c62828; }
.admin-operation-receipt > div { display: grid; gap: 0.1rem; }
.admin-operation-receipt > div span,
.admin-operation-receipt time { color: var(--theme-foreground-muted); }
.admin-quality-ledger { border-block: 1px solid var(--theme-foreground-faintest); }
.admin-quality-summary {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  border-bottom: 1px solid var(--theme-foreground-faintest);
}
.admin-quality-summary > div {
  display: flex;
  align-items: baseline;
  gap: 0.45rem;
  padding: 0.75rem 0.85rem;
  border-right: 1px solid var(--theme-foreground-faintest);
}
.admin-quality-summary > div:last-child { border-right: 0; }
.admin-quality-summary strong { font-size: 1.15rem; }
.admin-quality-summary span { color: var(--theme-foreground-muted); font-size: 0.72rem; }
.admin-quality-summary .warning strong { color: #b26a00; }
.admin-quality-summary .healthy strong { color: #2e7d32; }
.admin-quality-wiki { border-bottom: 1px solid var(--theme-foreground-faintest); }
.admin-quality-wiki:last-child { border-bottom: 0; }
.admin-quality-wiki > summary {
  display: grid;
  grid-template-columns: minmax(12rem, 1fr) auto;
  align-items: center;
  gap: 1rem;
  padding: 0.8rem 0.25rem;
  cursor: pointer;
  list-style: none;
}
.admin-quality-wiki > summary::-webkit-details-marker { display: none; }
.admin-quality-wiki > summary::before {
  content: "+";
  grid-column: 1;
  grid-row: 1;
  width: 1.25rem;
  color: var(--theme-foreground-muted);
  font-size: 1.05rem;
}
.admin-quality-wiki[open] > summary::before { content: "−"; }
.admin-quality-wiki-name {
  grid-column: 1;
  grid-row: 1;
  display: flex;
  align-items: baseline;
  gap: 0.7rem;
  padding-left: 1.65rem;
}
.admin-quality-wiki-name strong { font-size: 0.92rem; }
.admin-quality-wiki-name small,
.admin-quality-counts small { color: var(--theme-foreground-muted); }
.admin-quality-counts { display: flex; align-items: center; gap: 0.45rem; }
.admin-quality-counts b { border-radius: 999px; padding: 0.14rem 0.45rem; font-size: 0.66rem; }
.admin-quality-counts .critical { color: #b71c1c; background: color-mix(in srgb, #c62828 11%, transparent); }
.admin-quality-counts .warning { color: #9b5c00; background: color-mix(in srgb, #f57f17 12%, transparent); }
.admin-quality-counts .healthy { color: #246b2a; background: color-mix(in srgb, #2e7d32 10%, transparent); }
.admin-quality-alerts {
  display: grid;
  gap: 1px;
  margin: 0 0 0.8rem 1.65rem;
  background: var(--theme-foreground-faintest);
  border-left: 3px solid #f57f17;
}
.admin-quality-alerts > div {
  display: grid;
  grid-template-columns: minmax(8rem, 0.25fr) 1fr;
  gap: 0.7rem;
  padding: 0.5rem 0.65rem;
  background: var(--theme-background);
  font-size: 0.74rem;
}
.admin-quality-alerts > div.critical { border-left: 3px solid #c62828; }
.admin-quality-signal-strip {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  margin: 0 0 0.85rem 1.65rem;
  border: 1px solid var(--theme-foreground-faintest);
}
.admin-quality-signal-strip > div {
  display: grid;
  gap: 0.08rem;
  padding: 0.55rem 0.65rem;
  border-right: 1px solid var(--theme-foreground-faintest);
  border-bottom: 1px solid var(--theme-foreground-faintest);
}
.admin-quality-signal-strip > div:nth-child(3n) { border-right: 0; }
.admin-quality-signal-strip span,
.admin-quality-signal-strip small { color: var(--theme-foreground-muted); font-size: 0.67rem; }
.admin-quality-signal-strip strong { font-size: 0.76rem; }
.admin-quality-signal-strip .warning strong { color: #9b5c00; }
.admin-quality-signal-strip .critical strong { color: #b71c1c; }
.admin-quality-table-wrap { overflow-x: auto; margin-left: 1.65rem; padding-bottom: 0.9rem; }
.admin-quality-table { width: 100%; border-collapse: collapse; font-size: 0.72rem; }
.admin-quality-table th,
.admin-quality-table td { border-top: 1px solid var(--theme-foreground-faintest); padding: 0.62rem; vertical-align: top; text-align: left; }
.admin-quality-table thead th { color: var(--theme-foreground-muted); font-weight: 600; }
.admin-quality-table tbody th { width: 15%; border-left: 3px solid #2e7d32; }
.admin-quality-table tr.warning > th { border-left-color: #f57f17; }
.admin-quality-table tr.critical > th { border-left-color: #c62828; }
.admin-quality-table tbody th span,
.admin-quality-table tbody th small { display: block; color: var(--theme-foreground-muted); font-weight: 400; margin-top: 0.12rem; }
.admin-quality-evidence { display: grid; gap: 0.16rem; min-width: 13rem; }
.admin-quality-evidence > div { display: flex; gap: 0.35rem; align-items: baseline; }
.admin-quality-evidence span,
.admin-quality-evidence code { color: var(--theme-foreground-muted); font-size: 0.66rem; }
.admin-quality-evidence .pass { color: #2e7d32; }
.admin-quality-evidence .fail,
.admin-quality-invalid { color: #c62828; font-weight: 700; }
.admin-quality-empty { color: var(--theme-foreground-muted); }
.admin-quality-change { display: grid; gap: 0.18rem; min-width: 8rem; }
.admin-quality-change span,
.admin-quality-change small { color: var(--theme-foreground-muted); }
.admin-pipeline-board {
  display: grid;
  gap: 1.15rem;
}
.admin-control-strip {
  display: flex;
  flex-wrap: wrap;
  gap: 0.65rem;
  align-items: center;
  margin: 0.35rem 0 0.85rem;
}
.admin-control-chip {
  border-radius: 999px;
  border: 1px solid color-mix(in srgb, var(--theme-foreground-faintest) 80%, transparent);
  background: color-mix(in srgb, var(--theme-background) 92%, white 8%);
  padding: 0.45rem 0.72rem;
  display: flex;
  align-items: center;
  gap: 0.55rem;
}
.admin-control-chip strong {
  font-size: 0.82rem;
}
.admin-control-chip small {
  color: var(--theme-foreground-muted);
  font-size: 0.68rem;
}
.admin-publication-actions {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 0.5rem;
  padding: 0.8rem 0;
}
.admin-inline-advanced summary {
  cursor: pointer;
  color: var(--theme-foreground-muted);
  font-size: 0.72rem;
  font-weight: 700;
}
.admin-inline-advanced[open] {
  display: flex;
  align-items: center;
  gap: 0.5rem;
}
.admin-control-chip.online {
  border-color: color-mix(in srgb, #2e7d32 30%, transparent);
}
.admin-control-chip.offline {
  border-color: color-mix(in srgb, #c62828 28%, transparent);
}
.admin-control-chip.running {
  border-color: color-mix(in srgb, #1565c0 34%, transparent);
}
.admin-control-dot {
  width: 0.6rem;
  height: 0.6rem;
  border-radius: 999px;
  background: #2e7d32;
  box-shadow: 0 0 0 0.2rem color-mix(in srgb, #2e7d32 14%, transparent);
}
.admin-control-chip.offline .admin-control-dot {
  background: #c62828;
  box-shadow: 0 0 0 0.2rem color-mix(in srgb, #c62828 14%, transparent);
}
.admin-control-label {
  font-size: 0.68rem;
  text-transform: uppercase;
  letter-spacing: 0.06em;
  color: var(--theme-foreground-muted);
}
.admin-pipeline-summary {
  display: flex;
  flex-wrap: wrap;
  gap: 0.55rem;
  align-items: center;
}
.admin-summary-card {
  border: 1px solid color-mix(in srgb, var(--theme-foreground-faintest) 70%, transparent);
  border-radius: 14px;
  padding: 0.85rem 0.95rem;
  background:
    linear-gradient(180deg, color-mix(in srgb, var(--theme-background) 96%, white 4%), var(--theme-background));
  display: grid;
  gap: 0.2rem;
  min-height: 84px;
}
.admin-summary-card.compact {
  min-height: 0;
  display: inline-flex;
  align-items: center;
  gap: 0.5rem;
  padding: 0.42rem 0.7rem;
  border-radius: 999px;
  background: color-mix(in srgb, var(--theme-background) 94%, white 6%);
}
.admin-summary-primary {
  background:
    radial-gradient(circle at top right, color-mix(in srgb, #1565c0 16%, transparent), transparent 42%),
    linear-gradient(180deg, color-mix(in srgb, var(--theme-background) 96%, white 4%), var(--theme-background));
}
.admin-summary-primary.compact {
  background:
    radial-gradient(circle at top right, color-mix(in srgb, #1565c0 12%, transparent), transparent 48%),
    color-mix(in srgb, var(--theme-background) 93%, white 7%);
}
.admin-summary-card strong {
  font-size: 1.6rem;
  line-height: 1;
}
.admin-summary-card.compact strong {
  font-size: 0.95rem;
}
.admin-summary-label {
  color: var(--theme-foreground-muted);
  font-size: 0.75rem;
  text-transform: uppercase;
  letter-spacing: 0.06em;
}
.admin-summary-card.compact .admin-summary-label {
  font-size: 0.68rem;
}
.admin-summary-meta {
  color: var(--theme-foreground-muted);
  font-size: 0.75rem;
}
.admin-summary-card.compact .admin-summary-meta {
  font-size: 0.72rem;
}
.admin-summary-dot {
  width: 0.55rem;
  height: 0.55rem;
  border-radius: 999px;
  display: inline-block;
  margin-bottom: 0.2rem;
}
.admin-summary-card.compact .admin-summary-dot {
  margin-bottom: 0;
  width: 0.45rem;
  height: 0.45rem;
}
.admin-pipeline-cards {
  display: grid;
  gap: 0.7rem;
}
.pipeline-card {
  border: 1px solid color-mix(in srgb, var(--theme-foreground-faintest) 85%, transparent);
  border-radius: 18px;
  padding: 0.82rem 0.9rem 0.85rem;
  background:
    linear-gradient(180deg, color-mix(in srgb, var(--theme-background) 97%, white 3%), var(--theme-background));
  box-shadow: 0 14px 34px color-mix(in srgb, var(--theme-foreground-faintest) 28%, transparent);
  display: grid;
  gap: 0.6rem;
  position: relative;
  overflow: hidden;
}
.pipeline-card::before {
  content: "";
  position: absolute;
  inset: 0 auto auto 0;
  width: 100%;
  height: 4px;
  background: color-mix(in srgb, var(--theme-foreground-faintest) 90%, transparent);
}
.pipeline-card.running {
  border-color: color-mix(in srgb, #1565c0 45%, var(--theme-foreground-faintest));
  box-shadow: 0 16px 40px color-mix(in srgb, #1565c0 14%, transparent);
}
.pipeline-card.running::before,
.pipeline-card.status-running::before {
  background: linear-gradient(90deg, #1565c0, #42a5f5);
}
.pipeline-card.status-complete::before {
  background: linear-gradient(90deg, #2e7d32, #66bb6a);
}
.pipeline-card.status-needs_fetch::before {
  background: linear-gradient(90deg, #c62828, #ef5350);
}
.pipeline-card.status-needs_patrol_fetch::before {
  background: linear-gradient(90deg, #6a1b9a, #ab47bc);
}
.pipeline-card.status-needs_ingest::before {
  background: linear-gradient(90deg, #e65100, #fb8c00);
}
.pipeline-card.status-needs_compute::before {
  background: linear-gradient(90deg, #f57f17, #ffca28);
}
.pipeline-card.status-needs_patrol_compute::before {
  background: linear-gradient(90deg, #8e24aa, #ce93d8);
}
.pipeline-card.status-needs_merge::before {
  background: linear-gradient(90deg, #1565c0, #64b5f6);
}
.pipeline-card-top {
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  gap: 0.8rem;
  flex-wrap: wrap;
}
.pipeline-card-title {
  display: grid;
  gap: 0.1rem;
  min-width: 0;
}
.pipeline-card-heading {
  display: flex;
  gap: 0.45rem;
  align-items: center;
  flex-wrap: wrap;
}
.pipeline-card-heading strong {
  font-size: 1rem;
}
.pipeline-ghost-badge {
  border-radius: 999px;
  padding: 0.14rem 0.45rem;
  background: color-mix(in srgb, #6a1b9a 12%, transparent);
  color: #6a1b9a;
  font-size: 0.68rem;
  font-weight: 600;
}
.pipeline-inline-meta {
  display: inline-flex;
  align-items: center;
  border-radius: 999px;
  padding: 0.12rem 0.42rem;
  border: 1px solid color-mix(in srgb, var(--theme-foreground-faintest) 72%, transparent);
  color: var(--theme-foreground-muted);
  font-size: 0.68rem;
  line-height: 1.1;
  white-space: nowrap;
}
.pipeline-card-meta {
  display: flex;
  flex-wrap: wrap;
  gap: 0.35rem;
}
.pipeline-card-meta span {
  border-radius: 999px;
  padding: 0.16rem 0.48rem;
  background: color-mix(in srgb, var(--theme-foreground-faintest) 65%, transparent);
  color: var(--theme-foreground-muted);
  font-size: 0.72rem;
}
.pipeline-card-meta.compact span {
  background: none;
  border: 1px solid color-mix(in srgb, var(--theme-foreground-faintest) 72%, transparent);
}
.pipeline-card-actions {
  display: flex;
  align-items: center;
  gap: 0.4rem;
  flex-wrap: wrap;
}
.pipeline-stage-grid {
  display: grid;
  grid-template-columns: repeat(6, minmax(0, 1fr));
  gap: 0.45rem;
}
.pipeline-stage {
  border-radius: 12px;
  padding: 0.42rem 0.5rem;
  display: grid;
  gap: 0.08rem;
  min-height: 52px;
  border: 1px solid transparent;
  position: relative;
  overflow: hidden;
}
.pipeline-stage::after {
  content: "";
  position: absolute;
  top: 0.65rem;
  right: 0.7rem;
  width: 0.42rem;
  height: 0.42rem;
  border-radius: 999px;
  background: currentColor;
  opacity: 0.4;
}
.pipeline-stage-label {
  font-size: 0.64rem;
  text-transform: uppercase;
  letter-spacing: 0.05em;
  color: var(--theme-foreground-muted);
}
.pipeline-stage strong {
  font-size: 0.74rem;
  line-height: 1.15;
}
.pipeline-stage-action {
  justify-self: start;
  margin-top: 0.12rem;
  border: 0;
  border-radius: 999px;
  padding: 0.12rem 0.42rem;
  font-size: 0.62rem;
  font-weight: 700;
  text-transform: uppercase;
  letter-spacing: 0.04em;
  cursor: pointer;
  color: inherit;
  background: color-mix(in srgb, currentColor 10%, transparent);
}
.pipeline-stage-action:hover {
  background: color-mix(in srgb, currentColor 16%, transparent);
}
.pipeline-stage.done {
  background: color-mix(in srgb, #2e7d32 10%, transparent);
  border-color: color-mix(in srgb, #2e7d32 35%, transparent);
  color: #1f5b26;
}
.pipeline-stage.active {
  background: color-mix(in srgb, #1565c0 11%, transparent);
  border-color: color-mix(in srgb, #1565c0 45%, transparent);
  color: #0f5fa8;
}
.pipeline-stage.todo {
  background: color-mix(in srgb, #f57f17 11%, transparent);
  border-color: color-mix(in srgb, #f57f17 35%, transparent);
  color: #a65500;
}
.pipeline-stage.blocked {
  background: color-mix(in srgb, var(--theme-foreground-faintest) 80%, transparent);
  border-color: color-mix(in srgb, var(--theme-foreground-faintest) 95%, transparent);
  opacity: 0.78;
  color: var(--theme-foreground-muted);
}
.pipeline-live-panel {
  border-top: 1px solid var(--theme-foreground-faintest);
  padding-top: 0.65rem;
}
.admin-empty-state {
  padding: 0.9rem 1rem;
  color: var(--theme-foreground-muted);
}
.admin-badge {
  display: inline-block;
  color: white;
  padding: 0.12rem 0.44rem;
  border-radius: 999px;
  font-size: 0.68rem;
  font-weight: 600;
  text-align: center;
}
.admin-btn {
  display: inline-block;
  padding: 0.3rem 0.68rem;
  border: 1px solid var(--theme-foreground-faintest);
  border-radius: 999px;
  background: color-mix(in srgb, var(--theme-background) 92%, white 8%);
  color: var(--theme-foreground);
  font-size: 0.74rem;
  font-weight: 600;
  cursor: pointer;
  white-space: nowrap;
  transition: transform 140ms ease, background 140ms ease, border-color 140ms ease, box-shadow 140ms ease;
}
.admin-btn:hover {
  background: color-mix(in srgb, var(--theme-background) 85%, white 15%);
  border-color: color-mix(in srgb, #0f5fa8 22%, var(--theme-foreground-faintest));
  transform: translateY(-1px);
}
.admin-btn.primary {
  background: linear-gradient(135deg, #1565c0, #1976d2);
  color: white;
  border-color: #1565c0;
  box-shadow: 0 10px 22px color-mix(in srgb, #1565c0 24%, transparent);
}
.admin-btn.primary:hover { background: linear-gradient(135deg, #0d47a1, #1565c0); }
.admin-btn.refetch {
  background: #fff3e0;
  border-color: #e65100;
  color: #e65100;
}
.admin-btn.refetch:hover { background: #ffe0b2; }
.admin-btn.danger {
  background: #fdecea;
  border-color: #c62828;
  color: #c62828;
}
.admin-btn.danger:hover { background: #f9d6d2; }
.admin-btn.small { font-size: 0.75rem; padding: 0.2rem 0.5rem; }
[data-theme="dark"] .admin-btn.refetch {
  background: #3e2723;
  color: #ff9800;
}
[data-theme="dark"] .admin-btn.danger {
  background: #4e1b1b;
  color: #ffb4ab;
}
[data-theme="dark"] .pipeline-ghost-badge {
  color: #e1bee7;
  background: color-mix(in srgb, #6a1b9a 35%, transparent);
}
[data-theme="dark"] .pipeline-inline-meta {
  border-color: color-mix(in srgb, var(--theme-foreground-faintest) 78%, transparent);
}
[data-theme="dark"] .pipeline-stage.done {
  color: #a5d6a7;
}
[data-theme="dark"] .pipeline-stage.active {
  color: #90caf9;
}
[data-theme="dark"] .pipeline-stage.todo {
  color: #ffcc80;
}
.admin-fetch-actions {
  display: flex;
  gap: 0.5rem;
  align-items: center;
  flex-wrap: wrap;
}
.admin-project-picker {
  position: relative;
  min-width: min(34rem, 100%);
  flex: 1 1 28rem;
}
.admin-project-picker label,
.admin-project-picker .inputs-3a86ea {
  width: 100%;
}
.admin-project-picker-tip {
  margin-top: 0.25rem;
  font-size: 0.74rem;
  color: var(--theme-foreground-muted);
}
.admin-wiki-combobox {
  cursor: text;
}
.admin-project-picker-menu {
  position: absolute;
  z-index: 20;
  top: calc(100% + 0.45rem);
  left: 0;
  right: 0;
  max-height: 18rem;
  overflow-y: auto;
  padding: 0.35rem;
  border: 1px solid var(--theme-foreground-faintest);
  border-radius: 0.7rem;
  background: color-mix(in srgb, var(--theme-background) 94%, white 6%);
  box-shadow: 0 18px 40px rgba(0, 0, 0, 0.12);
  backdrop-filter: blur(10px) saturate(1.1);
}
.admin-project-picker-option {
  width: 100%;
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 0.8rem;
  padding: 0.55rem 0.7rem;
  border: 0;
  border-radius: 0.55rem;
  background: transparent;
  color: inherit;
  text-align: left;
  cursor: pointer;
}
.admin-project-picker-option:hover,
.admin-project-picker-option:focus-visible {
  background: color-mix(in srgb, var(--wk-blue) 14%, transparent);
  outline: none;
}
.admin-project-picker-option-label {
  min-width: 0;
  font-size: 0.86rem;
}
.admin-project-picker-option-code {
  flex: 0 0 auto;
  font-size: 0.75rem;
  color: var(--theme-foreground-muted);
}
.admin-project-picker-empty {
  padding: 0.7rem 0.8rem;
  font-size: 0.82rem;
  color: var(--theme-foreground-muted);
}
[data-theme="dark"] .admin-project-picker-menu {
  background: color-mix(in srgb, #171b22 92%, #242b36 8%);
  box-shadow: 0 18px 40px rgba(0, 0, 0, 0.35);
}
.admin-maintenance-actions,
.admin-action-group {
  display: flex;
  gap: 0.35rem;
  align-items: center;
  flex-wrap: wrap;
}
.admin-cmd {
  background: var(--theme-foreground-faintest);
  border: 1px solid var(--theme-foreground-faintest);
  border-radius: 4px;
  padding: 0.6rem 0.8rem;
  font-size: 0.85rem;
  overflow-x: auto;
  white-space: pre;
  user-select: all;
  cursor: copy;
}
.warning {
  background: #fff3e0;
  border-left: 4px solid #e65100;
  padding: 0.6rem 1rem;
  border-radius: 0 4px 4px 0;
  margin: 0.5rem 0;
}
[data-theme="dark"] .warning {
  background: #3e2723;
  border-left-color: #ff9800;
}
.admin-job-panel {
  border: 2px solid var(--theme-foreground-faintest);
  border-radius: 6px;
  margin: 1rem 0;
  overflow: hidden;
}
.admin-job-panel.running { border-color: #1565c0; }
.admin-job-panel.success { border-color: #2e7d32; }
.admin-job-panel.failed { border-color: #c62828; }
.admin-job-header {
  padding: 0.5rem 0.8rem;
  display: flex;
  gap: 1rem;
  align-items: center;
  font-size: 0.85rem;
}
.admin-job-panel.running .admin-job-header { background: #e3f2fd; color: #1565c0; }
.admin-job-panel.success .admin-job-header { background: #e8f5e9; color: #2e7d32; }
.admin-job-panel.failed .admin-job-header { background: #fbe9e7; color: #c62828; }
[data-theme="dark"] .admin-job-panel.running .admin-job-header { background: #0d47a1; color: #bbdefb; }
[data-theme="dark"] .admin-job-panel.success .admin-job-header { background: #1b5e20; color: #c8e6c9; }
[data-theme="dark"] .admin-job-panel.failed .admin-job-header { background: #b71c1c; color: #ffcdd2; }
.admin-progress {
  padding: 0.5rem 0.8rem 0.6rem;
  border-bottom: 1px solid var(--theme-foreground-faintest);
}
.admin-progress-info {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  font-size: 0.8rem;
  margin-bottom: 0.35rem;
}
.admin-progress-stage {
  font-weight: 700;
  text-transform: uppercase;
  font-size: 0.7rem;
  letter-spacing: 0.05em;
  padding: 0.1rem 0.4rem;
  border-radius: 3px;
  background: var(--theme-foreground-faintest);
}
.admin-progress-detail { flex: 1; color: var(--theme-foreground-muted); }
.admin-progress-pct { font-variant-numeric: tabular-nums; font-weight: 600; }
.admin-progress-track {
  height: 8px;
  background: var(--theme-foreground-faintest);
  border-radius: 4px;
  overflow: hidden;
}
.admin-progress-fill {
  height: 100%;
  border-radius: 4px;
  background: #1565c0;
  transition: width 0.4s ease;
}
.admin-pipeline-badges {
  display: flex;
  flex-wrap: wrap;
  gap: 0.4rem;
  padding: 0.5rem 0.8rem 0;
}
.admin-badge {
  font-size: 0.72rem;
  padding: 0.15rem 0.5rem;
  border-radius: 999px;
  border: 1px solid transparent;
}
.admin-badge.ok {
  color: #2e7d32;
  background: #e8f5e9;
  border-color: color-mix(in srgb, #2e7d32 30%, transparent);
}
.admin-badge.fail {
  color: #c62828;
  background: #fbe9e7;
  border-color: color-mix(in srgb, #c62828 30%, transparent);
}
[data-theme="dark"] .admin-badge.ok { background: #1b5e20; color: #c8e6c9; }
[data-theme="dark"] .admin-badge.fail { background: #b71c1c; color: #ffcdd2; }
.admin-refresh-panel {
  display: grid;
  gap: 0.75rem;
}
.admin-refresh-history {
  width: 100%;
  border-collapse: collapse;
  font-size: 0.82rem;
}
.admin-refresh-history th, .admin-refresh-history td {
  padding: 0.4rem 0.6rem;
  border-bottom: 1px solid var(--theme-foreground-faintest);
  text-align: left;
}
.admin-job-panel.running .admin-progress-fill {
  background: linear-gradient(90deg, #1565c0 0%, #42a5f5 50%, #1565c0 100%);
  background-size: 200% 100%;
  animation: progress-shimmer 1.5s ease-in-out infinite;
}
@keyframes progress-shimmer {
  0% { background-position: 200% 0; }
  100% { background-position: -200% 0; }
}
.admin-log-toggle {
  padding: 0.3rem 0.8rem;
  font-size: 0.75rem;
  cursor: pointer;
  color: var(--theme-foreground-muted);
  user-select: none;
}
.admin-log-toggle:hover { color: var(--theme-foreground); }
.admin-log-button {
  border: 0;
  background: transparent;
  text-align: left;
}
.admin-log-section {
  border-top: 1px solid var(--theme-foreground-faintest);
}
.admin-log-bar {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 0.3rem 0.8rem;
}
.admin-copy-btn {
  transition: all 0.15s ease;
}
.admin-icon-btn {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 1.9rem;
  height: 1.9rem;
  padding: 0;
  border: 1px solid var(--theme-foreground-faintest);
  border-radius: 999px;
  background: color-mix(in srgb, var(--theme-background) 88%, white 12%);
  color: var(--theme-foreground-muted);
  cursor: pointer;
}
.admin-icon-btn:hover {
  color: var(--theme-foreground);
  border-color: color-mix(in srgb, var(--theme-foreground-muted) 35%, transparent);
}
.admin-icon-btn svg {
  width: 0.9rem;
  height: 0.9rem;
  fill: currentColor;
}
.admin-copy-btn:active {
  transform: scale(0.95);
}
.admin-job-log {
  max-height: 300px;
  overflow-y: auto;
  padding: 0.6rem 0.8rem;
  margin: 0;
  font-size: 0.8rem;
  background: var(--theme-background-alt, #f8f8f8);
  white-space: pre-wrap;
  word-break: break-all;
}
.admin-job-log-full {
  max-height: none;
}
[data-theme="dark"] .admin-job-log { background: #1a1a1a; }
.admin-command-header {
  --admin-ink: #243347;
  --admin-blue: #315b8a;
  --admin-line: color-mix(in srgb, var(--theme-foreground-faintest) 88%, transparent);
  display: grid;
  grid-template-columns: minmax(13rem, 0.8fr) minmax(28rem, 2fr) auto;
  align-items: stretch;
  border-block: 1px solid var(--admin-line);
  margin: 1rem 0 1.5rem;
  background: color-mix(in srgb, var(--theme-background) 96%, #e8eef5 4%);
}
.admin-command-health {
  display: grid;
  align-content: center;
  gap: 0.12rem;
  padding: 1rem 1.1rem;
  border-left: 5px solid #2e7d32;
}
.admin-command-health.attention { border-left-color: #c13c32; }
.admin-command-health strong { font-size: 1.05rem; }
.admin-command-health > span:last-child { color: var(--theme-foreground-muted); font-size: 0.78rem; }
.admin-command-kicker {
  color: var(--theme-foreground-muted);
  font-size: 0.64rem;
  font-weight: 750;
  letter-spacing: 0.12em;
  text-transform: uppercase;
}
.admin-command-facts {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  margin: 0;
  border-inline: 1px solid var(--admin-line);
}
.admin-command-facts > div { padding: 0.9rem 1rem; border-left: 1px solid var(--admin-line); }
.admin-command-facts > div:first-child { border-left: 0; }
.admin-command-facts dt,
.admin-wiki-focus span {
  color: var(--theme-foreground-muted);
  font-size: 0.64rem;
  font-weight: 700;
  letter-spacing: 0.08em;
  text-transform: uppercase;
}
.admin-command-facts dd { margin: 0.3rem 0 0; font-size: 0.78rem; font-weight: 650; }
.admin-command-facts dd.ok { color: #2e7d32; }
.admin-command-facts dd.bad { color: #c62828; }
.admin-command-session { display: grid; align-content: center; gap: 0.2rem; padding: 0.9rem 1rem; font-size: 0.75rem; }
.admin-activity-ledger { border-top: 1px solid var(--theme-foreground-faintest); }
.admin-activity-row {
  appearance: none;
  width: 100%;
  display: grid;
  grid-template-columns: 7.5rem minmax(12rem, 1fr) 5rem 6rem;
  align-items: center;
  gap: 1rem;
  padding: 0.72rem 0.35rem 0.72rem 0.85rem;
  border: 0;
  border-bottom: 1px solid var(--theme-foreground-faintest);
  border-left: 3px solid transparent;
  background: transparent;
  color: inherit;
  text-align: left;
  cursor: pointer;
}
.admin-activity-row:hover { background: color-mix(in srgb, #315b8a 6%, transparent); }
.admin-activity-row:disabled { cursor: default; opacity: 1; }
.admin-activity-row.danger { border-left-color: #c13c32; background: color-mix(in srgb, #c13c32 4%, transparent); }
.admin-activity-row.active { border-left-color: #315b8a; }
.admin-activity-row.waiting { border-left-color: #7e6ab0; }
.admin-activity-row.success { border-left-color: #2e7d32; }
.admin-activity-state { font-size: 0.72rem; font-weight: 800; letter-spacing: 0.06em; text-transform: uppercase; }
.admin-activity-main { display: grid; min-width: 0; }
.admin-activity-main strong { font-family: var(--sans-serif); font-size: 0.88rem; }
.admin-activity-main span,
.admin-activity-source,
.admin-activity-row time { color: var(--theme-foreground-muted); font-size: 0.72rem; }
.admin-activity-source { text-transform: uppercase; letter-spacing: 0.06em; }
.admin-pipeline-summary.concise {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  border-block: 1px solid var(--theme-foreground-faintest);
}
.admin-pipeline-summary.concise > div { display: flex; align-items: baseline; gap: 0.45rem; padding: 0.7rem 0.85rem; border-left: 1px solid var(--theme-foreground-faintest); }
.admin-pipeline-summary.concise > div:first-child { border-left: 0; }
.admin-pipeline-summary.concise strong { font-size: 1.15rem; font-variant-numeric: tabular-nums; }
.admin-pipeline-summary.concise span { color: var(--theme-foreground-muted); font-size: 0.7rem; }
.admin-pipeline-summary.concise .danger strong { color: #c62828; }
.admin-pipeline-toolbar { display: flex; justify-content: space-between; align-items: end; gap: 1rem; }
.admin-pipeline-toolbar form { margin: 0; }
.admin-pipeline-toolbar > span { color: var(--theme-foreground-muted); font-size: 0.72rem; }
.admin-pipeline-list { border-top: 1px solid var(--theme-foreground-faintest); }
.admin-pipeline-entry { border-bottom: 1px solid var(--theme-foreground-faintest); }
.admin-pipeline-row {
  appearance: none;
  width: 100%;
  display: grid;
  grid-template-columns: minmax(10rem, 0.9fr) minmax(10rem, 0.9fr) minmax(16rem, 1.6fr) minmax(8rem, 0.8fr) 1rem;
  align-items: center;
  gap: 0.9rem;
  min-height: 4.2rem;
  padding: 0.55rem 0.75rem;
  border: 0;
  border-bottom: 0;
  background: transparent;
  color: inherit;
  text-align: left;
  cursor: pointer;
}
.admin-pipeline-row:hover,
.admin-pipeline-row.selected { background: color-mix(in srgb, #315b8a 7%, transparent); }
.admin-pipeline-row.selected { box-shadow: inset 3px 0 #315b8a; }
.admin-pipeline-identity,
.admin-pipeline-state,
.admin-pipeline-state > span { display: grid; min-width: 0; }
.admin-pipeline-identity strong { font-size: 0.9rem; }
.admin-pipeline-identity small,
.admin-pipeline-state small { overflow: hidden; color: var(--theme-foreground-muted); font-size: 0.68rem; text-overflow: ellipsis; white-space: nowrap; }
.admin-pipeline-state { grid-template-columns: 0.55rem minmax(0, 1fr); align-items: center; gap: 0.5rem; }
.admin-pipeline-state > i { width: 0.5rem; height: 0.5rem; border-radius: 50%; background: var(--state-color); box-shadow: 0 0 0 3px color-mix(in srgb, var(--state-color) 14%, transparent); }
.admin-pipeline-state strong { font-size: 0.76rem; letter-spacing: 0.01em; }
.admin-pipeline-message {
  display: -webkit-box;
  min-width: 0;
  overflow: hidden;
  color: var(--theme-foreground-muted);
  font-size: 0.73rem;
  line-height: 1.4;
  -webkit-box-orient: vertical;
  -webkit-line-clamp: 2;
}
.admin-stage-rail { display: grid; grid-template-columns: repeat(4, minmax(1.3rem, 1fr)); gap: 0.3rem; }
.admin-stage-rail > i { position: relative; height: 0.35rem; border-radius: 99px; background: color-mix(in srgb, var(--theme-foreground-faintest) 90%, transparent); }
.admin-stage-rail > i.done { background: #3d8a53; }
.admin-stage-rail > i.active { background: #315b8a; animation: admin-pulse 1.7s ease-in-out infinite; }
.admin-stage-rail > i.issue { background: #c13c32; }
.admin-stage-rail > i.future { background: color-mix(in srgb, var(--theme-foreground-muted) 22%, transparent); }
.admin-stage-rail > i.not-applicable { background: transparent; border: 1px dashed color-mix(in srgb, var(--theme-foreground-muted) 32%, transparent); }
.admin-stage-rail > i span { position: absolute; width: 1px; height: 1px; overflow: hidden; clip: rect(0 0 0 0); }
@keyframes admin-pulse { 50% { opacity: 0.45; } }
.admin-pipeline-lifecycle { color: var(--theme-foreground-muted); font-size: 0.68rem; text-transform: uppercase; letter-spacing: 0.05em; }
.admin-row-chevron { color: var(--theme-foreground-muted); font-size: 1.25rem; }
.admin-pipeline-dossier {
  margin: 0 0.75rem 1rem;
  border: 1px solid color-mix(in srgb, #315b8a 30%, var(--theme-foreground-faintest));
  border-left: 3px solid #315b8a;
  background: color-mix(in srgb, var(--theme-background) 96%, #dfe9f3 4%);
}
.admin-dossier-lead { display: grid; gap: 0.32rem; padding: 1rem 1.05rem; border-left: 4px solid #738091; }
.admin-dossier-lead.active { border-left-color: #315b8a; }
.admin-dossier-lead.danger { border-left-color: #c13c32; background: color-mix(in srgb, #c13c32 4%, transparent); }
.admin-dossier-lead.success { border-left-color: #3d8a53; }
.admin-dossier-lead > strong { font-size: 1.02rem; }
.admin-dossier-lead > p { max-width: 72ch; margin: 0; font-size: 0.82rem; line-height: 1.55; }
.admin-human-progress { display: grid; gap: 0.35rem; max-width: 46rem; margin-top: 0.45rem; }
.admin-human-progress > div:first-child { display: flex; justify-content: space-between; gap: 1rem; font-size: 0.75rem; }
.admin-human-progress-track { height: 0.5rem; overflow: hidden; border-radius: 99px; background: color-mix(in srgb, var(--theme-foreground-muted) 16%, transparent); }
.admin-human-progress-track i { display: block; height: 100%; border-radius: inherit; background: #315b8a; }
.admin-human-progress small { color: var(--theme-foreground-muted); font-size: 0.68rem; }
.admin-human-error { display: grid; gap: 0.15rem; max-width: 70ch; margin-top: 0.45rem; padding: 0.7rem 0.8rem; border-left: 3px solid #c13c32; background: color-mix(in srgb, #c13c32 7%, transparent); }
.admin-human-error strong { font-size: 0.72rem; }
.admin-human-error span { font-size: 0.76rem; line-height: 1.45; }
.admin-human-error small { margin-top: 0.2rem; color: var(--theme-foreground-muted); font-size: 0.7rem; line-height: 1.45; }
.admin-milestone-line { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); border-top: 1px solid var(--theme-foreground-faintest); }
.admin-milestone { position: relative; display: grid; grid-template-columns: 0.8rem minmax(0, 1fr); gap: 0.15rem 0.45rem; min-height: 4.2rem; padding: 0.75rem; border-left: 1px solid var(--theme-foreground-faintest); }
.admin-milestone:first-child { border-left: 0; }
.admin-milestone > i { grid-row: 1 / 3; width: 0.7rem; height: 0.7rem; margin-top: 0.14rem; border-radius: 50%; border: 2px solid #a2aab3; background: var(--theme-background); }
.admin-milestone.done > i { border-color: #3d8a53; background: #3d8a53; box-shadow: inset 0 0 0 2px var(--theme-background); }
.admin-milestone.active > i { border-color: #315b8a; background: #315b8a; animation: admin-pulse 1.7s ease-in-out infinite; }
.admin-milestone.issue > i { border-color: #c13c32; background: #c13c32; }
.admin-milestone.not-applicable > i { border-style: dashed; }
.admin-milestone > span { color: var(--theme-foreground-muted); font-size: 0.63rem; font-weight: 750; letter-spacing: 0.05em; text-transform: uppercase; }
.admin-milestone > strong { font-size: 0.72rem; line-height: 1.35; }
.admin-dossier-facts { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); margin: 0; border-top: 1px solid var(--theme-foreground-faintest); }
.admin-dossier-facts > div { min-width: 0; padding: 0.7rem 0.8rem; border-left: 1px solid var(--theme-foreground-faintest); border-bottom: 1px solid var(--theme-foreground-faintest); }
.admin-dossier-facts > div:nth-child(3n + 1) { border-left: 0; }
.admin-dossier-facts dt { color: var(--theme-foreground-muted); font-size: 0.61rem; font-weight: 750; letter-spacing: 0.07em; text-transform: uppercase; }
.admin-dossier-facts dd { margin: 0.2rem 0 0; overflow-wrap: anywhere; font-size: 0.72rem; font-weight: 650; }
.admin-dossier-actions { display: flex; flex-wrap: wrap; gap: 0.4rem; padding: 0.75rem 1rem; border-top: 1px solid var(--theme-foreground-faintest); }
.admin-advanced-actions { border-top: 1px solid var(--theme-foreground-faintest); }
.admin-advanced-actions summary { padding: 0.65rem 1rem; color: var(--theme-foreground-muted); font-size: 0.72rem; font-weight: 700; cursor: pointer; }
.admin-advanced-actions > div { display: flex; flex-wrap: wrap; gap: 0.4rem; padding: 0 1rem 0.8rem; }
.admin-dossier-log { border-top: 1px solid var(--theme-foreground-faintest); }
.admin-dossier-log summary { padding: 0.6rem 1rem; color: var(--theme-foreground-muted); font-size: 0.72rem; font-weight: 700; cursor: pointer; }
.admin-onboarding-console { display: grid; gap: 0.75rem; }
.admin-registration-callout { display: grid; gap: 0.2rem; padding: 0.75rem 0.9rem; border-left: 3px solid #d98c2f; background: color-mix(in srgb, #d98c2f 7%, transparent); }
.admin-registration-callout.registered { border-left-color: #3d8a53; background: color-mix(in srgb, #3d8a53 7%, transparent); }
.admin-registration-callout span { color: var(--theme-foreground-muted); font-size: 0.76rem; }
.admin-field-help { max-width: 48rem; margin-top: -0.4rem; color: var(--theme-foreground-muted); font-size: 0.72rem; line-height: 1.45; }
.admin-registration-policy { display: grid; grid-template-columns: minmax(16rem, 1.5fr) minmax(13rem, 1fr); gap: 0.8rem; max-width: 48rem; }
.admin-registration-policy form { margin: 0; }
.admin-empty-state { display: grid; gap: 0.2rem; border-block: 1px solid var(--theme-foreground-faintest); }
.admin-wiki-focus { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); margin-bottom: 0.75rem; border-block: 1px solid var(--theme-foreground-faintest); }
.admin-wiki-focus > div { display: grid; gap: 0.25rem; padding: 0.75rem 0.85rem; border-left: 1px solid var(--theme-foreground-faintest); }
.admin-wiki-focus > div:first-child { border-left: 0; }
.admin-wiki-focus strong { font-size: 0.78rem; }
.admin-run-evidence { display: grid; gap: 0.45rem; margin: 0.65rem 0; border-left: 3px solid #315b8a; background: color-mix(in srgb, #315b8a 5%, transparent); padding: 0.75rem 0.85rem; }
.admin-run-evidence.danger { border-left-color: #c13c32; background: color-mix(in srgb, #c13c32 5%, transparent); }
.admin-run-evidence.success { border-left-color: #2e7d32; }
.admin-run-evidence > div:first-child { display: flex; justify-content: space-between; gap: 1rem; }
.admin-run-evidence span { color: var(--theme-foreground-muted); font-size: 0.74rem; }
.admin-btn:disabled { cursor: not-allowed; opacity: 0.48; transform: none; box-shadow: none; }
.admin-refresh-panel { overflow-x: auto; }
.admin-health-alerts { display: grid; border-top: 1px solid var(--theme-foreground-faintest); }
.admin-health-alerts > div { display: grid; grid-template-columns: minmax(10rem, 0.45fr) minmax(16rem, 1.55fr); gap: 0.8rem; padding: 0.55rem 0.75rem; border-bottom: 1px solid var(--theme-foreground-faintest); border-left: 3px solid #c13c32; }
.admin-health-alerts > div.warning { border-left-color: #d98c2f; margin: 0; border-radius: 0; background: transparent; }
.admin-health-alerts strong { font-size: 0.68rem; letter-spacing: 0.05em; text-transform: uppercase; }
.admin-health-alerts span { color: var(--theme-foreground-muted); font-size: 0.74rem; }
.admin-health-clear { padding: 0.65rem 0.8rem; border-left: 3px solid #2e7d32; color: #2e7d32; font-size: 0.76rem; font-weight: 700; }
.admin-history-details { margin-top: 0.25rem; }
.admin-history-details summary { cursor: pointer; color: var(--theme-foreground-muted); font-size: 0.74rem; font-weight: 700; }
.admin-run-sheet { margin-top: 1rem; border-block: 1px solid var(--theme-foreground-faintest); background: color-mix(in srgb, var(--theme-background) 96%, #dfe9f3 4%); }
.admin-run-sheet-header { display: flex; justify-content: space-between; gap: 1rem; padding: 1rem; border-bottom: 1px solid var(--theme-foreground-faintest); }
.admin-run-sheet-header > div { display: grid; gap: 0.2rem; min-width: 0; }
.admin-run-sheet-header h3 { margin: 0; font-size: 1.05rem; }
.admin-run-sheet-header code { overflow: hidden; color: var(--theme-foreground-muted); font-size: 0.68rem; text-overflow: ellipsis; }
.admin-run-sheet-state { width: fit-content; color: #607d8b; font-size: 0.68rem; font-weight: 800; }
.admin-run-sheet-state.danger { color: #c13c32; }
.admin-run-sheet-state.active { color: #315b8a; }
.admin-run-sheet-state.success { color: #2e7d32; }
.admin-run-facts { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); border-bottom: 1px solid var(--theme-foreground-faintest); }
.admin-run-facts > div { display: grid; gap: 0.12rem; padding: 0.72rem 0.85rem; border-left: 1px solid var(--theme-foreground-faintest); }
.admin-run-facts > div:nth-child(3n + 1) { border-left: 0; }
.admin-run-facts span { color: var(--theme-foreground-muted); font-size: 0.62rem; font-weight: 700; }
.admin-run-facts strong { font-size: 0.78rem; }
.admin-run-facts small { color: var(--theme-foreground-muted); font-size: 0.65rem; }
.admin-run-timeline { position: relative; padding: 0.6rem 1rem; }
.admin-run-stage { display: grid; grid-template-columns: 0.85rem minmax(10rem, 1fr) 5rem; gap: 0.6rem; align-items: start; min-height: 2.8rem; }
.admin-run-stage > i { position: relative; width: 0.62rem; height: 0.62rem; margin-top: 0.28rem; border: 2px solid #738091; border-radius: 50%; background: var(--theme-background); }
.admin-run-stage:not(:last-child) > i::after { position: absolute; top: 0.72rem; left: 0.18rem; width: 1px; height: 2rem; background: var(--theme-foreground-faintest); content: ""; }
.admin-run-stage.succeeded > i { border-color: #2e7d32; background: #2e7d32; }
.admin-run-stage.failed > i { border-color: #c13c32; background: #c13c32; }
.admin-run-stage.running > i { border-color: #315b8a; background: #315b8a; }
.admin-run-stage > div { display: grid; }
.admin-run-stage strong { font-size: 0.76rem; }
.admin-run-stage span, .admin-run-stage time { color: var(--theme-foreground-muted); font-size: 0.66rem; }
.admin-run-stage p { grid-column: 2 / -1; margin: 0 0 0.75rem; color: #c13c32; font-size: 0.7rem; }
.admin-run-diagnosis { display: grid; gap: 0.3rem; padding: 0.9rem 1rem; border-top: 1px solid var(--theme-foreground-faintest); border-left: 4px solid #c13c32; }
.admin-run-diagnosis > span { color: #c13c32; font-size: 0.65rem; font-weight: 800; }
.admin-run-diagnosis > strong { max-width: 72ch; font-size: 0.85rem; }
.admin-run-diagnosis > p { max-width: 72ch; margin: 0; color: var(--theme-foreground-muted); font-size: 0.74rem; }
.admin-action-blocked { align-self: center; color: var(--theme-foreground-muted); font-size: 0.72rem; }
.admin-run-provenance { padding: 0.7rem 1rem; border-top: 1px solid var(--theme-foreground-faintest); }
.admin-run-provenance summary { cursor: pointer; font-size: 0.72rem; font-weight: 700; }
.admin-run-provenance pre { max-height: 24rem; overflow: auto; font-size: 0.68rem; white-space: pre-wrap; }
.admin-inline-advanced > div, .admin-recovery-actions { display: flex; flex-wrap: wrap; gap: 0.4rem; margin-top: 0.5rem; }
.admin-preflight { border-top: 1px solid var(--theme-foreground-faintest); border-left: 4px solid #738091; }
.admin-preflight.eligible { border-left-color: #2e7d32; }
.admin-preflight.blocked { border-left-color: #c13c32; }
.admin-preflight > header { display: flex; justify-content: space-between; gap: 1rem; padding: 0.75rem 0.9rem; }
.admin-preflight > header > div { display: grid; }
.admin-preflight > header span, .admin-preflight > header time { color: var(--theme-foreground-muted); font-size: 0.65rem; }
.admin-preflight > header strong { font-size: 0.82rem; }
.admin-preflight > p, .admin-preflight > ul { margin: 0; padding: 0 0.9rem 0.75rem; font-size: 0.72rem; }
.admin-preflight > ul { padding-left: 2rem; color: #c13c32; }
.admin-preflight details { border-top: 1px solid var(--theme-foreground-faintest); }
.admin-preflight summary { padding: 0.65rem 0.9rem; cursor: pointer; font-size: 0.7rem; font-weight: 700; }
.admin-change-plan { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); border-top: 1px solid var(--theme-foreground-faintest); }
.admin-change-plan > div { padding: 0.75rem 0.9rem; border-left: 1px solid var(--theme-foreground-faintest); }
.admin-change-plan > div:first-child { border-left: 0; }
.admin-change-plan > div > strong { font-size: 0.72rem; }
.admin-change-plan ul { display: grid; grid-template-columns: repeat(auto-fill, minmax(10rem, 1fr)); gap: 0.25rem 0.8rem; padding: 0; list-style: none; }
.admin-change-plan li { display: flex; justify-content: space-between; gap: 0.5rem; font-size: 0.68rem; }
.admin-change-plan li span, .admin-change-plan p { color: var(--theme-foreground-muted); }
.admin-lifecycle-console { border-top: 1px solid var(--theme-foreground-faintest); background: color-mix(in srgb, #315b8a 3%, transparent); }
.admin-lifecycle-console.qualification { background: color-mix(in srgb, #d98c2f 4%, transparent); }
.admin-lifecycle-console > header { display: flex; justify-content: space-between; align-items: start; gap: 1rem; padding: 0.8rem 1rem; border-bottom: 1px solid var(--theme-foreground-faintest); }
.admin-lifecycle-console > header > div { display: grid; gap: 0.15rem; }
.admin-lifecycle-console > header span { color: var(--theme-foreground-muted); font-size: 0.61rem; font-weight: 800; letter-spacing: 0.07em; text-transform: uppercase; }
.admin-lifecycle-console > header strong { font-size: 0.82rem; }
.admin-lifecycle-console > header code { max-width: 12rem; overflow: hidden; color: var(--theme-foreground-muted); font-size: 0.64rem; text-overflow: ellipsis; }
.admin-lifecycle-evidence { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); border-bottom: 1px solid var(--theme-foreground-faintest); }
.admin-lifecycle-evidence > div { display: grid; gap: 0.18rem; min-width: 0; padding: 0.65rem 0.8rem; border-left: 1px solid var(--theme-foreground-faintest); }
.admin-lifecycle-evidence > div:first-child { border-left: 0; }
.admin-lifecycle-evidence span, .admin-lifecycle-readonly span { color: var(--theme-foreground-muted); font-size: 0.6rem; font-weight: 750; letter-spacing: 0.05em; text-transform: uppercase; }
.admin-lifecycle-evidence strong, .admin-lifecycle-evidence code { overflow: hidden; font-size: 0.7rem; text-overflow: ellipsis; white-space: nowrap; }
.admin-lifecycle-policy { display: grid; grid-template-columns: repeat(3, minmax(10rem, 1fr)); gap: 0.55rem; padding: 0.7rem 1rem; }
.admin-lifecycle-select, .admin-lifecycle-number { width: 100%; min-height: 2.25rem; padding: 0.38rem 0.5rem; border: 1px solid var(--theme-foreground-faintest); border-radius: 0.25rem; background: var(--theme-background); color: var(--theme-foreground); font: inherit; font-size: 0.72rem; }
.admin-lifecycle-select:focus-visible, .admin-lifecycle-number:focus-visible { outline: 2px solid #315b8a; outline-offset: 2px; }
.admin-lifecycle-readonly { display: grid; align-content: center; gap: 0.1rem; min-height: 2.25rem; padding-inline: 0.55rem; border-left: 2px solid #315b8a; }
.admin-lifecycle-readonly strong { font-size: 0.72rem; text-transform: capitalize; }
.admin-lifecycle-actions { display: flex; flex-wrap: wrap; gap: 0.4rem; padding: 0 1rem 0.75rem; }
.admin-lifecycle-footnote, .admin-lifecycle-empty { margin: 0; padding: 0 1rem 0.75rem; color: var(--theme-foreground-muted); font-size: 0.67rem; line-height: 1.45; }
.admin-audit-ledger { border-block: 1px solid var(--theme-foreground-faintest); }
.admin-audit-ledger > header { display: grid; grid-template-columns: 10rem 10rem minmax(14rem, 1fr); }
.admin-audit-ledger > header > div { display: grid; gap: 0.15rem; min-width: 0; padding: 0.7rem 0.8rem; border-left: 1px solid var(--theme-foreground-faintest); }
.admin-audit-ledger > header > div:first-child { border-left: 0; }
.admin-audit-ledger > header span { color: var(--theme-foreground-muted); font-size: 0.61rem; font-weight: 750; letter-spacing: 0.05em; text-transform: uppercase; }
.admin-audit-ledger > header strong { font-size: 0.9rem; }
.admin-audit-ledger > header strong.success { color: #2e7d32; }
.admin-audit-ledger > header strong.danger { color: #c13c32; }
.admin-audit-ledger > header code { overflow: hidden; font-size: 0.68rem; text-overflow: ellipsis; white-space: nowrap; }
.admin-audit-warning { display: grid; gap: 0.15rem; padding: 0.7rem 0.8rem; border-top: 1px solid var(--theme-foreground-faintest); border-left: 4px solid #c13c32; background: color-mix(in srgb, #c13c32 6%, transparent); }
.admin-audit-warning strong { color: #c13c32; font-size: 0.72rem; }
.admin-audit-warning span { font-size: 0.68rem; overflow-wrap: anywhere; }
.admin-audit-table-wrap { max-height: 26rem; overflow: auto; border-top: 1px solid var(--theme-foreground-faintest); }
.admin-audit-table { width: 100%; border-collapse: collapse; font-size: 0.7rem; }
.admin-audit-table th { position: sticky; top: 0; z-index: 1; padding: 0.55rem 0.65rem; background: var(--theme-background); color: var(--theme-foreground-muted); font-size: 0.6rem; letter-spacing: 0.06em; text-align: left; text-transform: uppercase; }
.admin-audit-table td { padding: 0.55rem 0.65rem; border-top: 1px solid var(--theme-foreground-faintest); vertical-align: top; }
.admin-audit-table td:last-child { display: grid; gap: 0.12rem; }
.admin-audit-table small { color: var(--theme-foreground-muted); font-size: 0.58rem; }
.admin-audit-phase { font-weight: 750; }
.admin-audit-phase.applied, .admin-audit-phase.completed { color: #2e7d32; }
.admin-audit-phase.failed { color: #c13c32; }
.admin-audit-phase.requested { color: #315b8a; }
.admin-audit-empty { padding: 0.8rem; border-top: 1px solid var(--theme-foreground-faintest); color: var(--theme-foreground-muted); font-size: 0.72rem; }
[data-theme="dark"] .admin-command-header { --admin-ink: #d7e2ee; background: color-mix(in srgb, var(--theme-background) 96%, #26384c 4%); }
@media (prefers-reduced-motion: reduce) {
  .admin-stage-rail > i.active,
  .admin-job-panel.running .admin-progress-fill { animation: none; }
}
@media (max-width: 1100px) {
  .admin-command-header { grid-template-columns: 1fr; }
  .admin-command-facts { border: 0; border-block: 1px solid var(--admin-line); }
  .admin-command-session { grid-auto-flow: column; justify-content: start; }
  .admin-pipeline-row { grid-template-columns: minmax(9rem, 0.8fr) minmax(9rem, 0.8fr) minmax(13rem, 1.2fr) minmax(7rem, 0.7fr) 1rem; }
  .pipeline-stage-grid {
    grid-template-columns: repeat(3, minmax(0, 1fr));
  }
}
@media (max-width: 760px) {
  .admin-view-navigation {
    top: 0;
    grid-template-columns: repeat(4, minmax(5.2rem, 1fr));
    overflow-x: auto;
    overscroll-behavior-inline: contain;
    scrollbar-width: thin;
  }
  .admin-view-navigation button { min-height: 2.8rem; }
  .admin-operation-receipts > header { align-items: flex-start; }
  .admin-operation-receipts.empty { display: grid; }
  .admin-operation-receipt { grid-template-columns: 5.5rem minmax(0, 1fr); }
  .admin-operation-receipt time { grid-column: 2; }
  .admin-quality-summary { grid-template-columns: repeat(2, minmax(0, 1fr)); }
  .admin-quality-summary > div:nth-child(2) { border-right: 0; }
  .admin-quality-wiki > summary { grid-template-columns: 1fr; gap: 0.35rem; }
  .admin-quality-counts { padding-left: 1.65rem; flex-wrap: wrap; }
  .admin-quality-alerts,
  .admin-quality-signal-strip,
  .admin-quality-table-wrap { margin-left: 0; }
  .admin-quality-alerts > div { grid-template-columns: 1fr; gap: 0.15rem; }
  .admin-quality-signal-strip { grid-template-columns: repeat(2, minmax(0, 1fr)); }
  .admin-quality-signal-strip > div:nth-child(3n) { border-right: 1px solid var(--theme-foreground-faintest); }
  .admin-quality-signal-strip > div:nth-child(2n) { border-right: 0; }
  .admin-quality-table thead { display: none; }
  .admin-quality-table,
  .admin-quality-table tbody,
  .admin-quality-table tr,
  .admin-quality-table th,
  .admin-quality-table td { display: block; width: 100%; }
  .admin-quality-table tr { border-top: 1px solid var(--theme-foreground-faintest); padding: 0.7rem 0; }
  .admin-quality-table th,
  .admin-quality-table td { border-top: 0; padding: 0.35rem 0.5rem; }
  .admin-quality-table td::before { content: attr(data-label); display: block; color: var(--theme-foreground-muted); font-size: 0.65rem; margin-bottom: 0.25rem; }
  .admin-command-facts { grid-template-columns: repeat(2, minmax(0, 1fr)); }
  .admin-command-facts > div:nth-child(3) { border-left: 0; }
  .admin-command-facts > div:nth-child(n+3) { border-top: 1px solid var(--admin-line); }
  .admin-activity-row { grid-template-columns: 6.5rem minmax(0, 1fr) 4.5rem; gap: 0.6rem; }
  .admin-activity-source { display: none; }
  .admin-pipeline-summary.concise { grid-template-columns: repeat(2, minmax(0, 1fr)); }
  .admin-pipeline-summary.concise > div:nth-child(3) { border-left: 0; }
  .admin-pipeline-summary.concise > div:nth-child(n+3) { border-top: 1px solid var(--theme-foreground-faintest); }
  .admin-pipeline-row { grid-template-columns: minmax(7.8rem, 0.8fr) minmax(8.5rem, 1fr) 1rem; gap: 0.55rem 0.65rem; padding-block: 0.75rem; }
  .admin-pipeline-message { grid-column: 1 / -1; grid-row: 2; -webkit-line-clamp: 3; }
  .admin-stage-rail { grid-column: 1 / -1; grid-row: 3; }
  .admin-row-chevron { grid-column: 3; grid-row: 1; }
  .admin-pipeline-dossier { margin-inline: 0; }
  .admin-milestone-line { grid-template-columns: repeat(2, minmax(0, 1fr)); }
  .admin-milestone:nth-child(3),
  .admin-milestone:nth-child(4) { border-top: 1px solid var(--theme-foreground-faintest); }
  .admin-milestone:nth-child(3) { border-left: 0; }
  .admin-dossier-facts { grid-template-columns: repeat(2, minmax(0, 1fr)); }
  .admin-dossier-facts > div:nth-child(3n + 1) { border-left: 1px solid var(--theme-foreground-faintest); }
  .admin-dossier-facts > div:nth-child(odd) { border-left: 0; }
  .admin-registration-policy { grid-template-columns: 1fr; }
  .admin-wiki-focus { grid-template-columns: repeat(2, minmax(0, 1fr)); }
  .admin-wiki-focus > div:nth-child(3) { border-left: 0; }
  .admin-wiki-focus > div:nth-child(n+3) { border-top: 1px solid var(--theme-foreground-faintest); }
  .admin-run-evidence > div:first-child { display: grid; }
  .admin-run-facts { grid-template-columns: repeat(2, minmax(0, 1fr)); }
  .admin-run-facts > div:nth-child(3n + 1) { border-left: 1px solid var(--theme-foreground-faintest); }
  .admin-run-facts > div:nth-child(odd) { border-left: 0; }
  .admin-run-sheet-header { align-items: start; }
  .admin-run-stage { grid-template-columns: 0.85rem minmax(0, 1fr) 4rem; }
  .admin-change-plan { grid-template-columns: 1fr; }
  .admin-change-plan > div { border-left: 0; border-top: 1px solid var(--theme-foreground-faintest); }
  .admin-change-plan > div:first-child { border-top: 0; }
  .admin-lifecycle-evidence { grid-template-columns: repeat(2, minmax(0, 1fr)); }
  .admin-lifecycle-evidence > div:nth-child(3) { border-left: 0; border-top: 1px solid var(--theme-foreground-faintest); }
  .admin-lifecycle-evidence > div:nth-child(4) { border-top: 1px solid var(--theme-foreground-faintest); }
  .admin-lifecycle-policy { grid-template-columns: 1fr; }
  .admin-audit-ledger > header { grid-template-columns: repeat(2, minmax(0, 1fr)); }
  .admin-audit-ledger > header > div:last-child { grid-column: 1 / -1; border-top: 1px solid var(--theme-foreground-faintest); border-left: 0; }
  .admin-audit-table thead { display: none; }
  .admin-audit-table, .admin-audit-table tbody, .admin-audit-table tr, .admin-audit-table td { display: block; width: 100%; }
  .admin-audit-table tr { padding: 0.5rem 0; border-top: 1px solid var(--theme-foreground-faintest); }
  .admin-audit-table td { display: block; padding: 0.25rem 0.6rem; border-top: 0; }
  .admin-audit-table td:last-child { display: grid; }
  .admin-audit-table td::before { display: block; margin-bottom: 0.08rem; color: var(--theme-foreground-muted); content: attr(data-label); font-size: 0.56rem; font-weight: 700; letter-spacing: 0.04em; text-transform: uppercase; }
  .pipeline-stage-grid {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }
  .pipeline-card {
    padding: 0.85rem;
  }
  .pipeline-card-top {
    gap: 0.75rem;
  }
}
</style>
