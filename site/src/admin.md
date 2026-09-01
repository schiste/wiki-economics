---
title: Admin
---

# Operations

<div class="page-intro">

See what is running, what needs attention, and what is ready to publish. Durable operator, fleet, snapshot, and publication evidence is reconciled into the same six-stage path for every wiki.

</div>

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
function setSelectedWiki(value, userInitiated = true) {
  if (userInitiated) adminUiState.selectedWikiUser = true
  selectedWikiState.value = value
}
const adminUiState = globalThis.__wikiEconAdminState ??= {
  showRunningLog: false,
  showJobLog: false,
  onboardingWiki: null,
  snapshotVersion: "",
  snapshotVersionDirty: false,
  lastKnownRunner: null,
  onboardingMode: "qualification",
  onboardingResourceClass: "medium_large",
  notice: null
}
adminUiState.selectedWikiUser ??= false
let pollTimer = null
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

function preferredSnapshotVersion(wikiStatus = null) {
  return validSnapshotVersion(wikiStatus?.snapshot?.version)
    ?? validSnapshotVersion(wikiStatus?.raw?.version)
    ?? validSnapshotVersion(adminUiState.snapshotVersion)
    ?? validSnapshotVersion(jobStatus.value?.suggestedVersion)
    ?? null
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
    } else {
      apiAvailable.value = false
      jobStatus.value = data
    }
  } catch {
    apiAvailable.value = false
    jobStatus.value = null
  }
}

async function runCommand(action, wikiOrOptions = null) {
  try {
    const options = typeof wikiOrOptions === "string"
      ? {wiki: wikiOrOptions}
      : (wikiOrOptions ?? {})
    const requestedVersion = normalizeSnapshotVersion(options.version)
    if (requestedVersion && !SNAPSHOT_VERSION_RE.test(requestedVersion)) {
      alert("Invalid snapshot version. Use YYYY-MM.")
      return
    }
    const body = JSON.stringify({
      ...(options.wiki ? {wiki: options.wiki} : {}),
      ...(requestedVersion ? {version: requestedVersion} : {}),
      ...(options.requestId ? {requestId: options.requestId} : {}),
      ...(options.mode ? {mode: options.mode} : {}),
      ...(options.resourceClass ? {resourceClass: options.resourceClass} : {})
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
      alert("Admin authentication required. Sign in again to continue.")
      return null
    }
    if (data.error) { alert(data.error); return null }
    adminUiState.notice = data.queued
      ? `${actionLabel(action)} queued as ${data.requestId}`
      : `${actionLabel(action)} accepted`
    adminUiState.showRunningLog = false
    adminUiState.showJobLog = false
    await checkApi()
    // Start polling
    if (pollTimer) clearInterval(pollTimer)
    pollTimer = setInterval(async () => {
      await checkApi()
      const operations = jobStatus.value?.adminOperations
      if (jobStatus.value && !jobStatus.value.running && !operations?.counts?.running && !operations?.counts?.queued) {
        clearInterval(pollTimer)
        pollTimer = null
      }
    }, 500)
    return data
  } catch (e) {
    alert("Admin server not reachable. Run scripts/dev.sh or WIKI_ECON_ADMIN_ENABLED=1 node site/admin-server.cjs")
    return null
  }
}

async function registerWiki(wiki, mode, resourceClass, {start = false, version = null} = {}) {
  const result = await runCommand(start ? "onboard-wiki" : "register-wiki", {wiki, mode, resourceClass, version})
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
    case "patrol-compute": return "patrol compute"
    case "patrol-rebuild": return "rebuild patrol"
    case "merge": return "merge data"
    case "publish": return "publish candidates"
    case "site": return "rebuild site"
    case "fleet-recover": return "recover fleet"
    case "recover-admin": return "recover operator queue"
    case "qualify": return "qualify project"
    case "register-wiki": return "add project"
    case "onboard-wiki": return "add and start project"
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
      return "Compute the patrol-specific metrics incrementally, resuming from existing month shards; full rebuilds stay CLI-only."
    case "patrol-rebuild":
      return "Discard and rebuild only this wiki's patrol metrics from its validated patrol sources."
    case "merge":
      return "Regenerate merged publication data without rebuilding the website."
    case "publish":
      return "Select valid ready candidates, validate the publication, and atomically switch the live generation."
    case "site":
      return "Rebuild and validate only the website against the currently published data."
    case "fleet-recover":
      return "Reclaim stale fleet leases and requeue recoverable work without touching healthy tasks."
    case "recover-admin":
      return "Requeue an operator operation only after its dedicated worker heartbeat has been stale for ten minutes."
    case "qualify":
      return "Run fetch, ingest, compute, patrol, and validation as a publication-invisible qualification."
    case "register-wiki":
      return "Persist this project in the lifecycle registry before any data is downloaded."
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

checkApi()
// Poll every 3s to detect if server comes online or job finishes
const bgTimer = setInterval(checkApi, 3000)
// Clean up intervals on hot-reload to prevent stale Mutable references
invalidation.then(() => {
  clearInterval(bgTimer)
  if (pollTimer) clearInterval(pollTimer)
})
```

```js
const apiStatus = apiAvailable
const job = jobStatus
const auth = authState
const currentManifest = liveManifest || initialManifest || {generated_at: "unknown", wikis: {}, merged: []}
const currentWikis = currentManifest.wikis || {}
const lifecycleStates = job?.wikiStates || currentManifest.lifecycle?.wikis || {}
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
const latestPlanByWiki = new Map(snapshotPlans.map((plan) => [plan.wiki, plan]))
const supportedWikis = Array.from(new Set(job?.supportedWikis || [])).sort((a, b) => a.localeCompare(b))
const suggestedVersion = normalizeSnapshotVersion(job?.suggestedVersion) || ""
```

<p class="filter-desc">Last scanned: ${currentManifest.generated_at}${apiStatus ? html` · <span style="color:#2e7d32">API connected</span>` : html` · ${adminConnectionHelp()}`}</p>

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
  return operatorOperationByWiki.get(name) || wikiJobMap[name] || wikiJobHistory[name]?.[0] || null
}

function operationalState(name, wiki) {
  const direct = latestWikiJob(name)
  const fleetWork = fleetByWiki.get(name)
  if (direct?.running || direct?.state === "running" || direct?.state === "cancelling") return direct.state || "running"
  if (direct?.state === "queued") return "queued"
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
  running: 4,
  cancelling: 5,
  queued: 6,
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
const selectedWikiCandidate = (adminUiState.selectedWikiUser && wikiNames.includes(selectedWikiState) ? selectedWikiState : null)
  || runningWiki
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
const freshnessNeedsAttention = ["critical", "warning"].includes(freshness.status)
const operatorIssueCount = attentionCount + Number(freshnessNeedsAttention)
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
    <div><dt>API</dt><dd class=${apiStatus ? "ok" : "bad"}>${apiStatus ? "Connected" : "Offline"}</dd></div>
    <div><dt>Operator worker</dt><dd>${adminOperations.executionMode === "queue" ? "Dedicated 6 GiB" : "Local direct"} · ${adminOperations.counts?.queued || 0} queued</dd></div>
    <div><dt>Coverage</dt><dd>${publishedWikis.length} published · ${refreshWikis.length} scheduled</dd></div>
    <div><dt>Inventory scan</dt><dd>${formatRefreshTimestamp(currentManifest.generated_at)}</dd></div>
  </dl>
  <div class="admin-command-session">
    <span>${auth?.user?.email || (auth?.enabled ? "Sign-in required" : "Local operator")}</span>
    ${auth?.logoutUrl ? html`<a href=${auth.logoutUrl}>Sign out</a>` : ""}
  </div>
</div>`)
```

```js
topLevelJob
  ? html`<div class="admin-job-panel ${topLevelJob.running ? "running" : topLevelJob.cancelled ? "failed" : topLevelJob.exitCode === 0 ? "success" : "failed"}">
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

<div class="chart-section admin-activity-section">

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
  if (state === "queued") return "waiting"
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
  ...(adminRuns.recent || []).map((entry) => ({...entry, source: "operator"}))
]
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
        onclick=${() => { if (operation.wiki) setSelectedWiki(operation.wiki) }}
        ?disabled=${!operation.wiki}
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

</div>

<!-- ── Scheduled refresh panel ─────────────────────────────── -->

<div class="chart-section">

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
```

```js
display(html`<div class="admin-refresh-panel">
  <div class="admin-control-strip">
    <div class="admin-control-chip">
      <span class="admin-control-label">Health</span>
      <strong style=${"color:" + (freshness.status === "healthy" ? "#2e7d32" : freshness.status === "warning" ? "#b26a00" : "#c62828")}>${freshness.status}</strong>
    </div>
    <div class="admin-control-chip">
      <span class="admin-control-label">Publisher</span>
      <strong style=${"color:" + refreshHealthColors[refreshHealth.status]}>${refreshHealth.message}</strong>
    </div>
    <div class="admin-control-chip">
      <span class="admin-control-label">Last publication</span>
      <strong>${formatRefreshTimestamp(freshness.summary?.lastPublicationAt)}</strong>
    </div>
    <div class="admin-control-chip">
      <span class="admin-control-label">Snapshot</span>
      <strong>${freshness.summary?.selectedSnapshot || "—"}</strong>
    </div>
    <div class="admin-control-chip">
      <span class="admin-control-label">Peak memory</span>
      <strong>${formatRefreshBytes(scheduledRefresh.last?.memoryPeakBytes)} / ${formatRefreshBytes(scheduledRefresh.last?.memoryLimitBytes)}</strong>
    </div>
  </div>
  ${(freshness.alerts || []).length ? html`<div class="admin-health-alerts">
    ${(freshness.alerts || []).slice(0, 8).map((alert) => html`<div class=${alert.severity || "warning"}><strong>${alert.code.replaceAll("_", " ")}</strong><span>${alert.message}</span></div>`)}
    ${(freshness.alerts || []).length > 8 ? html`<span>${freshness.alerts.length - 8} more alerts in the public freshness record.</span>` : ""}
  </div>` : html`<div class="admin-health-clear">All publication checks pass.</div>`}
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

<!-- ── Pipeline status matrix ─────────────────────────────── -->

<div class="chart-section">

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
  planned: "#607d8b",
  stalled: "#c62828",
  quarantined: "#c62828",
  interrupted: "#c62828",
  failed: "#c62828",
  cancelled: "#795548"
}
const statusLabels = {
  complete: "Complete",
  needs_fetch: "Needs fetch",
  needs_patrol_fetch: "Needs patrol fetch",
  needs_ingest: "Needs ingest",
  needs_compute: "Needs compute",
  needs_patrol_compute: "Needs patrol compute",
  needs_merge: "Needs merge",
  running: "Running",
  queued: "Queued",
  planned: "Plan only",
  stalled: "Stalled",
  quarantined: "Quarantined",
  interrupted: "Interrupted",
  failed: "Failed",
  cancelled: "Cancelled"
}

const pipelineSteps = [
  {key: "fetch", label: "History"},
  {key: "patrol_fetch", label: "Patrol Source"},
  {key: "ingest", label: "Ingest"},
  {key: "compute", label: "Core Metrics"},
  {key: "patrol_compute", label: "Patrol Metric"},
  {key: "merge", label: "Site Data"},
]

function summarizeStatuses(entries) {
  return entries.reduce((acc, [, wiki]) => {
    const key = wiki.status || "needs_fetch"
    acc[key] = (acc[key] || 0) + 1
    return acc
  }, {})
}

function stageStateForWiki(wiki, stageKey, isRunning, runningProgress) {
  const runningStage = runningProgress?.stage || null
  const active = isRunning && runningStage === stageKey
  if (active) return "active"
  if (!isRunning && wiki.status === "complete") return "done"

  switch (stageKey) {
    case "fetch":
      return wiki.snapshot?.ready || wiki.raw.files > 0 ? "done" : "todo"
    case "patrol_fetch":
      return wiki.patrol?.source_ready ? "done" : (wiki.snapshot?.ready || wiki.raw.files > 0 ? "todo" : "blocked")
    case "ingest":
      return wiki.snapshot?.ready || (wiki.parquet.done > 0 && wiki.parquet.done >= wiki.parquet.total && wiki.parquet.in_progress === 0)
        ? "done"
        : wiki.raw.files > 0
        ? "todo"
        : "blocked"
    case "compute": {
      const coreMetricCount = (wiki.metrics || []).filter((metric) => metric.name !== "patrol").length
      return coreMetricCount >= 8
        ? "done"
        : wiki.snapshot?.ready || wiki.parquet.done > 0
        ? "todo"
        : "blocked"
    }
    case "patrol_compute":
      return wiki.patrol?.metric_ready ? "done" : (wiki.patrol?.source_ready ? "todo" : "blocked")
    case "merge":
      return wiki.dashboard.length > 0 ? "done" : ((wiki.metrics || []).length > 0 ? "todo" : "blocked")
    default:
      return "todo"
  }
}

function stageCaption(wiki, stageKey) {
  switch (stageKey) {
    case "fetch":
      return wiki.snapshot?.ready
        ? `Snapshot ${wiki.snapshot.version || "ready"}`
        : wiki.raw.files > 0 ? `${wiki.raw.files} files` : "Missing"
    case "patrol_fetch":
      return `${Number(wiki.patrol?.xml || 0) + Number(wiki.patrol?.events || 0) + Number(wiki.patrol?.rights || 0) + Number(wiki.patrol?.groups || 0)}/4 ready`
    case "ingest":
      return wiki.snapshot?.ready ? `${wiki.ingest?.rows || 0} rows` : `${wiki.parquet.done}/${wiki.parquet.total || 0}`
    case "compute":
      return `${(wiki.metrics || []).filter((metric) => metric.name !== "patrol").length}/8`
    case "patrol_compute":
      return wiki.patrol?.metric_ready ? "Ready" : "Pending"
    case "merge":
      return wiki.dashboard.length > 0 ? `${wiki.dashboard.length} files` : "Pending"
    default:
      return ""
  }
}

function stageAction(stageKey) {
  return ({
    fetch: "fetch",
    patrol_fetch: "patrol-fetch",
    ingest: "ingest",
    compute: "compute",
    patrol_compute: "patrol-compute",
    merge: "merge"
  })[stageKey]
}

function stateExplanation(name, wiki, state, lifecycle, direct, fleetWork) {
  if (!lifecycle) return `${name} is supported by the Rust source resolver, but has no lifecycle policy. Processing is intentionally blocked until an operator registers it.`
  if (state === "queued") return `The request is durable and waiting for the ${direct?.resourceClass || lifecycle.fleet_resource_class || "assigned"} worker. It is safe to close this page.`
  if (state === "running") return `A worker owns this operation. The heartbeat and current stage below distinguish healthy progress from a stalled process.`
  if (state === "stalled") return `The worker lease exists but its heartbeat is overdue. Recover the fleet lease before submitting duplicate work.`
  if (state === "quarantined") return `Automatic retries were exhausted. Review the final log excerpt, correct the cause, then explicitly retry.`
  if (["failed", "interrupted"].includes(state)) return `The last operation did not finish. Completed source transactions remain reusable; retrying resumes from validated receipts rather than starting blindly from zero.`
  if (state === "needs_fetch") return `No validated history source or selected snapshot is available yet. Fetch is the first unblocked stage.`
  if (state === "needs_patrol_fetch") return `Core history is present, but the independent patrol source generation is incomplete.`
  if (state === "needs_ingest") return `History sources exist but have not all been converted into validated metric-input fragments.`
  if (state === "needs_compute") return `The warehouse generation is ready, but one or more core metric families are missing or invalid.`
  if (state === "needs_patrol_compute") return `Patrol sources are ready, but the derived patrol metric has not been validated.`
  if (state === "needs_merge") return `Per-wiki metrics are ready. They have not yet been incorporated into the public publication generation.`
  if (state === "complete") return `${name} has a complete published artifact set. Recalculation controls remain available for algorithm changes or validation work.`
  if (fleetWork) return `Fleet state ${fleetWork.state} was inferred from the durable work item and lease evidence.`
  return `The state was inferred from lifecycle policy, snapshot receipts, source markers, metric artifacts, and publication files.`
}

function evidenceItems(name, wiki, lifecycle, direct, fleetWork, plan) {
  return [
    ["Lifecycle", lifecycle ? `${lifecycle.publication} / ${lifecycle.refresh}` : "not registered"],
    ["Snapshot", plan?.snapshot || wiki.snapshot?.version || wiki.raw?.version || "not selected"],
    ["History", wiki.snapshot?.ready ? `${wiki.ingest?.rows || 0} validated rows` : `${wiki.raw?.files || 0} raw files · ${wiki.parquet?.done || 0}/${wiki.parquet?.total || 0} ingested`],
    ["Metrics", `${(wiki.metrics || []).length} core/patrol artifacts · ${wiki.dashboard?.length || 0} published files`],
    ["Last activity", direct ? `${operationLabel(direct.state || (direct.exitCode === 0 ? "succeeded" : "failed"))} · ${relativeTime(operationTimestamp(direct))}` : "no operator run recorded"],
    ["Worker", fleetWork ? `${fleetWork.workerId || fleetWork.resourceClass || "unclaimed"} · ${fleetWork.heartbeatAt ? `heartbeat ${relativeTime(fleetWork.heartbeatAt)}` : "no heartbeat"}` : "no fleet lease"]
  ]
}

function pipelineDossier(name, wiki, state, lifecycle, direct, fleetWork, plan, isRunning) {
  const canRun = Boolean(apiStatus && lifecycle)
  const isQualification = lifecycle?.publication === "hidden" && lifecycle?.refresh === "qualification"
  const operationActive = ["queued", "running", "cancelling"].includes(direct?.state) || direct?.running
  const log = (direct?.log || []).join("")
  return html`<section class="admin-pipeline-dossier" aria-label=${`${name} pipeline details`}>
    <div class="admin-dossier-summary">
      <div><span class="admin-command-kicker">What this means</span><p>${stateExplanation(name, wiki, state, lifecycle, direct, fleetWork)}</p></div>
      <dl>${evidenceItems(name, wiki, lifecycle, direct, fleetWork, plan).map(([label, value]) => html`<div><dt>${label}</dt><dd>${value}</dd></div>`)}</dl>
    </div>
    <div class="admin-dossier-stages">
      ${pipelineSteps.map((step) => {
        const stepState = stageStateForWiki(wiki, step.key, isRunning, direct?.progress)
        const action = stageAction(step.key)
        return html`<div class="admin-dossier-stage ${stepState}">
          <span>${step.label}</span><strong>${stageCaption(wiki, step.key)}</strong>
          <button class="admin-stage-button" ?disabled=${!canRun || stepState === "blocked" || operationActive}
            title=${actionTooltipWithApi(action, apiStatus)}
            onclick=${() => runCommand(action, action === "merge" ? null : {wiki: name, version: action === "fetch" ? preferredSnapshotVersion(wiki) : null})}>
            ${stepState === "done" ? "Run again" : "Run stage"}
          </button>
        </div>`
      })}
    </div>
    <div class="admin-dossier-actions">
      ${!lifecycle ? html`<button class="admin-btn primary" ?disabled=${!apiStatus} onclick=${() => {
        if (confirm(`Add ${name} as a publication-invisible qualification project?`)) registerWiki(name, "qualification", "medium_large")
      }}>Add as qualification</button>` : html`
        <button class="admin-btn primary" ?disabled=${!apiStatus || operationActive}
          onclick=${() => runCommand(isQualification ? "qualify" : "run", {wiki: name, version: preferredSnapshotVersion(wiki)})}>
          ${isQualification ? "Run full qualification" : "Prepare full update"}
        </button>
        <button class="admin-btn" ?disabled=${!apiStatus || operationActive} onclick=${() => runCommand("patrol-rebuild", name)}>Rebuild patrol</button>
        <button class="admin-btn" ?disabled=${!apiStatus} onclick=${() => runCommand("cleanup", name)}>Clean stale staging</button>`}
      ${["stalled", "quarantined"].includes(state) ? html`<button class="admin-btn" ?disabled=${!apiStatus} onclick=${() => runCommand(direct?.requestId ? "recover-admin" : "fleet-recover")}>${direct?.requestId ? "Recover operator queue" : "Recover fleet lease"}</button>` : ""}
      ${operationActive ? html`<button class="admin-btn danger" ?disabled=${!apiStatus} onclick=${() => runCommand("cancel", {requestId: direct?.requestId, wiki: name})}>Cancel operation</button>` : ""}
    </div>
    ${log ? html`<details class="admin-dossier-log"><summary>Latest output (${(direct.log || []).length} chunks)</summary><pre class="admin-job-log">${log}</pre></details>` : ""}
  </section>`
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
          const isRunning = inlineRunningJob && name === runningWiki
          const state = operationalState(name, wiki)
          const lifecycle = lifecycleStates[name] || null
          const direct = latestWikiJob(name)
          const fleetWork = fleetByWiki.get(name)
          const plan = latestPlanByWiki.get(name)
          const stageDetail = isRunning
            ? `${inlineRunningJob.progress?.stage || inlineRunningJob.stage || "starting"} · ${inlineRunningJob.progress?.pct || 0}%`
            : fleetWork
            ? `${fleetWork.workerId || fleetWork.resourceClass || "fleet"}${fleetWork.snapshot ? ` · ${fleetWork.snapshot}` : ""}`
            : direct
            ? `${direct.stage || direct.action || "run"} · ${relativeTime(operationTimestamp(direct))}`
            : plan && !wiki.tracked
            ? `${plan.snapshot} plan · no worker`
            : lifecycle?.freshness || lifecycle?.refresh || "inventory"
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
            <span class="admin-stage-rail" aria-label="Pipeline stages">
              ${pipelineSteps.map((step) => {
                const stageState = stageStateForWiki(wiki, step.key, isRunning, inlineRunningJob?.progress)
                return html`<i class=${stageState} title=${`${step.label}: ${stageCaption(wiki, step.key)}`}><span>${step.label}</span></i>`
              })}
            </span>
            <span class="admin-pipeline-lifecycle">${lifecycle?.refresh || (plan ? "unmanaged" : "unknown")}</span>
            <span class="admin-row-chevron" aria-hidden="true">${expanded ? "⌄" : "›"}</span>
          </button>
          ${expanded ? pipelineDossier(name, wiki, state, lifecycle, direct, fleetWork, plan, isRunning) : ""}
          </div>`
        })}
      </div>`}
</div>`)
```

</div>

<!-- ── Fetch a new wiki ───────────────────────────────────── -->

<div class="chart-section">

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
const snapshotVersionDefault = adminUiState.snapshotVersionDirty
  ? adminUiState.snapshotVersion
  : (adminUiState.snapshotVersion || suggestedVersion || "")
const snapshotVersionInput = Inputs.text({
  label: "Snapshot",
  value: snapshotVersionDefault,
  placeholder: suggestedVersion || "YYYY-MM",
  submit: false
})
snapshotVersionInput.addEventListener("input", () => {
  adminUiState.snapshotVersion = snapshotVersionInput.value
  adminUiState.snapshotVersionDirty = true
})
const snapshotVersion = view(snapshotVersionInput)
```

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
      if (confirm(`Add ${onboardingWiki} and start its ${onboardingMode} pipeline${version ? ` for ${version}` : ""}?`)) {
        registerWiki(onboardingWiki, onboardingMode, onboardingResourceClass, {start: true, version})
      }
    }}>Add & start ${onboardingMode === "qualification" ? "qualification" : "preparation"}</button>
  ` : html`<button class="admin-btn primary" ?disabled=${!apiStatus || !onboardingCanRun} onclick=${() => {
        const w = onboardingWiki
        const version = normalizeSnapshotVersion(snapshotVersion)
        if (!w) { alert("Pick a supported Wikipedia project."); return }
        const action = onboardingLifecycle?.refresh === "qualification" ? "qualify" : "run"
        if (confirm(`${action === "qualify" ? "Qualify" : "Prepare"} ${w}${version ? ` at snapshot ${version}` : ""}?`)) {
          runCommand(action, {wiki: w, version})
        }
      }}>${onboardingLifecycle?.refresh === "qualification" ? "Run full qualification" : "Prepare project data"}</button>`}
  <button class="admin-btn" ?disabled=${!apiStatus || !onboardingCanRun} title=${actionTooltipWithApi("fetch", apiStatus)} onclick=${() => {
        const w = onboardingWiki
        const version = normalizeSnapshotVersion(snapshotVersion)
        if (!w) { alert("Pick a supported Wikipedia project."); return }
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

<!-- ── Wiki details ───────────────────────────────────────── -->

<div class="chart-section">

## Evidence and manual controls

<div class="note">Select a project in Pipeline Status to inspect its evidence and use stage-level recovery controls.</div>

```js
const w = hasSelectedWiki ? wikiMap.get(selectedWiki) || emptyWikiStatus(selectedWiki) : emptyWikiStatus("—")
const selectedWikiRunning = Boolean(
  (job?.running && job?.progress?.wiki === selectedWiki)
  || ["queued", "running", "cancelling"].includes(operatorOperationByWiki.get(selectedWiki)?.state)
)
const selectedLifecycle = lifecycleStates[selectedWiki] || null
const selectedJob = latestWikiJob(selectedWiki)
const selectedFleetWork = fleetByWiki.get(selectedWiki) || null
const selectedPlan = latestPlanByWiki.get(selectedWiki) || null
```

```js
!hasSelectedWiki
  ? html`<span></span>`
  : html`<div class="admin-wiki-focus">
      <div><span>Project</span><strong>${wikipediaProjectLabel(selectedWiki)}</strong></div>
      <div><span>Operational state</span><strong>${statusLabels[operationalState(selectedWiki, w)] || operationLabel(operationalState(selectedWiki, w))}</strong></div>
      <div><span>Lifecycle</span><strong>${selectedLifecycle?.refresh || "Not registered"}</strong></div>
      <div><span>Snapshot</span><strong>${selectedPlan?.snapshot || w.snapshot?.version || w.raw?.version || "—"}</strong></div>
    </div>
    ${!selectedLifecycle ? html`<div class="warning"><strong>Processing is blocked.</strong> ${selectedWiki} has a source plan but is not registered in the lifecycle. Add it as a publication-invisible qualification project before continuing.</div>` : ""}
    ${selectedFleetWork ? html`<div class="admin-run-evidence">
      <strong>Fleet ${operationLabel(selectedFleetWork.state)}</strong>
      <span>${selectedFleetWork.workerId || selectedFleetWork.resourceClass || "waiting for worker"} · snapshot ${selectedFleetWork.snapshot || "unknown"}${selectedFleetWork.heartbeatAt ? ` · heartbeat ${relativeTime(selectedFleetWork.heartbeatAt)}` : ""}</span>
    </div>` : ""}
    ${selectedJob ? html`<div class="admin-run-evidence ${operationTone(selectedJob.state || (selectedJob.exitCode === 0 ? "succeeded" : "failed"))}">
      <div><strong>${operationLabel(selectedJob.state || (selectedJob.running ? "running" : selectedJob.exitCode === 0 ? "succeeded" : "failed"))}</strong><span>${selectedJob.stage || selectedJob.action || "pipeline"} · ${relativeTime(operationTimestamp(selectedJob))}</span></div>
      <div class="admin-log-section">
        <div class="admin-log-bar">
          <button class="admin-log-toggle admin-log-button" data-lines=${String((selectedJob.log || []).length)} onclick=${(event) => toggleLogSection(event, `details-log-${selectedWiki}`)}>${adminUiState[`details-log-${selectedWiki}`] ? "Hide output" : "Show output"} (${(selectedJob.log || []).length} chunks)</button>
          ${copyIconButton(() => (selectedJob.log || []).join(""), `Copy ${selectedWiki} run log`)}
        </div>
        <pre class="admin-job-log" ?hidden=${!adminUiState[`details-log-${selectedWiki}`]}>${(selectedJob.log || []).join("")}</pre>
      </div>
    </div>` : ""}`
```

### Maintenance

```js
!hasSelectedWiki
  ? html`<span></span>`
  : html`${!apiStatus ? adminConnectionWarning() : ""}
    ${!selectedLifecycle ? html`<p class="filter-desc">Stage actions are unavailable until this project has a lifecycle policy.</p>` : ""}
    <div class="admin-maintenance-actions">
      <button class="admin-btn primary" ?disabled=${!apiStatus || !selectedLifecycle || selectedWikiRunning} title=${actionTooltipWithApi(selectedLifecycle?.refresh === "qualification" ? "qualify" : "run", apiStatus)} onclick=${() => runCommand(selectedLifecycle?.refresh === "qualification" ? "qualify" : "run", {wiki: selectedWiki, version: preferredSnapshotVersion(w)})}>${selectedLifecycle?.refresh === "qualification" ? "run full qualification" : "prepare update"}</button>
      <button class="admin-btn" ?disabled=${!apiStatus || !selectedLifecycle} title=${actionTooltipWithApi("fetch", apiStatus)} onclick=${() => runCommand("fetch", {wiki: selectedWiki, version: preferredSnapshotVersion(w)})}>fetch missing</button>
      <button class="admin-btn" ?disabled=${!apiStatus || !selectedLifecycle} title=${actionTooltipWithApi("patrol-fetch", apiStatus)} onclick=${() => runCommand("patrol-fetch", selectedWiki)}>fetch patrol</button>
      <button class="admin-btn" ?disabled=${!apiStatus || !selectedLifecycle} title=${actionTooltipWithApi("ingest", apiStatus)} onclick=${() => runCommand("ingest", selectedWiki)}>ingest</button>
      <button class="admin-btn" ?disabled=${!apiStatus || !selectedLifecycle} title=${actionTooltipWithApi("compute", apiStatus)} onclick=${() => runCommand("compute", selectedWiki)}>compute core</button>
      <button class="admin-btn" ?disabled=${!apiStatus || !selectedLifecycle} title=${actionTooltipWithApi("patrol-compute", apiStatus)} onclick=${() => runCommand("patrol-compute", selectedWiki)}>compute patrol only</button>
      <button class="admin-btn" ?disabled=${!apiStatus || !selectedLifecycle} title=${actionTooltipWithApi("patrol-rebuild", apiStatus)} onclick=${() => runCommand("patrol-rebuild", selectedWiki)}>rebuild patrol</button>
      <button class="admin-btn" ?disabled=${!apiStatus} title=${actionTooltipWithApi("merge", apiStatus)} onclick=${() => runCommand("merge")}>merge site data</button>
      <button class="admin-btn" ?disabled=${!apiStatus} title=${actionTooltipWithApi("publish", apiStatus)} onclick=${() => runCommand("publish")}>publish ready candidates</button>
      <button class="admin-btn" ?disabled=${!apiStatus} title=${actionTooltipWithApi("site", apiStatus)} onclick=${() => runCommand("site")}>rebuild website only</button>
      <button class="admin-btn" ?disabled=${!apiStatus} title=${actionTooltipWithApi("fleet-recover", apiStatus)} onclick=${() => runCommand("fleet-recover")}>recover stale fleet work</button>
      ${adminOperations.executionMode === "queue" ? html`<button class="admin-btn" ?disabled=${!apiStatus} title=${actionTooltipWithApi("recover-admin", apiStatus)} onclick=${() => runCommand("recover-admin")}>recover stale operator work</button>` : ""}
      <button class="admin-btn" ?disabled=${!apiStatus} title=${actionTooltipWithApi("cleanup", apiStatus)} onclick=${() => runCommand("cleanup", selectedWiki)}>cleanup</button>
      ${selectedWikiRunning ? html`<button class="admin-btn danger" ?disabled=${!apiStatus} title=${actionTooltipWithApi("cancel", apiStatus)} onclick=${() => runCommand("cancel", {requestId: selectedJob?.requestId, wiki: selectedWiki})}>cancel queued/running job</button>` : ""}
    </div>`
```

### Raw Dumps

```js
!hasSelectedWiki
  ? html`<div class="warning">No wiki is available yet. Start a pipeline run to populate this section.</div>`
  : w.raw.files > 0
  ? html`<p><strong>${w.raw.files}</strong> dump files, <strong>${w.raw.size}</strong> total · dump version <code>${w.raw.version}</code>
    ${apiStatus && selectedLifecycle ? html` · <button class="admin-btn refetch small" title=${actionTooltipWithApi("fetch", apiStatus)} onclick=${() => { if(confirm("Fetch missing dump files for " + selectedWiki + "? Existing files will be skipped.")) runCommand("fetch", {wiki: selectedWiki, version: preferredSnapshotVersion(w)}) }}>fetch missing</button>` : ""}
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
      ? html`<button class="admin-btn primary" title=${actionTooltipWithApi("fetch", apiStatus)} onclick=${() => runCommand("fetch", {wiki: selectedWiki, version: preferredSnapshotVersion(w)})}>Fetch missing</button>`
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
  : w.metrics.length > 0
  ? html`${Inputs.table(w.metrics.map(m => ({metric: m.name, size: m.size_kb + " KB"})), {
      header: {metric: "Metric", size: "Size"}, sort: "metric"
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
    ${apiStatus ? html`<button class="admin-btn small" title=${actionTooltipWithApi("merge", apiStatus)} onclick=${() => runCommand("merge")}>re-merge all</button>` : ""}`
  : html`<div class="warning">No site data found for <strong>${selectedWiki}</strong>.</div>
    ${apiStatus
      ? html`<button class="admin-btn" title=${actionTooltipWithApi("merge", apiStatus)} onclick=${() => runCommand("merge")}>Publish site data</button>`
      : html`<pre class="admin-cmd">cd ${currentManifest.data_dir}/.. && ${runnerCommand()} ${cliFlags(currentManifest)} merge</pre>`
    }`
```

</div>

<!-- ── Merged site data files ─────────────────────────────── -->

<div class="chart-section">

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
  grid-template-columns: minmax(10rem, 1.15fr) minmax(10rem, 1fr) minmax(18rem, 2fr) 6rem 1rem;
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
.admin-pipeline-state strong { font-size: 0.72rem; text-transform: uppercase; letter-spacing: 0.04em; }
.admin-stage-rail { display: grid; grid-template-columns: repeat(6, minmax(1.3rem, 1fr)); gap: 0.3rem; }
.admin-stage-rail > i { position: relative; height: 0.35rem; border-radius: 99px; background: color-mix(in srgb, var(--theme-foreground-faintest) 90%, transparent); }
.admin-stage-rail > i.done { background: #3d8a53; }
.admin-stage-rail > i.active { background: #315b8a; animation: admin-pulse 1.7s ease-in-out infinite; }
.admin-stage-rail > i.todo { background: #d98c2f; }
.admin-stage-rail > i.blocked { background: color-mix(in srgb, var(--theme-foreground-muted) 25%, transparent); }
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
.admin-dossier-summary { display: grid; grid-template-columns: minmax(16rem, 0.8fr) minmax(28rem, 1.4fr); }
.admin-dossier-summary > div { padding: 1rem; }
.admin-dossier-summary p { max-width: 62ch; margin: 0.35rem 0 0; font-size: 0.82rem; line-height: 1.55; }
.admin-dossier-summary dl { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); margin: 0; border-left: 1px solid var(--theme-foreground-faintest); }
.admin-dossier-summary dl div { min-width: 0; padding: 0.75rem; border-left: 1px solid var(--theme-foreground-faintest); border-bottom: 1px solid var(--theme-foreground-faintest); }
.admin-dossier-summary dl div:nth-child(3n + 1) { border-left: 0; }
.admin-dossier-summary dt { color: var(--theme-foreground-muted); font-size: 0.62rem; font-weight: 750; letter-spacing: 0.08em; text-transform: uppercase; }
.admin-dossier-summary dd { margin: 0.22rem 0 0; overflow-wrap: anywhere; font-size: 0.72rem; font-weight: 650; }
.admin-dossier-stages { display: grid; grid-template-columns: repeat(6, minmax(0, 1fr)); border-top: 1px solid var(--theme-foreground-faintest); }
.admin-dossier-stage { display: grid; align-content: start; gap: 0.18rem; min-height: 6.3rem; padding: 0.72rem; border-left: 1px solid var(--theme-foreground-faintest); box-shadow: inset 0 3px var(--stage-color, #8793a1); }
.admin-dossier-stage:first-child { border-left: 0; }
.admin-dossier-stage.done { --stage-color: #3d8a53; }
.admin-dossier-stage.active { --stage-color: #315b8a; }
.admin-dossier-stage.todo { --stage-color: #d98c2f; }
.admin-dossier-stage.blocked { --stage-color: #9aa0a7; opacity: 0.72; }
.admin-dossier-stage > span { color: var(--theme-foreground-muted); font-size: 0.62rem; font-weight: 750; letter-spacing: 0.06em; text-transform: uppercase; }
.admin-dossier-stage > strong { min-height: 2em; font-size: 0.74rem; }
.admin-stage-button { justify-self: start; margin-top: auto; padding: 0; border: 0; background: transparent; color: #315b8a; font-size: 0.68rem; font-weight: 750; cursor: pointer; }
.admin-stage-button:hover { text-decoration: underline; }
.admin-stage-button:disabled { color: var(--theme-foreground-muted); cursor: not-allowed; text-decoration: none; }
.admin-dossier-actions { display: flex; flex-wrap: wrap; gap: 0.4rem; padding: 0.75rem 1rem; border-top: 1px solid var(--theme-foreground-faintest); }
.admin-dossier-log { border-top: 1px solid var(--theme-foreground-faintest); }
.admin-dossier-log summary { padding: 0.6rem 1rem; color: var(--theme-foreground-muted); font-size: 0.72rem; font-weight: 700; cursor: pointer; }
.admin-onboarding-console { display: grid; gap: 0.75rem; }
.admin-registration-callout { display: grid; gap: 0.2rem; padding: 0.75rem 0.9rem; border-left: 3px solid #d98c2f; background: color-mix(in srgb, #d98c2f 7%, transparent); }
.admin-registration-callout.registered { border-left-color: #3d8a53; background: color-mix(in srgb, #3d8a53 7%, transparent); }
.admin-registration-callout span { color: var(--theme-foreground-muted); font-size: 0.76rem; }
.admin-registration-policy { display: grid; grid-template-columns: minmax(16rem, 1.5fr) minmax(13rem, 1fr); gap: 0.8rem; max-width: 48rem; }
.admin-registration-policy form { margin: 0; }
.admin-empty-state { display: grid; gap: 0.2rem; border-block: 1px solid var(--theme-foreground-faintest); }
.admin-wiki-focus { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); margin-bottom: 0.75rem; border-block: 1px solid var(--theme-foreground-faintest); }
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
[data-theme="dark"] .admin-command-header { --admin-ink: #d7e2ee; background: color-mix(in srgb, var(--theme-background) 96%, #26384c 4%); }
@media (prefers-reduced-motion: reduce) {
  .admin-stage-rail > i.active,
  .admin-job-panel.running .admin-progress-fill { animation: none; }
}
@media (max-width: 1100px) {
  .admin-command-header { grid-template-columns: 1fr; }
  .admin-command-facts { border: 0; border-block: 1px solid var(--admin-line); }
  .admin-command-session { grid-auto-flow: column; justify-content: start; }
  .admin-pipeline-row { grid-template-columns: minmax(9rem, 1fr) minmax(9rem, 1fr) minmax(14rem, 1.5fr) 5rem 1rem; }
  .admin-dossier-summary { grid-template-columns: 1fr; }
  .admin-dossier-summary dl { border-left: 0; border-top: 1px solid var(--theme-foreground-faintest); }
  .admin-dossier-stages { grid-template-columns: repeat(3, minmax(0, 1fr)); }
  .admin-dossier-stage:nth-child(4) { border-left: 0; }
  .pipeline-stage-grid {
    grid-template-columns: repeat(3, minmax(0, 1fr));
  }
}
@media (max-width: 760px) {
  .admin-command-facts { grid-template-columns: repeat(2, minmax(0, 1fr)); }
  .admin-command-facts > div:nth-child(3) { border-left: 0; }
  .admin-command-facts > div:nth-child(n+3) { border-top: 1px solid var(--admin-line); }
  .admin-activity-row { grid-template-columns: 6.5rem minmax(0, 1fr) 4.5rem; gap: 0.6rem; }
  .admin-activity-source { display: none; }
  .admin-pipeline-summary.concise { grid-template-columns: repeat(2, minmax(0, 1fr)); }
  .admin-pipeline-summary.concise > div:nth-child(3) { border-left: 0; }
  .admin-pipeline-summary.concise > div:nth-child(n+3) { border-top: 1px solid var(--theme-foreground-faintest); }
  .admin-pipeline-row { grid-template-columns: minmax(7.8rem, 0.8fr) minmax(8.5rem, 1fr) 1rem; gap: 0.65rem; }
  .admin-stage-rail { grid-column: 1 / -1; grid-row: 2; }
  .admin-pipeline-lifecycle { display: none; }
  .admin-row-chevron { grid-column: 3; grid-row: 1; }
  .admin-pipeline-dossier { margin-inline: 0; }
  .admin-dossier-summary dl { grid-template-columns: repeat(2, minmax(0, 1fr)); }
  .admin-dossier-summary dl div:nth-child(3n + 1) { border-left: 1px solid var(--theme-foreground-faintest); }
  .admin-dossier-summary dl div:nth-child(odd) { border-left: 0; }
  .admin-dossier-stages { grid-template-columns: repeat(2, minmax(0, 1fr)); }
  .admin-dossier-stage:nth-child(4) { border-left: 1px solid var(--theme-foreground-faintest); }
  .admin-dossier-stage:nth-child(odd) { border-left: 0; }
  .admin-registration-policy { grid-template-columns: 1fr; }
  .admin-wiki-focus { grid-template-columns: repeat(2, minmax(0, 1fr)); }
  .admin-wiki-focus > div:nth-child(3) { border-left: 0; }
  .admin-wiki-focus > div:nth-child(n+3) { border-top: 1px solid var(--theme-foreground-faintest); }
  .admin-run-evidence > div:first-child { display: grid; }
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
