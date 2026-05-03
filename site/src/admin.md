---
title: Admin
---

# Pipeline Admin

<div class="page-intro">

Monitor and manage the data pipeline. Each wiki now flows through six stages: **fetch** (history dumps) → **patrol fetch** (logging XML) → **ingest** (convert to parquet) → **compute** (core metrics) → **patrol compute** (patrol metrics) → **publish** (refresh site data). In local development, start the dev/operator admin server with `scripts/dev.sh` or `WIKI_ECON_ADMIN_ENABLED=1 node site/admin-server.cjs`. In VPS deployments, this page is intended to be served through the authenticated admin server.

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
const adminUiState = globalThis.__wikiEconAdminState ??= {
  showRunningLog: false,
  showJobLog: false,
  onboardingWiki: null,
  snapshotVersion: "",
  snapshotVersionDirty: false
}
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
  return validSnapshotVersion(wikiStatus?.raw?.version)
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
      ...(requestedVersion ? {version: requestedVersion} : {})
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
      return
    }
    if (data.error) { alert(data.error); return }
    adminUiState.showRunningLog = false
    adminUiState.showJobLog = false
    await checkApi()
    // Start polling
    if (pollTimer) clearInterval(pollTimer)
    pollTimer = setInterval(async () => {
      await checkApi()
      if (jobStatus.value && !jobStatus.value.running) {
        clearInterval(pollTimer)
        pollTimer = null
      }
    }, 500)
  } catch (e) {
    alert("Admin server not reachable. Run scripts/dev.sh or WIKI_ECON_ADMIN_ENABLED=1 node site/admin-server.cjs")
  }
}

function primaryActionForStatus(status) {
  switch (status) {
    case "needs_fetch": return "fetch"
    case "needs_patrol_fetch": return "patrol-fetch"
    case "needs_ingest": return "ingest"
    case "needs_compute": return "compute"
    case "needs_patrol_compute": return "patrol-compute"
    case "needs_merge": return "merge"
    case "complete": return "run"
    default: return null
  }
}

function actionLabel(action) {
  switch (action) {
    case "fetch": return "fetch missing"
    case "patrol-fetch": return "fetch patrol"
    case "ingest": return "ingest"
    case "compute": return "compute"
    case "patrol-compute": return "patrol compute"
    case "merge": return "publish"
    case "cleanup": return "cleanup"
    case "cancel": return "cancel"
    case "run": return "rerun"
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
    case "merge":
      return "Refresh the merged site data files served by the frontend."
    case "cleanup":
      return "Remove temporary files and invalid ingest markers for this wiki."
    case "cancel":
      return "Stop the currently running pipeline job."
    case "run":
      return "Run the full pipeline in sequence for this wiki."
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
const wikiJobMap = job?.wikiJobs || {}
const globalJob = job?.globalJob || null
const supportedWikis = Array.from(new Set(job?.supportedWikis || [])).sort((a, b) => a.localeCompare(b))
const suggestedVersion = normalizeSnapshotVersion(job?.suggestedVersion) || ""
```

<p class="filter-desc">Last scanned: ${currentManifest.generated_at}${apiStatus ? html` · <span style="color:#2e7d32">API connected</span>` : html` · ${adminConnectionHelp()}`}</p>

<!-- ── Job output panel ───────────────────────────────────── -->

```js
const trackedWikiEntries = Object.entries(currentWikis).sort((a, b) => a[0].localeCompare(b[0]))
const trackedWikiNames = trackedWikiEntries.map(([name]) => name)
```

```js
const effectiveJob = job?.job || null
const runningWiki = effectiveJob?.running ? effectiveJob.wiki ?? null : null
const selectedWiki = (runningWiki || trackedWikiNames[0] || "—").trim().toLowerCase()
const hasSelectedWiki = selectedWiki !== "—"
const inlineRunningJob = effectiveJob?.running && runningWiki ? effectiveJob : null
const topLevelJob = effectiveJob && !effectiveJob.wiki ? effectiveJob : globalJob
const wikiMap = new Map(trackedWikiEntries.map(([name, value]) => [name, {...value, tracked: true}]))
if (runningWiki && !wikiMap.has(runningWiki)) {
  wikiMap.set(runningWiki, emptyWikiStatus(runningWiki))
}
const wikiEntries = Array.from(wikiMap.entries()).sort(([leftName], [rightName]) => {
  const leftPriority =
    leftName === runningWiki ? 0 :
    1
  const rightPriority =
    rightName === runningWiki ? 0 :
    1
  if (leftPriority !== rightPriority) return leftPriority - rightPriority
  return leftName.localeCompare(rightName)
})
const wikiNames = wikiEntries.map(([name]) => name)
```

```js
display(html`<div class="admin-control-strip">
  <div class="admin-control-chip ${apiStatus ? "online" : "offline"}">
    <span class="admin-control-dot"></span>
    <strong>${apiStatus ? "Admin API online" : "Admin API offline"}</strong>
  </div>
  <div class="admin-control-chip ${runningWiki ? "running" : ""}">
    <span class="admin-control-label">Live run</span>
    <strong>${runningWiki || "Idle"}</strong>
  </div>
  <div class="admin-control-chip">
    <span class="admin-control-label">Details wiki</span>
    <strong>${hasSelectedWiki ? selectedWiki : "None"}</strong>
  </div>
  <div class="admin-control-chip">
    <span class="admin-control-label">Last scan</span>
    <strong>${currentManifest.generated_at}</strong>
  </div>
  <div class="admin-control-chip">
    <span class="admin-control-label">Signed in</span>
    <strong>${auth?.user?.email || (auth?.enabled ? "Required" : "Local mode")}</strong>
    ${auth?.logoutUrl ? html`<a href=${auth.logoutUrl}>sign out</a>` : ""}
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

<!-- ── Pipeline status matrix ─────────────────────────────── -->

<div class="chart-section">

## Pipeline Status

<div class="note">Each wiki must pass through the full history + patrol pipeline before its site data is ready. Green = done, orange = in progress or partial, red = missing.</div>

```js
const statusColors = {
  complete: "#2e7d32",
  needs_fetch: "#c62828",
  needs_patrol_fetch: "#6a1b9a",
  needs_ingest: "#e65100",
  needs_compute: "#f57f17",
  needs_patrol_compute: "#8e24aa",
  needs_merge: "#1565c0",
  running: "#1565c0"
}
const statusLabels = {
  complete: "Complete",
  needs_fetch: "Needs fetch",
  needs_patrol_fetch: "Needs patrol fetch",
  needs_ingest: "Needs ingest",
  needs_compute: "Needs compute",
  needs_patrol_compute: "Needs patrol compute",
  needs_merge: "Needs merge",
  running: "Running"
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

  switch (stageKey) {
    case "fetch":
      return wiki.raw.files > 0 ? "done" : "todo"
    case "patrol_fetch":
      return wiki.patrol?.source_ready ? "done" : (wiki.raw.files > 0 ? "todo" : "blocked")
    case "ingest":
      return wiki.parquet.done > 0 && wiki.parquet.done >= wiki.parquet.total && wiki.parquet.in_progress === 0
        ? "done"
        : wiki.raw.files > 0
        ? "todo"
        : "blocked"
    case "compute": {
      const coreMetricCount = (wiki.metrics || []).filter((metric) => metric.name !== "patrol").length
      return coreMetricCount >= 8
        ? "done"
        : wiki.parquet.done > 0
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
      return wiki.raw.files > 0 ? `${wiki.raw.files} files` : "Missing"
    case "patrol_fetch":
      return `${Number(wiki.patrol?.xml || 0) + Number(wiki.patrol?.events || 0) + Number(wiki.patrol?.rights || 0) + Number(wiki.patrol?.groups || 0)}/4 ready`
    case "ingest":
      return `${wiki.parquet.done}/${wiki.parquet.total || 0}`
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

function stageAction(stageKey, state) {
  if (state === "blocked" || state === "active") return null
  switch (stageKey) {
    case "fetch":
      return {action: "fetch", label: "fetch missing", needsWiki: true}
    case "patrol_fetch":
      return {action: "patrol-fetch", label: "fetch patrol", needsWiki: true}
    case "ingest":
      return {action: "ingest", label: "ingest", needsWiki: true}
    case "compute":
      return {action: "compute", label: "compute", needsWiki: true}
    case "patrol_compute":
      return {action: "patrol-compute", label: "compute patrol", needsWiki: true}
    case "merge":
      return {action: "merge", label: "publish", needsWiki: false}
    default:
      return null
  }
}

```

```js
const statusSummary = summarizeStatuses(wikiEntries)
display(html`<div class="admin-pipeline-board">
  <div class="admin-pipeline-summary">
    <div class="admin-summary-card admin-summary-primary compact">
      <span class="admin-summary-label">Wikis</span>
      <strong>${wikiEntries.length}</strong>
      <span class="admin-summary-meta">Updated ${currentManifest.generated_at}</span>
    </div>
    ${Object.entries(statusLabels)
      .filter(([key]) => key !== "running")
      .map(([key, label]) => html`<div class="admin-summary-card compact">
        <span class="admin-summary-dot" style=${`background:${statusColors[key]}`}></span>
        <span class="admin-summary-label">${label}</span>
        <strong>${statusSummary[key] || 0}</strong>
      </div>`)}
  </div>

  ${wikiEntries.length === 0
    ? html`<div class="admin-empty-state">No wikis are currently present in the manifest. Run a pipeline or refresh merged outputs to populate this view.</div>`
    : html`<div class="admin-pipeline-cards">
        ${wikiEntries.map(([name, w]) => {
          const isRunning = inlineRunningJob && name === runningWiki
          const statusKey = isRunning ? "running" : w.status
          const runningProgress = isRunning ? inlineRunningJob.progress : null
          const rowJob = isRunning ? inlineRunningJob : wikiJobMap[name] || null
          return html`<article class="pipeline-card status-${statusKey} ${isRunning ? "running" : ""}">
            <div class="pipeline-card-top">
              <div class="pipeline-card-title">
                <div class="pipeline-card-heading">
                  <strong>${name}</strong>
                  ${!w.tracked ? html`<span class="pipeline-ghost-badge">Not tracked yet</span>` : ""}
                  <span class="admin-badge" style="background:${statusColors[statusKey]}">${statusLabels[statusKey]}</span>
                  ${w.raw.version ? html`<span class="pipeline-inline-meta">dump <code>${w.raw.version}</code></span>` : ""}
                  ${w.raw.size && w.raw.size !== "0" ? html`<span class="pipeline-inline-meta">raw ${w.raw.size}</span>` : ""}
                  ${w.parquet.size && w.parquet.size !== "0 B" ? html`<span class="pipeline-inline-meta">parquet ${w.parquet.size}</span>` : ""}
                </div>
              </div>
              ${isRunning
                ? html`<div class="pipeline-card-actions">
                    <button class="admin-btn danger" title=${actionTooltip("cancel")} onclick=${() => runCommand("cancel")}>cancel</button>
                  </div>`
                : ""}
            </div>

            <div class="pipeline-stage-grid">
              ${pipelineSteps.map((step) => {
                const state = stageStateForWiki(w, step.key, isRunning, runningProgress)
                const stageCmd = stageAction(step.key, state)
                return html`<div class="pipeline-stage ${state}">
                  <span class="pipeline-stage-label">${step.label}</span>
                  <strong>${stageCaption(w, step.key)}</strong>
                  ${stageCmd
                    ? html`<button
                        class="pipeline-stage-action"
                        title=${actionTooltipWithApi(stageCmd.action, apiStatus)}
                        ?disabled=${!apiStatus}
                        onclick=${() => runCommand(stageCmd.action, stageCmd.needsWiki ? {wiki: name, version: preferredSnapshotVersion(w)} : null)}
                      >${stageCmd.label}</button>`
                    : ""}
                </div>`
              })}
            </div>

            ${isRunning
              ? html`<div class="pipeline-live-panel">
                  <div class="admin-progress">
                    <div class="admin-progress-info">
                      <span class="admin-progress-stage">${runningProgress.stage || "starting"}</span>
                      <span class="admin-progress-detail">${runningProgress.detail}</span>
                      <span class="admin-progress-pct">${runningProgress.pct}%</span>
                    </div>
                    <div class="admin-progress-track">
                      <div class="admin-progress-fill" style=${"width:" + runningProgress.pct + "%"}></div>
                    </div>
                  </div>
                  <div class="admin-log-section">
                    <div class="admin-log-bar">
                      <button
                        class="admin-log-toggle admin-log-button"
                        data-expand-label="Show live output"
                        data-collapse-label="Hide live output"
                        data-lines=${String((rowJob?.log || []).length)}
                        onclick=${(event) => toggleLogSection(event, `row-log-${name}`)}
                      >
                        ${adminUiState[`row-log-${name}`] ? "Hide live output" : "Show live output"} (${(rowJob?.log || []).length} lines)
                      </button>
                      ${copyIconButton(() => (rowJob?.log || []).join(""), `Copy ${name} live log`)}
                    </div>
                    <pre class="admin-job-log" ?hidden=${!adminUiState[`row-log-${name}`]}>${(rowJob?.log || []).join("")}</pre>
                  </div>
                </div>`
              : rowJob
              ? html`<div class="pipeline-live-panel ${rowJob.cancelled ? "failed" : rowJob.exitCode === 0 ? "success" : "failed"}">
                  <div class="admin-job-header compact">
                    <strong>${rowJob.cancelled ? "Cancelled" : rowJob.exitCode === 0 ? "Completed" : "Failed"}</strong>
                    <code>${rowJob.command || ""}</code>
                  </div>
                  <div class="admin-log-section">
                    <div class="admin-log-bar">
                      <button
                        class="admin-log-toggle admin-log-button"
                        data-expand-label="Show output"
                        data-collapse-label="Hide output"
                        data-lines=${String((rowJob.log || []).length)}
                        onclick=${(event) => toggleLogSection(event, `row-log-${name}`)}
                      >
                        ${adminUiState[`row-log-${name}`] ? "Hide output" : "Show output"} (${(rowJob.log || []).length} lines)
                      </button>
                      ${copyIconButton(() => (rowJob.log || []).join(""), `Copy ${name} log`)}
                    </div>
                    <pre class="admin-job-log" ?hidden=${!adminUiState[`row-log-${name}`]}>${(rowJob.log || []).join("")}</pre>
                  </div>
                </div>`
              : ""}
          </article>`
        })}
      </div>`}
</div>`)
```

</div>

<!-- ── Fetch a new wiki ───────────────────────────────────── -->

<div class="chart-section">

## Fetch a New Wiki

<div class="note">Pick a Wikipedia language edition to start the full pipeline (fetch → patrol fetch → ingest → compute → patrol compute → publish). The picker covers every Wikipedia language edition published in the <code>mediawiki_history</code> dumps; the CLI will surface a clear error if a particular wiki uses a partitioning shape (monthly for <code>enwiki</code>, etc.) that the local fetch planner does not yet support.</div>

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
html`<div class="admin-fetch-actions">
  ${!apiStatus ? adminConnectionWarning() : ""}
  ${onboardingWikiOptions.length === 0 ? html`<div class="warning">No supported onboarding projects were reported by the admin API yet.</div>` : ""}
  ${onboardingWikiUnknown ? html`<div class="warning">No project matches <code>${onboardingWikiTrimmed}</code>. Click the field to reopen the full project list, or keep typing to narrow it down.</div>` : ""}
  <button class="admin-btn primary" ?disabled=${!apiStatus} onclick=${() => {
        const w = onboardingWiki
        const version = normalizeSnapshotVersion(snapshotVersion)
        if (!w) { alert("Pick a supported Wikipedia project."); return }
        if (confirm("Run full pipeline for " + w + (version ? " at snapshot " + version : "") + "? This will download dumps, ingest, compute, and merge.")) {
          runCommand("run", {wiki: w, version})
        }
      }} title=${actionTooltipWithApi("run", apiStatus)}>Run full pipeline</button>
  <button class="admin-btn" ?disabled=${!apiStatus} title=${actionTooltipWithApi("fetch", apiStatus)} onclick=${() => {
        const w = onboardingWiki
        const version = normalizeSnapshotVersion(snapshotVersion)
        if (!w) { alert("Pick a supported Wikipedia project."); return }
        runCommand("fetch", {wiki: w, version})
      }}>Fetch missing</button>
  ${!apiStatus ? html`
      <pre class="admin-cmd">cd ${currentManifest.data_dir}/.. && WIKI_ECON_ADMIN_ENABLED=1 node site/admin-server.cjs</pre>
      <pre class="admin-cmd">cd ${currentManifest.data_dir}/.. && cargo run --release -- ${cliFlags(currentManifest)} run ${onboardingWiki || "frwiki"}${normalizeSnapshotVersion(snapshotVersion) ? ` --version ${normalizeSnapshotVersion(snapshotVersion)}` : ""}</pre>`
    : ""}
</div>`
```

</div>

<!-- ── Wiki details ───────────────────────────────────────── -->

<div class="chart-section">

## Wiki Details

<div class="note">Wiki Details follows the running wiki when a job is active; otherwise it shows the first tracked wiki.</div>

```js
const w = hasSelectedWiki ? wikiMap.get(selectedWiki) || emptyWikiStatus(selectedWiki) : emptyWikiStatus("—")
const selectedWikiRunning = Boolean(job?.running && job?.progress?.wiki === selectedWiki)
```

### Maintenance

```js
!hasSelectedWiki
  ? html`<span></span>`
  : html`${!apiStatus ? adminConnectionWarning() : ""}
    <div class="admin-maintenance-actions">
      <button class="admin-btn primary" ?disabled=${!apiStatus} title=${actionTooltipWithApi("run", apiStatus)} onclick=${() => runCommand("run", {wiki: selectedWiki, version: preferredSnapshotVersion(w)})}>run full pipeline</button>
      <button class="admin-btn" ?disabled=${!apiStatus} title=${actionTooltipWithApi("fetch", apiStatus)} onclick=${() => runCommand("fetch", {wiki: selectedWiki, version: preferredSnapshotVersion(w)})}>fetch missing</button>
      <button class="admin-btn" ?disabled=${!apiStatus} title=${actionTooltipWithApi("patrol-fetch", apiStatus)} onclick=${() => runCommand("patrol-fetch", selectedWiki)}>fetch patrol</button>
      <button class="admin-btn" ?disabled=${!apiStatus} title=${actionTooltipWithApi("ingest", apiStatus)} onclick=${() => runCommand("ingest", selectedWiki)}>ingest</button>
      <button class="admin-btn" ?disabled=${!apiStatus} title=${actionTooltipWithApi("compute", apiStatus)} onclick=${() => runCommand("compute", selectedWiki)}>compute core</button>
      <button class="admin-btn" ?disabled=${!apiStatus} title=${actionTooltipWithApi("patrol-compute", apiStatus)} onclick=${() => runCommand("patrol-compute", selectedWiki)}>compute patrol only</button>
      <button class="admin-btn" ?disabled=${!apiStatus} title=${actionTooltipWithApi("merge", apiStatus)} onclick=${() => runCommand("merge")}>publish site data</button>
      <button class="admin-btn" ?disabled=${!apiStatus} title=${actionTooltipWithApi("cleanup", apiStatus)} onclick=${() => runCommand("cleanup", selectedWiki)}>cleanup</button>
      ${selectedWikiRunning ? html`<button class="admin-btn danger" ?disabled=${!apiStatus} title=${actionTooltipWithApi("cancel", apiStatus)} onclick=${() => runCommand("cancel")}>cancel running job</button>` : ""}
    </div>`
```

### Raw Dumps

```js
!hasSelectedWiki
  ? html`<div class="warning">No wiki is available yet. Start a pipeline run to populate this section.</div>`
  : w.raw.files > 0
  ? html`<p><strong>${w.raw.files}</strong> dump files, <strong>${w.raw.size}</strong> total · dump version <code>${w.raw.version}</code>
    ${apiStatus ? html` · <button class="admin-btn refetch small" title=${actionTooltipWithApi("fetch", apiStatus)} onclick=${() => { if(confirm("Fetch missing dump files for " + selectedWiki + "? Existing files will be skipped.")) runCommand("fetch", {wiki: selectedWiki, version: preferredSnapshotVersion(w)}) }}>fetch missing</button>` : ""}
    </p>
    ${Inputs.table(w.raw.details.map(d => ({file: d.name, size: d.size, downloaded: d.date})), {
      header: {file: "File", size: "Size", downloaded: "Downloaded"},
      sort: "file", rows: 15
    })}`
  : html`<div class="warning">No raw dumps found for <strong>${selectedWiki}</strong>.</div>
    ${apiStatus
      ? html`<button class="admin-btn primary" title=${actionTooltipWithApi("fetch", apiStatus)} onclick=${() => runCommand("fetch", {wiki: selectedWiki, version: preferredSnapshotVersion(w)})}>Fetch missing</button>`
      : html`<pre class="admin-cmd">cd ${currentManifest.data_dir}/.. && cargo run --release -- ${cliFlags(currentManifest)} fetch ${selectedWiki}</pre>`
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
    ${apiStatus
      ? html`<button class="admin-btn" title=${actionTooltipWithApi("patrol-fetch", apiStatus)} onclick=${() => runCommand("patrol-fetch", selectedWiki)}>Fetch patrol data</button>`
      : html`<pre class="admin-cmd">cd ${currentManifest.data_dir}/.. && cargo run --release -- ${cliFlags(currentManifest)} patrol-fetch ${selectedWiki}</pre>`
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
    }${apiStatus && w.parquet.missing.length > 0 ? html` · <button class="admin-btn small" title=${actionTooltipWithApi("ingest", apiStatus)} onclick=${() => runCommand("ingest", selectedWiki)}>ingest missing</button>` : ""}
    </p>
    ${w.parquet.missing.length > 0
      ? html`<details><summary>Missing files</summary><ul>${w.parquet.missing.map(f => html`<li><code>${f}</code></li>`)}</ul></details>`
      : ""
    }`
  : html`<div class="warning">No ingested data for <strong>${selectedWiki}</strong>.</div>
    ${apiStatus
      ? html`<button class="admin-btn" title=${actionTooltipWithApi("ingest", apiStatus)} onclick=${() => runCommand("ingest", selectedWiki)}>Ingest ${selectedWiki}</button>`
      : html`<pre class="admin-cmd">cd ${currentManifest.data_dir}/.. && cargo run --release -- ${cliFlags(currentManifest)} ingest ${selectedWiki}</pre>`
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
    ${apiStatus ? html`<button class="admin-btn small" title=${actionTooltipWithApi("compute", apiStatus)} onclick=${() => runCommand("compute", selectedWiki)}>recompute</button>` : ""}`
  : html`<div class="warning">No metrics computed for <strong>${selectedWiki}</strong>.</div>
    ${apiStatus
      ? html`<button class="admin-btn" title=${actionTooltipWithApi("compute", apiStatus)} onclick=${() => runCommand("compute", selectedWiki)}>Compute ${selectedWiki}</button>`
      : html`<pre class="admin-cmd">cd ${currentManifest.data_dir}/.. && cargo run --release -- ${cliFlags(currentManifest)} compute ${selectedWiki}</pre>`
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
      : html`<pre class="admin-cmd">cd ${currentManifest.data_dir}/.. && cargo run --release -- ${cliFlags(currentManifest)} merge</pre>`
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
@media (max-width: 1100px) {
  .pipeline-stage-grid {
    grid-template-columns: repeat(3, minmax(0, 1fr));
  }
}
@media (max-width: 760px) {
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
