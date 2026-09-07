#!/usr/bin/env node
"use strict";

const fs = require("node:fs");
const path = require("node:path");

const {
  Cdp,
  evaluate,
  launchChrome,
  startServer,
  terminateChild,
  waitFor,
} = require("./browser-performance.cjs");

function assertContract(condition, message, evidence) {
  if (!condition) {
    throw new Error(`${message}${evidence == null ? "" : `: ${JSON.stringify(evidence)}`}`);
  }
}

async function runAdminBrowserWorkflow({distDir}) {
  const manifestName = fs.readdirSync(path.join(distDir, "_file", "data"))
    .find((name) => /^manifest\.[a-f0-9]+\.json$/.test(name));
  if (!manifestName) throw new Error("built admin fixture has no manifest attachment");
  const manifest = JSON.parse(fs.readFileSync(path.join(distDir, "_file", "data", manifestName), "utf8"));
  let operation = null;
  let statusReadsAfterQueue = 0;
  const apiRequests = [];
  const {server, origin} = await startServer(distDir, {
    handleRequest(request, response) {
      const url = new URL(request.url, origin || "http://localhost");
      if (url.pathname.startsWith("/admin-api/")) apiRequests.push(`${request.method} ${url.pathname}`);
      if (url.pathname === "/admin-api/status" && request.method === "GET") {
        if (operation) statusReadsAfterQueue += 1;
        if (operation && statusReadsAfterQueue >= 2) {
          operation = {...operation, state: "succeeded", stage: "preflight complete",
            updatedAt: "2026-09-06T12:01:00Z", finishedAt: "2026-09-06T12:01:00Z"};
        }
        const queued = operation?.state === "queued" ? [operation] : [];
        const recent = operation?.state === "succeeded" ? [operation] : [];
        response.setHeader("content-type", "application/json");
        response.end(JSON.stringify({
          manifest,
          runner: {label: "fixture runner"},
          wikiStates: {
            ...(manifest.lifecycle?.wikis || {}),
            dewiki: {publication: "hidden", refresh: "qualification", fleet_resource_class: "medium_large"},
          },
          refreshWikis: [],
          publishedWikis: Object.keys(manifest.wikis || {}),
          supportedWikis: [...Object.keys(manifest.wikis || {}), "dewiki"],
          wikiJobs: {}, wikiJobHistory: {}, snapshotPlans: [], qualifications: {
            dewiki: [{
              wiki: "dewiki", snapshot: "2026-08", runId: "qualified-dewiki", structurallyValid: true,
              qualifiedAtUnix: 1788526634, cutoffDate: "2026-08", artifactCount: 10,
              artifactBytes: 704892841, artifactRows: 118027706,
            }],
          },
          adminOperations: {executionMode: "queue", counts: {queued: queued.length, running: 0}, queued, running: [], recent},
          adminRuns: {active: null, recent: []},
          fleet: {counts: {quarantined: 5}, work: ["frwiki", "itwiki", "nlwiki", "ptwiki", "svwiki"].map((wiki) => ({
            wiki, state: "quarantined", snapshot: "2026-08", taskId: `${wiki}-task`,
            error: "workload profile Large has not completed production qualification",
          })), quarantine: [], recentFailures: []},
          freshness: {status: "healthy", alerts: [], summary: {}},
          operationalTruth: {
            public: {status: "healthy", alerts: [], scrub: {state: "valid"}},
            pipeline: {
              status: "degraded",
              issues: [],
              blockerGroups: [{
                code: "workload_profile_unqualified",
                summary: "The selected workload profile has not passed production qualification for this workload.",
                remediation: "Run the profile qualification before retrying these projects.",
                retryable: false,
                affectedWikis: ["frwiki", "itwiki", "nlwiki", "ptwiki", "svwiki"],
                states: ["quarantined"],
              }],
            },
            infrastructure: {status: "available", issues: [], activeRequests: []},
            wikis: {
              dewiki: {
                wiki: "dewiki",
                lifecycle: {publication: "hidden", refresh: "qualification"},
                snapshots: {latestAvailable: "2026-08", candidate: "2026-08", qualification: "2026-08", ready: null, published: null, cutoff: "2026-08"},
                candidate: null,
                qualification: {wiki: "dewiki", snapshot: "2026-08", runId: "qualified-dewiki", structurallyValid: true, artifactCount: 10, artifactBytes: 704892841},
                ready: null,
                activePublished: null,
                metrics: {candidate: {expected: [], present: [], missing: [], complete: true}, published: {expected: [], present: [], missing: [], complete: false}, details: []},
                quality: {summary: {healthy: 0, warning: 0, critical: 0, unavailable: 0}, metrics: [], signals: [], anomalies: []},
                issues: [], allowedActions: [],
              },
            }
          },
          lifecycleAudit: {events: [], invalid: []}
        }));
        return true;
      }
      if (url.pathname === "/admin-api/publication-preflight" && request.method === "POST") {
        operation = {
          requestId: "admin-e2e-preflight",
          action: "publication-preflight",
          state: "queued",
          requestedAt: "2026-09-06T12:00:00Z",
          updatedAt: "2026-09-06T12:00:00Z"
        };
        response.writeHead(202, {"content-type": "application/json"});
        response.end(JSON.stringify({queued: true, requestId: operation.requestId, operation}));
        return true;
      }
      return false;
    }
  });

  let chrome;
  let userData;
  let cdp;
  try {
    const launched = await launchChrome({budgets: {reference_device: {javascript_heap_limit_mib: 512}}});
    ({chrome, userData} = launched);
    const target = await fetch(`http://127.0.0.1:${launched.port}/json/new?about:blank`, {method: "PUT"}).then((response) => response.json());
    cdp = new Cdp(target.webSocketDebuggerUrl);
    await cdp.open();
    await Promise.all([cdp.send("Page.enable"), cdp.send("Runtime.enable")]);
    await cdp.send("Page.addScriptToEvaluateOnNewDocument", {
      source: "globalThis.__adminAlertCalls=0;globalThis.alert=()=>{globalThis.__adminAlertCalls+=1}"
    });
    await cdp.send("Page.navigate", {url: `${origin}/admin`});
    await waitFor(cdp, "document.readyState === 'complete'");
    await waitFor(cdp, "document.querySelectorAll('[data-admin-view-tab]').length === 4");
    await waitFor(cdp, "Boolean(document.querySelector('.admin-operation-receipts'))");
    await waitFor(cdp, "!document.querySelector('[data-admin-view-tab=overview]').disabled");

    const initial = await evaluate(cdp, `({
      selected: Array.from(document.querySelectorAll('[data-admin-view-tab]')).filter(tab => tab.getAttribute('aria-selected') === 'true').map(tab => tab.dataset.adminViewTab),
      visible: Array.from(document.querySelectorAll('[data-admin-view]')).filter(section => !section.hidden).map(section => section.dataset.adminView),
      controlsValid: Array.from(document.querySelectorAll('[data-admin-view-tab]')).every(tab => document.getElementById(tab.getAttribute('aria-controls'))),
      liveRegion: Boolean(document.querySelector('[role=log][aria-live=polite], [role=status][aria-live=polite]'))
    })`);
    assertContract(initial.selected.length === 1 && initial.selected[0] === "overview", "overview is not the single default view", initial);
    assertContract(initial.visible.every((view) => view.split(' ').includes("overview")), "another focused view leaked into overview", initial);
    assertContract(initial.controlsValid && initial.liveRegion, "tab or receipt accessibility contract is incomplete", initial);

    await evaluate(cdp, "document.querySelector('[data-admin-view-tab=overview]').focus()");
    await cdp.send("Input.dispatchKeyEvent", {type: "keyDown", key: "ArrowRight", code: "ArrowRight"});
    await cdp.send("Input.dispatchKeyEvent", {type: "keyUp", key: "ArrowRight", code: "ArrowRight"});
    await waitFor(cdp, "document.querySelector('[data-admin-view-tab=wikis]').getAttribute('aria-selected') === 'true'");
    const keyboard = await evaluate(cdp, `({
      view: new URL(location.href).searchParams.get('view'),
      focus: document.activeElement?.dataset?.adminViewTab,
      visible: Array.from(document.querySelectorAll('[data-admin-view]')).filter(section => !section.hidden).map(section => section.dataset.adminView)
    })`);
    assertContract(keyboard.view === "wikis" && keyboard.focus === "wikis", "keyboard navigation did not preserve focus and URL state", keyboard);
    assertContract(keyboard.visible.every((view) => view.split(' ').includes("wikis")), "wiki view leaked unrelated sections", keyboard);
    const operationalTruth = await evaluate(cdp, `({
      qualification: Array.from(document.querySelectorAll('.admin-pipeline-row')).find(row => row.textContent.includes('dewiki'))?.textContent,
      blockerPanels: document.querySelectorAll('.admin-shared-blockers article').length,
      blockerText: document.querySelector('.admin-shared-blockers')?.textContent
    })`);
    assertContract(operationalTruth.qualification?.includes("Qualification ready"), "dewiki qualification is not visible as completed", operationalTruth);
    assertContract(operationalTruth.blockerPanels === 1 && operationalTruth.blockerText.includes("5 projects share one blocker"),
      "shared fleet failure was rendered as unrelated interventions", operationalTruth);

    await evaluate(cdp, "document.querySelector('[data-admin-view-tab=overview]').click()");
    await waitFor(cdp, "Array.from(document.querySelectorAll('button')).some(button => button.textContent.includes('Run publication preflight') && !button.disabled)");
    await evaluate(cdp, "Array.from(document.querySelectorAll('button')).find(button => button.textContent.includes('Run publication preflight')).click()");
    await waitFor(cdp, "document.body.textContent.includes('Succeeded') && document.body.textContent.includes('preflight complete')", 10000);
    const receipt = await evaluate(cdp, `({
      text: document.querySelector('.admin-operation-receipt')?.textContent,
      stored: JSON.parse(localStorage.getItem('wiki-economics.admin.operation-receipts.v1') || '[]'),
      alerts: globalThis.__adminAlertCalls
    })`);
    assertContract(receipt.stored.some((entry) => entry.requestId === "admin-e2e-preflight" && entry.state === "succeeded"),
      "the operation receipt did not survive server reconciliation", receipt);
    assertContract(apiRequests.includes("POST /admin-api/publication-preflight"),
      "the operator action did not reach the admin API", apiRequests);
    assertContract(receipt.alerts === 0, "operator workflow opened a blocking browser alert", receipt);

    await cdp.send("Emulation.setDeviceMetricsOverride", {width: 390, height: 844, deviceScaleFactor: 1, mobile: true});
    const mobile = await evaluate(cdp, `({
      viewport: document.documentElement.clientWidth,
      documentWidth: document.documentElement.scrollWidth,
      tabHeights: Array.from(document.querySelectorAll('[data-admin-view-tab]')).map(tab => tab.getBoundingClientRect().height),
      receiptWidth: document.querySelector('.admin-operation-receipts')?.getBoundingClientRect().width
    })`);
    assertContract(mobile.documentWidth <= mobile.viewport, "admin page overflows the mobile viewport", mobile);
    assertContract(mobile.tabHeights.every((height) => height >= 44), "mobile view tabs miss the 44px touch target", mobile);
    assertContract(mobile.receiptWidth <= mobile.viewport, "operation receipt overflows the mobile viewport", mobile);

    return {schema_version: 1, views: 4, keyboard_navigation: true, durable_receipt: true,
      blocking_alerts: receipt.alerts, mobile_viewport: mobile.viewport, mobile_document_width: mobile.documentWidth};
  } finally {
    cdp?.close();
    if (chrome) await terminateChild(chrome);
    await new Promise((resolve) => server.close(resolve));
    if (userData) fs.rmSync(userData, {recursive: true, force: true, maxRetries: 5, retryDelay: 100});
  }
}

function parseArguments(argv) {
  if (argv.length !== 2 || argv[0] !== "--dist-dir") {
    throw new Error("usage: admin-browser-workflow.cjs --dist-dir PATH");
  }
  return {distDir: path.resolve(argv[1])};
}

if (require.main === module) {
  runAdminBrowserWorkflow(parseArguments(process.argv.slice(2)))
    .then((report) => process.stdout.write(`${JSON.stringify(report, null, 2)}\n`))
    .catch((error) => { console.error(error.stack || error.message); process.exitCode = 1; });
}

module.exports = {parseArguments, runAdminBrowserWorkflow};
