"use strict";

const fs = require("node:fs");

const SMALL_ACTIONS = new Set([
  "patrol-fetch",
  "promote-qualification",
  "retire-candidate",
  "fleet-recover",
  "quarantine-retry",
  "publication-recovery-audit",
  "publication-preflight",
]);

const MEDIUM_ACTIONS = new Set([
  "run",
  "qualify",
  "rebuild-candidate",
  "ingest",
  "compute",
  "patrol-compute",
  "patrol-rebuild",
  "merge",
  "publish",
  "site",
  "artifact-scrub",
  "rebuild-compatibility-cohort",
  "fetch",
]);

const ACTION_PRIORITY = Object.freeze({
  "fleet-recover": 0,
  "publication-recovery-audit": 1,
  "promote-qualification": 2,
  "retire-candidate": 3,
  "quarantine-retry": 4,
  "publication-preflight": 5,
  publish: 10,
  run: 20,
  qualify: 20,
  "rebuild-candidate": 20,
  "rebuild-compatibility-cohort": 15,
  compute: 25,
  "patrol-compute": 25,
  "patrol-rebuild": 25,
  ingest: 30,
  "patrol-fetch": 35,
  fetch: 40,
  merge: 50,
  site: 50,
  "artifact-scrub": 60,
});

function readLifecycle(path) {
  try {
    const value = JSON.parse(fs.readFileSync(path, "utf8"));
    return value?.schema_version === 1 && value.wikis ? value : {wikis: {}};
  } catch {
    return {wikis: {}};
  }
}

function operationPriority(request) {
  return ACTION_PRIORITY[request?.action] ?? 100;
}

function resourceClassFor(request, lifecyclePath) {
  if (SMALL_ACTIONS.has(request.action)) return "small";
  if (!MEDIUM_ACTIONS.has(request.action)) {
    throw new Error(`Unsupported admin operation ${request.action}`);
  }
  if (!request.wiki || new Set(["merge", "publish", "site", "artifact-scrub"]).has(request.action)) {
    return "medium_large";
  }
  const configured = readLifecycle(lifecyclePath).wikis?.[request.wiki]?.fleet_resource_class;
  return configured === "small" ? "small" : "medium_large";
}

function compareOperations(left, right) {
  return operationPriority(left) - operationPriority(right)
    || Date.parse(left?.requestedAt || 0) - Date.parse(right?.requestedAt || 0)
    || String(left?.requestId || "").localeCompare(String(right?.requestId || ""));
}

module.exports = {
  ACTION_PRIORITY,
  compareOperations,
  operationPriority,
  resourceClassFor,
};
