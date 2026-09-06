"use strict";

const fs = require("node:fs");
const path = require("node:path");

function readJson(file) {
  try {
    return JSON.parse(fs.readFileSync(file, "utf8"));
  } catch {
    return null;
  }
}

function directoryNames(directory) {
  try {
    return fs.readdirSync(directory, {withFileTypes: true})
      .filter((entry) => entry.isDirectory())
      .map((entry) => entry.name);
  } catch {
    return [];
  }
}

function jsonFileStems(directory) {
  try {
    return fs.readdirSync(directory, {withFileTypes: true})
      .filter((entry) => entry.isFile() && entry.name.endsWith(".json"))
      .map((entry) => entry.name.slice(0, -5));
  } catch {
    return [];
  }
}

function metricDefinitions(catalog) {
  if (catalog?.schema_version !== 1 || !Array.isArray(catalog.metrics)) return [];
  return catalog.metrics.filter((metric) =>
    typeof metric?.id === "string"
    && typeof metric?.family === "string"
    && typeof metric?.algorithm_version === "string");
}

function datasetApplies(contract, wiki, lifecycleEntry) {
  if (lifecycleEntry?.publication === "hidden" && lifecycleEntry?.refresh === "qualification") return true;
  if (lifecycleEntry?.publication !== "published") return false;
  if (contract?.coverage === "all_published") return true;
  return Array.isArray(contract?.wikis) && contract.wikis.includes(wiki);
}

function expectedMetricsForWiki(definitions, lifecycle, wiki) {
  const lifecycleEntry = lifecycle?.wikis?.[wiki] || null;
  const contracts = lifecycle?.publication_contract?.datasets || {};
  return definitions
    .filter((definition) => datasetApplies(contracts[definition.id], wiki, lifecycleEntry))
    .map((definition) => definition.id)
    .sort();
}

function candidateStatus(outputDir, wiki) {
  const value = readJson(path.join(outputDir, "_candidate-status", `${wiki}.json`));
  if (value?.schemaVersion !== 2 || value.wikis?.[0] !== wiki || typeof value.runId !== "string") return null;
  return value;
}

function readyIndex(outputDir, wiki) {
  const value = readJson(path.join(outputDir, "_ready-index", `${wiki}.json`));
  if (value?.schema_version !== 2 || value.wiki !== wiki) return null;
  return value;
}

function publicationGate(outputDir) {
  const value = readJson(path.join(outputDir, "publication-gate.json"));
  if (!Number.isInteger(value?.schema_version)
      || value.schema_version < 1
      || typeof value.run_id !== "string"
      || !value.selected_snapshot_versions
      || !value.metrics) return null;
  return value;
}

function scrubStatus(outputDir) {
  const value = readJson(path.join(outputDir, "_scrubs", "status.json"));
  if (!value) return {state: "missing", valid: false};
  const valid = value.schema_version === 1
    && ["succeeded", "failed"].includes(value.state)
    && typeof value.run_id === "string"
    && Number.isSafeInteger(value.updated_at_unix);
  return valid ? {...value, valid: true} : {state: "invalid", valid: false};
}

function completedSnapshots(dataDir, wiki) {
  const root = path.join(dataDir, "snapshots", wiki);
  return directoryNames(root)
    .filter((snapshot) => /^\d{4}-\d{2}$/.test(snapshot))
    .filter((snapshot) => {
      const plan = readJson(path.join(root, snapshot, "source-plan.json"));
      const inventory = readJson(path.join(root, snapshot, "remote-inventory.json"));
      return plan?.wiki === wiki
        && plan?.snapshot === snapshot
        && inventory?.wiki === wiki
        && inventory?.snapshot === snapshot;
    })
    .sort();
}

function validReceiptIdentity(value) {
  return typeof value === "string" && /^[0-9a-f]{64}$/.test(value);
}

function readyMetricIds(index, definitions, referenceName) {
  const reference = index?.[referenceName];
  if (!reference || !validReceiptIdentity(reference.ready_receipt_sha256)) return [];
  const families = reference.core_family_receipt_identities || {};
  return definitions
    .filter((definition) => definition.family === "patrol"
      ? validReceiptIdentity(reference.patrol_receipt_identity)
      : validReceiptIdentity(families[definition.family]))
    .map((definition) => definition.id)
    .sort();
}

function publishedMetricIds(gate, wiki, expected) {
  return expected.filter((metric) => {
    const proof = gate?.metrics?.[metric]?.wikis?.[wiki];
    return Number.isSafeInteger(proof?.rows)
      && proof.rows > 0
      && typeof proof.minimum_date === "string"
      && typeof proof.maximum_date === "string";
  });
}

function completeness(expected, present) {
  const presentSet = new Set(present);
  const missing = expected.filter((metric) => !presentSet.has(metric));
  const unexpected = present.filter((metric) => !expected.includes(metric));
  return {
    expected,
    present: expected.filter((metric) => presentSet.has(metric)),
    missing,
    unexpected,
    complete: expected.length > 0 && missing.length === 0,
  };
}

function normalizeCandidate(value) {
  if (!value) return null;
  return {
    state: value.state,
    runId: value.runId,
    snapshot: value.selectedSnapshot || null,
    startedAt: value.startedAt || null,
    finishedAt: value.finishedAt || null,
    heartbeatAt: value.heartbeatAt || null,
    currentStage: value.currentStage || value.failingStage || null,
    failingStage: value.failingStage || null,
    error: value.error || null,
    exitCode: value.exitCode ?? null,
    durationSecs: value.durationSecs ?? null,
    stageDurationsMs: value.stageDurationsMs || {},
    memoryPeakBytes: value.memoryPeakBytes ?? null,
    memoryLimitBytes: value.memoryLimitBytes ?? null,
    disk: value.disk || null,
    logFile: value.logFile || null,
  };
}

function latestTimestamp(...values) {
  return values.filter(Boolean).sort().at(-1) || null;
}

function wikiOperationalTruth({wiki, definitions, lifecycle, dataDir, outputDir, gate}) {
  const entry = lifecycle?.wikis?.[wiki] || null;
  const expected = expectedMetricsForWiki(definitions, lifecycle, wiki);
  const status = candidateStatus(outputDir, wiki);
  const index = readyIndex(outputDir, wiki);
  const completed = completedSnapshots(dataDir, wiki);
  const publishedSnapshot = gate?.selected_snapshot_versions?.[wiki]
    || index?.active_published?.snapshot
    || null;
  const publishedCutoff = gate?.cutoff_dates?.[wiki] || null;
  const readySnapshot = index?.newest_valid_ready?.snapshot || null;
  const candidate = normalizeCandidate(status);
  const latestAvailable = completed.at(-1) || readySnapshot || publishedSnapshot;
  const candidateCompleteness = completeness(expected, readyMetricIds(index, definitions, "newest_valid_ready"));
  const publishedCompleteness = completeness(expected, publishedMetricIds(gate, wiki, expected));
  const issues = [];
  const candidateIsNewer = candidate?.snapshot && (!publishedSnapshot || candidate.snapshot > publishedSnapshot);
  if (candidateIsNewer && candidate.state === "failed") {
    issues.push({
      code: "candidate_failed",
      severity: "critical",
      message: `${wiki} candidate ${candidate.snapshot} failed${candidate.failingStage ? ` during ${candidate.failingStage}` : ""}: ${candidate.error || "unknown error"}`,
      runId: candidate.runId,
    });
  }
  if (entry?.publication === "published" && !publishedCompleteness.complete) {
    issues.push({
      code: "published_metrics_incomplete",
      severity: "critical",
      message: `${wiki} is missing published metric proof for ${publishedCompleteness.missing.join(", ") || "the required metric set"}.`,
    });
  }
  if (latestAvailable && publishedSnapshot && latestAvailable > publishedSnapshot) {
    issues.push({
      code: "snapshot_pending",
      severity: candidate?.state === "failed" ? "critical" : "warning",
      message: `${wiki} has completed snapshot ${latestAvailable}, while ${publishedSnapshot} is public.`,
    });
  }
  return {
    wiki,
    lifecycle: entry,
    snapshots: {
      latestAvailable,
      candidate: candidate?.snapshot || readySnapshot,
      ready: readySnapshot,
      published: publishedSnapshot,
      cutoff: publishedCutoff,
    },
    candidate,
    ready: index?.newest_valid_ready || null,
    activePublished: index?.active_published || null,
    metrics: {
      candidate: candidateCompleteness,
      published: publishedCompleteness,
    },
    issues,
    updatedAt: latestTimestamp(candidate?.finishedAt, candidate?.heartbeatAt),
  };
}

function requestedMemoryForWork(work, capacity) {
  return capacity.resource_requests?.[work.resourceClass] || 0;
}

function infrastructureTruth({capacity, fleet, adminOperations, scheduledRefresh}) {
  if (capacity?.schema_version !== 1) return {status: "unknown", issues: ["Toolforge capacity configuration is missing or invalid."]};
  const runningFleet = (fleet?.work || []).filter((work) => work.state === "running");
  const activeRequests = runningFleet.map((work) => ({
    kind: "fleet",
    wiki: work.wiki,
    resourceClass: work.resourceClass,
    bytes: requestedMemoryForWork(work, capacity),
  }));
  if ((adminOperations?.counts?.running || 0) > 0) {
    activeRequests.push({kind: "admin_dispatcher", bytes: capacity.resource_requests.admin_dispatcher});
  }
  if (["starting", "running"].includes(scheduledRefresh?.last?.state)) {
    activeRequests.push({kind: "publisher", bytes: capacity.resource_requests.publisher});
  }
  const activeJobBytes = activeRequests.reduce((total, request) => total + Number(request.bytes || 0), 0);
  const usedBytes = Number(capacity.resident_service_memory_bytes || 0) + activeJobBytes;
  const limitBytes = Number(capacity.namespace_memory_limit_bytes || 0);
  const availableBytes = Math.max(0, limitBytes - usedBytes);
  const minimumJobBytes = Number(capacity.minimum_schedulable_job_bytes || 0);
  const constrained = activeRequests.length > 0 && availableBytes < minimumJobBytes;
  const issues = constrained ? [{
    code: "toolforge_memory_quota_contention",
    severity: "warning",
    message: `Active requested memory leaves ${availableBytes} bytes, below the ${minimumJobBytes}-byte minimum data-job request; other workers or publication must wait.`,
  }] : [];
  return {
    status: constrained ? "constrained" : "available",
    namespaceMemoryLimitBytes: limitBytes,
    residentServiceMemoryBytes: capacity.resident_service_memory_bytes,
    activeJobRequestedBytes: activeJobBytes,
    availableRequestedBytes: availableBytes,
    minimumSchedulableJobBytes: minimumJobBytes,
    activeRequests,
    issues,
    source: capacity.source || null,
    verifiedAt: capacity.verified_at || null,
  };
}

function buildOperationalTruth({root, dataDir, outputDir, lifecycle, freshness, fleet, adminOperations, scheduledRefresh}) {
  const catalog = readJson(path.join(root, "config", "generated", "metric-catalog.json"));
  const definitions = metricDefinitions(catalog);
  const gate = publicationGate(outputDir);
  const scrub = scrubStatus(outputDir);
  const wikiNames = new Set([
    ...Object.keys(lifecycle?.wikis || {}),
    ...jsonFileStems(path.join(outputDir, "_candidate-status")),
    ...jsonFileStems(path.join(outputDir, "_ready-index")),
  ]);
  const wikis = Object.fromEntries([...wikiNames].sort().map((wiki) => [wiki, wikiOperationalTruth({
    wiki, definitions, lifecycle, dataDir, outputDir, gate,
  })]));
  const pipelineIssues = Object.values(wikis).flatMap((wiki) => wiki.issues);
  const capacity = readJson(path.join(root, "config", "toolforge-capacity.json"));
  const infrastructure = infrastructureTruth({capacity, fleet, adminOperations, scheduledRefresh});
  const publicStatus = freshness?.status || (gate ? "healthy" : "unknown");
  const pipelineStatus = pipelineIssues.some((issue) => issue.severity === "critical")
    ? "degraded"
    : pipelineIssues.length > 0 ? "attention" : "healthy";
  return {
    schemaVersion: 1,
    generatedAt: new Date().toISOString(),
    metricCatalog: {
      schemaVersion: catalog?.schema_version ?? null,
      expectedMetricIds: definitions.map((definition) => definition.id).sort(),
    },
    public: {
      status: publicStatus,
      publicationRunId: gate?.run_id || freshness?.summary?.lastPublicationRunId || null,
      gateValid: Boolean(gate),
      selectedSnapshots: gate?.selected_snapshot_versions || {},
      cutoffDates: gate?.cutoff_dates || {},
      scrub,
      alerts: freshness?.alerts || [],
    },
    pipeline: {status: pipelineStatus, issues: pipelineIssues},
    infrastructure,
    wikis,
  };
}

module.exports = {
  buildOperationalTruth,
  completedSnapshots,
  expectedMetricsForWiki,
  infrastructureTruth,
  metricDefinitions,
};
