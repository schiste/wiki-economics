"use strict";

const fs = require("node:fs");
const path = require("node:path");
const {classifyError} = require("./admin-operation-status.cjs");
const {buildWikiQuality} = require("./admin-quality.cjs");

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
  const stage = value.currentStage || value.failingStage || null;
  const failure = classifyError(value.error || null);
  return {
    state: value.state,
    action: "prepare",
    runId: value.runId,
    snapshot: value.selectedSnapshot || null,
    selectedSnapshot: value.selectedSnapshot || null,
    startedAt: value.startedAt || null,
    finishedAt: value.finishedAt || null,
    heartbeatAt: value.heartbeatAt || null,
    currentStage: stage,
    stage,
    stageLabel: stage ? stage.replaceAll("_", " ") : null,
    failingStage: value.failingStage || null,
    error: value.error || null,
    errorSummary: failure.errorSummary,
    retryable: failure.retryable,
    remediationCode: failure.remediationCode,
    remediation: failure.remediation,
    exitCode: value.exitCode ?? null,
    durationSecs: value.durationSecs ?? null,
    stageDurationsMs: value.stageDurationsMs || {},
    stages: Array.isArray(value.stages) ? value.stages.map((stageEntry) => ({
      stage: stageEntry.stage || null,
      wiki: stageEntry.wiki || null,
      state: stageEntry.state || "unknown",
      startedAt: stageEntry.startedAt || null,
      finishedAt: stageEntry.finishedAt || null,
      durationMs: stageEntry.durationMs ?? null,
      reused: Boolean(stageEntry.reused),
      skipped: Boolean(stageEntry.skipped),
      error: stageEntry.error || null,
    })) : [],
    reusedStages: Array.isArray(value.reusedStages) ? value.reusedStages : [],
    skippedStages: Array.isArray(value.skippedStages) ? value.skippedStages : [],
    memoryCurrentBytes: value.memoryCurrentBytes ?? null,
    memoryPeakBytes: value.memoryPeakBytes ?? null,
    memoryLimitBytes: value.memoryLimitBytes ?? null,
    cpu: value.cpu || null,
    disk: value.disk || null,
    provenance: value.provenance || null,
    publication: value.publication || null,
    publishedSiteGeneration: value.publishedSiteGeneration || null,
    logFile: value.logFile || null,
  };
}

function allowedFailureActions(candidate, fleetWork = null) {
  const code = candidate?.remediationCode || null;
  if (fleetWork?.state === "stalled" || code === "fleet_lease_stale") {
    return [{id: "fleet-recover", label: "Recover stale lease", confirmation: "Authenticate stale leases and requeue recoverable work?"}];
  }
  if (fleetWork?.state === "quarantined" || code === "fleet_task_quarantined") {
    if (candidate?.retryable === false && code && code !== "fleet_task_quarantined") return [];
    return [{
      id: "quarantine-retry",
      label: "Retry quarantined task",
      wiki: fleetWork?.wiki || null,
      taskId: fleetWork?.taskId || null,
      confirmation: "Retry this exact quarantined task after confirming its failure cause has been corrected?",
    }];
  }
  if (code === "publication_evidence_mismatch") {
    return [
      {id: "publication-recovery-audit", label: "Audit publication recovery"},
      {id: "artifact-scrub", label: "Scrub published artifacts", confirmation: "Run a complete sequential integrity scrub?"},
    ];
  }
  if (code === "patrol_source_missing" || code === "patrol_semantic_failure") {
    return [{id: "patrol-rebuild", label: "Rebuild patrol only", wiki: candidate?.wiki || null}];
  }
  if (candidate?.retryable === true) {
    return [{id: "run", label: "Retry preparation", wiki: candidate?.wiki || null, acknowledgeBlockedRetry: false}];
  }
  return [];
}

function latestTimestamp(...values) {
  return values.filter(Boolean).sort().at(-1) || null;
}

function wikiOperationalTruth({wiki, definitions, lifecycle, dataDir, outputDir, gate, scrub, qualityPolicy, nowUnix, qualifications}) {
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
  const qualification = (qualifications?.[wiki] || []).find((entry) => entry.structurallyValid) || null;
  const candidate = normalizeCandidate(status);
  if (candidate) candidate.wiki = wiki;
  const latestAvailable = [completed.at(-1), qualification?.snapshot, readySnapshot, publishedSnapshot]
    .filter(Boolean).sort().at(-1) || null;
  const hiddenQualification = entry?.publication === "hidden" && entry?.refresh === "qualification";
  const candidateTargetSnapshot = status?.selectedSnapshot || (hiddenQualification ? qualification?.snapshot : readySnapshot) || readySnapshot;
  const candidateReadyIds = hiddenQualification && qualification
    ? expected.filter((metric) => qualification.metricIds?.includes(metric))
    : readySnapshot && readySnapshot === candidateTargetSnapshot
      ? readyMetricIds(index, definitions, "newest_valid_ready")
      : [];
  const candidateCompleteness = completeness(expected, candidateReadyIds);
  const publishedCompleteness = completeness(expected, publishedMetricIds(gate, wiki, expected));
  const candidateMetricSet = new Set(candidateCompleteness.present);
  const publishedMetricSet = new Set(publishedCompleteness.present);
  const metricDetails = definitions.filter((definition) => expected.includes(definition.id)).map((definition) => {
    const proof = gate?.metrics?.[definition.id]?.wikis?.[wiki] || null;
    return {
      id: definition.id,
      family: definition.family,
      algorithmVersion: definition.algorithm_version,
      candidateReady: candidateMetricSet.has(definition.id),
      publishedReady: publishedMetricSet.has(definition.id),
      publishedRows: proof?.rows ?? null,
      minimumDate: proof?.minimum_date ?? null,
      maximumDate: proof?.maximum_date ?? null,
      conservationTotal: proof?.conservation_total ?? null,
    };
  });
  const issues = [];
  const quality = buildWikiQuality({
    wiki,
    definitions: definitions.filter((definition) => expected.includes(definition.id)),
    dataDir,
    outputDir,
    gate,
    index,
    scrubStatus: scrub,
    policy: qualityPolicy,
    nowUnix,
  });
  issues.push(...quality.anomalies.map((anomaly) => ({
    ...anomaly,
    wiki,
    message: `${wiki}: ${anomaly.message}`,
  })));
  const candidateIsNewer = candidate?.snapshot && (!publishedSnapshot || candidate.snapshot > publishedSnapshot);
  if (candidateIsNewer && candidate.state === "failed") {
    issues.push({
      code: "candidate_failed",
      wiki,
      severity: "critical",
      message: `${wiki} candidate ${candidate.snapshot} failed${candidate.failingStage ? ` during ${candidate.failingStage}` : ""}: ${candidate.error || "unknown error"}`,
      runId: candidate.runId,
    });
  }
  if (entry?.publication === "published" && !publishedCompleteness.complete) {
    issues.push({
      code: "published_metrics_incomplete",
      wiki,
      severity: "critical",
      message: `${wiki} is missing published metric proof for ${publishedCompleteness.missing.join(", ") || "the required metric set"}.`,
    });
  }
  if (latestAvailable && publishedSnapshot && latestAvailable > publishedSnapshot) {
    if (candidate?.state !== "failed" && readySnapshot && readySnapshot > publishedSnapshot) {
      issues.push({
        code: "candidate_ready_not_published",
        wiki,
        severity: "warning",
        message: `${wiki} candidate ${readySnapshot} is ready, while ${publishedSnapshot} remains public.`,
      });
    } else if (!candidate || !["starting", "running"].includes(candidate.state)) {
      issues.push({
        code: "snapshot_pending",
        wiki,
        severity: "warning",
        message: `${wiki} has completed snapshot ${latestAvailable}, while ${publishedSnapshot} is public.`,
      });
    }
  }
  return {
    wiki,
    lifecycle: entry,
    snapshots: {
      latestAvailable,
      candidate: candidate?.snapshot || (hiddenQualification ? qualification?.snapshot : readySnapshot) || readySnapshot,
      ready: readySnapshot,
      qualification: qualification?.snapshot || null,
      published: publishedSnapshot,
      cutoff: publishedCutoff,
    },
    candidate,
    qualification,
    ready: index?.newest_valid_ready || null,
    activePublished: index?.active_published || null,
    metrics: {
      candidate: candidateCompleteness,
      published: publishedCompleteness,
      details: metricDetails,
    },
    quality,
    issues,
    updatedAt: latestTimestamp(candidate?.finishedAt, candidate?.heartbeatAt),
  };
}

function publicationOperations(outputDir, wikis, sourceIdentity = {}) {
  const preflight = readJson(path.join(outputDir, "_admin", "publication-preflight.json"));
  const recoveryAudit = readJson(path.join(outputDir, "_admin", "publication-recovery-audit.json"));
  const changePlan = readJson(path.join(outputDir, "publication-change-plan.json"));
  const validPreflight = preflight?.schema_version === 1 ? preflight : null;
  const preflightCurrent = Boolean(validPreflight)
    && (validPreflight.wikis || []).every((entry) => entry.candidate_run_id == null
      || wikis?.[entry.wiki]?.ready?.run_id === entry.candidate_run_id)
    && (!validPreflight.generating_commit || !sourceIdentity.sourceCommit
      || validPreflight.generating_commit === sourceIdentity.sourceCommit)
    && (!validPreflight.site_source_commit || !sourceIdentity.siteSourceCommit
      || validPreflight.site_source_commit === sourceIdentity.siteSourceCommit);
  return {
    preflight: validPreflight ? {...validPreflight, current: preflightCurrent} : null,
    recoveryAudit: recoveryAudit?.schema_version === 1 ? recoveryAudit : null,
    currentChangePlan: changePlan?.schema_version === 1 ? changePlan : null,
  };
}

function requestedCapacityForWork(work, capacity) {
  return {
    bytes: capacity.resource_requests?.[work.resourceClass] || 0,
    millicores: capacity.resource_cpu_requests_millicores?.[work.resourceClass] || 0,
  };
}

function infrastructureTruth({capacity, fleet, adminOperations, scheduledRefresh}) {
  if (capacity?.schema_version !== 1) return {status: "unknown", issues: ["Toolforge capacity configuration is missing or invalid."]};
  const runningFleet = (fleet?.work || []).filter((work) => work.state === "running");
  const activeRequests = runningFleet.map((work) => ({
    kind: "fleet",
    wiki: work.wiki,
    resourceClass: work.resourceClass,
    ...requestedCapacityForWork(work, capacity),
  }));
  if ((adminOperations?.counts?.running || 0) > 0) {
    activeRequests.push({
      kind: "admin_dispatcher",
      bytes: capacity.resource_requests.admin_dispatcher,
      millicores: capacity.resource_cpu_requests_millicores?.admin_dispatcher || 0,
    });
  }
  if (["starting", "running"].includes(scheduledRefresh?.last?.state)) {
    activeRequests.push({
      kind: "publisher",
      bytes: capacity.resource_requests.publisher,
      millicores: capacity.resource_cpu_requests_millicores?.publisher || 0,
    });
  }
  const activeJobBytes = activeRequests.reduce((total, request) => total + Number(request.bytes || 0), 0);
  const activeJobMillicores = activeRequests.reduce(
    (total, request) => total + Number(request.millicores || 0), 0,
  );
  const usedBytes = Number(capacity.resident_service_memory_bytes || 0) + activeJobBytes;
  const usedMillicores = Number(capacity.resident_service_cpu_millicores || 0) + activeJobMillicores;
  const limitBytes = Number(capacity.namespace_memory_limit_bytes || 0);
  const limitMillicores = Number(capacity.namespace_cpu_limit_millicores || 0);
  const availableBytes = Math.max(0, limitBytes - usedBytes);
  const availableMillicores = Math.max(0, limitMillicores - usedMillicores);
  const minimumJobBytes = Number(capacity.minimum_schedulable_job_bytes || 0);
  const minimumJobMillicores = Number(capacity.minimum_schedulable_job_millicores || 0);
  const memoryConstrained = availableBytes < minimumJobBytes;
  const cpuConstrained = availableMillicores < minimumJobMillicores;
  const constrained = activeRequests.length > 0 && (memoryConstrained || cpuConstrained);
  const gib = (bytes) => `${(bytes / 1024 ** 3).toFixed(1)} GiB`;
  const cpu = (millicores) => `${(millicores / 1000).toFixed(2)} CPU`;
  const constrainedResources = [
    ...(memoryConstrained ? ["memory"] : []),
    ...(cpuConstrained ? ["CPU"] : []),
  ].join(" and ");
  const issues = constrained ? [{
    code: "toolforge_resource_quota_contention",
    severity: "warning",
    message: `Active workloads leave ${gib(availableBytes)} and ${cpu(availableMillicores)} available; ${constrainedResources} is below the minimum data-job request, so other workers or publication must wait.`,
  }] : [];
  return {
    status: constrained ? "constrained" : "available",
    namespaceMemoryLimitBytes: limitBytes,
    namespaceCpuLimitMillicores: limitMillicores,
    perJobMemoryLimitBytes: capacity.per_job_memory_limit_bytes,
    perJobCpuLimitMillicores: capacity.per_job_cpu_limit_millicores,
    residentServiceMemoryBytes: capacity.resident_service_memory_bytes,
    residentServiceCpuMillicores: capacity.resident_service_cpu_millicores,
    activeJobRequestedBytes: activeJobBytes,
    activeJobRequestedMillicores: activeJobMillicores,
    availableRequestedBytes: availableBytes,
    availableRequestedMillicores: availableMillicores,
    minimumSchedulableJobBytes: minimumJobBytes,
    minimumSchedulableJobMillicores: minimumJobMillicores,
    activeRequests,
    issues,
    source: capacity.source || null,
    verifiedAt: capacity.verified_at || null,
  };
}

function buildOperationalTruth({root, dataDir, outputDir, lifecycle, freshness, fleet, adminOperations, qualifications = {}, scheduledRefresh, sourceIdentity}) {
  const catalog = readJson(path.join(root, "config", "generated", "metric-catalog.json"));
  const definitions = metricDefinitions(catalog);
  const gate = publicationGate(outputDir);
  const scrub = scrubStatus(outputDir);
  const qualityPolicy = readJson(path.join(root, "config", "quality-policy.json"));
  const nowUnix = Math.floor(Date.now() / 1000);
  const wikiNames = new Set([
    ...Object.keys(lifecycle?.wikis || {}),
    ...Object.keys(qualifications || {}),
    ...jsonFileStems(path.join(outputDir, "_candidate-status")),
    ...jsonFileStems(path.join(outputDir, "_ready-index")),
  ]);
  const wikis = Object.fromEntries([...wikiNames].sort().map((wiki) => [wiki, wikiOperationalTruth({
    wiki, definitions, lifecycle, dataDir, outputDir, gate, scrub, qualityPolicy, nowUnix, qualifications,
  })]));
  const pipelineIssues = Object.values(wikis).flatMap((wiki) => wiki.issues);
  if (qualityPolicy?.schema_version !== 1) pipelineIssues.unshift({
    code: "quality_policy_invalid",
    severity: "critical",
    message: "The quality anomaly policy is missing or invalid; candidate comparisons are not trustworthy.",
  });
  const pipelineActive = Object.values(wikis).some((wiki) => ["starting", "running"].includes(wiki.candidate?.state))
    || (fleet?.work || []).some((work) => ["running", "queued", "waiting_upstream"].includes(work.state));
  const capacity = readJson(path.join(root, "config", "toolforge-capacity.json"));
  const infrastructure = infrastructureTruth({capacity, fleet, adminOperations, scheduledRefresh});
  const publication = publicationOperations(outputDir, wikis, sourceIdentity);
  for (const wiki of Object.values(wikis)) {
    const fleetWork = (fleet?.work || []).find((entry) => entry.wiki === wiki.wiki) || null;
    if (wiki.candidate?.state === "failed" || ["stalled", "quarantined"].includes(fleetWork?.state)) {
      wiki.allowedActions = allowedFailureActions(wiki.candidate, fleetWork);
    } else {
      wiki.allowedActions = [];
    }
  }
  const blockerGroups = [];
  const blockers = new Map();
  for (const wiki of Object.values(wikis)) {
    const fleetWork = (fleet?.work || []).find((entry) => entry.wiki === wiki.wiki) || null;
    if (wiki.candidate?.state !== "failed" && !["stalled", "quarantined"].includes(fleetWork?.state)) continue;
    const failure = wiki.candidate?.errorSummary
      ? {
          errorSummary: wiki.candidate.errorSummary,
          remediationCode: wiki.candidate.remediationCode,
          remediation: wiki.candidate.remediation,
          retryable: wiki.candidate.retryable,
        }
      : classifyError(fleetWork?.error || fleetWork?.quarantineReason || "Fleet task requires operator review");
    const key = failure.remediationCode || failure.errorSummary || fleetWork?.state || "unknown";
    if (!blockers.has(key)) blockers.set(key, {
      code: failure.remediationCode || "unclassified_pipeline_failure",
      summary: failure.errorSummary || "Pipeline work requires review.",
      remediation: failure.remediation || "Inspect the latest run evidence before taking action.",
      retryable: failure.retryable,
      affectedWikis: [],
      states: new Set(),
    });
    blockers.get(key).affectedWikis.push(wiki.wiki);
    blockers.get(key).states.add(fleetWork?.state || wiki.candidate?.state || "failed");
  }
  for (const blocker of blockers.values()) blockerGroups.push({
    ...blocker,
    affectedWikis: blocker.affectedWikis.sort(),
    states: [...blocker.states].sort(),
  });
  blockerGroups.sort((left, right) => right.affectedWikis.length - left.affectedWikis.length
    || left.code.localeCompare(right.code));
  const publicStatus = freshness?.status || (gate ? "healthy" : "unknown");
  const pipelineStatus = pipelineIssues.some((issue) => issue.severity === "critical")
    ? "degraded"
    : pipelineIssues.length > 0 ? "attention" : pipelineActive ? "working" : "healthy";
  return {
    schemaVersion: 1,
    generatedAt: new Date().toISOString(),
    metricCatalog: {
      schemaVersion: catalog?.schema_version ?? null,
      expectedMetricIds: definitions.map((definition) => definition.id).sort(),
    },
    qualityPolicy: qualityPolicy?.schema_version === 1 ? qualityPolicy : null,
    public: {
      status: publicStatus,
      publicationRunId: gate?.run_id || freshness?.summary?.lastPublicationRunId || null,
      gateValid: Boolean(gate),
      selectedSnapshots: gate?.selected_snapshot_versions || {},
      cutoffDates: gate?.cutoff_dates || {},
      scrub,
      alerts: freshness?.alerts || [],
      publication,
    },
    pipeline: {
      status: pipelineStatus,
      issues: pipelineIssues,
      blockerGroups,
      counts: {
        active: Object.values(wikis).filter((wiki) => ["starting", "running"].includes(wiki.candidate?.state)
          || (fleet?.work || []).some((work) => work.wiki === wiki.wiki && work.state === "running")).length,
        queued: (fleet?.work || []).filter((work) => work.state === "queued").length,
        waitingUpstream: (fleet?.work || []).filter((work) => work.state === "waiting_upstream").length,
        quarantined: (fleet?.work || []).filter((work) => work.state === "quarantined").length,
        qualificationsReady: Object.values(wikis).filter((wiki) => wiki.qualification?.structurallyValid).length,
      },
    },
    infrastructure,
    wikis,
  };
}

module.exports = {
  buildOperationalTruth,
  completedSnapshots,
  expectedMetricsForWiki,
  infrastructureTruth,
  allowedFailureActions,
  metricDefinitions,
};
