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

function safeDescendant(root, relative) {
  if (typeof relative !== "string" || path.isAbsolute(relative)) return null;
  const resolvedRoot = path.resolve(root);
  const resolved = path.resolve(root, relative);
  return resolved.startsWith(`${resolvedRoot}${path.sep}`) ? resolved : null;
}

function validHash(value) {
  return typeof value === "string" && /^[0-9a-f]{64}$/.test(value);
}

function validReceiptDocument(document, prepared, expectedIdentity) {
  const receipt = document?.receipt;
  return Number.isInteger(document?.schema_version)
    && validHash(document?.receipt_sha256)
    && receipt && Number.isInteger(receipt.schema_version)
    && receipt.identity === expectedIdentity
    && validHash(receipt.artifact_sha256)
    && Number.isSafeInteger(receipt.bytes)
    && Number.isSafeInteger(receipt.rows)
    && Array.isArray(receipt.parquet_schema)
    && typeof receipt.algorithm_version === "string"
    && (!prepared || (
      prepared.receipt_identity === receipt.identity
      && prepared.receipt_sha256 === document.receipt_sha256
      && prepared.sha256 === receipt.artifact_sha256
      && prepared.bytes === receipt.bytes
      && prepared.rows === receipt.rows
    ));
}

function schemaIdentity(fields) {
  return Array.isArray(fields)
    ? fields.map((field) => `${field?.name || "?"}:${field?.data_type || "?"}`).join("|")
    : "";
}

function metricArtifact(ready, metric) {
  return (ready?.artifacts || []).find((artifact) => artifact?.path === `${ready.wiki}/${metric}.parquet`) || null;
}

function loadReady(outputDir, reference, wiki) {
  const candidateDir = safeDescendant(outputDir, reference?.candidate_relative);
  if (!candidateDir) return {candidateDir: null, ready: null};
  const ready = readJson(path.join(candidateDir, "ready.json"));
  if (ready?.wiki !== wiki || ready?.snapshot !== reference.snapshot || ready?.run_id !== reference.run_id) {
    return {candidateDir, ready: null};
  }
  return {candidateDir, ready};
}

function loadReceiptEvidence({artifactPath, prepared, expectedIdentity, definition, scrub}) {
  const document = readJson(`${artifactPath}.receipt.json`);
  let artifact = null;
  try { artifact = fs.statSync(artifactPath); } catch { artifact = null; }
  const valid = validReceiptDocument(document, prepared, expectedIdentity)
    && artifact?.isFile() && artifact.size === document.receipt.bytes;
  if (!valid) return {available: Boolean(document), valid: false};
  const receipt = document.receipt;
  const expectedSchema = definition?.schema || [];
  const schemaMatches = expectedSchema.length === 0
    || schemaIdentity(receipt.parquet_schema) === schemaIdentity(expectedSchema);
  const algorithmMatches = receipt.algorithm_version === definition.algorithm_version;
  const scrubEntry = scrub?.artifacts?.get(expectedIdentity) || null;
  const scrubMatches = Boolean(scrubEntry)
    && scrubEntry.receipt_sha256 === document.receipt_sha256
    && scrubEntry.artifact_sha256 === receipt.artifact_sha256;
  return {
    available: true,
    valid: true,
    schema: receipt.parquet_schema,
    schemaMatches,
    algorithmVersion: receipt.algorithm_version,
    algorithmMatches,
    rows: receipt.rows,
    bytes: receipt.bytes,
    minimumDate: receipt.minimum_date ?? null,
    maximumDate: receipt.maximum_date ?? null,
    totals: receipt.conservation_totals || {},
    artifactSha256: receipt.artifact_sha256,
    receiptSha256: document.receipt_sha256,
    orderingContract: receipt.ordering_contract || null,
    inputFingerprint: receipt.input_fingerprint || null,
    scrubbedAtUnix: scrubMatches ? scrub.scrubbedAtUnix : null,
    scrubAgeSeconds: scrubMatches ? scrub.ageSeconds : null,
  };
}

function loadScrub(outputDir, status, nowUnix) {
  if (status?.state !== "succeeded" || typeof status.run_id !== "string") return null;
  const report = readJson(path.join(outputDir, "_scrubs", `${status.run_id}.json`));
  if (report?.schema_version !== 2 || !Number.isSafeInteger(report.scrubbed_at_unix)
      || !Array.isArray(report.artifacts)) return null;
  const artifacts = new Map(report.artifacts
    .filter((artifact) => typeof artifact?.path === "string")
    .map((artifact) => [artifact.path, artifact]));
  return {
    scrubbedAtUnix: report.scrubbed_at_unix,
    ageSeconds: Math.max(0, nowUnix - report.scrubbed_at_unix),
    artifacts,
  };
}

function monthDistance(previous, candidate) {
  const parse = (value) => /^\d{4}-\d{2}$/.test(value || "")
    ? Number(value.slice(0, 4)) * 12 + Number(value.slice(5, 7))
    : null;
  const start = parse(previous);
  const end = parse(candidate);
  return start == null || end == null ? 1 : Math.max(1, end - start);
}

function signalPolicy(policy, signal) {
  return {...(policy?.default || {}), ...(policy?.signals?.[signal] || {})};
}

function compareNumber({signal, label, candidate, published, policy, months = 1, scope = null}) {
  if (!Number.isFinite(candidate) || !Number.isFinite(published) || published <= 0) return null;
  const rules = signalPolicy(policy, signal);
  if (published < Number(rules.minimum_baseline || 0)) return null;
  const delta = candidate - published;
  const fraction = delta / published;
  const increaseWarning = Number(rules.increase_warning_fraction_per_month ?? 0.25) * months;
  const increaseCritical = Number(rules.increase_critical_fraction_per_month ?? 1) * months;
  const decreaseWarning = Number(rules.decrease_warning_fraction ?? 0.01);
  const decreaseCritical = Number(rules.decrease_critical_fraction ?? 0.1);
  let severity = null;
  if (fraction <= -decreaseCritical || fraction >= increaseCritical) severity = "critical";
  else if (fraction <= -decreaseWarning || fraction >= increaseWarning) severity = "warning";
  if (!severity) return null;
  const direction = delta < 0 ? "fell" : "rose";
  return {
    code: `unexpected_${signal}_change`,
    severity,
    signal,
    scope,
    previous: published,
    candidate,
    delta,
    fraction,
    message: `${label} ${direction} ${Math.abs(fraction * 100).toFixed(1)}% (${published.toLocaleString()} → ${candidate.toLocaleString()}).`,
  };
}

function compareMetric(metric, policy, months) {
  const anomalies = [];
  const {candidate, published} = metric;
  if (!candidate?.valid || !published?.valid) return {changed: null, rows: null, totals: {}, anomalies};
  const algorithmComparable = candidate.algorithmVersion === published.algorithmVersion;
  if (algorithmComparable) {
    const rowAnomaly = compareNumber({
      signal: "rows", label: `${metric.id} rows`, candidate: candidate.rows,
      published: published.rows, policy, months, scope: metric.id,
    });
    if (rowAnomaly) anomalies.push(rowAnomaly);
  }
  const totals = {};
  for (const key of [...new Set([...Object.keys(candidate.totals), ...Object.keys(published.totals)])].sort()) {
    const current = Number(candidate.totals[key]);
    const previous = Number(published.totals[key]);
    const anomaly = algorithmComparable ? compareNumber({
      signal: "conservation_total", label: `${metric.id} ${key}`,
      candidate: current, published: previous, policy, months, scope: metric.id,
    }) : null;
    totals[key] = {
      candidate: Number.isFinite(current) ? current : null,
      published: Number.isFinite(previous) ? previous : null,
      delta: Number.isFinite(current) && Number.isFinite(previous) ? current - previous : null,
    };
    if (anomaly) anomalies.push(anomaly);
  }
  return {
    changed: candidate.artifactSha256 !== published.artifactSha256,
    algorithmComparable,
    rows: {candidate: candidate.rows, published: published.rows, delta: candidate.rows - published.rows},
    totals,
    anomalies,
  };
}

function patrolGenerationSignals(dataDir, wiki, snapshot) {
  if (!snapshot) return null;
  const root = path.join(dataDir, "patrol", wiki, "generations", snapshot);
  let directories;
  try {
    directories = fs.readdirSync(root, {withFileTypes: true}).filter((entry) => entry.isDirectory());
  } catch {
    return null;
  }
  const manifests = directories.map((entry) => readJson(path.join(root, entry.name, "generation.json")))
    .filter((manifest) => manifest?.wiki === wiki && manifest?.snapshot === snapshot && manifest?.stats);
  if (manifests.length !== 1) return null;
  const generation = manifests[0];
  return {
    patrol_events: generation.stats.patrol_events ?? null,
    rights_events: generation.stats.rights_events ?? null,
    account_creation_events: generation.stats.account_creation_events ?? null,
    local_account_block_events: generation.stats.local_account_block_events ?? null,
    indefinitely_blocked_accounts: generation.blocked_accounts?.rows ?? null,
  };
}

function readySignals(ready, generationSignals) {
  const recorded = ready?.quality_signals || {};
  return {
    editor_period_observations: recorded.editor_period_observations ?? null,
    patrol_events: recorded.patrol_events ?? generationSignals?.patrol_events ?? null,
    rights_events: recorded.rights_events ?? generationSignals?.rights_events ?? null,
    account_creation_events: recorded.account_creation_events ?? generationSignals?.account_creation_events ?? null,
    local_account_block_events: recorded.local_account_block_events ?? generationSignals?.local_account_block_events ?? null,
    indefinitely_blocked_accounts: recorded.indefinitely_blocked_accounts ?? generationSignals?.indefinitely_blocked_accounts ?? null,
  };
}

function publishedSignals(gate, wiki, publishedReady) {
  const recorded = gate?.wiki_proofs?.[wiki]?.quality_signals || publishedReady?.quality_signals || {};
  const patrol = gate?.patrol_sources?.[wiki] || {};
  const generation = patrol.generation || {};
  return {
    editor_period_observations: recorded.editor_period_observations ?? null,
    patrol_events: recorded.patrol_events ?? patrol.patrol_events ?? null,
    rights_events: recorded.rights_events ?? patrol.rights_events ?? null,
    account_creation_events: recorded.account_creation_events ?? patrol.account_creation_events ?? generation.account_creation_events ?? null,
    local_account_block_events: recorded.local_account_block_events ?? generation.local_account_block_events ?? null,
    indefinitely_blocked_accounts: recorded.indefinitely_blocked_accounts ?? generation.indefinitely_blocked_accounts ?? null,
  };
}

function buildWikiQuality({wiki, definitions, dataDir, outputDir, gate, index, scrubStatus, policy, nowUnix}) {
  const newest = loadReady(outputDir, index?.newest_valid_ready, wiki);
  const active = loadReady(outputDir, index?.active_published, wiki);
  const scrub = loadScrub(outputDir, scrubStatus, nowUnix);
  const publishedProof = gate?.wiki_proofs?.[wiki] || null;
  const metrics = definitions.map((definition) => {
    const candidatePrepared = metricArtifact(newest.ready, definition.id);
    const candidate = candidatePrepared && newest.candidateDir ? loadReceiptEvidence({
      artifactPath: path.join(newest.candidateDir, candidatePrepared.path),
      prepared: candidatePrepared,
      expectedIdentity: `${wiki}/${definition.id}.parquet`,
      definition,
      scrub: null,
    }) : null;
    const publishedPrepared = publishedProof?.artifacts?.[definition.id] || metricArtifact(active.ready, definition.id);
    const published = publishedPrepared ? loadReceiptEvidence({
      artifactPath: path.join(outputDir, wiki, `${definition.id}.parquet`),
      prepared: publishedPrepared,
      expectedIdentity: `${wiki}/${definition.id}.parquet`,
      definition,
      scrub,
    }) : null;
    const evidenceIssues = [];
    for (const [scope, evidence] of [["candidate", candidate], ["published", published]]) {
      if (evidence && !evidence.valid) evidenceIssues.push({
        code: `${scope}_metric_receipt_invalid`, severity: "critical", signal: "receipt",
        scope: definition.id, message: `${definition.id} ${scope} receipt is missing, malformed, or disagrees with its ready/publication proof.`,
      });
      if (evidence?.valid && !evidence.schemaMatches) evidenceIssues.push({
        code: `${scope}_metric_schema_mismatch`, severity: "critical", signal: "schema",
        scope: definition.id, message: `${definition.id} ${scope} schema disagrees with the metric registry.`,
      });
      if (evidence?.valid && !evidence.algorithmMatches) evidenceIssues.push({
        code: `${scope}_metric_algorithm_mismatch`, severity: "critical", signal: "algorithm",
        scope: definition.id, message: `${definition.id} ${scope} algorithm is not the registry version.`,
      });
    }
    const comparison = compareMetric({id: definition.id, candidate, published}, policy, monthDistance(
      index?.active_published?.snapshot, index?.newest_valid_ready?.snapshot,
    ));
    const anomalies = [...evidenceIssues, ...comparison.anomalies];
    return {
      id: definition.id,
      family: definition.family,
      expectedAlgorithmVersion: definition.algorithm_version,
      expectedSchema: definition.schema || [],
      candidate,
      published,
      comparison,
      anomalies,
      status: anomalies.some((item) => item.severity === "critical") ? "critical"
        : anomalies.length ? "warning" : candidate || published ? "healthy" : "unavailable",
    };
  });
  const candidateSignals = readySignals(newest.ready, patrolGenerationSignals(
    dataDir, wiki, index?.newest_valid_ready?.snapshot,
  ));
  const priorSignals = publishedSignals(gate, wiki, active.ready);
  const months = monthDistance(index?.active_published?.snapshot, index?.newest_valid_ready?.snapshot);
  const signalComparisons = Object.keys(policy?.signals || {}).map((signal) => {
    const candidate = candidateSignals[signal];
    const published = priorSignals[signal];
    return {
      signal,
      label: policy.signals[signal].label || signal.replaceAll("_", " "),
      candidate,
      published,
      anomaly: compareNumber({
        signal, label: policy.signals[signal].label || signal,
        candidate, published, policy, months, scope: wiki,
      }),
    };
  });
  const anomalies = [
    ...metrics.flatMap((metric) => metric.anomalies.map((anomaly) => ({...anomaly, metric: metric.id}))),
    ...signalComparisons.flatMap((entry) => entry.anomaly ? [entry.anomaly] : []),
  ];
  const scrubStale = scrub && scrub.ageSeconds > Number(policy.scrub_max_age_days || 14) * 86400;
  if (scrubStale) anomalies.push({
    code: "artifact_scrub_stale", severity: "warning", signal: "scrub", scope: wiki,
    message: `Published artifact scrub is ${Math.floor(scrub.ageSeconds / 86400)} days old.`,
  });
  return {
    policyValid: policy?.schema_version === 1,
    candidateSnapshot: index?.newest_valid_ready?.snapshot || null,
    publishedSnapshot: gate?.selected_snapshot_versions?.[wiki] || index?.active_published?.snapshot || null,
    scrub: scrub ? {scrubbedAtUnix: scrub.scrubbedAtUnix, ageSeconds: scrub.ageSeconds, stale: scrubStale} : null,
    metrics,
    signals: signalComparisons,
    anomalies,
    summary: {
      healthy: metrics.filter((metric) => metric.status === "healthy").length,
      warning: metrics.filter((metric) => metric.status === "warning").length,
      critical: metrics.filter((metric) => metric.status === "critical").length,
      unavailable: metrics.filter((metric) => metric.status === "unavailable").length,
      anomalyCount: anomalies.length,
    },
  };
}

module.exports = {
  buildWikiQuality,
  compareNumber,
  loadReceiptEvidence,
  monthDistance,
  safeDescendant,
};
