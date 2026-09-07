#!/usr/bin/env node
"use strict";

const crypto = require("node:crypto");
const fs = require("node:fs");
const path = require("node:path");
const {validateWikiLifecycle} = require("../scripts/wiki-lifecycle.cjs");

const AUDIT_SCHEMA_VERSION = 1;
const RESOURCE_CLASSES = new Set(["small", "medium_large", "isolated"]);
const REFRESH_MODES = new Set(["manual", "scheduled"]);
const YEAR_MONTH = /^\d{4}-(0[1-9]|1[0-2])$/;

function canonicalJson(value) {
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`;
  if (value && typeof value === "object") {
    return `{${Object.keys(value).sort().map((key) => `${JSON.stringify(key)}:${canonicalJson(value[key])}`).join(",")}}`;
  }
  return JSON.stringify(value);
}

function sha256(value) {
  return crypto.createHash("sha256").update(value).digest("hex");
}

function registryRevision(registry) {
  return sha256(canonicalJson(registry));
}

function readLifecycle(file) {
  let registry;
  try {
    registry = JSON.parse(fs.readFileSync(file, "utf8"));
  } catch (error) {
    throw new Error(`Unable to read wiki lifecycle registry ${file}: ${error.message}`);
  }
  return validateWikiLifecycle(registry, file);
}

function syncDirectory(directory) {
  let fd;
  try {
    fd = fs.openSync(directory, "r");
    fs.fsyncSync(fd);
  } catch (error) {
    if (!new Set(["EINVAL", "ENOTSUP", "EISDIR"]).has(error.code)) throw error;
  } finally {
    if (fd != null) fs.closeSync(fd);
  }
}

function atomicWriteJson(file, value) {
  fs.mkdirSync(path.dirname(file), {recursive: true});
  const temporary = path.join(
    path.dirname(file),
    `.${path.basename(file)}.${process.pid}.${crypto.randomBytes(6).toString("hex")}.tmp`,
  );
  const fd = fs.openSync(temporary, "wx", 0o600);
  try {
    fs.writeFileSync(fd, `${JSON.stringify(value, null, 2)}\n`, "utf8");
    fs.fsyncSync(fd);
    fs.closeSync(fd);
    fs.renameSync(temporary, file);
    syncDirectory(path.dirname(file));
  } catch (error) {
    try { fs.closeSync(fd); } catch {}
    try { fs.unlinkSync(temporary); } catch {}
    throw error;
  }
}

function immutableAuditEvent(auditDir, event) {
  fs.mkdirSync(auditDir, {recursive: true});
  const payload = {
    schemaVersion: AUDIT_SCHEMA_VERSION,
    ...event,
  };
  const eventSha256 = sha256(canonicalJson(payload));
  const document = {...payload, eventSha256};
  const safeRequest = String(event.requestId || "event").replace(/[^a-zA-Z0-9_.-]/g, "_");
  const safePhase = String(event.phase || "recorded").replace(/[^a-zA-Z0-9_.-]/g, "_");
  const file = path.join(auditDir, `${safeRequest}-${safePhase}-${eventSha256}.json`);
  if (fs.existsSync(file)) return {file, document};

  const temporary = path.join(auditDir, `.${path.basename(file)}.${process.pid}.tmp`);
  const fd = fs.openSync(temporary, "wx", 0o600);
  try {
    fs.writeFileSync(fd, `${JSON.stringify(document, null, 2)}\n`, "utf8");
    fs.fsyncSync(fd);
    fs.closeSync(fd);
    // link(2) is an atomic no-clobber commit on the shared Toolforge NFS.
    fs.linkSync(temporary, file);
    fs.unlinkSync(temporary);
    syncDirectory(auditDir);
    return {file, document};
  } catch (error) {
    try { fs.closeSync(fd); } catch {}
    try { fs.unlinkSync(temporary); } catch {}
    if (error.code === "EEXIST" && fs.existsSync(file)) return {file, document};
    throw error;
  }
}

function readAuditTrail(auditDir, limit = 104) {
  let names = [];
  try {
    names = fs.readdirSync(auditDir).filter((name) => name.endsWith(".json")).sort().reverse();
  } catch (error) {
    if (error.code === "ENOENT") return {events: [], invalid: []};
    throw error;
  }
  const events = [];
  const invalid = [];
  for (const name of names) {
    const file = path.join(auditDir, name);
    try {
      const document = JSON.parse(fs.readFileSync(file, "utf8"));
      const {eventSha256, ...payload} = document;
      if (document.schemaVersion !== AUDIT_SCHEMA_VERSION
        || !/^[a-f0-9]{64}$/.test(eventSha256 || "")
        || sha256(canonicalJson(payload)) !== eventSha256
        || !name.endsWith(`-${eventSha256}.json`)) {
        throw new Error("audit hash or schema mismatch");
      }
      if (events.length < limit) events.push(document);
    } catch (error) {
      invalid.push({file: name, error: error.message});
    }
  }
  events.sort((left, right) => Date.parse(right.recordedAt || 0) - Date.parse(left.recordedAt || 0));
  return {events: events.slice(0, limit), invalid};
}

function qualificationCandidates(outputDir, limitPerWiki = 10) {
  const root = path.join(outputDir, "_qualifications");
  const byWiki = {};
  let wikis = [];
  try {
    wikis = fs.readdirSync(root).sort();
  } catch (error) {
    if (error.code === "ENOENT") return byWiki;
    throw error;
  }
  for (const wiki of wikis) {
    if (!/^[a-z0-9_]+wiki$/.test(wiki)) continue;
    const wikiRoot = path.join(root, wiki);
    let snapshots = [];
    try { snapshots = fs.readdirSync(wikiRoot).sort().reverse(); } catch { continue; }
    const entries = [];
    for (const snapshot of snapshots) {
      if (!YEAR_MONTH.test(snapshot)) continue;
      const snapshotRoot = path.join(wikiRoot, snapshot);
      let runs = [];
      try { runs = fs.readdirSync(snapshotRoot).sort().reverse(); } catch { continue; }
      for (const runId of runs) {
        const receiptPath = path.join(snapshotRoot, runId, "qualification.json");
        try {
          const bytes = fs.readFileSync(receiptPath);
          const receipt = JSON.parse(bytes.toString("utf8"));
          const valid = receipt.schema_version === 2
            && receipt.publication_eligible === false
            && receipt.wiki === wiki
            && receipt.snapshot === snapshot
            && receipt.run_id === runId
            && Array.isArray(receipt.artifacts)
            && receipt.artifacts.length > 0;
          entries.push({
            wiki,
            snapshot,
            runId,
            qualifiedAtUnix: receipt.qualified_at_unix ?? null,
            cutoffDate: receipt.cutoff_date ?? null,
            artifactCount: receipt.artifacts?.length ?? 0,
            artifactBytes: (receipt.artifacts || []).reduce((total, artifact) => total + Number(artifact?.bytes || 0), 0),
            artifactRows: (receipt.artifacts || []).reduce((total, artifact) => total + Number(artifact?.rows || 0), 0),
            metricIds: (receipt.artifacts || [])
              .map((artifact) => path.basename(String(artifact?.path || ""), ".parquet"))
              .filter(Boolean)
              .sort(),
            workloadProfile: receipt.workload_profile?.profile ?? null,
            resourceClass: receipt.workload_profile?.resource_class ?? null,
            receiptSha256: sha256(bytes),
            structurallyValid: valid,
            error: valid ? null : "qualification receipt identity or schema mismatch",
          });
        } catch (error) {
          if (error.code !== "ENOENT") entries.push({
            wiki,
            snapshot,
            runId,
            structurallyValid: false,
            error: error.message,
          });
        }
      }
    }
    entries.sort((left, right) => (right.snapshot || "").localeCompare(left.snapshot || "")
      || Number(right.qualifiedAtUnix || 0) - Number(left.qualifiedAtUnix || 0)
      || (right.runId || "").localeCompare(left.runId || ""));
    if (entries.length > 0) byWiki[wiki] = entries.slice(0, limitPerWiki);
  }
  return byWiki;
}

function defaultRetentionPolicy() {
  return {
    source_recoverability: "redownloadable",
    history_input: "purge_after_ready",
    patrol_source: "purge_after_ready",
    computed_rollback_generations: 1,
  };
}

function registrationLifecycle(mode, resourceClass, operator) {
  const provenance = `toolforge-admin:${operator || "local-operator"}`;
  const base = {provenance, retention: defaultRetentionPolicy()};
  if (mode === "qualification") {
    return {...base, publication: "hidden", refresh: "qualification", fleet_resource_class: resourceClass};
  }
  if (mode === "manual") {
    return {...base, publication: "published", refresh: "manual", fleet_resource_class: resourceClass};
  }
  if (mode === "scheduled") {
    return {
      ...base,
      publication: "published",
      refresh: "scheduled",
      freshness_sla_days: 10,
      fleet_resource_class: resourceClass,
    };
  }
  throw new Error(`Unsupported lifecycle mode ${mode}`);
}

function updateExplicitDatasetCoverage(registry, wiki, published) {
  for (const contract of Object.values(registry.publication_contract?.datasets || {})) {
    if (!Array.isArray(contract.wikis)) continue;
    const covered = new Set(contract.wikis);
    if (published) covered.add(wiki);
    else covered.delete(wiki);
    contract.wikis = [...covered].sort();
  }
}

function positiveSla(value, fallback = null) {
  if (value == null || value === "") return fallback;
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed <= 0 || parsed > 365) {
    throw new Error("Freshness SLA must be an integer from 1 to 365 days");
  }
  return parsed;
}

function validatedResourceClass(value, fallback = null) {
  const resourceClass = value || fallback;
  if (!RESOURCE_CLASSES.has(resourceClass)) {
    throw new Error("Resource class must be small, medium_large, or isolated");
  }
  return resourceClass;
}

function mutateRegistry(registry, mutation, operator) {
  const next = structuredClone(registry);
  const wiki = mutation.wiki;
  const current = next.wikis[wiki] || null;
  switch (mutation.action) {
    case "register": {
      if (current) throw new Error(`${wiki} is already registered`);
      const mode = mutation.mode || "qualification";
      if (!new Set(["qualification", "manual", "scheduled"]).has(mode)) {
        throw new Error("Lifecycle mode must be qualification, manual, or scheduled");
      }
      const resourceClass = validatedResourceClass(mutation.resourceClass, "medium_large");
      next.wikis[wiki] = registrationLifecycle(mode, resourceClass, operator);
      updateExplicitDatasetCoverage(next, wiki, mode !== "qualification");
      break;
    }
    case "pause": {
      if (!current || current.publication !== "published") {
        throw new Error(`${wiki} must be a published project before scheduling can be paused`);
      }
      if (!new Set(["scheduled", "manual"]).has(current.refresh)) {
        throw new Error(`${wiki} scheduling cannot be paused from ${current.refresh}`);
      }
      current.refresh = "paused";
      break;
    }
    case "resume": {
      if (!current || current.publication !== "published" || current.refresh !== "paused") {
        throw new Error(`${wiki} must be a paused published project before scheduling can resume`);
      }
      const refresh = mutation.refresh || "scheduled";
      if (!REFRESH_MODES.has(refresh)) throw new Error("Resume mode must be manual or scheduled");
      current.refresh = refresh;
      if (refresh === "scheduled") {
        current.freshness_sla_days = positiveSla(mutation.freshnessSlaDays, current.freshness_sla_days || 10);
      }
      break;
    }
    case "configure": {
      if (!current) throw new Error(`${wiki} is not registered`);
      if (mutation.resourceClass == null && mutation.freshnessSlaDays == null) {
        throw new Error("Lifecycle configuration requires a resource class or freshness SLA");
      }
      if (mutation.resourceClass != null) {
        current.fleet_resource_class = validatedResourceClass(mutation.resourceClass);
      }
      if (mutation.freshnessSlaDays != null) {
        current.freshness_sla_days = positiveSla(mutation.freshnessSlaDays);
      }
      break;
    }
    case "promote": {
      if (!current || current.publication !== "hidden" || current.refresh !== "qualification") {
        throw new Error(`${wiki} must be hidden/qualification before promotion`);
      }
      const refresh = mutation.refresh || "manual";
      if (!REFRESH_MODES.has(refresh)) throw new Error("Promotion mode must be manual or scheduled");
      current.publication = "published";
      current.refresh = refresh;
      current.provenance = `toolforge-admin:${operator || "local-operator"}`;
      current.fleet_resource_class = validatedResourceClass(
        mutation.resourceClass,
        current.fleet_resource_class || "medium_large",
      );
      delete current.imported_cutoff;
      if (refresh === "scheduled") {
        current.freshness_sla_days = positiveSla(mutation.freshnessSlaDays, current.freshness_sla_days || 10);
      }
      updateExplicitDatasetCoverage(next, wiki, true);
      break;
    }
    default:
      throw new Error(`Unsupported lifecycle mutation ${mutation.action}`);
  }
  validateWikiLifecycle(next, "updated wiki lifecycle registry");
  return next;
}

function applyLifecycleMutation({
  lifecyclePath,
  auditDir,
  mutation,
  operator,
  requestId,
  expectedRevision = null,
  recordedAt = new Date().toISOString(),
}) {
  if (!/^[a-z0-9_]+wiki$/.test(mutation?.wiki || "")) throw new Error("Lifecycle mutation requires a valid wiki");
  if (mutation.snapshot != null && !YEAR_MONTH.test(mutation.snapshot)) throw new Error("Lifecycle snapshot must use YYYY-MM");
  const before = readLifecycle(lifecyclePath);
  const beforeRevision = registryRevision(before);
  if (expectedRevision && expectedRevision !== beforeRevision) {
    throw new Error("Lifecycle registry changed since this page was loaded; refresh before applying the action");
  }
  const after = mutateRegistry(before, mutation, operator);
  const afterRevision = registryRevision(after);
  if (afterRevision === beforeRevision) throw new Error("Lifecycle mutation did not change the registry");
  const auditPayload = {
    requestId,
    action: mutation.action,
    wiki: mutation.wiki,
    operator,
    recordedAt,
    beforeRevision,
    afterRevision,
    before: before.wikis[mutation.wiki] || null,
    after: after.wikis[mutation.wiki] || null,
    parameters: Object.fromEntries(Object.entries(mutation).filter(([key]) => key !== "wiki" && key !== "action")),
  };
  // Commit intent first. If either the registry replacement or final audit
  // write is interrupted, the immutable prepared event contains both exact
  // revisions and is sufficient for deterministic operator reconciliation.
  immutableAuditEvent(auditDir, {...auditPayload, phase: "prepared"});
  atomicWriteJson(lifecyclePath, after);
  const audit = immutableAuditEvent(auditDir, {...auditPayload, phase: "applied"});
  return {registry: after, previous: before.wikis[mutation.wiki] || null, current: after.wikis[mutation.wiki], beforeRevision, afterRevision, audit: audit.document};
}

module.exports = {
  applyLifecycleMutation,
  atomicWriteJson,
  canonicalJson,
  immutableAuditEvent,
  mutateRegistry,
  qualificationCandidates,
  readAuditTrail,
  readLifecycle,
  registryRevision,
  updateExplicitDatasetCoverage,
};
