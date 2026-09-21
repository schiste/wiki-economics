#!/usr/bin/env node
"use strict";

/*
 * Immutable, per-stage qualification evidence for the isolated Toolforge
 * pipeline.  The Rust stage receipts remain the semantic authority; this
 * document joins those receipts to process/resource evidence without reading
 * Parquet a second time.  In particular, artifact rows/bytes/checksums come
 * from the already-authenticated receipt identities.
 */

const fs = require("node:fs");
const path = require("node:path");
const crypto = require("node:crypto");

const SCHEMA_VERSION = 1;
const MAX_SAMPLES = 512;
const MAX_LOG_LINES = 100;
const STAGES = ["ingest", "metrics", "lifecycle", "page-week", "patrol", "publish"];

function required(name) {
  const value = process.env[name];
  if (!value) throw new Error(`${name} is required`);
  return value;
}

function readJson(file) {
  try {
    return JSON.parse(fs.readFileSync(file, "utf8"));
  } catch {
    return null;
  }
}

function readText(file) {
  try {
    return fs.readFileSync(file, "utf8").trim();
  } catch {
    return "";
  }
}

function atomicWrite(file, value) {
  fs.mkdirSync(path.dirname(file), {recursive: true});
  const temporary = `${file}.tmp.${process.pid}.${Date.now()}`;
  fs.writeFileSync(temporary, `${JSON.stringify(value, null, 2)}\n`, {mode: 0o600});
  fs.renameSync(temporary, file);
}

function sha256Buffer(value) {
  return crypto.createHash("sha256").update(value).digest("hex");
}

function sha256File(file) {
  try {
    const hash = crypto.createHash("sha256");
    hash.update(fs.readFileSync(file));
    return hash.digest("hex");
  } catch {
    return null;
  }
}

function relative(file) {
  const roots = [process.env.WIKI_ECON_DATA_DIR, process.env.WIKI_ECON_OUTPUT_DIR]
    .filter(Boolean).map((root) => path.resolve(root));
  for (const root of roots) {
    const rel = path.relative(root, file);
    if (rel && !rel.startsWith("..") && !path.isAbsolute(rel)) return rel;
  }
  return file;
}

function safeInteger(value) {
  return Number.isSafeInteger(value) && value >= 0 ? value : null;
}

function counter(file) {
  const value = Number(readText(file));
  return safeInteger(value);
}

function cpuSnapshot() {
  const root = process.env.WIKI_ECON_CGROUP_ROOT || "/sys/fs/cgroup";
  const values = {};
  for (const line of readText(path.join(root, "cpu.stat")).split(/\r?\n/)) {
    const [name, raw, ...extra] = line.trim().split(/\s+/);
    const value = Number(raw);
    if (name && extra.length === 0 && Number.isSafeInteger(value) && value >= 0) values[name] = value;
  }
  return {
    usage_usec: values.usage_usec ?? null,
    user_usec: values.user_usec ?? null,
    system_usec: values.system_usec ?? null,
    nr_periods: values.nr_periods ?? null,
    nr_throttled: values.nr_throttled ?? null,
    throttled_usec: values.throttled_usec ?? null,
  };
}

function cgroupSnapshot() {
  const root = process.env.WIKI_ECON_CGROUP_ROOT || "/sys/fs/cgroup";
  return {
    memory_current_bytes: counter(path.join(root, "memory.current")),
    memory_peak_bytes: counter(path.join(root, "memory.peak")),
    memory_limit_bytes: counter(path.join(root, "memory.max")),
    cpu: cpuSnapshot(),
  };
}

function statfs(directory) {
  try {
    const stats = fs.statfsSync(directory, {bigint: true});
    const total = stats.blocks * stats.bsize;
    const free = stats.bavail * stats.bsize;
    const totalBytes = Number(total);
    const freeBytes = Number(free);
    if (!Number.isSafeInteger(totalBytes) || !Number.isSafeInteger(freeBytes)) return null;
    return {
      path: directory,
      total_bytes: totalBytes,
      free_bytes: freeBytes,
      used_bytes: Math.max(0, totalBytes - freeBytes),
    };
  } catch {
    return {path: directory, total_bytes: null, free_bytes: null, used_bytes: null};
  }
}

function storageSample() {
  const data = process.env.WIKI_ECON_DATA_DIR || "/data/project/wiki-economics/data";
  const output = process.env.WIKI_ECON_OUTPUT_DIR || "/data/project/wiki-economics/output";
  const scratch = process.env.WIKI_ECON_SCRATCH_DIR || output;
  return {
    sampled_at: new Date().toISOString(),
    persistent: {data: statfs(data), output: statfs(output)},
    scratch: statfs(scratch),
  };
}

function storageHighWater(samples) {
  const roots = ["persistent.data", "persistent.output", "scratch"];
  const result = {};
  for (const name of roots) {
    const values = samples.map((sample) => name.split(".").reduce((value, key) => value?.[key], sample))
      .filter((value) => value && value.used_bytes != null);
    if (!values.length) {
      result[name.replace(".", "_")] = {initial: null, current: null, high_water: null, minimum_free: null};
      continue;
    }
    result[name.replace(".", "_")] = {
      initial: values[0].used_bytes,
      current: values.at(-1).used_bytes,
      high_water: Math.max(...values.map((value) => value.used_bytes)),
      minimum_free: Math.min(...values.map((value) => value.free_bytes)),
      total: values.at(-1).total_bytes,
      path: values.at(-1).path,
    };
  }
  const persistent = [result.persistent_data, result.persistent_output];
  const knownPersistent = persistent.filter((value) => value.initial != null);
  // The two roots normally share one NFS filesystem. Keep the logical sum
  // for capacity accounting, but expose the filesystem high-water mark
  // separately so operators do not mistake a double-counted root sum for
  // physical usage.
  result.persistent_logical_combined = knownPersistent.length === persistent.length ? {
    initial: persistent.reduce((sum, value) => sum + value.initial, 0),
    current: persistent.reduce((sum, value) => sum + value.current, 0),
    high_water: persistent.reduce((sum, value) => sum + value.high_water, 0),
  } : null;
  result.persistent_filesystem = result.persistent_data?.initial != null
    ? {...result.persistent_data}
    : result.persistent_output?.initial != null ? {...result.persistent_output} : null;
  return result;
}

function parseWikis() {
  try {
    const value = JSON.parse(process.env.WIKI_ECON_RUN_WIKIS_JSON || "[]");
    return Array.isArray(value) ? value.filter((wiki) => typeof wiki === "string") : [];
  } catch {
    return [];
  }
}

function stage() {
  const value = required("WIKI_ECON_REFRESH_STAGE");
  if (!STAGES.includes(value)) throw new Error(`unsupported qualification stage ${value}`);
  return value;
}

function snapshot() {
  const file = process.env.WIKI_ECON_RUN_SNAPSHOT_FILE;
  return file ? readText(file) || null : null;
}

function receiptRoot() {
  return process.env.WIKI_ECON_QUALIFICATION_RECEIPT_DIR
    || path.join(process.env.WIKI_ECON_OUTPUT_DIR || "/data/project/wiki-economics/output", "_qualification");
}

function safePart(value) {
  return String(value || "unknown").replace(/[^A-Za-z0-9._-]/g, "_");
}

function receiptRunId() {
  return safePart(process.env.WIKI_ECON_RUN_ID || required("WIKI_ECON_RUN_ID"));
}

function receiptFilename(suffix) {
  return `${safePart(stage())}.${receiptRunId()}${suffix}`;
}

function startPath() {
  return path.join(receiptRoot(), safePart(process.env.WIKI_ECON_PIPELINE_ID || required("WIKI_ECON_RUN_ID")), receiptFilename(".start.json"));
}

function finalPath() {
  return path.join(receiptRoot(), safePart(process.env.WIKI_ECON_PIPELINE_ID || required("WIKI_ECON_RUN_ID")), receiptFilename(".json"));
}

function start() {
  const started = Date.now();
  const value = {
    schema_version: SCHEMA_VERSION,
    kind: "wiki-economics-qualification-stage-start",
    stage: stage(),
    run_id: required("WIKI_ECON_RUN_ID"),
    pipeline_id: process.env.WIKI_ECON_PIPELINE_ID || process.env.WIKI_ECON_RUN_ID,
    wikis: parseWikis(),
    snapshot: snapshot(),
    started_at: new Date(started).toISOString(),
    start_epoch_ms: started,
    cgroup: cgroupSnapshot(),
    storage_samples: [storageSample()],
    cgroup_samples: [cgroupSnapshot()],
  };
  atomicWrite(startPath(), value);
  process.stdout.write(`${JSON.stringify({stage: value.stage, path: startPath()})}\n`);
}

function sample() {
  const file = startPath();
  const value = readJson(file);
  if (!value) return;
  value.storage_samples = [...(value.storage_samples || []), storageSample()].slice(-MAX_SAMPLES);
  value.cgroup_samples = [...(value.cgroup_samples || []), cgroupSnapshot()].slice(-MAX_SAMPLES);
  atomicWrite(file, value);
}

function existing(file) {
  try { return fs.statSync(file).isFile() ? file : null; } catch { return null; }
}

function stageReceiptCandidates(stageName, wikis, data, output, selectedSnapshot) {
  const paths = [];
  if (stageName === "ingest") {
    for (const wiki of wikis) {
      if (selectedSnapshot) {
        for (const name of ["fetch", "ingest"]) paths.push(path.join(data, "stages", wiki, selectedSnapshot, `${name}.json`));
      }
    }
  } else if (stageName === "metrics") {
    for (const wiki of wikis) for (const family of ["monthly", "activity_tiers"])
      paths.push(path.join(output, "_stages", "compute", family, `${wiki}.json`));
  } else if (stageName === "lifecycle") {
    for (const wiki of wikis) paths.push(path.join(output, "_stages", "compute", "lifecycle", `${wiki}.json`));
  } else if (stageName === "page-week") {
    for (const wiki of wikis) paths.push(path.join(output, "_stages", "compute", "page_week", `${wiki}.json`));
  } else if (stageName === "patrol") {
    for (const wiki of wikis) paths.push(path.join(output, "_stages", "patrol_compute", `${wiki}.json`));
  } else if (stageName === "publish") {
    for (const name of ["merge", "dashboard-defaults", "site"]) paths.push(path.join(output, "_stages", `${name}.json`));
  }
  return paths.map(existing).filter(Boolean);
}

function artifactIdentity(value, source) {
  if (!value || typeof value !== "object" || typeof value.identity !== "string") return null;
  return {
    identity: value.identity,
    source_receipt: relative(source),
    bytes: safeInteger(value.bytes),
    rows: safeInteger(value.rows),
    sha256: typeof value.sha256 === "string" ? value.sha256 : null,
    artifact_receipt_sha256: typeof value.artifact_receipt_sha256 === "string" ? value.artifact_receipt_sha256 : null,
    minimum_date: value.minimum_date ?? null,
    maximum_date: value.maximum_date ?? null,
    conservation_totals: value.conservation_totals || {},
  };
}

function stageArtifacts(stageName, wikis, data, output, selectedSnapshot) {
  const inputs = new Map();
  const outputs = new Map();
  const stageReceipts = [];
  for (const file of stageReceiptCandidates(stageName, wikis, data, output, selectedSnapshot)) {
    const value = readJson(file);
    if (!value) continue;
    stageReceipts.push({
      path: relative(file),
      sha256: sha256File(file),
      fingerprint: value.fingerprint || null,
      stage: value.stage || null,
      algorithm_version: value.algorithm_version || null,
      computation_version: value.computation_version || null,
    });
    for (const item of value.inputs || []) {
      const identity = artifactIdentity(item, file);
      if (identity) inputs.set(identity.identity, identity);
    }
    for (const item of value.outputs || []) {
      const identity = artifactIdentity(item, file);
      if (identity) outputs.set(identity.identity, identity);
    }
  }

  // Snapshot plans and remote inventories are small, durable source evidence;
  // include their own checksums even when the fetch receipt predates them.
  if (stageName === "ingest" && selectedSnapshot) {
    for (const wiki of wikis) {
      for (const name of ["source-plan.json", "remote-inventory.json", "workload-profile.json"]) {
        const file = existing(path.join(data, "snapshots", wiki, selectedSnapshot, name));
        if (!file) continue;
        const stat = fs.statSync(file);
        inputs.set(`snapshot/${wiki}/${selectedSnapshot}/${name}`, {
          identity: `snapshot/${wiki}/${selectedSnapshot}/${name}`,
          source_receipt: relative(file), bytes: stat.size, rows: null,
          sha256: sha256File(file), artifact_receipt_sha256: null,
          minimum_date: null, maximum_date: null, conservation_totals: {},
        });
      }
    }
  }
  // The site receipt already authenticates the generated distribution, but a
  // qualification receipt should still make its served bytes directly
  // inspectable without reopening the Rust receipt. Include only regular
  // files and keep the inventory bounded by the site build's own output.
  if (stageName === "publish") {
    const siteDir = process.env.WIKI_ECON_SITE_DIST_DIR;
    if (siteDir && fs.existsSync(siteDir)) {
      const visit = (directory) => {
        let entries = [];
        try { entries = fs.readdirSync(directory, {withFileTypes: true}); } catch { return; }
        for (const entry of entries) {
          const file = path.join(directory, entry.name);
          if (entry.isDirectory()) visit(file);
          else if (entry.isFile()) {
            try {
              const stat = fs.statSync(file);
              const identity = `site-dist/${path.relative(siteDir, file)}`;
              outputs.set(identity, {
                identity,
                source_receipt: relative(file),
                bytes: safeInteger(stat.size),
                rows: null,
                sha256: sha256File(file),
                artifact_receipt_sha256: null,
                minimum_date: null,
                maximum_date: null,
                conservation_totals: {},
              });
            } catch { /* the site publisher may be replacing a file atomically */ }
          }
        }
      };
      visit(siteDir);
    }
  }
  return {inputs: [...inputs.values()].sort((a, b) => a.identity.localeCompare(b.identity)), outputs: [...outputs.values()].sort((a, b) => a.identity.localeCompare(b.identity)), stage_receipts: stageReceipts};
}

function summary(artifacts) {
  const knownBytes = artifacts.filter((item) => item.bytes != null);
  const knownRows = artifacts.filter((item) => item.rows != null);
  return {
    artifacts: artifacts.length,
    rows: knownRows.reduce((sum, item) => sum + item.rows, 0),
    rows_known: artifacts.length > 0 && knownRows.length === artifacts.length,
    bytes: knownBytes.reduce((sum, item) => sum + item.bytes, 0),
    bytes_known: artifacts.length > 0 && knownBytes.length === artifacts.length,
  };
}

function percentile(values, fraction) {
  if (!values.length) return null;
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.min(sorted.length - 1, Math.floor((sorted.length - 1) * fraction))];
}

function findBucketValues(value, found = []) {
  if (!value || typeof value !== "object") return found;
  for (const [key, child] of Object.entries(value)) {
    if (/(?:bucket_staged_rows|bucket_rows|bucket_sizes)/i.test(key) && Array.isArray(child)) {
      const values = child.filter((item) => Number.isSafeInteger(item) && item >= 0);
      if (values.length) found.push(values);
    }
    findBucketValues(child, found);
  }
  return found;
}

function bucketDistribution(stageName, wikis, data, output, selectedSnapshot, stageReceipts) {
  const candidates = [];
  if (selectedSnapshot) for (const wiki of wikis) candidates.push(path.join(data, "snapshots", wiki, selectedSnapshot, "workload-profile.json"));
  for (const receipt of stageReceipts) candidates.push(path.join(output, receipt.path));
  let observed = [];
  for (const file of candidates.map(existing).filter(Boolean)) observed = observed.concat(findBucketValues(readJson(file)));
  const values = observed.sort((a, b) => b.length - a.length)[0] || [];
  const configured = {};
  for (const wiki of wikis) {
    const profile = selectedSnapshot && readJson(path.join(data, "snapshots", wiki, selectedSnapshot, "workload-profile.json"));
    if (profile?.parameters) configured[wiki] = {
      primary_buckets: profile.parameters.primary_buckets ?? null,
      secondary_buckets: profile.parameters.secondary_buckets ?? null,
      logical_buckets: (profile.parameters.primary_buckets && profile.parameters.secondary_buckets)
        ? profile.parameters.primary_buckets * profile.parameters.secondary_buckets : null,
    };
  }
  return {
    available: values.length > 0,
    distribution_status: values.length ? "observed" : "not_emitted",
    configured,
    expected_count: Object.values(configured).reduce((count, value) =>
      count + (Number.isSafeInteger(value.logical_buckets) ? value.logical_buckets : 0), 0) || null,
    observed_count: values.length,
    observed_nonempty: values.filter((value) => value > 0).length,
    observed_zero: values.filter((value) => value === 0).length,
    rows: values,
    min_rows: values.length ? Math.min(...values) : null,
    max_rows: values.length ? Math.max(...values) : null,
    p50_rows: percentile(values, 0.5),
    p95_rows: percentile(values, 0.95),
    source: values.length ? "weekly-aggregation-report" : "workload-profile-only",
    stage: stageName,
    warning: values.length ? null : "bucket-size distribution was not emitted by this stage",
  };
}

function eventAndLogFindings() {
  const events = readText(process.env.WIKI_ECON_RUN_EVENTS_FILE).split(/\r?\n/).filter(Boolean)
    .map((line) => { try { return JSON.parse(line); } catch { return null; } }).filter(Boolean);
  const logFile = process.env.WIKI_ECON_RUN_LOG_FILE;
  let logLines = [];
  if (logFile) {
    const text = readText(logFile);
    logLines = text.split(/\r?\n/).filter(Boolean).slice(-20_000);
  }
  const matching = (pattern) => logLines.filter((line) => pattern.test(line)).slice(-MAX_LOG_LINES);
  const warnings = matching(/\bwarn(?:ing)?\b|!!!/i);
  const retries = matching(/\bretr(?:y|ies|ied)\b|\battempt\b/i);
  const recovery = matching(/\brecover(?:y|ed)?\b|\bstale\b|\bquarantine\b/i);
  const eventText = (event) => JSON.stringify(event);
  const warningEvents = events.filter((event) => /\bwarn(?:ing)?\b/i.test(eventText(event)));
  const retryEvents = events.filter((event) => /\bretr(?:y|ies|ied)\b|\battempt\b/i.test(eventText(event)));
  const recoveryEvents = events.filter((event) => /\brecover(?:y|ed)?\b|\bstale\b|\bquarantine\b/i.test(eventText(event)));
  const failedEvents = events.filter((event) => event.event === "failed");
  return {
    warnings: [...warnings, ...warningEvents].slice(-MAX_LOG_LINES),
    retries: [...retries, ...retryEvents].slice(-MAX_LOG_LINES),
    recovery_events: [...recovery, ...recoveryEvents].slice(-MAX_LOG_LINES),
    event_count: events.length,
    failed_events: failedEvents,
    events: events.slice(-MAX_LOG_LINES),
  };
}

function cpuDelta(start, end) {
  const keys = ["usage_usec", "user_usec", "system_usec", "nr_periods", "nr_throttled", "throttled_usec"];
  return Object.fromEntries(keys.map((key) => [key, start?.[key] != null && end?.[key] != null
    ? Math.max(0, end[key] - start[key]) : null]));
}

function contract() {
  const number = (name) => {
    const value = Number(process.env[name]);
    return Number.isFinite(value) ? value : null;
  };
  return {
    memory_limit_bytes: number("WIKI_ECON_MEMORY_CEILING_BYTES"),
    requested_cpu_cores: number("WIKI_ECON_REQUESTED_CPU_CORES") || number("WIKI_ECON_CPU_LIMIT_CORES"),
    source_workers: number("WIKI_ECON_SOURCE_WORKERS"),
    thread_limit: number("WIKI_ECON_THREAD_LIMIT"),
    active_parquet_writers: number("WIKI_ECON_MAX_ACTIVE_PARQUET_WRITERS"),
    source_window_size: number("WIKI_ECON_SOURCE_WINDOW_SIZE"),
    job_identity: process.env.WIKI_ECON_JOB_IDENTITY || process.env.TOOLFORGE_JOB_NAME || null,
  };
}

function finish(exitCode) {
  const file = startPath();
  const started = readJson(file);
  if (!started) throw new Error(`qualification start receipt is missing: ${file}`);
  const finishedAt = Date.now();
  const endCgroup = cgroupSnapshot();
  const samples = [...(started.storage_samples || []), storageSample()];
  const data = process.env.WIKI_ECON_DATA_DIR || "/data/project/wiki-economics/data";
  const output = process.env.WIKI_ECON_OUTPUT_DIR || "/data/project/wiki-economics/output";
  const wikis = started.wikis || parseWikis();
  const selectedSnapshot = started.snapshot || snapshot();
  const artifacts = stageArtifacts(stage(), wikis, data, output, selectedSnapshot);
  const findings = eventAndLogFindings();
  const bucket = bucketDistribution(stage(), wikis, data, output, selectedSnapshot, artifacts.stage_receipts);
  const sampledCgroups = started.cgroup_samples || [];
  const memoryPeaks = [started.cgroup?.memory_peak_bytes, endCgroup.memory_peak_bytes,
    ...sampledCgroups.map((sample) => sample.memory_peak_bytes)].filter((value) => value != null);
  const document = {
    schema_version: SCHEMA_VERSION,
    kind: "wiki-economics-qualification-stage-receipt",
    stage: stage(),
    status: Number(exitCode) === 0 ? "succeeded" : "failed",
    exit_code: Number(exitCode),
    run_id: started.run_id,
    pipeline_id: started.pipeline_id,
    wikis,
    snapshot: selectedSnapshot,
    started_at: started.started_at,
    finished_at: new Date(finishedAt).toISOString(),
    wall_time_ms: Math.max(0, finishedAt - started.start_epoch_ms),
    input: summary(artifacts.inputs),
    output: summary(artifacts.outputs),
    inputs: artifacts.inputs,
    outputs: artifacts.outputs,
    fingerprints: {
      stage_receipts: artifacts.stage_receipts,
      receipt_seed_sha256: sha256Buffer(JSON.stringify({inputs: artifacts.inputs, outputs: artifacts.outputs, stage_receipts: artifacts.stage_receipts})),
    },
    resources: {
      cpu: cpuDelta(started.cgroup?.cpu, endCgroup.cpu),
      cgroup: {
        current_bytes: endCgroup.memory_current_bytes,
        peak_bytes: Math.max(...memoryPeaks, 0),
        limit_bytes: endCgroup.memory_limit_bytes,
      },
      storage: storageHighWater(samples),
      samples: samples.length,
    },
    bucket_size_distribution: bucket,
    contract: contract(),
    warnings: findings.warnings,
    retries: findings.retries,
    recovery_events: findings.recovery_events,
    observability: {
      event_count: findings.event_count,
      failed_events: findings.failed_events,
      recent_events: findings.events,
    },
    error: Number(exitCode) === 0 ? null : (process.env.WIKI_ECON_RUN_ERROR || `stage exited ${exitCode}`),
    receipt_sha256: null,
  };
  document.receipt_sha256 = sha256Buffer(JSON.stringify(document));
  atomicWrite(finalPath(), document);
  try { fs.unlinkSync(file); } catch {}
  process.stdout.write(`${JSON.stringify({stage: document.stage, status: document.status, path: finalPath(), receipt_sha256: document.receipt_sha256})}\n`);
}

const command = process.argv[2];
try {
  if (command === "start") start();
  else if (command === "sample") sample();
  else if (command === "finish") finish(process.argv[3] ?? 0);
  else throw new Error("usage: qualification-receipt.cjs start|sample|finish [exit-code]");
} catch (error) {
  process.stderr.write(`qualification-receipt: ${error.message}\n`);
  process.exitCode = 1;
}

module.exports = {bucketDistribution, cpuDelta, storageHighWater};
