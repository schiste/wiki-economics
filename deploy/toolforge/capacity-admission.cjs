#!/usr/bin/env node
"use strict";

const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const {spawn} = require("node:child_process");

const ROOT = path.resolve(__dirname, "../..");
const DEFAULT_CONFIG = path.join(ROOT, "config", "toolforge-capacity.json");
const LEASE_STALE_MS = 15 * 60 * 1_000;
const LOCK_STALE_MS = 30 * 1_000;

function readJson(file) {
  try { return JSON.parse(fs.readFileSync(file, "utf8")); } catch { return null; }
}

function atomicWriteJson(file, value) {
  fs.mkdirSync(path.dirname(file), {recursive: true});
  const temporary = path.join(path.dirname(file), `.${path.basename(file)}.${process.pid}.tmp`);
  const fd = fs.openSync(temporary, "wx", 0o600);
  try {
    fs.writeFileSync(fd, `${JSON.stringify(value, null, 2)}\n`, "utf8");
    fs.fsyncSync(fd);
    fs.closeSync(fd);
    fs.renameSync(temporary, file);
  } catch (error) {
    try { fs.closeSync(fd); } catch {}
    try { fs.unlinkSync(temporary); } catch {}
    throw error;
  }
}

function withGuard(root, action) {
  const guard = path.join(root, ".lock");
  fs.mkdirSync(root, {recursive: true});
  for (let attempt = 0; attempt < 100; attempt += 1) {
    try {
      fs.mkdirSync(guard);
      try { return action(); } finally { fs.rmdirSync(guard); }
    } catch (error) {
      if (error.code !== "EEXIST") throw error;
      try {
        if (Date.now() - fs.statSync(guard).mtimeMs > LOCK_STALE_MS) fs.rmdirSync(guard);
      } catch {}
      Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 50);
    }
  }
  throw new Error("capacity admission lock remained busy");
}

function validatedConfig(file) {
  const config = readJson(file);
  if (config?.schema_version !== 1) throw new Error(`Invalid capacity configuration ${file}`);
  for (const field of [
    "namespace_memory_limit_bytes",
    "namespace_cpu_limit_millicores",
    "per_job_memory_limit_bytes",
    "per_job_cpu_limit_millicores",
    "resident_service_memory_bytes",
    "resident_service_cpu_millicores",
    "minimum_schedulable_job_bytes",
    "minimum_schedulable_job_millicores",
  ]) {
    if (!Number.isSafeInteger(config[field]) || config[field] < 0) throw new Error(`Invalid capacity field ${field}`);
  }
  if (!Array.isArray(config.worker_reserve_resource_classes)) {
    throw new Error("Invalid capacity field worker_reserve_resource_classes");
  }
  const memoryClasses = Object.keys(config.resource_requests || {}).sort();
  const cpuClasses = Object.keys(config.resource_cpu_requests_millicores || {}).sort();
  if (JSON.stringify(memoryClasses) !== JSON.stringify(cpuClasses)) {
    throw new Error("Memory and CPU resource request classes must match");
  }
  for (const resourceClass of memoryClasses) requestFor(config, resourceClass);
  if (new Set(config.worker_reserve_resource_classes).size !== config.worker_reserve_resource_classes.length) {
    throw new Error("Capacity field worker_reserve_resource_classes contains duplicates");
  }
  for (const resourceClass of config.worker_reserve_resource_classes) {
    for (const [field, requests] of [
      ["resource_requests", config.resource_requests],
      ["resource_cpu_requests_millicores", config.resource_cpu_requests_millicores],
    ]) {
      if (!Number.isSafeInteger(requests?.[resourceClass]) || requests[resourceClass] <= 0) {
        throw new Error(`No ${field} capacity request is configured for reserved class ${resourceClass}`);
      }
    }
  }
  return config;
}

function requestFor(config, resourceClass) {
  const requestedBytes = config.resource_requests?.[resourceClass];
  const requestedMillicores = config.resource_cpu_requests_millicores?.[resourceClass];
  if (!Number.isSafeInteger(requestedBytes) || requestedBytes <= 0) {
    throw new Error(`No capacity request is configured for ${resourceClass}`);
  }
  if (!Number.isSafeInteger(requestedMillicores) || requestedMillicores <= 0) {
    throw new Error(`No CPU capacity request is configured for ${resourceClass}`);
  }
  if (requestedBytes > config.per_job_memory_limit_bytes) {
    throw new Error(`Memory request for ${resourceClass} exceeds the Toolforge per-job limit`);
  }
  if (requestedMillicores > config.per_job_cpu_limit_millicores) {
    throw new Error(`CPU request for ${resourceClass} exceeds the Toolforge per-job limit`);
  }
  return {requestedBytes, requestedMillicores};
}

function reservedCapacity(config) {
  return config.worker_reserve_resource_classes.reduce((total, resourceClass) => {
    const request = requestFor(config, resourceClass);
    return {
      bytes: total.bytes + request.requestedBytes,
      millicores: total.millicores + request.requestedMillicores,
    };
  }, {bytes: 0, millicores: 0});
}

function activeLeases(root, now = Date.now()) {
  const leases = [];
  for (const name of fs.readdirSync(root, {withFileTypes: true})) {
    if (!name.isFile() || !name.name.endsWith(".json")) continue;
    const file = path.join(root, name.name);
    const lease = readJson(file);
    const heartbeat = Date.parse(lease?.heartbeatAt || 0);
    if (lease?.schemaVersion !== 1 || !Number.isFinite(heartbeat) || now - heartbeat > LEASE_STALE_MS) {
      fs.renameSync(file, path.join(root, `.stale-${Date.now()}-${name.name}`));
      continue;
    }
    leases.push({...lease, file});
  }
  return leases;
}

function acquire({root, configFile, resourceClass, identity, now = Date.now()}) {
  const config = validatedConfig(configFile);
  const {requestedBytes, requestedMillicores} = requestFor(config, resourceClass);
  return withGuard(root, () => {
    const leases = activeLeases(root, now);
    const reserved = reservedCapacity(config);
    const workerBudgetBytes = config.namespace_memory_limit_bytes
      - config.resident_service_memory_bytes
      - reserved.bytes;
    const workerBudgetMillicores = config.namespace_cpu_limit_millicores
      - config.resident_service_cpu_millicores
      - reserved.millicores;
    const admittedBytes = leases.reduce((total, lease) => {
      if (!Number.isSafeInteger(lease.requestedBytes) || lease.requestedBytes <= 0) {
        throw new Error(`Active capacity lease ${lease.identity || lease.file} has an invalid memory request`);
      }
      return total + lease.requestedBytes;
    }, 0);
    const admittedMillicores = leases.reduce((total, lease) => {
      const fallback = config.resource_cpu_requests_millicores?.[lease.resourceClass] || 0;
      const requested = Number.isSafeInteger(lease.requestedMillicores) ? lease.requestedMillicores : fallback;
      if (requested <= 0) {
        throw new Error(`Active capacity lease ${lease.identity || lease.file} has an invalid CPU request`);
      }
      return total + requested;
    }, 0);
    const limitingResources = [];
    if (workerBudgetBytes < 0 || admittedBytes + requestedBytes > workerBudgetBytes) limitingResources.push("memory");
    if (workerBudgetMillicores < 0 || admittedMillicores + requestedMillicores > workerBudgetMillicores) {
      limitingResources.push("cpu");
    }
    if (limitingResources.length > 0) {
      return {
        admitted: false,
        requestedBytes,
        requestedMillicores,
        admittedBytes,
        admittedMillicores,
        workerBudgetBytes,
        workerBudgetMillicores,
        limitingResources,
        active: leases.length,
      };
    }
    const lease = {
      schemaVersion: 1,
      identity,
      resourceClass,
      requestedBytes,
      requestedMillicores,
      acquiredAt: new Date(now).toISOString(),
      heartbeatAt: new Date(now).toISOString(),
      pid: process.pid,
      pod: os.hostname(),
    };
    const file = path.join(root, `${identity}.json`);
    atomicWriteJson(file, lease);
    return {
      admitted: true,
      requestedBytes,
      requestedMillicores,
      admittedBytes,
      admittedMillicores,
      workerBudgetBytes,
      workerBudgetMillicores,
      limitingResources,
      active: leases.length,
      file,
      lease,
    };
  });
}

async function run() {
  const separator = process.argv.indexOf("--");
  const classIndex = process.argv.indexOf("--resource-class");
  if (separator < 0 || classIndex < 0 || classIndex + 1 >= separator || separator + 1 >= process.argv.length) {
    throw new Error("Usage: capacity-admission.cjs --resource-class <small|medium_large> -- <command> [args...]");
  }
  const resourceClass = process.argv[classIndex + 1];
  if (!new Set(["small", "medium_large"]).has(resourceClass)) throw new Error(`Unsupported capacity class ${resourceClass}`);
  const outputDir = path.resolve(process.env.WIKI_ECON_OUTPUT_DIR || path.join(ROOT, "output"));
  const leaseRoot = path.resolve(process.env.WIKI_ECON_CAPACITY_LEASE_DIR || path.join(outputDir, "_capacity-admission"));
  const configFile = path.resolve(process.env.WIKI_ECON_CAPACITY_CONFIG || DEFAULT_CONFIG);
  const identity = `${os.hostname().replace(/[^A-Za-z0-9_.-]/g, "-")}-${process.pid}`;
  const admission = acquire({root: leaseRoot, configFile, resourceClass, identity});
  if (!admission.admitted) {
    console.log(JSON.stringify({type: "capacity_admission", ...admission}));
    process.exitCode = 75;
    return;
  }
  console.log(JSON.stringify({type: "capacity_admission", ...admission, file: undefined, lease: undefined}));
  const [program, ...args] = process.argv.slice(separator + 1);
  const child = spawn(program, args, {
    stdio: "inherit",
    env: {...process.env, WIKI_ECON_CAPACITY_ADMITTED: "1"},
  });
  const heartbeat = setInterval(() => {
    const current = readJson(admission.file);
    if (current?.identity === identity) {
      atomicWriteJson(admission.file, {...current, heartbeatAt: new Date().toISOString()});
    }
  }, 30_000);
  const forward = (signal) => child.kill(signal);
  process.on("SIGTERM", forward);
  process.on("SIGINT", forward);
  const result = await new Promise((resolve) => {
    child.once("error", (error) => resolve({code: 1, error}));
    child.once("close", (code) => resolve({code: code ?? 1, error: null}));
  });
  clearInterval(heartbeat);
  try {
    const current = readJson(admission.file);
    if (current?.identity === identity) fs.unlinkSync(admission.file);
  } catch {}
  if (result.error) console.error(result.error.message);
  process.exitCode = result.code;
}

if (require.main === module) {
  run().catch((error) => {
    console.error(error.stack || error.message);
    process.exitCode = 1;
  });
}

module.exports = {acquire, activeLeases, requestFor, reservedCapacity, validatedConfig};
