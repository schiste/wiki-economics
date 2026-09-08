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
  for (const field of ["namespace_memory_limit_bytes", "resident_service_memory_bytes"]) {
    if (!Number.isSafeInteger(config[field]) || config[field] < 0) throw new Error(`Invalid capacity field ${field}`);
  }
  return config;
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
  const requestedBytes = config.resource_requests?.[resourceClass];
  if (!Number.isSafeInteger(requestedBytes) || requestedBytes <= 0) {
    throw new Error(`No capacity request is configured for ${resourceClass}`);
  }
  return withGuard(root, () => {
    const leases = activeLeases(root, now);
    const workerBudgetBytes = config.namespace_memory_limit_bytes
      - config.resident_service_memory_bytes
      - config.resource_requests.admin_dispatcher;
    const admittedBytes = leases.reduce((total, lease) => total + lease.requestedBytes, 0);
    if (workerBudgetBytes < 0 || admittedBytes + requestedBytes > workerBudgetBytes) {
      return {admitted: false, requestedBytes, admittedBytes, workerBudgetBytes, active: leases.length};
    }
    const lease = {
      schemaVersion: 1,
      identity,
      resourceClass,
      requestedBytes,
      acquiredAt: new Date(now).toISOString(),
      heartbeatAt: new Date(now).toISOString(),
      pid: process.pid,
      pod: os.hostname(),
    };
    const file = path.join(root, `${identity}.json`);
    atomicWriteJson(file, lease);
    return {admitted: true, requestedBytes, admittedBytes, workerBudgetBytes, active: leases.length, file, lease};
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

module.exports = {acquire, activeLeases, validatedConfig};
