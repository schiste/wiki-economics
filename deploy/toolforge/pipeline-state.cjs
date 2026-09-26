#!/usr/bin/env node

/*
 * Durable coordinator for the low-memory Toolforge pipeline.
 *
 * The Jobs Framework starts each stage in a fresh pod, so the filesystem is
 * the only shared coordination channel. This file is intentionally tiny and
 * dependency-free: state is written to a temporary sibling and atomically
 * renamed, and every transition authenticates the stage/run identity.
 */

const fs = require("node:fs");
const path = require("node:path");

const SCHEMA_VERSION = 1;
const STAGES = ["ingest", "metrics", "lifecycle", "page-week", "patrol", "publish"];
const PARALLEL_COMPUTE_STAGES = new Set(["metrics", "lifecycle", "page-week", "patrol"]);
const PREREQUISITES = {
  ingest: [],
  metrics: ["ingest"],
  lifecycle: ["ingest"],
  "page-week": ["ingest"],
  patrol: ["ingest"],
  publish: ["metrics", "lifecycle", "page-week", "patrol"],
};
const STATE_LOCK_STALE_MS = 120_000;
const STATE_LOCK_WAIT_MS = 30_000;
const LOCK_WAIT_CELL = new Int32Array(new SharedArrayBuffer(4));

function fail(message) {
  throw new Error(message);
}

function parseArgs(argv) {
  const result = {_: []};
  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    if (!token.startsWith("--")) {
      result._.push(token);
      continue;
    }
    const name = token.slice(2);
    const value = argv[index + 1];
    if (value === undefined || value.startsWith("--")) fail(`missing value for --${name}`);
    result[name] = value;
    index += 1;
  }
  return result;
}

function required(args, name) {
  if (!args[name]) fail(`missing --${name}`);
  return args[name];
}

function validStage(stage) {
  if (!STAGES.includes(stage)) fail(`unknown pipeline stage ${JSON.stringify(stage)}`);
}

function validSnapshot(snapshot) {
  if (!/^\d{4}-\d{2}$/.test(snapshot)) fail(`invalid pipeline snapshot ${JSON.stringify(snapshot)}`);
}

function parseWikis(value) {
  let wikis;
  try {
    wikis = JSON.parse(value);
  } catch (error) {
    fail(`invalid --wikis-json: ${error.message}`);
  }
  if (!Array.isArray(wikis) || wikis.length === 0 || wikis.some((wiki) => typeof wiki !== "string" || !wiki)) {
    fail("--wikis-json must be a non-empty JSON string array");
  }
  return [...wikis].sort();
}

function now() {
  return new Date().toISOString();
}

function activeStages(state) {
  if (Array.isArray(state.active_stages)) return [...new Set(state.active_stages)];
  return state.current_stage ? [state.current_stage] : [];
}

function setActiveStages(state, stages) {
  state.active_stages = [...new Set(stages)];
  state.current_stage = state.active_stages[0] || null;
}

function recoverStaleStages(state, staleAfterSecs) {
  if (!Number.isFinite(staleAfterSecs) || staleAfterSecs <= 0) return false;
  const active = activeStages(state);
  const remaining = [];
  let recovered = false;
  for (const stage of active) {
    const entry = state.stages[stage];
    const started = Date.parse(entry?.heartbeat_at || entry?.started_at || "");
    if (!Number.isFinite(started) || Date.now() - started <= staleAfterSecs * 1000) {
      remaining.push(stage);
      continue;
    }
    state.stages[stage] = {
      ...entry,
      status: "failed",
      failed_at: now(),
      error: `stage lease expired after ${Math.floor(staleAfterSecs)} seconds`,
    };
    recovered = true;
  }
  if (recovered) {
    setActiveStages(state, remaining);
    state.state = remaining.length > 0 ? "running" : "failed";
    state.updated_at = now();
  }
  return recovered;
}

function statePath(args) {
  return path.resolve(required(args, "state"));
}

function readState(file) {
  if (!fs.existsSync(file)) return null;
  let state;
  try {
    state = JSON.parse(fs.readFileSync(file, "utf8"));
  } catch (error) {
    fail(`invalid pipeline state ${file}: ${error.message}`);
  }
  if (!state || state.schema_version !== SCHEMA_VERSION || !state.pipeline_id || !state.snapshot || !Array.isArray(state.wikis)) {
    fail(`pipeline state is not schema ${SCHEMA_VERSION}: ${file}`);
  }
  if (!Array.isArray(state.active_stages)) {
    state.active_stages = state.current_stage ? [state.current_stage] : [];
  }
  validSnapshot(state.snapshot);
  for (const stage of STAGES) {
    const entry = state.stages?.[stage];
    if (entry && !["pending", "running", "succeeded", "failed"].includes(entry.status)) {
      fail(`invalid ${stage} state ${JSON.stringify(entry.status)}`);
    }
  }
  for (const stage of activeStages(state)) {
    validStage(stage);
  }
  setActiveStages(state, activeStages(state));
  return state;
}

function writeState(file, state) {
  fs.mkdirSync(path.dirname(file), {recursive: true});
  const temporary = `${file}.tmp.${process.pid}.${Date.now()}.${Math.random().toString(16).slice(2)}`;
  fs.writeFileSync(temporary, `${JSON.stringify(state, null, 2)}\n`, {mode: 0o600});
  fs.renameSync(temporary, file);
}

function blankStages() {
  return Object.fromEntries(STAGES.map((stage) => [stage, {status: "pending"}]));
}

function parallelStageCapacity() {
  const capacityPath = path.resolve(__dirname, "../../config/toolforge-capacity.json");
  let capacity;
  try {
    capacity = JSON.parse(fs.readFileSync(capacityPath, "utf8"));
  } catch (error) {
    fail(`cannot read Toolforge capacity policy ${capacityPath}: ${error.message}`);
  }
  const reserveClasses = capacity.worker_reserve_resource_classes;
  if (!Array.isArray(reserveClasses)) fail("Toolforge capacity policy has no worker reserve classes");
  const reservedMemory = reserveClasses.reduce((total, name) => {
    const value = capacity.resource_requests?.[name];
    if (!Number.isSafeInteger(value) || value < 0) fail(`invalid memory reserve for ${name}`);
    return total + value;
  }, 0);
  const reservedCpu = reserveClasses.reduce((total, name) => {
    const value = capacity.resource_cpu_requests_millicores?.[name];
    if (!Number.isSafeInteger(value) || value < 0) fail(`invalid CPU reserve for ${name}`);
    return total + value;
  }, 0);
  const availableMemory = capacity.namespace_memory_limit_bytes
    - capacity.resident_service_memory_bytes - reservedMemory;
  const availableCpu = capacity.namespace_cpu_limit_millicores
    - capacity.resident_service_cpu_millicores - reservedCpu;
  const perJobMemory = capacity.resource_requests?.pipeline;
  const perJobCpu = capacity.resource_cpu_requests_millicores?.pipeline;
  if (![availableMemory, availableCpu, perJobMemory, perJobCpu].every(Number.isSafeInteger)
    || availableMemory <= 0 || availableCpu <= 0 || perJobMemory <= 0 || perJobCpu <= 0) {
    fail("Toolforge capacity policy cannot admit a compute stage");
  }
  const memoryBound = Math.floor(availableMemory / perJobMemory);
  const cpuBound = Math.floor(availableCpu / perJobCpu);
  const limit = Math.min(PARALLEL_COMPUTE_STAGES.size, memoryBound, cpuBound);
  if (limit < 1) fail("Toolforge capacity reserves leave no room for a pipeline compute stage");
  return limit;
}

function withStateLock(file, operation) {
  const lock = `${file}.lock`;
  const deadline = Date.now() + STATE_LOCK_WAIT_MS;
  const token = `${process.pid}-${Date.now()}-${Math.random().toString(16).slice(2)}`;
  fs.mkdirSync(path.dirname(file), {recursive: true});
  while (true) {
    try {
      fs.mkdirSync(lock, {mode: 0o700});
      try {
        fs.writeFileSync(path.join(lock, "owner.json"), JSON.stringify({token, created_at: now()}), {mode: 0o600});
      } catch (error) {
        fs.rmSync(lock, {recursive: true, force: true});
        throw error;
      }
      break;
    } catch (error) {
      if (error.code !== "EEXIST") throw error;
    }
    try {
      const lockStat = fs.statSync(lock);
      if (Date.now() - lockStat.mtimeMs > STATE_LOCK_STALE_MS) {
        const stale = `${lock}.stale.${token}`;
        try {
          fs.renameSync(lock, stale);
          fs.rmSync(stale, {recursive: true, force: true});
          continue;
        } catch {}
      }
    } catch (error) {
      if (error.code === "ENOENT") continue;
      throw error;
    }
    if (Date.now() >= deadline) fail(`timed out acquiring pipeline state lock ${lock}`);
    Atomics.wait(LOCK_WAIT_CELL, 0, 0, 20);
  }
  try {
    return operation();
  } finally {
    try {
      const owner = JSON.parse(fs.readFileSync(path.join(lock, "owner.json"), "utf8"));
      if (owner.token === token) fs.rmSync(lock, {recursive: true, force: true});
    } catch {}
  }
}

function begin(args) {
  const file = statePath(args);
  const stage = required(args, "stage");
  const runId = required(args, "run-id");
  validStage(stage);
  const wikis = parseWikis(required(args, "wikis-json"));
  const suppliedSnapshot = args.snapshot;
  if (suppliedSnapshot) validSnapshot(suppliedSnapshot);
  let state = readState(file);

  if (!state) {
    if (stage !== "ingest" || !suppliedSnapshot) {
      fail("the ingest stage must create pipeline state with --snapshot first");
    }
    state = {
      schema_version: SCHEMA_VERSION,
      pipeline_id: runId,
      snapshot: suppliedSnapshot,
      wikis,
      state: "running",
      current_stage: stage,
      active_stages: [stage],
      stages: blankStages(),
      created_at: now(),
      updated_at: now(),
    };
  } else {
    if (JSON.stringify([...state.wikis].sort()) !== JSON.stringify(wikis)) {
      fail(`pipeline wiki set does not match state: expected ${state.wikis.join(",")}, got ${wikis.join(",")}`);
    }
    if (suppliedSnapshot && suppliedSnapshot !== state.snapshot) {
      fail(`pipeline snapshot does not match state: expected ${state.snapshot}, got ${suppliedSnapshot}`);
    }
    if (activeStages(state).length > 0) {
      const staleAfterSecs = Number(args["stale-after-secs"] || 0);
      if (recoverStaleStages(state, staleAfterSecs)) {
        writeState(file, state);
      }
    }
    if (stage === "ingest") {
      // A new ingest starts a new generation and invalidates all downstream
      // receipts. It is allowed only while no other pod owns a stage.
      if (activeStages(state).length > 0) {
        fail(`pipeline stages ${activeStages(state).join(", ")} are already running; refusing ingest reset`);
      }
      if (!suppliedSnapshot) fail("retrying ingest requires --snapshot");
      state.pipeline_id = runId;
      state.snapshot = suppliedSnapshot;
      state.stages = blankStages();
    } else {
      const active = activeStages(state);
      if (active.includes(stage)) fail(`pipeline stage ${stage} is already running; refusing duplicate work`);
      for (const prerequisite of PREREQUISITES[stage]) {
        if (state.stages[prerequisite]?.status !== "succeeded") {
          fail(`pipeline stage ${stage} requires completed ${prerequisite}`);
        }
      }
      if (stage === "publish" && active.length > 0) {
        fail(`publish requires all compute stages to stop; still running: ${active.join(", ")}`);
      }
      if (PARALLEL_COMPUTE_STAGES.has(stage)) {
        if (active.some((activeStage) => !PARALLEL_COMPUTE_STAGES.has(activeStage))) {
          fail(`pipeline stage ${stage} cannot overlap ${active.join(", ")}`);
        }
        if (active.length > 0 && Object.values(state.stages).some((candidate) => candidate.status === "failed")) {
          fail("pipeline has a failed compute stage; wait for active stages to finish before retrying");
        }
        const limit = parallelStageCapacity();
        if (active.length >= limit) {
          fail(`pipeline compute-stage concurrency limit ${limit} is full: ${active.join(", ")}`);
        }
      } else if (active.length > 0) {
        fail(`pipeline stage ${stage} cannot overlap ${active.join(", ")}`);
      }
    }
    state.state = "running";
    state.updated_at = now();
  }

  setActiveStages(state, [...activeStages(state), stage]);
  const previousStage = state.stages[stage];
  state.stages[stage] = {
    status: "running",
    run_id: runId,
    started_at: now(),
    ...(previousStage?.status === "failed"
      ? {
          previous_failure: {
            run_id: previousStage.run_id,
            failed_at: previousStage.failed_at,
            error: previousStage.error,
          },
        }
      : {}),
  };
  state.updated_at = now();
  writeState(file, state);
  process.stdout.write(`${JSON.stringify({pipeline_id: state.pipeline_id, snapshot: state.snapshot, stage})}\n`);
}

function heartbeat(args) {
  const file = statePath(args);
  const stage = required(args, "stage");
  const runId = required(args, "run-id");
  validStage(stage);
  const state = readState(file);
  if (!state) {
    if (stage === "ingest") {
      process.stdout.write(`${JSON.stringify({stage, active: false})}\n`);
      return;
    }
    fail("pipeline state does not exist");
  }
  const entry = state.stages[stage];
  if (entry?.run_id === runId && entry.status === "failed") {
    fail(`cannot heartbeat ${stage}: its pipeline lease was revoked`);
  }
  if (entry?.run_id === runId && entry.status === "succeeded") {
    process.stdout.write(`${JSON.stringify({stage, active: false})}\n`);
    return;
  }
  if (!activeStages(state).includes(stage) || entry?.status !== "running" || entry.run_id !== runId) {
    if (stage === "ingest" && !activeStages(state).includes(stage)) {
      process.stdout.write(`${JSON.stringify({stage, active: false})}\n`);
      return;
    }
    fail(`cannot heartbeat ${stage}: it is not owned by ${runId}`);
  }
  entry.heartbeat_at = now();
  state.updated_at = now();
  writeState(file, state);
  process.stdout.write(`${JSON.stringify({pipeline_id: state.pipeline_id, stage, active: true})}\n`);
}

function complete(args) {
  const file = statePath(args);
  const stage = required(args, "stage");
  const runId = required(args, "run-id");
  validStage(stage);
  const state = readState(file);
  if (!state) fail("pipeline state does not exist");
  const entry = state.stages[stage];
  if (!activeStages(state).includes(stage) || entry?.status !== "running" || entry.run_id !== runId) {
    fail(`cannot complete ${stage}: it is not owned by ${runId}`);
  }
  state.stages[stage] = {...entry, status: "succeeded", completed_at: now()};
  const active = activeStages(state).filter((activeStage) => activeStage !== stage);
  setActiveStages(state, active);
  state.state = active.length > 0
    ? "running"
    : stage === "publish"
      ? "succeeded"
      : Object.values(state.stages).some((candidate) => candidate.status === "failed")
        ? "failed"
        : "running";
  state.updated_at = now();
  writeState(file, state);
  process.stdout.write(`${JSON.stringify({pipeline_id: state.pipeline_id, snapshot: state.snapshot, stage, state: state.state})}\n`);
}

function failStage(args) {
  const file = statePath(args);
  const stage = required(args, "stage");
  const runId = required(args, "run-id");
  validStage(stage);
  const state = readState(file);
  if (!state) fail("pipeline state does not exist");
  const entry = state.stages[stage];
  if (!activeStages(state).includes(stage) || entry?.status !== "running" || entry.run_id !== runId) {
    fail(`cannot fail ${stage}: it is not owned by ${runId}`);
  }
  state.stages[stage] = {
    ...entry,
    status: "failed",
    failed_at: now(),
    error: String(args.error || "stage failed").slice(0, 2000),
  };
  const active = activeStages(state).filter((activeStage) => activeStage !== stage);
  setActiveStages(state, active);
  state.state = active.length > 0 ? "running" : "failed";
  state.updated_at = now();
  writeState(file, state);
  process.stdout.write(`${JSON.stringify({pipeline_id: state.pipeline_id, snapshot: state.snapshot, stage, state: state.state})}\n`);
}

function show(args) {
  const state = readState(statePath(args));
  if (!state) fail("pipeline state does not exist");
  process.stdout.write(`${JSON.stringify(state, null, 2)}\n`);
}

function main() {
  const args = parseArgs(process.argv.slice(2));
  const command = args._[0];
  if (["begin", "heartbeat", "complete", "fail"].includes(command)) {
    return withStateLock(statePath(args), () => {
      if (command === "begin") return begin(args);
      if (command === "heartbeat") return heartbeat(args);
      if (command === "complete") return complete(args);
      return failStage(args);
    });
  }
  if (command === "show") return show(args);
  fail("usage: pipeline-state.cjs <begin|heartbeat|complete|fail|show> --state PATH ...");
}

try {
  main();
} catch (error) {
  process.stderr.write(`pipeline-state: ${error.message}\n`);
  process.exitCode = 1;
}

module.exports = {PREREQUISITES, SCHEMA_VERSION, STAGES};
