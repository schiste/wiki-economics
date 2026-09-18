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
const PREREQUISITES = {
  ingest: [],
  metrics: ["ingest"],
  lifecycle: ["metrics"],
  "page-week": ["lifecycle"],
  patrol: ["page-week"],
  publish: ["patrol"],
};

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

function recoverStaleStage(state, staleAfterSecs) {
  if (!state.current_stage || !Number.isFinite(staleAfterSecs) || staleAfterSecs <= 0) return false;
  const entry = state.stages[state.current_stage];
  const started = Date.parse(entry?.started_at || "");
  if (!Number.isFinite(started) || Date.now() - started <= staleAfterSecs * 1000) return false;
  state.stages[state.current_stage] = {
    ...entry,
    status: "failed",
    failed_at: now(),
    error: `stage lease expired after ${Math.floor(staleAfterSecs)} seconds`,
  };
  state.current_stage = null;
  state.state = "failed";
  state.updated_at = now();
  return true;
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
  validSnapshot(state.snapshot);
  for (const stage of STAGES) {
    const entry = state.stages?.[stage];
    if (entry && !["pending", "running", "succeeded", "failed"].includes(entry.status)) {
      fail(`invalid ${stage} state ${JSON.stringify(entry.status)}`);
    }
  }
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
    if (state.current_stage) {
      const staleAfterSecs = Number(args["stale-after-secs"] || 0);
      if (!recoverStaleStage(state, staleAfterSecs)) {
        fail(`pipeline stage ${state.current_stage} is already running; refusing overlap`);
      }
    }
    if (stage === "ingest") {
      // A new ingest starts a new generation and invalidates all downstream
      // receipts. It is allowed only while no other pod owns a stage.
      if (!suppliedSnapshot) fail("retrying ingest requires --snapshot");
      state.pipeline_id = runId;
      state.snapshot = suppliedSnapshot;
      state.stages = blankStages();
    } else {
      for (const prerequisite of PREREQUISITES[stage]) {
        if (state.stages[prerequisite]?.status !== "succeeded") {
          fail(`pipeline stage ${stage} requires completed ${prerequisite}`);
        }
      }
    }
    state.state = "running";
    state.current_stage = stage;
    state.updated_at = now();
  }

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

function complete(args) {
  const file = statePath(args);
  const stage = required(args, "stage");
  const runId = required(args, "run-id");
  validStage(stage);
  const state = readState(file);
  if (!state) fail("pipeline state does not exist");
  const entry = state.stages[stage];
  if (state.current_stage !== stage || entry?.status !== "running" || entry.run_id !== runId) {
    fail(`cannot complete ${stage}: it is not owned by ${runId}`);
  }
  state.stages[stage] = {...entry, status: "succeeded", completed_at: now()};
  state.current_stage = null;
  state.state = stage === "publish" ? "succeeded" : "running";
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
  if (state.current_stage !== stage || entry?.status !== "running" || entry.run_id !== runId) {
    fail(`cannot fail ${stage}: it is not owned by ${runId}`);
  }
  state.stages[stage] = {
    ...entry,
    status: "failed",
    failed_at: now(),
    error: String(args.error || "stage failed").slice(0, 2000),
  };
  state.current_stage = null;
  state.state = "failed";
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
  if (command === "begin") return begin(args);
  if (command === "complete") return complete(args);
  if (command === "fail") return failStage(args);
  if (command === "show") return show(args);
  fail("usage: pipeline-state.cjs <begin|complete|fail|show> --state PATH ...");
}

try {
  main();
} catch (error) {
  process.stderr.write(`pipeline-state: ${error.message}\n`);
  process.exitCode = 1;
}

module.exports = {PREREQUISITES, SCHEMA_VERSION, STAGES};
