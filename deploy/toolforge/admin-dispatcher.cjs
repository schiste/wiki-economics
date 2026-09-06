#!/usr/bin/env node
"use strict";

const fs = require("node:fs");
const path = require("node:path");
const {spawn} = require("node:child_process");
const {summarizeOperationLog} = require("../../site/admin-operation-status.cjs");
const {
  applyLifecycleMutation,
  immutableAuditEvent,
  registryRevision,
  readLifecycle,
} = require("../../site/admin-lifecycle.cjs");

const ROOT = path.resolve(__dirname, "../..");
const DATA_DIR = path.resolve(process.env.WIKI_ECON_DATA_DIR || path.join(ROOT, "data"));
const OUTPUT_DIR = path.resolve(process.env.WIKI_ECON_OUTPUT_DIR || path.join(ROOT, "output"));
const LIFECYCLE_PATH = path.resolve(
  process.env.WIKI_ECON_WIKI_LIFECYCLE_FILE || path.join(ROOT, "config", "wiki-lifecycle.json"),
);
const QUEUE_DIR = path.resolve(process.env.WIKI_ECON_FLEET_QUEUE_DIR || path.join(OUTPUT_DIR, "_fleet"));
const OPERATION_DIR = path.resolve(
  process.env.WIKI_ECON_ADMIN_OPERATION_DIR || path.join(OUTPUT_DIR, "_admin", "operations"),
);
const BIN = process.env.WIKI_ECON_BIN || path.join(ROOT, "target", "release", "wiki-econ");
const LIFECYCLE_AUDIT_DIR = path.join(OUTPUT_DIR, "_admin", "lifecycle-audit");
const LOG_TAIL_BYTES = 128 * 1024;
const STALE_OPERATION_MS = Number.parseInt(process.env.WIKI_ECON_ADMIN_OPERATION_STALE_SECS || "600", 10) * 1_000;
const configuredUpstreamRetrySecs = Number.parseInt(
  process.env.WIKI_ECON_ADMIN_UPSTREAM_RETRY_SECS || "21600",
  10,
);
const UPSTREAM_RETRY_MS = Number.isSafeInteger(configuredUpstreamRetrySecs)
  && configuredUpstreamRetrySecs >= 60
  ? configuredUpstreamRetrySecs * 1_000
  : 21_600_000;

function directories() {
  const result = {
    queued: path.join(OPERATION_DIR, "queued"),
    running: path.join(OPERATION_DIR, "running"),
    history: path.join(OPERATION_DIR, "history"),
    logs: path.join(OPERATION_DIR, "logs"),
  };
  for (const directory of Object.values(result)) fs.mkdirSync(directory, {recursive: true});
  return result;
}

function readJson(file) {
  try {
    return JSON.parse(fs.readFileSync(file, "utf8"));
  } catch {
    return null;
  }
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

function commandFor(request) {
  const common = ["--data-dir", DATA_DIR, "--output-dir", OUTPUT_DIR, "--run-id", request.runId];
  const wiki = request.wiki;
  const version = request.version ? ["--version", request.version] : [];
  switch (request.action) {
    case "run":
      return {program: BIN, args: [...common, "prepare-wiki", wiki, ...version, "--lifecycle", LIFECYCLE_PATH]};
    case "qualify":
      return {program: BIN, args: [...common, "qualify-wiki", wiki, ...version, "--lifecycle", LIFECYCLE_PATH]};
    case "promote-qualification":
      return {program: BIN, args: [
        ...common,
        "promote-qualification", wiki,
        ...version,
        "--qualification-run-id", request.qualificationRunId,
        "--lifecycle", LIFECYCLE_PATH,
      ]};
    case "retire-candidate":
      return {program: BIN, args: [
        "--data-dir", DATA_DIR,
        "--output-dir", OUTPUT_DIR,
        "retire-candidate", wiki,
        ...version,
        "--candidate-run-id", request.candidateRunId,
        "--operator", request.requestedBy,
      ]};
    case "rebuild-candidate":
      return {program: BIN, args: [
        ...common,
        "prepare-wiki", wiki,
        ...version,
        "--rebuild",
        "--lifecycle", LIFECYCLE_PATH,
      ]};
    case "fetch":
      return {program: BIN, args: [...common, "fetch", wiki, ...version]};
    case "ingest":
      return {program: BIN, args: [...common, "ingest", wiki, ...version]};
    case "compute":
      return {program: BIN, args: [...common, "compute", wiki]};
    case "patrol-fetch":
      return {program: BIN, args: [...common, "patrol-fetch", wiki]};
    case "patrol-compute":
      return {program: BIN, args: [...common, "patrol-refresh", wiki]};
    case "patrol-rebuild":
      return {program: BIN, args: [...common, "patrol-refresh", wiki, "--rebuild"]};
    case "merge":
      return {program: BIN, args: [...common, "merge"]};
    case "fleet-recover":
      return {program: BIN, args: ["fleet-recover", "--queue-dir", QUEUE_DIR]};
    case "quarantine-retry":
      return {program: BIN, args: ["fleet-retry-quarantine", "--queue-dir", QUEUE_DIR, "--wiki", wiki, "--task-id", request.taskId]};
    case "publication-recovery-audit":
      return {program: BIN, args: [
        ...common,
        "publication-recovery-audit",
        "--site-dist-dir", path.join(ROOT, "site", "dist"),
        "--report", path.join(OUTPUT_DIR, "_admin", "publication-recovery-audit.json"),
      ]};
    case "publication-preflight":
      return {program: BIN, args: [
        ...common,
        "publication-preflight",
        "--lifecycle", LIFECYCLE_PATH,
        "--site-dist-dir", path.join(ROOT, "site", "dist"),
        "--report", path.join(OUTPUT_DIR, "_admin", "publication-preflight.json"),
      ]};
    case "artifact-scrub":
      return {program: "bash", args: [path.join(ROOT, "deploy", "toolforge", "run-artifact-scrub.sh")]};
    case "publish":
      return {program: "bash", args: [path.join(ROOT, "deploy", "toolforge", "run-publish-ready.sh")]};
    case "site":
      return {program: "bash", args: [path.join(ROOT, "deploy", "toolforge", "run-refresh-site.sh")]};
    default:
      throw new Error(`Unsupported admin operation ${request.action}`);
  }
}

function validateRequest(request) {
  if (request?.schemaVersion !== 1 || typeof request.requestId !== "string" || !request.requestId) {
    throw new Error("Admin operation has an invalid schema or request ID");
  }
  if (request.wiki != null && !/^[a-z0-9_]+wiki$/.test(request.wiki)) {
    throw new Error(`Admin operation has an invalid wiki ${request.wiki}`);
  }
  if (request.version != null && !/^\d{4}-\d{2}$/.test(request.version)) {
    throw new Error(`Admin operation has an invalid snapshot ${request.version}`);
  }
  if (request.action === "quarantine-retry" && !/^[a-f0-9]{64}$/.test(request.taskId || "")) {
    throw new Error("Quarantine retry requires an exact fleet task identity");
  }
  if (request.action === "promote-qualification") {
    if (!request.version || !/^[a-zA-Z0-9_.-]+$/.test(request.qualificationRunId || "")) {
      throw new Error("Qualification promotion requires an exact snapshot and qualification run ID");
    }
    if (request.lifecycleMutation?.action !== "promote"
      || request.lifecycleMutation?.wiki !== request.wiki
      || !/^[a-f0-9]{64}$/.test(request.lifecycleRevision || "")) {
      throw new Error("Qualification promotion has no authenticated lifecycle transition");
    }
  }
  if (request.action === "retire-candidate"
    && (!request.version || !/^[a-zA-Z0-9_.-]+$/.test(request.candidateRunId || "")
      || typeof request.requestedBy !== "string" || !request.requestedBy)) {
    throw new Error("Candidate retirement requires exact candidate and operator identities");
  }
  if (request.action === "rebuild-candidate" && !request.version) {
    throw new Error("Candidate rebuild requires an exact snapshot");
  }
  commandFor(request);
  return request;
}

function claimNextOperation() {
  const dirs = directories();
  const names = fs.readdirSync(dirs.queued).filter((name) => name.endsWith(".json")).sort();
  for (const name of names) {
    const queuedPath = path.join(dirs.queued, name);
    const runningPath = path.join(dirs.running, name);
    const queued = readJson(queuedPath);
    const notBefore = Date.parse(queued?.notBefore || 0);
    if (Number.isFinite(notBefore) && notBefore > Date.now()) continue;
    try {
      fs.renameSync(queuedPath, runningPath);
    } catch (error) {
      if (error.code === "ENOENT") continue;
      throw error;
    }
    return {dirs, runningPath, request: readJson(runningPath)};
  }
  return null;
}

function recoverStaleOperations() {
  const dirs = directories();
  const recovered = [];
  for (const name of fs.readdirSync(dirs.running).filter((entry) => entry.endsWith(".json")).sort()) {
    const runningPath = path.join(dirs.running, name);
    const request = readJson(runningPath);
    const heartbeatAge = Date.now() - Date.parse(request?.heartbeatAt || request?.updatedAt || request?.startedAt || 0);
    if (request?.schemaVersion !== 1 || !Number.isFinite(heartbeatAge) || heartbeatAge <= STALE_OPERATION_MS) continue;
    const now = new Date().toISOString();
    if (request.cancelRequested) {
      const cancelled = {
        ...request,
        state: "cancelled",
        exitCode: 130,
        finishedAt: now,
        updatedAt: now,
        recoveryReason: `heartbeat stale for ${Math.round(heartbeatAge / 1_000)} seconds`,
      };
      atomicWriteJson(path.join(dirs.history, `${Date.now()}-${request.requestId}.json`), cancelled);
      fs.unlinkSync(runningPath);
      continue;
    }
    if (Number(request.retryCount || 0) >= 2) {
      const failed = {
        ...request,
        state: "failed",
        exitCode: 1,
        finishedAt: now,
        updatedAt: now,
        error: "Admin operation exceeded its stale-heartbeat recovery limit",
        recoveryReason: `heartbeat stale for ${Math.round(heartbeatAge / 1_000)} seconds`,
      };
      atomicWriteJson(path.join(dirs.history, `${Date.now()}-${request.requestId}.json`), failed);
      fs.unlinkSync(runningPath);
      continue;
    }
    const queuedPath = path.join(dirs.queued, name);
    if (fs.existsSync(queuedPath)) continue;
    fs.renameSync(runningPath, queuedPath);
    atomicWriteJson(queuedPath, {
      ...request,
      state: "queued",
      retryCount: Number(request.retryCount || 0) + 1,
      recoveredAt: now,
      updatedAt: now,
      recoveryReason: `heartbeat stale for ${Math.round(heartbeatAge / 1_000)} seconds`,
    });
    recovered.push(request.requestId);
  }
  return recovered;
}

function logTail(file) {
  try {
    const size = fs.statSync(file).size;
    const fd = fs.openSync(file, "r");
    try {
      const start = Math.max(0, size - LOG_TAIL_BYTES);
      const buffer = Buffer.alloc(size - start);
      fs.readSync(fd, buffer, 0, buffer.length, start);
      return buffer.toString("utf8");
    } finally {
      fs.closeSync(fd);
    }
  } catch {
    return "";
  }
}

async function executeClaim(claim) {
  const {dirs, runningPath} = claim;
  let request;
  try {
    request = validateRequest(claim.request);
  } catch (error) {
    const invalid = {
      ...(claim.request || {schemaVersion: 1, requestId: path.basename(runningPath, ".json")}),
      state: "failed",
      error: error.message,
      exitCode: 2,
      finishedAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
    };
    atomicWriteJson(path.join(dirs.history, `${Date.now()}-${invalid.requestId}.json`), invalid);
    fs.unlinkSync(runningPath);
    return invalid;
  }

  const command = commandFor(request);
  const logPath = request.logPath || path.join(dirs.logs, `${request.requestId}.log`);
  const startedAt = new Date().toISOString();
  let state = {
    ...request,
    state: "running",
    startedAt,
    heartbeatAt: startedAt,
    updatedAt: startedAt,
    logPath,
    command: [command.program, ...command.args].join(" "),
  };
  atomicWriteJson(runningPath, state);
  fs.appendFileSync(logPath, `$ ${state.command}\nStarted: ${startedAt}\n`, "utf8");

  const child = spawn(command.program, command.args, {
    cwd: ROOT,
    env: {
      ...process.env,
      CARGO_TERM_COLOR: "never",
      NO_COLOR: "1",
      OBSERVABLE_TELEMETRY_DISABLE: "true",
      RUST_LOG: "info",
      WIKI_ECON_LOG_ANSI: "0",
      WIKI_ECON_DATA_DIR: DATA_DIR,
      WIKI_ECON_OUTPUT_DIR: OUTPUT_DIR,
      WIKI_ECON_WIKI_LIFECYCLE_FILE: LIFECYCLE_PATH,
      WIKI_ECON_RUN_ID: request.runId,
    },
  });
  const append = (chunk) => fs.appendFileSync(logPath, chunk);
  child.stdout.on("data", append);
  child.stderr.on("data", append);

  const heartbeat = setInterval(() => {
    const latest = readJson(runningPath) || state;
    if (latest.cancelRequested && !state.cancelRequested) {
      state.cancelRequested = true;
      child.kill("SIGTERM");
      fs.appendFileSync(logPath, "\n[cancellation requested by operator]\n", "utf8");
    }
    const now = new Date().toISOString();
    const operationSummary = summarizeOperationLog(state, logTail(logPath));
    state = {
      ...state,
      ...latest,
      ...operationSummary,
      state: state.cancelRequested ? "cancelling" : "running",
      heartbeatAt: now,
      updatedAt: now,
    };
    atomicWriteJson(runningPath, state);
  }, 5_000);

  const result = await new Promise((resolve) => {
    child.once("error", (error) => resolve({code: 1, signal: null, error}));
    child.once("close", (code, signal) => resolve({code: code ?? 1, signal, error: null}));
  });
  clearInterval(heartbeat);
  const finishedAt = new Date().toISOString();
  const cancelled = state.cancelRequested && result.signal === "SIGTERM";
  let effectiveCode = cancelled ? 130 : result.code;
  let effectiveError = result.error?.message || null;
  let lifecycleResult = null;
  if (effectiveCode === 0 && request.lifecycleMutation) {
    try {
      lifecycleResult = applyLifecycleMutation({
        lifecyclePath: LIFECYCLE_PATH,
        auditDir: LIFECYCLE_AUDIT_DIR,
        mutation: request.lifecycleMutation,
        operator: request.requestedBy,
        requestId: request.requestId,
        expectedRevision: request.lifecycleRevision,
      });
      fs.appendFileSync(logPath, "\n[lifecycle transition committed]\n", "utf8");
    } catch (error) {
      effectiveCode = 1;
      effectiveError = `Candidate operation succeeded but lifecycle transition failed: ${error.message}`;
      fs.appendFileSync(logPath, `\n[lifecycle transition failed: ${error.message}]\n`, "utf8");
    }
  }
  if (new Set(["promote-qualification", "retire-candidate", "rebuild-candidate"]).has(request.action)) {
    try {
      immutableAuditEvent(LIFECYCLE_AUDIT_DIR, {
        requestId: request.requestId,
        phase: effectiveCode === 0 ? "completed" : "failed",
        action: request.action,
        wiki: request.wiki,
        operator: request.requestedBy,
        recordedAt: finishedAt,
        outcome: effectiveCode === 0 ? "succeeded" : "failed",
        exitCode: effectiveCode,
        lifecycleRevision: lifecycleResult?.afterRevision || registryRevision(readLifecycle(LIFECYCLE_PATH)),
      });
    } catch (error) {
      effectiveCode = 1;
      effectiveError = `Immutable operator audit failed: ${error.message}`;
      fs.appendFileSync(logPath, `\n[immutable operator audit failed: ${error.message}]\n`, "utf8");
    }
  }
  const operationSummary = summarizeOperationLog(state, logTail(logPath));
  const waitingUpstream = effectiveCode === 75
    || operationSummary.remediationCode === "upstream_logging_waiting";
  const completed = {
    ...state,
    ...operationSummary,
    state: cancelled ? "cancelled" : effectiveCode === 0 ? "succeeded" : waitingUpstream ? "waiting_upstream" : "failed",
    exitCode: effectiveCode,
    signal: result.signal,
    error: effectiveError || (effectiveCode === 0 ? null : operationSummary.errorSummary),
    cancelRequested: Boolean(state.cancelRequested),
    finishedAt,
    heartbeatAt: finishedAt,
    updatedAt: finishedAt,
    logTail: logTail(logPath),
  };
  fs.appendFileSync(logPath, `\n[finished state=${completed.state} exit=${completed.exitCode}]\n`, "utf8");
  atomicWriteJson(path.join(dirs.history, `${Date.now()}-${request.requestId}.json`), completed);
  fs.unlinkSync(runningPath);
  if (completed.state === "waiting_upstream") {
    const notBefore = new Date(Date.now() + UPSTREAM_RETRY_MS).toISOString();
    atomicWriteJson(path.join(dirs.queued, path.basename(runningPath)), {
      ...request,
      state: "waiting_upstream",
      retryCount: Number(request.retryCount || 0),
      upstreamWaitCount: Number(request.upstreamWaitCount || 0) + 1,
      notBefore,
      heartbeatAt: finishedAt,
      updatedAt: finishedAt,
      logPath,
      error: completed.error,
      errorSummary: completed.errorSummary,
      remediationCode: completed.remediationCode,
      remediation: completed.remediation,
    });
  }
  return completed;
}

async function run() {
  recoverStaleOperations();
  const claim = claimNextOperation();
  if (!claim) {
    console.log("No queued admin operation");
    return null;
  }
  const completed = await executeClaim(claim);
  console.log(JSON.stringify({requestId: completed.requestId, state: completed.state, exitCode: completed.exitCode}));
  if (completed.state === "failed") process.exitCode = completed.exitCode || 1;
  return completed;
}

if (require.main === module) {
  run().catch((error) => {
    console.error(error.stack || error.message);
    process.exitCode = 1;
  });
}

module.exports = {claimNextOperation, commandFor, executeClaim, recoverStaleOperations, run, validateRequest};
