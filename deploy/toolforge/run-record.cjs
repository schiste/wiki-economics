"use strict";

const fs = require("node:fs");
const path = require("node:path");

const DEFAULT_HISTORY_LIMIT = 104;
const MIN_HISTORY_LIMIT = 52;
const MAX_HISTORY_LIMIT = 104;
const MAX_ERROR_CHARS = 500;

function readText(file) {
  try {
    return fs.readFileSync(file, "utf8").trim();
  } catch {
    return "";
  }
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
  const temporary = `${file}.tmp.${process.pid}`;
  try {
    fs.writeFileSync(temporary, `${JSON.stringify(value)}\n`, {mode: 0o600});
    fs.renameSync(temporary, file);
  } catch (error) {
    try { fs.unlinkSync(temporary); } catch {}
    throw error;
  }
}

function conciseError(value) {
  if (!value) return null;
  return String(value).replace(/[\r\n]+/g, " ").trim().slice(0, MAX_ERROR_CHARS) || null;
}

function readEvents(file) {
  if (!file) return [];
  return readText(file)
    .split(/\r?\n/)
    .filter(Boolean)
    .map((line) => {
      try { return JSON.parse(line); } catch { return null; }
    })
    .filter(Boolean);
}

function appendEvent(file, event, stage, wiki = null, durationMs = null, error = null) {
  if (!file) return;
  fs.mkdirSync(path.dirname(file), {recursive: true});
  const entry = {
    event,
    stage,
    wiki: wiki || null,
    at: new Date().toISOString().replace(/\.\d{3}Z$/, "Z"),
    durationMs: Number.isFinite(durationMs) ? durationMs : null,
    error: conciseError(error),
  };
  fs.appendFileSync(file, `${JSON.stringify(entry)}\n`, {mode: 0o600});
}

function stageLabel(stage) {
  return stage.wiki ? `${stage.stage}:${stage.wiki}` : stage.stage;
}

function foldStageEvents(events) {
  const stages = [];
  for (const event of events) {
    if (!event || typeof event.stage !== "string" || typeof event.event !== "string") continue;
    if (event.event === "started") {
      stages.push({
        stage: event.stage,
        wiki: event.wiki || null,
        state: "running",
        startedAt: event.at || null,
        finishedAt: null,
        durationMs: null,
        reused: false,
        skipped: false,
        error: null,
      });
      continue;
    }
    const active = [...stages].reverse().find((candidate) =>
      candidate.state === "running" &&
      candidate.stage === event.stage &&
      candidate.wiki === (event.wiki || null));
    if (event.event === "reused") {
      if (active) active.reused = true;
      continue;
    }
    if (event.event === "skipped") {
      if (active) active.skipped = true;
      continue;
    }
    if (!active || !["completed", "failed"].includes(event.event)) continue;
    active.state = event.event === "completed" ? "succeeded" : "failed";
    active.finishedAt = event.at || null;
    active.durationMs = Number.isFinite(event.durationMs) ? event.durationMs : null;
    active.error = conciseError(event.error);
  }

  const current = [...stages].reverse().find((stage) => stage.state === "running") || null;
  const failed = [...stages].reverse().find((stage) => stage.state === "failed") || null;
  const stageDurationsMs = {};
  for (const stage of stages) {
    if (stage.durationMs == null) continue;
    stageDurationsMs[stage.stage] = (stageDurationsMs[stage.stage] || 0) + stage.durationMs;
  }
  return {
    stages,
    current,
    failed,
    stageDurationsMs,
    reusedStages: stages.filter((stage) => stage.reused).map(stageLabel),
    skippedStages: stages.filter((stage) => stage.skipped).map(stageLabel),
  };
}

function finiteCounter(file) {
  const value = Number(readText(file));
  return Number.isSafeInteger(value) && value >= 0 ? value : null;
}

function diskSpace(directory) {
  try {
    const stats = fs.statfsSync(directory, {bigint: true});
    return {
      path: directory,
      freeBytes: Number(stats.bavail * stats.bsize),
      totalBytes: Number(stats.blocks * stats.bsize),
    };
  } catch {
    return {path: directory, freeBytes: null, totalBytes: null};
  }
}

function siteGeneration(distDir) {
  try {
    return fs.lstatSync(distDir).isSymbolicLink() ? fs.readlinkSync(distDir) : null;
  } catch {
    return null;
  }
}

function overallDateRange(wikis) {
  const minimumDates = Object.values(wikis || {}).map((wiki) => wiki.minimum_date).filter(Boolean).sort();
  const maximumDates = Object.values(wikis || {}).map((wiki) => wiki.maximum_date).filter(Boolean).sort();
  return {
    minimumDate: minimumDates[0] || null,
    maximumDate: maximumDates.at(-1) || null,
  };
}

function publicationSummary(gate, runId) {
  if (!gate || gate.run_id !== runId) return null;
  const metrics = {};
  for (const [name, metric] of Object.entries(gate.metrics || {})) {
    const dates = overallDateRange(metric.wikis);
    metrics[name] = {
      rows: metric.rows ?? null,
      conservationTotal: metric.conservation_total ?? null,
      edits: name === "page_weekly_edits" ? (metric.conservation_total ?? null) : null,
      ...dates,
    };
  }
  return {
    selectedSnapshots: gate.selected_snapshot_versions || {},
    cutoffDates: gate.cutoff_dates || {},
    metrics,
    patrolSources: gate.patrol_sources || {},
  };
}

function parseWikis(value) {
  try {
    const parsed = JSON.parse(value || "[]");
    return Array.isArray(parsed) ? parsed.filter((wiki) => typeof wiki === "string") : [];
  } catch {
    return [];
  }
}

function historyLimit(value) {
  const parsed = Number(value);
  if (!Number.isInteger(parsed)) return DEFAULT_HISTORY_LIMIT;
  return Math.min(MAX_HISTORY_LIMIT, Math.max(MIN_HISTORY_LIMIT, parsed));
}

function buildRecord(environment, finalExitCode = null) {
  const now = new Date();
  const runId = environment.WIKI_ECON_RUN_ID;
  if (!runId) throw new Error("WIKI_ECON_RUN_ID is required");
  const events = foldStageEvents(readEvents(environment.WIKI_ECON_RUN_EVENTS_FILE));
  const selectedSnapshot = readText(environment.WIKI_ECON_RUN_SNAPSHOT_FILE) || null;
  const requestedLiveState = readText(environment.WIKI_ECON_RUN_STATE_FILE);
  const liveState = ["starting", "running"].includes(requestedLiveState)
    ? requestedLiveState
    : "starting";
  const isFinal = finalExitCode != null;
  const state = isFinal ? (finalExitCode === 0 ? "succeeded" : "failed") : liveState;
  const startedAt = environment.WIKI_ECON_RUN_STARTED_AT;
  const startedEpoch = Number(environment.WIKI_ECON_RUN_START_EPOCH);
  const durationSecs = Number.isFinite(startedEpoch)
    ? Math.max(0, Math.floor(now.getTime() / 1000) - startedEpoch)
    : null;
  const publication = publicationSummary(readJson(environment.WIKI_ECON_RUN_PUBLICATION_FILE), runId);
  const shellError = conciseError(environment.WIKI_ECON_RUN_ERROR);
  const failingStage = state === "failed"
    ? (events.failed?.stage || events.current?.stage || environment.WIKI_ECON_RUN_FAILING_STAGE || null)
    : null;
  const error = state === "failed" ? (events.failed?.error || shellError) : null;
  const publishedSiteGeneration = siteGeneration(environment.WIKI_ECON_SITE_DIST_DIR);

  return {
    schemaVersion: 2,
    state,
    runId,
    startedAt: startedAt || null,
    finishedAt: isFinal ? now.toISOString().replace(/\.\d{3}Z$/, "Z") : null,
    heartbeatAt: now.toISOString().replace(/\.\d{3}Z$/, "Z"),
    exitCode: isFinal ? finalExitCode : null,
    wikis: parseWikis(environment.WIKI_ECON_RUN_WIKIS_JSON),
    selectedSnapshot,
    currentStage: isFinal ? null : events.current?.stage || null,
    currentWiki: isFinal ? null : events.current?.wiki || null,
    durationSecs,
    stageDurationsMs: events.stageDurationsMs,
    stages: events.stages,
    reusedStages: events.reusedStages,
    skippedStages: events.skippedStages,
    failingStage,
    error,
    provenance: {
      sourceCommit: environment.WIKI_ECON_SOURCE_COMMIT || null,
      binarySha256: environment.WIKI_ECON_BINARY_SHA256 || null,
      imageSourceRef: environment.WIKI_ECON_IMAGE_SOURCE_REF || null,
      imageSourceCommit: environment.WIKI_ECON_IMAGE_SOURCE_COMMIT || null,
    },
    publication,
    memoryCurrentBytes: finiteCounter("/sys/fs/cgroup/memory.current"),
    memoryPeakBytes: finiteCounter("/sys/fs/cgroup/memory.peak"),
    memoryLimitBytes: finiteCounter("/sys/fs/cgroup/memory.max"),
    disk: diskSpace(environment.WIKI_ECON_OUTPUT_DIR),
    publishedSiteGeneration,
    logFile: environment.WIKI_ECON_RUN_LOG_FILE ? path.basename(environment.WIKI_ECON_RUN_LOG_FILE) : null,
  };
}

function compactHistoryEntry(record) {
  return {
    schemaVersion: record.schemaVersion,
    state: record.state,
    runId: record.runId,
    startedAt: record.startedAt,
    finishedAt: record.finishedAt,
    exitCode: record.exitCode,
    wikis: record.wikis,
    selectedSnapshot: record.selectedSnapshot,
    durationSecs: record.durationSecs,
    stageDurationsMs: record.stageDurationsMs,
    reusedStages: record.reusedStages,
    skippedStages: record.skippedStages,
    failingStage: record.failingStage,
    error: record.error,
    memoryPeakBytes: record.memoryPeakBytes,
    memoryLimitBytes: record.memoryLimitBytes,
    diskFreeBytes: record.disk.freeBytes,
    publishedSiteGeneration: record.publishedSiteGeneration,
    logFile: record.logFile,
    publication: record.publication,
  };
}

function appendHistory(file, record, limit) {
  const entries = readText(file)
    .split(/\r?\n/)
    .filter(Boolean)
    .map((line) => {
      try { return JSON.parse(line); } catch { return null; }
    })
    .filter((entry) => entry && entry.runId !== record.runId);
  entries.push(compactHistoryEntry(record));
  const retained = entries.slice(-limit);
  const temporary = `${file}.tmp.${process.pid}`;
  fs.mkdirSync(path.dirname(file), {recursive: true});
  try {
    fs.writeFileSync(temporary, `${retained.map(JSON.stringify).join("\n")}\n`, {mode: 0o600});
    fs.renameSync(temporary, file);
  } catch (error) {
    try { fs.unlinkSync(temporary); } catch {}
    throw error;
  }
}

function writeRunRecord(environment, finalExitCode = null) {
  const record = buildRecord(environment, finalExitCode);
  if (finalExitCode != null) {
    appendHistory(
      environment.WIKI_ECON_RUN_HISTORY_FILE,
      record,
      historyLimit(environment.WIKI_ECON_REFRESH_HISTORY_LIMIT),
    );
  }
  atomicWriteJson(environment.WIKI_ECON_RUN_STATUS_FILE, record);
  return record;
}

function structuredSummaries(record) {
  const stages = record.stages.map((stage) => JSON.stringify({
    type: "wiki_econ_stage_summary",
    runId: record.runId,
    stage: stage.stage,
    wiki: stage.wiki,
    state: stage.state,
    durationMs: stage.durationMs,
    reused: stage.reused,
    skipped: stage.skipped,
    error: stage.error,
  }));
  stages.push(JSON.stringify({
    type: "wiki_econ_run_summary",
    runId: record.runId,
    state: record.state,
    durationSecs: record.durationSecs,
    selectedSnapshot: record.selectedSnapshot,
    reusedStages: record.reusedStages,
    skippedStages: record.skippedStages,
    failingStage: record.failingStage,
    error: record.error,
    memoryPeakBytes: record.memoryPeakBytes,
    memoryLimitBytes: record.memoryLimitBytes,
    diskFreeBytes: record.disk.freeBytes,
    publishedSiteGeneration: record.publishedSiteGeneration,
  }));
  return stages;
}

function rotateLogs(directory, limit = DEFAULT_HISTORY_LIMIT) {
  fs.mkdirSync(directory, {recursive: true});
  const retained = historyLimit(limit);
  const logs = fs.readdirSync(directory, {withFileTypes: true})
    .filter((entry) => entry.isFile() && /^[A-Za-z0-9._-]+\.log$/.test(entry.name))
    .map((entry) => entry.name)
    .sort();
  for (const name of logs.slice(0, Math.max(0, logs.length - retained))) {
    fs.unlinkSync(path.join(directory, name));
  }
}

if (require.main === module) {
  const command = process.argv[2];
  if (command === "write") {
    writeRunRecord(process.env);
  } else if (command === "finish") {
    const exitCode = Number(process.argv[3]);
    if (!Number.isInteger(exitCode)) throw new Error("finish requires an integer exit code");
    const record = writeRunRecord(process.env, exitCode);
    process.stdout.write(`${structuredSummaries(record).join("\n")}\n`);
  } else if (command === "event") {
    const durationMs = process.argv[6] === "" ? null : Number(process.argv[6]);
    appendEvent(
      process.env.WIKI_ECON_RUN_EVENTS_FILE,
      process.argv[3],
      process.argv[4],
      process.argv[5] || null,
      durationMs,
      process.argv[7] || null,
    );
  } else if (command === "rotate-logs") {
    rotateLogs(process.argv[3], process.argv[4]);
  } else {
    throw new Error("usage: node run-record.cjs write|finish|event|rotate-logs ...");
  }
}

module.exports = {
  appendEvent,
  appendHistory,
  atomicWriteJson,
  buildRecord,
  compactHistoryEntry,
  conciseError,
  diskSpace,
  foldStageEvents,
  historyLimit,
  publicationSummary,
  readEvents,
  rotateLogs,
  siteGeneration,
  structuredSummaries,
  writeRunRecord,
};
