"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const {after, test} = require("node:test");
const {
  appendEvent,
  buildRecord,
  conciseError,
  foldStageEvents,
  historyLimit,
  publicationSummary,
  readEvents,
  rotateLogs,
  structuredSummaries,
  writeRunRecord,
} = require("./run-record.cjs");

const fixtureRoot = fs.mkdtempSync(path.join(os.tmpdir(), "wiki-econ-run-record-"));
after(() => fs.rmSync(fixtureRoot, {recursive: true, force: true}));

function fixture(name) {
  const root = path.join(fixtureRoot, name);
  const output = path.join(root, "output");
  const lock = path.join(output, ".refresh-lock");
  const distGeneration = path.join(root, ".site-dist.build.abc123");
  const dist = path.join(root, "site-dist");
  fs.mkdirSync(lock, {recursive: true});
  fs.mkdirSync(distGeneration, {recursive: true});
  fs.symlinkSync(path.basename(distGeneration), dist);
  fs.writeFileSync(path.join(lock, "run-state"), "running\n");
  fs.writeFileSync(path.join(lock, "selected-snapshot"), "2026-07\n");
  const environment = {
    WIKI_ECON_BINARY_SHA256: "a".repeat(64),
    WIKI_ECON_IMAGE_SOURCE_COMMIT: "b".repeat(40),
    WIKI_ECON_IMAGE_SOURCE_REF: "main",
    WIKI_ECON_OUTPUT_DIR: output,
    WIKI_ECON_REFRESH_HISTORY_LIMIT: "104",
    WIKI_ECON_RUN_EVENTS_FILE: path.join(lock, "stage-events.jsonl"),
    WIKI_ECON_RUN_HISTORY_FILE: path.join(output, ".refresh-history.jsonl"),
    WIKI_ECON_RUN_ID: `${name}-run`,
    WIKI_ECON_RUN_LOG_FILE: path.join(output, "logs", `${name}-run.log`),
    WIKI_ECON_RUN_PUBLICATION_FILE: path.join(output, "publication-gate.json"),
    WIKI_ECON_RUN_SNAPSHOT_FILE: path.join(lock, "selected-snapshot"),
    WIKI_ECON_RUN_STARTED_AT: "2026-08-22T03:00:00Z",
    WIKI_ECON_RUN_START_EPOCH: String(Math.floor(Date.now() / 1000) - 10),
    WIKI_ECON_RUN_STATE_FILE: path.join(lock, "run-state"),
    WIKI_ECON_RUN_STATUS_FILE: path.join(output, ".refresh-status.json"),
    WIKI_ECON_RUN_WIKIS_JSON: '["nlwiki"]',
    WIKI_ECON_SITE_DIST_DIR: dist,
    WIKI_ECON_SOURCE_COMMIT: "c".repeat(40),
  };
  return {environment, lock, output};
}

test("stage events fold into durations, reuse, current stage, and concise failure", () => {
  const events = [
    {event: "started", stage: "fetch", wiki: "nlwiki", at: "2026-08-22T03:00:01Z"},
    {event: "reused", stage: "fetch", wiki: "nlwiki", at: "2026-08-22T03:00:02Z"},
    {event: "completed", stage: "fetch", wiki: "nlwiki", at: "2026-08-22T03:00:03Z", durationMs: 2000},
    {event: "started", stage: "compute", wiki: "nlwiki", at: "2026-08-22T03:00:04Z"},
    {event: "skipped", stage: "compute", wiki: "nlwiki", at: "2026-08-22T03:00:05Z"},
  ];
  const live = foldStageEvents(events);
  assert.equal(live.current.stage, "compute");
  assert.deepEqual(live.reusedStages, ["fetch:nlwiki"]);
  assert.deepEqual(live.skippedStages, ["compute:nlwiki"]);
  assert.deepEqual(live.stageDurationsMs, {fetch: 2000});

  const failed = foldStageEvents([...events, {
    event: "failed",
    stage: "compute",
    wiki: "nlwiki",
    at: "2026-08-22T03:00:06Z",
    durationMs: 2000,
    error: "out of memory\nwith context",
  }]);
  assert.equal(failed.failed.error, "out of memory with context");
  assert.equal(failed.current, null);
  assert.deepEqual(failed.stageDurationsMs, {fetch: 2000, compute: 2000});
});

test("live and final records combine provenance, resources, publication, and site generation", () => {
  const {environment} = fixture("complete");
  appendEvent(environment.WIKI_ECON_RUN_EVENTS_FILE, "started", "fetch", "nlwiki");
  appendEvent(environment.WIKI_ECON_RUN_EVENTS_FILE, "reused", "fetch", "nlwiki");
  appendEvent(environment.WIKI_ECON_RUN_EVENTS_FILE, "completed", "fetch", "nlwiki", 1200);
  appendEvent(environment.WIKI_ECON_RUN_EVENTS_FILE, "started", "compute", "nlwiki");
  const live = buildRecord(environment);
  assert.equal(live.schemaVersion, 2);
  assert.equal(live.state, "running");
  assert.equal(live.currentStage, "compute");
  assert.equal(live.selectedSnapshot, "2026-07");
  assert.equal(live.exitCode, null);
  assert.equal(live.provenance.imageSourceRef, "main");
  assert.equal(live.disk.path, environment.WIKI_ECON_OUTPUT_DIR);

  appendEvent(environment.WIKI_ECON_RUN_EVENTS_FILE, "completed", "compute", "nlwiki", 800);
  fs.writeFileSync(environment.WIKI_ECON_RUN_PUBLICATION_FILE, JSON.stringify({
    run_id: environment.WIKI_ECON_RUN_ID,
    selected_snapshot_versions: {nlwiki: "2026-07"},
    cutoff_dates: {nlwiki: "2026-08"},
    patrol_sources: {nlwiki: {patrol_events: 10, rights_events: 2}},
    metrics: {
      page_weekly_edits: {
        rows: 40,
        conservation_total: 70,
        wikis: {nlwiki: {minimum_date: "2001-08-06", maximum_date: "2026-07-27"}},
      },
    },
  }));
  const final = buildRecord(environment, 0);
  assert.equal(final.state, "succeeded");
  assert.equal(final.currentStage, null);
  assert.equal(final.publishedSiteGeneration, ".site-dist.build.abc123");
  assert.equal(final.logFile, "complete-run.log");
  assert.equal(final.publication.metrics.page_weekly_edits.rows, 40);
  assert.equal(final.publication.metrics.page_weekly_edits.edits, 70);
  assert.equal(final.publication.metrics.page_weekly_edits.maximumDate, "2026-07-27");
  assert.equal(final.publication.selectedSnapshots.nlwiki, "2026-07");
});

test("failed records prefer the Rust stage error and reject another run's publication", () => {
  const {environment} = fixture("failed");
  appendEvent(environment.WIKI_ECON_RUN_EVENTS_FILE, "started", "ingest", "nlwiki");
  appendEvent(environment.WIKI_ECON_RUN_EVENTS_FILE, "failed", "ingest", "nlwiki", 50, "bad parquet");
  environment.WIKI_ECON_RUN_ERROR = "generic shell failure";
  fs.writeFileSync(environment.WIKI_ECON_RUN_PUBLICATION_FILE, JSON.stringify({
    run_id: "older-run",
    metrics: {gdp: {rows: 1}},
  }));
  const record = buildRecord(environment, 9);
  assert.equal(record.state, "failed");
  assert.equal(record.failingStage, "ingest");
  assert.equal(record.error, "bad parquet");
  assert.equal(record.publication, null);
  assert.equal(record.publishedSiteGeneration, ".site-dist.build.abc123");
});

test("atomic status writes and compact history retains and deduplicates 104 runs", () => {
  const {environment, output} = fixture("history");
  const oldEntries = Array.from({length: 110}, (_, index) => JSON.stringify({
    runId: `old-${index}`,
    state: "succeeded",
  }));
  oldEntries.push("not-json");
  fs.writeFileSync(environment.WIKI_ECON_RUN_HISTORY_FILE, `${oldEntries.join("\n")}\n`);
  writeRunRecord(environment, 0);
  writeRunRecord(environment, 0);

  const status = JSON.parse(fs.readFileSync(environment.WIKI_ECON_RUN_STATUS_FILE, "utf8"));
  const history = fs.readFileSync(environment.WIKI_ECON_RUN_HISTORY_FILE, "utf8").trim().split("\n").map(JSON.parse);
  assert.equal(status.state, "succeeded");
  assert.equal(history.length, 104);
  assert.equal(history.at(-1).runId, environment.WIKI_ECON_RUN_ID);
  assert.equal(history.filter((entry) => entry.runId === environment.WIKI_ECON_RUN_ID).length, 1);
  assert.equal(fs.readdirSync(output).some((name) => name.includes(".tmp.")), false);
});

test("parsers and bounds fail safely", () => {
  const {environment} = fixture("parsers");
  fs.writeFileSync(environment.WIKI_ECON_RUN_EVENTS_FILE, '{"event":"started","stage":"fetch"}\ninvalid\n');
  assert.equal(readEvents(environment.WIKI_ECON_RUN_EVENTS_FILE).length, 1);
  assert.equal(conciseError("\n\n"), null);
  assert.equal(conciseError("x".repeat(600)).length, 500);
  assert.equal(historyLimit("1"), 52);
  assert.equal(historyLimit("1000"), 104);
  assert.equal(historyLimit("bad"), 104);
  assert.equal(publicationSummary({run_id: "other"}, "current"), null);
});

test("structured summaries are log-safe and per-run logs retain 104 files", () => {
  const {environment, output} = fixture("structured-logs");
  appendEvent(environment.WIKI_ECON_RUN_EVENTS_FILE, "started", "compute", "nlwiki");
  appendEvent(environment.WIKI_ECON_RUN_EVENTS_FILE, "completed", "compute", "nlwiki", 25);
  const summaries = structuredSummaries(buildRecord(environment, 0)).map(JSON.parse);
  assert.equal(summaries[0].type, "wiki_econ_stage_summary");
  assert.equal(summaries[0].durationMs, 25);
  assert.equal(summaries.at(-1).type, "wiki_econ_run_summary");

  const logs = path.join(output, "logs");
  fs.mkdirSync(logs);
  for (let index = 0; index < 110; index += 1) {
    fs.writeFileSync(path.join(logs, `run-${String(index).padStart(3, "0")}.log`), "log\n");
  }
  fs.writeFileSync(path.join(logs, "README.txt"), "keep\n");
  rotateLogs(logs, 104);
  assert.equal(fs.readdirSync(logs).filter((name) => name.endsWith(".log")).length, 104);
  assert.equal(fs.existsSync(path.join(logs, "run-000.log")), false);
  assert.equal(fs.existsSync(path.join(logs, "README.txt")), true);
});
