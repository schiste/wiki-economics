"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const {after, test} = require("node:test");
const {buildManifest, generationSummary, parquetRowCounter, safeReceiptOutput} = require("./manifest.json.cjs");

const root = fs.mkdtempSync(path.join(os.tmpdir(), "wiki-econ-manifest-"));
after(() => fs.rmSync(root, {recursive: true, force: true}));

const metrics = [
  "business_funnel", "gdp", "gdp_activity_tiers", "gdp_user_type_share", "inequality",
  "labor_churn", "labor_cohorts", "labor_monthly", "page_weekly_edits", "patrol",
];

function lifecycle() {
  return {
    schema_version: 1,
    publication_contract: {
      datasets: Object.fromEntries(metrics.map((name) => [name, name === "patrol" || name === "page_weekly_edits"
        ? {wikis: ["nlwiki"], minimum_rows_per_wiki: 1}
        : {coverage: "all_published", minimum_rows_per_wiki: 1}])),
    },
    wikis: {
      nlwiki: {publication: "published", refresh: "scheduled", provenance: "toolforge", freshness_sla_days: 10},
    },
  };
}

function fixture(name) {
  const directory = path.join(root, name);
  const dataDir = path.join(directory, "data");
  const outputDir = path.join(directory, "output");
  const snapshot = "2026-07";
  const analytical = path.join(dataDir, "parquet", "nlwiki", "_snapshots", snapshot, "year=2026", "part.parquet");
  fs.mkdirSync(path.dirname(analytical), {recursive: true});
  fs.writeFileSync(analytical, "valid-ingest-output");
  fs.mkdirSync(path.join(dataDir, "snapshots", "nlwiki"), {recursive: true});
  fs.writeFileSync(path.join(dataDir, "snapshots", "nlwiki", "current-snapshot.json"), JSON.stringify({
    schema_version: 1, wiki: "nlwiki", snapshot_version: snapshot,
  }));
  fs.mkdirSync(path.join(dataDir, "stages", "nlwiki", snapshot), {recursive: true});
  fs.writeFileSync(path.join(dataDir, "stages", "nlwiki", snapshot, "ingest.json"), JSON.stringify({
    schema_version: 1,
    stage: "ingest",
    scope: "nlwiki",
    selected_snapshot: snapshot,
    inputs: [{identity: "raw/2026-07.nlwiki.2026"}],
    outputs: [{
      identity: "analytical/year=2026/part.parquet",
      bytes: fs.statSync(analytical).size,
      rows: 42,
    }],
  }));
  const patrolDir = path.join(dataDir, "patrol", "nlwiki");
  fs.mkdirSync(patrolDir, {recursive: true});
  fs.writeFileSync(path.join(patrolDir, "nlwiki-latest-pages-logging.xml.gz"), "gzip");
  fs.writeFileSync(path.join(patrolDir, "autopatrol_groups.json"), JSON.stringify({autopatrol_groups: ["sysop"]}));
  fs.writeFileSync(path.join(patrolDir, "patrol.parquet"), "patrol");
  fs.writeFileSync(path.join(patrolDir, "rights.parquet"), "rights");
  fs.mkdirSync(path.join(outputDir, "nlwiki"), {recursive: true});
  fs.mkdirSync(path.join(outputDir, "_stages"), {recursive: true});
  fs.mkdirSync(path.join(outputDir, ".refresh-lock"), {recursive: true});
  fs.mkdirSync(path.join(outputDir, "logs"), {recursive: true});
  for (const metric of metrics) {
    fs.writeFileSync(path.join(outputDir, "nlwiki", `${metric}.parquet`), metric);
    fs.writeFileSync(path.join(outputDir, `${metric}.parquet`), metric);
  }
  return {analytical, dataDir, outputDir};
}

function rows(values) {
  return {
    async count(file) {
      if (file.endsWith("rights.parquet")) return values.rights;
      if (file.includes(`${path.sep}data${path.sep}`)) return values.events;
      return values.metric;
    },
    close() {},
  };
}

test("generation readiness follows the pointer and strict ingest receipt without raw dumps", async () => {
  const current = fixture("complete");
  const manifest = await buildManifest({
    root,
    dataDir: current.dataDir,
    outputDir: current.outputDir,
    lifecycle: lifecycle(),
    rowCounter: rows({events: 10, rights: 2, metric: 5}),
  });
  assert.equal(manifest.schema_version, 2);
  assert.equal(manifest.wikis.nlwiki.status, "complete");
  assert.equal(manifest.wikis.nlwiki.raw.files, 0);
  assert.equal(manifest.wikis.nlwiki.snapshot.version, "2026-07");
  assert.equal(manifest.wikis.nlwiki.ingest.ready, 1);
  assert.equal(manifest.wikis.nlwiki.ingest.rows, 42);
  assert.equal(manifest.wikis.nlwiki.patrol.event_rows, 10);
  assert.equal(manifest.wikis.nlwiki.patrol.rights_rows, 2);
  assert.equal(manifest.wikis._stages, undefined);
  assert.equal(manifest.wikis[".refresh-lock"], undefined);
  assert.equal(manifest.wikis.logs, undefined);
});

test("the production row counter consumes the validated Rust footer map", async () => {
  const countsFile = path.join(root, "row-counts.json");
  fs.writeFileSync(countsFile, JSON.stringify({"a'file.parquet": 17}));
  const counter = parquetRowCounter(countsFile);
  assert.equal(await counter.count("a'file.parquet"), 17);
  await assert.rejects(counter.count("missing.parquet"), /no valid entry/);
  await counter.close();
});

test("patrol readiness rejects existing but zero-row parquet files", async () => {
  const current = fixture("empty-patrol");
  const manifest = await buildManifest({
    root,
    dataDir: current.dataDir,
    outputDir: current.outputDir,
    lifecycle: lifecycle(),
    rowCounter: rows({events: 0, rights: 0, metric: 0}),
  });
  assert.equal(manifest.wikis.nlwiki.patrol.events, 0);
  assert.equal(manifest.wikis.nlwiki.patrol.rights, 0);
  assert.equal(manifest.wikis.nlwiki.status, "needs_patrol_fetch");
});

test("a damaged selected generation needs ingest instead of another fetch", () => {
  const current = fixture("damaged-ingest");
  fs.appendFileSync(current.analytical, "changed");
  const summary = generationSummary(current.dataDir, "nlwiki");
  assert.equal(summary.pointer_ready, 1);
  assert.equal(summary.ingest_ready, 0);
  assert.match(summary.error, /invalid ingest output/);
  assert.equal(safeReceiptOutput(current.dataDir, "nlwiki", "2026-07", {identity: "analytical/../escape"}), null);
  assert.equal(safeReceiptOutput(current.dataDir, "nlwiki", "2026-07", {identity: "unknown/file"}), null);
});
