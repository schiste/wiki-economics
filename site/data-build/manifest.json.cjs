#!/usr/bin/env node
"use strict";

const fs = require("node:fs");
const path = require("node:path");

const CORE_METRICS = [
  "business_funnel", "gdp", "gdp_activity_tiers", "gdp_user_type_share",
  "inequality", "labor_churn", "labor_cohorts", "labor_monthly",
];

function findRoot(start = __dirname) {
  let current = path.resolve(start);
  while (current !== path.dirname(current)) {
    if (fs.existsSync(path.join(current, "Cargo.toml"))) return current;
    current = path.dirname(current);
  }
  throw new Error("unable to locate repository root");
}

function readJson(file) {
  try { return JSON.parse(fs.readFileSync(file, "utf8")); } catch { return null; }
}

function statFile(file) {
  try {
    const stat = fs.statSync(file);
    return stat.isFile() ? stat : null;
  } catch { return null; }
}

function fileList(directory, extension = ".parquet") {
  try {
    return fs.readdirSync(directory, {withFileTypes: true})
      .filter((entry) => entry.isFile() && entry.name.endsWith(extension))
      .map((entry) => {
        const stat = fs.statSync(path.join(directory, entry.name));
        return {name: entry.name.slice(0, -extension.length), size_kb: Math.floor(stat.size / 1024)};
      })
      .sort((left, right) => left.name.localeCompare(right.name));
  } catch { return []; }
}

function humanBytes(bytes) {
  if (!Number.isFinite(bytes) || bytes <= 0) return "0";
  const units = ["B", "K", "M", "G", "T"];
  let value = bytes;
  let unit = 0;
  while (value >= 1024 && unit < units.length - 1) {
    value /= 1024;
    unit += 1;
  }
  return `${value >= 10 || unit === 0 ? value.toFixed(0) : value.toFixed(1)}${units[unit]}`;
}

function rawSummary(dataDir, wiki) {
  const directory = path.join(dataDir, "raw", wiki);
  let entries = [];
  try {
    entries = fs.readdirSync(directory, {withFileTypes: true})
      .filter((entry) => entry.isFile() && entry.name.endsWith(".bz2"))
      .map((entry) => {
        const stat = fs.statSync(path.join(directory, entry.name));
        return {name: entry.name, size: humanBytes(stat.size), date: stat.mtime.toISOString().slice(0, 10), bytes: stat.size};
      })
      .sort((left, right) => left.name.localeCompare(right.name));
  } catch {}
  const version = entries[0]?.name.match(/(?:^|\.)(\d{4}-\d{2})(?:\.|$)/)?.[1] || null;
  return {
    files: entries.length,
    size: humanBytes(entries.reduce((total, entry) => total + entry.bytes, 0)),
    version,
    details: entries.map(({bytes: _bytes, ...entry}) => entry),
  };
}

function safeReceiptOutput(dataDir, wiki, snapshot, artifact) {
  if (!artifact || typeof artifact.identity !== "string") return null;
  const separator = artifact.identity.indexOf("/");
  if (separator <= 0) return null;
  const layer = artifact.identity.slice(0, separator);
  const relative = artifact.identity.slice(separator + 1);
  const layerDir = layer === "analytical" ? "parquet" : layer === "warehouse" ? "warehouse" : null;
  if (!layerDir || !relative || path.isAbsolute(relative)) return null;
  const root = path.resolve(dataDir, layerDir, wiki, "_snapshots", snapshot);
  const output = path.resolve(root, relative);
  return output.startsWith(`${root}${path.sep}`) ? output : null;
}

function generationSummary(dataDir, wiki) {
  const pointer = readJson(path.join(dataDir, "snapshots", wiki, "current-snapshot.json"));
  const snapshot = pointer?.schema_version === 1 && pointer.wiki === wiki && /^\d{4}-\d{2}$/.test(pointer.snapshot_version)
    ? pointer.snapshot_version
    : null;
  if (!snapshot) {
    return {version: null, pointer_ready: 0, ingest_ready: 0, rows: 0, sources: 0, outputs: 0, bytes: 0, in_progress: 0,
      error: pointer ? "invalid snapshot pointer" : "snapshot pointer missing"};
  }

  const receiptPath = path.join(dataDir, "stages", wiki, snapshot, "ingest.json");
  const receipt = readJson(receiptPath);
  let error = null;
  let rows = 0;
  let bytes = 0;
  let validOutputs = 0;
  const structurallyValid = receipt?.schema_version === 1 && receipt.stage === "ingest" && receipt.scope === wiki
    && receipt.selected_snapshot === snapshot && Array.isArray(receipt.inputs)
    && Array.isArray(receipt.outputs) && receipt.outputs.length > 0;
  if (!structurallyValid) {
    error = receipt ? "ingest receipt does not match selected generation" : "ingest receipt missing";
  } else {
    for (const artifact of receipt.outputs) {
      const output = safeReceiptOutput(dataDir, wiki, snapshot, artifact);
      const stat = output && statFile(output);
      if (!stat || stat.size !== artifact.bytes || !Number.isSafeInteger(artifact.rows) || artifact.rows < 0) {
        error = `invalid ingest output: ${artifact.identity || "unknown"}`;
        break;
      }
      rows += artifact.rows;
      bytes += artifact.bytes;
      validOutputs += 1;
    }
    if (!error && rows <= 0) error = "ingest receipt contains zero rows";
  }

  const activeRoots = ["parquet", "warehouse"].map((layer) => path.join(dataDir, layer, wiki, "_snapshots", snapshot));
  let inProgress = 0;
  for (const root of activeRoots) {
    const pending = [root];
    while (pending.length > 0) {
      const current = pending.pop();
      let entries = [];
      try { entries = fs.readdirSync(current, {withFileTypes: true}); } catch { continue; }
      for (const entry of entries) {
        if (entry.isDirectory()) pending.push(path.join(current, entry.name));
        else if (entry.isFile() && entry.name.endsWith(".tmp")) inProgress += 1;
      }
    }
  }

  const sources = structurallyValid
    ? new Set(receipt.inputs.map((input) => input.identity).filter((identity) => typeof identity === "string")).size
    : 0;
  return {version: snapshot, pointer_ready: 1, ingest_ready: Number(!error && inProgress === 0), rows, sources,
    outputs: validOutputs, bytes, in_progress: inProgress, error};
}

function parquetRowCounter() {
  const duckdb = require("duckdb");
  const database = new duckdb.Database(":memory:");
  return {
    count(file) {
      return new Promise((resolve, reject) => {
        database.get("SELECT count(*) AS rows FROM read_parquet(?)", file, (error, row) => {
          if (error) reject(error);
          else resolve(Number(row.rows));
        });
      });
    },
    close() { database.close(); },
  };
}

async function patrolSummary(dataDir, outputDir, wiki, required, rowCounter) {
  const sourceDir = path.join(dataDir, "patrol", wiki);
  const xml = statFile(path.join(sourceDir, `${wiki}-latest-pages-logging.xml.gz`));
  const groups = readJson(path.join(sourceDir, "autopatrol_groups.json"));
  const patrolFile = path.join(sourceDir, "patrol.parquet");
  const rightsFile = path.join(sourceDir, "rights.parquet");
  const metricFile = path.join(outputDir, wiki, "patrol.parquet");
  const sourcePatrol = statFile(patrolFile);
  const sourceRights = statFile(rightsFile);
  const metric = statFile(metricFile);
  const count = async (file, stat) => {
    if (!stat) return 0;
    try { return await rowCounter.count(file); } catch { return 0; }
  };
  const [patrolRows, rightsRows, metricRows] = await Promise.all([
    count(patrolFile, sourcePatrol), count(rightsFile, sourceRights), count(metricFile, metric),
  ]);
  const groupsReady = Array.isArray(groups?.autopatrol_groups) && groups.autopatrol_groups.length > 0;
  return {
    required: Number(required),
    xml: Number(Boolean(xml?.size)),
    events: Number(patrolRows > 0),
    event_rows: patrolRows,
    rights: Number(rightsRows > 0),
    rights_rows: rightsRows,
    groups: Number(groupsReady),
    source_ready: Number(Boolean(xml?.size && groupsReady && patrolRows > 0 && rightsRows > 0)),
    metric_ready: Number(metricRows > 0),
    metric_rows: metricRows,
  };
}

function datasetApplies(contract, wiki) {
  return contract.coverage === "all_published" || (Array.isArray(contract.wikis) && contract.wikis.includes(wiki));
}

function discoverWikis(dataDir, outputDir, lifecycle) {
  const names = new Set(Object.keys(lifecycle.wikis || {}));
  const roots = ["raw", "parquet", "warehouse", "patrol"].map((name) => path.join(dataDir, name)).concat(outputDir);
  for (const root of roots) {
    let entries = [];
    try { entries = fs.readdirSync(root, {withFileTypes: true}); } catch { continue; }
    for (const entry of entries) {
      if (entry.isDirectory() && !entry.name.startsWith("_")) names.add(entry.name);
    }
  }
  return [...names].sort();
}

async function buildManifest(options = {}) {
  const root = options.root || findRoot();
  const dataDir = options.dataDir || process.env.WIKI_ECON_DATA_DIR || path.join(root, "data");
  const outputDir = options.outputDir || process.env.WIKI_ECON_OUTPUT_DIR || path.join(root, "output");
  const lifecycleFile = options.lifecycleFile || process.env.WIKI_ECON_WIKI_LIFECYCLE_FILE || path.join(root, "config", "wiki-lifecycle.json");
  const lifecycle = options.lifecycle || readJson(lifecycleFile);
  if (!lifecycle?.publication_contract?.datasets || !lifecycle?.wikis) throw new Error(`invalid wiki lifecycle registry: ${lifecycleFile}`);
  const rowCounter = options.rowCounter || parquetRowCounter();
  const merged = fileList(outputDir);
  const mergedNames = new Set(merged.map((entry) => entry.name));
  const wikis = {};
  try {
    for (const wiki of discoverWikis(dataDir, outputDir, lifecycle)) {
      const lifecycleEntry = lifecycle.wikis[wiki] || null;
      const published = lifecycleEntry?.publication === "published";
      const scheduled = lifecycleEntry?.refresh === "scheduled";
      const expected = Object.entries(lifecycle.publication_contract.datasets)
        .filter(([, contract]) => published && datasetApplies(contract, wiki)).map(([name]) => name);
      const requiredCore = CORE_METRICS.filter((metric) => expected.includes(metric));
      const patrolRequired = expected.includes("patrol");
      const raw = rawSummary(dataDir, wiki);
      const generation = generationSummary(dataDir, wiki);
      const metrics = fileList(path.join(outputDir, wiki));
      const metricNames = new Set(metrics.map((entry) => entry.name));
      const patrol = await patrolSummary(dataDir, outputDir, wiki, patrolRequired, rowCounter);
      const missingCore = requiredCore.filter((metric) => !metricNames.has(metric));
      const missingMerged = expected.filter((metric) => !mergedNames.has(metric));
      let status = "complete";
      if (scheduled && !generation.pointer_ready && raw.files === 0) status = "needs_fetch";
      else if (scheduled && !generation.ingest_ready) status = "needs_ingest";
      else if (scheduled && patrolRequired && !patrol.source_ready) status = "needs_patrol_fetch";
      else if (missingCore.length > 0 || (expected.includes("page_weekly_edits") && !metricNames.has("page_weekly_edits"))) status = "needs_compute";
      else if (patrolRequired && !patrol.metric_ready) status = "needs_patrol_compute";
      else if (missingMerged.length > 0) status = "needs_merge";
      wikis[wiki] = {
        raw,
        snapshot: lifecycleEntry?.refresh === "paused"
          ? {version: lifecycleEntry.imported_cutoff || null, mode: "imported", ready: 1}
          : {version: generation.version, mode: "generation", ready: generation.ingest_ready},
        ingest: {ready: generation.ingest_ready, rows: generation.rows, sources: generation.sources,
          outputs: generation.outputs, size: humanBytes(generation.bytes), in_progress: generation.in_progress, error: generation.error},
        parquet: {done: generation.ingest_ready ? generation.sources : 0, total: generation.sources,
          size: humanBytes(generation.bytes), in_progress: generation.in_progress, missing: generation.error ? [generation.error] : []},
        metrics,
        dashboard: merged,
        patrol,
        status,
      };
    }
  } finally { rowCounter.close?.(); }
  return {schema_version: 2, generated_at: new Date().toISOString().replace(/\.\d{3}Z$/, "Z"),
    data_dir: dataDir, output_dir: outputDir, lifecycle, wikis, merged};
}

async function main() {
  process.stdout.write(`${JSON.stringify(await buildManifest(), null, 2)}\n`);
}

if (require.main === module) {
  main().catch((error) => {
    console.error(error.stack || error.message);
    process.exitCode = 1;
  });
}

module.exports = {buildManifest, datasetApplies, discoverWikis, generationSummary, humanBytes, patrolSummary, safeReceiptOutput};
