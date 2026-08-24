#!/usr/bin/env node
"use strict";

const fs = require("node:fs");
const path = require("node:path");
const crypto = require("node:crypto");

const CORE_METRICS = [
  "business_funnel", "gdp", "gdp_activity_tiers", "gdp_user_type_share",
  "inequality", "labor_churn", "labor_cohorts", "labor_monthly",
];
const PUBLIC_JSON_ARTIFACTS = [
  "defaults_business", "defaults_edit_variation", "defaults_gdp",
  "defaults_inequality", "defaults_labor", "defaults_patrol",
  "meta_business", "meta_gdp", "meta_inequality", "meta_labor", "meta_patrol",
];
const ARTIFACT_LICENSE_SPDX = "MIT";
const BROWSER_INDEX = "browser-data-index.json";

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

function publicationLicensing(file = path.join(findRoot(), "config", "publication-licensing.json")) {
  const policy = readJson(file);
  if (policy?.schema_version !== 1
      || policy?.license?.spdx_identifier !== ARTIFACT_LICENSE_SPDX
      || !Array.isArray(policy?.source_datasets)
      || policy.source_datasets.length === 0
      || typeof policy?.attribution !== "string"
      || typeof policy?.independence_notice !== "string"
      || typeof policy?.trademark?.status !== "string"
      || policy?.toolforge?.open_source_license_spdx !== ARTIFACT_LICENSE_SPDX
      || policy?.toolforge?.open_data_license_spdx !== ARTIFACT_LICENSE_SPDX) {
    throw new Error(`invalid publication licensing policy: ${file}`);
  }
  return policy;
}

function determinismContract(file = path.join(findRoot(), "config", "determinism-contract.json")) {
  const contract = readJson(file);
  if (contract?.schema_version !== 1
      || contract?.contract_version !== "pipeline-byte-determinism-v1"
      || contract?.digest_algorithm !== "SHA-256"
      || contract?.partition_hash?.algorithm !== "splitmix64-finalizer"
      || contract?.partition_hash?.version !== 1
      || contract?.partition_hash?.seed_u64 !== 0
      || typeof contract?.source_order !== "string" || contract.source_order.length === 0
      || typeof contract?.fragment_order !== "string" || contract.fragment_order.length === 0
      || typeof contract?.fragment_row_order !== "string" || contract.fragment_row_order.length === 0
      || typeof contract?.final_merge_order !== "string" || contract.final_merge_order.length === 0
      || contract?.parquet_metadata_policy !== "no-wall-clock-fields-v1") {
    throw new Error(`invalid determinism contract: ${file}`);
  }
  return contract;
}

function repositoryRuntimeProvenance(root) {
  const packageManifest = readJson(path.join(root, "package.json"));
  const siteManifest = readJson(path.join(root, "site", "package.json"));
  const closure = readJson(path.join(root, "config", "site-dependency-closure.json"));
  const rust = fs.readFileSync(path.join(root, "rust-toolchain.toml"), "utf8").match(/channel\s*=\s*"([^"]+)"/)?.[1];
  return {
    schema_version: 1,
    source: "repository-pins",
    runtime: {node: packageManifest?.engines?.node, npm: packageManifest?.engines?.npm, rust},
    browser_packages: {
      build_tools: packageManifest?.dependencies || {},
      direct: siteManifest?.dependencies || {},
      generated: closure?.generated_packages || {},
    },
    system: {status: "not-applicable-to-deterministic-fixture"},
  };
}

function releaseProvenance(root, environment) {
  const binary = environment.WIKI_ECON_BIN;
  const file = environment.WIKI_ECON_RELEASE_PROVENANCE_FILE
    || (binary ? path.join(path.dirname(binary), "release-provenance.json") : null);
  const provenance = file ? readJson(file) : null;
  if (!provenance) {
    if (environment.WIKI_ECON_ENV === "production") {
      throw new Error(`production release provenance is missing: ${file || "WIKI_ECON_RELEASE_PROVENANCE_FILE"}`);
    }
    return repositoryRuntimeProvenance(root);
  }
  const pins = repositoryRuntimeProvenance(root);
  const expectedCommit = environment.WIKI_ECON_SOURCE_COMMIT || environment.WIKI_ECON_BUILD_COMMIT;
  if (provenance.schema_version !== 2
      || !/^[0-9a-f]{40}$/.test(provenance.source_commit || "")
      || (expectedCommit && provenance.source_commit !== expectedCommit)
      || !/^[0-9a-f]{64}$/.test(provenance.binary?.sha256 || "")
      || provenance.runtime?.node !== pins.runtime.node
      || provenance.runtime?.npm !== pins.runtime.npm
      || provenance.runtime?.rust !== pins.runtime.rust
      || JSON.stringify(provenance.browser_packages?.build_tools) !== JSON.stringify(pins.browser_packages.build_tools)
      || JSON.stringify(provenance.browser_packages?.direct) !== JSON.stringify(pins.browser_packages.direct)
      || JSON.stringify(provenance.browser_packages?.generated) !== JSON.stringify(pins.browser_packages.generated)
      || !provenance.system?.packages
      || Object.keys(provenance.system.packages).length === 0
      || provenance.supply_chain?.sbom_format !== "CycloneDX 1.6"
      || Object.keys(provenance.supply_chain?.sboms || {}).length !== 3
      || !provenance.supply_chain?.notices?.machine_readable?.sha256
      || !provenance.supply_chain?.notices?.human_readable?.sha256) {
    throw new Error(`invalid or mismatched release provenance: ${file}`);
  }
  return provenance;
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
        return {
          name: entry.name.slice(0, -extension.length),
          size_kb: Math.floor(stat.size / 1024),
          license_spdx: ARTIFACT_LICENSE_SPDX,
        };
      })
      .sort((left, right) => left.name.localeCompare(right.name));
  } catch { return []; }
}

function browserDataSummary(outputDir) {
  const indexPath = path.join(outputDir, BROWSER_INDEX);
  const index = readJson(indexPath);
  if (index?.schema_version !== 1
      || index?.cache_schema_version !== 1
      || !/^[0-9a-f]{64}$/.test(index?.generation || "")
      || index?.license_spdx !== ARTIFACT_LICENSE_SPDX
      || !Array.isArray(index?.entries)
      || index.entries.length === 0) {
    throw new Error(`invalid browser data index: ${indexPath}`);
  }
  const identities = new Set();
  const artifacts = index.entries.map((entry) => {
    if (!/^[a-z0-9_]+$/.test(entry?.metric || "")
        || !/^[a-z0-9_]+wiki$/.test(entry?.wiki || "")
        || entry.file !== `browser-data/${entry.metric}/${entry.wiki}.parquet`
        || typeof entry.minimum_date !== "string"
        || typeof entry.maximum_date !== "string"
        || !Number.isSafeInteger(entry.rows) || entry.rows <= 0
        || !Number.isSafeInteger(entry.bytes) || entry.bytes <= 0
        || !/^[0-9a-f]{64}$/.test(entry.sha256 || "")) {
      throw new Error(`invalid browser data entry: ${JSON.stringify(entry)}`);
    }
    const identity = `${entry.metric}/${entry.wiki}`;
    if (identities.has(identity)) throw new Error(`duplicate browser data entry: ${identity}`);
    identities.add(identity);
    const source = path.join(outputDir, entry.wiki, `${entry.metric}.parquet`);
    const stat = statFile(source);
    if (!stat || stat.size !== entry.bytes) throw new Error(`browser source size mismatch: ${source}`);
    const sha256 = crypto.createHash("sha256").update(fs.readFileSync(source)).digest("hex");
    if (sha256 !== entry.sha256) throw new Error(`browser source hash mismatch: ${source}`);
    return {
      name: entry.file,
      size_kb: Math.floor(entry.bytes / 1024),
      bytes: entry.bytes,
      rows: entry.rows,
      sha256: entry.sha256,
      license_spdx: ARTIFACT_LICENSE_SPDX,
      media_type: "application/vnd.apache.parquet",
    };
  });
  artifacts.push({
    name: BROWSER_INDEX,
    size_kb: Math.floor(fs.statSync(indexPath).size / 1024),
    license_spdx: ARTIFACT_LICENSE_SPDX,
    media_type: "application/json",
  });
  return {index, artifacts};
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

function workloadProfile(dataDir, wiki, snapshot) {
  if (!snapshot) return null;
  const file = path.join(dataDir, "snapshots", wiki, snapshot, "workload-profile.json");
  if (!fs.existsSync(file)) return null;
  const profile = readJson(file);
  const parameters = profile?.parameters;
  const expectedParameters = profile?.profile === "small"
    ? {source_workers: 2, primary_buckets: 32, secondary_buckets: 8}
    : profile?.profile === "large"
      ? {source_workers: 3, primary_buckets: 64, secondary_buckets: 32}
      : null;
  if (profile?.schema_version !== 1 || profile.wiki !== wiki || profile.snapshot !== snapshot
      || !["small", "large"].includes(profile.profile)
      || !["automatic", "manual_qualification_override"].includes(profile.selection_mode)
      || !Number.isSafeInteger(profile?.signals?.total_compressed_bytes)
      || profile.signals.total_compressed_bytes <= 0
      || !Number.isSafeInteger(profile?.signals?.source_count) || profile.signals.source_count <= 0
      || !Number.isSafeInteger(parameters?.source_workers) || parameters.source_workers <= 0
      || !Number.isSafeInteger(parameters?.primary_buckets) || parameters.primary_buckets <= 0
      || !Number.isSafeInteger(parameters?.secondary_buckets) || parameters.secondary_buckets <= 0
      || JSON.stringify(parameters) !== JSON.stringify(expectedParameters)) {
    throw new Error(`invalid workload profile: ${file}`);
  }
  return profile;
}

function parquetRowCounter(countsFile = process.env.WIKI_ECON_PARQUET_ROW_COUNTS_FILE) {
  if (!countsFile) throw new Error("WIKI_ECON_PARQUET_ROW_COUNTS_FILE is required");
  const counts = readJson(countsFile);
  if (!counts || Array.isArray(counts) || typeof counts !== "object") {
    throw new Error(`invalid Rust Parquet row-count map: ${countsFile}`);
  }
  return {
    async count(file) {
      const rows = counts[file];
      if (!Number.isSafeInteger(rows) || rows < 0) {
        throw new Error(`Rust row-count map has no valid entry for ${file}`);
      }
      return rows;
    },
    async close() {},
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
      if (entry.isDirectory() && /^[a-z0-9_]+wiki$/i.test(entry.name)) names.add(entry.name);
    }
  }
  return [...names].sort();
}

async function buildManifest(options = {}) {
  const root = options.root || findRoot();
  const repositoryRoot = options.repositoryRoot || findRoot();
  const dataDir = options.dataDir || process.env.WIKI_ECON_DATA_DIR || path.join(root, "data");
  const outputDir = options.outputDir || process.env.WIKI_ECON_OUTPUT_DIR || path.join(root, "output");
  const lifecycleFile = options.lifecycleFile || process.env.WIKI_ECON_WIKI_LIFECYCLE_FILE || path.join(root, "config", "wiki-lifecycle.json");
  const lifecycle = options.lifecycle || readJson(lifecycleFile);
  const licensing = options.licensing || publicationLicensing(options.licensingFile);
  const environment = options.environment || process.env;
  const generatedAt = options.generatedAt || new Date().toISOString().replace(/\.\d{3}Z$/, "Z");
  if (!lifecycle?.publication_contract?.datasets || !lifecycle?.wikis) throw new Error(`invalid wiki lifecycle registry: ${lifecycleFile}`);
  const rowCounter = options.rowCounter || parquetRowCounter();
  const merged = fileList(outputDir);
  const dashboardJson = fileList(outputDir, ".json")
    .filter((artifact) => PUBLIC_JSON_ARTIFACTS.includes(artifact.name));
  const browserData = browserDataSummary(outputDir);
  const downloadableArtifacts = [
    ...merged.map((artifact) => ({...artifact, name: `${artifact.name}.parquet`, media_type: "application/vnd.apache.parquet"})),
    ...dashboardJson.map((artifact) => ({...artifact, name: `${artifact.name}.json`, media_type: "application/json"})),
    ...browserData.artifacts,
  ].sort((left, right) => left.name.localeCompare(right.name));
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
      const selectedProfile = workloadProfile(dataDir, wiki, generation.version);
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
        workload_profile: selectedProfile,
        status,
      };
    }
  } finally { await rowCounter.close?.(); }
  const selectedSnapshots = Object.fromEntries(Object.entries(wikis)
    .filter(([, wiki]) => typeof wiki.snapshot?.version === "string")
    .map(([name, wiki]) => [name, wiki.snapshot.version]));
  const workloadProfiles = Object.fromEntries(Object.entries(wikis)
    .filter(([, wiki]) => wiki.workload_profile)
    .map(([name, wiki]) => [name, wiki.workload_profile]));
  return {
    schema_version: 3,
    generated_at: generatedAt,
    license: licensing.license,
    attribution: licensing.attribution,
    independence_notice: licensing.independence_notice,
    source_datasets: licensing.source_datasets,
    trademark: licensing.trademark,
    privacy: licensing.privacy,
    toolforge_open_licensing: licensing.toolforge,
    provenance: {
      run_id: environment.WIKI_ECON_RUN_ID || null,
      generating_commit: environment.WIKI_ECON_SOURCE_COMMIT || environment.WIKI_ECON_BUILD_COMMIT || null,
      generated_at: generatedAt,
      selected_snapshot_versions: selectedSnapshots,
      workload_profiles: workloadProfiles,
      determinism_contract: determinismContract(path.join(repositoryRoot, "config", "determinism-contract.json")),
      release_environment: releaseProvenance(repositoryRoot, environment),
    },
    data_dir: dataDir,
    output_dir: outputDir,
    lifecycle,
    wikis,
    merged,
    browser_data: browserData.index,
    downloadable_artifacts: downloadableArtifacts,
  };
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

module.exports = {BROWSER_INDEX, browserDataSummary, buildManifest, datasetApplies, determinismContract, discoverWikis, generationSummary, humanBytes, parquetRowCounter,
  patrolSummary, publicationLicensing, releaseProvenance, repositoryRuntimeProvenance, safeReceiptOutput};
