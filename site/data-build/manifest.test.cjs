"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const {after, test} = require("node:test");
const {buildManifest, determinismContract, generationSummary, parquetRowCounter, publicationLicensing, releaseProvenance, repositoryRootFromEnvironment, safeReceiptOutput} = require("./manifest.json.cjs");

const root = fs.mkdtempSync(path.join(os.tmpdir(), "wiki-econ-manifest-"));
const repositoryRoot = path.resolve(__dirname, "../..");
after(() => fs.rmSync(root, {recursive: true, force: true}));

const metrics = [
  "business_funnel", "gdp", "gdp_activity_tiers", "gdp_user_type_share", "inequality",
  "labor_churn", "labor_cohorts", "labor_monthly", "page_weekly_edits", "patrol",
];
const dashboardJson = [
  "defaults_business", "defaults_edit_variation", "defaults_gdp", "defaults_inequality",
  "defaults_labor", "defaults_patrol", "meta_business", "meta_gdp",
  "meta_inequality", "meta_labor", "meta_patrol",
];
const browserMetrics = metrics.filter((metric) => metric !== "page_weekly_edits");

test("detached site-source generators use the explicit runtime repository root", () => {
  const detached = path.join(root, "attested-site-source", "site", "data-build");
  fs.mkdirSync(detached, {recursive: true});
  assert.equal(
    repositoryRootFromEnvironment({WIKI_ECON_ROOT: repositoryRoot}, detached),
    repositoryRoot,
  );
  assert.throws(
    () => repositoryRootFromEnvironment({WIKI_ECON_ROOT: path.join(root, "missing-root")}, detached),
    /configured repository root is invalid/,
  );
  assert.throws(
    () => repositoryRootFromEnvironment({}, detached),
    /unable to locate repository root/,
  );
});

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
  const warehouse = path.join(dataDir, "warehouse", "nlwiki", "_snapshots", snapshot, "year=2026", "part.parquet");
  fs.mkdirSync(path.dirname(analytical), {recursive: true});
  fs.mkdirSync(path.dirname(warehouse), {recursive: true});
  fs.writeFileSync(analytical, "valid-ingest-output");
  fs.writeFileSync(warehouse, "valid-warehouse-output");
  fs.mkdirSync(path.join(dataDir, "snapshots", "nlwiki"), {recursive: true});
  fs.writeFileSync(path.join(dataDir, "snapshots", "nlwiki", "current-snapshot.json"), JSON.stringify({
    schema_version: 1, wiki: "nlwiki", snapshot_version: snapshot,
  }));
  fs.mkdirSync(path.join(dataDir, "snapshots", "nlwiki", snapshot), {recursive: true});
  fs.writeFileSync(path.join(dataDir, "snapshots", "nlwiki", snapshot, "source-plan.json"), JSON.stringify({
    schema_version: 1, wiki: "nlwiki", snapshot,
  }));
  fs.writeFileSync(path.join(dataDir, "snapshots", "nlwiki", snapshot, "workload-profile.json"), JSON.stringify({
    schema_version: 2,
    selection_algorithm_version: "adaptive-workload-profile-v2-measured",
    wiki: "nlwiki",
    snapshot,
    profile: "small",
    selection_mode: "automatic",
    signals: {
      total_compressed_bytes: 1024,
      source_count: 1,
      prior_measured_rows: 42,
      prior_fragment_count: null,
      historical_peak_memory_bytes: null,
      historical_peak_scratch_bytes: null,
      observed_throughput_rows_per_second: null,
    },
    parameters: {source_workers: 2, primary_buckets: 32, secondary_buckets: 8},
  }));
  fs.mkdirSync(path.join(dataDir, "stages", "nlwiki", snapshot), {recursive: true});
  fs.writeFileSync(path.join(dataDir, "stages", "nlwiki", snapshot, "ingest.json"), JSON.stringify({
    schema_version: 1,
    stage: "ingest",
    scope: "nlwiki",
    selected_snapshot: snapshot,
    inputs: [{identity: "raw/2026-07.nlwiki.2026"}],
    outputs: [
      {
        identity: "analytical/year=2026/part.parquet",
        bytes: fs.statSync(analytical).size,
        rows: 42,
      },
      {
        identity: "warehouse/year=2026/part.parquet",
        bytes: fs.statSync(warehouse).size,
        rows: 42,
      },
    ],
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
    if (metric !== "page_weekly_edits") fs.writeFileSync(path.join(outputDir, `${metric}.parquet`), metric);
  }
  for (const artifact of dashboardJson) {
    fs.writeFileSync(path.join(outputDir, `${artifact}.json`), "{}");
  }
  const browserEntries = browserMetrics.map((metric) => {
    const source = path.join(outputDir, "nlwiki", `${metric}.parquet`);
    const bytes = fs.statSync(source).size;
    const sha256 = require("node:crypto").createHash("sha256").update(fs.readFileSync(source)).digest("hex");
    return {metric, wiki: "nlwiki", minimum_date: "2026-01", maximum_date: "2026-07",
      file: `browser-data/${metric}/nlwiki.parquet`, rows: 5, bytes, sha256,
      artifact_receipt_sha256: "b".repeat(64), scope: "wiki", shard: null,
      aggregation_version: null};
  });
  fs.writeFileSync(path.join(outputDir, "browser-data-index.json"), JSON.stringify({
    schema_version: 3, cache_schema_version: 3, generation: "a".repeat(64), license_spdx: "MIT",
    entries: browserEntries,
  }));
  return {analytical, dataDir, outputDir, warehouse};
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

function installPatrolGeneration(dataDir, wiki, snapshot) {
  const crypto = require("node:crypto");
  const parserVersion = "patrol-logging-multigzip-monthly-v1";
  const parserIdentity = crypto.createHash("sha256").update(parserVersion).digest("hex");
  const generationRoot = path.join(dataDir, "patrol", wiki, "generations", snapshot, parserIdentity);
  const patrolFile = path.join(generationRoot, "patrol/year=2026/month=2026-07/part-00000.parquet");
  const rightsFile = path.join(generationRoot, "rights/year=2026/month=2026-07/part-00000.parquet");
  fs.mkdirSync(path.dirname(patrolFile), {recursive: true});
  fs.mkdirSync(path.dirname(rightsFile), {recursive: true});
  fs.writeFileSync(patrolFile, "monthly-patrol");
  fs.writeFileSync(rightsFile, "monthly-rights");
  const artifact = (file, relativePath, rows) => ({
    event_month: "2026-07",
    relative_path: relativePath,
    artifact_sha256: crypto.createHash("sha256").update(fs.readFileSync(file)).digest("hex"),
    bytes: fs.statSync(file).size,
    rows,
    observed_modified_unix_nanos: 1,
    ordering_contract: "timestamp-logical-fields-v1",
  });
  const generation = {
    schema_version: 2,
    wiki,
    snapshot,
    parser_version: parserVersion,
    source: {remote_url: "https://dumps.wikimedia.org/test.xml.gz", content_length: 123,
      etag: "fixture", last_modified: "Wed, 26 Aug 2026 00:00:00 GMT", downloaded_sha256: "a".repeat(64)},
    stats: {total_log_items: 15, patrol_events: 10, rights_events: 2, skipped_events: 3},
    autopatrol_groups: ["sysop"],
    patrol_months: [artifact(patrolFile, "patrol/year=2026/month=2026-07/part-00000.parquet", 10)],
    rights_months: [artifact(rightsFile, "rights/year=2026/month=2026-07/part-00000.parquet", 2)],
    rights_timeline_digest: "b".repeat(64),
    manifest_sha256: "c".repeat(64),
  };
  const manifestFile = path.join(generationRoot, "generation.json");
  fs.writeFileSync(manifestFile, `${JSON.stringify(generation, null, 2)}\n`);
  fs.writeFileSync(path.join(dataDir, "patrol", wiki, "current-generation.json"), JSON.stringify({
    schema_version: 1,
    wiki,
    snapshot,
    parser_version: parserVersion,
    manifest_relative_path: `generations/${snapshot}/${parserIdentity}/generation.json`,
    manifest_sha256: generation.manifest_sha256,
    manifest_file_sha256: crypto.createHash("sha256").update(fs.readFileSync(manifestFile)).digest("hex"),
  }));
  return {manifestFile, patrolFile};
}

function installRetentionReceipt(dataDir, wiki, snapshot) {
  const crypto = require("node:crypto");
  const plan = path.join(dataDir, "snapshots", wiki, snapshot, "source-plan.json");
  const directory = path.join(dataDir, "retention", wiki);
  fs.mkdirSync(directory, {recursive: true});
  fs.writeFileSync(path.join(directory, `${snapshot}.json`), JSON.stringify({
    schema_version: 1,
    wiki,
    snapshot,
    state: "applied",
    authorized_ready_sha256: "a".repeat(64),
    source_plan_sha256: crypto.createHash("sha256").update(fs.readFileSync(plan)).digest("hex"),
    history_input: "purge_after_ready",
    patrol_source: "purge_after_ready",
    authorized_at_unix: 1,
    applied_at_unix: 2,
    removed_bytes: 1024,
    removed_paths: [],
  }));
}

test("generation readiness follows the pointer and strict ingest receipt without raw dumps", async () => {
  const current = fixture("complete");
  const manifest = await buildManifest({
    root,
    dataDir: current.dataDir,
    outputDir: current.outputDir,
    lifecycle: lifecycle(),
    rowCounter: rows({events: 10, rights: 2, metric: 5}),
    generatedAt: "2026-08-23T12:00:00Z",
    environment: {WIKI_ECON_RUN_ID: "legal-run", WIKI_ECON_SOURCE_COMMIT: "a".repeat(40)},
  });
  assert.equal(manifest.schema_version, 3);
  assert.equal(manifest.license.spdx_identifier, "MIT");
  assert.equal(manifest.provenance.run_id, "legal-run");
  assert.equal(manifest.provenance.generating_commit, "a".repeat(40));
  assert.equal(manifest.provenance.generated_at, "2026-08-23T12:00:00Z");
  assert.deepEqual(manifest.provenance.selected_snapshot_versions, {nlwiki: "2026-07"});
  assert.equal(manifest.provenance.workload_profiles.nlwiki.profile, "small");
  assert.equal(manifest.provenance.determinism_contract.partition_hash.seed_u64, 0);
  assert.equal(manifest.wikis.nlwiki.workload_profile.parameters.primary_buckets, 32);
  assert.equal(manifest.provenance.release_environment.runtime.node, "24.15.0");
  assert.equal(manifest.provenance.release_environment.runtime.npm, "11.12.1");
  assert.equal(manifest.provenance.release_environment.runtime.rust, "1.98.0");
  assert.equal(manifest.provenance.release_environment.browser_packages.direct["apache-arrow"], "21.2.0");
  assert.equal(manifest.browser_data.entries.length, browserMetrics.length);
  assert.equal(manifest.source_datasets.length, 3);
  assert.match(manifest.attribution, /Wikimedia/);
  assert.match(manifest.trademark.status, /No trademark license is recorded/);
  assert.equal(manifest.toolforge_open_licensing.open_data_license_spdx, "MIT");
  assert.equal(manifest.merged.every((artifact) => artifact.license_spdx === "MIT"), true);
  assert.equal(manifest.downloadable_artifacts.length, metrics.length + dashboardJson.length + browserMetrics.length + 1);
  assert.equal(manifest.downloadable_artifacts.every((artifact) => artifact.license_spdx === "MIT"), true);
  assert.equal(manifest.downloadable_artifacts.some((artifact) => artifact.name === "defaults_gdp.json"), true);
  assert.equal(manifest.downloadable_artifacts.some((artifact) => artifact.name === "gdp.parquet"), true);
  assert.equal(manifest.downloadable_artifacts.some((artifact) => artifact.name === "nlwiki/page_weekly_edits.parquet"), true);
  assert.equal(manifest.wikis.nlwiki.metrics.every((artifact) => artifact.license_spdx === "MIT"), true);
  assert.equal(manifest.wikis.nlwiki.status, "complete");
  assert.equal(manifest.wikis.nlwiki.raw.files, 0);
  assert.equal(manifest.wikis.nlwiki.snapshot.version, "2026-07");
  assert.equal(manifest.wikis.nlwiki.ingest.ready, 1);
  assert.equal(manifest.wikis.nlwiki.ingest.rows, 42);
  assert.equal(manifest.wikis.nlwiki.ingest.outputs, 2);
  assert.equal(manifest.wikis.nlwiki.patrol.event_rows, 10);
  assert.equal(manifest.wikis.nlwiki.patrol.rights_rows, 2);
  assert.equal(manifest.wikis._stages, undefined);
  assert.equal(manifest.wikis[".refresh-lock"], undefined);
  assert.equal(manifest.wikis.logs, undefined);
});

test("published readiness survives policy-driven retirement of redownloadable inputs", async () => {
  const current = fixture("published-after-input-retirement");
  fs.rmSync(path.join(current.dataDir, "parquet", "nlwiki"), {recursive: true, force: true});
  fs.rmSync(path.join(current.dataDir, "warehouse", "nlwiki"), {recursive: true, force: true});
  fs.rmSync(path.join(current.dataDir, "stages", "nlwiki"), {recursive: true, force: true});
  fs.rmSync(path.join(current.dataDir, "snapshots", "nlwiki", "current-snapshot.json"), {force: true});
  fs.rmSync(path.join(current.dataDir, "patrol", "nlwiki"), {recursive: true, force: true});
  installRetentionReceipt(current.dataDir, "nlwiki", "2026-07");

  const registry = lifecycle();
  registry.wikis.nlwiki.retention = {
    source_recoverability: "redownloadable",
    history_input: "purge_after_ready",
    patrol_source: "purge_after_ready",
    computed_rollback_generations: 1,
  };
  const manifest = await buildManifest({
    root,
    dataDir: current.dataDir,
    outputDir: current.outputDir,
    lifecycle: registry,
    rowCounter: rows({events: 0, rights: 0, metric: 5}),
    generatedAt: "2026-09-02T08:00:00Z",
    environment: {WIKI_ECON_RUN_ID: "retained-publication", WIKI_ECON_SOURCE_COMMIT: "a".repeat(40)},
  });

  assert.equal(manifest.wikis.nlwiki.status, "complete");
  assert.equal(manifest.wikis.nlwiki.ingest.ready, 0);
  assert.equal(manifest.wikis.nlwiki.patrol.source_ready, 0);
  assert.equal(manifest.wikis.nlwiki.patrol.metric_ready, 1);
  assert.equal(manifest.wikis.nlwiki.snapshot.version, "2026-07");
  assert.equal(manifest.wikis.nlwiki.snapshot.mode, "retained-publication");
  assert.equal(manifest.wikis.nlwiki.retention.valid, 1);
  assert.equal(manifest.wikis.nlwiki.dashboard.length, metrics.length - 1);
});

test("retention policy without a matching receipt never hides missing inputs", async () => {
  const current = fixture("retention-policy-is-not-proof");
  fs.rmSync(path.join(current.dataDir, "parquet", "nlwiki"), {recursive: true, force: true});
  fs.rmSync(path.join(current.dataDir, "warehouse", "nlwiki"), {recursive: true, force: true});
  fs.rmSync(path.join(current.dataDir, "stages", "nlwiki"), {recursive: true, force: true});
  fs.rmSync(path.join(current.dataDir, "snapshots", "nlwiki", "current-snapshot.json"), {force: true});
  const registry = lifecycle();
  registry.wikis.nlwiki.retention = {
    source_recoverability: "redownloadable",
    history_input: "purge_after_ready",
    patrol_source: "purge_after_ready",
    computed_rollback_generations: 1,
  };
  const manifest = await buildManifest({
    root,
    dataDir: current.dataDir,
    outputDir: current.outputDir,
    lifecycle: registry,
    rowCounter: rows({events: 10, rights: 2, metric: 5}),
  });

  assert.equal(manifest.wikis.nlwiki.status, "needs_fetch");
  assert.equal(manifest.wikis.nlwiki.retention.valid, 0);
});

test("hidden qualifications never inherit another project's merged dashboard readiness", async () => {
  const current = fixture("hidden-qualification-isolation");
  const registry = lifecycle();
  registry.wikis.dewiki = {
    publication: "hidden",
    refresh: "qualification",
    provenance: "toolforge-admin:test",
  };

  const manifest = await buildManifest({
    root,
    dataDir: current.dataDir,
    outputDir: current.outputDir,
    lifecycle: registry,
    rowCounter: rows({events: 10, rights: 2, metric: 5}),
    generatedAt: "2026-09-02T08:00:00Z",
    environment: {WIKI_ECON_RUN_ID: "hidden-qualification", WIKI_ECON_SOURCE_COMMIT: "a".repeat(40)},
  });

  assert.equal(manifest.wikis.dewiki.status, "needs_fetch");
  assert.deepEqual(manifest.wikis.dewiki.dashboard, []);
});

test("patrol readiness follows the selected immutable generation after raw cleanup", async () => {
  const current = fixture("patrol-generation");
  const patrolDir = path.join(current.dataDir, "patrol", "nlwiki");
  for (const name of ["nlwiki-latest-pages-logging.xml.gz", "autopatrol_groups.json", "patrol.parquet", "rights.parquet"]) {
    fs.rmSync(path.join(patrolDir, name));
  }
  const source = installPatrolGeneration(current.dataDir, "nlwiki", "2026-07");
  const manifest = await buildManifest({
    root,
    dataDir: current.dataDir,
    outputDir: current.outputDir,
    lifecycle: lifecycle(),
    rowCounter: rows({events: 0, rights: 0, metric: 5}),
  });
  assert.equal(manifest.wikis.nlwiki.status, "complete");
  assert.equal(manifest.wikis.nlwiki.patrol.xml, 0);
  assert.equal(manifest.wikis.nlwiki.patrol.event_rows, 10);
  assert.equal(manifest.wikis.nlwiki.patrol.rights_rows, 2);
  assert.equal(manifest.wikis.nlwiki.patrol.source_generation.snapshot, "2026-07");

  fs.appendFileSync(source.manifestFile, "tampered");
  const damaged = await buildManifest({
    root,
    dataDir: current.dataDir,
    outputDir: current.outputDir,
    lifecycle: lifecycle(),
    rowCounter: rows({events: 0, rights: 0, metric: 5}),
  });
  assert.equal(damaged.wikis.nlwiki.status, "needs_patrol_fetch");
});

test("generation readiness rejects divergent analytical and warehouse row totals", () => {
  const current = fixture("divergent-layer-rows");
  const receiptPath = path.join(current.dataDir, "stages", "nlwiki", "2026-07", "ingest.json");
  const receipt = JSON.parse(fs.readFileSync(receiptPath, "utf8"));
  receipt.outputs.find((output) => output.identity.startsWith("warehouse/")).rows = 41;
  fs.writeFileSync(receiptPath, JSON.stringify(receipt));

  const summary = generationSummary(current.dataDir, "nlwiki");
  assert.equal(summary.rows, 42);
  assert.equal(summary.ingest_ready, 0);
  assert.match(summary.error, /layer row totals disagree/);
});

test("publication rejects a workload profile whose parameters do not match its name", async () => {
  const current = fixture("invalid-workload-profile");
  const file = path.join(current.dataDir, "snapshots", "nlwiki", "2026-07", "workload-profile.json");
  const profile = JSON.parse(fs.readFileSync(file, "utf8"));
  profile.parameters.primary_buckets = 64;
  fs.writeFileSync(file, JSON.stringify(profile));

  await assert.rejects(() => buildManifest({
    root,
    dataDir: current.dataDir,
    outputDir: current.outputDir,
    lifecycle: lifecycle(),
    rowCounter: rows({events: 10, rights: 2, metric: 5}),
    generatedAt: "2026-08-23T12:00:00Z",
    environment: {WIKI_ECON_RUN_ID: "invalid-profile-run", WIKI_ECON_SOURCE_COMMIT: "a".repeat(40)},
  }), /invalid workload profile/);
});

test("publication accepts retained workload profile schema v1", async () => {
  const current = fixture("legacy-workload-profile");
  const file = path.join(current.dataDir, "snapshots", "nlwiki", "2026-07", "workload-profile.json");
  const profile = JSON.parse(fs.readFileSync(file, "utf8"));
  profile.schema_version = 1;
  profile.selection_algorithm_version = "adaptive-workload-profile-v1";
  fs.writeFileSync(file, JSON.stringify(profile));

  const manifest = await buildManifest({
    root,
    dataDir: current.dataDir,
    outputDir: current.outputDir,
    lifecycle: lifecycle(),
    rowCounter: rows({events: 10, rights: 2, metric: 5}),
    generatedAt: "2026-08-23T12:00:00Z",
    environment: {WIKI_ECON_RUN_ID: "legacy-profile-run", WIKI_ECON_SOURCE_COMMIT: "a".repeat(40)},
  });
  assert.equal(manifest.wikis.nlwiki.workload_profile.schema_version, 1);
});

test("publication rejects a workload profile whose schema and algorithm disagree", async () => {
  const current = fixture("mismatched-workload-profile-version");
  const file = path.join(current.dataDir, "snapshots", "nlwiki", "2026-07", "workload-profile.json");
  const profile = JSON.parse(fs.readFileSync(file, "utf8"));
  profile.selection_algorithm_version = "adaptive-workload-profile-v1";
  fs.writeFileSync(file, JSON.stringify(profile));

  await assert.rejects(() => buildManifest({
    root,
    dataDir: current.dataDir,
    outputDir: current.outputDir,
    lifecycle: lifecycle(),
    rowCounter: rows({events: 10, rights: 2, metric: 5}),
    generatedAt: "2026-08-23T12:00:00Z",
    environment: {WIKI_ECON_RUN_ID: "mismatched-profile-run", WIKI_ECON_SOURCE_COMMIT: "a".repeat(40)},
  }), /invalid workload profile/);
});

test("publication licensing policy fails closed when required legal fields drift", () => {
  const file = path.join(root, "invalid-publication-licensing.json");
  fs.writeFileSync(file, JSON.stringify({schema_version: 1, license: {spdx_identifier: "MIT"}}));
  assert.throws(() => publicationLicensing(file), /invalid publication licensing policy/);
});

test("determinism provenance fails closed when its hash contract drifts", () => {
  const file = path.join(root, "invalid-determinism-contract.json");
  fs.writeFileSync(file, JSON.stringify({schema_version: 1, contract_version: "changed"}));
  assert.throws(() => determinismContract(file), /invalid determinism contract/);
});

test("production manifest generation requires valid observed release provenance", () => {
  assert.throws(() => releaseProvenance(repositoryRoot, {
    WIKI_ECON_ENV: "production",
    WIKI_ECON_RELEASE_PROVENANCE_FILE: path.join(root, "missing-release.json"),
  }), /production release provenance is missing/);
  const file = path.join(root, "invalid-release.json");
  fs.writeFileSync(file, JSON.stringify({schema_version: 1}));
  assert.throws(() => releaseProvenance(repositoryRoot, {
    WIKI_ECON_ENV: "production",
    WIKI_ECON_RELEASE_PROVENANCE_FILE: file,
  }), /invalid or mismatched release provenance/);

  const packageManifest = JSON.parse(fs.readFileSync(path.join(repositoryRoot, "package.json")));
  const siteManifest = JSON.parse(fs.readFileSync(path.join(repositoryRoot, "site", "package.json")));
  const closure = JSON.parse(fs.readFileSync(path.join(repositoryRoot, "config", "site-dependency-closure.json")));
  const validFile = path.join(root, "valid-release.json");
  const commit = "a".repeat(40);
  fs.writeFileSync(validFile, JSON.stringify({
    schema_version: 2,
    source_commit: commit,
    binary: {sha256: "b".repeat(64)},
    runtime: {node: "24.15.0", npm: "11.12.1", rust: "1.98.0"},
    browser_packages: {
      build_tools: packageManifest.dependencies,
      direct: siteManifest.dependencies,
      generated: closure.generated_packages,
    },
    system: {packages: {libc6: "fixture"}},
    supply_chain: {
      sbom_format: "CycloneDX 1.6",
      sboms: {rust_binary: {}, toolforge_site_image: {}, published_browser_bundle: {}},
      notices: {machine_readable: {sha256: "c".repeat(64)}, human_readable: {sha256: "d".repeat(64)}},
    },
  }));
  assert.equal(releaseProvenance(repositoryRoot, {
    WIKI_ECON_ENV: "production",
    WIKI_ECON_SOURCE_COMMIT: commit,
    WIKI_ECON_RELEASE_PROVENANCE_FILE: validFile,
  }).source_commit, commit);
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
