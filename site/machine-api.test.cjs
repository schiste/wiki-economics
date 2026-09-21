#!/usr/bin/env node

const assert = require("node:assert/strict");
const crypto = require("node:crypto");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const {Readable, Writable} = require("node:stream");
const test = require("node:test");

const {createMachineApi} = require("./machine-api.cjs");

function sha256(value) {
  return crypto.createHash("sha256").update(value).digest("hex");
}

function fixture() {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "wiki-econ-machine-api-"));
  const outputDir = path.join(root, "output");
  const siteDistDir = path.join(root, "site-dist");
  fs.mkdirSync(outputDir, {recursive: true});
  fs.mkdirSync(siteDistDir, {recursive: true});
  const records = [];

  function addArtifact(name, content, mediaType = "application/octet-stream", rootDir = outputDir) {
    const file = path.join(rootDir, ...name.split("/"));
    fs.mkdirSync(path.dirname(file), {recursive: true});
    fs.writeFileSync(file, content);
    const bytes = Buffer.byteLength(content);
    records.push({name, bytes, size_kb: Math.floor(bytes / 1024), sha256: sha256(content), media_type: mediaType, license_spdx: "MIT"});
  }

  addArtifact("gdp.parquet", "merged-gdp", "application/vnd.apache.parquet");
  addArtifact("frwiki/gdp.parquet", "frwiki-gdp", "application/vnd.apache.parquet");
  addArtifact("hiddenwiki/gdp.parquet", "hidden-gdp", "application/vnd.apache.parquet");
  addArtifact("hiddenwiki/not_metric.parquet", "hidden-not-metric", "application/vnd.apache.parquet");
  addArtifact("browser-data/gdp/frwiki.parquet", "browser-gdp", "application/vnd.apache.parquet", siteDistDir);
  addArtifact("browser-data/gdp/hiddenwiki.parquet", "hidden-browser-gdp", "application/vnd.apache.parquet", siteDistDir);
  addArtifact("browser-data/gdp/all-2026.parquet", "global-gdp", "application/vnd.apache.parquet", siteDistDir);
  addArtifact("browser-data/not_metric/frwiki.parquet", "browser-not-metric", "application/vnd.apache.parquet", siteDistDir);
  addArtifact("browser-data/page_weekly_edits/frwiki.parquet", "browser-non-partitioned", "application/vnd.apache.parquet", siteDistDir);
  addArtifact("page_weekly_edits.parquet", "merged-non-partitioned", "application/vnd.apache.parquet");
  addArtifact("meta_gdp.json", "{\"dataset\":\"gdp\"}", "application/json");
  addArtifact("leak.json", "private", "application/json");
  fs.writeFileSync(path.join(outputDir, "not-allowlisted.txt"), "private");

  const manifest = {
    schema_version: 3,
    generated_at: "2026-09-20T08:00:00Z",
    license: {spdx_identifier: "MIT"},
    attribution: "Wikimedia contributors",
    independence_notice: "Independent analysis",
    source_datasets: [{name: "Wikimedia dumps"}],
    privacy: {status: "aggregated"},
    provenance: {
      generating_commit: "abc123",
      selected_snapshot_versions: {frwiki: "2026-08", hiddenwiki: "2026-08"},
      workload_profiles: {hiddenwiki: {profile: "large"}},
    },
    lifecycle: {
      wikis: {
        frwiki: {publication: "published"},
        hiddenwiki: {publication: "hidden"},
      },
    },
    wikis: {
      frwiki: {snapshot: {version: "2026-08"}, status: "complete"},
      hiddenwiki: {snapshot: {version: "2026-08"}, status: "complete"},
    },
    downloadable_artifacts: records,
  };
  const metricCatalog = {
    schema_version: 1,
    metrics: [{
      id: "gdp",
      family: "monthly",
      algorithm_version: "test-v1",
      schema: [
        {name: "year_month", data_type: "string"},
        {name: "page_namespace", data_type: "i32"},
        {name: "user_type", data_type: "string"},
        {name: "total_edits", data_type: "u32"},
        {name: "net_bytes", data_type: "i64"},
        {name: "revert_rate", data_type: "f64"},
        {name: "unique_editors", data_type: "u32"},
        {name: "wiki", data_type: "string"},
      ],
      aggregation: [
        {kind: "additive", columns: ["total_edits", "net_bytes"]},
        {kind: "ratio", columns: ["revert_rate"], numerators: ["total_edits"], denominator: "total_edits"},
        {kind: "distinct_at_grain", columns: ["unique_editors"], grain: ["wiki", "year_month", "page_namespace", "user_type"]},
      ],
      publication: {
        scope: "merged_and_per_wiki",
        per_wiki_artifact: "{wiki}/gdp.parquet",
        merged_artifact: "gdp.parquet",
      },
      fingerprint: {artifact_identity: "gdp.parquet"},
      browser: {partitioning: "per_wiki_and_global_year_shards"},
    }, {
      id: "page_weekly_edits",
      family: "page_week",
      algorithm_version: "test-v1",
      schema: [{name: "week_start", data_type: "string"}],
      aggregation: [{kind: "additive", columns: ["edits"]}],
      publication: {
        scope: "per_wiki_only",
        per_wiki_artifact: "{wiki}/page_weekly_edits.parquet",
        merged_artifact: null,
      },
      fingerprint: {artifact_identity: "page_weekly_edits.parquet"},
      browser: {partitioning: "rust_defaults_only"},
    }],
  };
  return {root, outputDir, siteDistDir, manifest, metricCatalog};
}

class MockRequest extends Readable {
  constructor({method, url, headers, body}) {
    super();
    this.method = method;
    this.url = url;
    this.headers = headers;
    this.body = body ? Buffer.from(body) : null;
    this.sent = false;
  }

  _read() {
    if (this.sent) {
      this.push(null);
      return;
    }
    this.sent = true;
    if (this.body) this.push(this.body);
    this.push(null);
  }
}

class MockResponse extends Writable {
  constructor() {
    super();
    this.statusCode = 200;
    this.headers = new Map();
    this.headersSent = false;
    this.chunks = [];
  }

  _write(chunk, _encoding, callback) {
    this.chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
    callback();
  }

  setHeader(name, value) {
    this.headers.set(String(name).toLowerCase(), value);
  }

  getHeader(name) {
    return this.headers.get(String(name).toLowerCase());
  }

  writeHead(statusCode, headers = {}) {
    this.statusCode = statusCode;
    this.headersSent = true;
    for (const [name, value] of Object.entries(headers)) this.setHeader(name, value);
    return this;
  }

  end(chunk, encoding, callback) {
    if (chunk != null) this.write(chunk, encoding);
    return super.end(callback);
  }

  text() {
    return Buffer.concat(this.chunks).toString("utf8");
  }
}

async function invoke(api, {method = "GET", url = "/", headers = {}, body = ""} = {}) {
  const request = new MockRequest({method, url, headers: {host: "machine-api.test", ...headers}, body});
  const response = new MockResponse();
  await api.handleRequest(request, response, new URL(url, "http://machine-api.test"));
  if (!response.writableFinished) await new Promise((resolve) => response.once("finish", resolve));
  return response;
}

function responseJson(response) {
  return JSON.parse(response.text());
}

function startApi(t, options = {}) {
  const data = fixture();
  for (const wiki of options.extraWikis || []) {
    data.manifest.lifecycle.wikis[wiki] = {publication: "published"};
    data.manifest.wikis[wiki] = {snapshot: {version: "2026-08"}, status: "complete"};
    data.manifest.provenance.selected_snapshot_versions[wiki] = "2026-08";
  }
  for (const metric of options.extraMetrics || []) {
    const wiki = metric.testWiki || "frwiki";
    const name = `${wiki}/${metric.id}.parquet`;
    const content = `fixture-${metric.id}`;
    const file = path.join(data.outputDir, ...name.split("/"));
    fs.mkdirSync(path.dirname(file), {recursive: true});
    fs.writeFileSync(file, content);
    data.manifest.downloadable_artifacts.push({
      name,
      bytes: Buffer.byteLength(content),
      size_kb: 0,
      sha256: sha256(content),
      media_type: "application/vnd.apache.parquet",
      license_spdx: "MIT",
    });
    data.metricCatalog.metrics.push(metric);
  }
  const api = createMachineApi({
    outputDir: data.outputDir,
    artifactDirs: [data.siteDistDir],
    manifestLoader: () => data.manifest,
    metricCatalogLoader: () => data.metricCatalog,
    freshnessLoader: () => ({status: "fresh", alerts: [], checked_at: "2026-09-20T08:00:00Z"}),
    logger: options.logger || {info() {}, warn() {}, error() {}},
    rateLimitPerSecond: options.rateLimitPerSecond,
    rateLimitMaxClients: options.rateLimitMaxClients,
    metricRowsLoader: options.metricRowsLoader,
  });
  t.after(() => fs.rmSync(data.root, {recursive: true, force: true}));
  return {api, data};
}

function publishedMetric({id, schema, aggregation, receipt = {date_column: "year_month"}, family = "monthly"}) {
  return {
    id,
    family,
    algorithm_version: "test-v1",
    schema,
    aggregation,
    publication: {
      scope: "merged_and_per_wiki",
      per_wiki_artifact: `{wiki}/${id}.parquet`,
      merged_artifact: `${id}.parquet`,
    },
    receipt,
    fingerprint: {artifact_identity: `${id}.parquet`},
    browser: {partitioning: "rust_defaults_only"},
  };
}

const INEQUALITY_FIXTURE_METRIC = publishedMetric({
  id: "inequality",
  schema: [
    {name: "year_month", data_type: "string"},
    {name: "period", data_type: "string"},
    {name: "period_start", data_type: "string"},
    {name: "period_end", data_type: "string"},
    {name: "period_type", data_type: "string"},
    {name: "period_months", data_type: "u32"},
    {name: "user_type", data_type: "string"},
    {name: "gini", data_type: "f64"},
    {name: "total_editors", data_type: "u32"},
    {name: "total_edits", data_type: "u32"},
    {name: "wiki", data_type: "string"},
  ],
  aggregation: [
    {kind: "additive", columns: ["total_edits"]},
    {kind: "distinct_at_grain", columns: ["total_editors"], grain: ["wiki", "period", "period_type", "user_type"]},
    {kind: "non_composable", columns: ["gini"]},
  ],
  receipt: {date_column: "period_start"},
});

const CHURN_FIXTURE_METRIC = publishedMetric({
  id: "labor_churn",
  family: "lifecycle",
  schema: [
    {name: "period", data_type: "string"},
    {name: "active_editors", data_type: "u32"},
    {name: "arrivals", data_type: "u32"},
    {name: "departures", data_type: "u32"},
    {name: "period_type", data_type: "string"},
    {name: "period_months", data_type: "u32"},
    {name: "arrival_rate", data_type: "f64"},
    {name: "departure_rate", data_type: "f64"},
    {name: "wiki", data_type: "string"},
  ],
  aggregation: [
    {kind: "distinct_at_grain", columns: ["active_editors", "arrivals", "departures"], grain: ["wiki", "period", "period_type"]},
    {kind: "ratio", columns: ["arrival_rate"], numerators: ["arrivals"], denominator: "active_editors"},
    {kind: "ratio", columns: ["departure_rate"], numerators: ["departures"], denominator: "active_editors"},
  ],
  receipt: {date_column: "period"},
});

const PATROL_FIXTURE_METRIC = publishedMetric({
  id: "patrol",
  schema: [
    {name: "year_month", data_type: "string"},
    {name: "total_patrols", data_type: "i64"},
    {name: "patrolled_revisions", data_type: "i64"},
    {name: "autopatrolled_revisions", data_type: "i64"},
    {name: "total_revisions", data_type: "i64"},
    {name: "patrol_coverage_pct", data_type: "f64"},
    {name: "wiki", data_type: "string"},
  ],
  aggregation: [
    {kind: "additive", columns: ["total_patrols", "patrolled_revisions", "autopatrolled_revisions", "total_revisions"]},
    {kind: "ratio", columns: ["patrol_coverage_pct"], numerators: ["patrolled_revisions"], denominator: "total_revisions"},
  ],
});

test("public catalog exposes only published artifacts and groups browser partitions", async (t) => {
  const {api} = startApi(t);
  const response = await invoke(api, {url: "/api/v1/catalog"});
  const body = responseJson(response);
  assert.equal(response.statusCode, 200);
  assert.equal(body.api_schema_version, 1);
  assert.deepEqual(body.wikis.map((wiki) => wiki.wiki), ["frwiki"]);
  assert.ok(body.artifacts.some((artifact) => artifact.name === "gdp.parquet"));
  assert.ok(body.artifacts.some((artifact) => artifact.name === "frwiki/gdp.parquet"));
  assert.ok(body.artifacts.some((artifact) => artifact.name === "browser-data/gdp/frwiki.parquet"));
  assert.equal(body.artifacts.some((artifact) => artifact.name.includes("hiddenwiki")), false);
  assert.equal(body.artifacts.some((artifact) => artifact.name.includes("not_metric")), false);
  assert.equal(body.artifacts.some((artifact) => artifact.name.includes("page_weekly_edits")), false);
  assert.equal(body.artifacts.some((artifact) => artifact.name === "leak.json"), false);
  assert.deepEqual(
    body.datasets.find((dataset) => dataset.id === "gdp").artifacts.map((artifact) => artifact.dataset),
    ["gdp", "gdp", "gdp", "gdp", "gdp"],
  );
  assert.deepEqual(body.provenance.selected_snapshot_versions, {frwiki: "2026-08"});
  assert.deepEqual(body.provenance.workload_profiles, {});
  assert.equal(body.links.openapi, "/api/v1/openapi.json");

  const openapiResponse = await invoke(api, {url: "/api/v1/openapi.json"});
  const openapi = responseJson(openapiResponse);
  assert.equal(openapiResponse.statusCode, 200);
  assert.equal(openapi.openapi, "3.1.0");
  assert.ok(openapi.paths["/api/v1/artifacts/{path}"]);
});

test("artifact endpoint enforces the manifest allow-list and supports cache/range requests", async (t) => {
  const {api} = startApi(t);
  const first = await invoke(api, {url: "/api/v1/artifacts/frwiki/gdp.parquet"});
  assert.equal(first.statusCode, 200);
  assert.equal(first.text(), "frwiki-gdp");
  const etag = first.getHeader("etag");
  assert.match(etag, /^"[0-9a-f]{64}"$/);
  assert.equal((await invoke(api, {url: "/api/v1/artifacts/frwiki/gdp.parquet", headers: {"if-none-match": etag}})).statusCode, 304);

  const range = await invoke(api, {url: "/api/v1/artifacts/frwiki/gdp.parquet", headers: {range: "bytes=1-3"}});
  assert.equal(range.statusCode, 206);
  assert.equal(range.text(), "rwi");
  assert.equal(range.getHeader("content-range"), "bytes 1-3/10");

  const browser = await invoke(api, {url: "/api/v1/artifacts/browser-data/gdp/frwiki.parquet"});
  assert.equal(browser.statusCode, 200);
  assert.equal(browser.text(), "browser-gdp");

  assert.equal((await invoke(api, {url: "/api/v1/artifacts/hiddenwiki/gdp.parquet"})).statusCode, 404);
  assert.equal((await invoke(api, {url: "/api/v1/artifacts/browser-data/gdp/hiddenwiki.parquet"})).statusCode, 404);
  assert.equal((await invoke(api, {url: "/api/v1/artifacts/browser-data/not_metric/frwiki.parquet"})).statusCode, 404);
  assert.equal((await invoke(api, {url: "/api/v1/artifacts/browser-data/page_weekly_edits/frwiki.parquet"})).statusCode, 404);
  assert.equal((await invoke(api, {url: "/api/v1/artifacts/page_weekly_edits.parquet"})).statusCode, 404);
  assert.equal((await invoke(api, {url: "/api/v1/artifacts/not-allowlisted.txt"})).statusCode, 404);
  assert.equal((await invoke(api, {url: "/api/v1/artifacts/%2e%2e%2fnot-allowlisted.txt"})).statusCode, 404);
});

test("metrics contract returns bounded JSON, summaries, compact catalog, and renderings", async (t) => {
  const rows = [
    {year_month: "2025-01", total_edits: 10, net_bytes: 100, revert_rate: 0.1, unique_editors: 2, wiki: "frwiki"},
    {year_month: "2025-02", total_edits: 12, net_bytes: 120, revert_rate: 0.2, unique_editors: 3, wiki: "frwiki"},
    {year_month: "2026-01", total_edits: 20, net_bytes: 240, revert_rate: 0.15, unique_editors: 4, wiki: "frwiki"},
    {year_month: "2026-02", total_edits: 25, net_bytes: 300, revert_rate: 0.12, unique_editors: 5, wiki: "frwiki"},
  ];
  const {api} = startApi(t, {metricRowsLoader: async () => rows});
  const response = await invoke(api, {url: "/api/v1/metrics/gdp?wiki=frwiki&from=2025-01&to=2026-02&granularity=month&limit=2"});
  const body = responseJson(response);
  assert.equal(response.statusCode, 200);
  assert.equal(body.dataset, "gdp");
  assert.equal(body.algorithm_version, "test-v1");
  assert.equal(body.value_fingerprint_algorithm, "sha256-canonical-query-rows-v1");
  assert.match(body.value_fingerprint, /^[0-9a-f]{64}$/);
  assert.equal(body.provenance.value_fingerprint, body.value_fingerprint);
  assert.ok(body.definition);
  assert.ok(body.units.total_edits);
  assert.equal(body.rows_returned, 2);
  assert.equal(body.rows_total, 4);
  assert.equal(body.truncated, true);
  assert.ok(body.next_cursor);
  assert.equal(body.summary.latest.period, "2026-02");
  assert.equal(body.summary.total.total_edits, 67);
  const metricEtag = response.getHeader("etag");
  assert.match(metricEtag, /^"[0-9a-f]{64}"$/);
  assert.equal((await invoke(api, {url: "/api/v1/metrics/gdp?wiki=frwiki&from=2025-01&to=2026-02&granularity=month&limit=2", headers: {"if-none-match": metricEtag}})).statusCode, 304);

  const csv = await invoke(api, {url: "/api/v1/metrics/gdp?wiki=frwiki&format=csv&limit=1"});
  assert.equal(csv.statusCode, 200);
  assert.match(csv.getHeader("content-type"), /^text\/csv/);
  assert.match(csv.text(), /# dataset=gdp/);
  assert.match(csv.text(), /# value_fingerprint=[0-9a-f]{64}/);
  assert.match(csv.text(), /period,total_edits/);

  const transformedParquet = await invoke(api, {url: "/api/v1/metrics/gdp?wiki=frwiki&from=2026-01&format=parquet&limit=2"});
  assert.equal(transformedParquet.statusCode, 200);
  assert.equal(transformedParquet.getHeader("content-type"), "application/vnd.apache.parquet");
  assert.ok(transformedParquet.text().length > 8);

  const compact = responseJson(await invoke(api, {url: "/api/v1/catalog?compact=true"}));
  assert.equal(compact.api_schema_version, 1);
  assert.equal(compact.counts.wikis, 1);
  assert.equal(compact.datasets[0].artifact_count > 0, true);
  assert.equal(Object.hasOwn(compact.datasets[0], "artifacts"), false);
  assert.equal(Object.hasOwn(compact.wikis[0], "artifacts"), false);
  assert.equal((await invoke(api, {url: "/api/v1/metrics/does_not_exist"})).statusCode, 404);
  assert.equal((await invoke(api, {url: "/api/v1/metrics/page_weekly_edits"})).statusCode, 404);
});

test("nulls non-additive fields above publication grain and removes them from briefings", async (t) => {
  const rows = [
    {year_month: "2025-08", page_namespace: 0, user_type: "registered", total_edits: 10, net_bytes: 100, revert_rate: 0.1, unique_editors: 4, wiki: "frwiki"},
    {year_month: "2025-08", page_namespace: 1, user_type: "registered", total_edits: 5, net_bytes: 50, revert_rate: 0.1, unique_editors: 3, wiki: "frwiki"},
    {year_month: "2026-08", page_namespace: 0, user_type: "registered", total_edits: 20, net_bytes: 200, revert_rate: 0.1, unique_editors: 5, wiki: "frwiki"},
    {year_month: "2026-08", page_namespace: 1, user_type: "registered", total_edits: 8, net_bytes: 80, revert_rate: 0.1, unique_editors: 4, wiki: "frwiki"},
  ];
  const {api} = startApi(t, {metricRowsLoader: async () => rows});
  const coarse = responseJson(await invoke(api, {url: "/api/v1/metrics/gdp?wiki=frwiki&from=2026-08&to=2026-08"}));
  assert.equal(coarse.summary.latest.unique_editors, null);
  assert.equal(coarse.summary.yoy_change.unique_editors, null);
  assert.equal(coarse.summary.total.unique_editors, null);
  assert.equal(coarse.summary.min.unique_editors, null);
  assert.equal(coarse.summary.max.unique_editors, null);
  assert.equal(coarse.summary.top_n.unique_editors, null);
  assert.ok(coarse.data_quality_flags.some((flag) => flag.code === "non_additive_fields_null" && flag.fields.includes("unique_editors")));

  const exact = responseJson(await invoke(api, {url: "/api/v1/metrics/gdp?wiki=frwiki&from=2026-08&to=2026-08&group_by=page_namespace,user_type"}));
  assert.deepEqual(exact.rows.map((row) => row.unique_editors), [5, 4]);
  const briefing = responseJson(await invoke(api, {url: "/api/v1/wikis/frwiki/briefing"}));
  const gdpHeadline = briefing.headline_metrics.find((metric) => metric.dataset === "gdp");
  assert.equal(Object.hasOwn(gdpHeadline.latest, "unique_editors"), false);
  assert.equal(Object.hasOwn(gdpHeadline.trend, "unique_editors"), false);
});

test("classifies null dimensions, labels populations, and warns on robust outliers", async (t) => {
  const rows = Array.from({length: 8}, (_, index) => ({
    year_month: `2026-${String(index + 1).padStart(2, "0")}`,
    page_namespace: null,
    user_type: null,
    total_edits: index === 4 ? 1_000 : 10,
    net_bytes: index === 4 ? 10_000 : 100,
    revert_rate: 0.1,
    unique_editors: 2,
    wiki: "frwiki",
  }));
  const {api} = startApi(t, {metricRowsLoader: async () => rows});
  const response = responseJson(await invoke(api, {url: "/api/v1/metrics/gdp?wiki=frwiki&group_by=page_namespace,user_type"}));
  assert.equal(response.rows[0].page_namespace, "unknown");
  assert.equal(response.rows[0].user_type, "unknown");
  assert.ok(response.data_quality_flags.some((flag) => flag.code === "unknown_dimension" && flag.classification === "unknown"));
  assert.ok(response.data_quality_flags.some((flag) => flag.code === "outlier_detected" && flag.field === "total_edits"));
  assert.equal(response.population_scope, "wiki_month_namespace_user_type");
  assert.equal(response.rows[0].population_scope, response.population_scope);

  const csv = await invoke(api, {url: "/api/v1/metrics/gdp?wiki=frwiki&group_by=page_namespace,user_type&format=csv"});
  assert.match(csv.text(), /# population_scope=wiki_month_namespace_user_type/);
  assert.match(csv.text(), /# aggregation_contract=/);
});

test("aligns period-start metrics, filters period types, and announces missing months", async (t) => {
  const rows = [
    {year_month: "2001-01", period: "2001", period_start: "2001-01", period_end: "2001-12", period_type: "year", period_months: 12, user_type: "registered", gini: 0.2, total_editors: 10, total_edits: 100, wiki: "frwiki"},
    {year_month: "2001-01", period: "2001-01", period_start: "2001-01", period_end: "2001-01", period_type: "month", period_months: 1, user_type: "registered", gini: 0.1, total_editors: 2, total_edits: 10, wiki: "frwiki"},
    {year_month: "2001-03", period: "2001-03", period_start: "2001-03", period_end: "2001-03", period_type: "month", period_months: 1, user_type: "registered", gini: 0.3, total_editors: 3, total_edits: 20, wiki: "frwiki"},
  ];
  const {api} = startApi(t, {extraMetrics: [INEQUALITY_FIXTURE_METRIC], metricRowsLoader: async ({metric}) => metric.id === "inequality" ? rows : []});
  const monthly = responseJson(await invoke(api, {url: "/api/v1/metrics/inequality?wiki=frwiki&from=2001-01&to=2001-03&granularity=month"}));
  assert.deepEqual(monthly.rows.map((row) => row.period), ["2001-01", "2001-03"]);
  assert.equal(monthly.coverage.minimum_date, "2001-01");
  assert.equal(monthly.coverage.maximum_date, "2001-03");
  assert.deepEqual(monthly.coverage.missing_periods, ["2001-02"]);
  assert.ok(monthly.data_quality_flags.some((flag) => flag.code === "missing_periods"));
  assert.equal(monthly.rows[0].period_type, "month");
  assert.equal(monthly.rows[0].period_months, 1);

  const yearly = responseJson(await invoke(api, {url: "/api/v1/metrics/inequality?wiki=frwiki&granularity=year"}));
  assert.deepEqual(yearly.rows.map((row) => row.period), ["2001"]);
  assert.equal(yearly.coverage.minimum_date, "2001");
  assert.equal(yearly.rows[0].period_type, "year");
});

test("exposes churn period metadata and labels partial years; patrol status is explicit", async (t) => {
  const churnRows = [
    {period: "2025", period_type: "year", active_editors: 100, arrivals: 50, departures: 45, arrival_rate: 0.5, departure_rate: 0.45, wiki: "dewiki"},
    {period: "2026", period_type: "year", active_editors: 120, arrivals: 60, departures: 120, arrival_rate: 0.5, departure_rate: 1, wiki: "dewiki"},
    {period: "2026-07", period_type: "month", active_editors: 20, arrivals: 2, departures: 9, arrival_rate: 0.1, departure_rate: 0.45, wiki: "dewiki"},
    {period: "2026-08", period_type: "month", active_editors: 20, arrivals: 2, departures: 20, arrival_rate: 0.1, departure_rate: 1, wiki: "dewiki"},
  ];
  const patrolRows = [{year_month: "2026-08", total_patrols: 0, patrolled_revisions: 10, autopatrolled_revisions: 0, total_revisions: 10, patrol_coverage_pct: 100, wiki: "dewiki"}];
  const {api} = startApi(t, {
    extraWikis: ["dewiki"],
    extraMetrics: [{...CHURN_FIXTURE_METRIC, testWiki: "dewiki"}, {...PATROL_FIXTURE_METRIC, testWiki: "dewiki"}],
    metricRowsLoader: async ({metric}) => metric.id === "labor_churn" ? churnRows : metric.id === "patrol" ? patrolRows : [],
  });
  const churn = responseJson(await invoke(api, {url: "/api/v1/metrics/labor_churn?wiki=dewiki&granularity=year"}));
  assert.deepEqual(churn.rows.map((row) => row.period), ["2025"]);
  assert.deepEqual(churn.rows.map((row) => row.period_months), [12]);
  assert.equal(churn.rows[0].period_complete, true);
  assert.equal(churn.rows[0].observed_months, 12);
  assert.ok(churn.data_quality_flags.some((flag) => flag.code === "incomplete_period_excluded" && flag.periods.includes("2026")));
  const rawChurn = responseJson(await invoke(api, {url: "/api/v1/metrics/labor_churn?wiki=dewiki&granularity=year&raw=true"}));
  assert.deepEqual(rawChurn.rows.map((row) => row.period), ["2025", "2026"]);
  assert.equal(rawChurn.rows.at(-1).period_complete, false);
  assert.ok(rawChurn.data_quality_flags.some((flag) => flag.code === "raw_incomplete_period"));
  const patrol = responseJson(await invoke(api, {url: "/api/v1/metrics/patrol?wiki=dewiki&granularity=month"}));
  assert.equal(patrol.patrol_applicability, "not_applicable");
  assert.equal(patrol.rows[0].patrol_coverage_pct, null);
  assert.equal(patrol.patrol_status, "not_applicable");
  assert.ok(patrol.data_quality_flags.some((flag) => flag.code === "patrol_not_applicable"));
  const rawArtifact = await invoke(api, {url: "/api/v1/artifacts/dewiki/labor_churn.parquet"});
  assert.equal(rawArtifact.statusCode, 200);
  assert.equal(rawArtifact.getHeader("x-wiki-econ-raw"), "true");
  assert.match(rawArtifact.getHeader("x-wiki-econ-data-quality"), /raw_incomplete_period/);
  const briefing = responseJson(await invoke(api, {url: "/api/v1/wikis/dewiki/briefing"}));
  assert.equal(briefing.patrol_status, "not_applicable");
  assert.ok(briefing.data_quality_flags.some((flag) => flag.code === "patrol_not_applicable"));
  const churnHeadline = briefing.headline_metrics.find((metric) => metric.dataset === "labor_churn");
  assert.equal(churnHeadline.latest.period, "2026-07");
  assert.equal(churnHeadline.latest.departure_rate, 0.45);
  assert.ok(briefing.data_quality_flags.some((flag) => flag.code === "incomplete_period_excluded" && flag.periods.includes("2026-08")));
});

test("publishes a value fingerprint so stable algorithm versions cannot hide value changes", async (t) => {
  const rows = [
    {year_month: "2026-01", total_edits: 20, net_bytes: 200, revert_rate: 0.1, unique_editors: 4, wiki: "frwiki"},
  ];
  const {api} = startApi(t, {metricRowsLoader: async () => rows});
  const first = responseJson(await invoke(api, {url: "/api/v1/metrics/gdp?wiki=frwiki&granularity=month"}));
  const firstEtag = (await invoke(api, {url: "/api/v1/metrics/gdp?wiki=frwiki&granularity=month"})).getHeader("etag");
  rows[0].total_edits = 21;
  const secondResponse = await invoke(api, {url: "/api/v1/metrics/gdp?wiki=frwiki&granularity=month"});
  const second = responseJson(secondResponse);
  assert.equal(first.algorithm_version, second.algorithm_version);
  assert.notEqual(first.value_fingerprint, second.value_fingerprint);
  assert.notEqual(firstEtag, secondResponse.getHeader("etag"));
});

test("metric downloads fall back to the published browser partition when the raw wiki artifact is absent", async (t) => {
  const {api, data} = startApi(t);
  fs.rmSync(path.join(data.outputDir, "frwiki", "gdp.parquet"));
  const response = await invoke(api, {url: "/api/v1/metrics/gdp?wiki=frwiki&format=parquet"});
  assert.equal(response.statusCode, 200);
  assert.equal(response.text(), "browser-gdp");
});

test("wiki briefing, metric schema/explanation, and MCP analytical tools are usable", async (t) => {
  const rows = [
    {year_month: "2026-01", total_edits: 20, net_bytes: 200, revert_rate: 0.1, unique_editors: 4, wiki: "frwiki"},
    {year_month: "2026-02", total_edits: 25, net_bytes: 300, revert_rate: 0.12, unique_editors: 5, wiki: "frwiki"},
  ];
  const {api} = startApi(t, {metricRowsLoader: async () => rows});
  const briefing = responseJson(await invoke(api, {url: "/api/v1/wikis/frwiki/briefing"}));
  assert.equal(briefing.wiki, "frwiki");
  assert.equal(briefing.freshness.status, "fresh");
  assert.ok(Array.isArray(briefing.headline_metrics));
  assert.ok(Array.isArray(briefing.drill_down_links));
  assert.ok(Array.isArray(briefing.data_quality_flags));

  const schema = responseJson(await invoke(api, {url: "/api/v1/metrics/gdp/schema"}));
  assert.equal(schema.dataset, "gdp");
  assert.ok(schema.fields.some((field) => field.name === "total_edits" && field.unit === "edits"));
  const explanation = responseJson(await invoke(api, {url: "/api/v1/metrics/gdp/explain"}));
  assert.ok(explanation.methodology);

  const call = (message) => invoke(api, {
    method: "POST", url: "/mcp", headers: {"content-type": "application/json"}, body: JSON.stringify(message),
  });
  const metric = responseJson(await call({jsonrpc: "2.0", id: 10, method: "tools/call", params: {name: "get_metric", arguments: {dataset: "gdp", wiki: "frwiki", limit: 1}}}));
  assert.equal(metric.result.isError, false);
  assert.equal(metric.result.structuredContent.rows_returned, 1);
  const mcpBriefing = responseJson(await call({jsonrpc: "2.0", id: 11, method: "tools/call", params: {name: "get_wiki_briefing", arguments: {wiki: "frwiki"}}}));
  assert.equal(mcpBriefing.result.isError, false);
  const mcpSchema = responseJson(await call({jsonrpc: "2.0", id: 12, method: "tools/call", params: {name: "get_schema", arguments: {dataset: "gdp"}}}));
  assert.equal(mcpSchema.result.isError, false);
});

test("MCP endpoint provides discovery, tools, resources, and compatibility initialization", async (t) => {
  const {api} = startApi(t);
  const call = (message, headers = {}) => invoke(api, {
    method: "POST", url: "/mcp",
    headers: {"content-type": "application/json", accept: "application/json", ...headers},
    body: JSON.stringify(message),
  });

  const initializedResponse = await call({
    jsonrpc: "2.0", id: 1, method: "initialize", params: {protocolVersion: "2026-07-28"},
  }, {"mcp-protocol-version": "2026-07-28"});
  const initialized = responseJson(initializedResponse);
  assert.equal(initializedResponse.statusCode, 200);
  assert.equal(initialized.result.protocolVersion, "2026-07-28");
  assert.equal(initializedResponse.getHeader("mcp-protocol-version"), "2026-07-28");

  const tools = responseJson(await call({jsonrpc: "2.0", id: 2, method: "tools/list"}));
  assert.ok(tools.result.tools.some((tool) => tool.name === "get_dataset"));
  assert.ok(tools.result.tools.some((tool) => tool.name === "get_metric"));
  assert.ok(tools.result.tools.some((tool) => tool.name === "read_wiki_briefing"));

  const dataset = responseJson(await call({
    jsonrpc: "2.0", id: 3, method: "tools/call",
    params: {name: "get_dataset", arguments: {dataset: "gdp", wiki: "frwiki"}},
  }, {"mcp-method": "tools/call", "mcp-name": "get_dataset"}));
  assert.equal(dataset.result.isError, false);
  assert.equal(dataset.result.structuredContent.artifacts.length, 2);

  const resource = responseJson(await call({
    jsonrpc: "2.0", id: 4, method: "resources/read",
    params: {uri: "wiki-economics://catalog"},
  }));
  const catalog = JSON.parse(resource.result.contents[0].text);
  assert.deepEqual(catalog.wikis.map((wiki) => wiki.wiki), ["frwiki"]);

  const mismatch = responseJson(await call({jsonrpc: "2.0", id: 5, method: "ping"}, {"mcp-method": "tools/list"}));
  assert.equal(mismatch.error.code, -32600);
  const oversizedBatch = await call(Array.from({length: 65}, (_, id) => ({jsonrpc: "2.0", id, method: "ping"})));
  assert.equal(oversizedBatch.statusCode, 413);
  assert.equal((await invoke(api, {url: "/mcp"})).statusCode, 405);
});

test("rate limits machine calls per client and announces the rejection", async (t) => {
  const logs = [];
  const {api} = startApi(t, {
    rateLimitPerSecond: 2,
    logger: {
      info(message) { logs.push({level: "info", message}); },
      warn(message) { logs.push({level: "warn", message}); },
      error(message) { logs.push({level: "error", message}); },
    },
  });
  const request = {url: "/api/v1", headers: {"x-real-ip": "192.0.2.10"}};
  const first = await invoke(api, request);
  const second = await invoke(api, request);
  const rejected = await invoke(api, request);
  assert.equal(first.statusCode, 200);
  assert.equal(second.statusCode, 200);
  assert.equal(rejected.statusCode, 429);
  assert.equal(responseJson(rejected).code, "rate_limited");
  assert.equal(rejected.getHeader("ratelimit-limit"), "2");
  assert.equal(rejected.getHeader("ratelimit-remaining"), "0");
  assert.equal(rejected.getHeader("ratelimit-policy"), "2;w=1");
  assert.equal(rejected.getHeader("retry-after"), "1");
  assert.equal(logs.filter((entry) => entry.level === "warn").length, 1);

  const otherClient = await invoke(api, {url: "/api/v1", headers: {"x-real-ip": "192.0.2.11"}});
  assert.equal(otherClient.statusCode, 200);
  const discovery = responseJson(otherClient);
  assert.equal(discovery.security.rate_limit.window_seconds, 1);
  assert.equal(discovery.security.rate_limit.max_tracked_clients, 4096);
  assert.equal(discovery.security.mcp.max_batch_messages, 64);
  assert.ok(logs.some((entry) => entry.level === "info" && entry.message.includes("limit=2/s")));
});
