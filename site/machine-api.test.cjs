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
  fs.mkdirSync(outputDir, {recursive: true});
  const records = [];

  function addArtifact(name, content, mediaType = "application/octet-stream") {
    const file = path.join(outputDir, ...name.split("/"));
    fs.mkdirSync(path.dirname(file), {recursive: true});
    fs.writeFileSync(file, content);
    const bytes = Buffer.byteLength(content);
    records.push({name, bytes, size_kb: Math.floor(bytes / 1024), sha256: sha256(content), media_type: mediaType, license_spdx: "MIT"});
  }

  addArtifact("gdp.parquet", "merged-gdp", "application/vnd.apache.parquet");
  addArtifact("frwiki/gdp.parquet", "frwiki-gdp", "application/vnd.apache.parquet");
  addArtifact("hiddenwiki/gdp.parquet", "hidden-gdp", "application/vnd.apache.parquet");
  addArtifact("browser-data/gdp/frwiki.parquet", "browser-gdp", "application/vnd.apache.parquet");
  addArtifact("browser-data/gdp/all-2026.parquet", "global-gdp", "application/vnd.apache.parquet");
  addArtifact("meta_gdp.json", "{\"dataset\":\"gdp\"}", "application/json");
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
      selected_snapshot_versions: {frwiki: "2026-08"},
      workload_profiles: {},
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
      schema: [{name: "year_month", data_type: "string"}],
      aggregation: [{kind: "additive", columns: ["total_edits"]}],
      publication: {scope: "merged_and_per_wiki"},
      browser: {partitioning: "per_wiki_and_global_year_shards"},
    }],
  };
  return {root, outputDir, manifest, metricCatalog};
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

function startApi(t) {
  const data = fixture();
  const api = createMachineApi({
    outputDir: data.outputDir,
    manifestLoader: () => data.manifest,
    metricCatalogLoader: () => data.metricCatalog,
    freshnessLoader: () => ({status: "fresh", alerts: [], checked_at: "2026-09-20T08:00:00Z"}),
  });
  t.after(() => fs.rmSync(data.root, {recursive: true, force: true}));
  return {api, data};
}

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
  assert.deepEqual(
    body.datasets.find((dataset) => dataset.id === "gdp").artifacts.map((artifact) => artifact.dataset),
    ["gdp", "gdp", "gdp", "gdp"],
  );
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

  assert.equal((await invoke(api, {url: "/api/v1/artifacts/hiddenwiki/gdp.parquet"})).statusCode, 404);
  assert.equal((await invoke(api, {url: "/api/v1/artifacts/not-allowlisted.txt"})).statusCode, 404);
  assert.equal((await invoke(api, {url: "/api/v1/artifacts/%2e%2e%2fnot-allowlisted.txt"})).statusCode, 404);
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
  assert.equal((await invoke(api, {url: "/mcp"})).statusCode, 405);
});
