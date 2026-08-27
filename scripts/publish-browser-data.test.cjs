"use strict";

const assert = require("node:assert/strict");
const crypto = require("node:crypto");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const {afterEach, test} = require("node:test");
const {publishBrowserData} = require("./publish-browser-data.cjs");

const roots = [];
afterEach(() => {
  while (roots.length) fs.rmSync(roots.pop(), {recursive: true, force: true});
});

function fixture() {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "wiki-econ-browser-publish-"));
  roots.push(root);
  const dataDir = path.join(root, "data");
  const distDir = path.join(root, "dist");
  const source = path.join(dataDir, "nlwiki", "gdp.parquet");
  fs.mkdirSync(path.dirname(source), {recursive: true});
  fs.writeFileSync(source, "parquet-fixture");
  const bytes = fs.statSync(source).size;
  const sha256 = crypto.createHash("sha256").update(fs.readFileSync(source)).digest("hex");
  const index = {schema_version: 3, cache_schema_version: 3, generation: "a".repeat(64), license_spdx: "MIT",
    entries: [{metric: "gdp", wiki: "nlwiki", minimum_date: "2020-01", maximum_date: "2026-07",
      file: "browser-data/gdp/nlwiki.parquet", rows: 2, bytes, sha256,
      artifact_receipt_sha256: "b".repeat(64), scope: "wiki", shard: null,
      aggregation_version: null}]};
  fs.writeFileSync(path.join(dataDir, "browser-data-index.json"), JSON.stringify(index));
  return {dataDir, distDir, index, source};
}

test("publishes exactly the allowlisted indexed partitions", () => {
  const {dataDir, distDir, source} = fixture();
  fs.mkdirSync(path.join(dataDir, "ptwiki"), {recursive: true});
  fs.writeFileSync(path.join(dataDir, "ptwiki", "gdp.parquet"), "must-not-copy");
  const files = publishBrowserData({dataDir, distDir});
  assert.deepEqual(files, ["browser-data/gdp/nlwiki.parquet", "browser-data/index.json"]);
  assert.deepEqual(fs.readFileSync(path.join(distDir, "browser-data/gdp/nlwiki.parquet")), fs.readFileSync(source));
  assert.equal(fs.existsSync(path.join(distDir, "browser-data/gdp/ptwiki.parquet")), false);
});

test("publishes receipt-indexed global year shards from their isolated source", () => {
  const fixtureData = fixture();
  const source = path.join(fixtureData.dataDir, "_browser-global", "gdp", "2026.parquet");
  fs.mkdirSync(path.dirname(source), {recursive: true});
  fs.writeFileSync(source, "global-parquet-fixture");
  const bytes = fs.statSync(source).size;
  const sha256 = crypto.createHash("sha256").update(fs.readFileSync(source)).digest("hex");
  fixtureData.index.entries = [{metric: "gdp", wiki: "all", minimum_date: "2026-01", maximum_date: "2026-07",
    file: "browser-data/gdp/all-2026.parquet", rows: 2, bytes, sha256,
    artifact_receipt_sha256: "c".repeat(64), scope: "global", shard: "2026",
    aggregation_version: "global-browser-aggregate-v1"}];
  fs.writeFileSync(path.join(fixtureData.dataDir, "browser-data-index.json"), JSON.stringify(fixtureData.index));
  const files = publishBrowserData(fixtureData);
  assert.deepEqual(files, ["browser-data/gdp/all-2026.parquet", "browser-data/index.json"]);
  assert.deepEqual(fs.readFileSync(path.join(fixtureData.distDir, "browser-data/gdp/all-2026.parquet")), fs.readFileSync(source));
});

test("fails closed on traversal and changed content", () => {
  const unsafe = fixture();
  unsafe.index.entries[0].wiki = "../escape";
  fs.writeFileSync(path.join(unsafe.dataDir, "browser-data-index.json"), JSON.stringify(unsafe.index));
  assert.throws(() => publishBrowserData(unsafe), /unsafe browser data entry/);

  const changed = fixture();
  fs.appendFileSync(changed.source, "changed");
  assert.throws(() => publishBrowserData(changed), /does not match its index/);
});
