"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const {afterEach, test} = require("node:test");
const {packageIdentity, runtimeRemoteReferences, verifySiteDependencies} = require("./verify-site-dependencies.cjs");

const roots = [];
afterEach(() => {
  while (roots.length) fs.rmSync(roots.pop(), {recursive: true, force: true});
});

function fixture() {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "site-closure-"));
  roots.push(root);
  const dist = path.join(root, "dist");
  fs.mkdirSync(path.join(dist, "_npm", "d3@7.9.0"), {recursive: true});
  fs.writeFileSync(path.join(dist, "_npm", "d3@7.9.0", "module.js"), "export default 1;");
  fs.writeFileSync(path.join(dist, "index.html"), '<script type="module" src="./app.js"></script><a href="https://example.org">source</a>');
  const closureFile = path.join(root, "closure.json");
  const siteManifestFile = path.join(root, "package.json");
  const lockFile = path.join(root, "package-lock.json");
  fs.writeFileSync(closureFile, JSON.stringify({
    schema_version: 1,
    direct_browser_packages: {d3: "7.9.0"},
    generated_packages: {d3: "7.9.0"},
    allowed_wasm: [],
    forbidden_asset_patterns: ["_duckdb/", "_npm/@duckdb/"],
  }));
  fs.writeFileSync(siteManifestFile, JSON.stringify({dependencies: {d3: "7.9.0"}}));
  fs.writeFileSync(lockFile, JSON.stringify({packages: {"node_modules/d3": {version: "7.9.0"}}}));
  return {dist, closureFile, siteManifestFile, lockFile, vendorCacheDir: null};
}

test("package identities support scoped and unscoped generated paths", () => {
  assert.deepEqual(packageIdentity("_npm/d3@7.9.0/a.js"), {name: "d3", version: "7.9.0"});
  assert.deepEqual(packageIdentity("_npm/@observablehq/plot@0.6.17/a.js"), {name: "@observablehq/plot", version: "0.6.17"});
  assert.equal(packageIdentity("_observablehq/client.js"), null);
});

test("the exact generated closure accepts ordinary hyperlinks", () => {
  const current = fixture();
  assert.deepEqual(verifySiteDependencies(current.dist, current).packages, {d3: "7.9.0"});
});

test("undeclared packages, version drift, DuckDB, and remote runtime assets fail closed", () => {
  const current = fixture();
  fs.mkdirSync(path.join(current.dist, "_npm", "left-pad@1.3.0"), {recursive: true});
  fs.writeFileSync(path.join(current.dist, "_npm", "left-pad@1.3.0", "index.js"), "export default 1;");
  assert.throws(() => verifySiteDependencies(current.dist, current), /undeclared generated browser package/);
  fs.rmSync(path.join(current.dist, "_npm", "left-pad@1.3.0"), {recursive: true});
  fs.mkdirSync(path.join(current.dist, "_npm", "@duckdb", "duckdb-wasm@1.29.0"), {recursive: true});
  fs.writeFileSync(path.join(current.dist, "_npm", "@duckdb", "duckdb-wasm@1.29.0", "duckdb.wasm"), "wasm");
  assert.throws(() => verifySiteDependencies(current.dist, current), /unexpected DuckDB asset/);
  fs.rmSync(path.join(current.dist, "_npm", "@duckdb"), {recursive: true});
  fs.writeFileSync(path.join(current.dist, "app.js"), 'import("https://cdn.example/module.js")');
  assert.throws(() => verifySiteDependencies(current.dist, current), /remote runtime dependency/);
});

test("remote dependency detection ignores prose and flags executable resource loads", () => {
  assert.deepEqual(runtimeRemoteReferences("index.html", '<a href="https://example.org">link</a>'), []);
  assert.deepEqual(runtimeRemoteReferences("index.html", '<script src="https://cdn.example/app.js"></script>'), ["https://cdn.example/app.js"]);
});
