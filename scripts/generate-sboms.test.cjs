"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const {after, test} = require("node:test");
const {cargoGraph, makeBom, normalizeCargoLicense, propertyValue, treeSha256} = require("./generate-sboms.cjs");

const temporary = fs.mkdtempSync(path.join(os.tmpdir(), "wiki-econ-sbom-test-"));
after(() => fs.rmSync(temporary, {recursive: true, force: true}));

test("Cargo graph excludes the application from libraries but retains dependency edges", () => {
  const app = "path+file:///repo#wiki-econ@0.1.0";
  const dep = "registry+https://github.com/rust-lang/crates.io-index#anyhow@1.0.0";
  const graph = cargoGraph({
    workspace_members: [app],
    packages: [
      {id: app, name: "wiki-econ", version: "0.1.0", license: "MIT"},
      {id: dep, name: "anyhow", version: "1.0.0", license: "MIT OR Apache-2.0", repository: "https://github.com/dtolnay/anyhow"},
    ],
    resolve: {nodes: [{id: app, dependencies: [dep]}, {id: dep, dependencies: []}]},
  });
  assert.equal(graph.components.length, 1);
  assert.equal(graph.components[0].name, "anyhow");
  assert.deepEqual(graph.dependencies[0].dependsOn, ["pkg:cargo/anyhow@1.0.0"]);
});

test("legacy Cargo slash license choices become valid SPDX OR expressions", () => {
  assert.equal(normalizeCargoLicense("MIT/Apache-2.0"), "MIT OR Apache-2.0");
  assert.equal(normalizeCargoLicense("Apache-2.0 / MIT"), "Apache-2.0 OR MIT");
});

test("CycloneDX metadata carries artifact and commit identities without private helper fields", () => {
  const commit = "a".repeat(40);
  const bom = makeBom({
    artifact: "rust-binary",
    commit,
    timestamp: "2026-08-23T00:00:00.000Z",
    rootComponent: {type: "application", name: "wiki-econ", version: "0.1.0", identity: {"artifact-sha256": "b".repeat(64)}},
    components: [],
  });
  assert.equal(bom.bomFormat, "CycloneDX");
  assert.equal(propertyValue(bom, "source-commit"), commit);
  assert.equal(propertyValue(bom, "artifact-sha256"), "b".repeat(64));
  assert.equal(Object.hasOwn(bom.metadata.component, "identity"), false);
});

test("browser tree identity changes with paths as well as bytes", () => {
  const left = path.join(temporary, "left");
  const right = path.join(temporary, "right");
  fs.mkdirSync(left);
  fs.mkdirSync(right);
  fs.writeFileSync(path.join(left, "a.js"), "same");
  fs.writeFileSync(path.join(right, "b.js"), "same");
  assert.notEqual(treeSha256(left), treeSha256(right));
});
