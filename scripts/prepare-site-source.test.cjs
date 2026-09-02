"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const {after, test} = require("node:test");
const {prepareSiteSource} = require("./prepare-site-source.cjs");

const root = fs.mkdtempSync(path.join(os.tmpdir(), "prepare-site-source-"));
after(() => fs.rmSync(root, {recursive: true, force: true}));

test("clean site sources exclude stale state and contain only the reviewed cache", () => {
  const sourceDir = path.join(root, "source");
  const dataDir = path.join(root, "data");
  const vendorCacheDir = path.join(root, "vendor");
  const manifestPath = path.join(root, "manifest.json");
  const destinationDir = path.join(root, "prepared");
  fs.mkdirSync(path.join(sourceDir, ".observablehq", "cache"), {recursive: true});
  fs.mkdirSync(path.join(sourceDir, "data"), {recursive: true});
  fs.mkdirSync(dataDir);
  fs.mkdirSync(vendorCacheDir);
  fs.writeFileSync(path.join(sourceDir, "index.md"), "page");
  fs.writeFileSync(path.join(sourceDir, ".observablehq", "cache", "stale"), "stale");
  fs.writeFileSync(path.join(sourceDir, "data", "stale"), "stale");
  fs.writeFileSync(path.join(dataDir, "defaults.json"), "defaults");
  fs.writeFileSync(path.join(dataDir, "manifest.json"), "stale manifest");
  fs.writeFileSync(manifestPath, "current manifest");
  fs.writeFileSync(path.join(vendorCacheDir, "reviewed"), "reviewed");

  prepareSiteSource({sourceDir, destinationDir, dataDir, vendorCacheDir, manifestPath});
  assert.equal(fs.readFileSync(path.join(destinationDir, "index.md"), "utf8"), "page");
  assert.equal(fs.existsSync(path.join(destinationDir, ".observablehq", "cache", "stale")), false);
  assert.equal(fs.readFileSync(path.join(destinationDir, ".observablehq", "cache", "reviewed"), "utf8"), "reviewed");
  assert.equal(fs.readFileSync(path.join(destinationDir, "data", "defaults.json"), "utf8"), "defaults");
  assert.equal(fs.readFileSync(path.join(destinationDir, "data", "manifest.json"), "utf8"), "current manifest");
  assert.equal(fs.readFileSync(path.join(dataDir, "manifest.json"), "utf8"), "stale manifest");
});
