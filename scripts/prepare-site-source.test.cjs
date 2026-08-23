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
  const destinationDir = path.join(root, "prepared");
  fs.mkdirSync(path.join(sourceDir, ".observablehq", "cache"), {recursive: true});
  fs.mkdirSync(path.join(sourceDir, "data"), {recursive: true});
  fs.mkdirSync(dataDir);
  fs.mkdirSync(vendorCacheDir);
  fs.writeFileSync(path.join(sourceDir, "index.md"), "page");
  fs.writeFileSync(path.join(sourceDir, ".observablehq", "cache", "stale"), "stale");
  fs.writeFileSync(path.join(sourceDir, "data", "stale"), "stale");
  fs.writeFileSync(path.join(vendorCacheDir, "reviewed"), "reviewed");

  prepareSiteSource({sourceDir, destinationDir, dataDir, vendorCacheDir});
  assert.equal(fs.readFileSync(path.join(destinationDir, "index.md"), "utf8"), "page");
  assert.equal(fs.existsSync(path.join(destinationDir, ".observablehq", "cache", "stale")), false);
  assert.equal(fs.readFileSync(path.join(destinationDir, ".observablehq", "cache", "reviewed"), "utf8"), "reviewed");
  assert.equal(fs.realpathSync(path.join(destinationDir, "data")), fs.realpathSync(dataDir));
});
