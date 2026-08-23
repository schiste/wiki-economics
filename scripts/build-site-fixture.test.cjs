"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const {after, test} = require("node:test");
const {
  REQUIRED_ATTACHMENTS,
  REQUIRED_PAGES,
  listFiles,
  parseArguments,
  verifyBuild,
} = require("./build-site-fixture.cjs");

const root = fs.mkdtempSync(path.join(os.tmpdir(), "wiki-econ-built-site-"));
after(() => fs.rmSync(root, {recursive: true, force: true}));

test("built site verification requires every page and hashed attachment", () => {
  const dist = path.join(root, "complete");
  const data = path.join(dist, "_file", "data");
  fs.mkdirSync(data, {recursive: true});
  for (const page of REQUIRED_PAGES) fs.writeFileSync(path.join(dist, page), "<!doctype html>");
  for (const attachment of REQUIRED_ATTACHMENTS) {
    const extension = path.extname(attachment);
    const stem = attachment.slice(0, -extension.length);
    fs.writeFileSync(path.join(data, `${stem}.deadbeef${extension}`), "fixture");
  }

  assert.deepEqual(verifyBuild(dist), listFiles(dist));
  fs.rmSync(path.join(data, "meta_patrol.deadbeef.json"));
  assert.throws(() => verifyBuild(dist), /missing data attachment meta_patrol.json/);
});

test("fixture CLI arguments are strict and absolute", () => {
  const options = parseArguments(["--data-dir", "data", "--dist-dir", "dist"]);
  assert.equal(options["data-dir"], path.resolve("data"));
  assert.equal(options["dist-dir"], path.resolve("dist"));
  assert.throws(() => parseArguments(["--unknown", "value"]), /unknown argument/);
  assert.throws(() => parseArguments(["--data-dir", "data"]), /usage/);
});
