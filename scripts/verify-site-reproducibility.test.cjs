"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const {after, test} = require("node:test");
const {artifactHashes, compareArtifactHashes, parseArguments} = require("./verify-site-reproducibility.cjs");

const root = fs.mkdtempSync(path.join(os.tmpdir(), "site-repro-test-"));
after(() => fs.rmSync(root, {recursive: true, force: true}));

test("artifact hash comparison is path-ordered and byte-sensitive", () => {
  const left = path.join(root, "left");
  const right = path.join(root, "right");
  fs.mkdirSync(left);
  fs.mkdirSync(right);
  fs.writeFileSync(path.join(left, "index.html"), "same");
  fs.writeFileSync(path.join(right, "index.html"), "same");
  assert.deepEqual(compareArtifactHashes(artifactHashes(left), artifactHashes(right)), ["index.html"]);
  fs.writeFileSync(path.join(right, "index.html"), "different");
  assert.throws(() => compareArtifactHashes(artifactHashes(left), artifactHashes(right)), /not byte-for-byte deterministic/);
});

test("reproducibility CLI requires both absolute paths", () => {
  assert.deepEqual(parseArguments(["--data-dir", "data", "--work-dir", "work"]), {
    "data-dir": path.resolve("data"),
    "work-dir": path.resolve("work"),
  });
  assert.throws(() => parseArguments(["--data-dir", "data"]), /usage/);
});
