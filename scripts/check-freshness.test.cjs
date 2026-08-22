"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const {after, test} = require("node:test");
const {DEFAULT_URL, loadPayload, parseArguments, validatePayload} = require("./check-freshness.cjs");

const directory = fs.mkdtempSync(path.join(os.tmpdir(), "wiki-econ-freshness-check-"));
after(() => fs.rmSync(directory, {recursive: true, force: true}));

test("arguments default to the public Toolforge health endpoint", () => {
  assert.deepEqual(parseArguments([]), {url: DEFAULT_URL, file: null});
  assert.deepEqual(parseArguments(["--url", "https://example.test/health"]), {url: "https://example.test/health", file: null});
  assert.throws(() => parseArguments(["--unknown"]), /unknown argument/);
});

test("file payloads support deterministic scheduled-check tests", async () => {
  const file = path.join(directory, "health.json");
  fs.writeFileSync(file, JSON.stringify({schemaVersion: 1, status: "healthy", alerts: []}));
  assert.equal((await loadPayload({file, url: DEFAULT_URL})).status, "healthy");
  assert.throws(() => validatePayload({schemaVersion: 2, status: "healthy", alerts: []}), /unsupported payload/);
});
