#!/usr/bin/env node
"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const test = require("node:test");

const {
  buildStackReference,
  checkStackReference,
  validateNarrativeDocs,
} = require("./generate-stack-reference.cjs");

test("stack reference is deterministic and reflects authoritative metadata", () => {
  const first = buildStackReference();
  const second = buildStackReference();
  assert.equal(first, second);
  assert.match(first, /\| `polars` \| `\d+\.\d+\.\d+` \|/);
  assert.match(first, /\| `apache-arrow` \| `\d+\.\d+\.\d+` \|/);
  assert.match(first, /Scheduled\s+datasets are `frwiki`, `nlwiki`, `ptwiki`/);
  assert.match(first, /paused imported datasets are none/);
});

test("stale checked-in generated output fails closed", (context) => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "wiki-econ-stack-reference-"));
  context.after(() => fs.rmSync(directory, {recursive: true, force: true}));
  const output = path.join(directory, "stack-reference.md");
  fs.writeFileSync(output, "stale\n");
  assert.throws(() => checkStackReference(buildStackReference(), output), /is stale/);
});

test("narrative dependency versions and retired patrol claims are rejected", (context) => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "wiki-econ-doc-drift-"));
  context.after(() => fs.rmSync(directory, {recursive: true, force: true}));
  const versioned = path.join(directory, "versioned.md");
  const patrol = path.join(directory, "patrol.md");
  fs.writeFileSync(versioned, "We use Polars 0.1.0.\n");
  fs.writeFileSync(patrol, "The Python sidecar pipeline computes patrol data.\n");
  assert.throws(() => validateNarrativeDocs([versioned]), /dependency versions belong/);
  assert.throws(() => validateNarrativeDocs([patrol]), /stale production-path claim/);
});
