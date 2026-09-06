"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");

const source = fs.readFileSync(path.join(__dirname, "src", "admin.md"), "utf8");

test("admin uses authoritative metric completeness instead of an artifact-count threshold", () => {
  assert.doesNotMatch(source, /metrics\s*\|\|\s*\[\]\)\.length\s*>=\s*\d+/);
  assert.match(source, /metricCompletenessForMilestone/);
  assert.match(source, /missing \$\{metricTruth\.missing\.join/);
});

test("admin separates public, pipeline, and infrastructure health", () => {
  assert.match(source, /<dt>Public data<\/dt>/);
  assert.match(source, /<dt>Update pipeline<\/dt>/);
  assert.match(source, /<dt>Infrastructure<\/dt>/);
  assert.match(source, /operationalTruth\.public/);
  assert.match(source, /operationalTruth\.pipeline/);
  assert.match(source, /operationalTruth\.infrastructure/);
});

test("admin exposes atomic publication and never labels merge as publication", () => {
  assert.match(source, />Publish ready candidates<\/button>/);
  assert.match(source, /runCommand\("publish"\)/);
  assert.match(source, />Regenerate merged artifacts(?: only)?<\/button>/);
  assert.doesNotMatch(source, />Publish site data<\/button>/);
});

test("project status shows available, candidate, and published snapshots together", () => {
  assert.match(source, /Latest available/);
  assert.match(source, /Candidate/);
  assert.match(source, /Published \/ cutoff/);
  assert.match(source, /Published \$\{snapshots\.published/);
});
