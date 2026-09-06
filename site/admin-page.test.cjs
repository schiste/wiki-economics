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

test("admin run details expose timelines, resource evidence, and specific next actions", () => {
  assert.match(source, /class="admin-run-sheet"/);
  assert.match(source, /class="admin-run-timeline"/);
  assert.match(source, />Memory peak</);
  assert.match(source, />CPU time</);
  assert.match(source, />Diagnosis</);
  assert.match(source, /selectedRunTruth\.allowedActions/);
  assert.match(source, /searchParams\.set\("run"/);
});

test("publication controls require a current preflight and expose the change plan", () => {
  assert.match(source, />Run publication preflight</);
  assert.match(source, /preflightCanPublish/);
  assert.match(source, /Run a current, passing publication preflight first/);
  assert.match(source, /class="admin-change-plan"/);
  assert.match(source, />Will rebuild</);
  assert.match(source, />Will reuse</);
});

test("recovery workbench exposes audit, fleet recovery, quarantine retry, and scrub", () => {
  assert.match(source, />Audit publication recovery</);
  assert.match(source, />Recover stale fleet leases</);
  assert.match(source, />Scrub published artifacts</);
  assert.match(source, /quarantine-retry/);
});

test("quality ledger exposes receipt evidence, candidate deltas, and anomaly signals", () => {
  assert.match(source, /## Data quality/);
  assert.match(source, /admin-quality-table/);
  assert.match(source, /Published evidence/);
  assert.match(source, /Candidate evidence/);
  assert.match(source, /Schema \$\{evidence\.schema/);
  assert.match(source, /Algorithm \$\{evidence\.algorithmMatches/);
  assert.match(source, /receipt \$\{qualityHash/);
  assert.match(source, /quality\.signals/);
  assert.match(source, /quality\.anomalies/);
});
