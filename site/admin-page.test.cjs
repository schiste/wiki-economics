"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");

const source = fs.readFileSync(path.join(__dirname, "src", "admin.md"), "utf8");
const consoleSource = fs.readFileSync(path.join(__dirname, "src", "components", "admin-console.js"), "utf8");

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

test("hidden qualifications and shared blockers have explicit human states", () => {
  assert.match(source, /Qualification ready/);
  assert.match(source, /Qualification completed and remains hidden/);
  assert.match(source, /passed \$\{qualification\?\.artifactCount/);
  assert.match(source, /One setup constraint affects/);
  assert.match(source, /not \$\{blocker\.affectedWikis\?\.length \|\| 0\} separate data failures/);
  assert.match(source, /Automatic retry is disabled because unchanged inputs would fail again/);
  assert.doesNotMatch(source, /quarantined: "Needs intervention"/);
});

test("control room makes starting work, idle workers, and pending decisions explicit", () => {
  assert.match(source, /Start or update a project/);
  assert.match(source, /Choose a project and start work/);
  assert.match(source, /Idle is normal/);
  assert.match(source, /scheduled workers sleep between checks/);
  assert.match(source, /Review and promote/);
  assert.match(source, /The request is durable and resumable/);
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

test("lifecycle console exposes safe policy, promotion, rebuild, and retirement controls", () => {
  assert.match(source, /Promote exact qualification/);
  assert.match(source, /Pause scheduling/);
  assert.match(source, /Resume scheduling/);
  assert.match(source, /Save resource &amp; SLA policy/);
  assert.match(source, /Rebuild exact snapshot/);
  assert.match(source, /Retire unpublished candidate/);
  assert.match(source, /typedOperatorConfirmation/);
  assert.match(source, /lifecycleRevision/);
  assert.match(source, /qualificationRunId/);
  assert.match(source, /candidateRunId/);
});

test("operator audit ledger exposes authenticated immutable lifecycle evidence", () => {
  assert.match(source, /## Operator audit/);
  assert.match(source, /Append-only, content-addressed evidence/);
  assert.match(source, /lifecycleAudit\.invalid/);
  assert.match(source, /event\.eventSha256/);
  assert.match(source, /Registry revision/);
});

test("admin is split into keyboard-addressable focused views", () => {
  assert.match(source, /createAdminViewNavigation/);
  for (const view of ["overview", "wikis", "runs", "quality"]) {
    assert.match(source, new RegExp(`id="admin-view-${view}"`));
    assert.match(source, new RegExp(`data-admin-view="${view}"`));
  }
  assert.match(source, /\.admin-view-navigation button:focus-visible/);
  assert.match(source, /@media \(max-width: 760px\)[\s\S]*\.admin-view-navigation/);
});

test("operator outcomes use durable inline receipts instead of blocking alerts", () => {
  assert.match(source, /readOperationReceipts\(\)/);
  assert.match(source, /persistOperationReceipts/);
  assert.match(source, /role="log" aria-live="polite"/);
  assert.match(source, /Admin authentication is required/);
  assert.doesNotMatch(source, /\balert\s*\(/);
});

test("admin uses one adaptive poll with no fixed polling intervals", () => {
  assert.match(source, /createAdaptivePoll/);
  assert.match(source, /hasActiveAdminWork/);
  assert.doesNotMatch(source, /setInterval\s*\(/);
  assert.doesNotMatch(source, /pollTimer|bgTimer/);
});

test("project dossiers expose receipt-backed controls for every pipeline stage", () => {
  assert.match(source, /deriveProjectPipelineStages/);
  assert.match(source, /class="admin-stage-ledger"/);
  for (const label of [
    "Select snapshot", "Fetch history", "Ingest metric input", "Compute core metrics",
    "Fetch patrol history", "Compute patrol metrics", "Validate candidate", "Publish",
  ]) assert.match(`${source}\n${consoleSource}`, new RegExp(label));
  assert.match(source, /Buttons are disabled when an upstream invariant makes the action unsafe/);
  assert.doesNotMatch(source, /Advanced stage controls/);
});
