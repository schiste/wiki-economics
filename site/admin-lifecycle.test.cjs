#!/usr/bin/env node
"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const test = require("node:test");
const {
  applyLifecycleMutation,
  immutableAuditEvent,
  mutateRegistry,
  readAuditTrail,
  registryRevision,
} = require("./admin-lifecycle.cjs");

function registry() {
  return {
    schema_version: 1,
    publication_contract: {
      datasets: {
        gdp: {coverage: "all_published", minimum_rows_per_wiki: 1},
        patrol: {wikis: ["nlwiki"], minimum_rows_per_wiki: 1},
      },
    },
    wikis: {
      dewiki: {
        publication: "hidden",
        refresh: "qualification",
        provenance: "toolforge-admin:Alice",
        fleet_resource_class: "medium_large",
      },
      nlwiki: {
        publication: "published",
        refresh: "scheduled",
        provenance: "toolforge",
        freshness_sla_days: 10,
        fleet_resource_class: "small",
      },
    },
  };
}

test("lifecycle mutations cover promotion, pause, resume, and resource policy", () => {
  const promoted = mutateRegistry(registry(), {
    action: "promote",
    wiki: "dewiki",
    refresh: "scheduled",
    resourceClass: "isolated",
    freshnessSlaDays: 14,
  }, "Alice");
  assert.deepEqual(promoted.wikis.dewiki, {
    publication: "published",
    refresh: "scheduled",
    provenance: "toolforge-admin:Alice",
    fleet_resource_class: "isolated",
    freshness_sla_days: 14,
  });
  assert.deepEqual(promoted.publication_contract.datasets.patrol.wikis, ["dewiki", "nlwiki"]);

  const paused = mutateRegistry(promoted, {action: "pause", wiki: "dewiki"}, "Alice");
  assert.equal(paused.wikis.dewiki.refresh, "paused");
  const configured = mutateRegistry(paused, {
    action: "configure",
    wiki: "dewiki",
    resourceClass: "medium_large",
    freshnessSlaDays: 21,
  }, "Alice");
  assert.equal(configured.wikis.dewiki.fleet_resource_class, "medium_large");
  assert.equal(configured.wikis.dewiki.freshness_sla_days, 21);
  const resumed = mutateRegistry(configured, {
    action: "resume",
    wiki: "dewiki",
    refresh: "manual",
  }, "Alice");
  assert.equal(resumed.wikis.dewiki.refresh, "manual");
});

test("invalid or unsafe lifecycle transitions fail closed", () => {
  const fixture = registry();
  assert.throws(() => mutateRegistry(fixture, {action: "pause", wiki: "dewiki"}, "Alice"), /published/);
  assert.throws(() => mutateRegistry(fixture, {action: "resume", wiki: "nlwiki"}, "Alice"), /paused/);
  assert.throws(() => mutateRegistry(fixture, {action: "configure", wiki: "nlwiki"}, "Alice"), /requires/);
  assert.throws(() => mutateRegistry(fixture, {action: "configure", wiki: "nlwiki", resourceClass: "huge"}, "Alice"), /Resource class/);
  assert.throws(() => mutateRegistry(fixture, {action: "configure", wiki: "nlwiki", freshnessSlaDays: 0}, "Alice"), /Freshness SLA/);
  assert.throws(() => mutateRegistry(fixture, {action: "promote", wiki: "nlwiki"}, "Alice"), /hidden\/qualification/);
  assert.throws(() => mutateRegistry(fixture, {action: "unknown", wiki: "nlwiki"}, "Alice"), /Unsupported/);
});

test("lifecycle writes use optimistic revisions and immutable authenticated audit events", (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "wiki-econ-lifecycle-"));
  t.after(() => fs.rmSync(root, {recursive: true, force: true}));
  const lifecyclePath = path.join(root, "wiki-lifecycle.json");
  const auditDir = path.join(root, "audit");
  const initial = registry();
  fs.writeFileSync(lifecyclePath, JSON.stringify(initial));
  const expectedRevision = registryRevision(initial);

  const applied = applyLifecycleMutation({
    lifecyclePath,
    auditDir,
    mutation: {action: "pause", wiki: "nlwiki"},
    operator: "Alice",
    requestId: "lifecycle-pause-1",
    expectedRevision,
    recordedAt: "2026-09-06T10:00:00.000Z",
  });
  assert.equal(applied.current.refresh, "paused");
  assert.notEqual(applied.afterRevision, expectedRevision);
  assert.equal(fs.readdirSync(auditDir).length, 1);
  const trail = readAuditTrail(auditDir);
  assert.equal(trail.invalid.length, 0);
  assert.equal(trail.events[0].operator, "Alice");
  assert.equal(trail.events[0].beforeRevision, expectedRevision);
  assert.equal(trail.events[0].afterRevision, applied.afterRevision);

  assert.throws(() => applyLifecycleMutation({
    lifecyclePath,
    auditDir,
    mutation: {action: "resume", wiki: "nlwiki"},
    operator: "Bob",
    requestId: "lifecycle-resume-stale",
    expectedRevision,
  }), /changed since/);
  assert.equal(fs.readdirSync(auditDir).length, 1, "rejected mutations must not claim they were applied");

  const file = path.join(auditDir, fs.readdirSync(auditDir)[0]);
  const tampered = JSON.parse(fs.readFileSync(file, "utf8"));
  tampered.operator = "Mallory";
  fs.writeFileSync(file, JSON.stringify(tampered));
  const invalid = readAuditTrail(auditDir);
  assert.equal(invalid.events.length, 0);
  assert.equal(invalid.invalid.length, 1);
});

test("immutable audit writes are idempotent and never overwrite another payload", (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "wiki-econ-audit-"));
  t.after(() => fs.rmSync(root, {recursive: true, force: true}));
  const event = {
    requestId: "request-1",
    phase: "requested",
    action: "promote",
    wiki: "dewiki",
    operator: "Alice",
    recordedAt: "2026-09-06T10:00:00.000Z",
  };
  const first = immutableAuditEvent(root, event);
  const second = immutableAuditEvent(root, event);
  assert.equal(first.file, second.file);
  assert.equal(fs.readdirSync(root).length, 1);
  immutableAuditEvent(root, {...event, operator: "Bob"});
  assert.equal(fs.readdirSync(root).length, 2);
});
