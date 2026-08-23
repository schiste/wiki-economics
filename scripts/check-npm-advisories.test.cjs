"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const {after, test} = require("node:test");
const {advisoryId, auditGraph, loadExceptions, validateAuditReport} = require("./check-npm-advisories.cjs");

const root = fs.mkdtempSync(path.join(os.tmpdir(), "wiki-econ-audit-policy-"));
after(() => fs.rmSync(root, {recursive: true, force: true}));

function report(vulnerabilities = {}) {
  return {
    auditReportVersion: 2,
    vulnerabilities,
    metadata: {vulnerabilities: {total: Object.keys(vulnerabilities).length}},
  };
}

const esbuild = {
  severity: "low",
  via: [{url: "https://github.com/advisories/GHSA-g7r4-m6w7-qqqr"}],
};

test("a clean report passes with no exceptions", () => {
  const result = validateAuditReport(report(), "fixture");
  assert.deepEqual(result.allowedLow, []);
  assert.equal(advisoryId(esbuild.via[0].url), "GHSA-g7r4-m6w7-qqqr");
});

test("moderate advisories and new low advisories fail closed", () => {
  assert.throws(
    () => validateAuditReport(report({tar: {severity: "critical", via: []}}), "fixture"),
    /critical advisory is not allowed/,
  );
  assert.throws(
    () => validateAuditReport(report({other: {severity: "low", via: [{url: "https:\/\/example.test\/GHSA-new"}]}}), "fixture"),
    /not explicitly allowed/,
  );
});

test("auditGraph rejects npm output even when npm reports only a low finding", () => {
  const runner = (_command, _arguments, options) => {
    assert.equal(options.cwd, "/tmp/graph");
    return {status: 1, stdout: JSON.stringify(report({esbuild})), stderr: ""};
  };
  assert.throws(() => auditGraph("/tmp/graph", "fixture", runner), /not explicitly allowed/);
});

test("exception documents fail before expiry enters the warning window", () => {
  const file = path.join(root, "expiring.json");
  fs.writeFileSync(file, JSON.stringify({
    schema_version: 2,
    minimum_expiry_warning_days: 30,
    exceptions: [{
      advisory: "GHSA-g7r4-m6w7-qqqr",
      severity: "low",
      packages: ["esbuild"],
      expires_on: "2026-09-01",
      reason: "Temporary exception used only by this expiration regression test.",
    }],
  }));
  assert.throws(() => loadExceptions(file, new Date("2026-08-23T00:00:00Z")), /expires within 30 days/);
});
