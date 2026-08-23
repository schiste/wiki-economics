"use strict";

const assert = require("node:assert/strict");
const {test} = require("node:test");
const {advisoryId, auditGraph, validateAuditReport} = require("./check-npm-advisories.cjs");

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

test("the exact Observable esbuild advisory is accepted", () => {
  const result = validateAuditReport(report({
    "@observablehq/framework": {severity: "low", via: ["esbuild"]},
    esbuild,
  }), "fixture");
  assert.deepEqual(result.allowedLow, ["@observablehq/framework", "esbuild"]);
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

test("auditGraph validates parsed npm output even when npm exits for low findings", () => {
  const runner = (_command, _arguments, options) => {
    assert.equal(options.cwd, "/tmp/graph");
    return {status: 1, stdout: JSON.stringify(report({esbuild})), stderr: ""};
  };
  assert.deepEqual(auditGraph("/tmp/graph", "fixture", runner), {label: "fixture", allowedLow: ["esbuild"]});
});
