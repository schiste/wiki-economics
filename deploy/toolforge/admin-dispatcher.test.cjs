"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const {terminalOperationSummary} = require("./admin-dispatcher.cjs");

test("successful terminal operations clear stale failure classifications", () => {
  const summary = terminalOperationSummary(
    {
      stage: "patrol_preflight",
      progress: {completedSources: 26, reusedSources: 26},
    },
    [
      "UPSTREAM_WAITING: Wikimedia logging dump 20260901 is incomplete",
      "starting stage stage=qualification_validate wiki=dewiki",
      "wiki qualification is ready wiki=dewiki snapshot=2026-08",
    ].join("\n"),
    0,
  );

  assert.equal(summary.stage, "qualification_validate");
  assert.equal(summary.progress.completedSources, 26);
  assert.equal(summary.rawError, null);
  assert.equal(summary.errorSummary, null);
  assert.equal(summary.retryable, null);
  assert.equal(summary.remediationCode, null);
  assert.equal(summary.remediation, null);
});

test("failed terminal operations retain their current failure classification", () => {
  const summary = terminalOperationSummary(
    {},
    "Error: artifact-backed candidate authentication is allowed only after input retention",
    1,
  );

  assert.equal(
    summary.errorSummary,
    "artifact-backed candidate authentication is allowed only after input retention",
  );
  assert.equal(summary.retryable, true);
});
