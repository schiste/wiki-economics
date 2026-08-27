"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const {test} = require("node:test");
const {EXPECTED_IDS, validateFleetQualification} = require("./check-fleet-qualification.cjs");

const policy = JSON.parse(fs.readFileSync(path.join(__dirname, "..", "config", "fleet-qualification.json"), "utf8"));

test("fleet qualification ladder is complete and keeps enwiki isolated", () => {
  assert.equal(validateFleetQualification(policy), policy);
  assert.deepEqual(policy.stages.map((stage) => stage.id), EXPECTED_IDS);
  assert.equal(policy.stages.find((stage) => stage.id === "enwiki_isolated").publication_eligible, false);
});

test("fleet qualification cannot skip stages or make enwiki publishable", () => {
  const skipped = structuredClone(policy);
  skipped.stages[1].state = "passed";
  assert.throws(() => validateFleetQualification(skipped), /before its predecessor/);
  const enwiki = structuredClone(policy);
  enwiki.stages[5].publication_eligible = true;
  assert.throws(() => validateFleetQualification(enwiki), /enwiki isolated/);
  const reordered = structuredClone(policy);
  [reordered.stages[2], reordered.stages[3]] = [reordered.stages[3], reordered.stages[2]];
  assert.throws(() => validateFleetQualification(reordered), /out of order/);
});
