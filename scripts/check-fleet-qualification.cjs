#!/usr/bin/env node
"use strict";

const fs = require("node:fs");
const path = require("node:path");

const EXPECTED_IDS = [
  "local_deterministic_fixtures",
  "six_wiki_shadow",
  "hidden_medium_yearly",
  "hidden_large_yearly",
  "frwiki_concurrent_fleet",
  "enwiki_isolated",
  "gradual_fleet_batches",
];
const STATES = new Set(["pending", "running", "passed", "failed"]);
const RESOURCE_CLASSES = new Set(["small", "medium_large", "isolated", "mixed"]);

function validateFleetQualification(document, label = "fleet qualification policy") {
  if (document?.schema_version !== 1 || document?.policy_version !== "fleet-qualification-v1") {
    throw new Error(`${label} has an unsupported schema or policy version`);
  }
  if (!Array.isArray(document.current_production_wikis)
      || document.current_production_wikis.length === 0
      || new Set(document.current_production_wikis).size !== document.current_production_wikis.length) {
    throw new Error(`${label} has an invalid production wiki set`);
  }
  if (!Array.isArray(document.stages) || document.stages.length !== EXPECTED_IDS.length) {
    throw new Error(`${label} must contain the complete ordered qualification ladder`);
  }
  let encounteredIncomplete = false;
  for (let index = 0; index < document.stages.length; index += 1) {
    const stage = document.stages[index];
    if (stage?.step !== index + 1 || stage?.id !== EXPECTED_IDS[index]) {
      throw new Error(`${label} stage ${index + 1} is out of order`);
    }
    if (!STATES.has(stage.state) || !RESOURCE_CLASSES.has(stage.resource_class)
        || typeof stage.publication_eligible !== "boolean"
        || !Array.isArray(stage.required_evidence) || stage.required_evidence.length === 0) {
      throw new Error(`${label} stage ${stage.id} is incomplete`);
    }
    if (stage.state !== "passed") encounteredIncomplete = true;
    else if (encounteredIncomplete) throw new Error(`${label} cannot pass a stage before its predecessor`);
  }
  const enwiki = document.stages.find((stage) => stage.id === "enwiki_isolated");
  if (enwiki.resource_class !== "isolated" || enwiki.publication_eligible) {
    throw new Error(`${label} must keep enwiki isolated and publication-ineligible`);
  }
  return document;
}

function main() {
  const file = path.resolve(process.argv[2] || path.join(__dirname, "..", "config", "fleet-qualification.json"));
  validateFleetQualification(JSON.parse(fs.readFileSync(file, "utf8")), file);
}

if (require.main === module) {
  try { main(); } catch (error) { console.error(error.message); process.exitCode = 1; }
}

module.exports = {EXPECTED_IDS, validateFleetQualification};
