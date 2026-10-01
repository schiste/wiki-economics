"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const {spawnSync} = require("node:child_process");
const {afterEach, test} = require("node:test");

const script = path.join(__dirname, "run-fleet-isolated-prepare.sh");
const stageWrappers = [
  ["ingest", "run-refresh-pipeline-ingest.sh"],
  ["metrics", "run-refresh-metrics.sh"],
  ["lifecycle", "run-refresh-lifecycle.sh"],
  ["page-week", "run-refresh-page-week.sh"],
  ["patrol", "run-refresh-patrol.sh"],
  ["publish", "run-refresh-publish.sh"],
];
const roots = [];

afterEach(() => {
  while (roots.length) fs.rmSync(roots.pop(), {recursive: true, force: true});
});

function fixture({succeedingStages = []} = {}) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "wiki-econ-fleet-isolated-"));
  roots.push(root);
  const output = path.join(root, "output");
  const wrappers = path.join(root, "wrappers");
  const log = path.join(root, "stages.log");
  fs.mkdirSync(output, {recursive: true});
  fs.mkdirSync(wrappers, {recursive: true});
  for (const [stage, wrapper] of stageWrappers) {
    if (succeedingStages.includes(stage) && stage === "ingest") {
      const state = {
        schema_version: 1,
        pipeline_id: "original-ingest-run",
        snapshot: "2026-09",
        wikis: ["enwiki"],
        stages: {[stage]: {status: "succeeded"}},
      };
      fs.writeFileSync(path.join(output, ".pipeline-state-enwiki.json"), JSON.stringify(state));
    }
    const source = [
      "#!/bin/sh",
      "set -eu",
      "printf '%s %s %s\\n' '" + stage + "' \"$WIKI_ECON_RUN_ID\" \"$WIKI_ECON_PREPARE_SNAPSHOT\" >> '" + log + "'",
      "",
    ].join("\n");
    fs.writeFileSync(path.join(wrappers, wrapper), source, {mode: 0o755});
  }
  return {log, output, root, wrappers};
}

function run(state) {
  return spawnSync("bash", [script, "enwiki"], {
    encoding: "utf8",
    env: {
      ...process.env,
      WIKI_ECON_CAPACITY_ADMITTED: "1",
      WIKI_ECON_FLEET_PIPELINE_WRAPPER_DIR: state.wrappers,
      WIKI_ECON_OUTPUT_DIR: state.output,
      WIKI_ECON_PREPARE_SNAPSHOT: "2026-09",
      WIKI_ECON_RUN_ID: "fleet-isolated-run",
    },
  });
}

test("isolated worker runs all six monthly stages sequentially with distinct run IDs", () => {
  const state = fixture();
  const result = run(state);
  assert.equal(result.status, 0, result.stdout + "\n" + result.stderr);
  const rows = fs.readFileSync(state.log, "utf8").trim().split("\n");
  assert.deepEqual(rows.map((row) => row.split(" ")[0]), stageWrappers.map(([stage]) => stage));
  assert.deepEqual(
    rows.map((row) => row.split(" ")[1]),
    stageWrappers.map(([stage]) => "fleet-isolated-run-" + stage),
  );
  assert.ok(rows.every((row) => row.endsWith("2026-09")));
});

test("isolated worker resumes completed ingest and rejects non-enwiki tasks", () => {
  const state = fixture({succeedingStages: ["ingest"]});
  const result = run(state);
  assert.equal(result.status, 0, result.stdout + "\n" + result.stderr);
  const rows = fs.readFileSync(state.log, "utf8").trim().split("\n");
  assert.deepEqual(rows.map((row) => row.split(" ")[0]), stageWrappers.slice(1).map(([stage]) => stage));

  const unsupported = spawnSync("bash", [script, "dewiki"], {
    encoding: "utf8",
    env: {
      ...process.env,
      WIKI_ECON_CAPACITY_ADMITTED: "1",
      WIKI_ECON_OUTPUT_DIR: state.output,
      WIKI_ECON_PREPARE_SNAPSHOT: "2026-09",
      WIKI_ECON_RUN_ID: "fleet-isolated-run",
    },
  });
  assert.equal(unsupported.status, 2);
  assert.match(unsupported.stderr, /No isolated pipeline profile is configured for dewiki/);
});
