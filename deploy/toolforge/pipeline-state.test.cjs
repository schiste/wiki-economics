const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const {spawnSync} = require("node:child_process");
const {after, test} = require("node:test");

const helper = path.join(__dirname, "pipeline-state.cjs");
const root = fs.mkdtempSync(path.join(os.tmpdir(), "wiki-econ-pipeline-state-"));
const state = path.join(root, "pipeline.json");
const wikis = JSON.stringify(["enwiki"]);

after(() => fs.rmSync(root, {recursive: true, force: true}));

function run(...args) {
  return spawnSync(process.execPath, [helper, ...args], {encoding: "utf8"});
}

function json(result) {
  assert.equal(result.status, 0, `${result.stdout}\n${result.stderr}`);
  return JSON.parse(result.stdout);
}

test("pipeline state enforces the six stages and permits same-stage retry", () => {
  const first = json(run("begin", "--state", state, "--stage", "ingest", "--run-id", "run-1", "--snapshot", "2026-08", "--wikis-json", wikis));
  assert.equal(first.snapshot, "2026-08");
  assert.equal(json(run("complete", "--state", state, "--stage", "ingest", "--run-id", "run-1")).stage, "ingest");

  const outOfOrder = run("begin", "--state", state, "--stage", "page-week", "--run-id", "run-2", "--wikis-json", wikis);
  assert.notEqual(outOfOrder.status, 0);
  assert.match(outOfOrder.stderr, /requires completed lifecycle/);

  json(run("begin", "--state", state, "--stage", "metrics", "--run-id", "run-2", "--wikis-json", wikis));
  json(run("fail", "--state", state, "--stage", "metrics", "--run-id", "run-2", "--error", "test failure"));
  json(run("begin", "--state", state, "--stage", "metrics", "--run-id", "run-3", "--wikis-json", wikis));
  json(run("complete", "--state", state, "--stage", "metrics", "--run-id", "run-3"));

  const snapshotMismatch = run("begin", "--state", state, "--stage", "lifecycle", "--run-id", "run-4", "--snapshot", "2026-07", "--wikis-json", wikis);
  assert.notEqual(snapshotMismatch.status, 0);
  assert.match(snapshotMismatch.stderr, /snapshot does not match/);
});

test("pipeline state refuses overlapping ownership", () => {
  const begin = run("begin", "--state", state, "--stage", "lifecycle", "--run-id", "run-5", "--wikis-json", wikis);
  assert.equal(begin.status, 0, `${begin.stdout}\n${begin.stderr}`);
  const overlap = run("begin", "--state", state, "--stage", "lifecycle", "--run-id", "run-6", "--wikis-json", wikis);
  assert.notEqual(overlap.status, 0);
  assert.match(overlap.stderr, /already running/);
  json(run("complete", "--state", state, "--stage", "lifecycle", "--run-id", "run-5"));
});

test("pipeline state recovers a stale lease before retrying", () => {
  const value = JSON.parse(fs.readFileSync(state, "utf8"));
  value.current_stage = "lifecycle";
  value.state = "running";
  value.stages.lifecycle = {
    status: "running",
    run_id: "abandoned-run",
    started_at: "2020-01-01T00:00:00.000Z",
  };
  fs.writeFileSync(state, `${JSON.stringify(value)}\n`);
  const result = json(run(
    "begin",
    "--state", state,
    "--stage", "lifecycle",
    "--run-id", "recovery-run",
    "--stale-after-secs", "1",
    "--wikis-json", wikis,
  ));
  assert.equal(result.stage, "lifecycle");
  const recovered = JSON.parse(fs.readFileSync(state, "utf8"));
  assert.equal(recovered.stages.lifecycle.run_id, "recovery-run");
  assert.equal(recovered.stages.lifecycle.status, "running");
  assert.match(recovered.stages.lifecycle.previous_failure?.error || "", /lease expired/);
  json(run("complete", "--state", state, "--stage", "lifecycle", "--run-id", "recovery-run"));
});
