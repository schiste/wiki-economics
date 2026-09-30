const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const {spawnSync} = require("node:child_process");
const {after, test} = require("node:test");

const helper = path.join(__dirname, "pipeline-state.cjs");
const root = fs.mkdtempSync(path.join(os.tmpdir(), "wiki-econ-pipeline-state-"));
const wikis = JSON.stringify(["enwiki"]);

after(() => fs.rmSync(root, {recursive: true, force: true}));

// Each test owns its state file. The coordinator keeps a single mutable
// document per pipeline, so sharing one across tests made a single early
// assertion failure cascade into unrelated ones.
function freshState(name) {
  const file = path.join(root, `${name}.json`);
  return file;
}

function run(...args) {
  return spawnSync(process.execPath, [helper, ...args], {encoding: "utf8"});
}

function json(result) {
  assert.equal(result.status, 0, `${result.stdout}\n${result.stderr}`);
  return JSON.parse(result.stdout);
}

function refuses(pattern) {
  return (...args) => {
    const result = run(...args);
    assert.notEqual(result.status, 0, `expected refusal, got: ${result.stdout}`);
    assert.match(result.stderr, pattern);
    return result;
  };
}

function readState(file) {
  return JSON.parse(fs.readFileSync(file, "utf8"));
}

test("compute stages run in parallel up to the capacity-derived limit", () => {
  const state = freshState("parallel");
  const refusesConcurrency = refuses(/compute-stage concurrency limit/);

  json(run("begin", "--state", state, "--stage", "ingest", "--run-id", "run-1", "--snapshot", "2026-08", "--wikis-json", wikis));
  assert.equal(readState(state).snapshot, "2026-08");
  json(run("complete", "--state", state, "--stage", "ingest", "--run-id", "run-1"));

  // Every compute stage depends only on ingest, so the first two may start
  // together. The Toolforge envelope admits exactly two concurrent 6 GiB
  // compute stages, and the coordinator derives that from the capacity policy
  // rather than hardcoding it.
  json(run("begin", "--state", state, "--stage", "page-week", "--run-id", "run-2", "--wikis-json", wikis));
  json(run("begin", "--state", state, "--stage", "lifecycle", "--run-id", "run-3", "--wikis-json", wikis));
  refusesConcurrency("begin", "--state", state, "--stage", "metrics", "--run-id", "run-4", "--wikis-json", wikis);
  refusesConcurrency("begin", "--state", state, "--stage", "patrol", "--run-id", "run-5", "--wikis-json", wikis);

  // A duplicate begin for an already-running stage is refused before the
  // capacity check, so it must not be reported as a capacity failure.
  refuses(/already running/)("begin", "--state", state, "--stage", "page-week", "--run-id", "run-2b", "--wikis-json", wikis);

  // Draining one slot admits the next stage.
  json(run("complete", "--state", state, "--stage", "page-week", "--run-id", "run-2"));
  json(run("begin", "--state", state, "--stage", "patrol", "--run-id", "run-6", "--wikis-json", wikis));
  json(run("complete", "--state", state, "--stage", "lifecycle", "--run-id", "run-3"));
  json(run("complete", "--state", state, "--stage", "patrol", "--run-id", "run-6"));

  // publish is the join barrier: every compute stage must have succeeded.
  refuses(/requires completed metrics/)("begin", "--state", state, "--stage", "publish", "--run-id", "run-7", "--wikis-json", wikis);
  json(run("begin", "--state", state, "--stage", "metrics", "--run-id", "run-8", "--wikis-json", wikis));
  json(run("complete", "--state", state, "--stage", "metrics", "--run-id", "run-8"));
  assert.equal(json(run("begin", "--state", state, "--stage", "publish", "--run-id", "run-10", "--wikis-json", wikis)).stage, "publish");
  assert.equal(json(run("complete", "--state", state, "--stage", "publish", "--run-id", "run-10")).state, "succeeded");
});

test("a compute stage cannot start before ingest succeeds", () => {
  const state = freshState("prerequisites");
  json(run("begin", "--state", state, "--stage", "ingest", "--run-id", "run-1", "--snapshot", "2026-08", "--wikis-json", wikis));
  refuses(/requires completed ingest/)("begin", "--state", state, "--stage", "metrics", "--run-id", "run-2", "--wikis-json", wikis);
  json(run("complete", "--state", state, "--stage", "ingest", "--run-id", "run-1"));
  json(run("begin", "--state", state, "--stage", "metrics", "--run-id", "run-3", "--wikis-json", wikis));
});

test("a compute stage refuses to overlap the ingest stage", () => {
  const state = freshState("no-overlap");
  json(run("begin", "--state", state, "--stage", "ingest", "--run-id", "run-1", "--snapshot", "2026-08", "--wikis-json", wikis));
  // ingest is still running, so it has not succeeded and the prerequisite is
  // reported first; a completed ingest is required before any compute stage.
  refuses(/requires completed ingest/)("begin", "--state", state, "--stage", "metrics", "--run-id", "run-2", "--wikis-json", wikis);
  json(run("complete", "--state", state, "--stage", "ingest", "--run-id", "run-1"));
  json(run("begin", "--state", state, "--stage", "metrics", "--run-id", "run-3", "--wikis-json", wikis));
});

test("a failed compute stage blocks sibling retries until the active stage drains", () => {
  const state = freshState("failed-sibling");
  json(run("begin", "--state", state, "--stage", "ingest", "--run-id", "run-1", "--snapshot", "2026-08", "--wikis-json", wikis));
  json(run("complete", "--state", state, "--stage", "ingest", "--run-id", "run-1"));
  json(run("begin", "--state", state, "--stage", "metrics", "--run-id", "run-2", "--wikis-json", wikis));
  json(run("begin", "--state", state, "--stage", "lifecycle", "--run-id", "run-3", "--wikis-json", wikis));
  json(run("fail", "--state", state, "--stage", "metrics", "--run-id", "run-2", "--error", "out of memory"));

  refuses(/has a failed compute stage/)("begin", "--state", state, "--stage", "patrol", "--run-id", "run-4", "--wikis-json", wikis);
  json(run("complete", "--state", state, "--stage", "lifecycle", "--run-id", "run-3"));

  // Once nothing is active the same stage retries, and the failure is retained.
  json(run("begin", "--state", state, "--stage", "metrics", "--run-id", "run-5", "--wikis-json", wikis));
  const retried = readState(state);
  assert.equal(retried.stages.metrics.status, "running");
  assert.equal(retried.stages.metrics.previous_failure.error, "out of memory");
  assert.equal(retried.stages.metrics.previous_failure.run_id, "run-2");
});

test("a mismatched snapshot or wiki set is refused", () => {
  const state = freshState("identity");
  json(run("begin", "--state", state, "--stage", "ingest", "--run-id", "run-1", "--snapshot", "2026-08", "--wikis-json", wikis));
  json(run("complete", "--state", state, "--stage", "ingest", "--run-id", "run-1"));
  refuses(/snapshot does not match/)("begin", "--state", state, "--stage", "metrics", "--run-id", "run-2", "--snapshot", "2026-07", "--wikis-json", wikis);
  refuses(/wiki set does not match/)("begin", "--state", state, "--stage", "metrics", "--run-id", "run-3", "--wikis-json", JSON.stringify(["frwiki"]));
});

test("ingest refuses to reset a generation while a stage is running", () => {
  const state = freshState("ingest-reset");
  json(run("begin", "--state", state, "--stage", "ingest", "--run-id", "run-1", "--snapshot", "2026-08", "--wikis-json", wikis));
  json(run("complete", "--state", state, "--stage", "ingest", "--run-id", "run-1"));
  json(run("begin", "--state", state, "--stage", "metrics", "--run-id", "run-2", "--wikis-json", wikis));
  refuses(/refusing ingest reset/)("begin", "--state", state, "--stage", "ingest", "--run-id", "run-3", "--snapshot", "2026-09", "--wikis-json", wikis);

  // A clean generation may be replaced, and that rolls the snapshot forward
  // and invalidates downstream work.
  json(run("complete", "--state", state, "--stage", "metrics", "--run-id", "run-2"));
  json(run("begin", "--state", state, "--stage", "ingest", "--run-id", "run-4", "--snapshot", "2026-09", "--wikis-json", wikis));
  const reset = readState(state);
  assert.equal(reset.snapshot, "2026-09");
  assert.equal(reset.stages.metrics.status, "pending");
});

test("a completed generation rolls forward to the next snapshot", () => {
  const state = freshState("roll-forward");
  json(run("begin", "--state", state, "--stage", "ingest", "--run-id", "run-1", "--snapshot", "2026-08", "--wikis-json", wikis));
  json(run("complete", "--state", state, "--stage", "ingest", "--run-id", "run-1"));
  for (const stage of ["metrics", "lifecycle", "page-week", "patrol"]) {
    json(run("begin", "--state", state, "--stage", stage, "--run-id", `gen-1-${stage}`, "--wikis-json", wikis));
    json(run("complete", "--state", state, "--stage", stage, "--run-id", `gen-1-${stage}`));
  }
  json(run("begin", "--state", state, "--stage", "publish", "--run-id", "gen-1-publish", "--wikis-json", wikis));
  assert.equal(json(run("complete", "--state", state, "--stage", "publish", "--run-id", "gen-1-publish")).state, "succeeded");

  // The state document survives a completed generation, so the next month must
  // be able to open a new one. This previously failed with a snapshot mismatch.
  json(run("begin", "--state", state, "--stage", "ingest", "--run-id", "gen-2", "--snapshot", "2026-09", "--wikis-json", wikis));
  assert.equal(readState(state).snapshot, "2026-09");
  assert.equal(readState(state).stages["page-week"].status, "pending");
  json(run("complete", "--state", state, "--stage", "ingest", "--run-id", "gen-2"));
});

test("a stale lease is recovered and recorded before a retry", () => {
  const state = freshState("stale");
  json(run("begin", "--state", state, "--stage", "ingest", "--run-id", "run-1", "--snapshot", "2026-08", "--wikis-json", wikis));
  json(run("complete", "--state", state, "--stage", "ingest", "--run-id", "run-1"));
  json(run("begin", "--state", state, "--stage", "lifecycle", "--run-id", "abandoned-run", "--wikis-json", wikis));

  // Age the abandoned lease past the recovery threshold.
  const aged = readState(state);
  aged.stages.lifecycle.started_at = "2020-01-01T00:00:00.000Z";
  aged.stages.lifecycle.heartbeat_at = "2020-01-01T00:00:00.000Z";
  fs.writeFileSync(state, `${JSON.stringify(aged)}\n`);

  const result = json(run(
    "begin",
    "--state", state,
    "--stage", "lifecycle",
    "--run-id", "recovery-run",
    "--stale-after-secs", "1",
    "--wikis-json", wikis,
  ));
  assert.equal(result.stage, "lifecycle");
  const recovered = readState(state);
  assert.equal(recovered.stages.lifecycle.run_id, "recovery-run");
  assert.equal(recovered.stages.lifecycle.status, "running");
  assert.match(recovered.stages.lifecycle.previous_failure?.error || "", /lease expired/);
  json(run("complete", "--state", state, "--stage", "lifecycle", "--run-id", "recovery-run"));
});

test("a live lease is never stolen", () => {
  const state = freshState("live-lease");
  json(run("begin", "--state", state, "--stage", "ingest", "--run-id", "run-1", "--snapshot", "2026-08", "--wikis-json", wikis));
  json(run("complete", "--state", state, "--stage", "ingest", "--run-id", "run-1"));
  json(run("begin", "--state", state, "--stage", "lifecycle", "--run-id", "run-2", "--wikis-json", wikis));
  refuses(/already running/)("begin", "--state", state, "--stage", "lifecycle", "--run-id", "run-3", "--stale-after-secs", "3600", "--wikis-json", wikis);
  assert.equal(readState(state).stages.lifecycle.run_id, "run-2");
});
