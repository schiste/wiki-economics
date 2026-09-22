const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const {spawnSync} = require("node:child_process");
const {after, test} = require("node:test");

const helper = path.join(__dirname, "qualification-receipt.cjs");
const root = fs.mkdtempSync(path.join(os.tmpdir(), "wiki-econ-qualification-receipt-"));

after(() => fs.rmSync(root, {recursive: true, force: true}));

function write(file, value) {
  fs.mkdirSync(path.dirname(file), {recursive: true});
  fs.writeFileSync(file, value);
}

function run(command, environment) {
  const result = spawnSync(process.execPath, [helper, command, ...(command === "finish" ? ["0"] : [])], {
    env: {...process.env, ...environment},
    encoding: "utf8",
  });
  assert.equal(result.status, 0, `${command}: ${result.stdout}\n${result.stderr}`);
  return result;
}

test("captures complete immutable evidence for a page-week qualification stage", () => {
  const fixture = path.join(root, "page-week");
  const data = path.join(fixture, "data");
  const output = path.join(fixture, "output");
  const scratch = path.join(fixture, "scratch");
  const siteDist = path.join(fixture, "site-dist");
  const cgroup = path.join(fixture, "cgroup");
  const qualification = path.join(fixture, "qualification");
  const events = path.join(fixture, "events.jsonl");
  const log = path.join(fixture, "refresh.log");
  const snapshotFile = path.join(fixture, "selected-snapshot");
  fs.mkdirSync(cgroup, {recursive: true});
  fs.mkdirSync(scratch, {recursive: true});
  fs.mkdirSync(siteDist, {recursive: true});
  write(snapshotFile, "2026-08\n");
  write(path.join(cgroup, "memory.current"), "100\n");
  write(path.join(cgroup, "memory.peak"), "200\n");
  write(path.join(cgroup, "memory.max"), "6442450944\n");
  write(path.join(cgroup, "cpu.stat"), "usage_usec 1000\nuser_usec 700\nsystem_usec 300\nnr_periods 4\nnr_throttled 0\nthrottled_usec 0\n");
  const profile = path.join(data, "snapshots", "enwiki", "2026-08", "workload-profile.json");
  write(profile, JSON.stringify({
    schema_version: 1,
    parameters: {primary_buckets: 64, secondary_buckets: 32},
    bucket_staged_rows: [10, 20, 0, 40],
  }));
  write(path.join(data, "snapshots", "enwiki", "2026-08", "source-plan.json"), "source plan\n");
  write(path.join(data, "snapshots", "enwiki", "2026-08", "remote-inventory.json"), "remote inventory\n");
  write(path.join(output, "_stages", "compute", "page_week", "enwiki.json"), JSON.stringify({
    schema_version: 1,
    stage: "page-week",
    scope: "wiki:enwiki",
    selected_snapshot: "enwiki=2026-08",
    algorithm_version: "page-week-test-v1",
    computation_version: "test",
    fingerprint: "stage-fingerprint",
    inputs: [{identity: "warehouse/revisions", rows: 5, bytes: 123, sha256: "a".repeat(64)}],
    outputs: [{identity: "page_weekly_edits.parquet", rows: 8, bytes: 456, sha256: "b".repeat(64), artifact_receipt_sha256: "c".repeat(64)}],
  }));
  write(events, "");
  write(log, "");

  const environment = {
    WIKI_ECON_REFRESH_STAGE: "page-week",
    WIKI_ECON_RUN_ID: "qualification-run",
    WIKI_ECON_QUALIFICATION_RUN_KIND: "initial_candidate",
    WIKI_ECON_PIPELINE_ID: "qualification-pipeline",
    WIKI_ECON_RUN_WIKIS_JSON: JSON.stringify(["enwiki"]),
    WIKI_ECON_RUN_SNAPSHOT_FILE: snapshotFile,
    WIKI_ECON_DATA_DIR: data,
    WIKI_ECON_OUTPUT_DIR: output,
    WIKI_ECON_SCRATCH_DIR: scratch,
    WIKI_ECON_SITE_DIST_DIR: siteDist,
    WIKI_ECON_CGROUP_ROOT: cgroup,
    WIKI_ECON_RUN_EVENTS_FILE: events,
    WIKI_ECON_RUN_LOG_FILE: log,
    WIKI_ECON_QUALIFICATION_RECEIPT_DIR: qualification,
    WIKI_ECON_MEMORY_CEILING_BYTES: "6442450944",
    WIKI_ECON_REQUESTED_CPU_CORES: "4",
    WIKI_ECON_SOURCE_WORKERS: "1",
    WIKI_ECON_THREAD_LIMIT: "1",
    WIKI_ECON_MAX_ACTIVE_PARQUET_WRITERS: "16",
    WIKI_ECON_SOURCE_WINDOW_SIZE: "1",
  };

  run("start", environment);
  const firstStartPath = path.join(qualification, "qualification-pipeline", "page-week.qualification-run.start.json");
  assert.ok(fs.existsSync(firstStartPath));
  write(path.join(cgroup, "memory.current"), "400\n");
  write(path.join(cgroup, "memory.peak"), "500\n");
  write(path.join(cgroup, "cpu.stat"), "usage_usec 9000\nuser_usec 6000\nsystem_usec 3000\nnr_periods 8\nnr_throttled 1\nthrottled_usec 22\n");
  fs.appendFileSync(events, `${JSON.stringify({event: "warning", message: "source retry warning"})}\n`);
  fs.appendFileSync(events, `${JSON.stringify({event: "recovery", message: "recovered stale worker"})}\n`);
  fs.appendFileSync(log, "WARNING source retry attempt=2\nrecovery completed\n");
  run("sample", environment);
  const finish = run("finish", environment);
  const finalPath = path.join(qualification, "qualification-pipeline", "page-week.qualification-run.json");
  const receipt = JSON.parse(fs.readFileSync(finalPath, "utf8"));

  assert.equal(receipt.status, "succeeded");
  assert.equal(receipt.run_kind, "initial_candidate");
  assert.equal(receipt.exit_code, 0);
  assert.equal(receipt.snapshot, "2026-08");
  assert.equal(receipt.input.rows, 5);
  assert.equal(receipt.input.bytes, 123);
  assert.equal(receipt.output.rows, 8);
  assert.equal(receipt.output.bytes, 456);
  assert.equal(receipt.inputs[0].sha256, "a".repeat(64));
  assert.equal(receipt.outputs[0].artifact_receipt_sha256, "c".repeat(64));
  assert.equal(receipt.fingerprints.stage_receipts[0].fingerprint, "stage-fingerprint");
  assert.equal(receipt.resources.cpu.usage_usec, 8000);
  assert.equal(receipt.resources.cpu.nr_throttled, 1);
  assert.equal(receipt.resources.cgroup.peak_bytes, 500);
  assert.equal(receipt.resources.cgroup.limit_bytes, 6442450944);
  assert.ok(receipt.wall_time_ms >= 0);
  assert.ok(receipt.resources.storage.persistent_data.high_water != null);
  assert.ok(receipt.resources.storage.scratch.high_water != null);
  assert.ok(receipt.resources.storage.persistent_filesystem.high_water != null);
  assert.deepEqual(receipt.bucket_size_distribution.rows, [10, 20, 0, 40]);
  assert.equal(receipt.bucket_size_distribution.distribution_status, "observed");
  assert.equal(receipt.bucket_size_distribution.expected_count, 2048);
  assert.equal(receipt.bucket_size_distribution.configured.enwiki.logical_buckets, 2048);
  assert.match(receipt.warnings.join("\n"), /WARNING/);
  assert.match(receipt.retries.join("\n"), /retry/);
  assert.match(receipt.recovery_events.join("\n"), /recovery/);
  assert.equal(receipt.observability.event_count, 2);
  assert.match(finish.stdout, /receipt_sha256/);
  assert.equal(fs.existsSync(firstStartPath), false);

  // A retry gets a distinct immutable document; it must never replace the
  // evidence from the first attempt.
  const retryEnvironment = {...environment, WIKI_ECON_RUN_ID: "qualification-retry"};
  run("start", retryEnvironment);
  run("finish", retryEnvironment);
  assert.ok(fs.existsSync(finalPath));
  assert.ok(fs.existsSync(path.join(qualification, "qualification-pipeline", "page-week.qualification-retry.json")));
});
