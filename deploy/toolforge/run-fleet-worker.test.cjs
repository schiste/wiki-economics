"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const {spawnSync} = require("node:child_process");
const {afterEach, test} = require("node:test");

const script = path.join(__dirname, "run-fleet-worker.sh");
const roots = [];
afterEach(() => {
  while (roots.length) fs.rmSync(roots.pop(), {recursive: true, force: true});
});

function fixture({failPrepare = false} = {}) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "wiki-econ-fleet-worker-"));
  roots.push(root);
  const log = path.join(root, "calls.log");
  const claim = path.join(root, "claim.json");
  fs.writeFileSync(claim, JSON.stringify({
    task: {wiki: "testwiki", snapshot: "2026-08", task_id: "a".repeat(64)},
  }));
  const binary = path.join(root, "wiki-econ");
  fs.writeFileSync(binary, `#!/bin/sh
set -eu
command=""
receipt=""
resource_class=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    fleet-claim|fleet-heartbeat|fleet-complete|fleet-fail|fleet-recover) command="$1" ;;
    --receipt) shift; receipt="$1" ;;
    --resource-class) shift; resource_class="$1" ;;
  esac
  shift
done
if [ "$command" = fleet-claim ]; then
  printf '%s %s\n' "$command" "$resource_class" >> "${log}"
  cp "${claim}" "$receipt"
else
  printf '%s\n' "$command" >> "${log}"
fi
`, {mode: 0o755});
  const prepare = path.join(root, "prepare");
  fs.writeFileSync(prepare, `#!/bin/sh
set -eu
printf 'prepare %s %s %s\n' "$1" "$WIKI_ECON_PREPARE_SNAPSHOT" "$WIKI_ECON_RUN_ID" >> "${log}"
exit ${failPrepare ? 1 : 0}
`, {mode: 0o755});
  return {root, log, binary, prepare};
}

function runWorker({resourceClass = "small", ...options} = {}) {
  const state = fixture(options);
  const result = spawnSync("bash", [script, resourceClass, `${resourceClass}-test`, "--once"], {
    encoding: "utf8",
    env: {
      ...process.env,
      WIKI_ECON_ROOT: state.root,
      WIKI_ECON_DATA_DIR: path.join(state.root, "data"),
      WIKI_ECON_OUTPUT_DIR: path.join(state.root, "output"),
      WIKI_ECON_BIN: state.binary,
      WIKI_ECON_FLEET_PREPARE_WRAPPER: state.prepare,
      WIKI_ECON_FLEET_QUEUE_DIR: path.join(state.root, "queue"),
      WIKI_ECON_FLEET_HEARTBEAT_SECS: "1",
    },
  });
  return {...state, result, calls: fs.readFileSync(state.log, "utf8").trim().split("\n")};
}

test("one-shot worker pins the claimed snapshot and completes independently", () => {
  const {result, calls} = runWorker();
  assert.equal(result.status, 0, result.stderr || result.stdout);
  assert.equal(calls[0], "fleet-claim small");
  assert.match(calls[1], /^prepare testwiki 2026-08 fleet-small-test-testwiki-/);
  assert.equal(calls.at(-1), "fleet-complete");
  assert.ok(!calls.includes("fleet-fail"));
});

test("medium worker translates the queue resource class to Clap's CLI spelling", () => {
  const {result, calls} = runWorker({resourceClass: "medium_large"});
  assert.equal(result.status, 0, result.stderr || result.stdout);
  assert.equal(calls[0], "fleet-claim medium-large");
  assert.equal(calls.at(-1), "fleet-complete");
});

test("failed preparation is returned to the bounded retry path", () => {
  const {result, calls} = runWorker({failPrepare: true});
  assert.notEqual(result.status, 0);
  assert.ok(calls.includes("fleet-fail"));
  assert.ok(!calls.includes("fleet-complete"));
});

test("worker rejects unsupported resource classes before touching the queue", () => {
  const result = spawnSync("bash", [script, "isolated", "unsafe", "--once"], {encoding: "utf8"});
  assert.equal(result.status, 2);
  assert.match(result.stderr, /Unsupported fleet resource class/);
});
