"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const {spawnSync} = require("node:child_process");
const test = require("node:test");

test("compatibility cohort rebuilds exact wikis sequentially with bounded source windows", (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "wiki-econ-cohort-"));
  t.after(() => fs.rmSync(root, {recursive: true, force: true}));
  const binary = path.join(root, "wiki-econ");
  const log = path.join(root, "calls.log");
  const lifecycle = path.join(root, "lifecycle.json");
  fs.writeFileSync(lifecycle, "{}\n");
  fs.writeFileSync(binary, '#!/bin/sh\nprintf "%s\\n" "$*" >> "$COHORT_CALL_LOG"\n');
  fs.chmodSync(binary, 0o755);
  const script = path.join(__dirname, "run-compatibility-cohort.sh");
  const result = spawnSync(script, [
    "--run-id", "compat-1", "--lifecycle", lifecycle,
    "frwiki=2026-08", "nlwiki=2026-08",
  ], {
    encoding: "utf8",
    env: {
      ...process.env,
      WIKI_ECON_ENV: "local",
      WIKI_ECON_BIN: binary,
      WIKI_ECON_DATA_DIR: path.join(root, "data"),
      WIKI_ECON_OUTPUT_DIR: path.join(root, "output"),
      COHORT_CALL_LOG: log,
    },
  });
  assert.equal(result.status, 0, result.stderr);
  const calls = fs.readFileSync(log, "utf8").trim().split("\n");
  assert.equal(calls.length, 2);
  assert.match(calls[0], /--run-id compat-1-frwiki .*prepare-wiki frwiki --version 2026-08 --source-window-size 1 --rebuild/);
  assert.match(calls[1], /--run-id compat-1-nlwiki .*prepare-wiki nlwiki --version 2026-08 --source-window-size 1 --rebuild/);
  assert.match(result.stdout, /wiki=frwiki .*cohort_index=1 cohort_total=2/);
  assert.match(result.stdout, /candidate ready wiki=nlwiki .*cohort_index=2 cohort_total=2/);

  const retry = spawnSync(script, [
    "--run-id", "compat-1-retry", "--lifecycle", lifecycle,
    "frwiki=2026-08", "nlwiki=2026-08",
  ], {
    encoding: "utf8",
    env: {
      ...process.env,
      WIKI_ECON_ENV: "local",
      WIKI_ECON_BIN: binary,
      WIKI_ECON_DATA_DIR: path.join(root, "data"),
      WIKI_ECON_OUTPUT_DIR: path.join(root, "output"),
      COHORT_CALL_LOG: log,
    },
  });
  assert.equal(retry.status, 0, retry.stderr);
  const retriedCalls = fs.readFileSync(log, "utf8").trim().split("\n").slice(2);
  assert.equal(retriedCalls.length, 2);
  assert.doesNotMatch(retriedCalls[0], /--rebuild/);
  assert.doesNotMatch(retriedCalls[1], /--rebuild/);
  assert.match(retry.stdout, /Revalidating retained compatibility candidate wiki=frwiki/);
});

test("compatibility cohort retains successes and continues after one member fails", (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "wiki-econ-cohort-partial-"));
  t.after(() => fs.rmSync(root, {recursive: true, force: true}));
  const binary = path.join(root, "wiki-econ");
  const log = path.join(root, "calls.log");
  const lifecycle = path.join(root, "lifecycle.json");
  fs.writeFileSync(lifecycle, "{}\n");
  fs.writeFileSync(binary, [
    "#!/bin/sh",
    'printf "%s\\n" "$*" >> "$COHORT_CALL_LOG"',
    'case "$*" in *"prepare-wiki dewiki "*) exit 17 ;; esac',
    "exit 0",
    "",
  ].join("\n"));
  fs.chmodSync(binary, 0o755);
  const result = spawnSync(path.join(__dirname, "run-compatibility-cohort.sh"), [
    "--run-id", "compat-2", "--lifecycle", lifecycle,
    "dewiki=2026-08", "frwiki=2026-08",
  ], {
    encoding: "utf8",
    env: {
      ...process.env,
      WIKI_ECON_ENV: "local",
      WIKI_ECON_BIN: binary,
      WIKI_ECON_DATA_DIR: path.join(root, "data"),
      WIKI_ECON_OUTPUT_DIR: path.join(root, "output"),
      COHORT_CALL_LOG: log,
    },
  });
  assert.equal(result.status, 1);
  assert.equal(fs.readFileSync(log, "utf8").trim().split("\n").length, 2,
    "the member after the failure must still run");
  assert.match(result.stderr, /candidate failed wiki=dewiki/);
  assert.match(result.stderr, /failed members: dewiki:17/);
  assert.match(result.stdout, /candidate ready wiki=frwiki/);
});
