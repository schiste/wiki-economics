"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const {spawnSync} = require("node:child_process");
const {afterEach, test} = require("node:test");

const script = path.join(__dirname, "run-fleet-controller.sh");
const roots = [];

afterEach(() => {
  while (roots.length) fs.rmSync(roots.pop(), {recursive: true, force: true});
});

function fixture() {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "wiki-econ-fleet-controller-"));
  roots.push(root);
  const binary = path.join(root, "wiki-econ");
  const source = [
    "#!/bin/sh",
    "set -eu",
    "case \" $* \" in",
    "  *\" fleet-discover \"*)",
    "    if [ \"$FAKE_DISCOVERY_FAILURES\" = \"1\" ]; then",
    "      printf '%s\\n' '{\"scheduled\":1,\"failures\":[{\"wiki\":\"frwiki\",\"error\":\"fixture unavailable\"}]}'",
    "    else",
    "      printf '%s\\n' '{\"scheduled\":1,\"failures\":[]}'",
    "    fi",
    "    ;;",
    "  *) echo \"unexpected fake wiki-econ invocation: $*\" >&2; exit 2 ;;",
    "esac",
    "",
  ].join("\n");
  fs.writeFileSync(binary, source, {mode: 0o755});
  return {binary, root};
}

function run(state, extraEnv = {}) {
  return spawnSync("bash", [script], {
    encoding: "utf8",
    env: {
      ...process.env,
      FAKE_DISCOVERY_FAILURES: "0",
      WIKI_ECON_BIN: state.binary,
      WIKI_ECON_ENV: "test",
      WIKI_ECON_ROOT: state.root,
      WIKI_ECON_RUN_ID: "controller-test",
      ...extraEnv,
    },
  });
}

test("controller accepts a clean per-wiki discovery report", () => {
  const state = fixture();
  const result = run(state);
  assert.equal(result.status, 0, result.stdout + "\n" + result.stderr);
  assert.match(result.stdout, /\{"scheduled":1,"failures":\[\]\}/);
});

test("controller marks partial discovery as failed after reporting successful work", () => {
  const state = fixture();
  const result = run(state, {FAKE_DISCOVERY_FAILURES: "1"});
  assert.equal(result.status, 1);
  assert.match(result.stdout, /"wiki":"frwiki"/);
  assert.match(result.stderr, /1 per-wiki failure/);
});
