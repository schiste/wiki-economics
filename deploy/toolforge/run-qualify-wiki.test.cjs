"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const {spawnSync} = require("node:child_process");
const {test} = require("node:test");

const script = path.join(__dirname, "run-qualify-wiki.sh");

test("qualification wrapper confines data, output, status, and locks to its isolated root", () => {
  const fixture = fs.mkdtempSync(path.join(os.tmpdir(), "wiki-econ-qualification-"));
  try {
    const calls = path.join(fixture, "calls.txt");
    const binary = path.join(fixture, "wiki-econ");
    fs.writeFileSync(binary, `#!/bin/sh\nprintf '%s\\n' "$*" >> "${calls}"\ncase " $* " in\n  *" snapshot-resolve "*) printf '2026-07\\n' ;;\nesac\n`, {mode: 0o755});
    const qualificationRoot = path.join(fixture, "capacity", "qualifications");
    const result = spawnSync("bash", [script, "itwiki"], {
      encoding: "utf8",
      env: {
        ...process.env,
        WIKI_ECON_ROOT: fixture,
        WIKI_ECON_BIN: binary,
        WIKI_ECON_QUALIFICATION_ROOT: qualificationRoot,
        WIKI_ECON_RUN_ID: "qualify-itwiki-test",
      },
    });
    assert.equal(result.status, 0, result.stderr || result.stdout);
    const runRoot = path.join(qualificationRoot, "itwiki", "qualify-itwiki-test");
    const invocations = fs.readFileSync(calls, "utf8");
    assert.match(invocations, new RegExp(`--data-dir ${runRoot}/data`));
    assert.match(invocations, new RegExp(`--output-dir ${runRoot}/output`));
    assert.match(invocations, /qualify-wiki itwiki --version 2026-07/);
    assert.equal(JSON.parse(fs.readFileSync(path.join(qualificationRoot, "_status", "itwiki.json"))).state, "succeeded");
    assert.ok(fs.existsSync(path.join(runRoot, "logs", "qualification.log")));
    assert.ok(fs.existsSync(path.join(runRoot, "scratch")));
    assert.ok(!fs.existsSync(path.join(fixture, "data")));
  } finally {
    fs.rmSync(fixture, {recursive: true, force: true});
  }
});

test("qualification wrapper rejects unsafe wiki identifiers before creating state", () => {
  const result = spawnSync("bash", [script, "../itwiki"], {encoding: "utf8"});
  assert.equal(result.status, 2);
  assert.match(result.stderr, /Unsafe wiki identifier/);
});
