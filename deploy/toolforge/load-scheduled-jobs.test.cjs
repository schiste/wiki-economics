"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const {spawnSync} = require("node:child_process");
const {test} = require("node:test");

const script = path.join(__dirname, "load-scheduled-jobs.sh");

test("normal job loading allowlists schedules and removes one-off definitions", () => {
  const fixture = fs.mkdtempSync(path.join(os.tmpdir(), "wiki-econ-jobs-load-"));
  try {
    const calls = path.join(fixture, "calls.txt");
    const manifest = path.join(fixture, "jobs.yaml");
    const toolforge = path.join(fixture, "toolforge");
    fs.writeFileSync(manifest, "[]\n");
    fs.writeFileSync(toolforge, `#!/bin/sh
printf '%s\n' "$*" >> "${calls}"
case "$1 $2" in
  "jobs show") exit 0 ;;
  *) exit 0 ;;
esac
`, {mode: 0o755});

    const result = spawnSync("bash", [script, manifest], {
      encoding: "utf8",
      env: {...process.env, PATH: `${fixture}:${process.env.PATH}`},
    });
    assert.equal(result.status, 0, result.stderr || result.stdout);
    const invocations = fs.readFileSync(calls, "utf8").trim().split("\n");
    const loaded = invocations.filter((call) => call.startsWith("jobs load --job "));
    assert.deepEqual(loaded, [
      `jobs load --job wiki-econ-prepare-nlwiki ${manifest}`,
      `jobs load --job wiki-econ-prepare-ptwiki ${manifest}`,
      `jobs load --job wiki-econ-prepare-frwiki ${manifest}`,
      `jobs load --job wiki-econ-prepare-itwiki ${manifest}`,
      `jobs load --job wiki-econ-prepare-svwiki ${manifest}`,
      `jobs load --job wiki-econ-prepare-elwiki ${manifest}`,
      `jobs load --job wiki-econ-publish-ready ${manifest}`,
      `jobs load --job wiki-econ-artifact-scrub ${manifest}`,
    ]);
    for (const name of ["wiki-econ-refresh", "wiki-econ-ingest", "wiki-econ-compute", "wiki-econ-site"]) {
      assert.ok(invocations.includes(`jobs delete ${name}`));
      assert.ok(!loaded.some((call) => call.includes(name)));
    }
  } finally {
    fs.rmSync(fixture, {recursive: true, force: true});
  }
});

test("a missing manifest fails before Toolforge is contacted", () => {
  const result = spawnSync("bash", [script, "/definitely/missing/jobs.yaml"], {encoding: "utf8"});
  assert.equal(result.status, 1);
  assert.match(result.stderr, /manifest is missing/);
});
