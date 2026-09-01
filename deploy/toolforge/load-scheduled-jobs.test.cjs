"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const {spawnSync} = require("node:child_process");
const {test} = require("node:test");

const script = path.join(__dirname, "load-scheduled-jobs.sh");

test("fleet capacity uses a fixed controller and worker pool", () => {
  const manifest = fs.readFileSync(path.join(__dirname, "jobs.yaml"), "utf8");
  const fleetJobs = [...manifest.matchAll(/^- name: (wiki-econ-fleet-[a-z-]+)$/gm)]
    .map((match) => match[1]);
  assert.deepEqual(fleetJobs, [
    "wiki-econ-fleet-controller",
    "wiki-econ-fleet-small-a",
    "wiki-econ-fleet-small-b",
    "wiki-econ-fleet-medium",
  ]);
  assert.doesNotMatch(manifest, /^- name: wiki-econ-prepare-/m);
  assert.match(
    manifest,
    /- name: wiki-econ-admin-dispatcher[\s\S]*?command: deploy\/toolforge\/run-admin-dispatcher\.sh --once[\s\S]*?schedule: "3,13,23,33,43,53 \* \* \* \*"/,
  );
  assert.doesNotMatch(
    manifest,
    /- name: wiki-econ-admin-dispatcher[\s\S]*?continuous: true/,
  );
});

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
      `jobs load --job wiki-econ-fleet-controller ${manifest}`,
      `jobs load --job wiki-econ-fleet-small-a ${manifest}`,
      `jobs load --job wiki-econ-fleet-small-b ${manifest}`,
      `jobs load --job wiki-econ-fleet-medium ${manifest}`,
      `jobs load --job wiki-econ-admin-dispatcher ${manifest}`,
      `jobs load --job wiki-econ-publish-ready ${manifest}`,
      `jobs load --job wiki-econ-artifact-scrub ${manifest}`,
    ]);
    for (const name of [
      "wiki-econ-prepare-nlwiki", "wiki-econ-prepare-ptwiki", "wiki-econ-prepare-frwiki",
      "wiki-econ-prepare-itwiki", "wiki-econ-prepare-svwiki", "wiki-econ-prepare-elwiki",
      "wiki-econ-refresh", "wiki-econ-ingest", "wiki-econ-compute", "wiki-econ-site",
    ]) {
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
