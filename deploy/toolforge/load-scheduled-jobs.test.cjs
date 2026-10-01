"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const {spawnSync} = require("node:child_process");
const {test} = require("node:test");

const script = path.join(__dirname, "load-scheduled-jobs.sh");

function jobResources(manifest, name) {
  const escaped = name.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const block = manifest.match(new RegExp(`(?:^|\\n)- name: ${escaped}\\n(?<body>[\\s\\S]*?)(?=\\n- name:|$)`));
  assert.ok(block, `missing job ${name}`);
  const memory = block.groups.body.match(/^  mem: (\d+)(Gi|Mi)$/m);
  const cpu = block.groups.body.match(/^  cpu: "([0-9.]+)"$/m);
  assert.ok(memory, `missing memory request for ${name}`);
  assert.ok(cpu, `missing CPU request for ${name}`);
  return {
    bytes: Number(memory[1]) * (memory[2] === "Gi" ? 1024 ** 3 : 1024 ** 2),
    millicores: Number(cpu[1]) * 1000,
  };
}

test("fleet capacity uses a fixed controller and worker pool", () => {
  const manifest = fs.readFileSync(path.join(__dirname, "jobs.yaml"), "utf8");
  const fleetJobs = [...manifest.matchAll(/^- name: (wiki-econ-fleet-[a-z-]+)$/gm)]
    .map((match) => match[1]);
  assert.deepEqual(fleetJobs, [
    "wiki-econ-fleet-controller",
    "wiki-econ-fleet-small-a",
    "wiki-econ-fleet-small-b",
    "wiki-econ-fleet-medium",
    "wiki-econ-fleet-medium-b",
    "wiki-econ-fleet-isolated",
  ]);
  assert.match(
    manifest,
    /- name: wiki-econ-fleet-controller[\s\S]*?schedule: "0 \*\/6 \* \* \*"/,
  );
  assert.match(
    manifest,
    /- name: wiki-econ-fleet-isolated[\s\S]*?command: deploy\/toolforge\/run-fleet-worker\.sh isolated isolated-monthly --once[\s\S]*?schedule: "20 \*\/6 \* \* \*"[\s\S]*?mem: 6Gi[\s\S]*?cpu: "4"/,
  );
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
      `jobs load --job wiki-econ-fleet-medium-b ${manifest}`,
      `jobs load --job wiki-econ-fleet-isolated ${manifest}`,
      `jobs load --job wiki-econ-admin-dispatcher ${manifest}`,
      `jobs load --job wiki-econ-publish-ready ${manifest}`,
      `jobs load --job wiki-econ-artifact-scrub ${manifest}`,
      `jobs load --job wiki-econ-fingerprint-check ${manifest}`,
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

test("scheduled job resources match the capacity admission source of truth", () => {
  const manifest = fs.readFileSync(path.join(__dirname, "jobs.yaml"), "utf8");
  const capacity = JSON.parse(fs.readFileSync(path.join(__dirname, "../../config/toolforge-capacity.json"), "utf8"));
  const classes = {
    "wiki-econ-fleet-controller": "controller",
    "wiki-econ-fleet-small-a": "small",
    "wiki-econ-fleet-small-b": "small",
    "wiki-econ-fleet-medium": "medium_large",
    "wiki-econ-fleet-medium-b": "medium_large",
    "wiki-econ-fleet-isolated": "isolated",
    "wiki-econ-admin-dispatcher": "admin_dispatcher",
    "wiki-econ-publish-ready": "publisher",
    "wiki-econ-artifact-scrub": "scrubber",
  };
  for (const [name, resourceClass] of Object.entries(classes)) {
    const request = jobResources(manifest, name);
    assert.equal(request.bytes, capacity.resource_requests[resourceClass], `${name} memory drift`);
    assert.equal(
      request.millicores,
      capacity.resource_cpu_requests_millicores[resourceClass],
      `${name} CPU drift`,
    );
    assert.ok(request.bytes <= capacity.per_job_memory_limit_bytes);
    assert.ok(request.millicores <= capacity.per_job_cpu_limit_millicores);
  }
  const publisher = manifest
    .split("- name: wiki-econ-publish-ready\n")[1]
    ?.split("\n- name:", 1)[0];
  assert.ok(publisher, "missing ready publisher job");
  assert.match(
    publisher,
    /^  command: \/usr\/bin\/env RAYON_NUM_THREADS=4 POLARS_MAX_THREADS=4 deploy\/toolforge\/run-publish-ready\.sh$/m,
  );
});

test("a missing manifest fails before Toolforge is contacted", () => {
  const result = spawnSync("bash", [script, "/definitely/missing/jobs.yaml"], {encoding: "utf8"});
  assert.equal(result.status, 1);
  assert.match(result.stderr, /manifest is missing/);
});

test("wrapper memory defaults match the checked-in Toolforge capacity policy", () => {
  const capacity = JSON.parse(fs.readFileSync(
    path.join(__dirname, "..", "..", "config", "toolforge-capacity.json"), "utf8"));
  // The admission helper now injects the envelope for the admitted class, so
  // these fallbacks only apply to a direct invocation. They must still agree
  // with the policy: a wrapper that restates the ceiling is a chance to drift.
  const expected = {
    "deploy/toolforge/run-refresh.sh": capacity.per_job_memory_limit_bytes,
    "deploy/toolforge/run-prepare-wiki.sh": capacity.per_job_memory_limit_bytes,
    "deploy/toolforge/run-qualify-wiki.sh": capacity.per_job_memory_limit_bytes,
  };
  for (const [file, ceiling] of Object.entries(expected)) {
    const source = fs.readFileSync(path.join(__dirname, "..", "..", file), "utf8");
    assert.match(
      source,
      new RegExp(`WIKI_ECON_MEMORY_CEILING_BYTES="\\$\\{WIKI_ECON_MEMORY_CEILING_BYTES:-${ceiling}\\}"`),
      `${file} memory ceiling fallback is not per_job_memory_limit_bytes (${ceiling})`,
    );
  }

  // run-fleet-worker.sh serves two classes and must match each one.
  const worker = fs.readFileSync(
    path.join(__dirname, "run-fleet-worker.sh"), "utf8");
  assert.match(worker, new RegExp(
    `WIKI_ECON_MEMORY_CEILING_BYTES="\\$\\{WIKI_ECON_MEMORY_CEILING_BYTES:-${capacity.resource_requests.small}\\}"`));
  assert.match(worker, new RegExp(
    `WIKI_ECON_MEMORY_CEILING_BYTES="\\$\\{WIKI_ECON_MEMORY_CEILING_BYTES:-${capacity.resource_requests.medium_large}\\}"`));

  // And every scheduled job's declared mem: must not exceed the per-job limit.
  const jobs = fs.readFileSync(path.join(__dirname, "jobs.yaml"), "utf8");
  const unit = {K: 1024, M: 1024 ** 2, G: 1024 ** 3};
  for (const match of jobs.matchAll(/^\s*mem:\s*"?([0-9.]+)([KMG])(?:i)?"?\s*$/gm)) {
    const [, amount, suffix] = match;
    const bytes = Number(amount) * unit[suffix];
    assert.ok(bytes <= capacity.per_job_memory_limit_bytes,
      `jobs.yaml requests ${amount}${suffix} which exceeds per_job_memory_limit_bytes `
      + `(${capacity.per_job_memory_limit_bytes})`);
  }
});
