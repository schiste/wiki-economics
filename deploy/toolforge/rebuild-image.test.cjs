"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const {spawnSync} = require("node:child_process");
const {test} = require("node:test");

const script = path.join(__dirname, "rebuild-image.sh");
const commit = "a".repeat(40);
const digest = `tools.example/image:latest@sha256:${"b".repeat(64)}`;
const repository = "https://github.com/schiste/wiki-economics.git";

test("image rebuild rejects a moving ref before contacting Toolforge", () => {
  const result = spawnSync("bash", [script, repository, "main", commit], {encoding: "utf8"});
  assert.equal(result.status, 2);
  assert.match(result.stderr, /exact source commit as both ref and commit/);
});

test("image rebuild verifies and records the exact commit and immutable digest", () => {
  const fixture = fs.mkdtempSync(path.join(os.tmpdir(), "wiki-econ-image-build-"));
  try {
    const calls = path.join(fixture, "calls.txt");
    const toolforge = path.join(fixture, "toolforge");
    fs.writeFileSync(toolforge, `#!/bin/sh\nprintf '%s\\n' "$*" >> "${calls}"\ncase "$1 $2" in\n  "build start") printf '%s\\n' '{"new_build":{"name":"build-1"}}' ;;\n  "build show") printf '%s\\n' '{"build":{"status":"ok","ref":"${commit}","source_url":"${repository}","destination_image":"${digest}"}}' ;;\n  "jobs list") printf '%s\\n' '| wiki-econ-refresh | scheduled | ready |' ;;\nesac\n`, {mode: 0o755});
    const result = spawnSync("bash", [script, repository, commit, commit], {
      encoding: "utf8",
      env: {...process.env, PATH: `${fixture}:${process.env.PATH}`},
    });
    assert.equal(result.status, 0, result.stderr || result.stdout);
    const invocations = fs.readFileSync(calls, "utf8");
    assert.match(invocations, new RegExp(`build start --ref ${commit}`));
    assert.match(invocations, new RegExp(`envvars create WIKI_ECON_IMAGE_SOURCE_REF ${commit}`));
    assert.match(invocations, new RegExp(`envvars create WIKI_ECON_IMAGE_SOURCE_COMMIT ${commit}`));
    assert.match(invocations, new RegExp(`envvars create WIKI_ECON_IMAGE_DIGEST ${digest}`));
    assert.match(invocations, /webservice restart/);
  } finally {
    fs.rmSync(fixture, {recursive: true, force: true});
  }
});
