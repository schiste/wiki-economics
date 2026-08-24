"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const {spawnSync} = require("node:child_process");
const {test} = require("node:test");

const script = path.join(__dirname, "rebuild-image.sh");
const commit = "a".repeat(40);
const sourceRef = `toolforge-image-${commit}`;
const digest = `tools.example/image:latest@sha256:${"b".repeat(64)}`;
const repository = "https://github.com/schiste/wiki-economics.git";

test("image rebuild rejects a moving ref before contacting Toolforge", () => {
  const result = spawnSync("bash", [script, repository, "main", commit], {encoding: "utf8"});
  assert.equal(result.status, 2);
  assert.match(result.stderr, /requires deterministic source ref/);
});

test("image rebuild verifies its immutable tag and records the exact commit and digest", () => {
  const fixture = fs.mkdtempSync(path.join(os.tmpdir(), "wiki-econ-image-build-"));
  try {
    const calls = path.join(fixture, "calls.txt");
    const git = path.join(fixture, "git");
    const toolforge = path.join(fixture, "toolforge");
    fs.writeFileSync(git, `#!/bin/sh
printf '%s\n' "$*" >> "${calls}"
printf '%s\trefs/tags/%s\n' '${commit}' '${sourceRef}'
`, {mode: 0o755});
    fs.writeFileSync(toolforge, `#!/bin/sh
printf '%s\n' "$*" >> "${calls}"
case "$1 $2" in
  "build start") printf '%s\n' '{"new_build":{"name":"build-1"}}' ;;
  "build show") printf '%s\n' '{"build":{"status":"ok","ref":"${sourceRef}","source_url":"${repository}","destination_image":"${digest}"}}' ;;
  "jobs list") printf '%s\n' '| wiki-econ-refresh | scheduled | ready |' ;;
esac
`, {mode: 0o755});
    const result = spawnSync("bash", [script, repository, sourceRef, commit], {
      encoding: "utf8",
      env: {...process.env, PATH: `${fixture}:${process.env.PATH}`},
    });
    assert.equal(result.status, 0, result.stderr || result.stdout);
    const invocations = fs.readFileSync(calls, "utf8");
    assert.equal(invocations.match(/ls-remote/g)?.length, 2);
    assert.match(invocations, new RegExp(`build start --ref ${sourceRef}`));
    assert.match(invocations, new RegExp(`envvars create WIKI_ECON_IMAGE_SOURCE_REF ${sourceRef}`));
    assert.match(invocations, new RegExp(`envvars create WIKI_ECON_IMAGE_SOURCE_COMMIT ${commit}`));
    assert.match(invocations, new RegExp(`envvars create WIKI_ECON_IMAGE_DIGEST ${digest}`));
    assert.match(invocations, /webservice restart/);
  } finally {
    fs.rmSync(fixture, {recursive: true, force: true});
  }
});

test("image rebuild rejects a tag that moves during the build", () => {
  const fixture = fs.mkdtempSync(path.join(os.tmpdir(), "wiki-econ-image-build-moved-"));
  try {
    const counter = path.join(fixture, "git-count");
    const git = path.join(fixture, "git");
    const toolforge = path.join(fixture, "toolforge");
    fs.writeFileSync(git, `#!/bin/sh
count=0
[ ! -f "${counter}" ] || count=$(cat "${counter}")
count=$((count + 1))
printf '%s' "$count" > "${counter}"
if [ "$count" -eq 1 ]; then
  printf '%s\trefs/tags/%s\n' '${commit}' '${sourceRef}'
else
  printf '%s\trefs/tags/%s\n' '${"c".repeat(40)}' '${sourceRef}'
fi
`, {mode: 0o755});
    fs.writeFileSync(toolforge, `#!/bin/sh
case "$1 $2" in
  "build start") printf '%s\n' '{"new_build":{"name":"build-1"}}' ;;
  "build show") printf '%s\n' '{"build":{"status":"ok","ref":"${sourceRef}","source_url":"${repository}","destination_image":"${digest}"}}' ;;
esac
`, {mode: 0o755});
    const result = spawnSync("bash", [script, repository, sourceRef, commit], {
      encoding: "utf8",
      env: {...process.env, PATH: `${fixture}:${process.env.PATH}`},
    });
    assert.equal(result.status, 1);
    assert.match(result.stderr, /does not resolve exactly/);
    assert.doesNotMatch(result.stdout, /Recording image provenance/);
  } finally {
    fs.rmSync(fixture, {recursive: true, force: true});
  }
});
