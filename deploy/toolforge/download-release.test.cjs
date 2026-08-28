"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const {spawnSync} = require("node:child_process");
const {test} = require("node:test");

const script = path.join(__dirname, "download-release.sh");
const releaseSha = "a".repeat(40);
const archiveName = `wiki-econ-release-${releaseSha}.tar.gz`;
const checksumName = `${archiveName}.sha256`;

function fixture(mode = "nested") {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "wiki-econ-release-download-"));
  const bin = path.join(root, "bin");
  const destination = path.join(root, "release");
  fs.mkdirSync(bin);
  const gh = path.join(bin, "gh");
  fs.writeFileSync(gh, `#!/bin/bash
set -euo pipefail
if [ "$1 $2" = "run list" ]; then
  printf '12345\\n'
elif [ "$1 $2" = "run view" ]; then
  printf '${releaseSha}\\tcompleted\\tsuccess\\n'
elif [ "$1 $2" = "run download" ]; then
  destination=''
  while [ "$#" -gt 0 ]; do
    if [ "$1" = --dir ]; then destination=$2; break; fi
    shift
  done
  nested="$destination/wiki-economics/target/release"
  mkdir -p "$nested"
  printf 'release payload' > "$nested/${archiveName}"
  checksum=$(shasum -a 256 "$nested/${archiveName}" | awk '{print $1}')
  printf '%s  ${archiveName}\\n' "$checksum" > "$nested/${checksumName}"
  if [ '${mode}' = duplicate ]; then
    mkdir -p "$destination/duplicate"
    cp "$nested/${checksumName}" "$destination/duplicate/${checksumName}"
  elif [ '${mode}' = corrupt ]; then
    printf 'corrupt' >> "$nested/${archiveName}"
  fi
else
  exit 99
fi
`, {mode: 0o755});
  return {root, destination, env: {...process.env, PATH: `${bin}:${process.env.PATH}`}};
}

function run(testFixture) {
  return spawnSync("bash", [script, releaseSha, testFixture.destination], {
    encoding: "utf8",
    env: testFixture.env,
  });
}

test("release download follows its checksum manifest through a nested artifact layout", () => {
  const testFixture = fixture();
  try {
    const result = run(testFixture);
    assert.equal(result.status, 0, result.stderr || result.stdout);
    const archive = path.join(testFixture.destination, archiveName);
    const checksum = path.join(testFixture.destination, checksumName);
    assert.equal(fs.readFileSync(archive, "utf8"), "release payload");
    assert.equal(fs.existsSync(checksum), true);
    assert.match(result.stdout, /workflow_run_id=12345/);
    assert.match(result.stdout, new RegExp(`release_archive=${archive}`));
  } finally {
    fs.rmSync(testFixture.root, {recursive: true, force: true});
  }
});

test("release download rejects ambiguous checksum manifests", () => {
  const testFixture = fixture("duplicate");
  try {
    const result = run(testFixture);
    assert.notEqual(result.status, 0);
    assert.match(result.stderr, /exactly one/);
    assert.equal(fs.existsSync(path.join(testFixture.destination, archiveName)), false);
  } finally {
    fs.rmSync(testFixture.root, {recursive: true, force: true});
  }
});

test("release download rejects content that differs from its manifest", () => {
  const testFixture = fixture("corrupt");
  try {
    const result = run(testFixture);
    assert.notEqual(result.status, 0);
    assert.match(result.stderr, /checksum mismatch/);
  } finally {
    fs.rmSync(testFixture.root, {recursive: true, force: true});
  }
});
