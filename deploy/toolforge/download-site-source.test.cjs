"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const {spawnSync} = require("node:child_process");
const {test} = require("node:test");

const script = path.join(__dirname, "download-site-source.sh");
const releaseSha = "a".repeat(40);
const archiveName = `wiki-econ-site-source-${releaseSha}.tar.gz`;
const checksumName = `${archiveName}.sha256`;

function fixture(mode = "valid") {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "wiki-econ-site-source-download-"));
  const bin = path.join(root, "bin");
  const destination = path.join(root, "download");
  fs.mkdirSync(bin);
  fs.writeFileSync(path.join(bin, "gh"), `#!/bin/bash
set -euo pipefail
if [ "$1 $2" = "run list" ]; then
  printf '45678\\n'
elif [ "$1 $2" = "run view" ]; then
  printf '${releaseSha}\\tcompleted\\tsuccess\\n'
elif [ "$1 $2" = "run download" ]; then
  while [ "$#" -gt 0 ]; do
    if [ "$1" = --dir ]; then destination=$2; break; fi
    shift
  done
  mkdir -p "$destination/nested"
  printf 'site source' > "$destination/nested/${archiveName}"
  checksum=$(shasum -a 256 "$destination/nested/${archiveName}" | awk '{print $1}')
  printf '%s  ${archiveName}\\n' "$checksum" > "$destination/nested/${checksumName}"
  if [ '${mode}' = duplicate ]; then cp "$destination/nested/${checksumName}" "$destination/${checksumName}"; fi
else
  exit 99
fi
`, {mode: 0o755});
  return {root, destination, env: {...process.env, PATH: `${bin}:${process.env.PATH}`}};
}

test("site-source download resolves the exact successful commit artifact", () => {
  const value = fixture();
  try {
    const result = spawnSync("bash", [script, releaseSha, value.destination], {encoding: "utf8", env: value.env});
    assert.equal(result.status, 0, result.stderr || result.stdout);
    assert.equal(fs.readFileSync(path.join(value.destination, archiveName), "utf8"), "site source");
    assert.match(result.stdout, /workflow_run_id=45678/);
  } finally {
    fs.rmSync(value.root, {recursive: true, force: true});
  }
});

test("site-source download rejects ambiguous checksum manifests", () => {
  const value = fixture("duplicate");
  try {
    const result = spawnSync("bash", [script, releaseSha, value.destination], {encoding: "utf8", env: value.env});
    assert.notEqual(result.status, 0);
    assert.match(result.stderr, /exactly one/);
  } finally {
    fs.rmSync(value.root, {recursive: true, force: true});
  }
});
