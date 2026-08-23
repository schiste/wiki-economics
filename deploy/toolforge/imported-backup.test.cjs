"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const {spawnSync} = require("node:child_process");
const {after, test} = require("node:test");

const root = fs.mkdtempSync(path.join(os.tmpdir(), "wiki-econ-imported-backup-test-"));
after(() => fs.rmSync(root, {recursive: true, force: true}));
const create = path.join(__dirname, "create-imported-backup.sh");
const verify = path.join(__dirname, "verify-imported-backup.sh");
const restore = path.join(__dirname, "restore-imported-backup.sh");

test("imported backups are complete, checksummed, restorable, and tamper-evident", () => {
  const output = path.join(root, "output");
  for (const wiki of ["elwiki", "frwiki"]) {
    fs.mkdirSync(path.join(output, wiki), {recursive: true});
    fs.writeFileSync(path.join(output, wiki, "gdp.parquet"), `${wiki}-data`);
  }
  const lifecycle = path.join(root, "lifecycle.json");
  fs.writeFileSync(lifecycle, JSON.stringify({wikis: {
    elwiki: {publication: "published", provenance: "local-import"},
    frwiki: {publication: "published", provenance: "local-import"},
    nlwiki: {publication: "published", provenance: "toolforge"},
  }}));
  const archive = path.join(root, "imported.tar.gz");
  const fakeBin = path.join(root, "bin");
  fs.mkdirSync(fakeBin);
  fs.writeFileSync(path.join(fakeBin, "cp"), '#!/bin/sh\n[ "$1" != "-a" ] || exit 77\nexec /bin/cp "$@"\n', {mode: 0o755});
  const environment = {
    ...process.env,
    PATH: `${fakeBin}:${process.env.PATH}`,
    WIKI_ECON_OUTPUT_DIR: output,
    WIKI_ECON_WIKI_LIFECYCLE_FILE: lifecycle,
  };
  let result = spawnSync("bash", [create, archive], {encoding: "utf8", env: environment});
  assert.equal(result.status, 0, result.stderr);
  result = spawnSync("bash", [verify, archive], {encoding: "utf8"});
  assert.equal(result.status, 0, result.stderr);
  const restored = path.join(root, "restored");
  result = spawnSync("bash", [restore, archive, restored], {encoding: "utf8"});
  assert.equal(result.status, 0, result.stderr);
  assert.equal(fs.readFileSync(path.join(restored, "frwiki/gdp.parquet"), "utf8"), "frwiki-data");
  assert.equal(fs.existsSync(path.join(restored, "nlwiki")), false);
  assert.notEqual(spawnSync("bash", [restore, archive, restored]).status, 0);

  const changed = Buffer.from(fs.readFileSync(archive));
  changed[Math.floor(changed.length / 2)] ^= 0xff;
  fs.writeFileSync(path.join(root, "corrupt.tar.gz"), changed);
  assert.notEqual(spawnSync("bash", [verify, path.join(root, "corrupt.tar.gz")]).status, 0);
});
