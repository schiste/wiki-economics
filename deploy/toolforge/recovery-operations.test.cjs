"use strict";

const assert = require("node:assert/strict");
const crypto = require("node:crypto");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const {spawnSync} = require("node:child_process");
const {after, test} = require("node:test");

const fixtureRoot = fs.mkdtempSync(path.join(os.tmpdir(), "wiki-econ-recovery-ops-"));
after(() => fs.rmSync(fixtureRoot, {recursive: true, force: true}));
const recover = path.join(__dirname, "recover-stage.sh");
const rollbackDrill = path.join(__dirname, "drill-binary-rollback.sh");
const rebuildDrill = path.join(__dirname, "run-rebuild-drill.sh");

function executable(file, body) {
  fs.mkdirSync(path.dirname(file), {recursive: true});
  fs.writeFileSync(file, `#!/bin/bash\nset -eu\n${body}\n`, {mode: 0o755});
  return file;
}

test("stage recovery dispatches minimal replay commands and repairs a lost site link", () => {
  const root = path.join(fixtureRoot, "stage");
  const output = path.join(root, "output");
  const data = path.join(root, "data");
  const dist = path.join(root, "site-dist");
  const log = path.join(root, "commands.log");
  fs.mkdirSync(output, {recursive: true});
  const bin = executable(path.join(root, "wiki-econ"), `printf '%s\\n' "$*" >> "${log}"`);
  const refresh = executable(path.join(root, "refresh"), `printf 'refresh %s\\n' "$*" >> "${log}"`);
  const environment = {...process.env, WIKI_ECON_BIN: bin, WIKI_ECON_DATA_DIR: data,
    WIKI_ECON_OUTPUT_DIR: output, WIKI_ECON_SITE_DIST_DIR: dist, WIKI_ECON_RECOVERY_REFRESH_DRIVER: refresh};

  for (const args of [["ingest", "nlwiki", "2026-07"], ["compute", "nlwiki"],
    ["pointer", "nlwiki", "2026-07"], ["site"]]) {
    const result = spawnSync("bash", [recover, ...args], {encoding: "utf8", env: environment});
    assert.equal(result.status, 0, result.stderr);
  }
  const commands = fs.readFileSync(log, "utf8");
  assert.match(commands, /fetch nlwiki --version 2026-07/);
  assert.match(commands, /ingest nlwiki --version 2026-07/);
  assert.match(commands, /compute nlwiki/);
  assert.match(commands, /snapshot-repair nlwiki --version 2026-07/);
  assert.match(commands, /refresh .*--merge-only/);

  const generation = ".site-dist.build.recovered";
  fs.mkdirSync(path.join(root, generation));
  for (const page of ["business.html", "gdp.html", "inequality.html", "labor.html", "patrol.html", "edit-variation.html"]) {
    fs.writeFileSync(path.join(root, generation, page), page);
  }
  let result = spawnSync("bash", [recover, "site-link", generation], {encoding: "utf8", env: environment});
  assert.equal(result.status, 0, result.stderr);
  assert.equal(fs.readlinkSync(dist), generation);

  fs.mkdirSync(path.join(output, ".refresh-lock"));
  result = spawnSync("bash", [recover, "compute", "nlwiki"], {encoding: "utf8", env: environment});
  assert.equal(result.status, 75);
});

test("the rollback drill really switches binaries and restores the original release", () => {
  const root = path.join(fixtureRoot, "rollback");
  const app = path.join(root, "app");
  const reports = path.join(root, "reports");
  const original = "1".repeat(40);
  const candidate = "2".repeat(40);
  for (const sha of [original, candidate]) {
    const directory = path.join(app, "releases", sha);
    const binary = executable(path.join(directory, "wiki-econ"), '[ "$1" = "--help" ]');
    const checksum = crypto.createHash("sha256").update(fs.readFileSync(binary)).digest("hex");
    fs.writeFileSync(path.join(directory, "wiki-econ.sha256"), `${checksum}  wiki-econ\n`);
  }
  fs.symlinkSync(`releases/${original}`, path.join(app, "current"));
  const result = spawnSync("bash", [rollbackDrill, candidate], {encoding: "utf8", env: {
    ...process.env, WIKI_ECON_TOOLFORGE_APP_ROOT: app, WIKI_ECON_OUTPUT_DIR: path.join(root, "output"),
    WIKI_ECON_OPERATIONS_REPORT_DIR: reports,
  }});
  assert.equal(result.status, 0, result.stderr);
  assert.equal(fs.readlinkSync(path.join(app, "current")), `releases/${original}`);
  const report = JSON.parse(fs.readFileSync(path.join(reports, fs.readdirSync(reports)[0]), "utf8"));
  assert.equal(report.succeeded, true);
  assert.equal(report.rollback_release, candidate);
  assert.equal(report.restored_release, original);
});

test("the rebuild drill starts with only restored imports and keeps live paths untouched", () => {
  const root = path.join(fixtureRoot, "rebuild");
  const operations = path.join(root, "operations");
  const liveOutput = path.join(root, "live-output");
  const backup = path.join(root, "backup.tar.gz");
  fs.mkdirSync(liveOutput, {recursive: true});
  fs.writeFileSync(backup, "fixture");
  const restore = executable(path.join(root, "restore"), 'mkdir -p "$2/frwiki"; printf imported > "$2/frwiki/gdp.parquet"');
  const binary = executable(path.join(root, "wiki-econ"), "exit 0");
  const build = executable(path.join(root, "build-site"), `
dist=""; while [ "$#" -gt 0 ]; do if [ "$1" = "--dist-dir" ]; then dist=$2; shift 2; else shift; fi; done
mkdir -p "$dist"
for page in index business gdp inequality labor patrol edit-variation; do printf ok > "$dist/$page.html"; done`);
  const result = spawnSync("bash", [rebuildDrill, backup, "nlwiki", "ptwiki"], {encoding: "utf8", env: {
    ...process.env, WIKI_ECON_BIN: binary, WIKI_ECON_DATA_DIR: path.join(root, "data"),
    WIKI_ECON_OUTPUT_DIR: liveOutput, WIKI_ECON_OPERATIONS_ROOT: operations,
    WIKI_ECON_RESTORE_SCRIPT: restore, WIKI_ECON_BUILD_SITE_SCRIPT: build,
  }});
  assert.equal(result.status, 0, result.stderr);
  assert.deepEqual(fs.readdirSync(liveOutput), []);
  const reports = fs.readdirSync(path.join(operations, "reports"));
  assert.equal(reports.length, 1);
  assert.equal(JSON.parse(fs.readFileSync(path.join(operations, "reports", reports[0]))).succeeded, true);
  assert.deepEqual(fs.readdirSync(path.join(operations, "staging")), []);
});
