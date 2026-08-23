const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const crypto = require("node:crypto");
const {spawnSync} = require("node:child_process");
const {after, test} = require("node:test");

const script = path.join(__dirname, "prune-releases.sh");
const fixtureRoot = fs.mkdtempSync(path.join(os.tmpdir(), "wiki-econ-release-prune-"));

after(() => fs.rmSync(fixtureRoot, {recursive: true, force: true}));

function run(appRoot, retention = 3, extraEnv = {}) {
  return spawnSync("bash", [script, appRoot, String(retention)], {
    encoding: "utf8",
    env: {...process.env, ...extraEnv},
  });
}

function createRelease(appRoot, sha, modifiedMs, complete = true) {
  const release = path.join(appRoot, "releases", sha);
  fs.mkdirSync(release, {recursive: true});
  if (complete) {
    const binary = Buffer.from("#!/bin/sh\nexit 0\n");
    fs.writeFileSync(path.join(release, "wiki-econ"), binary, {mode: 0o755});
    const checksum = crypto.createHash("sha256").update(binary).digest("hex");
    fs.writeFileSync(path.join(release, "wiki-econ.sha256"), `${checksum}  wiki-econ\n`);
  }
  const modified = new Date(modifiedMs);
  fs.utimesSync(release, modified, modified);
  return release;
}

test("keeps the live release and two newest complete rollbacks", () => {
  const appRoot = path.join(fixtureRoot, "bounded");
  const currentSha = "a".repeat(40);
  const newestSha = "b".repeat(40);
  const secondSha = "c".repeat(40);
  const oldestSha = "d".repeat(40);
  const incompleteSha = "e".repeat(40);
  createRelease(appRoot, currentSha, 1_000);
  createRelease(appRoot, newestSha, 5_000);
  createRelease(appRoot, secondSha, 4_000);
  createRelease(appRoot, oldestSha, 3_000);
  createRelease(appRoot, incompleteSha, 6_000, false);
  const invalid = path.join(appRoot, "releases", "operator-notes");
  fs.mkdirSync(invalid);
  fs.symlinkSync(path.join("releases", currentSha), path.join(appRoot, "current"));

  const incoming = path.join(appRoot, "incoming");
  fs.mkdirSync(incoming);
  const stalePart = path.join(incoming, `${"f".repeat(40)}.part`);
  const staleProvenancePart = path.join(incoming, `${"f".repeat(40)}.provenance.part`);
  const staleBundlePart = path.join(incoming, `${"f".repeat(40)}.release.tar.gz.part`);
  const recentPart = path.join(incoming, `${"1".repeat(40)}.part`);
  const unrelatedPart = path.join(incoming, "notes.part");
  for (const file of [stalePart, staleProvenancePart, staleBundlePart, recentPart, unrelatedPart]) fs.writeFileSync(file, "partial");
  fs.utimesSync(stalePart, new Date(1_000), new Date(1_000));
  fs.utimesSync(staleProvenancePart, new Date(1_000), new Date(1_000));
  fs.utimesSync(staleBundlePart, new Date(1_000), new Date(1_000));

  const result = run(appRoot, 3, {WIKI_ECON_INCOMING_STALE_SECS: "3600"});
  assert.equal(result.status, 0, `${result.stdout}\n${result.stderr}`);
  assert.deepEqual(JSON.parse(result.stdout), {
    release_directories: 2,
    incoming_files: 3,
    retained_releases: 3,
  });
  for (const sha of [currentSha, newestSha, secondSha]) {
    assert.equal(fs.existsSync(path.join(appRoot, "releases", sha)), true);
  }
  for (const sha of [oldestSha, incompleteSha]) {
    assert.equal(fs.existsSync(path.join(appRoot, "releases", sha)), false);
  }
  assert.equal(fs.existsSync(invalid), true);
  assert.equal(fs.existsSync(stalePart), false);
  assert.equal(fs.existsSync(staleProvenancePart), false);
  assert.equal(fs.existsSync(staleBundlePart), false);
  assert.equal(fs.existsSync(recentPart), true);
  assert.equal(fs.existsSync(unrelatedPart), true);
  assert.equal(fs.readlinkSync(path.join(appRoot, "current")), path.join("releases", currentSha));
});

test("fails closed on a malformed live link", () => {
  const appRoot = path.join(fixtureRoot, "malformed");
  const release = createRelease(appRoot, "2".repeat(40), 1_000);
  fs.symlinkSync("elsewhere", path.join(appRoot, "current"));

  const result = run(appRoot);
  assert.notEqual(result.status, 0);
  assert.match(result.stderr, /Refusing unexpected current release target/);
  assert.equal(fs.existsSync(release), true);
});

test("fails closed when a supply-chain-aware live release loses its envelope", () => {
  const appRoot = path.join(fixtureRoot, "damaged-envelope");
  const sha = "9".repeat(40);
  const release = createRelease(appRoot, sha, 1_000);
  fs.writeFileSync(path.join(release, "release-provenance.json"), JSON.stringify({schema_version: 2, source_commit: sha}));
  fs.symlinkSync(path.join("releases", sha), path.join(appRoot, "current"));

  const result = run(appRoot);
  assert.notEqual(result.status, 0);
  assert.match(result.stderr, /current release is incomplete/);
  assert.equal(fs.existsSync(release), true);
});

test("a missing app root is a safe no-op", () => {
  const result = run(path.join(fixtureRoot, "missing"));
  assert.equal(result.status, 0, `${result.stdout}\n${result.stderr}`);
  assert.deepEqual(JSON.parse(result.stdout), {
    release_directories: 0,
    incoming_files: 0,
    retained_releases: 0,
  });
});

test("a current-only app remains valid under Bash 3.2 empty-array semantics", () => {
  const appRoot = path.join(fixtureRoot, "current-only");
  const currentSha = "3".repeat(40);
  createRelease(appRoot, currentSha, 1_000);
  fs.symlinkSync(path.join("releases", currentSha), path.join(appRoot, "current"));

  const result = run(appRoot);
  assert.equal(result.status, 0, `${result.stdout}\n${result.stderr}`);
  assert.deepEqual(JSON.parse(result.stdout), {
    release_directories: 0,
    incoming_files: 0,
    retained_releases: 1,
  });
  assert.equal(fs.existsSync(path.join(appRoot, "releases", currentSha)), true);
});
