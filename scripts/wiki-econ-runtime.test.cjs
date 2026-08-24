"use strict";

const assert = require("node:assert/strict");
const crypto = require("node:crypto");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const {spawnSync} = require("node:child_process");
const {after, test} = require("node:test");

const ROOT = path.resolve(__dirname, "..");
const LIBRARY = path.join(ROOT, "scripts", "lib", "wiki_econ.sh");
const fixtureRoot = fs.mkdtempSync(path.join(os.tmpdir(), "wiki-econ-runtime-"));
after(() => fs.rmSync(fixtureRoot, {recursive: true, force: true}));

function releaseFixture(name, commit = "a".repeat(40)) {
  const release = path.join(fixtureRoot, name, "releases", commit);
  const current = path.join(fixtureRoot, name, "current");
  fs.mkdirSync(release, {recursive: true});
  const binary = path.join(release, "wiki-econ");
  fs.writeFileSync(binary, "#!/usr/bin/env bash\nexit 0\n", {mode: 0o755});
  const checksum = crypto.createHash("sha256").update(fs.readFileSync(binary)).digest("hex");
  fs.writeFileSync(path.join(release, "release-provenance.json"), `${JSON.stringify({
    schema_version: 2,
    source_commit: commit,
    binary: {name: "wiki-econ", sha256: checksum},
  })}\n`);
  fs.symlinkSync(path.relative(path.dirname(current), release), current);
  return {binary: path.join(current, "wiki-econ"), checksum, commit, release};
}

function initialize(fixture, extra = {}) {
  return spawnSync("bash", ["-c", [
    "set -euo pipefail",
    `source "${LIBRARY}"`,
    "wiki_econ_init_runtime",
    "printf '%s\\n%s\\n' \"$WIKI_ECON_SOURCE_COMMIT\" \"$WIKI_ECON_BINARY_SHA256\"",
  ].join("; ")], {
    encoding: "utf8",
    env: {
      ...process.env,
      WIKI_ECON_BIN: fixture.binary,
      WIKI_ECON_ENV: "production",
      WIKI_ECON_IMAGE_SOURCE_COMMIT: fixture.commit,
      ...extra,
    },
  });
}

test("production runtime derives commit and checksum from the deployed release", () => {
  const fixture = releaseFixture("valid");
  const result = initialize(fixture);
  assert.equal(result.status, 0, result.stderr);
  assert.deepEqual(result.stdout.trim().split("\n"), [fixture.commit, fixture.checksum]);
});

test("production runtime rejects image, checksum, and sidecar disagreement", () => {
  const imageMismatch = releaseFixture("image-mismatch");
  let result = initialize(imageMismatch, {WIKI_ECON_IMAGE_SOURCE_COMMIT: "b".repeat(40)});
  assert.notEqual(result.status, 0);
  assert.match(result.stderr, /Binary and image source commits disagree/);

  const checksumMismatch = releaseFixture("checksum-mismatch");
  fs.appendFileSync(path.join(checksumMismatch.release, "wiki-econ"), "# changed\n");
  result = initialize(checksumMismatch);
  assert.notEqual(result.status, 0);
  assert.match(result.stderr, /checksum does not match release provenance/);

  const missingSidecar = releaseFixture("missing-sidecar");
  fs.unlinkSync(path.join(missingSidecar.release, "release-provenance.json"));
  result = initialize(missingSidecar);
  assert.notEqual(result.status, 0);
  assert.match(result.stderr, /has no release provenance/);
});
