"use strict";

const assert = require("node:assert/strict");
const crypto = require("node:crypto");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const {spawnSync} = require("node:child_process");
const {after, test} = require("node:test");
const {prepareBundle} = require("./site-source-bundle.cjs");

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
      WIKI_ECON_IMAGE_SOURCE_REF: `toolforge-image-${fixture.commit}`,
      WIKI_ECON_IMAGE_DIGEST: `registry/toolforge-image@sha256:${"1".repeat(64)}`,
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

test("production runtime treats binary and image as independently verified identities", () => {
  const imageMismatch = releaseFixture("image-mismatch");
  const imageCommit = "b".repeat(40);
  const result = initialize(imageMismatch, {
    WIKI_ECON_IMAGE_SOURCE_COMMIT: imageCommit,
    WIKI_ECON_IMAGE_SOURCE_REF: `toolforge-image-${imageCommit}`,
  });
  assert.equal(result.status, 0, result.stderr);
});

test("production runtime rejects malformed image, checksum, and sidecar identity", () => {
  const malformedImage = releaseFixture("image-malformed");
  let result = initialize(malformedImage, {WIKI_ECON_IMAGE_SOURCE_REF: "wrong-ref"});
  assert.notEqual(result.status, 0);
  assert.match(result.stderr, /image provenance is incomplete or malformed/);

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

test("production runtime verifies an immutable site-source release independently", () => {
  const fixture = releaseFixture("site-source");
  const siteCommit = "e".repeat(40);
  const siteRelease = path.join(fixtureRoot, "site-source-release", siteCommit);
  const provenance = prepareBundle(ROOT, siteRelease, siteCommit, "1788100000");
  let result = initialize(fixture, {
    WIKI_ECON_SITE_DIR: path.join(siteRelease, "site"),
    WIKI_ECON_SITE_SOURCE_REQUIRED: "1",
    WIKI_ECON_SITE_SOURCE_COMMIT: siteCommit,
    WIKI_ECON_SITE_SOURCE_SHA256: provenance.content_sha256,
  });
  assert.equal(result.status, 0, result.stderr);

  fs.appendFileSync(path.join(siteRelease, "site/src/style.css"), "tampered\n");
  result = initialize(fixture, {
    WIKI_ECON_SITE_DIR: path.join(siteRelease, "site"),
    WIKI_ECON_SITE_SOURCE_REQUIRED: "1",
    WIKI_ECON_SITE_SOURCE_COMMIT: siteCommit,
    WIKI_ECON_SITE_SOURCE_SHA256: provenance.content_sha256,
  });
  assert.notEqual(result.status, 0);
  assert.match(result.stderr, /failed identity verification/);
});
