"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const {after, test} = require("node:test");
const {makeBom} = require("./generate-sboms.cjs");
const {sha256, supplyChainArtifacts} = require("./release-provenance.cjs");
const {verifyReleaseBundle, writeChecksums} = require("./release-bundle.cjs");

const temporary = fs.mkdtempSync(path.join(os.tmpdir(), "wiki-econ-release-bundle-test-"));
after(() => fs.rmSync(temporary, {recursive: true, force: true}));

function json(file, value) {
  fs.writeFileSync(file, `${JSON.stringify(value, null, 2)}\n`);
}

function fixture(directory, commit) {
  fs.mkdirSync(directory, {recursive: true});
  fs.writeFileSync(path.join(directory, "wiki-econ"), "binary");
  const binaryHash = sha256(path.join(directory, "wiki-econ"));
  fs.writeFileSync(path.join(directory, "THIRD_PARTY_NOTICES.md"), "# Notices\n");
  json(path.join(directory, "third-party-notices.json"), {
    schema_version: 1,
    source_commit: commit,
    rust: [{name: "crate"}],
    toolforge_runtime: [{name: "node"}],
    toolforge_image_npm: [{name: "package"}],
    published_browser: [{name: "browser-package"}],
  });
  for (const [name, artifact] of [
    ["wiki-econ-rust-binary.cdx.json", "rust-binary"],
    ["wiki-econ-toolforge-site-image.cdx.json", "toolforge-site-image-closure"],
    ["wiki-econ-browser-bundle.cdx.json", "published-browser-bundle"],
  ]) {
    json(path.join(directory, name), makeBom({
      artifact,
      commit,
      timestamp: "2026-08-23T00:00:00.000Z",
      rootComponent: {
        type: "application",
        name: artifact,
        version: commit,
        identity: {"artifact-sha256": artifact === "rust-binary" ? binaryHash : "c".repeat(64)},
      },
      components: [],
    }));
  }
  json(path.join(directory, "release-provenance.json"), {
    schema_version: 2,
    source_commit: commit,
    binary: {sha256: sha256(path.join(directory, "wiki-econ"))},
    supply_chain: supplyChainArtifacts(directory, commit),
  });
  writeChecksums(directory);
}

test("the complete release envelope validates and tampering fails closed", () => {
  const commit = "d".repeat(40);
  const directory = path.join(temporary, "valid");
  fixture(directory, commit);
  assert.equal(verifyReleaseBundle(directory, commit).files, 8);
  fs.appendFileSync(path.join(directory, "wiki-econ-browser-bundle.cdx.json"), " ");
  assert.throws(() => verifyReleaseBundle(directory, commit), /checksum mismatch/);
});

test("a bundle for another commit is rejected", () => {
  const directory = path.join(temporary, "wrong-commit");
  fixture(directory, "e".repeat(40));
  assert.throws(() => verifyReleaseBundle(directory, "f".repeat(40)), /release provenance does not match/);
});
