"use strict";

const assert = require("node:assert/strict");
const crypto = require("node:crypto");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const {spawnSync} = require("node:child_process");
const {after, test} = require("node:test");
const {makeBom} = require("../../scripts/generate-sboms.cjs");
const {sha256, supplyChainArtifacts} = require("../../scripts/release-provenance.cjs");
const {PAYLOAD, writeChecksums} = require("../../scripts/release-bundle.cjs");

const script = path.join(__dirname, "install-binary.sh");
const temporary = fs.mkdtempSync(path.join(os.tmpdir(), "wiki-econ-install-test-"));
after(() => fs.rmSync(temporary, {recursive: true, force: true}));

function run(appRoot, sha, checksum, staged) {
  return spawnSync("bash", [script, sha, checksum, staged], {
    encoding: "utf8",
    env: {...process.env, WIKI_ECON_TOOLFORGE_APP_ROOT: appRoot},
  });
}

function json(file, value) {
  fs.writeFileSync(file, `${JSON.stringify(value, null, 2)}\n`);
}

function validArchive(appRoot, sha) {
  const source = path.join(appRoot, "bundle");
  const staged = path.join(appRoot, "incoming", `${sha}.release.tar.gz.part`);
  fs.mkdirSync(source, {recursive: true});
  fs.mkdirSync(path.dirname(staged), {recursive: true});
  fs.copyFileSync("/bin/true", path.join(source, "wiki-econ"));
  fs.writeFileSync(path.join(source, "THIRD_PARTY_NOTICES.md"), "# Notices\n");
  json(path.join(source, "third-party-notices.json"), {
    schema_version: 1,
    source_commit: sha,
    rust: [{name: "crate"}],
    toolforge_runtime: [{name: "node"}],
    toolforge_image_npm: [{name: "package"}],
    published_browser: [{name: "browser-package"}],
  });
  const binaryHash = sha256(path.join(source, "wiki-econ"));
  for (const [name, artifact] of [
    ["wiki-econ-rust-binary.cdx.json", "rust-binary"],
    ["wiki-econ-toolforge-site-image.cdx.json", "toolforge-site-image-closure"],
    ["wiki-econ-browser-bundle.cdx.json", "published-browser-bundle"],
  ]) {
    json(path.join(source, name), makeBom({
      artifact,
      commit: sha,
      timestamp: "2026-08-23T00:00:00.000Z",
      rootComponent: {
        type: "application",
        name: artifact,
        version: sha,
        identity: {"artifact-sha256": artifact === "rust-binary" ? binaryHash : "c".repeat(64)},
      },
      components: [],
    }));
  }
  json(path.join(source, "release-provenance.json"), {
    schema_version: 2,
    source_commit: sha,
    binary: {sha256: binaryHash},
    supply_chain: supplyChainArtifacts(source, sha),
  });
  writeChecksums(source);
  const archived = spawnSync("tar", ["-czf", staged, "-C", source, ...PAYLOAD, "SHA256SUMS"], {encoding: "utf8"});
  assert.equal(archived.status, 0, archived.stderr);
  return {staged, checksum: crypto.createHash("sha256").update(fs.readFileSync(staged)).digest("hex")};
}

test("remote installation rejects a transport checksum mismatch before extraction", () => {
  const appRoot = path.join(temporary, "checksum");
  const sha = "a".repeat(40);
  const staged = path.join(appRoot, "incoming", `${sha}.release.tar.gz.part`);
  fs.mkdirSync(path.dirname(staged), {recursive: true});
  fs.writeFileSync(staged, "not an archive");
  const result = run(appRoot, sha, "0".repeat(64), staged);
  assert.notEqual(result.status, 0);
  assert.match(result.stderr, /Checksum mismatch/);
  assert.equal(fs.existsSync(path.join(appRoot, "current")), false);
});

test("remote installation rejects an archive member outside the exact allowlist", () => {
  const appRoot = path.join(temporary, "member");
  const source = path.join(appRoot, "source");
  const sha = "b".repeat(40);
  const staged = path.join(appRoot, "incoming", `${sha}.release.tar.gz.part`);
  fs.mkdirSync(source, {recursive: true});
  fs.mkdirSync(path.dirname(staged), {recursive: true});
  fs.writeFileSync(path.join(source, "unexpected"), "payload");
  const archived = spawnSync("tar", ["-czf", staged, "-C", source, "unexpected"], {encoding: "utf8"});
  assert.equal(archived.status, 0, archived.stderr);
  const checksum = crypto.createHash("sha256").update(fs.readFileSync(staged)).digest("hex");
  const result = run(appRoot, sha, checksum, staged);
  assert.notEqual(result.status, 0);
  assert.match(result.stderr, /unexpected path/);
  assert.equal(fs.existsSync(path.join(appRoot, "current")), false);
});

test("remote installation switches current only after the complete envelope validates", {skip: process.platform !== "linux"}, () => {
  const appRoot = path.join(temporary, "valid");
  const sha = "c".repeat(40);
  const archive = validArchive(appRoot, sha);
  const result = run(appRoot, sha, archive.checksum, archive.staged);
  assert.equal(result.status, 0, `${result.stdout}\n${result.stderr}`);
  assert.equal(fs.readlinkSync(path.join(appRoot, "current")), path.join("releases", sha));
  assert.equal(fs.existsSync(path.join(appRoot, "releases", sha, "wiki-econ-rust-binary.cdx.json")), true);
  assert.equal(fs.existsSync(archive.staged), false);
});
