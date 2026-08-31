"use strict";

const assert = require("node:assert/strict");
const crypto = require("node:crypto");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const {spawnSync} = require("node:child_process");
const {test} = require("node:test");
const {prepareBundle} = require("../../scripts/site-source-bundle.cjs");

const script = path.join(__dirname, "install-site-source.sh");
const commit = "d".repeat(40);

function sourceFixture(root) {
  for (const directory of ["site/src", "site/data-build"]) fs.mkdirSync(path.join(root, directory), {recursive: true});
  const files = {
    "LICENSE": "MIT\n",
    "package.json": "{}\n", "package-lock.json": "{}\n", "site/package.json": "{}\n",
    "site/observablehq.config.js": "export default {};\n", "site/site-footer.js": "export const siteFooter = '';\n",
    "site/src/index.md": "# Home\n", "site/data-build/manifest.json.sh": "#!/bin/sh\n",
  };
  for (const [relative, content] of Object.entries(files)) fs.writeFileSync(path.join(root, relative), content);
}

function fixture() {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "wiki-econ-site-source-install-"));
  const source = path.join(root, "source");
  const bundle = path.join(root, "bundle");
  const sourceRoot = path.join(root, "site-sources");
  const lock = path.join(root, "output", ".publication.lock");
  fs.mkdirSync(source);
  sourceFixture(source);
  prepareBundle(source, bundle, commit, "1788100000");
  fs.mkdirSync(path.join(sourceRoot, "incoming"), {recursive: true});
  const archive = path.join(sourceRoot, "incoming", `${commit}.site-source.tar.gz.part`);
  const tar = spawnSync("tar", ["-czf", archive, "-C", bundle, "."], {encoding: "utf8"});
  assert.equal(tar.status, 0, tar.stderr);
  const checksum = crypto.createHash("sha256").update(fs.readFileSync(archive)).digest("hex");
  return {root, sourceRoot, lock, archive, checksum};
}

test("installer validates and atomically selects an immutable site-source release", () => {
  const value = fixture();
  try {
    const result = spawnSync("bash", [script, commit, value.checksum, value.archive], {
      encoding: "utf8",
      env: {...process.env, WIKI_ECON_TOOLFORGE_SITE_SOURCE_ROOT: value.sourceRoot, WIKI_ECON_PUBLICATION_LOCK_DIR: value.lock},
    });
    assert.equal(result.status, 0, result.stderr || result.stdout);
    assert.equal(fs.readlinkSync(path.join(value.sourceRoot, "current")), `releases/${commit}`);
    assert.equal(fs.existsSync(value.archive), false);
  } finally {
    fs.rmSync(value.root, {recursive: true, force: true});
  }
});

test("installer fails closed before switching current when the archive changes", () => {
  const value = fixture();
  try {
    fs.appendFileSync(value.archive, "tampered");
    const result = spawnSync("bash", [script, commit, value.checksum, value.archive], {
      encoding: "utf8",
      env: {...process.env, WIKI_ECON_TOOLFORGE_SITE_SOURCE_ROOT: value.sourceRoot, WIKI_ECON_PUBLICATION_LOCK_DIR: value.lock},
    });
    assert.notEqual(result.status, 0);
    assert.equal(fs.existsSync(path.join(value.sourceRoot, "current")), false);
  } finally {
    fs.rmSync(value.root, {recursive: true, force: true});
  }
});
