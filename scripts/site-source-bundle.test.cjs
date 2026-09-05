"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const {afterEach, test} = require("node:test");
const {prepareBundle, verifyBundle} = require("./site-source-bundle.cjs");

const roots = [];
afterEach(() => {
  while (roots.length) fs.rmSync(roots.pop(), {recursive: true, force: true});
});

function fixture() {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "wiki-econ-site-source-"));
  roots.push(root);
  for (const directory of ["config/generated", "site/src/components", "site/data-build"]) {
    fs.mkdirSync(path.join(root, directory), {recursive: true});
  }
  const files = {
    "LICENSE": "MIT\n",
    "config/generated/metric-catalog.json": "{\"schema_version\":1,\"metrics\":[]}\n",
    "package.json": "{}\n",
    "package-lock.json": "{}\n",
    "site/package.json": "{}\n",
    "site/observablehq.config.js": "export default {};\n",
    "site/site-footer.js": "export const siteFooter = '';\n",
    "site/src/index.md": "# Home\n",
    "site/src/style.css": "body {}\n",
    "site/src/components/example.js": "export const value = 1;\n",
    "site/data-build/manifest.json.sh": "#!/bin/sh\nexit 0\n",
  };
  for (const [relative, content] of Object.entries(files)) {
    const file = path.join(root, relative);
    fs.mkdirSync(path.dirname(file), {recursive: true});
    fs.writeFileSync(file, content, {mode: relative.endsWith(".sh") ? 0o755 : 0o644});
  }
  return root;
}

test("site-source bundle is deterministic and verifies exact files, modes, and commit", () => {
  const root = fixture();
  const first = path.join(root, "first");
  const second = path.join(root, "second");
  const commit = "a".repeat(40);
  const left = prepareBundle(root, first, commit, "1788100000");
  const right = prepareBundle(root, second, commit, "1788100000");
  assert.deepEqual(left, right);
  assert.equal(verifyBundle(first, commit).content_sha256, left.content_sha256);
  assert.equal(left.files.find((entry) => entry.path.endsWith(".sh")).mode, "0755");
});

test("site-source verification fails closed for changed, missing, extra, and linked files", () => {
  const commit = "b".repeat(40);

  let root = fixture();
  let destination = path.join(root, "changed");
  prepareBundle(root, destination, commit, "1788100000");
  fs.appendFileSync(path.join(destination, "site/src/style.css"), "changed\n");
  assert.throws(() => verifyBundle(destination, commit), /identity validation/);

  root = fixture();
  destination = path.join(root, "missing");
  prepareBundle(root, destination, commit, "1788100000");
  fs.rmSync(path.join(destination, "site/src/index.md"));
  assert.throws(() => verifyBundle(destination, commit), /path inventory/);

  root = fixture();
  destination = path.join(root, "extra");
  prepareBundle(root, destination, commit, "1788100000");
  fs.writeFileSync(path.join(destination, "unexpected"), "no\n");
  assert.throws(() => verifyBundle(destination, commit), /path inventory/);

  root = fixture();
  destination = path.join(root, "linked");
  prepareBundle(root, destination, commit, "1788100000");
  fs.rmSync(path.join(destination, "site/src/index.md"));
  fs.symlinkSync("style.css", path.join(destination, "site/src/index.md"));
  assert.throws(() => verifyBundle(destination, commit), /symbolic link/);
});

test("site-source preparation excludes build debris and refuses source symlinks", () => {
  const root = fixture();
  fs.mkdirSync(path.join(root, "site/src/.observablehq"));
  fs.writeFileSync(path.join(root, "site/src/.observablehq/cache"), "ignored");
  fs.symlinkSync("index.md", path.join(root, "site/src/linked.md"));
  assert.throws(
    () => prepareBundle(root, path.join(root, "bundle"), "c".repeat(40), "1788100000"),
    /symbolic link/,
  );
});
