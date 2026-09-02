"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const {after, test} = require("node:test");
const {publishStaticRoot} = require("./publish-static-root.cjs");

const root = fs.mkdtempSync(path.join(os.tmpdir(), "publish-static-root-"));
after(() => fs.rmSync(root, {recursive: true, force: true}));

test("only allowlisted root policy files are copied byte for byte", () => {
  const sourceDir = path.join(root, "source");
  const distDir = path.join(root, "dist");
  fs.mkdirSync(sourceDir);
  fs.mkdirSync(distDir);
  fs.writeFileSync(path.join(sourceDir, "robots.txt"), "User-agent: *\nCrawl-delay: 10\n");
  fs.writeFileSync(path.join(sourceDir, "unreviewed.txt"), "must not publish\n");

  assert.deepEqual(publishStaticRoot({sourceDir, distDir}), ["robots.txt"]);
  assert.equal(fs.readFileSync(path.join(distDir, "robots.txt"), "utf8"), "User-agent: *\nCrawl-delay: 10\n");
  assert.equal(fs.existsSync(path.join(distDir, "unreviewed.txt")), false);
});

test("a missing or symlinked required policy file fails closed", () => {
  const sourceDir = path.join(root, "unsafe-source");
  const distDir = path.join(root, "unsafe-dist");
  fs.mkdirSync(sourceDir);
  fs.mkdirSync(distDir);
  assert.throws(() => publishStaticRoot({sourceDir, distDir}), /missing or unsafe/);

  fs.symlinkSync("elsewhere", path.join(sourceDir, "robots.txt"));
  assert.throws(() => publishStaticRoot({sourceDir, distDir}), /missing or unsafe/);
});
