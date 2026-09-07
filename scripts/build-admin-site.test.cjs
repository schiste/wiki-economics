#!/usr/bin/env node
"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const test = require("node:test");
const {
  acquireDirectoryLock,
  buildAdminSite,
  parseArguments,
  prepareAdminSource,
  publishAdminBuild,
  verifyAdminBuild,
} = require("./build-admin-site.cjs");

test("standalone admin source contains only its declared operational inputs", (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "wiki-econ-admin-source-"));
  t.after(() => fs.rmSync(root, {recursive: true, force: true}));
  const site = path.join(root, "site");
  const destination = path.join(root, "source");
  fs.mkdirSync(path.join(site, "src", "components"), {recursive: true});
  fs.writeFileSync(path.join(site, "src", "admin.md"), "# Admin\n");
  fs.writeFileSync(path.join(site, "src", "style.css"), "body{}\n");
  fs.writeFileSync(path.join(site, "src", "components", "admin-console.js"), "export {};\n");
  fs.writeFileSync(path.join(root, "manifest.json"), "{}\n");

  prepareAdminSource({siteDir: site, manifestPath: path.join(root, "manifest.json"), destinationDir: destination});
  assert.deepEqual(fs.readdirSync(destination).sort(), ["admin.md", "components", "data", "style.css"]);
  assert.equal(fs.readFileSync(path.join(destination, "data", "manifest.json"), "utf8"), "{}\n");
});

test("standalone admin verification rejects public pages and requires an isolated base", (t) => {
  const dist = fs.mkdtempSync(path.join(os.tmpdir(), "wiki-econ-admin-dist-"));
  t.after(() => fs.rmSync(dist, {recursive: true, force: true}));
  fs.mkdirSync(path.join(dist, "_file", "data"), {recursive: true});
  fs.writeFileSync(path.join(dist, "admin.html"), '<base href="/admin-assets/">');
  fs.writeFileSync(path.join(dist, "style.css"), "body{}\n");
  fs.writeFileSync(path.join(dist, "_file", "data", "manifest.0123abcd.json"), "{}\n");
  assert.ok(verifyAdminBuild(dist).includes("admin.html"));
  fs.writeFileSync(path.join(dist, "gdp.html"), "unexpected");
  assert.throws(() => verifyAdminBuild(dist), /public pages/);
});

test("standalone admin publication switches one symlink and retains one rollback", (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "wiki-econ-admin-publish-"));
  t.after(() => fs.rmSync(root, {recursive: true, force: true}));
  const dist = path.join(root, "admin-dist");
  for (const id of ["one", "two", "three"]) {
    const build = path.join(root, `.admin-dist.build.${id}`);
    fs.mkdirSync(build);
    fs.writeFileSync(path.join(build, "admin.html"), id);
    publishAdminBuild({buildDir: build, distDir: dist, retention: 2});
  }
  assert.equal(fs.readFileSync(path.join(dist, "admin.html"), "utf8"), "three");
  assert.equal(fs.readdirSync(root).filter((name) => name.startsWith(".admin-dist.build.")).length, 2);
});

test("standalone admin CLI arguments are strict", () => {
  assert.equal(parseArguments([
    "--root", "/root", "--site-dir", "/site", "--manifest", "/manifest",
    "--dist-dir", "/dist", "--output-dir", "/output", "--run-id", "run",
  ])["run-id"], "run");
  assert.throws(() => parseArguments(["--root", "/root"]), /required/);
  assert.throws(() => parseArguments(["--wat", "value"]), /usage/);
});

test("standalone admin locking rejects overlap and recovers a demonstrably stale owner", (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "wiki-econ-admin-lock-"));
  t.after(() => fs.rmSync(root, {recursive: true, force: true}));
  const lockPath = path.join(root, ".admin-dist.lock");
  const first = acquireDirectoryLock(lockPath, "first", 1_000, 10_000);
  assert.throws(() => acquireDirectoryLock(lockPath, "overlap", 2_000, 10_000), /already active/);
  first.release();

  fs.mkdirSync(lockPath);
  fs.writeFileSync(path.join(lockPath, "owner.json"), "truncated");
  fs.utimesSync(lockPath, new Date(0), new Date(0));
  const recovered = acquireDirectoryLock(lockPath, "recovered", 20_000, 10_000);
  assert.equal(JSON.parse(fs.readFileSync(path.join(lockPath, "owner.json"), "utf8")).run_id, "recovered");
  recovered.release();
  assert.equal(fs.existsSync(lockPath), false);
});

test("standalone admin production builds are offline and byte deterministic", (t) => {
  const root = path.resolve(__dirname, "..");
  const temporary = fs.mkdtempSync(path.join(os.tmpdir(), "wiki-econ-admin-real-build-"));
  t.after(() => fs.rmSync(temporary, {recursive: true, force: true}));
  const manifest = path.join(temporary, "manifest.json");
  const output = path.join(temporary, "output");
  const dist = path.join(temporary, "admin-dist");
  fs.mkdirSync(output);
  fs.writeFileSync(manifest, '{"schema_version":3}\n');

  const first = buildAdminSite({
    root, siteDir: path.join(root, "site"), manifestPath: manifest,
    distDir: dist, outputDir: output, runId: "deterministic-a",
  });
  const second = buildAdminSite({
    root, siteDir: path.join(root, "site"), manifestPath: manifest,
    distDir: dist, outputDir: output, runId: "deterministic-b",
  });
  assert.deepEqual(
    second.files.map(({path: file, sha256}) => [file, sha256]),
    first.files.map(({path: file, sha256}) => [file, sha256]),
  );
  assert.ok(fs.statSync(path.join(dist, "admin-release.json")).isFile());
  assert.equal(fs.existsSync(path.join(dist, "inequality.html")), false);
});
