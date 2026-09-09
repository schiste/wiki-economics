#!/usr/bin/env node
"use strict";

const crypto = require("node:crypto");
const fs = require("node:fs");
const path = require("node:path");
const {spawnSync} = require("node:child_process");

const ADMIN_SOURCE_FILES = [
  "admin.md",
  "style.css",
  "components/admin-console.js",
];
const ADMIN_ASSET_ROOTS = ["_file", "_import", "_npm", "_observablehq"];

function sha256File(file) {
  return crypto.createHash("sha256").update(fs.readFileSync(file)).digest("hex");
}

function listFiles(directory, prefix = "") {
  const files = [];
  for (const entry of fs.readdirSync(directory, {withFileTypes: true})
    .sort((left, right) => left.name.localeCompare(right.name))) {
    const relative = prefix ? path.posix.join(prefix, entry.name) : entry.name;
    const absolute = path.join(directory, entry.name);
    if (entry.isSymbolicLink()) throw new Error(`admin build contains a symlink: ${relative}`);
    if (entry.isDirectory()) files.push(...listFiles(absolute, relative));
    else if (entry.isFile()) files.push(relative);
    else throw new Error(`admin build contains an unsupported entry: ${relative}`);
  }
  return files;
}

function copyRegularFile(source, destination) {
  const metadata = fs.lstatSync(source, {throwIfNoEntry: false});
  if (!metadata?.isFile() || metadata.isSymbolicLink()) {
    throw new Error(`required admin source is missing or unsafe: ${source}`);
  }
  fs.mkdirSync(path.dirname(destination), {recursive: true});
  fs.copyFileSync(source, destination, fs.constants.COPYFILE_EXCL);
  fs.utimesSync(destination, metadata.atime, metadata.mtime);
}

function prepareAdminSource({siteDir, manifestPath, destinationDir, vendorCacheDir = null}) {
  if (fs.existsSync(destinationDir)) throw new Error(`admin source destination already exists: ${destinationDir}`);
  fs.mkdirSync(destinationDir, {recursive: true});
  for (const relative of ADMIN_SOURCE_FILES) {
    copyRegularFile(path.join(siteDir, "src", relative), path.join(destinationDir, relative));
  }
  copyRegularFile(manifestPath, path.join(destinationDir, "data", "manifest.json"));
  if (vendorCacheDir) {
    const metadata = fs.lstatSync(vendorCacheDir, {throwIfNoEntry: false});
    if (!metadata?.isDirectory() || metadata.isSymbolicLink()) {
      throw new Error(`Observable vendor cache is missing or unsafe: ${vendorCacheDir}`);
    }
    fs.cpSync(vendorCacheDir, path.join(destinationDir, ".observablehq", "cache"), {
      recursive: true,
      dereference: false,
      errorOnExist: true,
      force: false,
    });
  }
  return ADMIN_SOURCE_FILES;
}

function rebaseAdminAssetUrls(distDir) {
  const absolute = path.join(distDir, "admin.html");
  const original = fs.readFileSync(absolute, "utf8");
  let rebased = original;
  for (const root of ADMIN_ASSET_ROOTS) {
    rebased = rebased.replaceAll(`/${root}/`, `/admin-assets/${root}/`);
  }
  if (rebased !== original) fs.writeFileSync(absolute, rebased);
}

function verifyAdminBuild(distDir) {
  const files = listFiles(distDir);
  if (!files.includes("admin.html")) throw new Error("standalone admin build is missing admin.html");
  const unexpectedPages = files.filter((file) => file.endsWith(".html") && file !== "admin.html");
  if (unexpectedPages.length) throw new Error(`standalone admin build contains public pages: ${unexpectedPages.join(", ")}`);
  const html = fs.readFileSync(path.join(distDir, "admin.html"), "utf8");
  if (!html.includes('<base href="/admin-assets/">')) {
    throw new Error("standalone admin build has no isolated asset base");
  }
  if (!files.some((file) => /^_file\/data\/manifest\.[a-f0-9]+\.json$/.test(file))) {
    throw new Error("standalone admin build has no immutable manifest attachment");
  }
  if (!files.includes("style.css")) throw new Error("standalone admin build is missing its isolated stylesheet");
  const unrebasedPattern = new RegExp(`(^|[^A-Za-z0-9_-])/_(?:${ADMIN_ASSET_ROOTS.map((root) => root.slice(1)).join("|")})/`, "m");
  const assetPattern = new RegExp(`/admin-assets/(_(?:${ADMIN_ASSET_ROOTS.map((root) => root.slice(1)).join("|")})/[^\\s\"'\\x60)<]+)`, "g");
  if (unrebasedPattern.test(html)) {
    throw new Error("standalone admin build contains a public-root asset URL: admin.html");
  }
  for (const match of html.matchAll(assetPattern)) {
    const relative = match[1].split(/[?#]/, 1)[0];
    const target = path.resolve(distDir, relative);
    if (!target.startsWith(`${path.resolve(distDir)}${path.sep}`)
        || !fs.statSync(target, {throwIfNoEntry: false})?.isFile()) {
      throw new Error(`standalone admin build references a missing isolated asset from admin.html: ${relative}`);
    }
  }
  return files;
}

function atomicWriteJson(file, value) {
  fs.mkdirSync(path.dirname(file), {recursive: true});
  const temporary = `${file}.tmp.${process.pid}`;
  try {
    fs.writeFileSync(temporary, `${JSON.stringify(value, null, 2)}\n`, {flag: "wx"});
    fs.renameSync(temporary, file);
  } catch (error) {
    try { fs.unlinkSync(temporary); } catch {}
    throw error;
  }
}

function acquireDirectoryLock(lockDir, runId, now = Date.now(), staleAfterMs = 30 * 60 * 1000) {
  const token = crypto.randomBytes(16).toString("hex");
  const create = () => {
    fs.mkdirSync(lockDir);
    try {
      atomicWriteJson(path.join(lockDir, "owner.json"), {
        schema_version: 1,
        run_id: runId,
        token,
        started_at: new Date(now).toISOString(),
        started_at_unix_ms: now,
      });
    } catch (error) {
      fs.rmSync(lockDir, {recursive: true, force: true});
      throw error;
    }
  };
  try {
    create();
  } catch (error) {
    if (error.code !== "EEXIST") throw error;
    const metadata = fs.statSync(lockDir, {throwIfNoEntry: false});
    if (!metadata?.isDirectory() || now - metadata.mtimeMs <= staleAfterMs) {
      throw new Error(`standalone admin build is already active: ${lockDir}`);
    }
    const stale = `${lockDir}.stale.${Math.floor(now)}.${crypto.randomBytes(4).toString("hex")}`;
    try {
      fs.renameSync(lockDir, stale);
    } catch (renameError) {
      throw new Error(`standalone admin build lock changed while recovering it: ${renameError.message}`);
    }
    fs.rmSync(stale, {recursive: true, force: true});
    create();
  }
  return {
    token,
    release() {
      let owner = null;
      try { owner = JSON.parse(fs.readFileSync(path.join(lockDir, "owner.json"), "utf8")); } catch {}
      if (owner?.token === token) fs.rmSync(lockDir, {recursive: true, force: true});
    },
  };
}

function publishAdminBuild({buildDir, distDir, retention = 2}) {
  const parent = path.dirname(distDir);
  const name = path.basename(distDir);
  if (!name || [".", "..", "/"].includes(name) || !path.basename(buildDir).startsWith(`.${name}.build.`)) {
    throw new Error("unsafe standalone admin publication paths");
  }
  fs.mkdirSync(parent, {recursive: true});
  const temporaryLink = path.join(parent, `.${name}.next.${process.pid}.${crypto.randomBytes(4).toString("hex")}`);
  try {
    if (fs.existsSync(distDir) && !fs.lstatSync(distDir).isSymbolicLink()) {
      throw new Error(`standalone admin destination must be absent or a symlink: ${distDir}`);
    }
    fs.symlinkSync(path.basename(buildDir), temporaryLink);
    fs.renameSync(temporaryLink, distDir);
  } finally {
    try { fs.unlinkSync(temporaryLink); } catch {}
  }
  const current = fs.realpathSync(distDir);
  const releases = fs.readdirSync(parent, {withFileTypes: true})
    .filter((entry) => entry.isDirectory() && entry.name.startsWith(`.${name}.build.`))
    .map((entry) => path.join(parent, entry.name))
    .sort((left, right) => fs.statSync(right).mtimeMs - fs.statSync(left).mtimeMs);
  for (const release of releases.slice(Math.max(1, retention))) {
    if (fs.realpathSync(release) !== current) fs.rmSync(release, {recursive: true, force: true});
  }
  return current;
}

function buildAdminSite({root, siteDir, manifestPath, distDir, outputDir, runId, runner = spawnSync}) {
  if (!/^[A-Za-z0-9._-]+$/.test(runId || "")) throw new Error("admin build requires a safe run ID");
  const parent = path.dirname(distDir);
  const name = path.basename(distDir);
  fs.mkdirSync(parent, {recursive: true});
  const lockDir = path.join(parent, `.${name}.lock`);
  const lock = acquireDirectoryLock(lockDir, runId);
  let sourceDir = null;
  let buildDir = null;
  let published = false;
  try {
    sourceDir = fs.mkdtempSync(path.join(parent, `.${name}.source.${runId}.`));
    buildDir = fs.mkdtempSync(path.join(parent, `.${name}.build.${runId}.`));
    prepareAdminSource({
      siteDir,
      manifestPath,
      destinationDir: path.join(sourceDir, "src"),
      vendorCacheDir: path.join(root, "site", "vendor", "observable-cache"),
    });
    const denyNetwork = path.join(root, "scripts", "deny-network.cjs");
    const result = runner(path.join(root, "node_modules", ".bin", "observable"), [
      "build", "--config", path.join(siteDir, "observablehq.config.js"),
    ], {
      cwd: root,
      env: {
        ...process.env,
        NODE_OPTIONS: `--require=${denyNetwork}${process.env.NODE_OPTIONS ? ` ${process.env.NODE_OPTIONS}` : ""}`,
        OBSERVABLE_TELEMETRY_DISABLE: "true",
        WIKI_ECON_ADMIN_STANDALONE: "1",
        WIKI_ECON_SITE_SOURCE_DIR: path.join(sourceDir, "src"),
        WIKI_ECON_SITE_DIST_DIR: buildDir,
      },
      encoding: "utf8",
    });
    if (result.error) throw result.error;
    if (result.status !== 0) throw new Error(`standalone admin build failed\n${result.stdout || ""}${result.stderr || ""}`);
    copyRegularFile(path.join(siteDir, "src", "style.css"), path.join(buildDir, "style.css"));
    rebaseAdminAssetUrls(buildDir);
    const files = verifyAdminBuild(buildDir);
    const receipt = {
      schema_version: 1,
      artifact: "wiki-econ-admin-site",
      run_id: runId,
      generated_at: new Date().toISOString(),
      source_commit: process.env.WIKI_ECON_SITE_SOURCE_COMMIT || null,
      source_sha256: process.env.WIKI_ECON_SITE_SOURCE_SHA256 || null,
      image_commit: process.env.WIKI_ECON_IMAGE_SOURCE_COMMIT || null,
      manifest_sha256: sha256File(manifestPath),
      files: files.map((file) => ({
        path: file,
        bytes: fs.statSync(path.join(buildDir, file)).size,
        sha256: sha256File(path.join(buildDir, file)),
      })),
    };
    atomicWriteJson(path.join(buildDir, "admin-release.json"), receipt);
    const current = publishAdminBuild({buildDir, distDir});
    published = true;
    atomicWriteJson(path.join(outputDir, "_stages", "admin-site.json"), receipt);
    return {...receipt, current};
  } finally {
    if (sourceDir) fs.rmSync(sourceDir, {recursive: true, force: true});
    if (buildDir && !published) fs.rmSync(buildDir, {recursive: true, force: true});
    lock.release();
  }
}

function parseArguments(argv) {
  const options = {};
  for (let index = 0; index < argv.length; index += 2) {
    const name = argv[index];
    const value = argv[index + 1];
    if (!["--root", "--site-dir", "--manifest", "--dist-dir", "--output-dir", "--run-id"].includes(name) || !value) {
      throw new Error("usage: build-admin-site.cjs --root PATH --site-dir PATH --manifest PATH --dist-dir PATH --output-dir PATH --run-id ID");
    }
    options[name.slice(2)] = value;
  }
  for (const name of ["root", "site-dir", "manifest", "dist-dir", "output-dir", "run-id"]) {
    if (!options[name]) throw new Error(`--${name} is required`);
  }
  return options;
}

function main() {
  const options = parseArguments(process.argv.slice(2));
  const receipt = buildAdminSite({
    root: path.resolve(options.root),
    siteDir: path.resolve(options["site-dir"]),
    manifestPath: path.resolve(options.manifest),
    distDir: path.resolve(options["dist-dir"]),
    outputDir: path.resolve(options["output-dir"]),
    runId: options["run-id"],
  });
  process.stdout.write(`${JSON.stringify(receipt)}\n`);
}

if (require.main === module) {
  try { main(); } catch (error) {
    console.error(error.stack || error.message);
    process.exitCode = 1;
  }
}

module.exports = {
  ADMIN_SOURCE_FILES,
  acquireDirectoryLock,
  buildAdminSite,
  listFiles,
  parseArguments,
  prepareAdminSource,
  publishAdminBuild,
  rebaseAdminAssetUrls,
  verifyAdminBuild,
};
