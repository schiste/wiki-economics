#!/usr/bin/env node
"use strict";

const fs = require("node:fs");
const crypto = require("node:crypto");
const os = require("node:os");
const path = require("node:path");
const {spawnSync} = require("node:child_process");
const {prepareSiteSource} = require("./prepare-site-source.cjs");
const {publishBrowserData} = require("./publish-browser-data.cjs");
const {verifySiteDependencies} = require("./verify-site-dependencies.cjs");

const REQUIRED_PAGES = [
  "admin.html",
  "business.html",
  "edit-variation.html",
  "gdp.html",
  "inequality.html",
  "labor.html",
  "legal.html",
  "patrol.html",
];
const REQUIRED_ATTACHMENTS = [
  "defaults_business.json",
  "defaults_edit_variation.json",
  "defaults_gdp.json",
  "defaults_inequality.json",
  "defaults_labor.json",
  "defaults_patrol.json",
  "manifest.json",
  "meta_business.json",
  "meta_gdp.json",
  "meta_inequality.json",
  "meta_labor.json",
  "meta_patrol.json",
];

function parseArguments(argv) {
  const options = {};
  for (let index = 0; index < argv.length; index += 1) {
    const name = argv[index];
    if (name !== "--data-dir" && name !== "--dist-dir") {
      throw new Error(`unknown argument: ${name}`);
    }
    const value = argv[index + 1];
    if (!value) throw new Error(`${name} requires a path`);
    options[name.slice(2)] = path.resolve(value);
    index += 1;
  }
  if (!options["data-dir"] || !options["dist-dir"]) {
    throw new Error("usage: build-site-fixture.cjs --data-dir PATH --dist-dir PATH");
  }
  return options;
}

function listFiles(directory, prefix = "") {
  const files = [];
  for (const entry of fs.readdirSync(directory, {withFileTypes: true})) {
    const relative = path.join(prefix, entry.name);
    if (entry.isDirectory()) files.push(...listFiles(path.join(directory, entry.name), relative));
    else if (entry.isFile()) files.push(relative);
  }
  return files.sort();
}

function verifyBuild(distDir) {
  for (const page of REQUIRED_PAGES) {
    const pagePath = path.join(distDir, page);
    if (!fs.statSync(pagePath, {throwIfNoEntry: false})?.isFile()) {
      throw new Error(`Observable build is missing page ${page}`);
    }
    const html = fs.readFileSync(pagePath, "utf8");
    if (!/href="(?:\.\/|\/)legal"/.test(html)) {
      throw new Error(`Observable page ${page} has no one-click legal link`);
    }
  }
  const files = listFiles(distDir);
  for (const attachment of REQUIRED_ATTACHMENTS) {
    const escaped = attachment.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    const hashedName = new RegExp(`(?:^|[/\\\\])${escaped.replace(/\\\.(json|parquet)$/, "\\.[a-f0-9]+\\.$1")}$`);
    if (!files.some((file) => hashedName.test(file))) {
      throw new Error(`Observable build is missing data attachment ${attachment}`);
    }
  }
  const browserIndexPath = path.join(distDir, "browser-data", "index.json");
  const browserIndex = JSON.parse(fs.readFileSync(browserIndexPath, "utf8"));
  if (browserIndex?.schema_version !== 1 || browserIndex?.cache_schema_version !== 2
      || !Array.isArray(browserIndex?.entries) || browserIndex.entries.length === 0) {
    throw new Error("Observable build has an invalid browser data index");
  }
  for (const entry of browserIndex.entries) {
    const artifact = path.join(distDir, ...entry.file.split("/"));
    const stat = fs.statSync(artifact, {throwIfNoEntry: false});
    const hash = stat?.isFile()
      ? crypto.createHash("sha256").update(fs.readFileSync(artifact)).digest("hex")
      : null;
    if (stat?.size !== entry.bytes || hash !== entry.sha256) {
      throw new Error(`Observable build has an invalid browser partition ${entry.file}`);
    }
  }
  if (files.some(file => /(?:^|[/\\])_file[/\\]data[/\\].*\.parquet$/.test(file))) {
    throw new Error("Observable build still embeds a combined Parquet attachment");
  }
  const manifestPath = files.find((file) => /(?:^|[/\\])manifest\.[a-f0-9]+\.json$/.test(file));
  const manifest = JSON.parse(fs.readFileSync(path.join(distDir, manifestPath), "utf8"));
  if (manifest?.schema_version !== 3
      || manifest?.license?.spdx_identifier !== "MIT"
      || !manifest?.provenance?.run_id
      || manifest?.provenance?.release_environment?.runtime?.node !== "24.15.0"
      || manifest?.provenance?.release_environment?.runtime?.npm !== "11.12.1"
      || manifest?.provenance?.release_environment?.runtime?.rust !== "1.98.0"
      || manifest?.provenance?.release_environment?.browser_packages?.direct?.["apache-arrow"] !== "21.2.0"
      || !Array.isArray(manifest?.source_datasets)
      || manifest.source_datasets.length === 0
      || manifest?.toolforge_open_licensing?.open_source_license_spdx !== "MIT"
      || manifest?.toolforge_open_licensing?.open_data_license_spdx !== "MIT"
      || typeof manifest?.trademark?.status !== "string") {
    throw new Error("Observable build has an incomplete licensing or provenance manifest");
  }
  const licensedDownloads = new Set((manifest.downloadable_artifacts || [])
    .filter((artifact) => artifact?.license_spdx === "MIT")
    .map((artifact) => artifact.name));
  const licensedNames = ["browser-data-index.json", ...browserIndex.entries.map(entry => entry.file)];
  if ([
    ...REQUIRED_ATTACHMENTS.filter((attachment) => attachment !== "manifest.json"),
    ...licensedNames,
  ].some((attachment) => !licensedDownloads.has(attachment))) {
    throw new Error("Observable build has a downloadable artifact without a discoverable MIT license");
  }
  return files;
}

function buildFixture({dataDir, distDir, root = path.resolve(__dirname, ".."), runner = spawnSync}) {
  if (!fs.statSync(dataDir, {throwIfNoEntry: false})?.isDirectory()) {
    throw new Error(`fixture data directory does not exist: ${dataDir}`);
  }
  const temporary = fs.mkdtempSync(path.join(os.tmpdir(), "wiki-econ-site-source-"));
  const sourceDir = path.join(temporary, "src");
  try {
    prepareSiteSource({
      sourceDir: path.join(root, "site", "src"),
      destinationDir: sourceDir,
      dataDir,
      vendorCacheDir: path.join(root, "site", "vendor", "observable-cache"),
    });
    fs.rmSync(distDir, {recursive: true, force: true});
    const denyNetwork = path.join(root, "scripts", "deny-network.cjs");
    const result = runner("npm", ["--workspace", "site", "run", "build"], {
      cwd: root,
      env: {
        ...process.env,
        NODE_OPTIONS: `--require=${denyNetwork}${process.env.NODE_OPTIONS ? ` ${process.env.NODE_OPTIONS}` : ""}`,
        OBSERVABLE_TELEMETRY_DISABLE: "true",
        WIKI_ECON_SITE_SOURCE_DIR: sourceDir,
        WIKI_ECON_SITE_DIST_DIR: distDir,
      },
      encoding: "utf8",
    });
    if (result.error) throw result.error;
    if (result.status !== 0) {
      throw new Error(`Observable build failed\n${result.stdout || ""}${result.stderr || ""}`);
    }
    publishBrowserData({dataDir, distDir});
    const files = verifyBuild(distDir);
    verifySiteDependencies(distDir);
    return files;
  } finally {
    fs.rmSync(temporary, {recursive: true, force: true});
  }
}

function main() {
  const options = parseArguments(process.argv.slice(2));
  const files = buildFixture({dataDir: options["data-dir"], distDir: options["dist-dir"]});
  process.stdout.write(`Verified Observable production build (${files.length} files).\n`);
}

if (require.main === module) {
  try {
    main();
  } catch (error) {
    console.error(error.stack || error.message);
    process.exitCode = 1;
  }
}

module.exports = {REQUIRED_ATTACHMENTS, REQUIRED_PAGES, buildFixture, listFiles, parseArguments, verifyBuild};
