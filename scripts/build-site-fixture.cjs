#!/usr/bin/env node
"use strict";

const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const {spawnSync} = require("node:child_process");

const REQUIRED_PAGES = [
  "index.html",
  "admin.html",
  "business.html",
  "edit-variation.html",
  "gdp.html",
  "inequality.html",
  "labor.html",
  "patrol.html",
];
const REQUIRED_ATTACHMENTS = [
  "business_funnel.parquet",
  "defaults_business.json",
  "defaults_edit_variation.json",
  "defaults_gdp.json",
  "defaults_inequality.json",
  "defaults_labor.json",
  "defaults_patrol.json",
  "gdp.parquet",
  "gdp_activity_tiers.parquet",
  "gdp_user_type_share.parquet",
  "inequality.parquet",
  "labor_churn.parquet",
  "labor_cohorts.parquet",
  "labor_monthly.parquet",
  "manifest.json",
  "meta_business.json",
  "meta_gdp.json",
  "meta_inequality.json",
  "meta_labor.json",
  "meta_patrol.json",
  "patrol.parquet",
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
    if (!fs.statSync(path.join(distDir, page), {throwIfNoEntry: false})?.isFile()) {
      throw new Error(`Observable build is missing page ${page}`);
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
  return files;
}

function buildFixture({dataDir, distDir, root = path.resolve(__dirname, ".."), runner = spawnSync}) {
  if (!fs.statSync(dataDir, {throwIfNoEntry: false})?.isDirectory()) {
    throw new Error(`fixture data directory does not exist: ${dataDir}`);
  }
  const temporary = fs.mkdtempSync(path.join(os.tmpdir(), "wiki-econ-site-source-"));
  const sourceDir = path.join(temporary, "src");
  try {
    fs.cpSync(path.join(root, "site", "src"), sourceDir, {
      recursive: true,
      dereference: false,
      verbatimSymlinks: true,
    });
    fs.rmSync(path.join(sourceDir, "data"), {recursive: true, force: true});
    fs.symlinkSync(dataDir, path.join(sourceDir, "data"), "dir");
    fs.rmSync(distDir, {recursive: true, force: true});
    const result = runner("npm", ["--workspace", "site", "run", "build"], {
      cwd: root,
      env: {
        ...process.env,
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
    return verifyBuild(distDir);
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
