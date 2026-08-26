#!/usr/bin/env node
"use strict";

const fs = require("node:fs");
const path = require("node:path");
const {execFileSync} = require("node:child_process");

const ROOT = path.resolve(__dirname, "..");
const DECLARATIONS = path.join(ROOT, "config", "compute-no-semantic-change.json");
const FAMILIES = {
  monthly: {
    versionFile: "src/compute/monthly/mod.rs",
    paths: [
      "src/compute/monthly/",
      "src/compute/inequality.rs",
      "src/compute/gdp.rs",
      "src/compute/labor.rs",
      "src/compute/mod.rs",
    ],
  },
  activity_tiers: {
    versionFile: "src/compute/activity/mod.rs",
    paths: ["src/compute/activity/", "src/compute/gdp.rs", "src/compute/mod.rs"],
  },
  lifecycle: {
    versionFile: "src/compute/lifecycle/mod.rs",
    paths: [
      "src/compute/lifecycle/",
      "src/compute/gdp.rs",
      "src/compute/labor.rs",
      "src/compute/mod.rs",
    ],
  },
  page_week: {
    versionFile: "src/compute/weekly/mod.rs",
    paths: ["src/compute/weekly/", "src/compute/mod.rs"],
  },
};

function versionFromSource(source, file) {
  if (source == null) return null;
  const match = source.match(/ALGORITHM_VERSION:\s*&str\s*=\s*"([^"]+)"/);
  if (!match) throw new Error(`${file} does not declare ALGORITHM_VERSION`);
  return match[1];
}

function familiesForPath(file) {
  return Object.entries(FAMILIES)
    .filter(([, policy]) => policy.paths.some(prefix =>
      prefix.endsWith("/") ? file.startsWith(prefix) : file === prefix,
    ))
    .map(([family]) => family);
}

function validateDeclarations(document) {
  if (document.schema_version !== 1 || !Array.isArray(document.declarations)) {
    throw new Error("compute no-semantic-change declarations have an unsupported schema");
  }
  for (const declaration of document.declarations) {
    if (!FAMILIES[declaration.family]) throw new Error(`unknown declaration family ${declaration.family}`);
    if (!Array.isArray(declaration.paths) || declaration.paths.length === 0) {
      throw new Error(`declaration for ${declaration.family} has no paths`);
    }
    if (typeof declaration.reason !== "string" || declaration.reason.trim().length < 12) {
      throw new Error(`declaration for ${declaration.family} needs a specific reason`);
    }
    if (typeof declaration.base_commit !== "string" || !/^[0-9a-f]{40}$/.test(declaration.base_commit)) {
      throw new Error(`declaration for ${declaration.family} needs an exact base_commit`);
    }
  }
  return document.declarations;
}

function enforce({base, changedFiles, beforeVersions, currentVersions, declarations}) {
  const errors = [];
  for (const family of Object.keys(FAMILIES)) {
    const relevant = changedFiles.filter(file => familiesForPath(file).includes(family));
    if (relevant.length === 0) continue;
    if (beforeVersions[family] !== currentVersions[family]) continue;
    const declared = new Set(
      declarations
        .filter(declaration => declaration.family === family && declaration.base_commit === base)
        .flatMap(declaration => declaration.paths),
    );
    const undeclared = relevant.filter(file => !declared.has(file));
    if (undeclared.length > 0) {
      errors.push(
        `${family} semantic sources changed without an algorithm-version bump or declaration: ${undeclared.join(", ")}`,
      );
    }
  }
  if (errors.length > 0) throw new Error(errors.join("\n"));
}

function git(args) {
  return execFileSync("git", args, {
    cwd: ROOT,
    encoding: "utf8",
    stdio: ["ignore", "pipe", "pipe"],
  }).trim();
}

function gitShow(base, file) {
  try {
    return git(["show", `${base}:${file}`]);
  } catch {
    return null;
  }
}

function main() {
  const baseFlag = process.argv.indexOf("--base");
  const explicitBase = baseFlag >= 0 ? process.argv[baseFlag + 1] : null;
  const baseReference = explicitBase || process.env.COMPUTE_VERSION_BASE || "HEAD";
  if (!baseReference) throw new Error("--base requires a Git revision");
  const base = git(["rev-parse", "--verify", `${baseReference}^{commit}`]);
  const tracked = git(["diff", "--name-only", "--diff-filter=ACMR", base, "--"])
    .split("\n")
    .filter(Boolean);
  const untracked = git(["ls-files", "--others", "--exclude-standard"])
    .split("\n")
    .filter(Boolean);
  const changedFiles = [...new Set([...tracked, ...untracked])].sort();
  const declarationDocument = JSON.parse(fs.readFileSync(DECLARATIONS, "utf8"));
  const declarations = validateDeclarations(declarationDocument);
  const beforeVersions = {};
  const currentVersions = {};
  for (const [family, policy] of Object.entries(FAMILIES)) {
    beforeVersions[family] = versionFromSource(gitShow(base, policy.versionFile), policy.versionFile);
    currentVersions[family] = versionFromSource(
      fs.readFileSync(path.join(ROOT, policy.versionFile), "utf8"),
      policy.versionFile,
    );
  }
  enforce({base, changedFiles, beforeVersions, currentVersions, declarations});
  console.log(`Verified compute-family versions for ${changedFiles.length} changed paths.`);
}

if (require.main === module) {
  try {
    main();
  } catch (error) {
    console.error(error.message);
    process.exitCode = 1;
  }
}

module.exports = {FAMILIES, enforce, familiesForPath, validateDeclarations, versionFromSource};
