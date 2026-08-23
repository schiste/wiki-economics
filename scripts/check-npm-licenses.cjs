#!/usr/bin/env node
"use strict";

const fs = require("node:fs");
const path = require("node:path");

const root = path.resolve(__dirname, "..");

function readJson(file) {
  return JSON.parse(fs.readFileSync(file, "utf8"));
}

function packageName(lockPath, entry) {
  if (entry.name) return entry.name;
  const marker = "node_modules/";
  const start = lockPath.lastIndexOf(marker);
  if (start < 0) return null;
  const remainder = lockPath.slice(start + marker.length);
  const parts = remainder.split("/");
  return parts[0].startsWith("@") ? parts.slice(0, 2).join("/") : parts[0];
}

function lockInventory(lock) {
  if (lock?.lockfileVersion !== 3 || !lock.packages) throw new Error("npm license policy requires a lockfileVersion 3 package lock");
  const identities = new Map();
  for (const [lockPath, entry] of Object.entries(lock.packages)) {
    if (!lockPath || entry.link || !entry.version || !lockPath.includes("node_modules/")) continue;
    const name = packageName(lockPath, entry);
    if (!name) throw new Error(`cannot identify package at ${lockPath}`);
    const identity = `${name}@${entry.version}`;
    if (typeof entry.license !== "string" || entry.license.trim() === "") {
      throw new Error(`${identity} has no SPDX license expression in package-lock.json`);
    }
    const previous = identities.get(identity);
    if (previous && previous.license !== entry.license) {
      throw new Error(`${identity} has conflicting license expressions: ${previous.license} and ${entry.license}`);
    }
    identities.set(identity, {name, version: entry.version, license: entry.license});
  }
  return [...identities.values()].sort((left, right) => left.name.localeCompare(right.name) || left.version.localeCompare(right.version));
}

function loadPolicy(file = path.join(root, "config", "npm-license-policy.json")) {
  const policy = readJson(file);
  if (policy?.schema_version !== 1 || !Array.isArray(policy.approved_spdx_expressions)
      || policy.approved_spdx_expressions.length === 0
      || !policy.approved_spdx_expressions.every((value) => typeof value === "string" && value.trim() === value && value.length > 0)) {
    throw new Error(`${file}: invalid npm license policy`);
  }
  if (new Set(policy.approved_spdx_expressions).size !== policy.approved_spdx_expressions.length) {
    throw new Error(`${file}: duplicate approved SPDX expression`);
  }
  return new Set(policy.approved_spdx_expressions);
}

function verifyNpmLicenses({lock, closure, approved}) {
  const inventory = lockInventory(lock);
  const byIdentity = new Map(inventory.map((component) => [`${component.name}@${component.version}`, component]));
  const errors = inventory
    .filter((component) => !approved.has(component.license))
    .map((component) => `${component.name}@${component.version}: unapproved license ${component.license}`);

  if (closure?.schema_version !== 1 || !closure.generated_packages) {
    errors.push("generated browser dependency closure has an unsupported schema");
  } else {
    for (const [name, version] of Object.entries(closure.generated_packages)) {
      if (!byIdentity.has(`${name}@${version}`)) errors.push(`browser bundle dependency ${name}@${version} is absent from package-lock.json`);
    }
  }
  if (errors.length > 0) throw new Error(`npm license policy failed:\n- ${errors.join("\n- ")}`);

  const browser = Object.entries(closure.generated_packages).map(([name, version]) => byIdentity.get(`${name}@${version}`));
  return {inventory, browser};
}

function main() {
  const approved = loadPolicy();
  const result = verifyNpmLicenses({
    lock: readJson(path.join(root, "package-lock.json")),
    closure: readJson(path.join(root, "config", "site-dependency-closure.json")),
    approved,
  });
  process.stdout.write(`Approved npm licenses for ${result.inventory.length} locked packages and ${result.browser.length} browser packages.\n`);
}

if (require.main === module) {
  try {
    main();
  } catch (error) {
    console.error(error.stack || error.message);
    process.exitCode = 1;
  }
}

module.exports = {loadPolicy, lockInventory, packageName, verifyNpmLicenses};
