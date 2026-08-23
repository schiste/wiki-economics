#!/usr/bin/env node
"use strict";

const fs = require("node:fs");
const path = require("node:path");
const {spawnSync} = require("node:child_process");

const root = path.resolve(__dirname, "..");

function readJson(file) {
  return JSON.parse(fs.readFileSync(file, "utf8"));
}

function cargoPatches(file) {
  const content = fs.readFileSync(file, "utf8");
  const section = content.match(/\[patch\.crates-io\]([\s\S]*?)(?=\n\[|$)/)?.[1];
  if (!section) return new Map();
  const patches = new Map();
  for (const match of section.matchAll(/^([A-Za-z0-9_-]+)\s*=\s*\{[^}]*\bpath\s*=\s*"([^"]+)"[^}]*\}/gm)) {
    patches.set(match[1], match[2]);
  }
  return patches;
}

function manifestIdentity(file) {
  const content = fs.readFileSync(file, "utf8");
  const packageSection = content.match(/\[package\]([\s\S]*?)(?=\n\[|$)/)?.[1] || "";
  return {
    name: packageSection.match(/^name\s*=\s*"([^"]+)"/m)?.[1],
    version: packageSection.match(/^version\s*=\s*"([^"]+)"/m)?.[1],
  };
}

function validateRegistry({registry, cargo, metadata, repositoryRoot = root}) {
  if (registry?.schema_version !== 1 || registry.registry !== "https://crates.io" || !Array.isArray(registry.patches)) {
    throw new Error("invalid vendored patch registry");
  }
  const registered = new Map();
  for (const entry of registry.patches) {
    if (!/^[A-Za-z0-9_-]+$/.test(entry?.crate || "") || !/^\d+\.\d+\.\d+(?:[-+][0-9A-Za-z.-]+)?$/.test(entry?.upstream_version || "")
        || !/^[0-9a-f]{64}$/.test(entry?.upstream_checksum_sha256 || "") || !/^vendor\/[A-Za-z0-9._-]+$/.test(entry?.path || "")
        || registered.has(entry.crate)) {
      throw new Error(`malformed or duplicate vendored patch registration: ${entry?.crate || "unknown"}`);
    }
    registered.set(entry.crate, entry);
  }
  if (cargo.size !== registered.size) throw new Error("Cargo patch set differs from the vendored patch registry");
  const packages = new Map(metadata.packages.map((entry) => [path.resolve(entry.manifest_path), entry]));
  for (const [crate, entry] of registered) {
    if (cargo.get(crate) !== entry.path) throw new Error(`${crate}: Cargo.toml patch path differs from ${entry.path}`);
    const manifest = path.join(repositoryRoot, entry.path, "Cargo.toml");
    const identity = manifestIdentity(manifest);
    if (identity.name !== crate || identity.version !== entry.upstream_version) {
      throw new Error(`${entry.path}: manifest is ${identity.name}@${identity.version}; expected ${crate}@${entry.upstream_version}`);
    }
    const resolved = packages.get(path.resolve(manifest));
    if (!resolved || resolved.name !== crate || resolved.version !== entry.upstream_version || resolved.source !== null) {
      throw new Error(`${crate}: cargo metadata does not resolve the registered local upstream version`);
    }
    const register = fs.readFileSync(path.join(repositoryRoot, entry.path, "PATCHES.md"), "utf8");
    if (!register.includes(`${crate} ${entry.upstream_version}`) || !register.includes(entry.upstream_checksum_sha256)) {
      throw new Error(`${entry.path}/PATCHES.md does not record the registered upstream version and checksum`);
    }
  }
  return [...registered.values()];
}

function main() {
  const metadataResult = spawnSync("cargo", ["metadata", "--locked", "--format-version", "1"], {
    cwd: root, encoding: "utf8", maxBuffer: 64 * 1024 * 1024,
  });
  if (metadataResult.error) throw metadataResult.error;
  if (metadataResult.status !== 0) throw new Error(`cargo metadata failed:\n${metadataResult.stderr || metadataResult.stdout}`);
  const patches = validateRegistry({
    registry: readJson(path.join(root, "config", "vendor-patches.json")),
    cargo: cargoPatches(path.join(root, "Cargo.toml")),
    metadata: JSON.parse(metadataResult.stdout),
  });
  process.stdout.write(`Verified ${patches.length} vendored patches against their registered crates.io versions and checksums.\n`);
}

if (require.main === module) {
  try {
    main();
  } catch (error) {
    console.error(error.stack || error.message);
    process.exitCode = 1;
  }
}

module.exports = {cargoPatches, manifestIdentity, validateRegistry};
