#!/usr/bin/env node
"use strict";

const fs = require("node:fs");
const path = require("node:path");
const crypto = require("node:crypto");

const root = path.resolve(__dirname, "..");

function listFiles(directory, prefix = "") {
  const files = [];
  for (const entry of fs.readdirSync(directory, {withFileTypes: true})) {
    const relative = path.posix.join(prefix, entry.name);
    if (entry.isDirectory()) files.push(...listFiles(path.join(directory, entry.name), relative));
    else if (entry.isFile()) files.push(relative);
  }
  return files.sort();
}

function packageIdentity(relative) {
  const match = relative.match(/^_npm\/(?:@([^/]+)\/)?([^/@]+)@([^/]+)\//);
  if (!match) return null;
  return {name: match[1] ? `@${match[1]}/${match[2]}` : match[2], version: match[3]};
}

function treeSha256(directory) {
  const hash = crypto.createHash("sha256");
  for (const relative of listFiles(directory).filter((file) => file.startsWith("_"))) {
    hash.update(relative.replaceAll(path.sep, "/"));
    hash.update("\0");
    hash.update(crypto.createHash("sha256").update(fs.readFileSync(path.join(directory, relative))).digest("hex"));
    hash.update("\n");
  }
  return hash.digest("hex");
}

function dependencyVersion(lock, name) {
  return lock.packages?.[`site/node_modules/${name}`]?.version
    || lock.packages?.[`node_modules/${name}`]?.version
    || null;
}

function runtimeRemoteReferences(relative, content) {
  const references = [];
  const addMatches = (expression) => {
    for (const match of content.matchAll(expression)) references.push(match[1]);
  };
  if (relative.endsWith(".html")) {
    addMatches(/<(?:script|img|iframe|audio|video|source)\b[^>]*\bsrc=["'](https?:\/\/[^"']+)["']/gi);
    addMatches(/<link\b[^>]*\bhref=["'](https?:\/\/[^"']+)["']/gi);
  }
  if (relative.endsWith(".css")) addMatches(/url\(["']?(https?:\/\/[^)'"\s]+)["']?\)/gi);
  if (relative.endsWith(".js") || relative.endsWith(".html")) {
    addMatches(/\b(?:import|fetch)\s*\(\s*["'](https?:\/\/[^"']+)["']/g);
    addMatches(/\bfrom\s*["'](https?:\/\/[^"']+)["']/g);
    addMatches(/\bnew\s+(?:Worker|SharedWorker)\s*\(\s*["'](https?:\/\/[^"']+)["']/g);
  }
  return references;
}

function verifySiteDependencies(distDir, options = {}) {
  const closureFile = options.closureFile || path.join(root, "config", "site-dependency-closure.json");
  const workspaceManifestFile = options.workspaceManifestFile || path.join(root, "package.json");
  const siteManifestFile = options.siteManifestFile || path.join(root, "site", "package.json");
  const lockFile = options.lockFile || path.join(root, "package-lock.json");
  const closure = JSON.parse(fs.readFileSync(closureFile, "utf8"));
  const workspaceManifest = JSON.parse(fs.readFileSync(workspaceManifestFile, "utf8"));
  const siteManifest = JSON.parse(fs.readFileSync(siteManifestFile, "utf8"));
  const lock = JSON.parse(fs.readFileSync(lockFile, "utf8"));
  if (closure.schema_version !== 1) throw new Error("unsupported site dependency closure schema");
  if (closure.build_tools?.["@observablehq/framework"] !== workspaceManifest.dependencies?.["@observablehq/framework"]
      || closure.build_tools?.esbuild !== workspaceManifest.overrides?.esbuild) {
    throw new Error("Observable Framework or esbuild override differs from the reviewed build-tool closure");
  }
  for (const [name, expected] of Object.entries(closure.build_tools)) {
    const locked = dependencyVersion(lock, name);
    if (locked !== expected) throw new Error(`${name} build-tool lock version ${locked || "missing"} differs from ${expected}`);
  }
  const vendorCacheDir = options.vendorCacheDir === null
    ? null
    : options.vendorCacheDir || path.join(root, "site", "vendor", "observable-cache");
  if (vendorCacheDir) {
    const actualCacheHash = treeSha256(vendorCacheDir);
    if (actualCacheHash !== closure.vendored_cache_sha256) {
      throw new Error(`vendored Observable cache hash ${actualCacheHash} differs from the reviewed closure`);
    }
    for (const [name, version] of Object.entries(closure.resolution_only_packages || {})) {
      const marker = path.join(vendorCacheDir, "_npm", `${name}@${version}`, "resolution-only.txt");
      if (!fs.statSync(marker, {throwIfNoEntry: false})?.isFile()) {
        throw new Error(`missing vendored resolution-only marker: ${name}@${version}`);
      }
    }
    for (const relative of listFiles(vendorCacheDir)) {
      const normalized = relative.replaceAll(path.sep, "/");
      for (const pattern of closure.forbidden_asset_patterns) {
        if (normalized.includes(pattern)) throw new Error(`unexpected DuckDB asset in vendored cache: ${normalized}`);
      }
      if (normalized.endsWith(".wasm") && !closure.allowed_wasm.includes(normalized)) {
        throw new Error(`unexpected WASM asset in vendored cache: ${normalized}`);
      }
      const identity = packageIdentity(normalized);
      const reviewedVersion = identity && (
        closure.generated_packages[identity.name]
        || closure.direct_browser_packages[identity.name]
        || closure.resolution_only_packages?.[identity.name]
      );
      if (identity && reviewedVersion !== identity.version) {
        throw new Error(`undeclared vendored browser package: ${identity.name}@${identity.version}`);
      }
    }
  }

  for (const [name, expected] of Object.entries(closure.direct_browser_packages)) {
    const declared = siteManifest.dependencies?.[name];
    if (declared !== expected) throw new Error(`${name} must be declared exactly as ${expected}; found ${declared || "undeclared"}`);
    const locked = dependencyVersion(lock, name);
    if (locked !== expected) throw new Error(`${name} lock version ${locked || "missing"} differs from ${expected}`);
  }
  for (const [name, version] of Object.entries(siteManifest.dependencies || {})) {
    if (/^[~^<>=*]|\bx\b|\|/.test(version)) throw new Error(`${name} uses a non-exact production version: ${version}`);
    const locked = dependencyVersion(lock, name);
    if (locked !== version) throw new Error(`${name} lock version ${locked || "missing"} differs from ${version}`);
  }

  const files = listFiles(distDir);
  const observed = new Map();
  for (const relative of files) {
    const normalized = relative.replaceAll(path.sep, "/");
    for (const pattern of closure.forbidden_asset_patterns) {
      if (normalized.includes(pattern)) throw new Error(`unexpected DuckDB asset: ${normalized}`);
    }
    if (normalized.endsWith(".wasm") && !closure.allowed_wasm.includes(normalized)) {
      throw new Error(`unexpected WASM asset: ${normalized}`);
    }
    const identity = packageIdentity(normalized);
    if (identity) {
      const expected = closure.generated_packages[identity.name];
      if (!expected) throw new Error(`undeclared generated browser package: ${identity.name}@${identity.version}`);
      if (identity.version !== expected) throw new Error(`unexpected ${identity.name} version ${identity.version}; expected ${expected}`);
      const previous = observed.get(identity.name);
      if (previous && previous !== identity.version) throw new Error(`multiple ${identity.name} versions in generated site`);
      observed.set(identity.name, identity.version);
    }
    if (/\.(?:css|html|js)$/.test(normalized)) {
      const content = fs.readFileSync(path.join(distDir, relative), "utf8");
      const remote = runtimeRemoteReferences(normalized, content);
      if (remote.length > 0) throw new Error(`remote runtime dependency in ${normalized}: ${remote[0]}`);
    }
  }
  for (const [name, expected] of Object.entries(closure.generated_packages)) {
    if (observed.get(name) !== expected) throw new Error(`required generated browser package is missing: ${name}@${expected}`);
  }
  return {files: files.length, packages: Object.fromEntries([...observed].sort())};
}

function main() {
  const distDir = process.argv[2];
  if (!distDir) throw new Error("usage: verify-site-dependencies.cjs DIST_DIR");
  const result = verifySiteDependencies(path.resolve(distDir));
  process.stdout.write(`Verified deterministic browser closure (${Object.keys(result.packages).length} packages, ${result.files} files).\n`);
}

if (require.main === module) {
  try {
    main();
  } catch (error) {
    console.error(error.message);
    process.exitCode = 1;
  }
}

module.exports = {dependencyVersion, listFiles, packageIdentity, runtimeRemoteReferences, treeSha256, verifySiteDependencies};
