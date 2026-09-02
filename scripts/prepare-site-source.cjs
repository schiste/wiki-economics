#!/usr/bin/env node
"use strict";

const fs = require("node:fs");
const path = require("node:path");

function materializeImmutableFile(source, destination) {
  try {
    fs.linkSync(source, destination);
  } catch (error) {
    if (error.code !== "EXDEV") throw error;
    const metadata = fs.statSync(source);
    fs.copyFileSync(source, destination);
    fs.utimesSync(destination, metadata.atime, metadata.mtime);
  }
}

function materializeImmutableTree(sourceDir, destinationDir) {
  fs.mkdirSync(destinationDir);
  for (const name of fs.readdirSync(sourceDir).sort()) {
    if (name === "manifest.json") continue;
    const source = path.join(sourceDir, name);
    const destination = path.join(destinationDir, name);
    const metadata = fs.lstatSync(source);
    if (metadata.isSymbolicLink()) throw new Error(`dashboard defaults contain a symlink: ${source}`);
    if (metadata.isDirectory()) {
      materializeImmutableTree(source, destination);
      continue;
    }
    if (!metadata.isFile()) throw new Error(`dashboard defaults contain an unsupported entry: ${source}`);
    materializeImmutableFile(source, destination);
  }
}

function prepareSiteSource({sourceDir, destinationDir, dataDir, vendorCacheDir, manifestPath}) {
  for (const [label, directory] of Object.entries({sourceDir, dataDir, vendorCacheDir})) {
    if (!fs.statSync(directory, {throwIfNoEntry: false})?.isDirectory()) {
      throw new Error(`${label} does not exist: ${directory}`);
    }
  }
  if (fs.existsSync(destinationDir)) throw new Error(`clean site source destination already exists: ${destinationDir}`);
  fs.cpSync(sourceDir, destinationDir, {
    recursive: true,
    dereference: false,
    verbatimSymlinks: true,
    filter(source) {
      const relative = path.relative(sourceDir, source);
      const first = relative.split(path.sep)[0];
      return first !== ".observablehq" && first !== "data";
    },
  });
  const cacheDir = path.join(destinationDir, ".observablehq", "cache");
  fs.mkdirSync(cacheDir, {recursive: true});
  fs.cpSync(vendorCacheDir, cacheDir, {recursive: true, dereference: false});
  if (!manifestPath || !fs.statSync(manifestPath, {throwIfNoEntry: false})?.isFile()) {
    throw new Error(`manifestPath does not exist: ${manifestPath}`);
  }
  const destinationDataDir = path.join(destinationDir, "data");
  materializeImmutableTree(dataDir, destinationDataDir);
  materializeImmutableFile(manifestPath, path.join(destinationDataDir, "manifest.json"));
  return destinationDir;
}

function main() {
  const [sourceDir, destinationDir, dataDir, vendorCacheDir, manifestPath] = process.argv.slice(2).map((value) => value && path.resolve(value));
  if (!manifestPath) {
    throw new Error("usage: prepare-site-source.cjs SOURCE_DIR DESTINATION_DIR DATA_DIR VENDOR_CACHE_DIR MANIFEST");
  }
  prepareSiteSource({sourceDir, destinationDir, dataDir, vendorCacheDir, manifestPath});
}

if (require.main === module) {
  try {
    main();
  } catch (error) {
    console.error(error.message);
    process.exitCode = 1;
  }
}

module.exports = {prepareSiteSource};
