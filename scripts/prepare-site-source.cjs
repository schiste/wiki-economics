#!/usr/bin/env node
"use strict";

const fs = require("node:fs");
const path = require("node:path");

function prepareSiteSource({sourceDir, destinationDir, dataDir, vendorCacheDir}) {
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
  fs.symlinkSync(dataDir, path.join(destinationDir, "data"), "dir");
  return destinationDir;
}

function main() {
  const [sourceDir, destinationDir, dataDir, vendorCacheDir] = process.argv.slice(2).map((value) => value && path.resolve(value));
  if (!vendorCacheDir) {
    throw new Error("usage: prepare-site-source.cjs SOURCE_DIR DESTINATION_DIR DATA_DIR VENDOR_CACHE_DIR");
  }
  prepareSiteSource({sourceDir, destinationDir, dataDir, vendorCacheDir});
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
