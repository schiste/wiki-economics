#!/usr/bin/env node
"use strict";

const fs = require("node:fs");
const path = require("node:path");

const ROOT_FILES = ["robots.txt"];

function publishStaticRoot({sourceDir, distDir}) {
  for (const [label, directory] of Object.entries({sourceDir, distDir})) {
    if (!fs.statSync(directory, {throwIfNoEntry: false})?.isDirectory()) {
      throw new Error(`${label} does not exist: ${directory}`);
    }
  }

  for (const name of ROOT_FILES) {
    const source = path.join(sourceDir, name);
    const metadata = fs.lstatSync(source, {throwIfNoEntry: false});
    if (!metadata?.isFile()) throw new Error(`required static root file is missing or unsafe: ${source}`);
    fs.copyFileSync(source, path.join(distDir, name));
  }

  return [...ROOT_FILES];
}

function main() {
  const [sourceDir, distDir] = process.argv.slice(2).map((value) => value && path.resolve(value));
  if (!distDir) throw new Error("usage: publish-static-root.cjs SOURCE_DIR DIST_DIR");
  publishStaticRoot({sourceDir, distDir});
}

if (require.main === module) {
  try {
    main();
  } catch (error) {
    console.error(error.message);
    process.exitCode = 1;
  }
}

module.exports = {ROOT_FILES, publishStaticRoot};
