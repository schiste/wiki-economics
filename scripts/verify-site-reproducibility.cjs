#!/usr/bin/env node
"use strict";

const crypto = require("node:crypto");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const {buildFixture, listFiles} = require("./build-site-fixture.cjs");

function artifactHashes(directory) {
  return Object.fromEntries(listFiles(directory).map((relative) => [
    relative.replaceAll(path.sep, "/"),
    crypto.createHash("sha256").update(fs.readFileSync(path.join(directory, relative))).digest("hex"),
  ]));
}

function compareArtifactHashes(left, right) {
  const names = [...new Set([...Object.keys(left), ...Object.keys(right)])].sort();
  const differences = names.filter((name) => left[name] !== right[name]);
  if (differences.length > 0) {
    throw new Error(`site build is not byte-for-byte deterministic; differing artifacts: ${differences.slice(0, 10).join(", ")}`);
  }
  return names;
}

function verifyReproducibility({dataDir, workDir, root = path.resolve(__dirname, "..")}) {
  const ownedWorkDir = !workDir;
  const directory = workDir || fs.mkdtempSync(path.join(os.tmpdir(), "wiki-econ-site-repro-"));
  const first = path.join(directory, "build-a");
  const second = path.join(directory, "build-b");
  try {
    fs.mkdirSync(directory, {recursive: true});
    buildFixture({dataDir, distDir: first, root});
    buildFixture({dataDir, distDir: second, root});
    const files = compareArtifactHashes(artifactHashes(first), artifactHashes(second));
    return {files, first, second};
  } finally {
    if (ownedWorkDir) fs.rmSync(directory, {recursive: true, force: true});
  }
}

function parseArguments(argv) {
  const options = {};
  for (let index = 0; index < argv.length; index += 2) {
    const name = argv[index];
    if (name !== "--data-dir" && name !== "--work-dir") throw new Error(`unknown argument: ${name}`);
    if (!argv[index + 1]) throw new Error(`${name} requires a path`);
    options[name.slice(2)] = path.resolve(argv[index + 1]);
  }
  if (!options["data-dir"] || !options["work-dir"]) {
    throw new Error("usage: verify-site-reproducibility.cjs --data-dir PATH --work-dir PATH");
  }
  return options;
}

function main() {
  const options = parseArguments(process.argv.slice(2));
  const result = verifyReproducibility({dataDir: options["data-dir"], workDir: options["work-dir"]});
  process.stdout.write(`Verified two byte-identical offline site builds (${result.files.length} artifacts).\n`);
}

if (require.main === module) {
  try {
    main();
  } catch (error) {
    console.error(error.stack || error.message);
    process.exitCode = 1;
  }
}

module.exports = {artifactHashes, compareArtifactHashes, parseArguments, verifyReproducibility};
