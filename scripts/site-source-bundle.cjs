#!/usr/bin/env node
"use strict";

const crypto = require("node:crypto");
const fs = require("node:fs");
const path = require("node:path");

const PROVENANCE_FILE = "site-source-provenance.json";
const ROOT_FILES = [
  "LICENSE",
  "package-lock.json",
  "package.json",
  "site/observablehq.config.js",
  "site/package.json",
  "site/site-footer.js",
];
const SOURCE_DIRECTORIES = ["site/data-build", "site/src"];

function sha256Buffer(value) {
  return crypto.createHash("sha256").update(value).digest("hex");
}

function sha256File(file) {
  return sha256Buffer(fs.readFileSync(file));
}

function canonicalFilesHash(files) {
  return sha256Buffer(Buffer.from(JSON.stringify(files), "utf8"));
}

function normalizedMode(stat) {
  return stat.mode & 0o111 ? "0755" : "0644";
}

function collectDirectory(root, relativeDirectory) {
  const directory = path.join(root, relativeDirectory);
  if (!fs.statSync(directory, {throwIfNoEntry: false})?.isDirectory()) {
    throw new Error(`site source directory is missing: ${relativeDirectory}`);
  }
  const files = [];
  function visit(current, relative) {
    for (const entry of fs.readdirSync(current, {withFileTypes: true}).sort((left, right) => left.name.localeCompare(right.name))) {
      if (entry.name === ".DS_Store" || entry.name === ".observablehq" || entry.name === "data") continue;
      const absolute = path.join(current, entry.name);
      const child = path.posix.join(relative, entry.name);
      if (entry.isSymbolicLink()) throw new Error(`site source contains a symbolic link: ${child}`);
      if (entry.isDirectory()) visit(absolute, child);
      else if (entry.isFile()) files.push(child);
      else throw new Error(`site source contains an unsupported entry: ${child}`);
    }
  }
  visit(directory, relativeDirectory);
  return files;
}

function sourcePaths(root) {
  const paths = [...ROOT_FILES];
  for (const directory of SOURCE_DIRECTORIES) paths.push(...collectDirectory(root, directory));
  return [...new Set(paths)].sort();
}

function copySource(root, destination, relative) {
  const source = path.join(root, relative);
  const stat = fs.lstatSync(source, {throwIfNoEntry: false});
  if (!stat?.isFile() || stat.isSymbolicLink()) throw new Error(`site source is missing a regular file: ${relative}`);
  const output = path.join(destination, relative);
  fs.mkdirSync(path.dirname(output), {recursive: true});
  fs.copyFileSync(source, output, fs.constants.COPYFILE_EXCL);
  fs.chmodSync(output, normalizedMode(stat) === "0755" ? 0o755 : 0o644);
  const outputStat = fs.statSync(output);
  return {
    path: relative,
    sha256: sha256File(output),
    bytes: outputStat.size,
    mode: normalizedMode(outputStat),
  };
}

function generatedAt(sourceDateEpoch) {
  if (!/^\d+$/.test(String(sourceDateEpoch || ""))) {
    throw new Error("SOURCE_DATE_EPOCH is required for deterministic site-source provenance");
  }
  return new Date(Number(sourceDateEpoch) * 1000).toISOString().replace(/\.000Z$/, "Z");
}

function writeAtomic(file, value) {
  const temporary = `${file}.tmp.${process.pid}`;
  try {
    fs.writeFileSync(temporary, `${JSON.stringify(value)}\n`, {flag: "wx", mode: 0o644});
    fs.renameSync(temporary, file);
  } catch (error) {
    try { fs.unlinkSync(temporary); } catch {}
    throw error;
  }
}

function prepareBundle(root, destination, commit, sourceDateEpoch = process.env.SOURCE_DATE_EPOCH) {
  if (!/^[0-9a-f]{40}$/.test(commit || "")) throw new Error("site-source commit must be an exact 40-character SHA");
  if (fs.existsSync(destination)) throw new Error(`site-source destination already exists: ${destination}`);
  fs.mkdirSync(destination, {recursive: true, mode: 0o755});
  const files = sourcePaths(root).map((relative) => copySource(root, destination, relative));
  const provenance = {
    schema_version: 1,
    artifact: "wiki-econ-site-source",
    source_commit: commit,
    generated_at: generatedAt(sourceDateEpoch),
    content_sha256: canonicalFilesHash(files),
    files,
  };
  writeAtomic(path.join(destination, PROVENANCE_FILE), provenance);
  return provenance;
}

function actualPaths(directory) {
  const files = [];
  function visit(current, relative = "") {
    for (const entry of fs.readdirSync(current, {withFileTypes: true}).sort((left, right) => left.name.localeCompare(right.name))) {
      const absolute = path.join(current, entry.name);
      const child = relative ? path.posix.join(relative, entry.name) : entry.name;
      if (entry.isSymbolicLink()) throw new Error(`site-source release contains a symbolic link: ${child}`);
      if (entry.isDirectory()) visit(absolute, child);
      else if (entry.isFile()) files.push(child);
      else throw new Error(`site-source release contains an unsupported entry: ${child}`);
    }
  }
  visit(directory);
  return files.sort();
}

function verifyBundle(directory, expectedCommit) {
  if (!/^[0-9a-f]{40}$/.test(expectedCommit || "")) throw new Error("expected commit must be an exact 40-character SHA");
  const provenancePath = path.join(directory, PROVENANCE_FILE);
  const stat = fs.lstatSync(provenancePath, {throwIfNoEntry: false});
  if (!stat?.isFile() || stat.isSymbolicLink()) throw new Error("site-source provenance is missing");
  const provenance = JSON.parse(fs.readFileSync(provenancePath, "utf8"));
  if (provenance.schema_version !== 1 || provenance.artifact !== "wiki-econ-site-source"
      || provenance.source_commit !== expectedCommit || !Array.isArray(provenance.files)
      || !/^[0-9a-f]{64}$/.test(provenance.content_sha256 || "")) {
    throw new Error("site-source provenance has an invalid identity");
  }
  const expectedPaths = [...provenance.files.map((entry) => entry.path), PROVENANCE_FILE].sort();
  const observedPaths = actualPaths(directory);
  if (JSON.stringify(observedPaths) !== JSON.stringify(expectedPaths)) {
    throw new Error("site-source release path inventory does not match provenance");
  }
  const seen = new Set();
  for (const entry of provenance.files) {
    if (!entry || typeof entry.path !== "string" || entry.path.startsWith("/")
        || entry.path.split("/").includes("..") || seen.has(entry.path)
        || !/^[0-9a-f]{64}$/.test(entry.sha256 || "")
        || !Number.isSafeInteger(entry.bytes) || entry.bytes < 0
        || !["0644", "0755"].includes(entry.mode)) {
      throw new Error("site-source provenance contains an invalid file entry");
    }
    seen.add(entry.path);
    const file = path.join(directory, entry.path);
    const fileStat = fs.lstatSync(file, {throwIfNoEntry: false});
    if (!fileStat?.isFile() || fileStat.isSymbolicLink() || fileStat.size !== entry.bytes
        || normalizedMode(fileStat) !== entry.mode || sha256File(file) !== entry.sha256) {
      throw new Error(`site-source file failed identity validation: ${entry.path}`);
    }
  }
  if (canonicalFilesHash(provenance.files) !== provenance.content_sha256) {
    throw new Error("site-source content identity does not match its file receipts");
  }
  return provenance;
}

function main() {
  const [command, first, second, third] = process.argv.slice(2);
  if (command === "--prepare" && first && second && third) {
    const result = prepareBundle(path.resolve(first), path.resolve(second), third);
    process.stdout.write(`${JSON.stringify(result)}\n`);
  } else if (command === "--verify" && first && second && !third) {
    const result = verifyBundle(path.resolve(first), second);
    process.stdout.write(`${JSON.stringify(result)}\n`);
  } else {
    throw new Error("usage: site-source-bundle.cjs --prepare ROOT DESTINATION COMMIT | --verify DIRECTORY COMMIT");
  }
}

if (require.main === module) {
  try {
    main();
  } catch (error) {
    console.error(error.stack || error.message);
    process.exitCode = 1;
  }
}

module.exports = {
  PROVENANCE_FILE,
  ROOT_FILES,
  SOURCE_DIRECTORIES,
  canonicalFilesHash,
  prepareBundle,
  sourcePaths,
  verifyBundle,
};
