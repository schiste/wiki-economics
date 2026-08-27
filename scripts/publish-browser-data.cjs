#!/usr/bin/env node
"use strict";

const crypto = require("node:crypto");
const fs = require("node:fs");
const path = require("node:path");

const INDEX_FILENAME = "browser-data-index.json";

function safeSource(dataDir, entry) {
  if (!/^[a-z0-9_]+$/.test(entry?.metric || "")) {
    throw new Error(`unsafe browser data entry: ${JSON.stringify(entry)}`);
  }
  if (entry.scope === "wiki"
      && /^[a-z0-9_]+wiki$/.test(entry.wiki || "")
      && entry.shard == null
      && entry.aggregation_version == null
      && entry.file === `browser-data/${entry.metric}/${entry.wiki}.parquet`) {
    return path.join(dataDir, entry.wiki, `${entry.metric}.parquet`);
  }
  if (entry.scope === "global"
      && entry.wiki === "all"
      && /^\d{4}$/.test(entry.shard || "")
      && entry.aggregation_version === "global-browser-aggregate-v1"
      && entry.file === `browser-data/${entry.metric}/all-${entry.shard}.parquet`) {
    return path.join(dataDir, "_browser-global", entry.metric, `${entry.shard}.parquet`);
  }
  throw new Error(`unsafe browser data entry: ${JSON.stringify(entry)}`);
}

function sha256(file) {
  return crypto.createHash("sha256").update(fs.readFileSync(file)).digest("hex");
}

function publishBrowserData({dataDir, distDir}) {
  const indexPath = path.join(dataDir, INDEX_FILENAME);
  const index = JSON.parse(fs.readFileSync(indexPath, "utf8"));
  if (index?.schema_version !== 3
      || index?.cache_schema_version !== 3
      || !/^[0-9a-f]{64}$/.test(index?.generation || "")
      || index?.license_spdx !== "MIT"
      || !Array.isArray(index?.entries)
      || index.entries.length === 0) {
    throw new Error(`invalid browser data index: ${indexPath}`);
  }
  const publicRoot = path.join(distDir, "browser-data");
  fs.mkdirSync(publicRoot, {recursive: true});
  const copied = [];
  const identities = new Set();
  for (const entry of index.entries) {
    const source = safeSource(dataDir, entry);
    const identity = entry.file;
    if (!/^[0-9a-f]{64}$/.test(entry?.artifact_receipt_sha256 || "")) {
      throw new Error(`browser data entry has no artifact receipt: ${identity}`);
    }
    if (identities.has(identity)) throw new Error(`duplicate browser data entry: ${identity}`);
    identities.add(identity);
    const stat = fs.statSync(source, {throwIfNoEntry: false});
    if (!stat?.isFile() || stat.size !== entry.bytes || sha256(source) !== entry.sha256) {
      throw new Error(`browser data source does not match its index: ${source}`);
    }
    const destination = path.join(distDir, ...entry.file.split("/"));
    fs.mkdirSync(path.dirname(destination), {recursive: true});
    fs.copyFileSync(source, destination);
    copied.push(entry.file);
  }
  fs.copyFileSync(indexPath, path.join(publicRoot, "index.json"));
  copied.push("browser-data/index.json");
  return copied.sort();
}

function main() {
  const [dataDir, distDir] = process.argv.slice(2);
  if (!dataDir || !distDir) throw new Error("usage: publish-browser-data.cjs DATA_DIR DIST_DIR");
  const files = publishBrowserData({dataDir: path.resolve(dataDir), distDir: path.resolve(distDir)});
  process.stdout.write(`Published ${files.length - 1} indexed browser data partitions.\n`);
}

if (require.main === module) {
  try {
    main();
  } catch (error) {
    console.error(error.message);
    process.exitCode = 1;
  }
}

module.exports = {INDEX_FILENAME, publishBrowserData, safeSource, sha256};
