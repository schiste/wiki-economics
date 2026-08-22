#!/usr/bin/env node
"use strict";

const fs = require("node:fs");

const DEFAULT_URL = "https://wiki-economics.toolforge.org/health/freshness.json";

function parseArguments(argv) {
  const options = {url: DEFAULT_URL, file: null};
  for (let index = 0; index < argv.length; index += 1) {
    if (argv[index] === "--url") options.url = argv[++index];
    else if (argv[index] === "--file") options.file = argv[++index];
    else throw new Error(`unknown argument: ${argv[index]}`);
  }
  return options;
}

function validatePayload(payload) {
  if (payload?.schemaVersion !== 1 || !["healthy", "warning", "critical"].includes(payload.status) || !Array.isArray(payload.alerts)) {
    throw new Error("freshness endpoint returned an unsupported payload");
  }
  return payload;
}

async function loadPayload(options) {
  if (options.file) return validatePayload(JSON.parse(fs.readFileSync(options.file, "utf8")));
  const response = await fetch(options.url, {
    headers: {"User-Agent": "wiki-economics-freshness-check/1"},
    signal: AbortSignal.timeout(30_000),
  });
  if (!response.ok) throw new Error(`freshness endpoint returned HTTP ${response.status}`);
  return validatePayload(await response.json());
}

async function main(argv = process.argv.slice(2)) {
  const options = parseArguments(argv);
  const payload = await loadPayload(options);
  process.stdout.write(`${JSON.stringify(payload, null, 2)}\n`);
  if (payload.status !== "healthy") {
    for (const alert of payload.alerts) {
      console.error(`[${alert.severity}] ${alert.code}: ${alert.message}`);
    }
    process.exitCode = 1;
  }
}

if (require.main === module) {
  main().catch((error) => {
    console.error(error.stack || error.message);
    process.exitCode = 1;
  });
}

module.exports = {DEFAULT_URL, loadPayload, main, parseArguments, validatePayload};
