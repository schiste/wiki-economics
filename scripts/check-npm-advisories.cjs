#!/usr/bin/env node
"use strict";

const path = require("node:path");
const {spawnSync} = require("node:child_process");

const SEVERITY = new Map([
  ["info", 0],
  ["low", 1],
  ["moderate", 2],
  ["high", 3],
  ["critical", 4],
]);

// Observable Framework 1.13.4 currently resolves esbuild 0.27.x. Its only
// remaining advisory is local to the Windows development server; production
// builds and Toolforge run on Linux and do not expose that server. Keep this
// allowlist exact so a new advisory, package, or severity fails closed.
const ALLOWED_LOW_PACKAGES = new Set(["@observablehq/framework", "esbuild"]);
const ALLOWED_LOW_ADVISORIES = new Set(["GHSA-g7r4-m6w7-qqqr"]);

function advisoryId(url) {
  return typeof url === "string" ? url.match(/GHSA-[a-z0-9-]+$/i)?.[0] || null : null;
}

function validateAuditReport(report, label) {
  if (report?.auditReportVersion !== 2 || !report.vulnerabilities || !report.metadata?.vulnerabilities) {
    throw new Error(`${label}: npm did not return an audit report`);
  }

  const errors = [];
  const names = Object.keys(report.vulnerabilities).sort();
  for (const name of names) {
    const vulnerability = report.vulnerabilities[name];
    const rank = SEVERITY.get(vulnerability.severity);
    if (rank === undefined) {
      errors.push(`${name}: unknown severity ${vulnerability.severity}`);
      continue;
    }
    if (rank >= SEVERITY.get("moderate")) {
      errors.push(`${name}: ${vulnerability.severity} advisory is not allowed`);
      continue;
    }
    if (rank === SEVERITY.get("low") && !ALLOWED_LOW_PACKAGES.has(name)) {
      errors.push(`${name}: low advisory is not explicitly allowed`);
    }
    for (const via of vulnerability.via || []) {
      if (typeof via === "string") {
        if (!report.vulnerabilities[via]) errors.push(`${name}: unresolved advisory path through ${via}`);
        continue;
      }
      const id = advisoryId(via.url);
      if (!id || !ALLOWED_LOW_ADVISORIES.has(id)) {
        errors.push(`${name}: advisory ${id || via.url || via.title || "unknown"} is not explicitly allowed`);
      }
    }
  }

  if (errors.length > 0) throw new Error(`${label}:\n- ${errors.join("\n- ")}`);
  return {label, allowedLow: names};
}

function auditGraph(directory, label, runner = spawnSync) {
  const result = runner("npm", ["audit", "--json"], {
    cwd: directory,
    encoding: "utf8",
    maxBuffer: 16 * 1024 * 1024,
  });
  if (result.error) throw result.error;
  let report;
  try {
    report = JSON.parse(result.stdout);
  } catch {
    throw new Error(`${label}: npm audit returned invalid JSON: ${result.stderr || result.stdout}`);
  }
  return validateAuditReport(report, label);
}

function main() {
  const root = path.resolve(__dirname, "..");
  const summaries = [auditGraph(root, "root"), auditGraph(path.join(root, "site"), "site")];
  for (const summary of summaries) {
    const detail = summary.allowedLow.length > 0 ? summary.allowedLow.join(", ") : "none";
    process.stdout.write(`${summary.label}: no moderate-or-higher advisories; accepted low packages: ${detail}\n`);
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

module.exports = {advisoryId, auditGraph, validateAuditReport};
