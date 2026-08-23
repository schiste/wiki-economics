#!/usr/bin/env node
"use strict";

const path = require("node:path");
const fs = require("node:fs");
const {spawnSync} = require("node:child_process");

const SEVERITY = new Map([
  ["info", 0],
  ["low", 1],
  ["moderate", 2],
  ["high", 3],
  ["critical", 4],
]);

const DEFAULT_EXCEPTIONS_FILE = path.resolve(__dirname, "..", "config", "npm-audit-exceptions.json");

function advisoryId(url) {
  return typeof url === "string" ? url.match(/GHSA-[a-z0-9-]+$/i)?.[0] || null : null;
}

function loadExceptions(file = process.env.WIKI_ECON_NPM_AUDIT_EXCEPTIONS || DEFAULT_EXCEPTIONS_FILE, today = new Date()) {
  const document = JSON.parse(fs.readFileSync(file, "utf8"));
  if (document?.schema_version !== 2 || !Number.isInteger(document.minimum_expiry_warning_days)
      || document.minimum_expiry_warning_days < 1 || document.minimum_expiry_warning_days > 365
      || !Array.isArray(document.exceptions)) {
    throw new Error(`${file}: invalid npm audit exception document`);
  }
  const advisories = new Map();
  const packages = new Set();
  const todayString = today.toISOString().slice(0, 10);
  const warningBoundary = new Date(today);
  warningBoundary.setUTCDate(warningBoundary.getUTCDate() + document.minimum_expiry_warning_days);
  const warningBoundaryString = warningBoundary.toISOString().slice(0, 10);
  for (const exception of document.exceptions) {
    if (!/^GHSA-[a-z0-9-]+$/i.test(exception?.advisory || "")
        || !SEVERITY.has(exception?.severity)
        || !Array.isArray(exception?.packages)
        || exception.packages.length === 0
        || !exception.packages.every((name) => typeof name === "string" && name.length > 0)
        || !/^\d{4}-\d{2}-\d{2}$/.test(exception?.expires_on || "")
        || typeof exception?.reason !== "string"
        || exception.reason.trim().length < 20) {
      throw new Error(`${file}: malformed exception for ${exception?.advisory || "unknown advisory"}`);
    }
    if (exception.expires_on < todayString) {
      throw new Error(`${file}: exception ${exception.advisory} expired on ${exception.expires_on}`);
    }
    if (exception.expires_on <= warningBoundaryString) {
      throw new Error(`${file}: exception ${exception.advisory} expires within ${document.minimum_expiry_warning_days} days on ${exception.expires_on}`);
    }
    if (advisories.has(exception.advisory)) {
      throw new Error(`${file}: duplicate exception ${exception.advisory}`);
    }
    const normalized = {...exception, packages: new Set(exception.packages)};
    advisories.set(exception.advisory, normalized);
    for (const name of exception.packages) packages.add(name);
  }
  return {advisories, packages, minimumExpiryWarningDays: document.minimum_expiry_warning_days};
}

function validateAuditReport(report, label, policy = loadExceptions()) {
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
    if (rank === SEVERITY.get("low") && !policy.packages.has(name)) {
      errors.push(`${name}: low advisory is not explicitly allowed`);
    }
    for (const via of vulnerability.via || []) {
      if (typeof via === "string") {
        if (!report.vulnerabilities[via]) errors.push(`${name}: unresolved advisory path through ${via}`);
        continue;
      }
      const id = advisoryId(via.url);
      const exception = id ? policy.advisories.get(id) : null;
      if (!exception || !exception.packages.has(name) || exception.severity !== vulnerability.severity) {
        errors.push(`${name}: advisory ${id || via.url || via.title || "unknown"} is not explicitly allowed`);
      }
    }
  }

  if (errors.length > 0) throw new Error(`${label}:\n- ${errors.join("\n- ")}`);
  return {label, allowedLow: names};
}

function auditGraph(directory, label, runner = spawnSync, policy = loadExceptions()) {
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
  return validateAuditReport(report, label, policy);
}

function main() {
  const root = path.resolve(__dirname, "..");
  const summaries = [auditGraph(root, "workspace")];
  for (const summary of summaries) {
    process.stdout.write(`${summary.label}: no unapproved npm advisories\n`);
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

module.exports = {advisoryId, auditGraph, loadExceptions, validateAuditReport};
