#!/usr/bin/env node
"use strict";

const fs = require("node:fs");
const path = require("node:path");

function parseArguments(argv) {
  const options = {
    reports: null,
    output: null,
    policy: path.resolve(__dirname, "../config/capacity-qualification.json"),
  };
  for (let index = 0; index < argv.length; index += 1) {
    const value = argv[index];
    if (value === "--reports") options.reports = path.resolve(argv[++index]);
    else if (value === "--output") options.output = path.resolve(argv[++index]);
    else if (value === "--policy") options.policy = path.resolve(argv[++index]);
    else throw new Error(`unknown argument: ${value}`);
  }
  if (!options.reports || !options.output) throw new Error("--reports and --output are required");
  return options;
}

function readJson(file) {
  return JSON.parse(fs.readFileSync(file, "utf8"));
}

function reportFiles(root) {
  if (!fs.existsSync(root)) return [];
  const pending = [root];
  const files = [];
  while (pending.length > 0) {
    const directory = pending.pop();
    for (const entry of fs.readdirSync(directory, {withFileTypes: true})) {
      const candidate = path.join(directory, entry.name);
      if (entry.isDirectory()) pending.push(candidate);
      else if (entry.isFile() && entry.name.endsWith(".json")) files.push(candidate);
    }
  }
  return files.sort();
}

function validatePolicy(policy) {
  if (policy?.schema_version !== 1 || !Number.isSafeInteger(policy.memory_limit_bytes)
      || !(policy.minimum_memory_headroom_percent >= 25)
      || !Number.isSafeInteger(policy.minimum_storage_reserve_bytes)
      || policy.cpu !== 1 || !policy.wikis) {
    throw new Error("invalid capacity qualification policy");
  }
  return policy;
}

function validateReport(report, policy, expectedWiki, expectedBuckets) {
  const aggregation = report?.aggregation;
  if (report?.schema_version !== 3 || report.wiki !== expectedWiki
      || report.bucket_count !== expectedBuckets || !/^\d{4}-\d{2}$/.test(report.selected_snapshot || "")
      || report.memory_limit_bytes !== policy.memory_limit_bytes
      || report.minimum_memory_headroom_percent < policy.minimum_memory_headroom_percent
      || report.observed_memory_headroom_percent < policy.minimum_memory_headroom_percent
      || report.storage_reserve_bytes < policy.minimum_storage_reserve_bytes
      || report.rayon_threads !== policy.cpu || report.polars_threads !== policy.cpu
      || report.memory_gate_passed !== true || report.storage_gate_passed !== true
      || !aggregation || aggregation.bucket_count !== expectedBuckets
      || !Array.isArray(aggregation.bucket_staged_rows)
      || aggregation.bucket_staged_rows.length !== expectedBuckets) {
    throw new Error(`capacity report is not production-equivalent: ${expectedWiki}/${expectedBuckets}`);
  }
  const stagedRows = aggregation.bucket_staged_rows.reduce((sum, rows) => sum + rows, 0);
  const largest = Math.max(...aggregation.bucket_staged_rows);
  if (stagedRows !== aggregation.staged_rows || largest !== aggregation.largest_bucket_staged_rows
      || !(aggregation.output_rows > 0) || !(aggregation.total_edits > 0)
      || !(aggregation.output_bytes > 0) || !(aggregation.scratch_peak_bytes > 0)
      || aggregation.working_storage_peak_bytes < aggregation.scratch_peak_bytes
      || report.persistent_storage_growth_peak_bytes !== aggregation.working_storage_peak_bytes
      || report.persistent_storage_peak_bytes < report.quota_root_bytes
      || !/^[0-9a-f]{64}$/.test(report.output_sha256 || "")) {
    throw new Error(`capacity report evidence is inconsistent: ${expectedWiki}/${expectedBuckets}`);
  }
  return report;
}

function comparableIdentity(report) {
  const aggregation = report.aggregation;
  return JSON.stringify({
    snapshot: report.selected_snapshot,
    rows: aggregation.output_rows,
    edits: aggregation.total_edits,
    minimum: aggregation.minimum_week_start,
    maximum: aggregation.maximum_week_start,
    sha256: report.output_sha256,
  });
}

function qualify(reports, policy) {
  validatePolicy(policy);
  const latest = new Map();
  for (const report of reports) {
    const key = `${report.wiki}:${report.bucket_count}`;
    const prior = latest.get(key);
    if (!prior || Number(report.generated_at_unix) > Number(prior.generated_at_unix)) latest.set(key, report);
  }

  const selected = {};
  for (const [wiki, requirements] of Object.entries(policy.wikis)) {
    selected[wiki] = {};
    for (const buckets of requirements.required_bucket_counts) {
      const report = latest.get(`${wiki}:${buckets}`);
      if (!report) throw new Error(`missing required capacity report: ${wiki}/${buckets}`);
      selected[wiki][buckets] = validateReport(report, policy, wiki, buckets);
    }
  }

  const frwikiReports = Object.values(selected.frwiki || {});
  if (frwikiReports.length !== 3 || new Set(frwikiReports.map(comparableIdentity)).size !== 1) {
    throw new Error("frwiki bucket variants do not produce identical deterministic output");
  }
  const recommendation = [...frwikiReports]
    .sort((left, right) => left.bucket_count - right.bucket_count)
    .find((report) => report.memory_gate_passed && report.storage_gate_passed);
  if (!recommendation) throw new Error("no frwiki bucket variant satisfies qualification gates");

  return {
    schema_version: 1,
    qualified: true,
    generated_at: new Date().toISOString(),
    policy,
    recommended_frwiki_bucket_count: recommendation.bucket_count,
    evidence: Object.fromEntries(Object.entries(selected).map(([wiki, variants]) => [wiki,
      Object.fromEntries(Object.entries(variants).map(([buckets, report]) => [buckets, {
        run_id: report.run_id,
        source_commit: report.source_commit,
        selected_snapshot: report.selected_snapshot,
        peak_memory_bytes: report.observed_memory_peak_bytes,
        memory_headroom_percent: report.observed_memory_headroom_percent,
        scratch_peak_bytes: report.aggregation.scratch_peak_bytes,
        working_storage_peak_bytes: report.aggregation.working_storage_peak_bytes,
        persistent_storage_peak_bytes: report.persistent_storage_peak_bytes,
        duration_ms: report.aggregation.elapsed_ms,
        reduction_duration_ms: report.aggregation.reduction_elapsed_ms,
        reconciliation_duration_ms: report.aggregation.reconciliation_elapsed_ms,
        bucket_staged_rows: report.aggregation.bucket_staged_rows,
        output_rows: report.aggregation.output_rows,
        output_bytes: report.aggregation.output_bytes,
        output_sha256: report.output_sha256,
      }]))])),
  };
}

function atomicWriteJson(file, value) {
  fs.mkdirSync(path.dirname(file), {recursive: true});
  const temporary = `${file}.tmp.${process.pid}`;
  try {
    fs.writeFileSync(temporary, `${JSON.stringify(value, null, 2)}\n`, {mode: 0o600});
    fs.renameSync(temporary, file);
  } catch (error) {
    try { fs.unlinkSync(temporary); } catch {}
    throw error;
  }
}

function main(argv = process.argv.slice(2)) {
  const options = parseArguments(argv);
  const policy = readJson(options.policy);
  const reports = reportFiles(options.reports).map(readJson);
  const result = qualify(reports, policy);
  atomicWriteJson(options.output, result);
  process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
}

if (require.main === module) {
  try { main(); } catch (error) { console.error(error.stack || error.message); process.exitCode = 1; }
}

module.exports = {atomicWriteJson, comparableIdentity, parseArguments, qualify, reportFiles, validatePolicy, validateReport};
