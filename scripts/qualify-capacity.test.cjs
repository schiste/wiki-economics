"use strict";

const assert = require("node:assert/strict");
const {test} = require("node:test");
const {qualify} = require("./qualify-capacity.cjs");

const policy = {
  schema_version: 1,
  memory_limit_bytes: 600,
  minimum_memory_headroom_percent: 25,
  minimum_storage_reserve_bytes: 50,
  cpu: 1,
  wikis: {
    nlwiki: {required_bucket_counts: [256]},
    ptwiki: {required_bucket_counts: [256]},
    frwiki: {required_bucket_counts: [256, 512, 1024]},
  },
};

function report(wiki, buckets, overrides = {}) {
  const bucketRows = Array.from({length: buckets}, (_, index) => index === 0 ? 30 : 0);
  return {
    schema_version: 3, run_id: `${wiki}-${buckets}`, source_commit: "a".repeat(40),
    generated_at_unix: buckets, wiki, selected_snapshot: "2026-07", bucket_count: buckets,
    rayon_threads: 1, polars_threads: 1, storage_reserve_bytes: 50,
    observed_memory_peak_bytes: 300, memory_limit_bytes: 600,
    observed_memory_headroom_percent: 50, minimum_memory_headroom_percent: 25,
    memory_gate_passed: true, storage_gate_passed: true, quota_root_bytes: 1000,
    persistent_storage_peak_bytes: 1200, persistent_storage_growth_peak_bytes: 200,
    output_sha256: "b".repeat(64),
    aggregation: {
      bucket_count: buckets, bucket_staged_rows: bucketRows, staged_rows: 30,
      largest_bucket_staged_rows: 30, output_rows: 20, total_edits: 40,
      minimum_week_start: "2001-01-01", maximum_week_start: "2026-07-27",
      output_bytes: 100, scratch_peak_bytes: 150, working_storage_peak_bytes: 200,
      elapsed_ms: 500, reduction_elapsed_ms: 300, reconciliation_elapsed_ms: 200,
    },
    ...overrides,
  };
}

function completeReports() {
  return [report("nlwiki", 256), report("ptwiki", 256), report("frwiki", 256),
    report("frwiki", 512), report("frwiki", 1024)];
}

test("qualification requires equivalent deterministic evidence and chooses the bounded default", () => {
  const result = qualify(completeReports(), policy);
  assert.equal(result.qualified, true);
  assert.equal(result.recommended_frwiki_bucket_count, 256);
  assert.equal(result.evidence.frwiki[1024].bucket_staged_rows.length, 1024);
});

test("qualification fails closed for missing, underprovisioned, or divergent reports", () => {
  assert.throws(() => qualify(completeReports().slice(1), policy), /missing required/);
  assert.throws(() => qualify(completeReports().map((value, index) => index === 2
    ? {...value, memory_limit_bytes: 500} : value), policy), /not production-equivalent/);
  assert.throws(() => qualify(completeReports().map((value, index) => index === 4
    ? {...value, output_sha256: "c".repeat(64)} : value), policy), /identical deterministic/);
});
