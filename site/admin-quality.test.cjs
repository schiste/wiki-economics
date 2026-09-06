"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const test = require("node:test");
const {buildWikiQuality, compareNumber, monthDistance, safeDescendant} = require("./admin-quality.cjs");

function writeJson(file, value) {
  fs.mkdirSync(path.dirname(file), {recursive: true});
  fs.writeFileSync(file, `${JSON.stringify(value)}\n`);
}

function writeArtifact(root, relative, {rows, total, hash, receiptHash}) {
  const artifact = path.join(root, relative);
  fs.mkdirSync(path.dirname(artifact), {recursive: true});
  fs.writeFileSync(artifact, "data");
  const prepared = {
    path: relative,
    bytes: 4,
    rows,
    sha256: hash,
    receipt_identity: "nlwiki/gdp.parquet",
    receipt_sha256: receiptHash,
  };
  writeJson(`${artifact}.receipt.json`, {
    schema_version: 1,
    receipt_sha256: receiptHash,
    receipt: {
      schema_version: 2,
      identity: "nlwiki/gdp.parquet",
      artifact_sha256: hash,
      bytes: 4,
      parquet_schema: [{name: "year_month", data_type: "string"}],
      rows,
      minimum_date: "2001-01",
      maximum_date: "2026-08",
      conservation_totals: {total_edits: total},
      minimum_wiki: "nlwiki",
      maximum_wiki: "nlwiki",
      ordering_contract: "wiki-major/v1",
      algorithm_version: "monthly-v1",
      input_fingerprint: "input",
    },
  });
  return prepared;
}

function policy() {
  const signal = {minimum_baseline: 10, decrease_warning_fraction: 0.01,
    decrease_critical_fraction: 0.1, increase_warning_fraction_per_month: 0.2,
    increase_critical_fraction_per_month: 0.5};
  return {
    schema_version: 1,
    scrub_max_age_days: 14,
    default: signal,
    signals: Object.fromEntries([
      ["editor_period_observations", "Editor observations"],
      ["patrol_events", "Patrol events"],
      ["rights_events", "Rights events"],
      ["account_creation_events", "Account creations"],
      ["local_account_block_events", "Account block events"],
      ["indefinitely_blocked_accounts", "Indefinitely blocked accounts"],
    ].map(([key, label]) => [key, {label}])),
  };
}

test("quality ledger authenticates receipts, scrub evidence, deltas, and every trend signal", (t) => {
  const outputDir = fs.mkdtempSync(path.join(os.tmpdir(), "wiki-econ-quality-"));
  t.after(() => fs.rmSync(outputDir, {recursive: true, force: true}));
  const candidateRelative = "_candidates/nlwiki/2026-08/candidate";
  const candidateDir = path.join(outputDir, candidateRelative);
  const candidateArtifact = writeArtifact(candidateDir, "nlwiki/gdp.parquet", {
    rows: 160, total: 1700, hash: "a".repeat(64), receiptHash: "b".repeat(64),
  });
  const publishedArtifact = writeArtifact(outputDir, "nlwiki/gdp.parquet", {
    rows: 100, total: 1000, hash: "c".repeat(64), receiptHash: "d".repeat(64),
  });
  const candidateSignals = {
    editor_period_observations: 180,
    patrol_events: 180,
    rights_events: 180,
    account_creation_events: 180,
    local_account_block_events: 180,
    indefinitely_blocked_accounts: 180,
  };
  writeJson(path.join(candidateDir, "ready.json"), {
    schema_version: 2, wiki: "nlwiki", snapshot: "2026-08", run_id: "candidate",
    quality_signals: candidateSignals, artifacts: [candidateArtifact],
  });
  const publishedRelative = "_candidates/nlwiki/2026-07/published";
  writeJson(path.join(outputDir, publishedRelative, "ready.json"), {
    schema_version: 2, wiki: "nlwiki", snapshot: "2026-07", run_id: "published",
    quality_signals: Object.fromEntries(Object.keys(candidateSignals).map((key) => [key, 100])),
    artifacts: [publishedArtifact],
  });
  writeJson(path.join(outputDir, "_scrubs/scrub-1.json"), {
    schema_version: 2, scrubbed_at_unix: 900,
    artifacts: [{path: "nlwiki/gdp.parquet", receipt_sha256: "d".repeat(64), artifact_sha256: "c".repeat(64)}],
  });
  const reference = (candidate_relative, snapshot, run_id) => ({candidate_relative, snapshot, run_id});
  const gate = {
    selected_snapshot_versions: {nlwiki: "2026-07"},
    wiki_proofs: {nlwiki: {artifacts: {gdp: publishedArtifact}, quality_signals:
      Object.fromEntries(Object.keys(candidateSignals).map((key) => [key, 100]))}},
  };
  const quality = buildWikiQuality({
    wiki: "nlwiki",
    definitions: [{id: "gdp", family: "monthly", algorithm_version: "monthly-v1",
      schema: [{name: "year_month", data_type: "string"}]}],
    dataDir: path.join(outputDir, "data"), outputDir, gate,
    index: {newest_valid_ready: reference(candidateRelative, "2026-08", "candidate"),
      active_published: reference(publishedRelative, "2026-07", "published")},
    scrubStatus: {state: "succeeded", run_id: "scrub-1"},
    policy: policy(), nowUnix: 1000,
  });

  assert.equal(quality.metrics[0].candidate.valid, true);
  assert.equal(quality.metrics[0].published.scrubAgeSeconds, 100);
  assert.equal(quality.metrics[0].comparison.rows.delta, 60);
  assert.equal(quality.metrics[0].comparison.totals.total_edits.delta, 700);
  assert.equal(quality.metrics[0].comparison.changed, true);
  for (const signal of Object.keys(candidateSignals)) {
    assert.ok(quality.anomalies.some((anomaly) => anomaly.code === `unexpected_${signal}_change`));
  }
  assert.ok(quality.anomalies.some((anomaly) => anomaly.code === "unexpected_rows_change"));
  assert.ok(quality.anomalies.some((anomaly) => anomaly.code === "unexpected_conservation_total_change"));
});

test("quality helpers reject path escape and scale expected growth by elapsed snapshot months", () => {
  assert.equal(safeDescendant("/safe/root", "../../escape"), null);
  assert.equal(monthDistance("2025-12", "2026-02"), 2);
  assert.equal(compareNumber({signal: "rows", label: "Rows", candidate: 130, published: 100,
    policy: policy(), months: 2}), null);
  assert.equal(compareNumber({signal: "rows", label: "Rows", candidate: 80, published: 100,
    policy: policy(), months: 1}).severity, "critical");
});
