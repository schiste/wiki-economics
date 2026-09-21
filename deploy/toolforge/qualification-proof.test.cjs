"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const {spawnSync} = require("node:child_process");
const {test} = require("node:test");

const {
  CHECKS,
  INVARIANTS,
  KIND_EVIDENCE,
  STAGES,
  buildProof,
  canonicalJson,
  validatePolicy,
  validateProof,
} = require("./qualification-proof.cjs");

const policy = JSON.parse(fs.readFileSync(path.join(__dirname, "..", "..", "config", "qualification-proof.json"), "utf8"));
const script = path.join(__dirname, "qualification-proof.cjs");
const digest = (letter) => letter.repeat(64);

function evidenceRef(kind) {
  return [{kind, ref: `evidence/${kind}.json`, sha256: digest("a")}];
}

function validEvidence() {
  const stageReceipts = STAGES.map((stage, index) => ({
    stage,
    status: "passed",
    snapshot: "2026-08",
    run_id: `enwiki-${stage}-${index + 1}`,
    receipt: {
      status: "succeeded",
      snapshot: "2026-08",
      path: `output/_qualification/enwiki/${stage}.json`,
      sha256: digest("b"),
    },
  }));
  return {
    schema_version: 1,
    kind: KIND_EVIDENCE,
    policy_version: policy.policy_version,
    wiki: "enwiki",
    snapshot: "2026-08",
    publication: {state: "hidden", refresh: "qualification", publication_eligible: false},
    stages: stageReceipts,
    checks: [
      {
        check: "source_inventory",
        status: "passed",
        evidence: evidenceRef("source-inventory"),
        source_plan_sha256: digest("c"),
        remote_inventory_sha256: digest("d"),
        expected_source_count: 309,
        source_count: 309,
        missing: [],
        duplicates: [],
        mixed_snapshots: false,
        history_logging_coverage_match: true,
      },
      {
        check: "snapshot_consistency",
        status: "passed",
        evidence: evidenceRef("snapshot-consistency"),
        source_plan_snapshot: "2026-08",
        snapshots: ["2026-08"],
        stage_snapshots: STAGES.map(() => "2026-08"),
        mixed_snapshots: false,
      },
      {
        check: "invariants",
        status: "passed",
        evidence: evidenceRef("invariants"),
        assertions: Object.fromEntries(INVARIANTS.map((name) => [name, true])),
        failure_count: 0,
      },
      {
        check: "deterministic_same_warehouse",
        status: "passed",
        evidence: evidenceRef("determinism"),
        same_warehouse: true,
        same_snapshot: true,
        byte_identical: true,
        artifact_count: 42,
        baseline_run_id: "enwiki-determinism-baseline",
        candidate_run_id: "enwiki-determinism-candidate",
        baseline_snapshot: "2026-08",
        candidate_snapshot: "2026-08",
        differing_artifacts: [],
      },
      {
        check: "noop_same_snapshot",
        status: "passed",
        evidence: evidenceRef("same-snapshot-noop"),
        no_op: true,
        work_performed: false,
        snapshot: "2026-08",
        reused_stage_count: STAGES.length,
        stage_results: STAGES.map((stage) => ({stage, status: "no_op"})),
      },
      {
        check: "interruption_resume",
        status: "passed",
        evidence: evidenceRef("interruption-resume"),
        stages: Object.fromEntries(STAGES.map((stage, index) => [stage, {
          interrupted: true,
          resumed: true,
          recovery_status: "passed",
          publication_mutated: false,
          orphaned_paths: [],
          resume_run_id: `enwiki-resume-${index + 1}`,
        }])),
      },
      {
        check: "patrol",
        status: "passed",
        evidence: evidenceRef("patrol"),
        applicability: "not_applicable",
        plausibility_passed: true,
        non_negative: true,
        patrol_rows: null,
        applicability_reason: "FlaggedRevs is the applicable moderation mechanism for this wiki.",
        headline_ratios_null: true,
        agents_blocked_from_comparison: true,
      },
      {
        check: "rollback_cleanup",
        status: "passed",
        evidence: evidenceRef("rollback-cleanup"),
        rollback_verified: true,
        restored_previous_identity: true,
        cleanup_verified: true,
        recovery_audit_status: "passed",
        publication_mutated: false,
        orphaned_paths: [],
      },
    ],
    failures: [],
    warnings: [],
  };
}

function copyEvidence() {
  return JSON.parse(JSON.stringify(validEvidence()));
}

test("valid evidence produces an authenticated qualified proof", () => {
  const evidence = validEvidence();
  validatePolicy(policy);
  assert.equal(validateProof(evidence, policy), evidence);
  const proof = buildProof(evidence, policy, "2026-09-21T10:00:00.000Z");
  assert.equal(proof.qualified, true);
  assert.equal(proof.status, "passed");
  assert.match(proof.proof_sha256, /^[0-9a-f]{64}$/);
  assert.doesNotThrow(() => validateProof(proof, policy));
  assert.equal(proof.proof_sha256, require("node:crypto").createHash("sha256").update(canonicalJson({...proof, proof_sha256: null})).digest("hex"));
});

test("CLI writes the proof atomically and reports its identity", () => {
  const fixture = fs.mkdtempSync(path.join(os.tmpdir(), "wiki-econ-proof-"));
  try {
    const evidencePath = path.join(fixture, "evidence.json");
    const outputPath = path.join(fixture, "proof.json");
    fs.writeFileSync(evidencePath, `${JSON.stringify(validEvidence())}\n`);
    const result = spawnSync(process.execPath, [script, "--policy", path.join(__dirname, "..", "..", "config", "qualification-proof.json"), "--evidence", evidencePath, "--output", outputPath], {encoding: "utf8"});
    assert.equal(result.status, 0, result.stderr);
    const proof = JSON.parse(fs.readFileSync(outputPath, "utf8"));
    assert.equal(proof.kind, "wiki-economics-qualification-proof");
    assert.equal(JSON.parse(result.stdout).proof_sha256, proof.proof_sha256);
    assert.deepEqual(fs.readdirSync(fixture).sort(), ["evidence.json", "proof.json"]);
  } finally {
    fs.rmSync(fixture, {recursive: true, force: true});
  }
});

const rejectionCases = [
  ["rejects missing sources", (proof) => { proof.checks[0].missing = ["enwiki-2026-02.xml.bz2"]; }, /missing source objects/],
  ["rejects duplicate sources", (proof) => { proof.checks[0].duplicates = ["enwiki-2026-02.xml.bz2"]; }, /duplicate source objects/],
  ["rejects mixed snapshots", (proof) => { proof.checks[1].stage_snapshots[3] = "2026-07"; }, /stage_snapshots/],
  ["rejects failed invariants", (proof) => { proof.checks[2].assertions.page_week_edit_conservation = false; }, /invariant failed/],
  ["rejects nondeterministic output", (proof) => { proof.checks[3].differing_artifacts = ["output.parquet"]; }, /differing artifacts/],
  ["rejects incomplete same-snapshot no-op", (proof) => { proof.checks[4].stage_results.pop(); }, /stage_results/],
  ["rejects an interruption without resume", (proof) => { proof.checks[5].stages.metrics.resumed = false; }, /interruption and resume/],
  ["rejects misleading patrol non-applicability", (proof) => { proof.checks[6].headline_ratios_null = false; }, /null ratios/],
  ["rejects failed rollback cleanup", (proof) => { proof.checks[7].cleanup_verified = false; }, /rollback and cleanup/],
  ["rejects publication eligibility", (proof) => { proof.publication.publication_eligible = true; }, /publication-ineligible/],
  ["rejects asserted-only checks", (proof) => { proof.checks[0].evidence = []; }, /hashed evidence reference/],
  ["rejects a receipt for another snapshot", (proof) => { proof.stages[0].receipt.snapshot = "2026-07"; }, /receipt snapshot/],
];

for (const [name, mutate, expected] of rejectionCases) {
  test(name, () => {
    const evidence = copyEvidence();
    mutate(evidence);
    assert.throws(() => validateProof(evidence, policy), expected);
  });
}

test("proof tampering is detected by the embedded digest", () => {
  const proof = buildProof(validEvidence(), policy, "2026-09-21T10:00:00.000Z");
  proof.checks[2].failure_count = 1;
  assert.throws(() => validateProof(proof, policy), /failure_count must be zero/);
  const clean = buildProof(validEvidence(), policy, "2026-09-21T10:00:00.000Z");
  clean.warnings.push("operator review required");
  assert.throws(() => validateProof(clean, policy), /proof_sha256 does not authenticate/);
});
