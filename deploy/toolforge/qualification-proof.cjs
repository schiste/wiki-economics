#!/usr/bin/env node
"use strict";

/*
 * Fail-closed acceptance proof for an isolated wiki qualification.
 *
 * The six stage receipts prove that work was written and measured.  This
 * document joins those receipts to the independent correctness and recovery
 * drills that must pass before a candidate can be considered qualified.  It
 * deliberately validates evidence rather than trying to infer correctness
 * from a few output files: a successful exit without a receipt, checksum, or
 * named observation is not proof.
 */

const fs = require("node:fs");
const path = require("node:path");
const crypto = require("node:crypto");

const SCHEMA_VERSION = 1;
const KIND_EVIDENCE = "wiki-economics-qualification-proof-evidence";
const KIND_PROOF = "wiki-economics-qualification-proof";
const STAGES = ["ingest", "metrics", "lifecycle", "page-week", "patrol", "publish"];
const CHECKS = [
  "source_inventory",
  "snapshot_consistency",
  "invariants",
  "deterministic_same_warehouse",
  "noop_same_snapshot",
  "interruption_resume",
  "patrol",
  "rollback_cleanup",
];
const INVARIANTS = [
  "productive_plus_reverted_equals_total",
  "rates_match_numerators_denominators",
  "rates_finite_and_bounded",
  "patrol_non_negative",
  "patrolled_le_total_revisions",
  "inequality_bounds",
  "cohort_milestones_monotonic",
  "page_week_edit_conservation",
];
const HEX64 = /^[0-9a-f]{64}$/i;
const SNAPSHOT = /^\d{4}-\d{2}$/;

function isRecord(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function fail(label, message) {
  throw new Error(`${label}: ${message}`);
}

function readJson(file, label = file) {
  try {
    return JSON.parse(fs.readFileSync(file, "utf8"));
  } catch (error) {
    fail(label, `cannot read JSON (${error.message})`);
  }
}

function sha256(value) {
  return crypto.createHash("sha256").update(value).digest("hex");
}

function canonicalize(value) {
  if (Array.isArray(value)) return value.map(canonicalize);
  if (!isRecord(value)) return value;
  return Object.fromEntries(Object.keys(value).sort().map((key) => [key, canonicalize(value[key])]));
}

function canonicalJson(value) {
  return JSON.stringify(canonicalize(value));
}

function atomicWrite(file, value) {
  fs.mkdirSync(path.dirname(file), {recursive: true});
  const temporary = `${file}.tmp.${process.pid}.${Date.now()}`;
  try {
    fs.writeFileSync(temporary, `${JSON.stringify(value, null, 2)}\n`, {mode: 0o600});
    fs.renameSync(temporary, file);
  } catch (error) {
    try { fs.unlinkSync(temporary); } catch {}
    throw error;
  }
}

function exactOrderedArray(value, expected, label) {
  if (!Array.isArray(value) || value.length !== expected.length
      || value.some((entry, index) => entry !== expected[index])) {
    fail(label, `must be exactly [${expected.join(", ")}] in order`);
  }
}

function nonEmptyString(value, label) {
  if (typeof value !== "string" || value.trim() === "") fail(label, "must be a non-empty string");
  return value;
}

function safeNonNegativeInteger(value, label, {positive = false} = {}) {
  if (!Number.isSafeInteger(value) || value < 0 || (positive && value === 0)) {
    fail(label, `must be a ${positive ? "positive" : "non-negative"} safe integer`);
  }
  return value;
}

function boolean(value, label) {
  if (typeof value !== "boolean") fail(label, "must be boolean");
  return value;
}

function validateHash(value, label) {
  if (typeof value !== "string" || !HEX64.test(value)) fail(label, "must be a SHA-256 hex digest");
  return value.toLowerCase();
}

function validateSnapshot(value, label) {
  if (typeof value !== "string" || !SNAPSHOT.test(value)) fail(label, "must use YYYY-MM format");
  return value;
}

function validatePolicy(policy, label = "qualification proof policy") {
  if (!isRecord(policy) || policy.schema_version !== SCHEMA_VERSION
      || typeof policy.policy_version !== "string" || policy.policy_version.trim() === "") {
    fail(label, "has an unsupported schema or policy version");
  }
  nonEmptyString(policy.wiki, `${label}.wiki`);
  exactOrderedArray(policy.required_stages, STAGES, `${label}.required_stages`);
  exactOrderedArray(policy.required_checks, CHECKS, `${label}.required_checks`);
  if (policy.publication_eligible !== false) fail(label, "must keep publication_eligible=false");
  if (policy.publication !== "hidden" || policy.refresh !== "qualification") {
    fail(label, "must require publication=hidden and refresh=qualification");
  }
  return policy;
}

function validateEvidenceReference(entry, label) {
  if (!isRecord(entry)) fail(label, "must be an object");
  nonEmptyString(entry.kind, `${label}.kind`);
  const reference = nonEmptyString(entry.ref, `${label}.ref`);
  if (path.isAbsolute(reference) || reference.split(/[\\/]+/).includes("..")) {
    fail(`${label}.ref`, "must be a relative, non-traversing evidence reference");
  }
  validateHash(entry.sha256, `${label}.sha256`);
  if (entry.observed_at != null) nonEmptyString(entry.observed_at, `${label}.observed_at`);
}

function validateEvidenceList(value, label) {
  if (!Array.isArray(value) || value.length === 0) fail(label, "must contain at least one hashed evidence reference");
  value.forEach((entry, index) => validateEvidenceReference(entry, `${label}[${index}]`));
}

function validatePublication(document, policy, label) {
  if (!isRecord(document.publication)) fail(label, "publication contract is missing");
  if (document.publication.state !== policy.publication
      || document.publication.refresh !== policy.refresh
      || document.publication.publication_eligible !== policy.publication_eligible) {
    fail(label, "candidate must remain hidden, qualification-only, and publication-ineligible");
  }
}

function validateStages(document, policy, label) {
  if (!Array.isArray(document.stages)) fail(label, "stages are missing");
  exactOrderedArray(document.stages.map((stage) => stage?.stage), policy.required_stages, `${label}.stages`);
  document.stages.forEach((stage, index) => {
    const prefix = `${label}.stages[${index}]`;
    if (!isRecord(stage) || stage.status !== "passed") fail(prefix, "must have status=passed");
    validateSnapshot(stage.snapshot, `${prefix}.snapshot`);
    if (stage.snapshot !== document.snapshot) fail(prefix, "snapshot does not match the proof target");
    nonEmptyString(stage.run_id, `${prefix}.run_id`);
    if (!isRecord(stage.receipt)) fail(prefix, "stage receipt is missing");
    if (stage.receipt.status !== "succeeded") fail(prefix, "receipt status must be succeeded");
    validateSnapshot(stage.receipt.snapshot, `${prefix}.receipt.snapshot`);
    if (stage.receipt.snapshot !== document.snapshot) fail(prefix, "receipt snapshot does not match the proof target");
    nonEmptyString(stage.receipt.path, `${prefix}.receipt.path`);
    validateHash(stage.receipt.sha256, `${prefix}.receipt.sha256`);
  });
  const runIds = document.stages.map((stage) => stage.run_id);
  if (new Set(runIds).size !== runIds.length) fail(label, "stage run IDs must be unique");
}

function validateSourceInventory(check, label) {
  validateHash(check.source_plan_sha256, `${label}.source_plan_sha256`);
  validateHash(check.remote_inventory_sha256, `${label}.remote_inventory_sha256`);
  const expected = safeNonNegativeInteger(check.expected_source_count, `${label}.expected_source_count`, {positive: true});
  const actual = safeNonNegativeInteger(check.source_count, `${label}.source_count`, {positive: true});
  if (actual !== expected) fail(label, "source count does not match the expected inventory");
  if (!Array.isArray(check.missing) || check.missing.length !== 0) fail(label, "missing source objects are not allowed");
  if (!Array.isArray(check.duplicates) || check.duplicates.length !== 0) fail(label, "duplicate source objects are not allowed");
  boolean(check.mixed_snapshots, `${label}.mixed_snapshots`);
  if (check.mixed_snapshots) fail(label, "mixed snapshots are not allowed");
  boolean(check.history_logging_coverage_match, `${label}.history_logging_coverage_match`);
  if (!check.history_logging_coverage_match) fail(label, "history and logging coverage must match");
}

function validateSnapshotConsistency(check, document, label) {
  validateSnapshot(check.source_plan_snapshot, `${label}.source_plan_snapshot`);
  if (check.source_plan_snapshot !== document.snapshot) fail(label, "source plan snapshot differs from target");
  if (!Array.isArray(check.snapshots) || check.snapshots.length === 0) fail(label, "observed snapshots are missing");
  check.snapshots.forEach((snapshot, index) => {
    validateSnapshot(snapshot, `${label}.snapshots[${index}]`);
    if (snapshot !== document.snapshot) fail(label, "an observed input has a different snapshot");
  });
  if (!Array.isArray(check.stage_snapshots)) fail(label, "stage snapshots are missing");
  exactOrderedArray(check.stage_snapshots, STAGES.map(() => document.snapshot), `${label}.stage_snapshots`);
  boolean(check.mixed_snapshots, `${label}.mixed_snapshots`);
  if (check.mixed_snapshots) fail(label, "mixed snapshots are not allowed");
}

function validateInvariants(check, label) {
  if (!isRecord(check.assertions)) fail(label, "invariant assertions are missing");
  INVARIANTS.forEach((name) => {
    boolean(check.assertions[name], `${label}.assertions.${name}`);
    if (!check.assertions[name]) fail(label, `invariant failed: ${name}`);
  });
  if (check.failure_count != null) safeNonNegativeInteger(check.failure_count, `${label}.failure_count`);
  if (check.failure_count !== 0) fail(label, "invariant failure_count must be zero");
}

function validateDeterminism(check, document, label) {
  ["same_warehouse", "same_snapshot", "byte_identical"].forEach((name) => {
    boolean(check[name], `${label}.${name}`);
    if (!check[name]) fail(label, `${name} must be true`);
  });
  safeNonNegativeInteger(check.artifact_count, `${label}.artifact_count`, {positive: true});
  nonEmptyString(check.baseline_run_id, `${label}.baseline_run_id`);
  nonEmptyString(check.candidate_run_id, `${label}.candidate_run_id`);
  if (check.baseline_run_id === check.candidate_run_id) fail(label, "baseline and candidate runs must be distinct");
  validateSnapshot(check.baseline_snapshot, `${label}.baseline_snapshot`);
  validateSnapshot(check.candidate_snapshot, `${label}.candidate_snapshot`);
  if (check.baseline_snapshot !== document.snapshot || check.candidate_snapshot !== document.snapshot) {
    fail(label, "determinism runs must use the proof snapshot");
  }
  if (!Array.isArray(check.differing_artifacts) || check.differing_artifacts.length !== 0) {
    fail(label, "differing artifacts are not allowed");
  }
}

function validateNoOp(check, document, label) {
  boolean(check.no_op, `${label}.no_op`);
  boolean(check.work_performed, `${label}.work_performed`);
  if (!check.no_op || check.work_performed) fail(label, "same-snapshot rerun must be a complete no-op");
  validateSnapshot(check.snapshot, `${label}.snapshot`);
  if (check.snapshot !== document.snapshot) fail(label, "no-op snapshot differs from proof target");
  if (check.reused_stage_count !== STAGES.length) fail(label, `must reuse all ${STAGES.length} stages`);
  if (!Array.isArray(check.stage_results)) fail(label, "no-op stage results are missing");
  exactOrderedArray(check.stage_results.map((stage) => stage?.stage), STAGES, `${label}.stage_results`);
  check.stage_results.forEach((stage, index) => {
    if (!isRecord(stage) || stage.status !== "no_op") fail(`${label}.stage_results[${index}]`, "must have status=no_op");
  });
}

function validateInterruptionResume(check, label) {
  if (!isRecord(check.stages)) fail(label, "per-stage interruption evidence is missing");
  const names = Object.keys(check.stages);
  exactOrderedArray(names, STAGES, `${label}.stages`);
  STAGES.forEach((stage) => {
    const entry = check.stages[stage];
    const prefix = `${label}.stages.${stage}`;
    if (!isRecord(entry)) fail(prefix, "must be an object");
    boolean(entry.interrupted, `${prefix}.interrupted`);
    boolean(entry.resumed, `${prefix}.resumed`);
    if (!entry.interrupted || !entry.resumed) fail(prefix, "must exercise interruption and resume");
    if (entry.recovery_status !== "passed") fail(prefix, "recovery_status must be passed");
    boolean(entry.publication_mutated, `${prefix}.publication_mutated`);
    if (entry.publication_mutated) fail(prefix, "interruption must not mutate publication");
    if (!Array.isArray(entry.orphaned_paths) || entry.orphaned_paths.length !== 0) fail(prefix, "orphaned paths remain");
    nonEmptyString(entry.resume_run_id, `${prefix}.resume_run_id`);
  });
}

function validatePatrol(check, label) {
  const applicability = check.applicability;
  if (!["applicable", "not_applicable", "unknown"].includes(applicability)) {
    fail(label, "applicability must be applicable, not_applicable, or unknown");
  }
  boolean(check.plausibility_passed, `${label}.plausibility_passed`);
  boolean(check.non_negative, `${label}.non_negative`);
  if (!check.plausibility_passed || !check.non_negative) fail(label, "patrol plausibility checks failed");
  if (applicability === "applicable") {
    safeNonNegativeInteger(check.patrol_rows, `${label}.patrol_rows`);
    boolean(check.patrolled_le_revisions, `${label}.patrolled_le_revisions`);
    if (!check.patrolled_le_revisions) fail(label, "patrolled revisions cannot exceed revisions");
  } else {
    if (check.patrol_rows != null) fail(label, "not-applicable patrol must not publish a count");
    nonEmptyString(check.applicability_reason, `${label}.applicability_reason`);
    boolean(check.headline_ratios_null, `${label}.headline_ratios_null`);
    boolean(check.agents_blocked_from_comparison, `${label}.agents_blocked_from_comparison`);
    if (!check.headline_ratios_null || !check.agents_blocked_from_comparison) {
      fail(label, "not-applicable patrol must null ratios and block comparisons");
    }
  }
}

function validateRollbackCleanup(check, label) {
  ["rollback_verified", "restored_previous_identity", "cleanup_verified", "publication_mutated"].forEach((name) => {
    boolean(check[name], `${label}.${name}`);
  });
  if (!check.rollback_verified || !check.restored_previous_identity || !check.cleanup_verified) {
    fail(label, "rollback and cleanup must be verified");
  }
  if (check.publication_mutated) fail(label, "rollback drill must not leave publication mutated");
  if (check.recovery_audit_status !== "passed") fail(label, "recovery_audit_status must be passed");
  if (!Array.isArray(check.orphaned_paths) || check.orphaned_paths.length !== 0) fail(label, "orphaned paths remain");
}

function validateChecks(document, policy, label) {
  if (!Array.isArray(document.checks)) fail(label, "checks are missing");
  exactOrderedArray(document.checks.map((check) => check?.check), policy.required_checks, `${label}.checks`);
  document.checks.forEach((check, index) => {
    const prefix = `${label}.checks[${index}]`;
    if (!isRecord(check) || check.status !== "passed") fail(prefix, "must have status=passed");
    validateEvidenceList(check.evidence, `${prefix}.evidence`);
    switch (check.check) {
      case "source_inventory": validateSourceInventory(check, prefix); break;
      case "snapshot_consistency": validateSnapshotConsistency(check, document, prefix); break;
      case "invariants": validateInvariants(check, prefix); break;
      case "deterministic_same_warehouse": validateDeterminism(check, document, prefix); break;
      case "noop_same_snapshot": validateNoOp(check, document, prefix); break;
      case "interruption_resume": validateInterruptionResume(check, prefix); break;
      case "patrol": validatePatrol(check, prefix); break;
      case "rollback_cleanup": validateRollbackCleanup(check, prefix); break;
      default: fail(prefix, `unsupported check ${check.check}`);
    }
  });
}

function validateProof(document, policy, label = "qualification proof evidence") {
  validatePolicy(policy);
  if (!isRecord(document) || document.schema_version !== SCHEMA_VERSION
      || ![KIND_EVIDENCE, KIND_PROOF].includes(document.kind)) {
    fail(label, "has an unsupported schema or kind");
  }
  if (document.policy_version !== policy.policy_version) fail(label, "policy version does not match");
  if (document.wiki !== policy.wiki) fail(label, "wiki does not match the qualification policy");
  validateSnapshot(document.snapshot, `${label}.snapshot`);
  validatePublication(document, policy, label);
  validateStages(document, policy, label);
  validateChecks(document, policy, label);
  if (!Array.isArray(document.failures) || document.failures.length !== 0) fail(label, "failures must be an empty array");
  if (document.warnings != null && !Array.isArray(document.warnings)) fail(label, "warnings must be an array");
  if (document.kind === KIND_PROOF) {
    if (document.status !== "passed" || document.qualified !== true) fail(label, "proof must be marked qualified and passed");
    nonEmptyString(document.generated_at, `${label}.generated_at`);
    validateHash(document.proof_sha256, `${label}.proof_sha256`);
    const copy = {...document, proof_sha256: null};
    if (sha256(canonicalJson(copy)) !== document.proof_sha256) fail(label, "proof_sha256 does not authenticate the proof");
  }
  return document;
}

function buildProof(evidence, policy, generatedAt = new Date().toISOString()) {
  validateProof(evidence, policy);
  const proof = JSON.parse(JSON.stringify(evidence));
  proof.kind = KIND_PROOF;
  proof.status = "passed";
  proof.qualified = true;
  proof.generated_at = generatedAt;
  proof.proof_sha256 = null;
  proof.proof_sha256 = sha256(canonicalJson(proof));
  return proof;
}

function parseArgs(argv) {
  const args = {policy: path.join(__dirname, "..", "..", "config", "qualification-proof.json")};
  for (let index = 0; index < argv.length; index += 1) {
    const option = argv[index];
    if (!["--policy", "--evidence", "--output"].includes(option) || index + 1 >= argv.length) {
      fail("arguments", `usage: ${path.basename(process.argv[1])} --evidence FILE --output FILE [--policy FILE]`);
    }
    args[option.slice(2)] = argv[++index];
  }
  ["evidence", "output"].forEach((name) => nonEmptyString(args[name], `--${name}`));
  return args;
}

function main(argv = process.argv.slice(2)) {
  const args = parseArgs(argv);
  const policy = validatePolicy(readJson(args.policy, args.policy), args.policy);
  const evidence = readJson(args.evidence, args.evidence);
  const proof = buildProof(evidence, policy);
  atomicWrite(path.resolve(args.output), proof);
  process.stdout.write(`${JSON.stringify({status: proof.status, qualified: proof.qualified, wiki: proof.wiki, snapshot: proof.snapshot, proof_sha256: proof.proof_sha256, output: path.resolve(args.output)})}\n`);
  return proof;
}

if (require.main === module) {
  try {
    main();
  } catch (error) {
    process.stderr.write(`QUALIFICATION PROOF FAILED: ${error.message}\n`);
    process.exitCode = 1;
  }
}

module.exports = {
  CHECKS,
  INVARIANTS,
  KIND_EVIDENCE,
  KIND_PROOF,
  SCHEMA_VERSION,
  STAGES,
  buildProof,
  canonicalJson,
  main,
  validatePolicy,
  validateProof,
};
