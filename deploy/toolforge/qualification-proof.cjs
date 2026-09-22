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
  "two_successful_runs",
  "rollover_safety",
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
  if (!Number.isSafeInteger(policy.minimum_successful_runs) || policy.minimum_successful_runs < 2) {
    fail(label, "minimum_successful_runs must be at least two");
  }
  exactOrderedArray(policy.required_run_kinds, ["initial_candidate", "rollover"], `${label}.required_run_kinds`);
  exactOrderedArray(policy.required_stage_receipts, STAGES, `${label}.required_stage_receipts`);
  exactOrderedArray(policy.required_receipt_kinds, ["capacity", "run"], `${label}.required_receipt_kinds`);
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

function validateRunStageReceipt(entry, expectedSnapshot, label) {
  if (!isRecord(entry)) fail(label, "stage receipt entry is missing");
  nonEmptyString(entry.stage, `${label}.stage`);
  if (entry.status !== "succeeded") fail(label, "stage receipt status must be succeeded");
  validateSnapshot(entry.snapshot, `${label}.snapshot`);
  if (entry.snapshot !== expectedSnapshot) fail(label, "stage receipt snapshot does not match its run");
  if (!isRecord(entry.receipt)) fail(label, "stage receipt reference is missing");
  if (entry.receipt.kind !== "wiki-economics-qualification-stage-receipt") {
    fail(label, "stage receipt has an unsupported kind");
  }
  nonEmptyString(entry.receipt.ref, `${label}.receipt.ref`);
  validateHash(entry.receipt.sha256, `${label}.receipt.sha256`);
}

function validateSuccessfulRuns(check, document, policy, label) {
  if (!Array.isArray(check.successful_runs)
      || check.successful_runs.length < policy.minimum_successful_runs) {
    fail(label, `must contain at least ${policy.minimum_successful_runs} successful runs`);
  }
  if (!Number.isSafeInteger(check.successful_runs_count)
      || check.successful_runs_count !== check.successful_runs.length) {
    fail(label, "successful_runs_count must match the recorded runs");
  }
  const seenRunIds = new Set();
  const seenKinds = new Set();
  check.successful_runs.forEach((run, index) => {
    const prefix = `${label}.successful_runs[${index}]`;
    if (!isRecord(run)) fail(prefix, "run is missing");
    nonEmptyString(run.run_id, `${prefix}.run_id`);
    if (seenRunIds.has(run.run_id)) fail(prefix, "run IDs must be distinct");
    seenRunIds.add(run.run_id);
    nonEmptyString(run.kind, `${prefix}.kind`);
    if (!policy.required_run_kinds.includes(run.kind)) fail(prefix, "run kind is not required by policy");
    seenKinds.add(run.kind);
    if (run.status !== "passed") fail(prefix, "run status must be passed");
    boolean(run.publication_eligible, `${prefix}.publication_eligible`);
    if (run.publication_eligible) fail(prefix, "qualification runs must remain publication-ineligible");
    boolean(run.receipt_contract_passed, `${prefix}.receipt_contract_passed`);
    if (!run.receipt_contract_passed) fail(prefix, "receipt contract did not pass");
    validateSnapshot(run.snapshot, `${prefix}.snapshot`);
    let expectedSnapshot = run.snapshot;
    if (run.kind === "initial_candidate") {
      if (run.snapshot !== document.snapshot) fail(prefix, "initial candidate must use the proof snapshot");
    }
    if (run.kind === "rollover") {
      validateSnapshot(run.baseline_snapshot, `${prefix}.baseline_snapshot`);
      validateSnapshot(run.candidate_snapshot, `${prefix}.candidate_snapshot`);
      if (run.baseline_snapshot !== document.snapshot || run.baseline_snapshot >= run.candidate_snapshot) {
        fail(prefix, "rollover must advance from the proof snapshot");
      }
      if (run.snapshot !== run.candidate_snapshot) fail(prefix, "rollover run snapshot must be its candidate snapshot");
      expectedSnapshot = run.candidate_snapshot;
    }
    if (!Array.isArray(run.stage_receipts)) fail(prefix, "stage receipts are missing");
    exactOrderedArray(run.stage_receipts.map((receipt) => receipt?.stage), policy.required_stage_receipts,
      `${prefix}.stage_receipts`);
    run.stage_receipts.forEach((receipt, receiptIndex) => {
      validateRunStageReceipt(receipt, expectedSnapshot, `${prefix}.stage_receipts[${receiptIndex}]`);
    });
    if (!Array.isArray(run.receipts)) fail(prefix, "run receipt bundle is missing");
    const receiptKinds = new Set();
    run.receipts.forEach((receipt, receiptIndex) => {
      const receiptLabel = `${prefix}.receipts[${receiptIndex}]`;
      validateEvidenceReference(receipt, receiptLabel);
      if (receiptKinds.has(receipt.kind)) fail(receiptLabel, "receipt kinds must be unique");
      receiptKinds.add(receipt.kind);
    });
    policy.required_receipt_kinds.forEach((kind) => {
      if (!receiptKinds.has(kind)) fail(prefix, `missing required ${kind} receipt`);
    });
    for (const field of ["warnings", "retries", "recovery_events"]) {
      if (!Array.isArray(run[field])) fail(prefix, `${field} must be recorded in the run receipt`);
    }
  });
  policy.required_run_kinds.forEach((kind) => {
    if (!seenKinds.has(kind)) fail(label, `missing successful run kind: ${kind}`);
  });
}

function validateGeneration(check, expectedSnapshot, label, {candidate = false} = {}) {
  if (!isRecord(check)) fail(label, "generation evidence is missing");
  validateSnapshot(check.snapshot, `${label}.snapshot`);
  if (check.snapshot !== expectedSnapshot) fail(label, "generation snapshot does not match the rollover pair");
  boolean(check.complete, `${label}.complete`);
  boolean(check.validated, `${label}.validated`);
  if (!check.complete || !check.validated) fail(label, "generation must be complete and validated");
  if (!candidate) {
    boolean(check.available_before, `${label}.available_before`);
    boolean(check.retained_during_rollover, `${label}.retained_during_rollover`);
    boolean(check.available_after, `${label}.available_after`);
    if (!check.available_before || !check.retained_during_rollover || !check.available_after) {
      fail(label, "the preceding generation must remain available throughout rollover");
    }
  } else {
    boolean(check.available_after, `${label}.available_after`);
    if (!check.available_after) fail(label, "the completed candidate generation is not available");
  }
  nonEmptyString(check.manifest_path, `${label}.manifest_path`);
  validateHash(check.manifest_sha256, `${label}.manifest_sha256`);
}

function validateRolloverSafety(check, document, label) {
  validateSnapshot(check.baseline_snapshot, `${label}.baseline_snapshot`);
  validateSnapshot(check.candidate_snapshot, `${label}.candidate_snapshot`);
  if (check.baseline_snapshot >= check.candidate_snapshot) {
    fail(label, "rollover must advance from the older snapshot to the newer snapshot");
  }
  if (check.baseline_snapshot !== document.snapshot) {
    fail(label, "rollover must retain the proof target as its preceding generation");
  }
  nonEmptyString(check.baseline_run_id, `${label}.baseline_run_id`);
  nonEmptyString(check.candidate_run_id, `${label}.candidate_run_id`);
  if (check.baseline_run_id === check.candidate_run_id) fail(label, "rollover runs must be distinct");
  validateGeneration(check.baseline_generation, check.baseline_snapshot, `${label}.baseline_generation`);
  validateGeneration(check.candidate_generation, check.candidate_snapshot, `${label}.candidate_generation`, {candidate: true});
  validateSnapshot(check.snapshot_pointer_before, `${label}.snapshot_pointer_before`);
  validateSnapshot(check.snapshot_pointer_after, `${label}.snapshot_pointer_after`);
  if (check.snapshot_pointer_before !== check.baseline_snapshot
      || check.snapshot_pointer_after !== check.candidate_snapshot) {
    fail(label, "snapshot pointer did not advance exactly across the rollover pair");
  }
  validateSnapshot(check.publication_snapshot_before, `${label}.publication_snapshot_before`);
  if (check.publication_snapshot_before !== check.baseline_snapshot) {
    fail(label, "rollover must begin with the preceding generation published");
  }
  boolean(check.publication_unchanged_during_rollover, `${label}.publication_unchanged_during_rollover`);
  if (!check.publication_unchanged_during_rollover) fail(label, "publication changed during candidate construction");
  if (!Array.isArray(check.observed_generation_snapshots)) fail(label, "observed generation snapshots are missing");
  exactOrderedArray(
    check.observed_generation_snapshots,
    [check.baseline_snapshot, check.candidate_snapshot],
    `${label}.observed_generation_snapshots`,
  );
  boolean(check.no_mixed_generations, `${label}.no_mixed_generations`);
  if (!check.no_mixed_generations) fail(label, "generation inputs or outputs were mixed");
  for (const name of ["mixed_snapshot_paths", "cross_generation_references"]) {
    if (!Array.isArray(check[name]) || check[name].length !== 0) fail(label, `${name} must be empty`);
  }
  boolean(check.cutoff_advanced, `${label}.cutoff_advanced`);
  boolean(check.conservation_passed, `${label}.conservation_passed`);
  boolean(check.previous_generation_available, `${label}.previous_generation_available`);
  if (!check.cutoff_advanced || !check.conservation_passed || !check.previous_generation_available) {
    fail(label, "rollover cutoff, conservation, and preceding-generation retention must pass");
  }
  if (!isRecord(check.storage)) fail(label, "rollover storage measurements are missing");
  const storage = check.storage;
  const capacity = safeNonNegativeInteger(storage.capacity_bytes, `${label}.storage.capacity_bytes`, {positive: true});
  const reserve = safeNonNegativeInteger(storage.required_reserve_bytes, `${label}.storage.required_reserve_bytes`, {positive: true});
  const initial = safeNonNegativeInteger(storage.persistent_initial_bytes, `${label}.storage.persistent_initial_bytes`);
  const highWater = safeNonNegativeInteger(storage.persistent_high_water_bytes, `${label}.storage.persistent_high_water_bytes`);
  const final = safeNonNegativeInteger(storage.persistent_final_bytes, `${label}.storage.persistent_final_bytes`);
  const scratchHighWater = safeNonNegativeInteger(storage.scratch_high_water_bytes, `${label}.storage.scratch_high_water_bytes`);
  const combinedHighWater = safeNonNegativeInteger(storage.combined_high_water_bytes, `${label}.storage.combined_high_water_bytes`);
  const minimumFree = safeNonNegativeInteger(storage.minimum_free_bytes, `${label}.storage.minimum_free_bytes`);
  if (highWater < initial || highWater < final) fail(label, "persistent high-water mark is inconsistent with before/after measurements");
  if (combinedHighWater < highWater || combinedHighWater < scratchHighWater) {
    fail(label, "combined rollover high-water mark is inconsistent with component measurements");
  }
  if (minimumFree < reserve || capacity < combinedHighWater + reserve) {
    fail(label, "rollover peak does not preserve the required storage reserve");
  }
  boolean(storage.within_budget, `${label}.storage.within_budget`);
  if (!storage.within_budget) fail(label, "rollover storage exceeded its budget");
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
      case "two_successful_runs": validateSuccessfulRuns(check, document, policy, prefix); break;
      case "rollover_safety": validateRolloverSafety(check, document, prefix); break;
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
