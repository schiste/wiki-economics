#!/usr/bin/env bash
set -euo pipefail

# Refresh wrapper invoked by the `wiki-econ-refresh` Toolforge Job.
#
# Unlike deploy/cloud-vps/run-refresh.sh, this does NOT retain a release
# history for output/site: Toolforge NFS is shared and retaining multiple full
# generations is expensive relative to the benefit. Metric files are atomically replaced,
# and the site builder atomically switches WIKI_ECON_SITE_DIST_DIR to a clean
# generated sibling before removing the previous site release.
#
# Raw dump cleanup is NOT done here: `wiki-econ run` (invoked by
# scripts/refresh.sh) downloads only a bounded source window and deletes each
# compressed source immediately after its strict ingest marker commits. That's
# safe because
# src/storage.rs::marker_manifest_is_valid verifies the durable source identity
# recorded at ingest plus every warehouse/analytical Parquet footer and row
# count. The raw .bz2 may be removed after that receipt commits, so later runs
# remain idempotent without weakening output validation.

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
# shellcheck disable=SC1091
. "$ROOT/scripts/lib/wiki_econ.sh"

# Publish a live run record + bounded terminal history to the shared NFS
# output dir so the admin webservice — a separate Toolforge pod with no shared
# process memory and no Toolforge/Kubernetes API access — can distinguish a
# running or hung refresh from the preceding success.
REFRESH_STARTED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
REFRESH_START_EPOCH="$(date +%s)"
REFRESH_HISTORY_LIMIT="${WIKI_ECON_REFRESH_HISTORY_LIMIT:-104}"
WIKI_ECON_RUN_ID="${WIKI_ECON_RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)-$$}"
export WIKI_ECON_RUN_ID
export CARGO_TERM_COLOR=never
export NO_COLOR=1
export OBSERVABLE_TELEMETRY_DISABLE=true
export WIKI_ECON_LOG_ANSI=0
# The job requests one CPU, but Toolforge's container cpuset currently exposes
# eight host CPUs. Match data-parallel pools to the real quota by default.
export RAYON_NUM_THREADS="${RAYON_NUM_THREADS:-1}"
export POLARS_MAX_THREADS="${POLARS_MAX_THREADS:-1}"
# Fail before a source transaction or logical partition can consume the
# reserves required to finish already-admitted work. These defaults describe
# the current maximum 6 GiB Toolforge job. If Toolforge grants a different
# per-job ceiling later, change these values only with a matching qualification
# receipt; the pipeline must not assume an unavailable 16 GiB profile.
export WIKI_ECON_MEMORY_CEILING_BYTES="${WIKI_ECON_MEMORY_CEILING_BYTES:-6442450944}"
export WIKI_ECON_MEMORY_RESERVE_BYTES="${WIKI_ECON_MEMORY_RESERVE_BYTES:-1610612736}"
export WIKI_ECON_PERSISTENT_STORAGE_RESERVE_BYTES="${WIKI_ECON_PERSISTENT_STORAGE_RESERVE_BYTES:-10737418240}"
export WIKI_ECON_BOUNDED_SCRATCH_RESERVE_BYTES="${WIKI_ECON_BOUNDED_SCRATCH_RESERVE_BYTES:-8589934592}"
export WIKI_ECON_ROLLBACK_GENERATION_RESERVE_BYTES="${WIKI_ECON_ROLLBACK_GENERATION_RESERVE_BYTES:-8589934592}"
export WIKI_ECON_SCRATCH_LIMIT_BYTES="${WIKI_ECON_SCRATCH_LIMIT_BYTES:-68719476736}"
export WIKI_ECON_MAX_OPEN_FILES="${WIKI_ECON_MAX_OPEN_FILES:-512}"
export WIKI_ECON_SOURCE_WORKERS="${WIKI_ECON_SOURCE_WORKERS:-1}"
export WIKI_ECON_REQUIRE_QUALIFIED_PROFILE=1
export WIKI_ECON_THREAD_LIMIT="${WIKI_ECON_THREAD_LIMIT:-1}"
export WIKI_ECON_MAX_LOGICAL_PARTITION_BYTES="${WIKI_ECON_MAX_LOGICAL_PARTITION_BYTES:-8589934592}"
export WIKI_ECON_MAX_ACTIVE_PARQUET_WRITERS="${WIKI_ECON_MAX_ACTIVE_PARQUET_WRITERS:-16}"
# Keep raw history storage bounded. Operators may raise this to 2–4 after
# checking NFS headroom; one source is the fail-safe Toolforge default.
WIKI_ECON_SOURCE_WINDOW_SIZE="${WIKI_ECON_SOURCE_WINDOW_SIZE:-1}"
if [[ ! "$WIKI_ECON_SOURCE_WINDOW_SIZE" =~ ^[1-4]$ ]]; then
  echo "Toolforge refresh requires WIKI_ECON_SOURCE_WINDOW_SIZE between 1 and 4 (got: $WIKI_ECON_SOURCE_WINDOW_SIZE)" >&2
  exit 2
fi
export WIKI_ECON_SOURCE_WINDOW_SIZE
# Rust owns the weekly layout and rejects configurations absent from the
# checked-in capacity qualification registry before expensive work starts.
# Which portion of the pipeline to run. `all` (the weekly scheduled job) runs
# everything; the six named stages are resumable on-demand jobs. `compute` and
# `site` remain compatibility entry points for the older monolithic workflow.
REFRESH_STAGE="${WIKI_ECON_REFRESH_STAGE:-all}"
case "$REFRESH_STAGE" in
  all|ingest|metrics|lifecycle|page-week|patrol|publish|compute|site) ;;
  *)
    echo "Toolforge refresh requires WIKI_ECON_REFRESH_STAGE to be all, ingest, metrics, lifecycle, page-week, patrol, publish, compute, or site (got: $REFRESH_STAGE)" >&2
    exit 2
    ;;
esac
PIPELINE_MODE="${WIKI_ECON_PIPELINE_MODE:-0}"
if [ "$PIPELINE_MODE" != "0" ] && [ "$PIPELINE_MODE" != "1" ]; then
  echo "WIKI_ECON_PIPELINE_MODE must be 0 or 1 (got: $PIPELINE_MODE)" >&2
  exit 2
fi
if [ "$PIPELINE_MODE" = "1" ]; then
  # The isolated qualification jobs are admitted at the existing 4-vCPU
  # Toolforge limit. Keep the receipt explicit about the requested contract;
  # operators can override this only when the job manifest changes with it.
  export WIKI_ECON_REQUESTED_CPU_CORES="${WIKI_ECON_REQUESTED_CPU_CORES:-4}"
fi
PIPELINE_STAGE_ACTIVE=0
PIPELINE_STATE_HELPER="$ROOT/deploy/toolforge/pipeline-state.cjs"
PIPELINE_STATE_FILE="${WIKI_ECON_PIPELINE_STATE_FILE:-}"
PIPELINE_ID=""
QUALIFICATION_RECEIPT_HELPER="$ROOT/deploy/toolforge/qualification-receipt.cjs"
QUALIFICATION_RECEIPT_DIR=""
QUALIFICATION_RECEIPT_ACTIVE=0
QUALIFICATION_RECEIPT_SAMPLER_PID=""
REFRESH_LOCK_HEARTBEAT_SECS="${WIKI_ECON_REFRESH_LOCK_HEARTBEAT_SECS:-60}"
REFRESH_LOCK_STALE_SECS="${WIKI_ECON_REFRESH_LOCK_STALE_SECS:-21600}"
REFRESH_LOCK_RECHECK_SECS="${WIKI_ECON_REFRESH_LOCK_RECHECK_SECS:-2}"
REFRESH_JOB_IDENTITY="${WIKI_ECON_JOB_IDENTITY:-${TOOLFORGE_JOB_NAME:-${JOB_NAME:-${HOSTNAME:-unknown-toolforge-job}}}}"
REFRESH_PROCESS_IDENTITY="${WIKI_ECON_PROCESS_IDENTITY:-${HOSTNAME:-$REFRESH_JOB_IDENTITY}}"
REFRESH_LOCK_TOKEN="${WIKI_ECON_RUN_ID}-$$-${REFRESH_START_EPOCH}"
REFRESH_LOCK_DIR=""
REFRESH_LOCK_HEARTBEAT_PID=""
REFRESH_LOCK_OWNED=0
SELECTED_SNAPSHOT=""
REFRESH_FAILURE_ERROR=""
REFRESH_FAILURE_STAGE=""
RUN_RECORD_HELPER="$ROOT/deploy/toolforge/run-record.cjs"
REFRESH_LOG_DIR=""
REFRESH_LOG_FILE=""

initialize_refresh_logging() {
  REFRESH_LOG_DIR="${WIKI_ECON_REFRESH_LOG_DIR:-$WIKI_ECON_OUTPUT_DIR/logs/refresh}"
  REFRESH_LOG_FILE="$REFRESH_LOG_DIR/$WIKI_ECON_RUN_ID.log"
  node "$RUN_RECORD_HELPER" rotate-logs "$REFRESH_LOG_DIR" "$REFRESH_HISTORY_LIMIT"
  if [ "${WIKI_ECON_REFRESH_LOG_TEE:-1}" = "1" ]; then
    exec > >(tee -a "$REFRESH_LOG_FILE") 2>&1
  else
    exec >> "$REFRESH_LOG_FILE" 2>&1
  fi
  echo "=== wiki-economics refresh start run_id=$WIKI_ECON_RUN_ID at=$REFRESH_STARTED_AT ==="
}

validate_lock_integer() {
  local name=$1 value=$2 allow_zero=${3:-0}
  if [[ ! "$value" =~ ^[0-9]+$ ]] || { [ "$allow_zero" -eq 0 ] && [ "$value" -eq 0 ]; }; then
    echo "$name must be a positive integer (got: $value)" >&2
    return 1
  fi
}

# This function runs from a process whose current directory is the acquired
# lock directory. If a stale lock is atomically moved aside, the process keeps
# writing to the original directory inode and cannot corrupt a successor's
# lock at the old path.
write_refresh_lock_metadata() {
  local heartbeat_at heartbeat_epoch
  heartbeat_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  heartbeat_epoch="$(date +%s)"
  WIKI_ECON_LOCK_RUN_ID="$WIKI_ECON_RUN_ID" \
  WIKI_ECON_LOCK_STARTED_AT="$REFRESH_STARTED_AT" \
  WIKI_ECON_LOCK_START_EPOCH="$REFRESH_START_EPOCH" \
  WIKI_ECON_LOCK_PID="$$" \
  WIKI_ECON_LOCK_JOB_IDENTITY="$REFRESH_JOB_IDENTITY" \
  WIKI_ECON_LOCK_PROCESS_IDENTITY="$REFRESH_PROCESS_IDENTITY" \
  WIKI_ECON_LOCK_OWNER_TOKEN="$REFRESH_LOCK_TOKEN" \
  WIKI_ECON_LOCK_HEARTBEAT_AT="$heartbeat_at" \
  WIKI_ECON_LOCK_HEARTBEAT_EPOCH="$heartbeat_epoch" \
    node - owner.json <<'NODE'
const fs = require("node:fs");

const output = process.argv[2];
const snapshot = fs.existsSync("selected-snapshot")
  ? fs.readFileSync("selected-snapshot", "utf8").trim()
  : null;
const metadata = {
  schema_version: 1,
  run_id: process.env.WIKI_ECON_LOCK_RUN_ID,
  started_at: process.env.WIKI_ECON_LOCK_STARTED_AT,
  start_epoch: Number(process.env.WIKI_ECON_LOCK_START_EPOCH),
  pid: Number(process.env.WIKI_ECON_LOCK_PID),
  job_identity: process.env.WIKI_ECON_LOCK_JOB_IDENTITY,
  process_identity: process.env.WIKI_ECON_LOCK_PROCESS_IDENTITY,
  owner_token: process.env.WIKI_ECON_LOCK_OWNER_TOKEN,
  selected_snapshot: snapshot || null,
  heartbeat_at: process.env.WIKI_ECON_LOCK_HEARTBEAT_AT,
  heartbeat_epoch: Number(process.env.WIKI_ECON_LOCK_HEARTBEAT_EPOCH),
};
const temporary = `${output}.tmp.${process.pid}`;
fs.writeFileSync(temporary, `${JSON.stringify(metadata)}\n`, {mode: 0o600});
fs.renameSync(temporary, output);
NODE
}

refresh_lock_is_stale() {
  local candidate=$1 now_epoch
  now_epoch="$(date +%s)"
  WIKI_ECON_LOCK_NOW_EPOCH="$now_epoch" \
  WIKI_ECON_LOCK_STALE_SECS="$REFRESH_LOCK_STALE_SECS" \
  WIKI_ECON_LOCK_PROCESS_IDENTITY="$REFRESH_PROCESS_IDENTITY" \
    node - "$candidate" <<'NODE'
const fs = require("node:fs");
const path = require("node:path");

const lockDir = process.argv[2];
const now = Number(process.env.WIKI_ECON_LOCK_NOW_EPOCH);
const staleAfter = Number(process.env.WIKI_ECON_LOCK_STALE_SECS);
let owner;
try {
  owner = JSON.parse(fs.readFileSync(path.join(lockDir, "owner.json"), "utf8"));
} catch {
  const age = now - Math.floor(fs.statSync(lockDir).mtimeMs / 1000);
  process.exit(age > staleAfter ? 0 : 1);
}

if (
  owner.process_identity === process.env.WIKI_ECON_LOCK_PROCESS_IDENTITY &&
  Number.isSafeInteger(owner.pid) &&
  owner.pid > 0
) {
  try {
    process.kill(owner.pid, 0);
    process.exit(1);
  } catch (error) {
    if (error.code === "ESRCH") process.exit(0);
    process.exit(1);
  }
}

const heartbeat = Number(owner.heartbeat_epoch);
process.exit(Number.isSafeInteger(heartbeat) && now - heartbeat > staleAfter ? 0 : 1);
NODE
}

report_refresh_lock_owner() {
  local owner_file="$REFRESH_LOCK_DIR/owner.json"
  echo "Another wiki-economics refresh is already running; refusing run $WIKI_ECON_RUN_ID." >&2
  if [ -f "$owner_file" ]; then
    printf 'Active lock owner: ' >&2
    tr -d '\n' < "$owner_file" >&2
    printf '\n' >&2
  else
    echo "Active lock has no readable owner metadata yet: $REFRESH_LOCK_DIR" >&2
  fi
}

start_refresh_lock_heartbeat() {
  (
    cd "$REFRESH_LOCK_DIR" || exit 0
    while sleep "$REFRESH_LOCK_HEARTBEAT_SECS"; do
      [ -f owner-token ] || exit 0
      [ "$(<owner-token)" = "$REFRESH_LOCK_TOKEN" ] || exit 0
      if ! write_refresh_lock_metadata || ! node "$RUN_RECORD_HELPER" write; then
        echo "Refresh heartbeat publication failed; terminating run $WIKI_ECON_RUN_ID" >&2
        kill -TERM "$$"
        exit 1
      fi
    done
  ) &
  REFRESH_LOCK_HEARTBEAT_PID=$!
}

acquire_refresh_lock() {
  local attempt stale_dir token_before token_after
  validate_lock_integer WIKI_ECON_REFRESH_LOCK_HEARTBEAT_SECS "$REFRESH_LOCK_HEARTBEAT_SECS"
  validate_lock_integer WIKI_ECON_REFRESH_LOCK_STALE_SECS "$REFRESH_LOCK_STALE_SECS"
  validate_lock_integer WIKI_ECON_REFRESH_LOCK_RECHECK_SECS "$REFRESH_LOCK_RECHECK_SECS" 1

  REFRESH_LOCK_DIR="${WIKI_ECON_REFRESH_LOCK_DIR:-$WIKI_ECON_OUTPUT_DIR/.refresh-lock}"
  mkdir -p "$(dirname "$REFRESH_LOCK_DIR")"

  for attempt in 1 2 3; do
    if mkdir "$REFRESH_LOCK_DIR" 2>/dev/null; then
      chmod 700 "$REFRESH_LOCK_DIR"
      printf '%s\n' "$REFRESH_LOCK_TOKEN" > "$REFRESH_LOCK_DIR/owner-token"
      if ! (
        cd "$REFRESH_LOCK_DIR"
        write_refresh_lock_metadata
      ); then
        rm -rf "$REFRESH_LOCK_DIR"
        return 1
      fi
      REFRESH_LOCK_OWNED=1
      echo "==> Acquired refresh lock: $REFRESH_LOCK_DIR"
      return 0
    fi

    if ! refresh_lock_is_stale "$REFRESH_LOCK_DIR"; then
      report_refresh_lock_owner
      return 75
    fi

    token_before="$(cat "$REFRESH_LOCK_DIR/owner-token" 2>/dev/null || true)"
    if [ "$REFRESH_LOCK_RECHECK_SECS" -gt 0 ]; then
      sleep "$REFRESH_LOCK_RECHECK_SECS"
    fi
    token_after="$(cat "$REFRESH_LOCK_DIR/owner-token" 2>/dev/null || true)"
    if [ "$token_before" != "$token_after" ] || ! refresh_lock_is_stale "$REFRESH_LOCK_DIR"; then
      report_refresh_lock_owner
      return 75
    fi

    stale_dir="${REFRESH_LOCK_DIR}.stale.${REFRESH_START_EPOCH}.$$.$attempt"
    if mv "$REFRESH_LOCK_DIR" "$stale_dir" 2>/dev/null; then
      echo "==> Recovered demonstrably stale refresh lock: $token_after" >&2
      rm -rf "$stale_dir"
    fi
  done

  echo "Unable to acquire refresh lock after stale-lock recovery attempts: $REFRESH_LOCK_DIR" >&2
  return 75
}

set_refresh_lock_snapshot() {
  local snapshot=$1
  [[ "$snapshot" =~ ^[0-9]{4}-[0-9]{2}$ ]] || {
    echo "Snapshot resolver returned an invalid version: $snapshot" >&2
    return 1
  }
  (
    cd "$REFRESH_LOCK_DIR"
    [ "$(<owner-token)" = "$REFRESH_LOCK_TOKEN" ]
    printf '%s\n' "$snapshot" > selected-snapshot.tmp
    mv selected-snapshot.tmp selected-snapshot
    write_refresh_lock_metadata
  )
  SELECTED_SNAPSHOT=$snapshot
  printf '%s\n' running > "$REFRESH_LOCK_DIR/run-state"
}

stop_refresh_lock_heartbeat() {
  if [ -n "$REFRESH_LOCK_HEARTBEAT_PID" ]; then
    kill "$REFRESH_LOCK_HEARTBEAT_PID" 2>/dev/null || true
    wait "$REFRESH_LOCK_HEARTBEAT_PID" 2>/dev/null || true
  fi
  REFRESH_LOCK_HEARTBEAT_PID=""
}

release_refresh_lock() {
  if [ "$REFRESH_LOCK_OWNED" -eq 1 ] &&
     [ -f "$REFRESH_LOCK_DIR/owner-token" ] &&
     [ "$(<"$REFRESH_LOCK_DIR/owner-token")" = "$REFRESH_LOCK_TOKEN" ]; then
    rm -rf "$REFRESH_LOCK_DIR"
    echo "==> Released refresh lock"
  fi
  REFRESH_LOCK_OWNED=0
}

detect_source_commit() {
  local target candidate
  if [ -n "${WIKI_ECON_SOURCE_COMMIT:-}" ]; then
    printf '%s\n' "$WIKI_ECON_SOURCE_COMMIT"
    return
  fi
  if [ -n "${WIKI_ECON_BIN:-}" ] && [ -L "$(dirname "$WIKI_ECON_BIN")" ]; then
    target="$(readlink "$(dirname "$WIKI_ECON_BIN")")"
    candidate="$(basename "$target")"
    if [[ "$candidate" =~ ^[0-9a-f]{40}$ ]]; then
      printf '%s\n' "$candidate"
      return
    fi
  fi
  git -C "$ROOT" rev-parse HEAD 2>/dev/null || true
}

binary_sha256() {
  if [ -z "${WIKI_ECON_BIN:-}" ] || [ ! -f "$WIKI_ECON_BIN" ]; then
    return
  fi
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$WIKI_ECON_BIN" | awk '{print $1}'
  else
    shasum -a 256 "$WIKI_ECON_BIN" | awk '{print $1}'
  fi
}

initialize_refresh_run_record() {
  WIKI_ECON_RUN_EVENTS_FILE="$REFRESH_LOCK_DIR/stage-events.jsonl"
  WIKI_ECON_RUN_RECORD_HELPER="$RUN_RECORD_HELPER"
  WIKI_ECON_RUN_STATE_FILE="$REFRESH_LOCK_DIR/run-state"
  WIKI_ECON_RUN_SNAPSHOT_FILE="$REFRESH_LOCK_DIR/selected-snapshot"
  WIKI_ECON_RUN_STATUS_FILE="$WIKI_ECON_OUTPUT_DIR/.refresh-status.json"
  WIKI_ECON_RUN_HISTORY_FILE="$WIKI_ECON_OUTPUT_DIR/.refresh-history.jsonl"
  WIKI_ECON_RUN_PUBLICATION_FILE="$WIKI_ECON_OUTPUT_DIR/publication-gate.json"
  WIKI_ECON_RUN_STARTED_AT="$REFRESH_STARTED_AT"
  WIKI_ECON_RUN_START_EPOCH="$REFRESH_START_EPOCH"
  WIKI_ECON_RUN_WIKIS_JSON="$(node -e 'process.stdout.write(JSON.stringify(process.argv.slice(1)))' "${wikis[@]}")"
  WIKI_ECON_SOURCE_COMMIT="$(detect_source_commit)"
  WIKI_ECON_BINARY_SHA256="$(binary_sha256)"
  WIKI_ECON_REFRESH_HISTORY_LIMIT="$REFRESH_HISTORY_LIMIT"
  export WIKI_ECON_RUN_EVENTS_FILE WIKI_ECON_RUN_RECORD_HELPER WIKI_ECON_RUN_STATE_FILE
  export WIKI_ECON_RUN_SNAPSHOT_FILE WIKI_ECON_RUN_STATUS_FILE
  export WIKI_ECON_RUN_HISTORY_FILE WIKI_ECON_RUN_PUBLICATION_FILE
  export WIKI_ECON_RUN_STARTED_AT WIKI_ECON_RUN_START_EPOCH WIKI_ECON_RUN_WIKIS_JSON
  export WIKI_ECON_SOURCE_COMMIT WIKI_ECON_BINARY_SOURCE_COMMIT WIKI_ECON_BINARY_SHA256
  export WIKI_ECON_IMAGE_SOURCE_REF WIKI_ECON_IMAGE_SOURCE_COMMIT WIKI_ECON_IMAGE_DIGEST
  export WIKI_ECON_SITE_SOURCE_COMMIT WIKI_ECON_SITE_SOURCE_SHA256 WIKI_ECON_SITE_SOURCE_ARCHIVE_SHA256
  export WIKI_ECON_REFRESH_HISTORY_LIMIT WIKI_ECON_SITE_DIST_DIR WIKI_ECON_OUTPUT_DIR
  WIKI_ECON_RUN_LOG_FILE="$REFRESH_LOG_FILE"
  export WIKI_ECON_RUN_LOG_FILE
  : > "$WIKI_ECON_RUN_EVENTS_FILE"
  printf '%s\n' starting > "$WIKI_ECON_RUN_STATE_FILE"
  node "$RUN_RECORD_HELPER" write
}

pipeline_stage_enabled() {
  [ "$PIPELINE_MODE" = "1" ] && case "$REFRESH_STAGE" in
    ingest|metrics|lifecycle|page-week|patrol|publish) return 0 ;;
  esac
  return 1
}

start_qualification_receipt() {
  pipeline_stage_enabled || return 0
  QUALIFICATION_RECEIPT_DIR="${WIKI_ECON_QUALIFICATION_RECEIPT_DIR:-$WIKI_ECON_OUTPUT_DIR/_qualification}"
  export WIKI_ECON_QUALIFICATION_RECEIPT_DIR="$QUALIFICATION_RECEIPT_DIR"
  if ! node "$QUALIFICATION_RECEIPT_HELPER" start; then
    REFRESH_FAILURE_STAGE=qualification_receipt
    REFRESH_FAILURE_ERROR="unable to start the qualification receipt for $REFRESH_STAGE"
    return 1
  fi
  QUALIFICATION_RECEIPT_ACTIVE=1
  local receipt_stage receipt_run_id start_file
  receipt_stage="${REFRESH_STAGE//[^A-Za-z0-9._-]/_}"
  receipt_run_id="${WIKI_ECON_RUN_ID//[^A-Za-z0-9._-]/_}"
  start_file="$QUALIFICATION_RECEIPT_DIR/${PIPELINE_ID}/${receipt_stage}.${receipt_run_id}.start.json"
  # Resource samples are deliberately sparse and bounded. The final sample is
  # always taken synchronously, so a short stage still has an end measurement.
  (
    while [ -f "$start_file" ]; do
      sleep "${WIKI_ECON_QUALIFICATION_SAMPLE_SECS:-15}" || exit 0
      [ -f "$start_file" ] || exit 0
      node "$QUALIFICATION_RECEIPT_HELPER" sample >/dev/null 2>&1 || exit 0
    done
  ) &
  QUALIFICATION_RECEIPT_SAMPLER_PID=$!
  echo "==> Qualification receipt started: $QUALIFICATION_RECEIPT_DIR/${PIPELINE_ID}/${REFRESH_STAGE}.json"
}

stop_qualification_receipt_sampler() {
  if [ -n "$QUALIFICATION_RECEIPT_SAMPLER_PID" ]; then
    kill "$QUALIFICATION_RECEIPT_SAMPLER_PID" 2>/dev/null || true
    wait "$QUALIFICATION_RECEIPT_SAMPLER_PID" 2>/dev/null || true
  fi
  QUALIFICATION_RECEIPT_SAMPLER_PID=""
}

finish_qualification_receipt() {
  local exit_code=$1
  [ "$QUALIFICATION_RECEIPT_ACTIVE" -eq 1 ] || return 0
  stop_qualification_receipt_sampler
  if ! node "$QUALIFICATION_RECEIPT_HELPER" sample; then
    REFRESH_FAILURE_STAGE=qualification_receipt
    REFRESH_FAILURE_ERROR="unable to capture the final qualification resource sample for $REFRESH_STAGE"
    QUALIFICATION_RECEIPT_ACTIVE=0
    return 1
  fi
  if ! node "$QUALIFICATION_RECEIPT_HELPER" finish "$exit_code"; then
    REFRESH_FAILURE_STAGE=qualification_receipt
    REFRESH_FAILURE_ERROR="unable to finalize the qualification receipt for $REFRESH_STAGE"
    QUALIFICATION_RECEIPT_ACTIVE=0
    return 1
  fi
  QUALIFICATION_RECEIPT_ACTIVE=0
  return 0
}

load_pipeline_wikis() {
  local state_json
  state_json="$(node "$PIPELINE_STATE_HELPER" show --state "$PIPELINE_STATE_FILE")" || {
    REFRESH_FAILURE_STAGE=pipeline_state
    REFRESH_FAILURE_ERROR="unable to read the existing pipeline state"
    echo "$REFRESH_FAILURE_ERROR: $PIPELINE_STATE_FILE" >&2
    return 1
  }
  mapfile -t wikis < <(printf '%s\n' "$state_json" | node -e '
    let input = "";
    process.stdin.on("data", (chunk) => { input += chunk; });
    process.stdin.on("end", () => {
      const state = JSON.parse(input);
      for (const wiki of state.wikis) process.stdout.write(`${wiki}\n`);
    });
  ')
  if [ "${#wikis[@]}" -eq 0 ]; then
    REFRESH_FAILURE_STAGE=pipeline_state
    REFRESH_FAILURE_ERROR="pipeline state contains no wikis"
    echo "$REFRESH_FAILURE_ERROR: $PIPELINE_STATE_FILE" >&2
    return 1
  fi
}

begin_pipeline_stage() {
  local response
  local -a begin_cmd=(
    node "$PIPELINE_STATE_HELPER" begin
    --state "$PIPELINE_STATE_FILE"
    --stage "$REFRESH_STAGE"
    --run-id "$WIKI_ECON_RUN_ID"
    --wikis-json "$WIKI_ECON_RUN_WIKIS_JSON"
    --stale-after-secs "${WIKI_ECON_PIPELINE_STALE_SECS:-21600}"
  )
  if [ "$REFRESH_STAGE" = "ingest" ]; then
    begin_cmd+=(--snapshot "$SELECTED_SNAPSHOT")
  fi
  if ! response="$("${begin_cmd[@]}")"; then
    REFRESH_FAILURE_STAGE=pipeline_state
    REFRESH_FAILURE_ERROR="pipeline stage $REFRESH_STAGE could not acquire its state lease"
    echo "$REFRESH_FAILURE_ERROR" >&2
    return 1
  fi
  PIPELINE_ID="$(printf '%s\n' "$response" | node -e '
    let input = "";
    process.stdin.on("data", (chunk) => { input += chunk; });
    process.stdin.on("end", () => {
      const value = JSON.parse(input);
      process.stdout.write(String(value.pipeline_id));
    });
  ')"
  SELECTED_SNAPSHOT="$(printf '%s\n' "$response" | node -e '
    let input = "";
    process.stdin.on("data", (chunk) => { input += chunk; });
    process.stdin.on("end", () => {
      const value = JSON.parse(input);
      process.stdout.write(String(value.snapshot));
    });
  ')"
  export WIKI_ECON_PIPELINE_ID="$PIPELINE_ID"
  export WIKI_ECON_PIPELINE_STATE_FILE="$PIPELINE_STATE_FILE"
  PIPELINE_STAGE_ACTIVE=1
  echo "==> Pipeline lease acquired: pipeline_id=$PIPELINE_ID stage=$REFRESH_STAGE snapshot=$SELECTED_SNAPSHOT"
}

finish_pipeline_stage() {
  local exit_code=$1
  [ "$PIPELINE_STAGE_ACTIVE" -eq 1 ] || return 0
  local result
  if [ "$exit_code" -eq 0 ]; then
    if ! result="$(node "$PIPELINE_STATE_HELPER" complete \
      --state "$PIPELINE_STATE_FILE" \
      --stage "$REFRESH_STAGE" \
      --run-id "$WIKI_ECON_RUN_ID")"; then
      echo "Unable to mark pipeline stage $REFRESH_STAGE complete" >&2
      REFRESH_FAILURE_STAGE=pipeline_state
      REFRESH_FAILURE_ERROR="unable to publish pipeline stage completion"
      node "$PIPELINE_STATE_HELPER" fail \
        --state "$PIPELINE_STATE_FILE" \
        --stage "$REFRESH_STAGE" \
        --run-id "$WIKI_ECON_RUN_ID" \
        --error "$REFRESH_FAILURE_ERROR" >/dev/null 2>&1 || true
      PIPELINE_STAGE_ACTIVE=0
      return 1
    fi
    echo "==> Pipeline stage completed: $result"
  else
    result="$(node "$PIPELINE_STATE_HELPER" fail \
      --state "$PIPELINE_STATE_FILE" \
      --stage "$REFRESH_STAGE" \
      --run-id "$WIKI_ECON_RUN_ID" \
      --error "${REFRESH_FAILURE_ERROR:-refresh exited with status $exit_code}" 2>&1)"
    if [ $? -ne 0 ]; then
      echo "Unable to mark pipeline stage $REFRESH_STAGE failed: $result" >&2
    else
      echo "==> Pipeline stage failed: $result" >&2
    fi
  fi
  PIPELINE_STAGE_ACTIVE=0
}

capture_refresh_error() {
  local exit_code=$1 command=$2
  if [ -z "$REFRESH_FAILURE_ERROR" ]; then
    REFRESH_FAILURE_ERROR="command exited $exit_code: $command"
  fi
}

finish_refresh() {
  local exit_code=$1
  trap - EXIT ERR INT TERM
  set +e
  stop_refresh_lock_heartbeat
  if [ "$exit_code" -ne 0 ] && [ -z "$REFRESH_FAILURE_ERROR" ]; then
    REFRESH_FAILURE_ERROR="refresh exited with status $exit_code"
  fi
  WIKI_ECON_RUN_ERROR="$REFRESH_FAILURE_ERROR"
  export WIKI_ECON_RUN_ERROR
  if ! finish_qualification_receipt "$exit_code"; then
    if [ "$exit_code" -eq 0 ]; then
      exit_code=1
    fi
  fi
  if ! finish_pipeline_stage "$exit_code"; then
    if [ "$exit_code" -eq 0 ]; then
      exit_code=1
      REFRESH_FAILURE_STAGE=pipeline_state
      REFRESH_FAILURE_ERROR="unable to publish pipeline stage completion"
    fi
  fi
  WIKI_ECON_RUN_ERROR="$REFRESH_FAILURE_ERROR"
  WIKI_ECON_RUN_FAILING_STAGE="$REFRESH_FAILURE_STAGE"
  export WIKI_ECON_RUN_ERROR WIKI_ECON_RUN_FAILING_STAGE
  if ! node "$RUN_RECORD_HELPER" finish "$exit_code"; then
    echo "Unable to publish terminal refresh run record" >&2
    if [ "$exit_code" -eq 0 ]; then
      exit_code=1
    fi
  fi
  if [ "$exit_code" -ne 0 ]; then
    echo "!!! REFRESH FAILED run_id=$WIKI_ECON_RUN_ID stage=${REFRESH_FAILURE_STAGE:-unknown} exit_code=$exit_code error=${REFRESH_FAILURE_ERROR:-unknown refresh failure} log_file=${REFRESH_LOG_FILE:-unknown} status_file=$WIKI_ECON_OUTPUT_DIR/.refresh-status.json" >&2
  fi
  echo "=== wiki-economics refresh end run_id=$WIKI_ECON_RUN_ID exit_code=$exit_code at=$(date -u +%Y-%m-%dT%H:%M:%SZ) ==="
  release_refresh_lock
  exit "$exit_code"
}

wiki_econ_init_runtime
PIPELINE_STATE_FILE="${PIPELINE_STATE_FILE:-$WIKI_ECON_OUTPUT_DIR/.pipeline-state.json}"
export PIPELINE_STATE_FILE
WIKI_ECON_QUALIFICATION_RECEIPT_DIR="${WIKI_ECON_QUALIFICATION_RECEIPT_DIR:-$WIKI_ECON_OUTPUT_DIR/_qualification}"
export WIKI_ECON_QUALIFICATION_RECEIPT_DIR
wiki_econ_ensure_local_dirs
initialize_refresh_logging

wikis=()
if pipeline_stage_enabled && [ "$REFRESH_STAGE" != "ingest" ]; then
  load_pipeline_wikis || exit 1
elif pipeline_stage_enabled && [ "$REFRESH_STAGE" = "ingest" ] && [ -n "${WIKI_ECON_PIPELINE_WIKIS:-}" ]; then
  # Qualification wikis (for example enwiki) are intentionally hidden from
  # the normal scheduled refresh resolver. The staged operator path may name
  # them explicitly, but the registry is still authoritative: unregistered
  # names are rejected before any network or storage work starts.
  pipeline_wikis_json="$(node - "$WIKI_ECON_WIKI_LIFECYCLE_FILE" <<'NODE'
const fs = require("node:fs");
const registry = JSON.parse(fs.readFileSync(process.argv[2], "utf8"));
const selected = (process.env.WIKI_ECON_PIPELINE_WIKIS || "")
  .split(/[ ,\t\r\n]+/).map((wiki) => wiki.trim()).filter(Boolean);
if (!selected.length) throw new Error("WIKI_ECON_PIPELINE_WIKIS is empty");
const unique = [...new Set(selected)].sort();
for (const wiki of unique) {
  const entry = registry.wikis?.[wiki];
  if (!entry) throw new Error(`pipeline wiki is not registered: ${wiki}`);
  const allowed = entry.refresh === "qualification"
    || entry.refresh === "manual"
    || entry.refresh === "scheduled";
  if (!allowed) throw new Error(`pipeline wiki ${wiki} has refresh state ${entry.refresh}`);
  if (entry.refresh === "qualification" && entry.publication !== "hidden") {
    throw new Error(`qualification pipeline wiki ${wiki} must be hidden`);
  }
}
process.stdout.write(JSON.stringify(unique));
NODE
  )" || {
    REFRESH_FAILURE_STAGE=pipeline_state
    REFRESH_FAILURE_ERROR="invalid WIKI_ECON_PIPELINE_WIKIS selection"
    echo "$REFRESH_FAILURE_ERROR" >&2
    exit 1
  }
  while IFS= read -r wiki; do
    [ -n "$wiki" ] && wikis+=("$wiki")
  done < <(printf '%s\n' "$pipeline_wikis_json" | node -e '
    const value = JSON.parse(require("fs").readFileSync(0, "utf8"));
    for (const wiki of value) process.stdout.write(`${wiki}\n`);
  ')
else
  refresh_wikis=$(node "$ROOT/scripts/wiki-lifecycle.cjs" refresh-wikis)
  while IFS= read -r wiki; do
    [ -n "$wiki" ] && wikis+=("$wiki")
  done <<< "$refresh_wikis"
  if [ "${#wikis[@]}" -eq 0 ]; then
    echo "Wiki lifecycle registry selected no scheduled refresh wikis" >&2
    exit 1
  fi
fi

if ! acquire_refresh_lock; then
  echo "=== wiki-economics refresh end run_id=$WIKI_ECON_RUN_ID exit_code=75 at=$(date -u +%Y-%m-%dT%H:%M:%SZ) ==="
  exit 75
fi
trap 'finish_refresh $?' EXIT
trap 'capture_refresh_error "$?" "$BASH_COMMAND"' ERR
trap 'exit 130' INT
trap 'exit 143' TERM

initialize_refresh_run_record
start_refresh_lock_heartbeat

if [ -z "${WIKI_ECON_BIN:-}" ]; then
  echo "Toolforge refresh requires WIKI_ECON_BIN for snapshot resolution" >&2
  exit 1
fi
CLEANUP_STARTED_EPOCH="$(date +%s)"
wiki_econ_record_stage_event started cleanup_stale
declare -a cleanup_cmd=(
  "$WIKI_ECON_BIN"
  --data-dir "$WIKI_ECON_DATA_DIR"
  --output-dir "$WIKI_ECON_OUTPUT_DIR"
  --run-id "$WIKI_ECON_RUN_ID"
  cleanup-stale
  --site-dist-dir "$WIKI_ECON_SITE_DIST_DIR"
  --minimum-age-secs "${WIKI_ECON_STALE_ARTIFACT_SECS:-21600}"
  --capacity-dir "${WIKI_ECON_CAPACITY_ROOT:-/data/project/wiki-economics/capacity}"
)
cleanup_cmd+=(--scratch-dir "${WIKI_ECON_SCRATCH_DIR:-$WIKI_ECON_OUTPUT_DIR}")
cleanup_cmd+=("${wikis[@]}")
if ! cleanup_summary="$(RUST_LOG="$WIKI_ECON_RUST_LOG" "${cleanup_cmd[@]}")"; then
  REFRESH_FAILURE_STAGE=cleanup_stale
  REFRESH_FAILURE_ERROR="safe abandoned-artifact cleanup failed"
  wiki_econ_record_stage_event failed cleanup_stale "" \
    "$(( ($(date +%s) - CLEANUP_STARTED_EPOCH) * 1000 ))" \
    "$REFRESH_FAILURE_ERROR"
  exit 1
fi
if ! release_cleanup_summary="$(
  "$ROOT/deploy/toolforge/prune-releases.sh" \
    "${WIKI_ECON_TOOLFORGE_APP_ROOT:-/data/project/wiki-economics/app}" \
    "${WIKI_ECON_RELEASE_RETENTION:-3}"
)"; then
  REFRESH_FAILURE_STAGE=cleanup_stale
  REFRESH_FAILURE_ERROR="safe binary release cleanup failed"
  wiki_econ_record_stage_event failed cleanup_stale "" \
    "$(( ($(date +%s) - CLEANUP_STARTED_EPOCH) * 1000 ))" \
    "$REFRESH_FAILURE_ERROR"
  exit 1
fi
wiki_econ_record_stage_event completed cleanup_stale "" \
  "$(( ($(date +%s) - CLEANUP_STARTED_EPOCH) * 1000 ))"
echo "==> Abandoned artifact cleanup: $cleanup_summary"
echo "==> Binary release cleanup: $release_cleanup_summary"
refresh_driver="${WIKI_ECON_REFRESH_DRIVER:-$ROOT/scripts/refresh.sh}"
declare -a refresh_driver_cmd=()
if [ "$REFRESH_STAGE" = "site" ]; then
  # A site-only deployment consumes the already-published receipt and does
  # not prepare history. Discovering a newer remote dump here would mutate
  # operational freshness state even though no worker was scheduled to build
  # it, making an otherwise healthy frontend release look critically stale.
  printf '%s\n' running > "$REFRESH_LOCK_DIR/run-state"
  refresh_driver_cmd+=(--stage site)
  echo "==> Toolforge site refresh against the current publication"
elif pipeline_stage_enabled && [ "$REFRESH_STAGE" != "ingest" ]; then
  # Every stage after ingest consumes the immutable snapshot and wiki set
  # recorded by the coordinator. Never re-resolve a newer remote snapshot in
  # the middle of a pipeline generation.
  begin_pipeline_stage
  set_refresh_lock_snapshot "$SELECTED_SNAPSHOT"
  start_qualification_receipt || exit 1
  echo "==> Toolforge pipeline refresh: ${wikis[*]} (snapshot $SELECTED_SNAPSHOT, stage $REFRESH_STAGE)"
  refresh_driver_cmd+=(--version "$SELECTED_SNAPSHOT" "${wikis[@]}" --stage "$REFRESH_STAGE")
else
  declare -a resolve_cmd=(
    "$WIKI_ECON_BIN"
    --data-dir "$WIKI_ECON_DATA_DIR"
    --output-dir "$WIKI_ECON_OUTPUT_DIR"
    --run-id "$WIKI_ECON_RUN_ID"
    snapshot-resolve
    "${wikis[@]}"
  )
  printf '==> %s' "${resolve_cmd[0]}"
  for arg in "${resolve_cmd[@]:1}"; do
    printf ' %q' "$arg"
  done
  printf '\n'
  # The Rust CLI emits verbose tracing records on stdout so standalone
  # invocations remain self-contained and useful to operators.  Do not feed
  # that mixed human log stream directly into the version validator: capture
  # and retain it in the refresh log, then extract the final machine-readable
  # YYYY-MM line.  A prior implementation treated the whole stream as the
  # version and failed every Toolforge refresh before ingest began.
  snapshot_resolve_output=""
  if ! snapshot_resolve_output="$(RUST_LOG="$WIKI_ECON_RUST_LOG" "${resolve_cmd[@]}")"; then
    REFRESH_FAILURE_STAGE=snapshot_resolve
    REFRESH_FAILURE_ERROR="snapshot resolver command failed"
    echo "Snapshot resolver failed for ${wikis[*]}" >&2
    printf '%s\n' "$snapshot_resolve_output" >&2
    exit 1
  fi
  printf '%s\n' "$snapshot_resolve_output"
  selected_snapshot=""
  while IFS= read -r snapshot_line; do
    if [[ "$snapshot_line" =~ ^[0-9]{4}-[0-9]{2}$ ]]; then
      selected_snapshot="$snapshot_line"
    fi
  done <<< "$snapshot_resolve_output"
  if [ -z "$selected_snapshot" ]; then
    REFRESH_FAILURE_STAGE=snapshot_resolve
    REFRESH_FAILURE_ERROR="snapshot resolver returned no YYYY-MM version"
    echo "Snapshot resolver returned no valid version for ${wikis[*]}" >&2
    exit 1
  fi
  set_refresh_lock_snapshot "$selected_snapshot"

  if pipeline_stage_enabled && [ "$REFRESH_STAGE" = "ingest" ]; then
    begin_pipeline_stage
    start_qualification_receipt || exit 1
  fi

  echo "==> Toolforge refresh: ${wikis[*]} (snapshot $SELECTED_SNAPSHOT, stage $REFRESH_STAGE)"
  refresh_driver_cmd+=(--version "$SELECTED_SNAPSHOT" "${wikis[@]}")
  if [ "$REFRESH_STAGE" != "all" ]; then
    refresh_driver_cmd+=(--stage "$REFRESH_STAGE")
  fi
fi
"$refresh_driver" "${refresh_driver_cmd[@]}"

ARTIFACT_CHECK_STARTED_EPOCH="$(date +%s)"
wiki_econ_record_stage_event started artifact_check
if pipeline_stage_enabled; then
  case "$REFRESH_STAGE" in
    ingest)
      # Ingest commits source/warehouse receipts per wiki; the next stage is
      # the first one with a stable metric artifact to validate.
      echo "==> Ingest stage committed source and analytical receipts"
      ;;
    metrics)
      required_metrics=(gdp.parquet gdp_user_type_share.parquet inequality.parquet labor_monthly.parquet gdp_activity_tiers.parquet editor_identity_coverage.json)
      for wiki in "${wikis[@]}"; do
        for required in "${required_metrics[@]}"; do
          if [ ! -f "$WIKI_ECON_OUTPUT_DIR/$wiki/$required" ]; then
            REFRESH_FAILURE_STAGE=artifact_check
            REFRESH_FAILURE_ERROR="metrics artifact is missing for $wiki: $required"
            wiki_econ_record_stage_event failed artifact_check "" \
              "$(( ($(date +%s) - ARTIFACT_CHECK_STARTED_EPOCH) * 1000 ))" \
              "$REFRESH_FAILURE_ERROR"
            echo "Metrics stage succeeded but required artifact is missing: $WIKI_ECON_OUTPUT_DIR/$wiki/$required" >&2
            exit 1
          fi
        done
      done
      ;;
    lifecycle)
      required_metrics=(business_funnel.parquet labor_churn.parquet labor_cohorts.parquet)
      for wiki in "${wikis[@]}"; do
        for required in "${required_metrics[@]}"; do
          if [ ! -f "$WIKI_ECON_OUTPUT_DIR/$wiki/$required" ]; then
            REFRESH_FAILURE_STAGE=artifact_check
            REFRESH_FAILURE_ERROR="lifecycle artifact is missing for $wiki: $required"
            wiki_econ_record_stage_event failed artifact_check "" \
              "$(( ($(date +%s) - ARTIFACT_CHECK_STARTED_EPOCH) * 1000 ))" \
              "$REFRESH_FAILURE_ERROR"
            echo "Lifecycle stage succeeded but required artifact is missing: $WIKI_ECON_OUTPUT_DIR/$wiki/$required" >&2
            exit 1
          fi
        done
      done
      ;;
    page-week)
      for wiki in "${wikis[@]}"; do
        required="$WIKI_ECON_OUTPUT_DIR/$wiki/page_weekly_edits.parquet"
        if [ ! -f "$required" ]; then
          REFRESH_FAILURE_STAGE=artifact_check
          REFRESH_FAILURE_ERROR="page-week artifact is missing for $wiki"
          wiki_econ_record_stage_event failed artifact_check "" \
            "$(( ($(date +%s) - ARTIFACT_CHECK_STARTED_EPOCH) * 1000 ))" \
            "$REFRESH_FAILURE_ERROR"
          echo "Page-week stage succeeded but required artifact is missing: $required" >&2
          exit 1
        fi
      done
      ;;
    patrol)
      for wiki in "${wikis[@]}"; do
        required="$WIKI_ECON_OUTPUT_DIR/$wiki/patrol.parquet"
        if [ ! -f "$required" ]; then
          REFRESH_FAILURE_STAGE=artifact_check
          REFRESH_FAILURE_ERROR="patrol artifact is missing for $wiki"
          wiki_econ_record_stage_event failed artifact_check "" \
            "$(( ($(date +%s) - ARTIFACT_CHECK_STARTED_EPOCH) * 1000 ))" \
            "$REFRESH_FAILURE_ERROR"
          echo "Patrol stage succeeded but required artifact is missing: $required" >&2
          exit 1
        fi
      done
      ;;
    publish)
      required_metrics=(manifest.json defaults_business.json defaults_gdp.json defaults_inequality.json defaults_labor.json defaults_patrol.json defaults_edit_variation.json business_funnel.parquet gdp.parquet gdp_activity_tiers.parquet gdp_user_type_share.parquet inequality.parquet labor_churn.parquet labor_cohorts.parquet labor_monthly.parquet patrol.parquet)
      for required in "${required_metrics[@]}"; do
        if [ ! -f "$WIKI_ECON_OUTPUT_DIR/$required" ]; then
          REFRESH_FAILURE_STAGE=artifact_check
          REFRESH_FAILURE_ERROR="published artifact is missing: $required"
          wiki_econ_record_stage_event failed artifact_check "" \
            "$(( ($(date +%s) - ARTIFACT_CHECK_STARTED_EPOCH) * 1000 ))" \
            "$REFRESH_FAILURE_ERROR"
          echo "Publish stage succeeded but required artifact is missing: $WIKI_ECON_OUTPUT_DIR/$required" >&2
          exit 1
        fi
      done
      for page in business.html gdp.html inequality.html labor.html patrol.html edit-variation.html; do
        if [ ! -f "$WIKI_ECON_SITE_DIST_DIR/$page" ]; then
          REFRESH_FAILURE_STAGE=artifact_check
          REFRESH_FAILURE_ERROR="published site page is missing: $page"
          wiki_econ_record_stage_event failed artifact_check "" \
            "$(( ($(date +%s) - ARTIFACT_CHECK_STARTED_EPOCH) * 1000 ))" \
            "$REFRESH_FAILURE_ERROR"
          echo "Publish stage succeeded but site page is missing: $WIKI_ECON_SITE_DIST_DIR/$page" >&2
          exit 1
        fi
      done
      ;;
  esac
else
  for required in \
    manifest.json \
    defaults_business.json \
    defaults_gdp.json \
    defaults_inequality.json \
    defaults_labor.json \
    defaults_patrol.json \
    defaults_edit_variation.json \
    business_funnel.parquet \
    gdp.parquet \
    gdp_activity_tiers.parquet \
    gdp_user_type_share.parquet \
    inequality.parquet \
    labor_churn.parquet \
    labor_cohorts.parquet \
    labor_monthly.parquet \
    patrol.parquet
  do
    if [ ! -f "$WIKI_ECON_OUTPUT_DIR/$required" ]; then
      REFRESH_FAILURE_STAGE=artifact_check
      REFRESH_FAILURE_ERROR="required artifact is missing: $required"
      wiki_econ_record_stage_event failed artifact_check "" \
        "$(( ($(date +%s) - ARTIFACT_CHECK_STARTED_EPOCH) * 1000 ))" \
        "$REFRESH_FAILURE_ERROR"
      echo "Refresh succeeded but required artifact is missing: $WIKI_ECON_OUTPUT_DIR/$required" >&2
      exit 1
    fi
  done

  for page in business.html gdp.html inequality.html labor.html patrol.html edit-variation.html; do
    if [ ! -f "$WIKI_ECON_SITE_DIST_DIR/$page" ]; then
      REFRESH_FAILURE_STAGE=artifact_check
      REFRESH_FAILURE_ERROR="published site page is missing: $page"
      wiki_econ_record_stage_event failed artifact_check "" \
        "$(( ($(date +%s) - ARTIFACT_CHECK_STARTED_EPOCH) * 1000 ))" \
        "$REFRESH_FAILURE_ERROR"
      echo "Site build is missing required page: $WIKI_ECON_SITE_DIST_DIR/$page" >&2
      exit 1
    fi
  done
fi

wiki_econ_record_stage_event completed artifact_check "" \
  "$(( ($(date +%s) - ARTIFACT_CHECK_STARTED_EPOCH) * 1000 ))"
echo "==> Toolforge refresh complete"
