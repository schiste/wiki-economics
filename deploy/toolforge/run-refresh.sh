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
# the current 6 GiB Toolforge job; a future enwiki job must override them with
# its separately qualified 16 GiB / 250 GiB-reserve profile.
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
# everything; `ingest`/`compute`/`site` are for on-demand jobs that trigger
# just one stage between scheduled runs.
REFRESH_STAGE="${WIKI_ECON_REFRESH_STAGE:-all}"
case "$REFRESH_STAGE" in
  all|ingest|compute|site) ;;
  *)
    echo "Toolforge refresh requires WIKI_ECON_REFRESH_STAGE to be all, ingest, compute, or site (got: $REFRESH_STAGE)" >&2
    exit 2
    ;;
esac
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
  export WIKI_ECON_SOURCE_COMMIT WIKI_ECON_BINARY_SHA256
  export WIKI_ECON_IMAGE_SOURCE_REF WIKI_ECON_IMAGE_SOURCE_COMMIT
  export WIKI_ECON_REFRESH_HISTORY_LIMIT WIKI_ECON_SITE_DIST_DIR WIKI_ECON_OUTPUT_DIR
  WIKI_ECON_RUN_LOG_FILE="$REFRESH_LOG_FILE"
  export WIKI_ECON_RUN_LOG_FILE
  : > "$WIKI_ECON_RUN_EVENTS_FILE"
  printf '%s\n' starting > "$WIKI_ECON_RUN_STATE_FILE"
  node "$RUN_RECORD_HELPER" write
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
  WIKI_ECON_RUN_FAILING_STAGE="$REFRESH_FAILURE_STAGE"
  export WIKI_ECON_RUN_ERROR WIKI_ECON_RUN_FAILING_STAGE
  if ! node "$RUN_RECORD_HELPER" finish "$exit_code"; then
    echo "Unable to publish terminal refresh run record" >&2
    if [ "$exit_code" -eq 0 ]; then
      exit_code=1
    fi
  fi
  echo "=== wiki-economics refresh end run_id=$WIKI_ECON_RUN_ID exit_code=$exit_code at=$(date -u +%Y-%m-%dT%H:%M:%SZ) ==="
  release_refresh_lock
  exit "$exit_code"
}

wiki_econ_init_runtime
wiki_econ_ensure_local_dirs
initialize_refresh_logging

refresh_wikis=$(node "$ROOT/scripts/wiki-lifecycle.cjs" refresh-wikis)
wikis=()
while IFS= read -r wiki; do
  [ -n "$wiki" ] && wikis+=("$wiki")
done <<< "$refresh_wikis"
if [ "${#wikis[@]}" -eq 0 ]; then
  echo "Wiki lifecycle registry selected no scheduled refresh wikis" >&2
  exit 1
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
if [ -n "${WIKI_ECON_SCRATCH_DIR:-}" ]; then
  cleanup_cmd+=(--scratch-dir "$WIKI_ECON_SCRATCH_DIR")
fi
cleanup_cmd+=("${wikis[@]}")
if ! cleanup_summary="$(RUST_LOG=error "${cleanup_cmd[@]}")"; then
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
selected_snapshot="$(RUST_LOG=error "${resolve_cmd[@]}")"
set_refresh_lock_snapshot "$selected_snapshot"

echo "==> Toolforge refresh: ${wikis[*]} (snapshot $SELECTED_SNAPSHOT, stage $REFRESH_STAGE)"
refresh_driver="${WIKI_ECON_REFRESH_DRIVER:-$ROOT/scripts/refresh.sh}"
declare -a refresh_driver_cmd=(--version "$SELECTED_SNAPSHOT" "${wikis[@]}")
if [ "$REFRESH_STAGE" != "all" ]; then
  refresh_driver_cmd+=(--stage "$REFRESH_STAGE")
fi
"$refresh_driver" "${refresh_driver_cmd[@]}"

ARTIFACT_CHECK_STARTED_EPOCH="$(date +%s)"
wiki_econ_record_stage_event started artifact_check
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

for page in index.html business.html gdp.html inequality.html labor.html patrol.html edit-variation.html; do
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

wiki_econ_record_stage_event completed artifact_check "" \
  "$(( ($(date +%s) - ARTIFACT_CHECK_STARTED_EPOCH) * 1000 ))"
echo "==> Toolforge refresh complete"
