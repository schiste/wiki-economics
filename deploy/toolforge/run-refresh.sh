#!/usr/bin/env bash
set -euo pipefail

# Refresh wrapper invoked by the `wiki-econ-refresh` Toolforge Job.
#
# Unlike deploy/cloud-vps/run-refresh.sh, this does NOT retain a release
# history for output/site: Toolforge's NFS quota is small (see
# deploy/toolforge/README.md), and retaining multiple full generations is
# expensive relative to the benefit. Metric files are atomically replaced,
# and the site builder atomically switches WIKI_ECON_SITE_DIST_DIR to a clean
# generated sibling before removing the previous site release.
#
# Raw dump cleanup is NOT done here: `wiki-econ run` (invoked by
# scripts/refresh.sh) deletes each wiki's raw .bz2 files itself immediately
# after that wiki's ingest stage succeeds, rather than waiting for every
# wiki in the batch plus the site build to finish. That's safe because
# src/storage.rs::marker_manifest_is_valid only checks that
# warehouse/analytical parquet outputs exist, never the raw .bz2 source —
# later runs stay idempotent without it.

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
# shellcheck disable=SC1091
. "$ROOT/scripts/lib/wiki_econ.sh"

# Write a status marker + rolling history to the shared NFS output dir on
# every exit (success, an early `exit 1` artifact check below, or a
# set -e-triggered failure) so the admin webservice — a separate Toolforge
# pod with no shared process memory and no Toolforge/Kubernetes API access —
# has a way to know whether the last scheduled refresh succeeded.
REFRESH_STARTED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
REFRESH_START_EPOCH="$(date +%s)"
REFRESH_HISTORY_LIMIT=20
WIKI_ECON_RUN_ID="${WIKI_ECON_RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)-$$}"
export WIKI_ECON_RUN_ID
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
      write_refresh_lock_metadata || exit 0
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
      start_refresh_lock_heartbeat
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
}

release_refresh_lock() {
  if [ -n "$REFRESH_LOCK_HEARTBEAT_PID" ]; then
    kill "$REFRESH_LOCK_HEARTBEAT_PID" 2>/dev/null || true
    wait "$REFRESH_LOCK_HEARTBEAT_PID" 2>/dev/null || true
  fi
  if [ "$REFRESH_LOCK_OWNED" -eq 1 ] &&
     [ -f "$REFRESH_LOCK_DIR/owner-token" ] &&
     [ "$(<"$REFRESH_LOCK_DIR/owner-token")" = "$REFRESH_LOCK_TOKEN" ]; then
    rm -rf "$REFRESH_LOCK_DIR"
    echo "==> Released refresh lock"
  fi
  REFRESH_LOCK_OWNED=0
}

read_cgroup_counter() {
  local path=$1 value
  if [ ! -r "$path" ]; then
    printf 'null'
    return
  fi
  value=$(<"$path")
  if [[ "$value" =~ ^[0-9]+$ ]]; then
    printf '%s' "$value"
  else
    printf 'null'
  fi
}

write_refresh_status() {
  local exit_code=$1
  if [ -z "${WIKI_ECON_OUTPUT_DIR:-}" ]; then
    return 0
  fi
  local finished_at duration wikis_json entry status_file history_file memory_peak memory_limit snapshot_json
  finished_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  duration=$(( $(date +%s) - REFRESH_START_EPOCH ))
  memory_peak=$(read_cgroup_counter /sys/fs/cgroup/memory.peak)
  memory_limit=$(read_cgroup_counter /sys/fs/cgroup/memory.max)
  snapshot_json=null
  if [ -n "$SELECTED_SNAPSHOT" ]; then
    snapshot_json="\"$SELECTED_SNAPSHOT\""
  fi
  wikis_json=$(printf '"%s",' "${wikis[@]:-}" | sed 's/,$//')
  entry=$(printf '{"runId":"%s","startedAt":"%s","finishedAt":"%s","exitCode":%d,"wikis":[%s],"selectedSnapshot":%s,"durationSecs":%d,"memoryPeakBytes":%s,"memoryLimitBytes":%s}' \
    "$WIKI_ECON_RUN_ID" "$REFRESH_STARTED_AT" "$finished_at" "$exit_code" "$wikis_json" "$snapshot_json" "$duration" "$memory_peak" "$memory_limit")
  status_file="$WIKI_ECON_OUTPUT_DIR/.refresh-status.json"
  history_file="$WIKI_ECON_OUTPUT_DIR/.refresh-history.jsonl"
  echo "$entry" > "${status_file}.tmp"
  mv "${status_file}.tmp" "$status_file"
  echo "$entry" >> "$history_file"
  tail -n "$REFRESH_HISTORY_LIMIT" "$history_file" > "${history_file}.tmp" && mv "${history_file}.tmp" "$history_file"
}

finish_refresh() {
  local exit_code=$1
  trap - EXIT INT TERM
  set +e
  write_refresh_status "$exit_code"
  release_refresh_lock
  exit "$exit_code"
}

wiki_econ_init_runtime
wiki_econ_ensure_local_dirs

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
  exit 75
fi
trap 'finish_refresh $?' EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

if [ -z "${WIKI_ECON_BIN:-}" ]; then
  echo "Toolforge refresh requires WIKI_ECON_BIN for snapshot resolution" >&2
  exit 1
fi
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

echo "==> Toolforge refresh: ${wikis[*]} (snapshot $SELECTED_SNAPSHOT)"
refresh_driver="${WIKI_ECON_REFRESH_DRIVER:-$ROOT/scripts/refresh.sh}"
"$refresh_driver" --version "$SELECTED_SNAPSHOT" "${wikis[@]}"

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
  page_weekly_edits.parquet \
  patrol.parquet
do
  if [ ! -f "$WIKI_ECON_OUTPUT_DIR/$required" ]; then
    echo "Refresh succeeded but required artifact is missing: $WIKI_ECON_OUTPUT_DIR/$required" >&2
    exit 1
  fi
done

for page in index.html business.html gdp.html inequality.html labor.html patrol.html edit-variation.html; do
  if [ ! -f "$WIKI_ECON_SITE_DIST_DIR/$page" ]; then
    echo "Site build is missing required page: $WIKI_ECON_SITE_DIST_DIR/$page" >&2
    exit 1
  fi
done

echo "==> Toolforge refresh complete"
