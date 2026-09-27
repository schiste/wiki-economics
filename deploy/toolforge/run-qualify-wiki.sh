#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 1 ]; then
  echo "Usage: run-qualify-wiki.sh WIKI" >&2
  exit 2
fi
wiki=$1
case "$wiki" in
  *[!a-z0-9_]*|'') echo "Unsafe wiki identifier: $wiki" >&2; exit 2 ;;
esac

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
if [ "${WIKI_ECON_CAPACITY_ADMITTED:-0}" != "1" ]; then
  if [ "$wiki" = "enwiki" ]; then
    qualification_resource_class=qualification
  else
    qualification_resource_class=isolated
  fi
  exec node "$ROOT/deploy/toolforge/capacity-admission.cjs" \
    --resource-class "$qualification_resource_class" -- "$0" "$@"
fi
: "${WIKI_ECON_BIN:?Toolforge qualification requires WIKI_ECON_BIN}"
qualification_root="${WIKI_ECON_QUALIFICATION_ROOT:-/data/project/wiki-economics/capacity/qualifications}"
case "$qualification_root" in
  ""|/) echo "Refusing unsafe qualification root: $qualification_root" >&2; exit 2 ;;
esac
export WIKI_ECON_RUN_ID="${WIKI_ECON_RUN_ID:-qualify-$wiki-$(date -u +%Y%m%dT%H%M%SZ)-$$}"
run_root="$qualification_root/$wiki/$WIKI_ECON_RUN_ID"
export WIKI_ECON_DATA_DIR="$run_root/data"
export WIKI_ECON_OUTPUT_DIR="$run_root/output"
export WIKI_ECON_SITE_DIST_DIR="$run_root/site-dist"
export WIKI_ECON_SCRATCH_DIR="$run_root/scratch"

# shellcheck disable=SC1091
. "$ROOT/scripts/lib/wiki_econ.sh"
wiki_econ_init_runtime
wiki_econ_ensure_local_dirs
mkdir -p "$WIKI_ECON_SCRATCH_DIR"

export WIKI_ECON_PREPARE_COMMAND=qualify-wiki
export CARGO_TERM_COLOR=never NO_COLOR=1 OBSERVABLE_TELEMETRY_DISABLE=true WIKI_ECON_LOG_ANSI=0
export WIKI_ECON_QUALIFICATION_RUN_KIND="${WIKI_ECON_QUALIFICATION_RUN_KIND:-initial_candidate}"
case "$WIKI_ECON_QUALIFICATION_RUN_KIND" in
  initial_candidate|rollover) ;;
  *) echo "Unsupported qualification run kind: $WIKI_ECON_QUALIFICATION_RUN_KIND" >&2; exit 2 ;;
esac

detect_cpu_limit_cores() {
  local quota="" period="" cores
  if [ -r /sys/fs/cgroup/cpu.max ]; then
    read -r quota period < /sys/fs/cgroup/cpu.max || true
  elif [ -r /sys/fs/cgroup/cpu/cpu.cfs_quota_us ] && [ -r /sys/fs/cgroup/cpu/cpu.cfs_period_us ]; then
    quota="$(< /sys/fs/cgroup/cpu/cpu.cfs_quota_us)"
    period="$(< /sys/fs/cgroup/cpu/cpu.cfs_period_us)"
  fi
  if [[ "$quota" =~ ^[0-9]+$ && "$period" =~ ^[0-9]+$ ]] && [ "$quota" -gt 0 ] && [ "$period" -gt 0 ]; then
    cores=$((quota / period))
    [ "$cores" -ge 1 ] || cores=1
    [ "$cores" -le 4 ] || cores=4
    printf '%s\n' "$cores"
    return
  fi
  # Unbounded local/dev cgroups do not prove any Toolforge CPU allocation.
  printf '1\n'
}

qualification_cpu_cores="${WIKI_ECON_REQUESTED_CPU_CORES:-$(detect_cpu_limit_cores)}"
if [[ ! "$qualification_cpu_cores" =~ ^[1-4]$ ]]; then
  echo "WIKI_ECON_REQUESTED_CPU_CORES must match the current 1–4 CPU Toolforge envelope (got: $qualification_cpu_cores)" >&2
  exit 2
fi
export WIKI_ECON_REQUESTED_CPU_CORES="$qualification_cpu_cores"

validate_worker_pool() {
  local name=$1 value=$2 ceiling=$3
  if [[ ! "$value" =~ ^[1-4]$ ]] || [ "$value" -gt "$ceiling" ]; then
    echo "$name must be between 1 and $ceiling for this Toolforge pod (got: $value)" >&2
    exit 2
  fi
}

export WIKI_ECON_REQUIRE_QUALIFIED_PROFILE=0
if [ "$wiki" = "enwiki" ]; then
  # Use the pod's CPU quota, capped at Toolforge's current four-CPU job limit.
  # The governor shrinks worker waves further when memory, scratch, disk, or
  # file handles are tight.
  export WIKI_ECON_THREAD_LIMIT="${WIKI_ECON_THREAD_LIMIT:-$qualification_cpu_cores}"
  export RAYON_NUM_THREADS="${RAYON_NUM_THREADS:-$WIKI_ECON_THREAD_LIMIT}"
  export POLARS_MAX_THREADS="${POLARS_MAX_THREADS:-$WIKI_ECON_THREAD_LIMIT}"
  export WIKI_ECON_SOURCE_WORKERS="${WIKI_ECON_SOURCE_WORKERS:-$WIKI_ECON_THREAD_LIMIT}"
  export WIKI_ECON_WEEKLY_WORKERS="${WIKI_ECON_WEEKLY_WORKERS:-$WIKI_ECON_THREAD_LIMIT}"
  # Enwiki's 6 GiB pod reached its cgroup limit with 32 simultaneous bucket
  # writers during page-week compaction. Keep the writer batch bounded while
  # retaining the four-CPU worker pool for weekly routing and reconciliation.
  export WIKI_ECON_MAX_ACTIVE_PARQUET_WRITERS="${WIKI_ECON_MAX_ACTIVE_PARQUET_WRITERS:-8}"
  export WIKI_ECON_FETCH_MAX_PARALLELISM="${WIKI_ECON_FETCH_MAX_PARALLELISM:-2}"
  validate_worker_pool WIKI_ECON_THREAD_LIMIT "$WIKI_ECON_THREAD_LIMIT" "$qualification_cpu_cores"
  validate_worker_pool RAYON_NUM_THREADS "$RAYON_NUM_THREADS" "$WIKI_ECON_THREAD_LIMIT"
  validate_worker_pool POLARS_MAX_THREADS "$POLARS_MAX_THREADS" "$WIKI_ECON_THREAD_LIMIT"
  validate_worker_pool WIKI_ECON_SOURCE_WORKERS "$WIKI_ECON_SOURCE_WORKERS" "$WIKI_ECON_THREAD_LIMIT"
  validate_worker_pool WIKI_ECON_WEEKLY_WORKERS "$WIKI_ECON_WEEKLY_WORKERS" "$WIKI_ECON_THREAD_LIMIT"
  if [[ ! "$WIKI_ECON_FETCH_MAX_PARALLELISM" =~ ^[1-2]$ ]]; then
    echo "WIKI_ECON_FETCH_MAX_PARALLELISM must be 1 or 2 for enwiki qualification (got: $WIKI_ECON_FETCH_MAX_PARALLELISM)" >&2
    exit 2
  fi
  # Enwiki qualification must consume the explicitly frozen snapshot. Never
  # silently advance to a newer dump while a qualification run is pending.
  : "${WIKI_ECON_PREPARE_SNAPSHOT:?enwiki qualification requires WIKI_ECON_PREPARE_SNAPSHOT to pin the frozen snapshot}"
  export WIKI_ECON_SOURCE_WINDOW_SIZE="${WIKI_ECON_SOURCE_WINDOW_SIZE:-$WIKI_ECON_SOURCE_WORKERS}"
  export WIKI_ECON_PERSISTENT_STORAGE_RESERVE_BYTES="${WIKI_ECON_PERSISTENT_STORAGE_RESERVE_BYTES:-268435456000}"
  # A recovered page-week stage may have only the candidate inputs, without a
  # persisted profile. Keep that fallback on enwiki's frozen 2,048-bucket
  # layout instead of silently using the generic 256-bucket default.
  if [ -n "${WIKI_ECON_WEEKLY_BUCKET_COUNT:-}" ]; then
    echo "enwiki qualification requires the explicit 64x32 weekly bucket layout; unset WIKI_ECON_WEEKLY_BUCKET_COUNT" >&2
    exit 2
  fi
  if [ "${WIKI_ECON_WEEKLY_PRIMARY_BUCKET_COUNT:-64}" != 64 ] \
    || [ "${WIKI_ECON_WEEKLY_SECONDARY_BUCKET_COUNT:-32}" != 32 ]; then
    echo "enwiki qualification requires WIKI_ECON_WEEKLY_PRIMARY_BUCKET_COUNT=64 and WIKI_ECON_WEEKLY_SECONDARY_BUCKET_COUNT=32" >&2
    exit 2
  fi
  export WIKI_ECON_WEEKLY_PRIMARY_BUCKET_COUNT=64
  export WIKI_ECON_WEEKLY_SECONDARY_BUCKET_COUNT=32
else
  export RAYON_NUM_THREADS="${RAYON_NUM_THREADS:-1}"
  export POLARS_MAX_THREADS="${POLARS_MAX_THREADS:-1}"
  export WIKI_ECON_THREAD_LIMIT="${WIKI_ECON_THREAD_LIMIT:-1}"
  export WIKI_ECON_SOURCE_WORKERS="${WIKI_ECON_SOURCE_WORKERS:-1}"
  export WIKI_ECON_WEEKLY_WORKERS="${WIKI_ECON_WEEKLY_WORKERS:-1}"
  export WIKI_ECON_MAX_ACTIVE_PARQUET_WRITERS="${WIKI_ECON_MAX_ACTIVE_PARQUET_WRITERS:-16}"
  export WIKI_ECON_REQUESTED_CPU_CORES="${WIKI_ECON_REQUESTED_CPU_CORES:-1}"
  export WIKI_ECON_SOURCE_WINDOW_SIZE="${WIKI_ECON_SOURCE_WINDOW_SIZE:-2}"
  export WIKI_ECON_PERSISTENT_STORAGE_RESERVE_BYTES="${WIKI_ECON_PERSISTENT_STORAGE_RESERVE_BYTES:-53687091200}"
fi
export WIKI_ECON_MEMORY_CEILING_BYTES="${WIKI_ECON_MEMORY_CEILING_BYTES:-6442450944}"
export WIKI_ECON_MEMORY_RESERVE_BYTES="${WIKI_ECON_MEMORY_RESERVE_BYTES:-1610612736}"
export WIKI_ECON_BOUNDED_SCRATCH_RESERVE_BYTES="${WIKI_ECON_BOUNDED_SCRATCH_RESERVE_BYTES:-8589934592}"
export WIKI_ECON_ROLLBACK_GENERATION_RESERVE_BYTES="${WIKI_ECON_ROLLBACK_GENERATION_RESERVE_BYTES:-8589934592}"
export WIKI_ECON_SCRATCH_LIMIT_BYTES="${WIKI_ECON_SCRATCH_LIMIT_BYTES:-68719476736}"
export WIKI_ECON_MAX_OPEN_FILES="${WIKI_ECON_MAX_OPEN_FILES:-512}"
export WIKI_ECON_MAX_LOGICAL_PARTITION_BYTES="${WIKI_ECON_MAX_LOGICAL_PARTITION_BYTES:-8589934592}"

log_dir="$run_root/logs"
mkdir -p "$log_dir" "$qualification_root/_status"
export WIKI_ECON_RUN_RECORD_HELPER="$ROOT/deploy/toolforge/run-record.cjs"
export WIKI_ECON_RUN_EVENTS_FILE="$log_dir/events.jsonl"
export WIKI_ECON_RUN_STATE_FILE="$log_dir/state"
export WIKI_ECON_RUN_SNAPSHOT_FILE="$log_dir/snapshot"
export WIKI_ECON_RUN_STATUS_FILE="$qualification_root/_status/$wiki.json"
export WIKI_ECON_RUN_HISTORY_FILE="$qualification_root/_status/$wiki.history.jsonl"
export WIKI_ECON_RUN_PUBLICATION_FILE="$run_root/publication-gate.json"
export WIKI_ECON_RUN_STARTED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
export WIKI_ECON_RUN_START_EPOCH="$(date +%s)"
export WIKI_ECON_RUN_WIKIS_JSON="$(node -e 'process.stdout.write(JSON.stringify(process.argv.slice(1)))' "$wiki")"
export WIKI_ECON_RUN_LOG_FILE="$log_dir/qualification.log"
export WIKI_ECON_REFRESH_HISTORY_LIMIT="${WIKI_ECON_REFRESH_HISTORY_LIMIT:-104}"
export WIKI_ECON_SITE_DIST_DIR WIKI_ECON_OUTPUT_DIR
exec >>"$WIKI_ECON_RUN_LOG_FILE" 2>&1
report_qualification_exit() {
  local status=$?
  if [ "$status" -ne 0 ]; then
    echo "!!! WIKI QUALIFICATION FAILED wiki=$wiki run_id=$WIKI_ECON_RUN_ID exit_code=$status log_file=$WIKI_ECON_RUN_LOG_FILE status_file=$WIKI_ECON_RUN_STATUS_FILE" >&2
  fi
}
trap report_qualification_exit EXIT

echo "=== wiki qualification start wiki=$wiki run_id=$WIKI_ECON_RUN_ID at=$(date -u +%Y-%m-%dT%H:%M:%SZ) ==="
"$ROOT/deploy/toolforge/run-with-lock.sh" \
  "$qualification_root/_locks/$wiki.lock" \
  "qualification:$wiki" \
  "${WIKI_ECON_QUALIFICATION_LOCK_STALE_SECS:-172800}" \
  "$ROOT/deploy/toolforge/prepare-wiki-transaction.sh" "$wiki"
echo "=== wiki qualification end wiki=$wiki run_id=$WIKI_ECON_RUN_ID at=$(date -u +%Y-%m-%dT%H:%M:%SZ) ==="
