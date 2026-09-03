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
export RAYON_NUM_THREADS="${RAYON_NUM_THREADS:-1}"
export POLARS_MAX_THREADS="${POLARS_MAX_THREADS:-1}"
export WIKI_ECON_THREAD_LIMIT="${WIKI_ECON_THREAD_LIMIT:-1}"
export WIKI_ECON_SOURCE_WORKERS="${WIKI_ECON_SOURCE_WORKERS:-1}"
export WIKI_ECON_MAX_ACTIVE_PARQUET_WRITERS="${WIKI_ECON_MAX_ACTIVE_PARQUET_WRITERS:-16}"
export WIKI_ECON_REQUIRE_QUALIFIED_PROFILE=0
export WIKI_ECON_SOURCE_WINDOW_SIZE="${WIKI_ECON_SOURCE_WINDOW_SIZE:-2}"
export WIKI_ECON_MEMORY_CEILING_BYTES="${WIKI_ECON_MEMORY_CEILING_BYTES:-6442450944}"
export WIKI_ECON_MEMORY_RESERVE_BYTES="${WIKI_ECON_MEMORY_RESERVE_BYTES:-1610612736}"
export WIKI_ECON_PERSISTENT_STORAGE_RESERVE_BYTES="${WIKI_ECON_PERSISTENT_STORAGE_RESERVE_BYTES:-53687091200}"
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

echo "=== wiki qualification start wiki=$wiki run_id=$WIKI_ECON_RUN_ID at=$(date -u +%Y-%m-%dT%H:%M:%SZ) ==="
"$ROOT/deploy/toolforge/run-with-lock.sh" \
  "$qualification_root/_locks/$wiki.lock" \
  "qualification:$wiki" \
  "${WIKI_ECON_QUALIFICATION_LOCK_STALE_SECS:-172800}" \
  "$ROOT/deploy/toolforge/prepare-wiki-transaction.sh" "$wiki"
echo "=== wiki qualification end wiki=$wiki run_id=$WIKI_ECON_RUN_ID at=$(date -u +%Y-%m-%dT%H:%M:%SZ) ==="
