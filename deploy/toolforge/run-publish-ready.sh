#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
# shellcheck disable=SC1091
. "$ROOT/scripts/lib/wiki_econ.sh"
wiki_econ_init_runtime
wiki_econ_ensure_local_dirs

: "${WIKI_ECON_BIN:?Toolforge publication requires WIKI_ECON_BIN}"
export WIKI_ECON_RUN_ID="${WIKI_ECON_RUN_ID:-publish-$(date -u +%Y%m%dT%H%M%SZ)-$$}"
export CARGO_TERM_COLOR=never NO_COLOR=1 OBSERVABLE_TELEMETRY_DISABLE=true WIKI_ECON_LOG_ANSI=0
export RAYON_NUM_THREADS="${RAYON_NUM_THREADS:-1}"
export POLARS_MAX_THREADS="${POLARS_MAX_THREADS:-1}"

log_dir="$WIKI_ECON_OUTPUT_DIR/logs/publication"
mkdir -p "$log_dir"
export WIKI_ECON_RUN_RECORD_HELPER="$ROOT/deploy/toolforge/run-record.cjs"
export WIKI_ECON_RUN_EVENTS_FILE="$log_dir/$WIKI_ECON_RUN_ID.events.jsonl"
export WIKI_ECON_RUN_STATE_FILE="$log_dir/$WIKI_ECON_RUN_ID.state"
export WIKI_ECON_RUN_SNAPSHOT_FILE="$log_dir/$WIKI_ECON_RUN_ID.snapshot"
export WIKI_ECON_RUN_STATUS_FILE="$WIKI_ECON_OUTPUT_DIR/.refresh-status.json"
export WIKI_ECON_RUN_HISTORY_FILE="$WIKI_ECON_OUTPUT_DIR/.refresh-history.jsonl"
export WIKI_ECON_RUN_PUBLICATION_FILE="$WIKI_ECON_OUTPUT_DIR/publication-gate.json"
export WIKI_ECON_RUN_STARTED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
export WIKI_ECON_RUN_START_EPOCH="$(date +%s)"
published_wikis=()
while IFS= read -r wiki; do
  [ -n "$wiki" ] && published_wikis+=("$wiki")
done < <(node "$ROOT/scripts/wiki-lifecycle.cjs" published-wikis)
export WIKI_ECON_RUN_WIKIS_JSON="$(node -e 'process.stdout.write(JSON.stringify(process.argv.slice(1)))' "${published_wikis[@]}")"
export WIKI_ECON_RUN_LOG_FILE="$log_dir/$WIKI_ECON_RUN_ID.log"
export WIKI_ECON_REFRESH_HISTORY_LIMIT="${WIKI_ECON_REFRESH_HISTORY_LIMIT:-104}"
export WIKI_ECON_SITE_DIST_DIR WIKI_ECON_OUTPUT_DIR
exec > >(tee -a "$log_dir/$WIKI_ECON_RUN_ID.log") 2>&1
echo "=== publication start run_id=$WIKI_ECON_RUN_ID at=$(date -u +%Y-%m-%dT%H:%M:%SZ) ==="

"$ROOT/deploy/toolforge/run-with-lock.sh" \
  "$WIKI_ECON_OUTPUT_DIR/.publication-lock" \
  publication \
  "${WIKI_ECON_PUBLICATION_LOCK_STALE_SECS:-21600}" \
  "$ROOT/deploy/toolforge/publish-ready-transaction.sh"

echo "=== publication end run_id=$WIKI_ECON_RUN_ID at=$(date -u +%Y-%m-%dT%H:%M:%SZ) ==="
