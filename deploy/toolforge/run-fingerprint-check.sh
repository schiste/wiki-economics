#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
# shellcheck disable=SC1091
. "$ROOT/scripts/lib/wiki_econ.sh"
wiki_econ_init_runtime
wiki_econ_ensure_local_dirs

: "${WIKI_ECON_BIN:?Toolforge fingerprint checks require WIKI_ECON_BIN}"
export CARGO_TERM_COLOR=never NO_COLOR=1 OBSERVABLE_TELEMETRY_DISABLE=true WIKI_ECON_LOG_ANSI=0
export RAYON_NUM_THREADS="${RAYON_NUM_THREADS:-1}"
export POLARS_MAX_THREADS="${POLARS_MAX_THREADS:-1}"

run_id="fingerprint-check-$(date -u +%Y%m%dT%H%M%SZ)-$$"
report="$WIKI_ECON_OUTPUT_DIR/fingerprint-drift-alert.json"
log_dir="$WIKI_ECON_OUTPUT_DIR/logs/publication"
mkdir -p "$log_dir"
log_file="$log_dir/$run_id.log"
exec > >(tee -a "$log_file") 2>&1
trap 'status=$?; if [ "$status" -ne 0 ]; then echo "!!! FINGERPRINT DRIFT CHECK FAILED run_id=$run_id report=$report log_file=$log_file" >&2; fi' EXIT
echo "=== fingerprint check start run_id=$run_id at=$(date -u +%Y-%m-%dT%H:%M:%SZ) ==="

"$ROOT/deploy/toolforge/run-with-lock.sh" \
  "$WIKI_ECON_OUTPUT_DIR/.publication.lock" \
  fingerprint-check \
  "${WIKI_ECON_PUBLICATION_LOCK_STALE_SECS:-21600}" \
  "$WIKI_ECON_BIN" \
  --data-dir "$WIKI_ECON_DATA_DIR" \
  --output-dir "$WIKI_ECON_OUTPUT_DIR" \
  publication-fingerprint-check \
  --lifecycle "${WIKI_ECON_WIKI_LIFECYCLE_FILE:-$ROOT/config/wiki-lifecycle.json}" \
  --report "$report"

echo "=== fingerprint check end run_id=$run_id at=$(date -u +%Y-%m-%dT%H:%M:%SZ) report=$report ==="
