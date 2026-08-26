#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
# shellcheck disable=SC1091
. "$ROOT/scripts/lib/wiki_econ.sh"
wiki_econ_init_runtime
wiki_econ_ensure_local_dirs

: "${WIKI_ECON_BIN:?Toolforge artifact scrub requires WIKI_ECON_BIN}"
run_id="${WIKI_ECON_RUN_ID:-scrub-$(date -u +%Y%m%dT%H%M%SZ)-$$}"
report_dir="$WIKI_ECON_OUTPUT_DIR/_scrubs"
log_dir="$WIKI_ECON_OUTPUT_DIR/logs/scrub"
mkdir -p "$report_dir" "$log_dir"
export CARGO_TERM_COLOR=never NO_COLOR=1 OBSERVABLE_TELEMETRY_DISABLE=true WIKI_ECON_LOG_ANSI=0

exec > >(tee -a "$log_dir/$run_id.log") 2>&1
echo "=== artifact scrub start run_id=$run_id at=$(date -u +%Y-%m-%dT%H:%M:%SZ) ==="
"$ROOT/deploy/toolforge/run-with-lock.sh" \
  "$WIKI_ECON_OUTPUT_DIR/.publication.lock" \
  artifact-scrub \
  "${WIKI_ECON_SCRUB_LOCK_STALE_SECS:-21600}" \
  "$WIKI_ECON_BIN" \
  --data-dir "$WIKI_ECON_DATA_DIR" \
  --output-dir "$WIKI_ECON_OUTPUT_DIR" \
  --run-id "$run_id" \
  artifact-scrub \
  --report "$report_dir/$run_id.json"
echo "=== artifact scrub end run_id=$run_id at=$(date -u +%Y-%m-%dT%H:%M:%SZ) ==="
