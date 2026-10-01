#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
# shellcheck disable=SC1091
. "$ROOT/scripts/lib/wiki_econ.sh"
wiki_econ_init_runtime
wiki_econ_ensure_local_dirs

: "${WIKI_ECON_BIN:?Toolforge fleet discovery requires WIKI_ECON_BIN}"
export WIKI_ECON_RUN_ID="${WIKI_ECON_RUN_ID:-fleet-controller-$(date -u +%Y%m%dT%H%M%SZ)-$$}"
export CARGO_TERM_COLOR=never NO_COLOR=1 WIKI_ECON_LOG_ANSI=0
queue_dir="${WIKI_ECON_FLEET_QUEUE_DIR:-$WIKI_ECON_OUTPUT_DIR/_fleet}"

report_fleet_controller_exit() {
  local status=$?
  if [ "$status" -ne 0 ]; then
    echo "!!! FLEET CONTROLLER FAILED run_id=$WIKI_ECON_RUN_ID exit_code=$status queue_dir=$queue_dir" >&2
  fi
}
trap report_fleet_controller_exit EXIT

echo "=== fleet controller start run_id=$WIKI_ECON_RUN_ID at=$(date -u +%Y-%m-%dT%H:%M:%SZ) ==="
controller_output="$(wiki_econ_run_cli fleet-discover \
  --lifecycle "$WIKI_ECON_WIKI_LIFECYCLE_FILE" \
  --queue-dir "$queue_dir")"
printf '%s\n' "$controller_output"
# wiki_econ_run_cli prints the invocation before the CLI's machine-readable
# report. Command substitution strips trailing newlines, so the report is the
# final output line.
report="${controller_output##*$'\n'}"
failure_count="$(node -e 'const report=JSON.parse(process.argv[1]); process.stdout.write(String(report.failures?.length || 0));' "$report")"
if [ "$failure_count" -gt 0 ]; then
  echo "Fleet discovery completed with $failure_count per-wiki failure(s); successful tasks remain queued for workers" >&2
  exit 1
fi
echo "=== fleet controller end run_id=$WIKI_ECON_RUN_ID at=$(date -u +%Y-%m-%dT%H:%M:%SZ) ==="
