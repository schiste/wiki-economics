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

echo "=== fleet controller start run_id=$WIKI_ECON_RUN_ID at=$(date -u +%Y-%m-%dT%H:%M:%SZ) ==="
wiki_econ_run_cli fleet-discover \
  --lifecycle "$WIKI_ECON_WIKI_LIFECYCLE_FILE" \
  --queue-dir "$queue_dir"
echo "=== fleet controller end run_id=$WIKI_ECON_RUN_ID at=$(date -u +%Y-%m-%dT%H:%M:%SZ) ==="
