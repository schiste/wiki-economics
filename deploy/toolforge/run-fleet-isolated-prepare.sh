#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 1 ]; then
  echo "Usage: run-fleet-isolated-prepare.sh WIKI" >&2
  exit 2
fi
wiki=$1
case "$wiki" in
  *[!a-z0-9_]*|'') echo "Unsafe wiki identifier: $wiki" >&2; exit 2 ;;
esac
if [ "$wiki" != "enwiki" ]; then
  echo "No isolated pipeline profile is configured for $wiki" >&2
  exit 2
fi
if [[ ! "${WIKI_ECON_PREPARE_SNAPSHOT:-}" =~ ^[0-9]{4}-[0-9]{2}$ ]]; then
  echo "The isolated fleet task must provide its pinned snapshot" >&2
  exit 2
fi
if [ "${WIKI_ECON_CAPACITY_ADMITTED:-0}" != "1" ]; then
  echo "The isolated pipeline must run under the capacity-admitted worker" >&2
  exit 75
fi

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
stage_wrapper_dir="${WIKI_ECON_FLEET_PIPELINE_WRAPPER_DIR:-$ROOT/deploy/toolforge}"
export WIKI_ECON_PIPELINE_WIKIS="$wiki"
export WIKI_ECON_PIPELINE_STATE_FILE="${WIKI_ECON_PIPELINE_STATE_FILE:-$WIKI_ECON_OUTPUT_DIR/.pipeline-state-enwiki.json}"
export WIKI_ECON_REQUESTED_CPU_CORES=4
export WIKI_ECON_THREAD_LIMIT=4
export RAYON_NUM_THREADS=4
export POLARS_MAX_THREADS=4
export WIKI_ECON_SOURCE_WORKERS=4
export WIKI_ECON_SOURCE_WINDOW_SIZE=4
export WIKI_ECON_WEEKLY_WORKERS=4
export WIKI_ECON_FETCH_MAX_PARALLELISM=2
export WIKI_ECON_MAX_ACTIVE_PARQUET_WRITERS=8
export WIKI_ECON_WEEKLY_PRIMARY_BUCKET_COUNT=64
export WIKI_ECON_WEEKLY_SECONDARY_BUCKET_COUNT=32
export WIKI_ECON_PERSISTENT_STORAGE_RESERVE_BYTES=268435456000
base_run_id="${WIKI_ECON_RUN_ID:?isolated fleet worker requires WIKI_ECON_RUN_ID}"

stage_succeeded() {
  node - "$WIKI_ECON_PIPELINE_STATE_FILE" "$1" "$wiki" "$WIKI_ECON_PREPARE_SNAPSHOT" <<'NODE'
const fs = require("node:fs");
const [file, stage, wiki, snapshot] = process.argv.slice(2);
if (!fs.existsSync(file)) process.exit(1);
let state;
try { state = JSON.parse(fs.readFileSync(file, "utf8")); } catch { process.exit(1); }
if (state?.snapshot !== snapshot
  || !Array.isArray(state.wikis)
  || JSON.stringify([...state.wikis].sort()) !== JSON.stringify([wiki])) process.exit(1);
process.exit(state.stages?.[stage]?.status === "succeeded" ? 0 : 1);
NODE
}

# Keep the heavy stages sequential inside the isolated 6-GiB pod. The existing
# pipeline coordinator supplies stage receipts and refuses an out-of-order or
# mixed-snapshot publish; fleet completion then makes the candidate visible to
# the scheduled global publisher.
stages=(
  "ingest run-refresh-pipeline-ingest.sh"
  "metrics run-refresh-metrics.sh"
  "lifecycle run-refresh-lifecycle.sh"
  "page-week run-refresh-page-week.sh"
  "patrol run-refresh-patrol.sh"
  "publish run-refresh-publish.sh"
)
for stage_entry in "${stages[@]}"; do
  read -r stage stage_wrapper <<< "$stage_entry"
  if stage_succeeded "$stage"; then
    echo "=== isolated pipeline stage already complete stage=$stage wiki=$wiki snapshot=$WIKI_ECON_PREPARE_SNAPSHOT ==="
    continue
  fi
  echo "=== isolated pipeline stage start stage=$stage wiki=$wiki snapshot=$WIKI_ECON_PREPARE_SNAPSHOT ==="
  # Preserve separate run logs and immutable resource receipts for each stage
  # while keeping the pipeline state pinned to the original ingest snapshot.
  export WIKI_ECON_RUN_ID="${base_run_id}-${stage}"
  "$stage_wrapper_dir/$stage_wrapper"
done
export WIKI_ECON_RUN_ID="$base_run_id"
