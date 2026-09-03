#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 2 ] || [ "$#" -gt 3 ]; then
  echo "Usage: run-fleet-worker.sh <small|medium_large> WORKER_ID [--once]" >&2
  exit 2
fi
resource_class=$1
worker_id=$2
once=0
if [ "${3:-}" = "--once" ]; then
  once=1
elif [ "$#" -eq 3 ]; then
  echo "Unknown fleet worker option: $3" >&2
  exit 2
fi
case "$resource_class" in small|medium_large) ;; *) echo "Unsupported fleet resource class: $resource_class" >&2; exit 2 ;; esac
case "$worker_id" in *[!A-Za-z0-9_-]*|'') echo "Unsafe worker ID: $worker_id" >&2; exit 2 ;; esac
cli_resource_class=$resource_class
if [ "$cli_resource_class" = medium_large ]; then
  cli_resource_class=medium-large
fi

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
# shellcheck disable=SC1091
. "$ROOT/scripts/lib/wiki_econ.sh"
wiki_econ_init_runtime
wiki_econ_ensure_local_dirs
: "${WIKI_ECON_BIN:?Toolforge fleet worker requires WIKI_ECON_BIN}"
if [ "$resource_class" = small ]; then
  export WIKI_ECON_MEMORY_CEILING_BYTES="${WIKI_ECON_MEMORY_CEILING_BYTES:-2147483648}"
  export WIKI_ECON_MEMORY_RESERVE_BYTES="${WIKI_ECON_MEMORY_RESERVE_BYTES:-536870912}"
else
  export WIKI_ECON_MEMORY_CEILING_BYTES="${WIKI_ECON_MEMORY_CEILING_BYTES:-6442450944}"
  export WIKI_ECON_MEMORY_RESERVE_BYTES="${WIKI_ECON_MEMORY_RESERVE_BYTES:-1610612736}"
fi

queue_dir="${WIKI_ECON_FLEET_QUEUE_DIR:-$WIKI_ECON_OUTPUT_DIR/_fleet}"
worker_root="$queue_dir/workers/$worker_id"
mkdir -p "$worker_root"
worker_instance_id="${WIKI_ECON_WORKER_INSTANCE_ID:-${HOSTNAME:-}}"
case "$worker_instance_id" in
  *[!A-Za-z0-9_.-]*|'')
    echo "Fleet worker requires a safe, non-empty pod instance identity" >&2
    exit 2
    ;;
esac
idle_secs="${WIKI_ECON_FLEET_IDLE_SECS:-60}"
heartbeat_secs="${WIKI_ECON_FLEET_HEARTBEAT_SECS:-60}"
lease_timeout_secs="${WIKI_ECON_FLEET_LEASE_TIMEOUT_SECS:-900}"
max_attempts="${WIKI_ECON_FLEET_MAX_ATTEMPTS:-3}"
upstream_retry_secs="${WIKI_ECON_UPSTREAM_RETRY_SECS:-21600}"
prepare_lock_stale_secs="${WIKI_ECON_PREPARE_LOCK_STALE_SECS:-$lease_timeout_secs}"
prepare_wrapper="${WIKI_ECON_FLEET_PREPARE_WRAPPER:-$ROOT/deploy/toolforge/run-prepare-wiki.sh}"
for value in "$idle_secs" "$heartbeat_secs" "$lease_timeout_secs" "$max_attempts" "$prepare_lock_stale_secs" "$upstream_retry_secs"; do
  [[ "$value" =~ ^[1-9][0-9]*$ ]] || { echo "Fleet timing and retry settings must be positive integers" >&2; exit 2; }
done
[ -x "$prepare_wrapper" ] || { echo "Fleet preparation wrapper is not executable: $prepare_wrapper" >&2; exit 2; }
export WIKI_ECON_PREPARE_LOCK_STALE_SECS="$prepare_lock_stale_secs"

while true; do
  # PIDs are namespaced per container and are therefore commonly identical
  # across overlapping Toolforge pods. Include the Kubernetes pod hostname so
  # independent runs of the same scheduled worker never share an NFS receipt.
  claim_receipt="$worker_root/claim-$worker_instance_id-$$.json"
  rm -f -- "$claim_receipt"
  "$WIKI_ECON_BIN" \
    --data-dir "$WIKI_ECON_DATA_DIR" \
    --output-dir "$WIKI_ECON_OUTPUT_DIR" \
    fleet-claim \
    --queue-dir "$queue_dir" \
    --resource-class "$cli_resource_class" \
    --worker-id "$worker_id" \
    --lease-timeout-secs "$lease_timeout_secs" \
    --receipt "$claim_receipt"

  if [ ! -f "$claim_receipt" ]; then
    if [ "$once" -eq 1 ]; then
      exit 0
    fi
    sleep "$idle_secs"
    continue
  fi

  read -r wiki snapshot task_id < <(node -e '
const fs = require("node:fs");
const claim = JSON.parse(fs.readFileSync(process.argv[1], "utf8"));
process.stdout.write(`${claim.task.wiki} ${claim.task.snapshot} ${claim.task.task_id}\n`);
' "$claim_receipt")
  heartbeat_pid=""
  task_finished=0
  task_error="worker exited before fleet completion"
  finish_task() {
    local status=$?
    trap - EXIT INT TERM
    if [ -n "$heartbeat_pid" ]; then
      kill "$heartbeat_pid" 2>/dev/null || true
      wait "$heartbeat_pid" 2>/dev/null || true
    fi
    if [ "$task_finished" -eq 0 ] && [ -f "$claim_receipt" ]; then
      "$WIKI_ECON_BIN" \
        --data-dir "$WIKI_ECON_DATA_DIR" \
        --output-dir "$WIKI_ECON_OUTPUT_DIR" \
        fleet-fail --queue-dir "$queue_dir" --receipt "$claim_receipt" \
        --max-attempts "$max_attempts" --error "$task_error" || true
    fi
    rm -f -- "$claim_receipt"
    exit "$status"
  }
  trap finish_task EXIT
  trap 'task_error="fleet worker interrupted"; exit 130' INT
  trap 'task_error="fleet worker terminated"; exit 143' TERM

  (
    while sleep "$heartbeat_secs"; do
      "$WIKI_ECON_BIN" \
        --data-dir "$WIKI_ECON_DATA_DIR" \
        --output-dir "$WIKI_ECON_OUTPUT_DIR" \
        fleet-heartbeat --queue-dir "$queue_dir" --receipt "$claim_receipt" || {
          kill -TERM "$$"
          exit 1
        }
    done
  ) &
  heartbeat_pid=$!

  export WIKI_ECON_RUN_ID="fleet-$worker_id-$wiki-${task_id:0:12}-$(date -u +%Y%m%dT%H%M%SZ)-$$"
  export WIKI_ECON_PREPARE_SNAPSHOT="$snapshot"
  task_error="candidate preparation failed for $wiki/$snapshot"
  set +e
  "$prepare_wrapper" "$wiki"
  prepare_status=$?
  set -e
  if [ "$prepare_status" -eq 75 ]; then
    task_error="waiting for upstream patrol inventory for $wiki/$snapshot"
    "$WIKI_ECON_BIN" \
      --data-dir "$WIKI_ECON_DATA_DIR" \
      --output-dir "$WIKI_ECON_OUTPUT_DIR" \
      fleet-defer --queue-dir "$queue_dir" --receipt "$claim_receipt" \
      --retry-after-secs "$upstream_retry_secs" --reason "$task_error"
    task_finished=1
    kill "$heartbeat_pid" 2>/dev/null || true
    wait "$heartbeat_pid" 2>/dev/null || true
    heartbeat_pid=""
    rm -f -- "$claim_receipt"
    trap - EXIT INT TERM
    unset WIKI_ECON_RUN_ID WIKI_ECON_PREPARE_SNAPSHOT
    if [ "$once" -eq 1 ]; then
      exit 0
    fi
    continue
  fi
  if [ "$prepare_status" -ne 0 ]; then
    exit "$prepare_status"
  fi
  task_error="fleet completion failed for $wiki/$snapshot"
  "$WIKI_ECON_BIN" \
    --data-dir "$WIKI_ECON_DATA_DIR" \
    --output-dir "$WIKI_ECON_OUTPUT_DIR" \
    fleet-complete --queue-dir "$queue_dir" --receipt "$claim_receipt"
  task_finished=1
  kill "$heartbeat_pid" 2>/dev/null || true
  wait "$heartbeat_pid" 2>/dev/null || true
  heartbeat_pid=""
  rm -f -- "$claim_receipt"
  trap - EXIT INT TERM
  unset WIKI_ECON_RUN_ID WIKI_ECON_PREPARE_SNAPSHOT

  if [ "$once" -eq 1 ]; then
    exit 0
  fi
done
