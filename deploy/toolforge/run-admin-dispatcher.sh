#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
# shellcheck disable=SC1091
. "$ROOT/scripts/lib/wiki_econ.sh"
wiki_econ_init_runtime
wiki_econ_ensure_local_dirs

mode=${1:---loop}
case "$mode" in
  --once|--loop) ;;
  *) echo "Usage: run-admin-dispatcher.sh [--once|--loop]" >&2; exit 2 ;;
esac

operation_root="${WIKI_ECON_ADMIN_OPERATION_DIR:-$WIKI_ECON_OUTPUT_DIR/_admin/operations}"
mkdir -p "$operation_root/queued" "$operation_root/running" "$operation_root/history" "$operation_root/logs"

if [ "$mode" = "--once" ]; then
  exec "$ROOT/deploy/toolforge/run-with-lock.sh" \
    "$WIKI_ECON_OUTPUT_DIR/.admin-dispatcher.lock" \
    admin-dispatcher \
    "${WIKI_ECON_ADMIN_DISPATCHER_LOCK_STALE_SECS:-900}" \
    node "$ROOT/deploy/toolforge/admin-dispatcher.cjs"
fi

# Local/manual loop mode is useful for development. Production uses --once
# on a staggered schedule so an idle queue reserves no Toolforge memory.
# shellcheck disable=SC2016
exec "$ROOT/deploy/toolforge/run-with-lock.sh" \
  "$WIKI_ECON_OUTPUT_DIR/.admin-dispatcher.lock" \
  admin-dispatcher \
  "${WIKI_ECON_ADMIN_DISPATCHER_LOCK_STALE_SECS:-900}" \
  bash -c '
    dispatcher=$1
    interval=${WIKI_ECON_ADMIN_DISPATCH_INTERVAL_SECS:-10}
    while true; do
      node "$dispatcher" || true
      sleep "$interval"
    done
  ' _ "$ROOT/deploy/toolforge/admin-dispatcher.cjs"
