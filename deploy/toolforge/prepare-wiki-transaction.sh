#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 1 ]; then
  echo "Usage: prepare-wiki-transaction.sh WIKI" >&2
  exit 2
fi
wiki=$1
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
# shellcheck disable=SC1091
. "$ROOT/scripts/lib/wiki_econ.sh"
wiki_econ_init_runtime

failure_error=""
: > "$WIKI_ECON_RUN_EVENTS_FILE"
: > "$WIKI_ECON_RUN_SNAPSHOT_FILE"
printf '%s\n' starting > "$WIKI_ECON_RUN_STATE_FILE"
node "$WIKI_ECON_RUN_RECORD_HELPER" write
printf '%s\n' running > "$WIKI_ECON_RUN_STATE_FILE"

finish_candidate() {
  local status=$?
  trap - EXIT
  if [ "$status" -ne 0 ]; then
    WIKI_ECON_RUN_ERROR="${failure_error:-candidate preparation exited with status $status}" \
      node "$WIKI_ECON_RUN_RECORD_HELPER" finish "$status" || true
  else
    node "$WIKI_ECON_RUN_RECORD_HELPER" finish 0
  fi
  exit "$status"
}
trap finish_candidate EXIT
trap 'failure_error="command failed: $BASH_COMMAND"' ERR

if [ -n "${WIKI_ECON_PREPARE_SNAPSHOT:-}" ]; then
  selected_snapshot="$WIKI_ECON_PREPARE_SNAPSHOT"
else
  selected_snapshot="$(
    RUST_LOG=error "$WIKI_ECON_BIN" \
      --data-dir "$WIKI_ECON_DATA_DIR" \
      --output-dir "$WIKI_ECON_OUTPUT_DIR" \
      --run-id "$WIKI_ECON_RUN_ID" \
      snapshot-resolve "$wiki"
  )"
fi
if [[ ! "$selected_snapshot" =~ ^[0-9]{4}-[0-9]{2}$ ]]; then
  echo "Snapshot resolver returned an invalid version: $selected_snapshot" >&2
  exit 1
fi
printf '%s\n' "$selected_snapshot" > "$WIKI_ECON_RUN_SNAPSHOT_FILE.tmp"
mv "$WIKI_ECON_RUN_SNAPSHOT_FILE.tmp" "$WIKI_ECON_RUN_SNAPSHOT_FILE"
node "$WIKI_ECON_RUN_RECORD_HELPER" write

prepare_command="${WIKI_ECON_PREPARE_COMMAND:-prepare-wiki}"
case "$prepare_command" in
  prepare-wiki|qualify-wiki) ;;
  *) echo "Unsupported preparation command: $prepare_command" >&2; exit 2 ;;
esac
wiki_econ_run_cli "$prepare_command" "$wiki" --version "$selected_snapshot" \
  --source-window-size "$WIKI_ECON_SOURCE_WINDOW_SIZE" \
  --lifecycle "$WIKI_ECON_WIKI_LIFECYCLE_FILE"
