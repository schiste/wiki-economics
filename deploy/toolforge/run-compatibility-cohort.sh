#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
# shellcheck disable=SC1091
. "$ROOT/scripts/lib/wiki_econ.sh"
wiki_econ_init_runtime
: "${WIKI_ECON_BIN:?Compatibility cohort requires WIKI_ECON_BIN}"

if [ "${1:-}" != "--run-id" ] || [ -z "${2:-}" ] || [ "${3:-}" != "--lifecycle" ] || [ -z "${4:-}" ] || [ "$#" -lt 5 ]; then
  echo "Usage: run-compatibility-cohort.sh --run-id RUN_ID --lifecycle FILE wiki=snapshot [...]" >&2
  exit 2
fi
base_run_id=$2
lifecycle=$4
shift 4

for item in "$@"; do
  wiki=${item%%=*}
  snapshot=${item#*=}
  [[ "$wiki" =~ ^[a-z0-9_]+wiki$ ]] || { echo "Invalid cohort wiki: $wiki" >&2; exit 2; }
  [[ "$snapshot" =~ ^[0-9]{4}-[0-9]{2}$ ]] || { echo "Invalid cohort snapshot: $snapshot" >&2; exit 2; }
  run_id="$base_run_id-$wiki"
  echo "==> Rebuilding compatibility candidate wiki=$wiki snapshot=$snapshot run_id=$run_id"
  "$WIKI_ECON_BIN" \
    --data-dir "$WIKI_ECON_DATA_DIR" \
    --output-dir "$WIKI_ECON_OUTPUT_DIR" \
    --run-id "$run_id" \
    prepare-wiki "$wiki" \
    --version "$snapshot" \
    --source-window-size 1 \
    --rebuild \
    --lifecycle "$lifecycle"
done
