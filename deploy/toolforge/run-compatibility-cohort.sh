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
cohort_total=$#
cohort_index=0
failures=()
binary_identity=${WIKI_ECON_BINARY_SOURCE_COMMIT:-local}
[[ "$binary_identity" =~ ^[a-f0-9]{40}$ ]] || binary_identity=local
completion_dir="$WIKI_ECON_OUTPUT_DIR/_admin/compatibility-completions"
mkdir -p "$completion_dir"

run_candidate() {
  local candidate_wiki=$1
  local candidate_snapshot=$2
  local candidate_run_id=$3
  shift 3
  "$WIKI_ECON_BIN" \
    --data-dir "$WIKI_ECON_DATA_DIR" \
    --output-dir "$WIKI_ECON_OUTPUT_DIR" \
    --run-id "$candidate_run_id" \
    prepare-wiki "$candidate_wiki" \
    --version "$candidate_snapshot" \
    --source-window-size 1 \
    "$@" \
    --lifecycle "$lifecycle"
}

for item in "$@"; do
  cohort_index=$((cohort_index + 1))
  wiki=${item%%=*}
  snapshot=${item#*=}
  [[ "$wiki" =~ ^[a-z0-9_]+wiki$ ]] || { echo "Invalid cohort wiki: $wiki" >&2; exit 2; }
  [[ "$snapshot" =~ ^[0-9]{4}-[0-9]{2}$ ]] || { echo "Invalid cohort snapshot: $snapshot" >&2; exit 2; }
  run_id="$base_run_id-$wiki"
  completion_marker="$completion_dir/$wiki.ready"
  completion_identity="$binary_identity $snapshot"
  if [ -f "$completion_marker" ] && [ "$(cat "$completion_marker")" = "$completion_identity" ]; then
    rebuild=0
    echo "==> Revalidating retained compatibility candidate wiki=$wiki snapshot=$snapshot run_id=$run_id cohort_index=$cohort_index cohort_total=$cohort_total"
  else
    rebuild=1
    echo "==> Rebuilding compatibility candidate wiki=$wiki snapshot=$snapshot run_id=$run_id cohort_index=$cohort_index cohort_total=$cohort_total"
  fi
  candidate_succeeded=0
  exit_code=0
  if [ "$rebuild" -eq 0 ]; then
    if run_candidate "$wiki" "$snapshot" "$run_id"; then
      candidate_succeeded=1
    else
      exit_code=$?
    fi
  elif run_candidate "$wiki" "$snapshot" "$run_id" --rebuild; then
    candidate_succeeded=1
  else
    exit_code=$?
  fi
  if [ "$candidate_succeeded" -eq 1 ]; then
    completion_tmp="$completion_marker.$$"
    printf '%s\n' "$completion_identity" >"$completion_tmp"
    mv "$completion_tmp" "$completion_marker"
    echo "==> Compatibility candidate ready wiki=$wiki snapshot=$snapshot cohort_index=$cohort_index cohort_total=$cohort_total"
  else
    failures+=("$wiki:$exit_code")
    echo "==> Compatibility candidate failed wiki=$wiki snapshot=$snapshot exit_code=$exit_code; continuing with the remaining cohort" >&2
  fi
done

if [ "${#failures[@]}" -gt 0 ]; then
  echo "Error: compatibility cohort completed with failed members: ${failures[*]}; successful candidates were retained" >&2
  exit 1
fi
