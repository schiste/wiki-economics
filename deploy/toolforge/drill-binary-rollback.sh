#!/usr/bin/env bash
set -euo pipefail

[ "$#" -eq 1 ] || { echo "Usage: drill-binary-rollback.sh <retained-candidate-sha>" >&2; exit 2; }
candidate=$1
[[ "$candidate" =~ ^[0-9a-f]{40}$ ]] || { echo "Invalid candidate SHA" >&2; exit 2; }
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
app_root="${WIKI_ECON_TOOLFORGE_APP_ROOT:-/data/project/wiki-economics/app}"
output_dir="${WIKI_ECON_OUTPUT_DIR:-/data/project/wiki-economics/output}"
lock_dir="${WIKI_ECON_REFRESH_LOCK_DIR:-$output_dir/.refresh-lock}"
[ ! -e "$lock_dir" ] || { echo "Refusing rollback drill while refresh lock exists" >&2; exit 75; }
current_target="$(readlink "$app_root/current")"
case "$current_target" in releases/*) original="${current_target#releases/}" ;; *) echo "Current release symlink is malformed" >&2; exit 1 ;; esac
[[ "$original" =~ ^[0-9a-f]{40}$ ]] || { echo "Current release SHA is malformed" >&2; exit 1; }
[ "$candidate" != "$original" ] || { echo "Rollback candidate must differ from current release" >&2; exit 1; }
rollback="${WIKI_ECON_ROLLBACK_SCRIPT:-$ROOT/deploy/toolforge/rollback-binary.sh}"
restored=0
restore_original() {
  if [ "$restored" -eq 0 ]; then
    WIKI_ECON_TOOLFORGE_APP_ROOT="$app_root" "$rollback" "$original" >/dev/null || true
  fi
}
trap restore_original EXIT

started_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
WIKI_ECON_TOOLFORGE_APP_ROOT="$app_root" "$rollback" "$candidate" >/dev/null
[ "$(readlink "$app_root/current")" = "releases/$candidate" ]
"$app_root/current/wiki-econ" --help >/dev/null
candidate_checksum="$(sha256sum "$app_root/current/wiki-econ" | awk '{print $1}')"
WIKI_ECON_TOOLFORGE_APP_ROOT="$app_root" "$rollback" "$original" >/dev/null
[ "$(readlink "$app_root/current")" = "releases/$original" ]
"$app_root/current/wiki-econ" --help >/dev/null
original_checksum="$(sha256sum "$app_root/current/wiki-econ" | awk '{print $1}')"
restored=1

report_dir="${WIKI_ECON_OPERATIONS_REPORT_DIR:-/data/project/wiki-economics/operations/reports}"
mkdir -p "$report_dir"
report="$report_dir/rollback-$(date -u +%Y%m%dT%H%M%SZ)-$$.json"
WIKI_ECON_DRILL_STARTED_AT="$started_at" WIKI_ECON_DRILL_ORIGINAL="$original" \
WIKI_ECON_DRILL_CANDIDATE="$candidate" WIKI_ECON_DRILL_ORIGINAL_SHA="$original_checksum" \
WIKI_ECON_DRILL_CANDIDATE_SHA="$candidate_checksum" node - "$report" <<'NODE'
const fs = require("node:fs");
const output = process.argv[2];
const value = {schema_version: 1, drill: "binary-rollback", succeeded: true,
  started_at: process.env.WIKI_ECON_DRILL_STARTED_AT,
  finished_at: new Date().toISOString().replace(/\.\d{3}Z$/, "Z"),
  original_release: process.env.WIKI_ECON_DRILL_ORIGINAL,
  rollback_release: process.env.WIKI_ECON_DRILL_CANDIDATE,
  original_binary_sha256: process.env.WIKI_ECON_DRILL_ORIGINAL_SHA,
  rollback_binary_sha256: process.env.WIKI_ECON_DRILL_CANDIDATE_SHA,
  restored_release: process.env.WIKI_ECON_DRILL_ORIGINAL};
const temporary = `${output}.tmp.${process.pid}`;
fs.writeFileSync(temporary, `${JSON.stringify(value, null, 2)}\n`, {mode: 0o600});
fs.renameSync(temporary, output);
NODE
echo "Rollback drill succeeded and restored $original; report=$report"
