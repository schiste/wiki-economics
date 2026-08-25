#!/usr/bin/env bash
set -euo pipefail

[ "$#" -ge 2 ] || { echo "Usage: run-rebuild-drill.sh <imported-backup.tar.gz> <scheduled-wiki...>" >&2; exit 2; }
backup=$1
shift
wikis=("$@")
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
bin_path="${WIKI_ECON_BIN:?WIKI_ECON_BIN is required}"
data_dir="${WIKI_ECON_DATA_DIR:-/data/project/wiki-economics/data}"
live_output="${WIKI_ECON_OUTPUT_DIR:-/data/project/wiki-economics/output}"
lock_dir="${WIKI_ECON_REFRESH_LOCK_DIR:-$live_output/.refresh-lock}"
[ ! -e "$lock_dir" ] || { echo "Refusing rebuild drill while refresh lock exists" >&2; exit 75; }
operations_root="${WIKI_ECON_OPERATIONS_ROOT:-/data/project/wiki-economics/operations}"
mkdir -p "$operations_root/staging" "$operations_root/reports"
run_id="rebuild-$(date -u +%Y%m%dT%H%M%SZ)-$$"
run_root="$(mktemp -d "$operations_root/staging/${run_id}.XXXXXX")"
output_dir="$run_root/output"
dist_dir="$run_root/site-dist"
cleanup() { rm -rf -- "$run_root"; }
trap cleanup EXIT
started_epoch="$(date +%s)"

"${WIKI_ECON_RESTORE_SCRIPT:-$ROOT/deploy/toolforge/restore-imported-backup.sh}" "$backup" "$output_dir"
WIKI_ECON_RUN_ID="$run_id-compute" WIKI_ECON_SCRATCH_DIR="$run_root/scratch" \
  "$bin_path" --data-dir "$data_dir" --output-dir "$output_dir" compute "${wikis[@]}"
WIKI_ECON_RUN_ID="$run_id" "$bin_path" --data-dir "$data_dir" --output-dir "$output_dir" --run-id "$run_id" merge
WIKI_ECON_RUN_ID="$run_id" "$bin_path" --data-dir "$data_dir" --output-dir "$output_dir" --run-id "$run_id" publication-validate
WIKI_ECON_RUN_ID="$run_id" WIKI_ECON_REQUIRE_PUBLICATION_GATE=1 WIKI_ECON_BIN="$bin_path" \
  "${WIKI_ECON_BUILD_SITE_SCRIPT:-$ROOT/scripts/build-site.sh}" --output-dir "$output_dir" --dist-dir "$dist_dir"

for page in business.html gdp.html inequality.html labor.html patrol.html edit-variation.html; do
  [ -f "$dist_dir/$page" ] || { echo "Rebuild drill site is missing $page" >&2; exit 1; }
done
report="$operations_root/reports/$run_id.json"
WIKI_ECON_DRILL_RUN_ID="$run_id" WIKI_ECON_DRILL_DURATION="$(( $(date +%s) - started_epoch ))" \
WIKI_ECON_DRILL_OUTPUT_BYTES="$(du -sk "$output_dir" | awk '{print $1 * 1024}')" \
WIKI_ECON_DRILL_SITE_BYTES="$(du -sk -L "$dist_dir" | awk '{print $1 * 1024}')" \
WIKI_ECON_DRILL_WIKIS="$(printf '%s\n' "${wikis[@]}" | node -e 'let s="";process.stdin.on("data",c=>s+=c);process.stdin.on("end",()=>process.stdout.write(JSON.stringify(s.trim().split(/\n/).filter(Boolean))))')" \
node - "$report" <<'NODE'
const fs = require("node:fs");
const output = process.argv[2];
const value = {schema_version: 1, drill: "rebuild-from-empty", succeeded: true,
  run_id: process.env.WIKI_ECON_DRILL_RUN_ID, finished_at: new Date().toISOString().replace(/\.\d{3}Z$/, "Z"),
  duration_secs: Number(process.env.WIKI_ECON_DRILL_DURATION), wikis: JSON.parse(process.env.WIKI_ECON_DRILL_WIKIS),
  output_bytes: Number(process.env.WIKI_ECON_DRILL_OUTPUT_BYTES), site_bytes: Number(process.env.WIKI_ECON_DRILL_SITE_BYTES)};
const temporary = `${output}.tmp.${process.pid}`;
fs.writeFileSync(temporary, `${JSON.stringify(value, null, 2)}\n`, {mode: 0o600});
fs.renameSync(temporary, output);
NODE
echo "Rebuild-from-empty drill succeeded; report=$report"
