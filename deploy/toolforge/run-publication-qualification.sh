#!/usr/bin/env bash
set -euo pipefail

# Measures the receipt-backed publication_prepare SLO for one changed wiki in
# an isolated hard-linked copy of production. The live data, output, site, and
# lifecycle registry are read-only inputs.

usage() {
  echo "Usage: run-publication-qualification.sh <wiki> [retained-baseline-run-id] [current-schema-overlay-metrics]" >&2
  exit 2
}

[ "$#" -ge 1 ] && [ "$#" -le 3 ] || usage
wiki=$1
baseline_run_id=${2:-}
overlay_metrics_csv=${3:-}
[[ "$wiki" =~ ^[a-z0-9]+wiki$ ]] || usage
[ -z "$baseline_run_id" ] || [[ "$baseline_run_id" =~ ^[A-Za-z0-9._-]+$ ]] || usage
IFS=',' read -r -a overlay_metrics <<< "$overlay_metrics_csv"
if [ -n "$overlay_metrics_csv" ]; then
  for metric in "${overlay_metrics[@]}"; do
    [[ "$metric" =~ ^[a-z0-9_]+\.parquet$ ]] || usage
  done
else
  overlay_metrics=()
fi

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
tool_root=${WIKI_ECON_TOOL_ROOT:-/data/project/wiki-economics}
source_data=${WIKI_ECON_DATA_DIR:-$tool_root/data}
source_output=${WIKI_ECON_OUTPUT_DIR:-$tool_root/output}
capacity_root=${WIKI_ECON_CAPACITY_DIR:-$tool_root/capacity/publication-qualifications}
binary=${WIKI_ECON_BIN:-$tool_root/app/current/wiki-econ}
source_lifecycle=${WIKI_ECON_WIKI_LIFECYCLE_FILE:-$ROOT/config/wiki-lifecycle.json}
run_id="publication-qualification-$wiki-$(date -u +%Y%m%dT%H%M%SZ)-$$"
work_root="$capacity_root/work/$run_id"
report_dir="$capacity_root/reports"
report_path="$report_dir/$run_id.json"

for command in cp find jq node readlink; do
  command -v "$command" >/dev/null 2>&1 || {
    echo "Required publication qualification command is missing: $command" >&2
    exit 1
  }
done
[ -x "$binary" ] || { echo "Publication qualification binary is not executable: $binary" >&2; exit 1; }
[ -d "$source_data" ] && [ -d "$source_output" ] || {
  echo "Production data/output roots are missing" >&2
  exit 1
}
[ -f "$source_lifecycle" ] || { echo "Lifecycle registry is missing: $source_lifecycle" >&2; exit 1; }
[ -L "$source_output/$wiki" ] || { echo "$wiki is not an immutable published candidate" >&2; exit 1; }

current_target=$(readlink "$source_output/$wiki")
current_relative=${current_target%/"$wiki"}
case "$current_relative" in
  _candidates/"$wiki"/*/*) ;;
  *) echo "Unexpected active candidate target for $wiki: $current_target" >&2; exit 1 ;;
esac

if [ -z "$baseline_run_id" ]; then
  baseline_record=$(
    find "$source_output/_generation-state/$wiki" -type f -name '*.json' -exec \
      jq -r 'select(.state == "superseded") | [.snapshot, .run_id] | @tsv' {} + \
      | sort | tail -n 1
  )
  [ -n "$baseline_record" ] || {
    echo "No retained superseded candidate exists for $wiki" >&2
    exit 1
  }
  IFS=$'\t' read -r baseline_snapshot baseline_run_id <<< "$baseline_record"
else
  baseline_matches=("$source_output/_candidates/$wiki"/*/"$baseline_run_id"/ready.json)
  [ "${#baseline_matches[@]}" -eq 1 ] && [ -f "${baseline_matches[0]}" ] || {
    echo "Retained baseline candidate $baseline_run_id does not exist for $wiki" >&2
    exit 1
  }
  baseline_ready=${baseline_matches[0]}
  baseline_snapshot=$(jq -r '.snapshot // empty' "$baseline_ready")
fi
[[ "$baseline_snapshot" =~ ^[0-9]{4}-[0-9]{2}$ ]] || {
  echo "Retained baseline has an invalid snapshot" >&2
  exit 1
}
baseline_relative="_candidates/$wiki/$baseline_snapshot/$baseline_run_id"
baseline_ready="$source_output/$baseline_relative/ready.json"
[ -f "$baseline_ready" ] || {
  echo "Retained baseline ready receipt is missing: $baseline_ready" >&2
  exit 1
}
[ "$(jq -r '.wiki // empty' "$baseline_ready")" = "$wiki" ] \
  && [ "$(jq -r '.run_id // empty' "$baseline_ready")" = "$baseline_run_id" ] || {
  echo "Retained baseline ready identity is invalid" >&2
  exit 1
}
[ "$baseline_relative" != "$current_relative" ] || {
  echo "Retained baseline and active candidate are identical" >&2
  exit 1
}

production_identity() {
  local identity
  identity=$(
  {
    readlink "$source_output/$wiki"
    sha256_file "$source_output/publication-gate.json"
    sha256_file "$source_output/browser-data-index.json"
    sha256_file "$source_output/_ready-index/$wiki.json"
  }
  )
  printf '%s' "$identity" | sha256_stdin
}

sha256_file() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  else
    shasum -a 256 "$1" | awk '{print $1}'
  fi
}

sha256_stdin() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum | awk '{print $1}'
  else
    shasum -a 256 | awk '{print $1}'
  fi
}

production_before=$(production_identity)
mkdir -p "$report_dir" "$capacity_root/work"
[ ! -e "$work_root" ] || { echo "Qualification workspace already exists: $work_root" >&2; exit 1; }
mkdir "$work_root"
cleanup() {
  case "$work_root" in
    "$capacity_root"/work/publication-qualification-*) rm -rf -- "$work_root" ;;
    *) echo "Refusing to clean unexpected qualification path: $work_root" >&2 ;;
  esac
}
trap cleanup EXIT

echo "==> Hard-linking immutable production inputs into $work_root"
cp -al -- "$source_data" "$work_root/data"
cp -al -- "$source_output" "$work_root/output"
cp -- "$source_lifecycle" "$work_root/lifecycle.json"
isolated_data="$work_root/data"
isolated_output="$work_root/output"
isolated_lifecycle="$work_root/lifecycle.json"

# A retained candidate may predate a schema migration in one family even when
# the remaining families form a useful changed/reused qualification pair. In
# that case, replace only the explicitly named incompatible artifact inside the
# isolated baseline with the current artifact and its ready-receipt entry. The
# overlay is recorded in the final report and never touches the live candidate.
if [ "${#overlay_metrics[@]}" -gt 0 ]; then
  baseline_ready_isolated="$isolated_output/$baseline_relative/ready.json"
  current_ready_isolated="$isolated_output/$current_relative/ready.json"
  ready_overlay="$work_root/baseline-ready.overlay.json"
  cp -- "$baseline_ready_isolated" "$ready_overlay"
  for metric in "${overlay_metrics[@]}"; do
    artifact_path="$wiki/$metric"
    baseline_artifact="$isolated_output/$baseline_relative/$artifact_path"
    current_artifact="$isolated_output/$current_relative/$artifact_path"
    [ -f "$baseline_artifact" ] && [ -f "$current_artifact" ] || {
      echo "Compatibility overlay artifact is missing: $artifact_path" >&2
      exit 1
    }
    replacement=$(jq -c --arg path "$artifact_path" \
      '[.artifacts[] | select(.path == $path)] | if length == 1 then .[0] else error("expected one current artifact receipt") end' \
      "$current_ready_isolated")
    jq --arg path "$artifact_path" --argjson replacement "$replacement" \
      '.artifacts |= map(if .path == $path then $replacement else . end)' \
      "$ready_overlay" > "$ready_overlay.next"
    mv "$ready_overlay.next" "$ready_overlay"
    rm -- "$baseline_artifact"
    ln "$current_artifact" "$baseline_artifact"
  done
  mv "$ready_overlay" "$baseline_ready_isolated"
fi

echo "==> Establishing retained $wiki candidate as the isolated baseline"
rm -- "$isolated_output/$wiki"
ln -s "$baseline_relative/$wiki" "$isolated_output/$wiki"
rm -f -- "$isolated_output/_ready-index/$wiki.json"
jq --arg wiki "$wiki" '.wikis[$wiki].refresh = "paused"' "$isolated_lifecycle" \
  > "$work_root/lifecycle.paused.json"
mv "$work_root/lifecycle.paused.json" "$isolated_lifecycle"

baseline_publication="$run_id-baseline"
"$binary" --data-dir "$isolated_data" --output-dir "$isolated_output" \
  --run-id "$baseline_publication" publication-prepare-ready --lifecycle "$isolated_lifecycle"
jq -e '.state == "selected" and (.entries | length) == 0' \
  "$isolated_output/_publication_transactions/$baseline_publication/selection.json" >/dev/null
"$binary" --data-dir "$isolated_data" --output-dir "$isolated_output" \
  --run-id "$baseline_publication" publication-commit-ready

cp -- "$source_lifecycle" "$isolated_lifecycle"
rm -f -- "$isolated_output/_ready-index/$wiki.json"
cp -- "$source_output/_ready-index/$wiki.json" "$isolated_output/_ready-index/$wiki.json"

memory_current_file=/sys/fs/cgroup/memory.current
cpu_stat_file=/sys/fs/cgroup/cpu.stat
memory_peak=0
read_cpu_value() {
  local key=$1
  awk -v key="$key" '$1 == key {print $2}' "$cpu_stat_file" 2>/dev/null || true
}
cpu_before=$(read_cpu_value usage_usec)
throttled_before=$(read_cpu_value throttled_usec)
started_ms=$(node -e 'process.stdout.write(String(Date.now()))')

echo "==> Measuring one-wiki changed publication preparation for $wiki"
"$binary" --data-dir "$isolated_data" --output-dir "$isolated_output" \
  --run-id "$run_id" publication-prepare-ready --lifecycle "$isolated_lifecycle" &
publication_pid=$!
while kill -0 "$publication_pid" 2>/dev/null; do
  if [ -r "$memory_current_file" ]; then
    current_memory=$(cat "$memory_current_file")
    [ "$current_memory" -le "$memory_peak" ] || memory_peak=$current_memory
  fi
  sleep 1
done
if ! wait "$publication_pid"; then
  echo "Changed-one-wiki publication preparation failed" >&2
  exit 1
fi
finished_ms=$(node -e 'process.stdout.write(String(Date.now()))')
duration_ms=$((finished_ms - started_ms))
cpu_after=$(read_cpu_value usage_usec)
throttled_after=$(read_cpu_value throttled_usec)
cpu_usec=$(( ${cpu_after:-0} - ${cpu_before:-0} ))
throttled_usec=$(( ${throttled_after:-0} - ${throttled_before:-0} ))

change_plan="$isolated_output/publication-change-plan.json"
selection="$isolated_output/_publication_transactions/$run_id/selection.json"
jq -e --arg wiki "$wiki" '
  .schema_version == 1
  and (.changed | length) > 0
  and (.reused | length) > 0
  and ([.changed[].wiki] | unique) == [$wiki]
' "$change_plan" >/dev/null
jq -e --arg wiki "$wiki" \
  '.state == "selected" and [.entries[].wiki] == [$wiki]' "$selection" >/dev/null
changed_count=$(jq '.changed | length' "$change_plan")
reused_count=$(jq '.reused | length' "$change_plan")
changed_families=$(jq -c '[.changed[].family]' "$change_plan")
overlay_metrics_json=$(printf '%s\n' "${overlay_metrics[@]}" | jq -Rsc \
  'split("\n") | map(select(length > 0))')

"$binary" --data-dir "$isolated_data" --output-dir "$isolated_output" \
  --run-id "$run_id" publication-rollback-ready --lifecycle "$isolated_lifecycle"

production_after=$(production_identity)
[ "$production_after" = "$production_before" ] || {
  echo "Production publication identity changed during isolated qualification" >&2
  exit 1
}
disk_free_kib=$(df -k "$capacity_root" | awk 'NR == 2 {print $4}')
disk_free_bytes=$((disk_free_kib * 1024))
binary_sha256=$(sha256_file "$binary")
release_provenance="$(dirname "$binary")/release-provenance.json"
if [ -f "$release_provenance" ]; then
  source_commit=$(jq -r '.source_commit // empty' "$release_provenance")
else
  source_commit=${WIKI_ECON_IMAGE_SOURCE_COMMIT:-unknown}
fi
generated_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
slo_ms=180000
slo_passed=false
[ "$duration_ms" -gt "$slo_ms" ] || slo_passed=true

jq -n \
  --arg run_id "$run_id" \
  --arg wiki "$wiki" \
  --arg source_commit "$source_commit" \
  --arg binary_sha256 "$binary_sha256" \
  --arg generated_at "$generated_at" \
  --arg snapshot "$baseline_snapshot" \
  --arg baseline_candidate "$baseline_run_id" \
  --arg active_candidate "${current_relative##*/}" \
  --arg production_identity "$production_after" \
  --argjson duration_ms "$duration_ms" \
  --argjson slo_ms "$slo_ms" \
  --argjson slo_passed "$slo_passed" \
  --argjson changed_families "$changed_families" \
  --argjson changed_count "$changed_count" \
  --argjson reused_count "$reused_count" \
  --argjson baseline_compatibility_overlays "$overlay_metrics_json" \
  --argjson memory_peak_bytes "$memory_peak" \
  --argjson cpu_usec "$cpu_usec" \
  --argjson throttled_usec "$throttled_usec" \
  --argjson disk_free_bytes "$disk_free_bytes" \
  '{
    schema_version: 1,
    mode: "publication-invisible",
    run_id: $run_id,
    wiki: $wiki,
    source_commit: $source_commit,
    binary_sha256: $binary_sha256,
    generated_at: $generated_at,
    snapshot: $snapshot,
    baseline_candidate: $baseline_candidate,
    baseline_compatibility_overlays: $baseline_compatibility_overlays,
    active_candidate: $active_candidate,
    production_identity_before_and_after: $production_identity,
    publication_prepare: {
      duration_ms: $duration_ms,
      slo_ms: $slo_ms,
      slo_passed: $slo_passed,
      changed_families: $changed_families,
      changed_count: $changed_count,
      reused_count: $reused_count,
      cgroup_memory_current_peak_bytes: $memory_peak_bytes,
      cpu_usage_usec: $cpu_usec,
      cpu_throttled_usec: $throttled_usec
    },
    disk_free_bytes_after: $disk_free_bytes,
    production_mutated: false
  }' > "$report_path.tmp"
mv "$report_path.tmp" "$report_path"
echo "Publication qualification report: $report_path"
jq . "$report_path"
[ "$slo_passed" = true ] || {
  echo "Changed-one-wiki publication exceeded the ${slo_ms}ms SLO" >&2
  exit 1
}
