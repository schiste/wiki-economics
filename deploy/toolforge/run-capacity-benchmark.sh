#!/usr/bin/env bash
set -euo pipefail

# One-off, read-only capacity qualification for a paused wiki. This command
# writes isolated benchmark outputs/reports and never changes lifecycle state,
# snapshot pointers, merged metrics, or the published site.

if [ "$#" -ne 2 ]; then
  echo "Usage: run-capacity-benchmark.sh <wiki> <256|512|1024>" >&2
  exit 2
fi

wiki=$1
bucket_count=$2
case "$bucket_count" in
  256|512|1024) ;;
  *)
    echo "Bucket count must be 256, 512, or 1024" >&2
    exit 2
    ;;
esac

requested_cpu="${WIKI_ECON_REQUESTED_CPU:-1}"
case "$requested_cpu" in
  1|2|4) ;;
  *) echo "Requested CPU must be 1, 2, or 4" >&2; exit 2 ;;
esac
case "$requested_cpu" in
  1) default_threads=1 ;;
  2) default_threads=2 ;;
  4) default_threads=3 ;;
esac
qualification_threads="${WIKI_ECON_QUALIFICATION_THREADS:-$default_threads}"
weekly_workers="${WIKI_ECON_WEEKLY_WORKERS:-1}"
case "$requested_cpu:$qualification_threads:$weekly_workers" in
  1:1:1|2:2:1|4:3:1|4:3:2) ;;
  *)
    echo "Unsupported qualification cell: cpu=$requested_cpu threads=$qualification_threads weekly_workers=$weekly_workers" >&2
    exit 2
    ;;
esac

bin_path="${WIKI_ECON_BIN:?WIKI_ECON_BIN is required}"
data_dir="${WIKI_ECON_DATA_DIR:-/data/project/wiki-economics/data}"
capacity_root="${WIKI_ECON_CAPACITY_ROOT:-/data/project/wiki-economics/capacity}"
root="$(cd "$(dirname "$0")/../.." && pwd)"
policy="${WIKI_ECON_CAPACITY_POLICY:-$root/config/capacity-qualification.json}"
raw_transient_bytes="$(node -e '
const fs = require("node:fs");
const policy = JSON.parse(fs.readFileSync(process.argv[1], "utf8"));
const entry = policy.wikis?.[process.argv[2]];
if (!entry?.required_bucket_counts?.includes(Number(process.argv[3]))) process.exit(2);
process.stdout.write(String(entry.raw_transient_requirement_bytes));
' "$policy" "$wiki" "$bucket_count")" || {
  echo "Wiki/bucket combination is absent from capacity policy: $wiki/$bucket_count" >&2
  exit 2
}
run_id="capacity-$(date -u +%Y%m%dT%H%M%SZ)-c${requested_cpu}-t${qualification_threads}-w${weekly_workers}-b${bucket_count}-$$"
output_dir="$capacity_root/output/$run_id"
scratch_dir="$capacity_root/scratch/$run_id"
report_path="$capacity_root/reports/$wiki/$run_id.json"

case "$capacity_root" in
  ""|/) echo "Refusing unsafe capacity root: $capacity_root" >&2; exit 1 ;;
esac
[[ "$run_id" =~ ^capacity-[A-Za-z0-9._-]+$ ]] || {
  echo "Refusing unsafe capacity run ID: $run_id" >&2
  exit 1
}

cleanup_capacity_staging() {
  rm -rf -- "$output_dir" "$scratch_dir"
}
trap cleanup_capacity_staging EXIT

mkdir -p "$output_dir" "$scratch_dir" "$(dirname "$report_path")"
export WIKI_ECON_RUN_ID="$run_id"
export WIKI_ECON_LOG_ANSI=0
export RAYON_NUM_THREADS="$qualification_threads"
export POLARS_MAX_THREADS="$qualification_threads"
export WIKI_ECON_THREAD_LIMIT="$qualification_threads"
if [[ -z "${WIKI_ECON_SOURCE_COMMIT:-}" ]]; then
  release_target="$(readlink "$(dirname "$bin_path")" 2>/dev/null || true)"
  source_commit="$(basename "$release_target")"
  export WIKI_ECON_SOURCE_COMMIT="$source_commit"
fi

quota_args=()
if [[ -n "${WIKI_ECON_NFS_QUOTA_BYTES:-}" ]]; then
  quota_args=(--nfs-quota-bytes "$WIKI_ECON_NFS_QUOTA_BYTES")
fi

echo "=== wiki-economics capacity benchmark start run_id=$run_id wiki=$wiki buckets=$bucket_count cpu=$requested_cpu threads=$qualification_threads weekly_workers=$weekly_workers ==="
"$bin_path" \
  --data-dir "$data_dir" \
  --output-dir "$output_dir" \
  --run-id "$run_id" \
  capacity-bench "$wiki" \
  --weekly-buckets "$bucket_count" \
  --weekly-secondary-buckets "${WIKI_ECON_WEEKLY_SECONDARY_BUCKETS:-1}" \
  --requested-cpu "$requested_cpu" \
  --scratch-dir "$scratch_dir" \
  --report "$report_path" \
  --raw-transient-bytes "${WIKI_ECON_CAPACITY_RAW_TRANSIENT_BYTES:-$raw_transient_bytes}" \
  ${quota_args[@]+"${quota_args[@]}"} \
  --storage-reserve-bytes "${WIKI_ECON_CAPACITY_STORAGE_RESERVE_BYTES:-53687091200}" \
  --quota-root /data/project/wiki-economics \
  --minimum-memory-headroom-percent "${WIKI_ECON_CAPACITY_MIN_HEADROOM_PERCENT:-25}"
echo "=== wiki-economics capacity benchmark end run_id=$run_id report=$report_path ==="
