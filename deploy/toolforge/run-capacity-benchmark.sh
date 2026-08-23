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

bin_path="${WIKI_ECON_BIN:?WIKI_ECON_BIN is required}"
data_dir="${WIKI_ECON_DATA_DIR:-/data/project/wiki-economics/data}"
capacity_root="${WIKI_ECON_CAPACITY_ROOT:-/data/project/wiki-economics/capacity}"
nfs_quota_bytes="${WIKI_ECON_NFS_QUOTA_BYTES:?Set WIKI_ECON_NFS_QUOTA_BYTES from a confirmed tool-specific quota}"
run_id="capacity-$(date -u +%Y%m%dT%H%M%SZ)-${bucket_count}-$$"
output_dir="$capacity_root/output/$run_id"
scratch_dir="$capacity_root/scratch/$run_id"
report_path="$capacity_root/reports/$wiki/$run_id.json"

mkdir -p "$output_dir" "$scratch_dir" "$(dirname "$report_path")"
export WIKI_ECON_RUN_ID="$run_id"
export WIKI_ECON_LOG_ANSI=0

echo "=== wiki-economics capacity benchmark start run_id=$run_id wiki=$wiki buckets=$bucket_count ==="
"$bin_path" \
  --data-dir "$data_dir" \
  --output-dir "$output_dir" \
  --run-id "$run_id" \
  capacity-bench "$wiki" \
  --weekly-buckets "$bucket_count" \
  --scratch-dir "$scratch_dir" \
  --report "$report_path" \
  --raw-transient-bytes "${WIKI_ECON_FRWIKI_RAW_TRANSIENT_BYTES:-33285996544}" \
  --nfs-quota-bytes "$nfs_quota_bytes" \
  --quota-root /data/project/wiki-economics \
  --minimum-memory-headroom-percent "${WIKI_ECON_CAPACITY_MIN_HEADROOM_PERCENT:-25}"
echo "=== wiki-economics capacity benchmark end run_id=$run_id report=$report_path ==="
