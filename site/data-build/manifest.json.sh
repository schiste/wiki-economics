#!/bin/bash
# Observable data loader — scans the pipeline directories and outputs status as JSON.

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$SCRIPT_DIR"
while [ "$ROOT" != "/" ] && [ ! -f "$ROOT/Cargo.toml" ]; do
  ROOT="$(dirname "$ROOT")"
done

DATA_DIR="${WIKI_ECON_DATA_DIR:-$ROOT/data}"
OUTPUT_DIR="${WIKI_ECON_OUTPUT_DIR:-$ROOT/output}"

json_file_list() {
  local glob_root="$1"
  printf '['
  local first_file=true
  for f in "$glob_root"/*.parquet; do
    [ -f "$f" ] || continue
    $first_file || printf ","
    first_file=false
    local name fsize fsize_kb
    name=$(basename "$f" .parquet)
    fsize=$(stat -f%z "$f" 2>/dev/null || stat -c%s "$f" 2>/dev/null)
    fsize_kb=$((fsize / 1024))
    printf '{"name":"%s","size_kb":%d}' "$name" "$fsize_kb"
  done
  printf ']'
}

merged_count() {
  ls "$OUTPUT_DIR"/*.parquet 2>/dev/null | wc -l | tr -d ' '
}

ALL_WIKIS=""
for d in "$DATA_DIR/raw"/*/ "$DATA_DIR/parquet"/*/ "$DATA_DIR/warehouse"/*/ "$DATA_DIR/patrol"/*/ "$OUTPUT_DIR"/*/; do
  [ -d "$d" ] || continue
  ALL_WIKIS="$ALL_WIKIS $(basename "$d")"
done
ALL_WIKIS=$(echo "$ALL_WIKIS" | tr ' ' '\n' | sort -u | grep -v '^$')

echo "{"
printf '  "generated_at": "%s",\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
printf '  "data_dir": "%s",\n' "$DATA_DIR"
printf '  "output_dir": "%s",\n' "$OUTPUT_DIR"

echo '  "wikis": {'
first_wiki=true
for wiki in $ALL_WIKIS; do
  $first_wiki || echo ","
  first_wiki=false
  printf '    "%s": {\n' "$wiki"

  raw_dir="$DATA_DIR/raw/$wiki"
  raw_count=0
  if [ -d "$raw_dir" ]; then
    raw_count=$(ls "$raw_dir"/*.bz2 2>/dev/null | wc -l | tr -d ' ')
    raw_size=$(du -sh "$raw_dir" 2>/dev/null | cut -f1 | tr -d ' ')
    dump_version=$(ls "$raw_dir"/*.bz2 2>/dev/null | head -1 | xargs basename 2>/dev/null | sed 's/\..*//' | grep -oE '[0-9]{4}-[0-9]{2}')
    [ -z "$dump_version" ] && dump_version="unknown"
    printf '      "raw": {"files": %d, "size": "%s", "version": "%s", "details": [' "$raw_count" "$raw_size" "$dump_version"
    first_f=true
    for f in "$raw_dir"/*.bz2; do
      [ -f "$f" ] || continue
      $first_f || printf ","
      first_f=false
      fname=$(basename "$f")
      fsize=$(du -h "$f" 2>/dev/null | cut -f1 | tr -d ' ')
      fdate=$(stat -f%Sm -t%Y-%m-%d "$f" 2>/dev/null || stat -c%y "$f" 2>/dev/null | cut -d' ' -f1)
      printf '{"name":"%s","size":"%s","date":"%s"}' "$fname" "$fsize" "$fdate"
    done
    printf ']},\n'
  else
    printf '      "raw": {"files": 0, "size": "0", "version": null, "details": []},\n'
  fi

  pq_dir="$DATA_DIR/parquet/$wiki"
  marker_dir="$pq_dir/_markers"
  done_pq=0
  tmp_count=0
  pq_total=$raw_count
  if [ -d "$pq_dir" ]; then
    if [ -d "$marker_dir" ]; then
      done_pq=$(/usr/bin/find "$marker_dir" -maxdepth 1 -type f -name '*.done' | wc -l | tr -d ' ')
    fi
    if [ "$done_pq" -gt "$pq_total" ]; then
      pq_total=$done_pq
    fi
    pq_size=$(du -sh "$pq_dir" 2>/dev/null | cut -f1 | tr -d ' ')
    tmp_count=$(/usr/bin/find "$pq_dir" -type f -name '*.tmp' ! -path '*/_markers/*' | wc -l | tr -d ' ')
    printf '      "parquet": {"done": %d, "total": %d, "size": "%s", "in_progress": %d, "missing": [' "$done_pq" "$pq_total" "$pq_size" "$tmp_count"
    first_m=true
    if [ -d "$raw_dir" ]; then
      for raw_f in "$raw_dir"/*.bz2; do
        [ -f "$raw_f" ] || continue
        base=$(basename "$raw_f" .tsv.bz2)
        if [ ! -f "$marker_dir/$base.done" ]; then
          $first_m || printf ","
          first_m=false
          printf '"%s"' "$(basename "$raw_f")"
        fi
      done
    fi
    printf ']},\n'
  else
    printf '      "parquet": {"done": 0, "total": 0, "size": "0", "in_progress": 0, "missing": []},\n'
  fi

  metrics_dir="$OUTPUT_DIR/$wiki"
  core_required_metrics="business_funnel gdp gdp_activity_tiers gdp_user_type_share inequality labor_churn labor_cohorts labor_monthly"
  missing_core_metric_count=0
  missing_patrol_metric=1
  if [ -d "$metrics_dir" ]; then
    printf '      "metrics": ['
    first_f=true
    for f in "$metrics_dir"/*.parquet; do
      [ -f "$f" ] || continue
      $first_f || printf ","
      first_f=false
      name=$(basename "$f" .parquet)
      fsize=$(stat -f%z "$f" 2>/dev/null || stat -c%s "$f" 2>/dev/null)
      fsize_kb=$((fsize / 1024))
      printf '{"name":"%s","size_kb":%d}' "$name" "$fsize_kb"
    done
    printf '],\n'
    for metric in $core_required_metrics; do
      [ -f "$metrics_dir/$metric.parquet" ] || missing_core_metric_count=$((missing_core_metric_count + 1))
    done
    [ -f "$metrics_dir/patrol.parquet" ] && missing_patrol_metric=0
  else
    printf '      "metrics": [],\n'
    missing_core_metric_count=8
  fi

  merged_ready=$(merged_count)
  printf '      "dashboard": %s,\n' "$(json_file_list "$OUTPUT_DIR")"

  patrol_dir="$DATA_DIR/patrol/$wiki"
  patrol_xml_ready=0
  patrol_events_ready=0
  patrol_rights_ready=0
  patrol_groups_ready=0
  [ -f "$patrol_dir/$wiki-latest-pages-logging.xml.gz" ] && patrol_xml_ready=1
  [ -f "$patrol_dir/patrol.parquet" ] && patrol_events_ready=1
  [ -f "$patrol_dir/rights.parquet" ] && patrol_rights_ready=1
  [ -f "$patrol_dir/autopatrol_groups.json" ] && patrol_groups_ready=1
  patrol_source_ready=0
  if [ "$patrol_xml_ready" -eq 1 ] && [ "$patrol_events_ready" -eq 1 ] && [ "$patrol_rights_ready" -eq 1 ] && [ "$patrol_groups_ready" -eq 1 ]; then
    patrol_source_ready=1
  fi
  printf '      "patrol": {"xml": %d, "events": %d, "rights": %d, "groups": %d, "source_ready": %d, "metric_ready": %d},\n' \
    "$patrol_xml_ready" "$patrol_events_ready" "$patrol_rights_ready" "$patrol_groups_ready" "$patrol_source_ready" "$((1 - missing_patrol_metric))"

  if [ "$raw_count" -eq 0 ]; then
    status="needs_fetch"
  elif [ "$patrol_source_ready" -eq 0 ]; then
    status="needs_patrol_fetch"
  elif [ "$done_pq" -lt "$raw_count" ] || [ "$tmp_count" -gt 0 ]; then
    status="needs_ingest"
  elif [ "$missing_core_metric_count" -gt 0 ]; then
    status="needs_compute"
  elif [ "$missing_patrol_metric" -gt 0 ]; then
    status="needs_patrol_compute"
  elif [ "$merged_ready" -eq 0 ]; then
    status="needs_merge"
  else
    status="complete"
  fi
  printf '      "status": "%s"\n' "$status"

  printf '    }'
done
echo ""
echo "  },"

printf '  "merged": %s\n' "$(json_file_list "$OUTPUT_DIR")"
echo "}"
