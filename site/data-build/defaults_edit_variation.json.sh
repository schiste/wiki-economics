#!/bin/bash
# Observable data loader — pre-computes default view for edit-variation.md

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
DIR="${WIKI_ECON_OUTPUT_DIR:-$ROOT/output}"

resolve_scalar() {
  duckdb :memory: -csv -noheader -c "$1" | tr -d '\r\n'
}

WIKI="${DEFAULT_WIKI:-$(resolve_scalar "SELECT wiki FROM '${DIR}/page_weekly_edits.parquet' GROUP BY wiki ORDER BY wiki LIMIT 1")}"

echo "{"
printf '"defaultWiki":"%s",\n' "$WIKI"

echo '"summary":'
duckdb :memory: -json -c "
  SELECT
    CAST(COUNT(*) AS BIGINT) AS rows,
    MIN(week_start) AS min_week,
    MAX(week_start) AS max_week
  FROM '${DIR}/page_weekly_edits.parquet'
  WHERE wiki='${WIKI}' AND page_namespace=0
"

echo ',"topVariation":'
duckdb :memory: -json -c "
  SELECT
    week_start,
    CAST(strptime(week_start, '%Y-%m-%d') + INTERVAL 6 DAY AS DATE) AS week_end,
    page_title,
    CAST(previous_week_edits AS BIGINT) AS previous_week_edits,
    CAST(edits AS BIGINT) AS edits,
    CAST(wow_change AS BIGINT) AS wow_change,
    CAST(wow_rate AS DOUBLE) AS wow_rate
  FROM '${DIR}/page_weekly_edits.parquet'
  WHERE wiki='${WIKI}'
    AND page_namespace=0
    AND previous_week_edits > 0
  ORDER BY wow_change DESC, edits DESC, page_title ASC
  LIMIT 20
"

echo "}"
