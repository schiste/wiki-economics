#!/bin/bash
# Observable data loader — pre-computes default view for inequality.md

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
DIR="${WIKI_ECON_OUTPUT_DIR:-$ROOT/output}"

resolve_scalar() {
  duckdb :memory: -csv -noheader -c "$1" | tr -d '\r\n'
}

WIKI="${DEFAULT_WIKI:-$(resolve_scalar "SELECT wiki FROM '${DIR}/inequality.parquet' GROUP BY wiki ORDER BY wiki LIMIT 1")}"
MAX_MONTH="${MAX_MONTH:-$(resolve_scalar "SELECT MAX(year_month) FROM '${DIR}/inequality.parquet'")}"

echo "{"
printf '"defaultWiki":"%s",\n' "$WIKI"
printf '"maxMonth":"%s",\n' "$MAX_MONTH"

# ── Metadata ─────────────────────────────────────────────────
echo '"wikis":'
duckdb :memory: -json -c "
  SELECT DISTINCT wiki FROM '${DIR}/inequality.parquet' ORDER BY wiki
"

echo ',"rangeByWiki":'
duckdb :memory: -json -c "
  SELECT wiki, MIN(year_month) as mn, MAX(year_month) as mx
  FROM '${DIR}/inequality.parquet'
  WHERE year_month <= '${MAX_MONTH}'
  GROUP BY wiki
"

# ── Inequality data ──────────────────────────────────────────
echo ',"data":'
duckdb :memory: -json -c "
  SELECT LEFT(year_month, 4) as period,
         CAST(SUM(total_editors) AS DOUBLE) as total_editors,
         CAST(SUM(total_edits) AS DOUBLE) as total_edits,
         CAST(SUM(min_editors_50pct) AS DOUBLE) as min_editors_50pct,
         CAST(AVG(gini) AS DOUBLE) as gini,
         CAST(AVG(theil) AS DOUBLE) as theil,
         CAST(AVG(palma) AS DOUBLE) as palma
  FROM '${DIR}/inequality.parquet'
  WHERE wiki='${WIKI}' AND user_type='registered'
        AND year_month <= '${MAX_MONTH}'
  GROUP BY 1 ORDER BY 1
"

echo "}"
