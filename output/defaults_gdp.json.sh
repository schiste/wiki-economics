#!/bin/bash
# Observable data loader — pre-computes default view for gdp.md

DIR="$(dirname "$0")"

resolve_scalar() {
  duckdb :memory: -csv -noheader -c "$1" | tr -d '\r\n'
}

WIKI="${DEFAULT_WIKI:-$(resolve_scalar "SELECT wiki FROM '${DIR}/gdp.parquet' GROUP BY wiki ORDER BY wiki LIMIT 1")}"
MAX_MONTH="${MAX_MONTH:-$(resolve_scalar "SELECT MAX(year_month) FROM '${DIR}/gdp.parquet'")}"

echo "{"
printf '"defaultWiki":"%s",\n' "$WIKI"
printf '"maxMonth":"%s",\n' "$MAX_MONTH"

# ── Metadata ─────────────────────────────────────────────────
echo '"wikis":'
duckdb :memory: -json -c "
  SELECT DISTINCT wiki FROM '${DIR}/gdp.parquet' ORDER BY wiki
"

echo ',"nsByWiki":'
duckdb :memory: -json -c "
  SELECT DISTINCT wiki, page_namespace
  FROM '${DIR}/gdp.parquet'
  ORDER BY wiki, page_namespace
"

echo ',"rangeByWiki":'
duckdb :memory: -json -c "
  SELECT wiki, MIN(year_month) as mn, MAX(year_month) as mx
  FROM '${DIR}/gdp.parquet'
  WHERE year_month <= '${MAX_MONTH}'
  GROUP BY wiki
"

# ── Main output (aggregated) ─────────────────────────────────
echo ',"output":'
duckdb :memory: -json -c "
  SELECT LEFT(year_month, 4) as period,
         CAST(SUM(gross_bytes_added) AS DOUBLE) as gross_bytes_added,
         CAST(SUM(net_bytes) AS DOUBLE) as net_bytes,
         CAST(SUM(total_edits) AS DOUBLE) as total_edits,
         CAST(SUM(productive_edits) AS DOUBLE) as productive_edits,
         CAST(SUM(reverted_edits) AS DOUBLE) as reverted_edits,
         CAST(SUM(unique_editors) AS DOUBLE) as unique_editors
  FROM '${DIR}/gdp.parquet'
  WHERE wiki='${WIKI}' AND user_type='registered' AND page_namespace=0
        AND year_month <= '${MAX_MONTH}'
  GROUP BY 1 ORDER BY 1
"

# ── By user type (all types, ns 0, for breakdown toggle) ─────
echo ',"byType":'
duckdb :memory: -json -c "
  SELECT year_month as period, user_type,
         CAST(SUM(gross_bytes_added) AS DOUBLE) as gross_bytes_added,
         CAST(SUM(net_bytes) AS DOUBLE) as net_bytes,
         CAST(SUM(total_edits) AS DOUBLE) as total_edits,
         CAST(SUM(reverted_edits) AS DOUBLE) as reverted_edits,
         CAST(SUM(unique_editors) AS DOUBLE) as unique_editors
  FROM '${DIR}/gdp.parquet'
  WHERE wiki='${WIKI}' AND page_namespace=0
        AND year_month <= '${MAX_MONTH}'
  GROUP BY 1, 2 ORDER BY 1, 2
"

# ── Sectoral output (by namespace, registered only) ──────────
echo ',"byNamespace":'
duckdb :memory: -json -c "
  SELECT year_month as period, page_namespace,
         CAST(SUM(total_edits) AS DOUBLE) as edits,
         CAST(SUM(gross_bytes_added) AS DOUBLE) as gross_bytes,
         CAST(SUM(net_bytes) AS DOUBLE) as net_bytes
  FROM '${DIR}/gdp.parquet'
  WHERE wiki='${WIKI}' AND user_type='registered' AND page_namespace=0
        AND year_month <= '${MAX_MONTH}'
  GROUP BY 1, 2 ORDER BY 1, 2
"

# ── Activity tiers ───────────────────────────────────────────
echo ',"tiers":'
duckdb :memory: -json -c "
  SELECT year_month as period, activity_tier,
         CAST(SUM(editors) AS DOUBLE) as editors,
         CAST(SUM(total_edits) AS DOUBLE) as total_edits,
         CAST(SUM(gross_bytes) AS DOUBLE) as gross_bytes,
         CAST(SUM(net_bytes) AS DOUBLE) as net_bytes
  FROM '${DIR}/gdp_activity_tiers.parquet'
  WHERE wiki='${WIKI}' AND user_type='registered'
        AND year_month <= '${MAX_MONTH}'
  GROUP BY 1, 2 ORDER BY 1, 2
"

# ── User type share (all types) ──────────────────────────────
echo ',"typeShare":'
duckdb :memory: -json -c "
  SELECT year_month as period, user_type,
         CAST(SUM(edits) AS DOUBLE) as edits
  FROM '${DIR}/gdp_user_type_share.parquet'
  WHERE wiki='${WIKI}'
        AND year_month <= '${MAX_MONTH}'
  GROUP BY 1, 2 ORDER BY 1, 2
"

echo "}"
