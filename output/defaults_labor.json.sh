#!/bin/bash
# Observable data loader — pre-computes default view for labor.md

DIR="$(dirname "$0")"

resolve_scalar() {
  duckdb :memory: -csv -noheader -c "$1" | tr -d '\r\n'
}

WIKI="${DEFAULT_WIKI:-$(resolve_scalar "SELECT wiki FROM '${DIR}/labor_monthly.parquet' GROUP BY wiki ORDER BY wiki LIMIT 1")}"
MAX_MONTH="${MAX_MONTH:-$(resolve_scalar "SELECT MAX(year_month) FROM '${DIR}/labor_monthly.parquet'")}"

echo "{"
printf '"defaultWiki":"%s",\n' "$WIKI"
printf '"maxMonth":"%s",\n' "$MAX_MONTH"

# ── Metadata ─────────────────────────────────────────────────
echo '"wikis":'
duckdb :memory: -json -c "
  SELECT DISTINCT wiki FROM '${DIR}/labor_monthly.parquet' ORDER BY wiki
"

echo ',"nsByWiki":'
duckdb :memory: -json -c "
  SELECT DISTINCT wiki, page_namespace
  FROM '${DIR}/labor_monthly.parquet'
  ORDER BY wiki, page_namespace
"

echo ',"rangeByWiki":'
duckdb :memory: -json -c "
  SELECT wiki, MIN(year_month) as mn, MAX(year_month) as mx
  FROM '${DIR}/labor_monthly.parquet'
  WHERE year_month <= '${MAX_MONTH}'
  GROUP BY wiki
"

# ── Workforce (main chart) ───────────────────────────────────
echo ',"workforce":'
duckdb :memory: -json -c "
  SELECT LEFT(year_month, 4) as period,
         CAST(SUM(unique_editors) AS DOUBLE) as unique_editors,
         CAST(SUM(total_edits) AS DOUBLE) as total_edits,
         CAST(SUM(net_bytes) AS DOUBLE) as net_bytes,
         CAST(SUM(reverted_edits) AS DOUBLE) as reverted_edits
  FROM '${DIR}/labor_monthly.parquet'
  WHERE wiki='${WIKI}' AND user_type='registered' AND page_namespace=0
        AND year_month <= '${MAX_MONTH}'
  GROUP BY 1 ORDER BY 1
"

# ── By user type (all types, for composition chart) ──────────
echo ',"byType":'
duckdb :memory: -json -c "
  SELECT year_month as period, user_type,
         CAST(SUM(unique_editors) AS DOUBLE) as editors
  FROM '${DIR}/labor_monthly.parquet'
  WHERE wiki='${WIKI}' AND page_namespace=0
        AND year_month <= '${MAX_MONTH}'
  GROUP BY 1, 2 ORDER BY 1, 2
"

# ── Churn data (registered only, monthly) ────────────────────
echo ',"churn":'
duckdb :memory: -json -c "
  SELECT period, period_type, active_editors, arrivals, departures,
         arrival_rate, departure_rate
  FROM '${DIR}/labor_churn.parquet'
  WHERE wiki='${WIKI}' AND period_type='month'
        AND period <= '${MAX_MONTH}'
  ORDER BY period
"

# ── Cohort data ──────────────────────────────────────────────
echo ',"cohorts":'
duckdb :memory: -json -c "
  SELECT cohort_year, year, initial_editors, survived_editors
  FROM '${DIR}/labor_cohorts.parquet'
  WHERE wiki='${WIKI}'
  ORDER BY cohort_year, year
"

echo "}"
