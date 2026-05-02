#!/bin/bash
# Observable data loader — pre-computes default view for business.md

DIR="$(dirname "$0")"

resolve_scalar() {
  duckdb :memory: -csv -noheader -c "$1" | tr -d '\r\n'
}

WIKI="${DEFAULT_WIKI:-$(resolve_scalar "SELECT wiki FROM '${DIR}/labor_monthly.parquet' GROUP BY wiki ORDER BY wiki LIMIT 1")}"
MAX_MONTH="${MAX_MONTH:-$(resolve_scalar "SELECT MAX(year_month) FROM '${DIR}/labor_monthly.parquet'")}"
MAX_YEAR="${MAX_MONTH%-*}"
MAX_MONTH_NUM="${MAX_MONTH#*-}"
MAX_QUARTER="${MAX_YEAR}-Q$(( (10#$MAX_MONTH_NUM - 1) / 3 + 1 ))"

# Quarter period expression for DuckDB
QUARTER_EXPR="LEFT(year_month, 4) || '-Q' || CAST(CEIL(CAST(SUBSTRING(year_month, 6, 2) AS INTEGER) / 3.0) AS INTEGER)"

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
  FROM '${DIR}/gdp.parquet'
  ORDER BY wiki, page_namespace
"

echo ',"rangeByWiki":'
duckdb :memory: -json -c "
  SELECT wiki, MIN(year_month) as mn, MAX(year_month) as mx
  FROM '${DIR}/labor_monthly.parquet'
  WHERE year_month <= '${MAX_MONTH}'
  GROUP BY wiki
"

# ── Churn data (registered, quarterly) ───────────────────────
echo ',"churn":'
duckdb :memory: -json -c "
  SELECT period, period_type, active_editors, arrivals, departures,
         arrival_rate, departure_rate
  FROM '${DIR}/labor_churn.parquet'
  WHERE wiki='${WIKI}' AND period_type='quarter'
        AND period <= '${MAX_QUARTER}'
  ORDER BY period
"

# ── Activity tiers (registered, quarterly aggregation) ───────
echo ',"tiers":'
duckdb :memory: -json -c "
  SELECT ${QUARTER_EXPR} as period, activity_tier as tier,
         CAST(SUM(editors) AS DOUBLE) as editors,
         CAST(SUM(total_edits) AS DOUBLE) as edits,
         CAST(SUM(net_bytes) AS DOUBLE) as net_bytes,
         CAST(SUM(gross_bytes) AS DOUBLE) as gross_bytes
  FROM '${DIR}/gdp_activity_tiers.parquet'
  WHERE wiki='${WIKI}' AND user_type='registered'
        AND year_month <= '${MAX_MONTH}'
  GROUP BY 1, 2 ORDER BY 1, 2
"

# ── GDP for survival rate (registered, ns 0, quarterly) ──────
echo ',"survival":'
duckdb :memory: -json -c "
  SELECT ${QUARTER_EXPR} as period,
         CAST(SUM(total_edits) AS DOUBLE) as total_edits,
         CAST(SUM(reverted_edits) AS DOUBLE) as reverted_edits
  FROM '${DIR}/gdp.parquet'
  WHERE wiki='${WIKI}' AND user_type='registered' AND page_namespace=0
        AND year_month <= '${MAX_MONTH}'
  GROUP BY 1 ORDER BY 1
"

# ── GDP for equilibrium (registered, all namespaces, quarterly)
echo ',"equilibrium":'
duckdb :memory: -json -c "
  SELECT ${QUARTER_EXPR} as period,
         page_namespace,
         CAST(SUM(total_edits) AS DOUBLE) as total_edits,
         CAST(SUM(reverted_edits) AS DOUBLE) as reverted_edits
  FROM '${DIR}/gdp.parquet'
  WHERE wiki='${WIKI}' AND user_type='registered'
        AND year_month <= '${MAX_MONTH}'
  GROUP BY 1, 2 ORDER BY 1, 2
"

# ── Cohort data ──────────────────────────────────────────────
echo ',"cohorts":'
duckdb :memory: -json -c "
  SELECT cohort_year, year, initial_editors, survived_editors
  FROM '${DIR}/labor_cohorts.parquet'
  WHERE wiki='${WIKI}'
  ORDER BY cohort_year, year
"

# ── GDP for yearly bytes per editor (registered, ns 0) ───────
echo ',"yearlyBytesPerEditor":'
duckdb :memory: -json -c "
  SELECT LEFT(year_month, 4) as year,
         CAST(SUM(net_bytes) AS DOUBLE) as net_bytes,
         CAST(SUM(unique_editors) AS DOUBLE) as unique_editors
  FROM '${DIR}/gdp.parquet'
  WHERE wiki='${WIKI}' AND user_type='registered' AND page_namespace=0
        AND year_month <= '${MAX_MONTH}'
  GROUP BY 1 ORDER BY 1
"

# ── Acquisition funnel ───────────────────────────────────────
echo ',"funnel":'
duckdb :memory: -json -c "
  SELECT * FROM '${DIR}/business_funnel.parquet'
  WHERE wiki='${WIKI}'
  ORDER BY cohort_year
"

echo "}"
