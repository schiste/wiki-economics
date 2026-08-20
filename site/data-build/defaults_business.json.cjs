#!/usr/bin/env node
"use strict";
// Observable data loader — pre-computes default view for business.md

const { queryRows, scalar, run } = require("./lib/duckdb-json.cjs");

const QUARTER_EXPR =
  "LEFT(year_month, 4) || '-Q' || CAST(CEIL(CAST(SUBSTRING(year_month, 6, 2) AS INTEGER) / 3.0) AS INTEGER)";

run(async (con, DIR) => {
  const WIKI =
    process.env.DEFAULT_WIKI ||
    (await scalar(con, `SELECT wiki FROM '${DIR}/labor_monthly.parquet' GROUP BY wiki ORDER BY wiki LIMIT 1`));
  const MAX_MONTH =
    process.env.MAX_MONTH || (await scalar(con, `SELECT MAX(year_month) FROM '${DIR}/labor_monthly.parquet'`));
  const [MAX_YEAR, MAX_MONTH_NUM] = [MAX_MONTH.slice(0, 4), MAX_MONTH.slice(5)];
  const MAX_QUARTER = `${MAX_YEAR}-Q${Math.floor((Number(MAX_MONTH_NUM) - 1) / 3) + 1}`;

  const [wikis, nsByWiki, rangeByWiki, churn, tiers, survival, equilibrium, cohorts, yearlyBytesPerEditor, funnel] =
    await Promise.all([
      queryRows(con, `SELECT DISTINCT wiki FROM '${DIR}/labor_monthly.parquet' ORDER BY wiki`),
      queryRows(
        con,
        `SELECT DISTINCT wiki, page_namespace
         FROM '${DIR}/gdp.parquet'
         ORDER BY wiki, page_namespace`
      ),
      queryRows(
        con,
        `SELECT wiki, MIN(year_month) as mn, MAX(year_month) as mx
         FROM '${DIR}/labor_monthly.parquet'
         WHERE year_month <= '${MAX_MONTH}'
         GROUP BY wiki`
      ),
      queryRows(
        con,
        `SELECT period, period_type, active_editors, arrivals, departures,
                arrival_rate, departure_rate
         FROM '${DIR}/labor_churn.parquet'
         WHERE wiki='${WIKI}' AND period_type='quarter'
               AND period <= '${MAX_QUARTER}'
         ORDER BY period`
      ),
      queryRows(
        con,
        `SELECT ${QUARTER_EXPR} as period, activity_tier as tier,
                CAST(SUM(editors) AS DOUBLE) as editors,
                CAST(SUM(total_edits) AS DOUBLE) as edits,
                CAST(SUM(net_bytes) AS DOUBLE) as net_bytes,
                CAST(SUM(gross_bytes) AS DOUBLE) as gross_bytes
         FROM '${DIR}/gdp_activity_tiers.parquet'
         WHERE wiki='${WIKI}' AND user_type='registered'
               AND year_month <= '${MAX_MONTH}'
         GROUP BY 1, 2 ORDER BY 1, 2`
      ),
      queryRows(
        con,
        `SELECT ${QUARTER_EXPR} as period,
                CAST(SUM(total_edits) AS DOUBLE) as total_edits,
                CAST(SUM(reverted_edits) AS DOUBLE) as reverted_edits
         FROM '${DIR}/gdp.parquet'
         WHERE wiki='${WIKI}' AND user_type='registered' AND page_namespace=0
               AND year_month <= '${MAX_MONTH}'
         GROUP BY 1 ORDER BY 1`
      ),
      queryRows(
        con,
        `SELECT ${QUARTER_EXPR} as period,
                page_namespace,
                CAST(SUM(total_edits) AS DOUBLE) as total_edits,
                CAST(SUM(reverted_edits) AS DOUBLE) as reverted_edits
         FROM '${DIR}/gdp.parquet'
         WHERE wiki='${WIKI}' AND user_type='registered'
               AND year_month <= '${MAX_MONTH}'
         GROUP BY 1, 2 ORDER BY 1, 2`
      ),
      queryRows(
        con,
        `SELECT cohort_year, year, initial_editors, survived_editors
         FROM '${DIR}/labor_cohorts.parquet'
         WHERE wiki='${WIKI}'
         ORDER BY cohort_year, year`
      ),
      queryRows(
        con,
        `SELECT LEFT(year_month, 4) as year,
                CAST(SUM(net_bytes) AS DOUBLE) as net_bytes,
                CAST(SUM(unique_editors) AS DOUBLE) as unique_editors
         FROM '${DIR}/gdp.parquet'
         WHERE wiki='${WIKI}' AND user_type='registered' AND page_namespace=0
               AND year_month <= '${MAX_MONTH}'
         GROUP BY 1 ORDER BY 1`
      ),
      queryRows(
        con,
        `SELECT * FROM '${DIR}/business_funnel.parquet'
         WHERE wiki='${WIKI}'
         ORDER BY cohort_year`
      ),
    ]);

  return {
    defaultWiki: WIKI,
    maxMonth: MAX_MONTH,
    wikis,
    nsByWiki,
    rangeByWiki,
    churn,
    tiers,
    survival,
    equilibrium,
    cohorts,
    yearlyBytesPerEditor,
    funnel,
  };
});
