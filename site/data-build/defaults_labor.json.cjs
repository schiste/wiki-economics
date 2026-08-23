#!/usr/bin/env node
"use strict";
// Observable data loader — pre-computes default view for labor.md

const { queryRows, scalar, run } = require("./lib/duckdb-json.cjs");

run(async (con, DIR) => {
  const WIKI =
    process.env.DEFAULT_WIKI ||
    (await scalar(con, `SELECT wiki FROM '${DIR}/labor_monthly.parquet' GROUP BY wiki ORDER BY wiki LIMIT 1`));
  const MAX_MONTH =
    process.env.MAX_MONTH || (await scalar(con, `SELECT MAX(year_month) FROM '${DIR}/labor_monthly.parquet'`));

  const [wikis, nsByWiki, rangeByWiki, workforce, byType, churn, cohorts] = await Promise.all([
    queryRows(con, `SELECT DISTINCT wiki FROM '${DIR}/labor_monthly.parquet' ORDER BY wiki`),
    queryRows(
      con,
      `SELECT DISTINCT wiki, page_namespace
       FROM '${DIR}/labor_monthly.parquet'
       ORDER BY wiki, page_namespace`
    ),
    queryRows(
      con,
      `SELECT wiki, MIN(year_month) as mn, MAX(year_month) as mx
       FROM '${DIR}/labor_monthly.parquet'
       WHERE year_month <= '${MAX_MONTH}'
       GROUP BY wiki ORDER BY wiki`
    ),
    queryRows(
      con,
      `SELECT LEFT(year_month, 4) as period,
              CAST(SUM(unique_editors) AS DOUBLE) as unique_editors,
              CAST(SUM(total_edits) AS DOUBLE) as total_edits,
              CAST(SUM(net_bytes) AS DOUBLE) as net_bytes,
              CAST(SUM(reverted_edits) AS DOUBLE) as reverted_edits
       FROM '${DIR}/labor_monthly.parquet'
       WHERE wiki='${WIKI}' AND user_type='registered' AND page_namespace=0
             AND year_month <= '${MAX_MONTH}'
       GROUP BY 1 ORDER BY 1`
    ),
    queryRows(
      con,
      `SELECT year_month as period, user_type,
              CAST(SUM(unique_editors) AS DOUBLE) as editors
       FROM '${DIR}/labor_monthly.parquet'
       WHERE wiki='${WIKI}' AND page_namespace=0
             AND year_month <= '${MAX_MONTH}'
       GROUP BY 1, 2 ORDER BY 1, 2`
    ),
    queryRows(
      con,
      `SELECT period, period_type, active_editors, arrivals, departures,
              arrival_rate, departure_rate
       FROM '${DIR}/labor_churn.parquet'
       WHERE wiki='${WIKI}' AND period_type='month'
             AND period <= '${MAX_MONTH}'
       ORDER BY period`
    ),
    queryRows(
      con,
      `SELECT cohort_year, year, initial_editors, survived_editors
       FROM '${DIR}/labor_cohorts.parquet'
       WHERE wiki='${WIKI}'
       ORDER BY cohort_year, year`
    ),
  ]);

  return { defaultWiki: WIKI, maxMonth: MAX_MONTH, wikis, nsByWiki, rangeByWiki, workforce, byType, churn, cohorts };
});
