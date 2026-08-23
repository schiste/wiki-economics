#!/usr/bin/env node
"use strict";
// Observable data loader — pre-computes default view for patrol.md

const { queryRows, scalar, run } = require("./lib/duckdb-json.cjs");

run(async (con, DIR) => {
  const WIKI =
    process.env.DEFAULT_WIKI ||
    (await scalar(con, `SELECT wiki FROM '${DIR}/patrol.parquet' GROUP BY wiki ORDER BY wiki LIMIT 1`));
  const MAX_MONTH =
    process.env.MAX_MONTH || (await scalar(con, `SELECT MAX(year_month) FROM '${DIR}/patrol.parquet'`));

  const [wikis, nsByWiki, rangeByWiki, patrol] = await Promise.all([
    queryRows(con, `SELECT DISTINCT wiki FROM '${DIR}/patrol.parquet' ORDER BY wiki`),
    queryRows(
      con,
      `SELECT DISTINCT wiki, page_namespace
       FROM '${DIR}/patrol.parquet'
       ORDER BY wiki, page_namespace`
    ),
    queryRows(
      con,
      `SELECT wiki, MIN(year_month) as mn, MAX(year_month) as mx
       FROM '${DIR}/patrol.parquet'
       WHERE year_month <= '${MAX_MONTH}'
       GROUP BY wiki ORDER BY wiki`
    ),
    queryRows(
      con,
      `SELECT LEFT(year_month, 4) as period,
              CAST(SUM(total_patrols) AS DOUBLE) as total_patrols,
              CAST(SUM(unique_patrollers) AS DOUBLE) as unique_patrollers,
              CAST(SUM(patrol_new_pages) AS DOUBLE) as patrol_new_pages,
              CAST(SUM(patrol_diffs) AS DOUBLE) as patrol_diffs,
              CAST(AVG(median_latency_hours) AS DOUBLE) as median_latency_hours,
              CAST(AVG(p90_latency_hours) AS DOUBLE) as p90_latency_hours,
              CAST(SUM(patrolled_revisions) AS DOUBLE) as patrolled_revisions,
              CAST(SUM(autopatrolled_revisions) AS DOUBLE) as autopatrolled_revisions,
              CAST(SUM(total_revisions) AS DOUBLE) as total_revisions,
              CAST(AVG(patrol_coverage_pct) AS DOUBLE) as patrol_coverage_pct,
              CAST(AVG(adjusted_coverage_pct) AS DOUBLE) as adjusted_coverage_pct,
              CAST(AVG(top1_pct) AS DOUBLE) as top1_pct,
              CAST(SUM(min_patrollers_50pct) AS DOUBLE) as min_patrollers_50pct
       FROM '${DIR}/patrol.parquet'
       WHERE wiki='${WIKI}' AND page_namespace=0 AND user_type='registered'
             AND year_month <= '${MAX_MONTH}'
       GROUP BY 1 ORDER BY 1`
    ),
  ]);

  return { defaultWiki: WIKI, maxMonth: MAX_MONTH, wikis, nsByWiki, rangeByWiki, patrol };
});
