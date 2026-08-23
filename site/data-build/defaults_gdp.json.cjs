#!/usr/bin/env node
"use strict";
// Observable data loader — pre-computes default view for gdp.md

const { connect, queryRows, scalar, run } = require("./lib/duckdb-json.cjs");

run(async (con, DIR) => {
  const WIKI =
    process.env.DEFAULT_WIKI ||
    (await scalar(con, `SELECT wiki FROM '${DIR}/gdp.parquet' GROUP BY wiki ORDER BY wiki LIMIT 1`));
  const MAX_MONTH =
    process.env.MAX_MONTH || (await scalar(con, `SELECT MAX(year_month) FROM '${DIR}/gdp.parquet'`));

  const [wikis, nsByWiki, rangeByWiki, output, byType, byNamespace, tiers, typeShare] = await Promise.all([
    queryRows(con, `SELECT DISTINCT wiki FROM '${DIR}/gdp.parquet' ORDER BY wiki`),
    queryRows(
      con,
      `SELECT DISTINCT wiki, page_namespace FROM '${DIR}/gdp.parquet' ORDER BY wiki, page_namespace`
    ),
    queryRows(
      con,
      `SELECT wiki, MIN(year_month) as mn, MAX(year_month) as mx
       FROM '${DIR}/gdp.parquet'
       WHERE year_month <= '${MAX_MONTH}'
       GROUP BY wiki ORDER BY wiki`
    ),
    queryRows(
      con,
      `SELECT LEFT(year_month, 4) as period,
              CAST(SUM(gross_bytes_added) AS DOUBLE) as gross_bytes_added,
              CAST(SUM(net_bytes) AS DOUBLE) as net_bytes,
              CAST(SUM(total_edits) AS DOUBLE) as total_edits,
              CAST(SUM(productive_edits) AS DOUBLE) as productive_edits,
              CAST(SUM(reverted_edits) AS DOUBLE) as reverted_edits,
              CAST(SUM(unique_editors) AS DOUBLE) as unique_editors
       FROM '${DIR}/gdp.parquet'
       WHERE wiki='${WIKI}' AND user_type='registered' AND page_namespace=0
             AND year_month <= '${MAX_MONTH}'
       GROUP BY 1 ORDER BY 1`
    ),
    queryRows(
      con,
      `SELECT year_month as period, user_type,
              CAST(SUM(gross_bytes_added) AS DOUBLE) as gross_bytes_added,
              CAST(SUM(net_bytes) AS DOUBLE) as net_bytes,
              CAST(SUM(total_edits) AS DOUBLE) as total_edits,
              CAST(SUM(reverted_edits) AS DOUBLE) as reverted_edits,
              CAST(SUM(unique_editors) AS DOUBLE) as unique_editors
       FROM '${DIR}/gdp.parquet'
       WHERE wiki='${WIKI}' AND page_namespace=0
             AND year_month <= '${MAX_MONTH}'
       GROUP BY 1, 2 ORDER BY 1, 2`
    ),
    queryRows(
      con,
      `SELECT year_month as period, page_namespace,
              CAST(SUM(total_edits) AS DOUBLE) as edits,
              CAST(SUM(gross_bytes_added) AS DOUBLE) as gross_bytes,
              CAST(SUM(net_bytes) AS DOUBLE) as net_bytes
       FROM '${DIR}/gdp.parquet'
       WHERE wiki='${WIKI}' AND user_type='registered' AND page_namespace=0
             AND year_month <= '${MAX_MONTH}'
       GROUP BY 1, 2 ORDER BY 1, 2`
    ),
    queryRows(
      con,
      `SELECT year_month as period, activity_tier,
              CAST(SUM(editors) AS DOUBLE) as editors,
              CAST(SUM(total_edits) AS DOUBLE) as total_edits,
              CAST(SUM(gross_bytes) AS DOUBLE) as gross_bytes,
              CAST(SUM(net_bytes) AS DOUBLE) as net_bytes
       FROM '${DIR}/gdp_activity_tiers.parquet'
       WHERE wiki='${WIKI}' AND user_type='registered'
             AND year_month <= '${MAX_MONTH}'
       GROUP BY 1, 2 ORDER BY 1, 2`
    ),
    queryRows(
      con,
      `SELECT year_month as period, user_type,
              CAST(SUM(edits) AS DOUBLE) as edits
       FROM '${DIR}/gdp_user_type_share.parquet'
       WHERE wiki='${WIKI}'
             AND year_month <= '${MAX_MONTH}'
       GROUP BY 1, 2 ORDER BY 1, 2`
    ),
  ]);

  return {
    defaultWiki: WIKI,
    maxMonth: MAX_MONTH,
    wikis,
    nsByWiki,
    rangeByWiki,
    output,
    byType,
    byNamespace,
    tiers,
    typeShare,
  };
});
