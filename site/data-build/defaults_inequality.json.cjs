#!/usr/bin/env node
"use strict";
// Observable data loader — pre-computes default view for inequality.md

const { queryRows, scalar, run } = require("./lib/duckdb-json.cjs");

run(async (con, DIR) => {
  const WIKI =
    process.env.DEFAULT_WIKI ||
    (await scalar(con, `SELECT wiki FROM '${DIR}/inequality.parquet' GROUP BY wiki ORDER BY wiki LIMIT 1`));
  const MAX_MONTH =
    process.env.MAX_MONTH || (await scalar(con, `SELECT MAX(year_month) FROM '${DIR}/inequality.parquet'`));

  const [wikis, rangeByWiki, data] = await Promise.all([
    queryRows(con, `SELECT DISTINCT wiki FROM '${DIR}/inequality.parquet' ORDER BY wiki`),
    queryRows(
      con,
      `SELECT wiki, MIN(year_month) as mn, MAX(year_month) as mx
       FROM '${DIR}/inequality.parquet'
       WHERE year_month <= '${MAX_MONTH}'
       GROUP BY wiki`
    ),
    queryRows(
      con,
      `SELECT LEFT(year_month, 4) as period,
              CAST(SUM(total_editors) AS DOUBLE) as total_editors,
              CAST(SUM(total_edits) AS DOUBLE) as total_edits,
              CAST(SUM(min_editors_50pct) AS DOUBLE) as min_editors_50pct,
              CAST(AVG(gini) AS DOUBLE) as gini,
              CAST(AVG(theil) AS DOUBLE) as theil,
              CAST(AVG(palma) AS DOUBLE) as palma
       FROM '${DIR}/inequality.parquet'
       WHERE wiki='${WIKI}' AND user_type='registered'
             AND year_month <= '${MAX_MONTH}'
       GROUP BY 1 ORDER BY 1`
    ),
  ]);

  return { defaultWiki: WIKI, maxMonth: MAX_MONTH, wikis, rangeByWiki, data };
});
