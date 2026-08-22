#!/usr/bin/env node
"use strict";
// Observable data loader — filter-bar metadata only for patrol.md (see
// defaults_patrol.json.cjs for the full precomputed default view).

const { queryRows, scalar, run } = require("./lib/duckdb-json.cjs");

run(async (con, DIR) => {
  const WIKI =
    process.env.DEFAULT_WIKI ||
    (await scalar(con, `SELECT wiki FROM '${DIR}/patrol.parquet' GROUP BY wiki ORDER BY wiki LIMIT 1`));
  const MAX_MONTH =
    process.env.MAX_MONTH || (await scalar(con, `SELECT MAX(year_month) FROM '${DIR}/patrol.parquet'`));

  const [wikis, nsByWiki, rangeByWiki] = await Promise.all([
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
       GROUP BY wiki`
    ),
  ]);

  return { defaultWiki: WIKI, maxMonth: MAX_MONTH, wikis, nsByWiki, rangeByWiki };
});
