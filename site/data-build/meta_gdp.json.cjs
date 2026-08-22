#!/usr/bin/env node
"use strict";
// Observable data loader — filter-bar metadata only for gdp.md (see defaults_gdp.json.cjs
// for the full precomputed default view, including bulk chart data).

const { queryRows, scalar, run } = require("./lib/duckdb-json.cjs");

run(async (con, DIR) => {
  const WIKI =
    process.env.DEFAULT_WIKI ||
    (await scalar(con, `SELECT wiki FROM '${DIR}/gdp.parquet' GROUP BY wiki ORDER BY wiki LIMIT 1`));
  const MAX_MONTH =
    process.env.MAX_MONTH || (await scalar(con, `SELECT MAX(year_month) FROM '${DIR}/gdp.parquet'`));

  const [wikis, nsByWiki, rangeByWiki] = await Promise.all([
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
       GROUP BY wiki`
    ),
  ]);

  return { defaultWiki: WIKI, maxMonth: MAX_MONTH, wikis, nsByWiki, rangeByWiki };
});
