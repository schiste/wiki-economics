#!/usr/bin/env node
"use strict";
// Observable data loader — filter-bar metadata only for inequality.md (see
// defaults_inequality.json.cjs for the full precomputed default view).

const { queryRows, scalar, run } = require("./lib/duckdb-json.cjs");

run(async (con, DIR) => {
  const WIKI =
    process.env.DEFAULT_WIKI ||
    (await scalar(con, `SELECT wiki FROM '${DIR}/inequality.parquet' GROUP BY wiki ORDER BY wiki LIMIT 1`));
  const MAX_MONTH =
    process.env.MAX_MONTH || (await scalar(con, `SELECT MAX(year_month) FROM '${DIR}/inequality.parquet'`));

  const [wikis, rangeByWiki] = await Promise.all([
    queryRows(con, `SELECT DISTINCT wiki FROM '${DIR}/inequality.parquet' ORDER BY wiki`),
    queryRows(
      con,
      `SELECT wiki, MIN(year_month) as mn, MAX(year_month) as mx
       FROM '${DIR}/inequality.parquet'
       WHERE year_month <= '${MAX_MONTH}'
       GROUP BY wiki ORDER BY wiki`
    ),
  ]);

  return { defaultWiki: WIKI, maxMonth: MAX_MONTH, wikis, rangeByWiki };
});
