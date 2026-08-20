#!/usr/bin/env node
"use strict";
// Observable data loader — pre-computes default view for edit-variation.md

const { queryRows, scalar, run } = require("./lib/duckdb-json.cjs");

run(async (con, DIR) => {
  const WIKI =
    process.env.DEFAULT_WIKI ||
    (await scalar(
      con,
      `SELECT wiki FROM '${DIR}/page_weekly_edits.parquet' GROUP BY wiki ORDER BY wiki LIMIT 1`
    ));

  const [summary, topVariation] = await Promise.all([
    queryRows(
      con,
      `SELECT
         CAST(COUNT(*) AS BIGINT) AS rows,
         MIN(week_start) AS min_week,
         MAX(week_start) AS max_week
       FROM '${DIR}/page_weekly_edits.parquet'
       WHERE wiki='${WIKI}' AND page_namespace=0`
    ),
    queryRows(
      con,
      `SELECT
         week_start,
         CAST(strptime(week_start, '%Y-%m-%d') + INTERVAL 6 DAY AS DATE) AS week_end,
         page_title,
         CAST(previous_week_edits AS BIGINT) AS previous_week_edits,
         CAST(edits AS BIGINT) AS edits,
         CAST(wow_change AS BIGINT) AS wow_change,
         CAST(wow_rate AS DOUBLE) AS wow_rate
       FROM '${DIR}/page_weekly_edits.parquet'
       WHERE wiki='${WIKI}'
         AND page_namespace=0
         AND previous_week_edits > 0
       ORDER BY wow_change DESC, edits DESC, page_title ASC
       LIMIT 20`
    ),
  ]);

  return {
    defaultWiki: WIKI,
    summary,
    topVariation,
  };
});
