"use strict";

const assert = require("node:assert/strict");
const {test} = require("node:test");
const {closeConnection, connect, queryRows, scalar, toJSONSafe} = require("./duckdb-json.cjs");

test("the DuckDB Node API preserves the generator JSON contract", async () => {
  const connection = await connect();
  try {
    const rows = await queryRows(
      connection,
      "SELECT 42::BIGINT AS count, DATE '2026-08-23' AS cutoff, 'nlwiki' AS wiki",
    );
    assert.deepEqual(rows, [{count: 42, cutoff: "2026-08-23", wiki: "nlwiki"}]);
    assert.equal(await scalar(connection, "SELECT 70964313::BIGINT AS edits"), "70964313");
  } finally {
    closeConnection(connection);
  }
});

test("JSON normalization remains recursive and null-safe", () => {
  assert.deepEqual(
    toJSONSafe({count: 42n, cutoff: new Date("2026-08-23T00:00:00Z"), values: [1n, null]}),
    {count: 42, cutoff: "2026-08-23", values: [1, null]},
  );
});
