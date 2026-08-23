"use strict";

const path = require("node:path");
const {DuckDBInstance} = require("@duckdb/node-api");

// DuckDB's Node binding returns BIGINT columns as JS BigInt and DATE columns
// as JS Date (UTC midnight). The DuckDB CLI's `-json` mode instead emits
// plain JSON numbers and "YYYY-MM-DD" strings — match that shape so ported
// generators produce byte-for-byte-equivalent output to the CLI-based ones.
function toJSONSafe(value) {
  if (value === null || value === undefined) return null;
  if (typeof value === "bigint") return Number(value);
  if (value instanceof Date) return value.toISOString().slice(0, 10);
  if (Array.isArray(value)) return value.map(toJSONSafe);
  if (typeof value === "object") {
    const out = {};
    for (const [key, val] of Object.entries(value)) out[key] = toJSONSafe(val);
    return out;
  }
  return value;
}

function outputDir() {
  return process.env.WIKI_ECON_OUTPUT_DIR || path.resolve(__dirname, "..", "..", "..", "output");
}

async function connect() {
  const instance = await DuckDBInstance.create(":memory:");
  return instance.connect();
}

async function queryRows(con, sql) {
  const reader = await con.runAndReadAll(sql);
  return reader.getRowObjectsJS().map(toJSONSafe);
}

function closeConnection(con) {
  if (typeof con?.closeSync === "function") con.closeSync();
  else con?.close?.();
}

async function scalar(con, sql) {
  const rows = await queryRows(con, sql);
  if (rows.length === 0) return null;
  const firstKey = Object.keys(rows[0])[0];
  const value = rows[0][firstKey];
  return value === null || value === undefined ? null : String(value);
}

// Runs `main(con, DIR)` and writes its returned object to stdout as JSON,
// matching the exit-code/stdout contract src/merge.rs expects from the
// bash generators (0 + JSON on stdout, non-zero + stderr on failure).
function run(main) {
  const DIR = outputDir();
  Promise.resolve()
    .then(async () => {
      const con = await connect();
      try {
        const result = await main(con, DIR);
        process.stdout.write(JSON.stringify(result));
      } finally {
        closeConnection(con);
      }
    })
    .catch((err) => {
      process.stderr.write(`${err && err.stack ? err.stack : err}\n`);
      process.exitCode = 1;
    });
}

module.exports = {closeConnection, connect, outputDir, queryRows, run, scalar, toJSONSafe};
