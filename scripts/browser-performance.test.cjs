"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const {EventEmitter} = require("node:events");
const {afterEach, test} = require("node:test");
const {parseArguments, terminateChild, validateProfile, validateStaticBudgets} = require("./browser-performance.cjs");

const budgets = JSON.parse(fs.readFileSync(path.resolve(__dirname, "../config/browser-performance-budgets.json")));
const roots = [];
afterEach(() => { while (roots.length) fs.rmSync(roots.pop(), {recursive: true, force: true}); });

test("static budgets reject oversized and DuckDB artifacts", () => {
  const dist = fs.mkdtempSync(path.join(os.tmpdir(), "wiki-econ-perf-static-"));
  roots.push(dist);
  fs.writeFileSync(path.join(dist, "app.js"), "ok");
  fs.writeFileSync(path.join(dist, "parquet.wasm"), "ok");
  assert.equal(validateStaticBudgets(dist, budgets).wasm_chunks, 1);
  fs.writeFileSync(path.join(dist, "duckdb.js"), "bad");
  assert.throws(() => validateStaticBudgets(dist, budgets), /DuckDB/);
});

test("profile budgets reject unrelated wiki downloads, latency, memory, and row drift", () => {
  const index = {entries: ["gdp", "gdp_activity_tiers", "gdp_user_type_share"].map(metric => ({metric, wiki: "nlwiki", rows: 10}))};
  const valid = {wiki: "nlwiki", duration_ms: 10, rows: 30, memory_headroom_ratio: 0.5,
    parquet_requests: ["https://example/browser-data/gdp/nlwiki.parquet"]};
  assert.doesNotThrow(() => validateProfile(valid, budgets, index));
  assert.throws(() => validateProfile({...valid, parquet_requests: ["https://example/ptwiki.parquet"]}, budgets, index), /unrelated/);
  assert.throws(() => validateProfile({...valid, duration_ms: 6000}, budgets, index), /took/);
  assert.throws(() => validateProfile({...valid, memory_headroom_ratio: 0.1}, budgets, index), /headroom/);
  assert.throws(() => validateProfile({...valid, rows: 29}, budgets, index), /index declares/);
});

test("argument parser requires a distribution directory", () => {
  assert.throws(() => parseArguments([]), /dist-dir/);
  assert.equal(parseArguments(["--dist-dir", "."])["dist-dir"], process.cwd());
});

test("IndexedDB implementation and performance policy share one byte ceiling", async () => {
  const cache = await import("../site/src/components/browser-cache.js");
  assert.equal(cache.DEFAULT_CACHE_MAX_BYTES, budgets.indexeddb.maximum_bytes);
});

test("browser teardown escalates to SIGKILL and waits for process exit", async () => {
  const child = new EventEmitter();
  child.exitCode = null;
  child.signalCode = null;
  const signals = [];
  child.kill = signal => {
    signals.push(signal);
    if (signal === "SIGKILL") process.nextTick(() => { child.signalCode = signal; child.emit("exit"); });
    return true;
  };
  await terminateChild(child, 1);
  assert.deepEqual(signals, ["SIGTERM", "SIGKILL"]);
});
