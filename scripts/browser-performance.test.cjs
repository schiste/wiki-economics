"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const {EventEmitter} = require("node:events");
const {afterEach, test} = require("node:test");
const {launchChrome, parseArguments, terminateChild, validateProfile, validateStaticBudgets} = require("./browser-performance.cjs");

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
  const index = {entries: ["gdp", "gdp_activity_tiers", "gdp_user_type_share"].map(metric => ({metric, wiki: "nlwiki", scope: "wiki", rows: 10}))};
  const valid = {wiki: "nlwiki", duration_ms: 10, rows: 30, memory_headroom_ratio: 0.5,
    parquet_requests: ["https://example/browser-data/gdp/nlwiki.parquet"]};
  assert.doesNotThrow(() => validateProfile(valid, budgets, index));
  assert.throws(() => validateProfile({...valid, parquet_requests: ["https://example/ptwiki.parquet"]}, budgets, index), /unrelated/);
  assert.throws(() => validateProfile({...valid, duration_ms: 6000}, budgets, index), /took/);
  assert.throws(() => validateProfile({...valid, memory_headroom_ratio: 0.1}, budgets, index), /headroom/);
  assert.throws(() => validateProfile({...valid, rows: 29}, budgets, index), /index declares/);
});

test("all-wiki profile accounts only for overlapping global shards", () => {
  const entries = ["2025", "2026", "2027"].flatMap(year =>
    ["gdp", "gdp_activity_tiers", "gdp_user_type_share"].map(metric => ({
      metric, wiki: "all", scope: "global", rows: 10, minimum_date: `${year}-01`, maximum_date: `${year}-12`,
      file: `browser-data/${metric}/all-${year}.parquet`,
    })));
  const index = {entries};
  const selected = entries.filter(entry => entry.maximum_date >= "2025-12" && entry.minimum_date <= "2026-01");
  const valid = {wiki: "all", start_period: "2025-12", end_period: "2026-01",
    duration_ms: 20, rows: 60, memory_headroom_ratio: 0.5,
    cache_hits: 3, parquet_requests: selected.slice(3).map(entry => `https://example/${entry.file}`)};
  assert.doesNotThrow(() => validateProfile(valid, budgets, index));
  assert.throws(() => validateProfile({...valid, cache_hits: 2}, budgets, index), /exactly the indexed/);
  assert.throws(() => validateProfile({...valid, parquet_requests: [...valid.parquet_requests, "https://example/other.parquet"]}, budgets, index), /exactly the indexed/);
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

test("browser startup retries with a clean profile", async () => {
  const profiles = [];
  const children = [];
  const spawnChrome = (_executable, arguments_) => {
    const child = new EventEmitter();
    child.exitCode = null;
    child.signalCode = null;
    child.stderr = new EventEmitter();
    child.kill = signal => {
      child.signalCode = signal;
      process.nextTick(() => child.emit("exit"));
      return true;
    };
    profiles.push(arguments_.find(argument => argument.startsWith("--user-data-dir=")).slice("--user-data-dir=".length));
    children.push(child);
    return child;
  };
  let calls = 0;
  const browser = await launchChrome({budgets, executable: "/test/chrome", spawnChrome,
    awaitActivePort: async () => {
      calls += 1;
      if (calls === 1) {
        children[0].stderr.emit("data", Buffer.from("transient startup failure"));
        throw new Error("missing DevToolsActivePort");
      }
      return "9222\n";
    }});
  roots.push(browser.userData);
  assert.equal(browser.port, "9222");
  assert.equal(calls, 2);
  assert.equal(fs.existsSync(profiles[0]), false);
  assert.equal(fs.existsSync(profiles[1]), true);
  await terminateChild(browser.chrome);
});
