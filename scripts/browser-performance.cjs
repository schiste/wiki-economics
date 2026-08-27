#!/usr/bin/env node
"use strict";

const fs = require("node:fs");
const http = require("node:http");
const os = require("node:os");
const path = require("node:path");
const {spawn} = require("node:child_process");

const root = path.resolve(__dirname, "..");
const defaultBudgets = path.join(root, "config", "browser-performance-budgets.json");

function listFiles(directory, prefix = "") {
  return fs.readdirSync(directory, {withFileTypes: true}).flatMap(entry => {
    const relative = path.posix.join(prefix, entry.name);
    return entry.isDirectory() ? listFiles(path.join(directory, entry.name), relative) : [relative];
  }).sort();
}

function validateStaticBudgets(distDir, budgets) {
  const files = listFiles(distDir);
  const javascript = files.filter(file => file.endsWith(".js")).map(file => ({file, bytes: fs.statSync(path.join(distDir, file)).size}));
  const wasm = files.filter(file => file.endsWith(".wasm")).map(file => ({file, bytes: fs.statSync(path.join(distDir, file)).size}));
  const oversizedJavaScript = javascript.filter(file => file.bytes > budgets.artifacts.maximum_javascript_chunk_bytes);
  const oversizedWasm = wasm.filter(file => file.bytes > budgets.artifacts.maximum_wasm_chunk_bytes);
  if (oversizedJavaScript.length) throw new Error(`JavaScript chunk exceeds budget: ${oversizedJavaScript[0].file}`);
  if (oversizedWasm.length) throw new Error(`WASM chunk exceeds budget: ${oversizedWasm[0].file}`);
  if (wasm.length > budgets.artifacts.maximum_wasm_chunks) throw new Error(`WASM chunk count ${wasm.length} exceeds budget`);
  if (files.some(file => /duckdb/i.test(file))) throw new Error("unexpected DuckDB browser artifact");
  return {javascript_chunks: javascript.length, largest_javascript_bytes: Math.max(0, ...javascript.map(file => file.bytes)),
    wasm_chunks: wasm.length, largest_wasm_bytes: Math.max(0, ...wasm.map(file => file.bytes))};
}

function validateProfile(profile, budgets, index) {
  const metrics = new Set(["gdp", "gdp_activity_tiers", "gdp_user_type_share"]);
  const selectedEntries = index.entries.filter(entry => metrics.has(entry.metric)
    && (profile.wiki === "all"
      ? entry.scope === "global" && entry.wiki === "all"
        && entry.maximum_date >= profile.start_period && entry.minimum_date <= profile.end_period
      : entry.scope === "wiki" && entry.wiki === profile.wiki));
  if (profile.wiki === "all") {
    const requestedFiles = new Set(profile.parquet_requests.map(request => new URL(request).pathname.slice(1)));
    const indexedFiles = new Set(selectedEntries.map(entry => entry.file));
    if ([...requestedFiles].some(file => !indexedFiles.has(file))
        || requestedFiles.size + profile.cache_hits !== selectedEntries.length) {
      throw new Error("all-wiki query did not load exactly the indexed metric partitions");
    }
  } else if (profile.parquet_requests.some(request => !request.includes(`/${profile.wiki}.parquet`))) {
    throw new Error(`${profile.wiki} downloaded data for an unrelated wiki`);
  }
  if (profile.wiki !== "all" && profile.parquet_requests.length === 0) {
    throw new Error(`${profile.wiki} custom query downloaded no Parquet data`);
  }
  if (profile.memory_headroom_ratio < budgets.reference_device.minimum_memory_headroom_ratio) {
    throw new Error(`${profile.wiki} browser memory headroom ${(profile.memory_headroom_ratio * 100).toFixed(1)}% is below budget`);
  }
  if (profile.duration_ms > budgets.custom_query.maximum_duration_ms) {
    throw new Error(`${profile.wiki} custom query took ${profile.duration_ms.toFixed(1)}ms`);
  }
  const indexedRows = selectedEntries.reduce((total, entry) => total + entry.rows, 0);
  if (profile.rows !== indexedRows) throw new Error(`${profile.wiki} loaded ${profile.rows} rows; index declares ${indexedRows}`);
  const normalized = profile.duration_ms / Math.max(1, profile.rows / 1000);
  if (normalized > budgets.custom_query.maximum_duration_ms_per_1000_rows) {
    throw new Error(`${profile.wiki} custom query took ${normalized.toFixed(1)}ms per 1,000 rows`);
  }
}

function contentType(file) {
  if (file.endsWith(".html")) return "text/html; charset=utf-8";
  if (file.endsWith(".js")) return "text/javascript; charset=utf-8";
  if (file.endsWith(".css")) return "text/css; charset=utf-8";
  if (file.endsWith(".json")) return "application/json";
  if (file.endsWith(".wasm")) return "application/wasm";
  if (file.endsWith(".parquet")) return "application/vnd.apache.parquet";
  return "application/octet-stream";
}

async function startServer(distDir) {
  const server = http.createServer((request, response) => {
    let pathname = decodeURIComponent(new URL(request.url, "http://localhost").pathname);
    if (pathname === "/") pathname = "/index.html";
    if (!path.extname(pathname)) pathname += ".html";
    const file = path.resolve(distDir, `.${pathname}`);
    if (!file.startsWith(`${path.resolve(distDir)}${path.sep}`) || !fs.statSync(file, {throwIfNoEntry: false})?.isFile()) {
      response.writeHead(404).end("not found");
      return;
    }
    response.setHeader("content-type", contentType(file));
    response.setHeader("cache-control", "no-store");
    fs.createReadStream(file).pipe(response);
  });
  await new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  return {server, origin: `http://127.0.0.1:${server.address().port}`};
}

function chromeExecutable() {
  const candidates = [process.env.CHROME_BIN,
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
    "/usr/bin/google-chrome", "/usr/bin/chromium", "/usr/bin/chromium-browser"].filter(Boolean);
  const executable = candidates.find(candidate => fs.existsSync(candidate));
  if (!executable) throw new Error("Chrome/Chromium not found; set CHROME_BIN");
  return executable;
}

async function waitForFile(file, timeoutMs = 10000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (fs.existsSync(file)) return fs.readFileSync(file, "utf8");
    await new Promise(resolve => setTimeout(resolve, 50));
  }
  throw new Error(`timed out waiting for ${file}`);
}

class Cdp {
  constructor(url) {
    this.nextId = 1;
    this.pending = new Map();
    this.listeners = new Map();
    this.socket = new WebSocket(url);
  }
  async open() {
    await new Promise((resolve, reject) => {
      this.socket.addEventListener("open", resolve, {once: true});
      this.socket.addEventListener("error", reject, {once: true});
    });
    this.socket.addEventListener("message", event => {
      const message = JSON.parse(event.data);
      if (message.id) {
        const pending = this.pending.get(message.id);
        this.pending.delete(message.id);
        if (message.error) pending.reject(new Error(message.error.message));
        else pending.resolve(message.result);
      } else {
        for (const listener of this.listeners.get(message.method) || []) listener(message.params);
      }
    });
  }
  send(method, params = {}) {
    return new Promise((resolve, reject) => {
      const id = this.nextId++;
      this.pending.set(id, {resolve, reject});
      this.socket.send(JSON.stringify({id, method, params}));
    });
  }
  on(method, listener) {
    const listeners = this.listeners.get(method) || [];
    listeners.push(listener);
    this.listeners.set(method, listeners);
  }
  close() { this.socket.close(); }
}

async function evaluate(cdp, expression, awaitPromise = true) {
  const result = await cdp.send("Runtime.evaluate", {expression, awaitPromise, returnByValue: true});
  if (result.exceptionDetails) throw new Error(result.exceptionDetails.text || "browser evaluation failed");
  return result.result.value;
}

async function waitFor(cdp, expression, timeoutMs = 10000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await evaluate(cdp, expression)) return;
    await new Promise(resolve => setTimeout(resolve, 50));
  }
  throw new Error(`browser condition timed out: ${expression}`);
}

async function navigate(cdp, url) {
  await cdp.send("Page.navigate", {url});
  await waitFor(cdp, "document.readyState === 'complete'");
  await waitFor(cdp, "document.body && document.body.dataset.wkStage === 'done'");
}

async function terminateChild(child, graceMs = 2000) {
  if (child.exitCode !== null || child.signalCode !== null) return;
  const exited = new Promise(resolve => child.once("exit", resolve));
  child.kill("SIGTERM");
  await Promise.race([exited, new Promise(resolve => setTimeout(resolve, graceMs))]);
  if (child.exitCode === null && child.signalCode === null) {
    child.kill("SIGKILL");
    await exited;
  }
}

async function runBrowserPerformance({distDir, budgets}) {
  const browserIndex = JSON.parse(fs.readFileSync(path.join(distDir, "browser-data", "index.json"), "utf8"));
  const staticArtifacts = validateStaticBudgets(distDir, budgets);
  const {server, origin} = await startServer(distDir);
  const userData = fs.mkdtempSync(path.join(os.tmpdir(), "wiki-econ-chrome-"));
  const activePort = path.join(userData, "DevToolsActivePort");
  const chrome = spawn(chromeExecutable(), ["--headless=new", "--no-sandbox", "--disable-gpu", "--disable-background-networking",
    "--disable-default-apps", "--disable-extensions", "--disable-sync", "--metrics-recording-only", "--no-first-run",
    `--js-flags=--max-old-space-size=${budgets.reference_device.javascript_heap_limit_mib}`,
    "--remote-debugging-port=0", `--user-data-dir=${userData}`, "about:blank"], {stdio: "ignore"});
  let cdp;
  try {
    const [port] = (await waitForFile(activePort)).trim().split("\n");
    const target = await fetch(`http://127.0.0.1:${port}/json/new?about:blank`, {method: "PUT"}).then(response => response.json());
    cdp = new Cdp(target.webSocketDebuggerUrl);
    await cdp.open();
    const requests = [];
    cdp.on("Network.requestWillBeSent", event => requests.push(event.request.url));
    await Promise.all([cdp.send("Page.enable"), cdp.send("Runtime.enable"), cdp.send("Network.enable"), cdp.send("Performance.enable")]);
    await cdp.send("Page.addScriptToEvaluateOnNewDocument", {source: `window.__wikiEconLoads=[];document.addEventListener("wiki-econ:data-load",event=>window.__wikiEconLoads.push(event.detail));`});

    let requestStart = requests.length;
    await navigate(cdp, `${origin}/gdp.html`);
    const defaultWikiPicker = await evaluate(cdp, `(() => {
      const select = document.querySelector(".filters-bar select");
      return select ? {
        label: select.selectedOptions[0]?.textContent?.trim(),
        query: new URLSearchParams(location.search).get("wiki"),
      } : null;
    })()`);
    if (defaultWikiPicker?.query !== "all" || defaultWikiPicker?.label !== "All wikis") {
      throw new Error(`default wiki picker is invalid: ${JSON.stringify(defaultWikiPicker)}`);
    }
    const defaultRequests = requests.slice(requestStart);
    if (defaultRequests.some(url => url.endsWith(".parquet") || url.endsWith(".wasm")
        || url.endsWith("/browser-data/index.json") || url.includes("/apache-arrow@")
        || url.includes("/parquet-wasm@"))) {
      throw new Error("warm default rendering fetched browser query data");
    }

    const profiles = [];
    for (const wiki of [...Object.keys(budgets.fixtures).sort(), "all"]) {
      requestStart = requests.length;
      let peakHeapUsed = 0;
      const sampler = setInterval(() => {
        void cdp.send("Performance.getMetrics").then(result => {
          peakHeapUsed = Math.max(peakHeapUsed,
            result.metrics.find(metric => metric.name === "JSHeapUsedSize")?.value || 0);
        }).catch(() => {});
      }, 20);
      const query = new URLSearchParams({wiki, types: "registered", gran: "month", start: "2025-12", end: "2026-01", ns: "0", breakdown: "false"});
      try {
        await cdp.send("Page.navigate", {url: `${origin}/gdp.html?${query}`});
        await waitFor(cdp, "document.readyState === 'complete'");
        await waitFor(cdp, "window.__wikiEconLoads && window.__wikiEconLoads.length > 0", 15000);
        await waitFor(cdp, "document.body.dataset.wkStage === 'done'", 15000);
      } finally {
        clearInterval(sampler);
      }
      const load = await evaluate(cdp, "window.__wikiEconLoads.at(-1)");
      const metrics = await cdp.send("Performance.getMetrics");
      const heapUsed = metrics.metrics.find(metric => metric.name === "JSHeapUsedSize")?.value || 0;
      peakHeapUsed = Math.max(peakHeapUsed, heapUsed);
      const heapLimit = await evaluate(cdp, "performance.memory?.jsHeapSizeLimit || 0");
      const profile = {wiki, start_period: "2025-12", end_period: "2026-01",
        duration_ms: load.durationMs, rows: load.rows, compressed_bytes: load.compressedBytes,
        cache_hits: load.cacheHits, peak_heap_used_bytes: peakHeapUsed, heap_limit_bytes: heapLimit,
        memory_headroom_ratio: heapLimit > 0 ? (heapLimit - peakHeapUsed) / heapLimit : 0,
        parquet_requests: requests.slice(requestStart).filter(url => url.endsWith(".parquet"))};
      validateProfile(profile, budgets, browserIndex);
      profiles.push(profile);
    }
    requestStart = requests.length;
    const warmQuery = new URLSearchParams({wiki: "nlwiki", types: "registered", gran: "month",
      start: "2025-12", end: "2026-01", ns: "0", breakdown: "false"});
    await cdp.send("Page.navigate", {url: `${origin}/gdp.html?${warmQuery}`});
    await waitFor(cdp, "window.__wikiEconLoads && window.__wikiEconLoads.length > 0", 15000);
    const warmLoad = await evaluate(cdp, "window.__wikiEconLoads.at(-1)");
    const warmRequests = requests.slice(requestStart).filter(url => url.endsWith(".parquet"));
    if (warmRequests.length > 0 || warmLoad.cacheHits !== 3) {
      throw new Error("warm IndexedDB query did not reuse all selected-wiki partitions");
    }
    return {schema_version: 1, chrome: chromeExecutable(), budgets, static_artifacts: staticArtifacts,
      default: {parquet_requests: 0, browser_index_requests: 0, wasm_requests: 0, query_runtime_requests: 0},
      warm_indexeddb: {wiki: "nlwiki", cache_hits: warmLoad.cacheHits, parquet_requests: warmRequests.length}, profiles};
  } finally {
    cdp?.close();
    await terminateChild(chrome);
    await new Promise(resolve => server.close(resolve));
    fs.rmSync(userData, {recursive: true, force: true, maxRetries: 5, retryDelay: 100});
  }
}

function parseArguments(argv) {
  const options = {budgets: defaultBudgets};
  for (let index = 0; index < argv.length; index += 2) {
    const name = argv[index];
    if (!["--dist-dir", "--budgets", "--report"].includes(name) || !argv[index + 1]) {
      throw new Error("usage: browser-performance.cjs --dist-dir PATH [--budgets PATH] [--report PATH]");
    }
    options[name.slice(2)] = path.resolve(argv[index + 1]);
  }
  if (!options["dist-dir"]) throw new Error("--dist-dir is required");
  return options;
}

async function main() {
  const options = parseArguments(process.argv.slice(2));
  const budgets = JSON.parse(fs.readFileSync(options.budgets, "utf8"));
  const report = await runBrowserPerformance({distDir: options["dist-dir"], budgets});
  const output = `${JSON.stringify(report, null, 2)}\n`;
  if (options.report) fs.writeFileSync(options.report, output);
  process.stdout.write(output);
}

if (require.main === module) main().catch(error => { console.error(error.stack || error.message); process.exitCode = 1; });

module.exports = {listFiles, parseArguments, runBrowserPerformance, terminateChild, validateProfile, validateStaticBudgets};
