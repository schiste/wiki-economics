import {createBrowserCache} from "./browser-cache.js";
import {ALL_WIKIS, isAllWikis} from "./wiki-scope.js";

const DEFAULT_INDEX_URL = "/browser-data/index.json";
let decoderPromise;

function decoder() {
  if (!decoderPromise) {
    decoderPromise = Promise.all([
      import("npm:apache-arrow"),
      import("npm:parquet-wasm"),
    ]).then(async ([Arrow, Parquet]) => {
      await Parquet.default({module_or_path: import.meta.resolve("npm:parquet-wasm/esm/parquet_wasm_bg.wasm")});
      return {Arrow, Parquet};
    });
  }
  return decoderPromise;
}

function arrowRowToObject(row) {
  const object = row.toJSON();
  for (const key in object) {
    if (typeof object[key] === "bigint") object[key] = Number(object[key]);
  }
  return object;
}

export function validateBrowserIndex(index) {
  if (index?.schema_version !== 3
      || !Number.isSafeInteger(index?.cache_schema_version)
      || index.cache_schema_version <= 0
      || !/^[0-9a-f]{64}$/.test(index?.generation || "")
      || index?.license_spdx !== "MIT"
      || !Array.isArray(index?.entries)
      || index.entries.length === 0
      || index.entries.some((entry) => !/^[0-9a-f]{64}$/.test(entry?.artifact_receipt_sha256 || "")
        || !["wiki", "global"].includes(entry?.scope))) {
    throw new Error("Invalid browser data index");
  }
  return index;
}

export function selectBrowserEntries(index, metrics, wiki, {startPeriod, endPeriod} = {}) {
  const requested = new Set(Object.values(metrics));
  const all = isAllWikis(wiki);
  const selected = index.entries.filter(entry => requested.has(entry.metric)
    && (all
      ? entry.scope === "global" && entry.wiki === ALL_WIKIS
        && (!startPeriod || entry.maximum_date >= startPeriod)
        && (!endPeriod || entry.minimum_date <= endPeriod)
      : entry.scope === "wiki" && entry.wiki === wiki));
  for (const metric of requested) {
    if (!selected.some(entry => entry.metric === metric)) {
      throw new Error(`Browser data index has no ${metric} partition for ${wiki}`);
    }
  }
  return selected.sort((left, right) => left.metric.localeCompare(right.metric)
    || left.wiki.localeCompare(right.wiki)
    || left.minimum_date.localeCompare(right.minimum_date) || left.file.localeCompare(right.file));
}

async function sha256Hex(buffer) {
  const digest = await crypto.subtle.digest("SHA-256", buffer);
  return Array.from(new Uint8Array(digest), byte => byte.toString(16).padStart(2, "0")).join("");
}

async function decodeParquet(buffer) {
  const {Arrow, Parquet} = await decoder();
  const table = Arrow.tableFromIPC(Parquet.readParquet(new Uint8Array(buffer)).intoIPCStream());
  return Array.from(table, arrowRowToObject);
}

function emitLoad(detail) {
  if (typeof document !== "undefined" && typeof CustomEvent !== "undefined") {
    document.dispatchEvent(new CustomEvent("wiki-econ:data-load", {detail}));
  }
}

export function makeRowsLoader(metrics, {
  indexUrl = DEFAULT_INDEX_URL,
  fetchImpl = (...args) => fetch(...args),
  cache = createBrowserCache(),
} = {}) {
  let indexPromise;
  let activeKey;
  let activePromise;

  async function indexAndUrl() {
    if (!indexPromise) {
      indexPromise = (async () => {
        const response = await fetchImpl(indexUrl, {cache: "no-cache"});
        if (!response.ok) throw new Error(`Unable to load browser data index (${response.status})`);
        return {index: validateBrowserIndex(await response.json()), url: response.url || indexUrl};
      })();
    }
    return indexPromise;
  }

  return async function loadRows(wiki, range = {}) {
    const {index, url} = await indexAndUrl();
    const entries = selectBrowserEntries(index, metrics, wiki, range);
    const key = `${wiki}:${entries.map(entry => entry.sha256).join(":")}`;
    if (activeKey === key && activePromise) return activePromise;
    activeKey = key;
    activePromise = (async () => {
      const started = performance.now();
      let cacheHits = 0;
      let compressedBytes = 0;
      const grouped = new Map();
      for (const entry of entries) {
        let buffer = await cache.get(entry, index.cache_schema_version);
        if (buffer) {
          cacheHits += 1;
        } else {
          const response = await fetchImpl(new URL(`/${entry.file}`, url).href);
          if (!response.ok) throw new Error(`Unable to load ${entry.file} (${response.status})`);
          buffer = await response.arrayBuffer();
          if (buffer.byteLength !== entry.bytes || await sha256Hex(buffer) !== entry.sha256) {
            throw new Error(`Browser data integrity check failed for ${entry.file}`);
          }
          void cache.put(entry, index.cache_schema_version, buffer).catch(() => false);
        }
        compressedBytes += entry.bytes;
        const rows = await decodeParquet(buffer);
        const list = grouped.get(entry.metric) || [];
        list.push(...rows);
        grouped.set(entry.metric, list);
      }
      const output = Object.fromEntries(Object.entries(metrics).map(([name, metric]) => [name, grouped.get(metric) || []]));
      emitLoad({wiki, metrics: [...new Set(Object.values(metrics))].sort(), compressedBytes,
        rows: Object.values(output).reduce((total, rows) => total + rows.length, 0), cacheHits,
        durationMs: performance.now() - started});
      return output;
    })();
    try {
      return await activePromise;
    } catch (error) {
      activeKey = undefined;
      activePromise = undefined;
      throw error;
    }
  };
}
