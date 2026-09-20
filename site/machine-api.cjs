#!/usr/bin/env node
"use strict";

// Public, read-only machine interfaces for the published wiki-economics data.
// This module deliberately uses the publication manifest as its allow-list:
// an artifact that is not in that manifest is never reachable through the API
// or MCP, even if a similarly named file happens to exist on NFS.

const fs = require("node:fs");
const path = require("node:path");
const crypto = require("node:crypto");

const API_PREFIX = "/api/v1";
const MCP_PATH = "/mcp";
const FRESHNESS_PATH = "/health/freshness.json";
const API_SCHEMA_VERSION = 1;
const MCP_SERVER_VERSION = "1.0.0";
const MCP_LATEST_PROTOCOL = "2026-07-28";
const MCP_SUPPORTED_PROTOCOLS = new Set([
  MCP_LATEST_PROTOCOL,
  "2025-11-25",
  "2024-11-05",
]);
const MAX_MCP_BODY_BYTES = 1024 * 1024;
const MAX_MCP_BATCH_MESSAGES = 64;
const RATE_WINDOW_MS = 1_000;
const DEFAULT_RATE_LIMIT_PER_SECOND = 30;
const DEFAULT_RATE_LIMIT_MAX_CLIENTS = 4_096;
const PUBLIC_CACHE_CONTROL = "public, max-age=60, must-revalidate";
const ARTIFACT_CACHE_CONTROL = "public, max-age=300, must-revalidate";
const JSON_MEDIA_TYPE = "application/json; charset=utf-8";

const DEFAULT_METRIC_CATALOG = path.resolve(__dirname, "../config/generated/metric-catalog.json");

const TOOL_DEFINITIONS = [
  {
    name: "list_published_wikis",
    title: "List published wikis",
    description: "List Wikimedia projects with a currently published generation.",
    inputSchema: {type: "object", properties: {}, additionalProperties: false},
  },
  {
    name: "list_datasets",
    title: "List published datasets",
    description: "List published metric definitions and their machine-downloadable artifacts.",
    inputSchema: {type: "object", properties: {}, additionalProperties: false},
  },
  {
    name: "get_dataset",
    title: "Get a dataset",
    description: "Resolve one published dataset to immutable metadata and download links.",
    inputSchema: {
      type: "object",
      properties: {
        dataset: {type: "string", pattern: "^[a-z0-9_]+$", description: "Metric or JSON dataset identifier."},
        wiki: {type: "string", pattern: "^[a-z0-9_]+wiki$", description: "Optional wiki-specific partition."},
      },
      required: ["dataset"],
      additionalProperties: false,
    },
  },
  {
    name: "get_freshness",
    title: "Get publication freshness",
    description: "Return the public freshness and alert assessment for the published data.",
    inputSchema: {type: "object", properties: {}, additionalProperties: false},
  },
];

function readJson(file) {
  return JSON.parse(fs.readFileSync(file, "utf8"));
}

function positiveInteger(value, fallback) {
  const text = String(value ?? "").trim();
  if (!/^\d+$/.test(text)) return fallback;
  const parsed = Number(text);
  return Number.isSafeInteger(parsed) && parsed > 0 ? parsed : fallback;
}

function requestHeader(req, name) {
  const value = req.headers?.[name.toLowerCase()];
  return Array.isArray(value) ? value[0] || "" : value || "";
}

function machineClientKey(req) {
  const forwarded = requestHeader(req, "x-forwarded-for").split(",")[0].trim();
  const peer = req.socket?.remoteAddress;
  return forwarded
    || requestHeader(req, "x-real-ip").trim()
    || (typeof peer === "string" ? peer.trim() : "")
    || "unknown";
}

function shortClientFingerprint(client) {
  return crypto.createHash("sha256").update(client).digest("hex").slice(0, 12);
}

function logMessage(logger, level, message) {
  const method = logger?.[level];
  if (typeof method !== "function") return;
  try { method.call(logger, message); } catch {}
}

function createRateLimiter({
  limitPerSecond = DEFAULT_RATE_LIMIT_PER_SECOND,
  maxClients = DEFAULT_RATE_LIMIT_MAX_CLIENTS,
  now = Date.now,
  logger = console,
} = {}) {
  const limit = positiveInteger(limitPerSecond, DEFAULT_RATE_LIMIT_PER_SECOND);
  const clientLimit = positiveInteger(maxClients, DEFAULT_RATE_LIMIT_MAX_CLIENTS);
  const buckets = new Map();
  let checks = 0;

  function prune(current) {
    checks += 1;
    if (checks % 128 !== 0) return;
    for (const [key, bucket] of buckets) {
      if (bucket.resetAt <= current) buckets.delete(key);
    }
  }

  function check(req) {
    const current = now();
    const key = machineClientKey(req);
    prune(current);
    let bucket = buckets.get(key);
    if (!bucket || bucket.resetAt <= current) {
      if (!bucket && buckets.size >= clientLimit) {
        const oldest = buckets.keys().next().value;
        if (oldest !== undefined) buckets.delete(oldest);
      }
      bucket = {count: 0, resetAt: current + RATE_WINDOW_MS, warned: false};
      buckets.set(key, bucket);
    }
    bucket.count += 1;
    const allowed = bucket.count <= limit;
    if (!allowed && !bucket.warned) {
      bucket.warned = true;
      logMessage(logger, "warn", `[machine-api] rate limit exceeded client=${shortClientFingerprint(key)} limit=${limit}/s`);
    }
    return {
      allowed,
      limit,
      remaining: Math.max(0, limit - bucket.count),
      resetAt: bucket.resetAt,
      now: current,
    };
  }

  return {check, size: () => buckets.size, limit, maxClients: clientLimit};
}

function rateLimitHeaders(decision) {
  const resetSeconds = Math.max(1, Math.ceil((decision.resetAt - decision.now) / 1_000));
  return {
    "RateLimit-Limit": String(decision.limit),
    "RateLimit-Remaining": String(decision.remaining),
    "RateLimit-Reset": String(resetSeconds),
    "RateLimit-Policy": `${decision.limit};w=1`,
  };
}

function jsonRpcError(id, code, message, data) {
  const error = {code, message};
  if (data !== undefined) error.data = data;
  return {jsonrpc: "2.0", id: id ?? null, error};
}

function jsonRpcResult(id, result) {
  return {jsonrpc: "2.0", id, result};
}

function validWiki(wiki) {
  return typeof wiki === "string" && /^[a-z0-9_]+wiki$/.test(wiki);
}

function validDataset(dataset) {
  return typeof dataset === "string" && /^[a-z0-9_]+$/.test(dataset);
}

function normalizeArtifactName(value) {
  if (typeof value !== "string" || value.length === 0 || value.length > 512) return null;
  let decoded;
  try {
    decoded = decodeURIComponent(value);
  } catch {
    return null;
  }
  if (!decoded || decoded.startsWith("/") || decoded.includes("\\") || decoded.includes("\0")) return null;
  const normalized = path.posix.normalize(decoded);
  if (normalized !== decoded || normalized === "." || normalized.startsWith("../") || normalized.includes("/../")) {
    return null;
  }
  return normalized;
}

function requestBaseUrl(req, configuredOrigin) {
  if (configuredOrigin) return configuredOrigin.replace(/\/+$/, "");
  const forwardedProto = requestHeader(req, "x-forwarded-proto") || "http";
  const forwardedHost = requestHeader(req, "x-forwarded-host") || requestHeader(req, "host") || "localhost";
  return `${forwardedProto.split(",")[0].trim()}://${forwardedHost.split(",")[0].trim()}`;
}

function addPublicHeaders(res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, HEAD, POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Range, MCP-Protocol-Version, Mcp-Method, Mcp-Name");
  res.setHeader("Access-Control-Expose-Headers", "Content-Length, Content-Range, ETag, Last-Modified, MCP-Protocol-Version, RateLimit-Limit, RateLimit-Remaining, RateLimit-Reset, RateLimit-Policy, Retry-After");
  res.setHeader("X-Content-Type-Options", "nosniff");
}

function writeJson(res, statusCode, body, cacheControl = PUBLIC_CACHE_CONTROL, extraHeaders = {}) {
  addPublicHeaders(res);
  res.writeHead(statusCode, {
    "Content-Type": JSON_MEDIA_TYPE,
    "Cache-Control": cacheControl,
    ...extraHeaders,
  });
  res.end(JSON.stringify(body));
}

function toPublicMetric(metric, artifacts) {
  return {
    id: metric.id,
    family: metric.family,
    algorithm_version: metric.algorithm_version,
    schema: metric.schema,
    aggregation: metric.aggregation,
    publication: metric.publication,
    browser: metric.browser,
    artifacts,
  };
}

function artifactUrl(req, name, configuredOrigin) {
  const base = requestBaseUrl(req, configuredOrigin);
  return new URL(`${API_PREFIX}/artifacts/${name.split("/").map(encodeURIComponent).join("/")}`, `${base}/`).toString();
}

function classifyArtifact(name, metricsById, publishedWikis) {
  if (name === "browser-data-index.json") {
    return publishedWikis.size > 0 ? {kind: "browser_index", dataset: null, wiki: null} : null;
  }

  const metadata = /^(?:defaults|meta)_([a-z0-9_]+)\.json$/.exec(name);
  if (metadata && metricsById.has(metadata[1])) {
    return {kind: "metric_metadata", dataset: metadata[1], wiki: null};
  }

  const rootMetric = /^([a-z0-9_]+)\.parquet$/.exec(name);
  if (rootMetric && metricsById.has(rootMetric[1])
      && metricsById.get(rootMetric[1]).publication?.scope !== "per_wiki_only") {
    return {kind: "merged_metric", dataset: rootMetric[1], wiki: null};
  }

  const wikiMetric = /^([a-z0-9_]+wiki)\/([a-z0-9_]+)\.parquet$/.exec(name);
  if (wikiMetric && publishedWikis.has(wikiMetric[1]) && metricsById.has(wikiMetric[2])) {
    return {kind: "wiki_metric", dataset: wikiMetric[2], wiki: wikiMetric[1]};
  }

  const browserMetric = /^browser-data\/([a-z0-9_]+)\/([^/]+)\.parquet$/.exec(name);
  const metric = browserMetric && metricsById.get(browserMetric[1]);
  if (!metric || metric.browser?.partitioning !== "per_wiki_and_global_year_shards") return null;
  const partition = browserMetric[2];
  if (validWiki(partition) && publishedWikis.has(partition)) {
    return {kind: "browser_wiki_partition", dataset: browserMetric[1], wiki: partition};
  }
  if (/^all-\d{4}$/.test(partition) && publishedWikis.size > 0) {
    return {kind: "browser_global_partition", dataset: browserMetric[1], wiki: null};
  }
  return null;
}

function publicArtifact(record, req, configuredOrigin, metricsById, publishedWikis) {
  const name = normalizeArtifactName(record?.name);
  if (!name) return null;
  const classification = classifyArtifact(name, metricsById, publishedWikis);
  if (!classification) return null;
  return {
    name,
    kind: classification.kind,
    dataset: classification.dataset,
    wiki: classification.wiki,
    bytes: Number.isSafeInteger(record.bytes) ? record.bytes : null,
    size_kb: Number.isSafeInteger(record.size_kb) ? record.size_kb : null,
    rows: Number.isSafeInteger(record.rows) ? record.rows : null,
    minimum_date: typeof record.minimum_date === "string" ? record.minimum_date : null,
    maximum_date: typeof record.maximum_date === "string" ? record.maximum_date : null,
    scope: typeof record.scope === "string" ? record.scope : null,
    shard: typeof record.shard === "string" ? record.shard : null,
    sha256: /^[0-9a-f]{64}$/i.test(record.sha256 || "") ? record.sha256 : null,
    artifact_receipt_sha256: /^[0-9a-f]{64}$/i.test(record.artifact_receipt_sha256 || "")
      ? record.artifact_receipt_sha256
      : null,
    media_type: typeof record.media_type === "string" ? record.media_type : "application/octet-stream",
    license_spdx: typeof record.license_spdx === "string" ? record.license_spdx : "MIT",
    url: artifactUrl(req, name, configuredOrigin),
  };
}

function buildPublicCatalog({manifest, metricCatalog, req, configuredOrigin}) {
  if (!manifest || typeof manifest !== "object" || manifest.schema_version !== 3) {
    throw new Error("published manifest is missing or unsupported");
  }
  if (!metricCatalog || metricCatalog.schema_version !== 1 || !Array.isArray(metricCatalog.metrics)) {
    throw new Error("metric catalog is missing or unsupported");
  }
  const lifecycle = manifest.lifecycle?.wikis || {};
  const manifestWikis = manifest.wikis || {};
  const publishedWikis = new Set(Object.entries(lifecycle)
    .filter(([wiki, entry]) => validWiki(wiki) && entry?.publication === "published" && manifestWikis[wiki])
    .map(([wiki]) => wiki));
  const metrics = metricCatalog.metrics.filter((metric) => {
    if (!validDataset(metric?.id)) return false;
    const scope = metric.publication?.scope;
    if (!["merged_and_per_wiki", "per_wiki_only"].includes(scope)) return false;
    if (metric.publication?.per_wiki_artifact !== `{wiki}/${metric.id}.parquet`) return false;
    if (scope === "merged_and_per_wiki" && metric.publication?.merged_artifact !== `${metric.id}.parquet`) return false;
    if (scope === "per_wiki_only" && metric.publication?.merged_artifact != null) return false;
    return metric.fingerprint?.artifact_identity === `${metric.id}.parquet`;
  });
  const metricsById = new Map(metrics.map((metric) => [metric.id, metric]));
  const artifacts = (manifest.downloadable_artifacts || [])
    .filter((record) => record && typeof record === "object")
    .map((record) => publicArtifact(record, req, configuredOrigin, metricsById, publishedWikis))
    .filter(Boolean)
    .sort((left, right) => left.name.localeCompare(right.name));
  const artifactsByDataset = new Map();
  for (const artifact of artifacts) {
    if (!artifact.dataset) continue;
    const list = artifactsByDataset.get(artifact.dataset) || [];
    list.push(artifact);
    artifactsByDataset.set(artifact.dataset, list);
  }
  const wikis = [...publishedWikis].sort().map((wiki) => {
    const state = manifest.wikis?.[wiki] || {};
    const wikiArtifacts = artifacts.filter((artifact) => artifact.wiki === wiki);
    return {
      wiki,
      snapshot: state.snapshot?.version || manifest.provenance?.selected_snapshot_versions?.[wiki] || null,
      status: state.status || null,
      metrics: wikiArtifacts.map((artifact) => artifact.dataset).filter((value, index, values) => values.indexOf(value) === index).sort(),
      artifacts: wikiArtifacts,
    };
  });
  const datasets = metrics
    .map((metric) => toPublicMetric(metric, artifactsByDataset.get(metric.id) || []))
    .filter((metric) => metric.artifacts.length > 0)
    .sort((left, right) => left.id.localeCompare(right.id));
  return {
    api_schema_version: API_SCHEMA_VERSION,
    generated_at: manifest.generated_at || null,
    license: manifest.license || null,
    attribution: manifest.attribution || null,
    independence_notice: manifest.independence_notice || null,
    source_datasets: manifest.source_datasets || [],
    privacy: manifest.privacy || null,
    provenance: {
      generating_commit: manifest.provenance?.generating_commit || null,
      selected_snapshot_versions: Object.fromEntries(
        Object.entries(manifest.provenance?.selected_snapshot_versions || {})
          .filter(([wiki]) => publishedWikis.has(wiki)),
      ),
      workload_profiles: Object.fromEntries(
        Object.entries(manifest.provenance?.workload_profiles || {})
          .filter(([wiki]) => publishedWikis.has(wiki)),
      ),
    },
    wikis,
    datasets,
    artifacts,
    links: {
      catalog: `${API_PREFIX}/catalog`,
      wikis: `${API_PREFIX}/wikis`,
      datasets: `${API_PREFIX}/datasets`,
      openapi: `${API_PREFIX}/openapi.json`,
      freshness: "/health/freshness.json",
      mcp: MCP_PATH,
    },
  };
}

function openApiDocument(req, configuredOrigin) {
  const server = requestBaseUrl(req, configuredOrigin);
  const rateLimited = {description: "Rate limit exceeded", headers: {"Retry-After": {schema: {type: "integer"}}}};
  return {
    openapi: "3.1.0",
    info: {
      title: "wiki-economics published data API",
      version: String(API_SCHEMA_VERSION),
      description: "Read-only access to manifest-allowlisted Wikimedia economic datasets.",
    },
    servers: [{url: server}],
    paths: {
      [`${API_PREFIX}`]: {get: {summary: "API discovery", responses: {"200": {description: "Endpoint index"}, "429": rateLimited}}},
      [`${API_PREFIX}/catalog`]: {get: {summary: "Published catalog", responses: {"200": {description: "Published datasets and artifacts"}, "429": rateLimited}}},
      [`${API_PREFIX}/wikis`]: {get: {summary: "Published wikis", responses: {"200": {description: "Published wiki list"}, "429": rateLimited}}},
      [`${API_PREFIX}/datasets`]: {get: {summary: "Published datasets", responses: {"200": {description: "Dataset definitions"}, "429": rateLimited}}},
      [`${API_PREFIX}/datasets/{dataset}`]: {
        get: {
          summary: "Resolve a dataset",
          parameters: [
            {name: "dataset", in: "path", required: true, schema: {type: "string"}},
            {name: "wiki", in: "query", required: false, schema: {type: "string"}},
          ],
          responses: {"200": {description: "Dataset metadata and artifact links"}, "404": {description: "Dataset not published"}, "429": rateLimited},
        },
      },
      [`${API_PREFIX}/artifacts/{path}`]: {
        get: {
          summary: "Download a published artifact",
          parameters: [{name: "path", in: "path", required: true, schema: {type: "string"}}],
          responses: {"200": {description: "Artifact bytes"}, "206": {description: "Partial artifact bytes"}, "404": {description: "Artifact not published"}, "429": rateLimited},
        },
        head: {summary: "Inspect an artifact", responses: {"200": {description: "Artifact headers"}, "429": rateLimited}},
      },
      [FRESHNESS_PATH]: {get: {summary: "Publication freshness", responses: {"200": {description: "Freshness and alert assessment"}, "429": rateLimited}}},
      [MCP_PATH]: {post: {summary: "MCP Streamable HTTP JSON-RPC", responses: {"200": {description: "JSON-RPC response"}, "429": rateLimited}}},
    },
  };
}

function datasetSelection(catalog, dataset, wiki) {
  if (!validDataset(dataset) || (wiki !== undefined && !validWiki(wiki))) return [];
  return catalog.datasets
    .filter((entry) => entry.id === dataset)
    .flatMap((entry) => entry.artifacts)
    .filter((artifact) => wiki === undefined || artifact.wiki === wiki);
}

function parseRange(header, size) {
  if (!header) return null;
  const match = /^bytes=(\d*)-(\d*)$/.exec(String(header).trim());
  if (!match) return {error: "Only one bytes range is supported"};
  let start = match[1] ? Number(match[1]) : null;
  let end = match[2] ? Number(match[2]) : null;
  if (start === null && end === null) return {error: "Invalid byte range"};
  if (start === null) {
    const suffix = end;
    if (!Number.isSafeInteger(suffix) || suffix <= 0) return {error: "Invalid byte range"};
    start = Math.max(0, size - suffix);
    end = size - 1;
  } else {
    if (!Number.isSafeInteger(start) || start < 0 || start >= size) return {error: "Range is outside the artifact"};
    end = end === null ? size - 1 : end;
    if (!Number.isSafeInteger(end) || end < start) return {error: "Invalid byte range"};
    end = Math.min(end, size - 1);
  }
  return {start, end};
}

function readBody(req, limit = MAX_MCP_BODY_BYTES) {
  return new Promise((resolve, reject) => {
    let body = "";
    let bytes = 0;
    let settled = false;
    const fail = (error) => {
      if (settled) return;
      settled = true;
      reject(error);
    };
    req.on("data", (chunk) => {
      if (settled) return;
      bytes += chunk.length;
      if (bytes > limit) {
        const error = new Error("request body is too large");
        error.code = "request_too_large";
        fail(error);
        return;
      }
      body += chunk;
    });
    req.on("error", fail);
    req.on("end", () => {
      if (!settled) {
        settled = true;
        resolve(body);
      }
    });
  });
}

function mcpResource(uri, name, description) {
  return {uri, name, description, mimeType: "application/json"};
}

function mcpText(value) {
  return {type: "text", text: JSON.stringify(value)};
}

function createMachineApi(options = {}) {
  const outputDir = path.resolve(options.outputDir || path.join(__dirname, "..", "output"));
  const logger = options.logger || console;
  const manifestLoader = options.manifestLoader || (() => readJson(path.join(outputDir, "manifest.json")));
  const metricCatalogLoader = options.metricCatalogLoader || (() => readJson(options.metricCatalogPath || DEFAULT_METRIC_CATALOG));
  const freshnessLoader = options.freshnessLoader || (() => ({status: "unknown", alerts: []}));
  const configuredOrigin = options.publicOrigin ? String(options.publicOrigin).replace(/\/+$/, "") : "";
  const rateLimiter = createRateLimiter({
    limitPerSecond: options.rateLimitPerSecond ?? process.env.WIKI_ECON_MACHINE_API_RATE_LIMIT_PER_SECOND,
    maxClients: options.rateLimitMaxClients ?? process.env.WIKI_ECON_MACHINE_API_RATE_LIMIT_MAX_CLIENTS,
    logger,
  });
  if (options.announceRateLimit !== false) {
    logMessage(logger, "info", `[machine-api] per-client rate limit enabled limit=${rateLimiter.limit}/s max_clients=${rateLimiter.maxClients}`);
  }

  function loadCatalog(req) {
    try {
      return buildPublicCatalog({
        manifest: manifestLoader(),
        metricCatalog: metricCatalogLoader(),
        req,
        configuredOrigin,
      });
    } catch (error) {
      logMessage(logger, "error", `[machine-api] published catalog unavailable: ${error.message}`);
      const safe = new Error("Published catalog unavailable");
      safe.code = "catalog_unavailable";
      throw safe;
    }
  }

  function loadFreshness() {
    try {
      return freshnessLoader();
    } catch (error) {
      logMessage(logger, "error", `[machine-api] freshness unavailable: ${error.message}`);
      const safe = new Error("Publication freshness unavailable");
      safe.code = "freshness_unavailable";
      throw safe;
    }
  }

  function loadArtifact(catalog, name) {
    const artifact = catalog.artifacts.find((candidate) => candidate.name === name);
    if (!artifact) return null;
    const file = path.resolve(outputDir, name);
    if (file !== outputDir && !file.startsWith(`${outputDir}${path.sep}`)) return null;
    let realFile;
    try {
      realFile = fs.realpathSync(file);
    } catch {
      return null;
    }
    let realRoot;
    try {
      realRoot = fs.realpathSync(outputDir);
    } catch {
      return null;
    }
    if (realFile !== realRoot && !realFile.startsWith(`${realRoot}${path.sep}`)) return null;
    let stat;
    try { stat = fs.statSync(realFile); } catch { return null; }
    if (!stat.isFile()) return null;
    return {artifact, file: realFile, stat};
  }

  function writeApiRoot(res) {
    writeJson(res, 200, {
      api_schema_version: API_SCHEMA_VERSION,
      name: "wiki-economics data API",
      read_only: true,
      endpoints: {
        catalog: `${API_PREFIX}/catalog`,
        wikis: `${API_PREFIX}/wikis`,
        datasets: `${API_PREFIX}/datasets`,
        openapi: `${API_PREFIX}/openapi.json`,
        artifacts: `${API_PREFIX}/artifacts/{path}`,
        freshness: "/health/freshness.json",
        mcp: MCP_PATH,
      },
      security: {
        rate_limit: {
          scope: "client",
          requests_per_second: rateLimiter.limit,
          window_seconds: RATE_WINDOW_MS / 1_000,
          max_tracked_clients: rateLimiter.maxClients,
          client_identity: "trusted X-Forwarded-For address, X-Real-IP, or socket peer",
          response_status: 429,
          headers: ["RateLimit-Limit", "RateLimit-Remaining", "RateLimit-Reset", "RateLimit-Policy", "Retry-After"],
        },
        mcp: {max_batch_messages: MAX_MCP_BATCH_MESSAGES},
      },
    });
  }

  function handleArtifact(req, res, catalog, encodedName) {
    const name = normalizeArtifactName(encodedName);
    const loaded = name ? loadArtifact(catalog, name) : null;
    if (!loaded) {
      writeJson(res, 404, {error: "Published artifact not found", code: "artifact_not_found"});
      return true;
    }
    const {artifact, file, stat} = loaded;
    const etag = artifact.sha256 ? `"${artifact.sha256}"` : `"${crypto.createHash("sha256").update(`${stat.size}:${stat.mtimeMs}`).digest("hex")}"`;
    if (requestHeader(req, "if-none-match") === etag) {
      addPublicHeaders(res);
      res.writeHead(304, {ETag: etag, "Cache-Control": ARTIFACT_CACHE_CONTROL});
      res.end();
      return true;
    }
    const range = parseRange(requestHeader(req, "range"), stat.size);
    if (range?.error) {
      addPublicHeaders(res);
      res.writeHead(416, {
        "Content-Type": JSON_MEDIA_TYPE,
        "Content-Range": `bytes */${stat.size}`,
        "Cache-Control": ARTIFACT_CACHE_CONTROL,
      });
      res.end(JSON.stringify({error: range.error, code: "invalid_range"}));
      return true;
    }
    const start = range?.start ?? 0;
    const end = range?.end ?? stat.size - 1;
    const length = end - start + 1;
    const headers = {
      "Content-Type": artifact.media_type || "application/octet-stream",
      "Content-Length": length,
      "Cache-Control": ARTIFACT_CACHE_CONTROL,
      ETag: etag,
      "Last-Modified": stat.mtime.toUTCString(),
      "Accept-Ranges": "bytes",
    };
    addPublicHeaders(res);
    if (range) {
      headers["Content-Range"] = `bytes ${start}-${end}/${stat.size}`;
      res.writeHead(206, headers);
    } else {
      res.writeHead(200, headers);
    }
    if (req.method === "HEAD") {
      res.end();
      return true;
    }
    fs.createReadStream(file, {start, end}).on("error", (error) => {
      if (!res.headersSent) res.writeHead(500);
      res.destroy(error);
    }).pipe(res);
    return true;
  }

  function resolveMcpResource(uri, req) {
    if (uri === "wiki-economics://catalog") return {mimeType: JSON_MEDIA_TYPE, value: loadCatalog(req)};
    if (uri === "wiki-economics://freshness") return {mimeType: JSON_MEDIA_TYPE, value: loadFreshness()};
    const prefix = "wiki-economics://artifact/";
    if (uri.startsWith(prefix)) {
      const name = normalizeArtifactName(uri.slice(prefix.length));
      const catalog = loadCatalog(req);
      const artifact = name ? catalog.artifacts.find((candidate) => candidate.name === name) : null;
      if (!artifact) throw new Error("Published artifact not found");
      return {mimeType: JSON_MEDIA_TYPE, value: {artifact}};
    }
    throw new Error("Unknown MCP resource URI");
  }

  function mcpLists(catalog) {
    const resources = [
      mcpResource("wiki-economics://catalog", "Published catalog", "Published datasets, wikis, schemas, hashes, and links."),
      mcpResource("wiki-economics://freshness", "Publication freshness", "Current freshness assessment and alerts."),
    ];
    return {
      tools: TOOL_DEFINITIONS,
      resources,
      resourceTemplates: [{
        uriTemplate: "wiki-economics://artifact/{artifact}",
        name: "Published artifact metadata",
        description: "Metadata and download URL for one manifest-allowlisted artifact.",
        mimeType: JSON_MEDIA_TYPE,
      }],
      catalog,
    };
  }

  function mcpToolCall(req, name, args = {}) {
    const catalog = loadCatalog(req);
    switch (name) {
      case "list_published_wikis":
        return catalog.wikis;
      case "list_datasets":
        return catalog.datasets;
      case "get_freshness":
        return loadFreshness();
      case "get_dataset": {
        const dataset = args.dataset;
        const wiki = args.wiki;
        if (!validDataset(dataset) || (wiki !== undefined && !validWiki(wiki))) {
          throw new Error("get_dataset requires a valid dataset and optional wiki identifier");
        }
        const artifacts = datasetSelection(catalog, dataset, wiki);
        if (artifacts.length === 0) throw new Error("Published dataset not found");
        return {
          dataset,
          wiki: wiki || null,
          artifacts,
          download_urls: artifacts.map((artifact) => artifact.url),
        };
      }
      default:
        throw new Error(`Unknown MCP tool: ${name}`);
    }
  }

  async function dispatchMcp(req, message) {
    const id = message && Object.prototype.hasOwnProperty.call(message, "id") ? message.id : undefined;
    if (!message || message.jsonrpc !== "2.0" || typeof message.method !== "string") {
      return jsonRpcError(id, -32600, "Invalid JSON-RPC request");
    }
    if (message.method === "notifications/initialized" || message.method === "notifications/cancelled") return null;
    try {
      switch (message.method) {
        case "initialize": {
          const requested = message.params?.protocolVersion;
          if (requested && !MCP_SUPPORTED_PROTOCOLS.has(requested)) {
            return jsonRpcError(id, -32602, `Unsupported MCP protocol version: ${requested}`, {
              supported: [...MCP_SUPPORTED_PROTOCOLS],
            });
          }
          return jsonRpcResult(id, {
            protocolVersion: requested || MCP_LATEST_PROTOCOL,
            capabilities: {
              tools: {listChanged: false},
              resources: {subscribe: false, listChanged: false},
            },
            serverInfo: {name: "wiki-economics", version: MCP_SERVER_VERSION},
            instructions: `Read-only access to published wiki-economics datasets. Use get_dataset for immutable download links. Public calls are limited to ${rateLimiter.limit} requests per second per client.`,
          });
        }
        case "ping":
          return jsonRpcResult(id, {});
        case "tools/list": {
          return jsonRpcResult(id, {
            tools: TOOL_DEFINITIONS,
            ttlMs: 60_000,
            cacheScope: "public",
          });
        }
        case "resources/list": {
          const catalog = loadCatalog(req);
          return jsonRpcResult(id, {
            resources: mcpLists(catalog).resources,
            ttlMs: 60_000,
            cacheScope: "public",
          });
        }
        case "resources/templates/list":
          return jsonRpcResult(id, {
            resourceTemplates: mcpLists(null).resourceTemplates,
            ttlMs: 60_000,
            cacheScope: "public",
          });
        case "resources/read": {
          const resource = resolveMcpResource(String(message.params?.uri || ""), req);
          return jsonRpcResult(id, {
            contents: [{uri: String(message.params.uri), mimeType: resource.mimeType, text: JSON.stringify(resource.value)}],
            ttlMs: 30_000,
            cacheScope: "public",
          });
        }
        case "tools/call": {
          const name = message.params?.name;
          const value = mcpToolCall(req, name, message.params?.arguments || {});
          const content = [mcpText(value)];
          if (name === "get_dataset") {
            for (const artifact of value.artifacts) {
              content.push({type: "resource_link", uri: artifact.url, name: artifact.name, mimeType: artifact.media_type, description: "Published immutable artifact"});
            }
          }
          return jsonRpcResult(id, {content, structuredContent: value, isError: false});
        }
        default:
          return jsonRpcError(id, -32601, `Method not found: ${message.method}`);
      }
    } catch (error) {
      if (message.method === "tools/call") {
        return jsonRpcResult(id, {content: [{type: "text", text: error.message}], isError: true});
      }
      return jsonRpcError(id, -32602, error.message);
    }
  }

  async function handleMcp(req, res) {
    if (req.method === "GET" || req.method === "HEAD") {
      addPublicHeaders(res);
      res.writeHead(405, {Allow: "POST", "Cache-Control": "no-store", "Content-Type": JSON_MEDIA_TYPE});
      res.end(req.method === "HEAD" ? undefined : JSON.stringify({error: "MCP Streamable HTTP endpoint accepts POST JSON-RPC requests"}));
      return true;
    }
    if (req.method !== "POST") {
      addPublicHeaders(res);
      res.writeHead(405, {Allow: "POST", "Cache-Control": "no-store"});
      res.end();
      return true;
    }
    const requestedProtocol = requestHeader(req, "mcp-protocol-version");
    if (requestedProtocol && !MCP_SUPPORTED_PROTOCOLS.has(requestedProtocol)) {
      writeJson(res, 400, jsonRpcError(null, -32602, `Unsupported MCP protocol version: ${requestedProtocol}`, {supported: [...MCP_SUPPORTED_PROTOCOLS]}), "no-store", {"MCP-Protocol-Version": MCP_LATEST_PROTOCOL});
      return true;
    }
    let body;
    try {
      body = JSON.parse(await readBody(req));
    } catch (error) {
      const statusCode = error.code === "request_too_large" ? 413 : 400;
      const rpcCode = statusCode === 413 ? -32600 : -32700;
      writeJson(res, statusCode, jsonRpcError(null, rpcCode, error.message === "request body is too large" ? error.message : "Parse error"), "no-store");
      return true;
    }
    const messages = Array.isArray(body) ? body : [body];
    if (messages.length === 0) {
      writeJson(res, 400, jsonRpcError(null, -32600, "Empty JSON-RPC batch"));
      return true;
    }
    if (messages.length > MAX_MCP_BATCH_MESSAGES) {
      writeJson(res, 413, jsonRpcError(null, -32600, `MCP batch exceeds ${MAX_MCP_BATCH_MESSAGES} messages`), "no-store");
      return true;
    }
    const headerMethod = requestHeader(req, "mcp-method");
    const headerName = requestHeader(req, "mcp-name");
    const responses = [];
    for (const message of messages) {
      if (headerMethod && message?.method !== headerMethod) {
        responses.push(jsonRpcError(message?.id, -32600, "Mcp-Method header does not match JSON-RPC method"));
        continue;
      }
      if (headerName && message?.method === "tools/call" && message?.params?.name !== headerName) {
        responses.push(jsonRpcError(message?.id, -32600, "Mcp-Name header does not match requested tool"));
        continue;
      }
      const response = await dispatchMcp(req, message);
      if (response) responses.push(response);
    }
    addPublicHeaders(res);
    res.setHeader("MCP-Protocol-Version", requestedProtocol || MCP_LATEST_PROTOCOL);
    if (responses.length === 0) {
      res.writeHead(202, {"Cache-Control": "no-store"});
      res.end();
    } else {
      res.writeHead(200, {"Content-Type": JSON_MEDIA_TYPE, "Cache-Control": "no-store"});
      res.end(JSON.stringify(Array.isArray(body) ? responses : responses[0]));
    }
    return true;
  }

  async function handleRequest(req, res, url) {
    const isMcp = url.pathname === MCP_PATH;
    const isApi = url.pathname === API_PREFIX || url.pathname.startsWith(`${API_PREFIX}/`);
    const isFreshness = url.pathname === FRESHNESS_PATH;
    if (!isMcp && !isApi && !isFreshness) return false;
    const rateDecision = rateLimiter.check(req);
    for (const [name, value] of Object.entries(rateLimitHeaders(rateDecision))) res.setHeader(name, value);
    if (!rateDecision.allowed) {
      const retryAfter = rateLimitHeaders(rateDecision)["RateLimit-Reset"];
      writeJson(res, 429, {
        error: "Rate limit exceeded",
        code: "rate_limited",
        limit_per_second: rateDecision.limit,
        retry_after_seconds: Number(retryAfter),
      }, "no-store", {"Retry-After": retryAfter});
      return true;
    }
    if (isFreshness) {
      if (req.method !== "GET" && req.method !== "HEAD" && req.method !== "OPTIONS") {
        addPublicHeaders(res);
        res.writeHead(405, {Allow: "GET, HEAD, OPTIONS", "Cache-Control": "no-store"});
        res.end();
        return true;
      }
      if (req.method === "OPTIONS") {
        addPublicHeaders(res);
        res.writeHead(204, {"Cache-Control": "no-store"});
        res.end();
        return true;
      }
      try {
        writeJson(res, 200, loadFreshness(), "no-store");
      } catch (error) {
        writeJson(res, 503, {error: error.message, code: error.code || "freshness_unavailable"}, "no-store", {"Retry-After": "30"});
      }
      return true;
    }
    if (req.method === "OPTIONS") {
      addPublicHeaders(res);
      res.writeHead(204, {"Cache-Control": "no-store"});
      res.end();
      return true;
    }
    if (isMcp) return handleMcp(req, res);
    if (req.method !== "GET" && req.method !== "HEAD") {
      addPublicHeaders(res);
      res.writeHead(405, {Allow: "GET, HEAD, OPTIONS", "Cache-Control": "no-store"});
      res.end();
      return true;
    }
    let catalog;
    try {
      catalog = loadCatalog(req);
    } catch (error) {
      writeJson(res, 503, {error: error.message, code: error.code || "catalog_unavailable"}, "no-store", {"Retry-After": "30"});
      return true;
    }
    const suffix = url.pathname.slice(API_PREFIX.length).replace(/^\/+/, "");
    if (!suffix) {
      writeApiRoot(res);
      return true;
    }
    if (suffix === "catalog") {
      writeJson(res, 200, catalog);
      return true;
    }
    if (suffix === "openapi.json") {
      writeJson(res, 200, openApiDocument(req, configuredOrigin));
      return true;
    }
    if (suffix === "wikis") {
      writeJson(res, 200, {api_schema_version: API_SCHEMA_VERSION, generated_at: catalog.generated_at, wikis: catalog.wikis});
      return true;
    }
    if (suffix === "datasets") {
      writeJson(res, 200, {api_schema_version: API_SCHEMA_VERSION, generated_at: catalog.generated_at, datasets: catalog.datasets});
      return true;
    }
    if (suffix.startsWith("datasets/")) {
      let dataset;
      try {
        dataset = decodeURIComponent(suffix.slice("datasets/".length));
      } catch {
        writeJson(res, 400, {error: "Invalid dataset identifier", code: "invalid_dataset"});
        return true;
      }
      if (!validDataset(dataset)) {
        writeJson(res, 400, {error: "Invalid dataset identifier", code: "invalid_dataset"});
        return true;
      }
      const wiki = url.searchParams.get("wiki") || undefined;
      const artifacts = datasetSelection(catalog, dataset, wiki);
      if (artifacts.length === 0) {
        writeJson(res, 404, {error: "Published dataset not found", code: "dataset_not_found"});
        return true;
      }
      writeJson(res, 200, {api_schema_version: API_SCHEMA_VERSION, dataset, wiki: wiki || null, artifacts});
      return true;
    }
    if (suffix.startsWith("artifacts/")) {
      return handleArtifact(req, res, catalog, suffix.slice("artifacts/".length));
    }
    writeJson(res, 404, {error: "Unknown machine API endpoint", code: "endpoint_not_found"});
    return true;
  }

  return {handleRequest, loadCatalog, loadArtifact, dispatchMcp};
}

module.exports = {
  API_PREFIX,
  FRESHNESS_PATH,
  MCP_PATH,
  MCP_LATEST_PROTOCOL,
  MAX_MCP_BATCH_MESSAGES,
  TOOL_DEFINITIONS,
  buildPublicCatalog,
  createRateLimiter,
  createMachineApi,
  normalizeArtifactName,
};
