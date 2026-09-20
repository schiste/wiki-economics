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
const MCP_SERVER_VERSION = "1.1.0";
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
const CSV_MEDIA_TYPE = "text/csv; charset=utf-8";
const PARQUET_MEDIA_TYPE = "application/vnd.apache.parquet";

// The metrics contract is intentionally bounded.  A caller can still request
// the immutable Parquet artifact for reproducible research, while JSON/CSV
// responses remain safe for chat clients and ordinary HTTP consumers.
const METRICS_PREFIX = `${API_PREFIX}/metrics`;
const DEFAULT_METRIC_LIMIT = 500;
const MAX_METRIC_LIMIT = 1_000;
const MAX_METRIC_WIKIS = 20;
const MAX_METRIC_SOURCE_BYTES = 512 * 1024 * 1024;
const MAX_METRIC_SOURCE_ROWS = 2_000_000;

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
  {
    name: "get_metric",
    title: "Get aggregated metric data",
    description: "Return bounded, server-aggregated metric rows with summary, semantics, coverage, and truncation metadata. Default granularity is month and the response is capped at 500 rows.",
    inputSchema: {
      type: "object",
      properties: {
        dataset: {type: "string", pattern: "^[a-z0-9_]+$"},
        wiki: {type: "string", pattern: "^[a-z0-9_]+wiki$"},
        from: {type: "string", description: "Inclusive YYYY, YYYY-MM, or ISO date boundary."},
        to: {type: "string", description: "Inclusive YYYY, YYYY-MM, or ISO date boundary."},
        granularity: {type: "string", enum: ["year", "month"]},
        group_by: {type: "array", items: {type: "string"}, maxItems: 8},
        agg: {type: "array", items: {type: "string"}, maxItems: 32},
        limit: {type: "integer", minimum: 1, maximum: MAX_METRIC_LIMIT},
        cursor: {type: "string"},
      },
      required: ["dataset"],
      additionalProperties: false,
    },
  },
  {
    name: "get_wiki_briefing",
    title: "Get wiki briefing",
    description: "Return a compact situation card for a published wiki: freshness, headline metrics, trends, ratios, movers, quality flags, and drill-down links.",
    inputSchema: {
      type: "object",
      properties: {wiki: {type: "string", pattern: "^[a-z0-9_]+wiki$"}},
      required: ["wiki"],
      additionalProperties: false,
    },
  },
  {
    name: "read_wiki_briefing",
    title: "Read wiki briefing (alias)",
    description: "Compatibility alias for get_wiki_briefing; return the compact situation card for one published wiki.",
    inputSchema: {
      type: "object",
      properties: {wiki: {type: "string", pattern: "^[a-z0-9_]+wiki$"}},
      required: ["wiki"],
      additionalProperties: false,
    },
  },
  {
    name: "get_schema",
    title: "Get metric schema",
    description: "Return fields, types, units, coverage, aggregation semantics, and algorithm version for a metric.",
    inputSchema: {
      type: "object",
      properties: {dataset: {type: "string", pattern: "^[a-z0-9_]+$"}},
      required: ["dataset"],
      additionalProperties: false,
    },
  },
  {
    name: "explain_metric",
    title: "Explain a metric",
    description: "Return the definition, methodology, caveats, units, and aggregation rules for a metric.",
    inputSchema: {
      type: "object",
      properties: {dataset: {type: "string", pattern: "^[a-z0-9_]+$"}},
      required: ["dataset"],
      additionalProperties: false,
    },
  },
  {
    name: "compare_wikis",
    title: "Compare wikis",
    description: "Compare the latest bounded values of a metric across two to twenty published wikis, including coverage and semantics.",
    inputSchema: {
      type: "object",
      properties: {
        dataset: {type: "string", pattern: "^[a-z0-9_]+$"},
        wikis: {type: "array", minItems: 2, maxItems: MAX_METRIC_WIKIS, items: {type: "string", pattern: "^[a-z0-9_]+wiki$"}},
        granularity: {type: "string", enum: ["year", "month"]},
      },
      required: ["dataset", "wikis"],
      additionalProperties: false,
    },
  },
];

const METRIC_SEMANTICS = {
  business_funnel: {
    definition: "Editor cohort retention and milestone reach by cohort year.",
    methodology: "Cohorts are formed from the first observed editing year; milestone columns count editors reaching at least the named edit threshold.",
    units: {cohort_size: "editors", reached_5: "editors", reached_25: "editors", reached_100: "editors"},
    caveats: ["Cohort retention is limited by the available observation window."],
  },
  gdp: {
    definition: "Monthly editing output and participation, split by namespace and user type.",
    methodology: "Revision events are assigned to an exact UTC calendar month; additive edit and byte measures are summed and rates are derived from their published numerator and denominator.",
    units: {gross_bytes_added: "bytes", net_bytes: "bytes", total_edits: "edits", productive_edits: "edits", reverted_edits: "edits", unique_editors: "editors", minor_edits: "edits", bytes_per_edit: "bytes/edit", bytes_per_editor: "bytes/editor", revert_rate: "ratio"},
    caveats: ["Net bytes can be negative.", "Unique editors are only additive at the published grain."],
  },
  gdp_activity_tiers: {
    definition: "Monthly editor activity tiers and output by user type.",
    methodology: "Editors are assigned to mutually exclusive activity tiers for each governed observation period.",
    units: {editors: "editors", total_edits: "edits", net_bytes: "bytes", gross_bytes: "bytes"},
    caveats: ["Tier counts should not be summed across overlapping periods."],
  },
  gdp_user_type_share: {
    definition: "Monthly editing output and editor counts by user type.",
    methodology: "Revision and editor events are grouped by exact calendar month and user type.",
    units: {edits: "edits", net_bytes: "bytes", editors: "editors"},
    caveats: [],
  },
  inequality: {
    definition: "Distribution of editing activity across editors, including Gini, Theil, Palma, and concentration measures.",
    methodology: "Inequality measures are computed from editor-level activity distributions; non-composable measures remain at their published grain.",
    units: {gini: "ratio", theil: "ratio", palma: "ratio", min_editors_50pct: "editors", total_editors: "editors", total_edits: "edits"},
    caveats: ["Gini, Palma, and the minimum-editor concentration measure are not composable across arbitrary groups."],
  },
  labor_churn: {
    definition: "Editor arrivals, departures, active population, and rates by observation period.",
    methodology: "Arrivals and departures are computed from adjacent governed observation windows; rates use active editors as the denominator.",
    units: {active_editors: "editors", arrivals: "editors", departures: "editors", arrival_rate: "ratio", departure_rate: "ratio"},
    caveats: ["The first and last observation periods can have edge effects."],
  },
  labor_cohorts: {
    definition: "Survival of editor cohorts over subsequent calendar years.",
    methodology: "Each cohort is tracked from its first observed editing year through the available snapshots.",
    units: {survived_editors: "editors", initial_editors: "editors"},
    caveats: ["Later cohorts have shorter follow-up windows."],
  },
  labor_monthly: {
    definition: "Monthly editor participation and editing output by namespace and user type.",
    methodology: "Revision events are grouped by exact month, namespace, and user type; editor counts are distinct at that grain.",
    units: {unique_editors: "editors", total_edits: "edits", net_bytes: "bytes", reverted_edits: "edits"},
    caveats: ["Unique editor counts are non-additive across namespaces and user types."],
  },
  page_weekly_edits: {
    definition: "Weekly edit activity for individual pages.",
    methodology: "Edits are assigned to ISO weeks and retained at page identity and namespace grain; week-over-week fields are published non-composable measures.",
    units: {edits: "edits", previous_week_edits: "edits", wow_change: "edits", wow_rate: "ratio"},
    caveats: ["This dataset can be very large; JSON responses are bounded and Parquet is recommended for bulk analysis."],
  },
  patrol: {
    definition: "Monthly patrol volume, latency, coverage, and concentration.",
    methodology: "Patrolled revisions and total revisions are counted from the governed patrol event and revision snapshots; coverage is derived from the published numerators and denominator.",
    units: {total_patrols: "patrols", unique_patrollers: "patrollers", patrol_new_pages: "pages", patrol_diffs: "diffs", median_latency_hours: "hours", p90_latency_hours: "hours", patrolled_revisions: "revisions", autopatrolled_revisions: "revisions", total_revisions: "revisions", patrol_coverage_pct: "percent", adjusted_coverage_pct: "percent", top1_pct: "percent", min_patrollers_50pct: "patrollers"},
    caveats: ["Coverage is only comparable when the revision and patrol snapshots cover the same period."],
  },
};

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

function metricSemantics(metric) {
  const known = METRIC_SEMANTICS[metric?.id] || {};
  const units = {...(known.units || {})};
  for (const field of metric?.schema || []) {
    if (field?.name && !units[field.name]) units[field.name] = field.unit || null;
  }
  return {
    definition: known.definition || `${metric?.id || "Metric"} published by wiki-economics.`,
    methodology: known.methodology || "See the published metric catalog and algorithm version for the computation contract.",
    units,
    caveats: Array.isArray(known.caveats) ? known.caveats : [],
    date_coverage: known.date_coverage || null,
  };
}

function schemaFields(metric) {
  return (Array.isArray(metric?.schema) ? metric.schema : [])
    .filter((field) => field && typeof field.name === "string")
    .map((field) => ({...field}));
}

function metricDateColumn(metric) {
  const explicit = metric?.receipt?.date_column;
  if (typeof explicit === "string" && explicit) return explicit;
  const names = schemaFields(metric).map((field) => field.name);
  return ["year_month", "period_start", "week_start", "period", "year", "cohort_year", "date"]
    .find((name) => names.includes(name)) || names[0] || "period";
}

function metricAggregationForField(metric, fieldName) {
  for (const rule of metric?.aggregation || []) {
    if (Array.isArray(rule.columns) && rule.columns.includes(fieldName)) return rule;
  }
  return null;
}

function fieldIsNumeric(field) {
  return /^(?:u?int|i\d+|u\d+|f\d+|float|double|decimal|number)/i.test(String(field?.data_type || ""))
    || ["integer", "number"].includes(String(field?.type || "").toLowerCase());
}

function safeJsonValue(value) {
  if (typeof value === "bigint") return Number.isSafeInteger(Number(value)) ? Number(value) : String(value);
  if (value instanceof Date) return value.toISOString();
  if (Array.isArray(value)) return value.map(safeJsonValue);
  if (value && typeof value === "object") {
    if (typeof value.toJSON === "function") {
      try { return safeJsonValue(value.toJSON()); } catch {}
    }
    return Object.fromEntries(Object.entries(value).map(([key, child]) => [key, safeJsonValue(child)]));
  }
  return value;
}

function normalizePeriod(value, granularity = "month") {
  if (value === undefined || value === null || value === "") return null;
  const text = String(value).trim();
  if (!text) return null;
  if (granularity === "year") {
    const year = /^(\d{4})/.exec(text);
    return year ? year[1] : text;
  }
  const month = /^(\d{4})[-/](\d{1,2})/.exec(text);
  if (month) return `${month[1]}-${month[2].padStart(2, "0")}`;
  const week = /^(\d{4})-W(\d{1,2})/i.exec(text);
  if (week) return `${week[1]}-W${week[2].padStart(2, "0")}`;
  return text.slice(0, 10);
}

function validDateBoundary(value) {
  if (value === undefined || value === null || value === "") return null;
  const text = String(value).trim();
  if (/^\d{4}$/.test(text)) return text;
  const month = /^(\d{4})-(\d{1,2})$/.exec(text);
  if (month) return Number(month[2]) >= 1 && Number(month[2]) <= 12 ? text : null;
  const date = /^(\d{4})-(\d{1,2})-(\d{1,2})$/.exec(text);
  if (date) {
    const candidate = new Date(Date.UTC(Number(date[1]), Number(date[2]) - 1, Number(date[3])));
    return candidate.getUTCFullYear() === Number(date[1])
      && candidate.getUTCMonth() === Number(date[2]) - 1
      && candidate.getUTCDate() === Number(date[3]) ? text : null;
  }
  const week = /^(\d{4})-W(\d{1,2})$/i.exec(text);
  return week && Number(week[2]) >= 1 && Number(week[2]) <= 53 ? text : null;
}

function boundaryPeriod(value, granularity, side = "from") {
  const valid = validDateBoundary(value);
  if (!valid) return null;
  if (granularity === "month" && /^\d{4}$/.test(valid)) return `${valid}-${side === "to" ? "12" : "01"}`;
  return normalizePeriod(valid, granularity);
}

function parseMetricList(value) {
  if (Array.isArray(value)) return value.map((item) => String(item).trim()).filter(Boolean);
  if (value === undefined || value === null || value === "") return [];
  return String(value).split(",").map((item) => item.trim()).filter(Boolean);
}

function parseCursor(value) {
  if (value === undefined || value === null || value === "") return 0;
  const text = String(value);
  if (/^\d+$/.test(text)) return Math.min(Number(text), Number.MAX_SAFE_INTEGER);
  try {
    const decoded = Buffer.from(text, "base64url").toString("utf8");
    return /^\d+$/.test(decoded) ? Number(decoded) : null;
  } catch {
    return null;
  }
}

function encodeCursor(offset) {
  return Buffer.from(String(offset), "utf8").toString("base64url");
}

function parseAggregateExpressions(raw, metric) {
  const fields = schemaFields(metric);
  const byName = new Map(fields.map((field) => [field.name, field]));
  const requested = parseMetricList(raw);
  const expressions = new Map();
  const implicitDimensions = new Set(["page_namespace", "page_id", "iso_year", "iso_week", "tier_rank", "period_months"]);
  const add = (fieldName, op) => {
    if (!byName.has(fieldName)) throw new Error(`Unknown aggregation field: ${fieldName}`);
    const normalized = String(op || "latest").toLowerCase();
    if (!["sum", "avg", "min", "max", "latest", "count", "distinct", "ratio"].includes(normalized)) {
      throw new Error(`Unsupported aggregation operation: ${op}`);
    }
    expressions.set(fieldName, normalized);
  };
  if (requested.length > 0) {
    for (const expression of requested) {
      const match = /^([^:=]+)[:=](sum|avg|min|max|latest|count|distinct|ratio)$/i.exec(expression);
      if (!match) throw new Error(`Aggregation must use field=sum|avg|min|max|latest|count|distinct: ${expression}`);
      add(match[1].trim(), match[2]);
    }
  }
  for (const field of fields) {
    if (expressions.has(field.name) || !fieldIsNumeric(field) || implicitDimensions.has(field.name)) continue;
    const rule = metricAggregationForField(metric, field.name);
    if (rule?.kind === "additive") add(field.name, "sum");
    else if (rule?.kind === "ratio") add(field.name, "ratio");
    else if (rule?.kind === "distinct_at_grain") add(field.name, "latest");
    else if (rule?.kind === "non_composable") add(field.name, "latest");
    else add(field.name, "latest");
  }
  return expressions;
}

function metricGroupFields(metric, requested) {
  const names = new Set(schemaFields(metric).map((field) => field.name));
  const dateColumn = metricDateColumn(metric);
  const result = [];
  for (const field of parseMetricList(requested)) {
    if (!names.has(field)) throw new Error(`Unknown group_by field: ${field}`);
    if (field !== dateColumn && !result.includes(field)) result.push(field);
  }
  return result;
}

function metricCoverage(rows, dateColumn, granularity) {
  const periods = rows.map((row) => normalizePeriod(row.period ?? row[dateColumn], granularity)).filter(Boolean).sort();
  return {minimum_date: periods[0] || null, maximum_date: periods.at(-1) || null};
}

function aggregateRows(sourceRows, metric, query) {
  const rows = Array.isArray(sourceRows) ? sourceRows : [];
  const dateColumn = metricDateColumn(metric);
  const granularity = query.granularity || "month";
  const from = boundaryPeriod(query.from, granularity, "from");
  const to = boundaryPeriod(query.to, granularity, "to");
  const groupFields = metricGroupFields(metric, query.groupBy);
  const expressions = parseAggregateExpressions(query.agg, metric);
  const fields = schemaFields(metric);
  const fieldNames = new Set(fields.map((field) => field.name));
  const buckets = new Map();
  for (const raw of rows) {
    const source = safeJsonValue(raw) || {};
    if (query.wiki && source.wiki && source.wiki !== query.wiki) continue;
    const sourcePeriod = source[dateColumn] ?? source.period ?? source.year_month ?? source.week_start ?? source.year;
    const period = normalizePeriod(sourcePeriod, granularity);
    if (!period || (from && period < from) || (to && period > to)) continue;
    const keyValues = [period, ...groupFields.map((field) => source[field] ?? null)];
    const key = JSON.stringify(keyValues);
    let bucket = buckets.get(key);
    if (!bucket) {
      bucket = {period};
      for (const field of groupFields) bucket[field] = source[field] ?? null;
      for (const field of fields) {
        if (!fieldNames.has(field.name) || field.name === dateColumn || field.name === "wiki" || groupFields.includes(field.name)) continue;
        bucket[field.name] = [];
      }
      buckets.set(key, bucket);
    }
    for (const [fieldName] of expressions) {
      if (!bucket[fieldName]) bucket[fieldName] = [];
      bucket[fieldName].push(source[fieldName]);
    }
  }
  const result = [];
  for (const bucket of buckets.values()) {
    const output = {period: bucket.period};
    for (const field of groupFields) output[field] = bucket[field];
    for (const [fieldName, op] of expressions) {
      const values = (bucket[fieldName] || []).filter((value) => value !== null && value !== undefined && value !== "");
      const numbers = values.map(Number).filter(Number.isFinite);
      let value = null;
      if (op === "sum") value = numbers.reduce((sum, item) => sum + item, 0);
      else if (op === "avg") value = numbers.length ? numbers.reduce((sum, item) => sum + item, 0) / numbers.length : null;
      else if (op === "min") value = numbers.length ? Math.min(...numbers) : null;
      else if (op === "max") value = numbers.length ? Math.max(...numbers) : null;
      else if (op === "count") value = values.length;
      else if (op === "distinct") value = new Set(values.map(String)).size;
      else if (op === "ratio") {
        const rule = metricAggregationForField(metric, fieldName);
        const numerator = (rule?.numerators || []).flatMap((name) => bucket[name] || []).map(Number).filter(Number.isFinite).reduce((sum, item) => sum + item, 0);
        const denominator = Number.isFinite(Number(rule?.denominator))
          ? Number(rule.denominator)
          : (bucket[rule?.denominator] || []).map(Number).filter(Number.isFinite).reduce((sum, item) => sum + item, 0);
        value = denominator ? numerator / denominator : null;
      }
      else value = values.length ? values.at(-1) : null;
      output[fieldName] = safeJsonValue(value);
    }
    result.push(output);
  }
  result.sort((left, right) => String(left.period).localeCompare(String(right.period)) || JSON.stringify(left).localeCompare(JSON.stringify(right)));
  return {rows: result, dateColumn, groupFields, expressions};
}

function summarizeRows(rows, expressions, granularity) {
  const numericFields = [...expressions.keys()];
  const latest = rows.at(-1) || null;
  const periods = [...new Set(rows.map((row) => row.period))];
  const yearOffset = granularity === "year" ? 1 : 12;
  const previous = latest && periods.length > yearOffset
    ? rows.filter((row) => row.period === periods[periods.length - 1 - yearOffset]).at(-1)
    : null;
  const aggregateObject = (kind) => Object.fromEntries(numericFields.map((field) => {
    const values = rows.map((row) => Number(row[field])).filter(Number.isFinite);
    if (!values.length) return [field, null];
    const operation = expressions.get(field);
    if (kind === "total") {
      if (operation === "sum" || operation === "count" || operation === "distinct") return [field, values.reduce((sum, value) => sum + value, 0)];
      if (operation === "avg") return [field, values.reduce((sum, value) => sum + value, 0) / values.length];
      return [field, null];
    }
    return [field, kind === "min" ? Math.min(...values) : Math.max(...values)];
  }));
  const yoyChange = Object.fromEntries(numericFields.map((field) => {
    const current = Number(latest?.[field]);
    const prior = Number(previous?.[field]);
    if (!Number.isFinite(current) || !Number.isFinite(prior)) return [field, null];
    return [field, {absolute: current - prior, percent: prior === 0 ? null : (current - prior) / Math.abs(prior)}];
  }));
  const topN = Object.fromEntries(numericFields.slice(0, 3).map((field) => [field, rows
    .filter((row) => Number.isFinite(Number(row[field])))
    .sort((a, b) => Number(b[field]) - Number(a[field]))
    .slice(0, 5)]));
  return {
    latest,
    total: aggregateObject("total"),
    min: aggregateObject("min"),
    max: aggregateObject("max"),
    yoy_change: yoyChange,
    top_n: topN,
  };
}

function csvEscape(value) {
  const text = value === null || value === undefined ? "" : typeof value === "object" ? JSON.stringify(value) : String(value);
  return /[",\n\r]/.test(text) ? `"${text.replaceAll('"', '""')}"` : text;
}

function rowsToCsv(rows, metadata) {
  const fields = [...new Set(rows.flatMap((row) => Object.keys(row)))];
  const lines = [
    `# dataset=${metadata.dataset}`,
    `# definition=${String(metadata.definition || "").replaceAll("\n", " ")}`,
    `# units=${JSON.stringify(metadata.units || {})}`,
    `# algorithm_version=${metadata.algorithm_version || ""}`,
    `# snapshot=${JSON.stringify(metadata.snapshot ?? null)}`,
    `# generated_at=${metadata.generated_at || ""}`,
    `# caveats=${JSON.stringify(metadata.caveats || [])}`,
    `# license=${JSON.stringify(metadata.license ?? null)}`,
    `# attribution=${JSON.stringify(metadata.attribution ?? null)}`,
    `# rows_returned=${metadata.rows_returned}`,
    `# rows_total=${metadata.rows_total}`,
    `# truncated=${metadata.truncated}`,
    fields.map(csvEscape).join(","),
  ];
  for (const row of rows) lines.push(fields.map((field) => csvEscape(row[field])).join(","));
  return `${lines.join("\n")}\n`;
}

function metricResponseEtag(metadata, rows) {
  const stable = {
    dataset: metadata.dataset,
    wiki: metadata.wiki,
    query: metadata.query,
    snapshot: metadata.snapshot,
    artifact_sha256: metadata.provenance?.artifact_sha256 || null,
    rows,
  };
  return `"${crypto.createHash("sha256").update(JSON.stringify(stable)).digest("hex")}"`;
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
  res.setHeader("Access-Control-Expose-Headers", "Content-Length, Content-Range, ETag, Last-Modified, MCP-Protocol-Version, RateLimit-Limit, RateLimit-Remaining, RateLimit-Reset, RateLimit-Policy, Retry-After, X-Wiki-Econ-Dataset, X-Wiki-Econ-Algorithm-Version, X-Wiki-Econ-Definition, X-Wiki-Econ-License, X-Wiki-Econ-Attribution, X-Wiki-Econ-Metadata, X-Wiki-Econ-Snapshot, X-Wiki-Econ-Generated-At, X-Wiki-Econ-Caveats, X-Wiki-Econ-Rows-Returned, X-Wiki-Econ-Rows-Total");
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
  const semantics = metricSemantics(metric);
  return {
    id: metric.id,
    family: metric.family,
    algorithm_version: metric.algorithm_version,
    definition: semantics.definition,
    units: semantics.units,
    caveats: semantics.caveats,
    schema: metric.schema,
    aggregation: metric.aggregation,
    publication: metric.publication,
    browser: metric.browser,
    artifacts,
  };
}

function compactPublicCatalog(catalog) {
  const datasets = catalog.datasets.map((dataset) => ({
    id: dataset.id,
    family: dataset.family,
    algorithm_version: dataset.algorithm_version,
    definition: dataset.definition,
    units: dataset.units,
    caveats: dataset.caveats,
    artifact_count: dataset.artifacts.length,
    wikis: [...new Set(dataset.artifacts.map((artifact) => artifact.wiki).filter(Boolean))].sort(),
  }));
  const wikis = catalog.wikis.map((wiki) => ({
    wiki: wiki.wiki,
    snapshot: wiki.snapshot,
    status: wiki.status,
    metrics: wiki.metrics,
    artifact_count: wiki.artifacts.length,
  }));
  return {
    api_schema_version: catalog.api_schema_version,
    metric_contract_version: "1.0",
    generated_at: catalog.generated_at,
    license: catalog.license,
    attribution: catalog.attribution,
    independence_notice: catalog.independence_notice,
    source_datasets: catalog.source_datasets,
    privacy: catalog.privacy,
    counts: {wikis: wikis.length, datasets: datasets.length, artifacts: catalog.artifacts.length},
    wikis,
    datasets,
    links: {...catalog.links, full_catalog: `${API_PREFIX}/catalog`},
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
      metrics: `${METRICS_PREFIX}/{dataset}`,
      wiki_briefing: `${API_PREFIX}/wikis/{wiki}/briefing`,
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
      [`${API_PREFIX}/catalog`]: {get: {summary: "Published catalog", parameters: [{name: "compact", in: "query", schema: {type: "boolean", default: false}, description: "Return a small discovery payload without per-artifact blobs."}], responses: {"200": {description: "Published datasets and artifacts"}, "429": rateLimited}}},
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
      [`${METRICS_PREFIX}/{dataset}`]: {
        get: {
          summary: "Query one metric",
          description: "Return bounded server-aggregated rows as JSON (default), CSV, or Parquet. JSON includes semantic metadata, summary, and explicit pagination/truncation fields.",
          parameters: [
            {name: "dataset", in: "path", required: true, schema: {type: "string"}},
            {name: "wiki", in: "query", schema: {type: "string"}},
            {name: "from", in: "query", schema: {type: "string"}},
            {name: "to", in: "query", schema: {type: "string"}},
            {name: "granularity", in: "query", schema: {type: "string", enum: ["year", "month"], default: "month"}},
            {name: "group_by", in: "query", schema: {type: "string", description: "Comma-separated schema fields."}},
            {name: "agg", in: "query", schema: {type: "string", description: "Comma-separated field=operation expressions."}},
            {name: "format", in: "query", schema: {type: "string", enum: ["json", "csv", "parquet"], default: "json"}},
            {name: "limit", in: "query", schema: {type: "integer", minimum: 1, maximum: MAX_METRIC_LIMIT, default: DEFAULT_METRIC_LIMIT}},
            {name: "cursor", in: "query", schema: {type: "string"}},
          ],
          responses: {"200": {description: "Metric response or rendered rows"}, "400": {description: "Invalid metric query"}, "404": {description: "Metric or wiki not published"}, "413": {description: "Source too large for a transformed response"}, "429": rateLimited},
        },
      },
      [`${METRICS_PREFIX}/{dataset}/schema`]: {get: {summary: "Metric schema", responses: {"200": {description: "Fields, types, units, and coverage"}, "404": {description: "Metric not found"}, "429": rateLimited}}},
      [`${METRICS_PREFIX}/{dataset}/explain`]: {get: {summary: "Metric methodology", responses: {"200": {description: "Definition and aggregation semantics"}, "404": {description: "Metric not found"}, "429": rateLimited}}},
      [`${API_PREFIX}/wikis/{wiki}/briefing`]: {get: {summary: "Wiki briefing", responses: {"200": {description: "Compact wiki situation card"}, "404": {description: "Wiki not published"}, "429": rateLimited}}},
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
  const injectedMetricRowsLoader = options.metricRowsLoader;
  const metricRowsCache = new Map();
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

  function resolveMetric(catalog, dataset) {
    if (!validDataset(dataset)) {
      const error = new Error("Invalid dataset identifier");
      error.code = "invalid_dataset";
      throw error;
    }
    const metric = catalog.datasets.find((entry) => entry.id === dataset);
    if (!metric) {
      const error = new Error("Published metric not found");
      error.code = "metric_not_found";
      throw error;
    }
    return metric;
  }

  function metricArtifact(catalog, metric, wiki) {
    const artifactName = wiki
      ? `${wiki}/${metric.id}.parquet`
      : metric.publication?.merged_artifact;
    if (!artifactName) return null;
    return loadArtifact(catalog, artifactName);
  }

  async function defaultMetricRowsLoader({catalog, metric, wiki}) {
    const loaded = metricArtifact(catalog, metric, wiki);
    if (!loaded) {
      const error = new Error("No published artifact is available for this metric and wiki");
      error.code = "metric_not_available";
      throw error;
    }
    if (loaded.stat.size > MAX_METRIC_SOURCE_BYTES) {
      const error = new Error(`Metric source is ${loaded.stat.size} bytes; transformed responses are capped at ${MAX_METRIC_SOURCE_BYTES} bytes. Use format=parquet for bulk download.`);
      error.code = "metric_source_too_large";
      throw error;
    }
    const cacheKey = `${loaded.artifact.name}:${loaded.stat.size}:${loaded.stat.mtimeMs}`;
    if (metricRowsCache.has(cacheKey)) return metricRowsCache.get(cacheKey);
    let parquet;
    let arrow;
    try {
      parquet = require("parquet-wasm/node");
      arrow = require("apache-arrow");
    } catch (error) {
      error.code = "metric_reader_unavailable";
      throw error;
    }
    const bytes = new Uint8Array(fs.readFileSync(loaded.file));
    const wasmTable = parquet.readParquet(bytes);
    const table = arrow.tableFromIPC(wasmTable.intoIPCStream());
    const rows = table.toArray().map((row) => safeJsonValue(row));
    if (rows.length > MAX_METRIC_SOURCE_ROWS) {
      const error = new Error(`Metric source contains ${rows.length} rows; transformed responses are capped at ${MAX_METRIC_SOURCE_ROWS} rows. Use format=parquet for bulk download.`);
      error.code = "metric_source_too_large";
      throw error;
    }
    if (metricRowsCache.size >= 8) metricRowsCache.delete(metricRowsCache.keys().next().value);
    metricRowsCache.set(cacheKey, rows);
    return rows;
  }

  async function loadMetricRows(args) {
    const loader = injectedMetricRowsLoader || defaultMetricRowsLoader;
    const loaded = await loader(args);
    const rows = Array.isArray(loaded) ? loaded : loaded?.rows;
    if (!Array.isArray(rows)) {
      const error = new Error("Metric row loader returned an invalid result");
      error.code = "invalid_metric_rows";
      throw error;
    }
    return rows.map(safeJsonValue);
  }

  function parseMetricQuery(url, catalog, metric, overrides = {}) {
    const get = (name) => overrides[name] !== undefined ? overrides[name] : url.searchParams.get(name);
    const wikiValue = get("wiki");
    const wiki = wikiValue === undefined || wikiValue === null || wikiValue === "" ? undefined : String(wikiValue);
    if (wiki !== undefined && !validWiki(wiki)) {
      const error = new Error("Invalid wiki identifier");
      error.code = "invalid_wiki";
      throw error;
    }
    if (wiki && !catalog.wikis.some((entry) => entry.wiki === wiki)) {
      const error = new Error("Wiki is not currently published");
      error.code = "wiki_not_published";
      throw error;
    }
    if (!wiki && metric.publication?.scope === "per_wiki_only") {
      const error = new Error("This metric requires a published wiki parameter");
      error.code = "wiki_required";
      throw error;
    }
    const granularity = String(get("granularity") || "month").toLowerCase();
    if (!["year", "month"].includes(granularity)) {
      const error = new Error("granularity must be year or month");
      error.code = "invalid_granularity";
      throw error;
    }
    const from = get("from") || undefined;
    const to = get("to") || undefined;
    if ((from && !validDateBoundary(from)) || (to && !validDateBoundary(to))) {
      const error = new Error("from and to must be YYYY, YYYY-MM, YYYY-MM-DD, or YYYY-Www");
      error.code = "invalid_date_boundary";
      throw error;
    }
    const format = String(get("format") || "json").toLowerCase();
    if (!["json", "csv", "parquet"].includes(format)) {
      const error = new Error("format must be json, csv, or parquet");
      error.code = "invalid_format";
      throw error;
    }
    const limitValue = get("limit");
    const parsedLimit = limitValue === undefined || limitValue === null || limitValue === ""
      ? DEFAULT_METRIC_LIMIT
      : positiveInteger(limitValue, 0);
    if (!parsedLimit || parsedLimit > MAX_METRIC_LIMIT) {
      const error = new Error(`limit must be an integer between 1 and ${MAX_METRIC_LIMIT}`);
      error.code = "invalid_limit";
      throw error;
    }
    const cursor = parseCursor(get("cursor"));
    if (cursor === null || !Number.isSafeInteger(cursor)) {
      const error = new Error("cursor must be a numeric offset or base64url cursor");
      error.code = "invalid_cursor";
      throw error;
    }
    return {
      dataset: metric.id,
      wiki,
      from,
      to,
      granularity,
      groupBy: get("group_by") ?? get("groupBy") ?? "",
      agg: get("agg") ?? "",
      format,
      limit: parsedLimit,
      cursor,
    };
  }

  function metricMetadata(catalog, metric, query, summary, coverage, rowsReturned, rowsTotal, truncated, nextCursor) {
    const semantics = metricSemantics(metric);
    const wikiState = query.wiki ? catalog.wikis.find((entry) => entry.wiki === query.wiki) : null;
    const artifact = metricArtifact(catalog, metric, query.wiki);
    return {
      api_schema_version: API_SCHEMA_VERSION,
      metric_contract_version: "1.0",
      dataset: metric.id,
      wiki: query.wiki || null,
      definition: semantics.definition,
      methodology: semantics.methodology,
      units: semantics.units,
      algorithm_version: metric.algorithm_version || null,
      snapshot: wikiState?.snapshot || (query.wiki ? null : catalog.provenance?.selected_snapshot_versions || null),
      generated_at: new Date().toISOString(),
      caveats: semantics.caveats,
      license: catalog.license,
      attribution: catalog.attribution,
      provenance: {
        source_datasets: catalog.source_datasets,
        artifact: artifact?.artifact?.name || null,
        artifact_sha256: artifact?.artifact?.sha256 || null,
      },
      coverage,
      query: {
        from: query.from || null,
        to: query.to || null,
        granularity: query.granularity,
        group_by: parseMetricList(query.groupBy),
        agg: parseMetricList(query.agg),
      },
      summary,
      rows_returned: rowsReturned,
      rows_total: rowsTotal,
      truncated,
      next_cursor: nextCursor,
    };
  }

  async function queryMetric(catalog, metric, query) {
    const sourceRows = await loadMetricRows({catalog, metric, wiki: query.wiki});
    const aggregated = aggregateRows(sourceRows, metric, query);
    const rowsTotal = aggregated.rows.length;
    const start = Math.min(query.cursor, rowsTotal);
    const rows = aggregated.rows.slice(start, start + query.limit);
    const truncated = start + rows.length < rowsTotal;
    const nextCursor = truncated ? encodeCursor(start + rows.length) : null;
    const coverage = metricCoverage(aggregated.rows, aggregated.dateColumn, query.granularity);
    const summary = summarizeRows(aggregated.rows, aggregated.expressions, query.granularity);
    const metadata = metricMetadata(catalog, metric, query, summary, coverage, rows.length, rowsTotal, truncated, nextCursor);
    return {metadata, rows, aggregated};
  }

  function publicMetricSchema(catalog, metric) {
    const semantics = metricSemantics(metric);
    const artifacts = metric.artifacts || [];
    const dates = artifacts.flatMap((artifact) => [artifact.minimum_date, artifact.maximum_date]).filter(Boolean).sort();
    return {
      api_schema_version: API_SCHEMA_VERSION,
      metric_contract_version: "1.0",
      dataset: metric.id,
      definition: semantics.definition,
      methodology: semantics.methodology,
      algorithm_version: metric.algorithm_version || null,
      date_column: metricDateColumn(metric),
      fields: schemaFields(metric).map((field) => ({...field, unit: semantics.units[field.name] || null})),
      aggregation: metric.aggregation || [],
      coverage: {minimum_date: dates[0] || null, maximum_date: dates.at(-1) || null},
      license: catalog.license,
      attribution: catalog.attribution,
      caveats: semantics.caveats,
    };
  }

  function publicMetricExplanation(catalog, metric) {
    const semantics = metricSemantics(metric);
    return {
      api_schema_version: API_SCHEMA_VERSION,
      metric_contract_version: "1.0",
      dataset: metric.id,
      definition: semantics.definition,
      methodology: semantics.methodology,
      units: semantics.units,
      algorithm_version: metric.algorithm_version || null,
      date_column: metricDateColumn(metric),
      aggregation: metric.aggregation || [],
      caveats: semantics.caveats,
      license: catalog.license,
      attribution: catalog.attribution,
    };
  }

  async function buildWikiBriefing(catalog, wiki) {
    if (!validWiki(wiki) || !catalog.wikis.some((entry) => entry.wiki === wiki)) {
      const error = new Error("Wiki is not currently published");
      error.code = "wiki_not_published";
      throw error;
    }
    const freshness = loadFreshness();
    const candidates = ["gdp", "labor_churn", "inequality", "patrol", "labor_monthly"]
      .map((id) => catalog.datasets.find((metric) => metric.id === id)).filter(Boolean);
    const headlineMetrics = [];
    const qualityFlags = [];
    const movers = [];
    const ratios = [];
    for (const metric of candidates) {
      try {
        const query = {dataset: metric.id, wiki, granularity: "month", groupBy: "", agg: "", from: undefined, to: undefined, limit: 24, cursor: 0};
        const result = await queryMetric(catalog, metric, query);
        const latest = result.metadata.summary.latest;
        headlineMetrics.push({dataset: metric.id, latest, trend: result.metadata.summary.yoy_change, coverage: result.metadata.coverage});
        for (const [field, change] of Object.entries(result.metadata.summary.yoy_change || {})) {
          if (change && Number.isFinite(change.percent)) movers.push({dataset: metric.id, field, ...change, latest: latest?.[field] ?? null});
        }
        for (const [field, value] of Object.entries(latest || {})) {
          const unit = metricSemantics(metric).units[field];
          if (unit === "ratio" || unit === "percent" || /(?:rate|gini|theil|palma|coverage|wow)/i.test(field)) {
            ratios.push({dataset: metric.id, field, value, unit: unit || null});
          }
        }
      } catch (error) {
        qualityFlags.push({severity: "warning", code: error.code || "metric_unavailable", dataset: metric.id, message: error.message});
      }
    }
    const status = freshness?.status || "unknown";
    if (!["fresh", "healthy", "ok"].includes(String(status).toLowerCase())) {
      qualityFlags.unshift({severity: "warning", code: "freshness", message: `Publication freshness status is ${status}`});
    }
    for (const alert of freshness?.alerts || []) qualityFlags.push({severity: "warning", code: "freshness_alert", message: typeof alert === "string" ? alert : alert.message || JSON.stringify(alert)});
    movers.sort((a, b) => Math.abs(Number(b.percent) || 0) - Math.abs(Number(a.percent) || 0));
    const state = catalog.wikis.find((entry) => entry.wiki === wiki);
    const lastSuccessfulAt = freshness?.summary?.lastSuccessfulAt || freshness?.lastSuccessfulAt || freshness?.last_successful_at || null;
    return {
      api_schema_version: API_SCHEMA_VERSION,
      metric_contract_version: "1.0",
      wiki,
      snapshot: state?.snapshot || null,
      generated_at: new Date().toISOString(),
      freshness: {
        status,
        lastSuccessfulAt,
        runId: freshness?.summary?.lastSuccessfulRunId || freshness?.lastSuccessfulRunId || freshness?.run_id || null,
        alerts: freshness?.alerts || [],
      },
      headline_metrics: headlineMetrics,
      key_ratios: ratios.slice(0, 12),
      biggest_movers: movers.slice(0, 5),
      data_quality_flags: qualityFlags,
      drill_down_links: candidates.map((metric) => `${METRICS_PREFIX}/${encodeURIComponent(metric.id)}?wiki=${encodeURIComponent(wiki)}&granularity=month`),
      license: catalog.license,
      attribution: catalog.attribution,
    };
  }

  async function compareWikis(catalog, metric, wikis, granularity = "month") {
    if (!["month", "year"].includes(granularity)) {
      const error = new Error("granularity must be year or month");
      error.code = "invalid_granularity";
      throw error;
    }
    const list = Array.isArray(wikis) ? [...new Set(wikis.map(String))] : parseMetricList(wikis);
    if (list.length < 2 || list.length > MAX_METRIC_WIKIS) throw new Error(`wikis must contain between 2 and ${MAX_METRIC_WIKIS} identifiers`);
    if (list.some((wiki) => !validWiki(wiki) || !catalog.wikis.some((entry) => entry.wiki === wiki))) throw new Error("All compared wikis must be currently published");
    const comparisons = [];
    for (const wiki of list) {
      const result = await queryMetric(catalog, metric, {dataset: metric.id, wiki, granularity, groupBy: "", agg: "", limit: 2, cursor: 0});
      comparisons.push({wiki, latest: result.metadata.summary.latest, previous: result.rows.at(-2) || null, trend: result.metadata.summary.yoy_change, coverage: result.metadata.coverage});
    }
    const semantics = metricSemantics(metric);
    return {
      api_schema_version: API_SCHEMA_VERSION,
      metric_contract_version: "1.0",
      dataset: metric.id,
      definition: semantics.definition,
      units: semantics.units,
      algorithm_version: metric.algorithm_version || null,
      granularity,
      comparisons,
      license: catalog.license,
      attribution: catalog.attribution,
      caveats: semantics.caveats,
    };
  }

  function rowsToParquet(rows) {
    const arrow = require("apache-arrow");
    const parquet = require("parquet-wasm/node");
    const table = arrow.tableFromJSON(rows.length ? rows : [{period: null}]);
    const wasmTable = parquet.Table.fromIPCStream(arrow.tableToIPC(table));
    const properties = new parquet.WriterPropertiesBuilder().build();
    return Buffer.from(parquet.writeParquet(wasmTable, properties));
  }

  function writeApiRoot(res) {
    writeJson(res, 200, {
      api_schema_version: API_SCHEMA_VERSION,
      metric_contract_version: "1.0",
      name: "wiki-economics data API",
      read_only: true,
      endpoints: {
        catalog: `${API_PREFIX}/catalog`,
        wikis: `${API_PREFIX}/wikis`,
        datasets: `${API_PREFIX}/datasets`,
        metrics: `${METRICS_PREFIX}/{dataset}?wiki=…&from=…&to=…&granularity=month&group_by=…&agg=…&format=json|csv|parquet`,
        wiki_briefing: `${API_PREFIX}/wikis/{wiki}/briefing`,
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

  function handleArtifact(req, res, catalog, encodedName, extraHeaders = {}) {
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
      res.writeHead(304, {ETag: etag, "Cache-Control": ARTIFACT_CACHE_CONTROL, ...extraHeaders});
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
        ...extraHeaders,
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
      ...extraHeaders,
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

  function metricErrorStatus(error) {
    if (["invalid_dataset", "invalid_wiki", "wiki_required", "invalid_granularity", "invalid_date_boundary", "invalid_format", "invalid_limit", "invalid_cursor"].includes(error.code)) return 400;
    if (["metric_not_found", "metric_not_available", "wiki_not_published"].includes(error.code)) return 404;
    if (error.code === "metric_source_too_large") return 413;
    return 503;
  }

  async function handleMetric(req, res, catalog, segments, url) {
    const dataset = decodeURIComponent(segments[1] || "");
    const metric = resolveMetric(catalog, dataset);
    const action = segments[2] || "";
    if (action === "schema") {
      if (segments.length !== 3) {
        writeJson(res, 404, {error: "Unknown metric endpoint", code: "endpoint_not_found"});
        return true;
      }
      writeJson(res, 200, publicMetricSchema(catalog, metric));
      return true;
    }
    if (action === "explain") {
      if (segments.length !== 3) {
        writeJson(res, 404, {error: "Unknown metric endpoint", code: "endpoint_not_found"});
        return true;
      }
      writeJson(res, 200, publicMetricExplanation(catalog, metric));
      return true;
    }
    if (segments.length !== 2) {
      writeJson(res, 404, {error: "Unknown metric endpoint", code: "endpoint_not_found"});
      return true;
    }
    const query = parseMetricQuery(url, catalog, metric);
    const isDirectParquet = query.format === "parquet"
      && !query.from && !query.to && !parseMetricList(query.groupBy).length && !parseMetricList(query.agg).length
      && query.cursor === 0 && !url.searchParams.has("limit");
    if (isDirectParquet) {
      const loaded = metricArtifact(catalog, metric, query.wiki);
      if (!loaded) {
        const error = new Error("No published artifact is available for this metric and wiki");
        error.code = "metric_not_available";
        throw error;
      }
      const semantics = metricSemantics(metric);
      return handleArtifact(req, res, catalog, loaded.artifact.name, {
        "X-Wiki-Econ-Dataset": metric.id,
        "X-Wiki-Econ-Algorithm-Version": metric.algorithm_version || "",
        "X-Wiki-Econ-Definition": semantics.definition,
        "X-Wiki-Econ-License": JSON.stringify(catalog.license || null),
        "X-Wiki-Econ-Attribution": String(catalog.attribution || ""),
        "X-Wiki-Econ-Snapshot": JSON.stringify(query.wiki ? catalog.wikis.find((entry) => entry.wiki === query.wiki)?.snapshot || null : catalog.provenance?.selected_snapshot_versions || null),
        "X-Wiki-Econ-Generated-At": new Date().toISOString(),
        "X-Wiki-Econ-Caveats": JSON.stringify(semantics.caveats),
        "X-Wiki-Econ-Metadata": `${METRICS_PREFIX}/${encodeURIComponent(metric.id)}/schema`,
      });
    }
    const result = await queryMetric(catalog, metric, query);
    const etag = metricResponseEtag(result.metadata, result.rows);
    if (requestHeader(req, "if-none-match") === etag) {
      addPublicHeaders(res);
      res.writeHead(304, {ETag: etag, "Cache-Control": PUBLIC_CACHE_CONTROL});
      res.end();
      return true;
    }
    if (query.format === "json") {
      writeJson(res, 200, {...result.metadata, rows: result.rows}, PUBLIC_CACHE_CONTROL, {ETag: etag});
      return true;
    }
    const metadata = result.metadata;
    if (query.format === "csv") {
      addPublicHeaders(res);
      res.writeHead(200, {
        "Content-Type": CSV_MEDIA_TYPE,
        "Cache-Control": PUBLIC_CACHE_CONTROL,
        "Content-Disposition": `attachment; filename="${metric.id}.csv"`,
        ETag: etag,
        "X-Wiki-Econ-Rows-Returned": String(metadata.rows_returned),
        "X-Wiki-Econ-Rows-Total": String(metadata.rows_total),
      });
      res.end(rowsToCsv(result.rows, metadata));
      return true;
    }
    let body;
    try {
      body = rowsToParquet(result.rows);
    } catch (error) {
      error.code = error.code || "metric_render_failed";
      throw error;
    }
    addPublicHeaders(res);
    res.writeHead(200, {
      "Content-Type": PARQUET_MEDIA_TYPE,
      "Content-Length": body.length,
      "Cache-Control": PUBLIC_CACHE_CONTROL,
      "Content-Disposition": `attachment; filename="${metric.id}.parquet"`,
      ETag: etag,
      "X-Wiki-Econ-Rows-Returned": String(metadata.rows_returned),
      "X-Wiki-Econ-Rows-Total": String(metadata.rows_total),
      "X-Wiki-Econ-Metadata": `${METRICS_PREFIX}/${encodeURIComponent(metric.id)}/schema`,
      "X-Wiki-Econ-Snapshot": JSON.stringify(result.metadata.snapshot ?? null),
      "X-Wiki-Econ-Generated-At": result.metadata.generated_at,
      "X-Wiki-Econ-Caveats": JSON.stringify(result.metadata.caveats || []),
    });
    res.end(body);
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

  async function mcpToolCall(req, name, args = {}) {
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
      case "get_metric": {
        const metric = resolveMetric(catalog, args.dataset);
        const query = parseMetricQuery(new URL(`http://machine-api${METRICS_PREFIX}/${metric.id}`), catalog, metric, {
          wiki: args.wiki,
          from: args.from,
          to: args.to,
          granularity: args.granularity || "month",
          groupBy: Array.isArray(args.group_by) ? args.group_by.join(",") : (args.group_by || ""),
          agg: Array.isArray(args.agg) ? args.agg.join(",") : (args.agg || ""),
          limit: args.limit,
          cursor: args.cursor,
          format: "json",
        });
        const result = await queryMetric(catalog, metric, query);
        return {...result.metadata, rows: result.rows};
      }
      case "get_wiki_briefing":
      case "read_wiki_briefing":
        return buildWikiBriefing(catalog, args.wiki);
      case "get_schema": {
        const metric = resolveMetric(catalog, args.dataset);
        return publicMetricSchema(catalog, metric);
      }
      case "explain_metric": {
        const metric = resolveMetric(catalog, args.dataset);
        return publicMetricExplanation(catalog, metric);
      }
      case "compare_wikis": {
        const metric = resolveMetric(catalog, args.dataset);
        return compareWikis(catalog, metric, args.wikis, args.granularity || "month");
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
          const value = await mcpToolCall(req, name, message.params?.arguments || {});
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
      const compact = ["1", "true", "yes"].includes(String(url.searchParams.get("compact") || "").toLowerCase());
      writeJson(res, 200, compact ? compactPublicCatalog(catalog) : catalog);
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
    if (suffix.startsWith("wikis/") && suffix.endsWith("/briefing")) {
      const encodedWiki = suffix.slice("wikis/".length, -"/briefing".length);
      let wiki;
      try { wiki = decodeURIComponent(encodedWiki); } catch {
        writeJson(res, 400, {error: "Invalid wiki identifier", code: "invalid_wiki"});
        return true;
      }
      try {
        writeJson(res, 200, await buildWikiBriefing(catalog, wiki));
      } catch (error) {
        writeJson(res, metricErrorStatus(error), {error: error.message, code: error.code || "briefing_unavailable"}, "no-store", error.code === "metric_source_too_large" ? {"Retry-After": "60"} : {});
      }
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
    if (suffix.startsWith("metrics/")) {
      const pieces = suffix.split("/");
      try {
        return await handleMetric(req, res, catalog, pieces, url);
      } catch (error) {
        logMessage(logger, "error", `[machine-api] metric request failed dataset=${pieces[1] || "unknown"} code=${error.code || "internal"}: ${error.message}`);
        writeJson(res, metricErrorStatus(error), {error: error.message, code: error.code || "metric_unavailable"}, "no-store", error.code === "metric_source_too_large" ? {"Retry-After": "60"} : {});
        return true;
      }
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
