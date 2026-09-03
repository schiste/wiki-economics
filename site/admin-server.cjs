#!/usr/bin/env node
// Admin server for the wiki-economics operator surface.
// - Local/dev mode: loopback-only API for scripts/dev.sh and Observable preview.
// - VPS mode: authenticated /admin page plus authenticated /admin-api/* routes.

const http = require("http");
const {spawn} = require("child_process");
const path = require("path");
const fs = require("fs");
const crypto = require("crypto");
const {
  buildAuthorizeUrl,
  escapeHtml,
  normalizeUsername,
  parseAllowedUsernames,
  parseCookies,
  randomToken,
  sanitizeNextPath,
  serializeCookie,
  signJsonToken,
  verifyJsonToken,
} = require("./admin-auth.cjs");
const {
  lifecyclePath,
  resolveRefreshWikis,
  validateWikiLifecycle,
  wikisWithState,
} = require("../scripts/wiki-lifecycle.cjs");
const {evaluateFreshness} = require("./freshness.cjs");
const {stripAnsi, summarizeOperationLog} = require("./admin-operation-status.cjs");

const ROOT = path.resolve(__dirname, "..");
const RUNTIME_ENV = process.env.WIKI_ECON_ENV || "local";
const ADMIN_ENABLED = (process.env.WIKI_ECON_ADMIN_ENABLED ?? (RUNTIME_ENV === "production" ? "0" : "1")) === "1";
const PORT = Number.parseInt(process.env.WIKI_ECON_ADMIN_PORT || "3001", 10);
const BIND_HOST = process.env.WIKI_ECON_ADMIN_BIND_HOST || "127.0.0.1";
const SITE_PORT = Number.parseInt(process.env.WIKI_ECON_SITE_PORT || "3000", 10);
const DATA_DIR = resolveConfiguredPath("WIKI_ECON_DATA_DIR", "data");
const OUTPUT_DIR = resolveConfiguredPath("WIKI_ECON_OUTPUT_DIR", "output");
const GENERATOR_DIR = resolveConfiguredPath("WIKI_ECON_GENERATOR_DIR", path.join("site", "data-build"));
const SITE_DIST_DIR = resolveConfiguredPath("WIKI_ECON_SITE_DIST_DIR", path.join("site", "dist"));
const CONFIGURED_BIN = process.env.WIKI_ECON_BIN || "";
const FLEET_QUEUE_DIR = process.env.WIKI_ECON_FLEET_QUEUE_DIR
  ? resolveConfiguredPath("WIKI_ECON_FLEET_QUEUE_DIR", path.join("output", "_fleet"))
  : path.join(OUTPUT_DIR, "_fleet");
const ADMIN_STATE_DIR = path.join(OUTPUT_DIR, "_admin");
const ADMIN_CURRENT_JOB_PATH = path.join(ADMIN_STATE_DIR, "current-job.json");
const ADMIN_JOB_HISTORY_PATH = path.join(ADMIN_STATE_DIR, "job-history.json");
const ADMIN_LIFECYCLE_HISTORY_PATH = path.join(ADMIN_STATE_DIR, "lifecycle-history.json");
const ADMIN_OPERATION_DIR = process.env.WIKI_ECON_ADMIN_OPERATION_DIR
  ? resolveConfiguredPath("WIKI_ECON_ADMIN_OPERATION_DIR", path.join("output", "_admin", "operations"))
  : path.join(ADMIN_STATE_DIR, "operations");
const ADMIN_EXECUTION_MODE = process.env.WIKI_ECON_ADMIN_EXECUTION_MODE
  || (RUNTIME_ENV === "production" ? "queue" : "direct");
const ADMIN_OPERATION_STALE_MS = Number.parseInt(process.env.WIKI_ECON_ADMIN_OPERATION_STALE_SECS || "600", 10) * 1_000;
const ADMIN_DISPATCH_MINUTES = parseDispatchMinutes(
  process.env.WIKI_ECON_ADMIN_DISPATCH_MINUTES || "3,13,23,33,43,53",
);
// These ledgers live on Toolforge NFS, so webservice pod replacement cannot
// turn an interrupted operator action into an apparently idle pipeline.
const DEFAULT_RUNNER = {
  program: "cargo",
  args: ["run", "--release", "--"],
  label: "cargo run --release --",
};
const LEGACY_API_PREFIX = "/api";
const PROXY_API_PREFIX = "/admin-api";
const ADMIN_PAGE_PATH = "/admin";
const ADMIN_LOGIN_PATH = "/admin/login";
const ADMIN_LOGOUT_PATH = "/admin/logout";
const ADMIN_OAUTH_START_PATH = "/admin/oauth/start";
const ADMIN_OAUTH_CALLBACK_PATH = "/admin/oauth/callback";
const FRESHNESS_STATUS_PATH = "/health/freshness.json";
const ADMIN_AUTH_MODE = process.env.WIKI_ECON_ADMIN_AUTH_MODE || "none";
const AUTH_ENABLED = ADMIN_AUTH_MODE !== "none";
const ADMIN_ALLOWED_USERNAMES = parseAllowedUsernames(process.env.WIKI_ECON_ADMIN_ALLOWED_USERNAMES || "");
const ADMIN_SESSION_SECRET = process.env.WIKI_ECON_ADMIN_SESSION_SECRET || "";
const ADMIN_SESSION_COOKIE_NAME = process.env.WIKI_ECON_ADMIN_SESSION_COOKIE_NAME || "wiki_econ_admin_session";
const ADMIN_OAUTH_STATE_COOKIE_NAME = process.env.WIKI_ECON_ADMIN_OAUTH_STATE_COOKIE_NAME || "wiki_econ_admin_oauth_state";
const ADMIN_SESSION_TTL_SECS = parsePositiveInt(process.env.WIKI_ECON_ADMIN_SESSION_TTL_SECS, 8 * 60 * 60);
const ADMIN_SECURE_COOKIES = (process.env.WIKI_ECON_ADMIN_SECURE_COOKIES ?? (RUNTIME_ENV === "production" ? "1" : "0")) === "1";
const ADMIN_PUBLIC_ORIGIN = normalizeConfiguredOrigin(process.env.WIKI_ECON_ADMIN_PUBLIC_ORIGIN || "");
const ADMIN_MEDIAWIKI_HOST = (process.env.WIKI_ECON_ADMIN_MEDIAWIKI_HOST || "https://meta.wikimedia.org").replace(/\/+$/, "");
const ADMIN_MEDIAWIKI_CLIENT_ID = process.env.WIKI_ECON_ADMIN_MEDIAWIKI_CLIENT_ID || "";
const ADMIN_MEDIAWIKI_CLIENT_SECRET = process.env.WIKI_ECON_ADMIN_MEDIAWIKI_CLIENT_SECRET || "";
const ALLOWED_ORIGINS = resolveAllowedOrigins();
// Display-only: the admin server can't query Toolforge Jobs state directly
// (no Kubernetes/Toolforge API credentials in the pod), so the configured
// schedule is sourced from tool-wide config, not confirmed against Toolforge.
const REFRESH_SCHEDULE = process.env.WIKI_ECON_REFRESH_SCHEDULE || null;
const WIKI_LIFECYCLE_PATH = lifecyclePath(ROOT);
const BASE_WIKI_LIFECYCLE_PATH = path.join(ROOT, "config", "wiki-lifecycle.json");
let WIKI_LIFECYCLE = loadOrInitializeWikiLifecycle();
let REFRESH_WIKIS = resolveRefreshWikis(WIKI_LIFECYCLE);
let PUBLISHED_WIKIS = wikisWithState(WIKI_LIFECYCLE, "publication", "published");

let currentJob = null;
let jobLog = [];
let jobExitCode = null;
let lastJob = null;
let lastWikiJobs = new Map();
let lastGlobalJob = null;
// Capped history alongside the single "last job" trackers above, so the UI
// can show more than just the most recent run per wiki without needing any
// persistence beyond process lifetime.
const JOB_HISTORY_LIMIT = 5;
const ADMIN_JOB_HISTORY_LIMIT = 104;
const PERSISTED_LOG_LIMIT_BYTES = 128 * 1024;
let lastWikiJobHistory = new Map();
let lastGlobalJobHistory = [];
let jobPersistenceTimer = null;
let manifestCache = null;
let manifestCacheAt = 0;
const MANIFEST_CACHE_TTL_MS = 1500;
const REQUIRED_MERGED_METRICS = 9;
let supportedWikisCache = null;

if (!ADMIN_ENABLED) {
  console.error("Admin API is disabled for this runtime. Set WIKI_ECON_ADMIN_ENABLED=1 to opt in.");
  process.exit(1);
}

if (RUNTIME_ENV === "production" && !AUTH_ENABLED) {
  console.error("Refusing to run the admin server in production without authentication. Set WIKI_ECON_ADMIN_AUTH_MODE=mediawiki.");
  process.exit(1);
}

if (!new Set(["direct", "queue"]).has(ADMIN_EXECUTION_MODE)) {
  console.error(`Unsupported WIKI_ECON_ADMIN_EXECUTION_MODE: ${ADMIN_EXECUTION_MODE}. Expected "direct" or "queue".`);
  process.exit(1);
}

if (AUTH_ENABLED && ADMIN_AUTH_MODE !== "mediawiki") {
  console.error(`Unsupported WIKI_ECON_ADMIN_AUTH_MODE: ${ADMIN_AUTH_MODE}. Expected "none" or "mediawiki".`);
  process.exit(1);
}

if (AUTH_ENABLED) {
  const missing = [];
  if (!ADMIN_MEDIAWIKI_CLIENT_ID) missing.push("WIKI_ECON_ADMIN_MEDIAWIKI_CLIENT_ID");
  if (!ADMIN_MEDIAWIKI_CLIENT_SECRET) missing.push("WIKI_ECON_ADMIN_MEDIAWIKI_CLIENT_SECRET");
  if (!ADMIN_SESSION_SECRET || ADMIN_SESSION_SECRET.length < 32) missing.push("WIKI_ECON_ADMIN_SESSION_SECRET (32+ chars)");
  if (ADMIN_ALLOWED_USERNAMES.size === 0) missing.push("WIKI_ECON_ADMIN_ALLOWED_USERNAMES");
  if (missing.length > 0) {
    console.error(`Missing required admin auth configuration: ${missing.join(", ")}`);
    process.exit(1);
  }
}

function parsePositiveInt(value, fallback) {
  const parsed = Number.parseInt(value || "", 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

function parseDispatchMinutes(value) {
  const minutes = String(value || "")
    .split(",")
    .map((part) => Number.parseInt(part.trim(), 10))
    .filter((minute) => Number.isInteger(minute) && minute >= 0 && minute < 60);
  return Array.from(new Set(minutes)).sort((left, right) => left - right);
}

function nextDispatchAt(after = new Date(), slotsAhead = 0) {
  if (ADMIN_DISPATCH_MINUTES.length === 0) return null;
  const cursor = new Date(after);
  cursor.setUTCSeconds(0, 0);
  cursor.setUTCMinutes(cursor.getUTCMinutes() + 1);
  let remaining = slotsAhead;
  for (let checked = 0; checked < 24 * 60 * 8; checked += 1) {
    if (ADMIN_DISPATCH_MINUTES.includes(cursor.getUTCMinutes())) {
      if (remaining === 0) return cursor.toISOString();
      remaining -= 1;
    }
    cursor.setUTCMinutes(cursor.getUTCMinutes() + 1);
  }
  return null;
}

function requestHeaderValue(req, name) {
  const raw = req.headers[name];
  if (Array.isArray(raw)) return raw[0] || "";
  return raw || "";
}

function resolveConfiguredPath(envVar, fallback) {
  const value = process.env[envVar];
  if (!value) return path.resolve(ROOT, fallback);
  return path.isAbsolute(value) ? value : path.resolve(ROOT, value);
}

function loadOrInitializeWikiLifecycle() {
  if (!fs.existsSync(WIKI_LIFECYCLE_PATH)) {
    const base = JSON.parse(fs.readFileSync(BASE_WIKI_LIFECYCLE_PATH, "utf8"));
    validateWikiLifecycle(base, BASE_WIKI_LIFECYCLE_PATH);
    atomicWriteJson(WIKI_LIFECYCLE_PATH, base);
  }
  return readWikiLifecycleFile();
}

function readWikiLifecycleFile() {
  let registry;
  try {
    registry = JSON.parse(fs.readFileSync(WIKI_LIFECYCLE_PATH, "utf8"));
  } catch (error) {
    throw new Error(`Unable to read wiki lifecycle registry ${WIKI_LIFECYCLE_PATH}: ${error.message}`);
  }
  validateWikiLifecycle(registry, WIKI_LIFECYCLE_PATH);
  return registry;
}

function normalizeConfiguredOrigin(value) {
  const trimmed = String(value || "").trim();
  return trimmed ? trimmed.replace(/\/+$/, "") : "";
}

function resolveAllowedOrigins() {
  const configured = process.env.WIKI_ECON_ALLOWED_ORIGINS;
  const origins = new Set();
  if (configured) {
    for (const entry of configured.split(",")) {
      const origin = normalizeConfiguredOrigin(entry);
      if (origin) origins.add(origin);
    }
  }
  if (ADMIN_PUBLIC_ORIGIN) origins.add(ADMIN_PUBLIC_ORIGIN);
  if (origins.size === 0) {
    origins.add(`http://127.0.0.1:${SITE_PORT}`);
    origins.add(`http://localhost:${SITE_PORT}`);
  }
  return origins;
}

function currentRequestOrigin(req) {
  if (ADMIN_PUBLIC_ORIGIN) return ADMIN_PUBLIC_ORIGIN;
  const proto = requestHeaderValue(req, "x-forwarded-proto") || (RUNTIME_ENV === "production" ? "https" : "http");
  const host = requestHeaderValue(req, "x-forwarded-host") || requestHeaderValue(req, "host") || `127.0.0.1:${PORT}`;
  return normalizeConfiguredOrigin(`${proto}://${host}`);
}

function externalUrl(req, pathname) {
  return new URL(pathname, `${currentRequestOrigin(req)}/`).toString();
}

function isOriginAllowed(origin, req) {
  const normalized = normalizeConfiguredOrigin(origin);
  if (!normalized) return false;
  if (ALLOWED_ORIGINS.has("*")) return true;
  if (ALLOWED_ORIGINS.has(normalized)) return true;
  return normalized === currentRequestOrigin(req);
}

function applyCors(req, res) {
  const origin = requestHeaderValue(req, "origin");
  if (!origin) return;
  if (ALLOWED_ORIGINS.has("*")) {
    res.setHeader("Access-Control-Allow-Origin", "*");
    return;
  }
  if (isOriginAllowed(origin, req)) {
    res.setHeader("Access-Control-Allow-Origin", origin);
    res.setHeader("Vary", "Origin");
  }
}

function appendSetCookie(res, cookieValue) {
  const current = res.getHeader("Set-Cookie");
  if (!current) {
    res.setHeader("Set-Cookie", [cookieValue]);
    return;
  }
  if (Array.isArray(current)) {
    res.setHeader("Set-Cookie", [...current, cookieValue]);
    return;
  }
  res.setHeader("Set-Cookie", [current, cookieValue]);
}

function clearAuthCookies(res) {
  const expired = new Date(0);
  appendSetCookie(res, serializeCookie(ADMIN_SESSION_COOKIE_NAME, "", {
    maxAge: 0,
    expires: expired,
    httpOnly: true,
    secure: ADMIN_SECURE_COOKIES,
    sameSite: "Lax",
    path: "/",
  }));
  appendSetCookie(res, serializeCookie(ADMIN_OAUTH_STATE_COOKIE_NAME, "", {
    maxAge: 0,
    expires: expired,
    httpOnly: true,
    secure: ADMIN_SECURE_COOKIES,
    sameSite: "Lax",
    path: "/",
  }));
}

function writeJson(res, statusCode, body, extraHeaders = {}) {
  res.writeHead(statusCode, {
    "Content-Type": "application/json; charset=utf-8",
    "Cache-Control": "no-store",
    ...extraHeaders,
  });
  res.end(JSON.stringify(body));
}

function writeHtml(res, statusCode, html) {
  res.writeHead(statusCode, {
    "Content-Type": "text/html; charset=utf-8",
    "Cache-Control": "no-store",
  });
  res.end(html);
}

function redirect(res, location, extraHeaders = {}) {
  res.writeHead(302, {
    Location: location,
    "Cache-Control": "no-store",
    ...extraHeaders,
  });
  res.end();
}

// Static fallback for the built Observable site. Only reached when nothing
// else in handleRequest matched, i.e. Cloud VPS never hits this path because
// nginx serves site assets directly and only proxies /admin* here.
const STATIC_MIME_TYPES = {
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".mjs": "text/javascript; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".svg": "image/svg+xml",
  ".png": "image/png",
  ".jpg": "image/jpeg",
  ".jpeg": "image/jpeg",
  ".gif": "image/gif",
  ".webp": "image/webp",
  ".ico": "image/x-icon",
  ".woff": "font/woff",
  ".woff2": "font/woff2",
  ".txt": "text/plain; charset=utf-8",
  ".csv": "text/csv; charset=utf-8",
  ".parquet": "application/octet-stream",
  ".map": "application/json; charset=utf-8",
  ".wasm": "application/wasm",
};

function staticContentType(filePath) {
  return STATIC_MIME_TYPES[path.extname(filePath).toLowerCase()] || "application/octet-stream";
}

function staticCacheControl(pathname) {
  if (pathname.startsWith("/_observablehq/") || pathname.startsWith("/_npm/")) {
    return "public, max-age=3600";
  }
  if (pathname.startsWith("/_file/")) {
    return "public, max-age=600";
  }
  return null;
}

// Mirrors nginx's `try_files $uri $uri.html $uri/ =404` against SITE_DIST_DIR.
function resolveStaticFile(pathname) {
  let decoded;
  try {
    decoded = decodeURIComponent(pathname);
  } catch {
    return null;
  }
  const relative = decoded.replace(/^\/+/, "");
  const candidateRoot = path.resolve(SITE_DIST_DIR, relative);
  if (candidateRoot !== SITE_DIST_DIR && !candidateRoot.startsWith(SITE_DIST_DIR + path.sep)) {
    return null;
  }
  const candidates = [candidateRoot, `${candidateRoot}.html`, path.join(candidateRoot, "index.html")];
  for (const candidate of candidates) {
    try {
      if (fs.statSync(candidate).isFile()) return candidate;
    } catch {
      // try next candidate
    }
  }
  return null;
}

function serveStaticAsset(req, res, pathname) {
  const filePath = resolveStaticFile(pathname);
  if (!filePath) return false;
  const cacheControl = staticCacheControl(pathname);
  const headers = { "Content-Type": staticContentType(filePath) };
  if (cacheControl) headers["Cache-Control"] = cacheControl;
  res.writeHead(200, headers);
  if (req.method === "HEAD") {
    res.end();
    return true;
  }
  fs.createReadStream(filePath).pipe(res);
  return true;
}

function loginUrlFor(nextPath = ADMIN_PAGE_PATH) {
  return `${ADMIN_LOGIN_PATH}?next=${encodeURIComponent(sanitizeNextPath(nextPath, ADMIN_PAGE_PATH))}`;
}

function authStatus(session, req) {
  return {
    enabled: AUTH_ENABLED,
    mode: AUTH_ENABLED ? ADMIN_AUTH_MODE : "none",
    authenticated: AUTH_ENABLED ? Boolean(session) : true,
    loginUrl: AUTH_ENABLED && !session ? loginUrlFor(ADMIN_PAGE_PATH) : null,
    logoutUrl: AUTH_ENABLED && session ? ADMIN_LOGOUT_PATH : null,
    user: session ? {
      username: session.username,
      name: session.name || session.username,
    } : null,
    publicOrigin: currentRequestOrigin(req),
  };
}

function unauthorizedApiResponse(res, req) {
  writeJson(res, 401, {
    error: "Authentication required",
    auth: authStatus(null, req),
  });
}

function requireTrustedOrigin(req, res) {
  const origin = requestHeaderValue(req, "origin");
  if (origin && isOriginAllowed(origin, req)) return true;

  const referer = requestHeaderValue(req, "referer");
  if (referer) {
    try {
      const refererOrigin = new URL(referer).origin;
      if (isOriginAllowed(refererOrigin, req)) return true;
    } catch {
      // ignore parse failures
    }
  }

  writeJson(res, 403, { error: "Untrusted origin" });
  return false;
}

function readSession(req) {
  if (!AUTH_ENABLED) return null;
  const cookies = parseCookies(requestHeaderValue(req, "cookie"));
  const payload = verifyJsonToken(cookies[ADMIN_SESSION_COOKIE_NAME], ADMIN_SESSION_SECRET);
  if (!payload || typeof payload !== "object") return null;
  if (!payload.username || !payload.exp) return null;
  if ((Number(payload.exp) || 0) <= Math.floor(Date.now() / 1000)) return null;
  const normalized = normalizeUsername(payload.username);
  if (!ADMIN_ALLOWED_USERNAMES.has(normalized)) return null;
  return {
    username: normalized,
    name: typeof payload.name === "string" ? payload.name : normalized,
    sub: typeof payload.sub === "string" ? payload.sub : "",
    provider: typeof payload.provider === "string" ? payload.provider : "",
  };
}

function issueSession(res, profile) {
  const expiresAt = Math.floor(Date.now() / 1000) + ADMIN_SESSION_TTL_SECS;
  const token = signJsonToken({
    username: profile.username,
    name: profile.name || profile.username,
    sub: profile.sub,
    provider: ADMIN_MEDIAWIKI_HOST,
    exp: expiresAt,
  }, ADMIN_SESSION_SECRET);
  appendSetCookie(res, serializeCookie(ADMIN_SESSION_COOKIE_NAME, token, {
    maxAge: ADMIN_SESSION_TTL_SECS,
    httpOnly: true,
    secure: ADMIN_SECURE_COOKIES,
    sameSite: "Lax",
    path: "/",
  }));
}

function renderAuthPage({ title, heading, message, actionUrl, actionLabel, secondaryActionUrl, secondaryActionLabel }) {
  const secondary = secondaryActionUrl && secondaryActionLabel
    ? `<p><a href="${escapeHtml(secondaryActionUrl)}">${escapeHtml(secondaryActionLabel)}</a></p>`
    : "";
  return `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>${escapeHtml(title)}</title>
  <style>
    :root { color-scheme: light dark; }
    body {
      margin: 0;
      min-height: 100vh;
      display: grid;
      place-items: center;
      font-family: system-ui, -apple-system, "Segoe UI", Roboto, sans-serif;
      background:
        radial-gradient(circle at top, rgba(17, 94, 89, 0.16), transparent 45%),
        linear-gradient(180deg, #f5f7fb 0%, #edf1f7 100%);
      color: #17202a;
    }
    .card {
      width: min(32rem, calc(100vw - 2rem));
      box-sizing: border-box;
      background: rgba(255,255,255,0.92);
      border: 1px solid rgba(23,32,42,0.08);
      border-radius: 1.25rem;
      box-shadow: 0 18px 50px rgba(23,32,42,0.12);
      padding: 2rem;
    }
    h1 { margin: 0 0 0.75rem; font-size: 1.6rem; }
    p { line-height: 1.55; }
    a.button {
      display: inline-block;
      margin-top: 1rem;
      padding: 0.8rem 1rem;
      border-radius: 999px;
      background: #0b5d57;
      color: #fff;
      font-weight: 600;
      text-decoration: none;
    }
    code {
      padding: 0.15rem 0.35rem;
      border-radius: 0.4rem;
      background: rgba(23,32,42,0.08);
      font-family: ui-monospace, "SFMono-Regular", Menlo, Consolas, monospace;
    }
  </style>
</head>
<body>
  <main class="card">
    <h1>${escapeHtml(heading)}</h1>
    <p>${message}</p>
    ${actionUrl && actionLabel ? `<a class="button" href="${escapeHtml(actionUrl)}">${escapeHtml(actionLabel)}</a>` : ""}
    ${secondary}
  </main>
</body>
</html>`;
}

function renderMissingAdminPage() {
  return renderAuthPage({
    title: "Admin page unavailable",
    heading: "Admin page unavailable",
    message: "The built admin page was not found in the current site release. Run the site build before exposing the authenticated admin surface.",
  });
}

function serveAdminPage(res) {
  const adminHtmlPath = path.join(SITE_DIST_DIR, "admin.html");
  if (!fs.existsSync(adminHtmlPath)) {
    writeHtml(res, 503, renderMissingAdminPage());
    return;
  }
  res.writeHead(200, {
    "Content-Type": "text/html; charset=utf-8",
    "Cache-Control": "no-store",
  });
  fs.createReadStream(adminHtmlPath).pipe(res);
}

function renderLoginPage(req, message, nextPath) {
  const next = sanitizeNextPath(nextPath, ADMIN_PAGE_PATH);
  return renderAuthPage({
    title: "Sign in to wiki-economics admin",
    heading: "Sign in to wiki-economics admin",
    message,
    actionUrl: `${ADMIN_OAUTH_START_PATH}?next=${encodeURIComponent(next)}`,
    actionLabel: "Continue to sign in",
    secondaryActionUrl: "/",
    secondaryActionLabel: "Back to dashboard",
  });
}

async function fetchJson(url, options = {}) {
  const response = await fetch(url, {
    ...options,
    headers: {
      Accept: "application/json",
      ...(options.headers || {}),
    },
  });
  const text = await response.text();
  if (!response.ok) {
    throw new Error(`${response.status} ${response.statusText}: ${text.slice(0, 300)}`);
  }
  return text ? JSON.parse(text) : {};
}

function mediawikiEndpoint(pathSuffix) {
  return `${ADMIN_MEDIAWIKI_HOST}${pathSuffix}`;
}

function normalizeMediawikiProfile(profile) {
  const username = normalizeUsername(profile.username);
  if (!username) {
    throw new Error("The identity provider did not return a username.");
  }
  if (!ADMIN_ALLOWED_USERNAMES.has(username)) {
    throw new Error(`The signed-in user ${username} is not in the configured allowlist.`);
  }
  return {
    username,
    name: typeof profile.realname === "string" && profile.realname.trim() ? profile.realname.trim() : username,
    sub: profile.sub != null ? String(profile.sub) : username,
  };
}

async function startMediawikiLogin(req, res, nextPath) {
  const state = randomToken(24);
  const next = sanitizeNextPath(nextPath, ADMIN_PAGE_PATH);
  const stateToken = signJsonToken({
    state,
    next,
    exp: Math.floor(Date.now() / 1000) + 10 * 60,
  }, ADMIN_SESSION_SECRET);
  appendSetCookie(res, serializeCookie(ADMIN_OAUTH_STATE_COOKIE_NAME, stateToken, {
    maxAge: 10 * 60,
    httpOnly: true,
    secure: ADMIN_SECURE_COOKIES,
    sameSite: "Lax",
    path: "/",
  }));
  const authorizeUrl = buildAuthorizeUrl({
    authorizationEndpoint: mediawikiEndpoint("/w/rest.php/oauth2/authorize"),
    clientId: ADMIN_MEDIAWIKI_CLIENT_ID,
    redirectUri: externalUrl(req, ADMIN_OAUTH_CALLBACK_PATH),
    state,
  });
  redirect(res, authorizeUrl);
}

async function finishMediawikiLogin(req, res, url) {
  const error = url.searchParams.get("error");
  const errorDescription = url.searchParams.get("error_description");
  if (error) {
    const message = errorDescription ? `${error}: ${errorDescription}` : error;
    clearAuthCookies(res);
    redirect(res, `${ADMIN_LOGIN_PATH}?error=${encodeURIComponent(message)}`);
    return;
  }

  const code = url.searchParams.get("code");
  const state = url.searchParams.get("state");
  const cookies = parseCookies(requestHeaderValue(req, "cookie"));
  const savedState = verifyJsonToken(cookies[ADMIN_OAUTH_STATE_COOKIE_NAME], ADMIN_SESSION_SECRET);
  clearAuthCookies(res);

  if (!code || !state || !savedState || typeof savedState !== "object") {
    redirect(res, `${ADMIN_LOGIN_PATH}?error=${encodeURIComponent("Missing or expired OAuth state.")}`);
    return;
  }
  if ((Number(savedState.exp) || 0) <= Math.floor(Date.now() / 1000)) {
    redirect(res, `${ADMIN_LOGIN_PATH}?error=${encodeURIComponent("OAuth state expired. Please try again.")}`);
    return;
  }
  if (savedState.state !== state) {
    redirect(res, `${ADMIN_LOGIN_PATH}?error=${encodeURIComponent("OAuth state mismatch. Please try again.")}`);
    return;
  }

  try {
    const tokenResponse = await fetchJson(mediawikiEndpoint("/w/rest.php/oauth2/access_token"), {
      method: "POST",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded",
      },
      body: new URLSearchParams({
        grant_type: "authorization_code",
        code,
        client_id: ADMIN_MEDIAWIKI_CLIENT_ID,
        client_secret: ADMIN_MEDIAWIKI_CLIENT_SECRET,
        redirect_uri: externalUrl(req, ADMIN_OAUTH_CALLBACK_PATH),
      }),
    });
    if (!tokenResponse.access_token) {
      throw new Error("MediaWiki OAuth token response did not contain an access token.");
    }
    const profile = await fetchJson(mediawikiEndpoint("/w/rest.php/oauth2/resource/profile"), {
      headers: {
        Authorization: `Bearer ${tokenResponse.access_token}`,
      },
    });
    const normalized = normalizeMediawikiProfile(profile);
    issueSession(res, normalized);
    redirect(res, sanitizeNextPath(savedState.next, ADMIN_PAGE_PATH));
  } catch (authError) {
    redirect(res, `${ADMIN_LOGIN_PATH}?error=${encodeURIComponent(authError.message)}`);
  }
}

function resolveRunner() {
  const customBin = CONFIGURED_BIN;
  if (customBin) {
    return {
      program: customBin,
      args: [],
      label: customBin,
    };
  }
  return DEFAULT_RUNNER;
}

function runnerInfo() {
  const runner = resolveRunner();
  return {
    mode: CONFIGURED_BIN ? "bin" : "cargo",
    label: runner.label,
  };
}

function recordJobHistory(completedJob) {
  if (completedJob.wiki) {
    const history = [completedJob, ...(lastWikiJobHistory.get(completedJob.wiki) || [])].slice(0, JOB_HISTORY_LIMIT);
    lastWikiJobHistory.set(completedJob.wiki, history);
  } else {
    lastGlobalJobHistory = [completedJob, ...lastGlobalJobHistory].slice(0, JOB_HISTORY_LIMIT);
  }
  persistJobHistory();
}

function readJsonFile(filePath) {
  try {
    return JSON.parse(fs.readFileSync(filePath, "utf8"));
  } catch {
    return null;
  }
}

function atomicWriteJson(filePath, value) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  const temporary = path.join(
    path.dirname(filePath),
    `.${path.basename(filePath)}.${process.pid}.${Date.now()}.tmp`,
  );
  const fd = fs.openSync(temporary, "wx", 0o600);
  try {
    try {
      fs.writeFileSync(fd, `${JSON.stringify(value, null, 2)}\n`, "utf8");
      fs.fsyncSync(fd);
    } finally {
      fs.closeSync(fd);
    }
    fs.renameSync(temporary, filePath);
  } catch (error) {
    try {
      fs.unlinkSync(temporary);
    } catch (cleanupError) {
      if (cleanupError.code !== "ENOENT") {
        console.error(`[admin] failed to clean temporary ledger ${temporary}: ${cleanupError.message}`);
      }
    }
    throw error;
  }
}

function reloadWikiLifecycle() {
  WIKI_LIFECYCLE = readWikiLifecycleFile();
  REFRESH_WIKIS = resolveRefreshWikis(WIKI_LIFECYCLE);
  PUBLISHED_WIKIS = wikisWithState(WIKI_LIFECYCLE, "publication", "published");
  return WIKI_LIFECYCLE;
}

function defaultRetentionPolicy() {
  return {
    source_recoverability: "redownloadable",
    history_input: "purge_after_ready",
    patrol_source: "purge_after_ready",
    computed_rollback_generations: 1,
  };
}

function registrationLifecycle(mode, resourceClass, operator) {
  const provenance = `toolforge-admin:${operator || "local-operator"}`;
  const base = {
    provenance,
    retention: defaultRetentionPolicy(),
  };
  if (mode === "qualification") {
    return {
      ...base,
      publication: "hidden",
      refresh: "qualification",
      fleet_resource_class: resourceClass || "medium_large",
    };
  }
  if (mode === "manual") {
    return {
      ...base,
      publication: "published",
      refresh: "manual",
      fleet_resource_class: resourceClass || "medium_large",
    };
  }
  if (mode === "scheduled") {
    return {
      ...base,
      publication: "published",
      refresh: "scheduled",
      freshness_sla_days: 10,
      fleet_resource_class: resourceClass || "medium_large",
    };
  }
  throw new Error(`Unsupported lifecycle mode ${mode}`);
}

function updateExplicitDatasetCoverage(registry, wiki, published) {
  for (const contract of Object.values(registry.publication_contract?.datasets || {})) {
    if (!Array.isArray(contract.wikis)) continue;
    const covered = new Set(contract.wikis);
    if (published) covered.add(wiki);
    else covered.delete(wiki);
    contract.wikis = [...covered].sort();
  }
}

function recordLifecycleChange(change) {
  const current = readJsonFile(ADMIN_LIFECYCLE_HISTORY_PATH);
  const changes = current?.schemaVersion === 1 && Array.isArray(current.changes)
    ? current.changes
    : [];
  atomicWriteJson(ADMIN_LIFECYCLE_HISTORY_PATH, {
    schemaVersion: 1,
    generatedAt: new Date().toISOString(),
    changes: [change, ...changes].slice(0, ADMIN_JOB_HISTORY_LIMIT),
  });
}

function registerWikiLifecycle({ wiki, mode, resourceClass, operator }) {
  const supported = new Set(loadSupportedWikipedias());
  if (!supported.has(wiki)) throw new Error(`${wiki} is not a supported Wikimedia history project`);
  if (!new Set(["qualification", "manual", "scheduled"]).has(mode)) {
    throw new Error("Lifecycle mode must be qualification, manual, or scheduled");
  }
  if (!new Set(["small", "medium_large", "isolated"]).has(resourceClass)) {
    throw new Error("Resource class must be small, medium_large, or isolated");
  }

  const registry = structuredClone(reloadWikiLifecycle());
  const previous = registry.wikis[wiki] ? structuredClone(registry.wikis[wiki]) : null;
  registry.wikis[wiki] = registrationLifecycle(mode, resourceClass, operator);
  updateExplicitDatasetCoverage(registry, wiki, mode !== "qualification");
  validateWikiLifecycle(registry, WIKI_LIFECYCLE_PATH);
  atomicWriteJson(WIKI_LIFECYCLE_PATH, registry);
  reloadWikiLifecycle();
  const updatedAt = new Date().toISOString();
  recordLifecycleChange({
    wiki,
    mode,
    resourceClass,
    operator: operator || "local-operator",
    updatedAt,
    previous,
    current: registry.wikis[wiki],
  });
  return { wiki, lifecycle: registry.wikis[wiki], previous, updatedAt };
}

function operationDirectories() {
  const directories = {
    queued: path.join(ADMIN_OPERATION_DIR, "queued"),
    running: path.join(ADMIN_OPERATION_DIR, "running"),
    history: path.join(ADMIN_OPERATION_DIR, "history"),
    logs: path.join(ADMIN_OPERATION_DIR, "logs"),
  };
  for (const directory of Object.values(directories)) fs.mkdirSync(directory, { recursive: true });
  return directories;
}

function operationLogTail(logPath, maxBytes = 64 * 1024) {
  if (!logPath) return [];
  try {
    const fd = fs.openSync(logPath, "r");
    try {
      const size = fs.fstatSync(fd).size;
      const start = Math.max(0, size - maxBytes);
      const buffer = Buffer.alloc(size - start);
      fs.readSync(fd, buffer, 0, buffer.length, start);
      return [buffer.toString("utf8")];
    } finally {
      fs.closeSync(fd);
    }
  } catch {
    return [];
  }
}

function operationEntries(directory, limit = ADMIN_JOB_HISTORY_LIMIT) {
  return safeReadDir(directory)
    .filter((name) => name.endsWith(".json"))
    .map((name) => readJsonFile(path.join(directory, name)))
    .filter((entry) => entry?.schemaVersion === 1)
    .map((entry) => {
      const heartbeatAge = Date.now() - Date.parse(entry.heartbeatAt || entry.updatedAt || entry.startedAt || 0);
      const stateEntry = ["running", "cancelling"].includes(entry.state)
        && Number.isFinite(heartbeatAge) && heartbeatAge > ADMIN_OPERATION_STALE_MS
        ? {...entry, originalState: entry.state, state: "stalled", heartbeatAgeMs: heartbeatAge}
        : entry;
      const rawLog = operationLogTail(entry.logPath).join("");
      return {
        ...stateEntry,
        ...summarizeOperationLog(stateEntry, rawLog),
        log: rawLog ? [stripAnsi(rawLog)] : [],
      };
    })
    .sort((left, right) => Date.parse(right.updatedAt || right.requestedAt || 0)
      - Date.parse(left.updatedAt || left.requestedAt || 0))
    .slice(0, limit);
}

function recoverStaleAdminOperations() {
  const directories = operationDirectories();
  const recovered = [];
  const cancelled = [];
  const exhausted = [];
  for (const name of safeReadDir(directories.running).filter((entry) => entry.endsWith(".json")).sort()) {
    const runningPath = path.join(directories.running, name);
    const request = readJsonFile(runningPath);
    const heartbeatAge = Date.now() - Date.parse(request?.heartbeatAt || request?.updatedAt || request?.startedAt || 0);
    if (request?.schemaVersion !== 1 || !Number.isFinite(heartbeatAge) || heartbeatAge <= ADMIN_OPERATION_STALE_MS) continue;
    const now = new Date().toISOString();
    if (request.cancelRequested) {
      atomicWriteJson(path.join(directories.history, `${Date.now()}-${request.requestId}.json`), {
        ...request, state: "cancelled", exitCode: 130, finishedAt: now, updatedAt: now,
        recoveryReason: `heartbeat stale for ${Math.round(heartbeatAge / 1_000)} seconds`,
      });
      fs.unlinkSync(runningPath);
      cancelled.push(request.requestId);
      continue;
    }
    if (Number(request.retryCount || 0) >= 2) {
      atomicWriteJson(path.join(directories.history, `${Date.now()}-${request.requestId}.json`), {
        ...request, state: "failed", exitCode: 1, finishedAt: now, updatedAt: now,
        error: "Admin operation exceeded its stale-heartbeat recovery limit",
        recoveryReason: `heartbeat stale for ${Math.round(heartbeatAge / 1_000)} seconds`,
      });
      fs.unlinkSync(runningPath);
      exhausted.push(request.requestId);
      continue;
    }
    const queuedPath = path.join(directories.queued, name);
    if (fs.existsSync(queuedPath)) throw new Error(`Cannot recover ${request.requestId}: queued request already exists`);
    fs.renameSync(runningPath, queuedPath);
    atomicWriteJson(queuedPath, {
      ...request,
      state: "queued",
      retryCount: Number(request.retryCount || 0) + 1,
      recoveredAt: now,
      updatedAt: now,
      recoveryReason: `heartbeat stale for ${Math.round(heartbeatAge / 1_000)} seconds`,
    });
    recovered.push(request.requestId);
  }
  return {recovered, cancelled, exhausted};
}

function readAdminOperations() {
  const directories = operationDirectories();
  const queued = operationEntries(directories.queued)
    .sort((left, right) => Date.parse(left.requestedAt || 0) - Date.parse(right.requestedAt || 0));
  const running = operationEntries(directories.running);
  const recent = operationEntries(directories.history);
  const scheduled = queued.map((entry, index) => ({
    ...entry,
    queuePosition: index + 1,
    earliestDispatchAt: entry.notBefore
      || (running.length === 0 && index === 0 ? nextDispatchAt() : null),
    waitingForActiveOperation: running.length > 0,
    waitingForEarlierRequest: index > 0,
  }));
  return {
    executionMode: ADMIN_EXECUTION_MODE,
    counts: {queued: queued.length, running: running.length},
    dispatcher: {
      kind: ADMIN_EXECUTION_MODE === "queue" ? "scheduled-single-flight" : "direct",
      minuteSlotsUtc: ADMIN_DISPATCH_MINUTES,
      nextDispatchAt: nextDispatchAt(),
      running: running.length > 0,
    },
    queued: scheduled,
    running,
    recent,
  };
}

function queueAdminOperation({ action, wiki, version, requestedBy, acknowledgeBlockedRetry = false }) {
  const directories = operationDirectories();
  const active = [
    ...operationEntries(directories.running, Number.MAX_SAFE_INTEGER),
    ...operationEntries(directories.queued, Number.MAX_SAFE_INTEGER),
  ];
  const sameScope = (entry) => wiki ? entry.wiki === wiki : entry.wiki == null;
  const conflict = active.find(sameScope);
  if (conflict) {
    throw new Error(
      `${wiki || "A global operation"} already has ${conflict.action || "work"} ${conflict.state}; `
      + `follow request ${conflict.requestId} instead of creating duplicate work`,
    );
  }
  const latestPlan = wiki
    ? readSnapshotPlans()
      .filter((plan) => plan.wiki === wiki)
      .sort((left, right) => right.snapshot.localeCompare(left.snapshot))[0]
    : null;
  const requestedSnapshot = version || latestPlan?.snapshot || null;
  const blockedFailure = operationEntries(directories.history, Number.MAX_SAFE_INTEGER).find((entry) => {
    const failedSnapshot = entry.selectedSnapshot || entry.version || null;
    const sameSnapshot = requestedSnapshot && failedSnapshot
      ? requestedSnapshot === failedSnapshot
      : (entry.version || null) === (version || null);
    return entry.state === "failed"
      && entry.retryable === false
      && entry.action === action
      && sameScope(entry)
      && sameSnapshot;
  });
  if (blockedFailure && !acknowledgeBlockedRetry) {
    throw new Error(
      `${blockedFailure.errorSummary} ${blockedFailure.remediation} `
      + "The admin will not repeat unchanged work automatically.",
    );
  }
  const requestId = adminRunId(action, wiki);
  const requestedAt = new Date().toISOString();
  const request = {
    schemaVersion: 1,
    requestId,
    runId: requestId,
    action,
    wiki: wiki || null,
    version: version || null,
    lifecyclePath: WIKI_LIFECYCLE_PATH,
    requestedBy: requestedBy || "local-operator",
    requestedAt,
    updatedAt: requestedAt,
    state: "queued",
    cancelRequested: false,
    blockedRetryAcknowledged: Boolean(blockedFailure && acknowledgeBlockedRetry),
    supersedesFailedRequestId: blockedFailure?.requestId || null,
    logPath: path.join(directories.logs, `${requestId}.log`),
  };
  atomicWriteJson(path.join(directories.queued, `${requestId}.json`), request);
  return request;
}

function cancelAdminOperation({requestId, wiki}) {
  const directories = operationDirectories();
  const queued = operationEntries(directories.queued, Number.MAX_SAFE_INTEGER);
  const running = operationEntries(directories.running, Number.MAX_SAFE_INTEGER);
  const matches = (entry) => requestId ? entry.requestId === requestId : wiki && entry.wiki === wiki;
  const queuedRequest = queued.find(matches);
  if (queuedRequest) {
    const source = path.join(directories.queued, `${queuedRequest.requestId}.json`);
    const cancelled = {
      ...queuedRequest,
      state: "cancelled",
      cancelRequested: true,
      finishedAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
    };
    atomicWriteJson(path.join(directories.history, `${Date.now()}-${queuedRequest.requestId}.json`), cancelled);
    fs.unlinkSync(source);
    return cancelled;
  }
  const runningRequest = running.find(matches);
  if (runningRequest) {
    const file = path.join(directories.running, `${runningRequest.requestId}.json`);
    const cancelling = {...runningRequest, cancelRequested: true, state: "cancelling", updatedAt: new Date().toISOString()};
    atomicWriteJson(file, cancelling);
    return cancelling;
  }
  return null;
}

function persistedLog(log) {
  const text = (log || []).join("");
  return text.length <= PERSISTED_LOG_LIMIT_BYTES
    ? text
    : `[earlier output omitted from the persisted admin ledger]\n${text.slice(-PERSISTED_LOG_LIMIT_BYTES)}`;
}

function serializableRunningJob() {
  if (!currentJob) return null;
  return {
    schemaVersion: 1,
    runId: currentJob.runId,
    command: currentJob.command,
    action: currentJob.action,
    wiki: currentJob.wiki,
    stage: currentJob.stage,
    state: currentJob.cancelRequested ? "cancelling" : "running",
    running: true,
    pid: currentJob.pid,
    startedAt: currentJob.startedAt,
    updatedAt: new Date().toISOString(),
    log: [persistedLog(jobLog)],
    diskHeadroom: currentJob.diskHeadroom ?? null,
    rawCleanup: currentJob.rawCleanup ?? null,
  };
}

function persistRunningJobNow() {
  if (jobPersistenceTimer) {
    clearTimeout(jobPersistenceTimer);
    jobPersistenceTimer = null;
  }
  const running = serializableRunningJob();
  if (running) atomicWriteJson(ADMIN_CURRENT_JOB_PATH, running);
}

function scheduleRunningJobPersistence() {
  if (!currentJob || jobPersistenceTimer) return;
  jobPersistenceTimer = setTimeout(() => {
    jobPersistenceTimer = null;
    try {
      persistRunningJobNow();
    } catch (error) {
      console.error(`[admin] failed to persist running job: ${error.message}`);
    }
  }, 500);
}

function clearPersistedRunningJob() {
  if (jobPersistenceTimer) {
    clearTimeout(jobPersistenceTimer);
    jobPersistenceTimer = null;
  }
  try {
    fs.unlinkSync(ADMIN_CURRENT_JOB_PATH);
  } catch (error) {
    if (error.code !== "ENOENT") throw error;
  }
}

function persistedHistoryJobs() {
  return [
    ...Array.from(lastWikiJobHistory.values()).flat(),
    ...lastGlobalJobHistory,
  ]
    .sort((left, right) => Date.parse(right.finishedAt || 0) - Date.parse(left.finishedAt || 0))
    .slice(0, ADMIN_JOB_HISTORY_LIMIT);
}

function persistJobHistory() {
  try {
    atomicWriteJson(ADMIN_JOB_HISTORY_PATH, {
      schemaVersion: 1,
      generatedAt: new Date().toISOString(),
      jobs: persistedHistoryJobs(),
    });
  } catch (error) {
    console.error(`[admin] failed to persist job history: ${error.message}`);
  }
}

function restorePersistedJobHistory() {
  const history = readJsonFile(ADMIN_JOB_HISTORY_PATH);
  const jobs = history?.schemaVersion === 1 && Array.isArray(history.jobs) ? history.jobs : [];
  for (const completedJob of [...jobs].reverse()) {
    if (!completedJob || completedJob.running || !completedJob.finishedAt) continue;
    lastJob = completedJob;
    if (completedJob.wiki) {
      lastWikiJobs.set(completedJob.wiki, completedJob);
      const wikiHistory = [completedJob, ...(lastWikiJobHistory.get(completedJob.wiki) || [])]
        .slice(0, JOB_HISTORY_LIMIT);
      lastWikiJobHistory.set(completedJob.wiki, wikiHistory);
    } else {
      lastGlobalJob = completedJob;
      lastGlobalJobHistory = [completedJob, ...lastGlobalJobHistory].slice(0, JOB_HISTORY_LIMIT);
    }
  }

  const abandoned = readJsonFile(ADMIN_CURRENT_JOB_PATH);
  if (abandoned?.schemaVersion === 1 && abandoned.running) {
    const finishedAt = new Date().toISOString();
    const interrupted = {
      ...abandoned,
      state: "interrupted",
      running: false,
      exitCode: null,
      interrupted: true,
      finishedAt,
      updatedAt: finishedAt,
      log: [...(abandoned.log || []), "\n[admin server restarted before this run reported completion]\n"],
    };
    lastJob = interrupted;
    if (interrupted.wiki) lastWikiJobs.set(interrupted.wiki, interrupted);
    else lastGlobalJob = interrupted;
    recordJobHistory(interrupted);
    clearPersistedRunningJob();
  }
}

// Best-effort read of the status marker `deploy/toolforge/run-refresh.sh`
// writes to the shared NFS output dir. The admin webservice and the
// `wiki-econ-refresh` scheduled Job run in separate Toolforge pods with no
// shared process memory and no Toolforge/Kubernetes API access from the
// admin pod, so this file is the only channel between them.
function readRefreshStatus() {
  let last = null;
  try {
    last = JSON.parse(fs.readFileSync(path.join(OUTPUT_DIR, ".refresh-status.json"), "utf8"));
  } catch {
    last = null;
  }
  let history = [];
  try {
    history = fs
      .readFileSync(path.join(OUTPUT_DIR, ".refresh-history.jsonl"), "utf8")
      .split(/\r?\n/)
      .filter(Boolean)
      .map((line) => {
        try {
          return JSON.parse(line);
        } catch {
          return null;
        }
      })
      .filter(Boolean);
  } catch {
    history = [];
  }
  return { schedule: REFRESH_SCHEDULE, last, history };
}

function readArtifactScrubStatus() {
  const file = path.join(OUTPUT_DIR, "_scrubs", "status.json");
  if (!fs.existsSync(file)) return null;
  try {
    return JSON.parse(fs.readFileSync(file, "utf8"));
  } catch {
    return {invalid: true};
  }
}

function directoryJsonEntries(directory, { directories = false, limit = 100 } = {}) {
  const entries = [];
  for (const entry of safeReadDir(directory)) {
    const entryPath = directories
      ? path.join(directory, entry, "owner.json")
      : path.join(directory, entry);
    const value = readJsonFile(entryPath);
    if (!value) continue;
    let modifiedAt = null;
    try {
      modifiedAt = fs.statSync(entryPath).mtime.toISOString();
    } catch {
      modifiedAt = null;
    }
    entries.push({ file: entry, modifiedAt, value });
  }
  return entries
    .sort((left, right) => Date.parse(right.modifiedAt || 0) - Date.parse(left.modifiedAt || 0))
    .slice(0, limit);
}

function fleetTaskFrom(value) {
  return value?.task || value || {};
}

function fleetWikiFrom(value, filename = "") {
  return fleetTaskFrom(value).wiki || value?.wiki || filename.match(/^([a-z0-9_]+wiki)(?:-|\.|$)/i)?.[1] || null;
}

function readFleetStatus(now = Date.now()) {
  const pendingEntries = directoryJsonEntries(path.join(FLEET_QUEUE_DIR, "pending"));
  const leaseEntries = directoryJsonEntries(path.join(FLEET_QUEUE_DIR, "leases"), { directories: true });
  const quarantineEntries = directoryJsonEntries(path.join(FLEET_QUEUE_DIR, "quarantine"), { limit: 25 });
  const failureEntries = directoryJsonEntries(path.join(FLEET_QUEUE_DIR, "failures"), { limit: 25 });
  const deferredEntries = directoryJsonEntries(path.join(FLEET_QUEUE_DIR, "deferred"), { limit: 100 });
  const deferredByWiki = new Map();
  for (const entry of deferredEntries) {
    const wiki = fleetWikiFrom(entry.value, entry.file);
    if (wiki && !deferredByWiki.has(wiki)) deferredByWiki.set(wiki, entry);
  }
  // Completed tasks are append-only evidence. The dashboard needs their count,
  // not every receipt body, so avoid decoding the full archive on each poll.
  const completedCount = safeReadDir(path.join(FLEET_QUEUE_DIR, "completed")).length;

  const byWiki = new Map();
  for (const entry of pendingEntries) {
    const task = fleetTaskFrom(entry.value);
    const wiki = fleetWikiFrom(entry.value, entry.file);
    if (!wiki) continue;
    const deferred = deferredByWiki.get(wiki);
    const notBeforeUnix = task.not_before_unix ?? null;
    const waitingUpstream = deferred
      && deferred.value?.claim?.task?.task_id === task.task_id
      && Number(notBeforeUnix || 0) * 1000 > now;
    byWiki.set(wiki, {
      wiki,
      state: waitingUpstream ? "waiting_upstream" : "queued",
      snapshot: task.snapshot ?? null,
      resourceClass: task.resource_class ?? null,
      attempt: task.attempt ?? 0,
      notBeforeUnix,
      error: waitingUpstream ? deferred.value.reason : null,
      updatedAt: entry.modifiedAt,
      taskId: task.task_id ?? null,
    });
  }
  for (const entry of leaseEntries) {
    const claim = entry.value;
    const task = fleetTaskFrom(claim);
    const wiki = fleetWikiFrom(claim, entry.file);
    if (!wiki) continue;
    const heartbeatAt = Number(claim.heartbeat_at_unix || 0) * 1000;
    const timeoutMs = Number(claim.lease_timeout_secs || 0) * 1000;
    const heartbeatAgeMs = heartbeatAt > 0 ? Math.max(0, now - heartbeatAt) : null;
    byWiki.set(wiki, {
      wiki,
      state: heartbeatAgeMs != null && timeoutMs > 0 && heartbeatAgeMs > timeoutMs ? "stalled" : "running",
      snapshot: task.snapshot ?? null,
      resourceClass: task.resource_class ?? null,
      attempt: task.attempt ?? 0,
      workerId: claim.worker_id ?? null,
      heartbeatAt: heartbeatAt > 0 ? new Date(heartbeatAt).toISOString() : null,
      heartbeatAgeMs,
      leaseTimeoutSecs: claim.lease_timeout_secs ?? null,
      updatedAt: entry.modifiedAt,
      taskId: task.task_id ?? null,
    });
  }
  for (const entry of quarantineEntries) {
    const wiki = fleetWikiFrom(entry.value, entry.file);
    if (!wiki || byWiki.has(wiki)) continue;
    const task = fleetTaskFrom(entry.value);
    byWiki.set(wiki, {
      wiki,
      state: "quarantined",
      snapshot: task.snapshot ?? null,
      resourceClass: task.resource_class ?? null,
      attempt: task.attempt ?? null,
      error: entry.value.error ?? entry.value.reason ?? "Fleet task requires operator review",
      updatedAt: entry.modifiedAt,
      taskId: task.task_id ?? null,
    });
  }

  const work = Array.from(byWiki.values()).sort((left, right) => {
    const priority = { stalled: 0, quarantined: 1, running: 2, waiting_upstream: 3, queued: 4 };
    return (priority[left.state] ?? 9) - (priority[right.state] ?? 9)
      || String(left.wiki).localeCompare(String(right.wiki));
  });
  return {
    queueDir: FLEET_QUEUE_DIR,
    counts: {
      queued: work.filter((entry) => entry.state === "queued").length,
      waitingUpstream: work.filter((entry) => entry.state === "waiting_upstream").length,
      running: work.filter((entry) => entry.state === "running").length,
      stalled: work.filter((entry) => entry.state === "stalled").length,
      quarantined: quarantineEntries.length,
      recentFailures: failureEntries.length,
      completed: completedCount,
    },
    work,
    quarantine: quarantineEntries.map((entry) => ({
      wiki: fleetWikiFrom(entry.value, entry.file),
      updatedAt: entry.modifiedAt,
      error: entry.value.error ?? entry.value.reason ?? null,
      task: fleetTaskFrom(entry.value),
    })),
    recentFailures: failureEntries.map((entry) => ({
      wiki: fleetWikiFrom(entry.value, entry.file),
      updatedAt: entry.modifiedAt,
      error: entry.value.error ?? entry.value.reason ?? null,
      task: fleetTaskFrom(entry.value),
    })),
  };
}

function readSnapshotPlans() {
  const root = path.join(DATA_DIR, "snapshots");
  const plans = [];
  for (const wiki of safeReadDir(root)) {
    for (const snapshot of safeReadDir(path.join(root, wiki))) {
      const planPath = path.join(root, wiki, snapshot, "source-plan.json");
      const plan = readJsonFile(planPath);
      if (!plan || plan.wiki !== wiki || plan.snapshot !== snapshot) continue;
      let updatedAt = null;
      try {
        updatedAt = fs.statSync(planPath).mtime.toISOString();
      } catch {
        updatedAt = null;
      }
      plans.push({
        wiki,
        snapshot,
        layout: plan.layout ?? null,
        sourceCount: Array.isArray(plan.sources) ? plan.sources.length : null,
        updatedAt,
      });
    }
  }
  return plans.sort((left, right) => Date.parse(right.updatedAt || 0) - Date.parse(left.updatedAt || 0));
}

function loadSupportedWikipedias() {
  if (supportedWikisCache) return supportedWikisCache;
  // Scrape the WIKIPEDIA_DATABASES constant from src/fetch.rs so the picker's
  // universe stays in lockstep with the Rust source. The CLI's actual
  // partitioning dispatch (yearly / all-time / monthly) lives elsewhere in
  // the same file; the picker shows the full set and lets the CLI surface
  // partitioning errors at fetch time for the rare cases where the dump
  // shape doesn't match the picker's offer.
  const fetchSourcePath = path.join(ROOT, "src", "fetch.rs");
  const source = fs.readFileSync(fetchSourcePath, "utf8");
  const match = source.match(/const WIKIPEDIA_DATABASES:\s*&\[&str\]\s*=\s*&\[(?<body>[\s\S]*?)\];/);
  if (!match?.groups?.body) return [];
  supportedWikisCache = Array.from(match.groups.body.matchAll(/"([^"]+)"/g), (entry) => entry[1]).sort();
  return supportedWikisCache;
}

function adminRunId(action, wiki, now = new Date()) {
  const timestamp = now.toISOString().replace(/[-:]/g, "").replace(/\.\d{3}Z$/, "Z");
  const subject = wiki || "global";
  return `admin-${timestamp}-${subject}-${action.replace(/[^a-z0-9_-]/gi, "-")}-${crypto.randomUUID().slice(0, 8)}`;
}

function normalizeVersion(value) {
  const trimmed = typeof value === "string" ? value.trim() : "";
  return trimmed || null;
}

function isValidVersion(version) {
  return /^\d{4}-\d{2}$/.test(version);
}

function safeReadDir(dir) {
  try {
    return fs.readdirSync(dir);
  } catch {
    return [];
  }
}

function countExisting(paths) {
  return paths.filter((entry) => fs.existsSync(entry)).length;
}

function setSyntheticJobLog(meta, lines, exitCode = 0) {
  const command = typeof meta === "string" ? meta : meta.command;
  const startedAt = new Date().toISOString().replace("T", " ").replace(/\.\d+Z$/, " UTC");
  jobLog = [`$ ${command}\nStarted: ${startedAt}\n`, ...lines.map((line) => line.endsWith("\n") ? line : `${line}\n`), `\n[exited with code ${exitCode}]`];
  jobExitCode = exitCode;
  const completedJob = {
    command,
    action: typeof meta === "string" ? null : (meta.action ?? null),
    wiki: typeof meta === "string" ? null : (meta.wiki ?? null),
    stage: typeof meta === "string" ? null : (meta.stage ?? meta.action?.replace("-", "_") ?? null),
    exitCode,
    running: false,
    log: [...jobLog],
    finishedAt: new Date().toISOString(),
    diskHeadroom: null,
    rawCleanup: null,
  };
  lastJob = completedJob;
  if (completedJob.wiki) {
    lastWikiJobs.set(completedJob.wiki, completedJob);
  } else {
    lastGlobalJob = completedJob;
  }
  recordJobHistory(completedJob);
  currentJob = null;
}

function refreshManifest(force = false) {
  const now = Date.now();
  if (!force && manifestCache && now - manifestCacheAt < MANIFEST_CACHE_TTL_MS) {
    return manifestCache;
  }

  manifestCache = JSON.parse(fs.readFileSync(path.join(OUTPUT_DIR, "manifest.json"), "utf8"));
  manifestCacheAt = now;
  return manifestCache;
}

function refreshManifestSafely(force = false) {
  try {
    return refreshManifest(force);
  } catch {
    return null;
  }
}

function markerManifestIsValid(markerPath) {
  try {
    const manifest = JSON.parse(fs.readFileSync(markerPath, "utf8"));
    const sourceId = path.basename(markerPath, ".done");
    if (manifest.schema_version !== 1 || manifest.source_id !== sourceId) return false;
    if (!Number.isSafeInteger(manifest.rows) || manifest.rows < 0) return false;
    if (!manifest.source || !Number.isSafeInteger(manifest.source.size_bytes) || manifest.source.size_bytes <= 0) return false;
    if (!/^[0-9a-f]{64}$/i.test(manifest.source.sha256 ?? "")) return false;
    if (manifest.rows === 0 && !manifest.allow_empty) return false;
    const outputs = [...(manifest.analytical_outputs ?? []), ...(manifest.warehouse_outputs ?? [])];
    if (manifest.rows > 0 && outputs.length < 2) return false;
    return outputs.every((output) => {
      if (!Number.isSafeInteger(output.rows) || output.rows < 0 || path.isAbsolute(output.path)) return false;
      const resolved = path.resolve(DATA_DIR, output.path);
      return resolved.startsWith(`${path.resolve(DATA_DIR)}${path.sep}`) && fs.existsSync(resolved);
    });
  } catch {
    return false;
  }
}

function walkFiles(root, predicate, acc = []) {
  if (!fs.existsSync(root)) return acc;
  for (const entry of fs.readdirSync(root, { withFileTypes: true })) {
    const entryPath = path.join(root, entry.name);
    if (entry.isDirectory()) {
      walkFiles(entryPath, predicate, acc);
    } else if (predicate(entryPath)) {
      acc.push(entryPath);
    }
  }
  return acc;
}

function cleanupWikiArtifacts(wiki) {
  const removed = [];
  const analyticalDir = path.join(DATA_DIR, "parquet", wiki);
  const warehouseDir = path.join(DATA_DIR, "warehouse", wiki);
  const staleBefore = Date.now() - 6 * 60 * 60 * 1000;
  const isOwnedStaleTemporary = (entry) =>
    /^\..+\.done\.\d+\.tmp$/.test(path.basename(entry)) && fs.statSync(entry).mtimeMs <= staleBefore;
  const tmpFiles = [
    ...walkFiles(analyticalDir, isOwnedStaleTemporary),
    ...walkFiles(warehouseDir, isOwnedStaleTemporary),
  ];
  for (const tmpPath of tmpFiles) {
    fs.rmSync(tmpPath, { force: true });
    removed.push(path.relative(ROOT, tmpPath));
  }

  const markerDir = path.join(analyticalDir, "_markers");
  for (const markerName of safeReadDir(markerDir)) {
    if (!markerName.endsWith(".done")) continue;
    const markerPath = path.join(markerDir, markerName);
    if (!markerManifestIsValid(markerPath)) {
      fs.rmSync(markerPath, { force: true });
      removed.push(path.relative(ROOT, markerPath));
    }
  }

  return {
    removed,
    tmpFiles: tmpFiles.length,
    invalidMarkers: removed.filter((entry) => entry.includes("_markers/")).length,
  };
}

// Returns the first line of `chunk` matching `pattern`, trimmed; falls back
// to the whole trimmed chunk if no single line matches (chunks aren't
// guaranteed to be line-aligned).
function firstMatchingLine(chunk, pattern) {
  for (const line of chunk.split(/\r?\n/)) {
    if (pattern.test(line)) return line.trim();
  }
  return chunk.trim();
}

function trackStageFromChunk(chunk) {
  if (!currentJob) return;

  // These two are incidental facts logged mid-stage by src/fetch.rs, not
  // stages themselves, so they're checked independently of the stage
  // if/else-if chain below rather than folded into it.
  if (/disk headroom check passed/i.test(chunk)) {
    currentJob.diskHeadroom = { ok: true, message: firstMatchingLine(chunk, /disk headroom check passed/i) };
  } else if (/insufficient disk space to fetch/i.test(chunk)) {
    currentJob.diskHeadroom = { ok: false, message: firstMatchingLine(chunk, /insufficient disk space to fetch/i) };
  }
  if (/cleaned up raw dump files/i.test(chunk)) {
    currentJob.rawCleanup = { done: true, message: firstMatchingLine(chunk, /cleaned up raw dump files/i) };
  }

  const explicitMatches = [...chunk.matchAll(/\bstage=([a-z_]+)/g)];
  if (explicitMatches.length > 0) {
    currentJob.stage = explicitMatches.at(-1)[1];
  }
  const fetchMatch = chunk.match(/Fetching (\d+) files/i);
  if (fetchMatch) {
    currentJob.stage = "fetch";
    currentJob.expectedTotal = Number.parseInt(fetchMatch[1], 10) || currentJob.expectedTotal;
  } else if (/Compute patrol metrics|Loading patrol data|Autopatrol groups:/i.test(chunk)) {
    currentJob.stage = "patrol_compute";
  } else if (/patrol log dump|Querying siteinfo API|Patrol:\s+\d+|Parsing logging XML/i.test(chunk)) {
    currentJob.stage = "patrol_fetch";
  } else if (/Ingesting|converting:|skipping source/i.test(chunk)) {
    currentJob.stage = "ingest";
  } else if (/Merged \d+ wiki patrol outputs|Wrote baked patrol defaults|merge outputs|merging wiki/i.test(chunk)) {
    currentJob.stage = "merge";
  } else if (/Computing .*metric|Computing revision indexes|Computing patrol latency|Counting revisions/i.test(chunk)) {
    currentJob.stage = currentJob.stage === "patrol_compute" ? "patrol_compute" : "compute";
  }
}

function appendJobLog(chunk) {
  jobLog.push(chunk);
  trackStageFromChunk(chunk);
  scheduleRunningJobPersistence();
}

function getProgress() {
  if (!currentJob) return null;

  const wiki = currentJob.wiki ?? null;
  const action = currentJob.action ?? null;
  if (!wiki && action !== "merge" && action !== "cancel") return null;

  const manifest = refreshManifestSafely() || { wikis: {}, merged: [] };
  const wikiStatus = wiki ? manifest.wikis?.[wiki] ?? null : null;
  const reportedStage = currentJob.stage || (action === "run" ? "fetch" : action);
  const stage = reportedStage === "source_window"
    ? "ingest"
    : reportedStage === "candidate_validate"
      ? "merge"
      : reportedStage;
  let done = 0;
  let total = 1;
  let detail = "starting...";

  switch (stage) {
    case "fetch": {
      done = wikiStatus?.raw?.files ?? 0;
      total = currentJob.expectedTotal || done || 1;
      detail = `${done}/${total} dump files downloaded`;
      break;
    }
    case "patrol_fetch": {
      total = 4;
      done = wikiStatus?.patrol
        ? Number(wikiStatus.patrol.xml) + Number(wikiStatus.patrol.events) + Number(wikiStatus.patrol.rights) + Number(wikiStatus.patrol.groups)
        : 0;
      detail = `${done}/${total} patrol logging artifacts ready`;
      break;
    }
    case "ingest": {
      done = wikiStatus?.parquet?.done ?? 0;
      total = wikiStatus?.parquet?.total ?? 1;
      const inProgress = wikiStatus?.parquet?.in_progress ?? 0;
      detail = `${done}/${total} source files ingested${inProgress > 0 ? ` · ${inProgress} temp files` : ""}`;
      break;
    }
    case "compute": {
      done = (wikiStatus?.metrics ?? []).filter((metric) => metric.name !== "patrol").length;
      total = 8;
      detail = `${done}/${total} core metric files computed`;
      break;
    }
    case "patrol_compute": {
      total = 1;
      done = Number(Boolean(wikiStatus?.patrol?.metric_ready));
      detail = done ? "patrol metrics written" : "computing patrol metrics";
      break;
    }
    case "merge": {
      done = manifest.merged?.length ?? 0;
      total = REQUIRED_MERGED_METRICS;
      detail = `${done}/${total} merged site data files ready`;
      break;
    }
    case "cleanup": {
      done = 1;
      total = 1;
      detail = wiki ? `cleanup completed for ${wiki}` : "cleanup completed";
      break;
    }
    case "cancel": {
      done = 1;
      total = 1;
      detail = "job cancellation requested";
      break;
    }
    default: {
      done = 0;
      total = 1;
    }
  }

  const pct = total > 0 ? Math.min(100, Math.round((done / total) * 100)) : 0;
  return {
    wiki,
    stage,
    done,
    total,
    pct,
    detail,
    diskHeadroom: currentJob.diskHeadroom ?? null,
    rawCleanup: currentJob.rawCleanup ?? null,
  };
}

function matchApiPath(pathname) {
  if (pathname.startsWith(`${LEGACY_API_PREFIX}/`)) return pathname.slice(LEGACY_API_PREFIX.length + 1);
  if (pathname.startsWith(`${PROXY_API_PREFIX}/`)) return pathname.slice(PROXY_API_PREFIX.length + 1);
  return null;
}

function wikiLifecycleStatus(now = Date.now()) {
  return Object.fromEntries(Object.entries(WIKI_LIFECYCLE.wikis).map(([wiki, entry]) => {
    const metricPath = path.join(OUTPUT_DIR, wiki, "gdp.parquet");
    let lastPublishedAt = null;
    try {
      lastPublishedAt = fs.statSync(metricPath).mtime.toISOString();
    } catch (error) {
      if (error.code !== "ENOENT") throw error;
    }
    let freshness = entry.refresh === "paused" ? "paused" : "manual";
    if (entry.refresh === "scheduled") {
      if (!lastPublishedAt) freshness = "missing";
      else {
        const ageDays = (now - Date.parse(lastPublishedAt)) / (24 * 60 * 60 * 1000);
        freshness = ageDays > entry.freshness_sla_days ? "overdue" : "current";
      }
    }
    return [wiki, {
      ...entry,
      imported_cutoff: entry.imported_cutoff ?? null,
      last_published_at: lastPublishedAt,
      freshness,
    }];
  }));
}

function buildStatusPayload(req, session) {
  reloadWikiLifecycle();
  const progress = getProgress();
  const scheduledRefresh = readRefreshStatus();
  const adminOperations = readAdminOperations();
  const effectiveJob = currentJob
    ? {
        runId: currentJob.runId,
        command: currentJob.command,
        action: currentJob.action,
        wiki: currentJob.wiki,
        stage: currentJob.stage,
        running: true,
        state: currentJob.cancelRequested ? "cancelling" : "running",
        startedAt: currentJob.startedAt,
        updatedAt: new Date().toISOString(),
        exitCode: null,
        log: jobLog,
        progress,
      }
    : lastJob;
  const manifest = refreshManifestSafely() || { error: "Manifest unavailable" };
  return {
    running: currentJob !== null,
    command: effectiveJob?.command ?? null,
    action: effectiveJob?.action ?? null,
    wiki: effectiveJob?.wiki ?? null,
    log: effectiveJob?.log ?? [],
    exitCode: effectiveJob?.exitCode ?? jobExitCode,
    progress,
    manifest,
    job: effectiveJob,
    wikiJobs: Object.fromEntries(lastWikiJobs.entries()),
    globalJob: lastGlobalJob,
    wikiJobHistory: Object.fromEntries(lastWikiJobHistory.entries()),
    globalJobHistory: lastGlobalJobHistory,
    supportedWikis: loadSupportedWikipedias(),
    adminEnabled: ADMIN_ENABLED,
    adminPort: PORT,
    environment: RUNTIME_ENV,
    executionMode: ADMIN_EXECUTION_MODE,
    enabledWikis: REFRESH_WIKIS,
    refreshWikis: REFRESH_WIKIS,
    publishedWikis: PUBLISHED_WIKIS,
    wikiLifecycle: WIKI_LIFECYCLE,
    wikiStates: wikiLifecycleStatus(),
    runner: runnerInfo(),
    scheduledRefresh,
    freshness: evaluateFreshness({
      ...scheduledRefresh,
      lifecycle: WIKI_LIFECYCLE,
      scrubStatus: readArtifactScrubStatus(),
    }),
    fleet: readFleetStatus(),
    snapshotPlans: readSnapshotPlans(),
    adminOperations,
    adminRuns: {
      active: currentJob ? serializableRunningJob() : (adminOperations.running[0] || null),
      recent: [...adminOperations.recent, ...persistedHistoryJobs()]
        .sort((left, right) => Date.parse(right.finishedAt || right.updatedAt || 0)
          - Date.parse(left.finishedAt || left.updatedAt || 0))
        .slice(0, ADMIN_JOB_HISTORY_LIMIT),
    },
    auth: authStatus(session, req),
  };
}

restorePersistedJobHistory();

async function handleRequest(req, res) {
  applyCors(req, res);
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  if (req.method === "OPTIONS") {
    res.writeHead(204);
    res.end();
    return;
  }

  const url = new URL(req.url, `http://localhost:${PORT}`);
  const session = AUTH_ENABLED ? readSession(req) : null;

  if (req.method === "GET" && url.pathname === FRESHNESS_STATUS_PATH) {
    reloadWikiLifecycle();
    writeJson(res, 200, evaluateFreshness({
      ...readRefreshStatus(),
      lifecycle: WIKI_LIFECYCLE,
      scrubStatus: readArtifactScrubStatus(),
    }));
    return;
  }

  if (req.method === "GET" && (url.pathname === ADMIN_PAGE_PATH || url.pathname === `${ADMIN_PAGE_PATH}.html`)) {
    if (AUTH_ENABLED && !session) {
      redirect(res, loginUrlFor(ADMIN_PAGE_PATH));
      return;
    }
    serveAdminPage(res);
    return;
  }

  if (AUTH_ENABLED && req.method === "GET" && url.pathname === ADMIN_LOGIN_PATH) {
    if (session) {
      redirect(res, sanitizeNextPath(url.searchParams.get("next"), ADMIN_PAGE_PATH));
      return;
    }
    const errorMessage = url.searchParams.get("error");
    const message = errorMessage
      ? `Sign-in failed: <code>${escapeHtml(errorMessage)}</code>`
      : "This admin surface is protected. Sign in with your Wikimedia account; your username must be in the authorized allowlist.";
    writeHtml(res, 200, renderLoginPage(req, message, url.searchParams.get("next")));
    return;
  }

  if (AUTH_ENABLED && req.method === "GET" && url.pathname === ADMIN_OAUTH_START_PATH) {
    await startMediawikiLogin(req, res, url.searchParams.get("next"));
    return;
  }

  if (AUTH_ENABLED && req.method === "GET" && url.pathname === ADMIN_OAUTH_CALLBACK_PATH) {
    await finishMediawikiLogin(req, res, url);
    return;
  }

  if (AUTH_ENABLED && (req.method === "GET" || req.method === "POST") && url.pathname === ADMIN_LOGOUT_PATH) {
    if (req.method === "POST" && !requireTrustedOrigin(req, res)) return;
    clearAuthCookies(res);
    writeHtml(res, 200, renderAuthPage({
      title: "Signed out",
      heading: "Signed out",
      message: "Your admin session has been cleared.",
      actionUrl: loginUrlFor(ADMIN_PAGE_PATH),
      actionLabel: "Sign in again",
      secondaryActionUrl: "/",
      secondaryActionLabel: "Back to dashboard",
    }));
    return;
  }

  const apiPath = matchApiPath(url.pathname);
  if (req.method === "GET" && apiPath === "status") {
    if (AUTH_ENABLED && !session) {
      unauthorizedApiResponse(res, req);
      return;
    }
    writeJson(res, 200, buildStatusPayload(req, session));
    return;
  }

  if (req.method === "POST" && apiPath) {
    if (AUTH_ENABLED && !session) {
      unauthorizedApiResponse(res, req);
      return;
    }
    if (AUTH_ENABLED && !requireTrustedOrigin(req, res)) {
      return;
    }

    let body = "";
    req.on("data", (chunk) => {
      body += chunk;
    });
    req.on("end", () => {
      let params = {};
      try {
        params = body ? JSON.parse(body) : {};
      } catch {
        writeJson(res, 400, { error: "Invalid JSON request body" });
        return;
      }
      const action = apiPath;

      const wiki = (params.wiki || "").replace(/[^a-z0-9_]/gi, "");
      const version = normalizeVersion(params.version);
      if (version && !isValidVersion(version)) {
        writeJson(res, 400, { error: "Invalid version. Use YYYY-MM." });
        return;
      }
      const operator = session?.username || "local-operator";

      if (action === "register-wiki") {
        const operations = readAdminOperations();
        if (currentJob || operations.running.length > 0) {
          writeJson(res, 409, {error: "Lifecycle changes are blocked while an operator job is running"});
          return;
        }
        if (!wiki) {
          writeJson(res, 400, {error: "register-wiki requires a wiki parameter"});
          return;
        }
        try {
          const registration = registerWikiLifecycle({
            wiki,
            mode: String(params.mode || "qualification"),
            resourceClass: String(params.resourceClass || "medium_large"),
            operator,
          });
          writeJson(res, 201, {registered: true, ...registration});
        } catch (error) {
          writeJson(res, 400, {error: error.message});
        }
        return;
      }

      if (action === "onboard-wiki") {
        const operations = readAdminOperations();
        if (currentJob || operations.running.length > 0) {
          writeJson(res, 409, {error: "Project onboarding is blocked while an operator job is running"});
          return;
        }
        if (!wiki) {
          writeJson(res, 400, {error: "onboard-wiki requires a wiki parameter"});
          return;
        }
        try {
          const mode = String(params.mode || "qualification");
          const registration = registerWikiLifecycle({
            wiki,
            mode,
            resourceClass: String(params.resourceClass || "medium_large"),
            operator,
          });
          const nextAction = mode === "qualification" ? "qualify" : "run";
          if (ADMIN_EXECUTION_MODE === "queue") {
            const request = queueAdminOperation({
              action: nextAction,
              wiki,
              version,
              requestedBy: operator,
              acknowledgeBlockedRetry: params.acknowledgeBlockedRetry === true,
            });
            writeJson(res, 202, {
              registered: true,
              queued: true,
              nextAction,
              requestId: request.requestId,
              operation: request,
              ...registration,
            });
          } else {
            // Local development deliberately keeps the historical direct
            // runner; the browser performs the returned follow-up only after
            // this durable registry update has completed.
            writeJson(res, 201, {registered: true, queued: false, nextAction, ...registration});
          }
        } catch (error) {
          writeJson(res, 400, {error: error.message});
        }
        return;
      }

      if (action === "cancel" && ADMIN_EXECUTION_MODE === "queue") {
        const cancelled = cancelAdminOperation({requestId: params.requestId || null, wiki: wiki || null});
        if (!cancelled) {
          writeJson(res, 409, {error: "No job is currently running"});
          return;
        }
        writeJson(res, 200, {started: false, cancelled: cancelled.state === "cancelled", operation: cancelled});
        return;
      }

      if (action === "recover-admin" && ADMIN_EXECUTION_MODE === "queue") {
        try {
          writeJson(res, 200, {started: false, recovered: true, ...recoverStaleAdminOperations()});
        } catch (error) {
          writeJson(res, 409, {error: error.message});
        }
        return;
      }

      if (currentJob) {
        if (action === "cancel") {
          currentJob.cancelRequested = true;
          currentJob.proc.kill("SIGTERM");
          appendJobLog(`\n[cancel requested for pid ${currentJob.pid}]`);
          persistRunningJobNow();
          writeJson(res, 200, { started: false, cancelled: true, pid: currentJob.pid });
          return;
        }
        writeJson(res, 409, { error: "A job is already running", command: currentJob.command });
        return;
      }

      const runId = adminRunId(action, wiki);

      reloadWikiLifecycle();
      const wikiActions = new Set([
        "fetch", "ingest", "compute", "run", "qualify",
        "patrol-fetch", "patrol-compute", "patrol-rebuild",
      ]);
      if (wikiActions.has(action) && wiki && !WIKI_LIFECYCLE.wikis[wiki]) {
        writeJson(res, 409, {
          error: `${wiki} is not registered in the wiki lifecycle; add it as a qualification or managed project before processing`,
          wiki,
          lifecycleRequired: true,
        });
        return;
      }

      const globalActions = new Set(["merge", "publish", "site", "fleet-recover"]);
      if (wikiActions.has(action) && !wiki) {
        writeJson(res, 400, {error: `${action} requires a wiki parameter`});
        return;
      }
      if (action === "qualify") {
        const lifecycle = WIKI_LIFECYCLE.wikis[wiki];
        if (lifecycle?.publication !== "hidden" || lifecycle?.refresh !== "qualification") {
          writeJson(res, 409, {error: `${wiki} must be registered as hidden/qualification before qualification`});
          return;
        }
      }
      if (action === "run") {
        const lifecycle = WIKI_LIFECYCLE.wikis[wiki];
        if (lifecycle?.publication !== "published" || !new Set(["manual", "scheduled"]).has(lifecycle?.refresh)) {
          writeJson(res, 409, {error: `${wiki} must be registered as a manual or scheduled published project before preparation`});
          return;
        }
      }

      if (ADMIN_EXECUTION_MODE === "queue" && (wikiActions.has(action) || globalActions.has(action))) {
        try {
          const request = queueAdminOperation({
            action,
            wiki,
            version,
            requestedBy: operator,
            acknowledgeBlockedRetry: params.acknowledgeBlockedRetry === true,
          });
          writeJson(res, 202, {
            started: false,
            queued: true,
            requestId: request.requestId,
            operation: request,
          });
        } catch (error) {
          writeJson(res, 409, {error: error.message});
        }
        return;
      }

      if (action === "cleanup") {
        if (!wiki) {
          writeJson(res, 400, { error: "cleanup requires a wiki parameter" });
          return;
        }
        const summary = cleanupWikiArtifacts(wiki);
        refreshManifestSafely(true);
        setSyntheticJobLog(
          {
            command: `cleanup ${wiki}`,
            action: "cleanup",
            wiki,
            stage: "cleanup",
          },
          [
            `Cleanup finished for ${wiki}`,
            `Removed ${summary.tmpFiles} temporary files`,
            `Removed ${summary.invalidMarkers} invalid markers`,
            ...(summary.removed.length > 0 ? ["", ...summary.removed.map((entry) => `- ${entry}`)] : ["No files removed"]),
          ],
          0,
        );
        writeJson(res, 200, { started: false, cleaned: true, summary });
        return;
      }

      let commandSpec = null;
      switch (action) {
        case "fetch":
        case "ingest":
        case "compute":
          commandSpec = wiki
            ? {
                program: resolveRunner().program,
                args: [
                  ...resolveRunner().args,
                  "--data-dir", DATA_DIR,
                  "--output-dir", OUTPUT_DIR,
                  "--run-id", runId,
                  action,
                  wiki,
                  ...(version && action === "fetch" ? ["--version", version] : []),
                ],
                label: `${resolveRunner().label} --data-dir ${DATA_DIR} --output-dir ${OUTPUT_DIR} --run-id ${runId} ${action} ${wiki}${version && action === "fetch" ? ` --version ${version}` : ""}`,
              }
            : null;
          break;
        case "run":
          commandSpec = wiki
            ? {
                program: resolveRunner().program,
                args: [
                  ...resolveRunner().args,
                  "--data-dir", DATA_DIR,
                  "--output-dir", OUTPUT_DIR,
                  "--run-id", runId,
                  "prepare-wiki", wiki,
                  ...(version ? ["--version", version] : []),
                  "--lifecycle", WIKI_LIFECYCLE_PATH,
                ],
                label: `${resolveRunner().label} --data-dir ${DATA_DIR} --output-dir ${OUTPUT_DIR} --run-id ${runId} prepare-wiki ${wiki}${version ? ` --version ${version}` : ""} --lifecycle ${WIKI_LIFECYCLE_PATH}`,
              }
            : null;
          break;
        case "qualify":
          commandSpec = wiki
            ? {
                program: resolveRunner().program,
                args: [
                  ...resolveRunner().args,
                  "--data-dir", DATA_DIR,
                  "--output-dir", OUTPUT_DIR,
                  "--run-id", runId,
                  "qualify-wiki", wiki,
                  ...(version ? ["--version", version] : []),
                  "--lifecycle", WIKI_LIFECYCLE_PATH,
                ],
                label: `${resolveRunner().label} --data-dir ${DATA_DIR} --output-dir ${OUTPUT_DIR} --run-id ${runId} qualify-wiki ${wiki}${version ? ` --version ${version}` : ""} --lifecycle ${WIKI_LIFECYCLE_PATH}`,
              }
            : null;
          break;
        case "merge":
          commandSpec = {
            program: resolveRunner().program,
            args: [...resolveRunner().args, "--data-dir", DATA_DIR, "--output-dir", OUTPUT_DIR, "--run-id", runId, "merge"],
            label: `${resolveRunner().label} --data-dir ${DATA_DIR} --output-dir ${OUTPUT_DIR} --run-id ${runId} merge`,
          };
          break;
        case "publish":
          commandSpec = {
            program: "bash",
            args: [path.join(ROOT, "deploy", "toolforge", "run-publish-ready.sh")],
            label: "deploy/toolforge/run-publish-ready.sh",
          };
          break;
        case "site":
          commandSpec = {
            program: "bash",
            args: [path.join(ROOT, "deploy", "toolforge", "run-refresh-site.sh")],
            label: "deploy/toolforge/run-refresh-site.sh",
          };
          break;
        case "fleet-recover":
          commandSpec = {
            program: resolveRunner().program,
            args: [...resolveRunner().args, "fleet-recover", "--queue-dir", FLEET_QUEUE_DIR],
            label: `${resolveRunner().label} fleet-recover --queue-dir ${FLEET_QUEUE_DIR}`,
          };
          break;
        case "patrol-fetch":
          commandSpec = wiki
            ? {
                program: resolveRunner().program,
                args: [...resolveRunner().args, "--data-dir", DATA_DIR, "--output-dir", OUTPUT_DIR, "--run-id", runId, "patrol-fetch", wiki],
                label: `${resolveRunner().label} --data-dir ${DATA_DIR} --output-dir ${OUTPUT_DIR} --run-id ${runId} patrol-fetch ${wiki}`,
              }
            : null;
          break;
        case "patrol-compute":
        case "patrol-rebuild":
          commandSpec = wiki
            ? {
                program: resolveRunner().program,
                args: [
                  ...resolveRunner().args,
                  "--data-dir", DATA_DIR,
                  "--output-dir", OUTPUT_DIR,
                  "--run-id", runId,
                  "patrol-refresh", wiki,
                  ...(action === "patrol-rebuild" ? ["--rebuild"] : []),
                ],
                label: `${resolveRunner().label} --data-dir ${DATA_DIR} --output-dir ${OUTPUT_DIR} --run-id ${runId} patrol-refresh ${wiki}${action === "patrol-rebuild" ? " --rebuild" : ""}`,
              }
            : null;
          break;
        case "cancel":
          writeJson(res, 409, { error: "No job is currently running" });
          return;
        default:
          commandSpec = null;
      }

      if (!commandSpec) {
        writeJson(res, 400, { error: "Invalid action or missing wiki parameter" });
        return;
      }

      const startTime = new Date().toISOString().replace("T", " ").replace(/\.\d+Z$/, " UTC");
      const startedAt = new Date().toISOString();
      jobLog = [`$ ${commandSpec.label}\nStarted: ${startTime}\n`];
      jobExitCode = null;

      const proc = spawn(commandSpec.program, commandSpec.args, {
        cwd: ROOT,
        env: {
          ...process.env,
          RUST_LOG: "info",
          PYTHONUNBUFFERED: "1",
          WIKI_ECON_DATA_DIR: DATA_DIR,
          WIKI_ECON_OUTPUT_DIR: OUTPUT_DIR,
          WIKI_ECON_GENERATOR_DIR: GENERATOR_DIR,
          WIKI_ECON_WIKI_LIFECYCLE_FILE: WIKI_LIFECYCLE_PATH,
          WIKI_ECON_SITE_DIST_DIR: SITE_DIST_DIR,
        },
      });
      currentJob = {
        runId,
        command: commandSpec.label,
        pid: proc.pid,
        proc,
        action,
        wiki: wiki || null,
          stage: ["run", "qualify"].includes(action)
            ? "fetch"
            : ["patrol-compute", "patrol-rebuild"].includes(action)
            ? "patrol_fetch"
            : action.replace("-", "_"),
        expectedTotal: null,
        cancelRequested: false,
        startedAt,
      };
      persistRunningJobNow();

      proc.stdout.on("data", (data) => appendJobLog(data.toString()));
      proc.stderr.on("data", (data) => appendJobLog(data.toString()));
      let processFinalized = false;
      const finalizeProcess = ({ code, signal = null, error = null }) => {
        // A failed spawn emits both `error` and `close`. Persist one terminal
        // record so the activity ledger cannot show duplicate failures.
        if (processFinalized) return;
        processFinalized = true;
        const cancelled = !error && currentJob?.cancelRequested && signal === "SIGTERM";
        const exitCode = cancelled ? 130 : error ? 1 : code;
        if (error) jobLog.push(`\n[failed to start: ${error.message}]`);
        else jobLog.push(`\n[exited with code ${cancelled ? "cancelled" : code}]`);
        jobExitCode = exitCode;
        const completedJob = {
          runId,
          command: commandSpec.label,
          action,
          wiki: wiki || null,
          stage: currentJob?.stage ?? action.replace("-", "_"),
          exitCode,
          cancelled,
          running: false,
          state: cancelled ? "cancelled" : exitCode === 0 ? "succeeded" : "failed",
          log: [...jobLog],
          startedAt,
          finishedAt: new Date().toISOString(),
          diskHeadroom: currentJob?.diskHeadroom ?? null,
          rawCleanup: currentJob?.rawCleanup ?? null,
        };
        lastJob = completedJob;
        if (completedJob.wiki) {
          lastWikiJobs.set(completedJob.wiki, completedJob);
        } else {
          lastGlobalJob = completedJob;
        }
        recordJobHistory(completedJob);
        currentJob = null;
        clearPersistedRunningJob();
        refreshManifestSafely(true);
      };
      proc.on("close", (code, signal) => finalizeProcess({ code, signal }));
      proc.on("error", (error) => finalizeProcess({ code: 1, error }));

      writeJson(res, 200, { started: true, command: commandSpec.label, pid: proc.pid });
      console.log(`[admin] started: ${commandSpec.label} (pid ${proc.pid})`);
    });
    return;
  }

  if ((req.method === "GET" || req.method === "HEAD") && serveStaticAsset(req, res, url.pathname)) {
    return;
  }

  res.writeHead(404, { "Cache-Control": "no-store" });
  res.end("Not found");
}

function createServer() {
  return http.createServer((req, res) => {
    Promise.resolve(handleRequest(req, res)).catch((error) => {
      console.error(`[admin] unhandled error: ${error.stack || error.message}`);
      if (!res.headersSent) {
        writeJson(res, 500, { error: "Internal server error" });
      } else {
        res.end();
      }
    });
  });
}

function startServer() {
  const server = createServer();
  server.listen(PORT, BIND_HOST, () => {
    const runner = resolveRunner();
    console.log(`Admin server listening on http://${BIND_HOST}:${PORT}`);
    console.log(`Runner: ${runner.label}`);
    console.log(`Working dir: ${ROOT}`);
    console.log(`Data dir: ${DATA_DIR}`);
    console.log(`Output dir: ${OUTPUT_DIR}`);
    console.log(`Generator dir: ${GENERATOR_DIR}`);
    console.log(`Site dist dir: ${SITE_DIST_DIR}`);
    console.log(`Allowed origins: ${Array.from(ALLOWED_ORIGINS).join(", ")}`);
    console.log(`Auth mode: ${ADMIN_AUTH_MODE}`);
    if (AUTH_ENABLED) {
      console.log(`Authorized admin usernames: ${ADMIN_ALLOWED_USERNAMES.size}`);
      console.log(`MediaWiki OAuth host: ${ADMIN_MEDIAWIKI_HOST}`);
    }
  });
  return server;
}

if (require.main === module) {
  startServer();
}

module.exports = {
  ADMIN_PAGE_PATH,
  FRESHNESS_STATUS_PATH,
  PROXY_API_PREFIX,
  createServer,
  handleRequest,
  startServer,
};
