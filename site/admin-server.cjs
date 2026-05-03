#!/usr/bin/env node
// Admin server for the wiki-economics operator surface.
// - Local/dev mode: loopback-only API for scripts/dev.sh and Observable preview.
// - VPS mode: authenticated /admin page plus authenticated /admin-api/* routes.

const http = require("http");
const { execFileSync, spawn } = require("child_process");
const path = require("path");
const fs = require("fs");
const {
  buildAuthorizeUrl,
  escapeHtml,
  normalizeEmail,
  parseAllowedEmails,
  parseCookies,
  randomToken,
  sanitizeNextPath,
  serializeCookie,
  signJsonToken,
  verifyJsonToken,
} = require("./admin-auth.cjs");

const ROOT = path.resolve(__dirname, "..");
const RUNTIME_ENV = process.env.WIKI_ECON_ENV || "local";
const ADMIN_ENABLED = (process.env.WIKI_ECON_ADMIN_ENABLED ?? (RUNTIME_ENV === "production" ? "0" : "1")) === "1";
const PORT = Number.parseInt(process.env.WIKI_ECON_ADMIN_PORT || "3001", 10);
const SITE_PORT = Number.parseInt(process.env.WIKI_ECON_SITE_PORT || "3000", 10);
const DATA_DIR = resolveConfiguredPath("WIKI_ECON_DATA_DIR", "data");
const OUTPUT_DIR = resolveConfiguredPath("WIKI_ECON_OUTPUT_DIR", "output");
const GENERATOR_DIR = resolveConfiguredPath("WIKI_ECON_GENERATOR_DIR", path.join("site", "data-build"));
const SITE_DIST_DIR = resolveConfiguredPath("WIKI_ECON_SITE_DIST_DIR", path.join("site", "dist"));
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
const ADMIN_AUTH_MODE = process.env.WIKI_ECON_ADMIN_AUTH_MODE || "none";
const AUTH_ENABLED = ADMIN_AUTH_MODE !== "none";
const ADMIN_ALLOWED_EMAILS = parseAllowedEmails(process.env.WIKI_ECON_ADMIN_ALLOWED_EMAILS || "");
const ADMIN_SESSION_SECRET = process.env.WIKI_ECON_ADMIN_SESSION_SECRET || "";
const ADMIN_SESSION_COOKIE_NAME = process.env.WIKI_ECON_ADMIN_SESSION_COOKIE_NAME || "wiki_econ_admin_session";
const ADMIN_OAUTH_STATE_COOKIE_NAME = process.env.WIKI_ECON_ADMIN_OAUTH_STATE_COOKIE_NAME || "wiki_econ_admin_oauth_state";
const ADMIN_SESSION_TTL_SECS = parsePositiveInt(process.env.WIKI_ECON_ADMIN_SESSION_TTL_SECS, 8 * 60 * 60);
const ADMIN_REQUIRE_VERIFIED_EMAIL = (process.env.WIKI_ECON_ADMIN_REQUIRE_VERIFIED_EMAIL || "1") !== "0";
const ADMIN_SECURE_COOKIES = (process.env.WIKI_ECON_ADMIN_SECURE_COOKIES ?? (RUNTIME_ENV === "production" ? "1" : "0")) === "1";
const ADMIN_PUBLIC_ORIGIN = normalizeConfiguredOrigin(process.env.WIKI_ECON_ADMIN_PUBLIC_ORIGIN || "");
const ADMIN_OIDC_ISSUER = process.env.WIKI_ECON_ADMIN_OIDC_ISSUER || "";
const ADMIN_OIDC_CLIENT_ID = process.env.WIKI_ECON_ADMIN_OIDC_CLIENT_ID || "";
const ADMIN_OIDC_CLIENT_SECRET = process.env.WIKI_ECON_ADMIN_OIDC_CLIENT_SECRET || "";
const ADMIN_OIDC_SCOPES = (process.env.WIKI_ECON_ADMIN_OIDC_SCOPES || "openid email profile").trim();
const ALLOWED_ORIGINS = resolveAllowedOrigins();

let currentJob = null;
let jobLog = [];
let jobExitCode = null;
let lastJob = null;
let lastWikiJobs = new Map();
let lastGlobalJob = null;
let manifestCache = null;
let manifestCacheAt = 0;
const MANIFEST_CACHE_TTL_MS = 1500;
const REQUIRED_MERGED_METRICS = 9;
let supportedWikisCache = null;
let oidcMetadataPromise = null;

if (!ADMIN_ENABLED) {
  console.error("Admin API is disabled for this runtime. Set WIKI_ECON_ADMIN_ENABLED=1 to opt in.");
  process.exit(1);
}

if (RUNTIME_ENV === "production" && !AUTH_ENABLED) {
  console.error("Refusing to run the admin server in production without authentication. Set WIKI_ECON_ADMIN_AUTH_MODE=oidc.");
  process.exit(1);
}

if (AUTH_ENABLED && ADMIN_AUTH_MODE !== "oidc") {
  console.error(`Unsupported WIKI_ECON_ADMIN_AUTH_MODE: ${ADMIN_AUTH_MODE}. Expected "none" or "oidc".`);
  process.exit(1);
}

if (AUTH_ENABLED) {
  const missing = [];
  if (!ADMIN_OIDC_ISSUER) missing.push("WIKI_ECON_ADMIN_OIDC_ISSUER");
  if (!ADMIN_OIDC_CLIENT_ID) missing.push("WIKI_ECON_ADMIN_OIDC_CLIENT_ID");
  if (!ADMIN_OIDC_CLIENT_SECRET) missing.push("WIKI_ECON_ADMIN_OIDC_CLIENT_SECRET");
  if (!ADMIN_SESSION_SECRET || ADMIN_SESSION_SECRET.length < 32) missing.push("WIKI_ECON_ADMIN_SESSION_SECRET (32+ chars)");
  if (ADMIN_ALLOWED_EMAILS.size === 0) missing.push("WIKI_ECON_ADMIN_ALLOWED_EMAILS");
  if (missing.length > 0) {
    console.error(`Missing required admin auth configuration: ${missing.join(", ")}`);
    process.exit(1);
  }
}

function parsePositiveInt(value, fallback) {
  const parsed = Number.parseInt(value || "", 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
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
      email: session.email,
      name: session.name || session.email,
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
  if (!payload.email || !payload.exp) return null;
  if ((Number(payload.exp) || 0) <= Math.floor(Date.now() / 1000)) return null;
  const normalized = normalizeEmail(payload.email);
  if (!ADMIN_ALLOWED_EMAILS.has(normalized)) return null;
  return {
    email: normalized,
    name: typeof payload.name === "string" ? payload.name : normalized,
    sub: typeof payload.sub === "string" ? payload.sub : "",
    provider: typeof payload.provider === "string" ? payload.provider : "",
  };
}

function issueSession(res, profile) {
  const expiresAt = Math.floor(Date.now() / 1000) + ADMIN_SESSION_TTL_SECS;
  const token = signJsonToken({
    email: profile.email,
    name: profile.name || profile.email,
    sub: profile.sub,
    provider: ADMIN_OIDC_ISSUER,
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
      font-family: "IBM Plex Sans", system-ui, sans-serif;
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
      font-family: "IBM Plex Mono", ui-monospace, monospace;
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

async function loadOidcMetadata() {
  if (!oidcMetadataPromise) {
    oidcMetadataPromise = (async () => {
      const issuer = new URL(ADMIN_OIDC_ISSUER.endsWith("/") ? ADMIN_OIDC_ISSUER : `${ADMIN_OIDC_ISSUER}/`);
      const discoveryUrl = new URL(".well-known/openid-configuration", issuer);
      const metadata = await fetchJson(discoveryUrl.toString());
      for (const key of ["authorization_endpoint", "token_endpoint", "userinfo_endpoint"]) {
        if (!metadata[key]) {
          throw new Error(`OIDC discovery document is missing ${key}`);
        }
      }
      return metadata;
    })();
  }
  return oidcMetadataPromise;
}

function normalizeOidcProfile(profile) {
  const email = normalizeEmail(profile.email);
  if (!email) {
    throw new Error("The identity provider did not return an email address.");
  }
  if (ADMIN_REQUIRE_VERIFIED_EMAIL && Object.hasOwn(profile, "email_verified") && profile.email_verified !== true) {
    throw new Error(`The identity provider returned an unverified email for ${email}.`);
  }
  if (!ADMIN_ALLOWED_EMAILS.has(email)) {
    throw new Error(`The signed-in email ${email} is not in the configured allowlist.`);
  }
  return {
    email,
    name: typeof profile.name === "string" && profile.name.trim() ? profile.name.trim() : email,
    sub: typeof profile.sub === "string" ? profile.sub : email,
  };
}

async function startOidcLogin(req, res, nextPath) {
  const metadata = await loadOidcMetadata();
  const state = randomToken(24);
  const nonce = randomToken(24);
  const next = sanitizeNextPath(nextPath, ADMIN_PAGE_PATH);
  const stateToken = signJsonToken({
    state,
    nonce,
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
    authorizationEndpoint: metadata.authorization_endpoint,
    clientId: ADMIN_OIDC_CLIENT_ID,
    redirectUri: externalUrl(req, ADMIN_OAUTH_CALLBACK_PATH),
    scopes: ADMIN_OIDC_SCOPES,
    state,
    nonce,
  });
  redirect(res, authorizeUrl);
}

async function finishOidcLogin(req, res, url) {
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
    const metadata = await loadOidcMetadata();
    const tokenResponse = await fetchJson(metadata.token_endpoint, {
      method: "POST",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded",
      },
      body: new URLSearchParams({
        grant_type: "authorization_code",
        code,
        client_id: ADMIN_OIDC_CLIENT_ID,
        client_secret: ADMIN_OIDC_CLIENT_SECRET,
        redirect_uri: externalUrl(req, ADMIN_OAUTH_CALLBACK_PATH),
      }),
    });
    if (!tokenResponse.access_token) {
      throw new Error("OIDC token response did not contain an access token.");
    }
    const profile = await fetchJson(metadata.userinfo_endpoint, {
      headers: {
        Authorization: `Bearer ${tokenResponse.access_token}`,
      },
    });
    const normalized = normalizeOidcProfile(profile);
    issueSession(res, normalized);
    redirect(res, sanitizeNextPath(savedState.next, ADMIN_PAGE_PATH));
  } catch (authError) {
    redirect(res, `${ADMIN_LOGIN_PATH}?error=${encodeURIComponent(authError.message)}`);
  }
}

function resolveRunner() {
  const customBin = process.env.WIKI_ECON_BIN;
  if (customBin) {
    return {
      program: customBin,
      args: [],
      label: customBin,
    };
  }
  return DEFAULT_RUNNER;
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

function suggestedSnapshotVersion(now = new Date()) {
  const currentMonth = now.getUTCMonth();
  const year = currentMonth === 0 ? now.getUTCFullYear() - 1 : now.getUTCFullYear();
  const month = currentMonth === 0 ? 12 : currentMonth;
  return `${year}-${String(month).padStart(2, "0")}`;
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
  };
  lastJob = completedJob;
  if (completedJob.wiki) {
    lastWikiJobs.set(completedJob.wiki, completedJob);
  } else {
    lastGlobalJob = completedJob;
  }
  currentJob = null;
}

function refreshManifest(force = false) {
  const now = Date.now();
  if (!force && manifestCache && now - manifestCacheAt < MANIFEST_CACHE_TTL_MS) {
    return manifestCache;
  }

  const manifestScript = path.join(GENERATOR_DIR, "manifest.json.sh");
  const output = execFileSync("/bin/bash", [manifestScript], {
    cwd: ROOT,
    encoding: "utf8",
    env: {
      ...process.env,
      WIKI_ECON_DATA_DIR: DATA_DIR,
      WIKI_ECON_OUTPUT_DIR: OUTPUT_DIR,
      WIKI_ECON_GENERATOR_DIR: GENERATOR_DIR,
    },
  });
  manifestCache = JSON.parse(output);
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
  const manifest = {
    rows: 0,
    analyticalPaths: [],
    warehousePaths: [],
  };
  for (const line of fs.readFileSync(markerPath, "utf8").split(/\r?\n/)) {
    const idx = line.indexOf("=");
    if (idx === -1) continue;
    const key = line.slice(0, idx);
    const value = line.slice(idx + 1);
    if (key === "rows") manifest.rows = Number.parseInt(value, 10) || 0;
    if (key === "analytical_path") manifest.analyticalPaths.push(path.join(DATA_DIR, value));
    if (key === "warehouse_path") manifest.warehousePaths.push(path.join(DATA_DIR, value));
  }
  if (manifest.rows === 0) return true;
  if (manifest.analyticalPaths.length === 0 || manifest.warehousePaths.length === 0) return false;
  return [...manifest.analyticalPaths, ...manifest.warehousePaths].every((entry) => fs.existsSync(entry));
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
  const tmpFiles = [
    ...walkFiles(analyticalDir, (entry) => entry.endsWith(".tmp")),
    ...walkFiles(warehouseDir, (entry) => entry.endsWith(".tmp")),
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

function trackStageFromChunk(chunk) {
  if (!currentJob) return;

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
}

function getProgress() {
  if (!currentJob) return null;

  const wiki = currentJob.wiki ?? null;
  const action = currentJob.action ?? null;
  if (!wiki && action !== "merge" && action !== "cancel") return null;

  const manifest = refreshManifestSafely() || { wikis: {}, merged: [] };
  const wikiStatus = wiki ? manifest.wikis?.[wiki] ?? null : null;
  const stage = currentJob.stage || (action === "run" ? "fetch" : action);
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
  return { wiki, stage, done, total, pct, detail };
}

function matchApiPath(pathname) {
  if (pathname.startsWith(`${LEGACY_API_PREFIX}/`)) return pathname.slice(LEGACY_API_PREFIX.length + 1);
  if (pathname.startsWith(`${PROXY_API_PREFIX}/`)) return pathname.slice(PROXY_API_PREFIX.length + 1);
  return null;
}

function buildStatusPayload(req, session) {
  const progress = getProgress();
  const effectiveJob = currentJob
    ? {
        command: currentJob.command,
        action: currentJob.action,
        wiki: currentJob.wiki,
        stage: currentJob.stage,
        running: true,
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
    supportedWikis: loadSupportedWikipedias(),
    suggestedVersion: suggestedSnapshotVersion(),
    adminEnabled: ADMIN_ENABLED,
    adminPort: PORT,
    auth: authStatus(session, req),
  };
}

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
      : "This admin surface is protected. Sign in with the configured OpenID Connect provider using an email address from the authorized allowlist.";
    writeHtml(res, 200, renderLoginPage(req, message, url.searchParams.get("next")));
    return;
  }

  if (AUTH_ENABLED && req.method === "GET" && url.pathname === ADMIN_OAUTH_START_PATH) {
    await startOidcLogin(req, res, url.searchParams.get("next"));
    return;
  }

  if (AUTH_ENABLED && req.method === "GET" && url.pathname === ADMIN_OAUTH_CALLBACK_PATH) {
    await finishOidcLogin(req, res, url);
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

      if (currentJob) {
        if (action === "cancel") {
          currentJob.cancelRequested = true;
          currentJob.proc.kill("SIGTERM");
          appendJobLog(`\n[cancel requested for pid ${currentJob.pid}]`);
          writeJson(res, 200, { started: false, cancelled: true, pid: currentJob.pid });
          return;
        }
        writeJson(res, 409, { error: "A job is already running", command: currentJob.command });
        return;
      }

      const wiki = (params.wiki || "").replace(/[^a-z0-9_]/gi, "");
      const version = normalizeVersion(params.version);
      if (version && !isValidVersion(version)) {
        writeJson(res, 400, { error: "Invalid version. Use YYYY-MM." });
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
        case "run":
          commandSpec = wiki
            ? {
                program: resolveRunner().program,
                args: [
                  ...resolveRunner().args,
                  "--data-dir", DATA_DIR,
                  "--output-dir", OUTPUT_DIR,
                  action,
                  wiki,
                  ...(version && (action === "fetch" || action === "run") ? ["--version", version] : []),
                ],
                label: `${resolveRunner().label} --data-dir ${DATA_DIR} --output-dir ${OUTPUT_DIR} ${action} ${wiki}${version && (action === "fetch" || action === "run") ? ` --version ${version}` : ""}`,
              }
            : null;
          break;
        case "merge":
          commandSpec = {
            program: resolveRunner().program,
            args: [...resolveRunner().args, "--data-dir", DATA_DIR, "--output-dir", OUTPUT_DIR, "merge"],
            label: `${resolveRunner().label} --data-dir ${DATA_DIR} --output-dir ${OUTPUT_DIR} merge`,
          };
          break;
        case "patrol-fetch":
          commandSpec = wiki
            ? {
                program: resolveRunner().program,
                args: [...resolveRunner().args, "--data-dir", DATA_DIR, "--output-dir", OUTPUT_DIR, "patrol-fetch", wiki],
                label: `${resolveRunner().label} --data-dir ${DATA_DIR} --output-dir ${OUTPUT_DIR} patrol-fetch ${wiki}`,
              }
            : null;
          break;
        case "patrol-compute":
          commandSpec = wiki
            ? {
                program: resolveRunner().program,
                args: [...resolveRunner().args, "--data-dir", DATA_DIR, "--output-dir", OUTPUT_DIR, "patrol-compute", wiki],
                label: `${resolveRunner().label} --data-dir ${DATA_DIR} --output-dir ${OUTPUT_DIR} patrol-compute ${wiki}`,
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
          WIKI_ECON_SITE_DIST_DIR: SITE_DIST_DIR,
        },
      });
      currentJob = {
        command: commandSpec.label,
        pid: proc.pid,
        proc,
        action,
        wiki: wiki || null,
        stage: action === "run" ? "fetch" : action.replace("-", "_"),
        expectedTotal: null,
        cancelRequested: false,
      };

      proc.stdout.on("data", (data) => appendJobLog(data.toString()));
      proc.stderr.on("data", (data) => appendJobLog(data.toString()));
      proc.on("close", (code, signal) => {
        const cancelled = currentJob?.cancelRequested && signal === "SIGTERM";
        const renderedExit = cancelled ? "cancelled" : code;
        jobLog.push(`\n[exited with code ${renderedExit}]`);
        jobExitCode = cancelled ? 130 : code;
        const completedJob = {
          command: commandSpec.label,
          action,
          wiki: wiki || null,
          stage: currentJob?.stage ?? action.replace("-", "_"),
          exitCode: cancelled ? 130 : code,
          cancelled,
          running: false,
          log: [...jobLog],
          finishedAt: new Date().toISOString(),
        };
        lastJob = completedJob;
        if (completedJob.wiki) {
          lastWikiJobs.set(completedJob.wiki, completedJob);
        } else {
          lastGlobalJob = completedJob;
        }
        currentJob = null;
        refreshManifestSafely(true);
      });
      proc.on("error", (error) => {
        jobLog.push(`\n[failed to start: ${error.message}]`);
        jobExitCode = 1;
        const failedJob = {
          command: commandSpec.label,
          action,
          wiki: wiki || null,
          stage: action.replace("-", "_"),
          exitCode: 1,
          running: false,
          log: [...jobLog],
          finishedAt: new Date().toISOString(),
        };
        lastJob = failedJob;
        if (failedJob.wiki) {
          lastWikiJobs.set(failedJob.wiki, failedJob);
        } else {
          lastGlobalJob = failedJob;
        }
        currentJob = null;
        refreshManifestSafely(true);
      });

      writeJson(res, 200, { started: true, command: commandSpec.label, pid: proc.pid });
      console.log(`[admin] started: ${commandSpec.label} (pid ${proc.pid})`);
    });
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
  server.listen(PORT, "127.0.0.1", () => {
    const runner = resolveRunner();
    console.log(`Admin server listening on http://127.0.0.1:${PORT}`);
    console.log(`Runner: ${runner.label}`);
    console.log(`Working dir: ${ROOT}`);
    console.log(`Data dir: ${DATA_DIR}`);
    console.log(`Output dir: ${OUTPUT_DIR}`);
    console.log(`Generator dir: ${GENERATOR_DIR}`);
    console.log(`Site dist dir: ${SITE_DIST_DIR}`);
    console.log(`Allowed origins: ${Array.from(ALLOWED_ORIGINS).join(", ")}`);
    console.log(`Auth mode: ${ADMIN_AUTH_MODE}`);
    if (AUTH_ENABLED) {
      console.log(`Authorized admin emails: ${ADMIN_ALLOWED_EMAILS.size}`);
      console.log(`OIDC issuer: ${ADMIN_OIDC_ISSUER}`);
    }
  });
  return server;
}

if (require.main === module) {
  startServer();
}

module.exports = {
  ADMIN_PAGE_PATH,
  PROXY_API_PREFIX,
  createServer,
  handleRequest,
  startServer,
};
