#!/usr/bin/env node

const crypto = require("crypto");

function normalizeEmail(value) {
  return typeof value === "string" ? value.trim().toLowerCase() : "";
}

function parseAllowedEmails(value) {
  const allowed = new Set();
  for (const entry of String(value || "").split(/[\n,;]+/)) {
    const email = normalizeEmail(entry);
    if (email) allowed.add(email);
  }
  return allowed;
}

function parseCookies(header) {
  const cookies = {};
  for (const entry of String(header || "").split(";")) {
    const trimmed = entry.trim();
    if (!trimmed) continue;
    const separator = trimmed.indexOf("=");
    if (separator === -1) continue;
    const name = trimmed.slice(0, separator).trim();
    const rawValue = trimmed.slice(separator + 1).trim();
    if (!name) continue;
    try {
      cookies[name] = decodeURIComponent(rawValue);
    } catch {
      cookies[name] = rawValue;
    }
  }
  return cookies;
}

function serializeCookie(name, value, options = {}) {
  const parts = [`${name}=${encodeURIComponent(value)}`];
  if (options.maxAge != null) parts.push(`Max-Age=${Math.max(0, Math.trunc(options.maxAge))}`);
  if (options.expires) parts.push(`Expires=${options.expires.toUTCString()}`);
  parts.push(`Path=${options.path || "/"}`);
  if (options.httpOnly !== false) parts.push("HttpOnly");
  if (options.secure) parts.push("Secure");
  if (options.sameSite) parts.push(`SameSite=${options.sameSite}`);
  if (options.domain) parts.push(`Domain=${options.domain}`);
  return parts.join("; ");
}

function base64urlEncode(input) {
  return Buffer.from(input).toString("base64url");
}

function hmacSignature(secret, body) {
  return crypto.createHmac("sha256", secret).update(body).digest("base64url");
}

function signJsonToken(payload, secret) {
  const body = base64urlEncode(JSON.stringify(payload));
  return `${body}.${hmacSignature(secret, body)}`;
}

function verifyJsonToken(token, secret) {
  if (typeof token !== "string" || !token.includes(".")) return null;
  const [body, signature, ...rest] = token.split(".");
  if (!body || !signature || rest.length > 0) return null;
  const expected = hmacSignature(secret, body);
  const actualBuffer = Buffer.from(signature);
  const expectedBuffer = Buffer.from(expected);
  if (actualBuffer.length !== expectedBuffer.length) return null;
  if (!crypto.timingSafeEqual(actualBuffer, expectedBuffer)) return null;
  try {
    return JSON.parse(Buffer.from(body, "base64url").toString("utf8"));
  } catch {
    return null;
  }
}

function randomToken(size = 32) {
  return crypto.randomBytes(size).toString("base64url");
}

function buildAuthorizeUrl({
  authorizationEndpoint,
  clientId,
  redirectUri,
  scopes,
  state,
  nonce,
}) {
  const url = new URL(authorizationEndpoint);
  url.searchParams.set("client_id", clientId);
  url.searchParams.set("redirect_uri", redirectUri);
  url.searchParams.set("response_type", "code");
  url.searchParams.set("scope", scopes);
  url.searchParams.set("state", state);
  url.searchParams.set("nonce", nonce);
  return url.toString();
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function sanitizeNextPath(value, fallback = "/admin") {
  if (typeof value !== "string" || !value.trim()) return fallback;
  try {
    const parsed = new URL(value, "https://example.invalid");
    if (parsed.origin !== "https://example.invalid") return fallback;
    const next = `${parsed.pathname}${parsed.search}${parsed.hash}`;
    return next.startsWith("/") ? next : fallback;
  } catch {
    return fallback;
  }
}

module.exports = {
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
};
