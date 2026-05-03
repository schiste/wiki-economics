#!/usr/bin/env node

const test = require("node:test");
const assert = require("node:assert/strict");

const {
  buildAuthorizeUrl,
  parseAllowedEmails,
  parseCookies,
  sanitizeNextPath,
  serializeCookie,
  signJsonToken,
  verifyJsonToken,
} = require("./admin-auth.cjs");

test("parseAllowedEmails normalizes common separator styles", () => {
  const allowed = parseAllowedEmails("Alice@example.org, bob@example.org\ncarol@example.org ; DAVE@example.org");
  assert.deepEqual(
    Array.from(allowed).sort(),
    [
      "alice@example.org",
      "bob@example.org",
      "carol@example.org",
      "dave@example.org",
    ],
  );
});

test("parseCookies decodes percent-escaped values", () => {
  assert.deepEqual(parseCookies("a=1; b=hello%20world; c=plain"), {
    a: "1",
    b: "hello world",
    c: "plain",
  });
});

test("serializeCookie emits security attributes", () => {
  const cookie = serializeCookie("session", "token", {
    httpOnly: true,
    secure: true,
    sameSite: "Lax",
    path: "/",
    maxAge: 3600,
  });
  assert.match(cookie, /^session=token; Max-Age=3600; Path=\/; HttpOnly; Secure; SameSite=Lax$/);
});

test("signed JSON tokens round-trip and detect tampering", () => {
  const secret = "0123456789abcdef0123456789abcdef";
  const token = signJsonToken({ email: "alice@example.org", exp: 42 }, secret);
  assert.deepEqual(verifyJsonToken(token, secret), { email: "alice@example.org", exp: 42 });

  const [body, signature] = token.split(".");
  const tampered = `${body}.tampered${signature.slice(8)}`;
  assert.equal(verifyJsonToken(tampered, secret), null);
});

test("buildAuthorizeUrl includes the core OAuth 2 query parameters", () => {
  const url = new URL(buildAuthorizeUrl({
    authorizationEndpoint: "https://issuer.example/authorize",
    clientId: "client-id",
    redirectUri: "https://tool.example/admin/oauth/callback",
    scopes: "openid email profile",
    state: "state-token",
    nonce: "nonce-token",
  }));
  assert.equal(url.origin, "https://issuer.example");
  assert.equal(url.pathname, "/authorize");
  assert.equal(url.searchParams.get("client_id"), "client-id");
  assert.equal(url.searchParams.get("redirect_uri"), "https://tool.example/admin/oauth/callback");
  assert.equal(url.searchParams.get("response_type"), "code");
  assert.equal(url.searchParams.get("scope"), "openid email profile");
  assert.equal(url.searchParams.get("state"), "state-token");
  assert.equal(url.searchParams.get("nonce"), "nonce-token");
});

test("sanitizeNextPath keeps local paths and rejects absolute URLs", () => {
  assert.equal(sanitizeNextPath("/admin?from=login"), "/admin?from=login");
  assert.equal(sanitizeNextPath("https://evil.example/phish"), "/admin");
  assert.equal(sanitizeNextPath("javascript:alert(1)"), "/admin");
});
