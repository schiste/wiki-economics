#!/usr/bin/env node

const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { Readable, Writable } = require("node:stream");
const test = require("node:test");

const SERVER_MODULE_PATH = require.resolve("./admin-server.cjs");
const { signJsonToken } = require("./admin-auth.cjs");

const LOCAL_ENV = {
  WIKI_ECON_ENV: "local",
  WIKI_ECON_ADMIN_ENABLED: "1",
  WIKI_ECON_ADMIN_AUTH_MODE: "none",
};

const HOSTED_ENV = {
  WIKI_ECON_ENV: "production",
  WIKI_ECON_ADMIN_ENABLED: "1",
  WIKI_ECON_ADMIN_AUTH_MODE: "oidc",
  WIKI_ECON_ADMIN_OIDC_ISSUER: "https://accounts.example.test",
  WIKI_ECON_ADMIN_OIDC_CLIENT_ID: "wiki-econ-test-client",
  WIKI_ECON_ADMIN_OIDC_CLIENT_SECRET: "wiki-econ-test-secret",
  WIKI_ECON_ADMIN_ALLOWED_EMAILS: "alice@example.org",
  WIKI_ECON_ADMIN_SESSION_SECRET: "0123456789abcdef0123456789abcdef",
  WIKI_ECON_ADMIN_SECURE_COOKIES: "0",
};

function loadAdminServer(envOverrides) {
  const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "wiki-econ-admin-test-"));
  const dataDir = path.join(tempRoot, "data");
  const outputDir = path.join(tempRoot, "output");
  const distDir = path.join(tempRoot, "dist");
  fs.mkdirSync(dataDir, { recursive: true });
  fs.mkdirSync(outputDir, { recursive: true });
  fs.mkdirSync(distDir, { recursive: true });
  fs.writeFileSync(
    path.join(distDir, "admin.html"),
    "<!doctype html><html><body><h1>Admin Test Page</h1></body></html>",
    "utf8",
  );

  const env = {
    WIKI_ECON_DATA_DIR: dataDir,
    WIKI_ECON_OUTPUT_DIR: outputDir,
    WIKI_ECON_SITE_DIST_DIR: distDir,
    ...envOverrides,
  };

  const previous = new Map();
  for (const [key, value] of Object.entries(env)) {
    previous.set(key, process.env[key]);
    process.env[key] = value;
  }

  delete require.cache[SERVER_MODULE_PATH];
  const module = require("./admin-server.cjs");

  for (const key of Object.keys(env)) {
    const oldValue = previous.get(key);
    if (oldValue == null) delete process.env[key];
    else process.env[key] = oldValue;
  }

  return { module, tempRoot };
}

async function startServer(t, envOverrides) {
  const { module, tempRoot } = loadAdminServer(envOverrides);
  t.after(() => {
    delete require.cache[SERVER_MODULE_PATH];
    fs.rmSync(tempRoot, { recursive: true, force: true });
  });
  return {
    module,
    host: "127.0.0.1:3443",
  };
}

function sessionCookie(secret, email = "alice@example.org") {
  const token = signJsonToken(
    {
      email,
      name: "Alice Example",
      sub: "alice-example",
      provider: "https://accounts.example.test",
      exp: Math.floor(Date.now() / 1000) + 60 * 60,
    },
    secret,
  );
  return `wiki_econ_admin_session=${encodeURIComponent(token)}`;
}

class MockRequest extends Readable {
  constructor({ method, url, headers, body }) {
    super();
    this.method = method;
    this.url = url;
    this.headers = headers;
    this._body = body ? Buffer.from(body) : null;
    this._sent = false;
  }

  _read() {
    if (this._sent) {
      this.push(null);
      return;
    }
    this._sent = true;
    if (this._body) this.push(this._body);
    this.push(null);
  }
}

class MockResponse extends Writable {
  constructor() {
    super();
    this.statusCode = 200;
    this.headers = new Map();
    this.headersSent = false;
    this.chunks = [];
  }

  _write(chunk, _encoding, callback) {
    this.chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
    callback();
  }

  setHeader(name, value) {
    this.headers.set(String(name).toLowerCase(), value);
  }

  getHeader(name) {
    return this.headers.get(String(name).toLowerCase());
  }

  writeHead(statusCode, headers = {}) {
    this.statusCode = statusCode;
    this.headersSent = true;
    for (const [name, value] of Object.entries(headers)) {
      this.setHeader(name, value);
    }
    return this;
  }

  end(chunk, encoding, callback) {
    if (chunk != null) {
      this.write(chunk, encoding);
    }
    return super.end(callback);
  }

  text() {
    return Buffer.concat(this.chunks).toString("utf8");
  }
}

async function invoke(module, { method = "GET", url = "/", headers = {}, body = "" }) {
  const request = new MockRequest({ method, url, headers, body });
  const response = new MockResponse();
  await module.handleRequest(request, response);
  if (!response.writableFinished) {
    await new Promise((resolve) => response.once("finish", resolve));
  }
  return response;
}

test("local mode exposes the legacy /api/status endpoint without auth", async (t) => {
  const { module, host } = await startServer(t, LOCAL_ENV);
  const response = await invoke(module, {
    url: "/api/status",
    headers: { host },
  });
  assert.equal(response.statusCode, 200);
  const body = JSON.parse(response.text());
  assert.equal(body.auth.enabled, false);
  assert.equal(body.auth.authenticated, true);
  assert.equal(body.adminEnabled, true);
});

test("hosted mode redirects /admin to the login page when no session is present", async (t) => {
  const { module, host } = await startServer(t, HOSTED_ENV);
  const response = await invoke(module, {
    url: "/admin",
    headers: { host },
  });
  assert.equal(response.statusCode, 302);
  assert.equal(response.getHeader("location"), "/admin/login?next=%2Fadmin");
});

test("hosted mode rejects unauthenticated /admin-api/status requests", async (t) => {
  const { module, host } = await startServer(t, HOSTED_ENV);
  const response = await invoke(module, {
    url: "/admin-api/status",
    headers: { host },
  });
  assert.equal(response.statusCode, 401);
  const body = JSON.parse(response.text());
  assert.equal(body.auth.enabled, true);
  assert.equal(body.auth.authenticated, false);
  assert.equal(body.auth.loginUrl, "/admin/login?next=%2Fadmin");
});

test("hosted mode serves /admin and /admin-api/status when a valid session cookie is present", async (t) => {
  const { module, host } = await startServer(t, HOSTED_ENV);
  const headers = {
    host,
    cookie: sessionCookie(HOSTED_ENV.WIKI_ECON_ADMIN_SESSION_SECRET),
  };

  const statusResponse = await invoke(module, {
    url: "/admin-api/status",
    headers,
  });
  assert.equal(statusResponse.statusCode, 200);
  const statusBody = JSON.parse(statusResponse.text());
  assert.equal(statusBody.auth.authenticated, true);
  assert.equal(statusBody.auth.user.email, "alice@example.org");

  const pageResponse = await invoke(module, {
    url: "/admin",
    headers,
  });
  assert.equal(pageResponse.statusCode, 200);
  const pageHtml = pageResponse.text();
  assert.match(pageHtml, /Admin Test Page/);
});

test("hosted mode enforces same-origin checks on mutating admin API requests", async (t) => {
  const { module, host } = await startServer(t, HOSTED_ENV);
  const cookie = sessionCookie(HOSTED_ENV.WIKI_ECON_ADMIN_SESSION_SECRET);

  const rejected = await invoke(module, {
    method: "POST",
    url: "/admin-api/cancel",
    headers: {
      host,
      cookie,
      "content-type": "application/json",
    },
    body: "{}",
  });
  assert.equal(rejected.statusCode, 403);
  assert.equal(JSON.parse(rejected.text()).error, "Untrusted origin");

  const accepted = await invoke(module, {
    method: "POST",
    url: "/admin-api/cancel",
    headers: {
      host,
      cookie,
      origin: `https://${host}`,
      "content-type": "application/json",
    },
    body: "{}",
  });
  assert.equal(accepted.statusCode, 409);
  assert.equal(JSON.parse(accepted.text()).error, "No job is currently running");
});
