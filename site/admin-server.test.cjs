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
  WIKI_ECON_ADMIN_AUTH_MODE: "mediawiki",
  WIKI_ECON_ADMIN_MEDIAWIKI_HOST: "https://meta.wikimedia.example.test",
  WIKI_ECON_ADMIN_MEDIAWIKI_CLIENT_ID: "wiki-econ-test-client",
  WIKI_ECON_ADMIN_MEDIAWIKI_CLIENT_SECRET: "wiki-econ-test-secret",
  WIKI_ECON_ADMIN_ALLOWED_USERNAMES: "Alice",
  WIKI_ECON_ADMIN_SESSION_SECRET: "0123456789abcdef0123456789abcdef",
  WIKI_ECON_ADMIN_SECURE_COOKIES: "0",
};

function loadAdminServer(envOverrides, wikiLifecycle = { schema_version: 1, wikis: {} }) {
  const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "wiki-econ-admin-test-"));
  const dataDir = path.join(tempRoot, "data");
  const outputDir = path.join(tempRoot, "output");
  const distDir = path.join(tempRoot, "dist");
  fs.mkdirSync(dataDir, { recursive: true });
  fs.mkdirSync(outputDir, { recursive: true });
  fs.mkdirSync(distDir, { recursive: true });
  const lifecyclePath = path.join(tempRoot, "wiki-lifecycle.json");
  fs.writeFileSync(lifecyclePath, JSON.stringify(wikiLifecycle), "utf8");
  fs.writeFileSync(
    path.join(distDir, "admin.html"),
    "<!doctype html><html><body><h1>Admin Test Page</h1></body></html>",
    "utf8",
  );

  const env = {
    WIKI_ECON_DATA_DIR: dataDir,
    WIKI_ECON_OUTPUT_DIR: outputDir,
    WIKI_ECON_SITE_DIST_DIR: distDir,
    WIKI_ECON_WIKI_LIFECYCLE_FILE: lifecyclePath,
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

async function startServer(t, envOverrides, wikiLifecycle) {
  const { module, tempRoot } = loadAdminServer(envOverrides, wikiLifecycle);
  t.after(() => {
    delete require.cache[SERVER_MODULE_PATH];
    fs.rmSync(tempRoot, { recursive: true, force: true });
  });
  return {
    module,
    host: "127.0.0.1:3443",
    distDir: path.join(tempRoot, "dist"),
    outputDir: path.join(tempRoot, "output"),
  };
}

function sessionCookie(secret, username = "Alice") {
  const token = signJsonToken(
    {
      username,
      name: "Alice Example",
      sub: "12345",
      provider: "https://meta.wikimedia.example.test",
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
  assert.equal(statusBody.auth.user.username, "Alice");

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

test("static fallback serves site assets from SITE_DIST_DIR with per-path cache-control", async (t) => {
  const { module, host, distDir } = await startServer(t, LOCAL_ENV);
  fs.writeFileSync(path.join(distDir, "index.html"), "<!doctype html><h1>Home</h1>", "utf8");
  fs.mkdirSync(path.join(distDir, "gdp"), { recursive: true });
  fs.writeFileSync(path.join(distDir, "gdp.html"), "<!doctype html><h1>GDP</h1>", "utf8");
  fs.mkdirSync(path.join(distDir, "_observablehq"), { recursive: true });
  fs.writeFileSync(path.join(distDir, "_observablehq", "client.js"), "console.log('ok')", "utf8");
  fs.mkdirSync(path.join(distDir, "_file"), { recursive: true });
  fs.writeFileSync(path.join(distDir, "_file", "data.csv"), "a,b\n1,2\n", "utf8");

  const index = await invoke(module, { url: "/", headers: { host } });
  assert.equal(index.statusCode, 200);
  assert.match(index.text(), /Home/);
  assert.equal(index.getHeader("cache-control"), undefined);

  // $uri.html fallback, mirroring nginx's try_files $uri $uri.html $uri/ =404
  const extensionless = await invoke(module, { url: "/gdp", headers: { host } });
  assert.equal(extensionless.statusCode, 200);
  assert.match(extensionless.text(), /GDP/);

  const observablehq = await invoke(module, { url: "/_observablehq/client.js", headers: { host } });
  assert.equal(observablehq.statusCode, 200);
  assert.equal(observablehq.getHeader("content-type"), "text/javascript; charset=utf-8");
  assert.equal(observablehq.getHeader("cache-control"), "public, max-age=3600");

  const file = await invoke(module, { url: "/_file/data.csv", headers: { host } });
  assert.equal(file.statusCode, 200);
  assert.equal(file.getHeader("cache-control"), "public, max-age=600");

  const missing = await invoke(module, { url: "/does-not-exist", headers: { host } });
  assert.equal(missing.statusCode, 404);
});

test("static fallback refuses to escape SITE_DIST_DIR via path traversal", async (t) => {
  const { module, host, distDir } = await startServer(t, LOCAL_ENV);
  fs.writeFileSync(path.join(path.dirname(distDir), "secret.txt"), "top secret", "utf8");

  const response = await invoke(module, {
    url: "/..%2Fsecret.txt",
    headers: { host },
  });
  assert.equal(response.statusCode, 404);
});

test("status distinguishes scheduled refresh wikis from paused published wikis", async (t) => {
  const { module, host, outputDir } = await startServer(t, {
    ...LOCAL_ENV,
    WIKI_ECON_ENABLED_WIKIS: "nlwiki",
  }, {
    schema_version: 1,
    wikis: {
      frwiki: {
        publication: "published",
        refresh: "paused",
        provenance: "local-import",
        imported_cutoff: "2026-03",
      },
      nlwiki: {
        publication: "published",
        refresh: "scheduled",
        provenance: "toolforge",
        freshness_sla_days: 10,
      },
    },
  });
  fs.mkdirSync(path.join(outputDir, "nlwiki"), { recursive: true });
  fs.writeFileSync(path.join(outputDir, "nlwiki", "gdp.parquet"), "published", "utf8");
  // resolveRunner() reads WIKI_ECON_BIN from process.env at request time
  // (not module-load time, unlike DATA_DIR/OUTPUT_DIR/ENABLED_WIKIS), so it
  // must be set around the request rather than via startServer()'s
  // load-then-restore env handling.
  const previousBin = process.env.WIKI_ECON_BIN;
  process.env.WIKI_ECON_BIN = "/usr/local/bin/wiki-econ";
  t.after(() => {
    if (previousBin == null) delete process.env.WIKI_ECON_BIN;
    else process.env.WIKI_ECON_BIN = previousBin;
  });

  const response = await invoke(module, { url: "/api/status", headers: { host } });
  assert.equal(response.statusCode, 200);
  const body = JSON.parse(response.text());
  assert.equal(body.runner.mode, "bin");
  assert.match(body.runner.label, /wiki-econ/);
  assert.deepEqual(body.enabledWikis, ["nlwiki"]);
  assert.deepEqual(body.refreshWikis, ["nlwiki"]);
  assert.deepEqual(body.publishedWikis, ["frwiki", "nlwiki"]);
  assert.equal(body.wikiLifecycle.wikis.frwiki.refresh, "paused");
  assert.equal(body.wikiStates.frwiki.freshness, "paused");
  assert.equal(body.wikiStates.frwiki.imported_cutoff, "2026-03");
  assert.equal(body.wikiStates.nlwiki.freshness, "current");
  assert.match(body.wikiStates.nlwiki.last_published_at, /^\d{4}-\d{2}-\d{2}T/);
});

test("status falls back to the cargo runner and no enabled wikis when unset", async (t) => {
  const { module, host } = await startServer(t, LOCAL_ENV);
  const response = await invoke(module, { url: "/api/status", headers: { host } });
  const body = JSON.parse(response.text());
  assert.equal(body.runner.mode, "cargo");
  assert.match(body.runner.label, /cargo run/);
  assert.deepEqual(body.enabledWikis, []);
});

test("scheduledRefresh tolerates a missing status file", async (t) => {
  const { module, host } = await startServer(t, LOCAL_ENV);
  const response = await invoke(module, { url: "/api/status", headers: { host } });
  const body = JSON.parse(response.text());
  assert.equal(body.scheduledRefresh.last, null);
  assert.deepEqual(body.scheduledRefresh.history, []);
  assert.equal(body.scheduledRefresh.schedule, null);
});

test("scheduledRefresh tolerates malformed JSON without throwing", async (t) => {
  const { module, host, outputDir } = await startServer(t, LOCAL_ENV);
  fs.writeFileSync(path.join(outputDir, ".refresh-status.json"), "{not valid json", "utf8");
  fs.writeFileSync(
    path.join(outputDir, ".refresh-history.jsonl"),
    'not json\n{"startedAt":"2026-08-16T03:00:00Z","finishedAt":"2026-08-16T03:12:00Z","exitCode":0,"wikis":["nlwiki"],"durationSecs":720}\n',
    "utf8",
  );
  const response = await invoke(module, { url: "/api/status", headers: { host } });
  assert.equal(response.statusCode, 200);
  const body = JSON.parse(response.text());
  assert.equal(body.scheduledRefresh.last, null);
  assert.equal(body.scheduledRefresh.history.length, 1);
  assert.equal(body.scheduledRefresh.history[0].exitCode, 0);
});

test("scheduledRefresh surfaces the last run and configured schedule", async (t) => {
  const { module, host, outputDir } = await startServer(t, {
    ...LOCAL_ENV,
    WIKI_ECON_REFRESH_SCHEDULE: "0 3 * * 0",
  });
  const entry = {
    startedAt: "2026-08-16T03:00:00Z",
    finishedAt: "2026-08-16T03:12:00Z",
    exitCode: 0,
    wikis: ["nlwiki"],
    durationSecs: 720,
    memoryPeakBytes: 3_221_225_472,
    memoryLimitBytes: 6_442_450_944,
  };
  fs.writeFileSync(path.join(outputDir, ".refresh-status.json"), JSON.stringify(entry), "utf8");
  fs.writeFileSync(path.join(outputDir, ".refresh-history.jsonl"), `${JSON.stringify(entry)}\n`, "utf8");

  const response = await invoke(module, { url: "/api/status", headers: { host } });
  const body = JSON.parse(response.text());
  assert.equal(body.scheduledRefresh.schedule, "0 3 * * 0");
  assert.deepEqual(body.scheduledRefresh.last, entry);
  assert.equal(body.scheduledRefresh.history.length, 1);
  assert.deepEqual(body.scheduledRefresh.history[0], entry);
});
