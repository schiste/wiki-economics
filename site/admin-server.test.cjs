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

function loadAdminServer(envOverrides, wikiLifecycle = {
  schema_version: 1,
  publication_contract: { datasets: {} },
  wikis: {},
}, setup = null) {
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
  if (setup) setup({ tempRoot, dataDir, outputDir, distDir });

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

async function startServer(t, envOverrides, wikiLifecycle, setup) {
  const { module, tempRoot } = loadAdminServer(envOverrides, wikiLifecycle, setup);
  t.after(() => {
    delete require.cache[SERVER_MODULE_PATH];
    fs.rmSync(tempRoot, { recursive: true, force: true });
  });
  return {
    module,
    host: "127.0.0.1:3443",
    tempRoot,
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
  assert.equal("suggestedVersion" in body, false, "calendar months must not masquerade as completed snapshots");
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

test("homepage routes serve the published portfolio without redirects", async (t) => {
  const { module, host, distDir } = await startServer(t, LOCAL_ENV);
  fs.writeFileSync(path.join(distDir, "index.html"), "<!doctype html><h1>Portfolio home</h1>", "utf8");

  for (const url of ["/", "/index", "/index.html"]) {
    const response = await invoke(module, { url, headers: { host } });
    assert.equal(response.statusCode, 200);
    assert.equal(response.getHeader("location"), undefined);
    assert.match(response.text(), /Portfolio home/);
  }

  const head = await invoke(module, { method: "HEAD", url: "/", headers: { host } });
  assert.equal(head.statusCode, 200);
  assert.equal(head.text(), "");
});

test("static fallback serves site assets from SITE_DIST_DIR with per-path cache-control", async (t) => {
  const { module, host, distDir } = await startServer(t, LOCAL_ENV);
  fs.mkdirSync(path.join(distDir, "gdp"), { recursive: true });
  fs.writeFileSync(path.join(distDir, "gdp.html"), "<!doctype html><h1>GDP</h1>", "utf8");
  fs.mkdirSync(path.join(distDir, "_observablehq"), { recursive: true });
  fs.writeFileSync(path.join(distDir, "_observablehq", "client.js"), "console.log('ok')", "utf8");
  fs.mkdirSync(path.join(distDir, "_file"), { recursive: true });
  fs.writeFileSync(path.join(distDir, "_file", "data.csv"), "a,b\n1,2\n", "utf8");

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
    WIKI_ECON_BIN: "/usr/local/bin/wiki-econ",
  }, {
    schema_version: 1,
    publication_contract: { datasets: {} },
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

test("scheduledRefresh surfaces an in-progress schema-v2 heartbeat", async (t) => {
  const {module, host, outputDir} = await startServer(t, LOCAL_ENV);
  const live = {
    schemaVersion: 2,
    state: "running",
    runId: "live-run",
    startedAt: "2026-08-22T03:00:00Z",
    finishedAt: null,
    heartbeatAt: "2026-08-22T03:05:00Z",
    exitCode: null,
    currentStage: "compute",
    currentWiki: "nlwiki",
    selectedSnapshot: "2026-07",
  };
  fs.writeFileSync(path.join(outputDir, ".refresh-status.json"), JSON.stringify(live), "utf8");

  const response = await invoke(module, {url: "/api/status", headers: {host}});
  const body = JSON.parse(response.text());
  assert.deepEqual(body.scheduledRefresh.last, live);
  assert.deepEqual(body.scheduledRefresh.history, []);
});

test("status keeps an untracked snapshot plan visible after its process has stopped", async (t) => {
  const { module, host } = await startServer(t, LOCAL_ENV, undefined, ({ dataDir }) => {
    const planDir = path.join(dataDir, "snapshots", "dewiki", "2026-08");
    fs.mkdirSync(planDir, { recursive: true });
    fs.writeFileSync(path.join(planDir, "source-plan.json"), JSON.stringify({
      schema_version: 1,
      wiki: "dewiki",
      snapshot: "2026-08",
      layout: "yearly",
      sources: [{ source_id: "dewiki-2001" }],
    }));
  });
  const response = await invoke(module, { url: "/api/status", headers: { host } });
  const body = JSON.parse(response.text());
  assert.deepEqual(body.snapshotPlans.map(({ wiki, snapshot, sourceCount }) => ({ wiki, snapshot, sourceCount })), [
    { wiki: "dewiki", snapshot: "2026-08", sourceCount: 1 },
  ]);
});

test("status restores an interrupted admin run from the durable ledger", async (t) => {
  const { module, host } = await startServer(t, LOCAL_ENV, undefined, ({ outputDir }) => {
    const adminDir = path.join(outputDir, "_admin");
    fs.mkdirSync(adminDir, { recursive: true });
    fs.writeFileSync(path.join(adminDir, "current-job.json"), JSON.stringify({
      schemaVersion: 1,
      runId: "admin-dewiki-test",
      command: "wiki-econ run dewiki",
      action: "run",
      wiki: "dewiki",
      stage: "ingest",
      state: "running",
      running: true,
      pid: 99,
      startedAt: "2026-09-01T10:00:00.000Z",
      updatedAt: "2026-09-01T10:01:00.000Z",
      log: ["started\n"],
    }));
  });
  const response = await invoke(module, { url: "/api/status", headers: { host } });
  const body = JSON.parse(response.text());
  assert.equal(body.wikiJobs.dewiki.state, "interrupted");
  assert.equal(body.wikiJobs.dewiki.interrupted, true);
  assert.equal(body.adminRuns.recent[0].runId, "admin-dewiki-test");
  assert.equal(body.adminRuns.active, null);
});

test("status classifies fleet leases from their heartbeat", async (t) => {
  const nowUnix = Math.floor(Date.now() / 1000);
  const { module, host } = await startServer(t, LOCAL_ENV, undefined, ({ outputDir }) => {
    const leaseDir = path.join(outputDir, "_fleet", "leases", "nlwiki");
    fs.mkdirSync(leaseDir, { recursive: true });
    fs.writeFileSync(path.join(leaseDir, "owner.json"), JSON.stringify({
      schema_version: 1,
      worker_id: "medium-1",
      lease_id: "a".repeat(64),
      claimed_at_unix: nowUnix - 20,
      heartbeat_at_unix: nowUnix - 5,
      lease_timeout_secs: 60,
      task: {
        schema_version: 1,
        queue_algorithm_version: "fleet-queue-v2",
        task_id: "task-nlwiki",
        wiki: "nlwiki",
        snapshot: "2026-08",
        resource_class: "medium_large",
        attempt: 0,
      },
    }));
  });
  const response = await invoke(module, { url: "/api/status", headers: { host } });
  const body = JSON.parse(response.text());
  assert.equal(body.fleet.counts.running, 1);
  assert.equal(body.fleet.counts.stalled, 0);
  assert.equal(body.fleet.work[0].wiki, "nlwiki");
  assert.equal(body.fleet.work[0].state, "running");
  assert.equal(body.fleet.work[0].workerId, "medium-1");
});

test("admin rejects processing a wiki missing from the lifecycle before spawning", async (t) => {
  const lifecycle = {
    schema_version: 1,
    publication_contract: { datasets: {} },
    wikis: {
      nlwiki: {
        refresh: "scheduled",
        publication: "published",
        provenance: "toolforge",
        freshness_sla_days: 40,
        retention: {
          source_recoverability: "redownloadable",
          history_input: "purge_after_ready",
          patrol_source: "purge_after_ready",
          computed_rollback_generations: 1,
        },
      },
    },
  };
  const { module, host } = await startServer(t, LOCAL_ENV, lifecycle);
  const response = await invoke(module, {
    method: "POST",
    url: "/api/run",
    headers: { host, "content-type": "application/json" },
    body: JSON.stringify({ wiki: "dewiki", version: "2026-08" }),
  });
  assert.equal(response.statusCode, 409);
  const body = JSON.parse(response.text());
  assert.equal(body.lifecycleRequired, true);
  assert.match(body.error, /dewiki is not registered/);
});

test("admin can durably register dewiki for private qualification and start it", async (t) => {
  const lifecycle = {
    schema_version: 1,
    publication_contract: {
      datasets: {
        page_weekly_edits: {wikis: ["nlwiki"], minimum_rows_per_wiki: 1},
      },
    },
    wikis: {
      nlwiki: {
        refresh: "scheduled",
        publication: "published",
        provenance: "toolforge",
        freshness_sla_days: 40,
        retention: {
          source_recoverability: "redownloadable",
          history_input: "purge_after_ready",
          patrol_source: "purge_after_ready",
          computed_rollback_generations: 1,
        },
      },
    },
  };
  const {module, host, tempRoot} = await startServer(t, {
    ...LOCAL_ENV,
    WIKI_ECON_BIN: "/usr/bin/true",
  }, lifecycle);
  const registered = await invoke(module, {
    method: "POST",
    url: "/api/register-wiki",
    headers: {host, "content-type": "application/json"},
    body: JSON.stringify({wiki: "dewiki", mode: "qualification", resourceClass: "medium_large"}),
  });
  assert.equal(registered.statusCode, 201);
  const registration = JSON.parse(registered.text());
  assert.equal(registration.lifecycle.publication, "hidden");
  assert.equal(registration.lifecycle.refresh, "qualification");
  const persisted = JSON.parse(fs.readFileSync(path.join(tempRoot, "wiki-lifecycle.json"), "utf8"));
  assert.equal(persisted.wikis.dewiki.refresh, "qualification");
  assert.deepEqual(persisted.publication_contract.datasets.page_weekly_edits.wikis, ["nlwiki"]);

  const started = await invoke(module, {
    method: "POST",
    url: "/api/qualify",
    headers: {host, "content-type": "application/json"},
    body: JSON.stringify({wiki: "dewiki", version: "2026-08"}),
  });
  assert.equal(started.statusCode, 200);
  assert.match(JSON.parse(started.text()).command, /qualify-wiki dewiki/);
  await new Promise((resolve) => setTimeout(resolve, 50));
  const status = JSON.parse((await invoke(module, {url: "/api/status", headers: {host}})).text());
  assert.equal(status.wikiStates.dewiki.refresh, "qualification");
  assert.equal(status.wikiJobs.dewiki.state, "succeeded");
});

test("one generic onboarding transaction registers and queues any supported project", async (t) => {
  const lifecycle = {
    schema_version: 1,
    publication_contract: {
      datasets: {page_weekly_edits: {wikis: ["nlwiki"], minimum_rows_per_wiki: 1}},
    },
    wikis: {nlwiki: {refresh: "manual", publication: "published", provenance: "toolforge"}},
  };
  const {module, host, tempRoot, outputDir} = await startServer(t, {
    ...LOCAL_ENV,
    WIKI_ECON_ADMIN_EXECUTION_MODE: "queue",
  }, lifecycle);

  for (const [wiki, mode, expectedAction] of [
    ["bgwiki", "qualification", "qualify"],
    ["kowiki", "manual", "run"],
    ["hiwiki", "scheduled", "run"],
  ]) {
    const response = await invoke(module, {
      method: "POST",
      url: "/api/onboard-wiki",
      headers: {host, "content-type": "application/json"},
      body: JSON.stringify({wiki, mode, resourceClass: "medium_large", version: "2026-08"}),
    });
    assert.equal(response.statusCode, 202);
    const body = JSON.parse(response.text());
    assert.equal(body.registered, true);
    assert.equal(body.queued, true);
    assert.equal(body.nextAction, expectedAction);
    assert.equal(body.operation.action, expectedAction);
    assert.equal(body.operation.wiki, wiki);
  }

  const persisted = JSON.parse(fs.readFileSync(path.join(tempRoot, "wiki-lifecycle.json"), "utf8"));
  assert.equal(persisted.wikis.bgwiki.refresh, "qualification");
  assert.equal(persisted.wikis.kowiki.refresh, "manual");
  assert.equal(persisted.wikis.hiwiki.refresh, "scheduled");
  assert.deepEqual(persisted.publication_contract.datasets.page_weekly_edits.wikis, ["hiwiki", "kowiki", "nlwiki"]);
  assert.equal(fs.readdirSync(path.join(outputDir, "_admin", "operations", "queued")).length, 3);
});

test("production execution mode queues heavy work and supports cancellation", async (t) => {
  const lifecycle = {
    schema_version: 1,
    publication_contract: {datasets: {}},
    wikis: {
      nlwiki: {
        refresh: "manual",
        publication: "published",
        provenance: "toolforge",
      },
    },
  };
  const {module, host, outputDir} = await startServer(t, {
    ...LOCAL_ENV,
    WIKI_ECON_ADMIN_EXECUTION_MODE: "queue",
  }, lifecycle);
  const queued = await invoke(module, {
    method: "POST",
    url: "/api/run",
    headers: {host, "content-type": "application/json"},
    body: JSON.stringify({wiki: "nlwiki", version: "2026-08"}),
  });
  assert.equal(queued.statusCode, 202);
  const queuedBody = JSON.parse(queued.text());
  assert.equal(queuedBody.queued, true);
  const queuedPath = path.join(
    outputDir,
    "_admin",
    "operations",
    "queued",
    `${queuedBody.requestId}.json`,
  );
  assert.equal(fs.existsSync(queuedPath), true);

  const status = JSON.parse((await invoke(module, {url: "/api/status", headers: {host}})).text());
  assert.equal(status.executionMode, "queue");
  assert.equal(status.adminOperations.counts.queued, 1);
  assert.equal(status.adminOperations.queued[0].wiki, "nlwiki");

  const cancelled = await invoke(module, {
    method: "POST",
    url: "/api/cancel",
    headers: {host, "content-type": "application/json"},
    body: JSON.stringify({requestId: queuedBody.requestId}),
  });
  assert.equal(cancelled.statusCode, 200);
  assert.equal(JSON.parse(cancelled.text()).cancelled, true);
  assert.equal(fs.existsSync(queuedPath), false);
});

test("stale operator work is explained as stalled and can be safely requeued", async (t) => {
  const lifecycle = {
    schema_version: 1,
    publication_contract: {datasets: {}},
    wikis: {nlwiki: {refresh: "manual", publication: "published", provenance: "toolforge"}},
  };
  const requestId = "admin-stale-nlwiki-compute";
  const {module, host, outputDir} = await startServer(t, {
    ...LOCAL_ENV,
    WIKI_ECON_ADMIN_EXECUTION_MODE: "queue",
    WIKI_ECON_ADMIN_OPERATION_STALE_SECS: "1",
  }, lifecycle, ({outputDir: fixtureOutput}) => {
    const running = path.join(fixtureOutput, "_admin", "operations", "running");
    const logs = path.join(fixtureOutput, "_admin", "operations", "logs");
    fs.mkdirSync(running, {recursive: true});
    fs.mkdirSync(logs, {recursive: true});
    const logPath = path.join(logs, `${requestId}.log`);
    fs.writeFileSync(logPath, [
      'run_id=test INFO starting stage stage="source_window" wiki="nlwiki"',
      'run_id=test INFO starting bounded source-window execution planned_sources=10 reused_sources=2 pending_sources=8',
      'run_id=test INFO committed ingest source source="source-1" rows=42',
    ].join("\n"));
    fs.writeFileSync(path.join(running, `${requestId}.json`), JSON.stringify({
      schemaVersion: 1,
      requestId,
      runId: requestId,
      action: "compute",
      wiki: "nlwiki",
      state: "running",
      retryCount: 0,
      startedAt: "2026-08-01T00:00:00Z",
      heartbeatAt: "2026-08-01T00:00:00Z",
      updatedAt: "2026-08-01T00:00:00Z",
      logPath,
    }));
  });

  const before = JSON.parse((await invoke(module, {url: "/api/status", headers: {host}})).text());
  assert.equal(before.adminOperations.running[0].state, "stalled");
  assert.equal(before.adminOperations.running[0].stage, "source_window");
  assert.equal(before.adminOperations.running[0].progress.completedSources, 3);
  assert.equal(before.adminOperations.running[0].progress.totalSources, 10);

  const recovered = await invoke(module, {
    method: "POST",
    url: "/api/recover-admin",
    headers: {host, "content-type": "application/json"},
    body: "{}",
  });
  assert.equal(recovered.statusCode, 200);
  assert.deepEqual(JSON.parse(recovered.text()).recovered, [requestId]);
  assert.equal(fs.existsSync(path.join(outputDir, "_admin", "operations", "queued", `${requestId}.json`)), true);
});

test("admin dispatcher claims and completes one queued operation", async (t) => {
  const operationRoot = fs.mkdtempSync(path.join(os.tmpdir(), "wiki-econ-admin-dispatcher-test-"));
  t.after(() => fs.rmSync(operationRoot, {recursive: true, force: true}));
  const queuedDir = path.join(operationRoot, "queued");
  fs.mkdirSync(queuedDir, {recursive: true});
  const request = {
    schemaVersion: 1,
    requestId: "admin-test-nlwiki-fetch",
    runId: "admin-test-nlwiki-fetch",
    action: "fetch",
    wiki: "nlwiki",
    version: "2026-08",
    requestedAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    state: "queued",
  };
  fs.writeFileSync(path.join(queuedDir, `${request.requestId}.json`), JSON.stringify(request));
  const dispatcherPath = require.resolve("../deploy/toolforge/admin-dispatcher.cjs");
  delete require.cache[dispatcherPath];
  const previousRoot = process.env.WIKI_ECON_ADMIN_OPERATION_DIR;
  const previousBin = process.env.WIKI_ECON_BIN;
  process.env.WIKI_ECON_ADMIN_OPERATION_DIR = operationRoot;
  process.env.WIKI_ECON_BIN = "/usr/bin/true";
  const dispatcher = require(dispatcherPath);
  const completed = await dispatcher.run();
  if (previousRoot == null) delete process.env.WIKI_ECON_ADMIN_OPERATION_DIR;
  else process.env.WIKI_ECON_ADMIN_OPERATION_DIR = previousRoot;
  if (previousBin == null) delete process.env.WIKI_ECON_BIN;
  else process.env.WIKI_ECON_BIN = previousBin;
  delete require.cache[dispatcherPath];
  assert.equal(completed.state, "succeeded");
  assert.equal(fs.readdirSync(path.join(operationRoot, "queued")).length, 0);
  assert.equal(fs.readdirSync(path.join(operationRoot, "running")).length, 0);
  assert.equal(fs.readdirSync(path.join(operationRoot, "history")).length, 1);
});

test("admin prepare action is durable and never invokes the legacy publishing run command", async (t) => {
  const lifecycle = {
    schema_version: 1,
    publication_contract: { datasets: {} },
    wikis: {
      nlwiki: {
        refresh: "scheduled",
        publication: "published",
        provenance: "toolforge",
        freshness_sla_days: 40,
        retention: {
          source_recoverability: "redownloadable",
          history_input: "purge_after_ready",
          patrol_source: "purge_after_ready",
          computed_rollback_generations: 1,
        },
      },
    },
  };
  const { module, host, outputDir } = await startServer(t, {
    ...LOCAL_ENV,
    WIKI_ECON_BIN: "/usr/bin/true",
  }, lifecycle);
  const started = await invoke(module, {
    method: "POST",
    url: "/api/run",
    headers: { host, "content-type": "application/json" },
    body: JSON.stringify({ wiki: "nlwiki", version: "2026-08" }),
  });
  assert.equal(started.statusCode, 200);
  assert.match(JSON.parse(started.text()).command, /prepare-wiki nlwiki/);
  assert.doesNotMatch(JSON.parse(started.text()).command, /\srun nlwiki/);

  await new Promise((resolve) => setTimeout(resolve, 50));
  const status = JSON.parse((await invoke(module, { url: "/api/status", headers: { host } })).text());
  assert.equal(status.wikiJobs.nlwiki.state, "succeeded");
  assert.match(status.wikiJobs.nlwiki.runId, /^admin-/);
  const ledger = JSON.parse(fs.readFileSync(path.join(outputDir, "_admin", "job-history.json"), "utf8"));
  assert.equal(ledger.jobs[0].wiki, "nlwiki");
  assert.equal(ledger.jobs[0].state, "succeeded");
});

test("a failed spawn produces one durable terminal record", async (t) => {
  const lifecycle = {
    schema_version: 1,
    publication_contract: { datasets: {} },
    wikis: {
      nlwiki: {
        refresh: "scheduled",
        publication: "published",
        provenance: "toolforge",
        freshness_sla_days: 40,
      },
    },
  };
  const { module, host, outputDir } = await startServer(t, {
    ...LOCAL_ENV,
    WIKI_ECON_BIN: "/definitely/missing/wiki-econ-test-bin",
  }, lifecycle);
  const started = await invoke(module, {
    method: "POST",
    url: "/api/run",
    headers: { host, "content-type": "application/json" },
    body: JSON.stringify({ wiki: "nlwiki", version: "2026-08" }),
  });
  assert.equal(started.statusCode, 200);

  await new Promise((resolve) => setTimeout(resolve, 50));
  const status = JSON.parse((await invoke(module, { url: "/api/status", headers: { host } })).text());
  assert.equal(status.wikiJobs.nlwiki.state, "failed");
  assert.match(status.wikiJobs.nlwiki.log.join(""), /failed to start/);
  const ledger = JSON.parse(fs.readFileSync(path.join(outputDir, "_admin", "job-history.json"), "utf8"));
  assert.equal(ledger.jobs.filter((entry) => entry.runId === status.wikiJobs.nlwiki.runId).length, 1);
});

test("public freshness status is machine-readable without an admin session", async (t) => {
  const {module, host, outputDir} = await startServer(t, HOSTED_ENV, {
    schema_version: 1,
    publication_contract: {datasets: {}},
    wikis: {
      nlwiki: {
        publication: "published",
        refresh: "scheduled",
        freshness_sla_days: 10,
        provenance: "toolforge",
      },
    },
  });
  const now = new Date().toISOString();
  const record = {
    schemaVersion: 2,
    state: "succeeded",
    exitCode: 0,
    runId: "healthy-run",
    startedAt: now,
    finishedAt: now,
    selectedSnapshot: "2026-07",
    memoryPeakBytes: 2 * 1024 ** 3,
    memoryLimitBytes: 6 * 1024 ** 3,
    diskFreeBytes: 100 * 1024 ** 3,
    publication: {
      selectedSnapshots: {nlwiki: "2026-07"},
      cutoffDates: {nlwiki: "2026-08"},
      metrics: {patrol: {rows: 100}},
      patrolSources: {nlwiki: {patrol_events: 1000, rights_events: 10}},
      browserData: {generation: "a".repeat(64), partitions: 9, rows: 1000, bytes: 1000, largestPartitionBytes: 500},
    },
  };
  fs.writeFileSync(path.join(outputDir, ".refresh-status.json"), JSON.stringify(record), "utf8");

  const health = await invoke(module, {url: "/health/freshness.json", headers: {host}});
  assert.equal(health.statusCode, 200);
  assert.equal(JSON.parse(health.text()).status, "healthy");

  fs.mkdirSync(path.join(outputDir, "_scrubs"), {recursive: true});
  fs.writeFileSync(path.join(outputDir, "_scrubs", "status.json"), JSON.stringify({
    schema_version: 1,
    state: "failed",
    run_id: "scrub-failed",
    updated_at_unix: 1,
    report_sha256: null,
    error: "semantic mismatch",
  }));
  const unhealthy = await invoke(module, {url: "/health/freshness.json", headers: {host}});
  assert.equal(unhealthy.statusCode, 200);
  assert.equal(JSON.parse(unhealthy.text()).alerts[0].code, "artifact_scrub_failed");

  const admin = await invoke(module, {url: "/admin-api/status", headers: {host}});
  assert.equal(admin.statusCode, 401);
});
