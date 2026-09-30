"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const test = require("node:test");
const {acquire, activeLeases} = require("./capacity-admission.cjs");

function fixture(t) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "wiki-econ-capacity-"));
  t.after(() => fs.rmSync(root, {recursive: true, force: true}));
  const configFile = path.join(root, "capacity.json");
  fs.writeFileSync(configFile, JSON.stringify({
    schema_version: 1,
    namespace_memory_limit_bytes: 24 * 1024 ** 3,
    namespace_cpu_limit_millicores: 16_000,
    per_job_memory_limit_bytes: 6 * 1024 ** 3,
    per_job_cpu_limit_millicores: 4_000,
    resident_service_memory_bytes: 512 * 1024 ** 2,
    resident_service_cpu_millicores: 1_000,
    minimum_schedulable_job_bytes: 2 * 1024 ** 3,
    minimum_schedulable_job_millicores: 1_000,
    worker_reserve_resource_classes: ["admin_dispatcher", "publisher"],
    resource_requests: {
      admin_dispatcher: 512 * 1024 ** 2,
      publisher: 6 * 1024 ** 3,
      small: 2 * 1024 ** 3,
      medium_large: 6 * 1024 ** 3,
    },
    resource_cpu_requests_millicores: {
      admin_dispatcher: 250,
      publisher: 1_000,
      small: 1_000,
      medium_large: 1_000,
    },
  }));
  return {root: path.join(root, "leases"), configFile};
}

test("capacity admission reserves publisher capacity and admits the expanded worker pool", (t) => {
  const options = fixture(t);
  for (const [resourceClass, identity] of [
    ["medium_large", "medium-1"],
    ["medium_large", "medium-2"],
    ["small", "small-1"],
    ["small", "small-2"],
  ]) {
    assert.equal(acquire({...options, resourceClass, identity}).admitted, true);
  }
  const full = acquire({...options, resourceClass: "small", identity: "small-3"});
  assert.equal(full.admitted, false);
  assert.equal(full.workerBudgetBytes, 17 * 1024 ** 3);
  assert.equal(full.admittedBytes, 16 * 1024 ** 3);
  assert.deepEqual(full.limitingResources, ["memory"]);
});

test("capacity admission records CPU alongside memory and enforces both budgets", (t) => {
  const options = fixture(t);
  const config = JSON.parse(fs.readFileSync(options.configFile, "utf8"));
  config.namespace_cpu_limit_millicores = 3_000;
  fs.writeFileSync(options.configFile, JSON.stringify(config));
  const admission = acquire({...options, resourceClass: "small", identity: "small-1"});
  assert.equal(admission.admitted, false);
  assert.equal(admission.requestedMillicores, 1_000);
  assert.equal(admission.workerBudgetMillicores, 750);
  assert.deepEqual(admission.limitingResources, ["cpu"]);
});

test("capacity admission accounts for leases written before CPU accounting was added", (t) => {
  const options = fixture(t);
  fs.mkdirSync(options.root, {recursive: true});
  fs.writeFileSync(path.join(options.root, "legacy.json"), JSON.stringify({
    schemaVersion: 1,
    identity: "legacy",
    resourceClass: "medium_large",
    requestedBytes: 6 * 1024 ** 3,
    heartbeatAt: new Date().toISOString(),
  }));
  const admission = acquire({...options, resourceClass: "small", identity: "small-1"});
  assert.equal(admission.admitted, true);
  assert.equal(admission.admittedBytes, 6 * 1024 ** 3);
  assert.equal(admission.admittedMillicores, 1_000);
});

test("capacity admission archives stale leases once and ignores archived files", (t) => {
  const options = fixture(t);
  fs.mkdirSync(options.root, {recursive: true});
  fs.writeFileSync(path.join(options.root, "expired.json"), JSON.stringify({
    schemaVersion: 1,
    identity: "expired",
    resourceClass: "small",
    requestedBytes: 2 * 1024 ** 3,
    requestedMillicores: 1_000,
    heartbeatAt: "2000-01-01T00:00:00.000Z",
  }));

  const first = acquire({...options, resourceClass: "small", identity: "new-1"});
  assert.equal(first.admitted, true);
  const archived = fs.readdirSync(options.root).filter((name) => name.startsWith(".stale-"));
  assert.equal(archived.length, 1);

  const second = acquire({...options, resourceClass: "small", identity: "new-2"});
  assert.equal(second.admitted, true);
  assert.equal(second.active, 1);
  assert.deepEqual(fs.readdirSync(options.root).filter((name) => name.startsWith(".stale-")), archived);
});

test("capacity admission tolerates a stale lease disappearing before it is archived", (t) => {
  const options = fixture(t);
  fs.mkdirSync(options.root, {recursive: true});
  const expiredFile = path.join(options.root, "expired.json");
  fs.writeFileSync(expiredFile, JSON.stringify({
    schemaVersion: 1,
    identity: "expired",
    resourceClass: "small",
    requestedBytes: 2 * 1024 ** 3,
    requestedMillicores: 1_000,
    heartbeatAt: "2000-01-01T00:00:00.000Z",
  }));

  const originalRenameSync = fs.renameSync;
  let removedConcurrently = false;
  fs.renameSync = (source, destination) => {
    if (source === expiredFile && !removedConcurrently) {
      fs.unlinkSync(source);
      removedConcurrently = true;
      const error = new Error("lease disappeared during stale cleanup");
      error.code = "ENOENT";
      throw error;
    }
    return originalRenameSync(source, destination);
  };
  try {
    const admission = acquire({...options, resourceClass: "small", identity: "new-1"});
    assert.equal(removedConcurrently, true);
    assert.equal(admission.admitted, true);
    assert.equal(admission.active, 0);
  } finally {
    fs.renameSync = originalRenameSync;
  }
});

test("a stale lease is archived under a name that cannot grow without bound", () => {
  // Before the `.stale-` skip was added, every pass re-archived the previous
  // archival, stacking one `.stale-<ts>-` prefix per pass until the name
  // exceeded the 255-byte limit and renameSync failed with ENAMETOOLONG. That
  // aborted admission for every later caller. Production still carries the
  // resulting debris, so the archived name must stay bounded by construction
  // rather than relying on a skip that a future refactor could drop.
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "wiki-econ-stale-name-"));
  try {
    // A qualification pod's hostname is long enough that a 70-byte lease name
    // plus nine archival prefixes reaches the limit.
    const identity = `wiki-econ-qualify-enwiki-aug-b3c1af0-bswtz-7-${"p".repeat(150)}`;
    const stale = {
      schemaVersion: 1,
      identity,
      resourceClass: "qualification",
      requestedBytes: 6442450944,
      requestedMillicores: 4000,
      heartbeatAt: 0,
    };
    const configFile = path.join(__dirname, "..", "..", "config", "toolforge-capacity.json");

    // Each iteration re-creates the lease exactly as a re-appearing stale
    // holder would, letting the archived set grow.
    for (let round = 0; round < 12; round += 1) {
      fs.writeFileSync(path.join(root, `${identity}.json`), JSON.stringify(stale));
      const admission = acquire({
        root,
        configFile: path.resolve(configFile),
        resourceClass: "qualification",
        identity,
        now: Date.now() + round * 60_000,
      });
      assert.equal(admission.admitted, true, "a stale lease must not block admission");
    }
    for (const entry of fs.readdirSync(root)) {
      assert.ok(
        Buffer.byteLength(entry) <= 255,
        `archived lease name is ${Buffer.byteLength(entry)} bytes: renameSync cannot create it`,
      );
    }
  } finally {
    fs.rmSync(root, {recursive: true, force: true});
  }
});
