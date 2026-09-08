"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const test = require("node:test");
const {acquire} = require("./capacity-admission.cjs");

function fixture(t) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "wiki-econ-capacity-"));
  t.after(() => fs.rmSync(root, {recursive: true, force: true}));
  const configFile = path.join(root, "capacity.json");
  fs.writeFileSync(configFile, JSON.stringify({
    schema_version: 1,
    namespace_memory_limit_bytes: 8 * 1024 ** 3,
    resident_service_memory_bytes: 512 * 1024 ** 2,
    resource_requests: {
      admin_dispatcher: 512 * 1024 ** 2,
      small: 2 * 1024 ** 3,
      medium_large: 6 * 1024 ** 3,
    },
  }));
  return {root: path.join(root, "leases"), configFile};
}

test("capacity admission permits one medium worker beside resident services", (t) => {
  const options = fixture(t);
  const admission = acquire({...options, resourceClass: "medium_large", identity: "medium-1"});
  assert.equal(admission.admitted, true);
  assert.equal(admission.workerBudgetBytes, 7 * 1024 ** 3);
});

test("capacity admission rejects small work while a medium worker owns the budget", (t) => {
  const options = fixture(t);
  assert.equal(acquire({...options, resourceClass: "medium_large", identity: "medium-1"}).admitted, true);
  const small = acquire({...options, resourceClass: "small", identity: "small-1"});
  assert.equal(small.admitted, false);
  assert.equal(small.active, 1);
});

test("capacity admission permits two small workers", (t) => {
  const options = fixture(t);
  assert.equal(acquire({...options, resourceClass: "small", identity: "small-1"}).admitted, true);
  assert.equal(acquire({...options, resourceClass: "small", identity: "small-2"}).admitted, true);
});
