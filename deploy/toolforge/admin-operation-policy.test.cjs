"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const test = require("node:test");
const {compareOperations, resourceClassFor} = require("./admin-operation-policy.cjs");

test("recovery and lifecycle requests sort ahead of bulk transfers", () => {
  const operations = [
    {requestId: "fetch", action: "fetch", requestedAt: "2026-09-01T00:00:00Z"},
    {requestId: "promote", action: "promote-qualification", requestedAt: "2026-09-01T00:01:00Z"},
    {requestId: "recover", action: "fleet-recover", requestedAt: "2026-09-01T00:02:00Z"},
  ].sort(compareOperations);
  assert.deepEqual(operations.map((entry) => entry.requestId), ["recover", "promote", "fetch"]);
});

test("wiki preparation uses its lifecycle resource class", (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "wiki-econ-admin-policy-"));
  t.after(() => fs.rmSync(root, {recursive: true, force: true}));
  const lifecycle = path.join(root, "lifecycle.json");
  fs.writeFileSync(lifecycle, JSON.stringify({
    schema_version: 1,
    wikis: {smallwiki: {fleet_resource_class: "small"}, largewiki: {fleet_resource_class: "medium_large"}},
  }));
  assert.equal(resourceClassFor({action: "run", wiki: "smallwiki"}, lifecycle), "small");
  assert.equal(resourceClassFor({action: "run", wiki: "largewiki"}, lifecycle), "medium_large");
  assert.equal(resourceClassFor({action: "rebuild-compatibility-cohort"}, lifecycle), "medium_large");
});
