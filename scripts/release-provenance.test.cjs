"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const {after, test} = require("node:test");
const {browserVersions, writeAtomic} = require("./release-provenance.cjs");

const temporary = fs.mkdtempSync(path.join(os.tmpdir(), "release-provenance-test-"));
after(() => fs.rmSync(temporary, {recursive: true, force: true}));

test("browser release provenance is exact and agrees with the npm lock", () => {
  const versions = browserVersions();
  assert.equal(versions.build_tools["@observablehq/framework"], "1.13.4");
  assert.equal(versions.direct["apache-arrow"], "21.2.0");
  assert.equal(versions.direct["parquet-wasm"], "0.7.2");
  assert.equal(versions.generated.d3, "7.9.0");
});

test("release provenance writes atomically and refuses replacement debris", () => {
  const destination = path.join(temporary, "release.json");
  writeAtomic(destination, {schema_version: 1});
  assert.deepEqual(JSON.parse(fs.readFileSync(destination)), {schema_version: 1});
  fs.writeFileSync(`${destination}.tmp.${process.pid}`, "debris");
  assert.throws(() => writeAtomic(destination, {schema_version: 2}), /EEXIST/);
});
