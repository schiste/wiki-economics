#!/usr/bin/env node

const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { execFileSync } = require("node:child_process");
const test = require("node:test");

const {
  loadWikiLifecycle,
  resolveRefreshWikis,
  splitWikiList,
  validateWikiLifecycle,
  wikisWithState,
} = require("./wiki-lifecycle.cjs");

const SCRIPT = path.join(__dirname, "wiki-lifecycle.cjs");
const REGISTRY = path.join(__dirname, "..", "config", "wiki-lifecycle.json");

function validRegistry() {
  return {
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
      testwiki: {
        publication: "hidden",
        refresh: "manual",
        provenance: "test",
      },
    },
  };
}

test("production lifecycle preserves imported wikis while scheduling only nlwiki", () => {
  const registry = loadWikiLifecycle(path.join(__dirname, ".."), {});
  assert.deepEqual(resolveRefreshWikis(registry, {}), ["nlwiki"]);
  assert.deepEqual(
    wikisWithState(registry, "publication", "published"),
    ["elwiki", "frwiki", "nlwiki", "ptwiki", "svwiki"],
  );
  assert.equal(registry.wikis.frwiki.imported_cutoff, "2026-03");
});

test("refresh overrides are backward compatible but fail closed on disagreement", () => {
  const registry = validRegistry();
  assert.deepEqual(resolveRefreshWikis(registry, { WIKI_ECON_REFRESH_WIKIS: "nlwiki" }), ["nlwiki"]);
  assert.deepEqual(resolveRefreshWikis(registry, { WIKI_ECON_ENABLED_WIKIS: "nlwiki nlwiki" }), ["nlwiki"]);
  assert.throws(
    () => resolveRefreshWikis(registry, {
      WIKI_ECON_REFRESH_WIKIS: "nlwiki",
      WIKI_ECON_ENABLED_WIKIS: "frwiki",
    }),
    /disagree/,
  );
  assert.throws(
    () => resolveRefreshWikis(registry, { WIKI_ECON_REFRESH_WIKIS: "missingwiki" }),
    /unregistered/,
  );
  assert.throws(
    () => resolveRefreshWikis(registry, { WIKI_ECON_REFRESH_WIKIS: "frwiki" }),
    /refresh state is paused/,
  );
  assert.deepEqual(splitWikiList(" nlwiki, frwiki\nptwiki "), ["nlwiki", "frwiki", "ptwiki"]);
});

test("registry validation rejects invalid lifecycle states and metadata", () => {
  const invalid = [
    [null, /must be a JSON object/],
    [{ schema_version: 2, wikis: {} }, /unsupported schema_version/],
    [{ schema_version: 1, wikis: [] }, /wikis must be a JSON object/],
    [{ schema_version: 1, wikis: { "bad-name": {} } }, /invalid wiki name/],
    [{ schema_version: 1, wikis: { nlwiki: null } }, /must be a JSON object/],
    [{ schema_version: 1, wikis: { nlwiki: { publication: "gone" } } }, /invalid publication/],
    [{ schema_version: 1, wikis: { nlwiki: { publication: "published", refresh: "never" } } }, /invalid refresh/],
    [{ schema_version: 1, wikis: { nlwiki: { publication: "retired", refresh: "manual" } } }, /refresh=paused/],
    [{ schema_version: 1, wikis: { nlwiki: { publication: "published", refresh: "paused", provenance: "" } } }, /provenance/],
    [{ schema_version: 1, wikis: { nlwiki: { publication: "published", refresh: "paused", provenance: "x", imported_cutoff: "2026-13" } } }, /YYYY-MM/],
    [{ schema_version: 1, wikis: { nlwiki: { publication: "published", refresh: "paused", provenance: "x", freshness_sla_days: 0 } } }, /positive integer/],
    [{ schema_version: 1, wikis: { nlwiki: { publication: "published", refresh: "scheduled", provenance: "x" } } }, /no freshness_sla_days/],
  ];
  for (const [registry, expected] of invalid) {
    assert.throws(() => validateWikiLifecycle(registry, "fixture"), expected);
  }
  assert.equal(validateWikiLifecycle(validRegistry()).schema_version, 1);
});

test("CLI validates and lists lifecycle selections", () => {
  const env = { ...process.env, WIKI_ECON_WIKI_LIFECYCLE_FILE: REGISTRY };
  assert.equal(execFileSync(process.execPath, [SCRIPT, "validate"], { env, encoding: "utf8" }), "");
  assert.equal(execFileSync(process.execPath, [SCRIPT, "refresh-wikis"], { env, encoding: "utf8" }), "nlwiki\n");
  assert.equal(
    execFileSync(process.execPath, [SCRIPT, "published-wikis"], { env, encoding: "utf8" }),
    "elwiki\nfrwiki\nnlwiki\nptwiki\nsvwiki\n",
  );
  assert.equal(JSON.parse(execFileSync(process.execPath, [SCRIPT, "json"], { env, encoding: "utf8" })).schema_version, 1);
  assert.throws(() => execFileSync(process.execPath, [SCRIPT, "unknown"], { env, stdio: "pipe" }));
});

test("registry loader reports missing and malformed files with context", () => {
  const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "wiki-lifecycle-test-"));
  try {
    assert.throws(
      () => loadWikiLifecycle(tempRoot, { WIKI_ECON_WIKI_LIFECYCLE_FILE: path.join(tempRoot, "missing.json") }),
      /Unable to read wiki lifecycle registry/,
    );
    const malformed = path.join(tempRoot, "malformed.json");
    fs.writeFileSync(malformed, "{", "utf8");
    assert.throws(
      () => loadWikiLifecycle(tempRoot, { WIKI_ECON_WIKI_LIFECYCLE_FILE: malformed }),
      /Unable to read wiki lifecycle registry/,
    );
  } finally {
    fs.rmSync(tempRoot, { recursive: true, force: true });
  }
});
