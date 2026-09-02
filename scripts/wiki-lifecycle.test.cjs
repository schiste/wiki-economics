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
const OPERATOR_DOCS = [
  path.join(__dirname, "..", "deploy", "toolforge", "README.md"),
  path.join(__dirname, "..", "docs", "production-topology.md"),
];

function validRegistry() {
  return {
    schema_version: 1,
    publication_contract: {
      datasets: {
        gdp: { coverage: "all_published", minimum_rows_per_wiki: 1 },
        patrol: { wikis: ["nlwiki"], minimum_rows_per_wiki: 1 },
      },
    },
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
        refresh: "qualification",
        provenance: "test",
      },
    },
  };
}

test("production lifecycle schedules qualified Toolforge wikis", () => {
  const registry = loadWikiLifecycle(path.join(__dirname, ".."), {});
  const productionWikis = [
    "afwiki", "arwiki", "arzwiki", "elwiki", "eswiki", "frwiki",
    "hawiki", "itwiki", "jawiki", "nlwiki", "ptwiki", "svwiki",
    "swwiki", "viwiki", "yowiki", "zhwiki",
  ];
  assert.deepEqual(
    resolveRefreshWikis(registry, {}),
    productionWikis,
  );
  assert.deepEqual(
    wikisWithState(registry, "publication", "published"),
    productionWikis,
  );
  assert.equal(registry.wikis.frwiki.refresh, "scheduled");
  assert.equal(registry.wikis.frwiki.provenance, "toolforge");
  assert.equal(registry.wikis.frwiki.freshness_sla_days, 10);
  assert.equal(registry.wikis.frwiki.imported_cutoff, undefined);
  assert.equal(registry.wikis.ptwiki.refresh, "scheduled");
  assert.equal(registry.wikis.ptwiki.provenance, "toolforge");
  assert.equal(registry.wikis.ptwiki.imported_cutoff, undefined);
  for (const wiki of productionWikis) {
    assert.equal(registry.wikis[wiki].publication, "published");
    assert.equal(registry.wikis[wiki].refresh, "scheduled");
    assert.equal(registry.wikis[wiki].provenance, "toolforge");
    assert.equal(registry.wikis[wiki].freshness_sla_days, 10);
    assert.equal(registry.wikis[wiki].imported_cutoff, undefined);
  }
  assert.equal(
    registry.publication_contract.datasets.page_weekly_edits.minimum_rows_by_wiki.elwiki,
    5_000_000,
  );
  assert.deepEqual(
    resolveRefreshWikis(registry, {WIKI_ECON_REFRESH_WIKIS: "frwiki nlwiki ptwiki"}),
    ["frwiki", "nlwiki", "ptwiki"],
  );
});

test("operator topology documents enumerate every scheduled wiki", () => {
  const registry = loadWikiLifecycle(path.join(__dirname, ".."), {});
  const scheduledWikis = resolveRefreshWikis(registry, {});
  for (const documentPath of OPERATOR_DOCS) {
    const document = fs.readFileSync(documentPath, "utf8");
    for (const wiki of scheduledWikis) {
      assert.match(document, new RegExp(`\\b${wiki}\\b`), `${documentPath} omits ${wiki}`);
    }
  }
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
  registry.wikis.testwiki.publication = "published";
  registry.wikis.testwiki.refresh = "manual";
  assert.deepEqual(
    resolveRefreshWikis(registry, { WIKI_ECON_REFRESH_WIKIS: "nlwiki testwiki" }),
    ["nlwiki", "testwiki"],
  );
  registry.wikis.testwiki.publication = "hidden";
  assert.throws(
    () => resolveRefreshWikis(registry, { WIKI_ECON_REFRESH_WIKIS: "testwiki" }),
    /publication state is hidden/,
  );
  assert.deepEqual(splitWikiList(" nlwiki, frwiki\nptwiki "), ["nlwiki", "frwiki", "ptwiki"]);
});

test("registry validation rejects invalid lifecycle states and metadata", () => {
  const contract = { publication_contract: { datasets: {} } };
  const invalid = [
    [null, /must be a JSON object/],
    [{ schema_version: 2, wikis: {} }, /unsupported schema_version/],
    [{ schema_version: 1, wikis: [] }, /wikis must be a JSON object/],
    [{ schema_version: 1, wikis: { "bad-name": {} }, ...contract }, /invalid wiki name/],
    [{ schema_version: 1, wikis: { nlwiki: null }, ...contract }, /must be a JSON object/],
    [{ schema_version: 1, wikis: { nlwiki: { publication: "gone" } }, ...contract }, /invalid publication/],
    [{ schema_version: 1, wikis: { nlwiki: { publication: "published", refresh: "never" } }, ...contract }, /invalid refresh/],
    [{ schema_version: 1, wikis: { nlwiki: { publication: "retired", refresh: "manual" } }, ...contract }, /refresh=paused/],
    [{ schema_version: 1, wikis: { nlwiki: { publication: "published", refresh: "qualification" } }, ...contract }, /publication=hidden/],
    [{ schema_version: 1, wikis: { nlwiki: { publication: "published", refresh: "paused", provenance: "" } }, ...contract }, /provenance/],
    [{ schema_version: 1, wikis: { nlwiki: { publication: "published", refresh: "paused", provenance: "x", imported_cutoff: "2026-13" } }, ...contract }, /YYYY-MM/],
    [{ schema_version: 1, wikis: { nlwiki: { publication: "published", refresh: "paused", provenance: "x", freshness_sla_days: 0 } }, ...contract }, /positive integer/],
    [{ schema_version: 1, wikis: { nlwiki: { publication: "published", refresh: "scheduled", provenance: "x" } }, ...contract }, /no freshness_sla_days/],
    [{ schema_version: 1, wikis: { nlwiki: { publication: "published", refresh: "paused", provenance: "x", fleet_resource_class: "huge" } }, ...contract }, /fleet_resource_class/],
  ];
  for (const [registry, expected] of invalid) {
    assert.throws(() => validateWikiLifecycle(registry, "fixture"), expected);
  }
  assert.equal(validateWikiLifecycle(validRegistry()).schema_version, 1);
});

test("registry validation rejects invalid publication dataset contracts", () => {
  const base = validRegistry();
  const invalid = [
    [{ "Bad-Name": { coverage: "all_published", minimum_rows_per_wiki: 1 } }, /invalid dataset name/],
    [{ gdp: null }, /must be a JSON object/],
    [{ gdp: { coverage: "all_published", wikis: ["nlwiki"], minimum_rows_per_wiki: 1 } }, /exactly one/],
    [{ gdp: { minimum_rows_per_wiki: 1 } }, /exactly one/],
    [{ gdp: { wikis: [], minimum_rows_per_wiki: 1 } }, /non-empty and unique/],
    [{ gdp: { wikis: ["nlwiki", "nlwiki"], minimum_rows_per_wiki: 1 } }, /non-empty and unique/],
    [{ gdp: { wikis: ["testwiki"], minimum_rows_per_wiki: 1 } }, /non-published/],
    [{ gdp: { coverage: "all_published", minimum_rows_per_wiki: 0 } }, /positive safe integer/],
    [{ gdp: { coverage: "all_published", minimum_rows_per_wiki: 1, minimum_rows_by_wiki: [] } }, /must be a JSON object/],
    [{ gdp: { coverage: "all_published", minimum_rows_per_wiki: 1, minimum_rows_by_wiki: {testwiki: 1} } }, /unexpected wiki/],
    [{ gdp: { coverage: "all_published", minimum_rows_per_wiki: 1, minimum_rows_by_wiki: {nlwiki: 0} } }, /positive safe integer/],
  ];
  for (const [datasets, expected] of invalid) {
    assert.throws(
      () => validateWikiLifecycle({ ...base, publication_contract: { datasets } }),
      expected,
    );
  }
});

test("registry accepts positive per-wiki row threshold overrides", () => {
  const registry = validRegistry();
  registry.publication_contract.datasets.gdp.minimum_rows_by_wiki = {
    frwiki: 2,
    nlwiki: 3,
  };
  assert.equal(validateWikiLifecycle(registry), registry);
});

test("CLI validates and lists lifecycle selections", () => {
  const env = { ...process.env, WIKI_ECON_WIKI_LIFECYCLE_FILE: REGISTRY };
  assert.equal(execFileSync(process.execPath, [SCRIPT, "validate"], { env, encoding: "utf8" }), "");
  assert.equal(
    execFileSync(process.execPath, [SCRIPT, "refresh-wikis"], { env, encoding: "utf8" }),
    "afwiki\narwiki\narzwiki\nelwiki\neswiki\nfrwiki\nhawiki\nitwiki\njawiki\nnlwiki\nptwiki\nsvwiki\nswwiki\nviwiki\nyowiki\nzhwiki\n",
  );
  assert.equal(
    execFileSync(process.execPath, [SCRIPT, "published-wikis"], { env, encoding: "utf8" }),
    "afwiki\narwiki\narzwiki\nelwiki\neswiki\nfrwiki\nhawiki\nitwiki\njawiki\nnlwiki\nptwiki\nsvwiki\nswwiki\nviwiki\nyowiki\nzhwiki\n",
  );
  assert.equal(
    execFileSync(process.execPath, [SCRIPT, "qualification-wikis"], { env, encoding: "utf8" }),
    "dewiki\n",
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
