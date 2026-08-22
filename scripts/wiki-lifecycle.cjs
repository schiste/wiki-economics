#!/usr/bin/env node

const fs = require("node:fs");
const path = require("node:path");

const PUBLICATION_STATES = new Set(["published", "hidden", "retired"]);
const REFRESH_STATES = new Set(["scheduled", "manual", "paused"]);
const WIKI_NAME = /^[a-z0-9_]+wiki$/;
const YEAR_MONTH = /^\d{4}-(0[1-9]|1[0-2])$/;

function lifecyclePath(root = path.resolve(__dirname, ".."), env = process.env) {
  return path.resolve(env.WIKI_ECON_WIKI_LIFECYCLE_FILE || path.join(root, "config", "wiki-lifecycle.json"));
}

function loadWikiLifecycle(root, env = process.env) {
  const file = lifecyclePath(root, env);
  let parsed;
  try {
    parsed = JSON.parse(fs.readFileSync(file, "utf8"));
  } catch (error) {
    throw new Error(`Unable to read wiki lifecycle registry ${file}: ${error.message}`);
  }
  validateWikiLifecycle(parsed, file);
  return parsed;
}

function validateWikiLifecycle(registry, label = "wiki lifecycle registry") {
  if (!registry || typeof registry !== "object" || Array.isArray(registry)) {
    throw new Error(`${label} must be a JSON object`);
  }
  if (registry.schema_version !== 1) {
    throw new Error(`${label} has unsupported schema_version ${registry.schema_version}`);
  }
  if (!registry.wikis || typeof registry.wikis !== "object" || Array.isArray(registry.wikis)) {
    throw new Error(`${label}.wikis must be a JSON object`);
  }

  for (const [wiki, entry] of Object.entries(registry.wikis)) {
    if (!WIKI_NAME.test(wiki)) throw new Error(`${label} has invalid wiki name ${wiki}`);
    if (!entry || typeof entry !== "object" || Array.isArray(entry)) {
      throw new Error(`${label}.wikis.${wiki} must be a JSON object`);
    }
    if (!PUBLICATION_STATES.has(entry.publication)) {
      throw new Error(`${label}.wikis.${wiki} has invalid publication state ${entry.publication}`);
    }
    if (!REFRESH_STATES.has(entry.refresh)) {
      throw new Error(`${label}.wikis.${wiki} has invalid refresh state ${entry.refresh}`);
    }
    if (entry.publication === "retired" && entry.refresh !== "paused") {
      throw new Error(`${label}.wikis.${wiki} must use refresh=paused when retired`);
    }
    if (typeof entry.provenance !== "string" || !entry.provenance.trim()) {
      throw new Error(`${label}.wikis.${wiki}.provenance must be a non-empty string`);
    }
    if (entry.imported_cutoff != null && !YEAR_MONTH.test(entry.imported_cutoff)) {
      throw new Error(`${label}.wikis.${wiki}.imported_cutoff must use YYYY-MM`);
    }
    if (entry.freshness_sla_days != null
      && (!Number.isInteger(entry.freshness_sla_days) || entry.freshness_sla_days <= 0)) {
      throw new Error(`${label}.wikis.${wiki}.freshness_sla_days must be a positive integer`);
    }
    if (entry.refresh === "scheduled" && entry.freshness_sla_days == null) {
      throw new Error(`${label}.wikis.${wiki} is scheduled but has no freshness_sla_days`);
    }
  }
  return registry;
}

function splitWikiList(value) {
  return (value || "").split(/[,\s]+/).map((wiki) => wiki.trim()).filter(Boolean);
}

function wikisWithState(registry, field, state) {
  return Object.entries(registry.wikis)
    .filter(([, entry]) => entry[field] === state)
    .map(([wiki]) => wiki)
    .sort();
}

function resolveRefreshWikis(registry, env = process.env) {
  const preferred = splitWikiList(env.WIKI_ECON_REFRESH_WIKIS);
  const legacy = splitWikiList(env.WIKI_ECON_ENABLED_WIKIS);
  if (preferred.length && legacy.length && preferred.join("\n") !== legacy.join("\n")) {
    throw new Error("WIKI_ECON_REFRESH_WIKIS and legacy WIKI_ECON_ENABLED_WIKIS disagree");
  }
  const selected = preferred.length ? preferred : legacy.length ? legacy : wikisWithState(registry, "refresh", "scheduled");
  for (const wiki of selected) {
    const entry = registry.wikis[wiki];
    if (!entry) throw new Error(`Refresh list contains unregistered wiki ${wiki}`);
    if (entry.refresh !== "scheduled") {
      throw new Error(`Refresh list contains ${wiki}, whose lifecycle refresh state is ${entry.refresh}`);
    }
  }
  return [...new Set(selected)].sort();
}

function runCli(argv = process.argv.slice(2), env = process.env) {
  const registry = loadWikiLifecycle(undefined, env);
  const command = argv[0] || "validate";
  if (command === "validate") return;
  if (command === "refresh-wikis") {
    process.stdout.write(`${resolveRefreshWikis(registry, env).join("\n")}\n`);
    return;
  }
  if (command === "published-wikis") {
    process.stdout.write(`${wikisWithState(registry, "publication", "published").join("\n")}\n`);
    return;
  }
  if (command === "json") {
    process.stdout.write(`${JSON.stringify(registry)}\n`);
    return;
  }
  throw new Error(`Unknown wiki lifecycle command: ${command}`);
}

if (require.main === module) {
  try {
    runCli();
  } catch (error) {
    console.error(error.message);
    process.exitCode = 1;
  }
}

module.exports = {
  lifecyclePath,
  loadWikiLifecycle,
  resolveRefreshWikis,
  runCli,
  splitWikiList,
  validateWikiLifecycle,
  wikisWithState,
};
