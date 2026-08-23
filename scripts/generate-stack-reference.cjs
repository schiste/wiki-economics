#!/usr/bin/env node
"use strict";

const fs = require("node:fs");
const path = require("node:path");
const {execFileSync} = require("node:child_process");

const ROOT = path.resolve(__dirname, "..");
const OUTPUT = path.join(ROOT, "docs", "generated", "stack-reference.md");

const RUST_ROLES = {
  anyhow: "application error propagation and context",
  bzip2: "streaming MediaWiki History decompression",
  chrono: "UTC dates, timestamps, and snapshot boundaries",
  clap: "command-line parsing",
  flate2: "concatenated gzip decoding for logging dumps",
  fs4: "portable file locking",
  hex: "digest encoding",
  indicatif: "operator progress reporting",
  polars: "Parquet/CSV dataframes, aggregation, and deterministic output",
  "quick-xml": "streaming MediaWiki logging XML parsing",
  rayon: "bounded parallel work",
  regex: "validated source-name and text parsing",
  reqwest: "Wikimedia dump and API HTTP client",
  rustix: "filesystem durability operations",
  serde: "typed serialization contracts",
  serde_json: "JSON manifests, receipts, and status records",
  sha2: "content and artifact fingerprints",
  tracing: "structured pipeline events",
  "tracing-subscriber": "structured log formatting and filtering",
};

const FRONTEND_ROLES = {
  "@observablehq/framework": "deterministic static-site compiler",
  "@observablehq/inputs": "interactive controls",
  "@observablehq/plot": "charts",
  "apache-arrow": "browser columnar data representation",
  d3: "browser transforms and scales",
  htl: "safe browser HTML templates",
  "parquet-wasm": "browser Parquet decoding",
  react: "exact Observable client JSX runtime resolution",
  "react-dom": "exact Observable client JSX renderer resolution",
};

const VERSION_LABELS = [
  ...Object.keys(RUST_ROLES),
  ...Object.keys(FRONTEND_ROLES),
  "Polars",
  "Observable Framework",
  "Node.js",
  "Node",
  "npm",
  "Rust",
  "Apache Arrow",
  "apache-arrow",
  "Parquet-WASM",
  "parquet-wasm",
  "@observablehq/framework",
  "@observablehq/inputs",
  "@observablehq/plot",
  "esbuild",
];

function readJson(relativePath) {
  return JSON.parse(fs.readFileSync(path.join(ROOT, relativePath), "utf8"));
}

function readText(relativePath) {
  return fs.readFileSync(path.join(ROOT, relativePath), "utf8").trim();
}

function cargoMetadata() {
  return JSON.parse(execFileSync(
    "cargo",
    ["metadata", "--locked", "--format-version", "1"],
    {cwd: ROOT, encoding: "utf8", maxBuffer: 64 * 1024 * 1024},
  ));
}

function exactVersion(value, label) {
  if (!/^\d+\.\d+\.\d+$/.test(value || "")) {
    throw new Error(`${label} must be pinned to an exact semantic version; found ${JSON.stringify(value)}`);
  }
  return value;
}

function lockPackage(lock, name, workspace = null) {
  const keys = workspace
    ? [`${workspace}/node_modules/${name}`, `node_modules/${name}`]
    : [`node_modules/${name}`];
  const entry = keys.map((key) => lock.packages?.[key]).find(Boolean);
  if (!entry) throw new Error(`package-lock.json has no resolved entry for ${name}`);
  return entry;
}

function resolvedRustDependencies(metadata) {
  const workspace = metadata.packages.find((pkg) => metadata.workspace_members.includes(pkg.id));
  if (!workspace) throw new Error("Cargo metadata has no workspace package");
  const node = metadata.resolve?.nodes.find((candidate) => candidate.id === workspace.id);
  if (!node) throw new Error("Cargo metadata has no resolved workspace node");
  const packages = new Map(metadata.packages.map((pkg) => [pkg.id, pkg]));
  const direct = workspace.dependencies.filter((dependency) => dependency.kind === null);
  const rows = direct.map((dependency) => {
    const edge = node.deps.find((candidate) => candidate.name === dependency.name.replaceAll("-", "_"));
    const resolved = edge && packages.get(edge.pkg);
    if (!resolved) throw new Error(`Cargo metadata cannot resolve direct dependency ${dependency.name}`);
    const role = RUST_ROLES[dependency.name];
    if (!role) throw new Error(`Add a documentation role for new direct Rust dependency ${dependency.name}`);
    if (!resolved.license) throw new Error(`Cargo metadata has no license for ${dependency.name}`);
    return {name: dependency.name, version: resolved.version, role, license: resolved.license};
  });
  const documented = new Set(rows.map((row) => row.name));
  for (const name of Object.keys(RUST_ROLES)) {
    if (!documented.has(name)) throw new Error(`Remove stale Rust documentation role ${name}`);
  }
  return {workspace, rows: rows.sort((left, right) => left.name.localeCompare(right.name))};
}

function resolvedFrontendDependencies(rootManifest, siteManifest, lock) {
  const declared = new Map([
    ...Object.entries(rootManifest.dependencies || {}).map(([name, version]) => [name, {version, workspace: null}]),
    ...Object.entries(siteManifest.dependencies || {}).map(([name, version]) => [name, {version, workspace: "site"}]),
  ]);
  const expectedNames = Object.keys(FRONTEND_ROLES).sort();
  const declaredNames = [...declared.keys()].sort();
  if (JSON.stringify(expectedNames) !== JSON.stringify(declaredNames)) {
    throw new Error(`Frontend documentation roles differ from direct production dependencies: expected ${declaredNames.join(", ")}`);
  }
  return declaredNames.map((name) => {
    const declaration = declared.get(name);
    const requested = exactVersion(declaration.version, name);
    const resolved = lockPackage(lock, name, declaration.workspace);
    if (resolved.version !== requested) {
      throw new Error(`${name} resolves to ${resolved.version}; manifest pins ${requested}`);
    }
    if (!resolved.license) throw new Error(`package-lock.json has no license for ${name}`);
    return {name, version: resolved.version, role: FRONTEND_ROLES[name], license: resolved.license};
  });
}

function escapeCell(value) {
  return String(value).replaceAll("|", "\\|").replaceAll("\n", " ");
}

function table(headers, rows) {
  return [
    `| ${headers.join(" | ")} |`,
    `| ${headers.map(() => "---").join(" | ")} |`,
    ...rows.map((row) => `| ${row.map(escapeCell).join(" | ")} |`),
  ].join("\n");
}

function buildStackReference(options = {}) {
  const metadata = options.metadata || cargoMetadata();
  const rootManifest = options.rootManifest || readJson("package.json");
  const siteManifest = options.siteManifest || readJson("site/package.json");
  const lock = options.lock || readJson("package-lock.json");
  const lifecycle = options.lifecycle || readJson("config/wiki-lifecycle.json");
  const {workspace, rows: rustDependencies} = resolvedRustDependencies(metadata);
  const frontendDependencies = resolvedFrontendDependencies(rootManifest, siteManifest, lock);

  const lockRoot = lock.packages?.[""];
  if (!lockRoot) throw new Error("package-lock.json has no root workspace record");
  const node = exactVersion(lockRoot.engines?.node, "Node.js");
  const npm = exactVersion(lockRoot.engines?.npm, "npm");
  if (rootManifest.engines?.node !== node || rootManifest.engines?.npm !== npm) {
    throw new Error("Node/npm root manifest and lockfile pins disagree");
  }
  if (rootManifest.packageManager !== `npm@${npm}` || rootManifest.volta?.node !== node || rootManifest.volta?.npm !== npm) {
    throw new Error("Node/npm manifest pins disagree");
  }
  const rust = exactVersion(workspace.rust_version, "Rust");
  const toolchain = readText("rust-toolchain.toml").match(/channel\s*=\s*"([^"]+)"/)?.[1];
  if (toolchain !== rust) throw new Error(`Cargo rust-version ${rust} differs from rust-toolchain.toml ${toolchain}`);

  const esbuildPin = exactVersion(rootManifest.overrides?.esbuild, "esbuild");
  const esbuild = lockPackage(lock, "esbuild");
  if (esbuild.version !== esbuildPin) throw new Error(`esbuild resolves to ${esbuild.version}; override pins ${esbuildPin}`);

  for (const forbidden of ["duckdb", "@duckdb/duckdb-wasm"]) {
    if (lock.packages?.[`node_modules/${forbidden}`]) {
      throw new Error(`Active browser/build dependency ${forbidden} contradicts the documented Arrow + parquet-wasm path`);
    }
  }
  for (const required of ["apache-arrow", "parquet-wasm"]) {
    if (!siteManifest.dependencies?.[required]) throw new Error(`Browser query dependency ${required} is not declared directly`);
  }

  const lifecycleRows = Object.entries(lifecycle.wikis)
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([wiki, entry]) => [
      `\`${wiki}\``, entry.publication, entry.refresh, entry.provenance,
      entry.refresh === "scheduled" ? `${entry.freshness_sla_days} days` : entry.imported_cutoff || "—",
    ]);
  const scheduled = lifecycleRows.filter((row) => row[2] === "scheduled").map((row) => row[0]).join(", ");
  const pausedImports = lifecycleRows
    .filter((row) => row[2] === "paused" && row[3] === "local-import")
    .map((row) => row[0]).join(", ");

  return `# Generated stack reference

<!-- Generated by scripts/generate-stack-reference.cjs. Do not edit by hand. -->

This file is deterministic and contains no generation timestamp. Run
\`node scripts/generate-stack-reference.cjs --write\` after changing an
authoritative manifest, or \`--check\` to verify the checked-in copy.

## Toolchains and site compiler

${table(
    ["Component", "Exact version", "Authoritative source"],
    [
      ["Rust", `\`${rust}\``, "Cargo metadata + `rust-toolchain.toml`"],
      ["Node.js", `\`${node}\``, "root lockfile + `package.json`"],
      ["npm", `\`${npm}\``, "root lockfile + `package.json`"],
      ["Observable Framework", `\`${lockPackage(lock, "@observablehq/framework").version}\``, "root lockfile"],
      ["esbuild", `\`${esbuild.version}\``, "root lockfile + override"],
    ],
  )}

## Direct Rust dependencies

This table is resolved from \`cargo metadata --locked\`, not copied from
\`Cargo.toml\` version requirements.

${table(
    ["Crate", "Resolved version", "Production role", "License"],
    rustDependencies.map((row) => [`\`${row.name}\``, `\`${row.version}\``, row.role, `\`${row.license}\``]),
  )}

## Direct browser and site dependencies

Versions and licenses are resolved from the root npm workspace lockfile. The
browser query path is Apache Arrow plus parquet-wasm; DuckDB is not an active
build or browser dependency.

${table(
    ["Package", "Resolved version", "Production role", "License"],
    frontendDependencies.map((row) => [`\`${row.name}\``, `\`${row.version}\``, row.role, `\`${row.license}\``]),
  )}

## Published wiki lifecycle

The table below is rendered from \`config/wiki-lifecycle.json\`. Scheduled
datasets are ${scheduled}; paused imported datasets are ${pausedImports}.

${table(
    ["Wiki", "Publication", "Refresh", "Provenance", "Freshness SLA / imported cutoff"],
    lifecycleRows,
  )}
`;
}

function markdownFiles(root = ROOT) {
  const files = [path.join(root, "README.md")];
  for (const directory of ["docs", "deploy"]) {
    const pending = [path.join(root, directory)];
    while (pending.length) {
      const current = pending.pop();
      for (const entry of fs.readdirSync(current, {withFileTypes: true})) {
        const target = path.join(current, entry.name);
        if (entry.isDirectory()) {
          if (path.relative(root, target) !== path.join("docs", "generated")) pending.push(target);
        } else if (entry.isFile() && entry.name.endsWith(".md")) {
          files.push(target);
        }
      }
    }
  }
  return files.sort();
}

function validateNarrativeDocs(files = markdownFiles()) {
  const staleClaims = [
    /Python sidecar pipeline/i,
    /Python patrol pipeline/i,
    /runs through the Python sidecar/i,
  ];
  const exactVersionClaim = /\b\d+\.\d+\.\d+\b/;
  const abbreviatedCoreClaim = /(?:Polars|Observable Framework)[^\n]{0,48}\b\d+\.\d+\b/i;
  const errors = [];
  for (const file of files) {
    const relative = path.relative(ROOT, file);
    const lines = fs.readFileSync(file, "utf8").split("\n");
    lines.forEach((line, index) => {
      for (const claim of staleClaims) {
        if (claim.test(line)) errors.push(`${relative}:${index + 1}: stale production-path claim: ${line.trim()}`);
      }
      if ((exactVersionClaim.test(line) && VERSION_LABELS.some((label) => line.toLowerCase().includes(label.toLowerCase())))
        || abbreviatedCoreClaim.test(line)) {
        errors.push(`${relative}:${index + 1}: dependency versions belong in docs/generated/stack-reference.md: ${line.trim()}`);
      }
    });
  }
  if (errors.length) throw new Error(`documentation drift detected:\n${errors.join("\n")}`);
}

function checkStackReference(expected = buildStackReference(), output = OUTPUT) {
  validateNarrativeDocs();
  const actual = fs.existsSync(output) ? fs.readFileSync(output, "utf8") : "";
  if (actual !== expected) {
    throw new Error("docs/generated/stack-reference.md is stale; run node scripts/generate-stack-reference.cjs --write");
  }
}

function main(argv = process.argv.slice(2)) {
  if (argv.length !== 1 || !["--check", "--write"].includes(argv[0])) {
    throw new Error("usage: generate-stack-reference.cjs --check|--write");
  }
  const rendered = buildStackReference();
  if (argv[0] === "--write") {
    fs.mkdirSync(path.dirname(OUTPUT), {recursive: true});
    fs.writeFileSync(OUTPUT, rendered);
    validateNarrativeDocs();
    process.stdout.write(`Wrote ${path.relative(ROOT, OUTPUT)}.\n`);
  } else {
    checkStackReference(rendered);
    process.stdout.write("Verified generated stack reference and narrative documentation.\n");
  }
}

if (require.main === module) {
  try {
    main();
  } catch (error) {
    console.error(error.message);
    process.exitCode = 1;
  }
}

module.exports = {
  buildStackReference,
  checkStackReference,
  markdownFiles,
  resolvedFrontendDependencies,
  resolvedRustDependencies,
  validateNarrativeDocs,
};
