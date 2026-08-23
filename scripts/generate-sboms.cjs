#!/usr/bin/env node
"use strict";

const crypto = require("node:crypto");
const fs = require("node:fs");
const path = require("node:path");
const {spawnSync} = require("node:child_process");
const {loadPolicy, verifyNpmLicenses} = require("./check-npm-licenses.cjs");
const {listFiles, verifySiteDependencies} = require("./verify-site-dependencies.cjs");

const root = path.resolve(__dirname, "..");
const PROPERTY_PREFIX = "org.wikimedia.toolforge.wiki-econ";

function readJson(file) {
  return JSON.parse(fs.readFileSync(file, "utf8"));
}

function sha256Buffer(value) {
  return crypto.createHash("sha256").update(value).digest("hex");
}

function sha256File(file) {
  return sha256Buffer(fs.readFileSync(file));
}

function treeSha256(directory) {
  const hash = crypto.createHash("sha256");
  for (const relative of listFiles(directory)) {
    const normalized = relative.replaceAll(path.sep, "/");
    hash.update(normalized);
    hash.update("\0");
    hash.update(sha256File(path.join(directory, relative)));
    hash.update("\n");
  }
  return hash.digest("hex");
}

function encodePurlName(name) {
  return name.split("/").map(encodeURIComponent).join("/");
}

function component(type, ecosystem, name, version, license, extra = {}) {
  return {
    type,
    "bom-ref": `pkg:${ecosystem}/${encodePurlName(name)}@${encodeURIComponent(version)}`,
    name,
    version,
    licenses: [{expression: license}],
    purl: `pkg:${ecosystem}/${encodePurlName(name)}@${encodeURIComponent(version)}`,
    ...extra,
  };
}

function normalizeCargoLicense(expression) {
  return expression.replace(/\s*\/\s*/g, " OR ");
}

function properties(values) {
  return Object.entries(values).sort(([left], [right]) => left.localeCompare(right)).map(([name, value]) => ({
    name: `${PROPERTY_PREFIX}.${name}`,
    value: String(value),
  }));
}

function propertyValue(document, name) {
  return document?.metadata?.component?.properties?.find((property) => property.name === `${PROPERTY_PREFIX}.${name}`)?.value || null;
}

function makeBom({artifact, commit, timestamp, rootComponent, components, dependencies = []}) {
  const {identity = {}, ...cycloneDxComponent} = rootComponent;
  return {
    bomFormat: "CycloneDX",
    specVersion: "1.6",
    version: 1,
    metadata: {
      timestamp,
      tools: {components: [{type: "application", name: "wiki-econ-sbom-generator", version: commit}]},
      component: {
        ...cycloneDxComponent,
        properties: properties({artifact, "source-commit": commit, ...identity}),
      },
    },
    components: [...components].sort((left, right) => left["bom-ref"].localeCompare(right["bom-ref"])),
    dependencies: [...dependencies].sort((left, right) => left.ref.localeCompare(right.ref)),
  };
}

function cargoGraph(metadata) {
  const workspaceIds = new Set(metadata.workspace_members || []);
  const packages = new Map(metadata.packages.map((entry) => [entry.id, entry]));
  const refs = new Map();
  const components = [];
  for (const entry of metadata.packages) {
    const ref = `pkg:cargo/${encodePurlName(entry.name)}@${encodeURIComponent(entry.version)}`;
    refs.set(entry.id, ref);
    if (workspaceIds.has(entry.id)) continue;
    if (!entry.license) throw new Error(`Cargo package ${entry.name}@${entry.version} has no SPDX license expression`);
    components.push(component("library", "cargo", entry.name, entry.version, normalizeCargoLicense(entry.license), {
      ...(entry.repository ? {externalReferences: [{type: "vcs", url: entry.repository}]} : {}),
    }));
  }
  const dependencies = [];
  for (const node of metadata.resolve?.nodes || []) {
    const entry = packages.get(node.id);
    const ref = workspaceIds.has(node.id)
      ? `pkg:cargo/${encodePurlName(entry.name)}@${encodeURIComponent(entry.version)}`
      : refs.get(node.id);
    dependencies.push({ref, dependsOn: node.dependencies.map((id) => refs.get(id)).filter(Boolean).sort()});
  }
  return {components, dependencies};
}

function npmComponents(inventory) {
  return inventory.map((entry) => component("library", "npm", entry.name, entry.version, entry.license));
}

function commandJson(command, args, cwd = root) {
  const result = spawnSync(command, args, {cwd, encoding: "utf8", maxBuffer: 64 * 1024 * 1024});
  if (result.error) throw result.error;
  if (result.status !== 0) throw new Error(`${command} ${args.join(" ")} failed:\n${result.stderr || result.stdout}`);
  return JSON.parse(result.stdout);
}

function closureHash(files, repositoryRoot = root) {
  const hash = crypto.createHash("sha256");
  for (const file of [...files].sort()) {
    hash.update(path.relative(repositoryRoot, file).replaceAll(path.sep, "/"));
    hash.update("\0");
    hash.update(sha256File(file));
    hash.update("\n");
  }
  return hash.digest("hex");
}

function workspacePackage(metadata) {
  const workspace = new Set(metadata.workspace_members || []);
  const entry = metadata.packages.find((candidate) => workspace.has(candidate.id) && candidate.name === "wiki-econ")
    || metadata.packages.find((candidate) => workspace.has(candidate.id));
  if (!entry) throw new Error("Cargo metadata does not contain the wiki-econ workspace package");
  return entry;
}

function generateDocuments({binary, browserDist, commit, sourceDateEpoch, repositoryRoot = root, cargoMetadata}) {
  if (!/^[0-9a-f]{40}$/.test(commit || "")) throw new Error("WIKI_ECON_BUILD_COMMIT must be an exact 40-character commit");
  if (!/^\d+$/.test(String(sourceDateEpoch || ""))) throw new Error("SOURCE_DATE_EPOCH is required for deterministic SBOMs");
  if (!fs.statSync(binary, {throwIfNoEntry: false})?.isFile()) throw new Error(`release binary is missing: ${binary}`);
  verifySiteDependencies(browserDist);

  const timestamp = new Date(Number(sourceDateEpoch) * 1000).toISOString();
  const metadata = cargoMetadata || commandJson("cargo", ["metadata", "--locked", "--format-version", "1"], repositoryRoot);
  const workspace = workspacePackage(metadata);
  const cargo = cargoGraph(metadata);
  const lock = readJson(path.join(repositoryRoot, "package-lock.json"));
  const workspaceManifest = readJson(path.join(repositoryRoot, "package.json"));
  const closure = readJson(path.join(repositoryRoot, "config", "site-dependency-closure.json"));
  const licensed = verifyNpmLicenses({
    lock,
    closure,
    approved: loadPolicy(path.join(repositoryRoot, "config", "npm-license-policy.json")),
  });
  const binaryHash = sha256File(binary);
  const browserHash = treeSha256(browserDist);
  const imageFiles = [
    "package.json", "package-lock.json", "site/package.json", "Procfile", "project.toml", "RustConfig",
    "config/site-dependency-closure.json", "config/npm-license-policy.json", "config/npm-audit-exceptions.json",
  ].map((name) => path.join(repositoryRoot, name)).filter((file) => fs.existsSync(file));
  const imageClosureHash = closureHash(imageFiles, repositoryRoot);
  const rootRef = `pkg:cargo/${encodePurlName(workspace.name)}@${encodeURIComponent(workspace.version)}`;

  const rust = makeBom({
    artifact: "rust-binary",
    commit,
    timestamp,
    rootComponent: {
      type: "application",
      "bom-ref": rootRef,
      name: workspace.name,
      version: workspace.version,
      hashes: [{alg: "SHA-256", content: binaryHash}],
      identity: {"artifact-sha256": binaryHash, "artifact-path": "wiki-econ"},
    },
    components: cargo.components,
    dependencies: cargo.dependencies,
  });
  const imageComponents = [
    ...npmComponents(licensed.inventory),
    component("application", "generic", "node", workspaceManifest.engines.node, "MIT"),
    component("application", "generic", "npm", workspaceManifest.engines.npm, "Artistic-2.0"),
  ];
  const image = makeBom({
    artifact: "toolforge-site-image-closure",
    commit,
    timestamp,
    rootComponent: {
      type: "container",
      "bom-ref": `urn:wiki-econ:toolforge-site-image:${commit}`,
      name: "wiki-econ-toolforge-site-image",
      version: commit,
      identity: {
        "artifact-sha256": imageClosureHash,
        scope: "source-runtime-and-node-dependency-closure",
        node: workspaceManifest.engines.node,
        npm: workspaceManifest.engines.npm,
      },
    },
    components: imageComponents,
    dependencies: [{ref: `urn:wiki-econ:toolforge-site-image:${commit}`, dependsOn: imageComponents.map((entry) => entry["bom-ref"]).sort()}],
  });
  const browserComponents = npmComponents(licensed.browser);
  const browser = makeBom({
    artifact: "published-browser-bundle",
    commit,
    timestamp,
    rootComponent: {
      type: "application",
      "bom-ref": `urn:wiki-econ:browser-bundle:${commit}`,
      name: "wiki-econ-browser-bundle",
      version: commit,
      hashes: [{alg: "SHA-256", content: browserHash}],
      identity: {"artifact-sha256": browserHash, "artifact-files": listFiles(browserDist).length},
    },
    components: browserComponents,
    dependencies: [{ref: `urn:wiki-econ:browser-bundle:${commit}`, dependsOn: browserComponents.map((entry) => entry["bom-ref"]).sort()}],
  });
  const notices = {
    schema_version: 1,
    source_commit: commit,
    generated_at: timestamp,
    statement: "Dependency licenses remain the property of their respective copyright holders; consult package sources for complete terms and notices.",
    rust: cargo.components.map(({name, version, licenses, purl}) => ({name, version, license: licenses[0].expression, purl})),
    toolforge_runtime: [
      {name: "node", version: workspaceManifest.engines.node, license: "MIT", purl: `pkg:generic/node@${workspaceManifest.engines.node}`},
      {name: "npm", version: workspaceManifest.engines.npm, license: "Artistic-2.0", purl: `pkg:generic/npm@${workspaceManifest.engines.npm}`},
    ],
    toolforge_image_npm: licensed.inventory,
    published_browser: licensed.browser,
  };
  return {rust, image, browser, notices};
}

function writeAtomic(file, value) {
  fs.mkdirSync(path.dirname(file), {recursive: true});
  const temporary = `${file}.tmp.${process.pid}`;
  fs.writeFileSync(temporary, `${JSON.stringify(value, null, 2)}\n`, {flag: "wx"});
  fs.renameSync(temporary, file);
}

function parseArguments(argv) {
  const options = {};
  for (let index = 0; index < argv.length; index += 2) {
    const name = argv[index];
    if (!["--binary", "--browser-dist", "--output-dir"].includes(name) || !argv[index + 1]) throw new Error("usage: generate-sboms.cjs --binary PATH --browser-dist PATH --output-dir PATH");
    options[name.slice(2)] = path.resolve(argv[index + 1]);
  }
  if (!options.binary || !options["browser-dist"] || !options["output-dir"]) throw new Error("usage: generate-sboms.cjs --binary PATH --browser-dist PATH --output-dir PATH");
  return options;
}

function main() {
  const options = parseArguments(process.argv.slice(2));
  const documents = generateDocuments({
    binary: options.binary,
    browserDist: options["browser-dist"],
    commit: process.env.WIKI_ECON_BUILD_COMMIT,
    sourceDateEpoch: process.env.SOURCE_DATE_EPOCH,
  });
  writeAtomic(path.join(options["output-dir"], "wiki-econ-rust-binary.cdx.json"), documents.rust);
  writeAtomic(path.join(options["output-dir"], "wiki-econ-toolforge-site-image.cdx.json"), documents.image);
  writeAtomic(path.join(options["output-dir"], "wiki-econ-browser-bundle.cdx.json"), documents.browser);
  writeAtomic(path.join(options["output-dir"], "third-party-notices.json"), documents.notices);
  process.stdout.write(`Generated three CycloneDX SBOMs and complete notices in ${options["output-dir"]}\n`);
}

if (require.main === module) {
  try {
    main();
  } catch (error) {
    console.error(error.stack || error.message);
    process.exitCode = 1;
  }
}

module.exports = {cargoGraph, generateDocuments, makeBom, normalizeCargoLicense, propertyValue, sha256File, treeSha256, writeAtomic};
