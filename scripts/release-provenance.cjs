#!/usr/bin/env node
"use strict";

const crypto = require("node:crypto");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const {spawnSync} = require("node:child_process");
const {expectedVersions, verifyCurrentRuntime} = require("./verify-runtime.cjs");

const root = path.resolve(__dirname, "..");

function readJson(file) {
  return JSON.parse(fs.readFileSync(file, "utf8"));
}

function sha256(file) {
  const hash = crypto.createHash("sha256");
  hash.update(fs.readFileSync(file));
  return hash.digest("hex");
}

function optionalCommand(command, args) {
  const result = spawnSync(command, args, {encoding: "utf8", env: {...process.env, LC_ALL: "C"}});
  return result.status === 0 ? result.stdout.trim() : null;
}

function lockedVersion(lock, name) {
  return lock.packages?.[`site/node_modules/${name}`]?.version
    || lock.packages?.[`node_modules/${name}`]?.version
    || null;
}

function browserVersions(repositoryRoot = root) {
  const workspace = readJson(path.join(repositoryRoot, "package.json"));
  const site = readJson(path.join(repositoryRoot, "site", "package.json"));
  const lock = readJson(path.join(repositoryRoot, "package-lock.json"));
  const closure = readJson(path.join(repositoryRoot, "config", "site-dependency-closure.json"));
  const direct = {};
  for (const [name, version] of Object.entries(site.dependencies || {}).sort(([left], [right]) => left.localeCompare(right))) {
    const locked = lockedVersion(lock, name);
    if (locked !== version) throw new Error(`release provenance found ${name}@${locked || "missing"}; expected ${version}`);
    direct[name] = version;
  }
  const buildTools = {};
  for (const [name, version] of Object.entries(workspace.dependencies || {}).sort(([left], [right]) => left.localeCompare(right))) {
    const locked = lockedVersion(lock, name);
    if (locked !== version) throw new Error(`release provenance found ${name}@${locked || "missing"}; expected ${version}`);
    buildTools[name] = version;
  }
  return {build_tools: buildTools, direct, generated: closure.generated_packages};
}

const SBOMS = {
  rust_binary: ["wiki-econ-rust-binary.cdx.json", "rust-binary"],
  toolforge_site_image: ["wiki-econ-toolforge-site-image.cdx.json", "toolforge-site-image-closure"],
  published_browser_bundle: ["wiki-econ-browser-bundle.cdx.json", "published-browser-bundle"],
};

function sbomProperty(document, name) {
  return document?.metadata?.component?.properties?.find((entry) => entry.name === `org.wikimedia.toolforge.wiki-econ.${name}`)?.value || null;
}

function supplyChainArtifacts(directory, sourceCommit) {
  if (!directory) throw new Error("supply-chain directory is required");
  const sboms = {};
  for (const [key, [name, artifact]] of Object.entries(SBOMS)) {
    const file = path.join(directory, name);
    const document = readJson(file);
    if (document.bomFormat !== "CycloneDX" || document.specVersion !== "1.6"
        || sbomProperty(document, "artifact") !== artifact
        || sbomProperty(document, "source-commit") !== sourceCommit
        || !/^[0-9a-f]{64}$/.test(sbomProperty(document, "artifact-sha256") || "")) {
      throw new Error(`${name} does not identify ${artifact} at ${sourceCommit}`);
    }
    sboms[key] = {file: name, sha256: sha256(file), artifact, artifact_sha256: sbomProperty(document, "artifact-sha256")};
  }
  const noticesName = "third-party-notices.json";
  const noticesFile = path.join(directory, noticesName);
  const notices = readJson(noticesFile);
  if (notices.schema_version !== 1 || notices.source_commit !== sourceCommit) throw new Error(`${noticesName} does not match ${sourceCommit}`);
  const humanName = "THIRD_PARTY_NOTICES.md";
  const humanFile = path.join(directory, humanName);
  if (!fs.statSync(humanFile, {throwIfNoEntry: false})?.isFile()) throw new Error(`${humanName} is missing`);
  return {
    sbom_format: "CycloneDX 1.6",
    sboms,
    notices: {
      machine_readable: {file: noticesName, sha256: sha256(noticesFile)},
      human_readable: {file: humanName, sha256: sha256(humanFile)},
    },
  };
}

function systemVersions(binary) {
  const osRelease = fs.statSync("/etc/os-release", {throwIfNoEntry: false})?.isFile()
    ? Object.fromEntries(fs.readFileSync("/etc/os-release", "utf8").trim().split("\n").map((line) => {
      const separator = line.indexOf("=");
      return [line.slice(0, separator), line.slice(separator + 1).replace(/^"|"$/g, "")];
    }))
    : {};
  const packageNames = ["libc6", "libgcc-s1", "libssl3", "libssl3t64", "libstdc++6", "zlib1g"];
  const packages = {};
  for (const packageName of packageNames) {
    const packageOutput = optionalCommand("dpkg-query", ["-W", "-f=${binary:Package}\t${Version}", packageName]);
    if (packageOutput) {
      const [name, version] = packageOutput.split("\t");
      if (name && version) packages[name] = version;
    }
  }
  return {
    platform: process.platform,
    architecture: process.arch,
    kernel_release: os.release(),
    os_release: osRelease,
    glibc: optionalCommand("ldd", ["--version"])?.split("\n")[0] || null,
    packages,
    binary_dynamic_libraries: binary && process.platform === "linux" ? optionalCommand("ldd", [binary])?.split("\n").filter(Boolean).sort() || [] : [],
  };
}

function buildReleaseProvenance({binary, sourceCommit, sourceDateEpoch, repositoryRoot = root}) {
  if (!/^[0-9a-f]{40}$/.test(sourceCommit || "")) throw new Error("WIKI_ECON_BUILD_COMMIT must be an exact 40-character commit");
  if (!/^\d+$/.test(String(sourceDateEpoch || ""))) throw new Error("SOURCE_DATE_EPOCH is required for deterministic provenance");
  if (!fs.statSync(binary, {throwIfNoEntry: false})?.isFile()) throw new Error(`release binary is missing: ${binary}`);
  const runtime = verifyCurrentRuntime(expectedVersions());
  const browser = browserVersions(repositoryRoot);
  return {
    schema_version: 1,
    source_commit: sourceCommit,
    generated_at: new Date(Number(sourceDateEpoch) * 1000).toISOString(),
    binary: {
      name: path.basename(binary),
      bytes: fs.statSync(binary).size,
      sha256: sha256(binary),
    },
    runtime,
    browser_packages: browser,
    dependency_manifests: {
      cargo_lock_sha256: sha256(path.join(repositoryRoot, "Cargo.lock")),
      npm_lock_sha256: sha256(path.join(repositoryRoot, "package-lock.json")),
      browser_closure_sha256: sha256(path.join(repositoryRoot, "config", "site-dependency-closure.json")),
    },
    system: systemVersions(binary),
  };
}

function writeAtomic(file, value) {
  fs.mkdirSync(path.dirname(file), {recursive: true});
  const temporary = `${file}.tmp.${process.pid}`;
  fs.writeFileSync(temporary, `${JSON.stringify(value, null, 2)}\n`, {flag: "wx"});
  fs.renameSync(temporary, file);
}

function main() {
  const binaryIndex = process.argv.indexOf("--binary");
  const outputIndex = process.argv.indexOf("--output");
  if (binaryIndex < 0 || outputIndex < 0 || !process.argv[binaryIndex + 1] || !process.argv[outputIndex + 1]) {
    throw new Error("usage: release-provenance.cjs --binary PATH --output PATH");
  }
  const binary = path.resolve(process.argv[binaryIndex + 1]);
  const output = path.resolve(process.argv[outputIndex + 1]);
  const provenance = buildReleaseProvenance({
    binary,
    sourceCommit: process.env.WIKI_ECON_BUILD_COMMIT,
    sourceDateEpoch: process.env.SOURCE_DATE_EPOCH,
  });
  writeAtomic(output, provenance);
  process.stdout.write(`Recorded deterministic release provenance: ${output}\n`);
}

if (require.main === module) {
  try {
    main();
  } catch (error) {
    console.error(error.stack || error.message);
    process.exitCode = 1;
  }
}

module.exports = {browserVersions, buildReleaseProvenance, lockedVersion, sha256, systemVersions, writeAtomic};
