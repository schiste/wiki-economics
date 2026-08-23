#!/usr/bin/env node
"use strict";

const crypto = require("node:crypto");
const fs = require("node:fs");
const path = require("node:path");
const {sbomProperty} = require("./release-provenance.cjs");

const PAYLOAD = [
  "THIRD_PARTY_NOTICES.md",
  "release-provenance.json",
  "third-party-notices.json",
  "wiki-econ",
  "wiki-econ-browser-bundle.cdx.json",
  "wiki-econ-rust-binary.cdx.json",
  "wiki-econ-toolforge-site-image.cdx.json",
];

function sha256(file) {
  return crypto.createHash("sha256").update(fs.readFileSync(file)).digest("hex");
}

function regularFile(directory, name) {
  const file = path.join(directory, name);
  const stat = fs.lstatSync(file, {throwIfNoEntry: false});
  if (!stat?.isFile() || stat.isSymbolicLink()) throw new Error(`release payload is missing a regular ${name}`);
  return file;
}

function checksumDocument(directory) {
  return `${PAYLOAD.map((name) => `${sha256(regularFile(directory, name))}  ${name}`).join("\n")}\n`;
}

function writeChecksums(directory) {
  const destination = path.join(directory, "SHA256SUMS");
  const temporary = `${destination}.tmp.${process.pid}`;
  fs.writeFileSync(temporary, checksumDocument(directory), {flag: "wx"});
  fs.renameSync(temporary, destination);
  return destination;
}

function parseChecksums(content) {
  const entries = new Map();
  for (const line of content.trimEnd().split("\n")) {
    const match = line.match(/^([0-9a-f]{64})  ([A-Za-z0-9_.-]+)$/);
    if (!match || entries.has(match[2])) throw new Error(`invalid or duplicate SHA256SUMS line: ${line}`);
    entries.set(match[2], match[1]);
  }
  return entries;
}

function verifyReleaseBundle(directory, expectedCommit) {
  if (!/^[0-9a-f]{40}$/.test(expectedCommit || "")) throw new Error("expected commit must be an exact 40-character SHA");
  const manifest = parseChecksums(fs.readFileSync(regularFile(directory, "SHA256SUMS"), "utf8"));
  if (manifest.size !== PAYLOAD.length || PAYLOAD.some((name) => !manifest.has(name))) throw new Error("SHA256SUMS does not contain the exact release payload");
  for (const name of PAYLOAD) {
    const actual = sha256(regularFile(directory, name));
    if (actual !== manifest.get(name)) throw new Error(`checksum mismatch for ${name}`);
  }

  const provenance = JSON.parse(fs.readFileSync(path.join(directory, "release-provenance.json")));
  if (provenance.schema_version !== 2 || provenance.source_commit !== expectedCommit
      || provenance.binary.sha256 !== manifest.get("wiki-econ")) {
    throw new Error("release provenance does not match the commit and binary");
  }
  const expectedSboms = {
    rust_binary: ["wiki-econ-rust-binary.cdx.json", "rust-binary"],
    toolforge_site_image: ["wiki-econ-toolforge-site-image.cdx.json", "toolforge-site-image-closure"],
    published_browser_bundle: ["wiki-econ-browser-bundle.cdx.json", "published-browser-bundle"],
  };
  for (const [key, [name, artifact]] of Object.entries(expectedSboms)) {
    const declared = provenance.supply_chain?.sboms?.[key];
    const sbom = JSON.parse(fs.readFileSync(path.join(directory, name)));
    if (declared?.file !== name || declared?.sha256 !== manifest.get(name) || declared?.artifact !== artifact
        || sbom.bomFormat !== "CycloneDX" || sbom.specVersion !== "1.6"
        || sbomProperty(sbom, "artifact") !== artifact || sbomProperty(sbom, "source-commit") !== expectedCommit
        || declared.artifact_sha256 !== sbomProperty(sbom, "artifact-sha256")) {
      throw new Error(`${name} failed provenance or artifact identity validation`);
    }
  }
  if (provenance.supply_chain.sboms.rust_binary.artifact_sha256 !== provenance.binary.sha256) {
    throw new Error("Rust SBOM artifact hash does not match the release binary");
  }
  const machine = provenance.supply_chain?.notices?.machine_readable;
  const human = provenance.supply_chain?.notices?.human_readable;
  if (machine?.file !== "third-party-notices.json" || machine.sha256 !== manifest.get(machine.file)
      || human?.file !== "THIRD_PARTY_NOTICES.md" || human.sha256 !== manifest.get(human.file)) {
    throw new Error("third-party notices do not match release provenance");
  }
  const notices = JSON.parse(fs.readFileSync(path.join(directory, machine.file)));
  if (notices.schema_version !== 1 || notices.source_commit !== expectedCommit
      || !Array.isArray(notices.rust) || notices.rust.length === 0
      || !Array.isArray(notices.toolforge_runtime) || notices.toolforge_runtime.length === 0
      || !Array.isArray(notices.toolforge_image_npm) || notices.toolforge_image_npm.length === 0
      || !Array.isArray(notices.published_browser) || notices.published_browser.length === 0) {
    throw new Error("machine-readable third-party notices are incomplete or identify another commit");
  }
  return {files: PAYLOAD.length + 1, commit: expectedCommit, binarySha256: manifest.get("wiki-econ")};
}

function parseArguments(argv) {
  if (argv.length !== 3 || !["--write", "--verify"].includes(argv[0])) {
    throw new Error("usage: release-bundle.cjs (--write|--verify) DIRECTORY COMMIT");
  }
  return {mode: argv[0], directory: path.resolve(argv[1]), commit: argv[2]};
}

function main() {
  const {mode, directory, commit} = parseArguments(process.argv.slice(2));
  if (mode === "--write") writeChecksums(directory);
  const result = verifyReleaseBundle(directory, commit);
  process.stdout.write(`Verified release bundle for ${result.commit} (${result.files} files).\n`);
}

if (require.main === module) {
  try {
    main();
  } catch (error) {
    console.error(error.stack || error.message);
    process.exitCode = 1;
  }
}

module.exports = {PAYLOAD, checksumDocument, parseChecksums, sha256, verifyReleaseBundle, writeChecksums};
