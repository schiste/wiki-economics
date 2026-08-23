#!/usr/bin/env node
"use strict";

const fs = require("node:fs");
const path = require("node:path");
const {execFileSync} = require("node:child_process");

const root = path.resolve(__dirname, "..");

function read(file) {
  return fs.readFileSync(path.join(root, file), "utf8").trim();
}

function fail(message) {
  throw new Error(`runtime closure violation: ${message}`);
}

function commandVersion(command, args = ["--version"]) {
  return execFileSync(command, args, {encoding: "utf8"}).trim().replace(/^v/, "");
}

function expectedVersions() {
  const manifest = JSON.parse(read("package.json"));
  const node = manifest.engines?.node;
  const npm = manifest.engines?.npm;
  if (!/^\d+\.\d+\.\d+$/.test(node || "")) fail("engines.node must be exact");
  if (!/^\d+\.\d+\.\d+$/.test(npm || "")) fail("engines.npm must be exact");
  if (manifest.packageManager !== `npm@${npm}`) fail("packageManager and engines.npm differ");
  if (manifest.volta?.node !== node || manifest.volta?.npm !== npm) fail("Volta pins differ from engines");
  if (read(".node-version") !== node || read(".nvmrc") !== node) fail("local Node pins differ from engines.node");

  const rustToolchain = read("rust-toolchain.toml").match(/channel\s*=\s*"([^"]+)"/)?.[1];
  const rustConfig = read("RustConfig").match(/^VERSION=([^\s]+)$/m)?.[1];
  const cargoRust = read("Cargo.toml").match(/^rust-version\s*=\s*"([^"]+)"/m)?.[1];
  if (!rustToolchain || rustToolchain !== rustConfig || rustToolchain !== cargoRust) {
    fail("RustConfig, rust-toolchain.toml, and Cargo.toml must pin the same Rust version");
  }
  return {node, npm, rust: rustToolchain};
}

function verifyCurrentRuntime(expected = expectedVersions()) {
  const actual = {
    node: process.versions.node,
    npm: commandVersion("npm"),
    rust: commandVersion("rustc").split(/\s+/)[1],
  };
  for (const name of Object.keys(expected)) {
    if (actual[name] !== expected[name]) fail(`${name} ${actual[name]} is running; expected ${expected[name]}`);
  }
  return actual;
}

function main() {
  const versions = verifyCurrentRuntime();
  process.stdout.write(`Verified runtime closure: Node ${versions.node}, npm ${versions.npm}, Rust ${versions.rust}.\n`);
}

if (require.main === module) {
  try {
    main();
  } catch (error) {
    console.error(error.message);
    process.exitCode = 1;
  }
}

module.exports = {commandVersion, expectedVersions, verifyCurrentRuntime};
