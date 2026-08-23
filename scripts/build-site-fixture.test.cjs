"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const {after, test} = require("node:test");
const {
  REQUIRED_ATTACHMENTS,
  REQUIRED_PAGES,
  listFiles,
  parseArguments,
  verifyBuild,
} = require("./build-site-fixture.cjs");

const root = fs.mkdtempSync(path.join(os.tmpdir(), "wiki-econ-built-site-"));
after(() => fs.rmSync(root, {recursive: true, force: true}));

test("built site verification requires every page and hashed attachment", () => {
  const dist = path.join(root, "complete");
  const data = path.join(dist, "_file", "data");
  fs.mkdirSync(data, {recursive: true});
  for (const page of REQUIRED_PAGES) {
    fs.writeFileSync(path.join(dist, page), '<!doctype html><a href="/legal">Legal</a>');
  }
  for (const attachment of REQUIRED_ATTACHMENTS) {
    const extension = path.extname(attachment);
    const stem = attachment.slice(0, -extension.length);
    const destination = path.join(data, `${stem}.deadbeef${extension}`);
    if (attachment === "manifest.json") {
      fs.writeFileSync(destination, JSON.stringify({
        schema_version: 3,
        license: {spdx_identifier: "MIT"},
        provenance: {run_id: "fixture"},
        source_datasets: [{id: "mediawiki_history"}],
        trademark: {status: "No trademark license is recorded"},
        toolforge_open_licensing: {
          open_source_license_spdx: "MIT",
          open_data_license_spdx: "MIT",
        },
        downloadable_artifacts: REQUIRED_ATTACHMENTS
          .filter((name) => name !== "manifest.json")
          .map((name) => ({name, license_spdx: "MIT"})),
      }));
    } else {
      fs.writeFileSync(destination, "fixture");
    }
  }

  assert.deepEqual(verifyBuild(dist), listFiles(dist));
  const manifestPath = path.join(data, "manifest.deadbeef.json");
  const manifest = JSON.parse(fs.readFileSync(manifestPath, "utf8"));
  manifest.downloadable_artifacts = manifest.downloadable_artifacts
    .filter((artifact) => artifact.name !== "defaults_gdp.json");
  fs.writeFileSync(manifestPath, JSON.stringify(manifest));
  assert.throws(() => verifyBuild(dist), /downloadable artifact without a discoverable MIT license/);
  manifest.downloadable_artifacts.push({name: "defaults_gdp.json", license_spdx: "MIT"});
  fs.writeFileSync(manifestPath, JSON.stringify(manifest));
  fs.rmSync(path.join(data, "meta_patrol.deadbeef.json"));
  assert.throws(() => verifyBuild(dist), /missing data attachment meta_patrol.json/);
});

test("built site verification fails closed on legal and manifest regressions", () => {
  const dist = path.join(root, "invalid-legal");
  const data = path.join(dist, "_file", "data");
  fs.mkdirSync(data, {recursive: true});
  for (const page of REQUIRED_PAGES) fs.writeFileSync(path.join(dist, page), '<a href="/legal">Legal</a>');
  for (const attachment of REQUIRED_ATTACHMENTS) {
    const extension = path.extname(attachment);
    const stem = attachment.slice(0, -extension.length);
    fs.writeFileSync(path.join(data, `${stem}.deadbeef${extension}`), attachment === "manifest.json" ? "{}" : "fixture");
  }

  fs.writeFileSync(path.join(dist, "index.html"), "<!doctype html>");
  assert.throws(() => verifyBuild(dist), /index.html has no one-click legal link/);
  fs.writeFileSync(path.join(dist, "index.html"), '<a href="/legal">Legal</a>');
  assert.throws(() => verifyBuild(dist), /incomplete licensing or provenance manifest/);
});

test("fixture CLI arguments are strict and absolute", () => {
  const options = parseArguments(["--data-dir", "data", "--dist-dir", "dist"]);
  assert.equal(options["data-dir"], path.resolve("data"));
  assert.equal(options["dist-dir"], path.resolve("dist"));
  assert.throws(() => parseArguments(["--unknown", "value"]), /unknown argument/);
  assert.throws(() => parseArguments(["--data-dir", "data"]), /usage/);
});
