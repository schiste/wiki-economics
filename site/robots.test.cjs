"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const {test} = require("node:test");

const robotsPath = path.join(__dirname, "src", "robots.txt");

function directives(contents) {
  return contents
    .split(/\r?\n/)
    .map((line) => line.replace(/#.*$/, "").trim())
    .filter(Boolean);
}

test("robots policy permits public pages while protecting expensive and operational paths", () => {
  const policy = directives(fs.readFileSync(robotsPath, "utf8"));

  assert.deepEqual(policy.slice(0, 2), ["User-agent: *", "Allow: /"]);
  for (const pathRule of [
    "/admin",
    "/admin-api/",
    "/health/",
    "/browser-data/",
    "/data/",
    "/*?*",
  ]) {
    assert.ok(policy.includes(`Disallow: ${pathRule}`), `missing robots exclusion for ${pathRule}`);
  }
  assert.ok(policy.includes("Crawl-delay: 10"));
  assert.equal(policy.some((line) => line === "Disallow: /"), false);
});
