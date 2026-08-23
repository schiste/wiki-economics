"use strict";

const assert = require("node:assert/strict");
const {test} = require("node:test");
const {lockInventory, packageName, verifyNpmLicenses} = require("./check-npm-licenses.cjs");

function fixture(license = "MIT") {
  return {
    lockfileVersion: 3,
    packages: {
      "": {name: "root", workspaces: ["site"]},
      site: {name: "dashboard", link: true},
      "node_modules/@scope/pkg": {version: "1.2.3", license},
      "site/node_modules/plain": {version: "2.0.0", license: "ISC"},
    },
  };
}

test("inventory identifies scoped and nested packages without treating workspace links as dependencies", () => {
  assert.equal(packageName("node_modules/@scope/pkg", {}), "@scope/pkg");
  assert.deepEqual(lockInventory(fixture()), [
    {name: "@scope/pkg", version: "1.2.3", license: "MIT"},
    {name: "plain", version: "2.0.0", license: "ISC"},
  ]);
});

test("license and browser closure checks fail closed", () => {
  const closure = {schema_version: 1, generated_packages: {"@scope/pkg": "1.2.3"}};
  const result = verifyNpmLicenses({lock: fixture(), closure, approved: new Set(["MIT", "ISC"])});
  assert.equal(result.inventory.length, 2);
  assert.equal(result.browser.length, 1);
  assert.throws(
    () => verifyNpmLicenses({lock: fixture("GPL-3.0-only"), closure, approved: new Set(["MIT", "ISC"])}),
    /unapproved license GPL-3.0-only/,
  );
  assert.throws(
    () => verifyNpmLicenses({lock: fixture(), closure: {schema_version: 1, generated_packages: {missing: "9.9.9"}}, approved: new Set(["MIT", "ISC"])}),
    /absent from package-lock.json/,
  );
});

test("truncated license metadata is never accepted", () => {
  assert.throws(() => lockInventory(fixture("")), /has no SPDX license expression/);
});
