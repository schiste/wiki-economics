"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const {
  enforce,
  familiesForPath,
  validateDeclarations,
  versionFromSource,
} = require("./check-compute-versions.cjs");

const versions = {
  monthly: "monthly-v1",
  activity_tiers: "activity-v1",
  lifecycle: "lifecycle-v1",
  page_week: "weekly-v1",
};
const base = "0123456789abcdef0123456789abcdef01234567";

test("family paths map shared and isolated implementations", () => {
  assert.deepEqual(familiesForPath("src/compute/weekly/reconcile.rs"), ["page_week"]);
  assert.deepEqual(familiesForPath("src/compute/inequality.rs"), ["monthly"]);
  assert.deepEqual(familiesForPath("src/compute/mod.rs"), [
    "monthly",
    "activity_tiers",
    "lifecycle",
    "page_week",
  ]);
  assert.deepEqual(familiesForPath("src/patrol.rs"), []);
});

test("semantic changes require a family bump or explicit declaration", () => {
  assert.throws(
    () => enforce({
      base,
      changedFiles: ["src/compute/weekly/reconcile.rs"],
      beforeVersions: versions,
      currentVersions: versions,
      declarations: [],
    }),
    /page_week semantic sources changed/,
  );
  enforce({
    base,
    changedFiles: ["src/compute/weekly/reconcile.rs"],
    beforeVersions: versions,
    currentVersions: {...versions, page_week: "weekly-v2"},
    declarations: [],
  });
  enforce({
    base,
    changedFiles: ["src/compute/weekly/reconcile.rs"],
    beforeVersions: versions,
    currentVersions: versions,
    declarations: [{
      family: "page_week",
      paths: ["src/compute/weekly/reconcile.rs"],
      reason: "Mechanical refactor with byte-identical output.",
      base_commit: base,
    }],
  });
  assert.throws(() => enforce({
    base: "fedcba9876543210fedcba9876543210fedcba98",
    changedFiles: ["src/compute/weekly/reconcile.rs"],
    beforeVersions: versions,
    currentVersions: versions,
    declarations: [{
      family: "page_week",
      paths: ["src/compute/weekly/reconcile.rs"],
      reason: "Declaration belongs to a different exact base.",
      base_commit: base,
    }],
  }), /page_week semantic sources changed/);
});

test("patrol changes never require history-family version changes", () => {
  enforce({
    base,
    changedFiles: ["src/patrol.rs"],
    beforeVersions: versions,
    currentVersions: versions,
    declarations: [],
  });
});

test("version and declaration documents fail closed", () => {
  assert.equal(
    versionFromSource('pub(crate) const ALGORITHM_VERSION: &str = "v2";', "family.rs"),
    "v2",
  );
  assert.throws(() => versionFromSource("const OTHER: &str = \"v1\";", "family.rs"));
  assert.throws(() => validateDeclarations({schema_version: 2, declarations: []}));
  assert.throws(() => validateDeclarations({
    schema_version: 1,
    declarations: [{family: "unknown", paths: ["x"], reason: "long enough reason", base_commit: base}],
  }));
  assert.throws(() => validateDeclarations({
    schema_version: 1,
    declarations: [{family: "monthly", paths: [], reason: "long enough reason", base_commit: base}],
  }));
  assert.throws(() => validateDeclarations({
    schema_version: 1,
    declarations: [{family: "monthly", paths: ["x"], reason: "short", base_commit: base}],
  }));
  assert.throws(() => validateDeclarations({
    schema_version: 1,
    declarations: [{family: "monthly", paths: ["x"], reason: "long enough reason", base_commit: "HEAD"}],
  }));
});
