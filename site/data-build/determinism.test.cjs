"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const {test} = require("node:test");

test("wiki range metadata queries have explicit deterministic ordering", () => {
  const scripts = fs.readdirSync(__dirname)
    .filter((name) => /^(defaults|meta)_.+\.json\.cjs$/.test(name))
    .sort();
  let rangeQueries = 0;
  for (const script of scripts) {
    const source = fs.readFileSync(path.join(__dirname, script), "utf8");
    for (const match of source.matchAll(/SELECT wiki, MIN\([\s\S]+?GROUP BY wiki([^`]*)`/g)) {
      rangeQueries += 1;
      assert.match(match[1], /ORDER BY wiki/, `${script} range metadata must be ordered by wiki`);
    }
  }
  assert.equal(rangeQueries, 10);
});
