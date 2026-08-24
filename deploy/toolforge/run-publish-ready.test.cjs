"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");

const script = fs.readFileSync(path.join(__dirname, "run-publish-ready.sh"), "utf8");

test("publisher run records resolve the authoritative published wiki set", () => {
  assert.match(script, /wiki-lifecycle\.cjs" published-wikis/);
  assert.match(script, /published_wikis\[@\]/);
  assert.doesNotMatch(script, /JSON\.stringify\(process\.argv\.slice\(1\)\)' nlwiki ptwiki frwiki/);
});
