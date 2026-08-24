"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const {spawnSync} = require("node:child_process");
const test = require("node:test");

const script = fs.readFileSync(path.join(__dirname, "run-publish-ready.sh"), "utf8");
const lockScript = path.join(__dirname, "run-with-lock.sh");

test("publisher run records resolve the authoritative published wiki set", () => {
  assert.match(script, /wiki-lifecycle\.cjs" published-wikis/);
  assert.match(script, /published_wikis\[@\]/);
  assert.doesNotMatch(script, /JSON\.stringify\(process\.argv\.slice\(1\)\)' nlwiki ptwiki frwiki/);
});

test("publisher uses a lock path accepted and released by the lock helper", (context) => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "wiki-econ-publisher-lock-"));
  context.after(() => fs.rmSync(directory, {recursive: true, force: true}));
  const lock = path.join(directory, ".publication.lock");
  assert.match(script, /WIKI_ECON_OUTPUT_DIR\/\.publication\.lock/);

  const result = spawnSync("bash", [lockScript, lock, "publication", "60", "true"], {
    encoding: "utf8",
    env: {...process.env, WIKI_ECON_RUN_ID: "publisher-lock-test", WIKI_ECON_LOCK_HEARTBEAT_SECS: "1"},
  });
  assert.equal(result.status, 0, result.stderr || result.stdout);
  assert.equal(fs.existsSync(lock), false);
});
