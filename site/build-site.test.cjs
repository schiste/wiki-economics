const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const {spawnSync} = require("node:child_process");
const {after, test} = require("node:test");

const repoRoot = path.resolve(__dirname, "..");
const fixtureRoot = fs.mkdtempSync(path.join(os.tmpdir(), "wiki-econ-site-build-"));
const fakeRoot = path.join(fixtureRoot, "repo");
const fakeSite = path.join(fakeRoot, "site");
const fakeBin = path.join(fixtureRoot, "bin");
const outputDir = path.join(fakeRoot, "output");
const distDir = path.join(fixtureRoot, "published", "site-dist");
const stageEvents = path.join(fixtureRoot, "site-stage-events.jsonl");

fs.mkdirSync(path.join(fakeSite, "node_modules"), {recursive: true});
fs.mkdirSync(path.join(fakeRoot, "deploy", "toolforge"), {recursive: true});
fs.mkdirSync(fakeBin, {recursive: true});
fs.mkdirSync(distDir, {recursive: true});
fs.writeFileSync(path.join(distDir, "index.html"), "old release\n");
fs.copyFileSync(
  path.join(repoRoot, "deploy", "toolforge", "run-record.cjs"),
  path.join(fakeRoot, "deploy", "toolforge", "run-record.cjs"),
);

const fakeNpm = path.join(fakeBin, "npm");
fs.writeFileSync(
  fakeNpm,
  `#!/bin/sh
set -eu
if [ -n "\${FAKE_NPM_LOG:-}" ]; then
  printf '%s\n' "$*" >> "$FAKE_NPM_LOG"
fi
mkdir -p "$WIKI_ECON_SITE_DIST_DIR"
printf 'new release\\n' > "$WIKI_ECON_SITE_DIST_DIR/index.html"
if [ "\${FAKE_BUILD_FAIL:-0}" = 1 ]; then
  exit 1
fi
`,
  {mode: 0o755},
);

const fakeWikiEcon = path.join(fakeBin, "wiki-econ");
fs.writeFileSync(fakeWikiEcon, "#!/bin/sh\nexit 0\n", {mode: 0o755});

after(() => fs.rmSync(fixtureRoot, {recursive: true, force: true}));

function runBuild(extraEnv = {}) {
  return spawnSync(
    path.join(repoRoot, "scripts", "build-site.sh"),
    ["--output-dir", outputDir, "--dist-dir", distDir],
    {
      cwd: repoRoot,
      encoding: "utf8",
      env: {
        ...process.env,
        PATH: `${fakeBin}${path.delimiter}${process.env.PATH}`,
        WIKI_ECON_ROOT: fakeRoot,
        WIKI_ECON_RUN_EVENTS_FILE: stageEvents,
        WIKI_ECON_SITE_DIR: fakeSite,
        ...extraEnv,
      },
    },
  );
}

function assertBuildSucceeded(result) {
  assert.equal(result.status, 0, `${result.stdout}\n${result.stderr}`);
}

test("site builds are switched atomically and failed staging is discarded", () => {
  const first = runBuild();
  assertBuildSucceeded(first);
  assert.equal(fs.lstatSync(distDir).isSymbolicLink(), true);
  const firstTarget = fs.readlinkSync(distDir);
  assert.match(firstTarget, /^\.site-dist\.build\./);
  assert.equal(fs.readFileSync(path.join(distDir, "index.html"), "utf8"), "new release\n");

  const second = runBuild();
  assertBuildSucceeded(second);
  const secondTarget = fs.readlinkSync(distDir);
  assert.notEqual(secondTarget, firstTarget);
  assert.equal(fs.existsSync(path.join(path.dirname(distDir), firstTarget)), false);

  const failed = runBuild({FAKE_BUILD_FAIL: "1"});
  assert.notEqual(failed.status, 0);
  assert.equal(fs.readlinkSync(distDir), secondTarget);

  const siblings = fs.readdirSync(path.dirname(distDir));
  assert.deepEqual(siblings.sort(), [secondTarget, path.basename(distDir)].sort());
  const events = fs.readFileSync(stageEvents, "utf8").trim().split("\n").map(JSON.parse);
  assert.equal(events.filter((event) => event.event === "started").length, 3);
  assert.equal(events.filter((event) => event.event === "completed").length, 2);
  assert.equal(events.filter((event) => event.event === "failed").length, 1);
  assert.equal(events.at(-1).stage, "site");
});

test("a reusable site skips Node dependency installation", () => {
  const cacheHitSite = path.join(fakeRoot, "cache-hit-site");
  const npmLog = path.join(fixtureRoot, "cache-hit-npm.log");
  fs.mkdirSync(cacheHitSite, {recursive: true});

  const result = runBuild({
    FAKE_NPM_LOG: npmLog,
    WIKI_ECON_BIN: fakeWikiEcon,
    WIKI_ECON_REQUIRE_PUBLICATION_GATE: "1",
    WIKI_ECON_RUN_ID: "cache-hit-run",
    WIKI_ECON_SITE_DIR: cacheHitSite,
  });

  assertBuildSucceeded(result);
  assert.match(result.stdout, /Site inputs unchanged; reusing published site/);
  assert.equal(fs.existsSync(npmLog), false);
  assert.equal(fs.existsSync(path.join(cacheHitSite, "node_modules")), false);
  const events = fs.readFileSync(stageEvents, "utf8").trim().split("\n").map(JSON.parse);
  assert.equal(events.at(-2).event, "reused");
  assert.equal(events.at(-1).event, "completed");
});
