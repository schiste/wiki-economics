const assert = require("node:assert/strict");
const crypto = require("node:crypto");
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

fs.mkdirSync(path.join(fakeRoot, "node_modules", ".bin"), {recursive: true});
fs.writeFileSync(
  path.join(fakeRoot, "node_modules", ".bin", "observable"),
  `#!/bin/sh
set -eu
if [ -n "\${FAKE_NPM_LOG:-}" ]; then
  printf '%s\\n' "$*" >> "$FAKE_NPM_LOG"
fi
mkdir -p "$WIKI_ECON_SITE_DIST_DIR"
printf 'new release\\n' > "$WIKI_ECON_SITE_DIST_DIR/inequality.html"
if [ "\${FAKE_BUILD_FAIL:-0}" = 1 ]; then exit 1; fi
`,
  {mode: 0o755},
);
fs.mkdirSync(path.join(fakeRoot, "deploy", "toolforge"), {recursive: true});
fs.mkdirSync(fakeBin, {recursive: true});
fs.mkdirSync(path.join(outputDir, "nlwiki"), {recursive: true});
const browserSource = path.join(outputDir, "nlwiki", "gdp.parquet");
fs.writeFileSync(browserSource, "browser-data");
const browserBytes = fs.statSync(browserSource).size;
const browserSha256 = crypto.createHash("sha256").update(fs.readFileSync(browserSource)).digest("hex");
fs.writeFileSync(path.join(outputDir, "browser-data-index.json"), JSON.stringify({
  schema_version: 3,
  cache_schema_version: 3,
  generation: "a".repeat(64),
  license_spdx: "MIT",
  entries: [{metric: "gdp", wiki: "nlwiki", minimum_date: "2026-01", maximum_date: "2026-07",
    file: "browser-data/gdp/nlwiki.parquet", rows: 1, bytes: browserBytes, sha256: browserSha256,
    artifact_receipt_sha256: "b".repeat(64), scope: "wiki", shard: null,
    aggregation_version: null}],
}));
fs.mkdirSync(distDir, {recursive: true});
fs.writeFileSync(path.join(distDir, "inequality.html"), "old release\n");
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
printf 'new release\\n' > "$WIKI_ECON_SITE_DIST_DIR/inequality.html"
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
        WIKI_ECON_VERIFY_SITE_CLOSURE: "0",
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
  assert.equal(fs.readFileSync(path.join(distDir, "inequality.html"), "utf8"), "new release\n");

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

test("a frontend-only rebuild reuses the validated Rust dashboard defaults", () => {
  const closureBin = path.join(fixtureRoot, "closure-bin");
  const commandLog = path.join(fixtureRoot, "defaults-command.log");
  const receiptMarker = path.join(fixtureRoot, "defaults-receipt.valid");
  const defaultsDir = path.join(path.dirname(distDir), "site-dist-defaults");
  const closureWikiEcon = path.join(closureBin, "wiki-econ");
  fs.mkdirSync(closureBin, {recursive: true});
  fs.mkdirSync(path.join(fakeSite, "src"), {recursive: true});
  fs.mkdirSync(path.join(fakeSite, "vendor", "observable-cache"), {recursive: true});
  fs.writeFileSync(path.join(fakeSite, "src", "index.md"), "# Frontend\n");
  fs.writeFileSync(path.join(outputDir, "manifest.json"), "{}\n");
  fs.copyFileSync(fakeNpm, path.join(closureBin, "npm"));
  fs.chmodSync(path.join(closureBin, "npm"), 0o755);
  fs.writeFileSync(
    closureWikiEcon,
    `#!/bin/sh
set -eu
command_name=""
destination=""
for argument in "$@"; do
  case "$argument" in
    publication-verify|site-fingerprint-check|site-fingerprint-record|dashboard-materialize|dashboard-defaults-fingerprint-check|dashboard-defaults-fingerprint-record)
      command_name="$argument"
      ;;
  esac
  if [ "$command_name" = dashboard-materialize ] && [ "$argument" != --destination-dir ]; then
    case "$argument" in /*) destination="$argument" ;; esac
  fi
done
printf '%s\n' "$command_name" >> "$FAKE_DEFAULTS_COMMAND_LOG"
case "$command_name" in
  site-fingerprint-check) exit 1 ;;
  dashboard-defaults-fingerprint-check) test -f "$FAKE_DEFAULTS_RECEIPT_MARKER" ;;
  dashboard-materialize)
    mkdir -p "$destination"
    printf '{"rows":1}\n' > "$destination/defaults_inequality.json"
    ;;
  dashboard-defaults-fingerprint-record) : > "$FAKE_DEFAULTS_RECEIPT_MARKER" ;;
esac
`,
    {mode: 0o755},
  );
  fs.writeFileSync(
    path.join(closureBin, "node"),
    `#!/bin/sh
set -eu
case "$1" in
  -e) exec ${JSON.stringify(process.execPath)} "$@" ;;
  */run-record.cjs|*/publish-browser-data.cjs|*/verify-site-dependencies.cjs) exit 0 ;;
  */prepare-site-source.cjs) mkdir -p "$3" ;;
  *) exec ${JSON.stringify(process.execPath)} "$@" ;;
esac
`,
    {mode: 0o755},
  );

  const env = {
    PATH: `${closureBin}${path.delimiter}${process.env.PATH}`,
    WIKI_ECON_BIN: closureWikiEcon,
    WIKI_ECON_REQUIRE_PUBLICATION_GATE: "1",
    WIKI_ECON_VERIFY_SITE_CLOSURE: "1",
    WIKI_ECON_SITE_DEFAULTS_DIR: defaultsDir,
    WIKI_ECON_RUN_RECORD_HELPER: path.join(fakeRoot, "deploy", "toolforge", "run-record.cjs"),
    FAKE_DEFAULTS_COMMAND_LOG: commandLog,
    FAKE_DEFAULTS_RECEIPT_MARKER: receiptMarker,
  };
  const first = runBuild({...env, WIKI_ECON_RUN_ID: "defaults-first"});
  assertBuildSucceeded(first);
  assert.equal(fs.lstatSync(defaultsDir).isSymbolicLink(), true);
  fs.writeFileSync(path.join(fakeSite, "src", "index.md"), "# Frontend changed\n");

  const second = runBuild({...env, WIKI_ECON_RUN_ID: "defaults-second"});
  assertBuildSucceeded(second);
  assert.match(second.stdout, /Dashboard defaults unchanged; reusing validated bundle/);
  const commands = fs.readFileSync(commandLog, "utf8").trim().split("\n");
  assert.equal(commands.filter((command) => command === "dashboard-materialize").length, 1);
  assert.equal(commands.filter((command) => command === "dashboard-defaults-fingerprint-check").length, 1);
});

test("production refuses a network dependency install when the image is incomplete", () => {
  const incompleteRoot = path.join(fixtureRoot, "incomplete-image");
  const npmLog = path.join(fixtureRoot, "incomplete-image-npm.log");
  fs.mkdirSync(incompleteRoot, {recursive: true});

  const result = runBuild({
    FAKE_NPM_LOG: npmLog,
    WIKI_ECON_ENV: "production",
    WIKI_ECON_ROOT: incompleteRoot,
    WIKI_ECON_RUN_RECORD_HELPER: path.join(fakeRoot, "deploy", "toolforge", "run-record.cjs"),
  });

  assert.notEqual(result.status, 0);
  assert.match(result.stderr, /dependencies are missing from the production image/);
  assert.equal(fs.existsSync(npmLog), false);
});
