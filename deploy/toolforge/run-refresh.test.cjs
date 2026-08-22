const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const {spawn, spawnSync} = require("node:child_process");
const {after, test} = require("node:test");

const repoRoot = path.resolve(__dirname, "../..");
const wrapper = path.join(repoRoot, "deploy", "toolforge", "run-refresh.sh");
const fixtureRoot = fs.mkdtempSync(path.join(os.tmpdir(), "wiki-econ-refresh-lock-"));

after(() => fs.rmSync(fixtureRoot, {recursive: true, force: true}));

function writeExecutable(file, contents) {
  fs.mkdirSync(path.dirname(file), {recursive: true});
  fs.writeFileSync(file, contents, {mode: 0o755});
}

function createFixture(name) {
  const root = path.join(fixtureRoot, name);
  const output = path.join(root, "output");
  const siteDist = path.join(root, "site", "dist");
  const ready = path.join(root, "driver-ready");
  const release = path.join(root, "driver-release");
  const driverArgs = path.join(root, "driver-args");
  const fakeBinary = path.join(root, "bin", "wiki-econ");
  const fakeDriver = path.join(root, "bin", "refresh-driver");
  fs.mkdirSync(output, {recursive: true});

  writeExecutable(
    fakeBinary,
    `#!/bin/sh
set -eu
case " $* " in
  *" snapshot-resolve "*) printf '%s\\n' "\${FAKE_SNAPSHOT:-2026-07}" ;;
  *) echo "unexpected fake wiki-econ invocation: $*" >&2; exit 2 ;;
esac
`,
  );

  writeExecutable(
    fakeDriver,
    `#!/bin/sh
set -eu
if [ -n "\${FAKE_DRIVER_ARGS:-}" ]; then
  printf '%s\n' "$*" > "$FAKE_DRIVER_ARGS"
fi
if [ -n "\${FAKE_DRIVER_READY:-}" ]; then
  : > "$FAKE_DRIVER_READY"
fi
while [ -n "\${FAKE_DRIVER_RELEASE:-}" ] && [ ! -f "$FAKE_DRIVER_RELEASE" ]; do
  sleep 0.05
done
if [ "\${FAKE_REFRESH_EXIT:-0}" -ne 0 ]; then
  exit "$FAKE_REFRESH_EXIT"
fi
mkdir -p "$WIKI_ECON_OUTPUT_DIR" "$WIKI_ECON_SITE_DIST_DIR"
for artifact in \\
  manifest.json defaults_business.json defaults_gdp.json defaults_inequality.json \\
  defaults_labor.json defaults_patrol.json defaults_edit_variation.json \\
  business_funnel.parquet gdp.parquet gdp_activity_tiers.parquet \\
  gdp_user_type_share.parquet inequality.parquet labor_churn.parquet \\
  labor_cohorts.parquet labor_monthly.parquet page_weekly_edits.parquet patrol.parquet
do
  : > "$WIKI_ECON_OUTPUT_DIR/$artifact"
done
for page in index.html business.html gdp.html inequality.html labor.html patrol.html edit-variation.html; do
  : > "$WIKI_ECON_SITE_DIST_DIR/$page"
done
`,
  );

  const env = {
    ...process.env,
    WIKI_ECON_BIN: fakeBinary,
    WIKI_ECON_DATA_DIR: path.join(root, "data"),
    WIKI_ECON_ENV: "test",
    WIKI_ECON_JOB_IDENTITY: "test-toolforge-job",
    WIKI_ECON_PROCESS_IDENTITY: "test-toolforge-process",
    WIKI_ECON_OUTPUT_DIR: output,
    WIKI_ECON_REFRESH_DRIVER: fakeDriver,
    WIKI_ECON_REFRESH_LOCK_HEARTBEAT_SECS: "1",
    WIKI_ECON_REFRESH_LOCK_RECHECK_SECS: "0",
    WIKI_ECON_REFRESH_LOCK_STALE_SECS: "3600",
    WIKI_ECON_ROOT: root,
    WIKI_ECON_SITE_DIST_DIR: siteDist,
    WIKI_ECON_SITE_DIR: path.join(root, "site"),
    WIKI_ECON_WIKI_LIFECYCLE_FILE: path.join(repoRoot, "config", "wiki-lifecycle.json"),
  };
  return {driverArgs, env, output, ready, release};
}

function runFixture(fixture, extraEnv = {}) {
  return spawnSync(wrapper, [], {
    cwd: repoRoot,
    encoding: "utf8",
    env: {...fixture.env, ...extraEnv},
  });
}

async function waitForFile(file, timeoutMs = 5000) {
  const deadline = Date.now() + timeoutMs;
  while (!fs.existsSync(file)) {
    assert.ok(Date.now() < deadline, `timed out waiting for ${file}`);
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
}

function collectChild(child) {
  return new Promise((resolve) => {
    let stdout = "";
    let stderr = "";
    child.stdout.on("data", (chunk) => { stdout += chunk; });
    child.stderr.on("data", (chunk) => { stderr += chunk; });
    child.on("close", (status, signal) => resolve({status, signal, stdout, stderr}));
  });
}

test("an active refresh owns metadata, rejects overlap, and releases cleanly", async () => {
  const fixture = createFixture("active");
  const first = spawn(wrapper, [], {
    cwd: repoRoot,
    env: {
      ...fixture.env,
      FAKE_DRIVER_ARGS: fixture.driverArgs,
      FAKE_DRIVER_READY: fixture.ready,
      FAKE_DRIVER_RELEASE: fixture.release,
      WIKI_ECON_RUN_ID: "active-run",
    },
    stdio: ["ignore", "pipe", "pipe"],
  });
  const firstResult = collectChild(first);
  await waitForFile(fixture.ready);

  const lockDir = path.join(fixture.output, ".refresh-lock");
  const owner = JSON.parse(fs.readFileSync(path.join(lockDir, "owner.json"), "utf8"));
  assert.equal(owner.run_id, "active-run");
  assert.equal(owner.job_identity, "test-toolforge-job");
  assert.equal(owner.process_identity, "test-toolforge-process");
  assert.equal(owner.selected_snapshot, "2026-07");
  assert.equal(owner.pid, first.pid);
  assert.match(owner.started_at, /^\d{4}-\d{2}-\d{2}T/);

  const second = runFixture(fixture, {WIKI_ECON_RUN_ID: "overlapping-run"});
  assert.equal(second.status, 75, `${second.stdout}\n${second.stderr}`);
  assert.match(second.stderr, /Another wiki-economics refresh is already running/);
  assert.equal(fs.existsSync(path.join(fixture.output, ".refresh-status.json")), false);

  fs.writeFileSync(fixture.release, "release\n");
  const completed = await firstResult;
  assert.equal(completed.status, 0, `${completed.stdout}\n${completed.stderr}`);
  assert.equal(fs.existsSync(lockDir), false);
  const status = JSON.parse(fs.readFileSync(path.join(fixture.output, ".refresh-status.json"), "utf8"));
  assert.equal(status.runId, "active-run");
  assert.equal(status.selectedSnapshot, "2026-07");
  assert.equal(status.exitCode, 0);
  assert.equal(
    fs.readFileSync(fixture.driverArgs, "utf8").trim(),
    "--version 2026-07 nlwiki",
  );
});

test("a demonstrably stale cross-job lock is recovered", () => {
  const fixture = createFixture("stale");
  const lockDir = path.join(fixture.output, ".refresh-lock");
  fs.mkdirSync(lockDir, {recursive: true});
  fs.writeFileSync(path.join(lockDir, "owner-token"), "abandoned-token\n");
  fs.writeFileSync(path.join(lockDir, "owner.json"), JSON.stringify({
    schema_version: 1,
    run_id: "abandoned-run",
    started_at: "2020-01-01T00:00:00Z",
    start_epoch: 1,
    pid: 999999,
    job_identity: "abandoned-toolforge-pod",
    process_identity: "abandoned-toolforge-pod",
    owner_token: "abandoned-token",
    selected_snapshot: "2020-01",
    heartbeat_at: "2020-01-01T00:00:00Z",
    heartbeat_epoch: 1,
  }));

  const result = runFixture(fixture, {
    WIKI_ECON_REFRESH_LOCK_STALE_SECS: "1",
    WIKI_ECON_RUN_ID: "recovery-run",
  });
  assert.equal(result.status, 0, `${result.stdout}\n${result.stderr}`);
  assert.match(result.stderr, /Recovered demonstrably stale refresh lock/);
  assert.equal(fs.existsSync(lockDir), false);
});

test("a recent malformed lock fails closed instead of being stolen", () => {
  const fixture = createFixture("malformed");
  const lockDir = path.join(fixture.output, ".refresh-lock");
  fs.mkdirSync(lockDir, {recursive: true});

  const result = runFixture(fixture, {WIKI_ECON_RUN_ID: "blocked-run"});
  assert.equal(result.status, 75, `${result.stdout}\n${result.stderr}`);
  assert.match(result.stderr, /Active lock has no readable owner metadata/);
  assert.equal(fs.existsSync(lockDir), true);
  assert.equal(fs.existsSync(path.join(fixture.output, ".refresh-status.json")), false);
});

test("failure records status and never strands the owned lock", () => {
  const fixture = createFixture("failure");
  const result = runFixture(fixture, {
    FAKE_REFRESH_EXIT: "9",
    WIKI_ECON_RUN_ID: "failed-run",
  });
  assert.equal(result.status, 9, `${result.stdout}\n${result.stderr}`);
  assert.equal(fs.existsSync(path.join(fixture.output, ".refresh-lock")), false);
  const status = JSON.parse(fs.readFileSync(path.join(fixture.output, ".refresh-status.json"), "utf8"));
  assert.equal(status.runId, "failed-run");
  assert.equal(status.selectedSnapshot, "2026-07");
  assert.equal(status.exitCode, 9);
});
