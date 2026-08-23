const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const {spawnSync} = require("node:child_process");
const {after, test} = require("node:test");

const script = path.join(__dirname, "run-capacity-benchmark.sh");
const fixtureRoot = fs.mkdtempSync(path.join(os.tmpdir(), "wiki-econ-capacity-run-"));

after(() => fs.rmSync(fixtureRoot, {recursive: true, force: true}));

function writeFakeBinary(root) {
  const binary = path.join(root, "wiki-econ");
  fs.mkdirSync(root, {recursive: true});
  fs.writeFileSync(binary, `#!/bin/sh
set -eu
report=""
while [ "$#" -gt 0 ]; do
  if [ "$1" = "--report" ]; then report=$2; shift 2; else shift; fi
done
printf '%s\n' '{"qualification":"retained"}' > "$report"
exit "\${FAKE_CAPACITY_EXIT:-0}"
`, {mode: 0o755});
  return binary;
}

for (const [name, exitCode] of [["success", 0], ["failure", 9]]) {
  test(`capacity ${name} retains its report but removes working data`, () => {
    const root = path.join(fixtureRoot, name);
    const capacityRoot = path.join(root, "capacity");
    const result = spawnSync("bash", [script, "frwiki", "256"], {
      encoding: "utf8",
      env: {
        ...process.env,
        FAKE_CAPACITY_EXIT: String(exitCode),
        WIKI_ECON_BIN: writeFakeBinary(path.join(root, "bin")),
        WIKI_ECON_CAPACITY_ROOT: capacityRoot,
        WIKI_ECON_DATA_DIR: path.join(root, "data"),
      },
    });
    assert.equal(result.status, exitCode, `${result.stdout}\n${result.stderr}`);
    assert.deepEqual(fs.readdirSync(path.join(capacityRoot, "output")), []);
    assert.deepEqual(fs.readdirSync(path.join(capacityRoot, "scratch")), []);
    const reportDir = path.join(capacityRoot, "reports", "frwiki");
    const reports = fs.readdirSync(reportDir);
    assert.equal(reports.length, 1);
    assert.deepEqual(JSON.parse(fs.readFileSync(path.join(reportDir, reports[0]), "utf8")), {
      qualification: "retained",
    });
  });
}
