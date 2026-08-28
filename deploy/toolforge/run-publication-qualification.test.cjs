"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const {spawnSync} = require("node:child_process");
const {test} = require("node:test");

const script = path.join(__dirname, "run-publication-qualification.sh");

test("changed-one-wiki qualification is isolated, measured, and self-cleaning", () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "wiki-econ-publication-qualification-"));
  try {
    const data = path.join(root, "data");
    const output = path.join(root, "output");
    const capacity = path.join(root, "capacity");
    const binary = path.join(root, "wiki-econ");
    const lifecycle = path.join(root, "lifecycle.json");
    const wiki = "nlwiki";
    const snapshot = "2026-07";
    const baseline = "baseline-run";
    const active = "active-run";
    fs.mkdirSync(data);
    fs.mkdirSync(path.join(output, "_ready-index"), {recursive: true});
    for (const runId of [baseline, active]) {
      const candidate = path.join(output, "_candidates", wiki, snapshot, runId);
      fs.mkdirSync(path.join(candidate, wiki), {recursive: true});
      fs.writeFileSync(path.join(candidate, "ready.json"), `${JSON.stringify({wiki, snapshot, run_id: runId})}\n`);
    }
    fs.symlinkSync(`_candidates/${wiki}/${snapshot}/${active}/${wiki}`, path.join(output, wiki));
    fs.writeFileSync(path.join(output, "publication-gate.json"), "gate\n");
    fs.writeFileSync(path.join(output, "browser-data-index.json"), "index\n");
    fs.writeFileSync(path.join(output, "_ready-index", `${wiki}.json`), "ready-index\n");
    fs.writeFileSync(lifecycle, `${JSON.stringify({wikis: {[wiki]: {publication: "published", refresh: "scheduled"}}})}\n`);
    fs.writeFileSync(binary, `#!/bin/bash
set -euo pipefail
output=''
run=''
for ((i=1; i<=$#; i++)); do
  value="\${!i}"
  if [ "$value" = --output-dir ]; then j=$((i+1)); output="\${!j}"; fi
  if [ "$value" = --run-id ]; then j=$((i+1)); run="\${!j}"; fi
done
if [[ "$*" == *" publication-prepare-ready "* ]]; then
  mkdir -p "$output/_publication_transactions/$run"
  if [[ "$run" == *-baseline ]]; then
    printf '{"schema_version":1,"state":"selected","entries":[]}\\n' > "$output/_publication_transactions/$run/selection.json"
  else
    printf '{"schema_version":1,"state":"selected","entries":[{"wiki":"${wiki}"}]}\\n' > "$output/_publication_transactions/$run/selection.json"
    printf '{"schema_version":1,"changed":[{"wiki":"${wiki}","family":"monthly"}],"reused":[{"wiki":"otherwiki","family":"monthly"}]}\\n' > "$output/publication-change-plan.json"
  fi
elif [[ "$*" == *" publication-commit-ready"* ]]; then
  :
elif [[ "$*" == *" publication-rollback-ready "* ]]; then
  :
elif [[ "$*" == *" --version"* ]]; then
  printf 'wiki-econ commit=%s\\n' '${"a".repeat(40)}'
fi
`, {mode: 0o755});

    const before = fs.readFileSync(path.join(output, "publication-gate.json"));
    const result = spawnSync("bash", [script, wiki, baseline], {
      encoding: "utf8",
      env: {
        ...process.env,
        WIKI_ECON_TOOL_ROOT: root,
        WIKI_ECON_DATA_DIR: data,
        WIKI_ECON_OUTPUT_DIR: output,
        WIKI_ECON_CAPACITY_DIR: capacity,
        WIKI_ECON_BIN: binary,
        WIKI_ECON_IMAGE_SOURCE_COMMIT: "a".repeat(40),
        WIKI_ECON_WIKI_LIFECYCLE_FILE: lifecycle,
      },
    });
    assert.equal(result.status, 0, result.stderr || result.stdout);
    assert.deepEqual(fs.readFileSync(path.join(output, "publication-gate.json")), before);
    assert.equal(fs.readlinkSync(path.join(output, wiki)), `_candidates/${wiki}/${snapshot}/${active}/${wiki}`);
    const reports = fs.readdirSync(path.join(capacity, "reports"));
    assert.equal(reports.length, 1);
    const report = JSON.parse(fs.readFileSync(path.join(capacity, "reports", reports[0])));
    assert.equal(report.mode, "publication-invisible");
    assert.equal(report.wiki, wiki);
    assert.equal(report.production_mutated, false);
    assert.deepEqual(report.publication_prepare.changed_families, ["monthly"]);
    assert.equal(report.publication_prepare.slo_passed, true);
    assert.deepEqual(fs.readdirSync(path.join(capacity, "work")), []);
  } finally {
    fs.rmSync(root, {recursive: true, force: true});
  }
});
