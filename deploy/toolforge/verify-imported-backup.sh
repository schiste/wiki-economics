#!/usr/bin/env bash
set -euo pipefail

[ "$#" -eq 1 ] || { echo "Usage: verify-imported-backup.sh <archive.tar.gz>" >&2; exit 2; }
archive=$1
[ -f "$archive" ] && [ ! -L "$archive" ] || { echo "Backup archive is missing or unsafe: $archive" >&2; exit 1; }
staging="$(mktemp -d "${TMPDIR:-/tmp}/wiki-econ-imported-verify.XXXXXX")"
trap 'rm -rf -- "$staging"' EXIT

while IFS= read -r entry; do
  case "$entry" in
    SHA256SUMS|backup-manifest.json|data|data/|data/*) ;;
    *) echo "Backup contains an unsafe path: $entry" >&2; exit 1 ;;
  esac
  case "$entry" in /*|../*|*/../*|*/..|*//* ) echo "Backup contains a non-normal path: $entry" >&2; exit 1 ;; esac
done < <(tar -tzf "$archive")
tar -xzf "$archive" -C "$staging"
[ -f "$staging/SHA256SUMS" ] && [ -f "$staging/backup-manifest.json" ] || {
  echo "Backup metadata is incomplete" >&2; exit 1;
}
if find "$staging" -type l -print -quit | grep -q .; then
  echo "Backup contains symbolic links" >&2; exit 1
fi
(cd "$staging" && sha256sum --check --strict --status SHA256SUMS)
node - "$staging" <<'NODE'
const fs = require("node:fs");
const path = require("node:path");
const root = process.argv[2];
const manifest = JSON.parse(fs.readFileSync(path.join(root, "backup-manifest.json"), "utf8"));
if (manifest.schema_version !== 1 || manifest.provenance !== "local-import"
    || !Array.isArray(manifest.wikis) || manifest.wikis.length === 0 || !Array.isArray(manifest.files)) {
  throw new Error("unsupported imported backup manifest");
}
const files = manifest.files.map((entry) => entry.file).sort();
const actual = fs.readFileSync(path.join(root, "SHA256SUMS"), "utf8").trim().split("\n")
  .filter(Boolean).map((line) => line.slice(66)).sort();
if (JSON.stringify(files) !== JSON.stringify(actual)) throw new Error("backup manifest file inventory mismatch");
for (const entry of manifest.files) {
  if (!/^data\/[A-Za-z0-9._\/-]+$/.test(entry.file) || !/^[0-9a-f]{64}$/.test(entry.sha256)
      || fs.statSync(path.join(root, entry.file)).size !== entry.bytes) {
    throw new Error(`invalid imported backup entry: ${entry.file}`);
  }
}
NODE
sha256sum "$archive"
