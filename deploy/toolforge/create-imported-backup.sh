#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 1 ]; then
  echo "Usage: create-imported-backup.sh <destination.tar.gz>" >&2
  exit 2
fi

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
output_dir="${WIKI_ECON_OUTPUT_DIR:-/data/project/wiki-economics/output}"
lifecycle="${WIKI_ECON_WIKI_LIFECYCLE_FILE:-$ROOT/config/wiki-lifecycle.json}"
archive=$1
archive_parent="$(dirname "$archive")"
archive_name="$(basename "$archive")"
case "$archive_name" in
  *.tar.gz) ;;
  *) echo "Backup destination must end in .tar.gz" >&2; exit 2 ;;
esac
[ ! -e "$archive" ] || { echo "Refusing to overwrite backup: $archive" >&2; exit 1; }
mkdir -p "$archive_parent"
staging="$(mktemp -d "${TMPDIR:-/tmp}/wiki-econ-imported-backup.XXXXXX")"
temporary_archive="$archive_parent/.${archive_name}.$$.$RANDOM.tmp"
cleanup() {
  rm -rf -- "$staging"
  rm -f -- "$temporary_archive"
}
trap cleanup EXIT

wikis=()
while IFS= read -r wiki; do
  [ -n "$wiki" ] && wikis+=("$wiki")
done < <(node -e '
const fs = require("node:fs");
const registry = JSON.parse(fs.readFileSync(process.argv[1], "utf8"));
for (const [wiki, entry] of Object.entries(registry.wikis || {}).sort()) {
  if (entry.publication === "published" && entry.provenance === "local-import") console.log(wiki);
}
' "$lifecycle")
[ "${#wikis[@]}" -gt 0 ] || { echo "Lifecycle contains no imported datasets" >&2; exit 1; }

mkdir -p "$staging/data"
for wiki in "${wikis[@]}"; do
  source="$output_dir/$wiki"
  [ -d "$source" ] || { echo "Imported dataset is missing: $source" >&2; exit 1; }
  cp -R -- "$source" "$staging/data/$wiki"
done
if find "$staging/data" -type l -print -quit | grep -q .; then
  echo "Imported backup refuses symbolic links" >&2
  exit 1
fi
(cd "$staging" && find data -type f -print0 | sort -z | xargs -0 sha256sum > SHA256SUMS)
WIKI_ECON_BACKUP_WIKIS="$(printf '%s\n' "${wikis[@]}" | node -e '
let value=""; process.stdin.on("data", chunk => value += chunk); process.stdin.on("end", () => {
  process.stdout.write(JSON.stringify(value.trim().split(/\n/).filter(Boolean)));
});
')" \
WIKI_ECON_BACKUP_SOURCE_COMMIT="${WIKI_ECON_SOURCE_COMMIT:-$(git -C "$ROOT" rev-parse HEAD 2>/dev/null || true)}" \
node - "$staging" <<'NODE'
const fs = require("node:fs");
const path = require("node:path");
const root = process.argv[2];
const checksums = fs.readFileSync(path.join(root, "SHA256SUMS"), "utf8").trim().split("\n").filter(Boolean).map((line) => {
  const match = line.match(/^([0-9a-f]{64})  (data\/[A-Za-z0-9._\/-]+)$/);
  if (!match) throw new Error(`invalid backup checksum line: ${line}`);
  return {sha256: match[1], file: match[2], bytes: fs.statSync(path.join(root, match[2])).size};
});
const manifest = {
  schema_version: 1,
  created_at: new Date().toISOString().replace(/\.\d{3}Z$/, "Z"),
  source_commit: process.env.WIKI_ECON_BACKUP_SOURCE_COMMIT || null,
  provenance: "local-import",
  wikis: JSON.parse(process.env.WIKI_ECON_BACKUP_WIKIS),
  files: checksums,
};
fs.writeFileSync(path.join(root, "backup-manifest.json"), `${JSON.stringify(manifest, null, 2)}\n`, {mode: 0o600});
NODE

if tar --version 2>&1 | grep -q 'GNU tar'; then
  (cd "$staging" && tar --sort=name --mtime='UTC 1970-01-01' --owner=0 --group=0 --numeric-owner \
    -cf - SHA256SUMS backup-manifest.json data | gzip -n > "$temporary_archive")
else
  # Portability path for operator workstations using bsdtar. Production
  # Toolforge exports use GNU tar and therefore the normalized form above.
  (cd "$staging" && tar -cf - SHA256SUMS backup-manifest.json data | gzip -n > "$temporary_archive")
fi
sync "$temporary_archive" 2>/dev/null || true
mv -- "$temporary_archive" "$archive"
sha256sum "$archive"
