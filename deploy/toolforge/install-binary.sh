#!/usr/bin/env bash
set -euo pipefail

# Runs as the Toolforge tool account. The release archive is untrusted input:
# validate its path set, checksums, provenance, and SBOM identities before an
# immutable release directory is created or the stable symlink is switched.

usage() {
  echo "Usage: install-binary.sh <40-character-git-sha> <archive-sha256> <staged-release-archive>" >&2
  exit 2
}

[ "$#" -eq 3 ] || usage

release_sha=$1
expected_archive_checksum=$2
staged_archive=$3
app_root="${WIKI_ECON_TOOLFORGE_APP_ROOT:-/data/project/wiki-economics/app}"

[[ "$release_sha" =~ ^[0-9a-f]{40}$ ]] || usage
[[ "$expected_archive_checksum" =~ ^[0-9a-f]{64}$ ]] || usage
expected_staging_path="$app_root/incoming/$release_sha.release.tar.gz.part"
[ "$staged_archive" = "$expected_staging_path" ] && [ -f "$staged_archive" ] && [ ! -L "$staged_archive" ] || {
  echo "Refusing missing or unexpected release staging path: $staged_archive" >&2
  exit 1
}

actual_archive_checksum="$(sha256sum "$staged_archive" | awk '{print $1}')"
[ "$actual_archive_checksum" = "$expected_archive_checksum" ] || { echo "Checksum mismatch for staged release archive" >&2; exit 1; }

entry_count=0
while IFS= read -r entry; do
  case "$entry" in
    SHA256SUMS|THIRD_PARTY_NOTICES.md|release-provenance.json|third-party-notices.json|wiki-econ|wiki-econ-browser-bundle.cdx.json|wiki-econ-rust-binary.cdx.json|wiki-econ-toolforge-site-image.cdx.json) ;;
    *) echo "Release archive contains an unexpected path: $entry" >&2; exit 1 ;;
  esac
  entry_count=$((entry_count + 1))
done < <(tar -tzf "$staged_archive")
[ "$entry_count" -eq 8 ] || { echo "Release archive must contain exactly eight files" >&2; exit 1; }

extracted="$app_root/incoming/.extract.$release_sha.$$"
temporary_link="$app_root/.current.tmp.$$"
cleanup() {
  rm -rf -- "$extracted"
  rm -f -- "$temporary_link"
}
trap cleanup EXIT
mkdir -m 0700 "$extracted"
tar -xzf "$staged_archive" -C "$extracted"

payload=(
  SHA256SUMS
  THIRD_PARTY_NOTICES.md
  release-provenance.json
  third-party-notices.json
  wiki-econ
  wiki-econ-browser-bundle.cdx.json
  wiki-econ-rust-binary.cdx.json
  wiki-econ-toolforge-site-image.cdx.json
)
for name in "${payload[@]}"; do
  [ -f "$extracted/$name" ] && [ ! -L "$extracted/$name" ] || { echo "Release payload is not a regular file: $name" >&2; exit 1; }
done

manifest_lines="$(wc -l < "$extracted/SHA256SUMS" | tr -d ' ')"
[ "$manifest_lines" -eq 7 ] || { echo "SHA256SUMS must identify exactly seven payload files" >&2; exit 1; }
while IFS= read -r line; do
  [[ "$line" =~ ^[0-9a-f]{64}\ \ ([A-Za-z0-9_.-]+)$ ]] || { echo "Malformed SHA256SUMS line" >&2; exit 1; }
  case "${BASH_REMATCH[1]}" in
    THIRD_PARTY_NOTICES.md|release-provenance.json|third-party-notices.json|wiki-econ|wiki-econ-browser-bundle.cdx.json|wiki-econ-rust-binary.cdx.json|wiki-econ-toolforge-site-image.cdx.json) ;;
    *) echo "SHA256SUMS identifies an unexpected file: ${BASH_REMATCH[1]}" >&2; exit 1 ;;
  esac
done < "$extracted/SHA256SUMS"
actual_manifest_names="$(awk '{print $2}' "$extracted/SHA256SUMS" | sort)"
expected_manifest_names="$(printf '%s\n' \
  THIRD_PARTY_NOTICES.md release-provenance.json third-party-notices.json wiki-econ \
  wiki-econ-browser-bundle.cdx.json wiki-econ-rust-binary.cdx.json \
  wiki-econ-toolforge-site-image.cdx.json | sort)"
[ "$actual_manifest_names" = "$expected_manifest_names" ] || { echo "SHA256SUMS does not contain the exact release payload" >&2; exit 1; }
(cd "$extracted" && sha256sum --check --strict --status SHA256SUMS)

binary_checksum="$(sha256sum "$extracted/wiki-econ" | awk '{print $1}')"
provenance="$extracted/release-provenance.json"
[ "$(jq -er '.schema_version' "$provenance")" = "2" ] && \
  [ "$(jq -er '.source_commit' "$provenance")" = "$release_sha" ] && \
  [ "$(jq -er '.binary.sha256' "$provenance")" = "$binary_checksum" ] || {
  echo "Release provenance failed commit or binary identity validation" >&2
  exit 1
}

verify_sbom() {
  local key=$1 name=$2 artifact=$3 sbom="$extracted/$2" actual declared
  actual="$(sha256sum "$sbom" | awk '{print $1}')"
  declared="$(jq -er --arg key "$key" '.supply_chain.sboms[$key].sha256' "$provenance")"
  [ "$actual" = "$declared" ] && \
    [ "$(jq -er --arg key "$key" '.supply_chain.sboms[$key].file' "$provenance")" = "$name" ] && \
    [ "$(jq -er --arg key "$key" '.supply_chain.sboms[$key].artifact' "$provenance")" = "$artifact" ] && jq -e \
    --arg artifact "$artifact" --arg commit "$release_sha" --arg hash "$(jq -er --arg key "$key" '.supply_chain.sboms[$key].artifact_sha256' "$provenance")" \
    '.bomFormat == "CycloneDX" and .specVersion == "1.6"
      and any(.metadata.component.properties[]; .name == "org.wikimedia.toolforge.wiki-econ.artifact" and .value == $artifact)
      and any(.metadata.component.properties[]; .name == "org.wikimedia.toolforge.wiki-econ.source-commit" and .value == $commit)
      and any(.metadata.component.properties[]; .name == "org.wikimedia.toolforge.wiki-econ.artifact-sha256" and .value == $hash)' \
    "$sbom" >/dev/null
}
verify_sbom rust_binary wiki-econ-rust-binary.cdx.json rust-binary || { echo "Rust binary SBOM identity validation failed" >&2; exit 1; }
[ "$(jq -er '.supply_chain.sboms.rust_binary.artifact_sha256' "$provenance")" = "$binary_checksum" ] || {
  echo "Rust binary SBOM does not identify the release binary" >&2
  exit 1
}
verify_sbom toolforge_site_image wiki-econ-toolforge-site-image.cdx.json toolforge-site-image-closure || { echo "Toolforge image SBOM identity validation failed" >&2; exit 1; }
verify_sbom published_browser_bundle wiki-econ-browser-bundle.cdx.json published-browser-bundle || { echo "Browser bundle SBOM identity validation failed" >&2; exit 1; }

if [ "$(jq -er '.supply_chain.notices.machine_readable.file' "$provenance")" != "third-party-notices.json" ] || \
  [ "$(jq -er '.supply_chain.notices.machine_readable.sha256' "$provenance")" != "$(sha256sum "$extracted/third-party-notices.json" | awk '{print $1}')" ] || \
  [ "$(jq -er '.supply_chain.notices.human_readable.file' "$provenance")" != "THIRD_PARTY_NOTICES.md" ] || \
  [ "$(jq -er '.supply_chain.notices.human_readable.sha256' "$provenance")" != "$(sha256sum "$extracted/THIRD_PARTY_NOTICES.md" | awk '{print $1}')" ]; then
  echo "Third-party notice checksums do not match release provenance" >&2
  exit 1
fi

jq -e --arg commit "$release_sha" \
  '.schema_version == 1 and .source_commit == $commit and (.rust | type == "array") and (.rust | length > 0)
    and (.toolforge_runtime | type == "array") and (.toolforge_runtime | length > 0)
    and (.toolforge_image_npm | type == "array") and (.toolforge_image_npm | length > 0)
    and (.published_browser | type == "array") and (.published_browser | length > 0)' \
  "$extracted/third-party-notices.json" >/dev/null || { echo "Third-party notices identity validation failed" >&2; exit 1; }

file_description="$(file -b "$extracted/wiki-econ")"
case "$file_description" in
  *ELF*64-bit*x86-64*) ;;
  *) echo "Expected a 64-bit x86-64 ELF binary, got: $file_description" >&2; exit 1 ;;
esac
chmod 0755 "$extracted/wiki-econ"
"$extracted/wiki-econ" --help >/dev/null
printf '%s  wiki-econ\n' "$binary_checksum" > "$extracted/wiki-econ.sha256"

release_dir="$app_root/releases/$release_sha"
if [ -e "$release_dir" ]; then
  [ -d "$release_dir" ] && [ ! -L "$release_dir" ] || { echo "Immutable release collision at $release_dir" >&2; exit 1; }
  (cd "$release_dir" && sha256sum --check --strict --status SHA256SUMS) || { echo "Existing immutable release is incomplete or changed" >&2; exit 1; }
  for name in "${payload[@]}" wiki-econ.sha256; do
    if [ ! -f "$release_dir/$name" ] || ! cmp -s "$extracted/$name" "$release_dir/$name"; then
      echo "Immutable release collision at $release_dir/$name" >&2
      exit 1
    fi
  done
else
  mv "$extracted" "$release_dir"
fi

"$release_dir/wiki-econ" --help >/dev/null
ln -s "releases/$release_sha" "$temporary_link"
mv -Tf "$temporary_link" "$app_root/current"
rm -f -- "$staged_archive"

echo "Installed verified wiki-econ release $release_sha"
echo "Stable binary: $app_root/current/wiki-econ"
