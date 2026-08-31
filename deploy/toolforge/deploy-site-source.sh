#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "Usage: deploy-site-source.sh <wiki-econ-site-source-SHA.tar.gz> <40-character-git-sha> <archive.sha256> [attestation-bundle.jsonl]" >&2
  exit 2
}

[ "$#" -ge 3 ] && [ "$#" -le 4 ] || usage
bundle_path=$1
release_sha=$2
bundle_checksum_path=$3
attestation_bundle_path=${4:-}
ssh_target="${TOOLFORGE_SSH_TARGET:?Set TOOLFORGE_SSH_TARGET to user@login.toolforge.org}"
repository="${WIKI_ECON_GITHUB_REPOSITORY:-schiste/wiki-economics}"
source_root=/data/project/wiki-economics/site-sources
expected_bundle_name="wiki-econ-site-source-$release_sha.tar.gz"

[[ "$release_sha" =~ ^[0-9a-f]{40}$ ]] || usage
[ -f "$bundle_path" ] && [ -f "$bundle_checksum_path" ] || { echo "Site-source archive or checksum is missing" >&2; exit 1; }
[ "$(basename "$bundle_path")" = "$expected_bundle_name" ] || { echo "Unexpected site-source archive name" >&2; exit 1; }
[ -z "$attestation_bundle_path" ] || [ -f "$attestation_bundle_path" ] || { echo "Attestation bundle is missing" >&2; exit 1; }

read -r expected_checksum checksum_name < "$bundle_checksum_path"
[[ "$expected_checksum" =~ ^[0-9a-f]{64}$ ]] && [ "$checksum_name" = "$expected_bundle_name" ] || {
  echo "Site-source checksum document is malformed" >&2; exit 1;
}
if command -v sha256sum >/dev/null 2>&1; then
  actual_checksum="$(sha256sum "$bundle_path" | awk '{print $1}')"
else
  actual_checksum="$(shasum -a 256 "$bundle_path" | awk '{print $1}')"
fi
[ "$actual_checksum" = "$expected_checksum" ] || { echo "Site-source archive checksum mismatch" >&2; exit 1; }

# Authenticate the exact archive before parsing or extracting any member.
attestation_args=("$bundle_path" --repo "$repository")
[ -z "$attestation_bundle_path" ] || attestation_args+=(--bundle "$attestation_bundle_path")
gh attestation verify "${attestation_args[@]}" >/dev/null

local_extract="$(mktemp -d "${TMPDIR:-/tmp}/wiki-econ-site-source-verify.XXXXXX")"
cleanup() { rm -rf -- "$local_extract"; }
trap cleanup EXIT
tar -xzf "$bundle_path" -C "$local_extract"
provenance="$(node "$(dirname "$0")/../../scripts/site-source-bundle.cjs" --verify "$local_extract" "$release_sha")"
content_sha="$(node -e 'const v=JSON.parse(process.argv[1]);process.stdout.write(v.content_sha256)' "$provenance")"

staged_bundle="$source_root/incoming/$release_sha.site-source.tar.gz.part"
ssh -o BatchMode=yes "$ssh_target" "become wiki-economics mkdir -p '$source_root/incoming' '$source_root/releases'"
ssh -o BatchMode=yes "$ssh_target" "become wiki-economics tee '$staged_bundle'" < "$bundle_path" >/dev/null
ssh -o BatchMode=yes "$ssh_target" \
  "become wiki-economics bash -s -- '$release_sha' '$expected_checksum' '$staged_bundle'" \
  < "$(dirname "$0")/install-site-source.sh"

ssh -o BatchMode=yes "$ssh_target" "become wiki-economics toolforge envvars create WIKI_ECON_SITE_DIR '$source_root/current/site'"
ssh -o BatchMode=yes "$ssh_target" "become wiki-economics toolforge envvars create WIKI_ECON_SITE_SOURCE_REQUIRED '1'"
ssh -o BatchMode=yes "$ssh_target" "become wiki-economics toolforge envvars create WIKI_ECON_SITE_SOURCE_COMMIT '$release_sha'"
ssh -o BatchMode=yes "$ssh_target" "become wiki-economics toolforge envvars create WIKI_ECON_SITE_SOURCE_SHA256 '$content_sha'"
ssh -o BatchMode=yes "$ssh_target" "become wiki-economics toolforge envvars create WIKI_ECON_SITE_SOURCE_ARCHIVE_SHA256 '$expected_checksum'"

echo "Deployed attested site source $release_sha without rebuilding the Toolforge image"
echo "Site source: $source_root/current/site"
echo "Content SHA-256: $content_sha"
