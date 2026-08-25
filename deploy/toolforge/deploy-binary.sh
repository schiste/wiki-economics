#!/usr/bin/env bash
set -euo pipefail

# Uploads one attested GitHub release envelope through the Toolforge SSH
# bastion. Both this workstation and the remote installer verify it before the
# stable release link can change.

usage() {
  echo "Usage: deploy-binary.sh <wiki-econ-release-SHA.tar.gz> <40-character-git-sha> <archive.sha256> [attestation-bundle.jsonl]" >&2
  exit 2
}

[ "$#" -ge 3 ] && [ "$#" -le 4 ] || usage

bundle_path=$1
release_sha=$2
bundle_checksum_path=$3
attestation_bundle_path=${4:-}
ssh_target="${TOOLFORGE_SSH_TARGET:?Set TOOLFORGE_SSH_TARGET to user@login.toolforge.org}"
github_repository="${WIKI_ECON_GITHUB_REPOSITORY:-schiste/wiki-economics}"
tool_account=wiki-economics
app_root="/data/project/$tool_account/app"
expected_bundle_name="wiki-econ-release-$release_sha.tar.gz"

[[ "$release_sha" =~ ^[0-9a-f]{40}$ ]] || usage
[ -f "$bundle_path" ] || { echo "Release archive is missing: $bundle_path" >&2; exit 1; }
[ -f "$bundle_checksum_path" ] || { echo "Release archive checksum is missing: $bundle_checksum_path" >&2; exit 1; }
if [ -n "$attestation_bundle_path" ]; then
  [ -f "$attestation_bundle_path" ] || {
    echo "Attestation bundle is missing: $attestation_bundle_path" >&2
    exit 1
  }
fi
[ "$(basename "$bundle_path")" = "$expected_bundle_name" ] || {
  echo "Unexpected release archive name; expected $expected_bundle_name" >&2
  exit 1
}
for command in gh jq node tar; do
  command -v "$command" >/dev/null 2>&1 || { echo "Required deployment command is missing: $command" >&2; exit 1; }
done

read -r expected_bundle_checksum checksum_name < "$bundle_checksum_path" || {
  echo "Cannot read release archive checksum" >&2
  exit 1
}
[[ "$expected_bundle_checksum" =~ ^[0-9a-f]{64}$ ]] && [ "$checksum_name" = "$expected_bundle_name" ] || {
  echo "Release archive checksum document is malformed" >&2
  exit 1
}
if command -v sha256sum >/dev/null 2>&1; then
  actual_bundle_checksum="$(sha256sum "$bundle_path" | awk '{print $1}')"
else
  actual_bundle_checksum="$(shasum -a 256 "$bundle_path" | awk '{print $1}')"
fi
[ "$actual_bundle_checksum" = "$expected_bundle_checksum" ] || {
  echo "Release archive checksum mismatch" >&2
  exit 1
}

local_extract="$(mktemp -d "${TMPDIR:-/tmp}/wiki-econ-release-verify.XXXXXX")"
cleanup() {
  rm -rf -- "$local_extract"
}
trap cleanup EXIT

entry_count=0
while IFS= read -r entry; do
  case "$entry" in
    SHA256SUMS|THIRD_PARTY_NOTICES.md|release-provenance.json|third-party-notices.json|wiki-econ|wiki-econ-browser-bundle.cdx.json|wiki-econ-rust-binary.cdx.json|wiki-econ-toolforge-site-image.cdx.json) ;;
    *) echo "Release archive contains an unexpected path: $entry" >&2; exit 1 ;;
  esac
  entry_count=$((entry_count + 1))
done < <(tar -tzf "$bundle_path")
[ "$entry_count" -eq 8 ] || { echo "Release archive must contain exactly eight files" >&2; exit 1; }
tar -xzf "$bundle_path" -C "$local_extract"
node "$(dirname "$0")/../../scripts/release-bundle.cjs" --verify "$local_extract" "$release_sha"
file_description="$(file -b "$local_extract/wiki-econ")"
case "$file_description" in
  *ELF*64-bit*x86-64*) ;;
  *) echo "Expected a 64-bit x86-64 ELF binary, got: $file_description" >&2; exit 1 ;;
esac

# Verify GitHub's Sigstore-backed attestation against the repository identity
# and exact archive digest before any SSH upload. An explicitly downloaded
# bundle keeps this check reproducible when GitHub or Sigstore root discovery
# is unavailable; gh still validates its certificate and transparency proof.
attestation_args=("$bundle_path" --repo "$github_repository")
if [ -n "$attestation_bundle_path" ]; then
  attestation_args+=(--bundle "$attestation_bundle_path")
fi
gh attestation verify "${attestation_args[@]}" >/dev/null

staged_bundle="$app_root/incoming/$release_sha.release.tar.gz.part"
ssh -o BatchMode=yes "$ssh_target" \
  "become $tool_account mkdir -p '$app_root/incoming' '$app_root/releases'"
ssh -o BatchMode=yes "$ssh_target" \
  "become $tool_account tee '$staged_bundle'" < "$bundle_path" >/dev/null
ssh -o BatchMode=yes "$ssh_target" \
  "become $tool_account bash -s -- '$release_sha' '$expected_bundle_checksum' '$staged_bundle'" \
  < "$(dirname "$0")/install-binary.sh"

if ! ssh -o BatchMode=yes "$ssh_target" \
  "become $tool_account bash -s -- '$app_root' '${WIKI_ECON_RELEASE_RETENTION:-3}'" \
  < "$(dirname "$0")/prune-releases.sh"; then
  echo "WARNING: release installed, but bounded release cleanup failed" >&2
fi

stable_binary="$app_root/current/wiki-econ"
ssh -o BatchMode=yes "$ssh_target" \
  "become $tool_account toolforge envvars create WIKI_ECON_BIN '$stable_binary'"
ssh -o BatchMode=yes "$ssh_target" \
  "become $tool_account toolforge webservice restart"

echo "Deployed attested release $release_sha to $stable_binary"
