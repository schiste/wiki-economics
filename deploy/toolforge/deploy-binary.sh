#!/usr/bin/env bash
set -euo pipefail

# Uploads a GitHub-built binary through the Toolforge SSH bastion. The remote
# installer performs all validation before changing the stable release link.

usage() {
  echo "Usage: deploy-binary.sh <linux-x86_64-binary> <40-character-git-sha> <release-provenance.json>" >&2
  exit 2
}

[ "$#" -eq 3 ] || usage

binary_path=$1
release_sha=$2
provenance_path=$3
ssh_target="${TOOLFORGE_SSH_TARGET:?Set TOOLFORGE_SSH_TARGET to user@login.toolforge.org}"
tool_account=wiki-economics
app_root="/data/project/$tool_account/app"

[[ "$release_sha" =~ ^[0-9a-f]{40}$ ]] || usage
if [ ! -x "$binary_path" ]; then
  echo "Binary is missing or not executable: $binary_path" >&2
  exit 1
fi
if [ ! -f "$provenance_path" ]; then
  echo "Release provenance is missing: $provenance_path" >&2
  exit 1
fi

file_description="$(file -b "$binary_path")"
case "$file_description" in
  *ELF*64-bit*x86-64*) ;;
  *)
    echo "Expected a 64-bit x86-64 ELF binary, got: $file_description" >&2
    exit 1
    ;;
esac

if command -v sha256sum >/dev/null 2>&1; then
  checksum="$(sha256sum "$binary_path" | awk '{print $1}')"
else
  checksum="$(shasum -a 256 "$binary_path" | awk '{print $1}')"
fi
if command -v sha256sum >/dev/null 2>&1; then
  provenance_checksum="$(sha256sum "$provenance_path" | awk '{print $1}')"
else
  provenance_checksum="$(shasum -a 256 "$provenance_path" | awk '{print $1}')"
fi

if [ "$(jq -er '.schema_version' "$provenance_path")" != "1" ] || \
   [ "$(jq -er '.source_commit' "$provenance_path")" != "$release_sha" ] || \
   [ "$(jq -er '.binary.sha256' "$provenance_path")" != "$checksum" ]; then
  echo "Release provenance does not match the commit and binary" >&2
  exit 1
fi

staged_binary="$app_root/incoming/$release_sha.part"
staged_provenance="$app_root/incoming/$release_sha.provenance.part"
ssh -o BatchMode=yes "$ssh_target" \
  "become $tool_account mkdir -p '$app_root/incoming' '$app_root/releases'"
ssh -o BatchMode=yes "$ssh_target" \
  "become $tool_account tee '$staged_binary'" < "$binary_path" >/dev/null
ssh -o BatchMode=yes "$ssh_target" \
  "become $tool_account tee '$staged_provenance'" < "$provenance_path" >/dev/null
ssh -o BatchMode=yes "$ssh_target" \
  "become $tool_account bash -s -- '$release_sha' '$checksum' '$staged_binary' '$provenance_checksum' '$staged_provenance'" \
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

echo "Deployed $release_sha to $stable_binary"
