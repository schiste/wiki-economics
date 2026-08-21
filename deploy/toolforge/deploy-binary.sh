#!/usr/bin/env bash
set -euo pipefail

# Uploads a GitHub-built binary through the Toolforge SSH bastion. The remote
# installer performs all validation before changing the stable release link.

usage() {
  echo "Usage: deploy-binary.sh <linux-x86_64-binary> <40-character-git-sha>" >&2
  exit 2
}

[ "$#" -eq 2 ] || usage

binary_path=$1
release_sha=$2
ssh_target="${TOOLFORGE_SSH_TARGET:?Set TOOLFORGE_SSH_TARGET to user@login.toolforge.org}"
tool_account="${TOOLFORGE_TOOL_ACCOUNT:-wiki-economics}"
app_root="/data/project/$tool_account/app"

[[ "$release_sha" =~ ^[0-9a-f]{40}$ ]] || usage
[[ "$tool_account" =~ ^[a-z0-9-]+$ ]] || {
  echo "Invalid Toolforge account name: $tool_account" >&2
  exit 1
}
if [ ! -x "$binary_path" ]; then
  echo "Binary is missing or not executable: $binary_path" >&2
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

staged_binary="$app_root/incoming/$release_sha.part"
ssh -o BatchMode=yes "$ssh_target" \
  "become $tool_account mkdir -p '$app_root/incoming' '$app_root/releases'"
ssh -o BatchMode=yes "$ssh_target" \
  "become $tool_account tee '$staged_binary'" < "$binary_path" >/dev/null
ssh -o BatchMode=yes "$ssh_target" \
  "become $tool_account bash -s -- '$release_sha' '$checksum' '$staged_binary'" \
  < "$(dirname "$0")/install-binary.sh"

stable_binary="$app_root/current/wiki-econ"
ssh -o BatchMode=yes "$ssh_target" \
  "become $tool_account toolforge envvars create WIKI_ECON_BIN '$stable_binary'"
ssh -o BatchMode=yes "$ssh_target" \
  "become $tool_account toolforge webservice restart"

echo "Deployed $release_sha to $stable_binary"
