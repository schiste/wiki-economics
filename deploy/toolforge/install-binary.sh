#!/usr/bin/env bash
set -euo pipefail

# Runs as the Toolforge tool account. Installs an already-built Linux binary
# into an immutable release directory and switches the stable symlink only
# after checksum, format, and smoke-test validation succeed.

usage() {
  echo "Usage: install-binary.sh <40-character-git-sha> <sha256> <staged-binary>" >&2
  exit 2
}

[ "$#" -eq 3 ] || usage

release_sha=$1
expected_checksum=$2
staged_binary=$3
app_root="${WIKI_ECON_TOOLFORGE_APP_ROOT:-/data/project/wiki-economics/app}"

[[ "$release_sha" =~ ^[0-9a-f]{40}$ ]] || usage
[[ "$expected_checksum" =~ ^[0-9a-f]{64}$ ]] || usage

expected_staging_path="$app_root/incoming/$release_sha.part"
if [ "$staged_binary" != "$expected_staging_path" ]; then
  echo "Refusing unexpected staging path: $staged_binary" >&2
  exit 1
fi
if [ ! -f "$staged_binary" ]; then
  echo "Staged binary does not exist: $staged_binary" >&2
  exit 1
fi

actual_checksum="$(sha256sum "$staged_binary" | awk '{print $1}')"
if [ "$actual_checksum" != "$expected_checksum" ]; then
  echo "Checksum mismatch for staged binary" >&2
  exit 1
fi

file_description="$(file -b "$staged_binary")"
case "$file_description" in
  *ELF*64-bit*x86-64*) ;;
  *)
    echo "Expected a 64-bit x86-64 ELF binary, got: $file_description" >&2
    exit 1
    ;;
esac

release_dir="$app_root/releases/$release_sha"
release_binary="$release_dir/wiki-econ"
temporary_binary="$release_dir/.wiki-econ.tmp.$$"
temporary_link="$app_root/.current.tmp.$$"

cleanup() {
  rm -f "$temporary_binary" "$temporary_link"
}
trap cleanup EXIT

mkdir -p "$release_dir"
if [ -e "$release_binary" ]; then
  installed_checksum="$(sha256sum "$release_binary" | awk '{print $1}')"
  if [ "$installed_checksum" != "$expected_checksum" ]; then
    echo "Immutable release collision at $release_binary" >&2
    exit 1
  fi
else
  install -m 0755 "$staged_binary" "$temporary_binary"
  "$temporary_binary" --help >/dev/null
  mv "$temporary_binary" "$release_binary"
fi

"$release_binary" --help >/dev/null
printf '%s  wiki-econ\n' "$expected_checksum" > "$release_dir/wiki-econ.sha256"
ln -s "releases/$release_sha" "$temporary_link"
mv -Tf "$temporary_link" "$app_root/current"
rm -f "$staged_binary"

echo "Installed wiki-econ release $release_sha"
echo "Stable binary: $app_root/current/wiki-econ"
