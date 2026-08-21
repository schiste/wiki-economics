#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "Usage: rollback-binary.sh <40-character-git-sha>" >&2
  exit 2
}

[ "$#" -eq 1 ] || usage

release_sha=$1
app_root="${WIKI_ECON_TOOLFORGE_APP_ROOT:-/data/project/wiki-economics/app}"
[[ "$release_sha" =~ ^[0-9a-f]{40}$ ]] || usage

release_dir="$app_root/releases/$release_sha"
release_binary="$release_dir/wiki-econ"
checksum_file="$release_dir/wiki-econ.sha256"

if [ ! -x "$release_binary" ] || [ ! -f "$checksum_file" ]; then
  echo "Release is incomplete or missing: $release_sha" >&2
  exit 1
fi

(cd "$release_dir" && sha256sum --check --status wiki-econ.sha256)
"$release_binary" --help >/dev/null

temporary_link="$app_root/.current.tmp.$$"
trap 'rm -f "$temporary_link"' EXIT
ln -s "releases/$release_sha" "$temporary_link"
mv -Tf "$temporary_link" "$app_root/current"

echo "Rolled back wiki-econ to $release_sha"
echo "Restart the webservice so it picks up the selected release."
