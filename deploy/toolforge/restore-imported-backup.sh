#!/usr/bin/env bash
set -euo pipefail

[ "$#" -eq 2 ] || { echo "Usage: restore-imported-backup.sh <archive.tar.gz> <empty-output-dir>" >&2; exit 2; }
archive=$1
destination=$2
[ ! -e "$destination" ] && [ ! -L "$destination" ] || {
  echo "Restore destination must not exist: $destination" >&2; exit 1;
}
"$(dirname "$0")/verify-imported-backup.sh" "$archive" >/dev/null
parent="$(dirname "$destination")"
name="$(basename "$destination")"
case "$name" in ''|.|..|/) echo "Unsafe restore destination: $destination" >&2; exit 1 ;; esac
mkdir -p "$parent"
staging="$(mktemp -d "$parent/.${name}.restore.XXXXXX")"
trap 'rm -rf -- "$staging"' EXIT
tar -xzf "$archive" -C "$staging"
mv -- "$staging/data" "$destination"
echo "Restored imported datasets to $destination"
