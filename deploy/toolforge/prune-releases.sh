#!/usr/bin/env bash
set -euo pipefail

# Bound immutable binary releases without ever following or deleting the live
# target. Incomplete uploads are separately reaped after a conservative age.

app_root="${1:-${WIKI_ECON_TOOLFORGE_APP_ROOT:-/data/project/wiki-economics/app}}"
retention="${2:-${WIKI_ECON_RELEASE_RETENTION:-3}}"
incoming_stale_secs="${WIKI_ECON_INCOMING_STALE_SECS:-86400}"

case "$app_root" in
  /*) ;;
  *) echo "App root must be absolute: $app_root" >&2; exit 2 ;;
esac
[ "$app_root" != "/" ] || { echo "Refusing app root /" >&2; exit 2; }
[[ "$retention" =~ ^[1-9][0-9]*$ ]] || {
  echo "Release retention must be a positive integer: $retention" >&2
  exit 2
}
[[ "$incoming_stale_secs" =~ ^[0-9]+$ ]] || {
  echo "Incoming stale age must be a non-negative integer: $incoming_stale_secs" >&2
  exit 2
}

releases_root="$app_root/releases"
incoming_root="$app_root/incoming"
current_link="$app_root/current"
current_sha=""

release_is_valid() {
  local directory=$1 expected filename actual
  [ -x "$directory/wiki-econ" ] && [ -f "$directory/wiki-econ.sha256" ] || return 1
  read -r expected filename < "$directory/wiki-econ.sha256" || return 1
  [[ "$expected" =~ ^[0-9a-f]{64}$ ]] && [ "$filename" = "wiki-econ" ] || return 1
  if command -v sha256sum >/dev/null 2>&1; then
    actual="$(sha256sum "$directory/wiki-econ" | awk '{print $1}')"
  else
    actual="$(shasum -a 256 "$directory/wiki-econ" | awk '{print $1}')"
  fi
  [ "$actual" = "$expected" ] && "$directory/wiki-econ" --help >/dev/null 2>&1
}

if [ -e "$current_link" ] || [ -d "$releases_root" ]; then
  if [ ! -L "$current_link" ] || [ ! -d "$releases_root" ]; then
    echo "Refusing release cleanup without a valid current symlink and releases directory" >&2
    exit 1
  fi
  current_target="$(readlink "$current_link")"
  if [[ ! "$current_target" =~ ^releases/([0-9a-f]{40})$ ]]; then
    echo "Refusing unexpected current release target: $current_target" >&2
    exit 1
  fi
  current_sha="${BASH_REMATCH[1]}"
  current_dir="$releases_root/$current_sha"
  if [ ! -d "$current_dir" ] || [ -L "$current_dir" ] || ! release_is_valid "$current_dir"; then
    echo "Refusing cleanup because the current release is incomplete: $current_sha" >&2
    exit 1
  fi
fi

mtime_epoch() {
  if stat -c %Y "$1" >/dev/null 2>&1; then
    stat -c %Y "$1"
  else
    stat -f %m "$1"
  fi
}

removed_releases=0
retained_releases=0
kept_noncurrent=0
if [ -n "$current_sha" ]; then
  retained_releases=1
  release_rows=()
  shopt -s nullglob
  for directory in "$releases_root"/*; do
    [ -d "$directory" ] && [ ! -L "$directory" ] || continue
    sha="$(basename "$directory")"
    [[ "$sha" =~ ^[0-9a-f]{40}$ ]] || continue
    [ "$sha" != "$current_sha" ] || continue
    release_rows+=("$(mtime_epoch "$directory"):$sha")
  done
  shopt -u nullglob

  while IFS=: read -r _ sha; do
    [ -n "$sha" ] || continue
    directory="$releases_root/$sha"
    if release_is_valid "$directory" && [ "$kept_noncurrent" -lt "$((retention - 1))" ]; then
      kept_noncurrent=$((kept_noncurrent + 1))
      retained_releases=$((retained_releases + 1))
      continue
    fi
    rm -rf -- "$directory"
    removed_releases=$((removed_releases + 1))
  done < <(printf '%s\n' ${release_rows[@]+"${release_rows[@]}"} | sort -rn)
fi

removed_incoming=0
now_epoch="$(date +%s)"
if [ -d "$incoming_root" ]; then
  shopt -s nullglob
  for candidate in "$incoming_root"/*.part; do
    [ -f "$candidate" ] && [ ! -L "$candidate" ] || continue
    name="$(basename "$candidate")"
    [[ "$name" =~ ^[0-9a-f]{40}(\.provenance)?\.part$ ]] || continue
    modified_epoch="$(mtime_epoch "$candidate")"
    age=$((now_epoch - modified_epoch))
    [ "$age" -ge "$incoming_stale_secs" ] || continue
    rm -f -- "$candidate"
    removed_incoming=$((removed_incoming + 1))
  done
  shopt -u nullglob
fi

printf '{"release_directories":%d,"incoming_files":%d,"retained_releases":%d}\n' \
  "$removed_releases" "$removed_incoming" "$retained_releases"
