#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "Usage: install-site-source.sh <40-character-git-sha> <archive-sha256> <staged-site-source-archive>" >&2
  exit 2
}

[ "$#" -eq 3 ] || usage
release_sha=$1
expected_archive_checksum=$2
staged_archive=$3
source_root="${WIKI_ECON_TOOLFORGE_SITE_SOURCE_ROOT:-/data/project/wiki-economics/site-sources}"
expected_staging_path="$source_root/incoming/$release_sha.site-source.tar.gz.part"

[[ "$release_sha" =~ ^[0-9a-f]{40}$ ]] || usage
[[ "$expected_archive_checksum" =~ ^[0-9a-f]{64}$ ]] || usage
[ "$staged_archive" = "$expected_staging_path" ] && [ -f "$staged_archive" ] && [ ! -L "$staged_archive" ] || {
  echo "Refusing missing or unexpected site-source staging path: $staged_archive" >&2
  exit 1
}
[ "$(sha256sum "$staged_archive" | awk '{print $1}')" = "$expected_archive_checksum" ] || {
  echo "Checksum mismatch for staged site-source archive" >&2
  exit 1
}

while IFS= read -r entry; do
  normalized=${entry#./}
  case "$normalized" in
    '') continue ;;
    /*|../*|*/../*|*/..) echo "Unsafe site-source archive path: $entry" >&2; exit 1 ;;
  esac
done < <(tar -tzf "$staged_archive")

mkdir -p "$source_root/incoming" "$source_root/releases"
extracted="$source_root/incoming/.extract.$release_sha.$$"
temporary_link="$source_root/.current.tmp.$$"
publication_lock="${WIKI_ECON_PUBLICATION_LOCK_DIR:-/data/project/wiki-economics/output/.publication.lock}"
deployment_lock_token="site-source-$release_sha-$$-$(date +%s)"
deployment_lock_owned=0
cleanup() {
  rm -rf -- "$extracted"
  rm -f -- "$temporary_link"
  if [ "$deployment_lock_owned" -eq 1 ] && [ -f "$publication_lock/owner-token" ] && \
     [ "$(<"$publication_lock/owner-token")" = "$deployment_lock_token" ]; then
    rm -f -- "$publication_lock/owner.json" "$publication_lock/owner.json.tmp.$$" "$publication_lock/owner-token"
    rmdir -- "$publication_lock" 2>/dev/null || true
  fi
}
trap cleanup EXIT
mkdir -m 0700 "$extracted"
tar --no-same-owner --no-same-permissions -xzf "$staged_archive" -C "$extracted"

provenance="$extracted/site-source-provenance.json"
[ -f "$provenance" ] && [ ! -L "$provenance" ] || { echo "Site-source provenance is missing" >&2; exit 1; }
[ "$(jq -er '.schema_version' "$provenance")" = 1 ] && \
  [ "$(jq -er '.artifact' "$provenance")" = wiki-econ-site-source ] && \
  [ "$(jq -er '.source_commit' "$provenance")" = "$release_sha" ] && \
  [[ "$(jq -er '.content_sha256' "$provenance")" =~ ^[0-9a-f]{64}$ ]] || {
  echo "Site-source provenance identity is invalid" >&2
  exit 1
}

expected_inventory="$extracted/.expected-inventory.$$"
actual_inventory="$extracted/.actual-inventory.$$"
jq -r '.files[].path, "site-source-provenance.json"' "$provenance" | LC_ALL=C sort > "$expected_inventory"
find "$extracted" -type f ! -name ".expected-inventory.$$" ! -name ".actual-inventory.$$" \
  -print | sed "s#^$extracted/##" | LC_ALL=C sort > "$actual_inventory"
cmp -s "$expected_inventory" "$actual_inventory" || { echo "Site-source path inventory does not match provenance" >&2; exit 1; }
rm -f -- "$expected_inventory" "$actual_inventory"

while IFS=$'\t' read -r relative expected_hash expected_bytes; do
  [ -n "$relative" ] && [[ "$relative" != /* ]] && [[ "$relative" != *".."* ]] || {
    echo "Invalid path in site-source provenance" >&2; exit 1;
  }
  file="$extracted/$relative"
  [ -f "$file" ] && [ ! -L "$file" ] || { echo "Site-source file is not regular: $relative" >&2; exit 1; }
  [ "$(wc -c < "$file" | tr -d ' ')" = "$expected_bytes" ] && \
    [ "$(sha256sum "$file" | awk '{print $1}')" = "$expected_hash" ] || {
    echo "Site-source identity mismatch: $relative" >&2; exit 1;
  }
done < <(jq -r '.files[] | [.path, .sha256, (.bytes|tostring)] | @tsv' "$provenance")

case "$(basename "$publication_lock")" in *.lock) ;; *) echo "Unsafe publication lock path" >&2; exit 2 ;; esac
mkdir -p "$(dirname "$publication_lock")"
if ! mkdir "$publication_lock" 2>/dev/null; then
  echo "Publication lock is active; refusing concurrent site-source deployment" >&2
  exit 75
fi
chmod 700 "$publication_lock"
printf '%s\n' "$deployment_lock_token" > "$publication_lock/owner-token"
deployment_lock_owned=1
jq -cn --arg run_id "site-source-$release_sha" --arg owner_token "$deployment_lock_token" \
  --arg started_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" --argjson pid "$$" --argjson heartbeat_epoch "$(date +%s)" \
  '{schema_version:1,run_id:$run_id,scope:"site-source-deployment",pid:$pid,job_identity:null,process_identity:null,owner_token:$owner_token,started_at:$started_at,heartbeat_epoch:$heartbeat_epoch}' \
  > "$publication_lock/owner.json.tmp.$$"
mv "$publication_lock/owner.json.tmp.$$" "$publication_lock/owner.json"

release_dir="$source_root/releases/$release_sha"
if [ -e "$release_dir" ]; then
  [ -d "$release_dir" ] && [ ! -L "$release_dir" ] || { echo "Immutable site-source collision" >&2; exit 1; }
  diff -qr "$extracted" "$release_dir" >/dev/null || { echo "Immutable site-source release differs" >&2; exit 1; }
else
  mv "$extracted" "$release_dir"
fi
ln -s "releases/$release_sha" "$temporary_link"
python3 -c 'import os,sys; os.replace(sys.argv[1], sys.argv[2])' "$temporary_link" "$source_root/current"
rm -f -- "$staged_archive"

while IFS= read -r old_release; do
  old_name="$(basename "$old_release")"
  [[ "$old_name" =~ ^[0-9a-f]{40}$ ]] || continue
  [ "$old_name" = "$release_sha" ] && continue
  release_count="$(find "$source_root/releases" -mindepth 1 -maxdepth 1 -type d -name '????????????????????????????????????????' | wc -l | tr -d ' ')"
  [ "$release_count" -le "${WIKI_ECON_SITE_SOURCE_RETENTION:-3}" ] && break
  rm -rf -- "$old_release"
done < <(find "$source_root/releases" -mindepth 1 -maxdepth 1 -type d -name '????????????????????????????????????????' -print | LC_ALL=C sort)

[ -f "$release_dir/site-source-provenance.json" ] || {
  echo "Installed site-source release disappeared during retention: $release_sha" >&2
  exit 1
}
echo "Installed verified site-source release $release_sha"
echo "Stable site source: $source_root/current/site"
echo "Content SHA-256: $(jq -r '.content_sha256' "$release_dir/site-source-provenance.json")"
