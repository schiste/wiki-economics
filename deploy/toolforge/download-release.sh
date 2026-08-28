#!/usr/bin/env bash
set -euo pipefail

# Downloads the successful GitHub Actions release artifact for one exact
# commit, discovers its archive through the signed checksum manifest, and
# normalizes GitHub's implementation-defined extraction layout.

usage() {
  echo "Usage: download-release.sh <40-character-git-sha> <destination-directory> [workflow-run-id]" >&2
  exit 2
}

[ "$#" -ge 2 ] && [ "$#" -le 3 ] || usage

release_sha=$1
destination=$2
run_id=${3:-}
repository=${WIKI_ECON_GITHUB_REPOSITORY:-schiste/wiki-economics}
artifact_name="wiki-econ-linux-x86_64-$release_sha"
archive_name="wiki-econ-release-$release_sha.tar.gz"
checksum_name="$archive_name.sha256"

[[ "$release_sha" =~ ^[0-9a-f]{40}$ ]] || usage
[ -z "$run_id" ] || [[ "$run_id" =~ ^[0-9]+$ ]] || usage
for command in gh find; do
  command -v "$command" >/dev/null 2>&1 || {
    echo "Required release-download command is missing: $command" >&2
    exit 1
  }
done

if [ -z "$run_id" ]; then
  run_id="$(gh run list --repo "$repository" --commit "$release_sha" --workflow CI \
    --limit 20 --json databaseId,status,conclusion \
    --jq 'map(select(.status == "completed" and .conclusion == "success")) | first | .databaseId // empty')"
  [ -n "$run_id" ] || {
    echo "No successful completed CI run found for $release_sha" >&2
    exit 1
  }
fi

IFS=$'\t' read -r run_sha run_status run_conclusion < <(
  gh run view "$run_id" --repo "$repository" --json headSha,status,conclusion \
    --jq '[.headSha, .status, .conclusion] | @tsv'
)
[ "$run_sha" = "$release_sha" ] && [ "$run_status" = completed ] && [ "$run_conclusion" = success ] || {
  echo "Workflow run $run_id is not a successful completed run for $release_sha" >&2
  exit 1
}

mkdir -p "$destination"
destination="$(cd "$destination" && pwd)"
download_root="$(mktemp -d "$destination/.wiki-econ-release-download.XXXXXX")"
cleanup() {
  rm -rf -- "$download_root"
}
trap cleanup EXIT

gh run download "$run_id" --repo "$repository" --name "$artifact_name" --dir "$download_root"

manifests=()
while IFS= read -r -d '' manifest; do
  manifests+=("$manifest")
done < <(find "$download_root" -type f -name "$checksum_name" -print0)
[ "${#manifests[@]}" -eq 1 ] || {
  echo "Release artifact must contain exactly one $checksum_name; found ${#manifests[@]}" >&2
  exit 1
}

checksum_path=${manifests[0]}
read -r expected_checksum declared_archive < "$checksum_path" || {
  echo "Cannot read release archive checksum manifest" >&2
  exit 1
}
[[ "$expected_checksum" =~ ^[0-9a-f]{64}$ ]] && [ "$declared_archive" = "$archive_name" ] || {
  echo "Release archive checksum manifest is malformed" >&2
  exit 1
}
archive_path="$(dirname "$checksum_path")/$declared_archive"
[ -f "$archive_path" ] && [ ! -L "$archive_path" ] || {
  echo "Release archive declared by the checksum manifest is missing" >&2
  exit 1
}
if command -v sha256sum >/dev/null 2>&1; then
  actual_checksum="$(sha256sum "$archive_path" | awk '{print $1}')"
else
  actual_checksum="$(shasum -a 256 "$archive_path" | awk '{print $1}')"
fi
[ "$actual_checksum" = "$expected_checksum" ] || {
  echo "Release archive checksum mismatch" >&2
  exit 1
}

archive_temporary="$destination/.$archive_name.$$.tmp"
checksum_temporary="$destination/.$checksum_name.$$.tmp"
cp "$archive_path" "$archive_temporary"
cp "$checksum_path" "$checksum_temporary"
mv "$archive_temporary" "$destination/$archive_name"
mv "$checksum_temporary" "$destination/$checksum_name"

printf 'workflow_run_id=%s\n' "$run_id"
printf 'release_archive=%s\n' "$destination/$archive_name"
printf 'release_checksum=%s\n' "$destination/$checksum_name"
