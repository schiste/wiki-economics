#!/usr/bin/env bash
set -euo pipefail

# Rebuilds the Toolforge buildpack image and restarts every continuous
# process against it, so a rebuild can never again leave a stale pod running
# with an old binary/env — the exact failure mode that caused the
# `spawn cargo ENOENT` incident (the wiki-econ-admin webservice's pod
# predated WIKI_ECON_BIN being set, so its runner fell back to a `cargo`
# binary that doesn't exist in the buildpack image).
#
# Run this from the Toolforge bastion, already inside `become
# wiki-economics` (or prefix every line with `become wiki-economics`
# yourself — this script assumes the `toolforge` CLI is already scoped to
# the tool account).
#
# Two things `toolforge` does NOT do on its own, which is why this script
# exists instead of just `toolforge build start <repo-url>`:
#   1. `toolforge build start`'s own log stream is flaky — it can disconnect
#      mid-build with `ChunkedEncodingError: Response ended prematurely` and
#      exit 0 even though the underlying Tekton pipeline keeps running. Real
#      status has to come from polling `toolforge build list`.
#   2. Pushing a new image under the `:latest` tag does not touch pods that
#      are already running — Kubernetes envvars and binaries are fixed at
#      container start. Every continuous process has to be restarted by
#      hand for a rebuild to actually take effect.

REPO_URL="${1:-${WIKI_ECON_REPO_URL:-https://github.com/schiste/wiki-economics.git}}"
SOURCE_REF="${2:-${WIKI_ECON_SOURCE_REF:-}}"
SOURCE_COMMIT="${3:-${WIKI_ECON_IMAGE_SOURCE_COMMIT:-}}"
POLL_INTERVAL_SECS="${WIKI_ECON_BUILD_POLL_INTERVAL:-20}"
POLL_TIMEOUT_SECS="${WIKI_ECON_BUILD_POLL_TIMEOUT:-1800}"

if [[ ! "$SOURCE_COMMIT" =~ ^[0-9a-f]{40}$ ]]; then
  echo "rebuild-image.sh requires the exact 40-character source commit as its third argument" >&2
  exit 2
fi
if [ "$SOURCE_REF" != "$SOURCE_COMMIT" ]; then
  echo "rebuild-image.sh requires the exact source commit as both ref and commit" >&2
  exit 2
fi

echo "==> Starting build for $REPO_URL at $SOURCE_REF"
# Detached JSON output gives us the exact build ID. This avoids both the
# flaky streaming-log connection and accidentally polling a different build
# that happened to start at the same time.
start_json="$(toolforge build start --ref "$SOURCE_REF" --detach --json "$REPO_URL")"
build_id="$(jq -er '.new_build.name // .new_build.build_id' <<< "$start_json")"

echo "==> Waiting for build $build_id to finish"
elapsed=0
build_status=""
while [ "$elapsed" -lt "$POLL_TIMEOUT_SECS" ]; do
  build_status="$(toolforge build show --json "$build_id" | jq -er '.build.status')"

  case "$build_status" in
    ok|error|cancelled|timeout)
      break
      ;;
  esac

  sleep "$POLL_INTERVAL_SECS"
  elapsed=$((elapsed + POLL_INTERVAL_SECS))
done

echo "==> Build $build_id finished with status: $build_status"

if [ "$build_status" != "ok" ]; then
  echo "Build did not succeed (status: ${build_status:-timed out}) — leaving running processes untouched." >&2
  echo "Inspect with: toolforge build logs $build_id" >&2
  exit 1
fi

build_json="$(toolforge build show --json "$build_id")"
actual_ref="$(jq -er '.build.ref' <<< "$build_json")"
actual_source="$(jq -er '.build.source_url' <<< "$build_json")"
image_digest="$(jq -er '.build.destination_image' <<< "$build_json")"
if [ "$actual_ref" != "$SOURCE_COMMIT" ] || [ "$actual_source" != "$REPO_URL" ]; then
  echo "Completed build provenance does not match requested source" >&2
  exit 1
fi
if [[ ! "$image_digest" =~ @sha256:[0-9a-f]{64}$ ]]; then
  echo "Completed build has no immutable image digest" >&2
  exit 1
fi

echo "==> Recording image provenance"
toolforge envvars create WIKI_ECON_IMAGE_SOURCE_REF "$SOURCE_REF"
toolforge envvars create WIKI_ECON_IMAGE_SOURCE_COMMIT "$SOURCE_COMMIT"
toolforge envvars create WIKI_ECON_IMAGE_DIGEST "$image_digest"

echo "==> Restarting wiki-econ-admin webservice"
toolforge webservice restart

echo "==> Restarting continuous Toolforge Jobs (if any)"
# wiki-econ-admin is deployed as a plain `toolforge webservice`, not loaded
# from jobs.yaml, and was restarted above. This loop is defensive
# future-proofing in case that ever changes, or another continuous job is
# added later.
toolforge jobs list 2>/dev/null | grep -E '^\| ' | tail -n +2 |
  while IFS='|' read -r _ name jobtype _; do
    name="$(echo "$name" | xargs)"
    jobtype="$(echo "$jobtype" | xargs)"
    case "$jobtype" in
      continuous*)
        echo "  restarting continuous job: $name"
        toolforge jobs restart "$name"
        ;;
    esac
  done

echo "==> Done. New image is live on every restarted process."
