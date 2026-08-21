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

REPO_URL="${WIKI_ECON_REPO_URL:-https://github.com/schiste/wiki-economics.git}"
POLL_INTERVAL_SECS="${WIKI_ECON_BUILD_POLL_INTERVAL:-20}"
POLL_TIMEOUT_SECS="${WIKI_ECON_BUILD_POLL_TIMEOUT:-1800}"

echo "==> Starting build for $REPO_URL"
# Ignore this command's own exit code / log output: see note (1) above.
# The build itself is a server-side pipeline independent of this CLI call.
toolforge build start "$REPO_URL" || true

echo "==> Waiting for the newest build to finish (polling toolforge build list)"
elapsed=0
build_status=""
build_id=""
while [ "$elapsed" -lt "$POLL_TIMEOUT_SECS" ]; do
  # Newest build is always the first data row; skip the single header line.
  build_line="$(toolforge build list | sed -n '2p')"
  build_id="$(awk '{print $1}' <<< "$build_line")"
  build_status="$(awk '{print $2}' <<< "$build_line")"

  if [ "$build_status" = "ok" ] || [ "$build_status" = "error" ] || [ "$build_status" = "failed" ]; then
    break
  fi

  sleep "$POLL_INTERVAL_SECS"
  elapsed=$((elapsed + POLL_INTERVAL_SECS))
done

echo "==> Build $build_id finished with status: $build_status"

if [ "$build_status" != "ok" ]; then
  echo "Build did not succeed (status: ${build_status:-timed out}) — leaving running processes untouched." >&2
  echo "Inspect with: toolforge build logs $build_id" >&2
  exit 1
fi

echo "==> Restarting wiki-econ-admin webservice"
toolforge webservice restart

echo "==> Restarting continuous Toolforge Jobs (if any)"
# wiki-econ-admin is deployed as a plain `toolforge webservice`, not loaded
# from jobs.yaml despite that file defining it with `continuous: true` (see
# the comment in jobs.yaml) — restarted above. This loop is defensive
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
