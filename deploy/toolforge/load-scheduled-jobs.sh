#!/usr/bin/env bash
set -euo pipefail

# Toolforge CLI 0.3.9 starts every unscheduled definition as a one-off when a
# manifest is loaded. Normal deployment must therefore load only this explicit
# schedule allowlist. The legacy definitions stay in jobs.yaml for deliberate
# `toolforge jobs load --job <name> ...` execution.

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
MANIFEST="${1:-$ROOT/deploy/toolforge/jobs.yaml}"

[ -f "$MANIFEST" ] || {
  echo "Toolforge jobs manifest is missing: $MANIFEST" >&2
  exit 1
}

scheduled_jobs=(
  wiki-econ-fleet-controller
  wiki-econ-fleet-small-a
  wiki-econ-fleet-small-b
  wiki-econ-fleet-medium
  wiki-econ-publish-ready
  wiki-econ-artifact-scrub
)
on_demand_jobs=(
  wiki-econ-prepare-nlwiki
  wiki-econ-prepare-ptwiki
  wiki-econ-prepare-frwiki
  wiki-econ-prepare-itwiki
  wiki-econ-prepare-svwiki
  wiki-econ-prepare-elwiki
  wiki-econ-refresh
  wiki-econ-ingest
  wiki-econ-compute
  wiki-econ-site
)

delete_if_loaded() {
  local job_name=$1
  if toolforge jobs show "$job_name" >/dev/null 2>&1; then
    echo "==> Removing existing job definition: $job_name"
    toolforge jobs delete "$job_name"
  fi
}

for job_name in "${on_demand_jobs[@]}"; do
  delete_if_loaded "$job_name"
done

for job_name in "${scheduled_jobs[@]}"; do
  delete_if_loaded "$job_name"
  echo "==> Loading scheduled job: $job_name"
  toolforge jobs load --job "$job_name" "$MANIFEST"
done

echo "==> Scheduled Toolforge pipeline jobs loaded; one-off jobs remain on-demand only"
