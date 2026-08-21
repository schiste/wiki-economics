#!/usr/bin/env bash
set -euo pipefail

# Refresh wrapper invoked by the `wiki-econ-refresh` Toolforge Job.
#
# Unlike deploy/cloud-vps/run-refresh.sh, this does NOT keep a
# releases/current symlink history for output/site: Toolforge's NFS quota is
# small (see deploy/toolforge/README.md), and retaining multiple full output
# generations is expensive relative to the benefit. This refreshes
# WIKI_ECON_OUTPUT_DIR / WIKI_ECON_SITE_DIST_DIR in place instead.
#
# Raw dump cleanup is NOT done here: `wiki-econ run` (invoked by
# scripts/refresh.sh) deletes each wiki's raw .bz2 files itself immediately
# after that wiki's ingest stage succeeds, rather than waiting for every
# wiki in the batch plus the site build to finish. That's safe because
# src/storage.rs::marker_manifest_is_valid only checks that
# warehouse/analytical parquet outputs exist, never the raw .bz2 source —
# later runs stay idempotent without it.

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
# shellcheck disable=SC1091
. "$ROOT/scripts/lib/wiki_econ.sh"

: "${WIKI_ECON_ENABLED_WIKIS:?Set WIKI_ECON_ENABLED_WIKIS (space or comma separated) in jobs.yaml}"

# Write a status marker + rolling history to the shared NFS output dir on
# every exit (success, an early `exit 1` artifact check below, or a
# set -e-triggered failure) so the admin webservice — a separate Toolforge
# pod with no shared process memory and no Toolforge/Kubernetes API access —
# has a way to know whether the last scheduled refresh succeeded.
REFRESH_STARTED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
REFRESH_START_EPOCH="$(date +%s)"
REFRESH_HISTORY_LIMIT=20

write_refresh_status() {
  local exit_code=$1
  if [ -z "${WIKI_ECON_OUTPUT_DIR:-}" ]; then
    return 0
  fi
  local finished_at duration wikis_json entry status_file history_file
  finished_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  duration=$(( $(date +%s) - REFRESH_START_EPOCH ))
  wikis_json=$(printf '"%s",' "${wikis[@]:-}" | sed 's/,$//')
  entry=$(printf '{"startedAt":"%s","finishedAt":"%s","exitCode":%d,"wikis":[%s],"durationSecs":%d}' \
    "$REFRESH_STARTED_AT" "$finished_at" "$exit_code" "$wikis_json" "$duration")
  status_file="$WIKI_ECON_OUTPUT_DIR/.refresh-status.json"
  history_file="$WIKI_ECON_OUTPUT_DIR/.refresh-history.jsonl"
  echo "$entry" > "$status_file"
  echo "$entry" >> "$history_file"
  tail -n "$REFRESH_HISTORY_LIMIT" "$history_file" > "${history_file}.tmp" && mv "${history_file}.tmp" "$history_file"
}
trap 'write_refresh_status $?' EXIT

wiki_econ_init_runtime
wiki_econ_ensure_local_dirs

IFS=', ' read -r -a wikis <<< "$WIKI_ECON_ENABLED_WIKIS"

echo "==> Toolforge refresh: ${wikis[*]}"
"$ROOT/scripts/refresh.sh" "${wikis[@]}"

for required in \
  manifest.json \
  defaults_business.json \
  defaults_gdp.json \
  defaults_inequality.json \
  defaults_labor.json \
  defaults_patrol.json \
  defaults_edit_variation.json \
  business_funnel.parquet \
  gdp.parquet \
  gdp_activity_tiers.parquet \
  gdp_user_type_share.parquet \
  inequality.parquet \
  labor_churn.parquet \
  labor_cohorts.parquet \
  labor_monthly.parquet \
  page_weekly_edits.parquet \
  patrol.parquet
do
  if [ ! -f "$WIKI_ECON_OUTPUT_DIR/$required" ]; then
    echo "Refresh succeeded but required artifact is missing: $WIKI_ECON_OUTPUT_DIR/$required" >&2
    exit 1
  fi
done

for page in index.html business.html gdp.html inequality.html labor.html patrol.html edit-variation.html; do
  if [ ! -f "$WIKI_ECON_SITE_DIST_DIR/$page" ]; then
    echo "Site build is missing required page: $WIKI_ECON_SITE_DIST_DIR/$page" >&2
    exit 1
  fi
done

echo "==> Toolforge refresh complete"
