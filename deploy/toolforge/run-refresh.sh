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
