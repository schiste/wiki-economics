#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
# shellcheck disable=SC1091
. "$ROOT/deploy/cloud-vps/lib.sh"
wiki_econ_cloud_load_env

mkdir -p "$WIKI_ECON_STATE_ROOT" "$WIKI_ECON_OUTPUT_RELEASES" "$WIKI_ECON_SITE_RELEASES"
wiki_econ_cloud_require_file "$WIKI_ECON_ENABLED_WIKIS_FILE"

exec 9>"$WIKI_ECON_STATE_ROOT/refresh.lock"
if ! flock -n 9; then
  echo "Another wiki-economics refresh is already running." >&2
  exit 1
fi

current_app="$(readlink -f "$WIKI_ECON_APP_CURRENT")"
wiki_econ_cloud_require_file "$current_app/scripts/refresh.sh"
wiki_econ_cloud_require_file "$current_app/target/release/wiki-econ"

release_stamp="$(wiki_econ_cloud_release_stamp)"
short_sha="$(git -C "$current_app" rev-parse --short HEAD)"
release_name="${release_stamp}-${short_sha}"
output_release="$WIKI_ECON_OUTPUT_RELEASES/$release_name"
site_release="$WIKI_ECON_SITE_RELEASES/$release_name"

mkdir -p "$output_release"

WIKI_ECON_ENV=production \
WIKI_ECON_BIN="$current_app/target/release/wiki-econ" \
WIKI_ECON_DATA_DIR="$WIKI_ECON_DATA_DIR" \
WIKI_ECON_OUTPUT_DIR="$output_release" \
WIKI_ECON_GENERATOR_DIR="$current_app/site/data-build" \
WIKI_ECON_SITE_DIST_DIR="$site_release" \
"$current_app/scripts/refresh.sh" --wikis-file "$WIKI_ECON_ENABLED_WIKIS_FILE"

for required in \
  manifest.json \
  defaults_business.json \
  defaults_gdp.json \
  defaults_inequality.json \
  defaults_labor.json \
  defaults_patrol.json \
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
  if [ ! -f "$output_release/$required" ]; then
    echo "Refresh succeeded but required artifact is missing: $output_release/$required" >&2
    exit 1
  fi
done

for page in index.html business.html gdp.html inequality.html labor.html patrol.html; do
  if [ ! -f "$site_release/$page" ]; then
    echo "Site build is missing required page: $site_release/$page" >&2
    exit 1
  fi
done

wiki_econ_cloud_switch_symlink "$WIKI_ECON_OUTPUT_CURRENT" "$output_release"
wiki_econ_cloud_switch_symlink "$WIKI_ECON_SITE_CURRENT" "$site_release"
wiki_econ_cloud_write_status "$WIKI_ECON_STATE_ROOT/last-success.json" "$release_name"

echo "Published refresh release: $release_name"
