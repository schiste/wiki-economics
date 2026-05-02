#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
# shellcheck disable=SC1091
. "$ROOT/deploy/cloud-vps/lib.sh"
wiki_econ_cloud_load_env
wiki_econ_cloud_require_service_user "$0" "$@"
wiki_econ_cloud_prepare_toolchain_env

release_stamp="$(wiki_econ_cloud_release_stamp)"
tmp_dir="$WIKI_ECON_APP_RELEASES/.tmp-$release_stamp"

mkdir -p "$WIKI_ECON_APP_RELEASES" "$WIKI_ECON_SITE_RELEASES"
rm -rf "$tmp_dir"
git clone --depth 1 --branch "$WIKI_ECON_REPO_REF" "$WIKI_ECON_REPO_URL" "$tmp_dir"

short_sha="$(git -C "$tmp_dir" rev-parse --short HEAD)"
release_name="${release_stamp}-${short_sha}"
release_dir="$WIKI_ECON_APP_RELEASES/$release_name"
site_release="$WIKI_ECON_SITE_RELEASES/$release_name"

mv "$tmp_dir" "$release_dir"

export CARGO_TARGET_DIR="$release_dir/target"
(cd "$release_dir" && cargo build --release)
(cd "$release_dir/site" && npm ci)

wiki_econ_cloud_switch_symlink "$WIKI_ECON_APP_CURRENT" "$release_dir"

if wiki_econ_cloud_has_merged_artifacts "$WIKI_ECON_OUTPUT_CURRENT"; then
  WIKI_ECON_ENV=production \
  WIKI_ECON_OUTPUT_DIR="$WIKI_ECON_OUTPUT_CURRENT" \
  WIKI_ECON_SITE_DIST_DIR="$site_release" \
  "$release_dir/scripts/build-site.sh"
  wiki_econ_cloud_switch_symlink "$WIKI_ECON_SITE_CURRENT" "$site_release"
else
  echo "No current merged artifacts found at $WIKI_ECON_OUTPUT_CURRENT; skipping site rebuild for this code deploy."
fi

echo "Deployed application release: $release_name"
