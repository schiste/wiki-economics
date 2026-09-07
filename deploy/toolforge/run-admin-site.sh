#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
# shellcheck disable=SC1091
. "$ROOT/scripts/lib/wiki_econ.sh"

wiki_econ_init_runtime
wiki_econ_ensure_site_deps

admin_dist="${WIKI_ECON_ADMIN_DIST_DIR:-/data/project/wiki-economics/admin-dist}"
run_id="${WIKI_ECON_RUN_ID:-admin-site-$(date -u +%Y%m%dT%H%M%SZ)-$$}"
manifest="$WIKI_ECON_OUTPUT_DIR/manifest.json"

[ -f "$manifest" ] || { echo "Published manifest is missing: $manifest" >&2; exit 1; }

node "$ROOT/scripts/build-admin-site.cjs" \
  --root "$ROOT" \
  --site-dir "$WIKI_ECON_SITE_DIR" \
  --manifest "$manifest" \
  --dist-dir "$admin_dist" \
  --output-dir "$WIKI_ECON_OUTPUT_DIR" \
  --run-id "$run_id"

echo "Standalone admin release is live at $admin_dist"
