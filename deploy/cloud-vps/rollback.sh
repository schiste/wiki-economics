#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
# shellcheck disable=SC1091
. "$ROOT/deploy/cloud-vps/lib.sh"
wiki_econ_cloud_load_env
wiki_econ_cloud_require_service_user "$0" "$@"

usage() {
  cat <<'EOF'
Usage: ./deploy/cloud-vps/rollback.sh [--app RELEASE] [--output RELEASE] [--site RELEASE]
EOF
}

APP_RELEASE=""
OUTPUT_RELEASE=""
SITE_RELEASE=""

while [ "$#" -gt 0 ]; do
  case "$1" in
    --app)
      shift
      APP_RELEASE="${1:-}"
      ;;
    --output)
      shift
      OUTPUT_RELEASE="${1:-}"
      ;;
    --site)
      shift
      SITE_RELEASE="${1:-}"
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      usage
      echo "Unknown option: $1" >&2
      exit 1
      ;;
  esac
  shift
done

if [ -z "$APP_RELEASE" ] && [ -z "$OUTPUT_RELEASE" ] && [ -z "$SITE_RELEASE" ]; then
  usage
  echo "At least one release target is required." >&2
  exit 1
fi

if [ -n "$APP_RELEASE" ]; then
  wiki_econ_cloud_switch_symlink "$WIKI_ECON_APP_CURRENT" "$WIKI_ECON_APP_RELEASES/$APP_RELEASE"
fi
if [ -n "$OUTPUT_RELEASE" ]; then
  wiki_econ_cloud_switch_symlink "$WIKI_ECON_OUTPUT_CURRENT" "$WIKI_ECON_OUTPUT_RELEASES/$OUTPUT_RELEASE"
fi
if [ -n "$SITE_RELEASE" ]; then
  wiki_econ_cloud_switch_symlink "$WIKI_ECON_SITE_CURRENT" "$WIKI_ECON_SITE_RELEASES/$SITE_RELEASE"
fi

echo "Rollback complete."
