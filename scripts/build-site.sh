#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
# shellcheck disable=SC1091
. "$ROOT/scripts/lib/wiki_econ.sh"

usage() {
  cat <<'EOF'
Usage: ./scripts/build-site.sh [options]

Builds the production Observable site against the current output artifact set.

Options:
  --output-dir PATH   Override the output artifact directory
  --dist-dir PATH     Override the site build output directory
  -h, --help          Show this help message
EOF
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --output-dir)
      shift
      WIKI_ECON_OUTPUT_DIR="${1:-}"
      ;;
    --dist-dir)
      shift
      WIKI_ECON_SITE_DIST_DIR="${1:-}"
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

wiki_econ_init_runtime
wiki_econ_ensure_local_dirs
wiki_econ_ensure_site_deps
mkdir -p "$(dirname "$WIKI_ECON_SITE_DIST_DIR")"

echo "==> Building Observable site"
echo "    output dir: $WIKI_ECON_OUTPUT_DIR"
echo "    dist dir:   $WIKI_ECON_SITE_DIST_DIR"

(cd "$WIKI_ECON_SITE_DIR" && npm run build)

echo "==> Site build complete"
