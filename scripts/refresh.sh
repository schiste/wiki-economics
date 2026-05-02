#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
# shellcheck disable=SC1091
. "$ROOT/scripts/lib/wiki_econ.sh"

usage() {
  cat <<'EOF'
Usage: ./scripts/refresh.sh [options] <wiki...>
       ./scripts/refresh.sh [options] --wikis-file FILE
       ./scripts/refresh.sh [options] --merge-only

Runs the shared batch refresh flow used by both local development and VPS ops.

Options:
  --version YYYY-MM   Pin the dump snapshot version for fetch/run
  --data-dir PATH     Override the data directory
  --output-dir PATH   Override the output artifact directory
  --dist-dir PATH     Override the site build output directory
  --wikis-file FILE   Read wiki names from a newline-delimited file
  --merge-only        Only refresh merged artifacts, then build the site
  --skip-site-build   Skip the Observable production build
  -h, --help          Show this help message
EOF
}

VERSION=""
WIKIS_FILE=""
MERGE_ONLY=0
SKIP_SITE_BUILD=0
declare -a WIKIS=()

while [ "$#" -gt 0 ]; do
  case "$1" in
    --version)
      shift
      VERSION="${1:-}"
      ;;
    --data-dir)
      shift
      WIKI_ECON_DATA_DIR="${1:-}"
      ;;
    --output-dir)
      shift
      WIKI_ECON_OUTPUT_DIR="${1:-}"
      ;;
    --dist-dir)
      shift
      WIKI_ECON_SITE_DIST_DIR="${1:-}"
      ;;
    --wikis-file)
      shift
      WIKIS_FILE="${1:-}"
      ;;
    --merge-only)
      MERGE_ONLY=1
      ;;
    --skip-site-build)
      SKIP_SITE_BUILD=1
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    --)
      shift
      while [ "$#" -gt 0 ]; do
        WIKIS+=("$1")
        shift
      done
      break
      ;;
    -*)
      usage
      echo "Unknown option: $1" >&2
      exit 1
      ;;
    *)
      WIKIS+=("$1")
      ;;
  esac
  shift
done

wiki_econ_init_runtime
wiki_econ_ensure_local_dirs

if [ -n "$WIKIS_FILE" ]; then
  while IFS= read -r wiki; do
    wiki="${wiki%%#*}"
    wiki="$(printf '%s' "$wiki" | xargs)"
    [ -n "$wiki" ] && WIKIS+=("$wiki")
  done < "$WIKIS_FILE"
fi

if [ "$MERGE_ONLY" -eq 0 ] && [ "${#WIKIS[@]}" -eq 0 ]; then
  usage
  echo "refresh.sh requires at least one wiki unless --merge-only is used." >&2
  exit 1
fi

echo "==> Refresh configuration"
wiki_econ_print_runtime

if [ "$MERGE_ONLY" -eq 1 ]; then
  wiki_econ_run_cli merge
else
  declare -a cmd=(run "${WIKIS[@]}")
  if [ -n "$VERSION" ]; then
    cmd+=(--version "$VERSION")
  fi
  wiki_econ_run_cli "${cmd[@]}"
fi

if [ "$SKIP_SITE_BUILD" -eq 0 ]; then
  "$ROOT/scripts/build-site.sh" \
    --output-dir "$WIKI_ECON_OUTPUT_DIR" \
    --dist-dir "$WIKI_ECON_SITE_DIST_DIR"
fi

echo "==> Refresh flow complete"
