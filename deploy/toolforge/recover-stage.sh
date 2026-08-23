#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat >&2 <<'EOF'
Usage:
  recover-stage.sh ingest <wiki> <snapshot>
  recover-stage.sh compute <wiki>
  recover-stage.sh site
  recover-stage.sh pointer <wiki> <snapshot>
  recover-stage.sh site-link <generation-directory-name>
EOF
  exit 2
}

[ "$#" -ge 1 ] || usage
action=$1
shift
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
data_dir="${WIKI_ECON_DATA_DIR:-/data/project/wiki-economics/data}"
output_dir="${WIKI_ECON_OUTPUT_DIR:-/data/project/wiki-economics/output}"
dist_dir="${WIKI_ECON_SITE_DIST_DIR:-/data/project/wiki-economics/site-dist}"
lock_dir="${WIKI_ECON_REFRESH_LOCK_DIR:-$output_dir/.refresh-lock}"
[ ! -e "$lock_dir" ] || { echo "Refusing recovery while a refresh lock exists: $lock_dir" >&2; exit 75; }

run_cli() {
  local bin_path="${WIKI_ECON_BIN:?WIKI_ECON_BIN is required}"
  "$bin_path" --data-dir "$data_dir" --output-dir "$output_dir" "$@"
}

case "$action" in
  ingest)
    [ "$#" -eq 2 ] || usage
    wiki=$1 snapshot=$2
    run_cli fetch "$wiki" --version "$snapshot"
    run_cli ingest "$wiki" --version "$snapshot"
    ;;
  compute)
    [ "$#" -eq 1 ] || usage
    run_cli compute "$1"
    ;;
  site)
    [ "$#" -eq 0 ] || usage
    WIKI_ECON_RUN_ID="recovery-site-$(date -u +%Y%m%dT%H%M%SZ)-$$" \
      "${WIKI_ECON_RECOVERY_REFRESH_DRIVER:-$ROOT/scripts/refresh.sh}" \
      --data-dir "$data_dir" --output-dir "$output_dir" --dist-dir "$dist_dir" --merge-only
    ;;
  pointer)
    [ "$#" -eq 2 ] || usage
    run_cli snapshot-repair "$1" --version "$2"
    ;;
  site-link)
    [ "$#" -eq 1 ] || usage
    generation=$1
    dist_parent="$(dirname "$dist_dir")"
    dist_name="$(basename "$dist_dir")"
    case "$generation" in ".${dist_name}.build."*) ;; *) echo "Unsafe site generation: $generation" >&2; exit 1 ;; esac
    target="$dist_parent/$generation"
    [ -d "$target" ] || { echo "Site generation does not exist: $target" >&2; exit 1; }
    for page in index.html business.html gdp.html inequality.html labor.html patrol.html edit-variation.html; do
      [ -f "$target/$page" ] || { echo "Site generation is missing $page" >&2; exit 1; }
    done
    if [ -e "$dist_dir" ] && [ ! -L "$dist_dir" ]; then
      echo "Refusing to replace non-symlink site path: $dist_dir" >&2
      exit 1
    fi
    temporary="$dist_parent/.${dist_name}.recovery.$$"
    trap 'rm -f -- "$temporary"' EXIT
    ln -s "$generation" "$temporary"
    node -e 'require("node:fs").renameSync(process.argv[1], process.argv[2])' "$temporary" "$dist_dir"
    ;;
  *) usage ;;
esac

echo "Recovery action completed: $action"
