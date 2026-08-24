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
  --version YYYY-MM      Pin the dump snapshot version for fetch/run
  --data-dir PATH        Override the data directory
  --output-dir PATH      Override the output artifact directory
  --dist-dir PATH        Override the site build output directory
  --wikis-file FILE      Read wiki names from a newline-delimited file
  --stage STAGE          Which part of the pipeline to run: all (default),
                          ingest, compute, or site
  --merge-only           Only refresh merged artifacts, then build the site
  --skip-site-build      Skip the Observable production build
  -h, --help             Show this help message

Stages:
  all      Fetch/ingest, compute, validate, build the site, finalize (today's
           full pipeline; used by the scheduled weekly refresh)
  ingest   Fetch, ingest, and patrol-fetch only; no compute, validate, or
           site build
  compute  Compute, patrol-compute, merge, then validate; no site build
  site     Build the site only, against whatever was last published by a
           compute run; no wikis required
EOF
}

VERSION=""
WIKIS_FILE=""
STAGE="all"
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
    --stage)
      shift
      STAGE="${1:-}"
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
WIKI_ECON_RUN_ID="${WIKI_ECON_RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)-$$}"
WIKI_ECON_REQUIRE_PUBLICATION_GATE=1
export WIKI_ECON_RUN_ID WIKI_ECON_REQUIRE_PUBLICATION_GATE

if [ -n "$WIKIS_FILE" ]; then
  while IFS= read -r wiki; do
    wiki="${wiki%%#*}"
    wiki="$(printf '%s' "$wiki" | xargs)"
    [ -n "$wiki" ] && WIKIS+=("$wiki")
  done < "$WIKIS_FILE"
fi

case "$STAGE" in
  all|ingest|compute|site) ;;
  *)
    usage
    echo "Unknown --stage value: $STAGE (expected all, ingest, compute, or site)" >&2
    exit 1
    ;;
esac

if [ "$MERGE_ONLY" -eq 0 ] && [ "$STAGE" != "site" ] && [ "${#WIKIS[@]}" -eq 0 ]; then
  usage
  echo "refresh.sh requires at least one wiki unless --merge-only or --stage site is used." >&2
  exit 1
fi

echo "==> Refresh configuration"
wiki_econ_print_runtime
echo "Run ID:       $WIKI_ECON_RUN_ID"
echo "Stage:        $STAGE"

if [ "$MERGE_ONLY" -eq 1 ]; then
  wiki_econ_run_cli merge
elif [ "$STAGE" != "site" ]; then
  declare -a cmd=(run "${WIKIS[@]}")
  if [ -n "$VERSION" ]; then
    cmd+=(--version "$VERSION")
  fi
  if [ "$STAGE" != "all" ]; then
    cmd+=(--stage "$STAGE")
  fi
  wiki_econ_run_cli "${cmd[@]}"
fi

if [ "$STAGE" != "ingest" ] && [ "$STAGE" != "site" ]; then
  wiki_econ_run_cli publication-validate
fi

RUN_SITE_BUILD=1
case "$STAGE" in
  ingest|compute)
    RUN_SITE_BUILD=0
    ;;
  all)
    [ "$SKIP_SITE_BUILD" -eq 1 ] && RUN_SITE_BUILD=0
    ;;
esac

if [ "$RUN_SITE_BUILD" -eq 1 ]; then
  "$ROOT/scripts/build-site.sh" \
    --output-dir "$WIKI_ECON_OUTPUT_DIR" \
    --dist-dir "$WIKI_ECON_SITE_DIST_DIR"
fi

# The previous warehouse generation is the rollback source until every
# downstream artifact, including the site, has published successfully. Only
# then is it safe to reclaim its NFS space. On-demand `ingest`/`compute`
# stages never build the site themselves, so they never finalize either —
# only `all` (the scheduled full pipeline) does.
if [ "$MERGE_ONLY" -eq 0 ] && [ "$STAGE" = "all" ] && [ "$RUN_SITE_BUILD" -eq 1 ]; then
  wiki_econ_run_cli snapshot-finalize "${WIKIS[@]}"
fi

echo "==> Refresh flow complete"
