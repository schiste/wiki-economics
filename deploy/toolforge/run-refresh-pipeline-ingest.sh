#!/usr/bin/env bash
set -euo pipefail

# Stage 1 of the resumable low-memory pipeline. Keep this wrapper separate
# from the legacy standalone ingest job so existing recovery workflows do not
# unexpectedly create or reset a six-stage coordinator state file.
export WIKI_ECON_REFRESH_STAGE=ingest
export WIKI_ECON_PIPELINE_MODE=1
exec "$(dirname "${BASH_SOURCE[0]}")/run-refresh.sh" "$@"
