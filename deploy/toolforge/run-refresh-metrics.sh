#!/usr/bin/env bash
set -euo pipefail

# Low-memory stage 2: monthly and activity-tier metric families. The shared
# refresh wrapper supplies the pinned pipeline snapshot and stage receipt.
export WIKI_ECON_REFRESH_STAGE=metrics
export WIKI_ECON_PIPELINE_MODE=1
exec "$(dirname "${BASH_SOURCE[0]}")/run-refresh.sh" "$@"
