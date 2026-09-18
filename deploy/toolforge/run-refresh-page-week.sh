#!/usr/bin/env bash
set -euo pipefail

# Low-memory stage 4: page-week aggregation in its own process.
export WIKI_ECON_REFRESH_STAGE=page-week
export WIKI_ECON_PIPELINE_MODE=1
exec "$(dirname "${BASH_SOURCE[0]}")/run-refresh.sh" "$@"
