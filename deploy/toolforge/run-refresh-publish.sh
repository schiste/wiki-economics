#!/usr/bin/env bash
set -euo pipefail

# Stage 6: merge, publication validation, site build, and finalization.
export WIKI_ECON_REFRESH_STAGE=publish
export WIKI_ECON_PIPELINE_MODE=1
exec "$(dirname "${BASH_SOURCE[0]}")/run-refresh.sh" "$@"
