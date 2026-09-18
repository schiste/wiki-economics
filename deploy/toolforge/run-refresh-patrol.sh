#!/usr/bin/env bash
set -euo pipefail

# Low-memory stage 5: patrol metrics and per-wiki patrol artifacts.
export WIKI_ECON_REFRESH_STAGE=patrol
export WIKI_ECON_PIPELINE_MODE=1
exec "$(dirname "${BASH_SOURCE[0]}")/run-refresh.sh" "$@"
