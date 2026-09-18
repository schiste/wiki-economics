#!/usr/bin/env bash
set -euo pipefail

# Low-memory stage 3: lifecycle computation in its own process. The compute
# command's --external path is deliberately selected by the shared driver.
export WIKI_ECON_REFRESH_STAGE=lifecycle
export WIKI_ECON_PIPELINE_MODE=1
exec "$(dirname "${BASH_SOURCE[0]}")/run-refresh.sh" "$@"
