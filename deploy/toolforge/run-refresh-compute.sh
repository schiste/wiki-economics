#!/usr/bin/env bash
set -euo pipefail

# Toolforge's Jobs Framework (CLI 0.3.9) has no per-job envvar support: `jobs
# run` takes no --envvar/--envvars flag, and `toolforge envvars create` is
# tool-wide only. This wrapper stands in for a per-job WIKI_ECON_REFRESH_STAGE
# so the on-demand `wiki-econ-compute` Job (see jobs.yaml) can reuse
# run-refresh.sh unmodified. See README.md's "On-demand stage jobs" section.

export WIKI_ECON_REFRESH_STAGE=compute
exec "$(dirname "${BASH_SOURCE[0]}")/run-refresh.sh" "$@"
