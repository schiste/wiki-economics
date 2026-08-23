#!/usr/bin/env bash
set -euo pipefail

[ "$#" -eq 1 ] || { echo "Usage: verify-clean-operator-path.sh <deployed-40-character-sha>" >&2; exit 2; }
release_sha=$1
[[ "$release_sha" =~ ^[0-9a-f]{40}$ ]] || { echo "Invalid release SHA" >&2; exit 2; }
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
target="${TOOLFORGE_SSH_TARGET:?Set TOOLFORGE_SSH_TARGET to user@login.toolforge.org}"

[ "$(git -C "$ROOT" rev-parse HEAD)" = "$release_sha" ] || { echo "Operator checkout is not the deployed commit" >&2; exit 1; }
git -C "$ROOT" diff --quiet
git -C "$ROOT" diff --cached --quiet
[ -z "$(git -C "$ROOT" status --porcelain --untracked-files=normal)" ] || { echo "Operator checkout is not clean" >&2; exit 1; }
for command in gh jq node ssh tar; do
  command -v "$command" >/dev/null || { echo "Missing operator command: $command" >&2; exit 1; }
done
gh auth status >/dev/null

ssh -o BatchMode=yes "$target" "become wiki-economics bash -s -- '$release_sha'" <<'REMOTE'
set -euo pipefail
release_sha=$1
app_root=/data/project/wiki-economics/app
[ "$(readlink "$app_root/current")" = "releases/$release_sha" ]
(cd "$app_root/releases/$release_sha" && sha256sum --check --strict --status SHA256SUMS)
(cd "$app_root/releases/$release_sha" && sha256sum --check --status wiki-econ.sha256)
"$app_root/current/wiki-econ" --help >/dev/null
toolforge envvars show WIKI_ECON_BIN | grep -F "$app_root/current/wiki-econ" >/dev/null
toolforge webservice status
REMOTE

echo "Clean operator SSH deployment path verified for $release_sha"
