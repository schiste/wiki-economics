#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
# shellcheck disable=SC1091
. "$ROOT/deploy/cloud-vps/lib.sh"

SERVICE_USER="${WIKI_ECON_SERVICE_USER:-wiki-econ}"
BASE_DIR="/srv/wiki-economics"

if [ "$(id -u)" -ne 0 ]; then
  echo "bootstrap.sh must run as root." >&2
  exit 1
fi

apt-get update
apt-get install -y git curl ca-certificates build-essential python3 python3-venv nodejs npm nginx
if ! apt-get install -y duckdb; then
  echo "Warning: failed to install duckdb from apt. Install a DuckDB CLI package manually before running refresh jobs." >&2
fi

if ! id "$SERVICE_USER" >/dev/null 2>&1; then
  useradd --system --create-home --home-dir "$BASE_DIR" --shell /bin/bash "$SERVICE_USER"
fi

install -d -o "$SERVICE_USER" -g "$SERVICE_USER" \
  "$BASE_DIR" \
  "$BASE_DIR/app/releases" \
  "$BASE_DIR/data" \
  "$BASE_DIR/output/releases" \
  "$BASE_DIR/site/releases" \
  "$BASE_DIR/state"

SERVICE_HOME="$(getent passwd "$SERVICE_USER" | cut -d: -f6)"
if ! runuser -u "$SERVICE_USER" -- test -x "$SERVICE_HOME/.cargo/bin/rustup"; then
  runuser -u "$SERVICE_USER" -- sh -lc 'curl https://sh.rustup.rs -sSf | sh -s -- -y --default-toolchain stable --profile minimal'
fi

runuser -u "$SERVICE_USER" -- sh -lc 'export PATH="$HOME/.cargo/bin:$PATH"; rustup default stable >/dev/null; rustup component add rustfmt clippy >/dev/null'

install -d /etc/wiki-economics
if [ ! -f /etc/wiki-economics.env ]; then
  cp "$ROOT/deploy/cloud-vps/env.example" /etc/wiki-economics.env
fi
if [ ! -f /etc/wiki-economics/wikis.txt ]; then
  printf 'frwiki\n' > /etc/wiki-economics/wikis.txt
fi

echo "Bootstrap complete."
echo "Next steps:"
echo "  1. Edit /etc/wiki-economics.env or render it from deployment secrets with deploy/cloud-vps/render-env.sh"
echo "  2. Edit /etc/wiki-economics/wikis.txt"
echo "  3. Run sudo -u $SERVICE_USER -H ./deploy/cloud-vps/deploy-release.sh"
echo "  4. Install the nginx and systemd files from deploy/cloud-vps/"
echo "  5. If you enable the hosted admin surface, provision OIDC client settings, a canonical public origin, and the email allowlist in /etc/wiki-economics.env"
