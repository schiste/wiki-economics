#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
# shellcheck disable=SC1091
. "$ROOT/scripts/lib/wiki_econ.sh"
wiki_econ_init_runtime
wiki_econ_ensure_local_dirs
cd "$WIKI_ECON_ROOT"

cleanup() {
  echo ""
  echo "Shutting down..."
  kill $ADMIN_PID $SITE_PID 2>/dev/null || true
  wait $ADMIN_PID $SITE_PID 2>/dev/null || true
  echo "Done."
}
trap cleanup EXIT INT TERM

# 1. Build Rust binary in release mode
echo "==> Building Rust binary (release)..."
cargo build --release

# 2. Install site dependencies if needed
wiki_econ_ensure_site_deps

# 3. Start admin API server (port 3001)
echo "==> Starting admin API server on :${WIKI_ECON_ADMIN_PORT}..."
WIKI_ECON_ENV=local \
WIKI_ECON_ADMIN_ENABLED=1 \
WIKI_ECON_DATA_DIR="$WIKI_ECON_DATA_DIR" \
WIKI_ECON_OUTPUT_DIR="$WIKI_ECON_OUTPUT_DIR" \
WIKI_ECON_GENERATOR_DIR="$WIKI_ECON_GENERATOR_DIR" \
WIKI_ECON_SITE_PORT="$WIKI_ECON_SITE_PORT" \
WIKI_ECON_ADMIN_PORT="$WIKI_ECON_ADMIN_PORT" \
node site/admin-server.cjs &
ADMIN_PID=$!

# 4. Start Observable dev server (port 3000)
echo "==> Starting Observable dev server on :${WIKI_ECON_SITE_PORT}..."
(cd site && npm run dev -- --host 127.0.0.1 --port "$WIKI_ECON_SITE_PORT") &
SITE_PID=$!

echo ""
echo "==> All services running:"
echo "    Dashboard:  http://127.0.0.1:${WIKI_ECON_SITE_PORT}/"
echo "    Admin page: http://127.0.0.1:${WIKI_ECON_SITE_PORT}/admin"
echo "    Admin API:  http://127.0.0.1:${WIKI_ECON_ADMIN_PORT}/"
echo ""
echo "Press Ctrl+C to stop everything."

wait
