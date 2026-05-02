#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

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
if [ ! -d site/node_modules ]; then
  echo "==> Installing site dependencies..."
  (cd site && npm install)
fi

# 3. Start admin API server (port 3001)
echo "==> Starting admin API server on :3001..."
node site/admin-server.cjs &
ADMIN_PID=$!

# 4. Start Observable dev server (port 3000)
echo "==> Starting Observable dev server on :3000..."
(cd site && npm run dev) &
SITE_PID=$!

echo ""
echo "==> All services running:"
echo "    Dashboard:  http://127.0.0.1:3000/"
echo "    Admin page: http://127.0.0.1:3000/admin"
echo "    Admin API:  http://127.0.0.1:3001/"
echo ""
echo "Press Ctrl+C to stop everything."

wait
