#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

echo "==> bash -n scripts/*.sh scripts/lib/*.sh site/data-build/*.sh deploy/cloud-vps/*.sh"
bash -n scripts/*.sh scripts/lib/*.sh site/data-build/*.sh deploy/cloud-vps/*.sh

echo "==> node --check site/admin-server.cjs"
node --check site/admin-server.cjs

echo "==> node --check site/observablehq.config.js"
node --check site/observablehq.config.js

echo "==> ./scripts/build-site.sh --help"
./scripts/build-site.sh --help

echo "==> ./scripts/refresh.sh --help"
./scripts/refresh.sh --help

echo "==> cargo fmt --all -- --check"
cargo fmt --all -- --check

echo "==> cargo clippy --all-targets --all-features -- -D warnings"
cargo clippy --all-targets --all-features -- -D warnings

echo "==> cargo test --all-targets --all-features"
cargo test --all-targets --all-features

echo "==> cargo doc --no-deps"
cargo doc --no-deps

echo "==> cargo llvm-cov --workspace --all-features --all-targets --lcov --output-path /tmp/wiki-economics-target/llvm-cov.info"
cargo llvm-cov --workspace --all-features --all-targets --lcov --output-path /tmp/wiki-economics-target/llvm-cov.info

echo "==> python3 scripts/check_lcov.py /tmp/wiki-economics-target/llvm-cov.info"
python3 scripts/check_lcov.py /tmp/wiki-economics-target/llvm-cov.info

echo "==> cargo deny check advisories bans licenses sources"
cargo deny check advisories bans licenses sources

echo "==> cargo audit -D warnings"
cargo audit -D warnings

echo "==> scripts/check_vendor_polars.sh"
scripts/check_vendor_polars.sh

echo "==> python3 -m py_compile scripts/fetch_patrol.py scripts/compute_patrol.py scripts/check_lcov.py scripts/test_fetch_patrol.py scripts/test_check_lcov.py"
python3 -m py_compile \
  scripts/fetch_patrol.py \
  scripts/compute_patrol.py \
  scripts/check_lcov.py \
  scripts/test_fetch_patrol.py \
  scripts/test_check_lcov.py

echo "==> python3 -m unittest discover -s scripts -p 'test_*.py'"
python3 -m unittest discover -s scripts -p 'test_*.py'

if [ -f "$ROOT/output/manifest.json" ]; then
  echo "==> ./scripts/build-site.sh"
  ./scripts/build-site.sh
else
  echo "==> skipping site build smoke check (output/manifest.json not present)"
fi

echo "==> all local checks passed"
