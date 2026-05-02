#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

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

echo "==> python3 -m py_compile scripts/fetch_patrol.py scripts/compute_patrol.py scripts/check_lcov.py scripts/test_fetch_patrol.py"
python3 -m py_compile \
  scripts/fetch_patrol.py \
  scripts/compute_patrol.py \
  scripts/check_lcov.py \
  scripts/test_fetch_patrol.py

echo "==> python3 -m unittest scripts/test_fetch_patrol.py"
python3 -m unittest scripts/test_fetch_patrol.py

echo "==> all local checks passed"
