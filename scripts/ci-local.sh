#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
SITE_FIXTURE_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/wiki-econ-site-ci.XXXXXX")"
trap 'rm -rf -- "$SITE_FIXTURE_ROOT"' EXIT

echo "==> bash -n scripts/*.sh scripts/lib/*.sh site/data-build/*.sh deploy/cloud-vps/*.sh deploy/toolforge/*.sh"
bash -n scripts/*.sh scripts/lib/*.sh site/data-build/*.sh deploy/cloud-vps/*.sh deploy/toolforge/*.sh

echo "==> node --check site/admin-auth.cjs"
node --check site/admin-auth.cjs

echo "==> node --check site/admin-server.cjs"
node --check site/admin-server.cjs

echo "==> node --check site/freshness.cjs scripts/check-freshness.cjs"
node --check site/freshness.cjs
node --check scripts/check-freshness.cjs
node --check scripts/check-npm-advisories.cjs

echo "==> node --check site/observablehq.config.js"
node --check site/observablehq.config.js

echo "==> node --check site/data-build/*.cjs"
for f in site/data-build/*.cjs; do node --check "$f"; done

echo "==> node --test site/admin-auth.test.cjs"
node --test site/admin-auth.test.cjs

echo "==> node --test site/admin-server.test.cjs"
node --test site/admin-server.test.cjs

echo "==> node --test site/build-site.test.cjs"
node --test site/build-site.test.cjs

echo "==> node --test site/data-build/manifest.test.cjs site/freshness.test.cjs"
node --test site/data-build/manifest.test.cjs
node --test site/freshness.test.cjs

echo "==> node --test deploy/toolforge/run-record.test.cjs"
node --test deploy/toolforge/run-record.test.cjs

echo "==> node --test deploy/toolforge/run-refresh.test.cjs"
node --test deploy/toolforge/run-refresh.test.cjs

echo "==> node --test scripts/check-freshness.test.cjs"
node --test scripts/check-freshness.test.cjs

echo "==> node --test scripts/check-npm-advisories.test.cjs"
node --test scripts/check-npm-advisories.test.cjs

echo "==> node --test scripts/build-site-fixture.test.cjs"
node --test scripts/build-site-fixture.test.cjs

echo "==> node --test scripts/wiki-lifecycle.test.cjs"
node --test scripts/wiki-lifecycle.test.cjs

echo "==> ./scripts/build-site.sh --help"
./scripts/build-site.sh --help

echo "==> ./scripts/refresh.sh --help"
./scripts/refresh.sh --help

echo "==> cargo fmt --all -- --check"
cargo fmt --all -- --check

echo "==> cargo clippy --locked --all-targets --all-features -- -D warnings"
cargo clippy --locked --all-targets --all-features -- -D warnings

echo "==> cargo test --locked --all-targets --all-features"
cargo test --locked --all-targets --all-features

echo "==> Generate fixture and build the real Observable production site"
cargo run --locked -- --output-dir "$SITE_FIXTURE_ROOT/data" site-fixture
node scripts/build-site-fixture.cjs \
  --data-dir "$SITE_FIXTURE_ROOT/data" \
  --dist-dir "$SITE_FIXTURE_ROOT/dist"

echo "==> cargo doc --locked --no-deps"
cargo doc --locked --no-deps

echo "==> cargo llvm-cov --locked --workspace --all-features --all-targets --lcov --output-path target/llvm-cov.info"
cargo llvm-cov --locked --workspace --all-features --all-targets --lcov --output-path target/llvm-cov.info

echo "==> python3 scripts/check_lcov.py target/llvm-cov.info"
python3 scripts/check_lcov.py target/llvm-cov.info

echo "==> cargo deny check advisories bans licenses sources"
cargo deny check advisories bans licenses sources

echo "==> cargo audit -D warnings"
cargo audit -D warnings

echo "==> node scripts/check-npm-advisories.cjs"
node scripts/check-npm-advisories.cjs

echo "==> scripts/check_vendor_patches.sh"
scripts/check_vendor_patches.sh

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
