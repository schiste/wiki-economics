#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
SITE_FIXTURE_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/wiki-econ-site-ci.XXXXXX")"
trap 'rm -rf -- "$SITE_FIXTURE_ROOT"' EXIT

echo "==> node scripts/verify-runtime.cjs"
node scripts/verify-runtime.cjs

echo "==> node scripts/generate-stack-reference.cjs --check"
node scripts/generate-stack-reference.cjs --check

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
node --check scripts/check-npm-licenses.cjs
node --check scripts/check-vendor-patches.cjs
node --check scripts/deny-network.cjs
node --check scripts/prepare-site-source.cjs
node --check scripts/release-provenance.cjs
node --check scripts/generate-sboms.cjs
node --check scripts/generate-stack-reference.cjs
node --check scripts/release-bundle.cjs
node --check scripts/verify-runtime.cjs
node --check scripts/verify-site-dependencies.cjs
node --check scripts/verify-site-reproducibility.cjs
node --check scripts/publish-browser-data.cjs
node --check scripts/browser-performance.cjs

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

echo "==> node --test deploy/toolforge/prune-releases.test.cjs"
node --test deploy/toolforge/prune-releases.test.cjs

echo "==> node --test deploy/toolforge/install-binary.test.cjs"
node --test deploy/toolforge/install-binary.test.cjs

echo "==> node --test deploy/toolforge/run-capacity-benchmark.test.cjs"
node --test deploy/toolforge/run-capacity-benchmark.test.cjs
node --test deploy/toolforge/run-qualify-wiki.test.cjs
node --test deploy/toolforge/rebuild-image.test.cjs
node --test deploy/toolforge/load-scheduled-jobs.test.cjs
node --test deploy/toolforge/imported-backup.test.cjs
node --test deploy/toolforge/recovery-operations.test.cjs

echo "==> node --test scripts/check-freshness.test.cjs"
node --test scripts/check-freshness.test.cjs

echo "==> node --test scripts/check-npm-advisories.test.cjs"
node --test scripts/check-npm-advisories.test.cjs

echo "==> node --test scripts/check-npm-licenses.test.cjs"
node --test scripts/check-npm-licenses.test.cjs

echo "==> node --test scripts/build-site-fixture.test.cjs"
node --test scripts/build-site-fixture.test.cjs

echo "==> dependency closure and reproducibility unit tests"
node --test scripts/prepare-site-source.test.cjs
node --test scripts/release-provenance.test.cjs
node --test scripts/generate-sboms.test.cjs
node --test scripts/generate-stack-reference.test.cjs
node --test scripts/release-bundle.test.cjs
node --test scripts/qualify-capacity.test.cjs
node --test scripts/verify-site-dependencies.test.cjs
node --test scripts/verify-site-reproducibility.test.cjs
node --test scripts/publish-browser-data.test.cjs
node --test scripts/browser-performance.test.cjs
node --test site/browser-cache.test.mjs

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

echo "==> Generate fixture and build the real Observable production site twice offline"
cargo run --locked -- --output-dir "$SITE_FIXTURE_ROOT/data" site-fixture
node scripts/verify-site-reproducibility.cjs \
  --data-dir "$SITE_FIXTURE_ROOT/data" \
  --work-dir "$SITE_FIXTURE_ROOT/reproducibility"

echo "==> Build deterministic nlwiki/ptwiki/frwiki fixture and enforce browser budgets"
cargo run --locked -- --output-dir "$SITE_FIXTURE_ROOT/performance-data" browser-performance-fixture
node scripts/build-site-fixture.cjs \
  --data-dir "$SITE_FIXTURE_ROOT/performance-data" \
  --dist-dir "$SITE_FIXTURE_ROOT/performance-dist"
node scripts/browser-performance.cjs \
  --dist-dir "$SITE_FIXTURE_ROOT/performance-dist" \
  --report "$SITE_FIXTURE_ROOT/browser-performance.json"

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

echo "==> node scripts/check-npm-licenses.cjs"
node scripts/check-npm-licenses.cjs

echo "==> scripts/check_vendor_patches.sh"
scripts/check_vendor_patches.sh

echo "==> python3 -m py_compile scripts/check_lcov.py scripts/test_check_lcov.py"
python3 -m py_compile \
  scripts/check_lcov.py \
  scripts/test_check_lcov.py

echo "==> python3 -m unittest discover -s scripts -p 'test_*.py'"
python3 -m unittest discover -s scripts -p 'test_*.py'

if [ -f "$ROOT/output/manifest.json" ] && [ -f "$ROOT/output/browser-data-index.json" ]; then
  echo "==> ./scripts/build-site.sh"
  ./scripts/build-site.sh
else
  echo "==> skipping optional live-output site smoke check (complete output set not present)"
fi

echo "==> all local checks passed"
