#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
SITE_FIXTURE_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/wiki-econ-site-ci.XXXXXX")"

# cargo-llvm-cov builds a full instrumented copy of the workspace (3-11 GB).
# Share one copy across every checkout of this repo instead of one per
# worktree. Runs serialize on a lock because llvm-cov deletes *.profraw in its
# target dir at start, so concurrent runs would corrupt each other's coverage.
export CARGO_LLVM_COV_TARGET_DIR="${CARGO_LLVM_COV_TARGET_DIR:-${XDG_CACHE_HOME:-$HOME/.cache}/wiki-economics/llvm-cov-target}"
LLVM_COV_LOCK="$CARGO_LLVM_COV_TARGET_DIR.lock"
LLVM_COV_LOCK_HELD=0

acquire_llvm_cov_lock() {
  mkdir -p "$(dirname "$LLVM_COV_LOCK")"
  while ! mkdir "$LLVM_COV_LOCK" 2>/dev/null; do
    holder="$(cat "$LLVM_COV_LOCK/pid" 2>/dev/null || true)"
    if [ -n "$holder" ] && ! kill -0 "$holder" 2>/dev/null; then
      echo "==> removing stale llvm-cov lock left by exited pid $holder"
      rm -rf -- "$LLVM_COV_LOCK"
      continue
    fi
    echo "==> waiting for llvm-cov lock $LLVM_COV_LOCK (pid ${holder:-starting})"
    sleep 10
  done
  echo "$$" > "$LLVM_COV_LOCK/pid"
  LLVM_COV_LOCK_HELD=1
}

release_llvm_cov_lock() {
  if [ "$LLVM_COV_LOCK_HELD" = 1 ]; then
    rm -rf -- "$LLVM_COV_LOCK"
    LLVM_COV_LOCK_HELD=0
  fi
}

trap 'release_llvm_cov_lock; rm -rf -- "$SITE_FIXTURE_ROOT"' EXIT

echo "==> node scripts/verify-runtime.cjs"
node scripts/verify-runtime.cjs

echo "==> node scripts/generate-stack-reference.cjs --check"
node scripts/generate-stack-reference.cjs --check

echo "==> node scripts/check-compute-versions.cjs"
node scripts/check-compute-versions.cjs

echo "==> bash -n scripts/*.sh scripts/lib/*.sh site/data-build/*.sh deploy/cloud-vps/*.sh deploy/toolforge/*.sh"
bash -n scripts/*.sh scripts/lib/*.sh site/data-build/*.sh deploy/cloud-vps/*.sh deploy/toolforge/*.sh

# CI runs shellcheck on every script, so the local mirror must too. Skip with a
# visible notice when the tool is absent, matching setup.sh --skip-quality-tools,
# rather than letting a contributor discover the gap from a red CI run.
echo "==> shellcheck"
if command -v shellcheck >/dev/null 2>&1; then
  shellcheck scripts/*.sh scripts/lib/*.sh site/data-build/*.sh \
    deploy/cloud-vps/*.sh deploy/toolforge/*.sh
else
  echo "    shellcheck not installed; skipping (CI still enforces it)" >&2
fi

echo "==> node --check site/admin-auth.cjs"
node --check site/admin-auth.cjs

echo "==> node --check site/admin-server.cjs"
node --check site/admin-server.cjs
node --check site/machine-api.cjs
node --check site/src/components/admin-console.js
node --check deploy/toolforge/qualification-receipt.cjs

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
node --check scripts/publish-static-root.cjs
node --check scripts/browser-performance.cjs
node --check scripts/admin-browser-workflow.cjs
node --check scripts/build-admin-site.cjs
node --check scripts/site-source-bundle.cjs

echo "==> node --check site/observablehq.config.js"
node --check site/observablehq.config.js

echo "==> node --check site/data-build/*.cjs"
for f in site/data-build/*.cjs; do node --check "$f"; done

echo "==> node --test (every suite, globbed)"
# Glob so a new test file can never be added without being executed. The
# previous explicit list omitted three suites entirely, including the
# pipeline coordinator, which is why its stale expectations went unnoticed.
shopt -s nullglob
node --test \
  'deploy/toolforge/*.test.cjs' \
  'scripts/*.test.cjs' \
  'site/*.test.cjs' \
  'site/*.test.mjs' \
  'site/data-build/*.test.cjs'

echo "==> ./scripts/build-site.sh --help"
./scripts/build-site.sh --help

echo "==> ./scripts/refresh.sh --help"
./scripts/refresh.sh --help

echo "==> cargo fmt --all -- --check"
cargo fmt --all -- --check

echo "==> cargo clippy --locked --all-targets --all-features -- -D warnings"
cargo clippy --locked --all-targets --all-features -- -D warnings

echo "==> cargo run --locked -- metric-catalog --check"
cargo run --locked -- metric-catalog --check

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
node scripts/admin-browser-workflow.cjs \
  --dist-dir "$SITE_FIXTURE_ROOT/performance-dist"

echo "==> cargo doc --locked --no-deps"
cargo doc --locked --no-deps

echo "==> cargo llvm-cov --locked --workspace --all-features --all-targets --lcov --output-path target/llvm-cov.info"
echo "    (shared target dir: $CARGO_LLVM_COV_TARGET_DIR)"
mkdir -p target
acquire_llvm_cov_lock
cargo llvm-cov --locked --workspace --all-features --all-targets --lcov --output-path target/llvm-cov.info
release_llvm_cov_lock

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

echo "==> node scripts/check-fleet-qualification.cjs"
node scripts/check-fleet-qualification.cjs

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
