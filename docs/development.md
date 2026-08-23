# Development Guide

This document records the policies and maintenance rules that still matter after the recent refactors.

## Local Quality Gates

For first-time local bootstrap, prefer:

```sh
./scripts/setup.sh
```

That setup script installs repo dependencies, ensures the Rust toolchain is ready, prepares local directories, and builds the CLI plus dashboard before you start iterating.

Preferred full local verification command:

```sh
./scripts/ci-local.sh
```

Expanded commands:

```sh
bash -n scripts/*.sh scripts/lib/*.sh site/data-build/*.sh deploy/cloud-vps/*.sh deploy/toolforge/*.sh
node --check site/admin-auth.cjs
node --check site/admin-server.cjs
node --check site/observablehq.config.js
for f in site/data-build/*.cjs; do node --check "$f"; done
node --test site/admin-auth.test.cjs
node --test site/admin-server.test.cjs
node scripts/generate-stack-reference.cjs --check
cargo fmt --all -- --check
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test --locked --all-targets --all-features
cargo doc --locked --no-deps
cargo llvm-cov --locked --workspace --all-features --all-targets --lcov --output-path target/llvm-cov.info
python3 scripts/check_lcov.py target/llvm-cov.info
cargo deny check advisories bans licenses sources
cargo audit -D warnings
node scripts/check-npm-advisories.cjs
node scripts/check-npm-licenses.cjs
scripts/check_vendor_patches.sh
python3 -m py_compile scripts/check_lcov.py scripts/test_check_lcov.py
python3 -m unittest discover -s scripts -p 'test_*.py'
```

Cargo build artifacts use the standard, gitignored `target/` directory. This
keeps local builds and CI cache paths aligned. Override `CARGO_TARGET_DIR` if
you need a different local cache location.

Expected bar:

- clippy warnings are errors
- coverage is expected to stay at 100% line coverage in the exported LCOV artifact
- dependency and advisory checks are part of the normal workflow

The local verification script also checks:

- shell syntax for the shared operational scripts
- Node syntax for the dev/operator admin server and Observable config
- helper entrypoint usage for `scripts/build-site.sh` and `scripts/refresh.sh`
- a production site build when merged dashboard artifacts are already present locally

## Runtime Profiles

The repo is intentionally one codebase with two orchestration modes:

- `local`: `scripts/setup.sh`, `scripts/dev.sh`, and the admin UI/API for interactive onboarding
- `production`: `deploy/cloud-vps/` or `deploy/toolforge/` wrappers, static site serving, scheduled refreshes, and an optional authenticated admin surface

The shared contract across both modes is:

- the Rust CLI
- the Rust patrol fetch and compute path
- the merged artifact format under `output/`
- the Observable production site build

The main rule is that deployment differences should live in scripts and env,
not in forked pipeline logic.

## Shared Operational Entry Points

The repo-level operational scripts are:

- `scripts/setup.sh` for local bootstrap
- `scripts/dev.sh` for the local dashboard plus dev/operator admin API
- `scripts/refresh.sh` for shared batch refresh orchestration
- `scripts/build-site.sh` for production-style Observable builds

Shared runtime path handling lives in `scripts/lib/wiki_econ.sh`.

Production wrappers under `deploy/cloud-vps/` and `deploy/toolforge/` should
call those shared scripts rather than reimplementing pipeline steps.

## CI Structure

GitHub Actions is split into five jobs:

- `quality`: formatting, clippy, Rust and Node checks, the small Python LCOV
  helper tests, and generated-document consistency
- `site`: a clean npm workspace install, advisory check, Rust-generated
  deterministic fixture, and real Observable production build with page and
  attachment verification
- `coverage`: `cargo llvm-cov` LCOV export plus `scripts/check_lcov.py` enforcing zero uncovered lines
- `security`: `cargo-deny`, `cargo-audit`, fail-closed npm advisory and license
  policies, REUSE, and registered vendored-patch validation
- `toolforge-release`: after the other jobs pass on `main`, selectively builds
  and retains the attested Linux release envelope, three SBOMs, checksums,
  provenance, and complete notices for an operator-driven SSH deploy

That split is intentional. Keep fast correctness failures separate from
coverage drift and dependency-policy drift. GitHub has no production
credentials; Toolforge deployment remains an explicit operator action. The
coverage run subsumes the ordinary Rust test suite.

Dependabot checks Cargo, npm, and pinned GitHub Actions weekly using
`.github/dependabot.yml`; its pull requests must still pass all five jobs.

The LCOV check is deliberate. `cargo llvm-cov --summary-only` can under-report line coverage on fully exercised lines because of sub-line region artifacts around `?` and similar expressions. CI treats the exported LCOV file as the source of truth for line coverage.

## Benchmarking Policy

Performance claims should be backed by the built-in benchmark command, not just by code inspection.

Benchmark after changes that affect:

- fetch behavior in ways that may shift downstream bottlenecks
- ingest shape or filtering
- analytical storage layout
- compute partitioning
- Polars version or Polars-facing query code

Use `docs/benchmarking.md` as the operator-facing reference. Treat `compute_all` timing as the primary number when comparing real pipeline performance across commits.

## Logging Policy

Runtime logging is based on `tracing` and `tracing-subscriber`.

Current conventions:

- `info` for stage boundaries and completed work
- `debug` for skip paths, compatibility branches, and extra detail
- `warn` for degraded but recoverable behavior

Prefer structured fields such as:

- `wiki`
- `metric`
- `rows`
- `columns`
- `bytes`
- `elapsed_ms`

Do not add new long-running operational logging with unstructured `println!`.

## Storage And Compute Contracts

The following are live architecture contracts, not incidental implementation details:

- analytical parquet lives under `data/parquet/<wiki>/_snapshots/<snapshot>/`
- warehouse parquet lives under `data/warehouse/<wiki>/_snapshots/<snapshot>/`
- ingest markers live inside the corresponding analytical snapshot generation
- `data/snapshots/<wiki>/current-snapshot.json` selects the only generation compute may read
- a marker is valid only when it still resolves to the analytical and warehouse outputs it claims
- partition names are `year=YYYY/year_month=YYYY-MM`
- compute prefers the partitioned incremental path when that layout exists
- compatibility fallback for older parquet layouts still exists for both full-wiki and partitioned loads and should not be broken casually
- per-wiki metric outputs should include a `wiki` column before merge
- merge uses Rust to refresh `defaults_*.json` and `meta_*.json`, then runs the
  checked-in `site/data-build/manifest.json.sh` entrypoint to validate and
  atomically publish `manifest.json`
- patrol fetch and compute are Rust subcommands; patrol compute participates in
  the same merge/default materialization path as the history metrics
- deterministic stage receipts live under `data/stages/` and `output/_stages/`; algorithm changes must increment the owning `*_ALGORITHM_VERSION` constant
- unpinned fetch/run commands resolve one completed snapshot for the entire run and fail when no dump exists within `WIKI_ECON_MAX_SNAPSHOT_LAG_MONTHS`

If any of these change, update `docs/architecture.md`, tests, and storage helpers together.

Dependency versions and lifecycle state are deliberately absent from this
narrative guide. Update their manifests, run
`node scripts/generate-stack-reference.cjs --write`, and let CI validate the
[generated stack reference](generated/stack-reference.md).

## Vendored `polars-utils` Patch

The workspace currently patches `polars-utils` through:

```toml
[patch.crates-io]
polars-utils = { path = "vendor/polars-utils" }
```

This is an in-tree fork, not a warning suppression mechanism.

### Why it exists

The current Polars dependency graph still needed a security-conscious intervention around the advisory-bearing `bincode 2` edge. The project policy was to fix the graph rather than ignore the advisory in tooling.

### What changed

`vendor/polars-utils/src/pl_serialize.rs` was patched so the compact serialization path also uses `rmp-serde` instead of the upstream `bincode` path.

That means:

- we own a small Polars-internal fork
- future Polars upgrades must review that patch explicitly
- upstream removal of the need for this patch is preferable to carrying it forever

### Maintenance Rules

If you touch the vendored patch:

1. compare the vendored files against upstream `polars-utils` for the target version
2. rerun the full workspace quality, coverage, and security commands
3. run the vendored crate tests directly:
   `cargo test --manifest-path vendor/polars-utils/Cargo.toml`
4. document why the fork is still needed

Do not let the vendored patch turn into a general-purpose divergence from upstream Polars behavior.

## Dependency And Security Policy

`cargo-deny` and `cargo-audit` are expected to stay meaningful.

Current policy choices that matter:

- wildcard dependencies are denied
- unknown registries and git sources are denied
- only a narrow set of permissive licenses is allowed
- the crate is private (`publish = false`)

There may still be upstream duplicate-version warnings in the dependency graph. They are not currently suppressed. Treat them as dependency-graph debt, not as a reason to weaken the checks.

## How To Extend The System Safely

For non-trivial changes:

1. decide which stage contract changes: fetch, ingest, storage, compute, merge, or CI
2. update the relevant docs under `docs/`
3. preserve compatibility paths deliberately, or remove them deliberately with test changes
4. benchmark if the change can plausibly alter runtime or memory behavior
5. rerun the full local quality gates

This project is intentionally strict about explicit contracts. Keep that discipline.
