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
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --all-features
cargo doc --no-deps
cargo llvm-cov --workspace --all-features --all-targets --lcov --output-path /tmp/wiki-economics-target/llvm-cov.info
python3 scripts/check_lcov.py /tmp/wiki-economics-target/llvm-cov.info
cargo deny check advisories bans licenses sources
cargo audit -D warnings
python3 -m py_compile scripts/fetch_patrol.py scripts/compute_patrol.py scripts/check_lcov.py scripts/test_fetch_patrol.py
python3 -m unittest scripts/test_fetch_patrol.py
```

Cargo build artifacts are intentionally routed out of the repo via
`.cargo/config.toml` to `/tmp/wiki-economics-target`. Override with
`CARGO_TARGET_DIR` if you want a different local cache location.

Expected bar:

- clippy warnings are errors
- coverage is expected to stay at 100% line coverage in the exported LCOV artifact
- dependency and advisory checks are part of the normal workflow

## CI Structure

GitHub Actions is split into three jobs:

- `quality`: formatting, clippy, Rust tests, Python patrol script checks, docs
- `coverage`: `cargo llvm-cov` LCOV export plus `scripts/check_lcov.py` enforcing zero uncovered lines
- `security`: `cargo-deny` and `cargo-audit`

That split is intentional. Keep fast correctness failures separate from coverage drift and dependency-policy drift.

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

- analytical parquet lives under `data/parquet/<wiki>/`
- warehouse parquet lives under `data/warehouse/<wiki>/`
- ingest markers live under `data/parquet/<wiki>/_markers/`
- a marker is valid only when it still resolves to the analytical and warehouse outputs it claims
- partition names are `year=YYYY/year_month=YYYY-MM`
- compute prefers the partitioned incremental path when that layout exists
- compatibility fallback for older parquet layouts still exists for both full-wiki and partitioned loads and should not be broken casually
- per-wiki metric outputs should include a `wiki` column before merge
- merge is responsible for refreshing shared dashboard artifacts in `output/` (`manifest.json`, `defaults_*.json`)
- patrol compute also refreshes its merged/default artifacts because it still runs through the Python sidecar pipeline

If any of these change, update `docs/architecture.md`, tests, and storage helpers together.

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
