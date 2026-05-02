# wiki-economics

`wiki-economics` is a Wikipedia research pipeline and dashboard for analyzing editor labor, content production, inequality, funnel health, and patrol behavior across Wikipedia language editions.

The repository currently has three main parts:

- A Rust CLI for fetch, ingest, compute, merge, and benchmarking workflows.
- A Python sidecar pipeline for patrol logging data.
- An Observable Framework site for publishing the resulting datasets and charts.

## Repository Status

This repository is curated for a public open-source release. Source code, documentation, vendored patches, lockfiles, and quality gates are tracked here; generated data, local caches, build outputs, and installed dependencies are intentionally excluded from version control.

## One-Command Setup

For a friendlier local bootstrap, run:

```sh
./scripts/setup.sh
```

That script will:

- check or install the main system dependencies when it can
- ensure the Rust toolchain and optional contributor cargo tools are present
- install the dashboard dependencies
- create the local `data/` and `output/` directories
- build the Rust CLI and Observable dashboard

Useful flags:

```sh
./scripts/setup.sh --skip-quality-tools
./scripts/setup.sh --skip-system-packages
./scripts/setup.sh --yes
```

## Quick Start

Prerequisites:

- Rust stable with `rustfmt` and `clippy`
- Python 3
- Node.js and npm
- DuckDB CLI for dashboard artifact generation

This repository does not bundle Wikimedia datasets or precomputed dashboard outputs. A clean clone starts with no `data/` or `output/` tree; fetch and compute those locally.

The current public release is intentionally Wikipedia-first. Local onboarding currently targets the supported yearly-partitioned language editions surfaced in the admin picker; monthly-partitioned giant projects such as `enwiki` still need dedicated fetch planning.

Build the Rust CLI:

```sh
cargo build --release
```

Run a small end-to-end example:

```sh
cargo run --release -- fetch frwiki
cargo run --release -- ingest frwiki
cargo run --release -- compute frwiki
cargo run --release -- merge
```

Pass `--version YYYY-MM` to `fetch` or `run` when you need a specific dump snapshot. If omitted, the CLI defaults to the previous UTC month.

Build the site:

```sh
cd site
npm install
npm run build
```

The Observable site reads generated dashboard artifacts from `site/src/data -> ../../output`. Build `output/` locally before expecting the dashboard pages to render real data.

Start the local dashboard and admin server together:

```sh
scripts/dev.sh
```

## Local Verification

Preferred full local verification command:

```sh
./scripts/ci-local.sh
```

Equivalent expanded commands:

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

## Project Guides

- [Architecture](docs/architecture.md)
- [Development](docs/development.md)
- [Benchmarking](docs/benchmarking.md)
- [Publishing](docs/release.md)
- [Stack and Data Sources](docs/stack-and-data-sources.md)

## Data and Artifacts

- `data/` is fetched or generated locally and is intentionally not committed.
- `output/` is generated locally and feeds the dashboard via `site/src/data -> ../../output`.
- `site/dist/` and `site/node_modules/` are build artifacts and local dependencies.

If you need small permanent fixtures for tests, add them deliberately rather than checking in ad hoc working data.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or [MIT license](LICENSE-MIT), at your option.
