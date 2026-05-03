# wiki-economics

`wiki-economics` is a Wikipedia research pipeline and dashboard for analyzing editor labor, content production, inequality, funnel health, and patrol behavior across Wikipedia language editions.

The repository currently has three main parts:

- A Rust CLI for fetch, ingest, compute, merge, and benchmarking workflows.
- A Python sidecar pipeline for patrol logging data.
- An Observable Framework site for publishing the resulting datasets and charts.

The repo now supports two runtime profiles from the same codebase:

- `local`: interactive development, local data onboarding, and the dev/operator admin UI
- `production`: static public dashboard serving, scheduled refresh orchestration for a VPS, and an optional authenticated admin surface

## Repository Status

This repository is curated for a public open-source release. Source code, documentation, vendored patches, lockfiles, and quality gates are tracked here; generated data, local caches, build outputs, and installed dependencies are intentionally excluded from version control.

## One-Command Setup

For a friendlier local bootstrap, run:

```sh
./scripts/setup.sh
```

or:

```sh
npm run setup
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

The current public release is intentionally Wikipedia-first. The admin picker covers every Wikipedia language edition published in the Wikimedia history dumps; the CLI still rejects monthly-partitioned giants such as `enwiki` until the dedicated fetch planner for those projects lands.

Build the Rust CLI:

```sh
cargo build --release
```

Run a small end-to-end refresh:

```sh
./scripts/refresh.sh frwiki
```

Expanded equivalent:

```sh
cargo run --release -- fetch frwiki
cargo run --release -- ingest frwiki
cargo run --release -- compute frwiki
cargo run --release -- merge
```

Pass `--version YYYY-MM` to `fetch` or `run` when you need a specific dump snapshot. If omitted, the CLI defaults to the previous UTC month.

Build the production site against the current local artifacts:

```sh
./scripts/build-site.sh
```

The Observable site reads generated dashboard artifacts from `site/src/data -> ../../output`. Build `output/` locally before expecting the dashboard pages to render real data.

Start the local dashboard and admin server together:

```sh
scripts/dev.sh
```

In local development, the admin API is a loopback-only operator tool. In VPS deployments, the supported hosted admin model is an authenticated OpenID Connect login flow with an env-driven email allowlist. No in-repo user database is used.

For hosted deployments, keep the allowlist and OIDC credentials in deployment
secrets and render them into `/etc/wiki-economics.env`; the recommended secret
names match the runtime env vars exactly (`WIKI_ECON_ADMIN_ALLOWED_EMAILS`,
`WIKI_ECON_ADMIN_SESSION_SECRET`, and so on).

## Local Verification

Preferred full local verification command:

```sh
./scripts/ci-local.sh
```

Equivalent expanded commands:

```sh
bash -n scripts/*.sh scripts/lib/*.sh site/data-build/*.sh deploy/cloud-vps/*.sh
node --check site/admin-auth.cjs
node --check site/admin-server.cjs
node --check site/observablehq.config.js
node --test site/admin-auth.test.cjs
node --test site/admin-server.test.cjs
./scripts/build-site.sh --help
./scripts/refresh.sh --help
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --all-features
cargo doc --no-deps
cargo llvm-cov --workspace --all-features --all-targets --lcov --output-path /tmp/wiki-economics-target/llvm-cov.info
python3 scripts/check_lcov.py /tmp/wiki-economics-target/llvm-cov.info
cargo deny check advisories bans licenses sources
cargo audit -D warnings
scripts/check_vendor_polars.sh
python3 -m py_compile scripts/fetch_patrol.py scripts/compute_patrol.py scripts/check_lcov.py scripts/test_fetch_patrol.py scripts/test_check_lcov.py
python3 -m unittest discover -s scripts -p 'test_*.py'
```

## Project Guides

- [Architecture](docs/architecture.md)
- [Admin Server](docs/admin-server.md)
- [Cloud VPS Deployment](docs/cloud-vps-deploy.md)
- [Development](docs/development.md)
- [Benchmarking](docs/benchmarking.md)
- [Dependencies and Licenses](docs/dependencies-and-licenses.md)
- [Publishing](docs/release.md)
- [Security Model](docs/security.md)
- [Stack and Data Sources](docs/stack-and-data-sources.md)
- [Troubleshooting](docs/troubleshooting.md)

## Platform Notes

- macOS and Linux are first-class platforms. CI runs on `ubuntu-latest`
  and the developer-side flow is exercised on macOS.
- Windows is supported on a best-effort basis. `site/src/data` is a
  symlink to `../../output`, which requires Developer Mode or
  `git config core.symlinks true` to clone correctly. See the
  troubleshooting guide for details.

## Data and Artifacts

- `data/` is fetched or generated locally and is intentionally not committed.
- `output/` is generated locally and feeds the dashboard via `site/src/data -> ../../output`.
- `site/data-build/` contains the checked-in generator scripts that materialize `manifest.json` and `defaults_*.json` into the active output directory.
- `site/dist/` and `site/node_modules/` are build artifacts and local dependencies.

If you need small permanent fixtures for tests, add them deliberately rather than checking in ad hoc working data.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or [MIT license](LICENSE-MIT), at your option.
