# wiki-economics

`wiki-economics` is a Wikipedia research pipeline and dashboard for analyzing editor labor, content production, inequality, funnel health, and patrol behavior across Wikipedia language editions.

The repository currently has three main parts:

- A Rust CLI for fetch, ingest, compute, merge, and benchmarking workflows.
- Rust patrol fetch and compute stages for MediaWiki logging data.
- An Observable Framework site for publishing the resulting datasets and charts.

The repo now supports two runtime profiles from the same codebase:

- `local`: interactive development, local data onboarding, and the dev/operator admin UI
- `production`: static public dashboard serving, Toolforge (or Cloud VPS)
  refresh orchestration, and an optional authenticated admin surface

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

- the pinned Rust toolchain with `rustfmt` and `clippy`
- the pinned Node.js and npm toolchain
- Python 3 for the standard-library LCOV checker only

Exact toolchain and dependency versions are generated from the checked-in
manifests in the [stack reference](docs/generated/stack-reference.md).

This repository does not bundle Wikimedia datasets or precomputed dashboard outputs. A clean clone starts with no `data/` or `output/` tree; fetch and compute those locally.

The current public release is intentionally Wikipedia-first. The admin picker covers every Wikipedia language edition published in the Wikimedia history dumps; the CLI still rejects monthly-partitioned giants such as `enwiki` until the dedicated fetch planner for those projects lands.

Build the Rust CLI:

```sh
cargo build --release --locked
```

Run one wiki through the Rust data pipeline locally (this downloads the real
history and logging dumps and is not a small fixture):

```sh
cargo run --release --locked -- run frwiki --version YYYY-MM
```

Expanded stage-oriented equivalent:

```sh
cargo run --release --locked -- fetch frwiki
cargo run --release --locked -- ingest frwiki --version YYYY-MM
cargo run --release --locked -- compute frwiki
```

The shared `scripts/refresh.sh` wrapper additionally enforces the complete
publication contract and is intended for a full scheduled/published artifact
set, not an isolated contributor experiment.

Pass `--version YYYY-MM` to `fetch`, `ingest`, or `run` when you need a
specific dump snapshot. If omitted, `fetch` and `run` resolve and pin the latest
completed Wikimedia snapshot, with a bounded fallback when the preceding UTC
month is not ready; standalone ingest infers the version from raw filenames.
Versioned ingest outputs are isolated by snapshot and atomically selected only
after the complete source set validates.

Successful stages write deterministic content-addressed receipts, so repeated
runs reuse valid fetch, ingest, compute, merge, and site outputs. See
[Deterministic Stage Fingerprints](docs/stage-fingerprints.md).

Build the production site against the current local artifacts:

```sh
./scripts/build-site.sh
```

The Observable site reads generated dashboard artifacts from `site/src/data -> ../../output`. Build `output/` locally before expecting the dashboard pages to render real data.

Start the local dashboard and admin server together:

```sh
scripts/dev.sh
```

In local development, the admin API is a loopback-only operator tool. In hosted deployments, the supported admin model is an authenticated `meta.wikimedia.org` OAuth 2 login flow with an env-driven username allowlist. No in-repo user database is used.

For hosted deployments, keep the allowlist and MediaWiki OAuth2 credentials in
the platform secret store: Toolforge tool-wide environment variables in the
current production topology, or `/etc/wiki-economics.env` on Cloud VPS. The
recommended secret names match the runtime environment variables exactly
(`WIKI_ECON_ADMIN_ALLOWED_USERNAMES`, `WIKI_ECON_ADMIN_SESSION_SECRET`, and so
on).

## Local Verification

Preferred full local verification command:

```sh
./scripts/ci-local.sh
```

Equivalent expanded commands:

```sh
bash -n scripts/*.sh scripts/lib/*.sh site/data-build/*.sh deploy/cloud-vps/*.sh deploy/toolforge/*.sh
node --check site/admin-auth.cjs
node --check site/admin-server.cjs
node --check site/observablehq.config.js
node scripts/generate-stack-reference.cjs --check
for f in site/data-build/*.cjs; do node --check "$f"; done
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
scripts/check_vendor_patches.sh
python3 -m py_compile scripts/check_lcov.py scripts/test_check_lcov.py
python3 -m unittest discover -s scripts -p 'test_*.py'
```

## Project Guides

- [Architecture](docs/architecture.md)
- [Production Topology](docs/production-topology.md)
- [Generated Stack Reference](docs/generated/stack-reference.md)
- [Admin Server](docs/admin-server.md)
- [Cloud VPS Deployment](docs/cloud-vps-deploy.md)
- [Toolforge Deployment](deploy/toolforge/README.md)
- [Development](docs/development.md)
- [Benchmarking](docs/benchmarking.md)
- [Dependencies and Licenses](docs/dependencies-and-licenses.md)
- [Deterministic Builds](docs/deterministic-builds.md)
- [Frontend Scalability](docs/frontend-scalability.md)
- [Legal, Licensing & Attribution](docs/legal.md)
- [Publishing](docs/release.md)
- [Publication Gate](docs/publication-gate.md)
- [Security Model](docs/security.md)
- [Stack and Data Sources](docs/stack-and-data-sources.md)
- [Troubleshooting](docs/troubleshooting.md)
- [Wiki Lifecycle Management](docs/wiki-lifecycle.md)

## Platform Notes

- macOS and Linux are first-class platforms. CI runs on Ubuntu 24.04.
  and the developer-side flow is exercised on macOS.
- Windows is supported on a best-effort basis. `site/src/data` is a
  symlink to `../../output`, which requires Developer Mode or
  `git config core.symlinks true` to clone correctly. See the
  troubleshooting guide for details.

## Data and Artifacts

- `data/` is fetched or generated locally and is intentionally not committed.
- `output/` is generated locally and feeds the dashboard via `site/src/data -> ../../output`.
- Rust materializes `defaults_*.json` and `meta_*.json`; `site/data-build/`
  contains the checked-in fail-closed `manifest.json` generator.
- `site/dist/` and root `node_modules/` are build artifacts and local dependencies.

If you need small permanent fixtures for tests, add them deliberately rather than checking in ad hoc working data.

## License

Project-owned software, documentation, site content, and generated aggregate
datasets are licensed under the [MIT license](LICENSE). Wikimedia source data,
trademarks, and privacy obligations remain governed by their respective
upstream terms; see [Legal, Licensing & Attribution](docs/legal.md).
