# Dependencies & Licenses

This page documents the primary software dependencies used by `wiki-economics`
and the license posture they bring into the repository.

It is an engineering inventory, not a legal opinion. The authoritative inputs
remain the checked-in manifests and lockfiles:

- `Cargo.toml`
- `Cargo.lock`
- `site/package.json`
- `site/package-lock.json`
- `vendor/polars-utils/`

## Scope

- The repository's own code is dual-licensed under `MIT OR Apache-2.0`.
- This document covers the main software dependencies that power the Rust
  pipeline, Python sidecar workflow, dashboard, and local build/query stack.
- Wikimedia dump content is separate from software licensing. The reuse terms
  for Wikimedia datasets and derived content should be evaluated independently
  from the licenses of the tools listed here. The canonical entry points are
  the [Wikimedia dumps legal page](https://dumps.wikimedia.org/legal.html) and
  the [Wikimedia Foundation Terms of Use](https://foundation.wikimedia.org/wiki/Policy:Terms_of_Use).
- External toolchains such as Rust, Python, Node.js, and npm are required to
  work on the repository locally, but they are installed outside this repo and
  are not vendored here.

## License Policy

Rust dependency licensing is enforced in CI with `cargo deny`.

The current allow-list in [`deny.toml`](../deny.toml) is:

- `Apache-2.0`
- `Apache-2.0 WITH LLVM-exception`
- `BSD-2-Clause`
- `BSD-3-Clause`
- `BSL-1.0`
- `ISC`
- `MIT`
- `Unicode-3.0`
- `Zlib`

That gives the project a deliberately permissive software-license posture.

## Primary Stack

| Component | Role | License |
|-----------|------|---------|
| `wiki-econ` | Rust CLI workspace in this repository | `MIT OR Apache-2.0` |
| Polars | Dataframe engine for ingest and compute | `MIT` |
| vendored `polars-utils` patch | In-tree patch carried under `vendor/polars-utils` | `MIT` |
| Observable Framework | Dashboard build/runtime framework | `ISC` |
| `duckdb` npm package | Browser-side and build-time query support | `MIT` |

## Direct Rust Dependencies

The table below reflects the direct workspace dependencies currently resolved by
Cargo for the main crate.

| Crate | Resolved version | Role | License |
|-------|------------------|------|---------|
| `polars` | `0.53.0` | dataframe operations, CSV/Parquet I/O, joins, aggregations | `MIT` |
| `reqwest` | `0.12.28` | HTTP downloads for Wikimedia dumps and API calls | `MIT OR Apache-2.0` |
| `rayon` | `1.11.0` | parallel iteration and multi-wiki processing | `MIT OR Apache-2.0` |
| `bzip2` | `0.5.2` | streaming decompression of `.tsv.bz2` dumps | `MIT OR Apache-2.0` |
| `quick-xml` | `0.38.4` | XML parsing for logging dumps | `MIT` |
| `flate2` | `1.1.9` | gzip/deflate support in auxiliary paths | `MIT OR Apache-2.0` |
| `clap` | `4.5.60` | CLI argument parsing | `MIT OR Apache-2.0` |
| `indicatif` | `0.18.4` | progress bars and operator feedback | `MIT` |
| `anyhow` | `1.0.102` | application error handling | `MIT OR Apache-2.0` |
| `tracing` | `0.1.44` | structured logging instrumentation | `MIT` |
| `tracing-subscriber` | `0.3.22` | log formatting and filtering | `MIT` |
| `chrono` | `0.4.41` | time/date handling | `MIT OR Apache-2.0` |
| `serde_json` | `1.0.149` | JSON generation and parsing | `MIT OR Apache-2.0` |
| `regex` | `1.12.3` | string parsing and validation | `MIT OR Apache-2.0` |

## Vendored Patch

The workspace patches `polars-utils` through:

```toml
[patch.crates-io]
polars-utils = { path = "vendor/polars-utils" }
```

That vendored crate remains under `MIT`, matching upstream Polars at the time
the fork was taken. The repository currently carries that patch to keep tighter
control over the dependency graph while still using the Polars ecosystem.

## Frontend And Query Dependencies

The dashboard has a very small direct JavaScript dependency surface.

| Package | Version spec | Role | License |
|---------|--------------|------|---------|
| `@observablehq/framework` | `^1.13.4` | site generation and client framework | `ISC` |
| `duckdb` | `^1.4.4` | build-time and browser query engine | `MIT` |

DuckDB is also required locally as a CLI for the checked-in
`site/data-build/*.json.sh` generators. The repository does not vendor the
DuckDB CLI binary itself; users install it separately through their platform
package manager.

## Python Sidecar

The patrol pipeline is implemented in repository-local Python scripts:

- `scripts/fetch_patrol.py`
- `scripts/compute_patrol.py`
- `scripts/test_fetch_patrol.py`

Those scripts currently rely on the Python standard library only. They do not
introduce a separate pinned third-party Python package set in this repository.

## Local Prerequisites

The following are required for local development, but are external tools rather
than tracked in-repo libraries:

- Rust stable plus `rustfmt` and `clippy`
- Python 3
- Node.js and npm
- DuckDB CLI

Treat their licenses as upstream toolchain concerns rather than part of the
repository's own vendored dependency inventory.

## Maintenance Notes

When direct dependencies change materially, update this document together with
the manifests and lockfiles.

Useful refresh commands:

```sh
cargo metadata --format-version 1
cargo deny check licenses
npm ls --prefix site --depth=0
```

For frontend package licenses specifically, inspect the installed manifests
under `site/node_modules/<package>/package.json` when needed.
