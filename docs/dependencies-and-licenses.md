# Dependencies & Licenses

This page documents the primary software dependencies used by `wiki-economics`
and the license posture they bring into the repository.

It is an engineering inventory, not a legal opinion. The authoritative inputs
remain the checked-in manifests and lockfiles:

- `Cargo.toml`
- `Cargo.lock`
- `package.json`
- `package-lock.json`
- `site/package.json`
- `vendor/polars-utils/`
- `THIRD_PARTY_NOTICES.md`
- `REUSE.toml`

## Scope

- Project-owned code, documentation, site content, and generated aggregate
  datasets are licensed under `MIT`.
- This document covers the main software dependencies that power the Rust
  pipeline, dashboard, and local build/query stack.
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
Node and browser licensing is independently enforced by
`scripts/check-npm-licenses.cjs` against the fail-closed SPDX-expression policy
in `config/npm-license-policy.json`. The policy covers every resolved lockfile
package and the exact generated Observable browser graph.
The same policy explicitly approves the MIT and Artistic-2.0 runtime licenses
recorded for Node.js and npm in the Toolforge image-closure SBOM.
Project file copyright and licensing declarations are enforced with the REUSE
3.3 specification and `reuse lint`. The canonical public publication policy is
`config/publication-licensing.json`; every downloadable artifact record carries
an SPDX `license_spdx` field, while each manifest also records provenance,
attribution, source datasets, and the Wikimedia trademark status.

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
| `wiki-econ` | Rust CLI workspace in this repository | `MIT` |
| Polars | Dataframe engine for ingest and compute | `MIT` |
| vendored `polars-utils` patch | In-tree patch carried under `vendor/polars-utils` | `MIT` |
| Observable Framework | Dashboard build/runtime framework | `ISC` |
| Arrow JavaScript + Parquet-WASM | Browser-side Parquet decoding | `Apache-2.0`; `MIT OR Apache-2.0` |

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

Observable Framework is an exact root build-tool dependency. The dashboard's
production browser dependencies are all direct, exact pins.

| Package | Exact version | Role | License |
|---------|--------------|------|---------|
| `@observablehq/framework` | `1.13.4` | root site-generation tool | `ISC` |
| `@observablehq/inputs` | `0.12.0` | interactive controls | `ISC` |
| `@observablehq/plot` | `0.6.17` | charts | `ISC` |
| `apache-arrow` | `21.2.0` | columnar browser data | `Apache-2.0` |
| `d3` | `7.9.0` | transforms and scales | `ISC` |
| `htl` | `1.0.0` | safe HTML templates | `ISC` |
| `parquet-wasm` | `0.7.2` | Parquet decoding | `MIT OR Apache-2.0` |

The repository uses one npm workspace and one root lockfile. Observable's exact
transformed ESM closure is checked in under `site/vendor/observable-cache`, and
`config/site-dependency-closure.json` records its versions and content hash.
Builds use this cache with network access disabled. Dashboard defaults are
generated by Rust; no native or WASM DuckDB package is distributed.

The root override fixes Observable's build edge at `esbuild 0.28.2`; the full
offline site build and byte-reproducibility check exercise that combination.
There are currently no npm advisory exceptions.

Every main-branch release retains CycloneDX 1.6 SBOMs for the Rust binary,
Toolforge image closure, and browser bundle. `third-party-notices.json` contains
the complete resolved Rust/npm/browser license inventory for that release,
while `THIRD_PARTY_NOTICES.md` remains the concise human entry point.

## Python helper

Python is used only by the standard-library LCOV validation helper and its
tests. The obsolete PyArrow patrol implementation has been removed, so there
is no Python third-party runtime dependency to pin.

## Local Prerequisites

The following are required for local development, but are external tools rather
than tracked in-repo libraries:

- Rust `1.98.0` plus `rustfmt` and `clippy`
- Python 3
- Node.js `24.15.0` and npm `11.12.1`

Treat their licenses as upstream toolchain concerns rather than part of the
repository's own vendored dependency inventory.

## Maintenance Notes

When direct dependencies change materially, update this document together with
the manifests and lockfiles.

Useful refresh commands:

```sh
cargo metadata --format-version 1
cargo deny check licenses
npm ls --workspace site --depth=0
```

For frontend package licenses specifically, inspect the installed manifests
under `node_modules/<package>/package.json` when needed.
