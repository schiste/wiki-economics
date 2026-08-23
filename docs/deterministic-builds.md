# Deterministic dependency closure

The project controls two dependency graphs:

1. npm installs are frozen by the root `package-lock.json`, the exact Observable
   build-tool version in `package.json`, and exact browser versions in
   `site/package.json`;
2. browser modules emitted by Observable are frozen by
   `config/site-dependency-closure.json` and the content-hashed offline cache in
   `site/vendor/observable-cache`.

Node.js, npm, and Rust are pinned in the package manifest, local version-manager
files, CI, `rust-toolchain.toml`, `Cargo.toml`, and Toolforge's Build Service
inputs. `scripts/verify-runtime.cjs` fails when these declarations or the
running tools disagree; the exact values are rendered in the
[generated stack reference](generated/stack-reference.md).

## Site proof

`scripts/prepare-site-source.cjs` creates a clean source tree, seeds only the
reviewed Observable cache, and links the validated publication data. The build
runs with `scripts/deny-network.cjs` loaded through `NODE_OPTIONS`; DNS, HTTP,
HTTPS, TCP, TLS, and `fetch` access fail immediately.

`scripts/verify-site-dependencies.cjs` then validates:

- every emitted browser package and exact version;
- the reviewed cache content hash;
- the only permitted WASM asset (`parquet-wasm`);
- absence of DuckDB assets;
- absence of remote scripts, styles, module imports, workers, or direct
  runtime fetches.

CI calls `scripts/verify-site-reproducibility.cjs`, which performs two builds
from separate clean source and output directories, with networking disabled,
and compares the SHA-256 hash of every artifact. The expected result is exact
byte equality; no normalization exception is currently needed.

## Release provenance and SBOMs

The Toolforge release job uses the commit timestamp as `SOURCE_DATE_EPOCH` and
creates one deterministic envelope containing:

- the Linux Rust binary;
- CycloneDX 1.6 SBOMs for the binary, Toolforge site-image source/npm closure,
  and verified published browser bundle;
- complete machine-readable notices plus the public human-readable notice;
- `release-provenance.json` schema 2 and an exact `SHA256SUMS` manifest.

The image SBOM deliberately describes the source and Node dependency closure
known to GitHub. Toolforge Build Service creates the eventual container, so its
platform-layer digest is operational Toolforge evidence rather than something
the GitHub build fabricates.

GitHub attests the final `.tar.gz` using artifact attestations. The workstation
deployment helper requires `gh attestation verify`, validates the inner release
envelope, and sends one archive. The Toolforge installer independently verifies
the archive hash, exact member allowlist, every payload checksum, commit,
binary identity, three SBOM identities, and notices before atomically changing
`current`. Production manifest generation then fails closed if schema-2 release
provenance is absent or differs from repository pins.
