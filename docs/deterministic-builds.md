# Deterministic dependency closure

The project controls two dependency graphs:

1. npm installs are frozen by the root `package-lock.json` and exact direct
   versions in `site/package.json`;
2. browser modules emitted by Observable are frozen by
   `config/site-dependency-closure.json` and the content-hashed offline cache in
   `site/vendor/observable-cache`.

Node `24.15.0`, npm `11.12.1`, and Rust `1.98.0` are pinned in the package
manifest, local version-manager files, CI, `rust-toolchain.toml`, `Cargo.toml`,
and Toolforge's Build Service inputs. `scripts/verify-runtime.cjs` fails when
these declarations or the running tools disagree.

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

## Release provenance

The Toolforge release job writes `release-provenance.json` using the commit
timestamp as `SOURCE_DATE_EPOCH`. It records the binary hash, exact runtimes,
direct and generated browser closure, dependency-manifest hashes, OS identity,
system package versions, and dynamic library resolution. Manual SSH deployment
validates and installs this record beside the immutable Rust binary. Production
manifest generation fails closed when the release record is absent or does not
match the source commit and repository pins.
