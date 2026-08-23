# Observable offline browser closure

This directory contains the exact jsDelivr-transformed ESM modules consumed by
the production Observable Framework build. `scripts/prepare-site-source.cjs`
copies the cache into a fresh source tree, and `scripts/deny-network.cjs`
prevents the build from resolving anything remotely.

The allowlist and exact versions live in
`config/site-dependency-closure.json`. Regenerate this cache only as part of a
reviewed dependency update, then run `scripts/verify-site-reproducibility.cjs`.
DuckDB modules and extensions are intentionally absent: the shipped site uses
Arrow and Parquet-WASM and the closure verifier rejects DuckDB assets.
