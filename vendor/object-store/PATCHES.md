# `object_store` patch register

This directory mirrors `object_store 0.13.2` from crates.io and is selected by
the workspace's `[patch.crates-io]` configuration.

## Why it exists

Polars 0.55 requires `object_store ^0.13.1`. That release line constrains
`quick-xml` below 0.41, which is affected by `RUSTSEC-2026-0194` and
`RUSTSEC-2026-0195`. The fixed `object_store 0.14` release is outside Polars'
accepted semver range. Although the application's trimmed Polars features do
not compile cloud storage, `cargo audit` correctly checks optional lockfile
edges as well.

## Local changes

- Raise the `quick-xml` dependency from `0.39.0` to `0.41.0`.

No Rust source is changed. The XML APIs used by `object_store 0.13.2` remain
compatible with quick-xml 0.41.

## Updating

Remove this patch once Polars accepts `object_store >=0.14`. Until then,
refresh from the matching crates.io release, reapply the dependency-only
change, and run `scripts/check_vendor_patches.sh`, `cargo deny check`,
`cargo audit`, and the full Rust test suite.
