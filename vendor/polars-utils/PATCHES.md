# `polars-utils` patch register

This directory mirrors `polars-utils 0.55.2` from crates.io and is selected by
the workspace's `[patch.crates-io]` configuration.

## Why it exists

Upstream's `serde` feature depends on the unmaintained `bincode 2.0.1`
(`RUSTSEC-2025-0141`). This repository's dependency policy fixes advisories
instead of allowing them.

## Local changes

- Remove the optional `bincode` dependency from `Cargo.toml` and from the
  `serde` feature.
- Use the already-required `rmp-serde` implementation for compact (`FC=false`)
  serialization as well as forward-compatible serialization.

This changes the internal serialized representation produced by Polars. The
application does not persist Polars logical plans, so there is no compatibility
contract with data written by the upstream bincode implementation.

## Updating

Refresh this directory from the matching crates.io release, reapply the two
changes above, then run `scripts/check_vendor_patches.sh`, `cargo deny check`,
and the full Rust test suite. Remove the patch when upstream removes `bincode`.
