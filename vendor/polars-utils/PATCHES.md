# vendor/polars-utils — patch register

This directory is a vendored copy of `polars-utils 0.53.0` substituted into
the build via the top-level `Cargo.toml`'s `[patch.crates-io]` block. The
substitution is checked-in (rather than carried via `cargo patch`) so the
hygiene check in `scripts/check_vendor_polars.sh` can verify the copy stays
documented.

## Why this vendor copy exists

The upstream dependency graph for the polars 0.53 family pulled in a
`bincode 2.x` edge that surfaced under `cargo audit`. The repo policy was
to fix the advisory rather than silence it. Vendoring `polars-utils` lets
us pin its dependency choices independently of upstream maintainer cadence.

## Pinned upstream version

`polars-utils 0.53.0` (crates.io).

## Contents of the diff against upstream

When this copy was first imported the only material difference between
`Cargo.toml` (the registry-normalized form) and `Cargo.toml.orig` (the
maintainer-authored source) was a workspace-resolution artifact: the
optional `bincode 2.x` dependency was substituted for `rmp-serde` during
crates.io normalization. No source-code patches under `src/` are applied;
this is a dependency-graph patch, not a code patch.

## How to update

1. Bump polars to a release that resolves the underlying advisory upstream.
2. Delete the `[patch.crates-io]` block and this entire vendor directory.
3. Verify `cargo deny check` and `cargo audit` are still clean.

## How to refresh the vendor copy

If polars 0.53 needs to stay pinned but the vendor copy needs a routine
refresh:

1. `cargo download polars-utils@0.53.0 -x` (or fetch from
   `https://crates.io/api/v1/crates/polars-utils/0.53.0/download`).
2. Replace the contents of this directory.
3. Re-run `./scripts/ci-local.sh`.
4. Update this file with anything new in the diff.
