#!/usr/bin/env bash
# Vendor patch hygiene: verifies that vendor/polars-utils carries a
# PATCHES.md describing whatever differs from the upstream crates.io
# release. The repo replaces polars-utils via [patch.crates-io] in the
# top-level Cargo.toml; cargo-deny's [sources] block only audits
# crates.io-resolved sources and is bypassed by [patch.crates-io] with a
# local path. This script is the only automated check we have that the
# vendored copy hasn't drifted away from documented intent.
#
# To resolve a failure here:
#   1. Edit vendor/polars-utils/PATCHES.md to describe what differs from
#      the upstream crates.io polars-utils release and why.
#   2. If the patch is no longer needed (upstream fixed it), remove the
#      [patch.crates-io] entry from the top-level Cargo.toml and delete
#      the vendor/polars-utils tree.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
VENDOR_DIR="$ROOT/vendor/polars-utils"
PATCHES_FILE="$VENDOR_DIR/PATCHES.md"

if [ ! -d "$VENDOR_DIR" ]; then
  echo "vendor/polars-utils is missing; either restore it or remove the" >&2
  echo "[patch.crates-io] entry in the top-level Cargo.toml." >&2
  exit 1
fi

if [ ! -f "$PATCHES_FILE" ]; then
  echo "vendor/polars-utils/PATCHES.md is missing." >&2
  echo "Add it to document what the vendored polars-utils diff against the" >&2
  echo "upstream crates.io release contains and why." >&2
  exit 1
fi

# Sanity: PATCHES.md should at least mention the upstream version it pins.
if ! grep -qE '\b0\.[0-9]+\.[0-9]+\b' "$PATCHES_FILE"; then
  echo "vendor/polars-utils/PATCHES.md does not name a pinned upstream version." >&2
  echo "Include the upstream crates.io polars-utils version this copy mirrors." >&2
  exit 1
fi

echo "vendor/polars-utils PATCHES.md is present and references an upstream version."
