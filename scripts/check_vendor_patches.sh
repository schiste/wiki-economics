#!/usr/bin/env bash
# Vendor patch hygiene: every local crates.io replacement must carry a
# PATCHES.md describing its upstream version and intentional differences.
#
# To resolve a failure here:
#   1. Edit the crate's PATCHES.md to describe the upstream version and diff.
#   2. If upstream fixed it, remove its [patch.crates-io] entry and vendor tree.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
for vendor_dir in "$ROOT"/vendor/*; do
  [ -d "$vendor_dir" ] || continue
  patches_file="$vendor_dir/PATCHES.md"
  relative_dir="${vendor_dir#"$ROOT/"}"

  if [ ! -f "$patches_file" ]; then
    echo "$relative_dir/PATCHES.md is missing." >&2
    exit 1
  fi

  if ! grep -qE '\b0\.[0-9]+\.[0-9]+\b' "$patches_file"; then
    echo "$relative_dir/PATCHES.md does not name an upstream version." >&2
    exit 1
  fi

  echo "$relative_dir: documented"
done
