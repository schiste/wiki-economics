#!/usr/bin/env bash
# Vendor patch hygiene: every local crates.io replacement must carry a
# PATCHES.md describing its upstream version and intentional differences.
#
# To resolve a failure here:
#   1. Edit the crate's PATCHES.md to describe the upstream version and diff.
#   2. If upstream fixed it, remove its [patch.crates-io] entry and vendor tree.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
exec node "$ROOT/scripts/check-vendor-patches.cjs"
