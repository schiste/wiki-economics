# Troubleshooting

A short list of the failure modes a stranger to the repo is most likely
to hit on a clean machine. If you encounter something not listed here,
please open an issue with the relevant log lines.

## "DuckDB CLI is still missing" after `./scripts/setup.sh`

`scripts/setup.sh` checks for the `duckdb` binary and prints an
actionable install URL when missing. On stock Debian/Ubuntu there is no
`duckdb` package in the default repositories, so the apt-based install
path silently no-ops. Install from
<https://duckdb.org/docs/installation/> (or `brew install duckdb` on
macOS), then rerun `./scripts/setup.sh`.

## The dashboard renders with no data after first install

`scripts/setup.sh` deliberately skips the Observable build when
`output/manifest.json` is missing — building against an empty `output/`
tree produces a hollow dashboard with no signposting. Run
`./scripts/refresh.sh frwiki` (or another supported wiki) first to
populate `output/`, then `npm run build:site` or rerun
`./scripts/setup.sh`.

## "No raw data for X. Run `fetch` first." during ingest

The ingest stage requires `data/raw/<wiki>/*.tsv.bz2` files to be
present. Run `cargo run --release -- fetch <wiki>` (or
`./scripts/refresh.sh <wiki>`) before invoking ingest.

## "No patrol data for X. Run `patrol-fetch` first." during compute

Patrol metrics require the patrol log dump to have been fetched. Run
`cargo run --release -- patrol-fetch <wiki>` first. If you are not
interested in patrol metrics, run `cargo run --release -- compute
<wiki>` once and ignore the patrol failure mode for now — the rest of
the pipeline still produces output.

## Ingest "marker is valid" skip when you expect a rebuild

Ingest is idempotent based on the marker manifest at
`data/parquet/<wiki>/_markers/<source>.done`. To force a rebuild, delete
the relevant marker file (or the whole `_markers/` directory) and rerun
ingest. The architecture document has more on this contract.

## `cargo llvm-cov` reports an uncovered line

The lcov gate hard-fails on any uncovered `DA:` (line) record; the
cause is almost always that a recent change introduced a new error
branch (typically a multi-line `?` propagation) that the test suite
does not yet exercise. The fix is either (a) add a test that hits the
new branch or (b) restructure the call so the `?` lives on the same
line as the call expression. Search the existing source for examples.

## Branch coverage requires nightly

`cargo-llvm-cov --branch` is a nightly-only flag. The repo pins stable,
so the standard CI flow does not collect branch coverage. Run
`cargo +nightly llvm-cov --branch --workspace --all-features
--all-targets --lcov --output-path /tmp/lcov.info` followed by
`python3 scripts/check_lcov.py --require-branches /tmp/lcov.info` for
local opt-in branch enforcement.

## `cargo deny` flags an unknown source

The `[patch.crates-io]` substitution to `vendor/polars-utils` is
expected. If a different unknown source surfaces, `cargo-deny` surfaced
it correctly and you should investigate. See
[`vendor/polars-utils/PATCHES.md`](../vendor/polars-utils/PATCHES.md)
for the rationale on the existing patch.

## Windows + the `site/src/data` symlink

`site/src/data` is a symlink to `../../output`. Windows requires
Developer Mode or `git config core.symlinks true` for `git clone` to
materialize the symlink. Without it, the dashboard build fails to
locate any data. On macOS and Linux this is automatic.
