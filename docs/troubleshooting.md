# Troubleshooting

A short list of the failure modes a stranger to the repo is most likely
to hit on a clean machine. If you encounter something not listed here,
please open an issue with the relevant log lines.

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

## Patrol Parquets are empty after a successful-looking fetch

Logging dumps can be concatenated multi-member gzip streams. The Rust patrol parser reads every
member and logs `total_log_items`, `patrol_events`, `rights_events`, and `skipped_events`. It rejects
a substantial dump when both relevant-event counts are zero, while preserving the preceding
Parquet outputs.

After correcting patrol ingestion, rebuild only the patrol-dependent artifacts; the historical
revision pipeline does not need to run again:

```bash
cargo run --release -- patrol-fetch nlwiki
cargo run --release -- patrol-compute nlwiki --rebuild
cargo run --release -- merge
./scripts/build-site.sh
```

Snapshot-aware patrol fetches no longer retain the gzip or mutable source
Parquets after commit. Readiness comes from
`data/patrol/<wiki>/current-generation.json` and its authenticated monthly
generation. If the site reports `needs_patrol_fetch`, verify that the patrol
pointer snapshot matches the core snapshot and that its manifest-file hash is
unchanged. See [incremental-patrol.md](incremental-patrol.md) before removing
any generation or cache artifact.

## Ingest "marker is valid" skip when you expect a rebuild

Ingest is idempotent based on the marker manifest at
`data/parquet/<wiki>/_snapshots/<snapshot>/_markers/<source>.done` for
versioned dumps. To force a rebuild, delete the relevant marker inside that
snapshot generation and rerun ingest with the same `--version`. Older local
fixtures may still use the legacy `data/parquet/<wiki>/_markers/` location
until their first versioned ingest. The architecture document has more on
this contract.

## Ingest reports multiple or incomplete snapshots

Standalone ingest refuses to guess when `data/raw/<wiki>/` contains multiple
snapshot versions, and it will not publish a generation missing an expected
yearly shard. Pass the intended `--version YYYY-MM` after fetch completes.
Explicit selection ignores older abandoned raw files; successful refresh
cleanup removes them afterward.

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
