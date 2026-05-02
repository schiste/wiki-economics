# Architecture

This document records the decisions that currently matter to the codebase.

## Pipeline Overview

The project has five distinct data layers rooted under `data/` and `output/`:

1. `data/raw/<wiki>/...tsv.bz2`
   Wikimedia MediaWiki History dump shards fetched from `dumps.wikimedia.org`.

2. `data/warehouse/<wiki>/year=<YYYY>/year_month=<YYYY-MM>/*.parquet`
   Filtered and normalized revision-create rows with a wider set of columns.

3. `data/parquet/<wiki>/year=<YYYY>/year_month=<YYYY-MM>/*.parquet`
   Ultra-slim analytical layer used by the compute pipeline.

4. `output/<wiki>/*.parquet`
   Final per-metric outputs, later merged into `output/*.parquet`.

5. `output/defaults_*.json` and `output/manifest.json`
   Materialized dashboard artifacts consumed through the `site/src/data -> output` symlink.

The checked-in generator sources for those dashboard artifacts live under
`site/data-build/*.json.sh`. Generated JSON belongs in `output/`, not next to
the checked-in scripts.

The online-facing layer should use `output/`. The compute pipeline should use `data/parquet/`. The `data/warehouse/` layer exists for future metric work that needs more than the analytical columns.

There is also an operational location that matters during ingest:

- `data/parquet/<wiki>/_markers/<source>.done`

Those marker files are the ingest completion contract for a source dump. They are written only after both analytical and warehouse parquet outputs complete successfully.

## Fetching

Fetch logic lives in [src/fetch.rs](../src/fetch.rs).

Important decisions:

- Downloads are streamed to disk. The code intentionally avoids buffering whole dump files in memory.
- Fetch uses a small internal transport boundary so retry, resume, and validation logic are testable without real network calls.
- Existing files are not trusted blindly. Fetch validates against remote metadata when possible.
- Partial files are resumed only when the server advertises range support.
- Concurrency is bounded. More parallelism looked attractive on paper but would compete with ingest for disk and bandwidth.
- Monthly-partitioned giant wikis are still rejected by the fetch planner. That is deliberate until the raw-file planning for those projects is implemented properly.

## Ingest

Ingest logic lives in [src/ingest.rs](../src/ingest.rs).

Important decisions:

- Ingest now filters to `event_entity = revision` and `event_type = create` before writing parquet. This is the single biggest storage reduction in the local pipeline.
- Ingest no longer writes a full temporary TSV to disk. It decompresses `bz2` into in-memory CSV chunks, parses them with Polars, and writes parquet partitions directly.
- Source files are tracked by marker files under `parquet/<wiki>/_markers/`. Reruns skip a source only when the marker still validates both the analytical and warehouse outputs for that source.
- Output is partitioned by `year=` and `year_month=` because the downstream metrics are monthly. This keeps month-scoped compute exact without loading an entire wiki.
- The slim analytical layer is intentionally duplicated from the warehouse layer. This trades some disk space for much lower compute-time scan and memory cost.
- Ingest failure cleanup must stay symmetric: if a source fails partway through, both analytical and warehouse partial outputs are removed and the marker is not left behind.

Schema contracts live in [src/schema.rs](../src/schema.rs):

- `INGEST_COLUMNS`: columns read from TSV
- `WAREHOUSE_COLUMNS`: richer normalized revision layer
- `ANALYTICAL_COLUMNS`: exact compute input contract

If a new metric needs more fields, the preferred path is:

1. add them to `WAREHOUSE_COLUMNS`
2. decide whether they are also needed in `ANALYTICAL_COLUMNS`
3. update the incremental compute path if they affect large-wiki processing

## Compute

Compute logic lives in [src/compute/mod.rs](../src/compute/mod.rs) and the per-family modules.

Important decisions:

- `load_wiki()` still exists and still loads a whole base frame into memory. It is retained for:
  - compatibility with older flat parquet layouts
  - benchmark split-stage measurements
  - tests and small-wiki workflows
- Compatibility with older parquet files is deliberate. Both `load_wiki()` and partition loading can still derive missing analytical columns from legacy data, which makes migrations safer and keeps old test fixtures useful.
- `compute_all()` prefers the partitioned incremental path whenever the analytical layer is laid out under `year=/year_month=` directories.
- Incremental compute processes one month partition at a time, computes exact month-scoped outputs, and maintains only the cross-month state needed for:
  - business funnel
  - labor cohorts
  - labor churn

This split is intentional:

- month-scoped metrics should never require a whole-wiki in-memory load
- cross-month state should be represented as compact maps or accumulators, not revision-level frames

If you add a new metric, decide which class it belongs to:

- month-local aggregation
- cross-month aggregation from compact per-user state
- true whole-history global analysis

The first two are acceptable in the incremental path. The third should be challenged before implementation.

## Merge

Merge logic lives in [src/merge.rs](../src/merge.rs).

Important decisions:

- merge only reads per-wiki metric files from `output/<wiki>/`
- merged outputs are written to `output/<metric>.parquet`
- merge also materializes the shared dashboard JSON artifacts (`defaults_*.json`, `manifest.json`) from the checked-in generators under `site/data-build/` so the site does not rely on stale Observable cache loaders
- merge assumes every per-wiki metric output already includes a `wiki` column

If a new metric omits the `wiki` column, merge will still concatenate files, but the combined output will be much less useful. Keep the `wiki` column in per-wiki outputs.

## Storage Helpers

Filesystem conventions are centralized in [src/storage.rs](../src/storage.rs).

Do not hardcode:

- `parquet/`
- `warehouse/`
- `_markers/`
- `year=.../year_month=...`

Use the helper functions instead. Earlier versions of the code duplicated path logic across modules, which made refactors brittle.

## Logging

Runtime logging uses `tracing`, configured in [src/main.rs](../src/main.rs).

Important decisions:

- normal command runs emit stage timing
- fetch/ingest/compute paths log structured fields rather than free-form `println!`
- tests initialize tracing through shared helpers to keep logging deterministic

When adding logs, prefer stable fields like:

- `wiki`
- `metric`
- `rows`
- `columns`
- `bytes`
- `elapsed_ms`

## Benchmarking

Benchmark logic lives in [src/bench.rs](../src/bench.rs).

Important decisions:

- split-stage benchmarks still use `load_wiki()` and per-module compute functions
- `compute_all` is benchmarked separately because it may use the incremental path
- benchmark outputs are optional and disposable by default

Interpret split timings and `compute_all` timings differently. They no longer necessarily measure the same execution model.

## Quality Gates

CI lives in [ci.yml](../.github/workflows/ci.yml).

The repo is expected to stay green on:

- `cargo fmt --all -- --check`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `cargo test --all-targets --all-features`
- `cargo doc --no-deps`
- `cargo deny check advisories bans licenses sources`
- `cargo audit -D warnings`
- `cargo llvm-cov --workspace --all-features --all-targets --lcov --output-path /tmp/wiki-economics-target/llvm-cov.info`
- `python3 scripts/check_lcov.py /tmp/wiki-economics-target/llvm-cov.info`

If you change the architecture significantly, expect to add tests instead of weakening the gates.

## Vendored Polars Patch

The repo vendors `polars-utils` under [vendor/polars-utils](../vendor/polars-utils) and patches it through [Cargo.toml](../Cargo.toml).

That patch exists because:

- the upstream dependency graph pulled in an advisory-bearing `bincode 2` edge
- the repo policy was to fix it without silencing the advisory

Implications:

- Polars upgrades are not just a version bump here
- any future Polars upgrade must re-check the vendored patch and dependency policy
- if upstream removes the need for the patch, deleting the vendor override is preferable to carrying it forever

## What To Preserve

Future changes should preserve these invariants unless there is a deliberate redesign:

- analytical compute input is partitioned by month
- compute outputs are stable parquet files under `output/<wiki>/`
- skip logic is source-marker-based, not guessed from output presence
- `compute_all()` remains able to process large partitioned datasets without loading a full wiki into memory
- logging and CI stay structured and strict
