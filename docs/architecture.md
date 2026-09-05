# Architecture

This document records the decisions that currently matter to the codebase.
Exact dependency versions and lifecycle states are generated in the
[stack reference](generated/stack-reference.md).
Metric schemas, ownership, publication and receipt contracts, fingerprint
identities, browser layouts, and aggregation semantics are generated from the
canonical Rust registry in the [metric catalog](generated/metric-catalog.md).

## Pipeline Overview

The project has these distinct data layers rooted under `data/` and `output/`:

1. `data/raw/<wiki>/...tsv.bz2`
   Wikimedia MediaWiki History dump shards fetched from `dumps.wikimedia.org`.

2. `data/metric-input/<wiki>/_snapshots/<snapshot>/_compacted/year=<YYYY>/year_month=<YYYY-MM>/*.parquet`
   The qualified 13-column input contract shared by core, weekly, and patrol
   compute for schema-v2 generations. Readers retain compatibility with the
   historical `warehouse/` plus `parquet/` schema-v1 layout.

4. `data/patrol/<wiki>/`
   An atomic patrol-generation pointer plus immutable, snapshot/parser-aware
   monthly patrol and rights Parquet inputs used by the bounded incremental
   Rust compute path. See [incremental-patrol.md](incremental-patrol.md).

5. `output/<wiki>/*.parquet`
   Final per-metric outputs, later merged into `output/*.parquet`.

6. `output/defaults_*.json`, `output/meta_*.json`, and `output/manifest.json`
   Materialized dashboard artifacts consumed through the `site/src/data -> output` symlink.

Rust dashboard materialization writes the default and metadata JSON. The
checked-in `site/data-build/manifest.json.sh` entrypoint validates those
artifacts and assembles publication provenance. Generated JSON belongs in
`output/`, not next to the checked-in script.

The online-facing layer uses `output/`. Compute resolves the layer named by the
selected generation manifest; new generations use `data/metric-input/`.

Two operational locations matter during ingest:

- `data/parquet/<wiki>/_snapshots/<snapshot>/_markers/<source>.done`
- `data/snapshots/<wiki>/current-snapshot.json`

Marker files are the ingest completion contract for a source dump. The current
snapshot pointer is atomically replaced only after every expected source for a
generation has valid outputs in exactly one supported layout. Compute resolves that
pointer and therefore never scans two full monthly snapshots together. The
older generation remains a rollback source until the site build succeeds;
`snapshot-finalize` then removes it and any pre-generation legacy partitions.

Editor identity is generation-aware. Ingest preserves
`event_user_text_historical` alongside `event_user_id`; metric computation uses
the numeric user ID when present and the historical actor text otherwise.
Revision-deleted identities with neither value remain in edit, byte, and
page-week totals but are excluded from editor-level distributions. Every clean
compute writes a deterministic `editor_identity_coverage.json` report with the
excluded edit contribution by month and user type. A qualification run may
discard and redownload only a finalized, reproducible input generation whose
ingest algorithm is obsolete; source plans and remote inventory receipts are
retained so that the replacement remains pinned and resumable.

## Fetching

Fetch logic lives in [src/fetch.rs](../src/fetch.rs).

Important decisions:

- Downloads are streamed to disk. The code intentionally avoids buffering whole dump files in memory.
- Fetch uses a small internal transport boundary so retry, resume, and validation logic are testable without real network calls.
- Existing files are not trusted blindly. Fetch validates against remote metadata when possible.
- Partial files are resumed only when the server advertises range support.
- Concurrency is bounded. More parallelism looked attractive on paper but would compete with ingest for disk and bandwidth.
- Yearly, all-time, and monthly source layouts are resolved into one immutable
  `source-plan.json` before fetch begins. `run` consumes that plan in windows
  of one source by default (configurable up to four), stages resumable partials
  with both run and source IDs, and never fetches outside the plan allowlist.
  Monthly planning plus bounded ingestion does not by itself qualify giant
  projects for production; bounded compute remains required before enabling
  enwiki.
- Remote completeness metadata is deliberately separate from the canonical
  plan. A successful full check writes immutable `remote-inventory.json` with
  the plan hash, source count, sizes and available HTTP identity headers.
  Snapshot resolution and workload sizing reuse it; an invalid receipt is
  ignored and rebuilt only after a new complete probe.

## Ingest

Ingest logic lives in [src/ingest.rs](../src/ingest.rs).

Important decisions:

- Ingest now filters to `event_entity = revision` and `event_type = create` before writing parquet. This is the single biggest storage reduction in the local pipeline.
- Ingest no longer writes a full temporary TSV to disk. It decompresses `bz2` into in-memory CSV chunks, parses them with Polars, and writes parquet partitions directly.
- Source files are tracked by marker files inside their immutable snapshot generation. Reruns skip a source only when the marker validates its exact metric-input output inventory (or both legacy layers for schema-v1 data).
- The full pipeline treats each source as a transaction: download to
  pipeline-owned staging, validate identity, stream-decode, validate Parquet
  footers and row totals, atomically commit the marker, then sync and delete
  the compressed input. A crash loses at most the current bounded window;
  committed sources are reused without downloading them again.
- Source-level recovery validates only the files recorded by that source's
  marker. Immediately before publication, one generation-wide validation
  checks the exact source allowlist and exact Parquet inventory. This preserves
  fail-closed completeness without rescanning the full generation once per
  source.
- Finalization writes an atomic, deterministic `generation-manifest.json` that
  allowlists every immutable metric-input fragment with its source
  identity, row count, byte size, and SHA-256. Once a snapshot is selected,
  core compute, weekly aggregation, patrol compute, and their fingerprints read
  only manifest-listed fragments; filesystem discovery is retained solely for
  imported legacy datasets that have no snapshot pointer. Unlisted or abandoned
  Parquets therefore cannot enter a computation.
- Once the ingest stage receipt authenticates that manifest and its source
  plan, ordinary readers memoize the parsed allowlist and skip fragment hashes
  and footer reads. Independent artifact scrubs and recovery retain the strict
  physical validation path.
- After every source marker commits, finalization transactionally compacts the
  immutable source fragments by event month. It deterministically sorts all 13
  columns, packs source fragments toward a 192 MiB compressed target, caps each
  output at 512 MiB, and uses only one active Parquet writer. The prepared
  transaction is recoverable on either side of the directory rename. A
  generation-manifest schema-3 allowlist is published only after row, footer,
  size, and SHA-256 validation; the ingest receipt is then the authority that
  permits deletion of the replaced source fragments.
- `WIKI_ECON_COMPACTION_TARGET_BYTES` may select 128–256 MiB and
  `WIKI_ECON_COMPACTION_MAX_BYTES` may select target–512 MiB. These values are
  recorded in the compaction manifest. They are workload policy, never a
  wiki-name branch.
- Versioned Wikimedia filenames must form one complete expected snapshot before `current-snapshot.json` is published. Explicit `run --version` and `ingest --version` selections ignore abandoned raw files from older snapshots.
- Readers retain a legacy-layout fallback only until the first generation pointer is published. Underscore-prefixed staging/generation directories are never recursively scanned as ordinary data.
- Output is partitioned by `year=` and `year_month=` because the downstream metrics are monthly. This keeps month-scoped compute exact without loading an entire wiki.
- New snapshots first write one qualified metric-input fragment per logical source output,
  eliminating the duplicated 28-column warehouse and 10-column analytical
  writes, and then compact those fragments without changing logical rows.
  Production schema qualification measured a 39.90–41.93% reduction across
  nlwiki, ptwiki, and frwiki.
- Ingest failure cleanup removes partial outputs from every supported layout
  and never leaves a success marker behind.
- Snapshot retirement happens only after compute, merge, and the atomic site publication all succeed. `--skip-site-build` deliberately retains the prior generation.

Schema contracts live in [src/schema.rs](../src/schema.rs):

- `INGEST_COLUMNS`: columns read from TSV
- `METRIC_INPUT_COLUMNS`: exact shared production compute contract
- `WAREHOUSE_COLUMNS` and `ANALYTICAL_COLUMNS`: schema-v1 compatibility

If a new metric needs more fields, the preferred path is:

1. add them to `INGEST_COLUMNS` and `METRIC_INPUT_COLUMNS`
2. increment the ingest/generation algorithm version
3. update every bounded consumer and requalify storage and performance

## Compute

Compute logic lives in [src/compute/mod.rs](../src/compute/mod.rs) and the per-family modules.

Important decisions:

- `load_wiki()` still exists and still loads a whole base frame into memory. It is retained for:
  - compatibility with older flat parquet layouts
  - benchmark split-stage measurements
  - tests and small-wiki workflows
- Compatibility with older parquet files is deliberate. Both `load_wiki()` and partition loading can still derive missing analytical columns from legacy data, which makes migrations safer and keeps old test fixtures useful.
- `compute_all()` prefers the partitioned incremental path whenever the selected generation is laid out under `year=/year_month=` directories.
- Production “incremental” execution still means bounded partition-at-a-time
  memory use inside one selected snapshot. Cross-snapshot metric reuse now has
  a publication-invisible implementation and exact clean-build qualification
  command; it is deliberately not eligible for production publication until
  multiple real rollovers pass. See
  [Cross-snapshot incremental computation](cross-snapshot-incremental.md).
- Incremental compute processes one month partition at a time, computes exact month-scoped outputs, and maintains only the cross-month state needed for:
  - business funnel
  - labor cohorts
  - labor churn

Core history outputs are invalidated as four independent families:

- monthly stateless: GDP, GDP user-type share, inequality, and labor monthly
- activity tiers: monthly, quarterly, and yearly tier labels derived from
  editor-month aggregates
- lifecycle: business funnel, labor cohorts, and labor churn
- page-week: bounded weekly reduction, boundary reconciliation, and previous
  week values

Patrol remains a fifth, separate input and receipt path. `ComputePlan` decides
which history families are reusable before reading a partition. During a full
or multi-family rebuild, each analytical partition is opened once and its
frame is offered only to the invalid monthly/activity/lifecycle accumulators.
Page-week retains its separate two-level disk-bucket scan because it projects
different page identity and timestamp columns. This preserves scan fusion
without coupling unrelated invalidation domains.

The family algorithm constants live beside their contracts under
`src/compute/{monthly,activity,lifecycle,weekly}/`. CI maps semantic source
paths to those constants. Shared compute-orchestration changes conservatively
map to every family unless an exact reviewed no-semantic-change declaration is
present.

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
- merge materializes `defaults_*.json` and `meta_*.json` in Rust, then invokes
  the checked-in `site/data-build/manifest.json.sh` validator to atomically
  publish `manifest.json`; the site never relies on stale Observable loaders
- manifest readiness follows `current-snapshot.json` and its matching ingest
  receipt; deleted raw transport files are diagnostic only and never make a
  completed generation look unfetched. Patrol readiness counts Parquet rows,
  so a header-only or zero-row file cannot appear ready.
- Rust dashboard generation and the manifest validator are critical: any
  missing, malformed, or failed artifact stops merge
- dashboard code and manifest-generator files are deterministic merge-
  fingerprint inputs, so a semantic change invalidates reuse
- merge requires every per-wiki metric output to include a non-null string
  `wiki` column and fails if inputs violate deterministic wiki-major order

Large metric inputs are projected and consumed sequentially by physical
Parquet row group. Merge writes bounded batches atomically and checks row
conservation as it advances; it never concatenates complete input frames in
memory. Current merged files have a deterministic wiki-major contract rather
than a global metric-key sort. Any future globally sorted artifact must use
external sorted runs and a bounded k-way merge.

## Storage Helpers

Filesystem conventions are centralized in [src/storage.rs](../src/storage.rs).

Do not hardcode:

- `parquet/`
- `warehouse/`
- `metric-input/`
- `_markers/`
- `year=.../year_month=...`

Use the helper functions instead. Earlier versions of the code duplicated path logic across modules, which made refactors brittle.

## Wiki lifecycle

Refresh scheduling does not define dataset ownership or retention. The
checked-in lifecycle registry independently records publication and refresh
states; see [Wiki lifecycle management](wiki-lifecycle.md). Merge filters on
publication state, while deployment orchestration filters on refresh state.
Paused imported datasets therefore remain published and retained.

## Publication gate

Script-driven refreshes use a run-scoped, fail-closed validation boundary
between merge and site build. Rust verifies schemas, lifecycle coverage,
plausibility thresholds, dates, snapshot selection, patrol sources, and row/
edit conservation before issuing `output/publication-gate.json`. The site
builder rechecks that receipt before its atomic switch. See
[Publication gate](publication-gate.md) for the artifact protocol and runbook.

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

The lcov gate hard-fails on any uncovered `DA:` (line) record. It also parses
`BRDA:` (branch) records when present and reports a coverage summary, but the
branch gate is informational by default: `cargo-llvm-cov` only emits branch
records on a nightly toolchain with the unstable `--branch` flag, and the repo
pins stable. Running `cargo +nightly llvm-cov --branch ...` followed by
`scripts/check_lcov.py --require-branches lcov.info` is supported for local
opt-in branch enforcement.

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
