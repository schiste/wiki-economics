# Rust resource governor

The `wiki-econ run` orchestrator admits work from an explicit resource budget.
It does not rely on the number of visible host CPUs, which is not a reliable
description of a Toolforge job's CPU or memory quota.

## Admission model

At snapshot start, Rust resolves the compressed size of every pending source.
The run fails before downloading data unless free persistent storage can hold:

1. the configured safety reserve;
2. the bounded scratch reserve;
3. the rollback-generation reserve;
4. a conservative candidate-generation estimate equal to all pending
   compressed sources; and
5. the largest concurrently active source window.

The current published generation is already charged to filesystem usage, so it
is not counted again as free space. The explicit rollback reserve keeps room for
one superseded publication while a candidate is built. The scratch reserve is
an admission floor, while `WIKI_ECON_SCRATCH_LIMIT_BYTES` remains the hard
runtime ceiling.

Each source then executes as an independent transaction: admit, download,
validate, ingest immutable fragments, commit its strict marker, and release its
reservation. Admission is serialized. The storage check includes reservations
held by other workers, preventing concurrent workers from all claiming the same
free bytes. If a runtime gate closes, no new source starts; already-admitted
transactions finish normally.

Weekly page aggregation validates every logical month before collecting it.
The production `256 x 1` layout writes stable primary-bucket row groups through
one staging writer and reads one primary bucket at a time. Larger workloads can
use two levels: monthly staging is compacted into 64 or 128 primary files in
writer-bounded batches, then one primary is streamed by Parquet row group into
16 or 32 secondary files. One or two independently admitted reconciliation
units may be loaded, sorted, and grouped at a time. Each worker writes an
immutable result artifact; the main thread appends those artifacts in explicit
logical-bucket order and immediately reclaims completed scratch. Primary
scratch is deleted after its validated secondary fragments are durable. The
run directory is reclaimed as one state transition.

Both levels use non-overlapping bits from the same stable page hash. Traversal
is primary-major then secondary-major, so repeated runs of one configuration
produce deterministic bytes. Row and edit totals are checked after monthly
reduction, primary routing, secondary routing, and final reconciliation.

## Configuration

All byte values are integer bytes.

| Environment variable | Meaning | Rust default |
| --- | --- | ---: |
| `WIKI_ECON_MEMORY_CEILING_BYTES` | Absolute process/cgroup memory budget | detected cgroup limit, otherwise 16 GiB |
| `WIKI_ECON_MEMORY_RESERVE_BYTES` | Memory that admission must keep unused | 25% of ceiling |
| `WIKI_ECON_PERSISTENT_STORAGE_RESERVE_BYTES` | Free persistent bytes retained after admitted work | 0 |
| `WIKI_ECON_BOUNDED_SCRATCH_RESERVE_BYTES` | Free bytes reserved for the current bounded scratch unit | 0 |
| `WIKI_ECON_ROLLBACK_GENERATION_RESERVE_BYTES` | Free bytes reserved for one rollback publication | 0 |
| `WIKI_ECON_SCRATCH_LIMIT_BYTES` | Maximum pipeline-owned scratch bytes | 64 GiB |
| `WIKI_ECON_MAX_OPEN_FILES` | File-descriptor admission ceiling | 512 |
| `WIKI_ECON_SOURCE_WORKERS` | Concurrent source transactions | 1 |
| `WIKI_ECON_THREAD_LIMIT` | Upper bound for Rayon and Polars pools | configured pool size, otherwise 1 |
| `WIKI_ECON_MAX_LOGICAL_PARTITION_BYTES` | Largest month accepted by compute | 8 GiB |
| `WIKI_ECON_MAX_ACTIVE_PARQUET_WRITERS` | Parquet writers allowed at once | 16 |
| `WIKI_ECON_WEEKLY_WORKERS` | Independently admitted weekly reconciliation workers; must not exceed the thread limit | 1 |
| `WIKI_ECON_WORKLOAD_PROFILE` | Manual `small`/`large` override for isolated qualification only | unset |
| `WIKI_ECON_REQUIRE_QUALIFIED_PROFILE` | Reject manual or unqualified profiles | false outside production; true in Toolforge wrappers |
| `WIKI_ECON_WEEKLY_BUCKET_COUNT` | Legacy flat primary count; cannot be combined with the next two settings | 256 |
| `WIKI_ECON_WEEKLY_PRIMARY_BUCKET_COUNT` | Primary count for an explicit one- or two-level layout: 32, 64, 128, 256, 512, or 1024 | 256 |
| `WIKI_ECON_WEEKLY_SECONDARY_BUCKET_COUNT` | Secondary count per primary: 1, 8, 16, or 32 | 1 |

`RAYON_NUM_THREADS` and `POLARS_MAX_THREADS` must not exceed
`WIKI_ECON_THREAD_LIMIT`. Environment parsing and contradictory budgets are
fail-closed. The weekly bucket variables remain available to `capacity-bench`
and legacy standalone commands; normal snapshot preparation persists and
consumes an adaptive profile instead.

## Adaptive workload profiles

At snapshot start Rust resolves one immutable
`data/snapshots/<wiki>/<snapshot>/workload-profile.json`. Selection uses the
canonical source plan's source count, the exact total compressed bytes obtained
from strict markers or remote metadata, and the last validated generation's
warehouse rows. It never branches on a wiki name.

| Profile | Preferred source workers | Primary buckets | Secondary buckets | Logical buckets |
| --- | ---: | ---: | ---: | ---: |
| `small` | 2 | 32 | 8 | 256 |
| `large` | 3 | 64 | 32 | 2,048 |

`small` is selected only at or below 64 GiB compressed, 64 sources, and five
billion prior measured rows. Exceeding any boundary selects `large`. Missing
source sizes fail closed. A missing prior row measurement is allowed for a
first generation because the source inventory and compressed bytes still bound
the decision.

Preferred source workers are capped by `WIKI_ECON_SOURCE_WORKERS` and the
source window. Production checks the resulting concurrency and logical bucket
count against `config/capacity-qualification.json`. A manual profile override,
an unknown wiki, or the currently unqualified `large` profile is rejected when
`WIKI_ECON_REQUIRE_QUALIFIED_PROFILE=1`. Qualification jobs may explicitly
override the profile with the production gate disabled; publishing it still
requires checked-in evidence and a new binary.

The Toolforge wrapper pins a 6 GiB ceiling, 1.5 GiB memory reserve, 10 GiB
safety reserve, 8 GiB bounded-scratch reserve, 8 GiB rollback reserve, one
effective source worker, and one compute thread for the existing job. The
adaptive `small` profile prefers two workers, but the checked-in production
capacity policy currently admits only the one-worker cap. These are operational
defaults, not enwiki qualification results.

Before a weekly worker starts, it reserves a conservative in-memory expansion
estimate plus result scratch. Admission proves that current cgroup memory,
existing worker reservations, the new estimate, and the mandatory reserve fit
below the ceiling. Scratch and file descriptors are checked the same way. A
closed gate rejects new work before Polars materializes the bucket; already
admitted workers finish and release their reservations through RAII even on an
error path.

Worker counts are physical admission limits, not computation versions.
`config/determinism-contract.json` pins the partition hash and every ordering
rule, while the compute algorithm version includes that contract plus the
selected primary/secondary topology. Qualification must compare exact
artifact SHA-256 values across at least two worker counts with topology held
constant; see `wiki-econ determinism-verify` in
[deterministic builds](deterministic-builds.md).

## Proposed enwiki qualification profile

Begin capacity experiments—not production scheduling—with:

```text
WIKI_ECON_MEMORY_CEILING_BYTES=17179869184
WIKI_ECON_MEMORY_RESERVE_BYTES=4294967296
WIKI_ECON_PERSISTENT_STORAGE_RESERVE_BYTES=268435456000
WIKI_ECON_BOUNDED_SCRATCH_RESERVE_BYTES=34359738368
WIKI_ECON_ROLLBACK_GENERATION_RESERVE_BYTES=68719476736
WIKI_ECON_SOURCE_WORKERS=2
WIKI_ECON_THREAD_LIMIT=4
RAYON_NUM_THREADS=4
POLARS_MAX_THREADS=4
WIKI_ECON_MAX_ACTIVE_PARQUET_WRITERS=32
WIKI_ECON_WEEKLY_PRIMARY_BUCKET_COUNT=64
WIKI_ECON_WEEKLY_SECONDARY_BUCKET_COUNT=32
```

Benchmark `64 x 16`, `64 x 32`, `128 x 16`, and `128 x 32`, with a writer
ceiling at least as large as the selected secondary count. Also test two and
three source workers and three and four compute threads. Keep a combination
only if the capacity report retains at least 25% sustained memory headroom and
the measured storage peak stays inside quota plus reserve. `capacity-bench`
accepts `--weekly-buckets <primary>` and
`--weekly-secondary-buckets <secondary>` and records all three counts
(primary, secondary, and logical) in report schema 5.

## Telemetry

Every admission, source completion, reduced partition, and reconciled bucket
records a structured JSON sample in the Rust log. Samples include:

- RSS plus cgroup current, peak, and limit memory;
- cgroup CPU user/system/total time, scheduling periods, throttled periods, and
  throttled time;
- RSS, cgroup current/recorded peak, and cgroup page-cache peak;
- process read/write bytes and derived throughput;
- scratch bytes and persistent-filesystem used/free bytes;
- open file descriptors plus active source and weekly-bucket workers;
- reserved source bytes plus bucket memory and scratch; and
- cumulative download bytes, ingested rows, durations, and throughput.

Linux production fails closed if memory or persistent-space telemetry needed
for a gate is unavailable. Non-Linux development hosts can run deterministic
tests without cgroup files, while still enforcing all available signals.
