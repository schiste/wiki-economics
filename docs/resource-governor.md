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
16 or 32 secondary files. Only one secondary reconciliation unit is loaded,
sorted, and grouped at a time. Primary scratch is deleted after its validated
secondary fragments are durable. Final-level scratch is retained until the
complete output has a footer, has been synced, and has been atomically renamed;
the run directory is then reclaimed as one state transition.

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
| `WIKI_ECON_WEEKLY_BUCKET_COUNT` | Legacy flat primary count; cannot be combined with the next two settings | 256 |
| `WIKI_ECON_WEEKLY_PRIMARY_BUCKET_COUNT` | Primary count for an explicit one- or two-level layout: 64, 128, 256, 512, or 1024 | 256 |
| `WIKI_ECON_WEEKLY_SECONDARY_BUCKET_COUNT` | Secondary count per primary: 1, 16, or 32 | 1 |

`RAYON_NUM_THREADS` and `POLARS_MAX_THREADS` must not exceed
`WIKI_ECON_THREAD_LIMIT`. Environment parsing and contradictory budgets are
fail-closed.

The Toolforge wrapper pins a 6 GiB ceiling, 1.5 GiB memory reserve, 10 GiB
safety reserve, 8 GiB bounded-scratch reserve, 8 GiB rollback reserve, one
source worker, and one compute thread for the existing job. These are
operational defaults, not enwiki qualification results.

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
(primary, secondary, and logical) in report schema 4.

## Telemetry

Every admission, source completion, reduced partition, and reconciled bucket
records a structured JSON sample in the Rust log. Samples include:

- RSS plus cgroup current, peak, and limit memory;
- cgroup CPU usage, throttled time, and throttle count;
- scratch bytes and persistent-filesystem used/free bytes;
- open file descriptors and active source workers;
- reserved source bytes; and
- cumulative download bytes, ingested rows, durations, and throughput.

Linux production fails closed if memory or persistent-space telemetry needed
for a gate is unavailable. Non-Linux development hosts can run deterministic
tests without cgroup files, while still enforcing all available signals.
