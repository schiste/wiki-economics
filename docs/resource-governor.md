# Rust resource governor

The `wiki-econ run` orchestrator admits work from an explicit resource budget.
It does not rely on the number of visible host CPUs, which is not a reliable
description of a Toolforge job's CPU or memory quota.

## Admission model

At snapshot start, Rust resolves the compressed size of every pending source.
The run fails before downloading data unless free persistent storage can hold:

1. the configured persistent-storage reserve;
2. a conservative candidate-generation estimate equal to all pending
   compressed sources; and
3. the largest concurrently active source window.

Each source then executes as an independent transaction: admit, download,
validate, ingest immutable fragments, commit its strict marker, and release its
reservation. Admission is serialized. The storage check includes reservations
held by other workers, preventing concurrent workers from all claiming the same
free bytes. If a runtime gate closes, no new source starts; already-admitted
transactions finish normally.

Weekly page aggregation validates every logical month before collecting it.
Scratch Parquets contain stable-bucket row groups, so 256, 512, or 1024 logical
buckets require only one active staging writer. Predicate pushdown reads the
selected row groups during reconciliation. This avoids the former one-open-file
per-bucket behavior without changing the published schema or aggregation.

## Configuration

All byte values are integer bytes.

| Environment variable | Meaning | Rust default |
| --- | --- | ---: |
| `WIKI_ECON_MEMORY_CEILING_BYTES` | Absolute process/cgroup memory budget | detected cgroup limit, otherwise 16 GiB |
| `WIKI_ECON_MEMORY_RESERVE_BYTES` | Memory that admission must keep unused | 25% of ceiling |
| `WIKI_ECON_PERSISTENT_STORAGE_RESERVE_BYTES` | Free persistent bytes retained after admitted work | 0 |
| `WIKI_ECON_SCRATCH_LIMIT_BYTES` | Maximum pipeline-owned scratch bytes | 64 GiB |
| `WIKI_ECON_MAX_OPEN_FILES` | File-descriptor admission ceiling | 512 |
| `WIKI_ECON_SOURCE_WORKERS` | Concurrent source transactions | 1 |
| `WIKI_ECON_THREAD_LIMIT` | Upper bound for Rayon and Polars pools | configured pool size, otherwise 1 |
| `WIKI_ECON_MAX_LOGICAL_PARTITION_BYTES` | Largest month accepted by compute | 8 GiB |
| `WIKI_ECON_MAX_ACTIVE_PARQUET_WRITERS` | Parquet writers allowed at once | 16 |

`RAYON_NUM_THREADS` and `POLARS_MAX_THREADS` must not exceed
`WIKI_ECON_THREAD_LIMIT`. Environment parsing and contradictory budgets are
fail-closed.

The Toolforge wrapper pins a 6 GiB ceiling, 1.5 GiB memory reserve, 10 GiB
persistent reserve, one source worker, and one compute thread for the existing
job. These are operational defaults, not enwiki qualification results.

## Proposed enwiki qualification profile

Begin capacity experiments—not production scheduling—with:

```text
WIKI_ECON_MEMORY_CEILING_BYTES=17179869184
WIKI_ECON_MEMORY_RESERVE_BYTES=4294967296
WIKI_ECON_PERSISTENT_STORAGE_RESERVE_BYTES=268435456000
WIKI_ECON_SOURCE_WORKERS=2
WIKI_ECON_THREAD_LIMIT=4
RAYON_NUM_THREADS=4
POLARS_MAX_THREADS=4
WIKI_ECON_MAX_ACTIVE_PARQUET_WRITERS=16
```

Test two and three source workers, three and four compute threads, and 16 and
32 writer ceilings. Keep the combination only if the capacity report retains
at least 25% sustained memory headroom and the measured storage peak stays
inside quota plus reserve.

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
