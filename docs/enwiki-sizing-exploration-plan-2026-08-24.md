# enwiki sizing, exploration, and qualification plan

**Report date:** 2026-08-24

**Candidate workload:** Complete enwiki MediaWiki History and logging data,
integrated with the existing `frwiki`, `nlwiki`, and `ptwiki` publication

**Current status:** Exploration only. Generic monthly source planning is
implemented, including bounded source-window fetch and transactional ingest,
but enwiki has not been computed by this project and must not be added to the
production schedule before the remaining bounded-compute and qualification
gates in this report pass.

**Reference evidence:**
[`frwiki-capacity-report-2026-08-24.md`](frwiki-capacity-report-2026-08-24.md)

## Executive summary

Enwiki is feasible, but it is not a configuration-only addition. The canonical
source planner now resolves its monthly inventory and the pipeline consumes it
through resumable one-to-four-source transactions. The present page-week
implementation cannot safely scale by merely raising its bucket count. Enwiki
still needs a hierarchical or capped-writer aggregation before a complete run
is attempted.

The recommended target production envelope is:

| Resource | Current Toolforge limit | Enwiki target |
| --- | ---: | ---: |
| Memory per job | 6 GiB | **16 GiB** |
| Namespace memory | 8 GiB | **24 GiB** |
| CPU per job | 3 vCPU maximum | **4 vCPU preferred; 3 workable** |
| Namespace CPU | 16 vCPU | Existing aggregate quota is sufficient |
| Guaranteed free working storage | Shared NFS; no per-tool quota exposed | **250 GiB with windowed ingestion** |
| Working storage without windowed ingestion | n/a | **400 GiB** |
| Qualification job allowance | Not explicitly configured | **24 hours** |

Production acceptance must require a measured peak below **12 GiB**, leaving
at least 25% headroom in a 16 GiB container. A temporary 24 GiB ceiling would
be useful for the first exploratory full run, but it must not be used to hide
an unbounded algorithm.

The storage request assumes that compressed sources are deleted immediately
after strict ingest validation. Retaining all raw sources until ingest
completes adds approximately 128 GiB and materially increases pressure on
Toolforge's shared NFS.

## Evidence classes and confidence

This report intentionally separates three kinds of evidence:

1. **Direct observation:** current Wikimedia source listings and live
   Toolforge quota output inspected on 2026-08-24.
2. **Measured baseline:** the complete production-equivalent frwiki
   qualification documented in the reference report.
3. **Modeled enwiki values:** ranges derived from the observed enwiki/frwiki
   compressed-byte and edit-count ratios. These are planning estimates, not
   enwiki benchmark results.

Exact enwiki CPU, wall-time, scratch, output, and cgroup peaks remain unknown
until the bounded implementation completes a full qualification run.

## Current source inventory

The newest completed common MediaWiki History snapshot observed on 2026-08-24
was `2026-07`.

The official
[enwiki `2026-07` MediaWiki History directory](https://dumps.wikimedia.org/other/mediawiki_history/2026-07/enwiki/)
contains one object per event month rather than the yearly objects used by the
currently scheduled wikis.

| Input measure | Observed value |
| --- | ---: |
| MediaWiki History source objects | 308 |
| Covered object names | `2001-01` through partial `2026-08` |
| Exact compressed core bytes | 137,313,328,202 |
| Compressed core size | 127.88 GiB |
| Largest source object | 918,511,092 bytes (875.96 MiB) |
| Current logging dump bytes | 6,869,371,776 |
| Current logging dump size | 6.40 GiB |
| Total compressed transfer before retries | 134.28 GiB |

The current logging source is the official
[`enwiki-latest-pages-logging.xml.gz`](https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-logging.xml.gz).

For scale comparison:

| Ratio | Value |
| --- | ---: |
| enwiki/frwiki compressed history bytes | 6.438x |
| enwiki/frwiki current edit count | 5.730x |
| Estimated enwiki logical history rows | approximately 1.36 billion |

The logical-row value is estimated from Wikimedia edit counts. It must not be
reported as an exact dump row count until strict ingest manifests record a
complete enwiki generation.

## Modeled retained and transient storage

Applying the observed scale ratios to the measured frwiki artifacts gives the
following planning range:

| Component | Enwiki planning estimate |
| --- | ---: |
| Warehouse and analytical generation | 46–52 GiB |
| `page_weekly_edits` output | 4.2–4.8 GiB |
| Page-week scratch | 11–12 GiB |
| Retained patrol/logging source | 6.4 GiB |
| Normal retained enwiki footprint | **56–65 GiB** |
| Safe steady allocation | **70–80 GiB** |

### Rollover with windowed ingestion

During a generation-safe rollover, both the live generation and the candidate
generation must coexist until compute, validation, merge, and site publication
succeed. A bounded fetch-and-ingest window adds only a few source objects at a
time.

The expected high-water components are:

- 56–65 GiB for the current live enwiki generation and outputs;
- 46–52 GiB for the candidate warehouse and analytical generation;
- 11–12 GiB of page-week scratch;
- approximately 5 GiB of candidate page-week output;
- logging, browser artifacts, site staging, and a 1–4 GiB raw download window;
- at least 25% operational reserve.

The calculated practical floor is approximately 175 GiB. The preflight gate
should require **250 GiB free** to cover growth, filesystem variability,
retries, abandoned staging awaiting safe cleanup, and concurrent activity on
the shared filesystem.

### Rollover with the compatibility separated fetch stage

If all compressed history files are retained until a later ingest stage, the
same rollover adds 127.88 GiB of raw input. The safe working requirement rises
to roughly 350 GiB; the capacity request should therefore be **400 GiB**.

Toolforge exposes shared NFS rather than a guaranteed per-tool storage quota.
The capacity must be agreed operationally, and the preflight check must still
fail closed when the actual reserve is below the selected threshold. The
official [Tools NFS almost full runbook](https://wikitech.wikimedia.org/wiki/Portal:Toolforge/Admin/Runbooks/ToolsNfsAlmostFull)
describes the shared-storage risk.

## Current implementation status and blockers

### Monthly source discovery and bounded ingestion (implemented)

The Rust snapshot planner now gives fetch, expected-source validation,
snapshot-completeness checks, ingest, fingerprints, and recovery one
deterministic monthly-source contract. The `run` orchestrator now executes a
configurable one-to-four-source window, atomically commits each strict marker,
deletes its compressed source immediately, resumes abandoned partial downloads,
and publishes the candidate pointer only after the complete marker/output
inventory validates. Per-source recovery checks only its recorded outputs; one
exact generation inventory scan runs before publication, avoiding a full-tree
scan for each of enwiki's hundreds of sources. Enwiki remains lifecycle-gated
because bounded compute and production qualification are still separate
requirements.

Finalization also publishes a deterministic fragment manifest containing the
complete analytical and warehouse allowlist plus rows, bytes, and SHA-256 for
each source-owned Parquet. Compute and patrol derive their file and partition
lists from that manifest, so abandoned files and concurrent source-worker
artifacts cannot leak into an enwiki computation.

The implemented source-plan contract:

- resolve the newest completed snapshot once and pin it to the run;
- enumerate the exact expected event-month objects for that snapshot;
- define and test the bounded partial-final-month behavior;
- reject gaps, duplicates, unexpected objects, and snapshot mixing;
- record every source identity in the immutable generation manifest;
- keep enwiki in `qualifying` lifecycle state until all gates pass.

### Page-week aggregation

Production continues to use the qualified flat `256 x 1` layout for frwiki.
The Rust aggregation now also implements an explicit two-level layout for
larger workloads. Monthly reductions use one staging writer; primary
compaction opens only the governed writer batch; one primary is then streamed
by Parquet row group into 16 or 32 secondary files; and one secondary bucket is
reconciled at a time. The prior flat 512/1024 frwiki experiments remain useful
evidence that increasing simultaneous encoder/file state is counterproductive:
the 1024-bucket flat run exhausted the 6 GiB cgroup.

At 256 buckets, the measured largest frwiki bucket contained 492,446 staged
rows. Scaling by the two observed enwiki ratios gives approximately 2.82–3.17
million rows in an enwiki bucket. Loading and reconciling a bucket of that size
would not preserve the current memory envelope.

The enwiki-capable design should expose two independent bounds:

1. enough stable logical buckets to bound rows per reconciliation unit; and
2. a small cap on simultaneously open Parquet writers.

A suitable starting matrix is `64 x 16`, `64 x 32`, `128 x 16`, and
`128 x 32` (1,024–4,096 logical buckets). The initial candidate is `64 x 32`:
2,048 logical buckets with no more than 32 secondary writers. Primary inputs
are consumed using their Parquet row-group boundaries rather than loaded as a
complete multi-million-row frame.

Stable key hashing, deterministic bucket order, deterministic row sorting,
edit conservation, and atomic publication remain mandatory. The implementation
checks conservation before and after every reduction/routing level and deletes
completed scratch immediately; enwiki activation still requires measured
capacity evidence.

### Merge and validation (implemented)

A modeled enwiki `page_weekly_edits.parquet` is 4–5 GiB and may contain roughly
680–770 million rows. The large-file paths now share a projected sequential
Parquet reader. It opens each input once, parses its footer once, advances in
physical row-group order, caps application batches, and advises completed byte
ranges out of the Linux page cache. It is used by two-level weekly routing,
per-wiki and root merge, publication validation, browser-index summaries,
stage fingerprint date ranges, and the page-week default generator.

Merge validates input/footer/output row conservation and deterministic
wiki-major order while writing bounded Parquet batches. The page-week default
generator retains only its top 20 candidates rather than sorting or
materializing the complete dataset. Benchmark artifact inventories read only
Parquet metadata.

The current publication contract does not require a global page/week key sort:
page-week files are deterministic in stable logical-bucket order, and root
files are deterministic in wiki-major input order. Adding an external sort now
would add I/O without strengthening a declared contract. If a future artifact
requires global key order, it must use bounded sorted runs followed by a k-way
merge and incremental Parquet output; an in-memory global Polars sort is not an
acceptable implementation.

Qualification must verify that:

- validators consume Parquet row groups sequentially;
- merge never materializes every wiki or the complete enwiki metric;
- kernel page cache is released as row groups and metrics complete;
- completed scratch must be removed incrementally;
- publication must remain generation-aware, atomic, and fail closed.

## Proposed enwiki execution model

Enwiki preparation should initially be independent from the scheduled
`frwiki`/`nlwiki`/`ptwiki` refresh:

```text
resolve and pin completed snapshot
    -> create candidate generation
    -> fetch a bounded source window
    -> validate, ingest, and checkpoint each source
    -> delete validated raw source
    -> compute bounded per-wiki metrics
    -> fetch and compute patrol data
    -> validate the complete candidate generation
    -> acquire the shared publication lock
    -> merge current per-wiki generations
    -> validate and atomically publish the site
    -> retire the preceding enwiki generation
```

This isolates a long enwiki preparation run from existing freshness. The final
shared publication section should remain short and protected by the existing
single-flight rules.

The weekly scheduler may continue checking for a new completed snapshot.
Deterministic stage fingerprints should make same-snapshot executions near
no-ops. A newly completed monthly snapshot triggers the expensive full
generation unless a separately proven, semantically equivalent incremental
source protocol is introduced.

## Resource request rationale

### Memory

The present live Toolforge limits observed through `toolforge jobs quota` and
the namespace `ResourceQuota` are:

- 6 GiB maximum memory per job;
- 8 GiB total namespace memory;
- 0.5 GiB reserved by the web service at rest.

The requested steady envelope is 16 GiB per batch job and 24 GiB for the
namespace. Acceptance requires the full pipeline to stay below 12 GiB, leaving
25% sustained headroom. The additional namespace capacity allows the web
service and operational processes to coexist with one enwiki batch job; it is
not permission to run multiple full refreshes concurrently.

### CPU

The namespace has a 16-vCPU aggregate quota, but jobs currently have a 3-vCPU
per-job ceiling. Four vCPUs would allow bounded concurrency across independent
bzip2 sources and speed Polars reductions. Three vCPUs remains a viable initial
configuration if the per-job ceiling cannot be raised.

Concurrency must be memory-governed. The orchestrator should lower the number
of concurrent source workers automatically when cgroup or scratch headroom
approaches its configured floor.

### Time and network

Pure transfer time for the current 134.28 GiB source set, excluding request
overhead, throttling, and retries, is approximately:

| Sustained throughput | Pure transfer time |
| ---: | ---: |
| 100 Mbit/s | 3.20 hours |
| 250 Mbit/s | 1.28 hours |
| 500 Mbit/s | 0.64 hours |

Decompression, ingest, compute, validation, and publication are additional.
Until measured, qualification should allow 24 hours and plan for a 6–12 hour
new-snapshot run. These are scheduling assumptions, not an SLO or benchmark.

## Exploration and qualification sequence

### Phase A: implementation and bounded component tests

1. Add deterministic monthly-source discovery and fixtures.
2. **Completed:** add windowed fetch-and-ingest with strict per-source
   checkpoints and source-level restart tests.
3. **Completed:** select a persisted adaptive workload profile from compressed
   bytes, source count, and prior measured rows; record its source concurrency
   and two-level bucket layout in compute and publication provenance. The
   `large` profile remains fail-closed in production until this plan's
   qualification gates are completed.
4. Implement capped-writer or hierarchical page-week aggregation.
5. Convert all large-file validation to one-pass row-group iteration.
6. Add cgroup CPU, memory, I/O, scratch, and persistent-storage telemetry.
7. Run the largest observed monthly source through fetch and ingest under the
   proposed cgroup.
8. Run representative old, average, and recent high-volume month fixtures.

### Phase B: full qualification

1. Build a complete `2026-07` candidate generation.
2. Record exact source and output rows, bytes, dates, checksums, durations,
   CPU-seconds, memory peaks, storage peaks, and rows per bucket.
3. Rerun compute from the same warehouse and require identical artifact bytes
   or an explicitly documented normalized equivalence.
4. Rerun the complete same-snapshot workflow and prove that all eligible
   stages are reused.
5. Compute patrol and rights data and require plausible non-zero counts.
6. Merge enwiki with current production data without increasing initial browser
   downloads for unrelated wikis.
7. Perform a rollback drill.
8. Kill fetch, ingest, compute, validation, and site build separately and prove
   resumability and cleanup.

### Phase C: rollover and activation

1. Process the next completed enwiki snapshot while the qualified generation
   remains live.
2. Prove that sources and generations do not mix.
3. Measure the complete rollover storage high-water mark.
4. Require output cutoff advancement and conservation.
5. Publish, validate public freshness, then retire the previous generation.
6. Complete a second successful full run before changing enwiki from
   `qualifying` to `scheduled`.

## Production acceptance gates

Enwiki may be scheduled only when all of the following hold:

- full-run cgroup peak is at most 12 GiB in a 16 GiB container;
- sustained memory headroom is at least 25%;
- startup storage reserve is at least 250 GiB with windowed ingestion;
- scratch and persistent high-water marks are recorded and remain within their
  budgets;
- no aggregation unit or writer collection grows with total history size;
- source, ingest, compute, merge, and site fingerprints are deterministic;
- page-edit conservation and publication semantic validation pass;
- patrol and rights events are plausible and non-zero;
- a same-snapshot rerun is a near-no-op;
- an interrupted run resumes without duplicating or losing history;
- a full snapshot rollover succeeds while the preceding generation remains
  publishable;
- unrelated wikis are not downloaded by an enwiki browser request;
- rollback restores the preceding complete site generation;
- two complete qualification runs, including one rollover, succeed.

## Decision

Do not attempt enwiki in the current 6 GiB production job and do not add it by
only increasing `WIKI_ECON_WEEKLY_BUCKETS`.

Continue with bounded open writers, hierarchical aggregation, and sequential
validation. Then request a
16 GiB per-job/24 GiB namespace memory envelope and an operational guarantee of
250 GiB working headroom. Use a separate qualification job, retain current
publication until every semantic gate passes, and replace the estimates in
this document with measured enwiki evidence before activation.
