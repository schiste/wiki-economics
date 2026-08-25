# Storage footprint and incremental-compute assessment

**Measured:** 2026-08-25 on the Toolforge production filesystem  
**Published snapshot:** `2026-07`  
**Published wikis:** elwiki, frwiki, itwiki, nlwiki, ptwiki, svwiki

## Executive conclusion

The source-window pipeline is already doing the important transient-storage
operation correctly: compressed MediaWiki History sources are downloaded,
ingested, validated, and deleted one window at a time. Production's six raw
history directories were empty during this measurement.

The 36.43 GiB retained footprint is nevertheless larger than necessary because
it contains three different kinds of duplication:

1. completed qualification workspaces that are no longer needed;
2. compressed patrol XML sources retained after their Parquet conversion; and
3. a combined 331-million-row weekly metric beside the authoritative per-wiki
   files.

Removing the first two classes brings the measured installation to about
24.40 GiB without changing any data or metric semantics. Removing the redundant
combined weekly artifact brings it to about 22.36 GiB. A later migration from
the duplicated analytical/warehouse layers to one exact metric-input layer is
the largest remaining structural saving, but must be qualified before assigning
it a firm target.

The current function named `compute_all_incremental` is incremental only in its
*memory use*: it scans one calendar partition at a time. It does not reuse
metric partitions across two different Wikimedia snapshots. Same-snapshot runs
are content-fingerprinted no-ops; a newly selected snapshot currently performs
a complete ingest and compute.

That distinction matters because a monthly MediaWiki History release is a full,
corrected history, not a guaranteed append-only delta. Wikimedia explicitly
warns that historical records may change because of suppressions, renames,
reverts, and page moves. Correct cross-snapshot reuse must therefore be based on
canonical content equality, not solely on the month name.

## Measured retained storage

All values below came from `du -x -B1` on the same filesystem. GiB values use
1,073,741,824 bytes.

| Class | Bytes | GiB | Assessment |
|---|---:|---:|---|
| Entire measured tool root | 39,116,439,552 | 36.43 | Baseline |
| `data/warehouse` | 18,029,461,504 | 16.79 | Rich revision layer; overlaps analytical layer |
| `data/parquet` | 2,797,211,648 | 2.61 | Slim analytical layer |
| `data/patrol` | 4,192,034,816 | 3.90 | Includes 3.37 GiB of committed gzip inputs |
| All `data` | 25,041,940,480 | 23.32 | Raw MediaWiki History directories were empty |
| All `output` | 4,436,082,688 | 4.13 | Includes root combined and live per-wiki outputs |
| Root combined `page_weekly_edits.parquet` | 2,187,396,717 | 2.04 | Redundant for the browser/default dashboard path |
| Live immutable candidate roots | 846,229,504 | 0.79 | Required targets for promoted wiki symlinks |
| Completed capacity/qualification roots | 9,305,817,088 | 8.67 | Reclaim after compact evidence retention |
| Binary releases and application files | 332,390,400 | 0.31 | Three rollback-capable releases; appropriate |

The six compressed patrol inputs total 3,616,304,114 bytes (3.37 GiB):

| Wiki | Compressed XML bytes |
|---|---:|
| elwiki | 80,803,496 |
| svwiki | 202,742,303 |
| nlwiki | 605,661,763 |
| itwiki | 712,093,905 |
| ptwiki | 818,114,122 |
| frwiki | 1,196,888,525 |

Commit `d998a2c` changes patrol fetch so a gzip is released only after both
derived Parquets have been closed, synced, renamed, and their directory entry
has been synced. Failed parses retain the source for diagnosis.

## Retention target

### Immediate, no semantic migration

| Transition | Retained GiB |
|---|---:|
| Measured baseline | 36.43 |
| Retire completed qualification working roots | 27.76 |
| Release already-committed patrol gzip inputs | 24.40 |
| Stop retaining the combined weekly root artifact | 22.36 |

The first two changes are pure retention changes. The third requires the
publication manifest and semantic gate to represent `page_weekly_edits` as a
partitioned dataset rather than requiring a physically concatenated root file.
The existing per-wiki files remain authoritative and must still pass schema,
row, edit-conservation, ordering, date, wiki-label, and hash validation.

### Qualified structural migration

Schema-v1 ingest wrote each revision to both a 10-column analytical layer and
a 28-column warehouse layer. Schema-v2 ingest now writes one versioned,
13-column metric-input schema containing the exact union consumed by every
Rust metric. Readers select the layout from the immutable generation manifest,
so active and rollback schema-v1 generations remain valid.

The production qualification covered nlwiki, ptwiki, and frwiki under the 6 GiB
Toolforge cgroup. It measured 5,541,144,344 bytes (5.16 GiB, 41.28%) of aggregate
savings with a 514,822,144-byte maximum cgroup peak. See the
[qualification report](metric-input-schema-qualification-2026-08-25.md).

## Correct incremental-compute contract

For a fixed selected snapshot, commit, algorithm version, workload profile, and
input identities, every eligible stage must be a no-op. This behavior exists
today through deterministic stage receipts.

Across two distinct full snapshots, a partition may be reused only when all of
the following are equal:

- wiki and logical event-month identity;
- canonical metric-input content digest;
- metric-family algorithm and schema version;
- deterministic hash/partition algorithm version;
- output schema and deterministic writer version.

The canonical input digest must not depend on source filename, snapshot label,
download order, source worker count, fragment chunk boundaries, filesystem
metadata, or Parquet timestamps. It represents the sorted logical rows and the
columns actually consumed by that metric family.

### Metric invalidation units

| Metric family | Safe reuse unit | Additional invalidation |
|---|---|---|
| GDP, inequality, activity tiers, monthly labor | Calendar month | Recompute changed months only |
| Page weekly edits | Calendar month contribution | Reconcile changed month plus adjacent boundary weeks |
| Patrol | Calendar month | Existing month parts are reusable when patrol, rights, and revision inputs match |
| Funnel and cohorts | Checkpointed editor state | Replay from earliest changed month |
| Churn | Checkpointed active-editor state | Replay from earliest changed month and affected following period |

Algorithm versions must be split by metric family. A weekly bucket-topology
change should invalidate weekly artifacts, not GDP; a patrol parser change
should invalidate patrol sources and outputs, not core history metrics.

## Proposed deterministic stage graph

For each new full snapshot:

1. Resolve and persist the immutable `SnapshotPlan`.
2. Process sources through the existing bounded source window.
3. Write immutable metric-input fragments and a canonical digest per logical
   event month.
4. Compare those digests with the last published generation.
5. Reuse immutable metric partitions whose digest and metric-family algorithm
   version match; recompute only invalid partitions.
6. Replay checkpointed stateful families from the earliest changed month.
7. Reconcile page-week boundary contributions and externally merge only the
   affected sorted runs.
8. Validate complete row/edit conservation against the newly ingested full
   snapshot.
9. Publish only after the candidate generation and site pass the fail-closed
   gate.
10. Retire superseded generations and scratch through lifecycle transitions.

An unchanged partition can be hard-linked or reflinked into the candidate when
the filesystem supports it, with an ordinary copy fallback. Concurrent workers
must never append to the same file. The manifest owns the allowlist, and every
published byte remains independent of worker completion order.

## Required qualification tests

- Two same-snapshot runs are byte-identical and the second performs no compute.
- Two snapshots with identical logical history but different filenames reuse
  every metric partition.
- A correction ten years in the past invalidates the affected month and the
  necessary stateful suffix, without retaining the old value.
- A changed month at a week boundary produces the same weekly bytes as a full
  rebuild.
- One metric-family algorithm bump leaves unrelated families reusable.
- A worker-count change produces identical fragment and publication hashes.
- A killed candidate run resumes source by source and never modifies the live
  generation.
- Incremental output is byte-for-byte equal to a clean full recomputation.
- Generation rollover remains within the declared current + candidate +
  bounded-scratch + rollback + reserve storage policy.

## Operational decisions

- Keep the current source-window ingest and bounded two-level weekly reducer.
- Keep manual SSH deployment, but serialize binary/image switches with the
  publication lock.
- Treat completed qualification roots as expiring working data; retain compact
  reports, logs, workload profiles, output hashes, and provenance instead.
- Use Toolforge's local Wikimedia dump mount when an exact planned object is
  available, with verified HTTP fallback. A 128 MiB production sample measured
  about 25.4 MB/s from the mount versus 4.3–4.6 MB/s over HTTP.
- Do not claim cross-snapshot incrementality until incremental-vs-full
  equivalence passes for a real rollover.

## Source semantics

Wikimedia's MediaWiki History README describes each monthly release as the full
history and cautions against treating releases as safely incremental:

<https://dumps.wikimedia.org/other/mediawiki_history/readme.html>
