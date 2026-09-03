# Incremental patrol pipeline

## Status and guarantees

The production Rust patrol path is generation-aware and monthly incremental.
It preserves the legacy singleton reader only as a migration fallback for
already-published candidates; every snapshot-aware fetch writes immutable
monthly source artifacts and never overwrites `patrol.parquet` or
`rights.parquet`.

The implementation guarantees:

- an exact dated source plan is pinned before any history source is downloaded;
- incomplete upstream logging dumps become a resumable `waiting_upstream`
  state rather than a late pipeline failure;
- a complete split logging dump is accepted when its recombined artifact is
  not yet available;
- concatenated multi-member gzip input is completely decoded;
- source transport identity and parse counts are retained;
- a parser change creates a different immutable generation;
- computation materializes at most one source month, one revision month, and
  bounded lookup/state structures at a time;
- rights changes invalidate the affected suffix, not unrelated core metrics;
- a same-snapshot rerun reuses authenticated source and compute receipts;
- incremental and clean patrol output is byte-identical.

## Source generation

For snapshot `2026-08`, the selected source layout is:

```text
data/snapshots/<wiki>/2026-08/
  patrol-source-plan.json
  # patrol-source-status.json exists only while waiting for upstream

data/patrol/<wiki>/
  current-generation.json
  generations/2026-08/<sha256(parser-version)>/
    generation.json
    patrol/year=YYYY/month=YYYY-MM/part-00000.parquet
    rights/year=YYYY/month=YYYY-MM/part-00000.parquet
```

The source plan records the history snapshot, required logging date, coverage,
layout, sorted source allowlist, upstream size/MD5/SHA-1 identities, and a
canonical plan hash. It is written atomically and is immutable for the run.
The generator accepts either the single completed recombined file or the
complete contiguous sequence of split files; missing and duplicate split
parts fail closed.

`generation.json` records each remote URL, content length, upstream and
downloaded checksums,
source-plan identity,
history snapshot and coverage, ETag and
Last-Modified values when supplied, downloaded SHA-256, parser version,
autopatrol groups, total/patrol/rights/skipped counts, and every monthly
artifact's rows, bytes, SHA-256, ordering contract, and observed modification
time. The manifest has a canonical semantic hash. The atomic current pointer
also records the exact manifest-file hash so non-Rust readiness tooling can
verify the selected receipt without parsing nanosecond integers imprecisely.

The downloaded gzip is deleted only after all monthly Parquets and the synced
manifest are durable. A failed build removes only its identified staging
directory. An incomplete final generation fails closed and is not silently
adopted.

## Incremental dependency graph

Patrol action month and metric month are not assumed to be identical. A log
action in February can reference a revision created in January. Each logging
month therefore has an authenticated source-month index containing:

- its patrol artifact identity;
- every referenced revision month and canonical month digest;
- an enriched, content-addressed event artifact;
- an unresolved revision count.

Unresolved revision IDs force reconsideration on the next snapshot. A resolved
cross-month reference invalidates the referenced metric month when that
revision month changes.

```text
patrol source month -+
revision month(s) ---+-> enriched source index -+
autopatrol groups ---+                           |
rights prefix through metric month -------------+-> patrol month artifact
canonical revision metric month ----------------+
```

Rights state advances through every month in the union timeline, including a
month with no patrol metric rows. At each December boundary Rust stores an
authenticated checkpoint containing the sorted active-user set and the exact
rights-prefix/groups identities. On a miss, computation restores the newest
matching prior checkpoint and replays the suffix. A changed historical rights
event necessarily changes all later prefix identities.

The final per-wiki `patrol.parquet` is assembled from deterministic monthly
parts. Patrol remains independent of core-history family receipts: a parser or
rights change never invalidates GDP, activity, lifecycle, or page-week output.

## Validation and recovery

Normal commands remain:

```sh
wiki-econ --data-dir "$WIKI_ECON_DATA_DIR" patrol-fetch nlwiki
wiki-econ --data-dir "$WIKI_ECON_DATA_DIR" --output-dir "$WIKI_ECON_OUTPUT_DIR" \
  patrol-compute nlwiki
```

Both commands pin themselves to the wiki's validated core
`current-snapshot.json` pointer; they do not independently resolve a snapshot.
Preparation and qualification call the same patrol preflight before starting
history transfer. Exit code `75` means the exact upstream logging dump is not
complete yet and is safe to retry later; it is not a corrupt local candidate.

Use `--rebuild` only to invalidate the patrol computation deliberately. The
source generation stays reusable unless its parser version changes.

After a run, verify:

1. `patrol-source-plan.json` selects the exact dated dump and covers the
   intended history snapshot;
2. `current-generation.json` selects the intended snapshot;
3. `generation.json` counts conserve `total = patrol + rights + skipped`;
4. the patrol compute stage reports reused versus rebuilt artifacts;
5. the output receipt and public manifest select the same snapshot;
6. patrol and rights counts remain non-zero for a substantial managed wiki.

Deleting cache data is not a repair strategy. A corrupt cache receipt or
artifact is rejected and only that content-addressed unit is rebuilt. A corrupt
source generation is fail-closed and should be quarantined for diagnosis before
refetching that snapshot.

## Regression qualification

Tests cover multi-member gzip, zero-event rejection, snapshot-aware source
reuse, historical and cross-month corrections, unresolved references,
rights-only months, checkpoint restoration/tampering, same-snapshot no-op, and
incremental-versus-clean SHA-256 equality. Before enabling enwiki patrol,
repeat the clean/incremental equivalence test with production-sized logging
history and enforce the same cgroup and scratch budgets as the core pipeline.
