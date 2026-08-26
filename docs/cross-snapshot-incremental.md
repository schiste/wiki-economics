# Cross-snapshot incremental computation

## Status

Implemented as a **publication-invisible qualification path**. Normal
`prepare-wiki`, compute, merge, and publication continue to use the existing
full-snapshot family receipts. Incremental artifacts cannot be selected by the
publisher and a qualification never changes the current snapshot pointer.

Promotion requires multiple real snapshot rollovers to produce byte-identical
incremental and clean artifacts under the production resource limits and the
worker/topology variants used for qualification.

## Canonical event-month identity

Schema-v3 ingest writes compacted metric-input shards sorted by the exact 13
logical columns. Before source fragments are retired, Rust reads those shards
sequentially and performs a bounded k-way merge. The digest contains:

- logical schema version;
- canonical encoding version and ordered field names;
- an explicit null/present marker for every value;
- fixed-width big-endian integer and Boolean values;
- length-prefixed UTF-8 strings;
- every logical row, including duplicates, in total key order.

The digest deliberately excludes snapshot/source filenames, fragment
boundaries, source-worker count, Parquet bytes and metadata, and physical input
order. Every month receipt also records its row and edit totals. An unsorted
fragment, wrong-month row, unexpected type, empty month, truncated receipt, or
inventory mismatch fails ingest before selection.

Receipts live at:

```text
data/incremental/month-identities/<wiki>/<snapshot>/<event-month>.json
data/incremental/month-identities/<wiki>/<snapshot>/inventory.json
```

## Reuse graph

Content-addressed cache keys include the logical input digest, owning family
algorithm version, and artifact kind.

```text
canonical month
  ├─ GDP / type share / inequality / labor month
  ├─ editor-month aggregate
  │    └─ ordered year digest → month / quarter / year activity tiers
  └─ sorted page-week contribution
       └─ bounded k-way merge → reconciled weekly output

ordered month prefix through December
  └─ deterministic lifecycle checkpoint

complete ordered month list
  └─ final lifecycle artifacts for identical snapshots
```

If a historical month changes, later lifecycle-prefix keys change as well. The
builder searches backward for the newest matching December checkpoint and
replays the suffix. Checkpoints contain cumulative editor totals, cohort spans,
active-period counts, and churn spans. The within-period deduplication set is
not serialized because a checkpoint is written only after December, where the
month, quarter, and year periods all close.

Weekly month contributions are sorted immutable runs. Qualification externally
merges one bounded batch per run, combines identical page/week keys (including
the two contributions to a week crossing a month boundary), and maintains the
previous-week state while writing bounded output batches. The published
two-level disk bucket implementation remains unchanged during qualification.

## Qualification command

Both generations must already exist as immutable schema-v3 generations:

```sh
wiki-econ \
  --data-dir /data/project/wiki-economics/data \
  cross-snapshot-qualify frwiki \
  --baseline-version 2026-07 \
  --candidate-version 2026-08 \
  --work-dir /data/project/wiki-economics/capacity/cross-snapshot/frwiki/2026-08/run-1 \
  --report /data/project/wiki-economics/capacity/cross-snapshot/frwiki/2026-08/run-1.json
```

The command:

1. seeds content-addressed caches from the baseline;
2. builds the candidate through reuse;
3. builds the same candidate again from an empty output directory;
4. compares every final Parquet path, byte count, and SHA-256;
5. composes and compares semantic receipts containing schemas, rows, date
   bounds, wiki bounds, ordering contracts, and conservation totals;
6. records unchanged, changed, and removed logical months plus cache reuse;
7. rechecks that the live snapshot pointer is unchanged;
8. writes a report marked `publication_eligible: false`.

Failure leaves the diagnostic workspace and a `qualification-failed` marker.
Cache artifacts and receipts are atomic, so a rerun in a fresh work directory
reuses every transaction completed before a kill. Partial Parquets and JSON
receipts are never considered valid.

To vary concurrency, start separate processes with the intended Polars/Rayon
limits and distinct work/report paths, then compare their candidate artifacts
with `determinism-verify`. Vary the persisted workload profiles to cover 256,
512, and 1024 weekly topologies; canonical month and contribution identities
remain topology-independent.

## Promotion gate

Do not connect this path to production candidate readiness until all are true:

- multiple real rollovers pass for nlwiki, ptwiki, and frwiki;
- identical logical snapshots under different source names rebuild zero cache
  artifacts;
- an old historical correction falls back to the checkpoint immediately before
  the change and produces clean-build bytes;
- the cross-month weekly boundary fixture and a real rollover match clean
  builds;
- one-worker and production-worker runs have identical SHA-256 sets;
- qualified bucket topologies have identical final artifacts;
- killed baseline and candidate builds resume from authenticated cache units;
- measured cache retention fits the generation storage reserve;
- no failure changes the current generation or publication symlink.

After promotion, the existing family receipt and publication gate remain the
authority. Cross-snapshot caches only supply candidate computation inputs; they
never bypass artifact receipts, semantic validation, or atomic publication.
