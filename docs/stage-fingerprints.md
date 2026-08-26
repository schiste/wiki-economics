# Deterministic Stage Fingerprints

The refresh pipeline uses content-addressed Rust receipts to avoid repeating
work whose inputs, computation contract, and outputs are unchanged. This is a
reuse mechanism, not a best-effort cache: any missing, changed, malformed, or
version-incompatible receipt causes the stage to run normally.

Receipts are stored outside published artifacts:

```text
data/stages/<wiki>/<snapshot>/fetch.json
data/stages/<wiki>/<snapshot>/ingest.json
data/snapshots/<wiki>/<snapshot>/remote-inventory.json
data/snapshots/<wiki>/<snapshot>/workload-profile.json
data/incremental/month-identities/<wiki>/<snapshot>/inventory.json
data/incremental/metric-cache/<wiki>/<kind>/<algorithm-hash>/<input-digest>/...
output/_ready-index/<wiki>.json
output/_stages/compute/monthly/<wiki>.json
output/_stages/compute/activity_tiers/<wiki>.json
output/_stages/compute/lifecycle/<wiki>.json
output/_stages/compute/page_week/<wiki>.json
output/_stages/patrol_compute/<wiki>.json
output/_stages/merge.json
output/_stages/site.json
```

Every Parquet emitted by compute, patrol compute, or merge also has an adjacent
`<artifact>.receipt.json`. This is the authoritative semantic receipt for that
artifact, not another stage cache. It contains the artifact SHA-256 and byte
count, exact Parquet fields, rows, date/wiki bounds, additive conservation
totals, ordering contract, algorithm version, and input fingerprint. The
receipt body has its own canonical SHA-256.

Writers accumulate those semantic fields from the batches already passing
through them. After the Parquet is closed and synced, the stage recorder makes
one sequential SHA-256 pass, publishes the receipt atomically, and removes the
short-lived semantic draft. Legacy artifacts are migrated by one bounded
semantic scan; later layers consume the receipt and do not rediscover rows or
dates from Parquet.

Each receipt records the selected snapshot, sorted logical input identities,
an explicit stage algorithm version, the Cargo computation version, the build
commit when supplied, and a deterministic fingerprint. The commit is retained
as provenance but is not a cache key: unrelated repository commits must not
force a core recomputation. File records include
SHA-256, bytes, Parquet schema and row count, and a minimum/maximum date when a
known date column exists. Filesystem modification time is recorded only as a
fast validation index and is deliberately excluded from the fingerprint.

## Reuse Rules

- **fetch** derives the complete source list first. A source already represented
  by a valid generation-scoped ingest marker is never probed for disk headroom
  or downloaded. Missing/invalid sources alone are fetched.
- **ingest** authenticates the immutable generation manifest through its stage
  receipt. Same-snapshot reuse validates only the small source plan and
  manifest identities, then consumes the manifest allowlist from a process-local
  `(data root, wiki, snapshot, manifest hash)` cache. It does not reconstruct
  the manifest, hash fragments, or reopen Parquet footers. Strict physical
  validation remains the compatibility, recovery, and scrub path.
- **compute** has four independently invalidated receipts: monthly stateless
  aggregates, activity tiers, stateful editor lifecycle, and page-week. All
  four bind the exact selected generation and their own Rust algorithm version;
  only page-week additionally binds the persisted adaptive workload profile
  and two-level bucket topology. When multiple nonweekly families are invalid,
  one `ComputePlan` scan feeds their accumulators together. Changing an
  activity threshold therefore cannot touch page-week, and changing page-week
  topology cannot touch GDP. A valid legacy `core-metrics-v8` receipt is split
  once by authenticating compatible outputs against their artifact receipts;
  rollout does not decode or re-hash activity, lifecycle, or page-week
  Parquets. Monthly outputs rebuild once because the family split also added a
  deterministic `user_type` tie-break order to GDP and labor monthly.
- **patrol compute** includes the selected ingest generation and all three
  locally validated patrol inputs (`patrol.parquet`, `rights.parquet`, and the
  autopatrol-group metadata). A changed algorithm or input invalidates patrol
  without invalidating core metrics.
- **candidate discovery** validates `ready.json`, every artifact-receipt identity, and the
  current compute and patrol receipts. A complete hit is a recorded no-op. On
  a partial hit, only receipt-covered stage files are copied atomically into
  the new immutable candidate; invalidated stages alone execute.
- **merge** gives every root metric its own sorted per-wiki input fingerprint
  and stage receipt. Only a metric whose wiki-run identities changed is
  rewritten; other root Parquets remain untouched. The small dashboard and
  manifest outputs retain a separate global orchestration receipt. A hit still
  issues a publication candidate for the current run ID.
- **site** includes the publication candidate's artifacts plus the Observable
  sources/configuration. Reuse runs only inside the fail-closed publication
  flow, after the current run receipt is verified.
- **publication** reads one atomic `_ready-index/<wiki>.json` per managed wiki.
  The index binds the newest ready and active published candidates to their
  ready-receipt hashes, core/patrol artifact-receipt identities, and workload
  profile. Directory discovery is used only to rebuild a missing or invalid
  index. If the sorted active ready hashes, lifecycle hash, merge/publication
  versions, and site-source fingerprint match `publication-gate.json`, the
  publisher records `no_op` before merge, Parquet validation, or site build.
  Otherwise gate schema 8 compares per-wiki family receipt identities and
  algorithm versions with the previous publication and authenticates unchanged
  evidence without reopening its Parquet rows.

Changing Rust logic without changing source data must increment the relevant
family `ALGORITHM_VERSION` constant. `scripts/check-compute-versions.cjs`
maps compute source paths to those constants and fails CI when semantic code
changes without a version bump. A refactor proven not to affect semantics may
instead add an exact, reviewed entry to
`config/compute-no-semantic-change.json`; wildcard exceptions are not accepted.
CI embeds `github.sha` as
`WIKI_ECON_BUILD_COMMIT`; manual builds remain deterministic because the
explicit algorithm version is always present.

## Snapshot Resolution

When `fetch` or `run` has no explicit `--version`, the binary resolves Wikimedia
once at run start and pins the newest complete snapshot common to every
requested wiki. A successful completeness pass atomically records a separate
`remote-inventory.json` containing the plan hash, remote sizes, range support,
ETag/Last-Modified values when supplied, and check time. Later triggers reuse
that immutable inventory; an incomplete newest month is tested from the newest
source backward, so enwiki does not repeat hundreds of historical HEAD probes.
Resolution starts with the preceding UTC month and falls back only
within `WIKI_ECON_MAX_SNAPSHOT_LAG_MONTHS` (default: `2`). A fallback emits a
warning; finding nothing within the bound fails the run.

A newly completed monthly snapshot therefore changes the pinned version and
invalidates the complete historical source generation. The pipeline does not
derive synthetic deltas from two Wikimedia history snapshots: suppressions,
corrections, and historical changes must remain observable. Repeated weekly
preparation runs against the same snapshot validate the current and ready
candidate fingerprints and exit as recorded no-ops when they are unchanged.
An explicit algorithm-version change can invalidate compute or patrol alone.

Schema-v3 ingest additionally records a logical identity for every event
month. The identity is computed by a bounded k-way merge of the compacted,
sorted 13-column runs and hashes a canonical binary row encoding. It is
independent of source filenames, fragment boundaries, worker completion order,
and Parquet metadata. Cross-snapshot qualification uses these identities for
content-addressed stateless month outputs, editor-month inputs, weekly month
contributions, complete lifecycle outputs, and prefix-authenticated yearly
lifecycle checkpoints. These caches are not publication receipts and cannot
make a candidate eligible by themselves; exact candidate-vs-clean artifact and
semantic receipt equality is mandatory.

## Corruption checks

The fast path always validates the canonical receipt hash. Matching artifact
size and mtime allow it to reuse the recorded content hash; either metadata
change triggers a byte hash. Ready candidates are immutable: compute and patrol
refuse to write after `ready.json` or `qualification.json` exists.

A monthly Toolforge job independently rehashes every published Parquet even
when metadata is unchanged:

```sh
wiki-econ --output-dir "$WIKI_ECON_OUTPUT_DIR" \
  artifact-scrub --report "$WIKI_ECON_OUTPUT_DIR/_scrubs/manual.json"
```

Any missing sidecar, modified receipt, or artifact/receipt mismatch fails the
scrub and leaves the published generation untouched.
