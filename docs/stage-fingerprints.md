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
output/_ready-index/<wiki>.json
output/_stages/compute/<wiki>.json
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
- **compute** reuses only the exact selected generation, explicit Rust
  algorithm version, persisted adaptive workload profile, and complete recorded
  metric inventory. The profile records total compressed bytes, source count,
  prior measured rows, preferred source concurrency, and the two-level bucket
  layout. When possible compute's other input is the validated ingest receipt,
  avoiding a second multi-gigabyte hash pass.
- **patrol compute** includes the selected ingest generation and all three
  locally validated patrol inputs (`patrol.parquet`, `rights.parquet`, and the
  autopatrol-group metadata). A changed algorithm or input invalidates patrol
  without invalidating core metrics.
- **candidate discovery** validates `ready.json`, every artifact-receipt identity, and the
  current compute and patrol receipts. A complete hit is a recorded no-op. On
  a partial hit, only receipt-covered stage files are copied atomically into
  the new immutable candidate; invalidated stages alone execute.
- **merge** includes published per-wiki Parquets, lifecycle configuration,
  Rust dashboard code, and the manifest validator. On a hit it preserves the merged files but issues
  a new publication candidate for the current run ID.
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

Changing Rust logic without changing source data must increment the relevant
`*_ALGORITHM_VERSION` constant. CI embeds `github.sha` as
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
