# Deterministic Stage Fingerprints

The refresh pipeline uses content-addressed Rust receipts to avoid repeating
work whose inputs, computation contract, and outputs are unchanged. This is a
reuse mechanism, not a best-effort cache: any missing, changed, malformed, or
version-incompatible receipt causes the stage to run normally.

Receipts are stored outside published artifacts:

```text
data/stages/<wiki>/<snapshot>/fetch.json
data/stages/<wiki>/<snapshot>/ingest.json
output/_stages/compute/<wiki>.json
output/_stages/merge.json
output/_stages/site.json
```

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
- **ingest** reuses only a receipt whose complete output generation still
  validates. It then atomically selects that generation again.
- **compute** reuses only the exact selected generation, explicit Rust
  algorithm version, and complete recorded metric inventory. When possible its
  input is the validated ingest receipt, avoiding a second multi-gigabyte hash
  pass.
- **merge** includes published per-wiki Parquets, lifecycle configuration,
  Rust dashboard code, and the manifest validator. On a hit it preserves the merged files but issues
  a new publication candidate for the current run ID.
- **site** includes the publication candidate's artifacts plus the Observable
  sources/configuration. Reuse runs only inside the fail-closed publication
  flow, after the current run receipt is verified.

Changing Rust logic without changing source data must increment the relevant
`*_ALGORITHM_VERSION` constant. CI embeds `github.sha` as
`WIKI_ECON_BUILD_COMMIT`; manual builds remain deterministic because the
explicit algorithm version is always present.

## Snapshot Resolution

When `fetch` or `run` has no explicit `--version`, the binary probes Wikimedia
once at run start and pins the newest complete snapshot common to every
requested wiki. It starts with the preceding UTC month and falls back only
within `WIKI_ECON_MAX_SNAPSHOT_LAG_MONTHS` (default: `2`). A fallback emits a
warning; finding nothing within the bound fails the run.

A newly completed monthly snapshot therefore changes the pinned version and
invalidates fetch, ingest, compute, merge, and site in order. Repeated weekly
runs against the same snapshot become no-ops except for independently updated
patrol data and publication validation.
