# Publication gate

Scheduled and script-driven refreshes publish only after Rust validates the
semantic data contract. File existence alone is not a success signal.

## Transaction model

`scripts/refresh.sh` assigns one `WIKI_ECON_RUN_ID` and passes it to every
`wiki-econ` command. The pipeline then creates three run-scoped records in the
output directory:

- `.publication-run.json` records the run ID, selected refresh wikis, and
  requested snapshot version before work begins.
- `.publication-candidate.json` is written only after every merged Parquet,
  Rust dashboard artifact, and the critical manifest validator succeed. Schema
  3 inventories SHA-256 identities for every artifact and the canonical
  artifact-receipt hash for each Parquet; size and mtime are retained only as a
  fast corruption index.
- `publication-gate.json` is the public operator receipt. It is written only
  after semantic validation passes and includes snapshot versions, per-wiki
  cutoffs, metric row and conservation totals, patrol source counts, and the
  candidate artifact inventory. Receipt schema 8 also records per-wiki metric
  family proofs and the exact changed/reused publication plan. It records the MIT SPDX
  identifier on every artifact plus the generating commit, run ID, source
  datasets, attribution, trademark status, privacy notice, and Toolforge open
  source/open data declaration.

An older receipt cannot authorize a new run because all three records must
contain the same run ID. `scripts/build-site.sh` verifies the receipt before
the Observable build and again immediately before switching the stable site
symlink. If an artifact changes during the build, publication stops and the
currently served site remains untouched.

## Validated contract

The Rust gate validates:

- the selected generation pointer for every scheduled wiki and the snapshot
  version requested by the full run;
- the complete published-wiki and per-dataset coverage declared in
  `config/wiki-lifecycle.json`;
- exact required Parquet column names and types;
- conservative minimum row counts for every expected wiki;
- root row counts against the sum of per-wiki inputs;
- additive conservation totals, including `page_weekly_edits.edits`;
- parseable minimum and maximum dates;
- paused imports against `imported_cutoff` and scheduled cutoffs against the
  selected snapshot (at most two monthly boundaries behind);
- snapshot-pointer freshness against `freshness_sla_days`;
- non-zero patrol and rights source rows for scheduled patrol datasets;
- the exact root metric set, rejecting missing and unexpected stale Parquets;
- non-empty Rust dashboard JSON objects plus a valid publication manifest; and
- a valid canonical semantic receipt for every Parquet, with unchanged
  artifact/receipt pairing from candidate readiness through site publication.

The gate consumes semantic receipts and does not reopen unchanged Parquets to
rediscover schemas, rows, dates, wiki ranges, or conservation totals. For each
publication it compares the current per-wiki family proofs with the preceding
gate, writes the deterministic change plan to
`publication-change-plan.json`, and reuses the preceding gate report for an
unchanged `wiki × metric family`. Changed artifacts receive a content-hash
verification; unchanged artifacts receive receipt and metadata verification.
Page-week
receipts validate edit conservation, stable ordering, and
`previous_week_edits` while bounded reconciliation batches pass through the
writer. A first migration of a legacy artifact performs one sequential scan.

The monthly `artifact-scrub` remains independent of this fast path. It
sequentially rereads every published Parquet, recomputes all semantic evidence
and SHA-256 identities, and requires exact equality with the authoritative
receipt. A failure is persisted in `_scrubs/status.json`, appears as a critical
alert at `/health/freshness.json`, and blocks later publication until a
successful scrub replaces the failed status.

## Operator checks

The normal refresh invokes validation automatically. To inspect or recheck a
specific run manually:

```sh
wiki-econ --data-dir "$WIKI_ECON_DATA_DIR" \
  --output-dir "$WIKI_ECON_OUTPUT_DIR" \
  --run-id "$WIKI_ECON_RUN_ID" publication-validate

wiki-econ --output-dir "$WIKI_ECON_OUTPUT_DIR" \
  --run-id "$WIKI_ECON_RUN_ID" publication-verify
```

On Toolforge, `.refresh-status.json` and `.refresh-history.jsonl` include the
same `runId`, so a failed job can be correlated with the candidate or receipt.
The live status copies validated row/edit/date summaries only from a gate whose
run ID matches; see the [refresh run record](run-record.md).
Do not copy an old receipt forward or edit its run ID; fix the failing source,
rerun merge/validation, and let the pipeline issue a new receipt.

## Changing dataset coverage

`publication_contract.datasets` in `config/wiki-lifecycle.json` is the
authoritative applicability and plausibility policy. Core metrics use
`coverage: "all_published"`; specialized metrics list their supported wikis.
Each dataset also declares `minimum_rows_per_wiki`. When differently sized
wikis share a metric, `minimum_rows_by_wiki` may provide explicit measured
overrides without weakening the default gate for every other wiki.

When enabling a new wiki or metric, update the contract in the same change and
choose a minimum that detects gross truncation without encoding an exact row
count. Rust requires the configured dataset names to match its compiled schema
contracts, so adding or removing a metric is an explicit code-and-config
change.
