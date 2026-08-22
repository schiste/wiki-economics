# Publication gate

Scheduled and script-driven refreshes publish only after Rust validates the
semantic data contract. File existence alone is not a success signal.

## Transaction model

`scripts/refresh.sh` assigns one `WIKI_ECON_RUN_ID` and passes it to every
`wiki-econ` command. The pipeline then creates three run-scoped records in the
output directory:

- `.publication-run.json` records the run ID, selected refresh wikis, and
  requested snapshot version before work begins.
- `.publication-candidate.json` is written only after every merged Parquet and
  every critical dashboard JSON generator succeeds. It inventories file sizes
  and modification timestamps for the exact artifact set produced by merge.
- `publication-gate.json` is the public operator receipt. It is written only
  after semantic validation passes and includes snapshot versions, per-wiki
  cutoffs, metric row and conservation totals, patrol source counts, and the
  candidate artifact inventory.

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
- non-empty JSON objects from every critical dashboard generator; and
- unchanged size/modification metadata from candidate creation through site
  publication.

The gate scans only the columns needed for aggregates. In particular, weekly
edit validation does not load page titles or build a page-sized hash table.

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
Do not copy an old receipt forward or edit its run ID; fix the failing source,
rerun merge/validation, and let the pipeline issue a new receipt.

## Changing dataset coverage

`publication_contract.datasets` in `config/wiki-lifecycle.json` is the
authoritative applicability and plausibility policy. Core metrics use
`coverage: "all_published"`; specialized metrics list their supported wikis.
Each dataset also declares `minimum_rows_per_wiki`.

When enabling a new wiki or metric, update the contract in the same change and
choose a minimum that detects gross truncation without encoding an exact row
count. Rust requires the configured dataset names to match its compiled schema
contracts, so adding or removing a metric is an explicit code-and-config
change.
