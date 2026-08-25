# Metric-input schema qualification — 2026-08-25

## Decision

The 13-column `qualified-metric-input-v1` schema is qualified for future snapshot generations. It preserves every input currently consumed by core metrics, page-week aggregation, and patrol reconciliation while deriving calendar keys and user classification during bounded reads.

Migration must be generation-versioned. Existing schema-v1 generations remain readable and immutable; a future snapshot is written as a schema-v2 generation with one metric-input layer, validated end to end, and only then published. This avoids an in-place rewrite of the active rollback generation.

## Production qualification

The benchmark ran on Toolforge against the active 2026-07 nlwiki, ptwiki, and frwiki generations under a 6 GiB cgroup. It projected one immutable fragment at a time, validated row conservation and each Parquet footer, measured the output, and immediately deleted the temporary fragment.

| Wiki | Rows | Existing two layers | Metric-input projection | Saving | Saving % | Elapsed | Peak cgroup memory |
|---|---:|---:|---:|---:|---:|---:|---:|
| nlwiki | 71,518,536 | 2,355,882,105 B | 1,415,821,775 B | 940,060,330 B | 39.90% | 2m 10s | 39,342,080 B |
| ptwiki | 72,553,673 | 2,533,150,749 B | 1,509,890,148 B | 1,023,260,601 B | 40.39% | 2m 09s | 42,086,400 B |
| frwiki | 237,798,382 | 8,533,569,820 B | 4,955,746,407 B | 3,577,823,413 B | 41.93% | 7m 48s | 514,822,144 B |
| **Total** | **381,870,591** | **13,422,602,674 B** | **7,881,458,330 B** | **5,541,144,344 B (5.16 GiB)** | **41.28%** | **12m 07s** | **514,822,144 B max** |

The largest temporary fragment was 1,646,023 bytes. The scratch directory was empty at completion. The job exited successfully at 2026-08-25T12:37:30Z.

## Provenance note

The immutable Toolforge job definition and deployed binary identify this run as `schema-b8ab1b7` from commit `b8ab1b7f6331e09e40905b85de16f0d542082835`, using image digest `sha256:fb9c69b998f19651f8e9a1dbb2b4911397cae035fecd73dcadda83083d2d18da`.

The raw schema-v1 report recorded `source_commit` and `run_id` as null because direct CLI invocation supplied `--run-id` without exporting the wrapper environment variables. The evidence JSON is retained byte-for-byte rather than silently rewritten. The benchmark now receives CLI provenance explicitly and falls back to the compile-time commit, preventing recurrence.

Raw evidence: [metric-input-schema-qualification-2026-08-25.json](evidence/metric-input-schema-qualification-2026-08-25.json).

## Rollout and expected effect

The change reduces persistent generation storage by about 40–42% for the qualified wikis. It also halves ingest writes: each normalized row is compressed once rather than once in the 28-column warehouse and again in the 10-column analytical layer.

The saving appears when a wiki builds and publishes its next snapshot generation. No active 2026-07 generation should be rewritten merely to realize the saving. After successful publication and rollback validation, lifecycle cleanup retires the superseded two-layer generation.

Qualification gates for rollout are:

1. identical metric rows, edit totals, date ranges, and deterministic output hashes between schema-v1 and schema-v2 fixtures;
2. strict schema-v2 marker and generation-manifest validation;
3. successful candidate compute, patrol, merge, site validation, and atomic publication;
4. no regression in memory or scratch budgets; and
5. rollback continues to read the preceding schema-v1 generation.
