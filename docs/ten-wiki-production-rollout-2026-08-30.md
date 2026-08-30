# Ten-wiki production rollout and optimization evaluation

**Report date:** 2026-08-30  
**Production scope:** afwiki, arwiki, arzwiki, eswiki, hawiki, jawiki,
swwiki, viwiki, yowiki, and zhwiki, added to elwiki, frwiki, itwiki, nlwiki,
ptwiki, and svwiki  
**Snapshot:** `2026-07`; published metric cutoff: `2026-08`  
**Platform:** Wikimedia Toolforge Jobs Framework; 1 vCPU per job; 2 GiB small
workers and 6 GiB medium/publisher jobs  
**Result:** all 16 wikis published, fleet scheduler active, public freshness
healthy, and steady-state publication reduced to a five-second no-op  
**Machine-readable evidence:**
[`evidence/ten-wiki-production-2026-08-30.json`](evidence/ten-wiki-production-2026-08-30.json)

The attested deployment identity for the final dashboard correction is the Git
commit containing this report. Binary checksum, image source commit, and image
digest are independently verified by the Toolforge deployment scripts.

## Executive result

The fleet prepared and published ten new projects without changing the six
existing projects' source snapshot. The publication contains 1,152,880,965
history edits and 644,072,684 page-week rows across 16 wikis. All selected
snapshots are `2026-07`, all per-wiki cutoffs are `2026-08`, and patrol source
receipts contain non-zero recognized events.

The first 16-wiki publication was a migration publication: 50 new
wiki/metric-family combinations changed while 30 existing combinations were
reused. It completed in 1,942 seconds. The immediately repeated publication
proved the authoritative-receipt fast path in five wall-clock seconds, of
which the Rust publication stage used 2,313 ms. That is approximately 794
times faster at the stage level and used 25,931,776 bytes peak memory instead
of 1,163,792,384 bytes, approximately 45 times less.

## Production preparation measurements

These are real fleet runs on Toolforge, not estimates. Wall time includes the
production preparation wrapper; CPU and memory are cgroup measurements.

| Wiki | Wall time | CPU time | Peak memory | 6 GiB headroom |
| --- | ---: | ---: | ---: | ---: |
| afwiki | 8m10s | 340s | 429,928,448 B | 93.3% |
| hawiki | 4m09s | 159s | 409,153,536 B | 93.6% |
| swwiki | 7m19s | 315s | 542,277,632 B | 91.6% |
| yowiki | 3m53s | 163s | 356,614,144 B | 94.5% |
| arzwiki | 15m35s | 777s | 881,471,488 B | 86.3% |
| arwiki | 1h12m52s | 2,921s | 2,340,966,400 B | 63.7% |
| jawiki | 1h49m29s | 4,320s | 2,332,098,560 B | 63.8% |
| eswiki | 2h40m49s | 6,308s | 2,467,278,848 B | 61.7% |
| viwiki | 1h08m22s | 3,010s | 2,316,734,464 B | 64.0% |
| zhwiki | 1h33m07s | 3,796s | 2,436,825,088 B | 62.2% |

Every run retained substantially more than the required 25% memory headroom.
The largest production peak was Spanish at 2.47 GB, 38.3% of the 6 GiB job
limit. The clean isolated qualification numbers and source-transfer totals are
reported separately in
[`ten-wiki-qualification-report-2026-08-29.md`](ten-wiki-qualification-report-2026-08-29.md).

## Publication and browser performance

| Measurement | Migration publication | Unchanged publication |
| --- | ---: | ---: |
| Wrapper wall time | 1,942s | 5s |
| Publication preparation | 1,837,253ms | 2,313ms |
| Site build | 69,000ms | skipped |
| Verification | 75ms | skipped |
| Commit | 4,272ms | skipped |
| Peak memory | 1,163,792,384 B | 25,931,776 B |
| CPU time | 1,865.9s | 2.36s |
| Result | committed | authenticated no-op |

The browser index is schema 3 with 378 partitions, including 234 global
annual shards. It contains 703,790 rows and 36,543,476 bytes; the largest
single shard is 1,903,839 bytes. Page-week remains per-wiki and no redundant
root combined page-week Parquet exists.

The canonical landing URL is:

`/inequality?wiki=all&types=registered&gran=year&start=2001-06&end=2026-08`

The Rust dashboard generator now records the exact precomputed default range
separately from the wider queryable metric range. Real offline browser tests
show that the canonical default requests no browser index, Parquet, Apache
Arrow, or WebAssembly. A custom all-wiki selection requests only annual
`all-YYYY.parquet` shards overlapping the selected range.

## Storage after promotion

| Area | Exact bytes |
| --- | ---: |
| Retained source/input state (`data/`) | 451,788,402 |
| Published and rollback metrics (`output/`) | 7,000,106,800 |
| Published site generation | 46,514,585 |
| Data plus output | 7,451,895,202 |

At capture time Toolforge reported 2,037,068,595,200 bytes free on the shared
filesystem, far above the 50 GiB operational reserve. Generation metadata
reported 16 published states, six superseded rollback states, five retired
states, and eight interrupted historical building records. Cleanup is
lifecycle-driven and runs before preparation; it does not delete candidates
by filename. Exactly one rollback candidate is retained for each of the six
previously published wikis, while newly introduced wikis have no older public
generation to retain.

## Operational behavior demonstrated

- A Rust/Node workload-profile schema mismatch caused two publisher attempts
  to fail closed and roll back; the previous public site stayed healthy.
- The corrected profile reader accepts authenticated schema-v1 and schema-v2
  records but rejects mismatched schema/algorithm pairs.
- A later authenticated no-op resolves a transient migration-duration alert
  without discarding semantic or resource evidence from the last real
  publication.
- The fixed fleet consists of one controller, two small workers, one
  medium/large worker, one publisher, and one scrubber. Enwiki has no worker
  and remains publication-ineligible.
- Public health returned `healthy` with no alerts after rollout.

## What improved

1. **Unchanged runs are genuinely cheap.** Receipt composition avoids
   rehashing or decoding unchanged weekly artifacts; the publisher fell from
   1,837 seconds of migration work to 2.313 seconds.
2. **Scale no longer multiplies browser startup.** The instant default uses
   Rust JSON, while custom queries download only one wiki or the overlapping
   global annual shards.
3. **Storage follows lifecycle state.** Redownloadable inputs are purged after
   authenticated readiness; bounded scratch is reclaimed progressively; one
   rollback generation is retained only where one exists.
4. **Failures do not leak into publication.** Candidate preparation,
   publication selection, site staging, and live switching are separate
   transactions with recovery evidence.
5. **The scheduler is fixed-size.** Six job definitions now represent 16
   managed wikis and can represent a much larger fleet without one scheduled
   Toolforge job per wiki.

## Three new lessons

1. **Cross-language schemas need one generated contract.** Rust correctly
   emitted workload profile v2, but the Node publication generator initially
   accepted only v1. Shared generated schemas or producer fixtures should be
   consumed by every language boundary.
2. **Production URL network budgets belong in CI.** Unit and fixture tests
   passed while the canonical `2001-06` URL missed the precomputed default
   because the wider metric range began in `2001-05`. Browser acceptance must
   assert request classes for the exact public landing URL.
3. **Long receipt migrations need progress observability.** The migration was
   healthy and memory-safe but spent about 30 minutes authenticating legacy
   artifacts. Per-family receipt-backfill counters would distinguish useful
   sequential I/O from a stall and improve remaining-time estimates.

## Acceptance checklist

- [x] Sixteen published wikis at snapshot `2026-07` and cutoff `2026-08`.
- [x] Public freshness healthy with no alerts.
- [x] All live data and the site come from one committed publication run.
- [x] Exactly the six allowlisted fleet/scrub/publisher jobs are scheduled.
- [x] No enwiki work is claimable.
- [x] Same-snapshot publication is a five-second no-op.
- [x] Default browser view loads no query data or query runtime.
- [x] Custom all-wiki query loads only overlapping global annual shards.
- [x] Storage remains far above the 50 GiB reserve.
- [x] Complete local Rust, Node, offline reproducibility, browser budget,
  licensing, advisory, SBOM, and 100% line-coverage gates pass.

