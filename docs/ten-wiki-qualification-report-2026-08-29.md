# Ten-wiki Toolforge qualification report

**Report date:** 2026-08-29  
**Snapshot:** `2026-07`, with metric cutoff `2026-08`  
**Scope:** afwiki, arwiki, arzwiki, eswiki, hawiki, jawiki, swwiki,
viwiki, yowiki, and zhwiki  
**Envelope:** Toolforge Jobs Framework, 1 vCPU, 6 GiB memory, one source
worker, 256 stable page-week buckets  
**Result:** all ten isolated qualifications passed; public promotion is a
separate fail-closed production transaction  
**Machine-readable evidence:**
[`evidence/ten-wiki-qualification-2026-08-29.json`](evidence/ten-wiki-qualification-2026-08-29.json)

## Executive summary

The ten projects processed 131 Wikimedia History sources containing
548,301,953 history rows. They transferred 44,604,391,665 compressed bytes
(41.54 GiB) and completed in 29,821 sequential job-seconds (8h17m01s). That is
near the low end of the earlier 7.9–11.3 job-hour estimate despite including
Hausa and Yoruba as the two additional African-language projects.

The largest observed cgroup peak was Spanish at 2,925,805,568 bytes
(2.72 GiB, 45.4% of the limit), leaving 54.6% headroom. Every project exceeded
the required 25% headroom. The ten runs consumed 18,887 CPU-seconds and only
105 seconds of cgroup throttling in aggregate.

The clean retained metric output is much smaller than the earlier 15.3 GB
estimate: 2,105,548,770 bytes (1.96 GiB) before shared aggregate/browser/site
packaging. Page-week accounts for 2,086,045,196 bytes, or 99.1% of that total.
This means storage work should target page-week representation; further
compression of the aggregate metrics would have negligible impact.

## Per-wiki measurements

Bytes below are exact. Wall time is the complete isolated clean build,
including snapshot resolution, source-window ingestion, patrol, compute, and
semantic qualification.

| Wiki | Transfer | History rows | Wall time | CPU time | Peak memory | Headroom | Metric bytes | Page-week rows | Patrol rows |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| afwiki | 221,266,863 | 2,915,825 | 7m35s | 290s | 451,317,760 | 93.0% | 18,586,384 | 1,891,253 | 6,950 |
| arwiki | 5,774,093,608 | 75,937,882 | 1h03m12s | 2,347s | 2,078,683,136 | 67.7% | 370,675,390 | 48,711,721 | 12,905 |
| arzwiki | 847,902,158 | 13,153,626 | 14m42s | 585s | 986,173,440 | 84.7% | 87,230,766 | 11,722,221 | 6,255 |
| eswiki | 15,279,716,819 | 174,179,057 | 2h51m33s | 6,854s | 2,925,805,568 | 54.6% | 508,123,117 | 77,377,726 | 15,164 |
| hawiki | 64,348,919 | 890,058 | 3m31s | 153s | 210,268,160 | 96.7% | 7,204,105 | 392,844 | 2,877 |
| jawiki | 9,832,417,546 | 110,278,685 | 1h30m52s | 3,228s | 2,016,972,800 | 68.7% | 351,848,271 | 61,278,032 | 13,664 |
| swwiki | 127,175,500 | 1,579,525 | 5m45s | 207s | 346,075,136 | 94.6% | 11,599,775 | 1,185,265 | 5,445 |
| viwiki | 4,475,150,852 | 75,329,306 | 56m34s | 2,217s | 2,543,980,544 | 60.5% | 400,977,982 | 63,663,167 | 11,348 |
| yowiki | 47,607,172 | 624,758 | 4m40s | 199s | 392,671,232 | 93.9% | 6,087,511 | 484,996 | 3,597 |
| zhwiki | 7,934,712,228 | 93,413,231 | 1h18m37s | 2,808s | 2,055,237,632 | 68.1% | 343,215,469 | 46,052,015 | 16,236 |

Spanish is the largest measured transfer and history workload in this batch.
Its detailed stages were:

| Stage | Duration |
| --- | ---: |
| Snapshot resolution | 0.242s |
| Source window plus generation finalization | 2h15m12.208s |
| Patrol fetch and multi-member parse | 4m35.131s |
| Core compute | 25m07.753s |
| Patrol compute | 6m36.579s |
| Semantic qualification | 0.563s |

Spanish page-week staging reduced 303 monthly partitions to 78,362,453 staged
rows and published 77,377,726 reconciled rows. Its peak occurred during
bounded reconciliation/publication, not download. At the peak, most cgroup
usage was filesystem page cache while process RSS remained near 495 MB.

## Storage behavior

The source-window implementation was observed directly during Spanish:

1. Exactly one compressed source was present at a time.
2. A source was removed only after immutable Parquet fragments and a strict
   source marker committed.
3. Generation finalization temporarily overlapped source fragments and
   compacted runs; the complete metric-input tree reached about 6.97 GB.
4. Page-week scratch peaked around 1.48 GB and was reclaimed progressively;
   it fell below 100 MB before final reconciliation completed.
5. Every isolated workspace was deleted after success. The five large
   workspaces with exact before-delete measurements reclaimed at least
   12,303,229,494 bytes. Only checksummed compact evidence remains.

The ten retained Toolforge evidence bundles occupy 9,583,202 bytes. The
pre-promotion production baseline remained 375,018,192 bytes under `data/` and
about 4.74 GB under `output/`; qualification never changed the public site,
publication receipt, or live wiki symlinks.

## Qualification and promotion decision

All ten outputs passed the same semantic gates used by production: required
schemas, plausible non-zero rows, snapshot and cutoff identity, page-edit
conservation, page-week ordering and previous-week consistency, patrol rows,
artifact SHA-256 receipts, and hidden publication state.

The measured outputs support promotion under these controls:

- small-community row thresholds are explicit only where measured legitimate
  output is below the existing global floor;
- Arabic, Spanish, Japanese, Vietnamese, and Chinese retain the
  `medium_large` fleet resource class;
- all source inputs are marked redownloadable and purge after a ready
  candidate; computed metrics retain one rollback generation;
- production must rebuild from one exact attested promotion commit rather
  than publishing isolated qualification paths;
- the publisher must switch all selected data and the site atomically, then
  apply retention and verify public freshness.

## What improved

- **Storage is bounded by work in progress.** Clean transfer was 44.60 GB, but
  no run retained all compressed sources and clean metric retention is only
  2.11 GB.
- **Weekly aggregation is bounded.** The largest run retained more than 54%
  memory headroom; the old global hash aggregation would not fit this batch.
- **Failures are cheap to resume.** Hausa’s initial patrol cleanup failure was
  fixed and resumed the authenticated source generation in 492 ms instead of
  downloading and ingesting it again.
- **Project size does not dictate memory linearly.** Vietnamese peaked above
  Japanese and Chinese despite much lower transfer, validating measured
  workload profiles rather than wiki-name branches.

## Three new improvement opportunities

1. Add fragment/byte progress counters to generation finalization. Spanish
   spent roughly fourteen quiet CPU-active minutes there, which looked like a
   stall even though heartbeat and CPU advanced.
2. Add completed-month counters to patrol source-index rebuilds. The bounded
   226-month Spanish rebuild was healthy but emitted no progress until its
   final merge.
3. Qualify two compute threads for source decoding and page-week work. The
   large runs are strongly single-core CPU-bound and have more than 54% memory
   headroom, while HTTP source downloads should remain serialized until
   separate rate-limit evidence supports concurrency.
