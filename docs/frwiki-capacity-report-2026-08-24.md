# frwiki Toolforge capacity and production report

**Report date:** 2026-08-24

**Workload:** Complete `2026-07` frwiki history, followed by the three-wiki
production publication (`frwiki`, `nlwiki`, `ptwiki`)

**Platform envelope:** Wikimedia Toolforge Kubernetes Jobs Framework, 1 vCPU,
6 GiB memory, one Rayon thread, one Polars thread

**Result:** frwiki is qualified, published, healthy, and scheduled; 256 stable
buckets is the only configuration that passed the 25% memory-headroom gate

**Machine-readable evidence:**
[`evidence/frwiki-capacity-2026-08-24.json`](evidence/frwiki-capacity-2026-08-24.json)

## Executive summary and capacity request

frwiki now runs correctly on Toolforge, but the present 6 GiB allocation is a
minimum viable envelope rather than a comfortable long-term one. The isolated
256-bucket aggregation qualified twice with **36.1–38.2%** memory headroom.
The first complete production cutover nevertheless reached **5.61 GiB at its
last live sample (93.6% of the limit)** and was then OOM-killed when publication
validation began. After changing validation to scan projected 250,000-row
batches and explicitly dropping merged-file cache, the recovery run completed
with a **4.44 GiB cgroup peak (74.05%)**, leaving **25.95% headroom**—only 0.95
percentage points above the project's qualification floor.

The defensible resource request is:

- **8 GiB RAM** for the scheduled refresh job. At the measured post-fix peak,
  this provides 44.46% headroom instead of 25.95%. It also leaves room for
  monthly history growth, filesystem-cache variability, and validation without
  weakening the fail-closed publication checks.
- **2 vCPU** to shorten full-refresh and recovery windows, subject to a fresh
  qualification before raising Rayon or Polars above one thread. CPU-seconds
  were not retained in this test series, so the CPU request is supported by
  long single-thread wall times, not by a fabricated utilization percentage.
  The safest rollout is to allocate 2 vCPU while retaining one thread, add
  cgroup CPU telemetry, then qualify two-thread execution independently.
- **No immediate NFS expansion is required.** The selected benchmark estimates
  41.54 GiB of additional space for a safe generation rollover, and policy
  requires a further 50 GiB reserve. Toolforge exposed 1.93 TiB free after the
  successful run. Because this is shared filesystem capacity and no per-tool
  quota is exposed, the operational requirement is continued access to at
  least **91.54 GiB free** during rollover, not a claim of guaranteed quota.

The memory increase is the priority. The CPU increase improves turnaround but
needs one more instrumented experiment before claiming a specific speedup.

## Scope and evidence quality

This report combines three evidence classes:

1. **Four isolated capacity jobs** run in fresh 6 GiB/1-vCPU containers against
   the complete frwiki warehouse: 256, 512, 1024, then a deterministic 256
   repeat. These records contain exact cgroup peaks, durations, disk peaks,
   logical totals, and hashes.
2. **Two production cutover attempts:** the initial full computation that was
   OOM-killed during publication validation, and the bounded-validator recovery
   run that published successfully.
3. **Post-publication inspection:** strict ingest manifests, current storage,
   publication-gate totals, browser partition index, job configuration, and
   public freshness health.

The benchmark binary was commit
`b2e81b830fc13f43292704dffcde189501288397`. Production was deployed from
`5e3c4060e1e18eacb866d2232513e3e43dd59041`, binary SHA-256
`1c15838715f7d71f07cc079002e8f67920494ae7815d8ecc449e6bc2c904ba6c`.
Between those commits, the weekly aggregation implementation did not change;
the material production fix was the bounded publication validator. This keeps
the isolated aggregation comparison applicable while separately measuring the
fixed end-to-end path.

All times below are UTC. Bytes are exact; GiB values divide by 1,073,741,824.

## Current Toolforge configuration

| Property | Production value |
| --- | ---: |
| Scheduled job | `wiki-econ-refresh` |
| Schedule | Sunday at 03:00 UTC (`0 3 * * 0`) |
| Managed wikis | `frwiki`, `nlwiki`, `ptwiki` |
| Memory limit | 6,442,450,944 bytes (6.00 GiB) |
| CPU limit | 1.0 core |
| Rayon threads | 1 |
| Polars threads | 1 |
| Image | `tool-wiki-economics/tool-wiki-economics:latest` |
| Persistent mounts | All Toolforge mounts |
| Whole-job retry | Disabled by design |
| Selected weekly buckets | 256; wrapper rejects other values |
| Scratch | Configurable and cleaned by run ID/age |

The refresh processes wikis sequentially, so adding frwiki does not sum three
compute peaks concurrently. The shared merge and publication stages still
touch the combined output, which is why isolated frwiki qualification alone did
not reveal the first cutover's validator OOM.

## Dataset volume and coverage

The refresh resolver selected `2026-07`, the newest completed common snapshot
available on 2026-08-24. The generation pointer now names that snapshot.

| Input/output measure | frwiki value |
| --- | ---: |
| Compressed source files | 26 yearly files |
| Compressed source bytes | 21,327,005,470 (19.86 GiB) |
| Logical source edit rows | 237,798,382 |
| Analytical Parquet shards | 4,201 |
| Warehouse Parquet shards | 4,201 |
| Rows in each warehouse representation | 237,798,382 |
| Calendar-month partitions scanned by weekly aggregation | 301 |
| Weekly staged rows before cross-partition reconciliation | 121,372,514 |
| Published page-week rows | 119,855,668 |
| Conserved page-week edits | 236,874,141 |
| Weekly date range | 2001-05-28 through 2026-07-27 |
| frwiki page-week Parquet | 787,584,999 bytes (751.10 MiB) |

The source-row and page-week edit totals are intentionally reported separately:
they are conservation totals for different metric contracts. The GDP/labor
pipeline conserves all 237,798,382 source rows; page-week aggregation conserves
236,874,141 edits under its own eligibility rules. The publication gate checks
each contract independently.

### Published frwiki metrics

| Metric | Rows | Conservation total | Coverage | File bytes |
| --- | ---: | ---: | --- | ---: |
| `page_weekly_edits` | 119,855,668 | 236,874,141 edits | 2001-05-28–2026-07-27 | 787,584,999 |
| `gdp` | 17,384 | 237,798,382 edits | 2001-06–2026-08 | 566,429 |
| `gdp_activity_tiers` | 3,159 | 237,798,382 edits | 2001-06–2026-08 | 45,740 |
| `gdp_user_type_share` | 877 | 237,798,382 edits | 2001-06–2026-08 | 11,505 |
| `inequality` | 589 | 210,577,347 eligible edits | 2001-10–2026-08 | 21,337 |
| `labor_monthly` | 17,384 | 237,798,382 edits | 2001-06–2026-08 | 158,768 |
| `labor_churn` | 425 | n/a | 2001–2026-Q3 | 12,606 |
| `patrol` | 14,518 | 5,346,508 events | 2007-01–2026-08 | 214,148 |

Patrol parsing recognized **5,364,943 patrol events** and **4,460 rights
events** from the concatenated multi-member gzip source. This is a semantic
readiness check, not merely an existence check.

The public browser path does not ship the 751 MiB page-week dataset on initial
load. Rust generated nine frwiki aggregate partitions containing 54,713 rows
and 1,036,399 bytes total; the largest is 566,429 bytes. The complete browser
generation contains 43 partitions, 193,725 rows, and 3,608,763 bytes.

The aggregate metrics can extend into `2026-08` because the selected dump
contains bounded records from that month. The page-week cutoff remains
`2026-07-27`. This is expected and does not mean that an incomplete `2026-08`
snapshot was selected.

## Isolated page-week capacity qualification

Every variant read the same 301 partitions and produced the same logical
119,855,668 rows, 236,874,141 edits, and date range. The resource gate required
at least 25% headroom below 6 GiB.

| Buckets/run | Total time | Peak cgroup | Headroom | Largest bucket | Scratch peak | Result |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| 256 / first | 12m 04.916s | 3.709 GiB | 38.18% | 492,446 rows | 1.855 GiB | Pass |
| 512 | 14m 37.198s | 5.075 GiB | 15.41% | 250,580 rows | 2.002 GiB | **Fail** |
| 1024 | 19m 11.907s | 6.000 GiB | 0.00% | 129,941 rows | 2.233 GiB | **Fail** |
| 256 / repeat | 12m 14.559s | 3.835 GiB | 36.08% | 492,446 rows | 1.855 GiB | Pass |

Detailed phase timing:

| Buckets | Reduction | Reconciliation | Total | Relative to first 256 run |
| ---: | ---: | ---: | ---: | ---: |
| 256 first | 7m 41.411s | 4m 23.484s | 12m 04.916s | baseline |
| 512 | 9m 43.650s | 4m 53.521s | 14m 37.198s | 21.0% slower |
| 1024 | 13m 35.115s | 5m 36.758s | 19m 11.907s | 58.9% slower |
| 256 repeat | 7m 45.492s | 4m 29.048s | 12m 14.559s | 1.3% slower |

The first 256 run sustained approximately **167,430 staged rows/second** and
**165,337 output rows/second** under a one-core quota. The repeat produced the
same bytes with a 1.3% wall-time difference, normal for shared infrastructure.

The counter-intuitive result is operationally important: increasing bucket
count reduced each bucket's row count but increased aggregate state, open-file
and allocator overhead. Peak memory rose by 36.8% at 512 buckets and 61.8% at
1024 buckets relative to the first 256 run. More buckets were both slower and
less memory-efficient on this workload.

### Determinism

The two independent 256-bucket runs produced identical bytes:

```text
e2c40b00596a69a2f3f03959d80cc077bee3187124b1407ad96567a65bc8a299
```

The 512/1024 files have different physical hashes and sizes because bucket
layout affects Parquet encoding, but their logical row/edit/date totals match.
Production therefore pins 256 buckets, which gives both a resource-safe and
byte-deterministic artifact for a fixed snapshot and computation version.

## Production cutover and recovery

### Initial full run: OOM at the publication boundary

Run `20260824T055214Z-7` performed the first full Toolforge-backed frwiki
refresh. Its frwiki stages completed successfully:

| frwiki stage | Wall time |
| --- | ---: |
| Fetch/reuse check | 1m 04.068s |
| Patrol fetch and parse | 5m 30.016s |
| Ingest/reuse validation | 4.495s |
| Core compute | 10m 51.993s |
| Patrol compute | 1m 22.384s |

The page-week compute published the exact qualified result and reached
4,523,180,032 bytes (4.213 GiB, 70.21%) cgroup peak. The three-wiki merge then
wrote 201,959,946 page-week rows in 8m 12.828s and reached 5,311,934,464 bytes
(4.947 GiB, 82.45%). The last live run-status sample was 6,028,140,544 bytes
(5.614 GiB, 93.57%); the pod was OOM-killed after entering
`publication_validate`.

Because the cgroup killed the process, the shell exit trap could not write a
terminal history entry. The durable log ends at the validator start. The last
live sample above was observed before the status file was later replaced by the
successful retry; it should be treated as the last sampled value, not the
unobservable instantaneous allocation that triggered the kill.

Publication is fail-closed, so the failed run did not switch the public site.
The fix changed validation from a full lazy scan to projected 250,000-row
batches and advises the kernel to discard the large merged Parquet cache after
use. It preserves the same schemas, rows, conservation totals, and dates.

### Successful recovery: bounded validation and stage reuse

Run `20260824T065411Z-7` started at 06:54:11 and completed at 07:04:19 in
**608 seconds (10m 08s)**. It reused the valid snapshot ingest, per-wiki core
compute, patrol compute, and merge results from the first attempt, then ran the
fixed validator and built/published the site.

| Stage across all three wikis | Wall time | Share of run | Reused? |
| --- | ---: | ---: | --- |
| Patrol fetch/validation | 4m 54.647s | 48.5% | No |
| Publication validation | 2m 16.583s | 22.5% | No |
| Core fetch/reuse checks | 1m 56.926s | 19.2% | Yes |
| Site build and switch | 35.000s | 5.8% | No |
| Ingest/reuse checks | 8.130s | 1.3% | Yes |
| Patrol compute/reuse checks | 8.258s | 1.4% | Yes |
| Cleanup, resolution, verification, finalization | 3.320s | 0.5% | Mixed |
| Core compute/reuse checks | 1.190s | 0.2% | Yes |
| Merge/reuse check | 0.036s | <0.1% | Yes |

Peak cgroup use was **4,770,750,464 bytes (4.443 GiB, 74.05%)**. Memory at
exit was 1,216,200,704 bytes. The recovery therefore met the 25% requirement,
but only narrowly. Its published site generation was
`.site-dist.build.20260824T065411Z-7.Jeg6o2`.

## CPU consumption and throughput

The measured facts are:

- every qualification and production job had a hard **1.0-core** CPU quota;
- `RAYON_NUM_THREADS=1` and `POLARS_MAX_THREADS=1`, preventing the container's
  visible host CPUs from causing accidental oversubscription;
- the isolated selected aggregation required 724.916–734.559 wall seconds;
- the first production frwiki core compute required 651.993 seconds;
- the complete first frwiki stage sequence required about 18m 53s before the
  other wikis, shared merge, and publication validation;
- the successful incremental recovery required 608 seconds for all three
  wikis and publication.

The retained schema did **not** sample `cpu.stat`, CPU-seconds, throttled time,
or Kubernetes CPU utilization. Therefore this report does not claim “100% CPU
used” or predict a linear two-core speedup. Long one-thread reduction and
reconciliation phases make 2 vCPU a reasonable operational request, but the
next qualification should record:

```text
usage_usec, user_usec, system_usec, nr_periods, nr_throttled, throttled_usec
```

at stage boundaries and periodically during long stages. Two-thread mode
should be accepted only if it retains at least 25% memory headroom and produces
the same logical result and deterministic 256-bucket artifact.

## RAM profile

| Boundary | Cgroup bytes | GiB | % of 6 GiB | Headroom |
| --- | ---: | ---: | ---: | ---: |
| Isolated 256, first | 3,982,630,912 | 3.709 | 61.82% | 38.18% |
| Isolated 256, repeat | 4,117,860,352 | 3.835 | 63.92% | 36.08% |
| Production frwiki weekly compute | 4,523,180,032 | 4.213 | 70.21% | 29.79% |
| Successful recovery, whole job | 4,770,750,464 | 4.443 | 74.05% | 25.95% |
| Initial production merge | 5,311,934,464 | 4.947 | 82.45% | 17.55% |
| Initial run, last live sample | 6,028,140,544 | 5.614 | 93.57% | 6.43% |
| 1024-bucket qualification | 6,442,450,944 | 6.000 | 100.00% | 0.00% |

Rust RSS was only about 1.5–1.6 GiB in the selected isolated runs while cgroup
usage was 3.7–3.8 GiB. This gap is expected: the cgroup also charges mapped
Parquet pages, filesystem cache, native allocator state, and process/runtime
overhead. Capacity decisions must use cgroup peak, not Rust RSS alone.

At 8 GiB, the measured fixed production peak would consume 55.54%, leaving
44.46% headroom. This is enough to absorb meaningful monthly growth while
keeping the existing 75% warning and 80% critical alerts useful.

## Storage consumption

### Current persistent footprint after cleanup

| Area | Bytes | GiB |
| --- | ---: | ---: |
| frwiki analytical generation | 1,156,916,105 | 1.077 |
| frwiki warehouse generation | 7,376,653,715 | 6.870 |
| frwiki patrol sources | 1,304,542,525 | 1.215 |
| frwiki stage metadata | 8,835,417 | 0.008 |
| frwiki raw core dumps | 0 | 0.000 |
| frwiki published outputs | 790,392,545 | 0.736 |
| **frwiki retained total** | **10,637,340,307** | **9.907** |
| All-wiki pipeline data | 16,556,484,491 | 15.419 |
| All-wiki published output | 1,938,007,553 | 1.805 |

Core raw downloads are deleted after strict ingest validation. Capacity-job
scratch/output staging is removed on exit while compact JSON reports remain.

### Peak and rollover planning

For the selected 256-bucket run:

| Storage measure | Value |
| --- | ---: |
| Measured scratch peak | 1,991,832,123 bytes (1.855 GiB) |
| Measured scratch + output working peak | 1,994,940,222 bytes (1.858 GiB) |
| Current analytical + warehouse generation | 8,533,569,820 bytes (7.948 GiB) |
| Conservative raw transient allowance | 33,285,996,544 bytes (31.000 GiB) |
| Estimated additional rollover requirement | 44,598,983,486 bytes (41.536 GiB) |
| Required reserve after rollover | 53,687,091,200 bytes (50.000 GiB) |
| Minimum free space at rollover start | 98,286,074,686 bytes (91.536 GiB) |
| Free after successful production run | 2,097,798,971,392 bytes (1.908 TiB) |

The conservative rollover model is larger than the measured 19.86 GiB raw
download because it is a fail-before-download planning allowance, not a claim
about the current compressed snapshot. Generation-aware publication keeps the
old generation until the new generation passes compute, merge, validation, and
site publication; capacity must cover that transactional overlap.

## Correctness, resilience, and live status

The capacity conclusion is not based on “a file exists.” The successful run
passed:

- snapshot-generation validation for all scheduled wikis;
- schema, non-zero row, date-range, and conservation checks;
- patrol and rights-event readiness checks;
- current-run artifact provenance checks;
- deterministic stage fingerprint validation;
- browser partition index hashing;
- atomic site generation switch and post-switch verification.

At 07:28:35 UTC on 2026-08-24, the public freshness endpoint reported:

| Field | Value |
| --- | --- |
| Overall status | `healthy` |
| Active alerts | none |
| Last successful run | `20260824T065411Z-7` |
| Scheduled wikis | `frwiki`, `nlwiki`, `ptwiki` |
| Published frwiki snapshot | `2026-07` |
| Current state | `succeeded` |

The public endpoint is
<https://wiki-economics.toolforge.org/health/freshness.json>. Current values
will naturally change after later refreshes; the checked response is preserved
in the companion evidence JSON.

## Conclusions and next measurements

1. **frwiki is viable on Toolforge today with 256 buckets.** Two isolated runs
   passed, were byte-identical, and the fixed production pipeline published.
2. **6 GiB has little growth margin.** The fixed end-to-end job is only 0.95
   percentage points above the project's required headroom, and a real
   validator failure already demonstrated the operational consequence.
3. **8 GiB is a proportionate request.** It moves the observed fixed peak from
   74.05% to 55.54% of allocation without relying on speculative scaling.
4. **256 buckets must remain pinned.** 512 and 1024 reduced bucket size but
   increased memory, storage, and time; both failed the resource policy.
5. **2 vCPU is useful but should be evidence-gated.** Add CPU accounting first,
   then qualify one- versus two-thread runs under the requested memory limit.
6. **Storage is currently adequate but shared.** Preserve the generation-aware
   rollover gate requiring 91.54 GiB free; do not interpret shared free space as
   a guaranteed per-tool quota.

After an allocation increase, repeat the 256-bucket job twice at 1 and 2
threads, capture cgroup CPU and memory counters, run one complete three-wiki
refresh, and retain the resulting JSON beside this report. Acceptance remains:
identical semantics and bytes, at least 25% sustained memory headroom, safe
rollover reserve, successful fail-closed publication, and healthy freshness.

## Reproduction references

- Capacity method and commands: [benchmarking.md](benchmarking.md)
- Qualification and recovery procedure:
  [operations-recovery.md](operations-recovery.md)
- Toolforge job and deployment details:
  [Toolforge README](../deploy/toolforge/README.md)
- Capacity policy:
  [`config/capacity-qualification.json`](../config/capacity-qualification.json)
- SLO policy: [`config/operations-slos.json`](../config/operations-slos.json)
- Machine-readable snapshot:
  [`evidence/frwiki-capacity-2026-08-24.json`](evidence/frwiki-capacity-2026-08-24.json)

The original retained Toolforge records are under:

```text
/data/project/wiki-economics/capacity/reports/frwiki/
/data/project/wiki-economics/capacity/frwiki-qualification.json
/data/project/wiki-economics/output/.refresh-status.json
/data/project/wiki-economics/output/.refresh-history.jsonl
/data/project/wiki-economics/output/logs/refresh/
/data/project/wiki-economics/output/publication-gate.json
```
