# itwiki, svwiki, and elwiki Toolforge promotion report

**Report date:** 2026-08-25  
**Promotion snapshot:** `2026-07`  
**Toolforge job limits:** 1 CPU, 6 GiB memory per job  
**Promotion policy commit:** `cdd3958210c48a85b477f4d8e527e6a0429bb92b`  
**Production runtime commit:** `db7000dd73fcf57ba9a8f290de99d15c5c5a4fd9`  
**Final operations commit:** `e127f740fa756c4d204567d70e4c5c9ce3b7080b`
**Evidence:** [`evidence/wiki-promotion-2026-08-25.json`](evidence/wiki-promotion-2026-08-25.json)

## Outcome

This report records the capacity qualification and production promotion of
itwiki, svwiki, and elwiki from hidden or paused imported datasets to normal,
generation-aware Toolforge processing. The promotion is deliberately split
into isolated per-wiki preparation followed by one fail-closed atomic
publication. A wiki is not considered live or scheduled merely because its
qualification succeeded.

The atomic six-wiki release passed its semantic gate and public freshness
validation. itwiki, svwiki, and elwiki are published at snapshot `2026-07`
alongside frwiki, nlwiki, and ptwiki. Their recurring schedules are activated
by the follow-up operations commit recorded in the production section.

## Qualification summary

All three wikis completed an isolated, publication-invisible qualification
against Wikimedia's completed `2026-07` history snapshot. Each run used the
production small workload profile: one active source worker, 32 primary by 8
secondary stable buckets (256 logical buckets), one Polars/Rayon thread, and a
hard 6 GiB cgroup memory ceiling.

| Wiki | Source layout | Compressed input | Revisions conserved | Weekly rows | Page-week Parquet | Peak RAM | 6 GiB used | Headroom | Largest bucket |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| itwiki | 26 yearly sources | 12,686,676,489 B (11.82 GiB) | 151,551,300 | 80,088,117 | 495,582,883 B | 2,546,249,728 B (2.37 GiB) | 39.52% | 60.48% | 332,714 rows |
| svwiki | 26 yearly sources | 4,636,133,895 B (4.32 GiB) | 59,428,767 | 43,282,560 | 286,811,791 B | 1,519,091,712 B (1.41 GiB) | 23.58% | 76.42% | 177,034 rows |
| elwiki | 1 all-time source | 958,230,899 B (913.84 MiB) | 11,728,354 | 5,982,821 | 47,177,496 B | 914,411,520 B (872.05 MiB) | 14.19% | 85.81% | 27,442 rows |

The required sustained memory headroom is 25%. All three runs passed. itwiki,
the largest, retained 60.48% headroom, or 35.48 percentage points more than the
qualification floor.

## Time and throughput

| Wiki | End-to-end qualification | Source window | Download | Download throughput | Ingest | Ingest throughput | Core compute | Patrol compute | Semantic validation |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| itwiki | 30m16s resumed run | 3m13s reused | 0s reused | n/a | 0s reused | n/a | 19m26s | 6m06s | 27.6s |
| svwiki | 57m55s | 43m25s | 17m57s | 4.30 MB/s | 24m31s | 40,399 rows/s | 10m59s | 2m08s | 14.6s |
| elwiki | 17m14s | 9m44s | 3m28s | 4.60 MB/s | 6m02s | 32,380 rows/s | 6m29s | 29.7s | 5.7s |

itwiki's first source-window attempt had already completed all 26 strict
source transactions before a historical null-page identity exposed a compute
bug. After the Rust fix, the same candidate safely reused all 26 source
manifests: source handling fell from 1h48m31s to 3m13s, a **33.7x speed-up and
97.0% reduction**, with zero redownload and zero reingest. That is a measured
recovery benefit of source-window execution rather than an estimate.

For completeness, the original itwiki source pass downloaded 11.82 GiB in
47m46s at 4.43 MB/s and ingested 151,551,300 revisions in 58m22s at 43,275
rows/s.

## CPU behavior

The cgroup CPU accounting below includes the complete final qualification job.
The one-core limit makes CPU usage directly comparable with wall time, while
throttled time identifies short bursts above the quota.

| Wiki | CPU usage | User CPU | System CPU | Throttled time | Throttled periods / periods |
| --- | ---: | ---: | ---: | ---: | ---: |
| itwiki | 1,482.14 CPU-s | 1,316.42 CPU-s | 165.72 CPU-s | 11.63s | 1,478 / 18,144 |
| svwiki | 2,216.84 CPU-s | 2,017.69 CPU-s | 199.15 CPU-s | 12.34s | 2,244 / 34,720 |
| elwiki | 725.08 CPU-s | 607.09 CPU-s | 117.99 CPU-s | 4.07s | 558 / 10,335 |

The jobs are a mixture of network wait and CPU-bound decode/aggregation.
CPU throttling was small relative to elapsed time; the qualification evidence
does not justify increasing production concurrency yet.

### Namespace scheduling quota

The live `toolforge jobs quota` response during promotion reported:

| Quota | Used with itwiki running | Limit |
| --- | ---: | ---: |
| Total memory | 6.5 GiB | 8.0 GiB |
| Total CPU | 1.5 | 16.0 |
| Running pods | 2 | 16 |
| Per-job memory | — | 6.0 GiB |
| Per-job CPU | — | 3.0 |

An attempted concurrent 6 GiB elwiki preparation was rejected before pod
creation with `Unable to start, out of quota for memory`; no dataset files were
touched. This proves that requested memory, not measured working-set memory or
CPU, is the current concurrency constraint. Production preparation therefore
remains sequential and its schedules are spread across different days.

## Data coverage and patrol correctness

| Wiki | Weekly date range | Logging items parsed | Patrol events | Rights events | Skipped log types | Patrol metric rows | Patrol Parquet |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| itwiki | 2001-08-20 – 2026-07-27 | 15,640,174 | 3,503,144 | 6,040 | 12,130,990 | 13,793 | 1,609,555 B |
| svwiki | 2001-05-21 – 2026-07-27 | 5,795,226 | 520,623 | 3,798 | 5,270,805 | 8,201 | 1,288,004 B |
| elwiki | 2001-03-05 – 2026-07-27 | 1,431,385 | 54,901 | 1,075 | 1,375,409 | 8,755 | 1,161,784 B |

The non-zero rights counts are particularly important: they prove the Rust
`MultiGzDecoder` consumed every member of Wikimedia's concatenated logging
gzip rather than silently stopping after the first member.

## Storage behavior

| Wiki | Retained isolated generation | Raw input after success | Page-week output share |
| --- | ---: | ---: | ---: |
| itwiki | 6,443,173,691 B (6.00 GiB) | 0 B | 495,582,883 B |
| svwiki | 2,287,655,884 B (2.13 GiB) | 0 B | 286,811,791 B |
| elwiki | 542,455,578 B (517.33 MiB) | 0 B | 47,177,496 B |

The three isolated qualification roots retain about 8.64 GiB in total. Raw
compressed sources are zero after success because each source is deleted only
after its fragments and strict manifest are durable. The maximum raw working
set is therefore one source window, not the sum of every source in a snapshot.

## Improvements demonstrated by this promotion

1. **Failure recovery became source-local.** The itwiki compute defect did not
   discard 1h48m of successful ingestion. All 26 immutable source transactions
   were verified and reused.
2. **Memory is bounded by stable buckets.** Even itwiki's 151.6 million edits
   peaked at 2.37 GiB, far below the 6 GiB ceiling, while conserving every
   revision exactly.
3. **Raw storage is self-cleaning.** Compressed inputs are removed after each
   strict source commit; successful roots retain no raw dump files.
4. **Publication is isolated from preparation.** None of the qualification or
   candidate work can mutate the current public release. The public symlink
   changes only after all wikis pass one semantic publication transaction.
5. **Operational evidence is attributable.** Run IDs, stage durations, cgroup
   CPU/RAM, disk headroom, snapshot, binary/image provenance, row counts, date
   ranges, and artifact hashes are retained as structured records.
6. **Historical null identities are preserved.** The itwiki qualification
   exposed revisions without a page ID in early history. Weekly aggregation
   now counts rows rather than counting non-null page IDs, and the conservation
   guard proves those edits are not lost.

## Production promotion evidence

All three production candidates completed independently and remained invisible
until one combined publication transaction succeeded. The production runs used
the same one-worker, one-thread, 256-logical-bucket safety override as the
qualifications.

| Wiki | Production run | End-to-end | Source window | Core compute | Patrol compute | Validation | Peak RAM | Headroom | CPU usage |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| itwiki | `prepare-itwiki-20260824T234707Z-7` | 2h26m16s | 1h56m34s | 19m19s | 6m21s | 27.1s | 2,637,127,680 B (2.46 GiB) | 59.07% | 5,436.38 CPU-s |
| svwiki | `prepare-svwiki-20260825T022159Z-7` | 57m16s | 42m44s | 10m20s | 2m07s | 12.9s | 1,760,911,360 B (1.64 GiB) | 72.67% | 2,001.44 CPU-s |
| elwiki | `prepare-elwiki-20260825T032231Z-6` | 22m18s | 15m06s | 5m29s | 27.1s | 5.8s | 1,022,341,120 B (975.0 MiB) | 84.13% | 531.08 CPU-s |

Production source throughput was 4.34 MB/s and 38,669 rows/s for itwiki,
3.81 MB/s and 45,736 rows/s for svwiki, and 1.50 MB/s and 45,512 rows/s for
elwiki. The slow elwiki download explains why its production run was 5m04s
longer than qualification even though its core compute was 15.2% faster.
Svwiki finished 39 seconds faster overall and its compute was 6.0% faster.
Itwiki core compute was 0.7% faster; its complete new production source pass
was 7.4% slower than the earlier isolated pass because of source throughput.

Every production page-week artifact exactly matched its independent
qualification hash:

| Wiki | Rows | Edits conserved | SHA-256 |
| --- | ---: | ---: | --- |
| itwiki | 80,088,117 | 151,551,300 | `560ae2cbb5fa22ca58f4502d6b7a93369fd76ca6e470bc1b3b99bc9a0c6850df` |
| svwiki | 43,282,560 | 59,428,767 | `9b8b764fbe5a88282aa447d6285c41543f4bb388b656e3cca7092547cb0ed683` |
| elwiki | 5,982,821 | 11,728,354 | `dbe4fbb0aec3b27723609986fa264c14c466a5f0f29e1aa1c947b433e721373a` |

This byte equality across separate runs proves that concurrency, run IDs,
host cache state, and production timing did not change physical outputs.
Published per-wiki generations occupy 499,970,875 B for itwiki, 290,297,768 B
for svwiki, and 50,364,199 B for elwiki. All three raw directories are empty
after success.

### Atomic six-wiki publication

The publisher run `publish-20260825T034656Z-7` completed in 5,578 seconds
(1h32m58s):

| Stage | Duration |
| --- | ---: |
| Publication prepare, merge, fingerprints, and semantic gates | 5,370.848s |
| Site build and validation | 205.000s |
| Final publication commit | 0.123s |

It merged 331,313,444 page-week rows and conserved 602,584,846 edits in a
2,187,396,717-byte Parquet. The final patrol metric contains 68,794 rows;
source validation recorded 31,023,530 patrol and rights events. Browser output
contains 54 wiki-partitioned artifacts, 262,970 rows, and 8,424,428 bytes; the
largest partition is 1,609,555 bytes. The site generation itself occupies
17,184,552 bytes.

The publisher used 5,473.39 CPU-seconds and peaked at 3,303,776,256 bytes
(3.08 GiB), or 51.28% of its 6 GiB limit, retaining 48.72% headroom. Its final
switch took only 123 ms; the preceding 89.5-minute preparation kept the old
site live throughout.

The live target is
`.site-dist.build.publish-20260825T034656Z-7.gCEXcT`. Public checks returned
HTTP 200 and `/health/freshness.json` reported `healthy`, no alerts, this run
as the last successful publication, and snapshot `2026-07` for all six wikis.
The deployed runtime is CI-built commit
`db7000dd73fcf57ba9a8f290de99d15c5c5a4fd9`, binary SHA-256
`c6f16d66fec0d1880aba7dd77f1f515c2ee9da2783be30145a8000a078456fca`,
and image digest
`sha256:69ce5d373b228c51b885cf8a318c205bd29d6cb3cfc465f677e7ca5067d8ad0a`.

### Final scheduled production state

After the data publication was validated, operations commit
`e127f740fa756c4d204567d70e4c5c9ce3b7080b` was pushed, passed the complete
remote CI workflow, and was deployed through the attested manual SSH path. It
does not rewrite the validated data generation: it installs the final runtime,
activates the reviewed schedules, and keeps the prior atomic site target live.

The final binary is 110,296,208 bytes with SHA-256
`1991bc8986d4a4d4f352ababab324fe818cd4af7f8d740f7debdb1c6e8db216f`.
The Toolforge image source ref is
`toolforge-image-e127f740fa756c4d204567d70e4c5c9ce3b7080b`, and its immutable
digest is
`sha256:237f91bc44cdf5247eab5bce0b6136d835f15a89a07efa91963fc96a9f459690`.
The clean-operator verifier confirmed that binary provenance, binary checksum,
image source commit, and image digest agree.

GitHub Actions run
[`32813988772`](https://github.com/schiste/wiki-economics/actions/runs/32813988772)
passed for this exact SHA. Its gates included Rust and Node security policy,
REUSE, formatting and linting, 100% line coverage, two byte-reproducible
offline site builds, browser performance budgets, SBOM generation, sealed
provenance, and artifact attestation.

The live recurring topology is:

| Job | UTC schedule |
| --- | --- |
| nlwiki preparation | Sunday 01:00 |
| ptwiki preparation | Sunday 01:10 |
| frwiki preparation | Sunday 01:20 |
| itwiki preparation | Monday 01:00 |
| svwiki preparation | Tuesday 01:00 |
| elwiki preparation | Wednesday 01:00 |
| atomic ready-candidate publisher | Every two hours at minute 30 |

The legacy monolithic refresh and its ingest/compute/site components are not
loaded as recurring jobs; they remain explicit on-demand recovery entrypoints.
The final Toolforge inventory contained exactly seven cron jobs and one
continuous admin service. A delayed external validation returned HTTP 200 in
0.502 seconds, reported all six wikis as scheduled and published at snapshot
`2026-07`, and remained `healthy` with an empty alert list.

## Three new learnings and next improvements

1. **Toolforge schedules declared limits, not expected working sets.** Two
   6 GiB jobs cannot overlap under the current 8 GiB namespace quota even when
   their measured combined peak would fit. Qualify elwiki and svwiki under
   smaller explicit cgroups before right-sizing their job requests, or request
   at least 14 GiB if two full-size preparations must overlap safely.
2. **The runtime trust chain had stopped at deployment.** Early run records
   showed a null binary commit even though the bundle was attested. The runtime
   initializer now reads the sealed sidecar, hashes the deployed binary, and
   rejects binary/image disagreement. A future Rust `build-info` command would
   remove the remaining dependence on a sidecar for embedded commit identity.
3. **Whole-metric fingerprint scans are now a publication bottleneck.** The
   six-wiki publisher spent 89.5 minutes in preparation and remained almost
   continuously CPU-bound after the bounded merge. Candidate receipts already
   contain immutable hashes; composing those hashes and retaining a single
   streaming semantic scan should eliminate redundant full Parquet passes
   without weakening validation.

An additional smaller opportunity is patrol-fetch reuse: the resumed itwiki
compute reused all 26 core sources but still spent about one minute reopening
the existing patrol input. A strict patrol-source transaction fingerprint
would make same-snapshot recovery a true no-op there as well.

Resource checkpoint logs are also too granular during bucket reconciliation
and patrol-part merging. Rate-limited progress summaries would preserve the
same observability while making failures and stage boundaries much easier to
find.
