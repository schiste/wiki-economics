# Benchmarking

Use the built-in benchmark command against existing analytical parquet data:

```sh
cargo run --release -- bench frwiki dewiki --warmup 1 --iterations 5
```

The benchmark expects the current analytical storage layout under `data/parquet/<wiki>/`.

## What It Measures

Per wiki it reports:

- `monthly`
- `activity_tiers`
- `lifecycle`
- `page_week`
- `compute_all`

Each family timing invokes the same uncached family executor used by the
production computation planner. `compute_all` then measures the complete
fingerprinted production path in a separate clean output directory.

Family timings intentionally bypass same-output fingerprint reuse. This keeps
them useful for comparing executor cost while `compute_all` remains the primary
end-to-end production number.

## Output Summary

The command also prints a lightweight output summary for the generated parquet files:

- metric name
- row count
- column count
- file size

## Keeping Outputs

To preserve benchmark outputs for inspection:

```sh
cargo run --release -- --output-dir output bench frwiki --iterations 3 --keep-outputs
```

Kept outputs are written under `output/bench/<wiki>/iter-<n>/`.

Each kept iteration has:

- `families/` for the production family-executor timings
- `full/` for the `compute_all` end-to-end timing

With the current storage layout, `data/parquet/<wiki>/` is the slim analytical layer, not the richer warehouse layer.

## Recommended Practice

For meaningful benchmark comparisons:

- use `--release`
- benchmark the same wiki set before and after a change
- include at least one representative larger wiki
- compare runtime and output shape together
- prefer `compute_all` when making claims about real pipeline speed

If a change affects ingest, storage layout, or Polars behavior, benchmark with data produced by the current ingest pipeline rather than mixing old and new parquet layouts.

## Toolforge capacity qualification

Before activating a large wiki, independent one-off jobs must measure its full
warehouse under Toolforge's 6 GiB cgroup and at least one variant must qualify.
Run each bucket count in a fresh container so `memory.peak` is isolated per
variant. Frwiki completed this gate on 2026-08-24; 256 buckets is the only
qualified production configuration and is enforced by the Toolforge wrapper:

Toolforge's persistent NFS currently has no per-tool quota, as documented by
the [ToolsNfsAlmostFull runbook](https://wikitech.wikimedia.org/wiki/Portal:Toolforge/Admin/Runbooks/ToolsNfsAlmostFull).
Leave `WIKI_ECON_NFS_QUOTA_BYTES` unset so the report identifies
`shared_filesystem_available` as its capacity source. If the platform later
adds an enforceable tool-specific limit, set that variable and the gate will
use the smaller of quota headroom and live filesystem free space. The wrapper
also retains 50 GiB after the estimated rollover requirement by default;
override `WIKI_ECON_CAPACITY_STORAGE_RESERVE_BYTES` only with documented
operational justification.

```sh
toolforge jobs run --image tool-wiki-economics/tool-wiki-economics:latest \
  --command 'deploy/toolforge/run-capacity-benchmark.sh frwiki 256' \
  --filelog --mount all --mem 6Gi --cpu 1 wiki-econ-frwiki-capacity-256
toolforge jobs run --image tool-wiki-economics/tool-wiki-economics:latest \
  --command 'deploy/toolforge/run-capacity-benchmark.sh frwiki 512' \
  --filelog --mount all --mem 6Gi --cpu 1 wiki-econ-frwiki-capacity-512
toolforge jobs run --image tool-wiki-economics/tool-wiki-economics:latest \
  --command 'deploy/toolforge/run-capacity-benchmark.sh frwiki 1024' \
  --filelog --mount all --mem 6Gi --cpu 1 wiki-econ-frwiki-capacity-1024
```

Reports are written atomically below
`/data/project/wiki-economics/capacity/reports/frwiki/`. Each report records:

- reduction, reconciliation, and final RSS/cgroup memory;
- peak disk-backed scratch bytes and the configured scratch root;
- current analytical plus warehouse generation bytes;
- capacity source, current tool-root usage, live filesystem free space, and
  optional configured-quota headroom, less the configured safety reserve;
- the estimated additional rollover requirement: 31 GiB raw transient, one
  replacement generation, peak scratch, and the weekly output;
- available filesystem bytes and pass/fail storage status;
- rows, edits, date range, largest bucket, output bytes, and SHA-256; and
- a fail-closed memory gate requiring at least 25% peak headroom.

Reports also contain all per-bucket staged row counts, separate reduction and
reconciliation durations, peak combined scratch-plus-output working storage,
project-root storage before/after, source commit, selected snapshot, and exact
thread configuration. Run `scripts/qualify-capacity.cjs` as described in
[the operations runbook](operations-recovery.md); comparing only the largest
bucket is not sufficient qualification evidence.

Compare all three reports. Rows, edit conservation, and date ranges must be
identical. Repeat the chosen topology and require the same output SHA-256.
Then run `wiki-econ determinism-verify` against isolated builds made with two
distinct worker counts, as described in [deterministic builds](deterministic-builds.md).
The snapshot, binary commit, computation version, and partition topology must
remain fixed for that comparison. Do not choose solely by runtime: prefer the
smallest bucket count that sustains at least 25% headroom, then use larger
counts only if they materially improve the measured maximum bucket or memory.

The scratch root is explicit for capacity jobs. Normal compute may set
`WIKI_ECON_SCRATCH_DIR`; the Toolforge refresh wrapper passes that root to
safe stale-artifact cleanup after acquiring the single-flight lock.

## CPU and bounded-worker qualification

Do not raise production concurrency from the one-worker default based on host
CPU visibility. Toolforge containers can see CPUs that are not included in
their cgroup quota. A controlled two-thread qualification uses the first six
publication-invisible jobs (the 1/1/1 and 2/2/1 rows for all three wikis). The
complete optimization matrix uses all twelve jobs:

| CPU quota | Polars/Rayon threads | Weekly workers |
| ---: | ---: | ---: |
| 1 | 1 | 1 |
| 2 | 2 | 1 |
| 4 | 3 | 1 |
| 4 | 3 | 2 |

Run the selected scope for nlwiki, ptwiki, and frwiki, one job at a time. The
on-demand definitions live in
`deploy/toolforge/cpu-qualification-jobs.yaml`. Loading a definition starts
it, so wait for a terminal state before loading the next one; overlapping runs
would contaminate both CPU and shared-NFS throughput evidence:

As checked with `toolforge jobs quota` on 2026-08-26, wiki-economics currently
has 16 aggregate CPUs but a 3-CPU per-job ceiling. The required 4-CPU cells
must not be launched until that per-job limit is raised to at least 4. Until
then, the six-cell scope can authorize at most the 2-CPU/2-thread profile. A
3-CPU substitution is useful exploratory evidence but does not satisfy the
complete matrix.

```sh
toolforge jobs load --job wiki-econ-cpu-nl-c1-t1-w1 \
  deploy/toolforge/cpu-qualification-jobs.yaml
toolforge jobs show wiki-econ-cpu-nl-c1-t1-w1
```

Repeat for six names in the controlled two-thread scope or all twelve names in
the complete matrix. The wrapper reuses each wiki's
active immutable warehouse generation, produces no publication candidate, and
removes isolated output and scratch on exit. Source downloads therefore remain
serialized in production and are outside this compute-only matrix. Prefer the
local dump mount for a later fetch/ingest-specific experiment.

Pass either the six controlled two-thread receipts or all twelve retained
receipt paths to the Rust evaluator:

```sh
wiki-econ cpu-qualify \
  --capacity-report /path/to/nl-c1-t1-w1.json \
  --capacity-report /path/to/nl-c2-t2-w1.json \
  --capacity-report /path/to/nl-c4-t3-w1.json \
  --capacity-report /path/to/nl-c4-t3-w2.json \
  --capacity-report /path/to/pt-c1-t1-w1.json \
  --capacity-report /path/to/pt-c2-t2-w1.json \
  --capacity-report /path/to/pt-c4-t3-w1.json \
  --capacity-report /path/to/pt-c4-t3-w2.json \
  --capacity-report /path/to/fr-c1-t1-w1.json \
  --capacity-report /path/to/fr-c2-t2-w1.json \
  --capacity-report /path/to/fr-c4-t3-w1.json \
  --capacity-report /path/to/fr-c4-t3-w2.json \
  --report /data/project/wiki-economics/capacity/cpu-qualification.json
```

The evaluator rejects missing or duplicate cells, wrong cgroup CPU quotas,
incomplete CPU/page-cache/I/O telemetry, inconsistent snapshots, any capacity
gate failure, less than 25% memory headroom, or different output SHA-256 values
across worker counts. A higher-CPU profile is recommended only if its aggregate
wall-time speedup across all three wikis is at least 15%. A failed evaluation
still writes the complete atomic report as qualification evidence.
