# Benchmarking

Use the built-in benchmark command against existing analytical parquet data:

```sh
cargo run --release -- bench frwiki dewiki --warmup 1 --iterations 5
```

The benchmark expects the current analytical storage layout under `data/parquet/<wiki>/`.

## What It Measures

Per wiki it reports:

- `load_wiki`
- `inequality`
- `labor`
- `gdp`
- `compute_all`

The benchmark measures two different execution styles:

- split metric timings (`load_wiki`, `inequality`, `labor`, `gdp`) still use one in-memory base frame
- `compute_all` is timed separately end-to-end and may use the partitioned incremental compute path

That distinction matters when comparing commits. If `compute_all` improves while `load_wiki` stays flat, the win likely came from the month-partitioned incremental path rather than from full-frame query changes.

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

- `split/` for the direct metric-module timings
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

`frwiki` must remain `refresh: paused` until three independent one-off jobs
measure its full warehouse under Toolforge's 6 GiB cgroup and at least one
variant qualifies. Run each bucket count in a fresh container so `memory.peak`
is isolated per variant:

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
identical. Repeat the chosen configuration once and require the same output
SHA-256 to prove byte determinism. Do not choose solely by runtime: prefer the
smallest bucket count that sustains at least 25% headroom, then use larger
counts only if they materially improve the measured maximum bucket or memory.

The scratch root is explicit for capacity jobs. Normal compute may set
`WIKI_ECON_SCRATCH_DIR`; the Toolforge refresh wrapper passes that root to
safe stale-artifact cleanup after acquiring the single-flight lock.
