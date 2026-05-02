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
