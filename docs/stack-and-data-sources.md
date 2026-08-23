# Stack & Data Sources

For software dependency and license inventory, see
[`docs/dependencies-and-licenses.md`](dependencies-and-licenses.md). Exact
resolved versions and wiki lifecycle states are generated in the
[stack reference](generated/stack-reference.md).

## Data sources

All data comes from publicly available Wikimedia dumps. No private APIs, CheckUser data, or non-public datasets are used.

Reuse of Wikimedia dump content is governed separately from this
project's MIT license. Refer to the [Wikimedia dumps
legal page](https://dumps.wikimedia.org/legal.html) and the [Wikimedia
Foundation Terms of Use](https://foundation.wikimedia.org/wiki/Policy:Terms_of_Use)
for the canonical reuse terms. As of this writing the dump *content* is
predominantly licensed under [CC BY-SA 4.0](https://creativecommons.org/licenses/by-sa/4.0/)
(with some metadata fields under [CC0](https://creativecommons.org/publicdomain/zero/1.0/));
verify against the official sources before publishing derivative
analytics, since the upstream terms can change.

### MediaWiki History dumps

The primary data source. These are tab-separated files published by the Wikimedia Foundation at [dumps.wikimedia.org/other/mediawiki_history](https://dumps.wikimedia.org/other/mediawiki_history/). Each row represents a revision event with fields covering:

- **Event metadata** — timestamp, type (create/delete/restore), entity (revision/page/user)
- **Editor state** — user ID, registration date, edit count at event time, bot flag, anonymous flag, temporary account flag, user groups
- **Page state** — page ID, title, namespace, creation timestamp, whether the page is a redirect
- **Revision details** — byte length before/after, SHA1, minor edit flag, deleted/suppressed flags, revert information

The project filters these to **revision-creation events only**. The exact input,
warehouse, and slim analytical column contracts live in
[`src/schema.rs`](../src/schema.rs); user classification is normalized during
ingest rather than carrying all of the source user-state columns into compute.

Dumps are partitioned yearly for most wikis and monthly for the largest projects (English Wikipedia, Wikidata, Commons).

### MediaWiki logging dumps

XML dumps of the `logging` table, fetched from `dumps.wikimedia.org/<wiki>/latest/<wiki>-latest-pages-logging.xml.gz`. Used specifically for:

- **Patrol events** (`log_type=patrol`) — records of editors reviewing new pages and edits
- **User rights changes** (`log_type=rights`) — used to reconstruct which editors held autopatrol permissions at any given time

The XML is streamed and parsed on-the-fly without loading the full file into memory. Wikimedia
logging dumps may contain concatenated gzip members, so the Rust reader decodes every member as
one continuous XML stream. The fetch log reports total log items, recognized patrol events,
recognized rights events, and skipped events. A substantial dump that produces no relevant events
is rejected instead of publishing empty Parquet files.

### MediaWiki API

A single lightweight query to the [MediaWiki siteinfo API](https://www.mediawiki.org/wiki/API:Siteinfo) fetches which user groups grant the `autopatrol` right (typically sysop and bot). This is combined with the rights-change log to build per-editor intervals of autopatrol membership.

## Stack

### Rust — compute engine

The core pipeline is a Rust CLI (`wiki-econ`) that handles fetching, ingesting,
computing, patrol processing, merging, validation, and deterministic dashboard
default generation. Key dependency roles are:

| Crate | Role |
|-------|------|
| **Polars** | Dataframe operations — lazy evaluation, CSV/Parquet I/O, aggregations, joins |
| **Rayon** | Parallel iteration for multi-wiki processing |
| **Reqwest** | HTTP client for downloading dumps, with retry and resume support |
| **bzip2** | Streaming decompression of `.tsv.bz2` dump files |
| **Clap** | CLI argument parsing (subcommands: `fetch`, `ingest`, `compute`, `merge`, `run`, `bench`) |
| **Tracing** | Structured logging with stable fields (wiki, metric, rows, bytes, elapsed_ms) |
| **Anyhow** | Error handling |

The pipeline processes data in four stages:

1. **Fetch** — streams dumps from Wikimedia to disk, supports resume on range-capable servers, bounded to 4 concurrent downloads
2. **Ingest** — decompresses bz2 into 32 MB in-memory chunks, parses CSV with Polars, writes Parquet partitions directly (no intermediate TSV on disk). Produces two layers: a wider warehouse layer and a slim analytical layer
3. **Compute** — reads one monthly Parquet partition at a time, computes metrics per month. Only cohort tracking, churn rates, and funnel state are carried across months. Outputs per-wiki Parquet files
4. **Merge** — concatenates per-wiki metric files into combined cross-wiki Parquet files

### Rust — patrol pipeline

`src/patrol.rs` handles patrol-specific logging dumps in the same Rust binary as
the core pipeline. It parses concatenated multi-member gzip streams, extracts
patrol and rights events, and computes latency, coverage, and concentration
metrics. The former PyArrow patrol scripts were removed after their regression
coverage was represented in Rust.

### Observable Framework — dashboard

The interactive dashboard is built with [Observable Framework](https://observablehq.com/framework/).
Each page is a Markdown file with embedded JavaScript that renders charts using
[Observable Plot](https://observablehq.com/plot/). The exact compiler and
browser-package versions come from the npm lockfile and are rendered in the
[generated stack reference](generated/stack-reference.md).

Key frontend patterns:

- **Pre-computed defaults** — the Rust merge stage uses Polars to produce deterministic JSON files for the default view of each page. This makes initial page load instant without a native Node database dependency
- **Arrow + Parquet-WASM** — the browser decodes the published Parquet files directly. The production dependency verifier rejects DuckDB modules and extensions, which are not needed by the current query path
- **Shared filter bar** — a single `filters.js` component provides consistent wiki, user type, namespace, date range, and granularity controls across all pages

### Browser query layer

Filtering runs over rows decoded by Arrow and Parquet-WASM. Server-side default
generation remains in Rust and Polars; neither native DuckDB nor DuckDB-WASM is
distributed by the current production build.

### Storage layout

```
data/
  raw/<wiki>/              ← downloaded .tsv.bz2 dumps
  patrol/<wiki>/           ← logging XML plus parsed patrol/right Parquet
  warehouse/<wiki>/_snapshots/<snapshot>/
    year=YYYY/
      year_month=YYYY-MM/   ← wide normalized Parquet
  parquet/<wiki>/_snapshots/<snapshot>/
    year=YYYY/
      year_month=YYYY-MM/   ← slim analytical Parquet
    _markers/               ← generation-scoped ingest markers
  snapshots/<wiki>/
    current-snapshot.json   ← atomically selected generation

output/
  <wiki>/                   ← per-wiki metric Parquet files
  *.parquet                 ← merged cross-wiki files
  defaults_*.json           ← deterministic Rust-generated dashboard defaults
  meta_*.json               ← deterministic Rust-generated dashboard metadata
  manifest.json             ← validated publication inventory and provenance

site/
  data-build/
    manifest.json.sh        ← fail-closed manifest entrypoint
    manifest.json.cjs       ← manifest validation and provenance assembly
  src/
    *.md                    ← Observable pages
    components/             ← shared JS (filters, charts)
    data/
      *.parquet             ← symlinked or copied from output/
      defaults_*.json       ← generated dashboard defaults from output/
      manifest.json         ← generated dashboard manifest from output/
```

### Quality gates

CI enforces:

- `cargo fmt` — consistent formatting
- `cargo clippy -D warnings` — no lints
- `cargo test` — full test suite
- `cargo llvm-cov ...` plus `scripts/check_lcov.py` — 100% LCOV line coverage
- `cargo deny` + `cargo audit` — no known vulnerabilities in dependencies
