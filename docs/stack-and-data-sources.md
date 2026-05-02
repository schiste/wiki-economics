# Stack & Data Sources

## Data sources

All data comes from publicly available Wikimedia dumps. No private APIs, CheckUser data, or non-public datasets are used.

### MediaWiki History dumps

The primary data source. These are tab-separated files published by the Wikimedia Foundation at [dumps.wikimedia.org/other/mediawiki_history](https://dumps.wikimedia.org/other/mediawiki_history/). Each row represents a revision event and contains 76 columns covering:

- **Event metadata** — timestamp, type (create/delete/restore), entity (revision/page/user)
- **Editor state** — user ID, registration date, edit count at event time, bot flag, anonymous flag, temporary account flag, user groups
- **Page state** — page ID, title, namespace, creation timestamp, whether the page is a redirect
- **Revision details** — byte length before/after, SHA1, minor edit flag, deleted/suppressed flags, revert information

The project filters these to **revision-creation events only** and retains 10 analytical columns: timestamp, user ID, user text, page namespace, byte diff, minor flag, bot flag, anonymous flag, temporary flag, and revert indicator.

Dumps are partitioned yearly for most wikis and monthly for the largest projects (English Wikipedia, Wikidata, Commons).

### MediaWiki logging dumps

XML dumps of the `logging` table, fetched from `dumps.wikimedia.org/<wiki>/latest/<wiki>-latest-pages-logging.xml.gz`. Used specifically for:

- **Patrol events** (`log_type=patrol`) — records of editors reviewing new pages and edits
- **User rights changes** (`log_type=rights`) — used to reconstruct which editors held autopatrol permissions at any given time

The XML is streamed and parsed on-the-fly without loading the full file into memory.

### MediaWiki API

A single lightweight query to the [MediaWiki siteinfo API](https://www.mediawiki.org/wiki/API:Siteinfo) fetches which user groups grant the `autopatrol` right (typically sysop and bot). This is combined with the rights-change log to build per-editor intervals of autopatrol membership.

## Stack

### Rust — compute engine

The core pipeline is a Rust CLI (`wiki-econ`) that handles fetching, ingesting, computing, and merging. Key dependencies:

| Crate | Role |
|-------|------|
| **Polars 0.53** | Dataframe operations — lazy evaluation, CSV/Parquet I/O, aggregations, joins |
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

### Python — patrol pipeline

Two scripts handle patrol-specific data that comes from logging dumps rather than revision history:

- `scripts/fetch_patrol.py` — downloads and parses XML logging dumps, extracts patrol events and user rights changes
- `scripts/compute_patrol.py` — joins patrol logs with revision data to compute latency, coverage, and concentration metrics. Classifies each patrolled revision by author type (registered/anonymous/temporary/bot) and namespace

### Observable Framework — dashboard

The interactive dashboard is built with [Observable Framework](https://observablehq.com/framework/) (v1.13). Each page is a Markdown file with embedded JavaScript that renders charts using [Observable Plot](https://observablehq.com/plot/).

Key frontend patterns:

- **Pre-computed defaults** — shell-based data loaders (`.json.sh`) run DuckDB queries at build time to produce ~80 KB JSON files for the default view of each page. This makes initial page load instant
- **DuckDB WASM** — when a user changes filters, [DuckDB compiled to WebAssembly](https://duckdb.org/docs/api/wasm/overview) queries Parquet files directly in the browser. The ~34 MB DuckDB runtime is lazy-loaded only when needed
- **Shared filter bar** — a single `filters.js` component provides consistent wiki, user type, namespace, date range, and granularity controls across all pages

### DuckDB — query layer

DuckDB serves two roles:

1. **Build-time** (shell scripts) — the `.json.sh` data loaders use the DuckDB CLI to aggregate Parquet files into pre-computed JSON defaults
2. **Client-side** (browser) — DuckDB-WASM runs SQL queries on Parquet files when users apply non-default filters, enabling interactive exploration without a backend server

### Storage layout

```
data/
  raw/<wiki>/              ← downloaded .tsv.bz2 dumps
  warehouse/<wiki>/        ← wide normalized Parquet (for future metrics)
    year=YYYY/
      year_month=YYYY-MM/
  parquet/<wiki>/           ← slim analytical Parquet (compute input)
    year=YYYY/
      year_month=YYYY-MM/
    _markers/               ← ingest completion markers

output/
  <wiki>/                   ← per-wiki metric Parquet files
  *.parquet                 ← merged cross-wiki files

site/
  src/
    *.md                    ← Observable pages
    components/             ← shared JS (filters, charts)
    data/
      *.parquet             ← symlinked or copied from output/
      defaults_*.json.sh    ← build-time data loaders
```

### Quality gates

CI enforces:

- `cargo fmt` — consistent formatting
- `cargo clippy -D warnings` — no lints
- `cargo test` — full test suite
- `cargo llvm-cov ...` plus `scripts/check_lcov.py` — 100% LCOV line coverage
- `cargo deny` + `cargo audit` — no known vulnerabilities in dependencies
