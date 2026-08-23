# Refresh run record

Toolforge refresh health is published atomically to
`output/.refresh-status.json`. Unlike the original exit-only marker, schema
version 2 is created immediately after the single-flight lock is acquired and
is refreshed for the lifetime of the job. A previous success therefore cannot
masquerade as the current state of a hung or failed refresh.

## State and heartbeat

`state` progresses through `starting`, `running`, and one terminal state:
`succeeded` or `failed`. While the job owns the refresh lock, the same
background process updates both the lock metadata and run record every 60
seconds. `heartbeatAt`, `currentStage`, `currentWiki`, `startedAt`, and
`durationSecs` make a stalled stage visible even when the Rust process has not
exited.

The wrapper stops that background writer before emitting the terminal record.
This gives `.refresh-status.json` one writer at a time and prevents a late
heartbeat from replacing `succeeded` or `failed` with `running`. Every update
uses a temporary sibling plus an atomic rename on the shared NFS filesystem.
If either lock or status heartbeat publication fails, the wrapper terminates
the refresh rather than continuing without trustworthy single-flight and
health evidence.

## Recorded data

The live/final record includes:

- run ID, selected snapshot, scheduled wikis, state, heartbeat, and current
  stage/wiki;
- exact binary source commit and SHA-256 plus buildpack image source ref and
  commit;
- each Rust or site stage's start/finish state, duration, reuse/skip flags, and
  concise failure;
- aggregate per-stage durations and lists of reused/skipped stages;
- cgroup current/peak/limit memory and NFS free/total disk space;
- validated metric rows, conservation totals, page-edit total, date ranges,
  cutoff dates, and patrol/rights source counts copied from the current run's
  `publication-gate.json`;
- the currently published site symlink generation; and
- the run-scoped log filename; and
- terminal exit code, failing stage, and a bounded single-line error.

The publication summary is accepted only when its run ID matches the live run,
so an old successful receipt cannot contaminate a failed record.

## Freshness alerts

`GET /health/freshness.json` is deliberately public and read-only. It evaluates
the atomic status and history records against the lifecycle registry and emits
`healthy`, `warning`, or `critical` plus machine-readable alerts. Checks cover
the last-success SLA, unpublished newer snapshots, cutoffs that fail to advance,
zero patrol data, stale heartbeat/stage runtime, memory pressure at 75%/80%,
less than 50 GiB of filesystem headroom, and browser-data total/partition
size. Missing browser-size evidence is itself critical. Thresholds come from
`config/operations-slos.json`. The authenticated admin status embeds
the same evaluation under `freshness`.

`.github/workflows/freshness.yml` reads that endpoint every six hours. It has
read-only repository permissions and no Toolforge or SSH secret; it alerts by
failing the check and never starts, retries, or deploys the production job.

## Stage event protocol

Rust writes compact JSON Lines events to the owner lock directory through
`WIKI_ECON_RUN_EVENTS_FILE`. `run_timed_stage` records start, completion,
duration, and errors; content-addressed reuse paths add a `reused` event. The
shared site script uses the same event format because the Observable build is
outside the Rust process. A partially visible final JSONL line is ignored by a
heartbeat and becomes visible on its next pass.

## History and operator checks

Only terminal records enter `output/.refresh-history.jsonl`. The writer keeps
104 compact, deduplicated entries—two years at the weekly schedule—and rewrites
the bounded file atomically. Set `WIKI_ECON_REFRESH_HISTORY_LIMIT` to a value
from 52 through 104 to retain between one and two years; out-of-range values
are clamped.

Toolforge logs are separated under `output/logs/refresh/<run-id>.log`, with the
same 52–104 retention bound. Each file has explicit run delimiters and terminal
JSON Lines summaries for every stage and the overall run. The wrapper disables
ANSI color and Observable telemetry (`OBSERVABLE_TELEMETRY_DISABLE=true`), and
the Rust tracing span adds `run_id` to every application event.

Inspect the current record with:

```sh
become wiki-economics jq . /data/project/wiki-economics/output/.refresh-status.json
```

For a `starting` or `running` record, compare `heartbeatAt` to the current UTC
time. The admin dashboard treats a live heartbeat older than five minutes as
stale. For a failed record, inspect `failingStage` and `error`, then correlate
the same `runId` with the Toolforge file log and publication receipts.
