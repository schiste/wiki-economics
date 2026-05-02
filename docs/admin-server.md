# Admin Server

The admin server (`site/admin-server.cjs`) is an operator-facing dev tool
that drives the wiki-economics pipeline from a small HTTP API. It is
designed for local use only; production deployments leave it disabled.

For its threat model and the path to remote auth (Wikimedia OAuth 2.0),
see [`security.md`](security.md).

## Lifecycle

`scripts/dev.sh` starts the admin server alongside the Observable
preview. Running `scripts/dev.sh` is the only supported way to bring the
admin server up; no separate `npm run admin` script exists.

To shut down: stop `scripts/dev.sh` (Ctrl-C). The admin server has no
background mode and no daemon support.

## Environment variables

| Variable | Default | Effect |
| --- | --- | --- |
| `WIKI_ECON_ADMIN_ENABLED` | `1` (local), `0` (production) | Master switch. When `0`, the server exits on startup with an explanatory message. |
| `WIKI_ECON_ADMIN_PORT` | `3001` | Loopback port to bind. |
| `WIKI_ECON_SITE_PORT` | `3000` | Used to construct the default CORS allowlist (`127.0.0.1:3000`, `localhost:3000`). |
| `WIKI_ECON_ALLOWED_ORIGINS` | (computed from `WIKI_ECON_SITE_PORT`) | Comma-separated origin allowlist. Set to `*` to bypass CORS — discouraged except for ephemeral debugging. |
| `WIKI_ECON_ENV` | `local` | When `production`, `WIKI_ECON_ADMIN_ENABLED` defaults to `0`. |
| `WIKI_ECON_BIN` | (uses `cargo run --release --`) | Override path to the compiled `wiki-econ` binary. The admin server invokes this for every job; pointing it at a release-mode binary skips the per-job rebuild. |
| `WIKI_ECON_DATA_DIR` | `data/` | Where the pipeline reads raw + intermediate parquet. |
| `WIKI_ECON_OUTPUT_DIR` | `output/` | Where the pipeline writes per-wiki and merged metric parquet. |
| `WIKI_ECON_GENERATOR_DIR` | `site/data-build/` | Where merge looks for dashboard JSON generators. |

## Endpoints

All endpoints are under `/api`. Mutating endpoints (`POST`) are subject
to the CORS allowlist; `GET` endpoints are not, on the basis that the
loopback bind itself is the security perimeter.

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/api/status` | Returns current job state, recent log lines, manifest age, and supported wiki list. |
| `POST` | `/api/fetch` | Run `wiki-econ fetch <wiki>` (one wiki per call). |
| `POST` | `/api/ingest` | Run `wiki-econ ingest <wiki>`. |
| `POST` | `/api/compute` | Run `wiki-econ compute <wiki>`. |
| `POST` | `/api/merge` | Run `wiki-econ merge`. |
| `POST` | `/api/run` | Run the full `fetch → ingest → compute → merge` pipeline for a wiki. |
| `POST` | `/api/patrol-fetch` | Run `wiki-econ patrol-fetch <wiki>`. |
| `POST` | `/api/patrol-compute` | Run `wiki-econ patrol-compute <wiki>` (optionally with `rebuild` and `limit_months`). |
| `POST` | `/api/cleanup` | Remove `.tmp`, marker files, and partial outputs for a wiki. |
| `POST` | `/api/cancel` | Cancel the current job. The cancellation is best-effort; long-running parquet writes finish before the cancel takes effect. |

A new POST while a job is running returns `409 Conflict`; the API does
not queue jobs.

## Job state machine

```
idle → running → success
              → failure
              → cancelled
```

The admin page (Observable Framework, `site/src/admin.md`) polls
`/api/status` every 1.5 s while a job is running and every 5 s while
idle. The 1.5 s cadence matches the manifest cache TTL inside the
server.

## Why it exits in production

The systemd unit at
[`deploy/cloud-vps/systemd/wiki-econ-refresh.service`](../deploy/cloud-vps/systemd/wiki-econ-refresh.service)
runs the refresh as a `Type=oneshot` batch driven by a timer. It does
not start the admin server. The nginx config at
[`deploy/cloud-vps/nginx/wiki-economics.conf`](../deploy/cloud-vps/nginx/wiki-economics.conf)
serves only the static dashboard; it returns 404 for `/admin` and never
proxies `/api`.

If you find a future deployment shape that wants an admin surface,
read [`security.md`](security.md) first — Wikimedia OAuth 2.0 is the
documented path.
