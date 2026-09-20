# Admin Server

The admin server (`site/admin-server.cjs`) powers the operator surface for
wiki-economics. It has two supported modes:

- **local/dev**: a loopback-only job-control API used by `scripts/dev.sh`
- **hosted production**: an authenticated admin page at `/admin` and an
  authenticated API under `/admin-api/*`, served by the Toolforge Build
  Service webservice or proxied through nginx on Wikimedia Cloud VPS

For the broader threat model, see [`security.md`](security.md).

## Supported lifecycle

- `scripts/dev.sh` starts the local admin API together with the Observable
  preview server.
- `deploy/cloud-vps/systemd/wiki-econ-admin.service` runs the authenticated
  admin server on a VPS.
- Toolforge runs `Procfile` as the `wiki-econ-admin` Build Service webservice;
  the separate refresh Job shares status and artifacts only through NFS.
- Local and Cloud VPS deployments default to `127.0.0.1`; the VPS exposes it
  only through nginx. Toolforge explicitly configures the container bind host
  and port through tool-wide environment variables and relies on Toolforge's
  ingress for routing plus application-level authentication.

## Authentication modes

The runtime is controlled by `WIKI_ECON_ADMIN_AUTH_MODE`:

- `none`
  Use for local development. No login flow is required.
- `mediawiki`
  Use for hosted deployments. The admin page and API require a Wikimedia
  account login via `meta.wikimedia.org`'s OAuth 2 flow, and the resulting
  username must be present in `WIKI_ECON_ADMIN_ALLOWED_USERNAMES`.

If `WIKI_ECON_ENV=production` and `WIKI_ECON_ADMIN_ENABLED=1`, the server
refuses to start with `WIKI_ECON_ADMIN_AUTH_MODE=none`.

## Environment variables

| Variable | Default | Effect |
| --- | --- | --- |
| `WIKI_ECON_ADMIN_ENABLED` | `1` (local), `0` (production) | Master switch. When `0`, the server exits on startup with an explanatory message. |
| `WIKI_ECON_ADMIN_PORT` | `3001` | Listen port; Toolforge sets the value expected by its webservice ingress. |
| `WIKI_ECON_ADMIN_BIND_HOST` | `127.0.0.1` | Listen address. Keep the default locally/on VPS; Toolforge must set the address expected by its webservice ingress. |
| `WIKI_ECON_SITE_PORT` | `3000` | Used for the local dev allowlist when the admin page runs from the Observable preview server. |
| `WIKI_ECON_ENV` | `local` | When `production`, the server enforces authenticated mode if enabled. |
| `WIKI_ECON_BIN` | (uses `cargo run --release --locked --`) | Override path to the compiled `wiki-econ` binary. |
| `WIKI_ECON_DATA_DIR` | `data/` | Where the pipeline reads raw + intermediate parquet. |
| `WIKI_ECON_OUTPUT_DIR` | `output/` | Where the pipeline writes per-wiki and merged metric parquet. |
| `WIKI_ECON_GENERATOR_DIR` | `site/data-build/` | Where merge finds the fail-closed publication manifest validator. |
| `WIKI_ECON_SITE_DIST_DIR` | `site/dist/` | Where the built `admin.html` is read from when serving `/admin`. |
| `WIKI_ECON_ALLOWED_ORIGINS` | local preview origins | Extra origin allowlist entries for CORS / CSRF checks. In hosted mode the request's own public origin is also accepted. |
| `WIKI_ECON_ADMIN_AUTH_MODE` | `none` | `none` for local dev, `mediawiki` for hosted admin. |
| `WIKI_ECON_ADMIN_ALLOWED_USERNAMES` | empty | Comma/newline-separated allowlist of authorized operator Wikimedia usernames (case-sensitive). |
| `WIKI_ECON_ADMIN_SESSION_SECRET` | empty | HMAC secret used to sign the short-lived session and OAuth-state cookies. Use 32+ random bytes. |
| `WIKI_ECON_ADMIN_SESSION_TTL_SECS` | `28800` | Session lifetime in seconds. |
| `WIKI_ECON_ADMIN_UPSTREAM_RETRY_SECS` | `21600` | Delay before automatically rechecking a qualification waiting on an incomplete Wikimedia logging dump. The wait does not consume the stale-process retry budget. |
| `WIKI_ECON_ADMIN_SECURE_COOKIES` | `1` in production | Adds the `Secure` flag to auth cookies. |
| `WIKI_ECON_ADMIN_MEDIAWIKI_HOST` | `https://meta.wikimedia.org` | Base URL of the MediaWiki OAuth2 host. |
| `WIKI_ECON_ADMIN_MEDIAWIKI_CLIENT_ID` | empty | OAuth2 consumer client ID, from `Special:OAuthConsumerRegistration`. |
| `WIKI_ECON_ADMIN_MEDIAWIKI_CLIENT_SECRET` | empty | OAuth2 consumer client secret. |
| `WIKI_ECON_WIKI_LIFECYCLE_FILE` | `config/wiki-lifecycle.json` | Validated publication/refresh lifecycle registry. |
| `WIKI_ECON_ADMIN_PUBLIC_ORIGIN` | unset | Optional canonical external origin. If unset, the server derives it from `X-Forwarded-*` headers. |
| `WIKI_ECON_MACHINE_API_RATE_LIMIT_PER_SECOND` | `30` | Fixed one-second request budget per machine-client identity for `/api/v1`, `/mcp`, and `/health/freshness.json`. Invalid values fall back to the default. |
| `WIKI_ECON_MACHINE_API_RATE_LIMIT_MAX_CLIENTS` | `4096` | Maximum number of in-memory client buckets per webservice process; oldest buckets are evicted when the cap is reached. |

## Routes

### Page and auth routes

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/admin` | Serve the built admin page after authentication. |
| `GET` | `/admin/login` | Render the hosted login page. |
| `GET` | `/admin/oauth/start` | Begin the MediaWiki OAuth2 authorization-code flow. |
| `GET` | `/admin/oauth/callback` | Finish the OAuth2 flow, validate username, issue a signed session cookie. |
| `GET`/`POST` | `/admin/logout` | Clear the admin session cookie. |

### API routes

`GET /health/freshness.json` is a public, read-only machine endpoint. It does
not expose logs or credentials and remains accessible when hosted admin auth is
enabled so an external scheduled monitor can detect a stalled refresh.

### Public data API and MCP

The same webservice exposes a public, read-only interface for automated
consumers. It is deliberately backed by `output/manifest.json`: only artifacts
listed there and associated with a wiki whose lifecycle publication is
`published` can be downloaded.

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/api/v1` | Versioned endpoint discovery. |
| `GET` | `/api/v1/openapi.json` | Small OpenAPI 3.1 description for generated clients. |
| `GET` | `/api/v1/catalog` | Full publication catalog, schemas, hashes, provenance, and links. |
| `GET` | `/api/v1/wikis` | Published wiki list and per-wiki artifact metadata. |
| `GET` | `/api/v1/datasets` | Published metric definitions and artifact metadata. |
| `GET` | `/api/v1/datasets/{dataset}?wiki={wiki}` | Resolve one dataset to download links. |
| `GET`/`HEAD` | `/api/v1/artifacts/{path}` | Stream an allow-listed Parquet/JSON artifact with ETag, range, and last-modified support. |
| `POST` | `/mcp` | MCP Streamable HTTP JSON-RPC endpoint. |

The MCP endpoint has no mutation tools. It supports `initialize`, `ping`, tool
and resource discovery, and these read-only tools: `list_published_wikis`,
`list_datasets`, `get_dataset`, and `get_freshness`. The `catalog` and
`freshness` resources are available at `wiki-economics://catalog` and
`wiki-economics://freshness`; artifact metadata uses the
`wiki-economics://artifact/{artifact}` template. It accepts the current
stateless protocol version (`2026-07-28`) plus the previous compatibility
versions used by existing clients. Every response includes cache hints where
the protocol supports them, while artifact bytes remain cacheable and
content-addressed through their SHA-256 ETag.

The public interface intentionally omits operator logs, credentials, raw
source paths, and unpublished/qualification data. Consumers should discover
the current generation through `/api/v1/catalog` rather than guessing file
names; a missing or invalid publication manifest returns `503` until the next
valid site publication is available.

Public machine calls are deliberately bounded. Each webservice process applies
the configured fixed one-second budget independently to the first address in a
trusted `X-Forwarded-For` header, then `X-Real-IP`, then the socket peer. Every
machine response advertises `RateLimit-Limit`, `RateLimit-Remaining`,
`RateLimit-Reset`, and `RateLimit-Policy`; an exhausted budget returns `429`
with `Retry-After` and a stable `rate_limited` error code. Startup logs announce
the active limit and client-bucket cap, and the API discovery document exposes
the same policy under `security.rate_limit`. The limiter is intentionally a
small per-process guard, not a replacement for a fleet-wide ingress limit; the
proxy must overwrite forwarded-client headers rather than trusting arbitrary
caller-supplied values.

The artifact filter is fail-closed: only known metric identities from the
generated catalog, canonical merged/per-wiki paths, declared browser partitions,
the browser index, and the generated `defaults_`/`meta_` JSON files can be
served. A record must also be present in the publication manifest, belong to a
published lifecycle wiki, and resolve beneath the real output directory.
Unknown files, hidden wikis, unsupported browser partitions, traversal paths,
and stale/unlisted records return `404` without revealing filesystem details.
MCP batches are capped at 64 JSON-RPC messages to keep one HTTP request from
turning into an unbounded burst of logical calls.

The server accepts both the legacy local prefix and the hosted prefix:

- local/dev: `/api/*`
- hosted/proxied: `/admin-api/*`

Supported endpoints:

| Method | Path suffix | Purpose |
| --- | --- | --- |
| `GET` | `/status` | Returns current job state plus the authoritative operational snapshot described below. |
| `POST` | `/register-wiki` | Add a supported project to the lifecycle without starting work. |
| `POST` | `/onboard-wiki` | Atomically register a project and queue its first preparation or qualification. |
| `POST` | `/fetch` | Run `wiki-econ fetch <wiki>`. |
| `POST` | `/ingest` | Run `wiki-econ ingest <wiki>`. |
| `POST` | `/compute` | Run `wiki-econ compute <wiki>`. |
| `POST` | `/merge` | Run `wiki-econ merge`. |
| `POST` | `/run` | Prepare and validate an immutable candidate for a managed wiki; it does not publish. |
| `POST` | `/qualify` | Prepare and validate a hidden qualification candidate. |
| `POST` | `/patrol-fetch` | Run `wiki-econ patrol-fetch <wiki>`. |
| `POST` | `/patrol-compute` | Run the guarded `wiki-econ patrol-refresh <wiki>` fetch→compute flow. |
| `POST` | `/cleanup` | Remove `.tmp`, invalid marker files, and partial outputs for a wiki. |
| `POST` | `/cancel` | Cancel the current job. |
| `POST` | `/publish` | Run the fail-closed ready-candidate publisher and atomically switch the site. |
| `POST` | `/site` | Rebuild and validate only the site against the current publication. |
| `POST` | `/fleet-recover` | Recover stale fleet leases. |
| `POST` | `/recover-admin` | Recover a stale durable operator request. |
| `POST` | `/rebuild-compatibility-cohort` | Derive the older incompatible candidate cohort from the current preflight and rebuild it sequentially. |

Production writes operations to a durable Toolforge queue. A 512 MiB dispatcher
only validates, prioritizes, and routes requests; the existing fixed small and
medium workers execute them under the shared capacity-admission budget. History
source preparation uses a one-source download→validate→ingest→commit→delete
transaction and resumes from strict source receipts.
Local direct mode permits one active operation and returns `409 Conflict` for
another start while it is running.

## Authoritative operational status

`GET /status` includes a versioned `operationalTruth` object. It composes
existing durable pipeline evidence without reopening Parquet data:

- `_candidate-status/<wiki>.json` for the latest preparation outcome and
  resource measurements;
- `_ready-index/<wiki>.json` for authenticated ready and active candidate
  identities;
- `publication-gate.json` for the published metric proofs, cutoff dates, and
  selected snapshots;
- `_scrubs/status.json` for independent artifact verification;
- completed `source-plan.json` plus `remote-inventory.json` pairs for the
  latest known completed snapshots;
- the generated metric registry for the exact expected metric set.

It deliberately reports three independent domains: public-data health, update
pipeline health, and infrastructure capacity. A valid public release can
therefore remain green while a newer candidate is red. Metric readiness is an
exact `MetricId` comparison; file counts never establish completeness.

The admin pod cannot query the Toolforge Jobs API. Configured namespace and
job-request limits live in `config/toolforge-capacity.json`; the operational
snapshot combines those limits with durable active fleet/admin/publisher work
to explain known scheduling contention. This is configured capacity evidence,
not a claim to replace `toolforge jobs list` during break-glass diagnosis.

## Hosted auth model

The hosted mode intentionally avoids project-local user management:

- Wikimedia (via `meta.wikimedia.org`'s OAuth2 flow) authenticates the operator
- the repo only checks whether the returned username is in
  `WIKI_ECON_ADMIN_ALLOWED_USERNAMES`
- the allowlist is expected to come from deployment secrets, not git

The current intended pattern is to keep the allowlist and OAuth2 credentials
in deployment secrets (`toolforge envvars create` on Toolforge or the root-only
environment file on Cloud VPS) and inject them into the runtime environment
rather than committing them. GitHub Actions intentionally has no production
credentials.

Recommended secret names:

- `WIKI_ECON_ADMIN_ALLOWED_USERNAMES`
- `WIKI_ECON_ADMIN_SESSION_SECRET`
- `WIKI_ECON_ADMIN_MEDIAWIKI_CLIENT_ID`
- `WIKI_ECON_ADMIN_MEDIAWIKI_CLIENT_SECRET`
- `WIKI_ECON_ADMIN_PUBLIC_ORIGIN`

The repository ships `deploy/cloud-vps/render-env.sh` so a deployment job can
forward those values directly and atomically rewrite the VPS env file without
hand-editing the operator allowlist on disk.

## CSRF and session handling

- OAuth2 state is stored in a signed short-lived cookie.
- Successful logins issue a signed session cookie with `HttpOnly` and
  `SameSite=Lax`; production deployments also set `Secure`.
- Mutating API routes perform same-origin checks using `Origin` and `Referer`
  when auth is enabled.

## Why the admin server still binds to loopback

Even in hosted mode, the server remains loopback-only and relies on nginx to:

- terminate TLS
- publish `/admin`
- proxy `/admin-api/*`
- forward the canonical host/protocol headers

This keeps the server's trust boundary narrow and avoids exposing raw job
control on a directly routable socket.
