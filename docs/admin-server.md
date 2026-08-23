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
| `WIKI_ECON_ADMIN_SECURE_COOKIES` | `1` in production | Adds the `Secure` flag to auth cookies. |
| `WIKI_ECON_ADMIN_MEDIAWIKI_HOST` | `https://meta.wikimedia.org` | Base URL of the MediaWiki OAuth2 host. |
| `WIKI_ECON_ADMIN_MEDIAWIKI_CLIENT_ID` | empty | OAuth2 consumer client ID, from `Special:OAuthConsumerRegistration`. |
| `WIKI_ECON_ADMIN_MEDIAWIKI_CLIENT_SECRET` | empty | OAuth2 consumer client secret. |
| `WIKI_ECON_WIKI_LIFECYCLE_FILE` | `config/wiki-lifecycle.json` | Validated publication/refresh lifecycle registry. |
| `WIKI_ECON_ADMIN_PUBLIC_ORIGIN` | unset | Optional canonical external origin. If unset, the server derives it from `X-Forwarded-*` headers. |

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

The server accepts both the legacy local prefix and the hosted prefix:

- local/dev: `/api/*`
- hosted/proxied: `/admin-api/*`

Supported endpoints:

| Method | Path suffix | Purpose |
| --- | --- | --- |
| `GET` | `/status` | Returns current job state, logs, manifest age, freshness evaluation, auth state, and supported wiki list. |
| `POST` | `/fetch` | Run `wiki-econ fetch <wiki>`. |
| `POST` | `/ingest` | Run `wiki-econ ingest <wiki>`. |
| `POST` | `/compute` | Run `wiki-econ compute <wiki>`. |
| `POST` | `/merge` | Run `wiki-econ merge`. |
| `POST` | `/run` | Run the full `fetch → ingest → compute → merge` pipeline for a wiki. |
| `POST` | `/patrol-fetch` | Run `wiki-econ patrol-fetch <wiki>`. |
| `POST` | `/patrol-compute` | Run `wiki-econ patrol-compute <wiki>`. |
| `POST` | `/cleanup` | Remove `.tmp`, invalid marker files, and partial outputs for a wiki. |
| `POST` | `/cancel` | Cancel the current job. |

A new POST while a job is running returns `409 Conflict`; jobs are not queued.

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
