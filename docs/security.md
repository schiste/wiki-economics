# Security Model

This document complements [`SECURITY.md`](../SECURITY.md). `SECURITY.md`
describes how to *report* a vulnerability; this file describes the
*current threat model* of the deployed surface and the design path for
extensions that affect that model.

For a triage of past audit findings and their resolutions, the public
issue tracker is the canonical record.

## Surfaces

The repository ships three runtime surfaces:

1. **The Rust CLI (`wiki-econ`).** Reads from `dumps.wikimedia.org`,
   writes to local disk. No inbound surface.
2. **The Node admin server (`site/admin-server.cjs`).** Operator-facing
   dev tool exposing job-control endpoints over loopback HTTP.
3. **The Observable Framework dashboard (`site/`).** Static pages served
   from a CDN/nginx in production; dev preview in development.

## Admin server threat model

**Current posture (the only deployed configuration we support):**

- Binds only to `127.0.0.1`, never `0.0.0.0`. The bind address is
  hard-coded; there is no environment variable to expose it.
- Disabled in the production runtime: the systemd unit does not start
  the server, and `WIKI_ECON_ADMIN_ENABLED` defaults to `0` whenever
  `WIKI_ECON_ENV=production`. The nginx config in
  [`deploy/cloud-vps/nginx/wiki-economics.conf`](../deploy/cloud-vps/nginx/wiki-economics.conf)
  also returns 404 for `/admin`.
- The CORS allowlist is the secondary defense layer: only origins
  matching `WIKI_ECON_ALLOWED_ORIGINS` (default: `127.0.0.1:3000`,
  `localhost:3000`) get a CORS header.
- Every action is dispatched via `child_process.spawn(program, [args])`
  with an explicit allowlist of action names. No code path reaches
  `bash -c` or any shell-string interpretation.
- The `wiki` parameter is sanitized to `[a-z0-9_]` before reaching
  `spawn`.

**What this protects against:**

- Casual local-network attackers (the loopback bind).
- Cross-origin request forgery from a non-allowlisted page (the CORS
  allowlist).
- Command injection via the `wiki` parameter (sanitization + array-form
  spawn).

**What this does NOT protect against:**

- Same-host XSS in any page served from `127.0.0.1:3000`. Such a page is
  in the CORS allowlist by default and can fire mutations.
- DNS-rebinding attacks that rebind a hostname to `127.0.0.1` to bypass
  CORS. Origin checking on the server is the line of defense; the
  Origin header is not trusted to be authentic.
- A rogue local process binding the admin port before the official
  server. The admin server fails closed if `bind` returns `EADDRINUSE`,
  but a process that binds first wins until the operator reboots.

These are accepted risks for a *dev/operator tool that runs on the same
host as the operator*. They become unacceptable the moment the admin
surface is reachable from another host.

## Forward path: Wikimedia OAuth 2.0

If the admin surface ever needs remote access (multi-operator workflow,
hosted dashboard with privileged actions, batch-trigger from a
collaboration tool), the right authentication path is **Wikimedia OAuth
2.0** rather than a project-local pre-shared secret.

Why Wikimedia OAuth:

- The likely operator population is already authenticated to Wikimedia
  (they are Wikipedia/Wikimedia community members or research
  collaborators).
- Wikimedia OAuth 2.0 supports user-group gating, which lets the admin
  surface restrict to a designated MediaWiki user group rather than
  manage a separate identity store.
- The dependency model is unidirectional: the project depends on
  Wikimedia identity, never the other way around. No project-side user
  database to hold or breach.

Sketch of the integration (do not implement until there is a deployment
need; the present production surface runs with admin disabled):

1. Register a consumer at
   <https://meta.wikimedia.org/wiki/Special:OAuthConsumerRegistration>.
   Request the minimum scope set, which for a read-mostly admin surface
   is typically `mwoauth-authonly`.
2. Add an OAuth callback endpoint (`/oauth/callback`) to
   `site/admin-server.cjs` that exchanges the authorization code for an
   access token via
   `https://meta.wikimedia.org/w/rest.php/oauth2/access_token`.
3. Validate access tokens by calling
   `https://meta.wikimedia.org/w/rest.php/oauth2/resource/profile` and
   reading `username` and `groups`.
4. Gate POST endpoints on either an explicit username allowlist
   maintained in `WIKI_ECON_ADMIN_USERS` or membership in a designated
   MediaWiki user group.
5. Issue a short-lived session cookie with `Secure; HttpOnly; SameSite=Lax`
   so subsequent requests do not need to round-trip OAuth.

Until that work happens, the admin surface stays loopback-only.

## Fetch surface

The fetch path is hardened against three concrete threats:

- **Off-host redirects** are rejected at the redirect-policy layer in
  `src/fetch.rs`. `dumps_host_only_redirect_policy` follows redirects
  only when the target host is `dumps.wikimedia.org`. Patrol downloads
  use the same policy.
- **CDN truncation, HTML-error-page-as-200, and corruption** are caught
  by post-download magic-byte validation: `verify_bz2_magic` for the
  TSV dumps, `verify_gzip_magic` for the patrol XML logs. A failed check
  removes the corrupt file and bubbles the error.
- **Untrusted CA chains** are excluded by `reqwest`'s default TLS stack,
  which uses the platform CA store.

What we **cannot** verify, because the upstream does not publish it:
end-to-end SHA1/MD5 of each downloaded file. The
`/other/mediawiki_history/` dump path on `dumps.wikimedia.org` does not
publish `dumpstatus.json`, `sha1sums.txt`, or `md5sums.txt`. This is an
upstream limitation; if Wikimedia begins publishing checksums for this
dump path the right move is to add cryptographic verification on top of
the magic-byte check.

## Schema drift on the TSV ingest path

`src/ingest.rs` reads MediaWiki history TSVs by *position*, not by
header — the dumps are headerless. `INGEST_COLUMNS` (in `src/schema.rs`)
defines which columns we keep, and the upstream column order is assumed
stable. If Wikimedia inserts a column before an existing one, we will
silently miscolumn the rows. Today this is acceptable because:

- The upstream schema has been stable for many years.
- Compute fails loudly when a downstream type-cast hits unexpected data.
- The marker-file invariant prevents partial outputs from being
  consumed.

If this changes, a schema fingerprint check at ingest time (hash the
expected column-name list against a documented constant) is the
hardening to add. We have not added it yet because the runtime cost
is non-zero on every ingest run and the threat is hypothetical.

## Supply chain

- `cargo deny check advisories bans licenses sources` runs on every CI
  push.
- `cargo audit -D warnings` runs on every CI push.
- Third-party GitHub Actions are pinned to commit SHAs (see
  [`ci.yml`](../.github/workflows/ci.yml)). Refresh the SHAs
  deliberately; do not switch back to floating `@v4` tags.
- The vendored `polars-utils` patch under `vendor/polars-utils` is
  documented in [`PATCHES.md`](../vendor/polars-utils/PATCHES.md). The
  `scripts/check_vendor_polars.sh` step in CI fails when that document
  is missing or stale.
