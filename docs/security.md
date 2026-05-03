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

The admin server now supports two explicit security postures.

### Local/dev posture

This remains the default developer experience:

- Binds only to `127.0.0.1`, never `0.0.0.0`. The bind address is
  hard-coded; there is no environment variable to expose it directly.
- Runs without a login flow when `WIKI_ECON_ADMIN_AUTH_MODE=none`.
- Accepts the local Observable preview origins by default
  (`127.0.0.1:3000`, `localhost:3000`) for CORS.

Accepted risks in that mode:

- Same-host XSS in a local page that is already on the allowlist.
- DNS rebinding against loopback-hosted development tooling.
- A rogue local process binding the admin port first.

These are acceptable only because the tool is assumed to be used by a
single operator on the same host.

### Hosted/VPS posture

The supported hosted deployment keeps the same loopback bind, but adds a
real authentication boundary in front of the operator surface:

- nginx publishes `/admin` and `/admin-api/*` and proxies them to the
  loopback-bound Node server.
- The Node server requires `WIKI_ECON_ADMIN_AUTH_MODE=oidc` when
  `WIKI_ECON_ENV=production`.
- Logins use an OpenID Connect authorization-code flow.
- The returned email address must be present in
  `WIKI_ECON_ADMIN_ALLOWED_EMAILS`.
- Sessions are stored in signed, short-lived cookies with `HttpOnly`,
  `SameSite=Lax`, and `Secure` in production.
- Mutating routes enforce same-origin checks using `Origin` and
  `Referer`.

That posture is intentionally simple:

- no project-side user database
- no in-repo list of operators
- no password auth
- no shell-string command execution

The intended secret sources are deployment secrets (for example GitHub
Actions secrets) rendered into `/etc/wiki-economics.env`.

Recommended practice:

- use secret names that match the runtime env vars exactly
- render them atomically with `deploy/cloud-vps/render-env.sh`
- treat `WIKI_ECON_ADMIN_PUBLIC_ORIGIN` as part of the trusted auth config,
  not as an incidental convenience

### What the admin server still protects against

- Casual local-network attackers (loopback bind).
- Cross-site request forgery against the hosted admin session
  (SameSite cookies plus origin checks).
- Command injection via the `wiki` parameter (sanitization + array-form
  `spawn`).
- Anonymous or unapproved logins in hosted mode (OIDC plus email
  allowlist).

### What this still does NOT protect against

- A compromised authenticated operator browser session.
- Misconfigured reverse proxy headers that cause the server to compute
  the wrong public origin for redirects or origin checks.
- Any identity provider that returns an email claim the project
  operator should not trust.

The hosted model therefore assumes:

- TLS termination is correct.
- nginx forwards canonical `Host` / `X-Forwarded-*` headers.
- the configured `WIKI_ECON_ADMIN_PUBLIC_ORIGIN` is correct when set.
- the chosen OIDC provider is one whose email claims you trust for
  operator identity.

## Why this is not Wikimedia OAuth today

The current hosted implementation uses generic OpenID Connect because
the requirement is an **email allowlist** sourced from deployment
secrets. That does not map cleanly to a Wikimedia-native login model:

- Wikimedia OAuth is a strong fit when you want username- or
  user-group-based authorization.
- It is not the right fit for a pure email-allowlist design.

If the project later wants Wikimedia-native operator auth, the natural
next step is a separate Wikimedia OAuth 2.0 mode that authorizes by
username or MediaWiki group rather than by email.

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
