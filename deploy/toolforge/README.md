# Toolforge Deployment

wiki-economics can also run on Wikimedia Toolforge, as an alternative to the
Cloud VPS deployment documented in `docs/cloud-vps-deploy.md`. The shared
contract still applies: this changes orchestration, not pipeline logic —
`scripts/setup.sh`, `scripts/refresh.sh`, and `scripts/build-site.sh` are
reused unmodified.

Toolforge is a materially different platform from Cloud VPS: no root, no
systemd, no persistent VM. Containers run as Kubernetes Jobs/webservices,
and storage is NFS-backed under `/data/project/<tool>` with a small default
quota — do not assume Cloud VPS's storage headroom carries over. Start with
**nlwiki only** (steady-state ≈2.76GB, first-backfill peak ≈8.71GB); add
frwiki (steady-state ≈9.8GB, peak ≈31GB) only after a quota increase sized
off its peak, not its steady-state footprint. enwiki is out of scope for
Toolforge entirely.

## Files

- No Dockerfile: Toolforge's Build Service only supports Cloud Native
  Buildpacks (`toolforge build start` does not accept a Dockerfile, and
  there's no documented path to reference a custom-built Docker image from
  `toolforge jobs`/`toolforge webservice`). The image is instead built by
  buildpack auto-detection from three files at the **repo root**:
  `Cargo.toml` (Rust CLI, detected by the Rust buildpack), `package.json` +
  `package-lock.json` (Node — `@observablehq/framework` and `duckdb`,
  needed by `scripts/build-site.sh` and the `site/data-build/*.cjs`
  generators; the `site/data-build/*.cjs` generators query Parquet through
  the `duckdb` npm package's Node bindings, so no separate DuckDB CLI is
  needed), and `Procfile` (`web: node site/admin-server.cjs`, which
  `admin-server.cjs` itself needs zero npm dependencies to run — it only
  uses Node built-ins + local `./admin-auth.cjs`). No Python — `src/patrol.rs`
  implements fetch/compute patrol natively; the Python scripts under
  `scripts/` are only used by `scripts/ci-local.sh`, not the production
  refresh path.

  **This combination is the open, untested question this deployment path
  answers.** Toolforge documents "Node + one other language" as a general,
  supported Build Service feature (the builder auto-injects the `nodejs`
  buildpack ahead of the primary-language buildpack — this isn't something
  configured via a `project.toml`), and Rust is a separately documented
  Toolforge buildpack. Whether the two combine cleanly in one image has not
  been verified end-to-end against a real Toolforge build — step 4 below is
  the actual test. If it fails, the fallback is splitting into two images
  (a Rust-only image for the CLI/refresh job, a Node-only image for the
  admin webservice, coordinating through the shared NFS data/output dirs)
  rather than reintroducing a Dockerfile, which Toolforge cannot consume.
- `jobs.yaml` — Toolforge Jobs definitions: `wiki-econ-admin` (continuous,
  serves `/admin*` and the built static site on one process/port — Toolforge
  has no per-tool nginx layer) and `wiki-econ-refresh` (scheduled, runs the
  pipeline and rebuilds the site).
- `run-refresh.sh` — wraps `scripts/refresh.sh` for the scheduled job.
  Unlike Cloud VPS's `run-refresh.sh`, this does not keep a `releases/`
  history: retaining multiple full output generations is expensive against
  a small NFS quota. Raw `.bz2` dump cleanup happens inside the pipeline
  itself, not in this script: `wiki-econ run` deletes each wiki's raw dump
  immediately after that wiki's ingest stage succeeds (`src/main.rs`'s
  `Commands::Run` loop, backed by `fetch::cleanup_raw_dump`), rather than
  waiting for every wiki in the batch plus the site build to finish. This
  keeps the on-disk transient peak lower — the raw dump doesn't linger
  through the rest of that wiki's own compute/patrol stages, through every
  other wiki's full pipeline, and through the final merge. It's safe because
  `src/storage.rs::marker_manifest_is_valid` only checks that the
  warehouse/analytical parquet outputs exist, never the raw source file —
  later refreshes stay idempotent without it. `wiki-econ fetch` also
  preflights available disk space against the summed remote dump size
  before downloading anything, so an undersized quota (e.g. frwiki's ~31GB
  peak) fails fast instead of after partially downloading a large dump.

## Runbook

None of the following can be run without a live Toolforge tool account, so
treat this as an operator checklist rather than something already executed:

1. **Request a tool account** and file a Phabricator "Toolforge (Quota
   requests)" ticket for storage, sized off the first wiki's transient peak
   (nlwiki ≈8.71GB; add frwiki's ≈31GB peak later once nlwiki is validated).
2. **Register a MediaWiki OAuth2 consumer** on `meta.wikimedia.org` via
   `Special:OAuthConsumerRegistration/propose`. Use OAuth2, set the callback
   URL to the exact production URL
   (`https://<tool-domain>/admin/oauth/callback` — must match exactly, no
   trailing slash differences), and pick "This consumer is for use only by
   ‎<your username>" (owner-only) so it's usable immediately without
   waiting on admin approval. This yields a client ID and, since it's a
   confidential client, a client secret (no PKCE needed).
3. **Store secrets**: `toolforge envvars create <NAME> <VALUE>` for
   `WIKI_ECON_ADMIN_MEDIAWIKI_CLIENT_ID`,
   `WIKI_ECON_ADMIN_MEDIAWIKI_CLIENT_SECRET`,
   `WIKI_ECON_ADMIN_SESSION_SECRET`, `WIKI_ECON_ADMIN_ALLOWED_USERNAMES`
   (comma/newline-separated Wikimedia usernames, case-sensitive),
   `WIKI_ECON_ADMIN_PUBLIC_ORIGIN` — same variable names
   `site/admin-server.cjs` already reads from `process.env`, see
   `deploy/cloud-vps/env.example` for the full list and expected shape.
   `WIKI_ECON_ADMIN_MEDIAWIKI_HOST` defaults to `https://meta.wikimedia.org`
   and only needs to be set as an envvar if that ever changes.
4. **Build the image**: `toolforge build start <repo-url>` (buildpack
   auto-detection from the repo root's `Cargo.toml` + `package.json` +
   `Procfile` — see "Files" above for why this combination is unverified).
   Watch the build logs closely: confirm both a Rust buildpack step and a
   Node buildpack step run, and that the final image actually contains a
   working `wiki-econ` binary on `PATH` alongside a Node runtime. If the
   build fails to combine the two runtimes, fall back to splitting into
   two separate buildpack-built images (Rust-only for the CLI/refresh job,
   Node-only for the admin webservice) rather than reaching for a
   Dockerfile, which Toolforge's Build Service cannot consume. Confirm the
   actual `toolforge build start` invocation against current Toolforge
   docs — the CLI surface changes over time.

   **For every rebuild after the first** (i.e. once `jobs.yaml` is loaded
   and the admin webservice is started per steps 6-7), use
   `deploy/toolforge/rebuild-image.sh` instead of a bare `toolforge build
   start`. Pushing a new image under `:latest` does not affect pods already
   running — Kubernetes env/binaries are fixed at container start — so a
   rebuild that isn't paired with an explicit restart silently leaves the
   old binary running indefinitely (this is exactly what caused a `spawn
   cargo ENOENT` incident: the admin webservice's pod predated
   `WIKI_ECON_BIN` being set tool-wide, and nothing restarted it after the
   image that would have fixed the fallback was pushed). The script polls
   `toolforge build list` for true build completion (working around
   `toolforge build start`'s own flaky log stream, which can disconnect
   with `ChunkedEncodingError` and exit 0 mid-build) and then restarts the
   admin webservice — and defensively, any Toolforge Job actually running
   as `continuous` — so the new image takes effect everywhere automatically.
5. **Point scripts at the compiled binary**: the buildpack run image ships
   no Rust toolchain — only the compiled `wiki-econ` binary, at
   `/workspace/target/release/wiki-econ`. `scripts/lib/wiki_econ.sh` (used
   by `run-refresh.sh` and every job/cron entrypoint) falls back to `cargo
   run --release --` when `WIKI_ECON_BIN` is unset, which fails on
   Toolforge since `cargo` isn't present. Set it once, tool-wide, before
   running any job: `toolforge envvars create WIKI_ECON_BIN
   /workspace/target/release/wiki-econ` (confirm the actual in-image path
   from the build logs — buildpack layer paths can change across
   `pack`/heroku-buildpack versions).
6. **Fill in `jobs.yaml`**: run `toolforge jobs images` to get the short
   image name the build produced, replace the `<TOOL_IMAGE>` placeholders
   in `jobs.yaml` with it, then `toolforge jobs load
   deploy/toolforge/jobs.yaml`.
7. **Start the admin webservice** (`wiki-econ-admin`) and confirm `/admin`
   and a static site asset both resolve on the assigned Toolforge domain.
8. **Trigger `wiki-econ-refresh` once manually** (`toolforge jobs run
   wiki-econ-refresh` or equivalent), then confirm: parquet outputs appear
   under `WIKI_ECON_OUTPUT_DIR`, the site builds under
   `WIKI_ECON_SITE_DIST_DIR`, the raw `.bz2` files are deleted afterward,
   and NFS usage stays under the granted quota. Re-run once more to confirm
   idempotency (marker manifest still validates without the raw file
   present).
9. **Measure real numbers** this plan couldn't produce without live access:
   peak RSS during ingest against the job's memory limit (`jobs.yaml`
   currently requests 4Gi for the refresh job, adjustable up to Toolforge's
   per-job ceiling), actual wall-clock refresh time (to size the cron
   schedule), and the patrol data (`data/patrol/<wiki>`) storage footprint,
   which was never measured empirically this session.
10. Once nlwiki is verified end-to-end, repeat for frwiki (after the quota
    bump), then decommission the Cloud VPS deployment.

## Object storage was considered and rejected for now

Cloud VPS/Toolforge also offers an S3/Swift-compatible object storage
service (Ceph rados gateway,
https://wikitech.wikimedia.org/wiki/Help:Object_storage_user_guide) that
could in principle hold raw dumps or parquet outputs off NFS. It isn't used
here:

- As of this writing, "there's no specific way for Toolforge users to use
  the object storage service" — it's provisioned at the Cloud VPS *project*
  level via Keystone/OpenStack credentials (`openstack ec2 credential
  create` + an `object_storage` role), not per-Toolforge-tool. Using it
  would mean requesting a separate Cloud VPS project just to hold
  credentials, on top of the Toolforge tool account.
- Its own default quota is 4096 objects / 8GB total — smaller than nlwiki's
  own transient peak (≈8.71GB) and far smaller than frwiki's (≈31GB), so it
  wouldn't remove the need for a quota-increase request anyway.
- It isn't backed up and uses a less redundant erasure-coding scheme than
  Cinder/VM storage — acceptable here in principle since raw dumps and
  parquet outputs are both regenerable, but it doesn't reduce the
  Phabricator quota-request step, just moves it.

Revisit this if Wikimedia ships a dedicated Toolforge-native integration
for it — check the object storage user guide linked above for updates
before assuming NFS quota is still the only lever.

## Open risks worth re-checking before relying on this

- **Rust + Node in one buildpack image**: see "Files" above — this is the
  single biggest open question and step 4 of the runbook is the actual
  test. No Dockerfile fallback exists on this platform; the fallback if
  buildpacks can't combine both runtimes is two separate images.
- **Non-root UID**: buildpack-built images don't pin a specific UID/GID —
  Toolforge assigns this at the Kubernetes level. If file permissions on
  mounted NFS paths misbehave, this is the first place to look.
- **Toolforge Jobs YAML schema**: `jobs.yaml` here is written from current
  documentation and may not match the exact schema Toolforge expects
  (field names, whether `port:` is valid for a `continuous` job, etc.) —
  validate with `toolforge jobs load --dry-run` (or equivalent) before a
  real load.
- **Admin-UI-triggered jobs**: `site/admin-server.cjs` can itself spawn
  `cargo run` / the compiled binary for on-demand fetch/ingest/compute runs
  from the admin UI. On Toolforge that spawns inside the `wiki-econ-admin`
  job's own container, competing for that job's (smaller) memory/CPU
  allocation rather than the refresh job's — confirm this is acceptable, or
  route admin-triggered runs through a separate one-off Toolforge Job.
