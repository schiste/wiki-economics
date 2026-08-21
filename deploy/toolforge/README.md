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

- No Dockerfile: Toolforge's Build Service still builds the Node runtime,
  npm dependencies, repository scripts, and `Procfile` into its supported
  Cloud Native Buildpack image. `RustConfig` sets `RUST_SKIP_BUILD=1`, so the
  detected Rust buildpack does not repeat the expensive Cargo build. Set the
  build-only variable `WIKI_ECON_BUILD_RUST=1` for an emergency manual build.
- `.github/workflows/ci.yml` builds the release binary on an x86-64 Ubuntu
  24.04 runner after quality, coverage, and security jobs pass. It validates
  the ELF format, dynamic libraries, and `--help`, uploads a 30-day artifact,
  and deploys through the Toolforge SSH bastion.
- `deploy-binary.sh` uploads to a staging path and calls
  `install-binary.sh` as the tool account. Releases live at
  `/data/project/wiki-economics/app/releases/<git-sha>/wiki-econ`; the stable
  runtime path is `/data/project/wiki-economics/app/current/wiki-econ`.
- `rollback-binary.sh` validates a retained release and atomically changes
  `current` without compiling or downloading anything.
- `rebuild-image.sh` rebuilds only the Toolforge image and restarts continuous
  processes. It uses detached JSON output and polls the exact build ID, so a
  disconnected log stream or concurrent build cannot produce a false result.
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

### One-time GitHub setup

Create a protected GitHub environment named `toolforge`, ideally with a
required reviewer, and add these environment secrets before merging the
deployment workflow:

- `TOOLFORGE_SSH_USER`: a Toolforge member's Developer account username.
- `TOOLFORGE_SSH_PRIVATE_KEY`: a dedicated SSH private key whose public key
  is registered for that Developer account. Do not reuse a personal default
  key if a narrowly scoped CI key can be registered.
- `TOOLFORGE_KNOWN_HOSTS`: the verified `login.toolforge.org` host-key lines.
  Copy known-good entries from an operator's `~/.ssh/known_hosts`; do not
  disable strict host-key checking in CI.

The corresponding user must be a member of the `wiki-economics` tool so
`become wiki-economics` succeeds. GitHub CLI configuration is optional; the
workflow uses the standard SSH client.

### First cutover

1. Merge only after the `toolforge` environment and all three secrets exist.
   The main-branch workflow waits for quality, coverage, and security checks,
   builds the Linux release, installs it on NFS, changes `WIKI_ECON_BIN` to
   `/data/project/wiki-economics/app/current/wiki-econ`, and restarts the
   webservice. It then rebuilds the source image at the exact Git SHA; Cargo
   is skipped in that Toolforge build.
2. Confirm the release and environment from the Toolforge bastion:

   ```sh
   become wiki-economics ls -l /data/project/wiki-economics/app/current
   become wiki-economics toolforge envvars show WIKI_ECON_BIN
   become wiki-economics /data/project/wiki-economics/app/current/wiki-econ --help
   become wiki-economics toolforge webservice status
   ```

3. Run `wiki-econ-refresh` manually, confirm the expected Parquet/site
   outputs, then run it a second time to verify marker-based idempotency.

### Normal releases

Every push to `main` is classified by changed path after CI passes:

- Rust inputs (`src/`, Cargo files, toolchain config, or `vendor/`) build and
  deploy a new binary using a persistent GitHub release cache.
- Node, site, shared script, `RustConfig`, or Toolforge deployment changes
  trigger the lightweight Toolforge image build and process restart.
- Documentation-only and unrelated CI changes perform no deployment work.

The release artifact and checksum are retained in GitHub for 30 days. NFS
release directories are deliberately not auto-deleted; automatic pruning can
turn a quota issue into the loss of a known-good rollback target.

### Rollback

List retained SHAs, switch to one, and restart the webservice:

```sh
ssh login.toolforge.org 'become wiki-economics find /data/project/wiki-economics/app/releases -mindepth 1 -maxdepth 1 -type d -printf "%f\n"'
ssh login.toolforge.org 'become wiki-economics bash /workspace/deploy/toolforge/rollback-binary.sh <40-character-sha>'
ssh login.toolforge.org 'become wiki-economics toolforge webservice restart'
```

If `/workspace` does not contain the desired script version, stream the local
copy instead:

```sh
ssh login.toolforge.org 'become wiki-economics bash -s -- <40-character-sha>' \
  < deploy/toolforge/rollback-binary.sh
ssh login.toolforge.org 'become wiki-economics toolforge webservice restart'
```

For disaster recovery when GitHub deployment is unavailable, start a manual
Toolforge build with:

```sh
toolforge build start --envvar WIKI_ECON_BUILD_RUST=1 \
  --ref main https://github.com/schiste/wiki-economics.git
```

That restores the old cold-build behavior and is expected to take
substantially longer.

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

- **Runner/runtime ABI**: the GitHub job intentionally uses Ubuntu 24.04
  x86-64, matching the current Toolforge runtime and OpenSSL 3. Re-check the
  binary with `file`, `ldd`, and `--help` after either platform changes its
  base image; the workflow performs the same checks before every deploy.
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
