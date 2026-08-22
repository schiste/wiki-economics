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
  and leaves deployment to an operator using the Toolforge SSH bastion.
- `deploy-binary.sh` uploads to a staging path and calls
  `install-binary.sh` as the tool account. Releases live at
  `/data/project/wiki-economics/app/releases/<git-sha>/wiki-econ`; the stable
  runtime path is `/data/project/wiki-economics/app/current/wiki-econ`.
- `rollback-binary.sh` validates a retained release and atomically changes
  `current` without compiling or downloading anything.
- `rebuild-image.sh` rebuilds only the Toolforge image and restarts continuous
  processes. It uses detached JSON output and polls the exact build ID, so a
  disconnected log stream or concurrent build cannot produce a false result.
- `jobs.yaml` — the loadable `wiki-econ-refresh` scheduled Job definition.
  `wiki-econ-admin` serves `/admin*` and the built static site as a separate
  buildservice webservice; it is not duplicated as a Toolforge Job.
- `run-refresh.sh` — wraps `scripts/refresh.sh` for the scheduled job.
  Unlike Cloud VPS's `run-refresh.sh`, this does not keep a `releases/`
  history: retaining multiple full output generations is expensive against
  a small NFS quota. Parquet files are written to temporary siblings and
  renamed only after successful completion. The Observable site is built in
  a clean hidden sibling directory, then the stable `site-dist` symlink is
  atomically switched and the prior site release is removed. Raw `.bz2` dump
  cleanup happens inside the pipeline itself, not in this script:
  `wiki-econ run` deletes each wiki's raw dump
  immediately after that wiki's ingest stage succeeds (`src/main.rs`'s
  `Commands::Run` loop, backed by `fetch::cleanup_raw_dump`), rather than
  waiting for every wiki in the batch plus the site build to finish. This
  keeps the on-disk transient peak lower — the raw dump doesn't linger
  through the rest of that wiki's own compute/patrol stages, through every
  other wiki's full pipeline, and through the final merge. It's safe because
  generation-scoped ingest markers validate warehouse/analytical outputs,
  never the raw source file — later refreshes stay idempotent without it.
  A new monthly snapshot is written beside the active generation and selected
  atomically; the previous generation is retained through compute, merge, and
  site publication, then `snapshot-finalize` removes it. This intentionally
  creates a short-lived two-generation storage peak during rollover.
  `wiki-econ fetch` also
  preflights available disk space against the summed remote dump size
  before downloading anything, so an undersized quota (e.g. frwiki's ~31GB
  peak) fails fast instead of after partially downloading a large dump.
  Every refresh also carries a unique run ID through merge, semantic
  validation, and the two pre-publication receipt checks. The run ID is stored
  in `.refresh-status.json`, `.refresh-history.jsonl`, and
  `publication-gate.json`; see the [publication gate
  runbook](../../docs/publication-gate.md).
  Unpinned refreshes resolve the latest completed dump once for all scheduled
  wikis and reuse deterministic receipts for unchanged stages. The maximum
  fallback is controlled by `WIKI_ECON_MAX_SNAPSHOT_LAG_MONTHS` (default `2`);
  exceeding it fails closed. Receipt layout and invalidation rules are
  documented in [stage fingerprints](../../docs/stage-fingerprints.md).
- `run-record.cjs` — the single atomic writer for live refresh status and the
  bounded terminal history. It folds Rust/site stage events together with
  cgroup, disk, deployment provenance, and publication-gate data.

### Refresh single-flight lock

Every scheduled or manual Toolforge refresh must enter through
`deploy/toolforge/run-refresh.sh`. The wrapper acquires an atomic NFS
directory lock at `$WIKI_ECON_OUTPUT_DIR/.refresh-lock` before it resolves or
downloads a snapshot. A second refresh exits with status `75`; importantly,
that rejected run does not replace `.refresh-status.json`, so monitoring keeps
reporting the last pipeline run that actually owned the publication path.

The lock's `owner.json` records the run ID, UTC start time, PID, Toolforge job
identity, pod/process identity, selected snapshot, owner token, and heartbeat.
Snapshot resolution happens once after acquisition, and the resulting version
is written into the lock before the pipeline is invoked with an explicit
`--version`. This makes both the lock record and the whole refresh refer to the
same immutable input generation.

The heartbeat is updated every 60 seconds. A lock from another pod is eligible
for recovery only after six hours without a heartbeat, is observed stale a
second time, and retains the same owner token across both observations. A lock
owned in the current process scope also uses `kill(pid, 0)` to detect an exited
owner immediately. Missing or malformed metadata fails closed until the lock
directory itself exceeds the stale threshold. Inspect a live owner with:

```sh
become wiki-economics jq . /data/project/wiki-economics/output/.refresh-lock/owner.json
```

The safety windows can be tuned with
`WIKI_ECON_REFRESH_LOCK_HEARTBEAT_SECS`,
`WIKI_ECON_REFRESH_LOCK_STALE_SECS`, and
`WIKI_ECON_REFRESH_LOCK_RECHECK_SECS`. `WIKI_ECON_REFRESH_LOCK_DIR` is useful
only for isolated tests; production runs should share the default path.
`WIKI_ECON_JOB_IDENTITY` may provide a human-readable job label, while
`WIKI_ECON_PROCESS_IDENTITY` should remain unique to a pod or host if set.

The same heartbeat publishes a schema-versioned live run record immediately
after lock acquisition, including current stage, resource/provenance data, and
the selected snapshot. Terminal records add validated data summaries, failure
context, and the published site generation; 104 compact weekly entries are
retained by default. See the [refresh run record](../../docs/run-record.md) for
the field contract and operator checks.

## Runbook

### Operator prerequisites

Production deployment is intentionally operator-driven over SSH. GitHub
Actions builds and retains the Linux release artifact, but has no Toolforge
credentials and does not deploy or restart services. The operator needs:

- an SSH identity registered with Wikimedia and membership in the
  `wiki-economics` tool, so `become wiki-economics` succeeds;
- GitHub CLI access to download the release artifact produced for the exact
  `main` commit being deployed.

The following tool-wide variables must exist before loading or running the
refresh Job (repeat `toolforge envvars create` to update an existing value):

```sh
become wiki-economics toolforge envvars create WIKI_ECON_BIN /data/project/wiki-economics/app/current/wiki-econ
become wiki-economics toolforge envvars create WIKI_ECON_REFRESH_WIKIS nlwiki
become wiki-economics toolforge envvars create WIKI_ECON_DATA_DIR /data/project/wiki-economics/data
become wiki-economics toolforge envvars create WIKI_ECON_OUTPUT_DIR /data/project/wiki-economics/output
become wiki-economics toolforge envvars create WIKI_ECON_SITE_DIST_DIR /data/project/wiki-economics/site-dist
become wiki-economics toolforge envvars create WIKI_ECON_REFRESH_SCHEDULE '0 3 * * 0'
```

`WIKI_ECON_REFRESH_WIKIS` is an optional operational override and must agree
with `config/wiki-lifecycle.json`. If it is absent, the scheduled entries in
that registry are used. The legacy `WIKI_ECON_ENABLED_WIKIS` name remains a
backward-compatible alias. Paused imported wikis remain published and are not
cleanup candidates; see [wiki lifecycle management](../../docs/wiki-lifecycle.md).

### First cutover

1. Wait for the main-branch quality, coverage, security, and Toolforge release
   artifact jobs to pass. Deploy that exact commit using the normal-release
   commands below. The immutable binary is installed on NFS and
   `WIKI_ECON_BIN` remains
   `/data/project/wiki-economics/app/current/wiki-econ`.
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

After CI passes, download and deploy the Linux artifact for the exact main
commit from an operator workstation:

```sh
release_sha=$(git rev-parse origin/main)
run_id=$(gh run list --repo schiste/wiki-economics --commit "$release_sha" \
  --workflow CI --limit 1 --json databaseId --jq '.[0].databaseId')
release_dir=$(mktemp -d)
gh run download "$run_id" --repo schiste/wiki-economics \
  --name "wiki-econ-linux-x86_64-$release_sha" --dir "$release_dir"
chmod +x "$release_dir/wiki-econ"
TOOLFORGE_SSH_TARGET=login.toolforge.org \
  deploy/toolforge/deploy-binary.sh "$release_dir/wiki-econ" "$release_sha"
```

When site, Node, shared script, or Toolforge deployment files changed, rebuild
the lightweight image from `main` (Cargo remains skipped). Toolforge's Build
Service accepts named refs rather than raw commit SHAs, so verify `main` before
and after the build when pinning it to a release:

```sh
test "$(git ls-remote origin refs/heads/main | cut -f1)" = "$release_sha"
ssh login.toolforge.org \
  "become wiki-economics bash -s -- 'https://github.com/schiste/wiki-economics.git' main '$release_sha'" \
  < deploy/toolforge/rebuild-image.sh
test "$(git ls-remote origin refs/heads/main | cut -f1)" = "$release_sha"
```

Reload `deploy/toolforge/jobs.yaml` when the job definition changes. The file
uses the field names emitted by Toolforge CLI 0.3.9's `jobs dump`; inspect a
fresh dump when upgrading the CLI. The release artifact and checksum are
retained in GitHub for 30 days. NFS release directories are deliberately not
auto-deleted; automatic pruning can turn a quota issue into the loss of a
known-good rollback target.

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

For disaster recovery when the GitHub release artifact is unavailable, start
a manual Toolforge build with:

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
- **Toolforge Jobs YAML schema**: `jobs.yaml` round-trips the schema emitted
  by Toolforge CLI 0.3.9 (`mem`, not `memory`). Re-check `toolforge jobs dump`
  after CLI upgrades before loading it in production.
- **Admin-UI-triggered jobs**: `site/admin-server.cjs` can itself spawn
  `cargo run` / the compiled binary for on-demand fetch/ingest/compute runs
  from the admin UI. On Toolforge that spawns inside the `wiki-econ-admin`
  job's own container, competing for that job's (smaller) memory/CPU
  allocation rather than the refresh job's — confirm this is acceptable, or
  route admin-triggered runs through a separate one-off Toolforge Job.
