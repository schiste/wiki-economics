# Toolforge Deployment

wiki-economics can also run on Wikimedia Toolforge, as an alternative to the
Cloud VPS deployment documented in `docs/cloud-vps-deploy.md`. The shared
contract still applies: this changes orchestration, not pipeline logic —
`scripts/setup.sh`, `scripts/refresh.sh`, and `scripts/build-site.sh` are
reused unmodified.

Toolforge is a materially different platform from Cloud VPS: no root, no
systemd, no persistent VM. Containers run as Kubernetes Jobs/webservices,
and storage is NFS-backed under `/data/project/<tool>`. Toolforge's shared NFS
currently has no per-tool quota; its free space is shared capacity, not a
private reservation. Measure live headroom and keep reproducible data bounded.
The [generated lifecycle table](../../docs/generated/stack-reference.md#published-wiki-lifecycle)
is the source of truth for scheduled Toolforge datasets and paused imports.
frwiki completed the measured capacity qualification documented in
[`docs/benchmarking.md`](../../docs/benchmarking.md) and uses the qualified
256-logical-bucket envelope through the adaptive `small` profile. enwiki
remains excluded from production scheduling while its adaptive `large` profile
and higher resource envelope are being qualified.

## Files

- No Dockerfile: Toolforge's Build Service still builds the Node runtime,
  the single root npm workspace and lockfile, repository scripts, and `Procfile` into its supported
  Cloud Native Buildpack image. `RustConfig` sets `RUST_SKIP_BUILD=1`, so the
  detected Rust buildpack does not repeat the expensive Cargo build. Set the
  build-only variable `WIKI_ECON_BUILD_RUST=1` for an emergency manual build.
  Refresh jobs reuse the image's root `node_modules`; production deliberately
  fails instead of running `npm ci` over the network.
  Root `package.json` pins Node.js and npm exactly; the generated
  [stack reference](../../docs/generated/stack-reference.md) renders the
  values used by CI and local version-manager files. Observable builds
  copy the reviewed cache under `site/vendor/observable-cache` into a clean
  source tree and run with network APIs disabled.
- `.github/workflows/ci.yml` builds the release binary on an x86-64 Ubuntu
  24.04 runner after the quality, site, coverage, and security jobs pass (the
  Rust-only ones — `quality-rust`, `coverage`, `security-rust` — are skipped
  rather than run when a change touches only site content, per
  [CI Structure](../../docs/development.md#ci-structure)). It validates
  the ELF format, dynamic libraries, and `--help`, produces three CycloneDX
  SBOMs, notices, checksums, provenance and a GitHub artifact attestation,
  uploads the sealed 30-day artifact, and leaves deployment to an operator
  using the Toolforge SSH bastion.
- `deploy-binary.sh` verifies the GitHub attestation and sealed release locally,
  uploads its single archive to a staging path, and calls
  `install-binary.sh` as the tool account. Releases live at
  `/data/project/wiki-economics/app/releases/<git-sha>/wiki-econ`; the stable
  runtime path is `/data/project/wiki-economics/app/current/wiki-econ`.
  Each immutable release also retains its SBOMs, complete notices,
  `SHA256SUMS`, and `release-provenance.json`, tying all identities to exact
  Node, npm, Rust, browser-package, lockfile, OS, and shared-library versions
  observed by CI.
  `prune-releases.sh` verifies checksums and smoke tests before retaining the
  live release plus two known-good rollback releases. It also removes exact-SHA
  interrupted uploads after 24 hours. Both limits are configurable with
  `WIKI_ECON_RELEASE_RETENTION` and `WIKI_ECON_INCOMING_STALE_SECS`.
- `rollback-binary.sh` validates a retained release and atomically changes
  `current` without compiling or downloading anything.
- `rebuild-image.sh` rebuilds only the Toolforge image and restarts continuous
  processes. It uses detached JSON output and polls the exact build ID, so a
  disconnected log stream or concurrent build cannot produce a false result.
- `jobs.yaml` — one Rust fleet controller, two fixed small-wiki workers, one
  fixed medium/large worker, and the short `wiki-econ-publish-ready` job, plus
  legacy on-demand recovery jobs. The controller represents the sixteen
  scheduled wikis declared by the lifecycle registry: afwiki, arwiki, arzwiki,
  elwiki, eswiki, frwiki, hawiki, itwiki, jawiki, nlwiki, ptwiki, svwiki,
  swwiki, viwiki, yowiki, and zhwiki. Adding a wiki does not add a Toolforge
  Job definition. Monthly layouts remain isolated
  because no production isolated worker exists.
  The full state machine and recovery boundary are documented in
  [per-wiki candidate preparation and publication](../../docs/candidate-publication.md).
  `wiki-econ-admin` serves `/admin*` and the built static site as a separate
  buildservice webservice; it is not duplicated as a Toolforge Job.
  The retired overview page is not built or published. Requests for `/`,
  `/index`, and `/index.html` receive an HTTP redirect to the canonical
  all-wiki registered-editor yearly inequality view; the query parameters are
  explicit so a bookmarked root request remains reproducible as dashboard
  defaults evolve.
- `run-refresh-ingest.sh`, `run-refresh-compute.sh`, `run-refresh-site.sh` —
  thin wrappers, one per on-demand Job, that export
  `WIKI_ECON_REFRESH_STAGE` and exec `run-refresh.sh` unmodified. Toolforge
  Jobs Framework CLI 0.3.9 has no per-job envvar support (`jobs run` takes no
  `--envvar`/`--envvars` flag; `toolforge envvars create` is tool-wide only),
  so `jobs.yaml` points each on-demand Job's `command:` at its wrapper
  instead of setting the stage through an `envvars:` field.
- `run-fleet-controller.sh`, `run-fleet-worker.sh`, `run-prepare-wiki.sh`,
  `run-with-lock.sh`, `run-publish-ready.sh`, and
  `publish-ready-transaction.sh` implement the production schedule. Long
  preparation holds a per-wiki NFS-safe heartbeat lock and never changes a
  live pointer. Publication alone holds the global lock while selecting ready
  candidates, merging, validating, building, and switching the site.
  Fleet tasks use atomic NFS leases and heartbeats, bounded retries, and
  quarantine; see the [fleet scheduler](../../docs/fleet-scheduler.md).
- `run-refresh.sh` — wraps `scripts/refresh.sh` as an on-demand compatibility
  and recovery path; it is no longer the scheduled production path.
  Unlike Cloud VPS's `run-refresh.sh`, this does not keep a `releases/`
  history: retaining multiple full output generations consumes shared NFS
  unnecessarily. Parquet files are written to temporary siblings and
  renamed only after successful completion. The Observable site is built in
  a clean hidden sibling directory, then the stable `site-dist` symlink is
  atomically switched and the prior site release is removed. Raw `.bz2` dump
  cleanup happens inside the pipeline itself, not in this script. `wiki-econ
  run` processes `WIKI_ECON_SOURCE_WINDOW_SIZE` planned sources at a time
  (Toolforge defaults to one and rejects values outside 1–4). Each source is
  downloaded to pipeline-owned staging, stream-ingested, validated, committed
  with an atomic strict marker, and immediately deleted. This bounds compressed
  raw storage to the selected window instead of retaining the whole wiki dump.
  Run-qualified partial files are adopted and resumed after interruption;
  completed source markers let a restart continue at the first unfinished
  source. The candidate generation is selected only after the exact plan,
  marker inventory, Parquet footers, and row totals all validate.
  The [Rust resource governor](../../docs/resource-governor.md) independently
  caps concurrency and admits each source against cgroup memory, filesystem
  reserve, scratch, and file-descriptor signals. Thus a window of four is an
  upper bound, not a promise that four sources start when resources are tight.
  A new monthly snapshot is written beside the active generation and selected
  atomically. Candidate lifecycle receipts advance through `building`,
  `validated`, `ready`, `published`, `superseded`, and `retired`; the previous
  published candidate is retained as rollback material, while inactive input
  snapshots are removed only after the replacement site passes validation.
  This intentionally creates a bounded current-plus-candidate-plus-rollback
  storage peak during rollover. Malformed objects inside pipeline-owned
  staging namespaces are quarantined rather than deleted.
  `wiki-econ fetch` also
  preflights available disk space against the summed remote dump size
  before downloading anything, so insufficient shared headroom (e.g. for
  frwiki's ~31GB rollover estimate) fails fast instead of after partially
  downloading a large dump.
  Capacity benchmarks retain their compact JSON qualification report but
  remove isolated output and scratch data on exit. Refresh startup reaps only
  expired `capacity-*` staging directories, covering hard-killed benchmark
  pods that cannot run their exit trap.
  The executable performance, backup, rebuild, rollback, pointer, marker,
  compute, and site recovery procedures are in the
  [operations runbook](../../docs/operations-recovery.md). SLO and capacity
  thresholds come from versioned JSON policy rather than duplicated shell
  constants.
  Every refresh also carries a unique run ID through merge, semantic
  validation, and the two pre-publication receipt checks. The run ID is stored
  in `.refresh-status.json`, `.refresh-history.jsonl`, and
  `publication-gate.json`; see the [publication gate
  runbook](../../docs/publication-gate.md).
  Scheduled candidate jobs resolve the latest completed dump as a discovery
  step, compare it with current and ready-candidate fingerprints, and record
  `noOp: true` without downloading or computing when nothing changed. Partial
  fingerprint hits run only invalidated stages. The maximum
  fallback is controlled by `WIKI_ECON_MAX_SNAPSHOT_LAG_MONTHS` (default `2`);
  exceeding it fails closed. Receipt layout and invalidation rules are
  documented in [stage fingerprints](../../docs/stage-fingerprints.md).
  Completed source inventories are retained beside each snapshot plan, so a
  same-snapshot enwiki trigger probes only a possibly incomplete newest source
  and reuses the prior completed inventory instead of issuing hundreds of
  historical HEAD requests. The publisher consumes atomic per-wiki ready
  indexes and a receipt-backed publication digest; unchanged publication
  preparation returns before merge and site build.
  A separate `wiki-econ-artifact-scrub` job runs monthly under the publication
  lock and independently rereads every published Parquet, recomputes its
  semantic summary, and rehashes it against the canonical receipt. Reports and
  durable success/failure status are retained under `output/_scrubs/`; a failed
  or malformed status raises a public freshness alert and blocks later
  publication until a successful scrub. This job does not deploy or republish
  data.
  The one-CPU Toolforge job also defaults `RAYON_NUM_THREADS` and
  `POLARS_MAX_THREADS` to `1`, because the container currently sees eight host
  CPUs despite its one-CPU quota. Sequential raw/hash/ingest I/O uses Linux
  cache-discard hints after durable writes or completed reads so reproducible
  dump files do not consume the 6 GiB memory limit as retained page cache.
  `WIKI_ECON_WEEKLY_WORKERS` likewise defaults to `1`. The separate
  publication-invisible CPU matrix in `cpu-qualification-jobs.yaml` measures
  1/2/4-CPU profiles for nlwiki, ptwiki, and frwiki before any production
  default changes; see [benchmarking](../../docs/benchmarking.md#cpu-and-bounded-worker-qualification).
- `run-record.cjs` — the single atomic writer for live refresh status and the
  bounded terminal history. It folds Rust/site stage events together with
  cgroup, disk, deployment provenance, and publication-gate data.
- Each refresh writes `output/logs/refresh/<run-id>.log`, disables ANSI and
  Observable telemetry, and ends with structured per-stage/run JSON summaries.
  Logs and terminal history retain 104 weekly entries by default.
- `/health/freshness.json` exposes a credential-free health assessment. A
  read-only GitHub schedule checks it every six hours; deployments and full
  refreshes remain manual SSH/Toolforge operations and are never retried by
  that workflow.

### Candidate and publication locks

Scheduled preparation uses one lock per wiki under
`output/_prepare-locks/<wiki>.lock`; publication alone uses
`output/.publication.lock`. Both are atomic directory locks with owner JSON,
heartbeats, stable owner tokens, double-checked stale recovery, and exit status
75 for contention. This allows long wiki builds to overlap without allowing
two writers for the same wiki or two public release switches.

See [the candidate publication runbook](../../docs/candidate-publication.md)
for transaction state, rollback, and inspection commands.

### Legacy refresh single-flight lock

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

### On-demand stage jobs

`wiki-econ-refresh` can still run the full fetch → ingest → compute → merge →
site pipeline, which is unnecessary when only one part changed — a
`site/`-only or `docs/`-only commit still has to wait through a full
fetch/compute cycle before the new page is live. `jobs.yaml` also loads three
on-demand-only Jobs (no `schedule:`, so they never run on their own), each
pointed at its own `run-refresh-<stage>.sh` wrapper (see the `run-refresh.sh`
bullet above) which sets `WIKI_ECON_REFRESH_STAGE` and reuses `run-refresh.sh`
unmodified:

- `wiki-econ-ingest` — fetch, ingest, and patrol-fetch only, per wiki.
- `wiki-econ-compute` — compute, patrol-compute, and merge only, then
  publication-validate. Assumes a prior ingest already populated the
  selected generation; the existing stage-fingerprint checks fail closed otherwise.
- `wiki-econ-site` — the Observable production build only, against whatever
  a prior compute last published. `build-site.sh` re-verifies the
  publication gate itself, so this needs no wikis and no `run` invocation.
  Before Observable runs, the deployed Rust binary regenerates dashboard JSON
  into a site-private temporary overlay from the authenticated merged
  Parquets. This lets a dashboard-generator fix ship without mutating the
  receipt-covered publication tree or recomputing history metrics; the
  overlay is removed after the atomic site switch.

Trigger one manually the same way an operator manually fires the scheduled
refresh, with `toolforge jobs restart <name>`:

```sh
become wiki-economics toolforge jobs restart wiki-econ-site
```

All four legacy jobs — `wiki-econ-refresh` and the three on-demand
stage jobs — still serialize through `run-refresh.sh`'s single shared
`.refresh-lock` (see below), so a manual site rebuild can't race a concurrent
scheduled refresh or another on-demand job; it will simply exit `75` and wait
for the next attempt.

Each on-demand run produces its own independent `.refresh-status.json` /
`.refresh-history.jsonl` entry rather than one unified ingest→compute→site
record — deliberately, for now, to avoid unifying run-record/event-log state
across three separate job invocations. `currentStage`/`stageDurationsMs` in
that entry only cover the stage that actually ran.

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

Published redownloadable wikis use per-project retention from
`config/wiki-lifecycle.json`. Audit before applying it manually:

```bash
wiki_econ_run_cli retention-audit --lifecycle "$WIKI_ECON_WIKI_LIFECYCLE_FILE" nlwiki
wiki_econ_run_cli retention-apply --lifecycle "$WIKI_ECON_WIKI_LIFECYCLE_FILE" nlwiki
```

The scheduled publisher runs the same apply step only after a committed
publication or verified no-op. Never delete generation directories manually;
imported or otherwise irreplaceable projects must use retained inputs.

### ptwiki production qualification

ptwiki joined the weekly schedule after the 2026-08-23 qualification against
snapshot `2026-07` completed under the normal 6 GiB / one-CPU Toolforge job:

- the complete refresh, publication gate, Observable build, and snapshot
  finalization succeeded in 75m30s with no OOM or OOM-kill event;
- `page_weekly_edits` produced 40,092,138 rows and conserved 72,037,971 edits
  from 2001-06-04 through 2026-07-27; its largest stable bucket was 169,249
  rows;
- patrol parsing produced 11,356,093 patrol events and 10,347 rights events;
  the computed patrol metric contained 11,519 rows through 2026-08;
- the cache-controlled release separately hashed the 6.35 GB snapshot and
  parsed the 781 MB multi-member patrol gzip inside a 1 GiB qualification pod,
  then ingested the largest 5,299,634-row yearly source inside another 1 GiB
  pod. Both jobs exited successfully with zero memory-pressure events.

The lifecycle registry is the production selector. Do not retain a duplicate
`WIKI_ECON_REFRESH_WIKIS` or legacy `WIKI_ECON_ENABLED_WIKIS` value unless an
operator is intentionally running a one-off subset.

### frwiki production qualification

frwiki joined the Toolforge lifecycle after the 2026-08-24 qualification
against snapshot `2026-07`. Two fresh 256-bucket containers produced the same
119,855,668-row output and identical SHA-256 with 36.1–38.2% memory headroom.
The 512-bucket alternative retained only 15.4% headroom and the 1024-bucket
alternative reached the 6 GiB cgroup limit, so `run-refresh.sh` fails closed
unless `WIKI_ECON_WEEKLY_BUCKET_COUNT=256`.

The first complete Toolforge refresh passed the publication gate and public
freshness checks on 2026-08-24. The scheduled current generation is now
`2026-07`; the checksummed imported `2026-03` backup remains a disaster-recovery
fallback rather than the live rollback boundary. Detailed measurements and the
resource rationale are preserved in the
[frwiki capacity report](../../docs/frwiki-capacity-report-2026-08-24.md).

### Publication-invisible wiki qualification

For a wiki registered as `publication=hidden`, `refresh=qualification`, or an
already-published imported wiki that remains `refresh=paused`, run a complete
isolated preparation with a one-off 6 GiB/1-vCPU job:

```sh
toolforge jobs run --image tool-wiki-economics/tool-wiki-economics:latest \
  --command 'deploy/toolforge/run-qualify-wiki.sh itwiki' \
  --filelog --mount all --mem 6Gi --cpu 1 wiki-econ-qualify-itwiki
```

The wrapper writes only below
`/data/project/wiki-economics/capacity/qualifications`; it does not change a
production snapshot pointer, ready-candidate set, merged artifact, or site
symlink. Inspect `qualification.json` and the isolated run status before
starting capacity variants or considering lifecycle promotion.

To measure the changed-one-wiki `publication_prepare` SLO independently from
a full migration or no-op, use an on-demand 6 GiB/1-vCPU job:

```sh
toolforge jobs run --image tool-wiki-economics/tool-wiki-economics:latest \
  --command 'deploy/toolforge/run-publication-qualification.sh elwiki' \
  --filelog --mount all --mem 6Gi --cpu 1 wiki-econ-qualify-publication-elwiki
```

This qualification uses a hard-linked, run-scoped copy and an authentic
retained rollback/current candidate pair. It never switches the public data or
site. The report is retained below
`capacity/publication-qualifications/reports`; the large workspace is removed
even when the qualification fails.

If the retained baseline predates a schema migration in exactly one metric,
name that metric explicitly as the third argument, for example
`run-publication-qualification.sh itwiki prepare-itwiki-20260825T202727Z-7
gdp_activity_tiers.parquet`. The isolated baseline then uses the current
schema-compatible artifact for that family, records the overlay in its report,
and still measures the genuinely changed remaining families. Never use an
overlay to bypass a semantic or gate failure.

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
release_dir=$(mktemp -d)
deploy/toolforge/download-release.sh "$release_sha" "$release_dir"
TOOLFORGE_SSH_TARGET=login.toolforge.org \
  deploy/toolforge/deploy-binary.sh \
    "$release_dir/wiki-econ-release-$release_sha.tar.gz" \
    "$release_sha" \
    "$release_dir/wiki-econ-release-$release_sha.tar.gz.sha256"
```

The downloader requires a successful completed CI run whose `headSha` is the
requested commit. It discovers the archive through its strict `.sha256`
manifest and normalizes it into `release_dir`, so deployment does not depend
on the directory prefixes GitHub chooses while extracting a multi-root
artifact. Duplicate manifests, a missing declared archive, or a hash mismatch
fail before deployment begins. Pass a known workflow run ID as the optional
third argument when reproducing a specific run.

If online Sigstore root or attestation discovery is unavailable, download the
GitHub attestation bundle while the operator has network access and pass it as
the optional fourth argument. Verification remains fail-closed and covers the
same artifact digest and repository identity:

```sh
release_archive="$release_dir/wiki-econ-release-$release_sha.tar.gz"
(
  cd "$release_dir"
  gh attestation download "$release_archive" --repo schiste/wiki-economics
)
attestation_bundle=$(find "$release_dir" -maxdepth 1 -name 'sha256-*.jsonl' -print -quit)
TOOLFORGE_SSH_TARGET=login.toolforge.org \
  deploy/toolforge/deploy-binary.sh \
    "$release_archive" \
    "$release_sha" \
    "$release_archive.sha256" \
    "$attestation_bundle"
```

When site, Node, shared script, or Toolforge deployment files changed, rebuild
the lightweight image from the exact release commit (Cargo remains skipped).
Toolforge Build Service resolves named Git refs rather than detached commit
IDs, so publish a deterministic lightweight deployment tag. The helper
resolves that tag to the release SHA both before and after the build, then
verifies the completed build's source URL, ref, and immutable image digest
before restarting anything:

```sh
image_ref="toolforge-image-$release_sha"
git tag "$image_ref" "$release_sha"
git push origin "refs/tags/$image_ref"
ssh login.toolforge.org \
  "become wiki-economics bash -s -- 'https://github.com/schiste/wiki-economics.git' '$image_ref' '$release_sha'" \
  < deploy/toolforge/rebuild-image.sh
```

Run `deploy/toolforge/load-scheduled-jobs.sh deploy/toolforge/jobs.yaml` when
the job definitions change. Do not bulk-load the YAML directly: Toolforge CLI
0.3.9 starts every unscheduled definition as a one-off. The allowlisted loader
keeps the legacy refresh/stage definitions absent until an operator deliberately
loads one with `toolforge jobs load --job <name> ...`. The YAML uses the field
names emitted by `jobs dump`; inspect a fresh dump when upgrading the CLI. The
release artifact and checksum are
retained in GitHub for 30 days. NFS storage is bounded to three
attestation- and envelope-verified, smoke-tested releases by default: the live target and two
rollback candidates. Cleanup fails closed if the live symlink is malformed or
incomplete and ignores every directory that is not an exact 40-character
commit SHA.

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
for it — check the object storage user guide linked above for updates before
assuming shared NFS is still the only practical persistent-storage option.

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
