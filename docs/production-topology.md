# Production topology

This is the contributor-facing map of the system that runs on Wikimedia
Toolforge. It describes what is deployed, which component owns each stage, how
persistent storage is laid out, and how to reproduce the deployment. Detailed
operator commands and rollback procedures remain in the
[Toolforge runbook](../deploy/toolforge/README.md).

Exact dependency versions and dataset lifecycle states are generated from
their authoritative manifests in the
[stack reference](generated/stack-reference.md). Do not copy those values into
this narrative document.

## Runtime ownership

| Concern | Production implementation | Persistent result |
| --- | --- | --- |
| Snapshot resolution and history fetch | Rust `wiki-econ` | raw Wikimedia dump shards, then strict fetch receipts |
| History ingest | Rust + Polars | immutable qualified metric-input Parquet generations |
| Core metric compute | Rust + Polars | per-wiki Parquet metrics |
| Patrol/rights fetch and compute | Rust streaming multi-member gzip/XML path + Polars | parsed patrol/right Parquet and per-wiki patrol metrics |
| Cross-wiki merge and dashboard defaults | Rust + Polars | merged Parquet, `defaults_*.json`, and `meta_*.json` |
| Publication manifest | checked-in Node manifest validator invoked by Rust merge | atomic `manifest.json` with licensing and provenance |
| Site compile | Observable Framework in the Toolforge Build Service image | staged static HTML, JavaScript, CSS, WASM, and data attachments |
| Browser query | Apache Arrow + parquet-wasm | in-browser filtering of published Parquet data |
| Public/admin service | Node `site/admin-server.cjs` from `Procfile` | serves the current static site and authenticated admin endpoints |

DuckDB is not in the production server or browser query path. Python is not a
pipeline runtime: the only retained Python code is the standard-library LCOV
coverage checker and its tests. There is no PyArrow dependency.

## Refresh and publication flow

```text
per-wiki preparation Jobs
  -> per-wiki NFS lock + live run record
  -> resolve one completed snapshot for that wiki
  -> fetch -> ingest immutable generation -> select generation
  -> core compute -> patrol fetch/compute
  -> semantic candidate validation -> mark candidate ready

short publisher Job
  -> global publication lock
  -> select the complete set of ready/current wiki generations
  -> merge + Rust dashboard defaults + validated manifest
  -> fail-closed semantic publication gate
  -> offline Observable build in a run-scoped hidden directory
  -> atomic site-dist symlink switch
  -> retire superseded generations
  -> retain status/history and prune bounded stale/release artifacts
```

Every stage receives the same run ID and selected snapshot. Stage receipts
permit unchanged work to be reused; explicit algorithm/schema versions force a
recompute even when inputs are unchanged. The site cannot switch unless the
publication receipt still matches immediately before publication. See
[stage fingerprints](stage-fingerprints.md), the
[publication gate](publication-gate.md), and the
[run record](run-record.md) for those contracts.

## Toolforge processes

There are independent Kubernetes workloads sharing the tool account's NFS
mount:

- `wiki-econ-prepare-elwiki`, `wiki-econ-prepare-frwiki`,
  `wiki-econ-prepare-itwiki`, `wiki-econ-prepare-nlwiki`,
  `wiki-econ-prepare-ptwiki`, and `wiki-econ-prepare-svwiki` are the scheduled
  per-wiki preparation Jobs defined in `deploy/toolforge/jobs.yaml`. Each owns
  only its wiki's candidate-generation paths and may run without blocking other
  wikis or the public site.
- `wiki-econ-publish-ready` is the short scheduled publisher. It alone acquires
  the global publication lock and mutates merged output and the live site.
- `wiki-econ-refresh` is retained as an unscheduled, on-demand compatibility
  Job for explicit recovery or operator drills; it is not the normal scheduler.
- `wiki-econ-admin` is a Build Service webservice launched from `Procfile`. It
  serves the current static site and reads the refresh status files. It has no
  shared process memory or Kubernetes control API with the pipeline Jobs.

The lifecycle registry—not a deployment-script wiki list—is authoritative.
The [generated lifecycle table](generated/stack-reference.md#published-wiki-lifecycle)
shows the current split: scheduled Toolforge datasets refresh weekly, while
paused local imports remain published and retained until their deliberate
reactivation procedure completes.

## Persistent storage

All production state is under `/data/project/wiki-economics` on shared NFS:

```text
app/
  incoming/                         verified upload staging
  releases/<git-sha>/               immutable binary + SBOM/provenance envelope
  current -> releases/<git-sha>/    active Rust binary

data/
  raw/<wiki>/                       transient history dumps, deleted after ingest
  patrol/<wiki>/                    logging source and parsed patrol/right data
  metric-input/<wiki>/_snapshots/<snapshot>/
    _compacted/year=<YYYY>/year_month=<YYYY-MM>/
  warehouse/, parquet/              schema-v1 rollback generations only
  snapshots/<wiki>/current-snapshot.json
  stages/                           fetch/ingest stage receipts

output/
  <wiki>/                           per-wiki metric outputs
  *.parquet                         current merged metrics
  defaults_*.json, meta_*.json      Rust dashboard artifacts
  manifest.json                     publication/provenance manifest
  publication-gate.json             current run's semantic receipt
  _stages/                          compute/merge/site receipts
  .refresh-status.json              atomically updated live status
  .refresh-history.jsonl            bounded terminal history
  logs/refresh/<run-id>.log         per-run file log

site-dist -> .site-dist.build.<run-id>.*
capacity/                            isolated qualification reports/staging
```

The active snapshot pointer selects exactly one immutable generation and its
manifest selects one storage layout. New logical schema-v2 generations contain
only the qualified metric-input layer; after transactional compaction their
generation-manifest schema is 3 and its authenticated allowlist points only at
`_compacted` fragments. Schema-v1 rollback generations remain readable.
During rollover the prior generation remains available until compute, merge,
validation, and the site switch succeed. Raw dumps are then disposable because
strict ingest markers or the receipt-authenticated compaction manifest prove
every source transaction. Cleanup retains the published candidate, one
rollback candidate, and one resumable building/validated candidate per wiki;
it removes only lifecycle-owned retired or expired paths and never follows or
deletes the live site symlink target.

Scratch is configurable with `WIKI_ECON_SCRATCH_DIR`; production capacity and
free-space gates must account for raw transport, the temporary second data
generation, compute scratch, output, and site staging together.

## Build and deployment boundaries

GitHub Actions has no Toolforge credentials. It tests the repository and emits
an attested Linux release envelope containing the Rust binary, checksums,
provenance, notices, and SBOMs. An operator downloads that exact main-branch
artifact and runs `deploy/toolforge/deploy-binary.sh`; both the workstation and
Toolforge installer verify its commit, attestation, checksum, member allowlist,
binary identity, provenance, and SBOM identities before switching `app/current`.

Toolforge Build Service separately builds the lightweight source/Node image.
`RustConfig` skips Cargo there during normal builds, so weekly refreshes use the
verified NFS binary instead of rebuilding Rust. `deploy/toolforge/rebuild-image.sh`
records the image source commit and restarts the webservice after a successful
image build. The image contains the single npm workspace and reviewed offline
Observable cache; refreshes fail if dependencies are absent instead of running
`npm ci` on the network.

## Reproduce the production topology

From a clean clone, first install the pinned toolchains shown in the
[generated reference](generated/stack-reference.md), then run:

```sh
npm ci
node scripts/verify-runtime.cjs
node scripts/generate-stack-reference.cjs --check
./scripts/ci-local.sh
```

For a fresh Toolforge tool account:

1. Build the main-branch source image with Toolforge Build Service and record
   its exact source commit using `deploy/toolforge/rebuild-image.sh`.
2. Download the successful GitHub `toolforge-release` artifact for that same
   commit and install it through `deploy/toolforge/deploy-binary.sh` over SSH.
3. Create the tool-wide `WIKI_ECON_BIN`, `WIKI_ECON_DATA_DIR`,
   `WIKI_ECON_OUTPUT_DIR`, and `WIKI_ECON_SITE_DIST_DIR` values shown in the
   [Toolforge runbook](../deploy/toolforge/README.md#operator-prerequisites).
4. Start the Build Service webservice from `Procfile`, run the allowlisted
   `deploy/toolforge/load-scheduled-jobs.sh`, and confirm the six preparation
   Jobs and publisher Job are waiting for their schedules, the legacy one-off
   Jobs are absent, and the webservice is healthy.
5. Run one preparation Job manually and then invoke
   `deploy/toolforge/run-publish-ready.sh`; validate the per-wiki run record,
   publication receipt, current site symlink, and public freshness endpoint
   before relying on the schedules.

Emergency Cargo compilation in Toolforge remains a disaster-recovery path,
not the normal deployment model. Rollback changes only the verified
`app/current` symlink and restarts the webservice; it does not rebuild data.
