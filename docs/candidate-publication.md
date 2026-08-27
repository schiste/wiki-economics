# Per-wiki candidate preparation and publication

Production refreshes use two distinct transaction scopes. Expensive work is
prepared independently for each managed wiki; only the short operation that
changes the public dataset and site is globally serialized.

## State model

`wiki-econ prepare-wiki <wiki>` resolves and pins one snapshot plan, ingests
it without changing `current-snapshot.json`, computes core and patrol metrics
against that explicit snapshot, and writes results below:

```text
output/_candidates/<wiki>/<snapshot>/<run-id>/<wiki>/*.parquet
```

The command writes `ready.json` last. That receipt records the wiki, snapshot,
run ID, cutoff, generating commit, and the row count, byte length, and SHA-256
of every required metric. It is written only after the generation manifest,
schemas, row-count contracts, wiki labels, dates, snapshot cutoff, and
patrol/rights sources pass validation. A directory without a valid
`ready.json` is never eligible for publication.

The same commit atomically updates `output/_ready-index/<wiki>.json`. The index
contains the newest valid ready candidate, the active published candidate,
snapshot, workload profile, core and patrol receipt identities, and the
ready-receipt SHA-256. Publishers read these compact indexes in the normal path;
candidate-directory discovery is retained as a self-healing fallback that
rebuilds a missing, truncated, or stale index.

Every attempt also has a durable record below
`output/_generation-state/<wiki>/<snapshot>/<run-id>.json`. Its guarded state
machine is:

```text
building -> validated -> ready -> published -> superseded -> retired
```

Exactly one interrupted `building` or `validated` attempt per wiki remains
resumable for the configured recovery window; the active run wins that slot,
otherwise the newest state receipt does. Ready and published generations are never removed
by age-based cleanup. Publication marks the previous live candidate
`superseded` only after the new site and data pass the publication gate, keeps
that one generation as rollback material, and transitions older superseded
generations to `retired` before deleting their directories. The compact state
receipt remains after data retirement as an audit trail.

Before creating a new candidate, preparation compares the resolved snapshot
with indexed ready candidates for that wiki and verifies four core-family
fingerprints plus the independent patrol fingerprint. A complete match finishes
as an explicit no-op and points its log at the existing `ready.json`. On a
partial match, each receipt-covered family is reused atomically in the new
candidate and `ComputePlan` runs only invalid families. Reusable families may
come from different immutable candidates; readiness is issued only after the
new candidate contains and authenticates the complete family set. The original
ready candidates remain immutable. Reuse attempts a copy-on-write filesystem
clone first, then an immutable hard link, and finally a byte copy. Files without
a valid ready/stage/artifact-receipt chain are never link candidates.

Preparation holds `output/_prepare-locks/<wiki>.lock`. Different wikis may run
concurrently; a second preparation for the same wiki exits with status 75.
The lock heartbeat also prevents stale cleanup from removing a candidate's
unselected input generation.

## Publication transaction

`wiki-econ publication-prepare-ready` runs under the sole global
`output/.publication.lock`. Before selection it compares the current
publication no-op digest: sorted active ready-receipt identities, lifecycle
hash, merge algorithm version, publication contract version, and site-source
fingerprint. A match records an immediate `no_op` after compact receipt and
artifact metadata checks. Otherwise it validates changed candidates, selects
the newest non-downgrading candidate for each managed wiki, and writes
a recovery journal under:

```text
output/_publication_transactions/<run-id>/selection.json
```

For each changed wiki it moves the old `output/<wiki>` to the transaction's
backup directory, installs a relative symlink to the immutable candidate, and
switches the snapshot pointer. It derives a `changed wiki × changed metric
family` plan from authenticated candidate receipts. Root aggregates are
rewritten only for changed families; page-week remains per-wiki and never has
a combined root artifact. Browser index entries whose artifact-receipt identity
did not change are reused byte-for-byte. The fail-closed gate composes unchanged
semantic reports from the preceding receipt and deeply verifies changed
artifacts. Any failure in
that sequence restores both the old wiki paths and snapshot pointers.

The production SLO for the receipt-backed `publication_prepare` stage is three
minutes for the current six-wiki set. The run record retains the exact change
plan and the public freshness endpoint raises
`incremental_publication_slow` when a successful incremental publication
exceeds that bound.

The site is built in its existing isolated staging directory and its symlink
is switched atomically. `publication-commit-ready` verifies the publication
receipt again, records `committing` in the recovery journal, advances
generation states, retires inactive input snapshots, and removes transaction
backups. If the site build fails before
its switch, `publication-rollback-ready` restores the previous dataset and
regenerates its combined artifacts. If cleanup/commit fails after the site
switch, the wrapper deliberately leaves the selected data and new site
together and reports the exact run ID whose commit must be retried.

Reclamation follows ownership receipts rather than directory names alone:
strict ingest success authorizes raw-source deletion, durable bucket append
authorizes scratch deletion, and lifecycle state authorizes candidate deletion.
Committed compaction plus its ingest receipt authorizes retirement of the
explicit source-fragment allowlist; an interrupted prepared compaction remains
recoverable and an ambiguous staging/final pair fails closed for quarantine.
Expired, well-identified site/run staging is removed by run ID. Malformed or
unknown objects found inside pipeline-owned candidate/staging namespaces are
moved to `_quarantine` with a JSON receipt instead of being deleted.

## Toolforge jobs

The scheduled jobs are:

| Job | Lock | Responsibility |
| --- | --- | --- |
| `wiki-econ-fleet-controller` | atomic queue writes | Discover every scheduled lifecycle wiki |
| `wiki-econ-fleet-small-a/b` | task lease + `<wiki>.lock` | Prepare small-class wiki candidates |
| `wiki-econ-fleet-medium` | task lease + `<wiki>.lock` | Prepare medium/large-class wiki candidates |
| `wiki-econ-publish-ready` | `.publication.lock` | Select, merge, build, validate, switch, retire |

The weekly controller schedule is a discovery trigger rather than an
instruction to rebuild. Each trigger resolves and pins the latest completed
snapshot. If that version and its stage fingerprints are already represented
by matching completed-task, notification, and ready-index evidence, no task is
queued. If a worker still reaches an unchanged candidate, its run record reports
`noOp: true`; no dump or patrol download and no compute stage starts.

Every Wikimedia monthly history dump is treated as a complete authoritative
snapshot. Snapshot rollover performs the complete source-generation ingest;
the pipeline deliberately does not infer cross-snapshot deltas.

The publisher runs every two hours. It may publish one wiki while another is
still computing; an incomplete or failed candidate is invisible to it. A
publication with no new ready candidate and an identical publication digest
is an immediate no-op; it does not walk candidate trees, decode Parquet, merge,
or invoke the site build.

The former `wiki-econ-refresh` and split stage jobs remain on-demand recovery
tools but are no longer scheduled. They use the legacy whole-refresh lock and
must not be started while candidate or publication jobs are active.

## Publication-invisible qualification

A new wiki enters the lifecycle as `publication=hidden` and
`refresh=qualification`. `deploy/toolforge/run-qualify-wiki.sh <wiki>` creates
an isolated run root below `capacity/qualifications/<wiki>/<run-id>` and invokes
`wiki-econ qualify-wiki`; it never writes into the production data or output
roots.

Qualification metrics live below `_qualifications`, not `_candidates`, and the
final `qualification.json` explicitly records `publication_eligible=false`.
The ready-candidate selector scans only `_candidates` for published
scheduled/manual wikis. Promoting lifecycle configuration therefore cannot
make an old qualification receipt publishable: a new production
`prepare-wiki` run is mandatory after qualification policy is committed.

The qualification wrapper disables the production capacity allowlist only
inside its isolated root so an unmeasured profile can run and produce evidence.
It retains the strict memory, storage-reserve, writer, file-descriptor, source
window, marker, and semantic gates. Qualification is operator-triggered and is
never loaded as a scheduled Toolforge Job.

## Recovery

Every publisher first runs the same fail-closed recovery engine used by the
operator CLI. It validates each journal's schema and run identity, then
correlates live candidate symlinks, snapshot pointers, candidate hashes, the
publication gate, and the deployed site's content-addressed receipt. Terminal
transactions are skipped. A transaction proven to have reached the site is
committed; one proven to have been incorporated by a later publication is
marked `reconciled`; one proven not to have reached the site is rolled back and
its previous aggregate/site generation is rebuilt. Evidence that admits more
than one interpretation is retained and described below
`output/_quarantine/publication-recovery/`; it is never deleted automatically.

The read-only audit command is safe to run without acquiring the publication
lock:

```sh
wiki-econ --data-dir DATA --output-dir OUTPUT \
  publication-recovery-audit --site-dist-dir SITE_DIST
```

Repair must run under the publication lock. Select one journal explicitly:

```sh
wiki-econ --data-dir DATA --output-dir OUTPUT publication-recover \
  --run-id publish-20260825T214725Z-7 \
  --lifecycle config/wiki-lifecycle.json \
  --site-dist-dir SITE_DIST
```

The JSON result reports `site_rebuild_required=true` when rollback regenerated
the previous aggregate gate. The production wrapper consumes an atomic report,
rebuilds that matching site before continuing, and then runs normal candidate
selection. Re-running either audit or repair is idempotent.

Inspect locks and retained journals with:

```sh
become wiki-economics find /data/project/wiki-economics/output/_prepare-locks -name owner.json -maxdepth 2 -print
become wiki-economics jq . /data/project/wiki-economics/output/.publication.lock/owner.json
become wiki-economics find /data/project/wiki-economics/output/_publication_transactions -name selection.json -print
```

Do not manually remove a journal or its backups, and do not guess between
`publication-rollback-ready` and `publication-commit-ready`. The recovery audit
exists specifically to prove which transition preserves data/site identity.
