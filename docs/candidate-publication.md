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

Every attempt also has a durable record below
`output/_generation-state/<wiki>/<snapshot>/<run-id>.json`. Its guarded state
machine is:

```text
building -> validated -> ready -> published -> superseded -> retired
```

Interrupted `building` or `validated` attempts remain resumable for the
configured recovery window. Ready and published generations are never removed
by age-based cleanup. Publication marks the previous live candidate
`superseded` only after the new site and data pass the publication gate, keeps
that one generation as rollback material, and transitions older superseded
generations to `retired` before deleting their directories. The compact state
receipt remains after data retirement as an audit trail.

Before creating a new candidate, preparation compares the resolved snapshot
with every valid ready candidate for that wiki and verifies the current core
and patrol stage fingerprints. When both fingerprints still match, the job
finishes successfully as an explicit no-op and points its log at the existing
`ready.json`. When only one fingerprint matches, its receipt-covered files are
copied atomically into the new candidate and only the invalidated stage runs.
The original ready candidate remains immutable.

Preparation holds `output/_prepare-locks/<wiki>.lock`. Different wikis may run
concurrently; a second preparation for the same wiki exits with status 75.
The lock heartbeat also prevents stale cleanup from removing a candidate's
unselected input generation.

## Publication transaction

`wiki-econ publication-prepare-ready` runs under the sole global
`output/.publication.lock`. It revalidates every ready receipt and artifact,
selects the newest non-downgrading candidate for each managed wiki, and writes
a recovery journal under:

```text
output/_publication_transactions/<run-id>/selection.json
```

For each changed wiki it moves the old `output/<wiki>` to the transaction's
backup directory, installs a relative symlink to the immutable candidate, and
switches the snapshot pointer. It then regenerates combined metrics and
browser partitions and runs the fail-closed publication gate. Any failure in
that sequence restores both the old wiki paths and snapshot pointers.

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
Expired, well-identified site/run staging is removed by run ID. Malformed or
unknown objects found inside pipeline-owned candidate/staging namespaces are
moved to `_quarantine` with a JSON receipt instead of being deleted.

## Toolforge jobs

The scheduled jobs are:

| Job | Lock | Responsibility |
| --- | --- | --- |
| `wiki-econ-prepare-nlwiki` | `nlwiki.lock` | Prepare and validate nlwiki |
| `wiki-econ-prepare-ptwiki` | `ptwiki.lock` | Prepare and validate ptwiki |
| `wiki-econ-prepare-frwiki` | `frwiki.lock` | Prepare and validate frwiki |
| `wiki-econ-publish-ready` | `.publication.lock` | Select, merge, build, validate, switch, retire |

The weekly preparation schedules are discovery triggers rather than an
instruction to rebuild. Each trigger resolves and pins the latest completed
snapshot. If that version and its stage fingerprints are already represented
by a ready candidate, the run record reports `noOp: true`; no dump or patrol
download and no compute stage starts.

Every Wikimedia monthly history dump is treated as a complete authoritative
snapshot. Snapshot rollover performs the complete source-generation ingest;
the pipeline deliberately does not infer cross-snapshot deltas.

The publisher runs every two hours. It may publish one wiki while another is
still computing; an incomplete or failed candidate is invisible to it. A
publication with no new ready candidate is a validated near-no-op and may
reuse both merge and site fingerprints.

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

Inspect locks and transactions with:

```sh
become wiki-economics find /data/project/wiki-economics/output/_prepare-locks -name owner.json -maxdepth 2 -print
become wiki-economics jq . /data/project/wiki-economics/output/.publication.lock/owner.json
become wiki-economics find /data/project/wiki-economics/output/_publication_transactions -name selection.json -print
```

For a failed pre-site-switch publication, the wrapper runs rollback
automatically. To retry explicitly, use the original run ID with
`publication-rollback-ready`. For a post-site-switch commit failure, use the
original run ID with `publication-commit-ready`; do not roll back only the data
after the site has changed.
