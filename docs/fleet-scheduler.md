# Fleet scheduler and qualification

The production scheduler checks the lifecycle registry every six hours. One
Rust controller discovers work independently for each scheduled language; two
small workers, two medium/large workers, and one isolated monthly worker claim
tasks from shared NFS. The existing publisher remains the only process allowed
to switch public data.

## Control plane

`wiki-econ fleet-discover` reads the lifecycle registry, resolves and persists
one canonical snapshot plan per scheduled wiki, derives a resource class from
measured signals, and writes an atomic task under `output/_fleet/pending/`.
Task records carry the wiki, snapshot, source layout/count, measured resource
signals, resource class, and queue algorithm version; the task identity is
stable for a wiki/snapshot under that version. Discovery is a no-op when the
same task is pending, when matching completed-task, ready-notification, and
live ready-index evidence remain valid, or when the resolved snapshot is
already present in the ready index even if it was promoted outside the fleet.

Workers claim tasks with an atomic `mkdir` lease. The pending task remains
visible while leased, and `owner.json` records the worker, lease identity,
claim time, heartbeat, timeout, and complete immutable task. A worker may write
only its claimed wiki candidate; the existing per-wiki preparation lock is a
second ownership boundary. Successful preparation must authenticate the exact
wiki and snapshot in `_ready-index/<wiki>.json` before the task can move to
`completed` and emit a publication-ready notification.

Failures use bounded exponential backoff. The third failed attempt is moved to
quarantine with a concise error. Expired leases are returned to the same retry
path; malformed or identity-ambiguous lease state is quarantined rather than
deleted. One failed or slow wiki therefore cannot consume another wiki's lease
or invalidate the current public generation.

## Resource classes

Classification has no wiki-name branches. It consumes canonical source layout
and count, compressed bytes, prior rows and fragments, historical cgroup memory
and scratch peaks, and conservative observed throughput. Runtime estimates use
prior rows divided by the worst non-zero observed throughput. These observations
are stored under `data/workload-observations/` and seed the next immutable
snapshot profile.

- `small` runs in the two fixed 2 GiB workers.
- `medium_large` runs in either of the two fixed 6 GiB workers.
- `isolated` runs in one fixed 6 GiB / 4-CPU worker. Monthly source layouts
  always select it and cannot be overridden into another class. Its stages run
  sequentially so the large monthly pipeline stays within the per-job limit.

The NFS-safe capacity admission layer accounts for both dimensions of the
16-CPU/24-GiB namespace quota and enforces the 4-CPU cap and unchanged 6-GiB
per-job memory ceiling. It reserves the 512 MiB web service, 512 MiB
dispatcher, and 6 GiB publisher before admitting fleet work. The resulting
17 GiB worker-memory budget safely admits the full two-medium/two-small pool
(16 GiB) while keeping
publication schedulable. Kubernetes quota remains the outer enforcement
boundary; rejected work stays durably queued. Enwiki's large monthly profile is
enabled by the receipt-backed August 2026 production promotion recorded in
`config/capacity-qualification.json`; the worker still applies the same source,
memory, scratch, and storage admission checks.

Authenticated admin work uses the same fixed workers. Recovery and lifecycle
operations sort ahead of bulk preparation and raw diagnostic transfers. A
stale worker returns the request to its original resource-class queue with the
same run identity, preserving every committed source and candidate receipt.

## Qualification ladder

[`config/fleet-qualification.json`](../config/fleet-qualification.json) is the
ordered, fail-closed promotion record:

1. local deterministic queue and failure fixtures;
2. publication-invisible shadow runs for the then-current scheduled set;
3. one hidden medium yearly wiki;
4. one hidden large yearly wiki;
5. a concurrent frwiki fleet run;
6. isolated enwiki qualification; and
7. gradual fleet batches.

Stages cannot pass out of order. The checked-in validator rejects an enwiki
stage that is not isolated or is publication-eligible. Qualification uses a
separate queue/output root and hidden lifecycle entries, so evidence collection
cannot change the live publication.

## Operations

The controller runs every six hours and resolves every scheduled wiki
separately. If one wiki cannot be resolved, it records that failure and
continues checking the rest; the controller job reports the partial failure
after preserving successful queue writes. Workers retry at their fixed cadence,
and the publisher checks for ready candidates every two hours. Operators can
inspect `pending`, `leases`, `failures`, `quarantine`, `completed`, and
`notifications/ready` without scanning candidate trees. `wiki-econ fleet-recover`
is an explicit, idempotent stale-lease pass. The old monolithic refresh and
stage jobs remain on-demand recovery tools.

`load-scheduled-jobs.sh` loads the six-hour discovery controller, fixed workers,
and publisher from the explicit schedule allowlist. It removes legacy per-wiki
jobs and leaves one-off pipeline definitions on-demand.
