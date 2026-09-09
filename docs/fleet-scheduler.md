# Fleet scheduler and qualification

The production scheduler has a fixed Toolforge footprint even when the managed
language set grows. One Rust controller discovers work, two small workers and
two medium/large workers claim it from shared NFS, and the existing publisher
remains the only process allowed to switch public data.

## Control plane

`wiki-econ fleet-discover` reads the lifecycle registry, resolves and persists
one canonical snapshot plan per scheduled wiki, derives a resource class from
measured signals, and writes an atomic task under `output/_fleet/pending/`.
Task identities include the wiki, snapshot, source layout/count, measured
resource signals, resource class, and queue algorithm version. Discovery is a
no-op when the same task is pending or when matching completed-task, ready
notification, and live ready-index evidence all remain valid.

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
- `isolated` has no production worker. Monthly source layouts always select it
  and cannot be overridden into another class.

The NFS-safe capacity admission layer accounts for both dimensions of the
16-CPU/24-GiB namespace quota and enforces the 4-CPU cap and unchanged 6-GiB
per-job memory ceiling. It reserves the 512 MiB web service, 512 MiB
dispatcher, and 6 GiB publisher before admitting fleet work. The resulting
17 GiB worker-memory budget safely admits the full two-medium/two-small pool
(16 GiB) while keeping
publication schedulable. Kubernetes quota remains the outer enforcement
boundary; rejected work stays durably queued. Enwiki remains `isolated` until
a separate capacity report qualifies it.

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

The scheduled wrappers are `run-fleet-controller.sh` and
`run-fleet-worker.sh`. Operators can inspect `pending`, `leases`, `failures`,
`quarantine`, `completed`, and `notifications/ready` without scanning candidate
trees. `wiki-econ fleet-recover` is an explicit, idempotent stale-lease pass.
The old monolithic refresh and stage jobs remain on-demand recovery tools.

Do not load the fleet schedules in production until the local and scheduled-set
shadow stages have passed. Loading them replaces legacy per-wiki job
definitions; it does not enable hidden or isolated wikis.
