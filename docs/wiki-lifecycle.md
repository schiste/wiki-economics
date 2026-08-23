# Wiki lifecycle management

Wiki publication, refresh scheduling, and physical retention are separate
decisions. The authoritative registry is
[`config/wiki-lifecycle.json`](../config/wiki-lifecycle.json). A wiki that is
not scheduled is not implicitly obsolete and must never be deleted merely
because it is absent from a refresh run.

## Lifecycle dimensions

`publication` controls whether merge may include a wiki:

- `published` — include its per-wiki metrics in merged dashboard artifacts.
- `hidden` — retain its files but exclude them from new merged artifacts.
- `retired` — no longer published or refreshable. Physical deletion remains a
  separate, explicit operator action.

`refresh` controls orchestration:

- `scheduled` — include the wiki in recurring refreshes and enforce its
  `freshness_sla_days`.
- `manual` — permit deliberate one-off work without adding it to the schedule.
- `paused` — preserve and publish the current generation without refreshing it.

Retired wikis must use `refresh=paused`. Imported historical datasets use
`publication=published`, `refresh=paused`, `provenance=local-import`, and an
`imported_cutoff` field so the dashboard and operators can distinguish a valid
historical snapshot from a failed refresh.

## Runtime behavior

`scripts/wiki-lifecycle.cjs` validates the registry and provides the canonical
published and scheduled wiki lists. Toolforge resolves scheduled wikis in this
order:

1. `WIKI_ECON_REFRESH_WIKIS`, when set;
2. legacy `WIKI_ECON_ENABLED_WIKIS`, for backward compatibility;
3. entries whose registry refresh state is `scheduled`.

An environment override must agree with the registry. It may explicitly select
a published `manual` wiki for a deliberate one-off run, but it cannot activate
an unknown, hidden, retired, or paused wiki. A full publication run that adds a
manual wiki must still include every scheduled wiki. If both environment
variables are set, they must agree.

The Rust merge reads `WIKI_ECON_WIKI_LIFECYCLE_FILE` and excludes `hidden` and
`retired` wiki directories. `scripts/lib/wiki_econ.sh` exports the checked-in
registry path for normal script-driven runs. Direct library/test calls without
that variable retain the legacy discover-all behavior.

The admin status API exposes:

- `refreshWikis` and the backward-compatible `enabledWikis` alias;
- `publishedWikis`;
- the validated `wikiLifecycle` registry;
- `wikiStates`, including freshness, imported cutoff, and last publication
  timestamp when a per-wiki GDP artifact exists.

The generated manifest also embeds the lifecycle registry.

`publication_contract.datasets` defines which published wikis each metric must
contain and its conservative `minimum_rows_per_wiki`. Use
`coverage=all_published` for core datasets and an explicit `wikis` array for a
specialized dataset such as patrol. The Rust
[publication gate](publication-gate.md) enforces this registry before the site
can switch, so lifecycle activation and dataset readiness stay one reviewed
contract.

## Safe transitions

### Reactivate an imported or paused wiki

1. Confirm Toolforge quota can hold its download, two warehouse generations,
   computed output, and patrol data at peak.
2. Change `refresh` from `paused` to `manual` and deploy the registry.
3. Run a one-off fetch, ingest, compute, patrol, merge, and site publication.
4. Validate row conservation, snapshot cutoff, peak memory, and published site
   data while the imported baseline remains recoverable.
5. Change `refresh` to `scheduled`, add `freshness_sla_days`, and deploy again.

For frwiki, step 1 also requires the three 256/512/1024 Toolforge capacity
reports described in [benchmarking](benchmarking.md). The selected variant
must retain at least 25% cgroup memory headroom, the NFS storage gate must
cover the raw transient plus a second immutable generation and scratch, and a
repeat run must produce the same output SHA-256. A successful run that merely
avoids OOM is not sufficient evidence to change its lifecycle state.

### Pause a scheduled wiki

Change only `refresh` to `paused`. Keep `publication=published`. Its current
dataset remains merged and visible, while scheduled-freshness paging stops and
the UI can label the cutoff as paused.

### Retire a wiki

1. Change `publication` to `hidden` and `refresh` to `paused`.
2. Run merge and site publication; verify the wiki is absent publicly.
3. Retain its data for the agreed recovery period.
4. Change `publication` to `retired`.
5. Delete physical data only through a separately reviewed operator procedure.

The admin `cleanup` action is intentionally not a retirement operation. It
removes temporary files and invalid markers only.
