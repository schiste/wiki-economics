# Performance qualification and recovery

The production operations policy is machine-readable in
`config/operations-slos.json`; capacity qualification is pinned separately in
`config/capacity-qualification.json`. A report is evidence only when its
cgroup limit, thread counts, reserve, snapshot, and bucket count match that
policy.

## Capacity qualification

Run nlwiki and ptwiki with 256 buckets and frwiki independently with 256, 512,
and 1024 buckets. Each run must use a fresh 6 GiB / one-CPU Toolforge
container so `memory.peak` belongs to one variant:

```sh
toolforge jobs run --image tool-wiki-economics/tool-wiki-economics:latest \
  --command 'deploy/toolforge/run-capacity-benchmark.sh nlwiki 256' \
  --filelog --mount all --mem 6Gi --cpu 1 wiki-econ-capacity-nl-256
toolforge jobs run --image tool-wiki-economics/tool-wiki-economics:latest \
  --command 'deploy/toolforge/run-capacity-benchmark.sh ptwiki 256' \
  --filelog --mount all --mem 6Gi --cpu 1 wiki-econ-capacity-pt-256
for buckets in 256 512 1024; do
  toolforge jobs run --image tool-wiki-economics/tool-wiki-economics:latest \
    --command "deploy/toolforge/run-capacity-benchmark.sh frwiki $buckets" \
    --filelog --mount all --mem 6Gi --cpu 1 "wiki-econ-capacity-fr-$buckets"
done
```

The Rust report records every bucket's staged rows; reduction,
reconciliation, and total duration; sampled/cgroup peak memory; scratch and
combined scratch-plus-output peak; project storage before/after and estimated
persistent peak; output rows, edit total, range, bytes, and SHA-256.

Combine the evidence after every job has emitted a complete report. A benchmark
may exit non-zero because its resource gate failed; that report is still
required experimental evidence:

```sh
node scripts/qualify-capacity.cjs \
  --reports /data/project/wiki-economics/capacity/reports \
  --output /data/project/wiki-economics/capacity/frwiki-qualification.json
```

This fails unless all variants used the production limits and the three
frwiki variants have identical logical output (snapshot, rows, edit total, and
date range). At least one frwiki variant must pass both resource gates; failed
alternatives remain visible in the qualification evidence. Repeat the selected
variant and require the same output SHA-256 before lifecycle activation.
Frwiki passed this gate on 2026-08-24 with two byte-identical 256-bucket runs;
production rejects other bucket counts. Any future bucket or resource-policy
change requires a new qualification with at least 25% memory headroom plus the
configured 50 GiB storage reserve.

## Imported-data backup and restore

The current `local-import` datasets (`elwiki` and `svwiki`) are not
reconstructible from Toolforge's current warehouse. Create the archive on
Toolforge, download it to physically separate storage, verify it there, and
then remove only the temporary Toolforge copy:

```sh
backup=/data/project/wiki-economics/operations/export/imported-2026-08-23.tar.gz
ssh login.toolforge.org \
  "become wiki-economics bash /workspace/deploy/toolforge/create-imported-backup.sh '$backup'"
scp "login.toolforge.org:$backup" ./imported-2026-08-23.tar.gz
bash deploy/toolforge/verify-imported-backup.sh ./imported-2026-08-23.tar.gz
ssh login.toolforge.org "become wiki-economics rm -- '$backup'"
```

Keep the printed archive SHA-256 beside the secondary copy. Restore always
targets a path that does not exist, so it cannot overlay live output:

```sh
bash deploy/toolforge/restore-imported-backup.sh ARCHIVE EMPTY_OUTPUT_DIRECTORY
```

## Recovery commands

Every command refuses to operate while `.refresh-lock` exists.

```sh
# Killed ingest or corrupt marker: refetch if necessary, then replay ingest.
deploy/toolforge/recover-stage.sh ingest nlwiki 2026-07

# Killed compute: atomic metric writers discard/rebuild abandoned outputs.
deploy/toolforge/recover-stage.sh compute nlwiki

# Killed site build: rerun merge, gate validation, and atomic site publication.
deploy/toolforge/recover-stage.sh site

# Corrupt current-generation pointer: validate all markers/Parquets, then repoint.
deploy/toolforge/recover-stage.sh pointer nlwiki 2026-07

# Lost site symlink: validate the retained generation's required pages, then relink.
deploy/toolforge/recover-stage.sh site-link .site-dist.build.RUN.GENERATION
```

A lost binary symlink is recovered by the checksum-verifying rollback command:

```sh
deploy/toolforge/rollback-binary.sh RETAINED_40_CHARACTER_SHA
```

## Drills

The rollback drill changes the live binary symlink to a retained release,
runs its smoke check, and restores the original even if the drill fails:

```sh
deploy/toolforge/drill-binary-rollback.sh RETAINED_40_CHARACTER_SHA
```

The rebuild drill never writes to live output. It restores imported artifacts
into a new staging root, recomputes scheduled datasets from active warehouse
and patrol sources, runs merge/publication validation, and performs a real
Observable production build:

```sh
deploy/toolforge/run-rebuild-drill.sh IMPORTED_BACKUP frwiki nlwiki ptwiki
```

Both commands write compact JSON evidence below
`/data/project/wiki-economics/operations/reports/`.

After deploying from a fresh clone, verify that the clean checkout, GitHub
credentials, SSH path, immutable release envelope, stable binary environment,
and webservice all identify the same commit:

```sh
TOOLFORGE_SSH_TARGET=login.toolforge.org \
  deploy/toolforge/verify-clean-operator-path.sh DEPLOYED_40_CHARACTER_SHA
```

## Production SLOs

The public `/health/freshness.json` check enforces:

- each scheduled wiki's lifecycle freshness SLA;
- cutoff advancement whenever the selected snapshot advances;
- heartbeat age and stage-specific runtime ceilings;
- memory warning at 75% and critical at 80% of the cgroup limit;
- at least 50 GiB filesystem reserve;
- non-zero patrol/rights data; and
- at most 2 GiB total indexed browser data and 512 MiB per partition.

Browser sizes come from the publication receipt generated before the site
symlink switches. Missing size evidence is critical, preventing an old or
unmeasured generation from appearing healthy.
