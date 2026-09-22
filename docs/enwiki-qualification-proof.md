# Enwiki correctness and recovery proof

Enwiki stays `publication=hidden`, `refresh=qualification`, and
`publication_eligible=false` until the isolated candidate has a complete proof
receipt. The proof is intentionally separate from the six stage receipts: a
stage can finish successfully while an inventory, invariant, recovery, or
rollback drill is still unproven.

## Acceptance sequence

Run the frozen `2026-08` candidate with one heavy job at a time and retain the
stage receipts. A promotion proof must contain two distinct successful
qualification runs: an `initial_candidate` run for `2026-08`, followed by a
`rollover` run for the next completed snapshot while the first generation is
retained. Each run must carry all six stage receipts plus its capacity and run
receipts. A receipt reference is not a prose assertion: it must include the
relative path and SHA-256 of the immutable receipt file. Then run every
independent acceptance check against the same warehouse and snapshot:

1. compare the planned source inventory with the remote inventory; reject
   missing, duplicate, mixed-snapshot, or mismatched history/logging inputs;
2. execute all edit, rate, inequality, patrol, cohort, and page-week
   conservation invariants;
3. run the same warehouse twice and compare every artifact byte-for-byte;
4. run the complete same-snapshot pipeline again and record a no-op for all six
   stages;
5. interrupt and resume each stage, checking that publication never changes and
   no orphaned paths remain;
6. classify patrol as `applicable`, `not_applicable`, or `unknown`, and require
   non-negative/plausible counts. Non-applicable patrol must publish null
   headline ratios and block agent comparisons;
7. process the next completed snapshot while the preceding generation remains
   available. Record both immutable generation manifests, the before/after
   snapshot pointers, cutoff advancement, conservation, and every measured
   persistent/scratch/combined storage high-water mark. Reject any mixed
   generation path or cross-generation reference, and require the configured
   storage reserve after the peak;
8. roll back to the previous identity, verify it is restored, and clean the
   candidate workspace idempotently.

The checks must be recorded as hashed evidence references. A prose assertion,
an unchecksummed log excerpt, a partial stage list, or an empty evidence list
is rejected.

## Produce the proof

The validator is dependency-free and safe to run on Toolforge after the
qualification stages have completed:

```sh
node deploy/toolforge/qualification-proof.cjs \
  --policy config/qualification-proof.json \
  --evidence /data/project/wiki-economics/capacity/qualifications/enwiki/RUN/proof-evidence.json \
  --output /data/project/wiki-economics/capacity/qualifications/enwiki/RUN/qualification-proof.json
```

The command writes the output atomically. A successful output has
`qualified=true`, `status=passed`, and a `proof_sha256` digest over the full
canonical proof. Keep the evidence files, stage receipt paths/checksums, and
the proof together under the immutable qualification run directory. A failed
command is loud (`QUALIFICATION PROOF FAILED`) and must not be converted into
a passed fleet or lifecycle marker.

Start the isolated first run only with the frozen snapshot and an explicit run
kind (the wrapper defaults non-enwiki runs, but enwiki evidence should always
be labelled):

```sh
WIKI_ECON_PREPARE_SNAPSHOT=2026-08 \
WIKI_ECON_QUALIFICATION_RUN_KIND=initial_candidate \
deploy/toolforge/run-qualify-wiki.sh enwiki
```

The rollover invocation must set `WIKI_ECON_QUALIFICATION_RUN_KIND=rollover`
and pin the next completed snapshot after its completeness check. It must run
with the first generation retained and never share a heavy-job slot with the
initial candidate. No invocation changes the lifecycle registry or scheduled
publication; those changes require the authenticated two-run proof and a
separate promotion action.

The policy requires these stages in order:

`ingest → metrics → lifecycle → page-week → patrol → publish`

and these checks:

`source_inventory`, `snapshot_consistency`, `invariants`,
`deterministic_same_warehouse`, `noop_same_snapshot`,
`interruption_resume`, `patrol`, `two_successful_runs`, `rollover_safety`,
`rollback_cleanup`.

The two-run gate is independent of the no-op and rollover checks: a complete
same-snapshot no-op does not count as the second successful qualification run,
and a rollover storage observation without the second run's six stage receipts
cannot satisfy promotion. All run receipts must record rows/bytes, checksums
and fingerprints, CPU and wall time, cgroup memory peak, persistent/scratch
high-water marks, bucket-size distribution, warnings, retries, and recovery
events. Missing or failed receipt fields keep the candidate hidden.

Only after the proof digest and the capacity/resource receipts have been
reviewed may an operator consider a separate promotion decision. This proof
does not promote, schedule, or publish enwiki. The rollover check is the
acceptance gate required before activation: it does not retire the preceding
generation, so a failed activation can still serve the retained candidate.
