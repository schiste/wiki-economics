---
name: toolforge-deploy
description: Deploy wiki-economics to Toolforge production over SSH — normal binary release, image rebuild, on-demand ingest/compute/site jobs, and status checks. Use whenever the user asks to deploy, redeploy, rebuild the Toolforge image, or trigger a production refresh/compute/site build for this repo.
---

# Toolforge deploy (wiki-economics)

Full reference: `deploy/toolforge/README.md`. This skill is the fast path for the
actions an operator actually runs, plus the failure modes hit in practice.
When anything here disagrees with the README, trust the README — it's the
source of truth and gets updated more often than this file.

Production deployment is operator-driven over SSH. There is no auto-deploy:
GitHub Actions builds and retains the release artifact but never touches
Toolforge.

## Before touching anything: check for concurrent operators

This repo's Toolforge tool account can be operated by more than one agent at
once (another Claude session, a Codex session, a human). All of them share
the same `current` release symlink, the same built image, and the same
`.refresh-lock`. Before rebuilding the image or running a job:

1. `ListAgents` — any peer session that might also be working on this repo.
2. If Chau7 is available, check `mcp__chau7__chau7_state_snapshot` (or grep
   its saved output for `.tabs[]`) for another tab with
   `cwd: ".../wiki-economics"` and `status: running`. It may be running its
   own ad hoc one-off jobs (e.g. `wiki-econ-repair-<wiki>-<sha>`,
   `wiki-econ-publish-<sha>`) outside `jobs.yaml`.
3. If the `current` symlink or a running job doesn't match what you expect,
   don't guess who owns it — ask the user, and check with any peer session
   directly (`SendMessage`) before proceeding. A peer's own denial is
   informative but not proof; get the user to confirm ownership either way.
4. If someone else's deploy is a superset of the commit you care about
   (ahead on `main`, same feature included), the simplest correct move is
   often to let their run finish and verify the result — not to race it with
   your own.

## Normal binary release + deploy

1. Wait for CI on the target commit to pass, including `toolforge-release`
   (it's skipped if `coverage` or another quality gate fails — check the run,
   don't assume).
2. Download the release artifact:
   ```sh
   gh run download <run-id> --repo <owner>/wiki-economics --name wiki-econ-linux-x86_64-<full-40-char-sha> --dir <dir>
   ```
   The tarball lands nested: `<dir>/wiki-economics/wiki-economics/target/release/wiki-econ-release-<sha>.tar.gz`
   — `find`/`ls` for it rather than assuming it's directly under `<dir>`.
3. Deploy it:
   ```sh
   TOOLFORGE_SSH_TARGET=<target> deploy/toolforge/deploy-binary.sh <bundle.tar.gz> <full-40-char-sha> <bundle.tar.gz.sha256>
   ```
   This verifies checksum, file manifest, `gh attestation verify`, and ELF
   arch locally before it ever opens an SSH connection, then installs,
   prunes old releases (keeps 3 by default), and restarts the webservice.
4. Confirm: `readlink -f /data/project/wiki-economics/app/current` should
   resolve to a `releases/<the-sha>` directory.

## Image rebuild

The built container image is a **separate artifact** from the binary deploy
and must be rebuilt to match, or `scripts/lib/wiki_econ.sh`'s runtime guard
fails closed with `Binary and image source commits disagree` on every job
run. Rebuild after every binary deploy that changes the commit:

1. Push an immutable tag first — the rebuild script verifies via
   `git ls-remote` that this ref resolves to the expected commit, both before
   and after the build:
   ```sh
   git tag toolforge-image-<full-40-char-sha> <sha> && git push origin toolforge-image-<full-40-char-sha>
   ```
2. Run `deploy/toolforge/rebuild-image.sh <repo-url> <ref> <sha>`.
3. **The Toolforge Build Service is sometimes down** (`BuildClientError: The
   build service seems to be down`). This is a real, external, sometimes
   30+-minute outage — not a one-off flake after 2+ consecutive identical
   failures. Retry on a ~5 minute interval rather than giving up; don't
   escalate to the user as broken until you've done a few retries.
4. On success the script restarts `wiki-econ-admin` and any `continuous*`
   jobs itself — no separate restart needed.

## On-demand stage jobs (ingest / compute / site)

`wiki-econ-ingest` / `wiki-econ-compute` / `wiki-econ-site` are defined in
`jobs.yaml` but deliberately **not** kept loaded — `toolforge jobs restart
<name>` fails with `Job does not exist` if it was never loaded. Always use:

```sh
become wiki-economics toolforge jobs load --job <name> /data/project/wiki-economics/jobs.yaml
```

This creates *and* immediately runs it as a one-off. It prints "N job(s)
loaded successfully" (counting every definition parsed from the manifest),
but with `--job <name>` only that one job is actually created — verify with
`toolforge jobs list` if in doubt.

Stage order matters: `wiki-econ-site` builds only against whatever a prior
`wiki-econ-compute` last published — it does **not** regenerate data. If a
new binary produces a new/renamed output artifact, `wiki-econ-site` will fail
with something like `Not found: /data/<new-artifact>.json` until
`wiki-econ-compute` runs again with the new binary. `wiki-econ-compute`
itself assumes a prior `wiki-econ-ingest` already populated the generation —
its stage-fingerprint checks fail closed otherwise.

`wiki-econ-compute`/`wiki-econ-ingest` can also fail with `qualified
production compute requires a persisted workload profile for <wiki>`. This
means `data/snapshots/<wiki>/<snapshot>/workload-profile.json` is missing —
only the `ingest` stage's `governed_snapshot_with_sizes` path persists it. If
markers/source data are already on disk this is cheap (no re-download):
running `wiki-econ-ingest` alone backfills the profile.

## Checking job state and logs

- `become wiki-economics toolforge jobs list` — state (`Running for Ns`,
  `Failed`, or gone entirely once a one-off completes and gets swept).
- Logs are `filelog: true`, i.e. plain files at
  `/data/project/wiki-economics/<job-name>.out` / `.err` — **not**
  retrievable via `toolforge jobs logs <name>` for this job type.
- Always read the actual `.out`/`.err` tail before declaring success or
  failure. A Monitor/poll loop reporting "SUCCESS" from a piped command
  without `set -o pipefail` is checking `tee`'s exit code, not the real one —
  seen once in practice, silently wrong.

## Scripting gotchas hit in production use

- **Never name a shell variable `status` in a script that might run under
  zsh** — it's a read-only special variable there and assignment crashes the
  script (`read-only variable: status`). Use `job_status` or similar.
- When piping a remote command's output through `tee` for logging, add
  `set -o pipefail` (or skip the pipe and redirect directly with `>>` then
  check `$?`) — otherwise `if cmd | tee file; then` always sees success.
- A `run_in_background: true` Bash call with a manually-appended `&` inside
  the command double-backgrounds and returns almost instantly; it does not
  kill the real process, but it's confusing to reason about — don't add your
  own `&` when the tool already backgrounds the command.

## Verifying the deploy actually landed

Don't declare success from job exit status alone:

```sh
curl -s -o /dev/null -w "HTTP %{http_code}\n" https://wiki-economics.toolforge.org/
curl -s https://wiki-economics.toolforge.org/ | grep -o '<marker-string-from-the-new-feature>'
```

and cross-check `readlink -f /data/project/wiki-economics/site-dist` against
the `publishedSiteGeneration` value in the job's terminal
`wiki_econ_run_summary` JSON line.
