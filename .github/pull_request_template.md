<!--
Thanks for the contribution. Quick reminders:
- Run `./scripts/ci-local.sh` before opening the PR.
- If the change touches the architecture (schema contracts, marker
  invariants, partition layout, redirect/host policies), update
  `docs/architecture.md` in the same PR.
- Don't include local data: `data/`, `output/`, `site/dist/`, and
  `site/node_modules/` are gitignored for a reason.
-->

## Summary

<!-- One paragraph: what does this change do, and why? -->

## Linked findings or issues

<!-- Reference any audit finding ID (e.g. C-1, H-3, M-7) or GitHub
issue this PR addresses. -->

## Quality gates

Please confirm each of the following ran cleanly locally:

- [ ] `cargo fmt --all -- --check`
- [ ] `cargo clippy --all-targets --all-features -- -D warnings`
- [ ] `cargo test --all-targets --all-features`
- [ ] `cargo doc --no-deps`
- [ ] `cargo deny check advisories bans licenses sources`
- [ ] `cargo audit -D warnings`
- [ ] `cargo llvm-cov ... --output-path /tmp/lcov.info && python3 scripts/check_lcov.py /tmp/lcov.info` (if Rust code changed)
- [ ] Shell scripts pass `bash -n` and (locally) `shellcheck`
- [ ] Docs updated for any architecture-visible behavior change

## Risk and rollback

<!-- What's the worst case if this change is wrong, and how would we
roll it back? "Revert the commit" is acceptable for low-risk changes.
For data-pipeline changes, mention any output-format implications. -->
