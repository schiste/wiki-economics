# Contributing

## Scope

This project favors reproducible pipeline changes, explicit storage contracts, and strict quality gates. Changes that affect fetch, ingest, compute, merge, dashboard defaults, or storage layout should update code, tests, and documentation together.

## Development Workflow

1. Start from a fresh branch.
2. Keep changes focused and small enough to review.
3. Add or update tests for non-trivial behavior changes.
4. Run `./scripts/ci-local.sh` or the equivalent local quality gates from [README.md](README.md) or [docs/development.md](docs/development.md).
5. Include architecture or operator-facing doc updates when contracts change.

## Pull Request Expectations

- Explain the user-facing or operator-facing change clearly.
- Call out data layout, compatibility, or performance implications.
- Include benchmark context for changes that plausibly affect runtime or memory behavior.
- Do not weaken lint, test, coverage, or security checks to make a change pass.

## Commit Hygiene

- Do not commit generated data under `data/` or `output/`.
- Do not commit `site/node_modules`, `site/dist`, or local caches.
- Keep vendored dependency changes narrowly scoped and documented.

## Review Focus

Review should prioritize:

- correctness and reproducibility
- compatibility with existing pipeline contracts
- benchmark and memory impact
- dependency and security posture
