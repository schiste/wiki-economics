# Changelog

All notable changes to this project are documented here.

This project follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
and [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Defer publication of wikis whose ready candidate is not publishable instead
  of blocking the whole site on them, behind `--defer-unavailable`. Readiness
  blockers become per-wiki deferrals; integrity problems (snapshot downgrade,
  stable-version fingerprint drift, cross-wiki merge-contract conflict, and
  recovery, scrub or gate failures) remain global blockers under both modes.

### Changed

- Promote dewiki and enwiki to published and scheduled refresh on a 14-day
  freshness SLA, matching the state the authenticated admin already applied in
  production.

### Fixed

- Compose receipt-backed retained-candidate migrations when multiple metric
  families need schema upgrades at once, while preserving the source candidate
  and validating every changed family before publication.
- Keep a retained candidate's migration lineage across candidate retirement.
  Retirement previously deleted the `migrated_from_retained_candidate` origin
  when it fell outside the rollback window, which left the retained candidate
  permanently unauthenticatable: the site kept serving, but later
  fingerprint-check, resume and rollback runs all failed.

## [0.1.3] - 2026-09-25

### Fixed

- Migrate authenticated retained activity-tier v5 receipts to v6 only when the
  Parquet bytes and semantic summaries remain unchanged.

## [0.1.2] - 2026-09-25

### Fixed

- Allow retention-authorized candidate migrations to verify immutable output
  receipts across package-version upgrades when their stage algorithms still
  match.

## [0.1.1] - 2026-09-24

### Fixed

- Migrate retained lifecycle candidates to the current schema without
  re-ingesting purged history, while preserving receipt lineage and rejecting
  obsolete metric schemas before publication.
- Run the compatibility cohort migration against the exact retained wiki set
  and snapshot.
- Quote the offline network-guard path so deterministic site builds work from
  local checkout paths that contain spaces.
- Discover isolated Toolforge qualification receipts in the authenticated
  admin and promote their validated artifacts into production ready candidates.
