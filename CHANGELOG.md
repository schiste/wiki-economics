# Changelog

All notable changes to this project are documented here.

This project follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
and [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
