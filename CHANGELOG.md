# Changelog

All notable changes to this project are documented here.

This project follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
and [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.1] - 2026-09-24

### Fixed

- Migrate retained lifecycle candidates to the current schema without
  re-ingesting purged history, while preserving receipt lineage and rejecting
  obsolete metric schemas before publication.
- Run the compatibility cohort migration against the exact retained wiki set
  and snapshot.
- Quote the offline network-guard path so deterministic site builds work from
  local checkout paths that contain spaces.
