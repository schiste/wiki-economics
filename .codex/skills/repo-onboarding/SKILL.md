---
name: repo-onboarding
description: Use when starting work in an unfamiliar repository, when the task asks for repo overview, setup, architecture, entrypoints, test commands, or where to begin. Skip for narrow file-scoped edits once the relevant paths are already known.
---

# Repo Onboarding: wiki-economics

## When to Use

- Load this skill first when the repository is unfamiliar or the request is broad.
- Recommended when: first task in repo, repo overview, setup or run instructions, architecture or entrypoints, where should I start, broad debugging or feature-localization request.
- Skip when: known file-scoped edit, follow-up inside already identified area, task already localized to concrete files.
- Use `.codex/skills/aethyme/SKILL.md` or `.claude/skills/aethyme/SKILL.md` for Aethyme's short operating contract after orientation; load its `references/` files only when needed.

## Repo Identity

- Kind: `repository`
- Languages: `rust, javascript`
- Package manager: `cargo`
- Key manifests: `Cargo.toml, Procfile, package.json, site/package.json, site/vendor/observable-cache/_npm/@observablehq/plot@0.6.17/package.json, site/vendor/observable-cache/_npm/apache-arrow@21.2.0/package.json, site/vendor/observable-cache/_npm/d3-array@3.2.4/package.json, site/vendor/observable-cache/_npm/d3-chord@3.0.1/package.json, site/vendor/observable-cache/_npm/d3-contour@4.0.2/package.json, site/vendor/observable-cache/_npm/d3-delaunay@6.0.4/package.json, site/vendor/observable-cache/_npm/d3-interpolate@3.0.1/package.json, site/vendor/observable-cache/_npm/d3-scale@4.0.2/package.json, site/vendor/observable-cache/_npm/d3-time-format@4.1.0/package.json, site/vendor/observable-cache/_npm/d3-time@3.1.0/package.json, site/vendor/observable-cache/_npm/d3-transition@3.0.1/package.json, site/vendor/observable-cache/_npm/d3@7.9.0/package.json, site/vendor/observable-cache/_npm/delaunator@5.1.0/package.json, site/vendor/observable-cache/_npm/interval-tree-1d@1.0.4/package.json, vendor/object-store/Cargo.toml, vendor/polars-utils/Cargo.toml`

## Workspaces

- `.` (primary; cargo; manifest `Cargo.toml`; high confidence)
- `.` (supporting; npm; manifest `package.json`; high confidence)
- `site` (supporting; npm; manifest `site/package.json`; high confidence)
- `vendor/polars-utils` (supporting; cargo; manifest `vendor/polars-utils/Cargo.toml`; high confidence)
- `vendor/object-store` (supporting; cargo; manifest `vendor/object-store/Cargo.toml`; high confidence)
- `site/vendor/observable-cache/_npm/@observablehq/plot@0.6.17` (supporting; npm; manifest `site/vendor/observable-cache/_npm/@observablehq/plot@0.6.17/package.json`; high confidence)
- `site/vendor/observable-cache/_npm/apache-arrow@21.2.0` (supporting; npm; manifest `site/vendor/observable-cache/_npm/apache-arrow@21.2.0/package.json`; high confidence)
- `site/vendor/observable-cache/_npm/d3-array@3.2.4` (supporting; npm; manifest `site/vendor/observable-cache/_npm/d3-array@3.2.4/package.json`; high confidence)

## Start Here

- `dev`: `npm run dev`
- `fast_test`: `cargo test`
- `build`: `cargo build`

## Supporting Commands

- `npm run dev` (dev; high confidence from `package.json`)
  Workspace: `.`
- `npm --prefix site run dev` (dev; high confidence from `site/package.json`)
  Workspace: `site`
- `npm --prefix site/vendor/observable-cache/_npm/@observablehq/plot@0.6.17 run dev` (dev; high confidence from `site/vendor/observable-cache/_npm/@observablehq/plot@0.6.17/package.json`)
  Workspace: `site/vendor/observable-cache/_npm/@observablehq/plot@0.6.17`
- `cargo run` (dev; medium confidence from `Cargo.toml`)
- `node site/admin-server.cjs` (dev; medium confidence from `Procfile:web`)

## Entrypoints

- `app`: `src/main.rs` (conventional Rust binary entrypoint; high confidence)
- `test`: `tests` (conventional test root; medium confidence)

## Additional Entrypoints

- `Procfile:web` (process; role=app; Procfile process entrypoint; high confidence)
- `src/main.rs` (file; role=app; conventional Rust binary entrypoint; high confidence)
- `package.json:scripts.dev` (script; role=app; package dev script; medium confidence)
- `package.json:scripts.start` (script; role=app; package start script; medium confidence)
- `site/admin-server.cjs` (file; role=app; Procfile process entrypoint target; medium confidence)

## Repo Map

- `.github` (automation; automation and CI configuration; high confidence)
- `deploy` (infrastructure; deployment or infrastructure configuration; high confidence)
- `docs` (docs; documentation area; high confidence)
- `scripts` (tooling; developer tooling or scripts; high confidence)
- `src` (source; conventional source directory; high confidence)
- `tests` (tests; conventional test directory; high confidence)
- `vendor` (generated_or_vendor; generated output or vendored dependencies; high confidence)

## Aethyme Recipes

- `aethyme explore --repo "$PWD" --request "<task>" --format answer-json`
  Purpose: Broad repository orientation for a user request
- `aethyme repo inspect "$PWD" --mode brief --json-output`
  Purpose: Quick deterministic repo summary
- `aethyme graph callers "$PWD" "<symbol-or-file>" --json-output`
  Purpose: Trace likely impact before editing

## Caution Zones

- `vendor` (likely generated, vendored, fixture, or migration-heavy area)

## Generated and Dangerous Paths

- Generated/vendor `.aethyme/generated`: tracked generated or vendored surface; verify ownership before editing
- Generated/vendor `config/generated`: tracked generated or vendored surface; verify ownership before editing
- Generated/vendor `docs/generated`: tracked generated or vendored surface; verify ownership before editing
- Generated/vendor `site/vendor`: tracked generated or vendored surface; verify ownership before editing
- Generated/vendor `vendor`: tracked generated or vendored surface; verify ownership before editing
- Sensitive `.aethyme/gates.toml`: repository validation policy; changes affect every broker submission
- Sensitive `.github/workflows`: repository automation; changes can affect publication or shared CI
- Sensitive `deploy`: deployment, infrastructure, or migration surface; review repository policy before editing

## Freshness

- Source digest: `829ffa6f14b3f0c5c9fd531bc1a354d174403720a7db0380f888c505a6cd0e00`
- Tracked source files: `589`
- Overrides applied: `False`
- Sections generated: `repo, workspaces, primary_workspace, commands, areas, entrypoints, caution_zones, generated_paths, dangerous_paths, navigation_recipes, summon, freshness`
