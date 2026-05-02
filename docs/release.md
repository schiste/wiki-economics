# Release Hygiene

This repository is designed to stay comfortable as a local working tree while
keeping the public Git baseline source-only and reproducible.

## Local-Only Material

Keep these locally. Do not commit them:

- `data/`
- generated `output/` artifacts such as `*.parquet`, `manifest.json`, and `defaults_*.json`
- `site/node_modules/`
- `site/dist/`
- local cache directories such as `site/src/.observablehq/`

The checked-in dashboard generator scripts live under `site/data-build/`. Live
data artifacts belong under `output/`.

## Tracked Source Baseline

The public repo should contain:

- Rust source under `src/`
- Python and shell utilities under `scripts/`
- deployment wrappers under `deploy/`
- frontend source under `site/src/` plus the checked-in data-build scripts under `site/data-build/`
- docs under `docs/`
- vendored patch code under `vendor/`
- repository policy/config files such as `.github/`, `.cargo/`, `Cargo.toml`,
  `Cargo.lock`, `deny.toml`, `.editorconfig`, `.gitattributes`, and `.gitignore`
- community and legal files such as `README.md`, `CONTRIBUTING.md`,
  `CODE_OF_CONDUCT.md`, `SECURITY.md`, `LICENSE`, `LICENSE-MIT`,
  and `LICENSE-APACHE`

## Verification Before A Release-Oriented Push

Run the canonical local verification command:

```sh
./scripts/ci-local.sh
```

That checks formatting, linting, tests, coverage, dependency policy, shell and
Node entrypoint syntax, and a site build when local merged artifacts are
present.

## Curated Staging Pattern

When preparing a release-oriented change, stage publishable source explicitly:

```sh
git add .cargo .github deploy docs scripts site src tests vendor
git add .editorconfig .gitattributes .gitignore
git add CODE_OF_CONDUCT.md CONTRIBUTING.md SECURITY.md README.md
git add LICENSE LICENSE-MIT LICENSE-APACHE
git add Cargo.toml Cargo.lock deny.toml rust-toolchain.toml package.json
git status --short
```

Before committing, verify that `data/`, generated `output/`, `site/node_modules/`,
and `site/dist/` are not staged.

## GitHub Repo Controls

For a public repo, keep these enabled:

1. Branch protection for `main`.
2. Required CI before merge.
3. Dependabot alerts and updates.
4. Issue/PR templates when the collaboration workflow settles.
