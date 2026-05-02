# Publishing Guide

This repository is meant to stay usable as a local working tree while only the
publishable source baseline is shared on GitHub.

## Local-Only Material

Keep these locally. Do not commit them:

- `data/`
- generated `output/*.parquet`, `output/*.json`, and per-wiki output folders
- `site/node_modules/`
- `site/dist/`
- local cache directories such as `site/src/.observablehq/`

The root `.gitignore` is set up to keep those out of version control while
still allowing the checked-in dashboard generator scripts under `output/*.json.sh`.

## Public Baseline Contents

The initial public commit should include:

- Rust source under `src/`
- tests under `tests/`
- Python and shell utilities under `scripts/`
- site source under `site/src/`, `site/package.json`, and `site/package-lock.json`
- docs under `docs/`
- vendored patch code under `vendor/`
- repository policy/config files such as `.github/`, `.cargo/`, `Cargo.toml`,
  `Cargo.lock`, `deny.toml`, `.editorconfig`, `.gitattributes`, and `.gitignore`
- community and legal files such as `README.md`, `CONTRIBUTING.md`,
  `CODE_OF_CONDUCT.md`, `SECURITY.md`, `LICENSE`, `LICENSE-MIT`,
  and `LICENSE-APACHE`

## Local Verification

Run the canonical local verification command before the first public push:

```sh
./scripts/ci-local.sh
```

If that passes, the repo is in the same state expected by CI.

## First Commit

This repository currently has no commit history, so be explicit about what gets
staged for the initial public baseline.

Recommended staging sequence:

```sh
git add .cargo .github docs scripts site src tests vendor
git add .editorconfig .gitattributes .gitignore
git add CODE_OF_CONDUCT.md CONTRIBUTING.md SECURITY.md README.md
git add LICENSE LICENSE-MIT LICENSE-APACHE
git add Cargo.toml Cargo.lock deny.toml rust-toolchain.toml package.json
git add output/*.json.sh
git status --short
```

Before committing, verify that `data/`, generated `output/` artifacts,
`site/node_modules/`, and `site/dist/` are not staged.

Create the initial commit:

```sh
git commit -m "Prepare public open-source baseline"
```

## First Push

The local default branch is `main`.

After creating the GitHub repository:

```sh
git remote add origin <your-github-repo-url>
git push -u origin main
```

## After The First Push

Do these in GitHub before normal collaboration starts:

1. Enable branch protection for `main`.
2. Require the CI checks to pass before merge.
3. Turn on Dependabot alerts and updates.
4. Add issue and PR templates when ready.
