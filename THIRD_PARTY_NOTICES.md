# Third-Party Notices

Wiki Economics incorporates open-source dependencies that retain their own
copyrights and licenses. This notice is informational and does not replace the
license text or notices supplied by each upstream project. Exact resolved
dependency identities are recorded in `Cargo.lock` and `package-lock.json`.

## Distributed Rust components

The production `wiki-econ` binary directly uses the following crates. Cargo
also links their transitive dependency graphs; `cargo deny check licenses`
verifies that every resolved crate uses an approved permissive license.

| Component | Resolved version | License |
|---|---:|---|
| anyhow | 1.0.104 | MIT OR Apache-2.0 |
| bzip2 | 0.5.2 | MIT OR Apache-2.0 |
| chrono | 0.4.45 | MIT OR Apache-2.0 |
| clap | 4.6.6 | MIT OR Apache-2.0 |
| flate2 | 1.1.9 | MIT OR Apache-2.0 |
| fs4 | 1.1.0 | MIT OR Apache-2.0 |
| hex | 0.4.3 | MIT OR Apache-2.0 |
| indicatif | 0.18.6 | MIT |
| Polars | 0.55.2 | MIT |
| quick-xml | 0.41.0 | MIT |
| rayon | 1.12.0 | MIT OR Apache-2.0 |
| regex | 1.13.1 | MIT OR Apache-2.0 |
| reqwest | 0.12.28 | MIT OR Apache-2.0 |
| rustix | 1.1.4 | Apache-2.0 WITH LLVM-exception OR Apache-2.0 OR MIT |
| serde | 1.0.229 | MIT OR Apache-2.0 |
| serde_json | 1.0.151 | MIT OR Apache-2.0 |
| sha2 | 0.11.0 | MIT OR Apache-2.0 |
| tracing | 0.1.44 | MIT |
| tracing-subscriber | 0.3.23 | MIT |

The repository carries two patched upstream crates:

- `vendor/object-store` 0.13.2 — Apache-2.0. Its upstream NOTICE is retained
  at `vendor/object-store/NOTICE.txt`.
- `vendor/polars-utils` 0.55.2 — MIT. Its upstream license is retained at
  `vendor/polars-utils/LICENSE`.

## Distributed browser components

Observable Framework bundles browser modules used to render charts and decode
published Parquet files. The current production build uses these primary
components:

| Component | Resolved version | License |
|---|---:|---|
| Observable Framework | 1.13.4 | ISC |
| Observable Inputs | 0.12.0 | ISC |
| Observable Inspector | 5.0.1 | ISC |
| Observable Runtime | 6.0.0 | ISC |
| Observable Plot | 0.6.17 | ISC |
| D3 | 7.9.0 | ISC |
| Apache Arrow JavaScript | 21.2.0 | Apache-2.0 |
| HTL | 1.0.0 | ISC |
| parquet-wasm | 0.7.2 | MIT OR Apache-2.0 |

The Observable build also includes small transitive browser modules. Their
exact identities are allowlisted in `config/site-dependency-closure.json`; the
redistributed offline cache is annotated by upstream license in `REUSE.toml`.
The current closure deliberately contains no DuckDB asset.

## License texts

The project-wide MIT text is in `LICENSE`. SPDX license texts used by vendored
or project-owned files are retained under `LICENSES/`; package-specific notices
remain beside vendored source. Upstream source and license locations are listed
in `docs/dependencies-and-licenses.md`.
