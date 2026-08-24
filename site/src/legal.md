---
title: Legal, Licensing & Attribution
---

# Legal, Licensing & Attribution

```js
const publicationManifestAttachment = FileAttachment("data/manifest.json");
const publicationManifest = await publicationManifestAttachment.json();
const publicationManifestUrl = await publicationManifestAttachment.url();
const publishedSnapshots = Object.entries(
  publicationManifest.provenance?.selected_snapshot_versions ?? {}
).map(([wiki, snapshot]) => `${wiki}: ${snapshot}`).join(", ") || "Not recorded";
```

## Publication provenance

This site publication was generated at
**${publicationManifest.provenance?.generated_at ?? publicationManifest.generated_at ?? "Not recorded"}**
from commit
**${publicationManifest.provenance?.generating_commit ?? "Not recorded"}**.
Its run ID is **${publicationManifest.provenance?.run_id ?? "Not recorded"}** and
its selected Wikimedia snapshots are **${publishedSnapshots}**.

The downloadable artifact inventory and its per-file SPDX identifiers are in
the ${html`<a href=${publicationManifestUrl} download="manifest.json">machine-readable publication manifest</a>`}.
Each listed artifact is licensed as stated in that manifest; upstream Wikimedia
source material remains governed separately as described below.

## Project license

Everything this project itself wrote (the code, docs, dashboard text and
charts, and the aggregate datasets it generates) is MIT licensed. In
practice that means: reuse it, modify it, ship it commercially, whatever you
need, as long as you keep the copyright notice. See the
[full license text](https://github.com/schiste/wiki-economics/blob/main/LICENSE)
for the exact terms.

That license only covers what we actually own. It doesn't reach into the
Wikimedia source data the dashboard runs on, the third-party software it's
built with, anyone's trademarks, or personal information; those are each
governed by their own terms, covered in the sections below.

## Wikimedia sources

The dashboard derives aggregates from public
[MediaWiki History dumps](https://dumps.wikimedia.org/other/mediawiki_history/),
per-wiki logging XML dumps, and the
[MediaWiki Action API](https://www.mediawiki.org/wiki/API:Main_page).
Source-data reuse remains subject to the
[Wikimedia dump legal information](https://dumps.wikimedia.org/legal.html),
the [Wikimedia Foundation Terms of Use](https://foundation.wikimedia.org/wiki/Policy:Terms_of_Use),
and any applicable project-specific terms.

## Attribution and independence

Wiki Economics uses data from Wikimedia projects and is independently
developed. It is **not affiliated with, sponsored by, or endorsed by the
Wikimedia Foundation**.

Wikipedia and other Wikimedia names and marks belong to the Wikimedia
Foundation. Their use is governed by the
[Wikimedia Foundation Trademark Policy](https://foundation.wikimedia.org/wiki/Trademark_policy),
not by this project’s MIT license.

## Privacy

### Hosting and what this app stores

Wiki Economics runs entirely on
[Wikimedia Toolforge](https://wikitech.wikimedia.org/wiki/Portal:Toolforge),
part of Wikimedia Cloud Services, operated by the Wikimedia Foundation. The
dashboard itself:

- Sets no cookies, runs no analytics, and loads no third-party trackers.
- Stores a light/dark theme preference only in your browser's own
  `localStorage`; that preference never leaves your browser.
- Stores, server-side, only aggregate statistics computed from public
  Wikimedia data dumps (per-namespace edit counts, editor cohorts, patrol
  metrics, and similar). It does not collect or store personal information
  about site visitors.
- Includes an authenticated operator admin panel used to run the data
  pipeline; it is not visitor-facing and processes no visitor data.

Toolforge's own infrastructure (web server and ingress logs, SSH access, and
so on) is governed by the Wikimedia Foundation's own policies, not by this
project. Toolforge projects adhere to the
[Wikimedia Privacy Policy](https://foundation.wikimedia.org/wiki/Policy:Privacy_policy);
see also the
[Wikimedia Cloud Services Terms of Use](https://wikitech.wikimedia.org/wiki/Wikitech:Cloud_Services_Terms_of_use)
for how Toolforge itself is operated.

### Reuse of published data

Public availability and copyright licensing do not remove privacy or data
protection obligations. Reusers must still consider applicable privacy law,
Wikimedia policies, and the context of any public identifier represented in a
dataset.

## Third-party software

Dependencies retain their upstream licenses. This table is copied from the
CI-verified
[generated stack reference](https://github.com/schiste/wiki-economics/blob/main/docs/generated/stack-reference.md);
see that file for the current, authoritative versions if this page has
drifted. Full notices are in the
[third-party notices](https://github.com/schiste/wiki-economics/blob/main/THIRD_PARTY_NOTICES.md)
and [dependency inventory](https://github.com/schiste/wiki-economics/blob/main/docs/dependencies-and-licenses.md).

### Toolchains and site compiler

| Component | Version |
| --- | --- |
| Rust | `1.98.0` |
| Node.js | `24.15.0` |
| npm | `11.12.1` |
| Observable Framework | `1.13.4` |
| esbuild | `0.28.2` |

### Direct Rust dependencies

<div class="wide-table">

| Crate | Version | Role | License |
| --- | --- | --- | --- |
| `anyhow` | `1.0.104` | application error propagation and context | `MIT OR Apache-2.0` |
| `bzip2` | `0.5.2` | streaming MediaWiki History decompression | `MIT OR Apache-2.0` |
| `chrono` | `0.4.45` | UTC dates, timestamps, and snapshot boundaries | `MIT OR Apache-2.0` |
| `clap` | `4.6.6` | command-line parsing | `MIT OR Apache-2.0` |
| `flate2` | `1.1.9` | concatenated gzip decoding for logging dumps | `MIT OR Apache-2.0` |
| `fs4` | `1.1.0` | portable file locking | `MIT OR Apache-2.0` |
| `hex` | `0.4.3` | digest encoding | `MIT OR Apache-2.0` |
| `indicatif` | `0.18.6` | operator progress reporting | `MIT` |
| `polars` | `0.55.2` | Parquet/CSV dataframes, aggregation, and deterministic output | `MIT` |
| `quick-xml` | `0.41.0` | streaming MediaWiki logging XML parsing | `MIT` |
| `rayon` | `1.12.0` | bounded parallel work | `MIT OR Apache-2.0` |
| `regex` | `1.13.1` | validated source-name and text parsing | `MIT OR Apache-2.0` |
| `reqwest` | `0.12.28` | Wikimedia dump and API HTTP client | `MIT OR Apache-2.0` |
| `rustix` | `1.1.4` | filesystem durability operations | `Apache-2.0 WITH LLVM-exception OR Apache-2.0 OR MIT` |
| `serde` | `1.0.229` | typed serialization contracts | `MIT OR Apache-2.0` |
| `serde_json` | `1.0.151` | JSON manifests, receipts, and status records | `MIT OR Apache-2.0` |
| `sha2` | `0.11.0` | content and artifact fingerprints | `MIT OR Apache-2.0` |
| `tracing` | `0.1.44` | structured pipeline events | `MIT` |
| `tracing-subscriber` | `0.3.23` | structured log formatting and filtering | `MIT` |
| `url` | `2.5.8` | typed canonical snapshot source URLs | `MIT OR Apache-2.0` |

</div>

### Direct browser and site dependencies

<div class="wide-table">

| Package | Version | Role | License |
| --- | --- | --- | --- |
| `@observablehq/framework` | `1.13.4` | deterministic static-site compiler | `ISC` |
| `@observablehq/inputs` | `0.12.0` | interactive controls | `ISC` |
| `@observablehq/plot` | `0.6.17` | charts | `ISC` |
| `apache-arrow` | `21.2.0` | browser columnar data representation | `Apache-2.0` |
| `d3` | `7.9.0` | browser transforms and scales | `ISC` |
| `htl` | `1.0.0` | safe browser HTML templates | `ISC` |
| `parquet-wasm` | `0.7.2` | browser Parquet decoding | `MIT OR Apache-2.0` |
| `react` | `19.2.8` | exact Observable client JSX runtime resolution | `MIT` |
| `react-dom` | `19.2.8` | exact Observable client JSX renderer resolution | `MIT` |

</div>
