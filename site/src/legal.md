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

Wiki Economics’ project-owned software, documentation, dashboard prose and
graphics, and generated aggregate datasets are available under the
[MIT License](https://github.com/schiste/wiki-economics/blob/main/LICENSE)
(`MIT` in SPDX notation).

The MIT grant applies only to material the project has the right to license.
It does not replace the terms governing Wikimedia source data, third-party
software, trademarks, or personal information.

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

Dependencies retain their upstream licenses. See the
[third-party notices](https://github.com/schiste/wiki-economics/blob/main/THIRD_PARTY_NOTICES.md),
[dependency inventory](https://github.com/schiste/wiki-economics/blob/main/docs/dependencies-and-licenses.md),
and [source repository](https://github.com/schiste/wiki-economics) for details.
