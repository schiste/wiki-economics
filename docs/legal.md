# Legal, Licensing & Attribution

## Project license

Project-owned software, documentation, dashboard prose and graphics, and
generated aggregate datasets are released under the [MIT license](../LICENSE).
The SPDX identifier is `MIT`.

The MIT grant applies only to material the project has the right to license.
It does not replace or override the terms governing Wikimedia source data,
third-party software, trademarks, or personal information.

## Wikimedia sources

The pipeline derives aggregates from publicly available Wikimedia sources:

- [MediaWiki History dumps](https://dumps.wikimedia.org/other/mediawiki_history/)
- per-wiki MediaWiki logging XML dumps under
  [`dumps.wikimedia.org`](https://dumps.wikimedia.org/)
- the [MediaWiki Action API](https://www.mediawiki.org/wiki/API:Main_page)

Reuse of source data remains subject to the [Wikimedia dump legal
information](https://dumps.wikimedia.org/legal.html), the [Wikimedia
Foundation Terms of Use](https://foundation.wikimedia.org/wiki/Policy:Terms_of_Use),
and any source- or project-specific terms. Published manifests identify the
source families and selected snapshots used for each release.

Every artifact in the publication inventory has a discoverable SPDX license
identifier. The public legal page links the current machine-readable manifest,
which records the selected snapshot, generating commit, timestamp, run ID,
source URLs, and attribution for that publication.

## Attribution and independence

Wiki Economics uses data from Wikimedia projects and is independently
developed. It is not affiliated with, sponsored by, or endorsed by the
Wikimedia Foundation.

Wikipedia and other Wikimedia names and marks belong to the Wikimedia
Foundation. Their use is governed by the [Wikimedia Foundation Trademark
Policy](https://foundation.wikimedia.org/wiki/Trademark_policy) and is not
granted by this project's MIT license.

**Recorded trademark status:** the project has no recorded Wikimedia trademark
permission. It therefore uses the descriptive name **Wiki Economics** and uses
Wikimedia marks only to identify the source projects. If permission is later
obtained, it must be documented here and in
`config/publication-licensing.json` before the public branding changes.

## Toolforge open licensing

The Toolforge deployment explicitly satisfies the platform's open-source and
open-data requirement: project source and generated aggregate datasets are
publicly available under the SPDX `MIT` license. The source repository is
<https://github.com/schiste/wiki-economics>, and each published manifest repeats
both the source and dataset license identifiers. This statement does not claim
ownership of, or relicense, upstream Wikimedia source material.

## Privacy

Public availability and copyright licensing do not remove privacy or data
protection obligations. The pipeline uses public Wikimedia data, retains raw
and warehouse data only for processing, and publishes analytical outputs; any
reuse must still consider applicable privacy law, Wikimedia policies, and the
context of public identifiers.

## Third-party software

Dependencies and vendored code retain their respective upstream licenses.
See [`THIRD_PARTY_NOTICES.md`](../THIRD_PARTY_NOTICES.md), the checked-in
lockfiles, and [`docs/dependencies-and-licenses.md`](dependencies-and-licenses.md)
for the current engineering inventory.
