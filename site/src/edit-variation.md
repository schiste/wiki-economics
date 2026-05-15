---
title: Edit Variation
---

# Edit Variation

<div class="page-intro">

This page surfaces the largest **week-over-week edit surges** on article pages in the Dutch Wikipedia dataset. It ranks article-weeks by absolute edit growth, excludes rows without a prior-week baseline, and links each row directly to the live wiki page.

</div>

<div class="note">

Current scope: <strong>Dutch Wikipedia only (`nlwiki`)</strong>. The ranking uses article namespace only (`page_namespace = 0`) and considers only weeks where the same page had at least one edit in the immediately preceding week.

</div>

```js
import * as Inputs from "npm:@observablehq/inputs"
import {html} from "npm:htl"
import {fmtNum} from "./components/filters.js"

const defaults = await FileAttachment("data/defaults_edit_variation.json").json()
const summary = defaults.summary?.[0] ?? {rows: 0, min_week: "—", max_week: "—"}
const topVariation = defaults.topVariation ?? []
```

```js
function articleUrl(title) {
  return `https://nl.wikipedia.org/wiki/${encodeURIComponent(title)}`
}

function displayTitle(title) {
  return title.replaceAll("_", " ")
}

function fmtPct(value) {
  if (value == null || Number.isNaN(value)) return "—"
  return `${(value * 100).toFixed(2)}%`
}

const topVariationTable = topVariation.map((row, index) => ({
  rank: index + 1,
  articleLabel: displayTitle(row.page_title),
  articleUrl: articleUrl(row.page_title),
  week: `${row.week_start} to ${row.week_end}`,
  previous: fmtNum(row.previous_week_edits),
  edits: fmtNum(row.edits),
  change: `+${fmtNum(row.wow_change)}`,
  wow: fmtPct(row.wow_rate),
}))
```

<div class="kpi-row">
  <div class="kpi-card">
    <div class="kpi-value">${fmtNum(summary.rows)}</div>
    <div class="kpi-label">Article-Week Rows</div>
    <div class="kpi-sub">nlwiki only</div>
  </div>
  <div class="kpi-card">
    <div class="kpi-value">${summary.min_week}</div>
    <div class="kpi-label">First Week</div>
    <div class="kpi-sub">weekly series start</div>
  </div>
  <div class="kpi-card">
    <div class="kpi-value">${summary.max_week}</div>
    <div class="kpi-label">Latest Week</div>
    <div class="kpi-sub">weekly series end</div>
  </div>
</div>

<div class="chart-section">

## Top 20 Weekly Surges

<div class="note">

Rows are ordered by <strong>absolute WoW edit gain</strong>, not by percentage growth. The linked article titles open the current page on Dutch Wikipedia.

</div>

```js
html`<table>
  <thead>
    <tr>
      <th style="text-align:center">#</th>
      <th>Article</th>
      <th style="text-align:center">Week</th>
      <th style="text-align:center">Previous Week</th>
      <th style="text-align:center">Edits</th>
      <th style="text-align:center">WoW Change</th>
      <th style="text-align:center">WoW Rate</th>
    </tr>
  </thead>
  <tbody>
    ${topVariationTable.map(row => html`<tr>
      <td style="text-align:center">${row.rank}</td>
      <td><a href="${row.articleUrl}" target="_blank" rel="noopener noreferrer">${row.articleLabel}</a></td>
      <td style="text-align:center">${row.week}</td>
      <td style="text-align:center">${row.previous}</td>
      <td style="text-align:center">${row.edits}</td>
      <td style="text-align:center">${row.change}</td>
      <td style="text-align:center">${row.wow}</td>
    </tr>`)}
  </tbody>
</table>`
```

</div>
