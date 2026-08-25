---
title: Wiki Economics
---

# Wiki Economics

<div class="page-intro">

Wikipedia can be understood through the lens of economics: every edit is a unit of production, every editor is a worker, and every namespace is a sector of the economy. This project applies economic indicators to Wikipedia activity data, letting you explore how the encyclopedia's workforce and output evolve over time.

</div>

```js
import {html} from "npm:htl@1.0.0"
import {fmtNum, fmtBytes} from "./components/filters.js"
import {withExport, pageExportBar} from "./components/exports.js"

const overview = await FileAttachment("data/defaults_overview.json").json()
const trend = overview.trend ?? []
const byWiki = overview.byWiki ?? []
```

```js
const latest = trend.at(-1) ?? {}
const revertRate = latest.total_edits > 0 ? latest.reverted_edits / latest.total_edits : 0
const freshestMonth = byWiki.reduce((max, row) => (row.latestMonth > max ? row.latestMonth : max), "")

// TODO(you): wikis currently reach this dashboard on different schedules --
// some refresh live, others sit frozen at an import cutoff (see
// config/wiki-lifecycle.json). Right now every row in the table below is
// treated the same regardless of how far behind it is. Decide how "stale"
// should be surfaced here: a fixed month-gap threshold vs. reading each
// wiki's provenance/refresh status from manifest.json directly, a quiet
// footnote vs. a visible badge, and whether staleness should ever affect the
// KPI totals above (it currently doesn't -- they're a plain sum). Wire your
// answer into the `wk-stale-row` class below; a `.wk-stale-row` style already
// exists in style.css.
function isWikiStale(row, freshestMonth) {
  return false
}
```

```js
pageExportBar([{name: "global_trend", data: trend}, {name: "wiki_breakdown", data: byWiki}])
```

<div class="kpi-row">
  <div class="kpi-card">
    <div class="kpi-value">${fmtNum(latest.total_edits)}</div>
    <div class="kpi-label">Total Edits</div>
    <div class="kpi-sub">latest year, all wikis</div>
  </div>
  <div class="kpi-card">
    <div class="kpi-value">${fmtBytes(latest.net_bytes)}</div>
    <div class="kpi-label">Net Output</div>
    <div class="kpi-sub">latest year, all wikis</div>
  </div>
  <div class="kpi-card">
    <div class="kpi-value">${fmtNum(latest.unique_editors)}</div>
    <div class="kpi-label">Unique Editors</div>
    <div class="kpi-sub">latest year, all wikis</div>
  </div>
  <div class="kpi-card">
    <div class="kpi-value">${(revertRate * 100).toFixed(1)}%</div>
    <div class="kpi-label">Revert Rate</div>
    <div class="kpi-sub">latest year, all wikis</div>
  </div>
</div>

<div class="chart-section">

## Output Across All Wikis

```js
withExport(Plot.plot({
  width,
  height: 320,
  x: {label: "Year"},
  y: {grid: true, label: "Total edits"},
  marks: [
    Plot.lineY(trend, {x: "period", y: "total_edits", stroke: "steelblue", strokeWidth: 2}),
    Plot.tip(trend, Plot.pointerX({x: "period", y: "total_edits", title: d => `${d.period}\nEdits: ${fmtNum(d.total_edits)}\nNet output: ${fmtBytes(d.net_bytes)}`})),
    Plot.ruleY([0]),
  ]
}), trend, "global_trend")
```

<div class="note">Registered-editor, article-namespace edits summed across every published wiki, by year. The current year is partial and will grow as more months are ingested.</div>

</div>

<div class="chart-section">

## By Wiki

<div class="note">Each wiki refreshes on its own schedule right now, so the "as of" month differs by row. Click a wiki to open its <a href="/gdp">Content Production</a> breakdown.</div>

```js
html`<table>
  <thead>
    <tr>
      <th>Wiki</th>
      <th style="text-align:center">As Of</th>
      <th style="text-align:center">Total Edits</th>
      <th style="text-align:center">Unique Editors</th>
      <th style="text-align:center">Net Output</th>
    </tr>
  </thead>
  <tbody>
    ${byWiki.map(row => html`<tr class="${isWikiStale(row, freshestMonth) ? "wk-stale-row" : ""}">
      <td><a href="/gdp?wiki=${encodeURIComponent(row.wiki)}">${row.wiki}</a></td>
      <td style="text-align:center">${row.latestMonth}</td>
      <td style="text-align:center">${fmtNum(row.total_edits)}</td>
      <td style="text-align:center">${fmtNum(row.unique_editors)}</td>
      <td style="text-align:center">${fmtBytes(row.net_bytes)}</td>
    </tr>`)}
  </tbody>
</table>`
```

</div>

<div class="landing-nav">

- **[Edit Distribution →](/inequality)**: How evenly are edits distributed? Gini, Theil, Palma, and fragility metrics.
- **[Edit Variation →](/edit-variation)**: The biggest week-over-week edit spikes, with direct links to the affected wiki pages.
- **[Community →](/labor)**: Editor workforce: arrivals, departures, retention, and cohort survival.
- **[Content Production →](/gdp)**: Output measured in bytes and edits, by namespace, user type, and activity tier.
- **[Patrol →](/patrol)**: Quality control: patrol volume, latency, coverage, and reviewer concentration.

</div>
