---
title: Edit Distribution
---

# Edit Distribution

<p class="page-intro">
How evenly are edits distributed among Wikipedia editors? This page tracks four complementary inequality metrics — <a href="https://en.wikipedia.org/wiki/Gini_coefficient">Gini</a>, <a href="https://en.wikipedia.org/wiki/Theil_index">Theil</a>, <a href="https://en.wikipedia.org/wiki/Palma_ratio">Palma</a>, and Fragility — to reveal whether a wiki's output is broadly shared or concentrated in the hands of a few prolific contributors. Together they paint a picture of editorial power concentration and community resilience.
</p>

```js
import {queryGrouped, fmtNum, createFilterBar, isDefaultView, parseDefaultsMeta, startLoading, doneLoading} from "./components/filters.js"
import {withExport, pageExportBar} from "./components/exports.js"

const defaults = await FileAttachment("data/defaults_inequality.json").json()
const {wikis, rangeByWiki, defaultWiki, maxMonth} = parseDefaultsMeta(defaults)
```

```js
const _preload = setTimeout(() => import("observablehq:stdlib/duckdb"), 1)
```

<!-- ── Sticky filter bar ─────────────────────────────────── -->

```js
const filters = view(createFilterBar({wikis, rangeByWiki, defaultWiki, maxMonth, showNamespaces: false}))
```

```js
const {wiki, userTypes, granularity, startPeriod, endPeriod} = filters
```

<!-- ── Data pipeline ─────────────────────────────────────── -->

```js
const useDefaults = isDefaultView(filters, defaults, {defaultNamespaces: null})
startLoading()
let ineqData
if (useDefaults) {
  ineqData = defaults.data
} else {
  const {DuckDBClient: DDB} = await import("observablehq:stdlib/duckdb")
  const db = await DDB.of({inequality: FileAttachment("data/inequality.parquet")})
  ineqData = await queryGrouped(db, "inequality", {
    sumCols: ["total_editors", "total_edits", "min_editors_50pct"],
    avgCols: ["gini", "theil", "palma"],
    wiki, userTypes, namespaces: null, startPeriod, endPeriod, granularity
  })
}
doneLoading()
const tickStep = Math.max(1, Math.floor(ineqData.length / 20))
const latest = ineqData.length > 0 ? ineqData[ineqData.length - 1] : null
```

```js
pageExportBar([{name: "edit_distribution", data: ineqData}])
```

<!-- ── KPI row ───────────────────────────────────────────── -->

<div class="kpi-row">
<div class="kpi-card">
  <div class="kpi-value">${latest ? latest.gini.toFixed(3) : "—"}</div>
  <div class="kpi-label">Current Gini</div>
  <div class="kpi-sub">${latest ? latest.period : ""}</div>
</div>
<div class="kpi-card">
  <div class="kpi-value">${latest ? latest.theil.toFixed(3) : "—"}</div>
  <div class="kpi-label">Current Theil</div>
  <div class="kpi-sub">${latest ? latest.period : ""}</div>
</div>
<div class="kpi-card">
  <div class="kpi-value">${latest ? latest.palma.toFixed(2) : "—"}</div>
  <div class="kpi-label">Current Palma</div>
  <div class="kpi-sub">${latest ? (latest.palma > 1 ? "top 10% dominate" : "relatively equal") : ""}</div>
</div>
<div class="kpi-card">
  <div class="kpi-value">${latest && latest.total_editors > 0 ? (latest.min_editors_50pct / latest.total_editors * 100).toFixed(1) + "%" : "—"}</div>
  <div class="kpi-label">Fragility</div>
  <div class="kpi-sub">${latest ? fmtNum(latest.min_editors_50pct) + " editors for 50% of output" : ""}</div>
</div>
</div>

<!-- ── Gini ───────────────────────────────────────────────── -->

<div class="chart-section">

## Gini Coefficient Over Time

<div class="note">The <a href="https://en.wikipedia.org/wiki/Gini_coefficient">Gini coefficient</a> ranges from 0 (every editor contributes equally) to 1 (a single editor does everything). The dashed line at 0.5 marks the midpoint. Most active wikis sit well above 0.5, reflecting a heavy-tailed edit distribution.</div>

```js
withExport(Plot.plot({
  width,
  height: 400,
  x: {type: "band", tickRotate: -45, tickFilter: (d, i) => i % tickStep === 0},
  y: {grid: true, label: "Gini coefficient", domain: [0, 1]},
  marks: [
    Plot.areaY(ineqData, {x: "period", y: "gini", fill: "tomato", fillOpacity: 0.2}),
    Plot.lineY(ineqData, {x: "period", y: "gini", stroke: "tomato", strokeWidth: 1.5}),
    Plot.ruleY([0.5], {stroke: "grey", strokeDasharray: "4"}),
    Plot.tip(ineqData, Plot.pointerX({x: "period", y: "gini", title: d => `${d.period}\nGini: ${d.gini.toFixed(3)}\nEditors: ${fmtNum(d.total_editors)}\nEdits: ${fmtNum(d.total_edits)}`}))
  ]
}), ineqData, "gini")
```

<details class="methodology"><summary>How is this calculated?</summary>

`Gini = (2 × Σ rank × edits_i) / (n × Total Edits) − (n + 1) / n`
where n = number of editors, sorted by ascending edit count, rank = 1…n

The standard Gini coefficient is applied to the distribution of edits across all active editors in each period. A value of 0 means perfect equality (every editor made the same number of edits), while 1 means maximum inequality (one editor made all edits). The coefficient is computed per user type and then averaged across selected types.

</details>
</div>

<!-- ── Theil ──────────────────────────────────────────────── -->

<div class="chart-section">

## Theil Index (Decomposable Inequality)

<div class="note">Unlike Gini, the <a href="https://en.wikipedia.org/wiki/Theil_index">Theil index</a> can be decomposed into <strong>between-group</strong> and <strong>within-group</strong> components, making it useful for understanding whether inequality comes from differences between editor classes or within them. Higher values indicate greater concentration of edits.</div>

```js
withExport(Plot.plot({
  width,
  height: 400,
  x: {type: "band", tickRotate: -45, tickFilter: (d, i) => i % tickStep === 0},
  y: {grid: true, label: "Theil T index"},
  marks: [
    Plot.areaY(ineqData, {x: "period", y: "theil", fill: "var(--theme-foreground-focus)", fillOpacity: 0.15}),
    Plot.lineY(ineqData, {x: "period", y: "theil", stroke: "var(--theme-foreground-focus)", strokeWidth: 1.5}),
    Plot.tip(ineqData, Plot.pointerX({x: "period", y: "theil", title: d => `${d.period}\nTheil: ${d.theil.toFixed(3)}\n${d.theil > 2 ? "Very high concentration" : d.theil > 1 ? "High concentration" : d.theil > 0.5 ? "Moderate concentration" : "Low concentration"}`}))
  ]
}), ineqData, "theil")
```

<details class="methodology"><summary>How is this calculated?</summary>

`Theil T = (1/n) × Σ (edits_i / mean_edits) × ln(edits_i / mean_edits)`

The Theil T index is an entropy-based inequality measure. It is computed as the weighted sum of each editor's share of total edits multiplied by the logarithm of that share relative to equal distribution. Unlike Gini, Theil is perfectly decomposable: total inequality can be split into between-group and within-group components, which makes it especially useful when multiple user types are selected.

</details>
</div>

<!-- ── Palma ──────────────────────────────────────────────── -->

<div class="chart-section">

## Palma Ratio (Top 10% / Bottom 40%)

<div class="note">The <a href="https://en.wikipedia.org/wiki/Palma_ratio">Palma ratio</a> divides the edit share of the <strong>top 10%</strong> of editors by the share of the <strong>bottom 40%</strong>. A ratio above 1 (dashed line) means the most active decile contributes more than the bottom four deciles combined. This metric focuses attention on the tails of the distribution rather than the middle.</div>

```js
withExport(Plot.plot({
  width,
  height: 400,
  x: {type: "band", tickRotate: -45, tickFilter: (d, i) => i % tickStep === 0},
  y: {grid: true, label: "Palma ratio"},
  marks: [
    Plot.areaY(ineqData, {x: "period", y: "palma", fill: "orange", fillOpacity: 0.2}),
    Plot.lineY(ineqData, {x: "period", y: "palma", stroke: "orange", strokeWidth: 1.5}),
    Plot.ruleY([1], {stroke: "grey", strokeDasharray: "4"}),
    Plot.tip(ineqData, Plot.pointerX({x: "period", y: "palma", title: d => `${d.period}\nPalma: ${d.palma.toFixed(2)}\n${d.palma > 1 ? "Top 10% contribute more than bottom 40%" : "Bottom 40% contribute more than top 10%"}`}))
  ]
}), ineqData, "palma")
```

<details class="methodology"><summary>How is this calculated?</summary>

`Palma = Edits by top 10% of editors / Edits by bottom 40% of editors`

The Palma ratio is the share of total edits made by the top 10% of editors divided by the share made by the bottom 40%. A Palma ratio greater than 1 means the top decile of editors produces more output than the bottom four deciles combined. This metric was originally proposed in [income inequality](https://en.wikipedia.org/wiki/Economic_inequality) research because the "middle" (deciles 5-9) tends to be stable, so the real action is in the tails.

</details>
</div>

<!-- ── Fragility ──────────────────────────────────────────── -->

<div class="chart-section">

## Fragility: Editors Needed for 50% of Output

<div class="note">This is the <strong><a href="https://en.wikipedia.org/wiki/Bus_factor">bus factor</a></strong> of the wiki: the minimum number of editors whose combined edits account for at least 50% of all output. The ratio version (% of total editors) lets you compare fragility across periods with different editor populations. A lower number means the community is more fragile — if those few editors leave, half the output disappears.</div>

```js
const fragility = ineqData.map(d => ({...d, fragility_pct: d.total_editors > 0 ? d.min_editors_50pct / d.total_editors * 100 : 0}))
```

```js
withExport(Plot.plot({
  width,
  height: 400,
  x: {type: "band", tickRotate: -45, tickFilter: (d, i) => i % tickStep === 0},
  y: {grid: true, label: "% of editors needed for 50% of output"},
  marks: [
    Plot.areaY(fragility, {x: "period", y: "fragility_pct", fill: "steelblue", fillOpacity: 0.2}),
    Plot.lineY(fragility, {x: "period", y: "fragility_pct", stroke: "steelblue", strokeWidth: 1.5}),
    Plot.tip(fragility, Plot.pointerX({x: "period", y: "fragility_pct", title: d => `${d.period}\nFragility: ${d.fragility_pct.toFixed(1)}% of editors\n(${fmtNum(d.min_editors_50pct)} of ${fmtNum(d.total_editors)} editors)`}))
  ]
}), fragility, "fragility")
```

<details class="methodology"><summary>How is this calculated?</summary>

`Fragility = min k where top-k editors' edits ≥ 50% × Total Edits`
`Fragility Ratio = Fragility / Total Editors × 100%`

Editors are ranked by their edit count in descending order. Starting from the most prolific editor, edits are accumulated until the running total exceeds 50% of the period's total edits. The count of editors needed to reach that threshold is the fragility index. The **ratio** divides this count by total active editors, making it comparable across periods with different community sizes. A lower value signals higher risk: if just a handful of editors disengage, the wiki loses half its productive capacity.

</details>
</div>
