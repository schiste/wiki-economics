---
title: Content Production
---

# Content Production

<div class="page-intro">

In national economics, **[GDP](https://en.wikipedia.org/wiki/Gross_domestic_product)** (Gross Domestic Product) measures the total value of goods and services produced. On Wikipedia, we borrow that metaphor: **output** is the content produced by editors, measured in bytes and edits. *Gross output* counts every byte added; *net output* subtracts deletions and reverts, showing what actually remains in the encyclopedia. Together these metrics reveal how productive the wiki economy is -- and how much of its labor goes to churn rather than lasting growth.

</div>

```js
import {queryGrouped, toPeriod, nsLabel, fmtNum, fmtBytes, createFilterBar, isDefaultView, parseDefaultsMeta, startLoading, doneLoading} from "./components/filters.js"
import {withExport, pageExportBar} from "./components/exports.js"

const defaults = await FileAttachment("data/defaults_gdp.json").json()
const {wikis, nsByWiki, rangeByWiki, defaultWiki, maxMonth} = parseDefaultsMeta(defaults)
```

```js
const _preload = setTimeout(() => import("observablehq:stdlib/duckdb"), 1)
```

<!-- ── Filters ────────────────────────────────────────────────── -->

```js
const filters = view(createFilterBar({
  wikis, nsByWiki, rangeByWiki, defaultWiki, maxMonth,
  extraInputs: [{key: "breakdown", input: Inputs.toggle({label: "Break down by user type", value: false})}]
}))
```

```js
const {wiki, userTypes, granularity, startPeriod, endPeriod, namespaces, breakdown} = filters
```

<!-- ── Data processing ────────────────────────────────────────── -->

```js
const useDefaults = isDefaultView(filters, defaults)

startLoading()
let output, byType
if (useDefaults) {
  output = defaults.output
  byType = defaults.byType
} else {
  const {DuckDBClient: DDB} = await import("observablehq:stdlib/duckdb")
  const db = await DDB.of({
    gdp: FileAttachment("data/gdp.parquet"),
    typeShare: FileAttachment("data/gdp_user_type_share.parquet"),
    tiers: FileAttachment("data/gdp_activity_tiers.parquet"),
  })
  const gdpRaw = await db.sql`SELECT year_month, page_namespace, user_type, gross_bytes_added, net_bytes, total_edits, productive_edits, reverted_edits, unique_editors FROM gdp WHERE wiki = ${wiki}`
  output = await queryGrouped(db, "gdp", {
    sumCols: ["gross_bytes_added", "net_bytes", "total_edits", "productive_edits", "reverted_edits", "unique_editors"],
    wiki, userTypes, namespaces, startPeriod, endPeriod, granularity
  })
  const byTypeRows = Array.from(gdpRaw)
    .filter(d => userTypes.includes(d.user_type) && namespaces.includes(d.page_namespace)
      && d.year_month >= startPeriod && d.year_month <= endPeriod)
    .map(d => ({...d, period: toPeriod(d.year_month, granularity)}))
  byType = d3.rollups(byTypeRows, v => ({
      gross_bytes_added: d3.sum(v, d => d.gross_bytes_added),
      net_bytes: d3.sum(v, d => d.net_bytes),
      total_edits: d3.sum(v, d => d.total_edits),
      reverted_edits: d3.sum(v, d => d.reverted_edits),
      unique_editors: d3.sum(v, d => d.unique_editors),
    }), d => d.period, d => d.user_type)
    .flatMap(([period, types]) => types.map(([user_type, agg]) => ({period, user_type, ...agg})))
    .sort((a, b) => d3.ascending(a.period, b.period))
}
doneLoading()

const tickStep = Math.max(1, Math.floor(output.length / 20))
const typeColor = {legend: true, domain: ["registered", "temporary", "anonymous", "bot"], range: ["steelblue", "orange", "gold", "tomato"]}
```

<!-- ── KPI row ────────────────────────────────────────────────── -->

```js
const latest = output.at(-1) ?? {}
const revertRate = latest.total_edits > 0 ? latest.reverted_edits / latest.total_edits : 0
```

```js
pageExportBar([{name: "content_production", data: output}])
```

<div class="kpi-row">
  <div class="kpi-card">
    <div class="kpi-value">${fmtBytes(latest.gross_bytes_added)}</div>
    <div class="kpi-label">Gross Output</div>
    <div class="kpi-sub">latest period</div>
  </div>
  <div class="kpi-card">
    <div class="kpi-value">${fmtBytes(latest.net_bytes)}</div>
    <div class="kpi-label">Net Output</div>
    <div class="kpi-sub">latest period</div>
  </div>
  <div class="kpi-card">
    <div class="kpi-value">${fmtNum(latest.total_edits)}</div>
    <div class="kpi-label">Total Edits</div>
    <div class="kpi-sub">latest period</div>
  </div>
  <div class="kpi-card">
    <div class="kpi-value">${(revertRate * 100).toFixed(1)}%</div>
    <div class="kpi-label">Revert Rate</div>
    <div class="kpi-sub">latest period</div>
  </div>
</div>

<!-- ── Chart 1: Gross vs Net Output ──────────────────────────── -->

<div class="chart-section">

## Gross vs Net Output

```js
withExport(breakdown
  ? Plot.plot({
      width,
      height: 400,
      color: typeColor,
      x: {type: "band", tickRotate: -45, tickFilter: (d, i) => i % tickStep === 0},
      y: {grid: true, label: "Net bytes"},
      marks: [
        Plot.lineY(byType, {x: "period", y: "net_bytes", stroke: "user_type", strokeWidth: 1.5}),
        Plot.tip(byType, Plot.pointerX({x: "period", y: "net_bytes", stroke: "user_type", title: d => `${d.period}\n${d.user_type}\nNet: ${fmtBytes(d.net_bytes)}`})),
        Plot.ruleY([0]),
      ]
    })
  : Plot.plot({
      width,
      height: 400,
      x: {type: "band", tickRotate: -45, tickFilter: (d, i) => i % tickStep === 0},
      y: {grid: true, label: "Bytes"},
      marks: [
        Plot.lineY(output, {x: "period", y: "gross_bytes_added", stroke: "steelblue", strokeWidth: 1, strokeOpacity: 0.5}),
        Plot.lineY(output, {x: "period", y: "net_bytes", stroke: "seagreen", strokeWidth: 1.5}),
        Plot.tip(output, Plot.pointerX({x: "period", y: "gross_bytes_added", title: d => `${d.period}\nGross: ${fmtBytes(d.gross_bytes_added)}\nNet: ${fmtBytes(d.net_bytes)}`})),
        Plot.ruleY([0]),
      ]
    }), breakdown ? byType : output, "output")
```

<div class="note">Blue (faint) = gross bytes added. Green = net bytes (after deletions). Toggle "Break down by user type" to see per-type output.</div>

<details class="methodology">
<summary>Methodology</summary>

`Gross Output = Σ positive byte diffs (content added)`
`Net Output = Σ all byte diffs = Bytes Added − Bytes Deleted`
`Content Churn = Gross − Net`

**Gross output** is the sum of positive byte diffs only -- content added. **Net output** is the sum of all byte diffs (additions minus deletions). The gap between the two represents *content churn*: edits that were later removed or reverted. A growing gap suggests increasing maintenance overhead; a narrow gap means most new content sticks.

</details>

</div>

<!-- ── Chart 2: Revert Rate ──────────────────────────────────── -->

<div class="chart-section">

## Revert Rate

```js
withExport(breakdown
  ? Plot.plot({
      width,
      height: 300,
      color: typeColor,
      x: {type: "band", tickRotate: -45, tickFilter: (d, i) => i % tickStep === 0},
      y: {grid: true, label: "Revert rate", percent: true},
      marks: [
        Plot.lineY(byType, {x: "period", y: d => d.total_edits > 0 ? d.reverted_edits / d.total_edits : 0, stroke: "user_type", strokeWidth: 1.5}),
        Plot.tip(byType, Plot.pointerX({x: "period", y: d => d.total_edits > 0 ? d.reverted_edits / d.total_edits : 0, stroke: "user_type", title: d => `${d.period}\n${d.user_type}\nRevert rate: ${(d.total_edits > 0 ? d.reverted_edits / d.total_edits * 100 : 0).toFixed(1)}%\nReverted: ${fmtNum(d.reverted_edits)} / ${fmtNum(d.total_edits)}`})),
      ]
    })
  : Plot.plot({
      width,
      height: 300,
      x: {type: "band", tickRotate: -45, tickFilter: (d, i) => i % tickStep === 0},
      y: {grid: true, label: "Revert rate", percent: true},
      marks: [
        Plot.lineY(output, {x: "period", y: d => d.total_edits > 0 ? d.reverted_edits / d.total_edits : 0, stroke: "tomato", strokeWidth: 1.5}),
        Plot.tip(output, Plot.pointerX({x: "period", y: d => d.total_edits > 0 ? d.reverted_edits / d.total_edits : 0, title: d => `${d.period}\nRevert rate: ${(d.total_edits > 0 ? d.reverted_edits / d.total_edits * 100 : 0).toFixed(1)}%\nReverted: ${fmtNum(d.reverted_edits)} / ${fmtNum(d.total_edits)} edits`})),
      ]
    }), breakdown ? byType : output, "revert_rate")
```

<details class="methodology">
<summary>Methodology</summary>

`Revert Rate = Reverted Edits / Total Edits × 100%`

Revert rate is the fraction of edits that were **identity-reverted** -- i.e., the same content was restored by a subsequent edit. High revert rates may indicate [edit wars](https://en.wikipedia.org/wiki/Edit_warring) or vandalism-and-cleanup cycles. A sustained high rate for a particular user type can signal systemic issues with content quality from that group.

</details>

</div>

<!-- ── Chart 3: Productivity ─────────────────────────────────── -->

<div class="chart-section">

## Productivity: Net Bytes Per Edit

```js
withExport(breakdown
  ? Plot.plot({
      width,
      height: 300,
      color: typeColor,
      x: {type: "band", tickRotate: -45, tickFilter: (d, i) => i % tickStep === 0},
      y: {grid: true, label: "Net bytes per edit"},
      marks: [
        Plot.lineY(byType, {x: "period", y: d => d.total_edits > 0 ? d.net_bytes / d.total_edits : 0, stroke: "user_type", strokeWidth: 1.5}),
        Plot.tip(byType, Plot.pointerX({x: "period", y: d => d.total_edits > 0 ? d.net_bytes / d.total_edits : 0, stroke: "user_type", title: d => `${d.period}\n${d.user_type}\n${fmtBytes(d.total_edits > 0 ? d.net_bytes / d.total_edits : 0)}/edit\nEdits: ${fmtNum(d.total_edits)}`})),
        Plot.ruleY([0]),
      ]
    })
  : Plot.plot({
      width,
      height: 300,
      x: {type: "band", tickRotate: -45, tickFilter: (d, i) => i % tickStep === 0},
      y: {grid: true, label: "Net bytes per edit"},
      marks: [
        Plot.lineY(output, {x: "period", y: d => d.total_edits > 0 ? d.net_bytes / d.total_edits : 0, stroke: "var(--theme-foreground-focus)", strokeWidth: 1.5}),
        Plot.tip(output, Plot.pointerX({x: "period", y: d => d.total_edits > 0 ? d.net_bytes / d.total_edits : 0, title: d => `${d.period}\n${fmtBytes(d.total_edits > 0 ? d.net_bytes / d.total_edits : 0)}/edit\nNet: ${fmtBytes(d.net_bytes)}\nEdits: ${fmtNum(d.total_edits)}`})),
        Plot.ruleY([0]),
      ]
    }), breakdown ? byType : output, "productivity")
```

<div class="note">Productivity can be negative when more content is deleted than added in a period. This is common during cleanup campaigns or policy changes that trigger mass removals.</div>

<details class="methodology">
<summary>Methodology</summary>

`Productivity = Net Bytes / Total Edits` (bytes per edit)

[Productivity](https://en.wikipedia.org/wiki/Productivity) is computed as **net bytes divided by total edits**. It measures the average content contribution per edit. Positive values mean the wiki is growing; negative values mean more content is being removed than added. This metric is sensitive to outliers -- a single large page creation or deletion can swing it dramatically.

</details>

</div>

<!-- ── Chart 4: Activity Tiers ───────────────────────────────── -->

<div class="chart-section">

## Activity Tiers

<div class="note">Editors bucketed by their monthly edit count. Shows how output concentrates among different activity levels. Filtered by selected user types and time range (namespace filter does not apply -- tiers are computed across all namespaces).</div>

```js
const tierOrder = ["1 edit", "2-4 edits", "5-24 edits", "25-99 edits", "100+ edits"]
const tierColors = ["#bdd7e7", "#6baed6", "#3182bd", "#08519c", "#022a5a"]

startLoading()
let tiersAgg
if (useDefaults) {
  tiersAgg = defaults.tiers.map(d => ({...d, activity_tier: d.activity_tier, editors: d.editors, total_edits: d.total_edits, gross_bytes: d.gross_bytes, net_bytes: d.net_bytes}))
} else {
  const {DuckDBClient: DDB} = await import("observablehq:stdlib/duckdb")
  const db = await DDB.of({tiers: FileAttachment("data/gdp_activity_tiers.parquet")})
  const tiersRaw = Array.from(await db.sql`SELECT * FROM tiers WHERE wiki = ${wiki}`)
  const tiersFiltered = tiersRaw
    .filter(d => userTypes.includes(d.user_type) && d.year_month >= startPeriod && d.year_month <= endPeriod)
    .map(d => ({...d, period: toPeriod(d.year_month, granularity)}))
  tiersAgg = d3.rollups(tiersFiltered, v => ({
      editors: d3.sum(v, d => d.editors),
      total_edits: d3.sum(v, d => d.total_edits),
      gross_bytes: d3.sum(v, d => d.gross_bytes),
      net_bytes: d3.sum(v, d => d.net_bytes),
    }), d => d.period, d => d.activity_tier)
    .flatMap(([period, tList]) => tList.map(([activity_tier, agg]) => ({period, activity_tier, ...agg})))
    .sort((a, b) => d3.ascending(a.period, b.period))
}
doneLoading()

const tierTick = Math.max(1, Math.floor(new Set(tiersAgg.map(d => d.period)).size / 20))
```

```js
const tierMetric = view(Inputs.radio(["editors", "total_edits", "gross_bytes", "net_bytes"], {
  label: "Metric",
  value: "editors",
  format: d => ({editors: "Editors", total_edits: "Edits", gross_bytes: "Gross bytes", net_bytes: "Net bytes"}[d])
}))
```

```js
const tierFmt = (tierMetric === "gross_bytes" || tierMetric === "net_bytes") ? fmtBytes : fmtNum
```

```js
withExport(Plot.plot({
  width,
  height: 400,
  color: {legend: true, domain: tierOrder, range: tierColors},
  x: {type: "band", tickRotate: -45, tickFilter: (d, i) => i % tierTick === 0},
  y: {grid: true, label: tierMetric},
  marks: [
    Plot.barY(tiersAgg, Plot.stackY({x: "period", y: tierMetric, fill: "activity_tier", order: d => tierOrder.indexOf(d.activity_tier)})),
    Plot.tip(tiersAgg, Plot.pointerX(Plot.stackY({x: "period", y: tierMetric, fill: "activity_tier", order: d => tierOrder.indexOf(d.activity_tier), title: d => `${d.period}\n${d.activity_tier}\n${tierFmt(d[tierMetric])}`}))),
    Plot.ruleY([0]),
  ]
}), tiersAgg, "activity_tiers")
```

### Tier Share Over Time

```js
const tierShareData = d3.rollups(tiersAgg, v => {
    const total = d3.sum(v, d => d[tierMetric]);
    return v.map(d => ({...d, share: total > 0 ? d[tierMetric] / total : 0}));
  }, d => d.period)
  .flatMap(([, rows]) => rows)
```

```js
withExport(Plot.plot({
  width,
  height: 300,
  color: {legend: true, domain: tierOrder, range: tierColors},
  x: {type: "band", tickRotate: -45, tickFilter: (d, i) => i % tierTick === 0},
  y: {grid: true, label: "Share", percent: true},
  marks: [
    Plot.areaY(tierShareData, Plot.stackY({x: "period", y: "share", fill: "activity_tier", order: d => tierOrder.indexOf(d.activity_tier)})),
    Plot.tip(tierShareData, Plot.pointerX(Plot.stackY({x: "period", y: "share", fill: "activity_tier", order: d => tierOrder.indexOf(d.activity_tier), title: d => `${d.period}\n${d.activity_tier}\nShare: ${(d.share * 100).toFixed(1)}%`}))),
  ]
}), tierShareData, "tier_share")
```

<div class="note">Stacked area shows what fraction of the selected metric comes from each activity tier. Use the metric radio to switch between editors, edits, and bytes.</div>

<details class="methodology">
<summary>Methodology</summary>

`Tier(editor) = bucket(edit_count): 1 | 2–4 | 5–24 | 25–99 | 100+`

Editors are bucketed by monthly edit count into five tiers: **1 edit** (one-time contributors), **2--4 edits**, **5--24 edits**, **25--99 edits**, and **100+ edits** (power users). The stacked bar shows absolute values; the stacked area shows shares. This reveals how output concentrates among activity levels -- typically a small number of power users produce a disproportionate share of total edits and bytes.

</details>

</div>

<!-- ── Chart 5: User Type Share ───────────────────────────────── -->

<div class="chart-section">

## User Type Share of Economy

```js
startLoading()
let shareAgg
if (useDefaults) {
  // Compute shares from pre-aggregated type share data
  const shareGrouped = d3.rollups(defaults.typeShare, v => d3.sum(v, d => d.edits), d => d.period, d => d.user_type)
  shareAgg = shareGrouped
    .flatMap(([period, types]) => {
      const total = d3.sum(types, ([, v]) => v);
      return types.map(([user_type, edits]) => ({period, user_type, edits, share: total > 0 ? edits / total : 0}));
    })
    .sort((a, b) => d3.ascending(a.period, b.period))
} else {
  const {DuckDBClient: DDB} = await import("observablehq:stdlib/duckdb")
  const db = await DDB.of({typeShare: FileAttachment("data/gdp_user_type_share.parquet")})
  const shareRaw = await db.sql`SELECT * FROM typeShare WHERE wiki = ${wiki}`
  const shareData = Array.from(shareRaw)
    .filter(d => d.year_month >= startPeriod && d.year_month <= endPeriod)
    .map(d => ({...d, period: toPeriod(d.year_month, granularity)}))
  shareAgg = d3.rollups(shareData, v => d3.sum(v, d => d.edits), d => d.period, d => d.user_type)
    .flatMap(([period, types]) => {
      const total = d3.sum(types, ([, v]) => v);
      return types.map(([user_type, edits]) => ({period, user_type, edits, share: total > 0 ? edits / total : 0}));
    })
    .sort((a, b) => d3.ascending(a.period, b.period))
}
doneLoading()
```

```js
withExport(Plot.plot({
  width,
  height: 300,
  color: typeColor,
  x: {type: "band", tickRotate: -45, tickFilter: (d, i) => i % tickStep === 0},
  y: {grid: true, label: "Share of edits", percent: true},
  marks: [
    Plot.areaY(shareAgg, {x: "period", y: "share", fill: "user_type", order: "user_type"}),
    Plot.tip(shareAgg, Plot.pointerX({x: "period", y: "share", fill: "user_type", title: d => `${d.period}\n${d.user_type}\nShare: ${(d.share * 100).toFixed(1)}%\nEdits: ${fmtNum(d.edits)}`})),
  ]
}), shareAgg, "user_type_share")
```

<div class="note">All user types are shown regardless of the filter selection above, so you can always see the full composition of the editing population.</div>

<details class="methodology">
<summary>Methodology</summary>

`Share(type) = Edits by type / Total Edits × 100%`

This stacked area shows what fraction of total edits comes from each user type over time. **All user types are displayed regardless of the filter** so the full composition is always visible. The shift from anonymous to registered editing over the years reflects both policy changes (account creation incentives, [CAPTCHA](https://en.wikipedia.org/wiki/CAPTCHA)) and the maturation of the editor community.

</details>

</div>

<!-- ── Chart 6: Sectoral Output ──────────────────────────────── -->

<div class="chart-section">

## Sectoral Output (by Namespace)

```js
startLoading()
let sectorAgg
if (useDefaults) {
  // Default view has only ns 0, so sectoral is just one sector
  sectorAgg = defaults.byNamespace.map(d => ({
    period: d.period, ns_label: nsLabel(d.page_namespace),
    edits: d.edits, gross_bytes: d.gross_bytes, net_bytes: d.net_bytes
  }))
} else {
  const {DuckDBClient: DDB} = await import("observablehq:stdlib/duckdb")
  const db = await DDB.of({gdp: FileAttachment("data/gdp.parquet")})
  const gdpRaw = await db.sql`SELECT year_month, page_namespace, user_type, gross_bytes_added, net_bytes, total_edits FROM gdp WHERE wiki = ${wiki}`
  const sectorRows = Array.from(gdpRaw)
    .filter(d => userTypes.includes(d.user_type) && namespaces.includes(d.page_namespace)
      && d.year_month >= startPeriod && d.year_month <= endPeriod)
    .map(d => ({...d, period: toPeriod(d.year_month, granularity), ns_label: nsLabel(d.page_namespace)}))
  sectorAgg = d3.rollups(sectorRows, v => ({
      edits: d3.sum(v, d => d.total_edits),
      gross_bytes: d3.sum(v, d => d.gross_bytes_added),
      net_bytes: d3.sum(v, d => d.net_bytes),
    }), d => d.period, d => d.ns_label)
    .flatMap(([period, nsList]) => nsList.map(([ns_label, agg]) => ({period, ns_label, ...agg})))
    .sort((a, b) => d3.ascending(a.period, b.period))
}
doneLoading()
```

```js
withExport(Plot.plot({
  width,
  height: 400,
  color: {legend: true},
  x: {type: "band", tickRotate: -45, tickFilter: (d, i) => i % tickStep === 0},
  y: {grid: true, label: "Edits"},
  marks: [
    Plot.barY(sectorAgg, Plot.stackY({x: "period", y: "edits", fill: "ns_label", order: "sum"})),
    Plot.tip(sectorAgg, Plot.pointerX(Plot.stackY({x: "period", y: "edits", fill: "ns_label", order: "sum", title: d => `${d.period}\n${d.ns_label}\nEdits: ${fmtNum(d.edits)}\nGross: ${fmtBytes(d.gross_bytes)}\nNet: ${fmtBytes(d.net_bytes)}`}))),
    Plot.ruleY([0])
  ]
}), sectorAgg, "sectoral_output")
```

<div class="note">The Article namespace (ns 0) is typically dominant, but Talk, User, and Wikipedia namespaces reveal the community maintenance work that keeps the encyclopedia running.</div>

<details class="methodology">
<summary>Methodology</summary>

`Sector Edits = Σ edits WHERE page_namespace = ns`

Edits are broken down by **namespace** (sector). The Article namespace is typically dominant, but Talk, User, and Wikipedia namespaces show community maintenance work -- discussions, user page housekeeping, and policy pages. Other namespaces like Template and Module reflect technical infrastructure. Comparing sectoral output over time can reveal shifts in where the community invests its effort.

</details>

</div>
