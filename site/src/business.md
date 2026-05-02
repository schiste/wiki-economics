---
title: Business Health
---

# Business Health

<div class="page-intro">

Wikipedia as a **[Knowledge-as-a-Service](https://en.wikipedia.org/wiki/As_a_service) (KaaS)** platform. This page applies business metrics to Wikipedia's community data — treating editors as the workforce, edits as output, and bytes as revenue.

</div>

```js
import {queryGrouped, toPeriod, fmtNum, fmtBytes, createFilterBar, isDefaultView, parseDefaultsMeta, startLoading, doneLoading} from "./components/filters.js"
import {withExport, pageExportBar} from "./components/exports.js"

const defaults = await FileAttachment("data/defaults_business.json").json()
const {wikis, nsByWiki, rangeByWiki, defaultWiki, maxMonth} = parseDefaultsMeta(defaults)
```

```js
const _preload = setTimeout(() => import("observablehq:stdlib/duckdb"), 1)
```

```js
const filters = view(createFilterBar({wikis, nsByWiki, rangeByWiki, defaultWiki, maxMonth}))
```

```js
const {wiki, userTypes, granularity, startPeriod, endPeriod, namespaces} = filters
```

```js
const _bizFiles = {
  labor: FileAttachment("data/labor_monthly.parquet"),
  churn: FileAttachment("data/labor_churn.parquet"),
  cohorts: FileAttachment("data/labor_cohorts.parquet"),
  gdp: FileAttachment("data/gdp.parquet"),
  tiers: FileAttachment("data/gdp_activity_tiers.parquet"),
  funnel: FileAttachment("data/business_funnel.parquet"),
}
let _bizDb = null
async function getDb() {
  if (!_bizDb) {
    const {DuckDBClient: DDB} = await import("observablehq:stdlib/duckdb")
    _bizDb = await DDB.of(_bizFiles)
  }
  return _bizDb
}

const useDefaults = isDefaultView(filters, defaults)
```

<!-- ── Data pipelines ─────────────────────────────────── -->

```js
// Churn data (registered editors only, pre-aggregated)
startLoading()
const startP = toPeriod(startPeriod, granularity)
const endP = toPeriod(endPeriod, granularity)
let churnData
if (useDefaults) {
  churnData = defaults.churn
} else {
  const churnRaw = Array.from(await (await getDb()).sql`SELECT period, period_type, active_editors, arrivals, departures, arrival_rate, departure_rate FROM churn WHERE wiki = ${wiki}`)
  churnData = churnRaw.filter(d => d.period_type === granularity && d.period >= startP && d.period <= endP)
}
doneLoading()
```

```js
// Activity tier data — respects user type filter
startLoading()
let tierAgg
if (useDefaults) {
  tierAgg = defaults.tiers
} else {
  const tiersRaw = Array.from(await (await getDb()).sql`SELECT year_month, user_type, activity_tier, editors, total_edits, gross_bytes, net_bytes FROM tiers WHERE wiki = ${wiki}`)
  const tierFiltered = tiersRaw
    .filter(d => userTypes.includes(d.user_type) && d.year_month >= startPeriod && d.year_month <= endPeriod)
    .map(d => ({...d, period: toPeriod(d.year_month, granularity)}))
  tierAgg = d3.rollups(tierFiltered, v => ({
    editors: d3.sum(v, d => d.editors),
    edits: d3.sum(v, d => d.total_edits),
    net_bytes: d3.sum(v, d => d.net_bytes),
    gross_bytes: d3.sum(v, d => d.gross_bytes),
  }), d => d.period, d => d.activity_tier)
    .flatMap(([period, tiers]) => tiers.map(([tier, vals]) => ({period, tier, ...vals})))
    .sort((a, b) => d3.ascending(a.period, b.period))
}
doneLoading()

const tierOrder = ["1-4 edits", "5-24 edits", "25-99 edits", "100+ edits"]
```

```js
// GDP data — respects user type and namespace filters
startLoading()
let survivalByPeriod, gdpRaw
if (useDefaults) {
  survivalByPeriod = defaults.survival.map(d => ({
    ...d,
    survival_rate: d.total_edits > 0 ? (d.total_edits - d.reverted_edits) / d.total_edits : 0
  }))
  gdpRaw = null
} else {
  gdpRaw = Array.from(await (await getDb()).sql`SELECT year_month, page_namespace, user_type, net_bytes, total_edits, reverted_edits, unique_editors FROM gdp WHERE wiki = ${wiki}`)
  const gdpFiltered = gdpRaw
    .filter(d => userTypes.includes(d.user_type) && namespaces.includes(d.page_namespace) && d.year_month >= startPeriod && d.year_month <= endPeriod)
    .map(d => ({...d, period: toPeriod(d.year_month, granularity)}))
  survivalByPeriod = d3.rollups(gdpFiltered, v => {
    const edits = d3.sum(v, d => d.total_edits)
    const reverted = d3.sum(v, d => d.reverted_edits)
    return {total_edits: edits, reverted_edits: reverted, survival_rate: edits > 0 ? (edits - reverted) / edits : 0}
  }, d => d.period)
    .map(([period, v]) => ({period, ...v}))
    .sort((a, b) => d3.ascending(a.period, b.period))
}
doneLoading()
```

```js
// Controversial equilibrium: talk page edits vs content page reverts
const talkNs = [1, 3, 5, 7, 9, 11, 13, 15, 101, 829, 1729]
const contentNs = [0, 2, 4, 6, 8, 10, 12, 14, 100, 828, 1728]

startLoading()
let eqByPeriod
if (useDefaults) {
  // defaults.equilibrium has per-period per-namespace aggregates
  const eqGrouped = d3.rollups(defaults.equilibrium, v => {
    const talkEdits = d3.sum(v.filter(d => talkNs.includes(d.page_namespace)), d => d.total_edits)
    const contentReverts = d3.sum(v.filter(d => contentNs.includes(d.page_namespace)), d => d.reverted_edits)
    return {talk_edits: talkEdits, content_reverts: contentReverts, ratio: contentReverts > 0 ? talkEdits / contentReverts : null}
  }, d => d.period)
  eqByPeriod = eqGrouped
    .map(([period, v]) => ({period, ...v}))
    .filter(d => d.ratio != null)
    .sort((a, b) => d3.ascending(a.period, b.period))
} else {
  const gdpForEq = gdpRaw
    .filter(d => userTypes.includes(d.user_type) && d.year_month >= startPeriod && d.year_month <= endPeriod)
    .map(d => ({...d, period: toPeriod(d.year_month, granularity)}))
  eqByPeriod = d3.rollups(gdpForEq, v => {
    const talkEdits = d3.sum(v.filter(d => talkNs.includes(d.page_namespace)), d => d.total_edits)
    const contentReverts = d3.sum(v.filter(d => contentNs.includes(d.page_namespace)), d => d.reverted_edits)
    return {talk_edits: talkEdits, content_reverts: contentReverts, ratio: contentReverts > 0 ? talkEdits / contentReverts : null}
  }, d => d.period)
    .map(([period, v]) => ({period, ...v}))
    .filter(d => d.ratio != null)
    .sort((a, b) => d3.ascending(a.period, b.period))
}
doneLoading()
```

```js
// Cohort data for LTV
startLoading()
let cohortData, yearlyBytesPerEditor
if (useDefaults) {
  cohortData = defaults.cohorts
  yearlyBytesPerEditor = defaults.yearlyBytesPerEditor.map(d => ({
    year: d.year,
    bytesPerEditor: d.unique_editors > 0 ? d.net_bytes / d.unique_editors : 0
  }))
} else {
  cohortData = Array.from(await (await getDb()).sql`SELECT * FROM cohorts WHERE wiki = ${wiki} ORDER BY cohort_year, year`)
  yearlyBytesPerEditor = d3.rollups(
    gdpRaw.filter(d => userTypes.includes(d.user_type) && d.page_namespace === 0),
    v => {
      const editors = d3.sum(v, d => d.unique_editors)
      const bytes = d3.sum(v, d => d.net_bytes)
      return editors > 0 ? bytes / editors : 0
    },
    d => d.year_month.slice(0, 4)
  ).map(([year, bytesPerEditor]) => ({year, bytesPerEditor}))
}
doneLoading()

// Estimated LTV per cohort: sum of (survival_rate x avg_bytes_per_editor) for each year
const ltvByCohort = d3.groups(cohortData, d => d.cohort_year)
  .filter(([, rows]) => rows[0].initial_editors >= 10)
  .map(([cohort, rows]) => {
    let cumulativeBytes = 0
    for (const r of rows) {
      const survival = r.initial_editors > 0 ? r.survived_editors / r.initial_editors : 0
      const yearBpe = yearlyBytesPerEditor.find(y => y.year === r.year)
      if (yearBpe) cumulativeBytes += survival * yearBpe.bytesPerEditor
    }
    return {cohort, initial_editors: rows[0].initial_editors, ltv_bytes: cumulativeBytes}
  })
```

```js
// KPI computations — aggregated over entire filtered period
const avgChurnRate = churnData.length > 0 ? d3.mean(churnData, d => d.departure_rate) : null
const totalEdits = d3.sum(survivalByPeriod, d => d.total_edits)
const totalReverted = d3.sum(survivalByPeriod, d => d.reverted_edits)
const overallSurvival = totalEdits > 0 ? (totalEdits - totalReverted) / totalEdits : null
const totalTalk = d3.sum(eqByPeriod, d => d.talk_edits)
const totalContentReverts = d3.sum(eqByPeriod, d => d.content_reverts)
const overallEqRatio = totalContentReverts > 0 ? totalTalk / totalContentReverts : null
const totalAllEditors = d3.sum(tierAgg, d => d.editors)
const totalPowerEditors = d3.sum(tierAgg.filter(d => d.tier === "100+ edits"), d => d.editors)
const overallConversion = totalAllEditors > 0 ? totalPowerEditors / totalAllEditors : 0

const tickStep = Math.max(1, Math.floor(churnData.length / 20))
```

```js
pageExportBar([
  {name: "churn", data: churnData},
  {name: "activity_tiers", data: tierAgg},
  {name: "edit_survival", data: survivalByPeriod},
  {name: "equilibrium", data: eqByPeriod},
])
```

<!-- ── KPI Row ────────────────────────────────────────── -->

<div class="kpi-row">
  <div class="kpi-card">
    <div class="kpi-value">${avgChurnRate != null ? (avgChurnRate * 100).toFixed(1) + "%" : "—"}</div>
    <div class="kpi-label">Avg Churn Rate</div>
    <div class="kpi-sub">mean over period</div>
  </div>
  <div class="kpi-card">
    <div class="kpi-value">${(overallConversion * 100).toFixed(1)}%</div>
    <div class="kpi-label">Power Conversion</div>
    <div class="kpi-sub">100+ edits share</div>
  </div>
  <div class="kpi-card">
    <div class="kpi-value">${overallSurvival != null ? (overallSurvival * 100).toFixed(1) + "%" : "—"}</div>
    <div class="kpi-label">Edit Survival</div>
    <div class="kpi-sub">across period</div>
  </div>
  <div class="kpi-card">
    <div class="kpi-value">${overallEqRatio != null ? overallEqRatio.toFixed(1) : "—"}</div>
    <div class="kpi-label">Talk / Revert Ratio</div>
    <div class="kpi-sub">${overallEqRatio != null ? (overallEqRatio > 1 ? "healthy consensus" : "revert-heavy") : "—"}</div>
  </div>
</div>

<!-- ═══════════════════════════════════════════════════ -->
<!-- SaaS-Repurposed Metrics                            -->
<!-- ═══════════════════════════════════════════════════ -->

<div class="chart-section">

## 1. Community Composition

<div class="note">

How is the editing workforce distributed across activity levels? Each period's editors are bucketed by edit count: casual (1-4), regular (5-24), active (25-99), and power (100+). A stable or growing "100+ edits" band indicates a healthy core; a thinning top layer signals power-editor attrition even if overall numbers hold.

</div>

```js
const funnelTick = Math.max(1, Math.floor(tierAgg.filter(d => d.tier === tierOrder[0]).length / 20))
```

```js
withExport(Plot.plot({
  width,
  height: 400,
  color: {legend: true, domain: tierOrder, range: ["#b8d4e3", "#6baed6", "#2171b5", "#08306b"]},
  x: {type: "band", tickRotate: -45, tickFilter: (d, i) => i % funnelTick === 0},
  y: {grid: true, label: "Editors"},
  marks: [
    Plot.areaY(tierAgg.filter(d => tierOrder.includes(d.tier)), {
      x: "period", y: "editors", fill: "tier",
      order: tierOrder,
    }),
    Plot.tip(tierAgg.filter(d => tierOrder.includes(d.tier)), Plot.pointerX({x: "period", y: "editors", fill: "tier",
      title: d => `${d.period}\n${d.tier}: ${fmtNum(d.editors)} editors\n${fmtNum(d.edits)} edits · ${fmtBytes(d.net_bytes)} net`
    })),
  ]
}), tierAgg, "community_composition")
```

<details class="methodology"><summary>How is this calculated?</summary>

`Tier(editor) = bucket(edit_count): 1–4 | 5–24 | 25–99 | 100+`

Each period, every editor matching the selected user types is placed into one bucket based on their edit count *in that period*: 1-4 (casual), 5-24 (regular), 25-99 (active), 100+ (power). The stacked area shows the workforce snapshot — not a progression funnel. An editor may appear in different tiers across periods.

</details>
</div>

<div class="chart-section">

## 2. Acquisition Funnel

<div class="note">

**SaaS equivalent: [Conversion funnel](https://en.wikipedia.org/wiki/Purchase_funnel).** For each cohort year of new registered editors, what fraction *ever* reached 5+, 25+, or 100+ cumulative edits? Unlike the composition chart above, this tracks individual editors across their full lifetime. Declining conversion rates signal that newer cohorts are less likely to become committed contributors — recent cohorts also have less time, so expect a natural drop-off for the last few years.

</div>

```js
startLoading()
const funnelData = useDefaults
  ? defaults.funnel
  : Array.from(await (await getDb()).sql`SELECT * FROM funnel WHERE wiki = ${wiki}`)
  .map(d => ({
    ...d,
    pct_5: d.cohort_size > 0 ? d.reached_5 / d.cohort_size : 0,
    pct_25: d.cohort_size > 0 ? d.reached_25 / d.cohort_size : 0,
    pct_100: d.cohort_size > 0 ? d.reached_100 / d.cohort_size : 0,
  }))
doneLoading()

const funnelLong = funnelData.flatMap(d => [
  {cohort_year: d.cohort_year, milestone: "5+ edits", pct: d.pct_5, editors: d.reached_5, cohort_size: d.cohort_size},
  {cohort_year: d.cohort_year, milestone: "25+ edits", pct: d.pct_25, editors: d.reached_25, cohort_size: d.cohort_size},
  {cohort_year: d.cohort_year, milestone: "100+ edits", pct: d.pct_100, editors: d.reached_100, cohort_size: d.cohort_size},
])
```

```js
withExport(Plot.plot({
  width,
  height: 400,
  color: {legend: true, domain: ["5+ edits", "25+ edits", "100+ edits"], range: ["#6baed6", "#2171b5", "#08306b"]},
  x: {type: "point", label: "Cohort year", tickRotate: -45},
  y: {grid: true, label: "Conversion rate", percent: true},
  marks: [
    Plot.lineY(funnelLong, {x: "cohort_year", y: "pct", stroke: "milestone", strokeWidth: 1.5}),
    Plot.dot(funnelLong, {x: "cohort_year", y: "pct", fill: "milestone", r: 2.5}),
    Plot.tip(funnelLong, Plot.pointerX({x: "cohort_year", y: "pct", stroke: "milestone",
      title: d => `${d.cohort_year} cohort (n=${fmtNum(d.cohort_size)})\n${d.milestone}: ${fmtNum(d.editors)} (${(d.pct * 100).toFixed(1)}%)`
    })),
  ]
}), funnelLong, "acquisition_funnel")
```

<details class="methodology"><summary>How is this calculated?</summary>

`Conversion(milestone) = Editors ever reaching milestone / Cohort Size × 100%`

Each registered editor is assigned to a cohort based on their first-ever edit year. Their total cumulative edits across all time are counted, and we check whether they ever crossed 5, 25, or 100 edits. The conversion rate is the fraction of the cohort that reached each milestone. Recent cohorts (last 2-3 years) naturally show lower rates because editors haven't had enough time to accumulate edits. Note: this metric always uses registered editors regardless of the user type filter.

</details>
</div>

<div class="chart-section">

## 3. Lifetime Contribution Value (LTV)

<div class="note">

**SaaS equivalent: [Lifetime Value](https://en.wikipedia.org/wiki/Customer_lifetime_value).** Instead of revenue, LTV is the estimated total net bytes a typical editor from each cohort contributes over their entire tenure, accounting for attrition. Older cohorts have higher LTV because survivors compound their contributions year over year.

</div>

```js
withExport(Plot.plot({
  width,
  height: 400,
  x: {type: "band", label: "Cohort year", tickRotate: -45},
  y: {grid: true, label: "Estimated LTV (bytes)"},
  color: {scheme: "blues"},
  marks: [
    Plot.barY(ltvByCohort, {x: "cohort", y: "ltv_bytes", fill: "ltv_bytes", tip: true,
      title: d => `Cohort: ${d.cohort}\nInitial editors: ${fmtNum(d.initial_editors)}\nEstimated LTV: ${fmtBytes(d.ltv_bytes)}`
    }),
    Plot.ruleY([0]),
  ]
}), ltvByCohort, "ltv")
```

<details class="methodology"><summary>How is this calculated?</summary>

`LTV(cohort) = Σ over years (Survival Rate × Avg Net Bytes per Editor)`

For each cohort year, LTV = sum over all years of (cohort survival rate x average net bytes per editor in that year). Average bytes per editor is computed from article namespace (ns 0) data for the selected user types. Cohorts with fewer than 10 initial editors are excluded.

</details>
</div>

<div class="chart-section">

## 4. Edit Survival Rate (Longevity Index)

<div class="note">

**SaaS equivalent: Product quality / [NPS](https://en.wikipedia.org/wiki/Net_promoter_score).** Instead of measuring how long an edit "lives" before being overwritten, we track the fraction of edits that survive (are not reverted). High survival means consensus-driven, high-quality writing. A decline signals rising edit wars or declining content standards.

</div>

```js
const survTick = Math.max(1, Math.floor(survivalByPeriod.length / 20))
```

```js
withExport(Plot.plot({
  width,
  height: 360,
  x: {type: "band", tickRotate: -45, tickFilter: (d, i) => i % survTick === 0},
  y: {grid: true, label: "Edit survival rate", domain: [0.5, 1], percent: true},
  marks: [
    Plot.areaY(survivalByPeriod, {x: "period", y: "survival_rate", fill: "steelblue", fillOpacity: 0.15, y1: 0.5}),
    Plot.lineY(survivalByPeriod, {x: "period", y: "survival_rate", stroke: "steelblue", strokeWidth: 1.5}),
    Plot.ruleY([0.9], {stroke: "seagreen", strokeDasharray: "4", strokeOpacity: 0.6}),
    Plot.tip(survivalByPeriod, Plot.pointerX({x: "period", y: "survival_rate",
      title: d => `${d.period}\nSurvival: ${(d.survival_rate * 100).toFixed(1)}%\nEdits: ${fmtNum(d.total_edits)}\nReverted: ${fmtNum(d.reverted_edits)}`
    })),
  ]
}), survivalByPeriod, "edit_survival")
```

<details class="methodology"><summary>How is this calculated?</summary>

`Edit Survival = (Total Edits − Reverted Edits) / Total Edits × 100%`

Edit survival rate = (total edits - reverted edits) / total edits. The dashed green line at 90% marks a healthy threshold. Respects selected user types and namespaces.

</details>
</div>

<div class="chart-section">

## 5. Net Knowledge Retention (NRR Proxy)

<div class="note">

**SaaS equivalent: [Net Revenue Retention](https://en.wikipedia.org/wiki/Customer_retention).** Are existing power editors producing *more* over time? This shows the output (net bytes) by activity tier. If the "100+ edits" tier's output grows even as casual editor output shrinks, the platform has strong net retention — its most engaged users are getting more productive.

</div>

```js
withExport(Plot.plot({
  width,
  height: 400,
  color: {legend: true, domain: tierOrder, range: ["#b8d4e3", "#6baed6", "#2171b5", "#08306b"]},
  x: {type: "band", tickRotate: -45, tickFilter: (d, i) => i % funnelTick === 0},
  y: {grid: true, label: "Net bytes"},
  marks: [
    Plot.lineY(tierAgg.filter(d => tierOrder.includes(d.tier)), {
      x: "period", y: "net_bytes", stroke: "tier", strokeWidth: 1.5,
    }),
    Plot.tip(tierAgg.filter(d => tierOrder.includes(d.tier)), Plot.pointerX({x: "period", y: "net_bytes", stroke: "tier",
      title: d => `${d.period}\n${d.tier}\nNet: ${fmtBytes(d.net_bytes)}\nEditors: ${fmtNum(d.editors)}`
    })),
    Plot.ruleY([0]),
  ]
}), tierAgg, "net_knowledge_retention")
```

<details class="methodology"><summary>How is this calculated?</summary>

`Net Bytes(tier) = Σ byte_diff WHERE activity_tier = tier`

Net bytes produced by each activity tier per period. Positive growth in the "100+ edits" tier over time indicates strong net retention. Respects selected user types.

</details>
</div>

<div class="chart-section">

## 6. Controversial Equilibrium Score

<div class="note">

**A metric that tracks the ratio of talk page edits to content page reverts.** Healthy community governance is marked by more discussion and fewer reverts. A high talk-to-revert ratio signals that editors resolve disputes through consensus; a low ratio signals a "toxic" environment where [edit wars](https://en.wikipedia.org/wiki/Edit_warring) replace deliberation.

</div>

```js
const eqTick = Math.max(1, Math.floor(eqByPeriod.length / 20))
```

```js
withExport(Plot.plot({
  width,
  height: 360,
  x: {type: "band", tickRotate: -45, tickFilter: (d, i) => i % eqTick === 0},
  y: {grid: true, label: "Talk edits / Content reverts"},
  marks: [
    Plot.areaY(eqByPeriod, {x: "period", y: "ratio", fill: "var(--wk-teal)", fillOpacity: 0.15}),
    Plot.lineY(eqByPeriod, {x: "period", y: "ratio", stroke: "var(--wk-teal)", strokeWidth: 1.5}),
    Plot.ruleY([1], {stroke: "grey", strokeDasharray: "4"}),
    Plot.tip(eqByPeriod, Plot.pointerX({x: "period", y: "ratio",
      title: d => `${d.period}\nTalk edits: ${fmtNum(d.talk_edits)}\nContent reverts: ${fmtNum(d.content_reverts)}\nRatio: ${d.ratio.toFixed(2)} ${d.ratio > 1 ? "(consensus-driven)" : "(revert-heavy)"}`
    })),
  ]
}), eqByPeriod, "equilibrium")
```

<details class="methodology"><summary>How is this calculated?</summary>

`Equilibrium = Talk Page Edits / Content Page Reverts`

Talk page edits (namespaces 1, 3, 5, 7, etc.) divided by content page reverts (namespaces 0, 2, 4, etc.). A ratio above 1 (dashed line) means more words are spent discussing than reverting. This metric always uses all namespaces regardless of the namespace filter, but respects the user type filter.

</details>
</div>

<div class="chart-section">

## 7. Productivity by Activity Tier

<div class="note">

**Bytes per editor** broken down by activity tier reveals whether power editors are becoming more or less efficient. If the "100+ edits" tier produces declining bytes per editor, it may signal burnout, increasingly administrative work, or a shift from content creation to maintenance.

</div>

```js
const bytesPerEditorByTier = tierAgg
  .filter(d => tierOrder.includes(d.tier) && d.editors > 0)
  .map(d => ({...d, bytes_per_editor: d.net_bytes / d.editors}))
```

```js
withExport(Plot.plot({
  width,
  height: 400,
  color: {legend: true, domain: tierOrder, range: ["#b8d4e3", "#6baed6", "#2171b5", "#08306b"]},
  x: {type: "band", tickRotate: -45, tickFilter: (d, i) => i % funnelTick === 0},
  y: {grid: true, label: "Net bytes per editor"},
  marks: [
    Plot.lineY(bytesPerEditorByTier, {x: "period", y: "bytes_per_editor", stroke: "tier", strokeWidth: 1.5}),
    Plot.tip(bytesPerEditorByTier, Plot.pointerX({x: "period", y: "bytes_per_editor", stroke: "tier",
      title: d => `${d.period}\n${d.tier}: ${fmtBytes(d.bytes_per_editor)}/editor\n${fmtNum(d.editors)} editors`
    })),
    Plot.ruleY([0]),
  ]
}), bytesPerEditorByTier, "productivity_by_tier")
```

<details class="methodology"><summary>How is this calculated?</summary>

`Bytes per Editor = Net Bytes / Unique Editors` (per tier, per period)

Net bytes divided by unique editors within each activity tier per period. Respects selected user types.

</details>
</div>
