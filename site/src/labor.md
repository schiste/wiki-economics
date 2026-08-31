---
title: Community
---

# Community

<div class="page-intro">

Wikipedia's **[labor market](https://en.wikipedia.org/wiki/Labour_economics)** is the pool of editors who contribute their time and expertise.
Like a real economy, the wiki has hiring (new arrivals), attrition (departures), and a workforce
whose size and composition shift over time. This page tracks active editors, their user types,
[churn](https://en.wikipedia.org/wiki/Churn_rate) dynamics, and long-term [cohort](https://en.wikipedia.org/wiki/Cohort_analysis) retention: the vital signs of the community's labor supply.

</div>

```js
import {makeRowsLoader, makeJsonLoader, toPeriod, fmtNum, createFilterBar, isDefaultView, parseDefaultsMeta, startLoading, doneLoading, aggregateChurn, aggregateCohorts, wikiMatches} from "./components/filters.js"
import {withExport, pageExportBar} from "./components/exports.js"
import {identityTransition} from "./components/editor-identities.js"

const meta = await FileAttachment("data/meta_labor.json").json()
const {wikis, nsByWiki, rangeByWiki, defaultWiki, maxMonth} = parseDefaultsMeta(meta)
const loadDefaults = makeJsonLoader(FileAttachment("data/defaults_labor.json"))
const identityTransitions = await FileAttachment("data/editor_identity_transitions.json").json()
```

```js
const filters = view(createFilterBar({wikis, nsByWiki, rangeByWiki, defaultWiki, maxMonth}))
```

```js
const {wiki, userTypes, granularity, startPeriod, endPeriod, namespaces} = filters
```

```js
const loadLaborRows = makeRowsLoader({
  labor: "labor_monthly",
  tiers: "gdp_activity_tiers",
  cohorts: "labor_cohorts",
  churn: "labor_churn",
})

const useDefaults = isDefaultView(filters, meta)
const startP = toPeriod(startPeriod, granularity)
const endP = toPeriod(endPeriod, granularity)

startLoading(useDefaults)
let workforce, churnData
try {
if (useDefaults) {
  const defaults = await loadDefaults()
  workforce = defaults.workforce
  churnData = defaults.churn
} else {
  const {tiers: tierRaw, churn: churnRaw} = await loadLaborRows(wiki, {startPeriod, endPeriod})
  workforce = d3.rollups(
    tierRaw.filter(d => wikiMatches(d, wiki) && userTypes.includes(d.user_type)
      && d.period_type === granularity && d.period >= startP && d.period <= endP),
    rows => ({
      unique_editors: d3.sum(rows, d => d.editors),
      total_edits: d3.sum(rows, d => d.total_edits),
    }),
    d => d.period,
  ).map(([period, values]) => ({period, ...values})).sort((a, b) => d3.ascending(a.period, b.period))
  churnData = aggregateChurn(churnRaw.filter(d => wikiMatches(d, wiki) && d.period_type === granularity && d.period >= startP && d.period <= endP))
}
} finally {
  doneLoading()
}
const tickStep = Math.max(1, Math.floor(workforce.length / 20))
const editorIdentityTransition = identityTransition(identityTransitions, wiki, granularity)
```

```js
const latestWf = workforce.length > 0 ? workforce[workforce.length - 1] : null
const latestChurn = churnData.length > 0 ? churnData[churnData.length - 1] : null
```

```js
pageExportBar([
  {name: "workforce", data: workforce},
  {name: "churn", data: churnData},
])
```

<div class="kpi-row">
  <div class="kpi-card">
    <div class="kpi-value">${fmtNum(latestWf?.unique_editors)}</div>
    <div class="kpi-label">Active Editors</div>
    <div class="kpi-sub">${latestWf ? latestWf.period : "—"}</div>
  </div>
  <div class="kpi-card">
    <div class="kpi-value">${fmtNum(latestWf?.total_edits)}</div>
    <div class="kpi-label">Total Edits</div>
    <div class="kpi-sub">${latestWf ? latestWf.period : "—"}</div>
  </div>
  <div class="kpi-card">
    <div class="kpi-value">${latestChurn ? (latestChurn.arrival_rate * 100).toFixed(1) + "%" : "—"}</div>
    <div class="kpi-label">Arrival Rate</div>
    <div class="kpi-sub">${latestChurn ? latestChurn.period : "—"}</div>
  </div>
  <div class="kpi-card">
    <div class="kpi-value">${latestChurn ? (latestChurn.departure_rate * 100).toFixed(1) + "%" : "—"}</div>
    <div class="kpi-label">Departure Rate</div>
    <div class="kpi-sub">${latestChurn ? latestChurn.period : "—"}</div>
  </div>
</div>

<div class="chart-section">

## Workforce Over Time

<div class="note">

**Active editor identities** per period. This is an exact wiki-wide distinct count for the selected month, quarter, or year; it is not a sum of namespace or monthly counts. The namespace filter therefore does not apply to this chart.

${editorIdentityTransition ? html`<strong>Measurement boundary:</strong> temporary accounts were enabled on ${editorIdentityTransition.effective_date}. The dashed line marks the first full affected period; compare identity classes across it with care.` : ""}

</div>

```js
withExport(Plot.plot({
  width,
  height: 400,
  x: {type: "band", tickRotate: -45, tickFilter: (d, i) => i % tickStep === 0},
  y: {grid: true, label: "Unique editors"},
  marks: [
    Plot.areaY(workforce, {x: "period", y: "unique_editors", fill: "steelblue", fillOpacity: 0.2}),
    Plot.lineY(workforce, {x: "period", y: "unique_editors", stroke: "steelblue", strokeWidth: 1.5}),
    ...(editorIdentityTransition ? [Plot.ruleX([editorIdentityTransition.period], {stroke: "#b45309", strokeWidth: 2, strokeDasharray: "5,4"})] : []),
    Plot.tip(workforce, Plot.pointerX({x: "period", y: "unique_editors",
      title: d => `${d.period}\nEditors: ${fmtNum(d.unique_editors)}\nEdits: ${fmtNum(d.total_edits)}`
    })),
  ]
}), workforce, "workforce")
```

<details class="methodology">
<summary>How is this calculated?</summary>

`Active Editor Identities = COUNT(DISTINCT canonical_editor_identity) per period`

An identity is counted once per wiki and selected period even if it edits in multiple namespaces or months. Permanent and temporary accounts use their user ID; historical IP actors use their actor text internally. The grouping identity is never published. An account or IP actor is not necessarily one person, and cross-wiki totals remain editor–wiki observations because local actor IDs are not a safe global identity.

</details>
</div>

<div class="chart-section">

## Workforce by User Type

<div class="note">

Same metric broken down by editor classification. The **temporary accounts** category reflects Wikimedia's migration of logged-out editors from public IP actors to temporary accounts, not by itself a real change in editing behavior.

</div>

```js
startLoading(useDefaults)
let typeAgg
try {
if (useDefaults) {
  const defaults = await loadDefaults()
  typeAgg = defaults.byType
} else {
  const {tiers: tierRaw} = await loadLaborRows(wiki, {startPeriod, endPeriod})
  const byType = tierRaw.filter(d => wikiMatches(d, wiki) && d.period_type === granularity
    && d.period >= startP && d.period <= endP)
  typeAgg = d3.rollups(byType, v => d3.sum(v, d => d.editors), d => d.period, d => d.user_type)
    .flatMap(([period, types]) => types.map(([user_type, editors]) => ({period, user_type, editors})))
    .sort((a, b) => d3.ascending(a.period, b.period))
}
} finally {
  doneLoading()
}
```

```js
withExport(Plot.plot({
  width,
  height: 400,
  color: {legend: true, domain: ["registered", "temporary", "anonymous", "bot"], range: ["steelblue", "orange", "gold", "tomato"]},
  x: {type: "band", tickRotate: -45, tickFilter: (d, i) => i % tickStep === 0},
  y: {grid: true, label: "Editors"},
  marks: [
    Plot.lineY(typeAgg, {x: "period", y: "editors", stroke: "user_type", strokeWidth: 1.5}),
    ...(editorIdentityTransition ? [Plot.ruleX([editorIdentityTransition.period], {stroke: "#b45309", strokeWidth: 2, strokeDasharray: "5,4"})] : []),
    Plot.tip(typeAgg, Plot.pointerX({x: "period", y: "editors", stroke: "user_type",
      title: d => `${d.period}\n${d.user_type}: ${fmtNum(d.editors)} editors`
    })),
  ]
}), typeAgg, "workforce_by_type")
```

<details class="methodology">
<summary>How is this calculated?</summary>

`Editors(type) = COUNT(DISTINCT editor_id) WHERE user_type = type`

Same exact wiki-wide identity count broken down by classification: **registered** (permanent account), **temporary** (browser-bound temporary account), **anonymous** (historical IP actor), and **bot** (flagged bot account). All types are shown regardless of the user-type filter. These identity classes are not interchangeable estimates of people.

</details>
</div>

<div class="chart-section">

## Churn Rate

<div class="note">

Churn measures the **flow** of editors in and out of the community. A healthy wiki maintains a balance between arrivals and departures. When departures consistently exceed arrivals, the workforce is shrinking.

</div>

```js
const churnTick = Math.max(1, Math.floor(churnData.length / 20))

const churnLong = churnData.flatMap(d => [
  {period: d.period, metric: "Arrival rate", rate: d.arrival_rate, active_editors: d.active_editors, arrivals: d.arrivals, departures: d.departures, arrival_rate: d.arrival_rate, departure_rate: d.departure_rate},
  {period: d.period, metric: "Departure rate", rate: d.departure_rate, active_editors: d.active_editors, arrivals: d.arrivals, departures: d.departures, arrival_rate: d.arrival_rate, departure_rate: d.departure_rate},
])
```

```js
withExport(Plot.plot({
  width,
  height: 400,
  color: {legend: true, domain: ["Arrival rate", "Departure rate"], range: ["seagreen", "tomato"]},
  x: {type: "band", tickRotate: -45, tickFilter: (d, i) => i % churnTick === 0},
  y: {grid: true, label: "Rate", percent: true},
  marks: [
    Plot.lineY(churnLong, {x: "period", y: "rate", stroke: "metric", strokeWidth: 1.5}),
    Plot.ruleY([0]),
    Plot.tip(churnLong, Plot.pointerX({x: "period", y: "rate", stroke: "metric",
      title: d => `${d.period}\nActive: ${fmtNum(d.active_editors)}\nArrivals: ${fmtNum(d.arrivals)} (${(d.arrival_rate * 100).toFixed(1)}%)\nDepartures: ${fmtNum(d.departures)} (${(d.departure_rate * 100).toFixed(1)}%)`
    })),
  ]
}), churnData, "churn")
```

<div class="note"><strong>Caveat:</strong> The last period's departure rate is artificially high, since editors whose last edit is recent aren't necessarily gone. Registered editors only.</div>

<details class="methodology">
<summary>How is this calculated?</summary>

`Arrival Rate = New Editors (first edit in period) / Active Editors × 100%`
`Departure Rate = Departing Editors (last edit in period) / Active Editors × 100%`

**Arrival rate** = editors whose first-ever edit falls in this period / total active editors. **Departure rate** = editors whose last-ever edit falls in this period / total active editors. Registered editors only. Computed separately per granularity in Rust to avoid invalid cross-granularity aggregation.

</details>
</div>

<div class="chart-section">

## Cohort Retention

<div class="note"><a href="https://en.wikipedia.org/wiki/Survival_analysis">Survival</a> heatmap for registered editors. Each row is a cohort (year of first edit). Colors show what fraction of that cohort is still around (last edit in that year or later). The last column is partial data. The "n=" column shows initial cohort size; small cohorts can produce misleading stability.</div>

```js
startLoading(useDefaults)
let cohortData
try {
if (useDefaults) {
  const defaults = await loadDefaults()
  cohortData = defaults.cohorts
} else {
  const {cohorts} = await loadLaborRows(wiki, {startPeriod, endPeriod})
  cohortData = aggregateCohorts(cohorts.filter(d => wikiMatches(d, wiki)))
}
} finally {
  doneLoading()
}
const latestYear = d3.max(cohortData, d => d.year)

const heatmap = cohortData
  .filter(d => d.year >= d.cohort_year && d.initial_editors > 0)
  .map(d => ({
    ...d,
    retention: d.survived_editors / d.initial_editors,
    years_since: parseInt(d.year) - parseInt(d.cohort_year),
    is_partial: d.year === latestYear,
  }))

const cohortSizes = Array.from(d3.rollup(heatmap, v => v[0].initial_editors, d => d.cohort_year))
  .map(([cohort_year, n]) => ({cohort_year, year: "n=", initial_editors: n}))
```

```js
withExport(Plot.plot({
  width: Math.min(width, 900),
  height: Math.max(300, heatmap.filter(d => d.years_since === 0).length * 18 + 60),
  padding: 0,
  marginLeft: 60,
  marginBottom: 40,
  color: {
    scheme: "YlOrRd",
    reverse: true,
    domain: [0, 1],
    label: "Survival rate",
    legend: true
  },
  x: {label: "Activity year", tickRotate: -45},
  y: {label: "Cohort year"},
  marks: [
    Plot.cell(heatmap.filter(d => !d.is_partial), {x: "year", y: "cohort_year", fill: "retention", inset: 0.5}),
    Plot.cell(heatmap.filter(d => d.is_partial), {x: "year", y: "cohort_year", fill: "#ddd", inset: 0.5}),
    Plot.cell(cohortSizes, {x: "year", y: "cohort_year", fill: "#f5f5f5", inset: 0.5}),
    Plot.text(heatmap.filter(d => !d.is_partial), {
      x: "year", y: "cohort_year",
      text: d => d.retention >= 0.01 ? (d.retention * 100).toFixed(0) + "%" : "<1%",
      fontSize: 9,
      fill: d => d.retention > 0.5 ? "black" : "white"
    }),
    Plot.text(heatmap.filter(d => d.is_partial), {
      x: "year", y: "cohort_year",
      text: d => (d.retention * 100).toFixed(0) + "%*",
      fontSize: 9,
      fill: "#999"
    }),
    Plot.text(cohortSizes, {
      x: "year", y: "cohort_year",
      text: d => d.initial_editors.toLocaleString(),
      fontSize: 9,
      fontWeight: "bold",
      fill: "black"
    })
  ]
}), heatmap, "cohort_retention")
```

<details class="methodology">
<summary>How is this calculated?</summary>

`Survival(cohort, year) = Editors with last_edit ≥ year / Initial Cohort Size × 100%`

Survival-based retention. Each editor is assigned to the cohort of their first edit year. "Survived to year Y" means their last edit is in year Y or later. The metric is monotonically decreasing by design: once an editor's last-edit year is passed, they drop out permanently. The "n=" column shows initial cohort size; small cohorts (< 20) can produce misleading retention curves.

</details>
</div>

<div class="chart-section">

### Survival Summary

```js
const summaryYear = Array.from(new Set(cohortData.map(d => d.year))).sort().slice(-2)[0]
const survivalSummary = heatmap
  .filter(d => d.year === summaryYear && d.cohort_year <= summaryYear)
  .sort((a, b) => d3.ascending(a.cohort_year, b.cohort_year))
```

```js
survivalSummary.length > 0 ? Inputs.table(survivalSummary.map(d => ({
  cohort: d.cohort_year,
  initial: d.initial_editors,
  current: d.survived_editors,
  survival: (d.retention * 100).toFixed(1) + "%",
  years: d.years_since,
})), {
  header: {cohort: "Cohort", initial: "Initial size", current: `Survived to ${summaryYear}`, survival: "Survival rate", years: "Years"},
  sort: "cohort"
}) : html`<p>No survival data available.</p>`
```

</div>
