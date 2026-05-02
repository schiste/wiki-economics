---
title: Patrol
---

# Patrol

<div class="page-intro">

**[Patrolling](https://en.wikipedia.org/wiki/Wikipedia:Patrolled_edit)** is Wikipedia's quality-control mechanism: experienced editors review new edits and pages to catch vandalism, errors, and policy violations. This page tracks patrol volume, reviewer concentration, response times, and coverage signals — an informative, still-evolving view of the community's immune system.

</div>

<div class="note">

Patrol metrics on this page are currently **informative and under active work**. Historical patrol logging, autopatrol-right inference, and revision matching are not fully consistent across periods and Wikipedias yet, so treat the charts as directional rather than definitive.

</div>

```js
import {queryGrouped, fmtNum, createFilterBar, isDefaultView, parseDefaultsMeta, startLoading, doneLoading} from "./components/filters.js"
import {withExport, pageExportBar} from "./components/exports.js"

const defaults = await FileAttachment("data/defaults_patrol.json").json()
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
const useDefaults = isDefaultView(filters, defaults)
startLoading()
let data
if (useDefaults) {
  data = defaults.patrol
} else {
  const {DuckDBClient: DDB} = await import("observablehq:stdlib/duckdb")
  const db = await DDB.of({
    patrol: FileAttachment("data/patrol.parquet"),
  })
  data = await queryGrouped(db, "patrol", {
    sumCols: ["total_patrols", "unique_patrollers", "patrol_new_pages", "patrol_diffs",
              "patrolled_revisions", "autopatrolled_revisions", "total_revisions", "min_patrollers_50pct"],
    avgCols: ["median_latency_hours", "p90_latency_hours", "patrol_coverage_pct", "adjusted_coverage_pct", "top1_pct"],
    wiki, userTypes, namespaces, startPeriod, endPeriod, granularity
  })
}
doneLoading()
```

```js
const totalPatrols = data.reduce((s, d) => s + d.total_patrols, 0)
const maxPatrollers = Math.max(...data.map(d => d.unique_patrollers))
const latestWithLatency = data.filter(d => d.median_latency_hours != null)
const latestLatency = latestWithLatency.length > 0 ? latestWithLatency[latestWithLatency.length - 1].median_latency_hours : null
const latestCoverage = data.length > 0 ? data[data.length - 1].patrol_coverage_pct : 0
const latestAdjusted = data.length > 0 ? data[data.length - 1].adjusted_coverage_pct : 0
const periodRange = data.length > 0 ? `${data[0].period} — ${data[data.length - 1].period}` : "—"
```

```js
pageExportBar([{name: "patrol", data: data}])
```

<div class="kpi-row">
  <div class="kpi-card">
    <div class="kpi-value">${fmtNum(totalPatrols)}</div>
    <div class="kpi-label">Total Patrols</div>
    <div class="kpi-sub">${periodRange}</div>
  </div>
  <div class="kpi-card">
    <div class="kpi-value">${fmtNum(maxPatrollers)}</div>
    <div class="kpi-label">Peak Monthly Patrollers</div>
    <div class="kpi-sub">${periodRange}</div>
  </div>
  <div class="kpi-card">
    <div class="kpi-value">${latestLatency != null ? latestLatency.toFixed(1) + "h" : "—"}</div>
    <div class="kpi-label">Median Patrol Latency</div>
    <div class="kpi-sub">latest period</div>
  </div>
  <div class="kpi-card">
    <div class="kpi-value">${latestAdjusted != null ? latestAdjusted.toFixed(1) + "%" : "—"}</div>
    <div class="kpi-label">Adjusted Coverage Estimate</div>
    <div class="kpi-sub">${latestCoverage != null ? latestCoverage.toFixed(1) + "% manual" : "latest period"}</div>
  </div>
</div>

<div class="chart-section">

## Monthly Patrol Volume

<div class="note">

Number of edits reviewed (patrolled) each month. Declining patrol volume against stable edit counts signals a growing review backlog.

</div>

```js
const tick = Math.max(1, Math.floor(data.length / 20))
```

```js
withExport(Plot.plot({
  width,
  height: 320,
  x: {type: "band", tickRotate: -45, tickFilter: (d, i) => i % tick === 0},
  y: {grid: true, label: "Patrol events"},
  marks: [
    Plot.barY(data, {x: "period", y: "total_patrols", fill: "steelblue"}),
    Plot.tip(data, Plot.pointerX({x: "period", y: "total_patrols", title: d => `${d.period}\nPatrols: ${fmtNum(d.total_patrols)}\nPatrollers: ${fmtNum(d.unique_patrollers)}`})),
    Plot.ruleY([0])
  ]
}), data, "patrol_volume")
```

<details class="methodology">
<summary>How is this calculated?</summary>

Each bar is the count of `log_type=patrol` events from the MediaWiki logging dump for that period. This includes manual patrols only — autopatrol logging was disabled by Wikimedia in 2018.

</details>
</div>

<div class="chart-section">

## Patrol Coverage

<div class="note">

**Patrol coverage** is the percentage of revisions that appear to have been reviewed. The solid line shows manual patrols only. The dashed line shows an **adjusted coverage estimate** — including edits by users with the autopatrol right, whose edits are automatically marked as patrolled but not logged.

</div>

```js
withExport(Plot.plot({
  width,
  height: 320,
  x: {type: "band", tickRotate: -45, tickFilter: (d, i) => i % tick === 0},
  y: {grid: true, label: "% of revisions patrolled", domain: [0, 100]},
  marks: [
    Plot.areaY(data, {x: "period", y: "adjusted_coverage_pct", fill: "seagreen", fillOpacity: 0.08}),
    Plot.lineY(data, {x: "period", y: "adjusted_coverage_pct", stroke: "seagreen", strokeWidth: 1, strokeDasharray: "4"}),
    Plot.areaY(data, {x: "period", y: "patrol_coverage_pct", fill: "seagreen", fillOpacity: 0.15}),
    Plot.lineY(data, {x: "period", y: "patrol_coverage_pct", stroke: "seagreen", strokeWidth: 1.5}),
    Plot.tip(data, Plot.pointerX({x: "period", y: "adjusted_coverage_pct", title: d => `${d.period}\nManual: ${d.patrol_coverage_pct?.toFixed(1)}%\nAdjusted: ${d.adjusted_coverage_pct?.toFixed(1)}%\nPatrolled: ${fmtNum(d.patrolled_revisions)}\nAutopatrolled: ${fmtNum(d.autopatrolled_revisions)}\nTotal: ${fmtNum(d.total_revisions)}`})),
    Plot.ruleY([0])
  ]
}), data, "patrol_coverage")
```

<details class="methodology">
<summary>How is this calculated?</summary>

`Manual Coverage = patrolled revisions / total revisions × 100%`
`Adjusted Coverage Estimate = (patrolled + autopatrolled) / total revisions × 100%`

A revision is "patrolled" if its `revision_id` appears as `current_revision_id` in the patrol log. "Autopatrolled" revisions are estimated by identifying edits made by users who held an autopatrol-granting group at the time of the edit, based on the user rights change log and current site configuration. That estimate is useful, but still being refined because the underlying logging and rights history are not perfectly consistent across periods.

</details>
</div>

<div class="chart-section">

## Patrol Latency

<div class="note">

**Patrol latency** measures how quickly edits get reviewed — the time between when an edit is made and when a patroller marks it. The dashed line shows the P90 (worst-case for 90% of edits).

</div>

```js
const latData = data.filter(d => d.median_latency_hours != null)
const latTick = Math.max(1, Math.floor(latData.length / 20))
```

```js
withExport(Plot.plot({
  width,
  height: 320,
  x: {type: "band", tickRotate: -45, tickFilter: (d, i) => i % latTick === 0},
  y: {grid: true, label: "Hours to patrol"},
  marks: [
    Plot.areaY(latData, {x: "period", y: "p90_latency_hours", fill: "tomato", fillOpacity: 0.1}),
    Plot.lineY(latData, {x: "period", y: "p90_latency_hours", stroke: "tomato", strokeWidth: 1, strokeDasharray: "4"}),
    Plot.lineY(latData, {x: "period", y: "median_latency_hours", stroke: "steelblue", strokeWidth: 2}),
    Plot.tip(latData, Plot.pointerX({x: "period", y: "median_latency_hours", title: d => `${d.period}\nMedian: ${d.median_latency_hours?.toFixed(1)}h\nP90: ${d.p90_latency_hours?.toFixed(1)}h`})),
    Plot.ruleY([0])
  ]
}), latData, "patrol_latency")
```

<details class="methodology">
<summary>How is this calculated?</summary>

`Latency = patrol_timestamp − revision_timestamp`

For each patrol event, the corresponding revision's creation time is looked up and the difference computed. Median and 90th percentile are computed per period. Only events with a matched revision and latency under 1 year are included.

</details>
</div>

<div class="chart-section">

## Patroller Fragility

<div class="note">

How many patrollers would need to leave before 50% of patrol work goes undone? Lower = more fragile. This measures concentration of the patrol workforce.

</div>

```js
const fragData = data.map(d => ({...d, fragility_pct: d.unique_patrollers > 0 ? d.min_patrollers_50pct / d.unique_patrollers * 100 : 0}))
```

```js
withExport(Plot.plot({
  width,
  height: 320,
  x: {type: "band", tickRotate: -45, tickFilter: (d, i) => i % tick === 0},
  y: {grid: true, label: "% of patrollers needed for 50% of work"},
  marks: [
    Plot.areaY(fragData, {x: "period", y: "fragility_pct", fill: "var(--theme-foreground-focus)", fillOpacity: 0.12}),
    Plot.lineY(fragData, {x: "period", y: "fragility_pct", stroke: "var(--theme-foreground-focus)", strokeWidth: 1.5}),
    Plot.tip(fragData, Plot.pointerX({x: "period", y: "fragility_pct", title: d => `${d.period}\nFragility: ${d.fragility_pct.toFixed(1)}%\n(${fmtNum(d.min_patrollers_50pct)} of ${fmtNum(d.unique_patrollers)} patrollers)`})),
    Plot.ruleY([0])
  ]
}), fragData, "patroller_fragility")
```

<details class="methodology">
<summary>How is this calculated?</summary>

`Fragility = min k where top-k patrollers' patrols ≥ 50% × Total Patrols`
`Fragility Ratio = Fragility / Unique Patrollers × 100%`

Patrollers are ranked by patrol count descending. The fragility index is the minimum number whose cumulative patrols reach 50% of the total.

</details>
</div>

<div class="chart-section">

## New Pages vs. Diff Patrols

<div class="note">

Patrol work breaks down into two types: reviewing **new pages** (is this page legitimate?) and reviewing **diffs** to existing pages (is this edit constructive?).

</div>

```js
const stackData = data.flatMap(d => [
  {period: d.period, type: "New pages", value: d.patrol_new_pages},
  {period: d.period, type: "Diffs", value: d.patrol_diffs}
])
```

```js
withExport(Plot.plot({
  width,
  height: 320,
  x: {type: "band", tickRotate: -45, tickFilter: (d, i) => i % (tick * 2) === 0},
  y: {grid: true, label: "Patrol events"},
  color: {domain: ["New pages", "Diffs"], range: ["#1565c0", "#ff8f00"]},
  marks: [
    Plot.barY(stackData, Plot.stackY({x: "period", y: "value", fill: "type"})),
    Plot.tip(data, Plot.pointerX({x: "period", y: "total_patrols", title: d => `${d.period}\nNew pages: ${fmtNum(d.patrol_new_pages)} (${d.total_patrols > 0 ? (d.patrol_new_pages / d.total_patrols * 100).toFixed(0) : 0}%)\nDiffs: ${fmtNum(d.patrol_diffs)}`})),
    Plot.ruleY([0])
  ]
}), stackData, "patrol_types")
```

</div>
