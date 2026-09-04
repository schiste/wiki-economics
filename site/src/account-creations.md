---
title: Account Creation
---

# Account Creation

<div class="page-intro">

How many permanent local accounts are created on Swedish Wikipedia—and how many ever make at least one publicly attributable edit? This staging metric follows each monthly registration cohort through the complete selected history snapshot.

</div>

```js
import * as Inputs from "npm:@observablehq/inputs"
import * as Plot from "npm:@observablehq/plot"
import {fmtNum, toPeriod} from "./components/filters.js"
import {withExport, pageExportBar} from "./components/exports.js"

const report = await FileAttachment("data/_staging/account-creations/svwiki.json").json()
```

```js
const granularity = view(Inputs.select(
  new Map([["Monthly", "month"], ["Quarterly", "quarter"], ["Yearly", "year"]]),
  {label: "Group registrations by", value: "month"}
))
```

```js
const data = d3.rollups(
  report.rows,
  rows => ({
    accounts_created: d3.sum(rows, d => d.accounts_created),
    accounts_with_edits: d3.sum(rows, d => d.accounts_with_edits),
    accounts_without_edits: d3.sum(rows, d => d.accounts_without_edits),
    temporary_accounts_excluded: d3.sum(rows, d => d.temporary_accounts_excluded),
  }),
  d => toPeriod(d.year_month, granularity)
).map(([period, counts]) => ({period, ...counts}))
  .sort((a, b) => d3.ascending(a.period, b.period))

const split = data.flatMap(d => [
  {...d, status: "Made at least one edit", accounts: d.accounts_with_edits},
  {...d, status: "No edits in the snapshot", accounts: d.accounts_without_edits},
])
const totalCreated = d3.sum(data, d => d.accounts_created)
const totalWithEdits = d3.sum(data, d => d.accounts_with_edits)
const conversion = totalCreated > 0 ? totalWithEdits / totalCreated : null
const latest = data.at(-1)
const tickStep = Math.max(1, Math.floor(data.length / 18))
```

```js
pageExportBar([
  {name: "account_creations", data},
  {name: "account_creation_edit_split", data: split},
])
```

<div class="kpi-row">
  <div class="kpi-card">
    <div class="kpi-value">${fmtNum(latest?.accounts_created)}</div>
    <div class="kpi-label">Latest cohort</div>
    <div class="kpi-sub">${latest?.period ?? "—"}</div>
  </div>
  <div class="kpi-card">
    <div class="kpi-value">${conversion == null ? "—" : (conversion * 100).toFixed(1) + "%"}</div>
    <div class="kpi-label">Ever edited</div>
    <div class="kpi-sub">across observed cohorts</div>
  </div>
  <div class="kpi-card">
    <div class="kpi-value">${fmtNum(d3.sum(data, d => d.temporary_accounts_excluded))}</div>
    <div class="kpi-label">Temporary accounts excluded</div>
    <div class="kpi-sub">kept outside permanent cohorts</div>
  </div>
</div>

<div class="chart-section">

## Account creations across time

```js
withExport(Plot.plot({
  width,
  height: 380,
  x: {type: "band", label: "Creation period", tickRotate: -45, tickFilter: (d, i) => i % tickStep === 0},
  y: {grid: true, label: "Permanent local accounts created", zero: true},
  marks: [
    Plot.areaY(data, {x: "period", y: "accounts_created", fill: "#6baed6", fillOpacity: 0.2}),
    Plot.lineY(data, {x: "period", y: "accounts_created", stroke: "#2171b5", strokeWidth: 1.6}),
    Plot.tip(data, Plot.pointerX({x: "period", y: "accounts_created", title: d => `${d.period}\nCreated: ${fmtNum(d.accounts_created)}`})),
    Plot.ruleY([0]),
  ]
}), data, "account_creations")
```

</div>

<div class="chart-section">

## Created accounts with and without edits

```js
withExport(Plot.plot({
  width,
  height: 400,
  color: {legend: true, domain: ["Made at least one edit", "No edits in the snapshot"], range: ["#2171b5", "#cbd5e1"]},
  x: {type: "band", label: "Creation period", tickRotate: -45, tickFilter: (d, i) => i % tickStep === 0},
  y: {grid: true, label: "Permanent local accounts", zero: true},
  marks: [
    Plot.barY(split, {x: "period", y: "accounts", fill: "status", order: ["Made at least one edit", "No edits in the snapshot"]}),
    Plot.tip(split, Plot.pointerX({x: "period", y: "accounts", fill: "status", title: d => `${d.period}\n${d.status}: ${fmtNum(d.accounts)}\nCohort total: ${fmtNum(d.accounts_created)}`})),
    Plot.ruleY([0]),
  ]
}), split, "account_creation_edit_split")
```

<details class="methodology"><summary>Definition and limitations</summary>

`Accounts created = Accounts with edits + Accounts without edits`

Account creation comes from Wikimedia's snapshot-pinned `newusers` logging records. When the dump contains repeated creation records for one stable local user ID, the earliest observed creation month is used once. Permanent local accounts are matched against every publicly attributable revision in the selected MediaWiki History snapshot. “Without edits” therefore means no matching public revision through **${report.snapshot}**, not necessarily no private, suppressed, or future activity. Temporary accounts and later duplicate creation records are excluded from both sides of the permanent-account split. Logging coverage begins when the wiki's public account-creation log becomes available.

Source: svwiki logging dump `${report.logging_dump_date}` · history snapshot `${report.snapshot}` · algorithm `${report.metric_version}` · license `${report.license_spdx}`.

</details>

</div>
