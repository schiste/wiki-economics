---
title: Account Creation
---

# Account Creation

<div class="page-intro">

How many permanent local accounts are created on Swedish Wikipedia, how many ever make a publicly attributable edit, and how many remain indefinitely locally blocked? This staging metric follows each monthly registration cohort through the complete selected history snapshot.

</div>

```js
import * as Inputs from "npm:@observablehq/inputs"
import * as Plot from "npm:@observablehq/plot"
import {fmtNum, toPeriod} from "./components/filters.js"
import {withExport, pageExportBar} from "./components/exports.js"
import {ACCOUNT_SCOPE_ALL, ACCOUNT_SCOPE_EXCLUDE_INDEFINITE, applyAccountScope} from "./components/account-cohorts.js"

const report = await FileAttachment("data/_staging/account-creations/svwiki.json").json()
```

```js
const granularity = view(Inputs.select(
  new Map([["Monthly", "month"], ["Quarterly", "quarter"], ["Yearly", "year"]]),
  {label: "Group registrations by", value: "month"}
))
```

```js
const accountScope = view(Inputs.radio(new Map([
  ["Include indefinitely blocked", ACCOUNT_SCOPE_ALL],
  ["Exclude indefinitely blocked", ACCOUNT_SCOPE_EXCLUDE_INDEFINITE],
]), {label: "Account scope", value: ACCOUNT_SCOPE_ALL}))
```

```js
const scopedRows = report.rows.map(row => applyAccountScope(row, accountScope))

const data = d3.rollups(
  scopedRows,
  rows => ({
    accounts_created: d3.sum(rows, d => d.accounts_created),
    accounts_with_edits: d3.sum(rows, d => d.accounts_with_edits),
    accounts_without_edits: d3.sum(rows, d => d.accounts_without_edits),
    indefinitely_blocked_accounts: d3.sum(rows, d => d.indefinitely_blocked_accounts),
    indefinitely_blocked_with_edits: d3.sum(rows, d => d.indefinitely_blocked_with_edits),
    indefinitely_blocked_without_edits: d3.sum(rows, d => d.indefinitely_blocked_without_edits),
    excluded_indefinitely_blocked_accounts: d3.sum(rows, d => d.excluded_indefinitely_blocked_accounts),
    temporary_accounts_excluded: d3.sum(rows, d => d.temporary_accounts_excluded),
  }),
  d => toPeriod(d.year_month, granularity)
).map(([period, counts]) => ({period, account_scope: accountScope, ...counts}))
  .sort((a, b) => d3.ascending(a.period, b.period))

const split = data.flatMap(d => [
  {...d, status: "Made at least one edit", accounts: d.accounts_with_edits},
  {...d, status: "No edits in the snapshot", accounts: d.accounts_without_edits},
])
const indefinitelyBlockedSplit = data.flatMap(d => [
  {...d, status: "Made at least one edit", accounts: d.indefinitely_blocked_with_edits},
  {...d, status: "No edits in the snapshot", accounts: d.indefinitely_blocked_without_edits},
])
const totalCreated = d3.sum(data, d => d.accounts_created)
const totalWithEdits = d3.sum(data, d => d.accounts_with_edits)
const totalIndefinitelyBlocked = d3.sum(data, d => d.indefinitely_blocked_accounts)
const totalExcludedIndefinitelyBlocked = d3.sum(data, d => d.excluded_indefinitely_blocked_accounts)
const conversion = totalCreated > 0 ? totalWithEdits / totalCreated : null
const indefiniteBlockRate = totalCreated > 0 ? totalIndefinitelyBlocked / totalCreated : null
const latest = data.at(-1)
const tickStep = Math.max(1, Math.floor(data.length / 18))
```

```js
pageExportBar([
  {name: "account_creations", data},
  {name: "account_creation_edit_split", data: split},
  {name: "account_creation_indefinite_blocks", data: indefinitelyBlockedSplit},
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
    <div class="kpi-value">${fmtNum(accountScope === ACCOUNT_SCOPE_EXCLUDE_INDEFINITE ? totalExcludedIndefinitelyBlocked : totalIndefinitelyBlocked)}</div>
    <div class="kpi-label">${accountScope === ACCOUNT_SCOPE_EXCLUDE_INDEFINITE ? "Indefinitely blocked excluded" : "Indefinitely locally blocked"}</div>
    <div class="kpi-sub">${accountScope === ACCOUNT_SCOPE_EXCLUDE_INDEFINITE ? "removed from every account view" : indefiniteBlockRate == null ? "—" : (indefiniteBlockRate * 100).toFixed(1) + "% of permanent cohorts"}</div>
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

## Indefinitely locally blocked accounts

<div class="note">${accountScope === ACCOUNT_SCOPE_EXCLUDE_INDEFINITE ? "The selected account scope excludes this entire cohort, so this graph is intentionally empty." : "Blocked accounts remain split by whether a public edit can be attributed to them."}</div>

```js
withExport(Plot.plot({
  width,
  height: 400,
  color: {legend: true, domain: ["Made at least one edit", "No edits in the snapshot"], range: ["#7f1d1d", "#fca5a5"]},
  x: {type: "band", label: "Creation period", tickRotate: -45, tickFilter: (d, i) => i % tickStep === 0},
  y: {grid: true, label: "Accounts indefinitely locally blocked at cutoff", zero: true},
  marks: [
    Plot.barY(indefinitelyBlockedSplit, {x: "period", y: "accounts", fill: "status", order: ["Made at least one edit", "No edits in the snapshot"]}),
    Plot.tip(indefinitelyBlockedSplit, Plot.pointerX({x: "period", y: "accounts", fill: "status", title: d => `${d.period}\n${d.status}: ${fmtNum(d.accounts)}\nIndefinitely blocked: ${fmtNum(d.indefinitely_blocked_accounts)}`})),
    Plot.ruleY([0]),
  ]
}), indefinitelyBlockedSplit, "account_creation_indefinite_blocks")
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

The **Account scope** filter applies before monthly cohorts are grouped into quarters or years and affects every account KPI, graph, tooltip, and CSV on this page. “Exclude indefinitely blocked” subtracts the exact blocked-with-edits and blocked-without-edits subsets, preserving the displayed cohort identity `Accounts created = Accounts with edits + Accounts without edits`. Temporary accounts remain excluded under either setting.

Account creation comes from Wikimedia's snapshot-pinned `newusers` logging records. When the dump contains repeated creation records for one stable local user ID, the earliest observed creation month is used once. Legacy records without a target ID use the unique creation-log ID and target username, and are matched to revisions by historical or current username. Redacted legacy records without any public target identity are counted as creations but cannot match an edit or block.

“Indefinitely locally blocked” means the latest matching public local `block`, `reblock`, or `unblock` transition at the **${report.snapshot}** cutoff leaves an indefinite block in force. It is not a claim that the account is community-banned or globally locked/blocked, and it can include indefinite partial blocks. Hidden targets and username histories unavailable in public data cannot be matched.

“Without edits” means no matching public revision through **${report.snapshot}**, not necessarily no private, suppressed, renamed, or future activity. Temporary accounts and later duplicate stable-ID records are excluded from every permanent-account cohort. Logging coverage begins when the wiki's public account-creation log becomes available.

Source: svwiki logging dump `${report.logging_dump_date}` · history snapshot `${report.snapshot}` · algorithm `${report.metric_version}` · license `${report.license_spdx}`.

</details>

</div>
