---
title: Portfolio
toc: false
---

```js
import * as Plot from "npm:@observablehq/plot"
import {html} from "npm:htl@1.0.0"
import {fmtBytes, fmtNum} from "./components/filters.js"

const [gdp, labor, inequality, patrol, variation] = await Promise.all([
  FileAttachment("data/defaults_gdp.json").json(),
  FileAttachment("data/defaults_labor.json").json(),
  FileAttachment("data/defaults_inequality.json").json(),
  FileAttachment("data/defaults_patrol.json").json(),
  FileAttachment("data/defaults_edit_variation.json").json(),
])

const wikiNames = {
  afwiki: "Afrikaans", arwiki: "Arabic", arzwiki: "Egyptian Arabic",
  elwiki: "Greek", eswiki: "Spanish", frwiki: "French", hawiki: "Hausa",
  itwiki: "Italian", jawiki: "Japanese", nlwiki: "Dutch", ptwiki: "Portuguese",
  svwiki: "Swedish", swwiki: "Swahili", viwiki: "Vietnamese", yowiki: "Yoruba",
  zhwiki: "Chinese",
}
const wikis = (gdp.wikis ?? []).map(({wiki}) => ({wiki, name: wikiNames[wiki] ?? wiki}))
const latestOutput = gdp.output?.at(-1) ?? {}
const latestWorkforce = labor.workforce?.at(-1) ?? {}
const latestChurn = labor.churn?.at(-1) ?? {}
const latestInequality = inequality.data?.at(-1) ?? {}
const latestPatrol = patrol.patrol?.at(-1) ?? {}
const topVariation = variation.topVariation?.slice(0, 5) ?? []
const productiveShare = latestOutput.total_edits > 0
  ? latestOutput.productive_edits / latestOutput.total_edits * 100
  : null
const netCommunityChange = (latestChurn.arrivals ?? 0) - (latestChurn.departures ?? 0)
const coverageStart = [...(gdp.rangeByWiki ?? [])]
  .filter(({wiki}) => wiki !== "all")
  .map(({mn}) => mn)
  .sort()[0] ?? "—"

function pct(value, digits = 1) {
  return Number.isFinite(value) ? `${value.toFixed(digits)}%` : "—"
}

function articleUrl(wiki, title) {
  const language = wiki.endsWith("wiki") ? wiki.slice(0, -4) : wiki
  return `https://${language}.wikipedia.org/wiki/${encodeURIComponent(title)}`
}

function displayTitle(title) {
  return String(title ?? "").replaceAll("_", " ")
}
```

<header class="portfolio-hero">
  <div class="portfolio-eyebrow">Wikipedia activity portfolio · ${wikis.length} language editions · ${coverageStart}—${gdp.maxMonth ?? "present"}</div>
  <div class="portfolio-hero-grid">
    <div>
      <p class="portfolio-deck">A combined view of production, participation, concentration, and review across every Wikipedia edition currently managed by this project.</p>
      <div class="portfolio-actions">
        <a class="portfolio-action primary" href="#choose-wiki">Choose a Wikipedia</a>
        <a class="portfolio-action" href="#dimensions">Explore a dimension</a>
      </div>
    </div>
    <div class="portfolio-scope" aria-label="Managed Wikipedia editions">
      <span class="portfolio-scope-label">In this release</span>
      <div class="portfolio-code-rail">
        ${wikis.map(({wiki}) => html`<span title=${wikiNames[wiki] ?? wiki}>${wiki.replace(/wiki$/, "")}</span>`)}
      </div>
      <p>This is a portfolio of separate communities. Editor counts are <strong>editor–wiki observations</strong>, not deduplicated people across languages.</p>
    </div>
  </div>
</header>

<section class="portfolio-ledger" aria-labelledby="portfolio-pulse">
  <div class="portfolio-section-heading">
    <span>01</span>
    <div>
      <h2 id="portfolio-pulse">The portfolio pulse</h2>
      <p>Exact additive measures from the latest available combined periods.</p>
    </div>
  </div>

  <div class="portfolio-ledger-row">
    <div class="portfolio-ledger-name"><span class="portfolio-dot production"></span>Production</div>
    <div class="portfolio-ledger-stat"><strong>${fmtNum(latestOutput.total_edits)}</strong><span>edits in ${latestOutput.period ?? "latest year"}</span></div>
    <div class="portfolio-ledger-stat"><strong>${fmtBytes(latestOutput.net_bytes)}</strong><span>net content retained</span></div>
    <div class="portfolio-ledger-stat"><strong>${pct(productiveShare)}</strong><span>productive edits</span></div>
    <a href="/gdp">Open production →</a>
  </div>

  <div class="portfolio-ledger-row">
    <div class="portfolio-ledger-name"><span class="portfolio-dot community"></span>Community</div>
    <div class="portfolio-ledger-stat"><strong>${fmtNum(latestChurn.active_editors ?? latestWorkforce.unique_editors)}</strong><span>active editor–wiki observations</span></div>
    <div class="portfolio-ledger-stat"><strong>${fmtNum(latestChurn.arrivals)}</strong><span>arrivals in ${latestChurn.period ?? "latest month"}</span></div>
    <div class="portfolio-ledger-stat"><strong>${netCommunityChange >= 0 ? "+" : ""}${fmtNum(netCommunityChange)}</strong><span>net community movement</span></div>
    <a href="/labor">Open community →</a>
  </div>

  <div class="portfolio-ledger-row">
    <div class="portfolio-ledger-name"><span class="portfolio-dot governance"></span>Review</div>
    <div class="portfolio-ledger-stat"><strong>${fmtNum(latestPatrol.total_patrols)}</strong><span>patrol actions in ${latestPatrol.period ?? "latest year"}</span></div>
    <div class="portfolio-ledger-stat"><strong>${pct(latestPatrol.patrol_coverage_pct)}</strong><span>explicit patrol coverage</span></div>
    <div class="portfolio-ledger-stat"><strong>${pct(latestPatrol.adjusted_coverage_pct)}</strong><span>including autopatrol</span></div>
    <a href="/patrol">Open patrol →</a>
  </div>
</section>

<section class="portfolio-figure" aria-labelledby="production-history">
  <div class="portfolio-section-heading">
    <span>02</span>
    <div>
      <h2 id="production-history">Combined production history</h2>
      <p>Annual edits summed across the managed editions; the latest year may be partial.</p>
    </div>
  </div>

```js
Plot.plot({
  width,
  height: 330,
  marginLeft: 64,
  x: {label: null, tickFormat: d => String(d)},
  y: {grid: true, label: "Edits", tickFormat: fmtNum},
  marks: [
    Plot.areaY(gdp.output ?? [], {x: "period", y: "total_edits", fill: "var(--wk-blue)", fillOpacity: 0.13}),
    Plot.lineY(gdp.output ?? [], {x: "period", y: "total_edits", stroke: "var(--wk-blue)", strokeWidth: 2.2}),
    Plot.dot((gdp.output ?? []).slice(-1), {x: "period", y: "total_edits", fill: "var(--wk-coral)", r: 4}),
    Plot.ruleY([0]),
  ],
})
```
</section>

<section class="portfolio-split">
  <div class="portfolio-concentration">
    <div class="portfolio-section-heading compact">
      <span>03</span>
      <div>
        <h2>Concentration</h2>
        <p>The one inequality statistic that composes exactly across wikis.</p>
      </div>
    </div>
    <div class="portfolio-theil">
      <strong>${Number.isFinite(latestInequality.theil) ? latestInequality.theil.toFixed(3) : "—"}</strong>
      <span>Combined Theil index · ${latestInequality.period ?? "latest year"}</span>
    </div>
    <p>Gini, Palma, and fragility are intentionally not blended: weighted averages of those per-wiki measures would look precise but be mathematically misleading.</p>
    <a class="portfolio-text-link" href="/inequality">Compare concentration within a specific wiki →</a>
  </div>

  <div class="portfolio-movements">
    <div class="portfolio-section-heading compact">
      <span>04</span>
      <div>
        <h2>Largest recent movements</h2>
        <p>Top article-week edit gains across the portfolio.</p>
      </div>
    </div>
    <ol>
      ${topVariation.map(row => html`<li>
        <span>${row.wiki.replace(/wiki$/, "")}</span>
        <a href=${articleUrl(row.wiki, row.page_title)} target="_blank" rel="noopener noreferrer">${displayTitle(row.page_title)}</a>
        <strong>+${fmtNum(row.wow_change)}</strong>
      </li>`)}
    </ol>
    <a class="portfolio-text-link" href="/edit-variation">Inspect weekly variation by wiki →</a>
  </div>
</section>

<section id="dimensions" class="portfolio-explore">
  <div class="portfolio-section-heading">
    <span>05</span>
    <div>
      <h2>Choose a dimension</h2>
      <p>Each detailed dashboard asks you to select one Wikipedia edition.</p>
    </div>
  </div>
  <div class="portfolio-dimensions">
    <a href="/gdp"><span>01</span><strong>Content production</strong><small>Gross and net output, edit mix, activity tiers</small></a>
    <a href="/labor"><span>02</span><strong>Community</strong><small>Workforce, arrivals, departures, and cohorts</small></a>
    <a href="/inequality"><span>03</span><strong>Edit distribution</strong><small>Gini, Theil, Palma, and contributor fragility</small></a>
    <a href="/patrol"><span>04</span><strong>Patrol</strong><small>Review volume, coverage, latency, and concentration</small></a>
    <a href="/business"><span>05</span><strong>Business health</strong><small>Survival, equilibrium, funnels, and output per editor</small></a>
    <a href="/edit-variation"><span>06</span><strong>Edit variation</strong><small>Largest week-over-week article movements</small></a>
  </div>
</section>

<section id="choose-wiki" class="portfolio-wikis">
  <div class="portfolio-section-heading">
    <span>06</span>
    <div>
      <h2>Choose a Wikipedia</h2>
      <p>Start with its production record, then move between dimensions using the same wiki filter.</p>
    </div>
  </div>
  <div class="portfolio-wiki-grid">
    ${wikis.map(({wiki, name}) => html`<a href=${`/gdp?wiki=${wiki}`}>
      <span>${wiki.replace(/wiki$/, "")}</span>
      <strong>${name}</strong>
      <small>Open profile →</small>
    </a>`)}
  </div>
</section>

<div class="portfolio-method-note">
  <strong>How to read this page.</strong> Combined totals are sums across wiki communities. The Theil index is recomputed from sufficient statistics. Non-additive statistics are shown only inside a specific wiki, where their meaning remains valid.
</div>
