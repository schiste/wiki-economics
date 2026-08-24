---
title: About
---

# About Wiki Economics

<div class="page-intro">

Every discipline has its own idea of what "healthy" means. Biologists check vitals. Economists check GDP, employment, and how evenly the gains are shared, then watch how those move over time, not just where they sit today. Wiki Economics borrows that last toolkit and points it at Wikipedia.

</div>

## Where this started

When I wrote my ["What now?" essay](https://meta.wikimedia.org/wiki/User:Schiste/what-now) on Meta-Wikimedia, I leaned heavily on traffic data: page views, readership, the usual web metrics. What I was missing was a proper way to look at community health. So I decided to build one: a way to analyze and compare community health across projects, instead of guessing at it from traffic numbers alone.

I'm not a Wikipedia researcher by training. I come from economics and business, so that's the toolbox I reached for. Not to invent new metrics, but to borrow ones economists already trust and adapt them to a wiki. Edit concentration becomes an inequality question: the same Gini and Theil indices the World Bank runs on national income, pointed at edit counts instead. Editor arrivals and departures become a labor market question: retention, churn, and a wiki's own version of key person risk (how many core contributors could you lose before the whole enterprise stalls). Bytes added and reverted become a production question: gross output versus what actually survives review, the wiki equivalent of GDP versus GDP after you fix the typos. Patrol becomes a regulatory economics question: is the "inspection" workforce keeping pace with output, or quietly falling behind.

## What this actually is

To be clear about the epistemics here: this is **experimental**, not a finished instrument. Think of it less as an audited annual report and more as an economist wandering into a wiki with a calculator, muttering "well, actually, in GDP terms...". The goal is to play with the data, try the metrics on for size, and produce concrete, comparable numbers people can actually argue with: the sort of thing you can put in front of a community and ask, "does this match what you're feeling on the ground, or is the metaphor breaking down here?" If a chart starts a real conversation about where a community is heading, it's done its job, whether or not the number behind it survives the conversation.

## What's next

The current release is intentionally Wikipedia-first, starting with the yearly-partitioned language editions that fetch cleanly from Wikimedia dumps. From here:

- **Wider language coverage.** More Wikipedia editions are being qualified and brought into the regular publication schedule as their onboarding and capacity work is validated.
- **English Wikipedia.** Enwiki's scale (monthly-partitioned dumps, an order of magnitude larger than the wikis onboarded so far) is under active feasibility exploration. It's not on the production schedule yet; it needs its own compute and capacity qualification first.
- **Metric refinement.** Definitions get sharpened as feedback comes in about where the economic metaphors hold and where wiki-specific dynamics break them.

## Feedback

This project is explicitly seeking input from the communities it measures. Which indicators are useful, which are missing, where do the economic analogies hold up or fall apart, is the naming clear to a volunteer community: all of that shapes where this goes next. Leave feedback on the [talk page on Meta-Wiki](https://meta.wikimedia.org/wiki/Talk:Next_25/Wiki_Economics), or open an issue on [GitHub](https://github.com/schiste/wiki-economics).
