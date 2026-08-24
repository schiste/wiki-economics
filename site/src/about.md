---
title: About
---

# About Wiki Economics

<div class="page-intro">

The goal is simple: give Wikipedia communities clear, comparable data to analyze their own health across time — not a one-off snapshot, but a running series you can watch move.

</div>

## Why economics?

Every edit is a unit of production. Every editor is a worker. Every namespace is a sector of the economy. Wiki Economics borrows well-understood frameworks from economics — GDP, labor statistics, inequality metrics, quality control — to give Wikipedia communities a complementary lens on their own health, resilience, and dynamics.

That's not about reducing editors to numbers. It's about surfacing structural patterns that are hard to see in raw activity logs: Is output concentrated in too few hands? Are newcomers being retained? Is quality control keeping pace with content production? Every wiki community asks these questions already — economics just gives them a shared, peer-reviewed vocabulary to answer with, instead of ad-hoc definitions reinvented per wiki.

The same framing pays off twice: the metrics are well-defined and already validated outside Wikipedia (Gini, Theil, and Palma are standard tools at the World Bank and OECD), and they're immediately legible to anyone who reads economic reporting — which includes most institutional stakeholders a community might need to make its case to.

## What "across time" means in practice

A single number rarely tells a community anything actionable. What matters is the trend: is edit concentration rising or falling, is the newcomer cohort retaining better or worse than last year's, is patrol latency creeping up as volume grows. Wiki Economics is built to answer those comparisons directly — every indicator is computed month by month and can be viewed at monthly, quarterly, or yearly granularity, filtered by wiki, user type, or namespace, so a community can watch its own trajectory rather than read a single point-in-time report.

## Where it stands today

Wiki Economics is in early development. The pipeline runs, the dashboard is live, and the four indicator families — [Edit Distribution](/inequality), [Community](/labor), [Content Production](/gdp), and [Patrol](/patrol) — are computed and published for a growing set of Wikipedia language editions. The metric definitions aren't frozen: they're expected to be refined as wiki communities weigh in on whether the economic analogies hold up against their lived experience.

## What's next

The current release is intentionally Wikipedia-first, starting with the yearly-partitioned language editions that fetch cleanly from Wikimedia dumps. From here:

- **Wider language coverage.** More Wikipedia editions are being qualified and brought into the regular publication schedule as their onboarding and capacity work is validated.
- **English Wikipedia.** Enwiki's scale — monthly-partitioned dumps, an order of magnitude larger than the wikis onboarded so far — is under active feasibility exploration. It's not on the production schedule yet; it needs its own compute and capacity qualification first.
- **Metric refinement.** Definitions get sharpened as feedback comes in about where the economic metaphors hold and where wiki-specific dynamics break them.

## Feedback

This project is explicitly seeking input from the communities it measures. Which indicators are useful, which are missing, where do the economic analogies hold up or fall apart, is the naming clear to a volunteer community — all of that shapes where this goes next. Leave feedback on the [talk page on Meta-Wiki](https://meta.wikimedia.org/wiki/Talk:Next_25/Wiki_Economics), or open an issue on [GitHub](https://github.com/schiste/wiki-economics).
