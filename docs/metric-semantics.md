# Metric aggregation semantics

This document is the normative contract for composing published metric rows.
When a requested view cannot be reconstructed exactly from the stored
sufficient statistics, publication and browser code must return an unavailable
value rather than a weighted approximation.

## Population identity

An editor identity is the canonical local MediaWiki user ID when available, or
the historical actor text for legacy IP actors. The grouping key is internal
and is never published. Identities are local to a wiki, so a fleet-wide count
is explicitly a count of **editor–wiki observations**, not global people.

A distinct count is valid only at the grain where it was computed. It may be
summed across wikis because those local identity spaces are treated as disjoint.
It must not be summed across time periods or namespaces. Period-aware activity
and inequality metrics assign each identity one deterministic user type per
period, making those particular type strata disjoint and safely composable.

`gdp.unique_editors` and `labor_monthly.unique_editors` therefore describe one
wiki × month × namespace × user-type row and are non-additive outside that
grain. Exact wiki-wide month, quarter, and year populations come from
`gdp_activity_tiers`, where each identity is classified once per period.

## GDP

- Gross output is the sum of positive revision byte differences.
- Net output is the sum of all revision byte differences.
- Bytes per edit is `net_bytes / total_edits`.
- Bytes per editor is `net_bytes / exact distinct editors at the stated grain`.
- Revert rate is `reverted_edits / total_edits`.

Gross output is never the numerator of a productivity ratio. Ratios are always
recomputed from their additive numerator and denominator after a valid merge;
precomputed ratios are never averaged.

## Inequality

For each wiki, user type, and fixed calendar month, quarter, or year, revisions
are first reduced to one edit total per canonical editor identity. Gini, Theil
T, Palma, fragility, total editors, and total edits are then computed directly
from that period distribution. An identity active in several constituent
months is counted once.

Gini, Palma, and fragility cannot be reconstructed from grouped summaries and
are unavailable after combining populations. Theil may be composed only across
populations known to be disjoint. The all-wiki Theil uses disjoint local
editor–wiki populations. Within a period, every identity receives one user type
using the precedence bot, temporary, anonymous, then registered, so those rows
also form disjoint populations.

## Patrol

Patrol events, new-page patrols, diff patrols, patrolled revisions,
autopatrolled revisions, and total revisions are additive. Coverage rates are
recomputed from the summed revision counts.

Unique patrollers, median latency, P90 latency, top-patroller concentration,
and minimum patrollers responsible for 50% are non-additive. They are exact at
one wiki × month × namespace × user-type row only and become unavailable when
multiple rows are combined. A weighted average of quantiles or concentration
statistics is not a pooled statistic.

## Activity-tier denominators

Activity tiers aggregate an identity's edits across the complete selected
calendar period before assigning the tier. Period thresholds scale by one,
three, or twelve months. Counts and per-editor ratios derived from this family
therefore use exact period populations rather than sums of monthly distinct
counts.
