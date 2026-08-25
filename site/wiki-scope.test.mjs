import assert from "node:assert/strict";
import {test} from "node:test";
import {
  ALL_WIKIS,
  aggregateChurn,
  aggregateCohorts,
  aggregateFunnel,
  aggregateInequalityByPeriod,
  aggregatePatrolByPeriod,
  combinedNamespaces,
  combinedRange,
  formatWiki,
  wikiMatches,
  withAllWikis,
} from "./src/components/wiki-scope.js";
import {selectBrowserEntries} from "./src/components/browser-data.js";

test("all-wiki scope has a stable value, label, range, and namespace union", () => {
  assert.deepEqual(withAllWikis(["ptwiki", "nlwiki"]), [ALL_WIKIS, "ptwiki", "nlwiki"]);
  assert.equal(formatWiki(ALL_WIKIS), "All wikis");
  assert.equal(wikiMatches({wiki: "nlwiki"}, ALL_WIKIS), true);
  assert.equal(wikiMatches({wiki: "nlwiki"}, "ptwiki"), false);
  assert.deepEqual(combinedRange(new Map([
    ["nlwiki", {mn: "2001-01", mx: "2026-07"}],
    ["ptwiki", {mn: "2002-03", mx: "2026-06"}],
  ])), {mn: "2001-01", mx: "2026-07"});
  assert.deepEqual(combinedNamespaces(new Map([
    ["nlwiki", [1, 0, null]],
    ["ptwiki", [2, 0]],
  ])), [0, 1, 2, null]);
});

test("all-wiki count reducers sum primitives and recompute rates", () => {
  assert.deepEqual(aggregateChurn([
    {period: "2026-01", period_type: "month", active_editors: 10, arrivals: 2, departures: 1},
    {period: "2026-01", period_type: "month", active_editors: 5, arrivals: 1, departures: 2},
  ]), [{period: "2026-01", period_type: "month", active_editors: 15, arrivals: 3,
    departures: 3, arrival_rate: 0.2, departure_rate: 0.2}]);
  assert.deepEqual(aggregateCohorts([
    {cohort_year: "2020", year: "2021", initial_editors: 10, survived_editors: 4},
    {cohort_year: "2020", year: "2021", initial_editors: 5, survived_editors: 3},
  ]), [{cohort_year: "2020", year: "2021", initial_editors: 15, survived_editors: 7}]);
  assert.deepEqual(aggregateFunnel([
    {cohort_year: "2020", cohort_size: 10, reached_5: 5, reached_25: 2, reached_100: 1},
    {cohort_year: "2020", cohort_size: 4, reached_5: 3, reached_25: 1, reached_100: 0},
  ]), [{cohort_year: "2020", cohort_size: 14, reached_5: 8, reached_25: 3, reached_100: 1}]);
});

test("non-additive portfolio statistics are weighted and coverage is recomputed", () => {
  assert.deepEqual(aggregateInequalityByPeriod([
    {period: "2026", total_editors: 10, total_edits: 30, min_editors_50pct: 2, gini: 0.4, theil: 0.2, palma: 1},
    {period: "2026", total_editors: 30, total_edits: 70, min_editors_50pct: 4, gini: 0.8, theil: 0.6, palma: 3},
  ]), [{period: "2026", total_editors: 40, total_edits: 100, min_editors_50pct: 6,
    gini: 0.7, theil: 0.5, palma: 2.5}]);

  const patrol = aggregatePatrolByPeriod([
    {period: "2026", total_patrols: 10, unique_patrollers: 2, patrol_new_pages: 4, patrol_diffs: 6,
      patrolled_revisions: 8, autopatrolled_revisions: 1, total_revisions: 20,
      min_patrollers_50pct: 1, median_latency_hours: 1, p90_latency_hours: 3, top1_pct: 40},
    {period: "2026", total_patrols: 30, unique_patrollers: 4, patrol_new_pages: 12, patrol_diffs: 18,
      patrolled_revisions: 12, autopatrolled_revisions: 3, total_revisions: 30,
      min_patrollers_50pct: 2, median_latency_hours: 3, p90_latency_hours: 5, top1_pct: 20},
  ])[0];
  assert.equal(patrol.total_patrols, 40);
  assert.equal(patrol.median_latency_hours, 2.5);
  assert.equal(patrol.patrol_coverage_pct, 40);
  assert.equal(patrol.adjusted_coverage_pct, 48);
});

test("all-wiki browser selection includes every requested partition deterministically", () => {
  const index = {entries: [
    {wiki: "ptwiki", metric: "gdp", minimum_date: "2020-01", file: "pt-gdp"},
    {wiki: "nlwiki", metric: "labor", minimum_date: "2001-01", file: "nl-labor"},
    {wiki: "nlwiki", metric: "gdp", minimum_date: "2001-01", file: "nl-gdp"},
    {wiki: "ptwiki", metric: "labor", minimum_date: "2020-01", file: "pt-labor"},
  ]};
  assert.deepEqual(
    selectBrowserEntries(index, {gdp: "gdp", labor: "labor"}, ALL_WIKIS).map(entry => entry.file),
    ["nl-gdp", "pt-gdp", "nl-labor", "pt-labor"],
  );
  assert.throws(() => selectBrowserEntries(
    {entries: index.entries.filter(entry => entry.file !== "pt-labor")},
    {gdp: "gdp", labor: "labor"},
    ALL_WIKIS,
  ), /no labor partition for ptwiki/);
});
