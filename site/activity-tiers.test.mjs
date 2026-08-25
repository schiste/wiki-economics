import assert from "node:assert/strict";
import {test} from "node:test";

import {
  activityTierLabels,
  activityTierMonths,
} from "./src/components/activity-tiers.js";

test("activity tiers preserve their monthly rate across calendar periods", () => {
  assert.equal(activityTierMonths("month"), 1);
  assert.equal(activityTierMonths("quarter"), 3);
  assert.equal(activityTierMonths("year"), 12);
  assert.deepEqual(activityTierLabels("month"), [
    "1 edit", "2-4 edits", "5-24 edits", "25-99 edits", "100+ edits",
  ]);
  assert.deepEqual(activityTierLabels("quarter"), [
    "1-3 edits", "4-14 edits", "15-74 edits", "75-299 edits", "300+ edits",
  ]);
  assert.deepEqual(activityTierLabels("year"), [
    "1-12 edits", "13-59 edits", "60-299 edits", "300-1199 edits", "1200+ edits",
  ]);
});
