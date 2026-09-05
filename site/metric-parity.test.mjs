import assert from "node:assert/strict";
import {readFile} from "node:fs/promises";
import {test} from "node:test";
import {
  aggregateActivityByPeriod,
  aggregateGdpByPeriod,
  aggregateInequalityByPeriod,
  aggregatePatrolByPeriod,
} from "./src/components/wiki-scope.js";

const fixture = JSON.parse(await readFile(
  new URL("../tests/fixtures/adversarial-metric-parity.json", import.meta.url),
));

function assertClose(actual, expected, path = "custom") {
  if (typeof actual === "number" && typeof expected === "number") {
    assert.ok(Math.abs(actual - expected) <= 1e-10, `${path}: ${actual} != ${expected}`);
    return;
  }
  if (Array.isArray(expected)) {
    assert.equal(actual.length, expected.length, `${path}: array length`);
    expected.forEach((value, index) => assertClose(actual[index], value, `${path}[${index}]`));
    return;
  }
  if (expected && typeof expected === "object") {
    assert.deepEqual(Object.keys(actual).sort(), Object.keys(expected).sort(), `${path}: keys`);
    for (const [key, value] of Object.entries(expected)) {
      assertClose(actual[key], value, `${path}.${key}`);
    }
    return;
  }
  assert.equal(actual, expected, path);
}

test("adversarial custom queries match the cross-layer Rust parity contract", () => {
  assert.equal(fixture.schema_version, 1);
  assert.ok(new Set(fixture.history.map(row => row.wiki)).size > 1);
  const renamed = fixture.history.filter(row => row.wiki === "alphawiki" && row.user_id === 10);
  assert.ok(new Set(renamed.map(row => row.timestamp.slice(0, 7))).size > 1);
  assert.ok(new Set(renamed.map(row => row.namespace)).size > 1);
  assert.ok(new Set(renamed.map(row => row.historical_name)).size > 1);
  for (const property of ["bot", "anonymous", "temporary", "indefinitely_blocked"]) {
    assert.ok(fixture.history.some(row => row[property]), `missing ${property} account`);
  }
  const recurringPatroller = fixture.patrol_events.filter(row =>
    row.wiki === "alphawiki" && row.patroller === "Patroller A");
  assert.ok(new Set(recurringPatroller.map(row => row.year_month)).size > 1);

  const gdp = aggregateGdpByPeriod(fixture.custom_inputs.gdp)[0];
  const activity = aggregateActivityByPeriod(fixture.custom_inputs.activity)[0];
  const inequality = aggregateInequalityByPeriod(fixture.custom_inputs.inequality)[0];
  const patrol = aggregatePatrolByPeriod(fixture.custom_inputs.patrol)[0];
  const actual = {
    gdp: {
      period: gdp.period,
      gross_bytes_added: gdp.gross_bytes_added,
      net_bytes: gdp.net_bytes,
      total_edits: gdp.total_edits,
      productive_edits: gdp.productive_edits,
      reverted_edits: gdp.reverted_edits,
      bytes_per_edit: gdp.net_bytes / gdp.total_edits,
    },
    activity,
    inequality,
    patrol,
  };
  assertClose(actual, fixture.expected);
});
