import assert from "node:assert/strict";
import {test} from "node:test";
import {
  ACCOUNT_SCOPE_ALL,
  ACCOUNT_SCOPE_EXCLUDE_INDEFINITE,
  applyAccountScope,
} from "./src/components/account-cohorts.js";

const cohort = {
  year_month: "2026-01",
  accounts_created: 20,
  accounts_with_edits: 8,
  accounts_without_edits: 12,
  indefinitely_blocked_accounts: 5,
  indefinitely_blocked_with_edits: 3,
  indefinitely_blocked_without_edits: 2,
  temporary_accounts_excluded: 4,
};

test("the all-account scope preserves every cohort count", () => {
  assert.deepEqual(applyAccountScope(cohort, ACCOUNT_SCOPE_ALL), {
    ...cohort,
    account_scope: ACCOUNT_SCOPE_ALL,
    excluded_indefinitely_blocked_accounts: 0,
  });
});

test("indefinite-block exclusion subtracts exact edit-status subsets", () => {
  const filtered = applyAccountScope(cohort, ACCOUNT_SCOPE_EXCLUDE_INDEFINITE);
  assert.deepEqual(filtered, {
    ...cohort,
    account_scope: ACCOUNT_SCOPE_EXCLUDE_INDEFINITE,
    accounts_created: 15,
    accounts_with_edits: 5,
    accounts_without_edits: 10,
    indefinitely_blocked_accounts: 0,
    indefinitely_blocked_with_edits: 0,
    indefinitely_blocked_without_edits: 0,
    excluded_indefinitely_blocked_accounts: 5,
  });
  assert.equal(filtered.accounts_created,
    filtered.accounts_with_edits + filtered.accounts_without_edits);
});

test("invalid scopes and non-conserving cohort rows fail closed", () => {
  assert.throws(() => applyAccountScope(cohort, "blocked-ish"), /unsupported account scope/);
  assert.throws(() => applyAccountScope({...cohort, accounts_created: 19}, ACCOUNT_SCOPE_ALL),
    /do not conserve/);
  assert.throws(() => applyAccountScope({
    ...cohort,
    indefinitely_blocked_with_edits: 9,
    indefinitely_blocked_accounts: 11,
  }, ACCOUNT_SCOPE_EXCLUDE_INDEFINITE), /not subsets/);
  assert.throws(() => applyAccountScope({...cohort, accounts_created: NaN}, ACCOUNT_SCOPE_ALL),
    /invalid non-negative account count/);
});
