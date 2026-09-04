export const ACCOUNT_SCOPE_ALL = "all_permanent_accounts";
export const ACCOUNT_SCOPE_EXCLUDE_INDEFINITE = "exclude_indefinitely_blocked";

const COUNT_FIELDS = [
  "accounts_created",
  "accounts_with_edits",
  "accounts_without_edits",
  "indefinitely_blocked_accounts",
  "indefinitely_blocked_with_edits",
  "indefinitely_blocked_without_edits",
];

function requireCount(row, field) {
  const value = row[field];
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new Error(`invalid non-negative account count for ${field}`);
  }
  return value;
}

/**
 * Apply the staging account scope to one mutually exclusive monthly cohort.
 *
 * The report contains exact blocked-with-edits and blocked-without-edits
 * subsets, so exclusion is lossless and remains additive when the caller
 * subsequently groups months into quarters or years.
 */
export function applyAccountScope(row, scope) {
  for (const field of COUNT_FIELDS) requireCount(row, field);

  if (row.accounts_created !== row.accounts_with_edits + row.accounts_without_edits) {
    throw new Error("account edit-status cohorts do not conserve the created total");
  }
  if (row.indefinitely_blocked_accounts
      !== row.indefinitely_blocked_with_edits + row.indefinitely_blocked_without_edits) {
    throw new Error("blocked edit-status cohorts do not conserve the blocked total");
  }
  if (row.indefinitely_blocked_with_edits > row.accounts_with_edits
      || row.indefinitely_blocked_without_edits > row.accounts_without_edits) {
    throw new Error("blocked cohorts are not subsets of their account cohorts");
  }

  if (scope === ACCOUNT_SCOPE_ALL) {
    return {
      ...row,
      account_scope: scope,
      excluded_indefinitely_blocked_accounts: 0,
    };
  }
  if (scope !== ACCOUNT_SCOPE_EXCLUDE_INDEFINITE) {
    throw new Error(`unsupported account scope: ${scope}`);
  }

  return {
    ...row,
    account_scope: scope,
    accounts_created: row.accounts_created - row.indefinitely_blocked_accounts,
    accounts_with_edits: row.accounts_with_edits - row.indefinitely_blocked_with_edits,
    accounts_without_edits: row.accounts_without_edits - row.indefinitely_blocked_without_edits,
    indefinitely_blocked_accounts: 0,
    indefinitely_blocked_with_edits: 0,
    indefinitely_blocked_without_edits: 0,
    excluded_indefinitely_blocked_accounts: row.indefinitely_blocked_accounts,
  };
}
