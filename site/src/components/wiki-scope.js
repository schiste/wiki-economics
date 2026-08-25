export const ALL_WIKIS = "all";
export const ALL_WIKIS_LABEL = "All wikis";

export function isAllWikis(wiki) {
  return wiki === ALL_WIKIS;
}

export function wikiMatches(row, wiki) {
  return isAllWikis(wiki) || row.wiki === wiki;
}

export function formatWiki(wiki) {
  return isAllWikis(wiki) ? ALL_WIKIS_LABEL : wiki;
}

export function withAllWikis(wikis) {
  return [ALL_WIKIS, ...wikis.filter(wiki => !isAllWikis(wiki))];
}

export function combinedRange(rangeByWiki) {
  let mn = null;
  let mx = null;
  for (const [wiki, range] of rangeByWiki) {
    if (isAllWikis(wiki)) continue;
    if (range?.mn && (!mn || range.mn < mn)) mn = range.mn;
    if (range?.mx && (!mx || range.mx > mx)) mx = range.mx;
  }
  return {mn: mn ?? "", mx: mx ?? ""};
}

export function combinedNamespaces(nsByWiki) {
  const namespaces = new Set();
  for (const [wiki, values] of nsByWiki) {
    if (isAllWikis(wiki)) continue;
    for (const value of values) namespaces.add(value);
  }
  return [...namespaces].sort((left, right) => {
    if (left == null) return right == null ? 0 : 1;
    if (right == null) return -1;
    return left - right;
  });
}

/**
 * Combine churn rows as a portfolio of wiki communities. Editor counts are
 * editor-wiki participations; rates are recomputed from the additive counts.
 */
export function aggregateChurn(rows) {
  const grouped = new Map();
  for (const row of rows) {
    const entry = grouped.get(row.period) ?? {
      period: row.period,
      period_type: row.period_type,
      active_editors: 0,
      arrivals: 0,
      departures: 0,
    };
    entry.active_editors += row.active_editors ?? 0;
    entry.arrivals += row.arrivals ?? 0;
    entry.departures += row.departures ?? 0;
    grouped.set(row.period, entry);
  }
  return [...grouped.values()]
    .sort((left, right) => left.period.localeCompare(right.period))
    .map(entry => ({
      ...entry,
      arrival_rate: entry.active_editors > 0 ? entry.arrivals / entry.active_editors : 0,
      departure_rate: entry.active_editors > 0 ? entry.departures / entry.active_editors : 0,
    }));
}

export function aggregateCohorts(rows) {
  const grouped = new Map();
  for (const row of rows) {
    const key = `${row.cohort_year}\u0000${row.year}`;
    const entry = grouped.get(key) ?? {
      cohort_year: row.cohort_year,
      year: row.year,
      initial_editors: 0,
      survived_editors: 0,
    };
    entry.initial_editors += row.initial_editors ?? 0;
    entry.survived_editors += row.survived_editors ?? 0;
    grouped.set(key, entry);
  }
  return [...grouped.values()].sort((left, right) =>
    left.cohort_year.localeCompare(right.cohort_year) || left.year.localeCompare(right.year));
}

export function aggregateFunnel(rows) {
  const grouped = new Map();
  for (const row of rows) {
    const entry = grouped.get(row.cohort_year) ?? {
      cohort_year: row.cohort_year,
      cohort_size: 0,
      reached_5: 0,
      reached_25: 0,
      reached_100: 0,
    };
    for (const column of ["cohort_size", "reached_5", "reached_25", "reached_100"]) {
      entry[column] += row[column] ?? 0;
    }
    grouped.set(row.cohort_year, entry);
  }
  return [...grouped.values()].sort((left, right) => left.cohort_year.localeCompare(right.cohort_year));
}

/**
 * Aggregate already-computed inequality rows. These are editor-weighted
 * cross-wiki statistics, not a pooled re-computation from editor-level rows.
 */
export function aggregateInequalityByPeriod(rows) {
  const grouped = new Map();
  for (const row of rows) {
    const entry = grouped.get(row.period) ?? {
      period: row.period,
      total_editors: 0,
      total_edits: 0,
      min_editors_50pct: 0,
      gini: 0,
      theil: 0,
      palma: 0,
    };
    const weight = row.total_editors ?? 0;
    entry.total_editors += weight;
    entry.total_edits += row.total_edits ?? 0;
    entry.min_editors_50pct += row.min_editors_50pct ?? 0;
    for (const column of ["gini", "theil", "palma"]) {
      if (row[column] != null && weight > 0) {
        entry[column] += row[column] * weight;
        entry[`${column}_weight`] = (entry[`${column}_weight`] ?? 0) + weight;
      }
    }
    grouped.set(row.period, entry);
  }
  return [...grouped.values()]
    .sort((left, right) => left.period.localeCompare(right.period))
    .map(entry => {
      const output = {...entry};
      for (const column of ["gini", "theil", "palma"]) {
        const weight = output[`${column}_weight`] ?? 0;
        output[column] = weight > 0 ? output[column] / weight : null;
        delete output[`${column}_weight`];
      }
      return output;
    });
}

/**
 * Patrol counts are additive. Coverage is recomputed from its component
 * counts; distribution summaries use patrol-volume weighting.
 */
export function aggregatePatrolByPeriod(rows) {
  const grouped = new Map();
  const sumColumns = [
    "total_patrols", "unique_patrollers", "patrol_new_pages", "patrol_diffs",
    "patrolled_revisions", "autopatrolled_revisions", "total_revisions",
    "min_patrollers_50pct",
  ];
  for (const row of rows) {
    const entry = grouped.get(row.period) ?? {
      period: row.period,
      median_latency_hours: 0,
      p90_latency_hours: 0,
      top1_pct: 0,
    };
    for (const column of sumColumns) entry[column] = (entry[column] ?? 0) + (row[column] ?? 0);
    const weight = row.total_patrols ?? 0;
    if (weight > 0) {
      for (const column of ["median_latency_hours", "p90_latency_hours", "top1_pct"]) {
        if (row[column] != null) {
          entry[column] += row[column] * weight;
          entry[`${column}_weight`] = (entry[`${column}_weight`] ?? 0) + weight;
        }
      }
    }
    grouped.set(row.period, entry);
  }
  return [...grouped.values()]
    .sort((left, right) => left.period.localeCompare(right.period))
    .map(entry => {
      const output = {...entry};
      for (const column of ["median_latency_hours", "p90_latency_hours", "top1_pct"]) {
        const weight = output[`${column}_weight`] ?? 0;
        output[column] = weight > 0 ? output[column] / weight : null;
        delete output[`${column}_weight`];
      }
      output.patrol_coverage_pct = output.total_revisions > 0
        ? output.patrolled_revisions / output.total_revisions * 100 : 0;
      output.adjusted_coverage_pct = output.total_revisions > 0
        ? (output.patrolled_revisions + output.autopatrolled_revisions) / output.total_revisions * 100 : 0;
      return output;
    });
}
