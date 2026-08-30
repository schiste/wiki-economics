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

/**
 * Return the concrete wiki scopes available to analytical detail pages.
 * "all" is a publication scope used by the portfolio homepage, not a wiki.
 */
export function detailWikis(wikis) {
  return [...new Set(wikis.filter(wiki => wiki && !isAllWikis(wiki)))];
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
 * Compare a live filter state with the exact selection represented by a
 * precomputed dashboard artifact. The artifact range may intentionally be
 * narrower than the complete queryable source range.
 */
export function matchesDefaultSelection(filters, {
  wiki,
  range,
  userTypes = ["registered"],
  granularity = "year",
  namespaces = [0],
}) {
  if (!wiki || !range) return false;
  return filters.wiki === wiki
    && filters.granularity === granularity
    && filters.startPeriod === range.mn
    && filters.endPeriod === range.mx
    && (filters.userTypes == null
      ? userTypes == null || userTypes.length === 0
      : userTypes != null
        && filters.userTypes.length === userTypes.length
        && userTypes.every(type => filters.userTypes.includes(type)))
    && (filters.namespaces == null
      ? namespaces == null
      : namespaces != null
        && filters.namespaces.length === namespaces.length
        && namespaces.every(namespace => filters.namespaces.includes(namespace)));
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

/** Aggregate counts and the exactly decomposable Theil statistic.
 * Gini, Palma, and fragility cannot be recovered from grouped summaries, so
 * they remain available only when a selected period contains one source row.
 */
export function aggregateInequalityByPeriod(rows) {
  const grouped = new Map();
  for (const row of rows) {
    const entry = grouped.get(row.period) ?? {
      period: row.period,
      total_editors: 0,
      total_edits: 0,
      source_rows: 0,
      within_theil: 0,
      edit_mean_log: 0,
      single: row,
    };
    const weight = row.total_editors ?? 0;
    entry.total_editors += weight;
    const edits = row.total_edits ?? 0;
    entry.total_edits += edits;
    entry.source_rows += 1;
    if (edits > 0 && weight > 0 && row.theil != null) {
      entry.within_theil += edits * row.theil;
      entry.edit_mean_log += edits * Math.log(edits / weight);
    }
    grouped.set(row.period, entry);
  }
  return [...grouped.values()]
    .sort((left, right) => left.period.localeCompare(right.period))
    .map(entry => {
      const exactTheil = entry.total_edits > 0 && entry.total_editors > 0
        ? (entry.within_theil + entry.edit_mean_log
          - entry.total_edits * Math.log(entry.total_edits / entry.total_editors)) / entry.total_edits
        : null;
      return {
        period: entry.period,
        total_editors: entry.total_editors,
        total_edits: entry.total_edits,
        min_editors_50pct: entry.source_rows === 1 ? entry.single.min_editors_50pct : null,
        gini: entry.source_rows === 1 ? entry.single.gini : null,
        theil: exactTheil,
        palma: entry.source_rows === 1 ? entry.single.palma : null,
      };
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
  ];
  for (const row of rows) {
    const entry = grouped.get(row.period) ?? {
      period: row.period,
      median_latency_hours: 0,
      p90_latency_hours: 0,
      top1_pct: 0,
      source_rows: 0,
      single: row,
    };
    for (const column of sumColumns) entry[column] = (entry[column] ?? 0) + (row[column] ?? 0);
    entry.source_rows += 1;
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
      if (output.source_rows > 1) {
        output.median_latency_hours = null;
        output.p90_latency_hours = null;
        output.top1_pct = null;
        output.min_patrollers_50pct = null;
      } else {
        output.min_patrollers_50pct = output.single.min_patrollers_50pct;
      }
      delete output.source_rows;
      delete output.single;
      return output;
    });
}
