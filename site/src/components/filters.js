import * as Inputs from "npm:@observablehq/inputs";
import {html} from "npm:htl";

/**
 * Convert a YYYY-MM string to a period key based on granularity.
 */
export function toPeriod(ym, granularity) {
  if (granularity === "year") return ym.slice(0, 4);
  if (granularity === "quarter") {
    const q = Math.ceil(parseInt(ym.slice(5, 7)) / 3);
    return `${ym.slice(0, 4)}-Q${q}`;
  }
  return ym;
}

/**
 * Namespace labels for display.
 */
export const NS_LABELS = {
  0: "Article", 1: "Talk", 2: "User", 3: "User Talk",
  4: "Wikipedia", 5: "Wikipedia Talk", 6: "File", 7: "File Talk",
  8: "MediaWiki", 9: "MediaWiki Talk", 10: "Template", 11: "Template Talk",
  12: "Help", 13: "Help Talk", 14: "Category", 15: "Category Talk",
  100: "Portal", 101: "Portal Talk",
  828: "Module", 829: "Module Talk",
  1728: "Draft", 1729: "Draft Talk",
  2300: "Gadget", 2301: "Gadget definition", 2302: "Gadget Talk",
  2303: "Gadget definition Talk"
};

export function nsLabel(n) {
  if (n == null) return "(no namespace)";
  return NS_LABELS[n] ?? `ns ${n}`;
}

/**
 * Filter rows by user types, namespaces, and period range, then add a `period` key.
 * If namespaces is null/undefined, skip namespace filtering (for data without page_namespace).
 */
export function filterRows(rows, {userTypes, namespaces, startPeriod, endPeriod, granularity}) {
  return Array.from(rows)
    .filter(d =>
      userTypes.includes(d.user_type) &&
      d.year_month >= startPeriod &&
      d.year_month <= endPeriod &&
      (namespaces == null || namespaces.includes(d.page_namespace))
    )
    .map(d => ({...d, period: toPeriod(d.year_month, granularity)}));
}

/**
 * Aggregate filtered rows by period, summing the given numeric columns.
 */
export function aggregateByPeriod(rows, sumCols) {
  const map = new Map();
  for (const row of rows) {
    const key = row.period;
    if (!map.has(key)) {
      const entry = {period: key};
      for (const c of sumCols) entry[c] = 0;
      map.set(key, entry);
    }
    const entry = map.get(key);
    for (const c of sumCols) entry[c] += (row[c] ?? 0);
  }
  return Array.from(map.values()).sort((a, b) => a.period < b.period ? -1 : 1);
}

/**
 * Format large numbers: 1234567 → "1.2M", 45000 → "45K", 900 → "900"
 */
export function fmtNum(n) {
  if (n == null || isNaN(n)) return "—";
  const abs = Math.abs(n);
  const sign = n < 0 ? "−" : "";
  if (abs >= 1e9) return sign + (abs / 1e9).toFixed(1) + "B";
  if (abs >= 1e6) return sign + (abs / 1e6).toFixed(1) + "M";
  if (abs >= 1e4) return sign + (abs / 1e3).toFixed(1) + "K";
  if (abs >= 1e3) return sign + Math.round(abs).toLocaleString();
  if (Number.isInteger(n)) return sign + String(abs);
  return sign + abs.toFixed(1);
}

/**
 * Loading-state counter for chart sections.
 * Uses RAF debounce so dependent cells don't cause a flash.
 */
let _loadingCount = 0;
let _loadingRaf = 0;

export function startLoading() {
  _loadingCount++;
  cancelAnimationFrame(_loadingRaf);
  document.body.classList.add("wk-loading");
}

export function doneLoading() {
  _loadingCount = Math.max(0, _loadingCount - 1);
  if (_loadingCount === 0) {
    _loadingRaf = requestAnimationFrame(() => {
      if (_loadingCount === 0) document.body.classList.remove("wk-loading");
    });
  }
}

/**
 * Format bytes: 1234567 → "1.2 MB"
 */
export function fmtBytes(n) {
  if (n == null || isNaN(n)) return "—";
  const abs = Math.abs(n);
  const sign = n < 0 ? "−" : "";
  if (abs >= 1e9) return sign + (abs / 1e9).toFixed(1) + " GB";
  if (abs >= 1e6) return sign + (abs / 1e6).toFixed(1) + " MB";
  if (abs >= 1e3) return sign + (abs / 1e3).toFixed(1) + " KB";
  return sign + abs + " B";
}

const FILTER_STATE_STORAGE_KEY = "wiki-econ.filters.v1";
const USER_TYPE_OPTIONS = ["registered", "temporary", "anonymous", "bot"];
const GRANULARITY_OPTIONS = ["month", "quarter", "year"];
const PERIOD_RE = /^\d{4}-\d{2}$/;

function getFilterStorage() {
  try {
    return globalThis.localStorage ?? null;
  } catch {
    return null;
  }
}

function readPersistedFilters(storageKey = FILTER_STATE_STORAGE_KEY) {
  const storage = getFilterStorage();
  if (!storage) return null;
  try {
    const raw = storage.getItem(storageKey);
    return raw ? JSON.parse(raw) : null;
  } catch {
    return null;
  }
}

function writePersistedFilters(state, storageKey = FILTER_STATE_STORAGE_KEY) {
  const storage = getFilterStorage();
  if (!storage) return;
  try {
    storage.setItem(storageKey, JSON.stringify(state));
  } catch {
    // Ignore storage failures so filters keep working in private mode.
  }
}

function resolveDefaultWiki(defaultWiki, wikis) {
  if (defaultWiki && wikis.includes(defaultWiki)) return defaultWiki;
  return wikis[0] ?? null;
}

function deriveMaxMonth(rangeByWiki, explicitMaxMonth = null) {
  if (explicitMaxMonth) return explicitMaxMonth;
  let maxMonth = null;
  for (const range of rangeByWiki.values()) {
    if (range?.mx && (!maxMonth || range.mx > maxMonth)) maxMonth = range.mx;
  }
  return maxMonth;
}

function fallbackRange(rangeByWiki, maxMonth = null) {
  let minMonth = null;
  let derivedMaxMonth = maxMonth;
  for (const range of rangeByWiki.values()) {
    if (range?.mn && (!minMonth || range.mn < minMonth)) minMonth = range.mn;
    if (range?.mx && (!derivedMaxMonth || range.mx > derivedMaxMonth)) derivedMaxMonth = range.mx;
  }
  if (!minMonth && !derivedMaxMonth) return {mn: "", mx: ""};
  return {
    mn: minMonth ?? derivedMaxMonth ?? "",
    mx: derivedMaxMonth ?? minMonth ?? ""
  };
}

function isValidPeriod(period) {
  return typeof period === "string" && PERIOD_RE.test(period);
}

function normalizeAllowedSelection(selected, allowedValues, fallbackValues) {
  if (Array.isArray(selected)) {
    return selected.filter(value => allowedValues.includes(value));
  }
  return fallbackValues.filter(value => allowedValues.includes(value));
}

function normalizeNamespaces(selected, allowedValues, fallbackValues) {
  if (Array.isArray(selected)) {
    return selected.filter(value => allowedValues.includes(value));
  }
  const fallback = fallbackValues.filter(value => allowedValues.includes(value));
  return fallback.length > 0 ? fallback : (allowedValues.length > 0 ? [allowedValues[0]] : []);
}

function normalizeRangeSelection(selection, range) {
  if (!range) return {startPeriod: "", endPeriod: ""};
  let startPeriod = isValidPeriod(selection?.startPeriod) ? selection.startPeriod : range.mn;
  let endPeriod = isValidPeriod(selection?.endPeriod) ? selection.endPeriod : range.mx;
  if (range.mn && startPeriod < range.mn) startPeriod = range.mn;
  if (range.mx && startPeriod > range.mx) startPeriod = range.mx;
  if (range.mn && endPeriod < range.mn) endPeriod = range.mn;
  if (range.mx && endPeriod > range.mx) endPeriod = range.mx;
  if (startPeriod && endPeriod && startPeriod > endPeriod) {
    startPeriod = range.mn;
    endPeriod = range.mx;
  }
  return {startPeriod, endPeriod};
}

function persistedRangeForWiki(persisted, wiki) {
  if (!persisted || !wiki) return null;
  return persisted.rangeByWiki?.[wiki]
    ?? (persisted.wiki === wiki
      ? {startPeriod: persisted.startPeriod, endPeriod: persisted.endPeriod}
      : null);
}

function persistedNamespacesForWiki(persisted, wiki) {
  if (!persisted || !wiki) return null;
  return persisted.namespacesByWiki?.[wiki]
    ?? (persisted.wiki === wiki ? persisted.namespaces : null);
}

/**
 * Build a human-readable description of the active filters.
 */
export function describeFilters({wiki, userTypes, granularity, startPeriod, endPeriod, namespaces}) {
  const wikiName = wiki ?? "all wikis";
  const types = userTypes == null ? null : (userTypes.length ? userTypes.join(", ") : "no user types");
  const gran = granularity ?? "month";
  const period = `${startPeriod ?? "start"} to ${endPeriod ?? "end"}`;
  const nsPart = namespaces
    ? (namespaces.length <= 3
      ? namespaces.map(n => nsLabel(n)).join(", ")
      : `${namespaces.length} namespaces`)
    : "all namespaces";
  const parts = [`Showing ${wikiName}`, types, `by ${gran}`, period, nsPart].filter(Boolean);
  return parts.join(" · ");
}

/**
 * Execute a grouped aggregation query entirely in DuckDB SQL.
 * Replaces the SELECT * → filterRows() → aggregateByPeriod() pattern.
 * Pushes all filtering, period grouping, and aggregation into SQL so DuckDB
 * can leverage column pruning on parquet (reads only needed column chunks)
 * and avoids serializing raw rows to the JS main thread.
 *
 * @param {DuckDBClient} db
 * @param {string} table - Table name (must match DuckDBClient.of key)
 * @param {Object} opts
 * @param {string[]} [opts.sumCols] - Columns to SUM
 * @param {string[]} [opts.avgCols] - Columns to AVG (e.g. gini, theil)
 * @param {string} opts.wiki
 * @param {string[]} opts.userTypes
 * @param {number[]|null} opts.namespaces - null to skip namespace filter
 * @param {string} opts.startPeriod
 * @param {string} opts.endPeriod
 * @param {string} opts.granularity - "month" | "quarter" | "year"
 */
export async function queryGrouped(db, table, {
  sumCols = [],
  avgCols = [],
  wiki, userTypes, namespaces, startPeriod, endPeriod, granularity
}) {
  const periodExpr = granularity === "year" ? "LEFT(year_month, 4)"
    : granularity === "quarter"
      ? "LEFT(year_month, 4) || '-Q' || CAST(CEIL(CAST(SUBSTRING(year_month, 6, 2) AS INTEGER) / 3.0) AS INTEGER)"
    : "year_month"

  const selects = [`${periodExpr} as period`]
  for (const c of sumCols) selects.push(`CAST(SUM("${c}") AS DOUBLE) as "${c}"`)
  for (const c of avgCols) selects.push(`CAST(AVG("${c}") AS DOUBLE) as "${c}"`)

  const conditions = [`wiki = '${wiki}'`]
  if (userTypes?.length)
    conditions.push(`user_type IN (${userTypes.map(t => `'${t}'`).join(",")})`)
  if (namespaces?.length)
    conditions.push(`page_namespace IN (${namespaces.join(",")})`)
  conditions.push(`year_month >= '${startPeriod}'`)
  conditions.push(`year_month <= '${endPeriod}'`)

  const sql = `SELECT ${selects.join(", ")} FROM "${table}" WHERE ${conditions.join(" AND ")} GROUP BY 1 ORDER BY 1`
  return Array.from(await db.query(sql))
}

/**
 * Check whether the current filter state matches the pre-computed defaults.
 * Used to decide whether to show instant defaults or query DuckDB.
 */
export function isDefaultView(filters, defaults, {defaultUserTypes = ["registered"], defaultGranularity = "year", defaultNamespaces = [0]} = {}) {
  const defaultWiki = resolveDefaultWiki(
    defaults.defaultWiki,
    defaults.wikis.map(d => d.wiki)
  )
  if (!defaultWiki) return false
  const range = defaults.rangeByWiki.find(d => d.wiki === defaultWiki)
  if (!range) return false
  const {wiki, userTypes, granularity, startPeriod, endPeriod, namespaces} = filters
  return wiki === defaultWiki
    && granularity === defaultGranularity
    && startPeriod === range.mn
    && endPeriod === range.mx
    && (userTypes == null
      ? defaultUserTypes == null || defaultUserTypes.length === 0
      : defaultUserTypes != null
        && userTypes.length === defaultUserTypes.length
        && defaultUserTypes.every(t => userTypes.includes(t)))
    && (namespaces == null
      ? defaultNamespaces == null
      : defaultNamespaces != null
        && namespaces.length === defaultNamespaces.length
        && defaultNamespaces.every(n => namespaces.includes(n)))
}

/**
 * Parse metadata from a defaults JSON object into the format expected by createFilterBar.
 */
export function parseDefaultsMeta(defaults) {
  const wikis = defaults.wikis.map(d => d.wiki)
  let nsByWiki = null
  if (defaults.nsByWiki) {
    nsByWiki = new Map()
    for (const {wiki, page_namespace} of defaults.nsByWiki) {
      if (!nsByWiki.has(wiki)) nsByWiki.set(wiki, [])
      nsByWiki.get(wiki).push(page_namespace)
    }
  }
  const rangeByWiki = new Map(
    defaults.rangeByWiki.map(d => [d.wiki, {mn: d.mn, mx: d.mx}])
  )
  const resolvedDefaultWiki = resolveDefaultWiki(defaults.defaultWiki, wikis)
  return {
    wikis,
    nsByWiki,
    rangeByWiki,
    defaultWiki: resolvedDefaultWiki,
    maxMonth: deriveMaxMonth(rangeByWiki, defaults.maxMonth ?? null)
  }
}

/**
 * Create a compound filter bar input for Observable Framework pages.
 * Returns a DOM element with a .value property and "input" event dispatching.
 * Use with view() to create reactive bindings:
 *   const filters = view(createFilterBar({wikis, nsByWiki, rangeByWiki}))
 *
 * @param {Object} options
 * @param {string[]} options.wikis - Available wiki names
 * @param {Map<string, number[]>} options.nsByWiki - Namespace IDs per wiki (null to disable)
 * @param {Map<string, {mn: string, mx: string}>} options.rangeByWiki - Date range per wiki
 * @param {string|null} [options.maxMonth=null]
 * @param {string|null} [options.defaultWiki=null]
 * @param {string[]} [options.defaultUserTypes=["registered"]]
 * @param {string} [options.defaultGranularity="year"]
 * @param {number[]} [options.defaultNamespaces=[0]]
 * @param {boolean} [options.showNamespaces=true]
 * @param {{key: string, input: Element}[]} [options.extraInputs=[]] - Extra inputs for the filters row
 */
export function createFilterBar({
  wikis,
  nsByWiki = null,
  rangeByWiki,
  maxMonth = null,
  defaultWiki = null,
  defaultUserTypes = ["registered"],
  defaultGranularity = "year",
  defaultNamespaces = [0],
  showNamespaces = true,
  showUserTypes = true,
  extraInputs = [],
}) {
  const persisted = readPersistedFilters();
  const resolvedDefaultWiki = resolveDefaultWiki(defaultWiki, wikis);
  const initWiki = wikis.includes(persisted?.wiki) ? persisted.wiki : resolvedDefaultWiki;
  const derivedMaxMonth = deriveMaxMonth(rangeByWiki, maxMonth);
  const defaultRange = rangeByWiki.get(initWiki) ?? fallbackRange(rangeByWiki, derivedMaxMonth);
  const initialRange = normalizeRangeSelection(persistedRangeForWiki(persisted, initWiki), defaultRange);
  const initialUserTypes = showUserTypes
    ? normalizeAllowedSelection(persisted?.userTypes, USER_TYPE_OPTIONS, defaultUserTypes)
    : null;
  const initialGranularity = GRANULARITY_OPTIONS.includes(persisted?.granularity)
    ? persisted.granularity
    : defaultGranularity;

  const wikiInput = Inputs.select(wikis, {label: "Wiki", value: initWiki});
  const userTypesInput = showUserTypes ? Inputs.checkbox(
    USER_TYPE_OPTIONS,
    {label: "User types", value: initialUserTypes}
  ) : null;
  const granularityInput = Inputs.radio(
    GRANULARITY_OPTIONS,
    {label: "Time scale", value: initialGranularity}
  );

  const startInput = Inputs.text({label: "From", value: initialRange.startPeriod, placeholder: "YYYY-MM"});
  const startEl = startInput.querySelector("input");
  startEl.maxLength = 7;
  startEl.pattern = "\\d{4}-\\d{2}";
  startEl.size = 7;
  const endInput = Inputs.text({label: "To", value: initialRange.endPeriod, placeholder: "YYYY-MM"});
  const endEl = endInput.querySelector("input");
  endEl.maxLength = 7;
  endEl.pattern = "\\d{4}-\\d{2}";
  endEl.size = 7;

  // Namespace checkbox (only when namespaces are shown)
  let nsInput = null;
  let nsSummary = null;
  if (showNamespaces && nsByWiki) {
    const initNs = nsByWiki.get(initWiki) ?? [];
    const defNs = normalizeNamespaces(
      persistedNamespacesForWiki(persisted, initWiki),
      initNs,
      defaultNamespaces
    );
    nsInput = Inputs.checkbox(initNs, {
      label: "Namespaces",
      value: defNs,
      format: nsLabel
    });
  }

  for (const {key, input} of extraInputs) {
    const persistedValue = persisted?.extra?.[key];
    if (persistedValue === undefined) continue;
    try {
      input.value = persistedValue;
    } catch {
      // Ignore input types that do not expose writable value setters.
    }
    const checkbox = input.querySelector?.("input[type=checkbox]");
    if (checkbox && typeof persistedValue === "boolean") checkbox.checked = persistedValue;
  }

  // Compound value getter
  function getValue() {
    const v = {
      wiki: wikiInput.value,
      userTypes: userTypesInput ? userTypesInput.value : null,
      granularity: granularityInput.value,
      startPeriod: startInput.value,
      endPeriod: endInput.value,
      namespaces: nsInput ? nsInput.value : null,
    };
    for (const {key, input} of extraInputs) v[key] = input.value;
    return v;
  }

  // Filter description (updated on every change)
  const descEl = html`<p class="filter-desc"></p>`;
  function updateDesc() {
    descEl.textContent = describeFilters(getValue());
  }

  // Namespace row: accordion + select/clear buttons
  let nsRow = null;
  if (nsInput) {
    nsSummary = html`<summary></summary>`;
    const updateNsSummary = () => {
      const allNs = nsByWiki.get(wikiInput.value) ?? [];
      const sel = nsInput ? nsInput.value : [];
      nsSummary.textContent = `Namespaces (${sel.length} of ${allNs.length} selected)`;
    };
    updateNsSummary();

    const details = html`<details>${nsSummary}${nsInput}</details>`;
    const selectAll = html`<a href="#">Select all</a>`;
    selectAll.onclick = (e) => {
      e.preventDefault();
      nsInput.querySelectorAll("input[type=checkbox]").forEach(c => { c.checked = true; });
      nsInput.dispatchEvent(new Event("input", {bubbles: true}));
    };
    const clearAll = html`<a href="#">Clear all</a>`;
    clearAll.onclick = (e) => {
      e.preventDefault();
      nsInput.querySelectorAll("input[type=checkbox]").forEach(c => { c.checked = false; });
      nsInput.dispatchEvent(new Event("input", {bubbles: true}));
    };

    nsRow = html`<div class="ns-row">${details}${html`<span class="ns-actions">${selectAll} ${clearAll}</span>`}</div>`;
    nsInput.addEventListener("input", updateNsSummary);
  }

  // Assemble layout
  const dateRange = html`<span class="date-range">${startInput}${endInput}</span>`;
  const extraEls = extraInputs.map(({input}) => input);
  const filtersRow = html`<div class="filters-row">${wikiInput}${userTypesInput || ""}${granularityInput}${dateRange}${extraEls}</div>`;
  const container = html`<div class="filters-bar">${filtersRow}${nsRow}${descEl}</div>`;

  // Expose compound value
  Object.defineProperty(container, "value", {get: getValue, enumerable: true});

  const dispatch = () => {
    updateDesc();
    const value = getValue();
    const existing = readPersistedFilters() ?? {};
    const persistedRanges = {
      ...(existing.rangeByWiki ?? {}),
      [value.wiki]: {
        startPeriod: value.startPeriod,
        endPeriod: value.endPeriod
      }
    };
    const persistedNamespaces = {
      ...(existing.namespacesByWiki ?? {}),
      ...(value.namespaces != null ? {[value.wiki]: value.namespaces} : {})
    };
    const extra = {...(existing.extra ?? {})};
    for (const {key, input} of extraInputs) extra[key] = input.value;
    writePersistedFilters({
      wiki: value.wiki,
      userTypes: value.userTypes,
      granularity: value.granularity,
      startPeriod: value.startPeriod,
      endPeriod: value.endPeriod,
      namespaces: value.namespaces,
      rangeByWiki: persistedRanges,
      namespacesByWiki: persistedNamespaces,
      extra
    });
    container.dispatchEvent(new Event("input", {bubbles: true}));
  };

  // Forward sub-input events to compound dispatch
  for (const el of [userTypesInput, granularityInput, startInput, endInput].filter(Boolean)) {
    el.addEventListener("input", dispatch);
  }
  if (nsInput) nsInput.addEventListener("input", dispatch);
  for (const {input} of extraInputs) input.addEventListener("input", dispatch);

  // Wiki change: update date range and rebuild namespace checkboxes
  wikiInput.addEventListener("input", () => {
    const w = wikiInput.value;
    const currentPersisted = readPersistedFilters();

    // Update date range
    const r = rangeByWiki.get(w) ?? fallbackRange(rangeByWiki, derivedMaxMonth);
    const storedRange = normalizeRangeSelection(persistedRangeForWiki(currentPersisted, w), r);
    const si = startInput.querySelector("input");
    const ei = endInput.querySelector("input");
    if (si) { si.value = storedRange.startPeriod; startInput.value = storedRange.startPeriod; }
    if (ei) { ei.value = storedRange.endPeriod; endInput.value = storedRange.endPeriod; }

    // Rebuild namespace checkboxes for the new wiki
    if (nsInput && nsByWiki) {
      const newNs = nsByWiki.get(w) ?? [];
      const defNs = normalizeNamespaces(
        persistedNamespacesForWiki(currentPersisted, w),
        newNs,
        defaultNamespaces
      );
      const newNsInput = Inputs.checkbox(newNs, {
        label: "Namespaces",
        value: defNs,
        format: nsLabel
      });
      nsInput.replaceWith(newNsInput);
      nsInput = newNsInput;
      nsInput.addEventListener("input", () => {
        const allNs = nsByWiki.get(wikiInput.value) ?? [];
        nsSummary.textContent = `Namespaces (${nsInput.value.length} of ${allNs.length} selected)`;
        dispatch();
      });
      const allNs = nsByWiki.get(w) ?? [];
      nsSummary.textContent = `Namespaces (${nsInput.value.length} of ${allNs.length} selected)`;
    }

    dispatch();
  });

  // Set initial description
  updateDesc();

  // Reparent into <main> so position:sticky works (Observable wraps each
  // cell in a small .observablehq--block div; sticky only sticks within its parent).
  requestAnimationFrame(() => {
    const block = container.closest(".observablehq--block");
    const main = container.closest("main");
    if (block && main) {
      main.insertBefore(container, block);
      block.style.display = "none";
    }
  });

  return container;
}
