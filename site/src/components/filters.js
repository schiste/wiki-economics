import * as Inputs from "npm:@observablehq/inputs";
import {html} from "npm:htl@1.0.0";
import {
  ALL_WIKIS,
  detailWikis,
  formatWiki,
  matchesDefaultSelection,
  wikiMatches,
} from "./wiki-scope.js";
export {activityTierLabels, activityTierMonths} from "./activity-tiers.js";
export {makeRowsLoader} from "./browser-data.js";
export {
  ALL_WIKIS,
  aggregateActivityByPeriod,
  aggregateChurn,
  aggregateCohorts,
  aggregateFunnel,
  aggregateGdpByPeriod,
  aggregateInequalityByPeriod,
  aggregatePatrolByPeriod,
  formatWiki,
  isAllWikis,
  wikiMatches,
} from "./wiki-scope.js";

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
 * Return a wrapped version of fn that only runs after `wait` ms have
 * passed without another call — collapses a burst of events (e.g. keystrokes)
 * into a single trailing invocation.
 */
export function debounce(fn, wait) {
  let timer;
  return (...args) => {
    clearTimeout(timer);
    timer = setTimeout(() => fn(...args), wait);
  };
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
 * Lazily load a single JSON FileAttachment, memoizing the in-flight promise
 * so multiple cells that need it in the same tick share one fetch.
 */
export function makeJsonLoader(file) {
  let promise = null;
  return function loadJson() {
    if (!promise) {
      promise = file.json();
    }
    return promise;
  };
}

/**
 * Filter rows by wiki, user types, namespaces, and period range, then add a
 * `period` key. A null/empty userTypes or namespaces skips that filter
 * (shows everything); wiki is always matched exactly when provided.
 */
export function filterRows(rows, {wiki, userTypes, namespaces, startPeriod, endPeriod, granularity}) {
  return rows
    .filter(d =>
      (wiki == null || wikiMatches(d, wiki)) &&
      (!userTypes?.length || userTypes.includes(d.user_type)) &&
      d.year_month >= startPeriod &&
      d.year_month <= endPeriod &&
      (!namespaces?.length || namespaces.includes(d.page_namespace))
    )
    .map(d => ({...d, period: toPeriod(d.year_month, granularity)}));
}

/**
 * Aggregate filtered rows by period: SUM the given sumCols, AVG the given
 * avgCols (skipping nulls in the average, matching SQL AVG semantics).
 */
export function aggregateByPeriod(rows, {sumCols = [], avgCols = []} = {}) {
  const map = new Map();
  for (const row of rows) {
    const key = row.period;
    if (!map.has(key)) {
      const entry = {period: key};
      for (const c of sumCols) entry[c] = 0;
      for (const c of avgCols) entry[c] = {sum: 0, count: 0};
      map.set(key, entry);
    }
    const entry = map.get(key);
    for (const c of sumCols) entry[c] += (row[c] ?? 0);
    for (const c of avgCols) {
      if (row[c] != null) {
        entry[c].sum += row[c];
        entry[c].count += 1;
      }
    }
  }
  const result = Array.from(map.values()).sort((a, b) => a.period < b.period ? -1 : 1);
  for (const entry of result) {
    for (const c of avgCols) entry[c] = entry[c].count > 0 ? entry[c].sum / entry[c].count : null;
  }
  return result;
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
 * Debounces removal via setTimeout (not requestAnimationFrame) so dependent
 * cells don't cause a flash — rAF callbacks are suspended entirely while the
 * document is hidden (backgrounded tab), which would otherwise leave the
 * loading indicator stuck until the tab regains visibility.
 *
 * The progress bar's stage reflects the actual code path a caller is about
 * to take (`useDefaults`), not a fake timer: "start" for precomputed
 * defaults (fast), "query" when a cell instead has to decode the full
 * Parquet dataset and aggregate it in-browser.
 */
let _loadingCount = 0;
let _loadingTimer = 0;
let _progressEls = null;

const STAGES = {
  start: {pct: 15, text: "Loading…", trickle: false},
  query: {pct: 40, text: "Loading data…", trickle: true},
  done: {pct: 100, text: "", trickle: false},
};

function ensureProgressUI() {
  if (_progressEls) return _progressEls;
  const wrap = document.createElement("div");
  wrap.className = "wk-progress";
  const bar = document.createElement("div");
  bar.className = "wk-progress-bar";
  wrap.appendChild(bar);
  const status = document.createElement("div");
  status.className = "wk-progress-status";
  document.body.append(wrap, status);
  _progressEls = {bar, status};
  return _progressEls;
}

function setStage(stage) {
  const {bar, status} = ensureProgressUI();
  const s = STAGES[stage];
  bar.classList.toggle("wk-progress-trickle", !!s.trickle);
  bar.style.width = s.pct + "%";
  status.textContent = s.text;
  document.body.dataset.wkStage = stage;
}

/**
 * @param {boolean} useDefaults - whether this load will use precomputed
 *   defaults (fast) or run a live in-browser query over Parquet data (slower).
 */
export function startLoading(useDefaults = true) {
  _loadingCount++;
  clearTimeout(_loadingTimer);
  document.body.classList.add("wk-loading");
  setStage(useDefaults ? "start" : "query");
}

export function doneLoading() {
  _loadingCount = Math.max(0, _loadingCount - 1);
  if (_loadingCount === 0) {
    setStage("done");
    _loadingTimer = setTimeout(() => {
      if (_loadingCount === 0) {
        document.body.classList.remove("wk-loading");
        if (_progressEls) _progressEls.bar.classList.remove("wk-progress-trickle");
      }
    }, 0);
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

/**
 * Read filter overrides from the page's URL query string, so a shared link
 * reproduces the exact view it was copied from. Only keys actually present
 * in the URL are returned (sparse), so callers can layer this over persisted
 * localStorage state without clobbering fields the link didn't specify.
 */
function readUrlFilters(extraKeys = []) {
  if (typeof location === "undefined") return null;
  const params = new URLSearchParams(location.search);
  if (![...params.keys()].length) return null;
  const out = {};
  if (params.has("wiki")) out.wiki = params.get("wiki");
  if (params.has("types")) out.userTypes = params.get("types") ? params.get("types").split(",") : [];
  if (params.has("gran")) out.granularity = params.get("gran");
  if (params.has("start")) out.startPeriod = params.get("start");
  if (params.has("end")) out.endPeriod = params.get("end");
  if (params.has("ns")) out.namespaces = params.get("ns") ? params.get("ns").split(",").map(Number) : [];
  const extra = {};
  for (const key of extraKeys) {
    if (!params.has(key)) continue;
    const raw = params.get(key);
    extra[key] = raw === "true" ? true : raw === "false" ? false : raw;
  }
  if (Object.keys(extra).length) out.extra = extra;
  return out;
}

/**
 * Layer URL-provided filter overrides on top of persisted localStorage
 * state: the URL wins field-by-field (so a partial link like `?wiki=nlwiki`
 * only overrides wiki, leaving everything else as the visitor last left it).
 */
function mergeFilterSources(base, override) {
  if (!override) return base ?? {};
  const merged = {...(base ?? {}), ...override};
  if (override.extra) merged.extra = {...(base?.extra ?? {}), ...override.extra};
  return merged;
}

/**
 * Write the full current filter state into the URL query string via
 * replaceState, so the address bar always reflects a shareable snapshot of
 * the view — including on first load, before the user has touched anything.
 * Uses replaceState (not pushState) so every filter tweak doesn't pollute
 * browser history.
 */
function writeUrlFilters(value, extraKeys = []) {
  if (typeof history === "undefined" || typeof location === "undefined") return;
  const params = new URLSearchParams();
  if (value.wiki != null) params.set("wiki", value.wiki);
  if (value.userTypes != null) params.set("types", value.userTypes.join(","));
  if (value.granularity != null) params.set("gran", value.granularity);
  if (value.startPeriod) params.set("start", value.startPeriod);
  if (value.endPeriod) params.set("end", value.endPeriod);
  if (value.namespaces != null) params.set("ns", value.namespaces.join(","));
  for (const key of extraKeys) {
    if (value[key] !== undefined) params.set(key, String(value[key]));
  }
  const query = params.toString();
  const url = `${location.pathname}${query ? `?${query}` : ""}${location.hash}`;
  history.replaceState(history.state, "", url);
}

function resolveDefaultWiki(defaultWiki, wikis) {
  if (defaultWiki && wikis.includes(defaultWiki)) return defaultWiki;
  if (wikis.includes(ALL_WIKIS)) return ALL_WIKIS;
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
  const wikiName = wiki == null ? "all wikis" : formatWiki(wiki);
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
 * Grouped aggregation over an array of already-loaded rows: filters by wiki,
 * user types, namespaces, and period range (see filterRows), buckets by
 * period at the requested granularity, and sums/averages the given columns.
 *
 * @param {object[]} rows - Plain row objects (e.g. from makeRowsLoader)
 * @param {Object} opts
 * @param {string[]} [opts.sumCols] - Columns to SUM
 * @param {string[]} [opts.avgCols] - Columns to AVG (e.g. gini, theil)
 * @param {string} opts.wiki
 * @param {string[]} opts.userTypes
 * @param {number[]|null} opts.namespaces - null/empty to skip namespace filter
 * @param {string} opts.startPeriod
 * @param {string} opts.endPeriod
 * @param {string} opts.granularity - "month" | "quarter" | "year"
 */
export function queryGrouped(rows, {
  sumCols = [],
  avgCols = [],
  wiki, userTypes, namespaces, startPeriod, endPeriod, granularity
}) {
  const filtered = filterRows(rows, {wiki, userTypes, namespaces, startPeriod, endPeriod, granularity});
  return aggregateByPeriod(filtered, {sumCols, avgCols});
}

/**
 * Check whether the current filter state matches the pre-computed defaults.
 * Used to decide whether to show instant defaults or query DuckDB.
 */
export function isDefaultView(filters, defaults, {defaultUserTypes = ["registered"], defaultGranularity = "year", defaultNamespaces = [0]} = {}) {
  const parsed = parseDefaultsMeta(defaults)
  const defaultWiki = parsed.defaultWiki
  // The Rust defaults intentionally describe the all-wiki portfolio. Detail
  // pages expose concrete wikis only, so they must query the selected wiki
  // instead of mistaking the portfolio payload for that wiki's fast path.
  if (!defaultWiki || defaults.defaultWiki !== defaultWiki) return false
  return matchesDefaultSelection(filters, {
    wiki: defaultWiki,
    range: parsed.defaultRange ?? parsed.rangeByWiki.get(defaultWiki),
    userTypes: defaultUserTypes,
    granularity: defaultGranularity,
    namespaces: defaultNamespaces,
  })
}

/**
 * Parse metadata from a defaults JSON object into the format expected by createFilterBar.
 */
export function parseDefaultsMeta(defaults) {
  const wikis = detailWikis(defaults.wikis.map(d => d.wiki))
  const wikiSet = new Set(wikis)
  let nsByWiki = null
  if (defaults.nsByWiki) {
    nsByWiki = new Map()
    for (const {wiki, page_namespace} of defaults.nsByWiki) {
      if (!wikiSet.has(wiki)) continue
      if (!nsByWiki.has(wiki)) nsByWiki.set(wiki, [])
      nsByWiki.get(wiki).push(page_namespace)
    }
  }
  const rangeByWiki = new Map(
    defaults.rangeByWiki
      .filter(d => wikiSet.has(d.wiki))
      .map(d => [d.wiki, {mn: d.mn, mx: d.mx}])
  )
  const resolvedDefaultWiki = resolveDefaultWiki(defaults.defaultWiki, wikis)
  const sourceDefaultRange = defaults.defaultRange
  const sourceRangeIsApplicable = defaults.defaultWiki === resolvedDefaultWiki
  const defaultRange = sourceRangeIsApplicable
    && sourceDefaultRange
    && isValidPeriod(sourceDefaultRange.mn)
    && isValidPeriod(sourceDefaultRange.mx)
    && sourceDefaultRange.mn <= sourceDefaultRange.mx
    ? {mn: sourceDefaultRange.mn, mx: sourceDefaultRange.mx}
    : rangeByWiki.get(resolvedDefaultWiki) ?? null
  return {
    wikis,
    nsByWiki,
    rangeByWiki,
    defaultWiki: resolvedDefaultWiki,
    defaultRange,
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
 * @param {{mn: string, mx: string}|null} [options.defaultRange=null]
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
  defaultRange = null,
  defaultUserTypes = ["registered"],
  defaultGranularity = "year",
  defaultNamespaces = [0],
  showNamespaces = true,
  showUserTypes = true,
  extraInputs = [],
}) {
  const extraKeys = extraInputs.map(({key}) => key);
  const persisted = mergeFilterSources(readPersistedFilters(), readUrlFilters(extraKeys));
  const resolvedDefaultWiki = resolveDefaultWiki(defaultWiki, wikis);
  const initWiki = wikis.includes(persisted?.wiki) ? persisted.wiki : resolvedDefaultWiki;
  const derivedMaxMonth = deriveMaxMonth(rangeByWiki, maxMonth);
  const sourceRange = rangeByWiki.get(initWiki) ?? fallbackRange(rangeByWiki, derivedMaxMonth);
  const initialDefaultRange = initWiki === resolvedDefaultWiki && defaultRange ? defaultRange : sourceRange;
  const initialRange = normalizeRangeSelection(persistedRangeForWiki(persisted, initWiki), initialDefaultRange);
  const initialUserTypes = showUserTypes
    ? normalizeAllowedSelection(persisted?.userTypes, USER_TYPE_OPTIONS, defaultUserTypes)
    : null;
  const initialGranularity = GRANULARITY_OPTIONS.includes(persisted?.granularity)
    ? persisted.granularity
    : defaultGranularity;

  const wikiInput = Inputs.select(wikis, {label: "Wiki", value: initWiki, format: formatWiki});
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
    writeUrlFilters(value, extraKeys);
    container.dispatchEvent(new Event("input", {bubbles: true}));
  };

  // Forward sub-input events to compound dispatch. The From/To fields are
  // free-text typing (not a click), so each keystroke would otherwise trigger
  // a full re-query (a live in-browser aggregation on the slow path) until
  // the value settles — debounce those two so only the pause-after-typing fires.
  for (const el of [userTypesInput, granularityInput].filter(Boolean)) {
    el.addEventListener("input", dispatch);
  }
  const debouncedDispatch = debounce(dispatch, 300);
  for (const el of [startInput, endInput]) {
    el.addEventListener("input", debouncedDispatch);
  }
  if (nsInput) nsInput.addEventListener("input", dispatch);
  for (const {input} of extraInputs) input.addEventListener("input", dispatch);

  // "Break down by user type" toggle: select every user type while it's on,
  // and revert to the default selection when it's switched back off.
  const breakdownEntry = userTypesInput ? extraInputs.find(({key}) => key === "breakdown") : null;
  if (breakdownEntry) {
    breakdownEntry.input.addEventListener("input", () => {
      const nextTypes = breakdownEntry.input.value ? USER_TYPE_OPTIONS : defaultUserTypes;
      // Inputs.checkbox gives each box a positional index as its DOM value
      // (not the option string), so match against USER_TYPE_OPTIONS by index.
      userTypesInput.querySelectorAll("input[type=checkbox]").forEach((c, i) => {
        c.checked = nextTypes.includes(USER_TYPE_OPTIONS[i]);
      });
      userTypesInput.dispatchEvent(new Event("input", {bubbles: true}));
    });
  }

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

  // Set initial description and make the first-paint state shareable
  updateDesc();
  writeUrlFilters(getValue(), extraKeys);

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
