import {html} from "npm:htl";

/**
 * Convert an array of objects to a CSV string.
 */
function toCSV(data) {
  if (!data || data.length === 0) return "";
  const cols = Object.keys(data[0]);
  const header = cols.join(",");
  const rows = data.map(d => cols.map(c => {
    const v = d[c];
    if (v == null) return "";
    const s = String(v);
    return (s.includes(",") || s.includes('"') || s.includes("\n"))
      ? '"' + s.replace(/"/g, '""') + '"'
      : s;
  }).join(","));
  return [header, ...rows].join("\n");
}

function triggerDownload(blob, filename) {
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
}

/**
 * Inline computed styles from source SVG onto a clone so it renders
 * correctly when serialized (CSS variables won't resolve otherwise).
 */
function inlineStyles(source, clone) {
  const props = ["fill", "stroke", "color", "opacity", "font-family", "font-size",
    "font-weight", "stroke-width", "stroke-dasharray", "stroke-opacity",
    "fill-opacity", "text-anchor", "dominant-baseline"];
  const srcEls = source.querySelectorAll("*");
  const cloneEls = clone.querySelectorAll("*");
  for (let i = 0; i < srcEls.length && i < cloneEls.length; i++) {
    const cs = getComputedStyle(srcEls[i]);
    for (const p of props) {
      const v = cs.getPropertyValue(p);
      if (v) cloneEls[i].style.setProperty(p, v);
    }
  }
}

async function svgToPNG(svgEl) {
  const rect = svgEl.getBoundingClientRect();
  const clone = svgEl.cloneNode(true);
  clone.setAttribute("width", rect.width);
  clone.setAttribute("height", rect.height);
  clone.setAttribute("xmlns", "http://www.w3.org/2000/svg");
  inlineStyles(svgEl, clone);

  const svgStr = new XMLSerializer().serializeToString(clone);
  const svgBlob = new Blob([svgStr], {type: "image/svg+xml;charset=utf-8"});
  const url = URL.createObjectURL(svgBlob);

  const img = new Image();
  await new Promise((resolve, reject) => {
    img.onload = resolve;
    img.onerror = reject;
    img.src = url;
  });

  const scale = 2;
  const canvas = document.createElement("canvas");
  canvas.width = rect.width * scale;
  canvas.height = rect.height * scale;
  const ctx = canvas.getContext("2d");
  ctx.scale(scale, scale);
  ctx.fillStyle = "#ffffff";
  ctx.fillRect(0, 0, rect.width, rect.height);
  ctx.drawImage(img, 0, 0, rect.width, rect.height);
  URL.revokeObjectURL(url);

  return new Promise(resolve => canvas.toBlob(resolve, "image/png"));
}

/**
 * Print a single chart section as PDF, with note visible
 * and methodology details unfolded.
 */
function printChartSection(section) {
  // Open all <details> so methodology is visible in the PDF
  const details = section.querySelectorAll("details");
  const wasOpen = Array.from(details).map(d => d.open);
  details.forEach(d => { d.open = true; });

  // Mark the target for CSS print rules
  document.body.classList.add("wk-print-chart");
  section.classList.add("wk-print-target");

  const cleanup = () => {
    document.body.classList.remove("wk-print-chart");
    section.classList.remove("wk-print-target");
    details.forEach((d, i) => { d.open = wasOpen[i]; });
    window.removeEventListener("afterprint", cleanup);
  };
  window.addEventListener("afterprint", cleanup);
  window.print();
}

/**
 * Wrap a Plot element with export buttons (CSV + PNG + PDF).
 * The PDF button prints the parent .chart-section with note and methodology.
 * Usage: withExport(Plot.plot({...}), data, "chart-name")
 */
export function withExport(plot, data, name) {
  const wrap = html`<div class="chart-export-wrap"></div>`;
  const bar = html`<div class="chart-export-bar">
    <button class="export-btn" data-type="csv" title="Download data as CSV">CSV</button>
    <button class="export-btn" data-type="png" title="Download chart as PNG">PNG</button>
    <button class="export-btn" data-type="pdf" title="Print chart with methodology as PDF">PDF</button>
  </div>`;
  wrap.appendChild(plot);
  wrap.appendChild(bar);

  bar.querySelector('[data-type="csv"]').onclick = () => {
    triggerDownload(new Blob([toCSV(data)], {type: "text/csv"}), `${name}.csv`);
  };
  bar.querySelector('[data-type="png"]').onclick = async () => {
    const svg = wrap.querySelector("svg");
    if (!svg) return;
    const blob = await svgToPNG(svg);
    if (blob) triggerDownload(blob, `${name}.png`);
  };
  bar.querySelector('[data-type="pdf"]').onclick = () => {
    const section = wrap.closest(".chart-section");
    if (section) printChartSection(section);
  };

  return wrap;
}

/**
 * Page-level export bar with PDF (print) and per-dataset CSV buttons.
 * Usage: pageExportBar([{name: "inequality", data: ineqData}, ...])
 */
export function pageExportBar(datasets) {
  const bar = html`<div class="page-export-bar">
    <span class="page-export-label">Export</span>
    <button class="export-btn" data-action="pdf" title="Print / Save as PDF">PDF</button>
    ${datasets.map(({name}) =>
      html`<button class="export-btn" data-action="csv" data-name="${name}" title="Download ${name} data as CSV">CSV: ${name}</button>`
    )}
  </div>`;

  bar.querySelector('[data-action="pdf"]').onclick = () => window.print();
  bar.querySelectorAll('[data-action="csv"]').forEach((btn, i) => {
    btn.onclick = () => {
      const ds = datasets[i];
      triggerDownload(new Blob([toCSV(ds.data)], {type: "text/csv"}), `${ds.name}.csv`);
    };
  });

  return bar;
}
