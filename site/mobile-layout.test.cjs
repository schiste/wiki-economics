const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const {pathToFileURL} = require("node:url");
const {test} = require("node:test");

const siteRoot = __dirname;
const configPath = path.join(siteRoot, "observablehq.config.js");
const style = fs.readFileSync(path.join(siteRoot, "src", "style.css"), "utf8");
const variation = fs.readFileSync(path.join(siteRoot, "src", "edit-variation.md"), "utf8");

async function renderedHead() {
  const configUrl = `${pathToFileURL(configPath).href}?mobile-layout-test=${Date.now()}`;
  const config = (await import(configUrl)).default;
  return config.head();
}

test("mobile navigation exposes one semantic controller and close action", async () => {
  const head = await renderedHead();

  assert.match(head, /menuButton\.type="button"/);
  assert.match(head, /menuButton\.setAttribute\("aria-controls","observablehq-sidebar"\)/);
  assert.match(head, /menuButton\.setAttribute\("aria-expanded","false"\)/);
  assert.match(head, /menuButton\.setAttribute\("aria-label","Open navigation"\)/);
  assert.equal((head.match(/menuWord(?:Top|Bottom)\.className="mobile-menu-word"/g) || []).length, 2);
  assert.match(head, /menuIcon\.textContent="›"/);
  assert.match(head, /closeButton\.setAttribute\("aria-label","Close navigation"\)/);
  assert.match(head, /sidebar\.setAttribute\("aria-label","Primary navigation"\)/);
  assert.match(head, /sidebar\.insertBefore\(navToolbar,sidebar\.firstElementChild\.nextSibling\)/);
  assert.match(head, /activeLinks\.length\?activeLinks\[activeLinks\.length-1\]\.textContent:"Portfolio"/);
});

test("mobile navigation locks scrolling and manages focus deterministically", async () => {
  const head = await renderedHead();

  assert.match(head, /document\.documentElement\.classList\.add\("mobile-nav-open"\)/);
  assert.match(head, /document\.body\.style\.position="fixed"/);
  assert.match(head, /window\.scrollTo\(0,scrollLockY\)/);
  assert.match(head, /closeButton\.focus\(\{preventScroll:true\}\)/);
  assert.match(head, /menuButton\.focus\(\{preventScroll:true\}\)/);
  assert.match(head, /event\.key!=="Tab"/);
  assert.match(head, /sidebarToggle\.tabIndex=mobileQuery\.matches\?-1:0/);
});

test("small-screen CSS presents a discreet borderless rail with an accessible hit target", () => {
  assert.match(style, /\.mobile-masthead,\s*\.mobile-menu-button,\s*\.mobile-nav-toolbar \{\s*display: none;/);
  assert.match(style, /@media \(max-width: 1007px\)/);
  assert.match(style, /\.mobile-masthead \{[\s\S]*?display: grid;/);
  assert.match(style, /--wk-mobile-menu-surface: color-mix/);
  assert.match(style, /--wk-mobile-menu-visual-width: 30px;/);
  assert.match(style, /\.mobile-menu-button \{[\s\S]*?position: fixed;[\s\S]*?width: 44px;[\s\S]*?min-height: 8rem;[\s\S]*?background: transparent;[\s\S]*?border: 0;[\s\S]*?box-shadow: none;/);
  assert.match(style, /\.mobile-menu-button::before \{[\s\S]*?width: var\(--wk-mobile-menu-visual-width\);[\s\S]*?background: var\(--wk-mobile-menu-surface\);/);
  assert.match(style, /\.mobile-menu-word \{[\s\S]*?writing-mode: vertical-rl;[\s\S]*?text-orientation: upright;/);
  assert.match(style, /#observablehq-sidebar \{[\s\S]*?background: var\(--wk-mobile-menu-surface\);/);
  assert.match(style, /#observablehq-center \{[\s\S]*?margin-left: 3rem;/);
  assert.match(style, /#observablehq-sidebar-toggle \{[\s\S]*?clip-path: inset\(50%\);/);
  assert.match(style, /#observablehq-sidebar \{[\s\S]*?width: min\(20rem, 86vw\);/);
  assert.match(style, /#observablehq-sidebar-backdrop \{[\s\S]*?z-index: 900;/);
  assert.match(style, /#observablehq-sidebar \.observablehq-link a,[\s\S]*?min-height: 44px;/);
  assert.match(style, /\.filters-bar \{[\s\S]*?top: var\(--wk-mobile-masthead-height\);/);
  assert.match(style, /\.filters-toggle \{[\s\S]*?min-width: 44px;[\s\S]*?min-height: 44px;/);
});

test("320px layouts wrap filters and bound wide content locally", () => {
  assert.match(style, /form:has\(input\[type="checkbox"\]\):not\(:has\(> input\[type="checkbox"\]\)\) > div \{[\s\S]*?flex-wrap: wrap;/);
  assert.match(style, /@media \(max-width: 360px\)[\s\S]*?\.portfolio-concentration,[\s\S]*?min-width: 0;/);
  assert.match(style, /\.portfolio-theil \{[\s\S]*?flex-wrap: wrap;/);
  assert.match(variation, /html`<div class="wide-table"><table>/);
  assert.match(variation, /<\/table><\/div>`/);
});
