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
  assert.match(head, /for\(var menuLineIndex=0;menuLineIndex<3;menuLineIndex\+\+\)/);
  assert.match(head, /menuLine\.className="mobile-menu-line"/);
  assert.match(head, /masthead\.appendChild\(menuButton\)/);
  assert.doesNotMatch(head, /document\.body\.insertBefore\(menuButton,center\)/);
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

test("small-screen CSS presents a conventional masthead hamburger with an accessible hit target", () => {
  assert.match(style, /\.mobile-masthead,\s*\.mobile-menu-button,\s*\.mobile-nav-toolbar \{\s*display: none;/);
  assert.match(style, /@media \(max-width: 1007px\)/);
  assert.match(style, /\.mobile-masthead \{[\s\S]*?display: grid;/);
  assert.match(style, /--wk-mobile-menu-surface: color-mix/);
  assert.match(style, /\.mobile-masthead \{[\s\S]*?left: 0;[\s\S]*?width: 100vw;[\s\S]*?max-width: 100vw;[\s\S]*?grid-template-columns: minmax\(0, 1fr\) auto;/);
  assert.match(style, /\.mobile-menu-button \{[\s\S]*?width: 44px;[\s\S]*?height: 44px;[\s\S]*?min-height: 44px;[\s\S]*?display: inline-grid;[\s\S]*?background: transparent;[\s\S]*?border: 0;[\s\S]*?box-shadow: none;/);
  assert.match(style, /\.mobile-menu-icon \{[\s\S]*?width: 1\.25rem;[\s\S]*?height: 0\.875rem;[\s\S]*?flex-direction: column;/);
  assert.match(style, /\.mobile-menu-line \{[\s\S]*?height: 2px;[\s\S]*?background: currentColor;/);
  assert.doesNotMatch(style, /--wk-mobile-menu-visual-width/);
  assert.doesNotMatch(style, /\.mobile-menu-word/);
  assert.match(style, /#observablehq-sidebar \{[\s\S]*?background: var\(--wk-mobile-menu-surface\);/);
  assert.doesNotMatch(style, /margin-left: 3rem/);
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
