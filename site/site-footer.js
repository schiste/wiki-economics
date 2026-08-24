// Shared footer markup for observablehq.config.js. Framework applies this
// same HTML to every page via the global `footer` config property (see
// getHtml() in @observablehq/framework/dist/markdown.js) unless a page's
// frontmatter sets its own `footer:` (none of the pages in src/ do that), so
// this renders identically across the whole site. Content pages (indicators,
// staging) live in the sidebar; this footer is for project-level links only.
export const siteFooter = `<div class="site-footer">
<p><strong>Wiki Economics</strong> is an independent project by Christophe Henner that applies economic indicators to Wikipedia activity data. It is not affiliated with or endorsed by the Wikimedia Foundation. It sets no cookies and runs no analytics or trackers, drawing entirely on public <a href="https://dumps.wikimedia.org/">Wikimedia data dumps</a> and running on <a href="https://wikitech.wikimedia.org/wiki/Portal:Toolforge">Wikimedia Toolforge</a>.</p>
<div class="site-footer-links">
<a href="/legal" class="site-footer-legal">Legal</a>
<a href="/about">About</a>
<a href="https://meta.wikimedia.org/wiki/Next_25/Wiki_Economics">Meta-Wiki</a>
<a href="https://github.com/schiste/wiki-economics">GitHub</a>
</div>
</div>`;
