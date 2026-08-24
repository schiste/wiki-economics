// Shared footer markup for observablehq.config.js. Framework applies this
// same HTML to every page via the global `footer` config property (see
// getHtml() in @observablehq/framework/dist/markdown.js) unless a page's
// frontmatter sets its own `footer:` — none of the pages in src/ do that, so
// this renders identically across the whole site.
export const siteFooter = `<div class="site-footer">
<p><strong>Wiki Economics</strong> applies economic indicators to Wikipedia activity data. An independent project by Christophe Henner, not endorsed by the Wikimedia Foundation. No analytics, cookies, or trackers — runs entirely on <a href="https://wikitech.wikimedia.org/wiki/Portal:Toolforge">Wikimedia Toolforge</a> from public <a href="https://dumps.wikimedia.org/">Wikimedia data dumps</a>.</p>
<div class="site-footer-links">
<a href="https://meta.wikimedia.org/wiki/Next_25/Wiki_Economics">Meta-Wiki</a>
<a href="https://github.com/schiste/wiki-economics">GitHub</a>
<a href="/legal">Legal &amp; MIT license</a>
</div>
</div>`;
