// Shared footer markup for observablehq.config.js. Framework applies this
// same HTML to every page via the global `footer` config property (see
// getHtml() in @observablehq/framework/dist/markdown.js) unless a page's
// frontmatter sets its own `footer:` — none of the pages in src/ do that, so
// this renders identically across the whole site.
export const siteFooter = `<div class="site-footer">
<div class="site-footer-vision">
<h3>About</h3>
<p><strong>Wiki Economics</strong> applies economic indicators — GDP, labor statistics, inequality metrics, quality control — to Wikipedia activity data, surfacing structural patterns that are hard to see in raw edit logs. An independent project created by Christophe Henner.</p>
</div>
<div class="site-footer-links">
<h3>Links</h3>
<a href="https://meta.wikimedia.org/wiki/Next_25/Wiki_Economics">Project page on Meta-Wiki</a>
<a href="https://github.com/schiste/wiki-economics">Source on GitHub</a>
<a href="/legal">MIT license</a>
</div>
<div class="site-footer-privacy">
<h3>Privacy</h3>
<span class="privacy-pill">No tracking</span>
<p>No analytics, cookies, or trackers. Runs entirely on <a href="https://wikitech.wikimedia.org/wiki/Portal:Toolforge">Wikimedia Toolforge</a>, computed from public Wikimedia data dumps.</p>
</div>
</div>
<div class="legal-footer">
<span>Wiki Economics · <a href="/legal">MIT licensed</a></span>
<span>Uses public <a href="https://dumps.wikimedia.org/">Wikimedia data</a> · Independent and not endorsed by the Wikimedia Foundation</span>
</div>`;
