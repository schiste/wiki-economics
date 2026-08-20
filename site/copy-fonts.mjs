#!/usr/bin/env node
// Observable Framework's build only copies files it can trace through HTML
// attributes or JS imports — it never parses url() references inside CSS,
// so the self-hosted @font-face fonts in src/style.css are invisible to it.
// Copy them into the built site verbatim after `observable build` runs.
import {mkdirSync, copyFileSync, readdirSync} from "node:fs";
import path from "node:path";

const distDir = process.env.WIKI_ECON_SITE_DIST_DIR
  ? path.resolve(process.env.WIKI_ECON_SITE_DIST_DIR)
  : "dist";

const srcDir = path.join("src", "fonts");
const destDir = path.join(distDir, "fonts");

mkdirSync(destDir, {recursive: true});
for (const file of readdirSync(srcDir)) {
  if (!file.endsWith(".woff2")) continue;
  copyFileSync(path.join(srcDir, file), path.join(destDir, file));
}
