#!/usr/bin/env node
// Dev/operator API server for the admin page.
// Run alongside `npm run dev` or `scripts/dev.sh`.
// Production deployments should keep this server disabled.

const http = require("http");
const { execFileSync, spawn } = require("child_process");
const path = require("path");
const fs = require("fs");

const ROOT = path.resolve(__dirname, "..");
const RUNTIME_ENV = process.env.WIKI_ECON_ENV || "local";
const ADMIN_ENABLED = (process.env.WIKI_ECON_ADMIN_ENABLED ?? (RUNTIME_ENV === "production" ? "0" : "1")) === "1";
const PORT = Number.parseInt(process.env.WIKI_ECON_ADMIN_PORT || "3001", 10);
const SITE_PORT = Number.parseInt(process.env.WIKI_ECON_SITE_PORT || "3000", 10);
const DATA_DIR = resolveConfiguredPath("WIKI_ECON_DATA_DIR", "data");
const OUTPUT_DIR = resolveConfiguredPath("WIKI_ECON_OUTPUT_DIR", "output");
const GENERATOR_DIR = resolveConfiguredPath("WIKI_ECON_GENERATOR_DIR", path.join("site", "data-build"));
const DEFAULT_RUNNER = {
  program: "cargo",
  args: ["run", "--release", "--"],
  label: "cargo run --release --",
};
const ALLOWED_ORIGINS = resolveAllowedOrigins();

let currentJob = null;
let jobLog = [];
let jobExitCode = null;
let lastJob = null;
let lastWikiJobs = new Map();
let lastGlobalJob = null;
let manifestCache = null;
let manifestCacheAt = 0;
const MANIFEST_CACHE_TTL_MS = 1500;
const REQUIRED_MERGED_METRICS = 9;
let supportedWikisCache = null;

if (!ADMIN_ENABLED) {
  console.error("Admin API is disabled for this runtime. Set WIKI_ECON_ADMIN_ENABLED=1 to opt in.");
  process.exit(1);
}

function resolveConfiguredPath(envVar, fallback) {
  const value = process.env[envVar];
  if (!value) return path.resolve(ROOT, fallback);
  return path.isAbsolute(value) ? value : path.resolve(ROOT, value);
}

function resolveAllowedOrigins() {
  const configured = process.env.WIKI_ECON_ALLOWED_ORIGINS;
  if (!configured) {
    return new Set([
      `http://127.0.0.1:${SITE_PORT}`,
      `http://localhost:${SITE_PORT}`,
    ]);
  }
  return new Set(
    configured
      .split(",")
      .map((entry) => entry.trim())
      .filter(Boolean),
  );
}

function applyCors(req, res) {
  const origin = req.headers.origin;
  if (!origin) return;
  if (ALLOWED_ORIGINS.has("*")) {
    res.setHeader("Access-Control-Allow-Origin", "*");
    return;
  }
  if (ALLOWED_ORIGINS.has(origin)) {
    res.setHeader("Access-Control-Allow-Origin", origin);
    res.setHeader("Vary", "Origin");
  }
}

function resolveRunner() {
  const customBin = process.env.WIKI_ECON_BIN;
  if (customBin) {
    return {
      program: customBin,
      args: [],
      label: customBin,
    };
  }
  return DEFAULT_RUNNER;
}

function loadSupportedWikipedias() {
  if (supportedWikisCache) return supportedWikisCache;
  // Scrape the WIKIPEDIA_DATABASES constant from src/fetch.rs so the picker's
  // universe stays in lockstep with the Rust source. The CLI's actual
  // partitioning dispatch (yearly / all-time / monthly) lives elsewhere in
  // the same file; the picker shows the full set and lets the CLI surface
  // partitioning errors at fetch time for the rare cases where the dump
  // shape doesn't match the picker's offer.
  const fetchSourcePath = path.join(ROOT, "src", "fetch.rs");
  const source = fs.readFileSync(fetchSourcePath, "utf8");
  const match = source.match(/const WIKIPEDIA_DATABASES:\s*&\[&str\]\s*=\s*&\[(?<body>[\s\S]*?)\];/);
  if (!match?.groups?.body) return [];
  supportedWikisCache = Array.from(match.groups.body.matchAll(/"([^"]+)"/g), (entry) => entry[1]).sort();
  return supportedWikisCache;
}

function suggestedSnapshotVersion(now = new Date()) {
  const currentMonth = now.getUTCMonth();
  const year = currentMonth === 0 ? now.getUTCFullYear() - 1 : now.getUTCFullYear();
  const month = currentMonth === 0 ? 12 : currentMonth;
  return `${year}-${String(month).padStart(2, "0")}`;
}

function normalizeVersion(value) {
  const trimmed = typeof value === "string" ? value.trim() : "";
  return trimmed || null;
}

function isValidVersion(version) {
  return /^\d{4}-\d{2}$/.test(version);
}

function safeReadDir(dir) {
  try {
    return fs.readdirSync(dir);
  } catch {
    return [];
  }
}

function countFiles(dir, ext) {
  return safeReadDir(dir).filter(f => f.endsWith(ext)).length;
}

function countExisting(paths) {
  return paths.filter((entry) => fs.existsSync(entry)).length;
}

function setSyntheticJobLog(meta, lines, exitCode = 0) {
  const command = typeof meta === "string" ? meta : meta.command;
  const startedAt = new Date().toISOString().replace("T", " ").replace(/\.\d+Z$/, " UTC");
  jobLog = [`$ ${command}\nStarted: ${startedAt}\n`, ...lines.map((line) => line.endsWith("\n") ? line : `${line}\n`), `\n[exited with code ${exitCode}]`];
  jobExitCode = exitCode;
  const completedJob = {
    command,
    action: typeof meta === "string" ? null : (meta.action ?? null),
    wiki: typeof meta === "string" ? null : (meta.wiki ?? null),
    stage: typeof meta === "string" ? null : (meta.stage ?? meta.action?.replace("-", "_") ?? null),
    exitCode,
    running: false,
    log: [...jobLog],
    finishedAt: new Date().toISOString(),
  };
  lastJob = completedJob;
  if (completedJob.wiki) {
    lastWikiJobs.set(completedJob.wiki, completedJob);
  } else {
    lastGlobalJob = completedJob;
  }
  currentJob = null;
}

function refreshManifest(force = false) {
  const now = Date.now();
  if (!force && manifestCache && now - manifestCacheAt < MANIFEST_CACHE_TTL_MS) {
    return manifestCache;
  }

  const manifestScript = path.join(GENERATOR_DIR, "manifest.json.sh");
  const output = execFileSync("/bin/bash", [manifestScript], {
    cwd: ROOT,
    encoding: "utf8",
    env: {
      ...process.env,
      WIKI_ECON_DATA_DIR: DATA_DIR,
      WIKI_ECON_OUTPUT_DIR: OUTPUT_DIR,
      WIKI_ECON_GENERATOR_DIR: GENERATOR_DIR,
    },
  });
  manifestCache = JSON.parse(output);
  manifestCacheAt = now;
  return manifestCache;
}

function markerManifestIsValid(markerPath) {
  const manifest = {
    rows: 0,
    analyticalPaths: [],
    warehousePaths: [],
  };
  for (const line of fs.readFileSync(markerPath, "utf8").split(/\r?\n/)) {
    const idx = line.indexOf("=");
    if (idx === -1) continue;
    const key = line.slice(0, idx);
    const value = line.slice(idx + 1);
    if (key === "rows") manifest.rows = Number.parseInt(value, 10) || 0;
    if (key === "analytical_path") manifest.analyticalPaths.push(path.join(DATA_DIR, value));
    if (key === "warehouse_path") manifest.warehousePaths.push(path.join(DATA_DIR, value));
  }
  if (manifest.rows === 0) return true;
  if (manifest.analyticalPaths.length === 0 || manifest.warehousePaths.length === 0) return false;
  return [...manifest.analyticalPaths, ...manifest.warehousePaths].every((entry) => fs.existsSync(entry));
}

function walkFiles(root, predicate, acc = []) {
  if (!fs.existsSync(root)) return acc;
  for (const entry of fs.readdirSync(root, { withFileTypes: true })) {
    const entryPath = path.join(root, entry.name);
    if (entry.isDirectory()) {
      walkFiles(entryPath, predicate, acc);
    } else if (predicate(entryPath)) {
      acc.push(entryPath);
    }
  }
  return acc;
}

function cleanupWikiArtifacts(wiki) {
  const removed = [];
  const analyticalDir = path.join(DATA_DIR, "parquet", wiki);
  const warehouseDir = path.join(DATA_DIR, "warehouse", wiki);
  const tmpFiles = [
    ...walkFiles(analyticalDir, (entry) => entry.endsWith(".tmp")),
    ...walkFiles(warehouseDir, (entry) => entry.endsWith(".tmp")),
  ];
  for (const tmpPath of tmpFiles) {
    fs.rmSync(tmpPath, { force: true });
    removed.push(path.relative(ROOT, tmpPath));
  }

  const markerDir = path.join(analyticalDir, "_markers");
  for (const markerName of safeReadDir(markerDir)) {
    if (!markerName.endsWith(".done")) continue;
    const markerPath = path.join(markerDir, markerName);
    if (!markerManifestIsValid(markerPath)) {
      fs.rmSync(markerPath, { force: true });
      removed.push(path.relative(ROOT, markerPath));
    }
  }

  return {
    removed,
    tmpFiles: tmpFiles.length,
    invalidMarkers: removed.filter((entry) => entry.includes("_markers/")).length,
  };
}

function trackStageFromChunk(chunk) {
  if (!currentJob) return;

  const explicitMatches = [...chunk.matchAll(/\bstage=([a-z_]+)/g)];
  if (explicitMatches.length > 0) {
    currentJob.stage = explicitMatches.at(-1)[1];
  }
  const fetchMatch = chunk.match(/Fetching (\d+) files/i);
  if (fetchMatch) {
    currentJob.stage = "fetch";
    currentJob.expectedTotal = Number.parseInt(fetchMatch[1], 10) || currentJob.expectedTotal;
  } else if (/Compute patrol metrics|Loading patrol data|Autopatrol groups:/i.test(chunk)) {
    currentJob.stage = "patrol_compute";
  } else if (/patrol log dump|Querying siteinfo API|Patrol:\s+\d+|Parsing logging XML/i.test(chunk)) {
    currentJob.stage = "patrol_fetch";
  } else if (/Ingesting|converting:|skipping source/i.test(chunk)) {
    currentJob.stage = "ingest";
  } else if (/Merged \d+ wiki patrol outputs|Wrote baked patrol defaults|merge outputs|merging wiki/i.test(chunk)) {
    currentJob.stage = "merge";
  } else if (/Computing .*metric|Computing revision indexes|Computing patrol latency|Counting revisions/i.test(chunk)) {
    currentJob.stage = currentJob.stage === "patrol_compute" ? "patrol_compute" : "compute";
  }
}

function appendJobLog(chunk) {
  jobLog.push(chunk);
  trackStageFromChunk(chunk);
}

function getProgress() {
  if (!currentJob) return null;

  const wiki = currentJob.wiki ?? null;
  const action = currentJob.action ?? null;
  if (!wiki && action !== "merge" && action !== "cancel") return null;

  let manifest;
  try {
    manifest = refreshManifest();
  } catch {
    manifest = { wikis: {}, merged: [] };
  }
  const wikiStatus = wiki ? manifest.wikis?.[wiki] ?? null : null;
  const stage = currentJob.stage || (action === "run" ? "fetch" : action);
  let done = 0;
  let total = 1;
  let detail = "starting...";

  switch (stage) {
    case "fetch": {
      done = wikiStatus?.raw?.files ?? 0;
      total = currentJob.expectedTotal || done || 1;
      detail = `${done}/${total} dump files downloaded`;
      break;
    }
    case "patrol_fetch": {
      total = 4;
      done = wikiStatus?.patrol
        ? Number(wikiStatus.patrol.xml) + Number(wikiStatus.patrol.events) + Number(wikiStatus.patrol.rights) + Number(wikiStatus.patrol.groups)
        : 0;
      detail = `${done}/${total} patrol logging artifacts ready`;
      break;
    }
    case "ingest": {
      done = wikiStatus?.parquet?.done ?? 0;
      total = wikiStatus?.parquet?.total ?? 1;
      const inProgress = wikiStatus?.parquet?.in_progress ?? 0;
      detail = `${done}/${total} source files ingested${inProgress > 0 ? ` · ${inProgress} temp files` : ""}`;
      break;
    }
    case "compute": {
      done = (wikiStatus?.metrics ?? []).filter((metric) => metric.name !== "patrol").length;
      total = 8;
      detail = `${done}/${total} core metric files computed`;
      break;
    }
    case "patrol_compute": {
      total = 1;
      done = Number(Boolean(wikiStatus?.patrol?.metric_ready));
      detail = done ? "patrol metrics written" : "computing patrol metrics";
      break;
    }
    case "merge": {
      done = manifest.merged?.length ?? 0;
      total = REQUIRED_MERGED_METRICS;
      detail = `${done}/${total} merged site data files ready`;
      break;
    }
    case "cleanup": {
      done = 1;
      total = 1;
      detail = wiki ? `cleanup completed for ${wiki}` : "cleanup completed";
      break;
    }
    case "cancel": {
      done = 1;
      total = 1;
      detail = "job cancellation requested";
      break;
    }
    default: {
      done = 0;
      total = 1;
    }
  }

  const pct = total > 0 ? Math.min(100, Math.round((done / total) * 100)) : 0;
  return { wiki, stage, done, total, pct, detail };
}

const server = http.createServer((req, res) => {
  applyCors(req, res);
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  if (req.method === "OPTIONS") { res.writeHead(204); res.end(); return; }

  const url = new URL(req.url, `http://localhost:${PORT}`);

  // GET /api/status — poll job progress
  if (req.method === "GET" && url.pathname === "/api/status") {
    let manifest = null;
    try {
      manifest = refreshManifest();
    } catch (error) {
      manifest = { error: error.message };
    }
    let progress = null;
    try {
      progress = getProgress();
    } catch {
      progress = null;
    }
    const effectiveJob = currentJob
      ? {
          command: currentJob.command,
          action: currentJob.action,
          wiki: currentJob.wiki,
          stage: currentJob.stage,
          running: true,
          exitCode: null,
          log: jobLog,
          progress,
        }
      : lastJob;
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({
      running: currentJob !== null,
      command: effectiveJob?.command ?? null,
      action: effectiveJob?.action ?? null,
      wiki: effectiveJob?.wiki ?? null,
      log: effectiveJob?.log ?? [],
      exitCode: effectiveJob?.exitCode ?? jobExitCode,
      progress,
      manifest,
      job: effectiveJob,
      wikiJobs: Object.fromEntries(lastWikiJobs.entries()),
      globalJob: lastGlobalJob,
      supportedWikis: loadSupportedWikipedias(),
      suggestedVersion: suggestedSnapshotVersion(),
      adminEnabled: ADMIN_ENABLED,
      adminPort: PORT,
    }));
    return;
  }

  // POST /api/<action> — start a pipeline command
  if (req.method === "POST" && url.pathname.startsWith("/api/")) {
    let body = "";
    req.on("data", c => body += c);
    req.on("end", () => {
      const params = body ? JSON.parse(body) : {};
      const action = url.pathname.slice(5);

      if (currentJob) {
        if (action === "cancel") {
          currentJob.cancelRequested = true;
          currentJob.proc.kill("SIGTERM");
          appendJobLog(`\n[cancel requested for pid ${currentJob.pid}]`);
          res.writeHead(200, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ started: false, cancelled: true, pid: currentJob.pid }));
          return;
        }
        res.writeHead(409, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ error: "A job is already running", command: currentJob.command }));
        return;
      }

      const wiki = (params.wiki || "").replace(/[^a-z0-9_]/gi, ""); // sanitize
      const version = normalizeVersion(params.version);
      if (version && !isValidVersion(version)) {
        res.writeHead(400, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ error: "Invalid version. Use YYYY-MM." }));
        return;
      }

      if (action === "cleanup") {
        if (!wiki) {
          res.writeHead(400, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ error: "cleanup requires a wiki parameter" }));
          return;
        }
        const summary = cleanupWikiArtifacts(wiki);
        refreshManifest(true);
        setSyntheticJobLog(
          {
            command: `cleanup ${wiki}`,
            action: "cleanup",
            wiki,
            stage: "cleanup",
          },
          [
            `Cleanup finished for ${wiki}`,
            `Removed ${summary.tmpFiles} temporary files`,
            `Removed ${summary.invalidMarkers} invalid markers`,
            ...(summary.removed.length > 0 ? ["", ...summary.removed.map((entry) => `- ${entry}`)] : ["No files removed"]),
          ],
          0,
        );
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ started: false, cleaned: true, summary }));
        return;
      }

      let commandSpec = null;
      switch (action) {
        case "fetch":
        case "ingest":
        case "compute":
        case "run":
          commandSpec = wiki
            ? {
                program: resolveRunner().program,
                args: [
                  ...resolveRunner().args,
                  "--data-dir", DATA_DIR,
                  "--output-dir", OUTPUT_DIR,
                  action,
                  wiki,
                  ...(version && (action === "fetch" || action === "run") ? ["--version", version] : []),
                ],
                label: `${resolveRunner().label} --data-dir ${DATA_DIR} --output-dir ${OUTPUT_DIR} ${action} ${wiki}${version && (action === "fetch" || action === "run") ? ` --version ${version}` : ""}`,
              }
            : null;
          break;
        case "merge":
          commandSpec = {
            program: resolveRunner().program,
            args: [...resolveRunner().args, "--data-dir", DATA_DIR, "--output-dir", OUTPUT_DIR, "merge"],
            label: `${resolveRunner().label} --data-dir ${DATA_DIR} --output-dir ${OUTPUT_DIR} merge`,
          };
          break;
        case "patrol-fetch":
          commandSpec = wiki
            ? {
                program: resolveRunner().program,
                args: [...resolveRunner().args, "--data-dir", DATA_DIR, "--output-dir", OUTPUT_DIR, "patrol-fetch", wiki],
                label: `${resolveRunner().label} --data-dir ${DATA_DIR} --output-dir ${OUTPUT_DIR} patrol-fetch ${wiki}`,
              }
            : null;
          break;
        case "patrol-compute":
          commandSpec = wiki
            ? {
                program: resolveRunner().program,
                args: [...resolveRunner().args, "--data-dir", DATA_DIR, "--output-dir", OUTPUT_DIR, "patrol-compute", wiki],
                label: `${resolveRunner().label} --data-dir ${DATA_DIR} --output-dir ${OUTPUT_DIR} patrol-compute ${wiki}`,
              }
            : null;
          break;
        case "cancel":
          res.writeHead(409, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ error: "No job is currently running" }));
          return;
        default:
          commandSpec = null;
      }

      if (!commandSpec) {
        res.writeHead(400, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ error: "Invalid action or missing wiki parameter" }));
        return;
      }
      const startTime = new Date().toISOString().replace("T", " ").replace(/\.\d+Z$/, " UTC");
      jobLog = [`$ ${commandSpec.label}\nStarted: ${startTime}\n`];
      jobExitCode = null;

      const proc = spawn(commandSpec.program, commandSpec.args, {
        cwd: ROOT,
        env: {
          ...process.env,
          RUST_LOG: "info",
          PYTHONUNBUFFERED: "1",
          WIKI_ECON_DATA_DIR: DATA_DIR,
          WIKI_ECON_OUTPUT_DIR: OUTPUT_DIR,
          WIKI_ECON_GENERATOR_DIR: GENERATOR_DIR,
        },
      });
      currentJob = {
        command: commandSpec.label,
        pid: proc.pid,
        proc,
        action,
        wiki: wiki || null,
        stage: action === "run" ? "fetch" : action.replace("-", "_"),
        expectedTotal: null,
        cancelRequested: false,
      };

      proc.stdout.on("data", d => appendJobLog(d.toString()));
      proc.stderr.on("data", d => appendJobLog(d.toString()));
      proc.on("close", (code, signal) => {
        const cancelled = currentJob?.cancelRequested && signal === "SIGTERM";
        const renderedExit = cancelled ? "cancelled" : code;
        jobLog.push(`\n[exited with code ${renderedExit}]`);
        jobExitCode = cancelled ? 130 : code;
        const completedJob = {
          command: commandSpec.label,
          action,
          wiki: wiki || null,
          stage: currentJob?.stage ?? action.replace("-", "_"),
          exitCode: cancelled ? 130 : code,
          cancelled,
          running: false,
          log: [...jobLog],
          finishedAt: new Date().toISOString(),
        };
        lastJob = completedJob;
        if (completedJob.wiki) {
          lastWikiJobs.set(completedJob.wiki, completedJob);
        } else {
          lastGlobalJob = completedJob;
        }
        currentJob = null;
        refreshManifest(true);
      });
      proc.on("error", error => {
        jobLog.push(`\n[failed to start: ${error.message}]`);
        jobExitCode = 1;
        const failedJob = {
          command: commandSpec.label,
          action,
          wiki: wiki || null,
          stage: action.replace("-", "_"),
          exitCode: 1,
          running: false,
          log: [...jobLog],
          finishedAt: new Date().toISOString(),
        };
        lastJob = failedJob;
        if (failedJob.wiki) {
          lastWikiJobs.set(failedJob.wiki, failedJob);
        } else {
          lastGlobalJob = failedJob;
        }
        currentJob = null;
        refreshManifest(true);
      });

      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ started: true, command: commandSpec.label, pid: proc.pid }));
      console.log(`[admin] started: ${commandSpec.label} (pid ${proc.pid})`);
    });
    return;
  }

  res.writeHead(404);
  res.end("Not found");
});

server.listen(PORT, "127.0.0.1", () => {
  const runner = resolveRunner();
  console.log(`Admin API server listening on http://127.0.0.1:${PORT}`);
  console.log(`Runner: ${runner.label}`);
  console.log(`Working dir: ${ROOT}`);
  console.log(`Data dir: ${DATA_DIR}`);
  console.log(`Output dir: ${OUTPUT_DIR}`);
  console.log(`Generator dir: ${GENERATOR_DIR}`);
  console.log(`Allowed origins: ${Array.from(ALLOWED_ORIGINS).join(", ")}`);
});
