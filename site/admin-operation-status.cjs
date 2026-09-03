"use strict";

const ANSI_ESCAPE = /\x1b\[[0-?]*[ -/]*[@-~]/g;

const STAGE_LABELS = Object.freeze({
  snapshot_resolve: "Choosing a completed snapshot",
  patrol_preflight: "Checking logging-dump readiness",
  source_window: "Downloading and ingesting history",
  patrol_fetch: "Preparing patrol sources",
  compute: "Computing metrics",
  patrol_compute: "Computing patrol metrics",
  candidate_validate: "Validating the candidate",
  candidate_ready: "Marking the candidate ready",
  publication_prepare: "Preparing publication",
  publication_verify: "Verifying publication",
  site: "Building the website",
  publication_commit: "Publishing",
});

function stripAnsi(value) {
  return String(value || "").replace(ANSI_ESCAPE, "");
}

function matches(text, expression) {
  return Array.from(text.matchAll(expression));
}

function lastCapture(text, expression, index = 1) {
  const found = matches(text, expression);
  return found.length ? found.at(-1)[index] : null;
}

function lastAlternative(text, expression) {
  const found = matches(text, expression);
  if (!found.length) return null;
  return found.at(-1).slice(1).find(Boolean) || null;
}

function lastInteger(text, expression) {
  const value = lastCapture(text, expression);
  if (value == null) return null;
  const parsed = Number.parseInt(value, 10);
  return Number.isSafeInteger(parsed) && parsed >= 0 ? parsed : null;
}

function unique(values) {
  return Array.from(new Set(values.filter(Boolean)));
}

function classifyError(message) {
  if (!message) return {errorSummary: null, retryable: null, remediationCode: null, remediation: null};
  if (/UPSTREAM_WAITING:.*Wikimedia logging dump/i.test(message)) {
    const dumpDate = message.match(/logging dump (\d{8})/)?.[1] || "required";
    return {
      errorSummary: `Waiting for Wikimedia to finish the ${dumpDate} logging dump. Completed history ingestion is retained and will not be downloaded again.`,
      retryable: true,
      remediationCode: "upstream_logging_waiting",
      remediation: "No repair is required. Recheck upstream readiness later; the next run resumes with patrol preparation.",
    };
  }
  if (/editor identity is unavailable/i.test(message)) {
    return {
      errorSummary: "The ingested history contains editors without a usable ID or name. Retrying unchanged inputs will fail again; the input generation needs a compatible identity policy before metrics can be computed.",
      retryable: false,
      remediationCode: "editor_identity_unavailable",
      remediation: "Correct or rebuild the qualified metric-input generation, then explicitly acknowledge the retry.",
    };
  }
  if (/HTTP 404 Not Found/i.test(message)) {
    return {
      errorSummary: "The requested Wikimedia snapshot is not available. Leave the snapshot field blank to use the latest completed dump, or choose an available exact version.",
      retryable: false,
      remediationCode: "snapshot_unavailable",
      remediation: "Select an available snapshot or leave the snapshot blank before retrying.",
    };
  }
  return {
    errorSummary: message.replace(/^Error:\s*/i, "").trim(),
    retryable: true,
    remediationCode: null,
    remediation: "Review the log, correct any external cause, and retry. Validated source transactions remain reusable.",
  };
}

function conciseError(message) {
  return classifyError(message).errorSummary;
}

function summarizeOperationLog(entry = {}, rawLog = "") {
  const text = stripAnsi(rawLog);
  const startedStages = matches(text, /\bstarting stage\s+stage=(?:"([a-z_]+)"|([a-z_]+))/g)
    .map((match) => match[1] || match[2]);
  const stage = startedStages.at(-1) || entry.stage || null;
  const selectedSnapshot = lastAlternative(
    text,
    /\bselected completed Wikimedia snapshot\s+version=(?:"([0-9]{4}-[0-9]{2})"|([0-9]{4}-[0-9]{2}))/g,
  ) || entry.selectedSnapshot || entry.snapshot || entry.version || null;
  const selectedSnapshotFallback = selectedSnapshot
    || lastAlternative(text, /\bsnapshot=(?:"([0-9]{4}-[0-9]{2})"|([0-9]{4}-[0-9]{2}))/g);

  const plannedSources = lastInteger(text, /\bplanned_sources=(?:"?(\d+)"?)/g)
    ?? entry.progress?.totalSources
    ?? null;
  const reusedSources = lastInteger(text, /\breused_sources=(?:"?(\d+)"?)/g)
    ?? entry.progress?.reusedSources
    ?? 0;
  const completedSourceIds = unique([
    ...(entry.progress?.completedSourceIds || []),
    ...matches(text, /\bcommitted ingest source[^\n]*\bsource=(?:"([^"]+)"|([^\s]+))/g)
      .map((match) => match[1] || match[2]),
  ]);
  const completedSummary = lastInteger(text, /"ingested_sources":(\d+)/g);
  const completedSources = Math.max(
    reusedSources + completedSourceIds.length,
    completedSummary == null ? 0 : reusedSources + completedSummary,
    entry.progress?.completedSources || 0,
  );
  const currentSource = lastAlternative(
    text,
    /\bstarting source-window download[^\n]*\bsource=(?:"([^"]+)"|([^\s]+))/g,
  ) || entry.progress?.currentSource || null;
  const downloadedBytes = lastInteger(text, /"downloaded_bytes":(\d+)/g)
    ?? entry.progress?.downloadedBytes
    ?? null;
  const ingestedRows = lastInteger(text, /"ingested_rows":(\d+)/g)
    ?? entry.progress?.ingestedRows
    ?? null;
  const plannedBytes = lastInteger(text, /\b(?:planned_bytes|total_compressed_bytes)=(?:"?(\d+)"?)/g)
    ?? entry.progress?.plannedBytes
    ?? null;
  const reusedBytes = lastInteger(text, /\breused_bytes=(?:"?(\d+)"?)/g)
    ?? entry.progress?.reusedBytes
    ?? 0;
  const completedBytes = Math.min(
    plannedBytes || Number.MAX_SAFE_INTEGER,
    reusedBytes + (downloadedBytes || 0),
  );

  const errorLine = lastCapture(text, /^Error:\s*(.+)$/gm);
  const rawError = errorLine || entry.rawError || entry.error || null;
  const failure = classifyError(rawError);

  let percent = null;
  let detail = null;
  if (stage === "source_window" && plannedSources) {
    percent = plannedBytes
      ? Math.min(100, Math.round((completedBytes / plannedBytes) * 100))
      : Math.min(100, Math.round((completedSources / plannedSources) * 100));
    detail = `${Math.min(completedSources, plannedSources)} of ${plannedSources} history files safely ingested`;
    if (currentSource && completedSources < plannedSources) detail += ` · ${currentSource}`;
  } else if (stage) {
    detail = STAGE_LABELS[stage] || stage.replaceAll("_", " ");
  }

  return {
    stage,
    stageLabel: stage ? (STAGE_LABELS[stage] || stage.replaceAll("_", " ")) : null,
    selectedSnapshot: selectedSnapshotFallback,
    progress: {
      stage,
      percent,
      detail,
      totalSources: plannedSources,
      completedSources,
      reusedSources,
      completedSourceIds,
      currentSource,
      downloadedBytes,
      plannedBytes,
      reusedBytes,
      completedBytes,
      ingestedRows,
    },
    rawError,
    ...failure,
  };
}

module.exports = {
  STAGE_LABELS,
  classifyError,
  conciseError,
  stripAnsi,
  summarizeOperationLog,
};
