"use strict";

const ANSI_ESCAPE = /\x1b\[[0-?]*[ -/]*[@-~]/g;

const STAGE_LABELS = Object.freeze({
  snapshot_resolve: "Choosing a completed snapshot",
  patrol_preflight: "Checking logging-dump readiness",
  qualification_discovery: "Recovering reusable qualification work",
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

const RESUMABLE_STAGES = new Set([
  "snapshot_resolve",
  "patrol_preflight",
  "source_window",
  "patrol_fetch",
  "compute",
  "patrol_compute",
  "candidate_validate",
  "candidate_ready",
  "publication_prepare",
  "publication_verify",
  "site",
  "publication_commit",
]);

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

function currentSourceTransfer(text) {
  const progress = matches(
    text,
    /\bsource download progress[^\n]*\bdownloaded_bytes=(?:"?(\d+)"?)[^\n]*\bexpected_bytes=(?:"?(\d+)"?)[^\n]*\bbytes_per_second=(?:"?(\d+)"?)/g,
  ).at(-1);
  if (!progress) return null;
  const completedSampleIndex = text.lastIndexOf("resource governor source progress");
  if ((progress.index ?? -1) < completedSampleIndex) return null;
  return {
    downloadedBytes: Number.parseInt(progress[1], 10),
    expectedBytes: Number.parseInt(progress[2], 10),
    bytesPerSecond: Number.parseInt(progress[3], 10),
  };
}

function classifyError(message) {
  if (!message) return {errorSummary: null, retryable: null, remediationCode: null, remediation: null};
  if (/UPSTREAM_WAITING:.*Wikimedia logging dump/i.test(message)) {
    const dumpDate = message.match(/logging dump (\d{8})/)?.[1] || "required";
    return {
      errorSummary: `Waiting for Wikimedia to finish the ${dumpDate} logging dump. Completed history ingestion is retained and will not be downloaded again.`,
      retryable: true,
      remediationCode: "upstream_logging_waiting",
      remediation: "No repair is required. The admin will recheck automatically; the next run resumes with patrol preparation.",
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
  if (/No patrol data for .*Run `patrol-fetch` first/i.test(message)) {
    return {
      errorSummary: "Patrol computation has no validated source generation.",
      retryable: false,
      remediationCode: "patrol_source_missing",
      remediation: "Use Patrol refresh; it checks upstream readiness, fetches the selected generation, and only then computes patrol metrics.",
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
  if (/workload profile .* has not completed production qualification/i.test(message)) {
    return {
      errorSummary: "The selected workload profile has not passed production qualification for this workload.",
      retryable: false,
      remediationCode: "workload_profile_unqualified",
      remediation: "Run the profile qualification with measured memory, scratch, duration, and deterministic-output evidence before retrying this project.",
    };
  }
  if (/publication preflight is blocked/i.test(message)) {
    return {
      errorSummary: "Publication preflight rejected the current candidate set. The existing public generation remains unchanged.",
      retryable: false,
      remediationCode: "publication_preflight_blocked",
      remediation: "Open the publication workbench for the grouped blockers and change plan. Correct those candidate-level incompatibilities before running preflight again.",
    };
  }
  if (/out of memory|oom|memory(?:\.max)?|cannot allocate memory|exceeded.*memory/i.test(message)) {
    return {
      errorSummary: "The run exceeded its safe memory budget.",
      retryable: false,
      remediationCode: "memory_budget_exceeded",
      remediation: "Review the run resource evidence, select a qualified lower-memory profile or increase capacity, then explicitly retry.",
    };
  }
  if (/disk|storage|no space left|reserve.*(?:below|exhausted)|quota exceeded/i.test(message)) {
    return {
      errorSummary: "The run could not preserve the required storage reserve.",
      retryable: false,
      remediationCode: "storage_reserve_exhausted",
      remediation: "Run the retention audit or safe cleanup, verify the configured reserve is available, then explicitly retry.",
    };
  }
  if (/publication.*(?:receipt|gate|candidate|site).*(?:mismatch|disagree|invalid)|(?:receipt|hash).*(?:mismatch|changed|invalid)/i.test(message)) {
    return {
      errorSummary: "Publication evidence no longer agrees with the artifact or site generation it authenticates.",
      retryable: false,
      remediationCode: "publication_evidence_mismatch",
      remediation: "Run publication recovery audit and artifact scrub. Publish again only after both reports are clean.",
    };
  }
  if (/lease heartbeat expired|stale lease|worker.*stopped reporting/i.test(message)) {
    return {
      errorSummary: "A fleet worker stopped reporting before it released its lease.",
      retryable: true,
      remediationCode: "fleet_lease_stale",
      remediation: "Run fleet recovery to authenticate the stale lease and requeue only recoverable work.",
    };
  }
  if (/retry_limit_exhausted|retry limit exhausted|automatic retries.*exhausted/i.test(message)) {
    return {
      errorSummary: "Automatic retries were exhausted and the fleet task is quarantined.",
      retryable: false,
      remediationCode: "fleet_task_quarantined",
      remediation: "Review the final failure and its inputs. After correcting the cause, explicitly retry this exact quarantined task.",
    };
  }
  if (/multi-member|zero relevant events|patrol.*zero|rights.*zero/i.test(message)) {
    return {
      errorSummary: "Patrol data failed its semantic event-count checks.",
      retryable: false,
      remediationCode: "patrol_semantic_failure",
      remediation: "Inspect the selected logging source and parser counts, then run a patrol-only rebuild after correcting the cause.",
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
  const sourceTransfer = currentSourceTransfer(text);
  const completedBytes = Math.min(
    plannedBytes || Number.MAX_SAFE_INTEGER,
    reusedBytes + (downloadedBytes || 0) + (sourceTransfer?.downloadedBytes || 0),
  );
  const downloadBytesPerSecond = lastInteger(text, /"download_bytes_per_second":(\d+)/g)
    ?? entry.progress?.downloadBytesPerSecond
    ?? null;
  const ingestRowsPerSecond = lastInteger(text, /"ingest_rows_per_second":(\d+)/g)
    ?? entry.progress?.ingestRowsPerSecond
    ?? null;
  const memoryCurrentBytes = lastInteger(text, /"cgroup_current_bytes":(\d+)/g)
    ?? entry.progress?.memoryCurrentBytes
    ?? null;
  const memoryPeakBytes = lastInteger(text, /"cgroup_peak_bytes":(\d+)/g)
    ?? entry.progress?.memoryPeakBytes
    ?? null;
  const scratchBytes = lastInteger(text, /"scratch_bytes":(\d+)/g)
    ?? entry.progress?.scratchBytes
    ?? null;
  const persistentAvailableBytes = lastInteger(text, /"persistent_available_bytes":(\d+)/g)
    ?? entry.progress?.persistentAvailableBytes
    ?? null;
  const effectiveDownloadRate = sourceTransfer?.bytesPerSecond || downloadBytesPerSecond;
  const etaSeconds = plannedBytes && effectiveDownloadRate
    ? Math.max(0, Math.ceil((plannedBytes - completedBytes) / effectiveDownloadRate))
    : entry.progress?.etaSeconds ?? null;

  const errorLine = lastCapture(text, /^Error:\s*(.+)$/gm);
  const succeeded = entry.state === "succeeded" || entry.exitCode === 0;
  const rawError = succeeded ? null : errorLine || entry.rawError || entry.error || null;
  const failure = succeeded
    ? {errorSummary: null, retryable: null, remediationCode: null, remediation: null}
    : classifyError(rawError);

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
      downloadBytesPerSecond,
      currentSourceDownloadedBytes: sourceTransfer?.downloadedBytes ?? null,
      currentSourceExpectedBytes: sourceTransfer?.expectedBytes ?? null,
      currentSourceBytesPerSecond: sourceTransfer?.bytesPerSecond ?? null,
      ingestRowsPerSecond,
      etaSeconds,
      memoryCurrentBytes,
      memoryPeakBytes,
      scratchBytes,
      persistentAvailableBytes,
    },
    recovery: stage ? {
      resumable: RESUMABLE_STAGES.has(stage),
      stableRunId: entry.recoveryPolicy?.stableRunId ?? true,
      preservesValidatedTransactions: entry.recoveryPolicy?.preserveValidatedTransactions ?? true,
      retryCount: Number(entry.retryCount || 0),
      maximumStaleRetries: Number(entry.recoveryPolicy?.maximumStaleRetries ?? 2),
    } : null,
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
