"use strict";

const OPERATIONS_SLOS = require("../config/operations-slos.json");

const DAY_MS = 24 * 60 * 60 * 1000;
const DEFAULT_THRESHOLDS = Object.freeze({
  memoryWarningRatio: OPERATIONS_SLOS.memory.warning_ratio,
  memoryCriticalRatio: OPERATIONS_SLOS.memory.critical_ratio,
  minimumDiskFreeBytes: OPERATIONS_SLOS.storage.minimum_free_bytes,
  heartbeatStaleMs: OPERATIONS_SLOS.heartbeat.maximum_age_ms,
  incrementalPublicationMaximumMs: OPERATIONS_SLOS.publication.incremental_maximum_duration_ms,
  maximumBrowserBytes: OPERATIONS_SLOS.browser_artifacts.maximum_total_bytes,
  maximumBrowserPartitionBytes: OPERATIONS_SLOS.browser_artifacts.maximum_partition_bytes,
  stageLimitsMs: Object.freeze(OPERATIONS_SLOS.stage_maximum_duration_ms),
});

function timestamp(value) {
  const parsed = Date.parse(value || "");
  return Number.isFinite(parsed) ? parsed : null;
}

function successfulRuns(last, history) {
  const byRun = new Map();
  for (const record of [...(history || []), last].filter(Boolean)) {
    if (record.state !== "succeeded" && record.exitCode !== 0) continue;
    if (!record.runId) continue;
    byRun.set(record.runId, record);
  }
  return [...byRun.values()].sort((left, right) => (timestamp(left.finishedAt) || 0) - (timestamp(right.finishedAt) || 0));
}

function stageStart(record) {
  const current = [...(record?.stages || [])].reverse().find((stage) =>
    stage.state === "running" && stage.stage === record.currentStage && stage.wiki === (record.currentWiki || null));
  return timestamp(current?.startedAt);
}

function evaluateFreshness({last = null, history = [], lifecycle, scrubStatus = null, now = Date.now(), thresholds = {}}) {
  const settings = {
    ...DEFAULT_THRESHOLDS,
    ...thresholds,
    stageLimitsMs: {...DEFAULT_THRESHOLDS.stageLimitsMs, ...(thresholds.stageLimitsMs || {})},
  };
  const scheduledWikis = Object.entries(lifecycle?.wikis || {})
    .filter(([, entry]) => entry.publication === "published" && entry.refresh === "scheduled");
  const successes = successfulRuns(last, history);
  const latestSuccess = successes.at(-1) || null;
  const publicationSuccesses = successes.filter((record) => record.publication);
  const latestPublication = publicationSuccesses.at(-1) || null;
  const previousPublication = publicationSuccesses.at(-2) || null;
  const alerts = [];
  const alert = (code, severity, message, details = {}) => alerts.push({code, severity, message, ...details});

  if (scrubStatus) {
    const valid = scrubStatus.schema_version === 1
      && ["succeeded", "failed"].includes(scrubStatus.state)
      && typeof scrubStatus.run_id === "string"
      && scrubStatus.run_id.length > 0
      && Number.isSafeInteger(scrubStatus.updated_at_unix);
    if (!valid) {
      alert("artifact_scrub_status_invalid", "critical", "The durable artifact scrub status is malformed; publication is blocked.");
    } else if (scrubStatus.state === "failed") {
      alert("artifact_scrub_failed", "critical", `Artifact scrub ${scrubStatus.run_id} failed; publication is blocked.`, {
        runId: scrubStatus.run_id,
        error: scrubStatus.error || "unknown scrub failure",
      });
    }
  }

  for (const [wiki, entry] of scheduledWikis) {
    const finished = timestamp(latestPublication?.finishedAt);
    const maximumAgeMs = Number(entry.freshness_sla_days) * DAY_MS;
    if (!finished) {
      alert("refresh_success_missing", "critical", `No successful refresh is recorded for ${wiki}.`, {wiki});
    } else if (Number.isFinite(maximumAgeMs) && now - finished > maximumAgeMs) {
      alert("refresh_success_old", "critical", `The last successful refresh for ${wiki} exceeds its ${entry.freshness_sla_days}-day SLA.`, {
        wiki, ageMs: now - finished, thresholdMs: maximumAgeMs,
      });
    }
  }

  if (["starting", "running"].includes(last?.state)) {
    const heartbeat = timestamp(last.heartbeatAt || last.startedAt);
    if (!heartbeat || now - heartbeat > settings.heartbeatStaleMs) {
      alert("heartbeat_stalled", "critical", `Refresh ${last.runId || "unknown"} has a stale heartbeat.`, {
        runId: last.runId || null, stage: last.currentStage || null,
        ageMs: heartbeat ? now - heartbeat : null, thresholdMs: settings.heartbeatStaleMs,
      });
    }
    const started = stageStart(last);
    const stageLimit = settings.stageLimitsMs[last.currentStage];
    if (started && stageLimit && now - started > stageLimit) {
      alert("stage_runtime_exceeded", "critical", `Stage ${last.currentStage} exceeded its runtime limit.`, {
        runId: last.runId || null, stage: last.currentStage, wiki: last.currentWiki || null,
        ageMs: now - started, thresholdMs: stageLimit,
      });
    }
  }

  const publishedSnapshots = latestPublication?.publication?.selectedSnapshots || {};
  for (const [wiki] of scheduledWikis) {
    const published = publishedSnapshots[wiki] || latestSuccess?.selectedSnapshot || null;
    if (last?.selectedSnapshot && published && last.selectedSnapshot > published) {
      alert("selected_dump_unpublished", "critical", `Selected dump ${last.selectedSnapshot} for ${wiki} is newer than published ${published}.`, {
        wiki, selectedSnapshot: last.selectedSnapshot, publishedSnapshot: published,
      });
    }
  }

  if (latestPublication && previousPublication
      && latestPublication.selectedSnapshot > previousPublication.selectedSnapshot) {
    for (const [wiki] of scheduledWikis) {
      const currentCutoff = latestPublication.publication?.cutoffDates?.[wiki] || null;
      const previousCutoff = previousPublication.publication?.cutoffDates?.[wiki] || null;
      if (currentCutoff && previousCutoff && currentCutoff <= previousCutoff) {
        alert("output_cutoff_stalled", "critical", `The ${wiki} cutoff did not advance with snapshot ${latestPublication.selectedSnapshot}.`, {
          wiki, selectedSnapshot: latestPublication.selectedSnapshot, cutoff: currentCutoff, previousCutoff,
        });
      }
    }
  }

  if (latestPublication?.publication) {
    const patrolRows = latestPublication.publication.metrics?.patrol?.rows;
    if (Number.isFinite(patrolRows) && patrolRows <= 0) {
      alert("patrol_output_zero", "critical", "The published patrol metric contains zero rows.", {rows: patrolRows});
    }
    for (const [wiki] of scheduledWikis) {
      const source = latestPublication.publication.patrolSources?.[wiki];
      if (source && (!(source.patrol_events > 0) || !(source.rights_events > 0))) {
        alert("patrol_source_zero", "critical", `The ${wiki} patrol source contains zero patrol or rights rows.`, {
          wiki, patrolEvents: source.patrol_events ?? null, rightsEvents: source.rights_events ?? null,
        });
      }
    }
  }

  const peak = latestPublication?.memoryPeakBytes;
  const limit = latestPublication?.memoryLimitBytes;
  if (Number.isFinite(peak) && Number.isFinite(limit) && limit > 0) {
    const ratio = peak / limit;
    if (ratio >= settings.memoryCriticalRatio) {
      alert("memory_pressure", "critical", `Refresh peak memory reached ${(ratio * 100).toFixed(1)}% of its limit.`, {
        ratio, peakBytes: peak, limitBytes: limit, threshold: settings.memoryCriticalRatio,
      });
    } else if (ratio >= settings.memoryWarningRatio) {
      alert("memory_pressure", "warning", `Refresh peak memory reached ${(ratio * 100).toFixed(1)}% of its limit.`, {
        ratio, peakBytes: peak, limitBytes: limit, threshold: settings.memoryWarningRatio,
      });
    }
  }

  const diskFree = latestSuccess?.diskFreeBytes ?? latestSuccess?.disk?.freeBytes;
  if (Number.isFinite(diskFree) && diskFree < settings.minimumDiskFreeBytes) {
    alert("disk_headroom_low", "critical", "Refresh disk headroom is below the configured safe threshold.", {
      freeBytes: diskFree, thresholdBytes: settings.minimumDiskFreeBytes,
    });
  }

  if (latestPublication) {
    const incrementalDuration = latestPublication.stageDurationsMs?.publication_prepare;
    if (latestPublication.publication?.changePlan
        && Number.isFinite(incrementalDuration)
        && incrementalDuration > settings.incrementalPublicationMaximumMs) {
      alert("incremental_publication_slow", "critical", "Incremental publication exceeded its three-minute SLO.", {
        runId: latestPublication.runId,
        durationMs: incrementalDuration,
        thresholdMs: settings.incrementalPublicationMaximumMs,
        changedFamilies: latestPublication.publication.changePlan.changed?.length ?? null,
      });
    }
    const browser = latestPublication.publication?.browserData;
    if (!browser || !Number.isFinite(browser.bytes) || !Number.isFinite(browser.largestPartitionBytes)) {
      alert("browser_artifact_evidence_missing", "critical", "The successful publication has no validated browser artifact size evidence.");
    } else {
      if (browser.bytes > settings.maximumBrowserBytes) {
        alert("browser_artifact_total_exceeded", "critical", "Published browser artifacts exceed the total-size SLO.", {
          bytes: browser.bytes, thresholdBytes: settings.maximumBrowserBytes,
        });
      }
      if (browser.largestPartitionBytes > settings.maximumBrowserPartitionBytes) {
        alert("browser_artifact_partition_exceeded", "critical", "A published browser partition exceeds the per-file size SLO.", {
          bytes: browser.largestPartitionBytes, thresholdBytes: settings.maximumBrowserPartitionBytes,
        });
      }
    }
  }

  const status = alerts.some((entry) => entry.severity === "critical")
    ? "critical"
    : alerts.length > 0 ? "warning" : "healthy";
  return {
    schemaVersion: 1,
    generatedAt: new Date(now).toISOString(),
    status,
    alerts,
    summary: {
      scheduledWikis: scheduledWikis.map(([wiki]) => wiki),
      currentRunId: last?.runId || null,
      currentState: last?.state || null,
      currentStage: last?.currentStage || null,
      heartbeatAt: last?.heartbeatAt || null,
      lastSuccessfulRunId: latestSuccess?.runId || null,
      lastSuccessfulAt: latestSuccess?.finishedAt || null,
      lastPublicationRunId: latestPublication?.runId || null,
      lastPublicationAt: latestPublication?.finishedAt || null,
      selectedSnapshot: last?.selectedSnapshot || latestSuccess?.selectedSnapshot || null,
      publishedSnapshots,
      artifactScrub: scrubStatus,
      slos: {
        memoryWarningRatio: settings.memoryWarningRatio,
        memoryCriticalRatio: settings.memoryCriticalRatio,
        minimumDiskFreeBytes: settings.minimumDiskFreeBytes,
        heartbeatStaleMs: settings.heartbeatStaleMs,
        incrementalPublicationMaximumMs: settings.incrementalPublicationMaximumMs,
        maximumBrowserBytes: settings.maximumBrowserBytes,
        maximumBrowserPartitionBytes: settings.maximumBrowserPartitionBytes,
      },
    },
  };
}

module.exports = {DAY_MS, DEFAULT_THRESHOLDS, OPERATIONS_SLOS, evaluateFreshness, successfulRuns};
