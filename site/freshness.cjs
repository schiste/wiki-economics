"use strict";

const DAY_MS = 24 * 60 * 60 * 1000;
const GIB = 1024 ** 3;
const DEFAULT_THRESHOLDS = Object.freeze({
  memoryWarningRatio: 0.75,
  memoryCriticalRatio: 0.80,
  minimumDiskFreeBytes: 20 * GIB,
  heartbeatStaleMs: 3 * 60 * 1000,
  stageLimitsMs: Object.freeze({
    snapshot_resolve: 5 * 60 * 1000,
    fetch: 45 * 60 * 1000,
    patrol_fetch: 45 * 60 * 1000,
    ingest: 45 * 60 * 1000,
    cleanup_raw: 5 * 60 * 1000,
    compute: 2 * 60 * 60 * 1000,
    patrol_compute: 60 * 60 * 1000,
    merge: 30 * 60 * 1000,
    publication_validate: 30 * 60 * 1000,
    site: 20 * 60 * 1000,
    publication_verify: 5 * 60 * 1000,
    snapshot_finalize: 5 * 60 * 1000,
    artifact_check: 5 * 60 * 1000,
  }),
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

function evaluateFreshness({last = null, history = [], lifecycle, now = Date.now(), thresholds = {}}) {
  const settings = {
    ...DEFAULT_THRESHOLDS,
    ...thresholds,
    stageLimitsMs: {...DEFAULT_THRESHOLDS.stageLimitsMs, ...(thresholds.stageLimitsMs || {})},
  };
  const scheduledWikis = Object.entries(lifecycle?.wikis || {})
    .filter(([, entry]) => entry.publication === "published" && entry.refresh === "scheduled");
  const successes = successfulRuns(last, history);
  const latestSuccess = successes.at(-1) || null;
  const previousSuccess = successes.at(-2) || null;
  const alerts = [];
  const alert = (code, severity, message, details = {}) => alerts.push({code, severity, message, ...details});

  for (const [wiki, entry] of scheduledWikis) {
    const finished = timestamp(latestSuccess?.finishedAt);
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

  const publishedSnapshots = latestSuccess?.publication?.selectedSnapshots || {};
  for (const [wiki] of scheduledWikis) {
    const published = publishedSnapshots[wiki] || latestSuccess?.selectedSnapshot || null;
    if (last?.selectedSnapshot && published && last.selectedSnapshot > published) {
      alert("selected_dump_unpublished", "critical", `Selected dump ${last.selectedSnapshot} for ${wiki} is newer than published ${published}.`, {
        wiki, selectedSnapshot: last.selectedSnapshot, publishedSnapshot: published,
      });
    }
  }

  if (latestSuccess && previousSuccess && latestSuccess.selectedSnapshot > previousSuccess.selectedSnapshot) {
    for (const [wiki] of scheduledWikis) {
      const currentCutoff = latestSuccess.publication?.cutoffDates?.[wiki] || null;
      const previousCutoff = previousSuccess.publication?.cutoffDates?.[wiki] || null;
      if (currentCutoff && previousCutoff && currentCutoff <= previousCutoff) {
        alert("output_cutoff_stalled", "critical", `The ${wiki} cutoff did not advance with snapshot ${latestSuccess.selectedSnapshot}.`, {
          wiki, selectedSnapshot: latestSuccess.selectedSnapshot, cutoff: currentCutoff, previousCutoff,
        });
      }
    }
  }

  if (latestSuccess?.publication) {
    const patrolRows = latestSuccess.publication.metrics?.patrol?.rows;
    if (Number.isFinite(patrolRows) && patrolRows <= 0) {
      alert("patrol_output_zero", "critical", "The published patrol metric contains zero rows.", {rows: patrolRows});
    }
    for (const [wiki] of scheduledWikis) {
      const source = latestSuccess.publication.patrolSources?.[wiki];
      if (source && (!(source.patrol_events > 0) || !(source.rights_events > 0))) {
        alert("patrol_source_zero", "critical", `The ${wiki} patrol source contains zero patrol or rights rows.`, {
          wiki, patrolEvents: source.patrol_events ?? null, rightsEvents: source.rights_events ?? null,
        });
      }
    }
  }

  const peak = latestSuccess?.memoryPeakBytes;
  const limit = latestSuccess?.memoryLimitBytes;
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
      selectedSnapshot: last?.selectedSnapshot || latestSuccess?.selectedSnapshot || null,
      publishedSnapshots,
    },
  };
}

module.exports = {DAY_MS, DEFAULT_THRESHOLDS, evaluateFreshness, successfulRuns};
