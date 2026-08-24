use chrono::Utc;
use serde::Serialize;
use std::env;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::Path;
use tracing::warn;

const RUN_EVENTS_FILE_ENV: &str = "WIKI_ECON_RUN_EVENTS_FILE";
const MAX_STAGE_ERROR_CHARS: usize = 500;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct StageEvent<'a> {
    event: &'a str,
    stage: &'a str,
    wiki: Option<&'a str>,
    at: String,
    duration_ms: Option<u64>,
    error: Option<&'a str>,
}

fn concise_error(error: &anyhow::Error) -> String {
    let flattened = format!("{error:#}").replace(['\n', '\r'], " ");
    flattened.chars().take(MAX_STAGE_ERROR_CHARS).collect()
}

fn append_stage_event_to(path: &Path, event: &StageEvent<'_>) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    serde_json::to_writer(&mut file, event)?;
    file.write_all(b"\n")?;
    Ok(())
}

fn record_stage_event(
    event: &str,
    stage: &str,
    wiki: Option<&str>,
    duration_ms: Option<u64>,
    error: Option<&str>,
) {
    let path = env::var_os(RUN_EVENTS_FILE_ENV);
    record_stage_event_at(
        path.as_deref().map(Path::new),
        event,
        stage,
        wiki,
        duration_ms,
        error,
    );
}

fn record_stage_event_at(
    path: Option<&Path>,
    event: &str,
    stage: &str,
    wiki: Option<&str>,
    duration_ms: Option<u64>,
    error: Option<&str>,
) {
    let Some(path) = path else {
        return;
    };
    let stage_event = StageEvent {
        event,
        stage,
        wiki,
        at: Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        duration_ms,
        error,
    };
    if let Err(write_error) = append_stage_event_to(path, &stage_event) {
        warn!(
            path = %path.display(),
            error = %write_error,
            "unable to append run-stage observability event"
        );
    }
}

pub fn record_stage_started(stage: &str, wiki: Option<&str>) {
    record_stage_event("started", stage, wiki, None, None);
}

pub fn record_stage_completed(stage: &str, wiki: Option<&str>, duration_ms: u64) {
    record_stage_event("completed", stage, wiki, Some(duration_ms), None);
}

pub fn record_stage_failed(
    stage: &str,
    wiki: Option<&str>,
    duration_ms: u64,
    error: &anyhow::Error,
) {
    let error = concise_error(error);
    record_stage_event("failed", stage, wiki, Some(duration_ms), Some(&error));
}

pub fn record_stage_reused(stage: &str, wiki: Option<&str>) {
    record_stage_event("reused", stage, wiki, None, None);
}

pub fn record_stage_skipped(stage: &str, wiki: Option<&str>) {
    record_stage_event("skipped", stage, wiki, None, None);
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
pub struct MemorySnapshot {
    pub rss_bytes: Option<u64>,
    pub cgroup_current_bytes: Option<u64>,
    pub cgroup_peak_bytes: Option<u64>,
    pub cgroup_limit_bytes: Option<u64>,
}

impl MemorySnapshot {
    pub fn capture() -> Self {
        Self {
            rss_bytes: fs::read_to_string("/proc/self/status")
                .ok()
                .and_then(|status| parse_proc_status_rss(&status)),
            cgroup_current_bytes: read_byte_counter("/sys/fs/cgroup/memory.current"),
            cgroup_peak_bytes: read_byte_counter("/sys/fs/cgroup/memory.peak"),
            cgroup_limit_bytes: read_byte_counter("/sys/fs/cgroup/memory.max"),
        }
    }
}

fn read_byte_counter(path: &str) -> Option<u64> {
    fs::read_to_string(path)
        .ok()
        .and_then(|value| parse_byte_counter(&value))
}

fn parse_byte_counter(value: &str) -> Option<u64> {
    value.trim().parse().ok()
}

fn parse_proc_status_rss(status: &str) -> Option<u64> {
    let kib = status.lines().find_map(|line| {
        line.strip_prefix("VmRSS:")?
            .split_whitespace()
            .next()?
            .parse::<u64>()
            .ok()
    })?;
    kib.checked_mul(1024)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::TestDir;

    #[test]
    fn parses_proc_rss_in_kibibytes() {
        assert_eq!(
            parse_proc_status_rss("Name:\twiki-econ\nVmRSS:\t42 kB\n"),
            Some(43_008)
        );
        assert_eq!(parse_proc_status_rss("Name:\twiki-econ\n"), None);
        assert_eq!(parse_proc_status_rss("VmRSS:\tnot-a-number kB\n"), None);
    }

    #[test]
    fn parses_finite_cgroup_counters_only() {
        assert_eq!(parse_byte_counter("6291456\n"), Some(6_291_456));
        assert_eq!(parse_byte_counter("max\n"), None);
    }

    #[test]
    fn captures_available_process_counters() {
        let snapshot = MemorySnapshot::capture();
        #[cfg(target_os = "linux")]
        assert!(snapshot.rss_bytes.is_some());
        #[cfg(not(target_os = "linux"))]
        assert_eq!(snapshot, MemorySnapshot::default());
    }

    #[test]
    fn stage_events_are_json_lines_and_errors_are_concise() -> anyhow::Result<()> {
        let dir = TestDir::new()?;
        let events = dir.path().join("nested/events.jsonl");
        record_stage_event_at(
            Some(&events),
            "started",
            "compute",
            Some("nlwiki"),
            None,
            None,
        );
        record_stage_event_at(
            Some(&events),
            "completed",
            "compute",
            Some("nlwiki"),
            Some(42),
            None,
        );

        let lines: Vec<serde_json::Value> = fs::read_to_string(&events)?
            .lines()
            .map(serde_json::from_str)
            .collect::<Result<_, _>>()?;
        assert_eq!(lines.len(), 2);
        assert_eq!(lines[0]["event"], "started");
        assert_eq!(lines[0]["stage"], "compute");
        assert_eq!(lines[0]["wiki"], "nlwiki");
        assert_eq!(lines[1]["durationMs"], 42);
        assert_eq!(
            concise_error(&anyhow::anyhow!("first\nsecond")),
            "first second"
        );
        assert_eq!(
            concise_error(&anyhow::anyhow!("{}", "x".repeat(600))).len(),
            MAX_STAGE_ERROR_CHARS
        );
        Ok(())
    }

    #[test]
    fn stage_event_recording_is_optional_and_best_effort() -> anyhow::Result<()> {
        record_stage_event_at(None, "started", "fetch", None, None, None);
        record_stage_event_at(
            Some(Path::new("")),
            "failed",
            "fetch",
            None,
            Some(1),
            Some("empty event path"),
        );
        let dir = TestDir::new()?;
        let blocker = dir.path().join("blocker");
        fs::write(&blocker, "not a directory")?;
        record_stage_event_at(
            Some(&blocker.join("events.jsonl")),
            "failed",
            "fetch",
            None,
            Some(1),
            Some("network failure"),
        );
        Ok(())
    }
}
