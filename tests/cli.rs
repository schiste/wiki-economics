use std::process::Command;
use std::{fs, path::Path};

#[path = "../src/test_support.rs"]
mod test_support;

use test_support::{TestDir, init_test_tracing};

fn instrumented_binary() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_wiki-econ"));
    if let Ok(profile_file) = std::env::var("LLVM_PROFILE_FILE") {
        command.env("LLVM_PROFILE_FILE", profile_file);
    }
    command
}

#[test]
fn binary_entrypoint_rejects_incomplete_merge_inputs() {
    init_test_tracing();
    let output_dir = TestDir::new().expect("temp dir");

    let output = instrumented_binary()
        .arg("--output-dir")
        .arg(output_dir.path())
        .arg("merge")
        .output()
        .expect("binary should run");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("missing merged dashboard input"));
}

#[test]
fn binary_entrypoint_rejects_unsafe_wiki_before_fetch() {
    init_test_tracing();
    let data_dir = TestDir::new().expect("temp dir");

    let output = instrumented_binary()
        .arg("--data-dir")
        .arg(data_dir.path())
        .arg("fetch")
        .arg("--version")
        .arg("2026-07")
        .arg("../enwiki")
        .output()
        .expect("binary should run");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("invalid wiki database name"));
}

fn stage_events(path: &Path) -> Vec<serde_json::Value> {
    fs::read_to_string(path)
        .expect("stage events should exist")
        .lines()
        .map(|line| serde_json::from_str(line).expect("stage event should be JSON"))
        .collect()
}

#[test]
fn binary_entrypoint_records_successful_and_failed_stage_events() {
    let data_dir = TestDir::new().expect("temp dir");
    let events = data_dir.path().join("run-events.jsonl");

    let success = instrumented_binary()
        .env("WIKI_ECON_RUN_EVENTS_FILE", &events)
        .env("WIKI_ECON_LOG_ANSI", "0")
        .env("RUST_LOG", "info")
        .arg("--data-dir")
        .arg(data_dir.path())
        .arg("--run-id")
        .arg("production-test-run")
        .arg("snapshot-finalize")
        .arg("nlwiki")
        .output()
        .expect("binary should run");
    assert!(success.status.success());
    let success_log = format!(
        "{}{}",
        String::from_utf8_lossy(&success.stdout),
        String::from_utf8_lossy(&success.stderr)
    );
    assert!(
        success_log.contains("run_id=production-test-run"),
        "log did not contain the run ID: {success_log}"
    );
    assert!(!success_log.contains("\u{1b}["));

    let failure = instrumented_binary()
        .env("WIKI_ECON_RUN_EVENTS_FILE", &events)
        .arg("--data-dir")
        .arg(data_dir.path())
        .arg("ingest")
        .arg("nlwiki")
        .arg("--version")
        .arg("2026-07")
        .output()
        .expect("binary should run");
    assert!(!failure.status.success());

    let events = stage_events(&events);
    assert_eq!(events.len(), 4);
    assert_eq!(events[0]["event"], "started");
    assert_eq!(events[1]["event"], "completed");
    assert_eq!(events[1]["stage"], "snapshot_finalize");
    assert_eq!(events[2]["event"], "started");
    assert_eq!(events[3]["event"], "failed");
    assert_eq!(events[3]["stage"], "ingest");
    assert!(
        events[3]["error"]
            .as_str()
            .is_some_and(|error| !error.is_empty())
    );
}
