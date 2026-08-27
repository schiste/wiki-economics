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
fn fleet_cli_discovers_claims_heartbeats_completes_and_recovers() {
    let data_dir = TestDir::new().expect("temp data dir");
    let output_dir = TestDir::new().expect("temp output dir");
    let queue_dir = output_dir.path().join("_fleet");
    let lifecycle = data_dir.path().join("lifecycle.json");
    let receipt = data_dir.path().join("claim.json");
    fs::write(
        &lifecycle,
        r#"{"schema_version":1,"wikis":{"testwiki":{"publication":"published","refresh":"scheduled"}}}"#,
    )
    .expect("lifecycle should be writable");

    let base = || {
        let mut command = instrumented_binary();
        command
            .arg("--data-dir")
            .arg(data_dir.path())
            .arg("--output-dir")
            .arg(output_dir.path());
        command
    };
    let discovered = base()
        .arg("--run-id")
        .arg("fleet-cli-test")
        .arg("fleet-discover")
        .arg("--lifecycle")
        .arg(&lifecycle)
        .arg("--queue-dir")
        .arg(&queue_dir)
        .arg("--snapshot")
        .arg("2026-08")
        .output()
        .expect("fleet discovery should run");
    assert!(
        discovered.status.success(),
        "{}",
        String::from_utf8_lossy(&discovered.stderr)
    );
    assert!(queue_dir.join("pending/testwiki.json").is_file());

    let claim = base()
        .arg("fleet-claim")
        .arg("--queue-dir")
        .arg(&queue_dir)
        .arg("--resource-class")
        .arg("small")
        .arg("--worker-id")
        .arg("cli-worker")
        .arg("--receipt")
        .arg(&receipt)
        .output()
        .expect("fleet claim should run");
    assert!(claim.status.success());
    assert!(receipt.is_file());

    let heartbeat = base()
        .arg("fleet-heartbeat")
        .arg("--queue-dir")
        .arg(&queue_dir)
        .arg("--receipt")
        .arg(&receipt)
        .output()
        .expect("fleet heartbeat should run");
    assert!(heartbeat.status.success());

    fs::create_dir_all(output_dir.path().join("_ready-index"))
        .expect("ready index directory should be writable");
    fs::write(
        output_dir.path().join("_ready-index/testwiki.json"),
        r#"{"schema_version":1,"wiki":"testwiki","newest_valid_ready":{"snapshot":"2026-08"}}"#,
    )
    .expect("ready index should be writable");
    let complete = base()
        .arg("fleet-complete")
        .arg("--queue-dir")
        .arg(&queue_dir)
        .arg("--receipt")
        .arg(&receipt)
        .output()
        .expect("fleet completion should run");
    assert!(
        complete.status.success(),
        "{}",
        String::from_utf8_lossy(&complete.stderr)
    );

    let rediscovered = base()
        .arg("--run-id")
        .arg("fleet-cli-test-2")
        .arg("fleet-discover")
        .arg("--lifecycle")
        .arg(&lifecycle)
        .arg("--queue-dir")
        .arg(&queue_dir)
        .arg("--snapshot")
        .arg("2026-09")
        .output()
        .expect("changed snapshot discovery should run");
    assert!(rediscovered.status.success());
    let retry_claim = base()
        .arg("fleet-claim")
        .arg("--queue-dir")
        .arg(&queue_dir)
        .arg("--resource-class")
        .arg("small")
        .arg("--worker-id")
        .arg("retry-worker")
        .arg("--receipt")
        .arg(&receipt)
        .output()
        .expect("changed snapshot claim should run");
    assert!(retry_claim.status.success());
    let failed = base()
        .arg("fleet-fail")
        .arg("--queue-dir")
        .arg(&queue_dir)
        .arg("--receipt")
        .arg(&receipt)
        .arg("--error")
        .arg("fixture failure")
        .output()
        .expect("fleet failure should run");
    assert!(failed.status.success());

    let recover = base()
        .arg("fleet-recover")
        .arg("--queue-dir")
        .arg(&queue_dir)
        .output()
        .expect("fleet recovery should run");
    assert!(recover.status.success());

    let no_claim_receipt = data_dir.path().join("no-claim.json");
    let no_claim = base()
        .arg("fleet-claim")
        .arg("--queue-dir")
        .arg(&queue_dir)
        .arg("--resource-class")
        .arg("small")
        .arg("--worker-id")
        .arg("idle-worker")
        .arg("--receipt")
        .arg(&no_claim_receipt)
        .output()
        .expect("empty fleet claim should run");
    assert!(no_claim.status.success());
    assert!(!no_claim_receipt.exists());
    assert!(String::from_utf8_lossy(&no_claim.stdout).contains("\"claimed\":false"));
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
