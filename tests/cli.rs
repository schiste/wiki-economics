use std::process::Command;

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
fn binary_entrypoint_runs_merge_command() {
    init_test_tracing();
    let output_dir = TestDir::new().expect("temp dir");

    let status = instrumented_binary()
        .arg("--output-dir")
        .arg(output_dir.path())
        .arg("merge")
        .status()
        .expect("binary should run");

    assert!(status.success());
}

#[test]
fn binary_entrypoint_reports_monthly_fetch_error() {
    init_test_tracing();
    let data_dir = TestDir::new().expect("temp dir");

    let output = instrumented_binary()
        .arg("--data-dir")
        .arg(data_dir.path())
        .arg("fetch")
        .arg("enwiki")
        .output()
        .expect("binary should run");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("not yet supported"));
}
