use anyhow::{Context, Result};
use serde::Serialize;
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::compute::{
    ResourcePeak, WeeklyAggregationConfig, WeeklyAggregationReport, benchmark_page_weekly_edits,
};
use crate::{observability::MemorySnapshot, storage};

const REPORT_SCHEMA_VERSION: u32 = 1;

#[derive(Clone, Debug, Serialize)]
pub struct CapacityBenchmarkReport {
    pub schema_version: u32,
    pub wiki: String,
    pub bucket_count: usize,
    pub scratch_root: String,
    pub raw_transient_requirement_bytes: u64,
    pub current_generation_bytes: u64,
    pub estimated_rollover_additional_bytes: u64,
    pub nfs_quota_bytes: u64,
    pub quota_root_bytes: u64,
    pub quota_available_bytes: u64,
    pub filesystem_available_bytes: u64,
    pub effective_available_bytes: u64,
    pub storage_gate_passed: bool,
    pub minimum_memory_headroom_percent: u8,
    pub observed_memory_peak_bytes: u64,
    pub memory_limit_bytes: u64,
    pub observed_memory_headroom_percent: f64,
    pub memory_gate_passed: bool,
    pub output_sha256: String,
    pub aggregation: WeeklyAggregationReport,
}

pub struct CapacityBenchmarkOptions<'a> {
    pub wiki: &'a str,
    pub data_dir: &'a Path,
    pub output_dir: &'a Path,
    pub scratch_root: &'a Path,
    pub quota_root: &'a Path,
    pub report_path: &'a Path,
    pub bucket_count: usize,
    pub raw_transient_requirement_bytes: u64,
    pub nfs_quota_bytes: u64,
    pub minimum_memory_headroom_percent: u8,
    pub telemetry_override: Option<MemorySnapshot>,
}

pub fn run(options: CapacityBenchmarkOptions<'_>) -> Result<CapacityBenchmarkReport> {
    anyhow::ensure!(
        options.minimum_memory_headroom_percent <= 100,
        "minimum memory headroom percent must be at most 100"
    );
    anyhow::ensure!(
        options.nfs_quota_bytes > 0,
        "capacity benchmark requires a verified positive NFS quota"
    );
    anyhow::ensure!(
        options.quota_root.is_dir(),
        "capacity quota root does not exist: {}",
        options.quota_root.display()
    );
    let config = WeeklyAggregationConfig::new(
        options.bucket_count,
        Some(options.scratch_root.to_path_buf()),
    )?;
    let filesystem_available_bytes =
        fs4::available_space(options.scratch_root).with_context(|| {
            format!(
                "failed to inspect scratch filesystem {}",
                options.scratch_root.display()
            )
        })?;
    let quota_root_bytes = directory_bytes(options.quota_root)?;
    let quota_available_bytes = options.nfs_quota_bytes.saturating_sub(quota_root_bytes);
    let effective_available_bytes = filesystem_available_bytes.min(quota_available_bytes);
    let current_generation_bytes = active_generation_bytes(options.data_dir, options.wiki)?;

    let mut aggregation =
        benchmark_page_weekly_edits(options.wiki, options.data_dir, options.output_dir, &config)?;
    if let Some(telemetry) = options.telemetry_override {
        aggregation.final_memory = telemetry;
    }
    let output_path = options
        .output_dir
        .join(options.wiki)
        .join("page_weekly_edits.parquet");
    let (_, output_sha256) = storage::sha256_file(&output_path)?;
    let estimated_rollover_additional_bytes = options
        .raw_transient_requirement_bytes
        .checked_add(current_generation_bytes)
        .and_then(|value| value.checked_add(aggregation.scratch_peak_bytes))
        .and_then(|value| value.checked_add(aggregation.output_bytes))
        .context("rollover storage estimate overflow")?;
    let storage_gate_passed = effective_available_bytes >= estimated_rollover_additional_bytes;

    let observed_memory_peak_bytes = observed_memory_peak(&aggregation)?;
    let memory_limit_bytes = aggregation
        .final_memory
        .cgroup_limit_bytes
        .context("capacity benchmark requires a finite cgroup memory limit")?;
    let observed_memory_headroom_percent =
        memory_headroom_percent(observed_memory_peak_bytes, memory_limit_bytes)?;
    let memory_gate_passed =
        observed_memory_headroom_percent >= f64::from(options.minimum_memory_headroom_percent);

    let report = CapacityBenchmarkReport {
        schema_version: REPORT_SCHEMA_VERSION,
        wiki: options.wiki.to_string(),
        bucket_count: options.bucket_count,
        scratch_root: options.scratch_root.to_string_lossy().into_owned(),
        raw_transient_requirement_bytes: options.raw_transient_requirement_bytes,
        current_generation_bytes,
        estimated_rollover_additional_bytes,
        nfs_quota_bytes: options.nfs_quota_bytes,
        quota_root_bytes,
        quota_available_bytes,
        filesystem_available_bytes,
        effective_available_bytes,
        storage_gate_passed,
        minimum_memory_headroom_percent: options.minimum_memory_headroom_percent,
        observed_memory_peak_bytes,
        memory_limit_bytes,
        observed_memory_headroom_percent,
        memory_gate_passed,
        output_sha256,
        aggregation,
    };
    atomic_write_json(options.report_path, &report)?;

    anyhow::ensure!(
        report.storage_gate_passed,
        "frwiki storage gate failed: {} effective available bytes, {} estimated additional bytes required",
        report.effective_available_bytes,
        report.estimated_rollover_additional_bytes
    );
    anyhow::ensure!(
        report.memory_gate_passed,
        "frwiki memory gate failed: {:.2}% headroom, at least {}% required",
        report.observed_memory_headroom_percent,
        report.minimum_memory_headroom_percent
    );
    Ok(report)
}

fn observed_memory_peak(report: &WeeklyAggregationReport) -> Result<u64> {
    let sampled = [
        report.reduction_peak,
        report.reconciliation_peak,
        ResourcePeak {
            rss_bytes: report.final_memory.rss_bytes,
            cgroup_current_bytes: report.final_memory.cgroup_current_bytes,
            scratch_bytes: Some(0),
        },
    ]
    .into_iter()
    .filter_map(|peak| peak.cgroup_current_bytes)
    .max();
    sampled
        .into_iter()
        .chain(report.final_memory.cgroup_peak_bytes)
        .max()
        .context("capacity benchmark requires cgroup memory telemetry")
}

fn memory_headroom_percent(peak_bytes: u64, limit_bytes: u64) -> Result<f64> {
    anyhow::ensure!(limit_bytes > 0, "cgroup memory limit must be positive");
    let used_ratio = peak_bytes as f64 / limit_bytes as f64;
    Ok(((1.0 - used_ratio).max(0.0)) * 100.0)
}

fn active_generation_bytes(data_dir: &Path, wiki: &str) -> Result<u64> {
    let analytical = storage::active_analytical_wiki_dir(data_dir, wiki)?;
    let warehouse = storage::active_warehouse_wiki_dir(data_dir, wiki)?;
    directory_bytes(&analytical)?
        .checked_add(directory_bytes(&warehouse)?)
        .context("active generation byte count overflow")
}

fn directory_bytes(path: &Path) -> Result<u64> {
    if !path.exists() {
        return Ok(0);
    }
    let mut total = 0_u64;
    let mut pending = vec![path.to_path_buf()];
    while let Some(directory) = pending.pop() {
        for entry in fs::read_dir(&directory)? {
            let entry = entry?;
            let file_type = entry.file_type()?;
            if file_type.is_dir() {
                pending.push(entry.path());
            } else {
                let metadata = entry.path().symlink_metadata()?;
                total = total
                    .checked_add(metadata.len())
                    .context("directory byte count overflow")?;
            }
        }
    }
    Ok(total)
}

fn atomic_write_json(path: &Path, report: &CapacityBenchmarkReport) -> Result<()> {
    let parent = path
        .parent()
        .context("capacity report path has no parent")?;
    fs::create_dir_all(parent)?;
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let temporary = parent.join(format!(
        ".{}.{}-{nonce}.tmp",
        path.file_name()
            .and_then(|name| name.to_str())
            .context("capacity report has no valid filename")?,
        std::process::id()
    ));
    let result = (|| -> Result<()> {
        let mut file = File::create(&temporary)?;
        serde_json::to_writer_pretty(&mut file, report)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        fs::rename(&temporary, path)?;
        File::open(parent)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::TestDir;
    use polars::prelude::*;

    fn write_weekly_fixture(data_dir: &Path, wiki: &str) -> Result<()> {
        for (month, timestamps) in [
            (
                "2026-01",
                vec!["2026-01-05 00:00:00.0", "2026-01-06 00:00:00.0"],
            ),
            ("2026-02", vec!["2026-02-02 00:00:00.0"]),
        ] {
            let directory = data_dir
                .join("warehouse")
                .join(wiki)
                .join("year=2026")
                .join(format!("year_month={month}"));
            fs::create_dir_all(&directory)?;
            let rows = timestamps.len();
            let mut frame = df!(
                "event_timestamp" => timestamps,
                "page_id" => vec![42_i64; rows],
                "page_namespace" => vec![0_i32; rows],
                "page_title" => vec!["Capacity"; rows],
            )
            .expect("capacity fixture frame");
            ParquetWriter::new(File::create(directory.join("part.parquet"))?).finish(&mut frame)?;
        }
        Ok(())
    }

    fn telemetry(peak: u64, limit: u64) -> MemorySnapshot {
        MemorySnapshot {
            rss_bytes: Some(peak / 2),
            cgroup_current_bytes: Some(peak),
            cgroup_peak_bytes: Some(peak),
            cgroup_limit_bytes: Some(limit),
        }
    }

    #[test]
    fn memory_headroom_is_bounded_and_percentage_based() -> Result<()> {
        assert_eq!(memory_headroom_percent(3, 4)?, 25.0);
        assert_eq!(memory_headroom_percent(5, 4)?, 0.0);
        assert!(memory_headroom_percent(1, 0).is_err());
        Ok(())
    }

    #[test]
    fn directory_bytes_counts_nested_regular_files() -> Result<()> {
        let root = TestDir::new()?;
        fs::create_dir_all(root.path().join("nested"))?;
        fs::write(root.path().join("one"), b"123")?;
        fs::write(root.path().join("nested/two"), b"4567")?;
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink("nested", root.path().join("nested-link"))?;
            assert_eq!(directory_bytes(root.path())?, 13);
        }
        #[cfg(not(unix))]
        assert_eq!(directory_bytes(root.path())?, 7);
        assert_eq!(directory_bytes(&root.path().join("missing"))?, 0);
        Ok(())
    }

    #[test]
    fn capacity_run_writes_atomic_report_and_enforces_memory_gate() -> Result<()> {
        let data = TestDir::new()?;
        let output = TestDir::new()?;
        let scratch = TestDir::new()?;
        let reports = TestDir::new()?;
        write_weekly_fixture(data.path(), "frwiki")?;
        let report_path = reports.path().join("pass.json");

        let report = run(CapacityBenchmarkOptions {
            wiki: "frwiki",
            data_dir: data.path(),
            output_dir: output.path(),
            scratch_root: scratch.path(),
            quota_root: data.path(),
            report_path: &report_path,
            bucket_count: 256,
            raw_transient_requirement_bytes: 0,
            nfs_quota_bytes: 1_000_000_000,
            minimum_memory_headroom_percent: 25,
            telemetry_override: Some(telemetry(50, 100)),
        })?;
        assert!(report.memory_gate_passed);
        assert!(report.storage_gate_passed);
        assert_eq!(report.observed_memory_headroom_percent, 50.0);
        assert_eq!(report.aggregation.total_edits, 3);
        assert_eq!(report.output_sha256.len(), 64);
        let stored: serde_json::Value = serde_json::from_slice(&fs::read(&report_path)?)?;
        assert_eq!(stored["schema_version"], REPORT_SCHEMA_VERSION);

        let storage_failed_report = reports.path().join("storage-failed.json");
        let error = run(CapacityBenchmarkOptions {
            wiki: "frwiki",
            data_dir: data.path(),
            output_dir: output.path(),
            scratch_root: scratch.path(),
            quota_root: data.path(),
            report_path: &storage_failed_report,
            bucket_count: 256,
            raw_transient_requirement_bytes: 1,
            nfs_quota_bytes: 1,
            minimum_memory_headroom_percent: 25,
            telemetry_override: Some(telemetry(50, 100)),
        })
        .expect_err("exhausted confirmed quota should fail the storage gate");
        assert!(error.to_string().contains("storage gate failed"));
        assert!(storage_failed_report.is_file());

        let failed_report = reports.path().join("failed.json");
        let error = run(CapacityBenchmarkOptions {
            wiki: "frwiki",
            data_dir: data.path(),
            output_dir: output.path(),
            scratch_root: scratch.path(),
            quota_root: data.path(),
            report_path: &failed_report,
            bucket_count: 256,
            raw_transient_requirement_bytes: 0,
            nfs_quota_bytes: 1_000_000_000,
            minimum_memory_headroom_percent: 25,
            telemetry_override: Some(telemetry(80, 100)),
        })
        .expect_err("20% memory headroom should fail a 25% gate");
        assert!(error.to_string().contains("memory gate failed"));
        assert!(failed_report.is_file());

        let blocked_report = reports.path().join("blocked.json");
        fs::create_dir(&blocked_report)?;
        assert!(atomic_write_json(&blocked_report, &report).is_err());
        assert!(fs::read_dir(reports.path())?.all(|entry| {
            !entry
                .expect("report directory entry")
                .file_name()
                .to_string_lossy()
                .ends_with(".tmp")
        }));
        Ok(())
    }

    #[test]
    fn capacity_run_rejects_ambiguous_capacity_inputs() -> Result<()> {
        let data = TestDir::new()?;
        let output = TestDir::new()?;
        let scratch = TestDir::new()?;
        let reports = TestDir::new()?;
        write_weekly_fixture(data.path(), "frwiki")?;
        let report = reports.path().join("invalid.json");
        let missing_quota = reports.path().join("missing-quota");
        let missing_scratch = reports.path().join("missing-scratch");

        let invalid = [
            (101, 1_000_000_000, data.path(), scratch.path(), 256),
            (25, 0, data.path(), scratch.path(), 256),
            (
                25,
                1_000_000_000,
                missing_quota.as_path(),
                scratch.path(),
                256,
            ),
            (
                25,
                1_000_000_000,
                data.path(),
                missing_scratch.as_path(),
                256,
            ),
            (25, 1_000_000_000, data.path(), scratch.path(), 128),
        ];
        for (minimum_headroom, quota, quota_root, scratch_root, buckets) in invalid {
            let result = run(CapacityBenchmarkOptions {
                wiki: "frwiki",
                data_dir: data.path(),
                output_dir: output.path(),
                scratch_root,
                quota_root,
                report_path: &report,
                bucket_count: buckets,
                raw_transient_requirement_bytes: 0,
                nfs_quota_bytes: quota,
                minimum_memory_headroom_percent: minimum_headroom,
                telemetry_override: Some(telemetry(50, 100)),
            });
            assert!(result.is_err());
        }

        let overflow = run(CapacityBenchmarkOptions {
            wiki: "frwiki",
            data_dir: data.path(),
            output_dir: output.path(),
            scratch_root: scratch.path(),
            quota_root: data.path(),
            report_path: &report,
            bucket_count: 256,
            raw_transient_requirement_bytes: u64::MAX,
            nfs_quota_bytes: u64::MAX,
            minimum_memory_headroom_percent: 25,
            telemetry_override: Some(telemetry(50, 100)),
        })
        .expect_err("rollover arithmetic must fail closed on overflow");
        assert!(overflow.to_string().contains("overflow"));

        let missing_telemetry = run(CapacityBenchmarkOptions {
            wiki: "frwiki",
            data_dir: data.path(),
            output_dir: output.path(),
            scratch_root: scratch.path(),
            quota_root: data.path(),
            report_path: &report,
            bucket_count: 256,
            raw_transient_requirement_bytes: 0,
            nfs_quota_bytes: 1_000_000_000,
            minimum_memory_headroom_percent: 25,
            telemetry_override: Some(MemorySnapshot::default()),
        })
        .expect_err("missing cgroup telemetry must fail closed");
        assert!(
            missing_telemetry
                .to_string()
                .contains("cgroup memory telemetry")
        );
        Ok(())
    }
}
