use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

const REPORT_SCHEMA_VERSION: u32 = 1;
const CAPACITY_REPORT_SCHEMA_VERSION: u32 = 5;
const REQUIRED_WIKIS: [&str; 3] = ["nlwiki", "ptwiki", "frwiki"];
const MINIMUM_MULTICPU_SPEEDUP: f64 = 1.15;
const CPU_LIMIT_TOLERANCE: f64 = 0.05;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub(crate) struct CpuProfile {
    pub cpu: usize,
    pub threads: usize,
    pub weekly_workers: usize,
}

const REQUIRED_PROFILES: [CpuProfile; 4] = [
    CpuProfile {
        cpu: 1,
        threads: 1,
        weekly_workers: 1,
    },
    CpuProfile {
        cpu: 2,
        threads: 2,
        weekly_workers: 1,
    },
    CpuProfile {
        cpu: 4,
        threads: 3,
        weekly_workers: 1,
    },
    CpuProfile {
        cpu: 4,
        threads: 3,
        weekly_workers: 2,
    },
];
const TWO_THREAD_PROFILES: [CpuProfile; 2] = [REQUIRED_PROFILES[0], REQUIRED_PROFILES[1]];

#[derive(Debug, Deserialize)]
struct CapacityReceipt {
    schema_version: u32,
    wiki: String,
    selected_snapshot: String,
    requested_cpu: usize,
    rayon_threads: usize,
    polars_threads: usize,
    weekly_workers: usize,
    cgroup_cpu_limit_cores: Option<f64>,
    cpu_utilization_cores: Option<f64>,
    read_bytes_per_second: Option<u64>,
    write_bytes_per_second: Option<u64>,
    observed_memory_peak_bytes: u64,
    memory_limit_bytes: u64,
    observed_memory_headroom_percent: f64,
    memory_gate_passed: bool,
    storage_gate_passed: bool,
    output_sha256: String,
    aggregation: AggregationReceipt,
}

#[derive(Debug, Deserialize)]
struct AggregationReceipt {
    elapsed_ms: u64,
    scratch_peak_bytes: u64,
    working_storage_peak_bytes: u64,
    resources: ResourceReceipt,
}

#[derive(Debug, Deserialize)]
struct ResourceReceipt {
    samples: u64,
    rss_peak_bytes: Option<u64>,
    cgroup_current_peak_bytes: Option<u64>,
    cgroup_reported_peak_bytes: Option<u64>,
    page_cache_peak_bytes: Option<u64>,
    cpu_usage_usec: Option<u64>,
    cpu_user_usec: Option<u64>,
    cpu_system_usec: Option<u64>,
    cpu_periods: Option<u64>,
    cpu_throttled_periods: Option<u64>,
    cpu_throttled_usec: Option<u64>,
    read_bytes: Option<u64>,
    write_bytes: Option<u64>,
}

#[derive(Debug, Serialize)]
pub(crate) struct CpuQualificationReport {
    schema_version: u32,
    generated_at_unix: u64,
    complete: bool,
    qualification_scope: String,
    deterministic: bool,
    telemetry_complete: bool,
    minimum_memory_headroom_percent: f64,
    selected_profile: Option<CpuProfile>,
    selected_profile_speedup: Option<f64>,
    promotion_eligible: bool,
    failures: Vec<String>,
    profiles: Vec<ProfileSummary>,
    cells: Vec<CellSummary>,
}

#[derive(Debug, Serialize)]
struct ProfileSummary {
    profile: CpuProfile,
    total_wall_ms: u64,
    speedup_over_baseline: f64,
    mean_cpu_utilization_cores: f64,
    maximum_throttled_percent: f64,
}

#[derive(Debug, Serialize)]
struct CellSummary {
    wiki: String,
    snapshot: String,
    profile: CpuProfile,
    wall_ms: u64,
    cpu_time_usec: u64,
    cpu_user_usec: u64,
    cpu_system_usec: u64,
    throttled_usec: u64,
    throttled_periods: u64,
    cpu_periods: u64,
    throttled_percent: f64,
    rss_peak_bytes: u64,
    cgroup_peak_bytes: u64,
    page_cache_peak_bytes: u64,
    scratch_peak_bytes: u64,
    working_storage_peak_bytes: u64,
    read_bytes: u64,
    write_bytes: u64,
    read_bytes_per_second: u64,
    write_bytes_per_second: u64,
    output_sha256: String,
    memory_headroom_percent: f64,
}

pub(crate) fn run(paths: &[PathBuf], report_path: &Path) -> Result<CpuQualificationReport> {
    let mut failures = Vec::new();
    let mut receipts = BTreeMap::new();
    for path in paths {
        let file = File::open(path)
            .with_context(|| format!("failed to open capacity report {}", path.display()))?;
        let receipt: CapacityReceipt = serde_json::from_reader(file)
            .with_context(|| format!("invalid capacity report {}", path.display()))?;
        anyhow::ensure!(
            receipt.schema_version == CAPACITY_REPORT_SCHEMA_VERSION,
            "capacity report {} has unsupported schema {}",
            path.display(),
            receipt.schema_version
        );
        let profile = CpuProfile {
            cpu: receipt.requested_cpu,
            threads: receipt.rayon_threads,
            weekly_workers: receipt.weekly_workers,
        };
        let key = (receipt.wiki.clone(), profile);
        anyhow::ensure!(
            receipts.insert(key.clone(), receipt).is_none(),
            "duplicate CPU qualification cell for {} {:?}",
            key.0,
            key.1
        );
    }

    let required_profiles = qualification_profiles(&receipts);
    let required_keys = REQUIRED_WIKIS
        .into_iter()
        .flat_map(|wiki| {
            required_profiles
                .iter()
                .copied()
                .map(move |profile| (wiki.to_string(), profile))
        })
        .collect::<BTreeSet<_>>();
    let actual_keys = receipts.keys().cloned().collect::<BTreeSet<_>>();
    for missing in required_keys.difference(&actual_keys) {
        failures.push(format!(
            "missing matrix cell for {} cpu={} threads={} weekly_workers={}",
            missing.0, missing.1.cpu, missing.1.threads, missing.1.weekly_workers
        ));
    }
    for unexpected in actual_keys.difference(&required_keys) {
        failures.push(format!(
            "unexpected matrix cell for {} cpu={} threads={} weekly_workers={}",
            unexpected.0, unexpected.1.cpu, unexpected.1.threads, unexpected.1.weekly_workers
        ));
    }

    let mut cells = Vec::new();
    let mut deterministic = true;
    let mut telemetry_complete = true;
    let mut minimum_headroom = 100.0_f64;
    for wiki in REQUIRED_WIKIS {
        let wiki_receipts = required_profiles
            .iter()
            .copied()
            .filter_map(|profile| receipts.get(&(wiki.to_string(), profile)))
            .collect::<Vec<_>>();
        let snapshots = wiki_receipts
            .iter()
            .map(|receipt| receipt.selected_snapshot.as_str())
            .collect::<BTreeSet<_>>();
        if snapshots.len() > 1 {
            failures.push(format!("{wiki} matrix cells use different snapshots"));
        }
        let hashes = wiki_receipts
            .iter()
            .map(|receipt| receipt.output_sha256.as_str())
            .collect::<BTreeSet<_>>();
        if hashes.len() > 1 {
            deterministic = false;
            failures.push(format!(
                "{wiki} output hashes differ across CPU/worker profiles"
            ));
        }
        for receipt in wiki_receipts {
            if receipt.polars_threads != receipt.rayon_threads {
                failures.push(format!(
                    "{} has mismatched Polars ({}) and Rayon ({}) thread limits",
                    receipt.wiki, receipt.polars_threads, receipt.rayon_threads
                ));
            }
            match receipt.cgroup_cpu_limit_cores {
                Some(limit)
                    if (limit - receipt.requested_cpu as f64).abs() <= CPU_LIMIT_TOLERANCE => {}
                Some(limit) => failures.push(format!(
                    "{} requested {} CPU but cgroup exposes {limit:.2}",
                    receipt.wiki, receipt.requested_cpu
                )),
                None => failures.push(format!(
                    "{} has no finite cgroup CPU quota telemetry",
                    receipt.wiki
                )),
            }
            if !receipt.memory_gate_passed || !receipt.storage_gate_passed {
                failures.push(format!(
                    "{} {:?} failed a capacity gate",
                    receipt.wiki,
                    CpuProfile {
                        cpu: receipt.requested_cpu,
                        threads: receipt.rayon_threads,
                        weekly_workers: receipt.weekly_workers,
                    }
                ));
            }
            minimum_headroom = minimum_headroom.min(receipt.observed_memory_headroom_percent);
            let resources = &receipt.aggregation.resources;
            let required_telemetry = resources.samples >= 2
                && resources.rss_peak_bytes.is_some()
                && resources.cgroup_current_peak_bytes.is_some()
                && resources.cgroup_reported_peak_bytes.is_some()
                && resources.page_cache_peak_bytes.is_some()
                && resources.cpu_usage_usec.is_some()
                && resources.cpu_user_usec.is_some()
                && resources.cpu_system_usec.is_some()
                && resources.cpu_periods.is_some()
                && resources.cpu_throttled_periods.is_some()
                && resources.cpu_throttled_usec.is_some()
                && resources.read_bytes.is_some()
                && resources.write_bytes.is_some()
                && receipt.cpu_utilization_cores.is_some()
                && receipt.read_bytes_per_second.is_some()
                && receipt.write_bytes_per_second.is_some();
            if !required_telemetry {
                telemetry_complete = false;
                failures.push(format!(
                    "{} {:?} has incomplete resource telemetry",
                    receipt.wiki,
                    CpuProfile {
                        cpu: receipt.requested_cpu,
                        threads: receipt.rayon_threads,
                        weekly_workers: receipt.weekly_workers,
                    }
                ));
                continue;
            }
            let cpu_periods = resources.cpu_periods.unwrap_or(0);
            let throttled_periods = resources.cpu_throttled_periods.unwrap_or(0);
            cells.push(CellSummary {
                wiki: receipt.wiki.clone(),
                snapshot: receipt.selected_snapshot.clone(),
                profile: CpuProfile {
                    cpu: receipt.requested_cpu,
                    threads: receipt.rayon_threads,
                    weekly_workers: receipt.weekly_workers,
                },
                wall_ms: receipt.aggregation.elapsed_ms,
                cpu_time_usec: resources.cpu_usage_usec.unwrap_or(0),
                cpu_user_usec: resources.cpu_user_usec.unwrap_or(0),
                cpu_system_usec: resources.cpu_system_usec.unwrap_or(0),
                throttled_usec: resources.cpu_throttled_usec.unwrap_or(0),
                throttled_periods,
                cpu_periods,
                throttled_percent: percentage(throttled_periods, cpu_periods),
                rss_peak_bytes: resources.rss_peak_bytes.unwrap_or(0),
                cgroup_peak_bytes: resources
                    .cgroup_current_peak_bytes
                    .unwrap_or(0)
                    .max(resources.cgroup_reported_peak_bytes.unwrap_or(0)),
                page_cache_peak_bytes: resources.page_cache_peak_bytes.unwrap_or(0),
                scratch_peak_bytes: receipt.aggregation.scratch_peak_bytes,
                working_storage_peak_bytes: receipt.aggregation.working_storage_peak_bytes,
                read_bytes: resources.read_bytes.unwrap_or(0),
                write_bytes: resources.write_bytes.unwrap_or(0),
                read_bytes_per_second: receipt.read_bytes_per_second.unwrap_or(0),
                write_bytes_per_second: receipt.write_bytes_per_second.unwrap_or(0),
                output_sha256: receipt.output_sha256.clone(),
                memory_headroom_percent: receipt.observed_memory_headroom_percent,
            });
            anyhow::ensure!(
                receipt.observed_memory_peak_bytes <= receipt.memory_limit_bytes,
                "{} observed memory peak exceeds its cgroup limit",
                receipt.wiki
            );
        }
    }

    if minimum_headroom < 25.0 {
        failures.push(format!(
            "minimum observed memory headroom is {minimum_headroom:.2}%, below 25%"
        ));
    }
    let complete = actual_keys == required_keys;
    let mut profiles = Vec::new();
    let baseline_total = total_wall(&cells, required_profiles[0]);
    for &profile in required_profiles {
        if let (Some(total_wall_ms), Some(baseline)) = (total_wall(&cells, profile), baseline_total)
        {
            let profile_cells = cells
                .iter()
                .filter(|cell| cell.profile == profile)
                .collect::<Vec<_>>();
            let utilization = profile_cells
                .iter()
                .filter_map(|cell| {
                    receipts
                        .get(&(cell.wiki.clone(), profile))?
                        .cpu_utilization_cores
                })
                .sum::<f64>()
                / profile_cells.len() as f64;
            let throttled = profile_cells
                .iter()
                .map(|cell| cell.throttled_percent)
                .fold(0.0_f64, f64::max);
            profiles.push(ProfileSummary {
                profile,
                total_wall_ms,
                speedup_over_baseline: baseline as f64 / total_wall_ms as f64,
                mean_cpu_utilization_cores: utilization,
                maximum_throttled_percent: throttled,
            });
        }
    }
    let selected = profiles.iter().min_by_key(|profile| profile.total_wall_ms);
    let selected_profile = selected.map(|profile| profile.profile);
    let selected_profile_speedup = selected.map(|profile| profile.speedup_over_baseline);
    let improvement_justified = selected_profile
        .zip(selected_profile_speedup)
        .is_some_and(|(profile, speedup)| profile.cpu > 1 && speedup >= MINIMUM_MULTICPU_SPEEDUP);
    if complete && !improvement_justified {
        failures.push(format!(
            "no multi-CPU profile improves aggregate wall time by at least {:.0}%",
            (MINIMUM_MULTICPU_SPEEDUP - 1.0) * 100.0
        ));
    }
    let promotion_eligible = complete
        && deterministic
        && telemetry_complete
        && minimum_headroom >= 25.0
        && improvement_justified
        && failures.is_empty();
    let report = CpuQualificationReport {
        schema_version: REPORT_SCHEMA_VERSION,
        generated_at_unix: SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
        complete,
        qualification_scope: if required_profiles == TWO_THREAD_PROFILES.as_slice() {
            "two_thread".to_string()
        } else {
            "full_matrix".to_string()
        },
        deterministic,
        telemetry_complete,
        minimum_memory_headroom_percent: minimum_headroom,
        selected_profile,
        selected_profile_speedup,
        promotion_eligible,
        failures,
        profiles,
        cells,
    };
    atomic_write_json(report_path, &report)?;
    anyhow::ensure!(
        report.promotion_eligible,
        "CPU qualification failed; inspect {}",
        report_path.display()
    );
    Ok(report)
}

fn qualification_profiles(
    receipts: &BTreeMap<(String, CpuProfile), CapacityReceipt>,
) -> &'static [CpuProfile] {
    let observed = receipts
        .keys()
        .map(|(_, profile)| *profile)
        .collect::<BTreeSet<_>>();
    let two_thread = TWO_THREAD_PROFILES.into_iter().collect::<BTreeSet<_>>();
    if observed.is_subset(&two_thread) {
        &TWO_THREAD_PROFILES
    } else {
        &REQUIRED_PROFILES
    }
}

fn total_wall(cells: &[CellSummary], profile: CpuProfile) -> Option<u64> {
    let selected = cells
        .iter()
        .filter(|cell| cell.profile == profile)
        .collect::<Vec<_>>();
    (selected.len() == REQUIRED_WIKIS.len()).then(|| {
        selected
            .into_iter()
            .fold(0_u64, |total, cell| total.saturating_add(cell.wall_ms))
    })
}

fn percentage(numerator: u64, denominator: u64) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        numerator as f64 * 100.0 / denominator as f64
    }
}

fn atomic_write_json(path: &Path, report: &CpuQualificationReport) -> Result<()> {
    let parent = path
        .parent()
        .context("CPU qualification report has no parent")?;
    fs::create_dir_all(parent)?;
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let temporary = parent.join(format!(
        ".{}.{}-{nonce}.tmp",
        path.file_name()
            .and_then(|name| name.to_str())
            .context("CPU qualification report has no UTF-8 filename")?,
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
    use serde_json::json;

    fn write_matrix(root: &Path, changed_hash: bool) -> Result<Vec<PathBuf>> {
        let mut paths = Vec::new();
        for wiki in REQUIRED_WIKIS {
            for profile in REQUIRED_PROFILES {
                let wall_ms = 1_000_u64 / profile.cpu as u64
                    - if profile.weekly_workers == 2 { 100 } else { 0 };
                let hash = if changed_hash && wiki == "frwiki" && profile.weekly_workers == 2 {
                    "b".repeat(64)
                } else {
                    "a".repeat(64)
                };
                let path = root.join(format!(
                    "{wiki}-{}-{}-{}.json",
                    profile.cpu, profile.threads, profile.weekly_workers
                ));
                let receipt = serde_json::to_vec(&json!({
                    "schema_version": CAPACITY_REPORT_SCHEMA_VERSION,
                    "wiki": wiki,
                    "selected_snapshot": "2026-08",
                    "requested_cpu": profile.cpu,
                    "rayon_threads": profile.threads,
                    "polars_threads": profile.threads,
                    "weekly_workers": profile.weekly_workers,
                    "cgroup_cpu_limit_cores": profile.cpu as f64,
                    "cpu_utilization_cores": profile.cpu as f64 * 0.7,
                    "read_bytes_per_second": 100,
                    "write_bytes_per_second": 50,
                    "observed_memory_peak_bytes": 3_000,
                    "memory_limit_bytes": 6_000,
                    "observed_memory_headroom_percent": 50.0,
                    "memory_gate_passed": true,
                    "storage_gate_passed": true,
                    "output_sha256": hash,
                    "aggregation": {
                        "elapsed_ms": wall_ms,
                        "scratch_peak_bytes": 200,
                        "working_storage_peak_bytes": 300,
                        "resources": {
                            "samples": 2,
                            "rss_peak_bytes": 2_000,
                            "cgroup_current_peak_bytes": 3_000,
                            "cgroup_reported_peak_bytes": 3_000,
                            "page_cache_peak_bytes": 500,
                            "cpu_usage_usec": wall_ms * 700,
                            "cpu_user_usec": wall_ms * 600,
                            "cpu_system_usec": wall_ms * 100,
                            "cpu_periods": 100,
                            "cpu_throttled_periods": 2,
                            "cpu_throttled_usec": 10,
                            "read_bytes": 1_000,
                            "write_bytes": 500
                        }
                    }
                }))?;
                fs::write(&path, receipt)?;
                paths.push(path);
            }
        }
        Ok(paths)
    }

    fn update_receipt(path: &Path, update: impl FnOnce(&mut serde_json::Value)) -> Result<()> {
        let mut value: serde_json::Value = serde_json::from_slice(&fs::read(path)?)?;
        update(&mut value);
        fs::write(path, serde_json::to_vec(&value)?)?;
        Ok(())
    }

    #[test]
    fn complete_deterministic_matrix_selects_fastest_qualified_profile() -> Result<()> {
        let root = TestDir::new()?;
        let paths = write_matrix(root.path(), false)?;
        let report_path = root.path().join("qualification.json");
        let report = run(&paths, &report_path)?;
        assert!(report.promotion_eligible);
        assert_eq!(report.selected_profile, Some(REQUIRED_PROFILES[3]));
        assert!(report_path.is_file());
        Ok(())
    }

    #[test]
    fn controlled_two_thread_matrix_can_qualify_without_four_cpu_quota() -> Result<()> {
        let root = TestDir::new()?;
        let paths = write_matrix(root.path(), false)?
            .into_iter()
            .filter(|path| {
                let receipt: serde_json::Value =
                    serde_json::from_slice(&fs::read(path).expect("capacity receipt"))
                        .expect("JSON");
                receipt["requested_cpu"]
                    .as_u64()
                    .is_some_and(|cpu| cpu <= 2)
            })
            .collect::<Vec<_>>();
        let report = run(&paths, &root.path().join("two-thread.json"))?;
        assert!(report.promotion_eligible);
        assert_eq!(report.qualification_scope, "two_thread");
        assert_eq!(report.selected_profile, Some(TWO_THREAD_PROFILES[1]));
        Ok(())
    }

    #[test]
    fn differing_worker_output_fails_closed_but_retains_report() -> Result<()> {
        let root = TestDir::new()?;
        let paths = write_matrix(root.path(), true)?;
        let report_path = root.path().join("qualification.json");
        let error = run(&paths, &report_path).expect_err("hash mismatch must reject promotion");
        assert!(error.to_string().contains("CPU qualification failed"));
        let stored: serde_json::Value = serde_json::from_slice(&fs::read(report_path)?)?;
        assert_eq!(stored["deterministic"], false);
        assert_eq!(stored["promotion_eligible"], false);
        Ok(())
    }

    #[test]
    fn malformed_incomplete_and_unexpected_matrices_fail_closed() -> Result<()> {
        let invalid_schema = TestDir::new()?;
        let paths = write_matrix(invalid_schema.path(), false)?;
        update_receipt(&paths[0], |value| value["schema_version"] = json!(4))?;
        assert!(run(&paths, &invalid_schema.path().join("result.json")).is_err());

        let missing = TestDir::new()?;
        let mut paths = write_matrix(missing.path(), false)?;
        paths.pop();
        assert!(run(&paths, &missing.path().join("result.json")).is_err());

        let unexpected = TestDir::new()?;
        let paths = write_matrix(unexpected.path(), false)?;
        update_receipt(&paths[0], |value| value["wiki"] = json!("dewiki"))?;
        let report_path = unexpected.path().join("result.json");
        assert!(run(&paths, &report_path).is_err());
        let report: serde_json::Value = serde_json::from_slice(&fs::read(report_path)?)?;
        let failures = report["failures"].as_array().context("failure array")?;
        assert!(failures.iter().any(|failure| {
            failure
                .as_str()
                .is_some_and(|value| value.contains("unexpected"))
        }));
        Ok(())
    }

    #[test]
    fn semantic_resource_and_speedup_failures_are_retained() -> Result<()> {
        let invalid = TestDir::new()?;
        let paths = write_matrix(invalid.path(), false)?;
        update_receipt(&paths[0], |value| {
            value["selected_snapshot"] = json!("2026-07");
            value["polars_threads"] = json!(9);
            value["cgroup_cpu_limit_cores"] = json!(9.0);
            value["memory_gate_passed"] = json!(false);
            value["observed_memory_headroom_percent"] = json!(20.0);
        })?;
        update_receipt(&paths[1], |value| {
            value["cgroup_cpu_limit_cores"] = serde_json::Value::Null;
        })?;
        update_receipt(&paths[2], |value| {
            value["aggregation"]["resources"]["samples"] = json!(1);
        })?;
        assert!(run(&paths, &invalid.path().join("result.json")).is_err());

        let no_speedup = TestDir::new()?;
        let paths = write_matrix(no_speedup.path(), false)?;
        for path in &paths {
            update_receipt(path, |value| {
                value["aggregation"]["elapsed_ms"] = json!(1_000);
            })?;
        }
        assert!(run(&paths, &no_speedup.path().join("result.json")).is_err());

        let impossible_peak = TestDir::new()?;
        let paths = write_matrix(impossible_peak.path(), false)?;
        update_receipt(&paths[0], |value| {
            value["observed_memory_peak_bytes"] = json!(7_000);
        })?;
        assert!(run(&paths, &impossible_peak.path().join("result.json")).is_err());
        Ok(())
    }

    #[test]
    fn qualification_helpers_and_atomic_failure_paths_are_covered() -> Result<()> {
        assert_eq!(percentage(1, 0), 0.0);
        assert_eq!(percentage(1, 4), 25.0);
        assert_eq!(total_wall(&[], REQUIRED_PROFILES[0]), None);

        let root = TestDir::new()?;
        let paths = write_matrix(root.path(), false)?;
        let report_path = root.path().join("blocked.json");
        fs::create_dir(&report_path)?;
        assert!(run(&paths, &report_path).is_err());
        assert!(fs::read_dir(root.path())?.all(|entry| {
            !entry
                .expect("qualification fixture entry")
                .file_name()
                .to_string_lossy()
                .ends_with(".tmp")
        }));
        Ok(())
    }
}
