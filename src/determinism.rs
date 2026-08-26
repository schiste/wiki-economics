use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs::{self, File};
use std::io::Write;
use std::path::{Component, Path};

const CONTRACT_JSON: &str = include_str!("../config/determinism-contract.json");
const CONTRACT_SCHEMA_VERSION: u32 = 1;
pub(crate) const CONTRACT_VERSION: &str = "pipeline-byte-determinism-v1";
pub(crate) const PARTITION_HASH_ALGORITHM: &str = "splitmix64-finalizer";
pub(crate) const PARTITION_HASH_VERSION: u32 = 1;
pub(crate) const PARTITION_HASH_SEED: u64 = 0;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PartitionHashContract {
    pub(crate) algorithm: String,
    pub(crate) version: u32,
    pub(crate) seed_u64: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DeterminismContract {
    pub(crate) schema_version: u32,
    pub(crate) contract_version: String,
    pub(crate) digest_algorithm: String,
    pub(crate) partition_hash: PartitionHashContract,
    pub(crate) source_order: String,
    pub(crate) fragment_order: String,
    pub(crate) fragment_row_order: String,
    pub(crate) final_merge_order: String,
    pub(crate) parquet_metadata_policy: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ArtifactDigest {
    pub(crate) identity: String,
    pub(crate) bytes: u64,
    pub(crate) sha256: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ConcurrencyQualificationReport {
    pub(crate) schema_version: u32,
    pub(crate) determinism_contract: DeterminismContract,
    pub(crate) algorithm_version: String,
    pub(crate) artifact_extension: String,
    pub(crate) baseline_workers: usize,
    pub(crate) candidate_workers: usize,
    pub(crate) artifact_count: usize,
    pub(crate) aggregate_sha256: String,
    pub(crate) artifacts: Vec<ArtifactDigest>,
}

pub(crate) fn contract() -> Result<DeterminismContract> {
    let contract: DeterminismContract = serde_json::from_str(CONTRACT_JSON)?;
    validate_contract(&contract)?;
    Ok(contract)
}

fn validate_contract(contract: &DeterminismContract) -> Result<()> {
    ensure!(
        contract.schema_version == CONTRACT_SCHEMA_VERSION
            && contract.contract_version == CONTRACT_VERSION
            && contract.digest_algorithm == "SHA-256"
            && contract.partition_hash.algorithm == PARTITION_HASH_ALGORITHM
            && contract.partition_hash.version == PARTITION_HASH_VERSION
            && contract.partition_hash.seed_u64 == PARTITION_HASH_SEED,
        "embedded determinism contract does not match the Rust implementation"
    );
    for (label, value) in [
        ("source order", contract.source_order.as_str()),
        ("fragment order", contract.fragment_order.as_str()),
        ("fragment row order", contract.fragment_row_order.as_str()),
        ("final merge order", contract.final_merge_order.as_str()),
        (
            "Parquet metadata policy",
            contract.parquet_metadata_policy.as_str(),
        ),
    ] {
        ensure!(!value.is_empty(), "determinism contract has no {label}");
    }
    Ok(())
}

pub(crate) fn partition_algorithm_version(
    primary_buckets: usize,
    secondary_buckets: usize,
) -> String {
    format!(
        "{CONTRACT_VERSION}-{PARTITION_HASH_ALGORITHM}-v{PARTITION_HASH_VERSION}-seed{PARTITION_HASH_SEED:016x}-primary{primary_buckets}-secondary{secondary_buckets}"
    )
}

pub(crate) fn stable_page_hash(page_id: Option<i64>) -> u64 {
    let mut value = page_id.map_or(u64::MAX, |value| value as u64) ^ PARTITION_HASH_SEED;
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

pub(crate) fn qualify_concurrency(
    baseline_root: &Path,
    candidate_root: &Path,
    artifact_extension: &str,
    baseline_workers: usize,
    candidate_workers: usize,
    algorithm_version: &str,
    report_path: &Path,
) -> Result<ConcurrencyQualificationReport> {
    ensure!(
        baseline_workers > 0 && candidate_workers > 0,
        "qualification worker counts must be positive"
    );
    ensure!(
        baseline_workers != candidate_workers,
        "concurrency qualification requires two distinct worker counts"
    );
    ensure!(
        !algorithm_version.is_empty(),
        "concurrency qualification requires an algorithm version"
    );
    validate_extension(artifact_extension)?;
    let baseline = collect_artifacts(baseline_root, artifact_extension)?;
    let candidate = collect_artifacts(candidate_root, artifact_extension)?;
    ensure!(
        !baseline.is_empty(),
        "concurrency qualification found no .{artifact_extension} artifacts"
    );
    ensure!(
        baseline == candidate,
        "concurrency changed artifact identities or SHA-256 bytes"
    );
    let aggregate_sha256 = aggregate_digest(&baseline);
    let report = ConcurrencyQualificationReport {
        schema_version: 1,
        determinism_contract: contract()?,
        algorithm_version: algorithm_version.to_string(),
        artifact_extension: artifact_extension.to_string(),
        baseline_workers,
        candidate_workers,
        artifact_count: baseline.len(),
        aggregate_sha256,
        artifacts: baseline,
    };
    write_atomic_json(report_path, &report)?;
    Ok(report)
}

fn validate_extension(extension: &str) -> Result<()> {
    ensure!(
        !extension.is_empty()
            && extension
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit()),
        "artifact extension must be lowercase ASCII letters or digits"
    );
    Ok(())
}

pub(crate) fn collect_artifacts(root: &Path, extension: &str) -> Result<Vec<ArtifactDigest>> {
    ensure!(
        root.is_dir(),
        "artifact root is not a directory: {}",
        root.display()
    );
    let mut pending = vec![root.to_path_buf()];
    let mut paths = Vec::new();
    while let Some(directory) = pending.pop() {
        let mut entries = fs::read_dir(&directory)?.collect::<std::io::Result<Vec<_>>>()?;
        entries.sort_by_key(std::fs::DirEntry::file_name);
        for entry in entries {
            let file_type = entry.file_type()?;
            let path = entry.path();
            ensure!(!file_type.is_symlink(), "artifact set contains a symlink");
            if file_type.is_dir() {
                pending.push(path);
            } else {
                ensure!(
                    file_type.is_file(),
                    "artifact set contains an unsupported filesystem entry"
                );
                if path.extension().and_then(|value| value.to_str()) == Some(extension) {
                    paths.push(path);
                }
            }
        }
    }
    paths.sort();
    let mut artifacts = Vec::with_capacity(paths.len());
    for path in paths {
        let relative = path.strip_prefix(root)?;
        ensure!(
            relative
                .components()
                .all(|component| matches!(component, Component::Normal(_))),
            "artifact identity is not a safe relative path"
        );
        let identity = relative
            .components()
            .map(|component| component.as_os_str().to_string_lossy())
            .collect::<Vec<_>>()
            .join("/");
        let metadata = fs::metadata(&path)?;
        let (bytes, sha256) = crate::storage::sha256_file(&path)?;
        ensure!(bytes == metadata.len(), "artifact changed while hashing");
        artifacts.push(ArtifactDigest {
            identity,
            bytes,
            sha256,
        });
    }
    Ok(artifacts)
}

pub(crate) fn aggregate_digest(artifacts: &[ArtifactDigest]) -> String {
    let mut digest = Sha256::new();
    for artifact in artifacts {
        digest.update(artifact.identity.as_bytes());
        digest.update(b"\0");
        digest.update(artifact.bytes.to_string().as_bytes());
        digest.update(b"\0");
        digest.update(artifact.sha256.as_bytes());
        digest.update(b"\n");
    }
    hex::encode(digest.finalize())
}

fn write_atomic_json(path: &Path, report: &ConcurrencyQualificationReport) -> Result<()> {
    let parent = path
        .parent()
        .context("qualification report has no parent")?;
    fs::create_dir_all(parent)?;
    let temporary = parent.join(format!(
        ".{}.{}.tmp",
        path.file_name()
            .and_then(|name| name.to_str())
            .context("qualification report has no valid filename")?,
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

    #[test]
    fn embedded_contract_and_partition_hash_are_fixed() -> Result<()> {
        let embedded = contract()?;
        assert_eq!(embedded.partition_hash.seed_u64, 0);
        assert_eq!(stable_page_hash(Some(0)), 0);
        assert_eq!(stable_page_hash(Some(42)), 12_058_926_934_050_108_962);
        assert_eq!(stable_page_hash(None), 13_029_008_266_876_403_067);
        assert_eq!(
            partition_algorithm_version(32, 8),
            "pipeline-byte-determinism-v1-splitmix64-finalizer-v1-seed0000000000000000-primary32-secondary8"
        );
        Ok(())
    }

    #[test]
    fn concurrency_qualification_is_sorted_exact_and_timestamp_free() -> Result<()> {
        let root = TestDir::new()?;
        let baseline = root.path().join("baseline");
        let candidate = root.path().join("candidate");
        fs::create_dir_all(baseline.join("nested"))?;
        fs::create_dir_all(candidate.join("nested"))?;
        fs::write(baseline.join("z.parquet"), b"z")?;
        fs::write(candidate.join("z.parquet"), b"z")?;
        fs::write(baseline.join("nested/a.parquet"), b"a")?;
        fs::write(candidate.join("nested/a.parquet"), b"a")?;
        fs::write(baseline.join("ignored.json"), b"different-one")?;
        fs::write(candidate.join("ignored.json"), b"different-two")?;
        let report_path = root.path().join("qualification.json");
        let report = qualify_concurrency(
            &baseline,
            &candidate,
            "parquet",
            1,
            3,
            "compute-primary32-secondary8",
            &report_path,
        )
        .expect("identical artifact sets must qualify");
        assert_eq!(report.artifact_count, 2);
        assert_eq!(report.artifacts[0].identity, "nested/a.parquet");
        assert_eq!(report.artifacts[1].identity, "z.parquet");
        let first_report_bytes = fs::read(&report_path)?;
        assert!(!String::from_utf8_lossy(&first_report_bytes).contains("generated_at"));
        qualify_concurrency(
            &baseline,
            &candidate,
            "parquet",
            1,
            3,
            "compute-primary32-secondary8",
            &report_path,
        )
        .expect("repeated qualification must succeed");
        assert_eq!(fs::read(&report_path)?, first_report_bytes);

        fs::write(candidate.join("z.parquet"), b"changed")?;
        assert!(
            qualify_concurrency(
                &baseline,
                &candidate,
                "parquet",
                1,
                3,
                "compute-primary32-secondary8",
                &report_path,
            )
            .is_err()
        );
        assert!(
            qualify_concurrency(&baseline, &candidate, "bad-ext", 1, 2, "v1", &report_path)
                .is_err()
        );
        assert!(
            qualify_concurrency(&baseline, &candidate, "parquet", 1, 1, "v1", &report_path)
                .is_err()
        );
        Ok(())
    }

    #[test]
    fn concurrency_qualification_rejects_missing_empty_and_unwritable_sets() -> Result<()> {
        let root = TestDir::new()?;
        let empty = root.path().join("empty");
        fs::create_dir(&empty)?;
        let report = root.path().join("report.json");
        assert!(qualify_concurrency(&empty, &empty, "parquet", 1, 2, "v1", &report).is_err());
        assert!(
            qualify_concurrency(
                &root.path().join("missing"),
                &empty,
                "parquet",
                1,
                2,
                "v1",
                &report
            )
            .is_err()
        );
        assert!(qualify_concurrency(&empty, &empty, "parquet", 1, 2, "", &report).is_err());
        let directory_report = root.path().join("directory-report");
        fs::create_dir(&directory_report)?;
        fs::write(empty.join("one.parquet"), b"one")?;
        assert!(
            qualify_concurrency(&empty, &empty, "parquet", 1, 2, "v1", &directory_report).is_err()
        );
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn concurrency_qualification_rejects_symlinks() -> Result<()> {
        use std::os::unix::fs::symlink;

        let root = TestDir::new()?;
        let baseline = root.path().join("baseline");
        let candidate = root.path().join("candidate");
        fs::create_dir(&baseline)?;
        fs::create_dir(&candidate)?;
        fs::write(baseline.join("one.parquet"), b"one")?;
        fs::write(candidate.join("one.parquet"), b"one")?;
        symlink(
            baseline.join("one.parquet"),
            baseline.join("linked.parquet"),
        )
        .expect("test symlink must be created");
        assert!(
            qualify_concurrency(
                &baseline,
                &candidate,
                "parquet",
                1,
                2,
                "v1",
                &root.path().join("report.json"),
            )
            .is_err()
        );
        Ok(())
    }
}
