use anyhow::{Context, Result, ensure};
use polars::prelude::{DataFrame, ParquetCompression, ParquetReader, ParquetWriter, SerReader};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::{canonical_month, storage};

const CACHE_RECEIPT_SCHEMA_VERSION: u32 = 1;
const CACHE_WRITER_VERSION: &str = "polars-parquet-zstd-row-group-100000-v1";
const ROW_GROUP_ROWS: usize = 100_000;

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct CacheStats {
    pub(crate) reused_artifacts: u64,
    pub(crate) rebuilt_artifacts: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct CacheReceipt {
    schema_version: u32,
    wiki: String,
    kind: String,
    algorithm_version: String,
    input_digest: String,
    writer_version: String,
    artifact_sha256: String,
    bytes: u64,
    rows: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct QualificationReport {
    pub(crate) schema_version: u32,
    pub(crate) publication_eligible: bool,
    pub(crate) wiki: String,
    pub(crate) baseline_snapshot: String,
    pub(crate) candidate_snapshot: String,
    pub(crate) baseline_cache: CacheStats,
    pub(crate) candidate_cache: CacheStats,
    pub(crate) artifact_count: usize,
    pub(crate) aggregate_sha256: String,
    pub(crate) artifacts: Vec<crate::determinism::ArtifactDigest>,
}

pub(crate) struct CrossSnapshotCache {
    wiki: String,
    root: PathBuf,
    months: BTreeMap<String, canonical_month::MonthIdentity>,
    stats: RefCell<CacheStats>,
}

impl CrossSnapshotCache {
    pub(crate) fn new(data_dir: &Path, wiki: &str, snapshot: &str) -> Result<Self> {
        let inventory = canonical_month::ensure_snapshot_inventory(data_dir, wiki, snapshot)?;
        let months = inventory
            .identities
            .into_iter()
            .map(|identity| (identity.event_month.clone(), identity))
            .collect();
        Ok(Self {
            wiki: wiki.to_string(),
            root: data_dir.join("incremental").join("metric-cache").join(wiki),
            months,
            stats: RefCell::new(CacheStats::default()),
        })
    }

    pub(crate) fn month_digest(&self, event_month: &str) -> Result<&str> {
        self.months
            .get(event_month)
            .map(|identity| identity.digest.as_str())
            .with_context(|| format!("canonical identity is missing month {event_month}"))
    }

    pub(crate) fn derived_digest(
        &self,
        kind: &str,
        algorithm_version: &str,
        input_digests: &[&str],
    ) -> String {
        let mut digest = Sha256::new();
        digest.update(b"wiki-economics\0derived-incremental-input\0");
        update_string(&mut digest, kind);
        update_string(&mut digest, algorithm_version);
        for input in input_digests {
            update_string(&mut digest, input);
        }
        hex::encode(digest.finalize())
    }

    pub(crate) fn load(
        &self,
        kind: &str,
        algorithm_version: &str,
        input_digest: &str,
        artifact: &str,
    ) -> Result<Option<DataFrame>> {
        let path = self.artifact_path(kind, algorithm_version, input_digest, artifact)?;
        let receipt_path = receipt_path(&path);
        if !path.is_file() || !receipt_path.is_file() {
            return Ok(None);
        }
        let receipt: CacheReceipt = serde_json::from_slice(&fs::read(&receipt_path)?)
            .with_context(|| {
                format!(
                    "invalid incremental cache receipt {}",
                    receipt_path.display()
                )
            })?;
        ensure!(
            receipt.schema_version == CACHE_RECEIPT_SCHEMA_VERSION
                && receipt.wiki == self.wiki
                && receipt.kind == kind
                && receipt.algorithm_version == algorithm_version
                && receipt.input_digest == input_digest
                && receipt.writer_version == CACHE_WRITER_VERSION,
            "incremental cache receipt identity changed"
        );
        let metadata = fs::metadata(&path)?;
        ensure!(
            metadata.is_file() && metadata.len() == receipt.bytes,
            "incremental cache artifact size changed"
        );
        let (_, sha256) = storage::sha256_file(&path)?;
        ensure!(
            sha256 == receipt.artifact_sha256,
            "incremental cache artifact hash changed"
        );
        let frame = ParquetReader::new(File::open(&path)?)
            .set_low_memory(true)
            .finish()?;
        ensure!(
            u64::try_from(frame.height())? == receipt.rows,
            "incremental cache row count changed"
        );
        self.stats.borrow_mut().reused_artifacts += 1;
        Ok(Some(frame))
    }

    pub(crate) fn store(
        &self,
        kind: &str,
        algorithm_version: &str,
        input_digest: &str,
        artifact: &str,
        frame: &mut DataFrame,
    ) -> Result<()> {
        let path = self.artifact_path(kind, algorithm_version, input_digest, artifact)?;
        let parent = path
            .parent()
            .context("incremental cache artifact has no parent")?;
        fs::create_dir_all(parent)?;
        let temporary = path.with_extension(format!("parquet.{}.tmp", std::process::id()));
        let result = (|| -> Result<()> {
            let mut file = File::create(&temporary)?;
            ParquetWriter::new(&mut file)
                .with_compression(ParquetCompression::Zstd(None))
                .with_row_group_size(Some(ROW_GROUP_ROWS))
                .set_parallel(false)
                .finish(frame)?;
            file.sync_all()?;
            fs::rename(&temporary, &path)?;
            File::open(parent)?.sync_all()?;
            let (bytes, artifact_sha256) = storage::sha256_file(&path)?;
            let receipt = CacheReceipt {
                schema_version: CACHE_RECEIPT_SCHEMA_VERSION,
                wiki: self.wiki.clone(),
                kind: kind.to_string(),
                algorithm_version: algorithm_version.to_string(),
                input_digest: input_digest.to_string(),
                writer_version: CACHE_WRITER_VERSION.to_string(),
                artifact_sha256,
                bytes,
                rows: u64::try_from(frame.height())?,
            };
            atomic_json(&receipt_path(&path), &receipt)
        })();
        if result.is_err() {
            let _ = fs::remove_file(&temporary);
            let _ = fs::remove_file(&path);
            let _ = fs::remove_file(receipt_path(&path));
        }
        result?;
        self.stats.borrow_mut().rebuilt_artifacts += 1;
        Ok(())
    }

    pub(crate) fn stats(&self) -> CacheStats {
        *self.stats.borrow()
    }

    pub(crate) fn load_json<T: DeserializeOwned>(
        &self,
        kind: &str,
        algorithm_version: &str,
        input_digest: &str,
        artifact: &str,
    ) -> Result<Option<T>> {
        let path = self.artifact_path_with_extension(
            kind,
            algorithm_version,
            input_digest,
            artifact,
            "json",
        )?;
        if !path.is_file() {
            return Ok(None);
        }
        let value = serde_json::from_slice(&fs::read(&path)?)
            .with_context(|| format!("invalid incremental checkpoint {}", path.display()))?;
        self.stats.borrow_mut().reused_artifacts += 1;
        Ok(Some(value))
    }

    pub(crate) fn store_json<T: Serialize>(
        &self,
        kind: &str,
        algorithm_version: &str,
        input_digest: &str,
        artifact: &str,
        value: &T,
    ) -> Result<()> {
        let path = self.artifact_path_with_extension(
            kind,
            algorithm_version,
            input_digest,
            artifact,
            "json",
        )?;
        atomic_json(&path, value)?;
        self.stats.borrow_mut().rebuilt_artifacts += 1;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn for_test(
        root: &Path,
        wiki: &str,
        identities: Vec<canonical_month::MonthIdentity>,
    ) -> Self {
        Self {
            wiki: wiki.to_string(),
            root: root.to_path_buf(),
            months: identities
                .into_iter()
                .map(|identity| (identity.event_month.clone(), identity))
                .collect(),
            stats: RefCell::new(CacheStats::default()),
        }
    }

    fn artifact_path(
        &self,
        kind: &str,
        algorithm_version: &str,
        input_digest: &str,
        artifact: &str,
    ) -> Result<PathBuf> {
        self.artifact_path_with_extension(
            kind,
            algorithm_version,
            input_digest,
            artifact,
            "parquet",
        )
    }

    fn artifact_path_with_extension(
        &self,
        kind: &str,
        algorithm_version: &str,
        input_digest: &str,
        artifact: &str,
        extension: &str,
    ) -> Result<PathBuf> {
        for (label, value) in [
            ("kind", kind),
            ("artifact", artifact),
            ("extension", extension),
        ] {
            ensure!(
                !value.is_empty()
                    && value
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_')),
                "unsafe incremental cache {label}"
            );
        }
        ensure!(
            input_digest.len() == 64 && input_digest.bytes().all(|byte| byte.is_ascii_hexdigit()),
            "invalid incremental input digest"
        );
        let algorithm_digest = hex::encode(Sha256::digest(algorithm_version.as_bytes()));
        Ok(self
            .root
            .join(kind)
            .join(algorithm_digest)
            .join(input_digest)
            .join(format!("{artifact}.{extension}")))
    }
}

fn update_string(digest: &mut Sha256, value: &str) {
    digest.update((value.len() as u64).to_be_bytes());
    digest.update(value.as_bytes());
}

fn receipt_path(artifact: &Path) -> PathBuf {
    artifact.with_extension("parquet.cache.json")
}

fn atomic_json<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    let parent = path
        .parent()
        .context("incremental cache receipt has no parent")?;
    fs::create_dir_all(parent)?;
    let temporary = path.with_extension(format!("json.{}.tmp", std::process::id()));
    let mut bytes = serde_json::to_vec_pretty(value)?;
    bytes.push(b'\n');
    let result = (|| -> Result<()> {
        let mut file = File::create(&temporary)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        fs::rename(&temporary, path)?;
        File::open(parent)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(temporary);
    }
    result
}

pub(crate) fn qualify(
    data_dir: &Path,
    wiki: &str,
    baseline_snapshot: &str,
    candidate_snapshot: &str,
    work_root: &Path,
    report_path: &Path,
) -> Result<QualificationReport> {
    storage::validate_snapshot_version(baseline_snapshot)?;
    storage::validate_snapshot_version(candidate_snapshot)?;
    ensure!(
        baseline_snapshot < candidate_snapshot,
        "cross-snapshot qualification requires an older baseline snapshot"
    );
    ensure!(
        !work_root.exists(),
        "cross-snapshot qualification root already exists: {}",
        work_root.display()
    );
    fs::create_dir_all(work_root)?;
    let baseline_root = work_root.join("baseline-cache-seed");
    let incremental_root = work_root.join("candidate-incremental");
    let clean_root = work_root.join("candidate-clean");
    let result = (|| -> Result<QualificationReport> {
        let baseline_cache = crate::compute::compute_cross_snapshot_qualification_build(
            wiki,
            baseline_snapshot,
            data_dir,
            &baseline_root,
            true,
        )?;
        let candidate_cache = crate::compute::compute_cross_snapshot_qualification_build(
            wiki,
            candidate_snapshot,
            data_dir,
            &incremental_root,
            true,
        )?;
        crate::compute::compute_cross_snapshot_qualification_build(
            wiki,
            candidate_snapshot,
            data_dir,
            &clean_root,
            false,
        )?;
        let incremental = crate::determinism::collect_artifacts(&incremental_root, "parquet")?;
        let clean = crate::determinism::collect_artifacts(&clean_root, "parquet")?;
        ensure!(
            !clean.is_empty(),
            "qualification produced no metric artifacts"
        );
        ensure!(
            incremental == clean,
            "incremental and clean metric artifacts are not byte-identical"
        );
        let report = QualificationReport {
            schema_version: 1,
            publication_eligible: false,
            wiki: wiki.to_string(),
            baseline_snapshot: baseline_snapshot.to_string(),
            candidate_snapshot: candidate_snapshot.to_string(),
            baseline_cache,
            candidate_cache,
            artifact_count: clean.len(),
            aggregate_sha256: crate::determinism::aggregate_digest(&clean),
            artifacts: clean,
        };
        atomic_json(report_path, &report)?;
        Ok(report)
    })();
    if result.is_err() {
        let failure = work_root.join("qualification-failed");
        let mut file = File::create(&failure)?;
        file.write_all(b"cross-snapshot qualification failed; artifacts retained for diagnosis\n")?;
        file.sync_all()?;
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::TestDir;
    use polars::prelude::Column;

    fn identity() -> canonical_month::MonthIdentity {
        canonical_month::MonthIdentity {
            schema_version: 1,
            wiki: "testwiki".to_string(),
            event_month: "2024-02".to_string(),
            logical_schema_version: canonical_month::LOGICAL_SCHEMA_VERSION,
            encoding_version: canonical_month::ENCODING_VERSION.to_string(),
            ordering_contract: "test".to_string(),
            digest: "ab".repeat(32),
            rows: 2,
            edits: 2,
        }
    }

    #[test]
    fn cache_is_content_addressed_strict_and_counted() -> Result<()> {
        let root = TestDir::new()?;
        let cache = CrossSnapshotCache::for_test(root.path(), "testwiki", vec![identity()]);
        assert_eq!(cache.month_digest("2024-02")?, "ab".repeat(32));
        assert!(cache.month_digest("2024-03").is_err());
        let mut frame = DataFrame::new_infer_height(vec![Column::new("value".into(), [1_i64, 2])])?;
        let digest = cache.month_digest("2024-02")?;
        assert!(cache.load("monthly", "v1", digest, "gdp")?.is_none());
        cache.store("monthly", "v1", digest, "gdp", &mut frame)?;
        assert_eq!(cache.load("monthly", "v1", digest, "gdp")?, Some(frame));
        assert_eq!(
            cache.stats(),
            CacheStats {
                reused_artifacts: 1,
                rebuilt_artifacts: 1,
            }
        );

        let artifact = cache.artifact_path("monthly", "v1", digest, "gdp")?;
        fs::write(&artifact, b"corrupt")?;
        assert!(cache.load("monthly", "v1", digest, "gdp").is_err());
        assert!(cache.artifact_path("../bad", "v1", digest, "gdp").is_err());
        let checkpoint = BTreeMap::from([("through".to_string(), "2024-12".to_string())]);
        assert!(
            cache
                .load_json::<BTreeMap<String, String>>(
                    "lifecycle_checkpoint",
                    "v1",
                    digest,
                    "state",
                )?
                .is_none()
        );
        cache.store_json("lifecycle_checkpoint", "v1", digest, "state", &checkpoint)?;
        assert_eq!(
            cache.load_json("lifecycle_checkpoint", "v1", digest, "state")?,
            Some(checkpoint)
        );
        Ok(())
    }

    #[test]
    fn derived_digest_is_ordered_and_algorithm_scoped() {
        let root = TestDir::new().expect("test root should exist");
        let cache = CrossSnapshotCache::for_test(root.path(), "testwiki", vec![identity()]);
        let a = cache.derived_digest("activity_year", "v1", &["a", "b"]);
        assert_eq!(a, cache.derived_digest("activity_year", "v1", &["a", "b"]));
        assert_ne!(a, cache.derived_digest("activity_year", "v1", &["b", "a"]));
        assert_ne!(a, cache.derived_digest("activity_year", "v2", &["a", "b"]));
    }
}
