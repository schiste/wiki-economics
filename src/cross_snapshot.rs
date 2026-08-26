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

const CACHE_RECEIPT_SCHEMA_VERSION: u32 = 3;
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
    #[serde(default)]
    observed_modified_unix_nanos: u128,
    #[serde(default)]
    receipt_sha256: String,
}

impl CacheReceipt {
    fn canonical_hash(&self) -> Result<String> {
        let mut canonical = self.clone();
        canonical.receipt_sha256.clear();
        Ok(hex::encode(Sha256::digest(serde_json::to_vec(&canonical)?)))
    }
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
    pub(crate) unchanged_months: Vec<String>,
    pub(crate) changed_months: Vec<String>,
    pub(crate) removed_months: Vec<String>,
    pub(crate) artifact_count: usize,
    pub(crate) aggregate_sha256: String,
    pub(crate) artifacts: Vec<crate::determinism::ArtifactDigest>,
    pub(crate) semantic_summaries: Vec<QualificationSemanticSummary>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct QualificationSemanticSummary {
    pub(crate) identity: String,
    pub(crate) rows: u64,
    pub(crate) minimum_date: Option<String>,
    pub(crate) maximum_date: Option<String>,
    pub(crate) conservation_totals: BTreeMap<String, i128>,
    pub(crate) artifact_sha256: String,
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
        let receipt = self.validate_artifact(&path, kind, algorithm_version, input_digest)?;
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

    pub(crate) fn reusable(
        &self,
        kind: &str,
        algorithm_version: &str,
        input_digest: &str,
        artifact: &str,
    ) -> Result<bool> {
        let path = self.artifact_path(kind, algorithm_version, input_digest, artifact)?;
        if !path.is_file() || !receipt_path(&path).is_file() {
            return Ok(false);
        }
        self.validate_artifact(&path, kind, algorithm_version, input_digest)?;
        Ok(true)
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
            let mut receipt = CacheReceipt {
                schema_version: CACHE_RECEIPT_SCHEMA_VERSION,
                wiki: self.wiki.clone(),
                kind: kind.to_string(),
                algorithm_version: algorithm_version.to_string(),
                input_digest: input_digest.to_string(),
                writer_version: CACHE_WRITER_VERSION.to_string(),
                artifact_sha256,
                bytes,
                rows: u64::try_from(frame.height())?,
                observed_modified_unix_nanos: modified_nanos(&path)?,
                receipt_sha256: String::new(),
            };
            receipt.receipt_sha256 = receipt.canonical_hash()?;
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

    fn validate_artifact(
        &self,
        path: &Path,
        kind: &str,
        algorithm_version: &str,
        input_digest: &str,
    ) -> Result<CacheReceipt> {
        let receipt_path = receipt_path(path);
        let receipt: CacheReceipt = serde_json::from_slice(&fs::read(&receipt_path)?)
            .with_context(|| {
                format!(
                    "invalid incremental cache receipt {}",
                    receipt_path.display()
                )
            })?;
        ensure!(
            matches!(receipt.schema_version, 1 | 2 | CACHE_RECEIPT_SCHEMA_VERSION)
                && receipt.wiki == self.wiki
                && receipt.kind == kind
                && receipt.algorithm_version == algorithm_version
                && receipt.input_digest == input_digest
                && receipt.writer_version == CACHE_WRITER_VERSION,
            "incremental cache receipt identity changed"
        );
        ensure!(
            receipt.schema_version < CACHE_RECEIPT_SCHEMA_VERSION
                || receipt.receipt_sha256 == receipt.canonical_hash()?,
            "incremental cache receipt hash changed"
        );
        let metadata = fs::metadata(path)?;
        ensure!(
            metadata.is_file() && metadata.len() == receipt.bytes,
            "incremental cache artifact size changed"
        );
        if receipt.schema_version < CACHE_RECEIPT_SCHEMA_VERSION
            || receipt.observed_modified_unix_nanos != modified_nanos(path)?
        {
            let (_, sha256) = storage::sha256_file(path)?;
            ensure!(
                sha256 == receipt.artifact_sha256,
                "incremental cache artifact hash changed"
            );
        }
        Ok(receipt)
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

fn modified_nanos(path: &Path) -> Result<u128> {
    Ok(fs::metadata(path)?
        .modified()?
        .duration_since(std::time::UNIX_EPOCH)
        .with_context(|| format!("{} has a pre-epoch mtime", path.display()))?
        .as_nanos())
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
    let current_snapshot_before = storage::current_snapshot_version(data_dir, wiki)?;
    let baseline_inventory =
        canonical_month::ensure_snapshot_inventory(data_dir, wiki, baseline_snapshot)?;
    let candidate_inventory =
        canonical_month::ensure_snapshot_inventory(data_dir, wiki, candidate_snapshot)?;
    let baseline_months = baseline_inventory
        .identities
        .iter()
        .map(|identity| (identity.event_month.as_str(), identity.digest.as_str()))
        .collect::<BTreeMap<_, _>>();
    let candidate_months = candidate_inventory
        .identities
        .iter()
        .map(|identity| (identity.event_month.as_str(), identity.digest.as_str()))
        .collect::<BTreeMap<_, _>>();
    let unchanged_months = candidate_months
        .iter()
        .filter(|(month, digest)| baseline_months.get(**month) == Some(digest))
        .map(|(month, _)| (*month).to_string())
        .collect::<Vec<_>>();
    let changed_months = candidate_months
        .iter()
        .filter(|(month, digest)| baseline_months.get(**month) != Some(digest))
        .map(|(month, _)| (*month).to_string())
        .collect::<Vec<_>>();
    let removed_months = baseline_months
        .keys()
        .filter(|month| !candidate_months.contains_key(**month))
        .map(|month| (*month).to_string())
        .collect::<Vec<_>>();
    fs::create_dir_all(work_root)?;
    let baseline_root = work_root.join("baseline-cache-seed");
    let incremental_root = work_root.join("candidate-incremental");
    let clean_root = work_root.join("candidate-clean");
    let result = (|| -> Result<QualificationReport> {
        let baseline_result = crate::compute::compute_cross_snapshot_qualification_build(
            wiki,
            baseline_snapshot,
            data_dir,
            &baseline_root,
            true,
        );
        let baseline_cache = baseline_result?;
        let candidate_result = crate::compute::compute_cross_snapshot_qualification_build(
            wiki,
            candidate_snapshot,
            data_dir,
            &incremental_root,
            true,
        );
        let candidate_cache = candidate_result?;
        let clean_result = crate::compute::compute_cross_snapshot_qualification_build(
            wiki,
            candidate_snapshot,
            data_dir,
            &clean_root,
            false,
        );
        clean_result?;
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
        let mut semantic_summaries = Vec::with_capacity(clean.len());
        for artifact in &clean {
            let incremental_path = incremental_root.join(&artifact.identity);
            let clean_path = clean_root.join(&artifact.identity);
            let incremental_receipt_result = crate::artifact_receipt::scan_and_write(
                &incremental_path,
                &artifact.identity,
                "cross-snapshot-equivalence-v1",
                candidate_snapshot,
            );
            let incremental_receipt = incremental_receipt_result?;
            let clean_receipt_result = crate::artifact_receipt::scan_and_write(
                &clean_path,
                &artifact.identity,
                "cross-snapshot-equivalence-v1",
                candidate_snapshot,
            );
            let clean_receipt = clean_receipt_result?;
            ensure!(
                incremental_receipt.receipt == clean_receipt.receipt,
                "incremental and clean semantic receipts disagree for {}",
                artifact.identity
            );
            let receipt = clean_receipt.receipt;
            semantic_summaries.push(QualificationSemanticSummary {
                identity: artifact.identity.clone(),
                rows: receipt.rows,
                minimum_date: receipt.minimum_date,
                maximum_date: receipt.maximum_date,
                conservation_totals: receipt.conservation_totals,
                artifact_sha256: receipt.artifact_sha256,
            });
        }
        let report = QualificationReport {
            schema_version: 1,
            publication_eligible: false,
            wiki: wiki.to_string(),
            baseline_snapshot: baseline_snapshot.to_string(),
            candidate_snapshot: candidate_snapshot.to_string(),
            baseline_cache,
            candidate_cache,
            unchanged_months: unchanged_months.clone(),
            changed_months: changed_months.clone(),
            removed_months: removed_months.clone(),
            artifact_count: clean.len(),
            aggregate_sha256: crate::determinism::aggregate_digest(&clean),
            artifacts: clean,
            semantic_summaries,
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
    ensure!(
        storage::current_snapshot_version(data_dir, wiki)? == current_snapshot_before,
        "cross-snapshot qualification changed the live generation"
    );
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
        let receipt = receipt_path(&artifact);
        let artifact_bytes = fs::read(&artifact)?;
        fs::write(&artifact, &artifact_bytes)?;
        assert!(cache.reusable("monthly", "v1", digest, "gdp")?);
        let receipt_bytes = fs::read(&receipt)?;
        fs::write(&receipt, b"not json")?;
        assert!(cache.load("monthly", "v1", digest, "gdp").is_err());
        fs::write(&receipt, &receipt_bytes)?;
        let mut changed_receipt: CacheReceipt = serde_json::from_slice(&receipt_bytes)?;
        changed_receipt.rows += 1;
        fs::write(&receipt, serde_json::to_vec(&changed_receipt)?)?;
        assert!(cache.reusable("monthly", "v1", digest, "gdp").is_err());
        fs::write(&receipt, receipt_bytes)?;
        fs::write(&artifact, b"corrupt")?;
        assert!(cache.load("monthly", "v1", digest, "gdp").is_err());
        assert!(cache.artifact_path("../bad", "v1", digest, "gdp").is_err());
        assert!(
            cache
                .load_json::<BTreeMap<String, String>>("../bad", "v1", digest, "state")
                .is_err()
        );
        assert!(
            cache
                .store_json(
                    "../bad",
                    "v1",
                    digest,
                    "state",
                    &BTreeMap::<String, String>::new(),
                )
                .is_err()
        );
        let checkpoint = BTreeMap::from([("through".to_string(), "2024-12".to_string())]);
        let missing_checkpoint = cache.load_json::<BTreeMap<String, String>>(
            "lifecycle_checkpoint",
            "v1",
            digest,
            "state",
        );
        assert!(missing_checkpoint?.is_none());
        cache.store_json("lifecycle_checkpoint", "v1", digest, "state", &checkpoint)?;
        assert_eq!(
            cache.load_json("lifecycle_checkpoint", "v1", digest, "state")?,
            Some(checkpoint)
        );

        let mut failed_frame =
            DataFrame::new_infer_height(vec![Column::new("value".into(), [3_i64])])?;
        let failed_artifact = cache.artifact_path("monthly", "v1", digest, "failed")?;
        fs::create_dir_all(receipt_path(&failed_artifact))?;
        assert!(
            cache
                .store("monthly", "v1", digest, "failed", &mut failed_frame)
                .is_err()
        );
        assert!(!failed_artifact.exists());

        let blocked_checkpoint_result = cache.artifact_path_with_extension(
            "lifecycle_checkpoint",
            "v1",
            digest,
            "blocked",
            "json",
        );
        let blocked_checkpoint = blocked_checkpoint_result?;
        fs::create_dir_all(&blocked_checkpoint)?;
        assert!(
            cache
                .store_json(
                    "lifecycle_checkpoint",
                    "v1",
                    digest,
                    "blocked",
                    &BTreeMap::from([("value", 1_u64)]),
                )
                .is_err()
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

    #[test]
    fn qualification_rejects_reversed_versions_and_existing_workspaces() -> Result<()> {
        let root = TestDir::new()?;
        let work = root.path().join("work");
        assert!(
            qualify(
                root.path(),
                "testwiki",
                "2026-08",
                "2026-07",
                &work,
                &root.path().join("report.json"),
            )
            .is_err()
        );
        fs::create_dir(&work)?;
        assert!(
            qualify(
                root.path(),
                "testwiki",
                "2026-07",
                "2026-08",
                &work,
                &root.path().join("report.json"),
            )
            .is_err()
        );
        Ok(())
    }
}
