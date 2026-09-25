//! Receipt-only compatibility steps for retention-authorized candidates.

use anyhow::{Context, Result, ensure};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};

use crate::{artifact_receipt, fingerprint, metric_registry::MetricFamily, storage};

const LEGACY_ACTIVITY_TIER_ALGORITHM: &str = "activity-tiers-v5-exclusive-period-user-type";
const ACTIVITY_TIER_MIGRATION_ID: &str =
    "retained-activity-tiers-v5-to-v6-equivalent-output-receipt-v1";
const LEGACY_LIFECYCLE_ALGORITHM: &str =
    "editor-lifecycle-v3-explicit-identified-registered-editors";
const LIFECYCLE_MIGRATION_ID: &str = "retained-lifecycle-v3-to-v4-period-months-v1";

fn family_receipt_path(candidate_dir: &Path, wiki: &str, family: MetricFamily) -> PathBuf {
    candidate_dir
        .join("_stages")
        .join("compute")
        .join(family.name())
        .join(format!("{wiki}.json"))
}

fn family_outputs(
    candidate_dir: &Path,
    wiki: &str,
    family: MetricFamily,
) -> Vec<fingerprint::TrackedPath> {
    family
        .metrics()
        .iter()
        .map(|metric| {
            fingerprint::TrackedPath::new(
                format!("output/{wiki}/{metric}.parquet"),
                candidate_dir.join(wiki).join(format!("{metric}.parquet")),
            )
        })
        .collect()
}

fn family_spec<'a>(
    wiki: &'a str,
    snapshot: &'a str,
    family: MetricFamily,
    algorithm_version: &'a str,
) -> fingerprint::StageSpec<'a> {
    fingerprint::StageSpec {
        stage: match family {
            MetricFamily::ActivityTiers => "compute_activity_tiers",
            MetricFamily::Lifecycle => "compute_lifecycle",
            _ => unreachable!("only retained activity and lifecycle migrations are supported"),
        },
        scope: wiki,
        selected_snapshot: Some(snapshot),
        algorithm_version,
    }
}

fn migration_inputs(
    wiki: &str,
    snapshot: &str,
    source_run_id: &str,
    source_candidate_dir: &Path,
    family: MetricFamily,
    source_algorithm: &str,
    migration_id: &str,
) -> Result<(Vec<fingerprint::TrackedPath>, String)> {
    let source_receipt_path = family_receipt_path(source_candidate_dir, wiki, family);
    let source_outputs = family_outputs(source_candidate_dir, wiki, family);
    let source_reusable = fingerprint::retained_outputs_reusable(
        &source_receipt_path,
        family_spec(wiki, snapshot, family, source_algorithm),
        &source_outputs,
    )?;
    ensure!(
        source_reusable,
        "retained candidate {wiki} is not an authenticated {} candidate",
        family.name()
    );
    let source_stage_receipt = fingerprint::read_receipt(&source_receipt_path)?;
    ensure!(
        source_stage_receipt.algorithm_version == source_algorithm,
        "retained candidate {wiki} {} receipt has an unsupported source algorithm",
        family.name()
    );

    let mut migration_digest = Sha256::new();
    migration_digest.update(format!("{migration_id}\n").as_bytes());
    migration_digest.update(source_stage_receipt.fingerprint.as_bytes());
    let mut inputs = vec![fingerprint::TrackedPath::new(
        format!(
            "retained-candidate/{wiki}/{snapshot}/{source_run_id}/{}-stage-receipt",
            family.name()
        ),
        source_receipt_path,
    )];
    for metric in family.metrics() {
        let source_path = source_candidate_dir
            .join(wiki)
            .join(format!("{metric}.parquet"));
        let source_document = artifact_receipt::read(&source_path)?;
        let source_document = artifact_receipt::verify(
            &source_path,
            &source_document.receipt.identity,
            Some(&source_document.receipt_sha256),
            artifact_receipt::VerificationMode::Fast,
        )?;
        ensure!(
            source_document.receipt.algorithm_version == source_algorithm,
            "retained candidate {wiki} {metric} artifact is not on the expected source algorithm"
        );
        let (source_bytes, source_sha256) = storage::sha256_file(&source_path)?;
        ensure!(
            source_bytes == source_document.receipt.bytes
                && source_sha256 == source_document.receipt.artifact_sha256,
            "retained candidate {wiki} {metric} bytes do not match their source receipt"
        );
        migration_digest.update(source_document.receipt_sha256.as_bytes());
        inputs.push(fingerprint::TrackedPath::new(
            format!("retained-candidate/{wiki}/{snapshot}/{source_run_id}/{metric}.parquet"),
            source_path.clone(),
        ));
        inputs.push(fingerprint::TrackedPath::new(
            format!(
                "retained-candidate/{wiki}/{snapshot}/{source_run_id}/{metric}.parquet.receipt.json"
            ),
            artifact_receipt::sidecar_path(&source_path)?,
        ));
    }
    inputs.sort_by(|left, right| left.identity.cmp(&right.identity));
    Ok((inputs, hex::encode(migration_digest.finalize())))
}

pub(crate) fn activity_tier_migration_required(
    wiki: &str,
    snapshot: &str,
    source_candidate_dir: &Path,
) -> Result<bool> {
    let source_receipt_path =
        family_receipt_path(source_candidate_dir, wiki, MetricFamily::ActivityTiers);
    let outputs = family_outputs(source_candidate_dir, wiki, MetricFamily::ActivityTiers);
    let current = fingerprint::retained_outputs_reusable(
        &source_receipt_path,
        family_spec(
            wiki,
            snapshot,
            MetricFamily::ActivityTiers,
            crate::compute::activity::ALGORITHM_VERSION,
        ),
        &outputs,
    )?;
    if current {
        return Ok(false);
    }
    let legacy = fingerprint::retained_outputs_reusable(
        &source_receipt_path,
        family_spec(
            wiki,
            snapshot,
            MetricFamily::ActivityTiers,
            LEGACY_ACTIVITY_TIER_ALGORITHM,
        ),
        &outputs,
    )?;
    ensure!(
        legacy,
        "retained candidate {wiki} has an outdated or invalid activity_tiers family"
    );
    Ok(true)
}

pub(crate) fn stage_activity_tier_receipts(
    wiki: &str,
    snapshot: &str,
    source_run_id: &str,
    source_candidate_dir: &Path,
    staged_candidate_dir: &Path,
) -> Result<()> {
    let (inputs, migration_fingerprint) = migration_inputs(
        wiki,
        snapshot,
        source_run_id,
        source_candidate_dir,
        MetricFamily::ActivityTiers,
        LEGACY_ACTIVITY_TIER_ALGORITHM,
        ACTIVITY_TIER_MIGRATION_ID,
    )?;
    for metric in MetricFamily::ActivityTiers.metrics() {
        let source_path = source_candidate_dir
            .join(wiki)
            .join(format!("{metric}.parquet"));
        let staged_path = staged_candidate_dir
            .join(wiki)
            .join(format!("{metric}.parquet"));
        let source_document = artifact_receipt::read(&source_path)?;
        let (staged_bytes, staged_sha256) = storage::sha256_file(&staged_path)?;
        ensure!(
            staged_bytes == source_document.receipt.bytes
                && staged_sha256 == source_document.receipt.artifact_sha256,
            "retained candidate {wiki} {metric} staging copy differs from its authenticated source"
        );
        artifact_receipt::scan_and_write(
            &staged_path,
            &source_document.receipt.identity,
            crate::compute::activity::ALGORITHM_VERSION,
            &migration_fingerprint,
        )?;
    }
    fingerprint::record(
        &family_receipt_path(staged_candidate_dir, wiki, MetricFamily::ActivityTiers),
        family_spec(
            wiki,
            snapshot,
            MetricFamily::ActivityTiers,
            crate::compute::activity::ALGORITHM_VERSION,
        ),
        &inputs,
        &family_outputs(staged_candidate_dir, wiki, MetricFamily::ActivityTiers),
    )?;
    let current = fingerprint::retained_outputs_reusable(
        &family_receipt_path(staged_candidate_dir, wiki, MetricFamily::ActivityTiers),
        family_spec(
            wiki,
            snapshot,
            MetricFamily::ActivityTiers,
            crate::compute::activity::ALGORITHM_VERSION,
        ),
        &family_outputs(staged_candidate_dir, wiki, MetricFamily::ActivityTiers),
    )?;
    ensure!(
        current,
        "retained activity-tier migration for {wiki} did not produce a current v6 receipt"
    );
    Ok(())
}

/// Rebind the lifecycle migration receipt inputs from its temporary staging
/// source to the authenticated retained candidate. The Parquet projection is
/// still verified independently by the lifecycle migration validator.
pub(crate) fn rebind_lifecycle_receipts(
    wiki: &str,
    snapshot: &str,
    source_run_id: &str,
    source_candidate_dir: &Path,
    target_candidate_dir: &Path,
) -> Result<()> {
    let (inputs, migration_fingerprint) = migration_inputs(
        wiki,
        snapshot,
        source_run_id,
        source_candidate_dir,
        MetricFamily::Lifecycle,
        LEGACY_LIFECYCLE_ALGORITHM,
        LIFECYCLE_MIGRATION_ID,
    )?;
    for metric in MetricFamily::Lifecycle.metrics() {
        let source_path = source_candidate_dir
            .join(wiki)
            .join(format!("{metric}.parquet"));
        let target_path = target_candidate_dir
            .join(wiki)
            .join(format!("{metric}.parquet"));
        let source_document = artifact_receipt::read(&source_path)?;
        artifact_receipt::scan_and_write(
            &target_path,
            &source_document.receipt.identity,
            crate::compute::lifecycle::ALGORITHM_VERSION,
            &migration_fingerprint,
        )?;
    }
    fingerprint::record(
        &family_receipt_path(target_candidate_dir, wiki, MetricFamily::Lifecycle),
        family_spec(
            wiki,
            snapshot,
            MetricFamily::Lifecycle,
            crate::compute::lifecycle::ALGORITHM_VERSION,
        ),
        &inputs,
        &family_outputs(target_candidate_dir, wiki, MetricFamily::Lifecycle),
    )?;
    let current = fingerprint::retained_outputs_reusable(
        &family_receipt_path(target_candidate_dir, wiki, MetricFamily::Lifecycle),
        family_spec(
            wiki,
            snapshot,
            MetricFamily::Lifecycle,
            crate::compute::lifecycle::ALGORITHM_VERSION,
        ),
        &family_outputs(target_candidate_dir, wiki, MetricFamily::Lifecycle),
    )?;
    ensure!(
        current,
        "retained lifecycle migration for {wiki} lost its authenticated source receipt"
    );
    Ok(())
}

pub(crate) fn validate_activity_tier_migration(
    wiki: &str,
    snapshot: &str,
    source_run_id: &str,
    source_candidate_dir: &Path,
    target_candidate_dir: &Path,
) -> Result<()> {
    let (expected_inputs, _) = migration_inputs(
        wiki,
        snapshot,
        source_run_id,
        source_candidate_dir,
        MetricFamily::ActivityTiers,
        LEGACY_ACTIVITY_TIER_ALGORITHM,
        ACTIVITY_TIER_MIGRATION_ID,
    )?;
    let target_receipt_path =
        family_receipt_path(target_candidate_dir, wiki, MetricFamily::ActivityTiers);
    let target_reusable = fingerprint::retained_outputs_reusable(
        &target_receipt_path,
        family_spec(
            wiki,
            snapshot,
            MetricFamily::ActivityTiers,
            crate::compute::activity::ALGORITHM_VERSION,
        ),
        &family_outputs(target_candidate_dir, wiki, MetricFamily::ActivityTiers),
    )?;
    ensure!(
        target_reusable,
        "retained candidate {wiki} activity-tier v6 receipt is invalid"
    );
    let target_stage_receipt = fingerprint::read_receipt(&target_receipt_path)?;
    ensure!(
        target_stage_receipt.inputs.len() == expected_inputs.len(),
        "retained candidate {wiki} activity-tier migration has an incomplete source inventory"
    );
    for (record, input) in target_stage_receipt.inputs.iter().zip(&expected_inputs) {
        ensure!(
            record.identity == input.identity && fingerprint::artifact_matches(record, input)?,
            "retained candidate {wiki} activity-tier migration source evidence changed"
        );
    }

    for metric in MetricFamily::ActivityTiers.metrics() {
        let source_path = source_candidate_dir
            .join(wiki)
            .join(format!("{metric}.parquet"));
        let target_path = target_candidate_dir
            .join(wiki)
            .join(format!("{metric}.parquet"));
        let source_document = artifact_receipt::read(&source_path)?;
        let target_document = artifact_receipt::read(&target_path)?;
        let source_document = artifact_receipt::verify(
            &source_path,
            &source_document.receipt.identity,
            Some(&source_document.receipt_sha256),
            artifact_receipt::VerificationMode::Fast,
        )?;
        let target_document = artifact_receipt::verify(
            &target_path,
            &target_document.receipt.identity,
            Some(&target_document.receipt_sha256),
            artifact_receipt::VerificationMode::Fast,
        )?;
        let source = source_document.receipt;
        let target = target_document.receipt;
        ensure!(
            source.algorithm_version == LEGACY_ACTIVITY_TIER_ALGORITHM
                && target.algorithm_version == crate::compute::activity::ALGORITHM_VERSION
                && source.identity == target.identity
                && source.artifact_sha256 == target.artifact_sha256
                && source.bytes == target.bytes
                && source.parquet_schema == target.parquet_schema
                && source.rows == target.rows
                && source.minimum_date == target.minimum_date
                && source.maximum_date == target.maximum_date
                && source.conservation_totals == target.conservation_totals
                && source.minimum_wiki == target.minimum_wiki
                && source.maximum_wiki == target.maximum_wiki
                && source.ordering_contract == target.ordering_contract,
            "retained candidate {wiki} {metric} changed during activity-tier receipt migration"
        );
    }
    Ok(())
}

pub(crate) struct TemporaryCandidateDirectory {
    path: PathBuf,
}

impl TemporaryCandidateDirectory {
    pub(crate) fn create(path: PathBuf) -> Result<Self> {
        std::fs::create_dir(&path).with_context(|| {
            format!(
                "failed to create retained migration staging directory {}",
                path.display()
            )
        })?;
        Ok(Self { path })
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TemporaryCandidateDirectory {
    fn drop(&mut self) {
        let Ok(metadata) = std::fs::symlink_metadata(&self.path) else {
            return;
        };
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            return;
        }
        if let Err(error) = std::fs::remove_dir_all(&self.path) {
            tracing::warn!(path = %self.path.display(), %error, "failed to remove temporary retained migration source");
        } else if let Some(parent) = self.path.parent() {
            if let Ok(directory) = std::fs::File::open(parent) {
                let _ = directory.sync_all();
            }
        }
    }
}
