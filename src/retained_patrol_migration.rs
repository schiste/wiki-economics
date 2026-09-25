//! Receipt-backed migration of retained patrol outputs across the v5 to v6
//! coverage-rounding correction. The authenticated Parquet already contains
//! the integer counts needed to correct both derived percentage columns.

use anyhow::{Context, Result, ensure};
use polars::prelude::*;
use std::fs::{self, File};
use std::path::{Component, Path, PathBuf};

use crate::{artifact_receipt, fingerprint, storage};

const LEGACY_PATROL_ALGORITHM: &str = "patrol-metrics-v5-complete-snapshot-months";

fn patrol_output(candidate_dir: &Path, wiki: &str) -> PathBuf {
    candidate_dir.join(wiki).join("patrol.parquet")
}

fn patrol_stage_receipt(candidate_dir: &Path, wiki: &str) -> PathBuf {
    candidate_dir
        .join("_stages")
        .join("patrol_compute")
        .join(format!("{wiki}.json"))
}

fn patrol_output_tracked(candidate_dir: &Path, wiki: &str) -> fingerprint::TrackedPath {
    fingerprint::TrackedPath::new(
        format!("output/{wiki}/patrol.parquet"),
        patrol_output(candidate_dir, wiki),
    )
}

fn current_receipt_valid(candidate_dir: &Path, wiki: &str, snapshot: &str) -> Result<bool> {
    let output = patrol_output(candidate_dir, wiki);
    if !fingerprint::retained_outputs_reusable(
        &patrol_stage_receipt(candidate_dir, wiki),
        patrol_spec(wiki, snapshot, crate::patrol::algorithm_version()),
        &[patrol_output_tracked(candidate_dir, wiki)],
    )? {
        return Ok(false);
    }
    let document = artifact_receipt::read(&output)?;
    let document = artifact_receipt::verify(
        &output,
        &format!("output/{wiki}/patrol.parquet"),
        Some(&document.receipt_sha256),
        artifact_receipt::VerificationMode::Fast,
    )?;
    Ok(document.receipt.algorithm_version == crate::patrol::algorithm_version())
}

fn ensure_same_output_bytes(source: &Path, target: &Path, wiki: &str) -> Result<()> {
    let (source_bytes, source_sha256) = storage::sha256_file(source)?;
    let (target_bytes, target_sha256) =
        storage::sha256_file(target).context("retained patrol staging copy is missing")?;
    ensure!(
        source_bytes == target_bytes && source_sha256 == target_sha256,
        "retained candidate {wiki} patrol staging copy differs from its authenticated source"
    );
    Ok(())
}

fn patrol_spec<'a>(
    wiki: &'a str,
    snapshot: &'a str,
    algorithm_version: &'a str,
) -> fingerprint::StageSpec<'a> {
    fingerprint::StageSpec {
        stage: "patrol_compute",
        scope: wiki,
        selected_snapshot: Some(snapshot),
        algorithm_version,
    }
}

fn valid_component(value: &str) -> bool {
    if value.is_empty() {
        return false;
    }
    let mut components = Path::new(value).components();
    matches!(components.next(), Some(Component::Normal(_))) && components.next().is_none()
}

fn source_migration_inputs(
    wiki: &str,
    snapshot: &str,
    source_run_id: &str,
    source_candidate_dir: &Path,
) -> Result<Vec<fingerprint::TrackedPath>> {
    ensure!(
        valid_component(wiki) && valid_component(snapshot) && valid_component(source_run_id),
        "unsafe retained patrol migration identity"
    );
    storage::validate_snapshot_version(snapshot)?;

    let source_output = patrol_output(source_candidate_dir, wiki);
    let source_stage_path = patrol_stage_receipt(source_candidate_dir, wiki);
    let outputs = [patrol_output_tracked(source_candidate_dir, wiki)];
    ensure!(
        fingerprint::retained_outputs_reusable(
            &source_stage_path,
            patrol_spec(wiki, snapshot, LEGACY_PATROL_ALGORITHM),
            &outputs,
        )?,
        "retained candidate {wiki} does not have an authenticated patrol v5 receipt"
    );

    let source_stage = fingerprint::read_receipt(&source_stage_path)?;
    ensure!(
        source_stage.stage == "patrol_compute"
            && source_stage.scope == wiki
            && source_stage.selected_snapshot.as_deref() == Some(snapshot)
            && source_stage.algorithm_version == LEGACY_PATROL_ALGORITHM,
        "retained candidate {wiki} patrol v5 stage identity is invalid"
    );

    let source_document = artifact_receipt::read(&source_output)?;
    let source_document = artifact_receipt::verify(
        &source_output,
        &format!("output/{wiki}/patrol.parquet"),
        Some(&source_document.receipt_sha256),
        artifact_receipt::VerificationMode::Fast,
    )?;
    ensure!(
        source_document.receipt.algorithm_version == LEGACY_PATROL_ALGORITHM,
        "retained candidate {wiki} patrol artifact is not authenticated as v5"
    );

    let source_identity = format!("retained-patrol-migration/{wiki}/{snapshot}/{source_run_id}");
    let mut inputs = vec![
        fingerprint::TrackedPath::new(
            format!("{source_identity}/stage-receipt"),
            source_stage_path,
        ),
        fingerprint::TrackedPath::new(
            format!("{source_identity}/patrol.parquet"),
            source_output.clone(),
        ),
        fingerprint::TrackedPath::new(
            format!("{source_identity}/patrol.parquet.receipt.json"),
            artifact_receipt::sidecar_path(&source_output)?,
        ),
    ];
    inputs.sort_by(|left, right| left.identity.cmp(&right.identity));
    Ok(inputs)
}

/// Return true only when the candidate has the known authenticated v5 receipt
/// that can be transformed from its retained integer counts.
pub(crate) fn migration_required(
    wiki: &str,
    snapshot: &str,
    source_run_id: &str,
    source_candidate_dir: &Path,
) -> Result<bool> {
    ensure!(
        valid_component(wiki) && valid_component(source_run_id),
        "unsafe retained patrol migration identity"
    );
    storage::validate_snapshot_version(snapshot)?;
    if current_receipt_valid(source_candidate_dir, wiki, snapshot)? {
        return Ok(false);
    }
    source_migration_inputs(wiki, snapshot, source_run_id, source_candidate_dir)?;
    Ok(true)
}

/// Migrate an authenticated v5 patrol output in the copied candidate, or
/// verify that a current v6 candidate was copied without changing its bytes.
pub(crate) fn migrate_if_required(
    wiki: &str,
    snapshot: &str,
    source_run_id: &str,
    source_candidate_dir: &Path,
    target_candidate_dir: &Path,
) -> Result<bool> {
    if migration_required(wiki, snapshot, source_run_id, source_candidate_dir)? {
        migrate_candidate(
            wiki,
            snapshot,
            source_run_id,
            source_candidate_dir,
            target_candidate_dir,
        )?;
        return Ok(true);
    }
    ensure!(
        current_receipt_valid(target_candidate_dir, wiki, snapshot)?,
        "retained candidate {wiki} current patrol receipt was lost while staging"
    );
    ensure_same_output_bytes(
        &patrol_output(source_candidate_dir, wiki),
        &patrol_output(target_candidate_dir, wiki),
        wiki,
    )?;
    Ok(false)
}

/// Rewrite the two coverage columns in a copied candidate, then record a new
/// stage receipt whose inputs identify the authenticated retained v5 source.
pub(crate) fn migrate_candidate(
    wiki: &str,
    snapshot: &str,
    source_run_id: &str,
    source_candidate_dir: &Path,
    target_candidate_dir: &Path,
) -> Result<()> {
    let inputs = source_migration_inputs(wiki, snapshot, source_run_id, source_candidate_dir)?;
    let source_output = patrol_output(source_candidate_dir, wiki);
    let target_output = patrol_output(target_candidate_dir, wiki);
    ensure_same_output_bytes(&source_output, &target_output, wiki)?;

    rewrite_coverage_columns(&target_output)?;
    fingerprint::record(
        &patrol_stage_receipt(target_candidate_dir, wiki),
        patrol_spec(wiki, snapshot, crate::patrol::algorithm_version()),
        &inputs,
        &[patrol_output_tracked(target_candidate_dir, wiki)],
    )?;
    ensure!(
        fingerprint::retained_outputs_reusable(
            &patrol_stage_receipt(target_candidate_dir, wiki),
            patrol_spec(wiki, snapshot, crate::patrol::algorithm_version()),
            &[patrol_output_tracked(target_candidate_dir, wiki)],
        )?,
        "retained patrol migration for {wiki} did not produce a current v6 receipt"
    );
    Ok(())
}

/// Verify source lineage, stage receipt inputs, artifact receipts, preserved
/// columns, and exact v6 ratios on a migrated candidate.
pub(crate) fn validate_migration(
    wiki: &str,
    snapshot: &str,
    source_run_id: &str,
    source_candidate_dir: &Path,
    target_candidate_dir: &Path,
    migration_expected: bool,
) -> Result<()> {
    let migration_required =
        migration_required(wiki, snapshot, source_run_id, source_candidate_dir)?;
    ensure!(
        migration_required == migration_expected,
        "retained candidate {wiki} patrol migration provenance does not match its source receipt"
    );
    if !migration_expected {
        ensure!(
            current_receipt_valid(target_candidate_dir, wiki, snapshot)?,
            "retained candidate {wiki} current patrol receipt is invalid"
        );
        ensure_same_output_bytes(
            &patrol_output(source_candidate_dir, wiki),
            &patrol_output(target_candidate_dir, wiki),
            wiki,
        )?;
        return Ok(());
    }
    let expected_inputs =
        source_migration_inputs(wiki, snapshot, source_run_id, source_candidate_dir)?;
    let target_output = patrol_output(target_candidate_dir, wiki);
    let target_stage_path = patrol_stage_receipt(target_candidate_dir, wiki);
    let target_outputs = [patrol_output_tracked(target_candidate_dir, wiki)];
    ensure!(
        fingerprint::retained_outputs_reusable(
            &target_stage_path,
            patrol_spec(wiki, snapshot, crate::patrol::algorithm_version()),
            &target_outputs,
        )?,
        "retained candidate {wiki} patrol v6 receipt is invalid"
    );

    let target_stage = fingerprint::read_receipt(&target_stage_path)?;
    ensure!(
        target_stage.inputs.len() == expected_inputs.len(),
        "retained candidate {wiki} patrol migration has an incomplete source inventory"
    );
    for (record, input) in target_stage.inputs.iter().zip(&expected_inputs) {
        ensure!(
            record.identity == input.identity && fingerprint::artifact_matches(record, input)?,
            "retained candidate {wiki} patrol migration source evidence changed"
        );
    }

    let target_document = artifact_receipt::read(&target_output)?;
    let target_document = artifact_receipt::verify(
        &target_output,
        &format!("output/{wiki}/patrol.parquet"),
        Some(&target_document.receipt_sha256),
        artifact_receipt::VerificationMode::Fast,
    )?;
    ensure!(
        target_document.receipt.algorithm_version == crate::patrol::algorithm_version(),
        "retained candidate {wiki} patrol artifact is not authenticated as v6"
    );

    let source_output = patrol_output(source_candidate_dir, wiki);
    let source_frame = ParquetReader::new(File::open(&source_output)?).finish()?;
    let target_frame = ParquetReader::new(File::open(&target_output)?).finish()?;
    let source_columns = source_frame
        .get_column_names()
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    let target_columns = target_frame
        .get_column_names()
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    ensure!(
        source_frame.height() == target_frame.height() && source_columns == target_columns,
        "retained patrol migration changed the output shape"
    );

    const PRESERVED_COLUMNS: [&str; 15] = [
        "year_month",
        "wiki",
        "page_namespace",
        "user_type",
        "total_patrols",
        "unique_patrollers",
        "patrol_new_pages",
        "patrol_diffs",
        "median_latency_hours",
        "p90_latency_hours",
        "patrolled_revisions",
        "autopatrolled_revisions",
        "total_revisions",
        "top1_pct",
        "min_patrollers_50pct",
    ];
    let source_columns = PRESERVED_COLUMNS
        .iter()
        .map(|name| source_frame.column(name).cloned())
        .collect::<PolarsResult<Vec<_>>>()?;
    let target_columns = PRESERVED_COLUMNS
        .iter()
        .map(|name| target_frame.column(name).cloned())
        .collect::<PolarsResult<Vec<_>>>()?;
    ensure!(
        DataFrame::new_infer_height(source_columns)?
            .equals_missing(&DataFrame::new_infer_height(target_columns)?),
        "retained patrol migration changed a non-coverage column"
    );
    validate_coverage_columns(&target_frame)?;
    Ok(())
}

fn non_null_i64_values(frame: &DataFrame, name: &str) -> Result<Vec<i64>> {
    let column = frame.column(name)?.cast(&DataType::Int64)?;
    column
        .i64()?
        .iter()
        .map(|value| {
            value
                .copied()
                .with_context(|| format!("patrol {name} contains a null count"))
        })
        .collect()
}

fn calculated_coverage(frame: &DataFrame) -> Result<(Vec<f64>, Vec<f64>)> {
    let patrolled = non_null_i64_values(frame, "patrolled_revisions")?;
    let autopatrolled = non_null_i64_values(frame, "autopatrolled_revisions")?;
    let total = non_null_i64_values(frame, "total_revisions")?;
    ensure!(
        patrolled.len() == autopatrolled.len() && patrolled.len() == total.len(),
        "patrol count columns have different lengths"
    );

    let mut patrol_coverage = Vec::with_capacity(total.len());
    let mut adjusted_coverage = Vec::with_capacity(total.len());
    for index in 0..total.len() {
        let (patrolled, autopatrolled, total) =
            (patrolled[index], autopatrolled[index], total[index]);
        ensure!(
            patrolled >= 0 && autopatrolled >= 0 && total >= 0,
            "patrol count columns contain a negative value at row {index}"
        );
        if total == 0 {
            patrol_coverage.push(0.0);
            adjusted_coverage.push(0.0);
        } else {
            patrol_coverage.push(100.0 * patrolled as f64 / total as f64);
            adjusted_coverage
                .push(100.0 * (patrolled as f64 + autopatrolled as f64) / total as f64);
        }
    }
    Ok((patrol_coverage, adjusted_coverage))
}

fn rewrite_coverage_columns(path: &Path) -> Result<()> {
    let mut frame = ParquetReader::new(File::open(path)?).finish()?;
    let (patrol_coverage, adjusted_coverage) = calculated_coverage(&frame)?;
    frame.with_column(Series::new("patrol_coverage_pct".into(), patrol_coverage));
    frame.with_column(Series::new(
        "adjusted_coverage_pct".into(),
        adjusted_coverage,
    ));

    let parent = path.parent().context("patrol artifact has no parent")?;
    let temporary = parent.join(format!(".patrol-migration-{}.tmp", std::process::id()));
    if temporary.exists() {
        fs::remove_file(&temporary)?;
    }
    let write_result = (|| -> Result<()> {
        let mut file = File::create(&temporary)?;
        ParquetWriter::new(&mut file)
            .with_compression(ParquetCompression::Zstd(None))
            .finish(&mut frame)?;
        file.sync_all()?;
        fs::rename(&temporary, path)?;
        File::open(parent)?.sync_all()?;
        Ok(())
    })();
    if write_result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    write_result
}

fn validate_coverage_columns(frame: &DataFrame) -> Result<()> {
    let (expected_patrol, expected_adjusted) = calculated_coverage(frame)?;
    let observed_patrol = frame.column("patrol_coverage_pct")?.f64()?;
    let observed_adjusted = frame.column("adjusted_coverage_pct")?.f64()?;
    ensure!(
        observed_patrol.len() == expected_patrol.len()
            && observed_adjusted.len() == expected_adjusted.len(),
        "retained patrol migration changed the coverage row count"
    );
    for index in 0..expected_patrol.len() {
        ensure!(
            observed_patrol.get(index) == Some(expected_patrol[index])
                && observed_adjusted.get(index) == Some(expected_adjusted[index]),
            "retained patrol migration has an incorrect v6 coverage value at row {index}"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::TestDir;
    use anyhow::Result;

    fn write_legacy_source(
        root: &Path,
        wiki: &str,
        snapshot: &str,
        run_id: &str,
    ) -> Result<PathBuf> {
        let candidate = root
            .join("_candidates")
            .join(wiki)
            .join(snapshot)
            .join(run_id);
        let wiki_dir = candidate.join(wiki);
        fs::create_dir_all(&wiki_dir)?;
        let path = patrol_output(&candidate, wiki);

        let patrolled = [1_i64, 0];
        let autopatrolled = [2_i64, 0];
        let total = [3_i64, 0];
        let old_patrol_coverage = [patrolled[0] as f64 / total[0] as f64 * 100.0, 0.0];
        let old_adjusted_coverage = [
            (patrolled[0] as f64 + autopatrolled[0] as f64) / total[0] as f64 * 100.0,
            0.0,
        ];
        let mut frame = DataFrame::new_infer_height(vec![
            Column::new("year_month".into(), vec!["2026-03", "2026-03"]),
            Column::new("wiki".into(), vec![wiki, wiki]),
            Column::new("page_namespace".into(), vec![0_i32, 0]),
            Column::new("user_type".into(), vec!["registered", "registered"]),
            Column::new("total_patrols".into(), vec![1_i64, 0]),
            Column::new("unique_patrollers".into(), vec![1_i32, 0]),
            Column::new("patrol_new_pages".into(), vec![0_i64, 0]),
            Column::new("patrol_diffs".into(), vec![1_i64, 0]),
            Column::new("median_latency_hours".into(), vec![0.5_f64, 0.0]),
            Column::new("p90_latency_hours".into(), vec![1.0_f64, 0.0]),
            Column::new("patrolled_revisions".into(), patrolled.to_vec()),
            Column::new("autopatrolled_revisions".into(), autopatrolled.to_vec()),
            Column::new("total_revisions".into(), total.to_vec()),
            Column::new("patrol_coverage_pct".into(), old_patrol_coverage.to_vec()),
            Column::new(
                "adjusted_coverage_pct".into(),
                old_adjusted_coverage.to_vec(),
            ),
            Column::new("top1_pct".into(), vec![100.0_f64, 0.0]),
            Column::new("min_patrollers_50pct".into(), vec![1_i32, 0]),
        ])?;
        ParquetWriter::new(File::create(&path)?)
            .with_compression(ParquetCompression::Zstd(None))
            .finish(&mut frame)?;

        let source_input = root.join(format!("{run_id}-patrol-generation-manifest"));
        fs::write(&source_input, b"authenticated legacy source fixture")?;
        fingerprint::record(
            &patrol_stage_receipt(&candidate, wiki),
            patrol_spec(wiki, snapshot, LEGACY_PATROL_ALGORITHM),
            &[fingerprint::TrackedPath::new(
                format!("patrol-generation/{wiki}/{snapshot}/manifest"),
                source_input,
            )],
            &[patrol_output_tracked(&candidate, wiki)],
        )?;
        Ok(candidate)
    }

    fn copy_legacy_candidate(source: &Path, target: &Path, wiki: &str) -> Result<()> {
        fs::create_dir_all(target.join(wiki))?;
        fs::create_dir_all(
            patrol_stage_receipt(target, wiki)
                .parent()
                .context("target stage receipt has no parent")?,
        )?;
        let source_output = patrol_output(source, wiki);
        let target_output = patrol_output(target, wiki);
        fs::copy(&source_output, &target_output)?;
        fs::copy(
            artifact_receipt::sidecar_path(&source_output)?,
            artifact_receipt::sidecar_path(&target_output)?,
        )?;
        fs::copy(
            patrol_stage_receipt(source, wiki),
            patrol_stage_receipt(target, wiki),
        )?;
        Ok(())
    }

    #[test]
    fn retained_patrol_v5_migration_recalculates_ratios_and_preserves_source() -> Result<()> {
        let root = TestDir::new()?;
        let wiki = "nlwiki";
        let snapshot = "2026-03";
        let source = write_legacy_source(root.path(), wiki, snapshot, "legacy-source")?;
        let target = root
            .path()
            .join("_candidates")
            .join(wiki)
            .join(snapshot)
            .join("migrated");
        copy_legacy_candidate(&source, &target, wiki)?;
        let stale_temporary = patrol_output(&target, wiki)
            .parent()
            .context("patrol output has no parent")?
            .join(format!(".patrol-migration-{}.tmp", std::process::id()));
        fs::write(&stale_temporary, b"stale migration temporary")?;

        let source_output = patrol_output(&source, wiki);
        let (_, source_sha256_before) = storage::sha256_file(&source_output)?;
        ensure!(migration_required(
            wiki,
            snapshot,
            "legacy-source",
            &source
        )?);
        ensure!(migrate_if_required(
            wiki,
            snapshot,
            "legacy-source",
            &source,
            &target
        )?);
        validate_migration(wiki, snapshot, "legacy-source", &source, &target, true)?;
        ensure!(!migration_required(wiki, snapshot, "migrated", &target)?);
        ensure!(!migrate_if_required(
            wiki, snapshot, "migrated", &target, &target
        )?);
        validate_migration(wiki, snapshot, "migrated", &target, &target, false)?;

        let (_, source_sha256_after) = storage::sha256_file(&source_output)?;
        ensure!(source_sha256_before == source_sha256_after);
        let source_frame = ParquetReader::new(File::open(&source_output)?).finish()?;
        let target_frame =
            ParquetReader::new(File::open(patrol_output(&target, wiki))?).finish()?;
        ensure!(
            source_frame.column("patrol_coverage_pct")?.f64()?.get(0)
                != target_frame.column("patrol_coverage_pct")?.f64()?.get(0)
        );
        ensure!(!stale_temporary.exists());
        validate_coverage_columns(&target_frame)?;
        Ok(())
    }

    #[test]
    fn retained_patrol_migration_rejects_unsafe_identity_and_bad_counts() -> Result<()> {
        let root = TestDir::new()?;
        ensure!(migration_required("../unsafe", "2026-03", "source", root.path()).is_err());
        ensure!(migration_required("", "2026-03", "source", root.path()).is_err());

        let negative_counts = DataFrame::new_infer_height(vec![
            Column::new("patrolled_revisions".into(), vec![-1_i64]),
            Column::new("autopatrolled_revisions".into(), vec![0_i64]),
            Column::new("total_revisions".into(), vec![1_i64]),
        ])?;
        ensure!(calculated_coverage(&negative_counts).is_err());
        let null_counts = DataFrame::new_infer_height(vec![
            Column::new("patrolled_revisions".into(), vec![None::<i64>]),
            Column::new("autopatrolled_revisions".into(), vec![Some(0_i64)]),
            Column::new("total_revisions".into(), vec![Some(1_i64)]),
        ])?;
        ensure!(calculated_coverage(&null_counts).is_err());

        let incorrect_coverage = DataFrame::new_infer_height(vec![
            Column::new("patrolled_revisions".into(), vec![1_i64]),
            Column::new("autopatrolled_revisions".into(), vec![0_i64]),
            Column::new("total_revisions".into(), vec![2_i64]),
            Column::new("patrol_coverage_pct".into(), vec![0.0_f64]),
            Column::new("adjusted_coverage_pct".into(), vec![0.0_f64]),
        ])?;
        ensure!(validate_coverage_columns(&incorrect_coverage).is_err());
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn failed_patrol_migration_write_cleans_up_temporary_file() -> Result<()> {
        use std::os::unix::fs::PermissionsExt;

        let root = TestDir::new()?;
        let candidate = write_legacy_source(root.path(), "nlwiki", "2026-03", "legacy")?;
        let output = patrol_output(&candidate, "nlwiki");
        let parent = output.parent().context("patrol output has no parent")?;
        let original_permissions = fs::metadata(parent)?.permissions();
        let mut restricted_permissions = original_permissions.clone();
        restricted_permissions.set_mode(original_permissions.mode() & !0o222);
        fs::set_permissions(parent, restricted_permissions)?;
        let result = rewrite_coverage_columns(&output);
        fs::set_permissions(parent, original_permissions)?;
        ensure!(result.is_err());
        let temporary = parent.join(format!(".patrol-migration-{}.tmp", std::process::id()));
        ensure!(!temporary.exists());
        Ok(())
    }

    #[test]
    fn retained_patrol_v5_migration_rejects_tampering_and_unknown_algorithm() -> Result<()> {
        let root = TestDir::new()?;
        let wiki = "nlwiki";
        let snapshot = "2026-03";
        let source = write_legacy_source(root.path(), wiki, snapshot, "legacy-source")?;
        let target = root
            .path()
            .join("_candidates")
            .join(wiki)
            .join(snapshot)
            .join("tampered");
        copy_legacy_candidate(&source, &target, wiki)?;
        fs::write(patrol_output(&target, wiki), b"tampered candidate copy")?;
        let error = migrate_candidate(wiki, snapshot, "legacy-source", &source, &target)
            .expect_err("a staging copy that differs from its source must fail closed");
        ensure!(error.to_string().contains("staging copy differs"));

        let unsupported_root = root.path().join("unsupported");
        fs::create_dir_all(&unsupported_root)?;
        let unsupported_candidate =
            write_legacy_source(&unsupported_root, wiki, snapshot, "legacy-source")?;
        let receipt_path = patrol_stage_receipt(&unsupported_candidate, wiki);
        let receipt_bytes = fs::read(&receipt_path)?;
        let mut receipt: serde_json::Value = serde_json::from_slice(&receipt_bytes)?;
        receipt["algorithm_version"] = serde_json::Value::String("patrol-v99".to_string());
        fs::write(&receipt_path, serde_json::to_vec_pretty(&receipt)?)?;
        let error = migration_required(wiki, snapshot, "legacy-source", &unsupported_candidate)
            .expect_err("an unknown source algorithm must fail closed");
        ensure!(error.to_string().contains("authenticated patrol v5"));
        Ok(())
    }
}
