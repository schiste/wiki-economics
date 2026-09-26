//! Receipt-backed migration of retained monthly outputs from v5 to v6.
//!
//! The only data change is to make the three GDP ratios null when their
//! denominator is zero. All other monthly outputs are retained byte-for-byte.

use anyhow::{Context, Result, ensure};
use polars::prelude::*;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Component, Path, PathBuf};

use crate::{artifact_receipt, fingerprint, metric_registry::MetricFamily, storage};

pub(crate) const MIGRATION_ID: &str = "retained-monthly-v5-to-v6-null-zero-denominator-ratios-v1";
const LEGACY_ALGORITHM: &str = "monthly-stateless-v5-exact-period-inequality";
const CURRENT_ALGORITHM: &str = MetricFamily::Monthly.base_algorithm_version();
const RATIO_COLUMNS: [&str; 3] = ["bytes_per_edit", "bytes_per_editor", "revert_rate"];

fn stage_receipt(candidate_dir: &Path, wiki: &str) -> PathBuf {
    candidate_dir
        .join("_stages")
        .join("compute")
        .join("monthly")
        .join(format!("{wiki}.json"))
}

fn output(candidate_dir: &Path, wiki: &str, metric: &str) -> PathBuf {
    candidate_dir.join(wiki).join(format!("{metric}.parquet"))
}

fn report_path(candidate_dir: &Path, wiki: &str) -> PathBuf {
    crate::compute::editor_identity_report_path(candidate_dir, wiki)
}

fn stage_spec<'a>(
    wiki: &'a str,
    snapshot: &'a str,
    algorithm: &'a str,
) -> fingerprint::StageSpec<'a> {
    fingerprint::StageSpec {
        stage: "compute_monthly",
        scope: wiki,
        selected_snapshot: Some(snapshot),
        algorithm_version: algorithm,
    }
}

fn tracked_outputs(candidate_dir: &Path, wiki: &str) -> Vec<fingerprint::TrackedPath> {
    let mut outputs = MetricFamily::Monthly
        .metrics()
        .iter()
        .map(|metric| {
            fingerprint::TrackedPath::new(
                format!("output/{wiki}/{metric}.parquet"),
                output(candidate_dir, wiki, metric),
            )
        })
        .collect::<Vec<_>>();
    outputs.push(fingerprint::TrackedPath::new(
        format!("output/{wiki}/{}", crate::compute::EDITOR_IDENTITY_REPORT),
        report_path(candidate_dir, wiki),
    ));
    outputs
}

fn valid_component(value: &str) -> bool {
    if value.is_empty() {
        return false;
    }
    let mut components = Path::new(value).components();
    matches!(components.next(), Some(Component::Normal(_))) && components.next().is_none()
}

fn validate_report(
    candidate_dir: &Path,
    wiki: &str,
    snapshot: &str,
    algorithm: &str,
) -> Result<crate::compute::EditorIdentityCoverageReport> {
    let report: crate::compute::EditorIdentityCoverageReport = serde_json::from_slice(
        &fs::read(report_path(candidate_dir, wiki))
            .with_context(|| format!("missing {wiki} editor identity coverage report"))?,
    )?;
    let mut previous: Option<(&str, &str)> = None;
    let mut totals = (0_u64, 0_u64, 0_u64);
    for period in &report.periods {
        let key = (period.year_month.as_str(), period.user_type.as_str());
        ensure!(
            previous.is_none_or(|prior| prior < key)
                && period.total_edits == period.identified_edits + period.excluded_edits,
            "retained {wiki} monthly migration found invalid identity coverage periods"
        );
        previous = Some(key);
        totals.0 = totals
            .0
            .checked_add(period.total_edits)
            .context("identity coverage total overflow")?;
        totals.1 = totals
            .1
            .checked_add(period.identified_edits)
            .context("identity coverage identified total overflow")?;
        totals.2 = totals
            .2
            .checked_add(period.excluded_edits)
            .context("identity coverage excluded total overflow")?;
    }
    ensure!(
        report.schema_version == 1
            && report.wiki == wiki
            && report.snapshot.as_deref() == Some(snapshot)
            && report.algorithm_version == algorithm
            && report.total_edits == totals.0
            && report.identified_edits == totals.1
            && report.excluded_edits == totals.2
            && report.total_edits == report.identified_edits + report.excluded_edits,
        "retained {wiki} monthly migration found an invalid identity coverage report"
    );
    Ok(report)
}

fn verify_artifact(path: &Path, wiki: &str, metric: &str, algorithm: &str) -> Result<()> {
    let expected_identity = format!("output/{wiki}/{metric}.parquet");
    let document = artifact_receipt::read(path)?;
    let document = artifact_receipt::verify(
        path,
        &expected_identity,
        Some(&document.receipt_sha256),
        artifact_receipt::VerificationMode::Fast,
    )?;
    ensure!(
        document.receipt.algorithm_version == algorithm,
        "retained candidate {wiki} {metric} artifact is not authenticated as {algorithm}"
    );
    Ok(())
}

fn current_receipt_valid(candidate_dir: &Path, wiki: &str, snapshot: &str) -> Result<bool> {
    let receipt_path = stage_receipt(candidate_dir, wiki);
    if !fingerprint::retained_outputs_reusable(
        &receipt_path,
        stage_spec(wiki, snapshot, CURRENT_ALGORITHM),
        &tracked_outputs(candidate_dir, wiki),
    )? {
        return Ok(false);
    }
    for metric in MetricFamily::Monthly.metrics() {
        verify_artifact(
            &output(candidate_dir, wiki, metric),
            wiki,
            metric,
            CURRENT_ALGORITHM,
        )?;
    }
    let _ = validate_report(candidate_dir, wiki, snapshot, CURRENT_ALGORITHM)?;
    Ok(true)
}

fn source_migration_inputs(
    wiki: &str,
    snapshot: &str,
    source_run_id: &str,
    source_candidate_dir: &Path,
) -> Result<Vec<fingerprint::TrackedPath>> {
    ensure!(
        valid_component(wiki) && valid_component(snapshot) && valid_component(source_run_id),
        "unsafe retained monthly migration identity"
    );
    storage::validate_snapshot_version(snapshot)?;
    let receipt_path = stage_receipt(source_candidate_dir, wiki);
    ensure!(
        fingerprint::retained_outputs_reusable(
            &receipt_path,
            stage_spec(wiki, snapshot, LEGACY_ALGORITHM),
            &tracked_outputs(source_candidate_dir, wiki),
        )?,
        "retained candidate {wiki} does not have an authenticated monthly v5 receipt"
    );
    let stage = fingerprint::read_receipt(&receipt_path)?;
    ensure!(
        stage.stage == "compute_monthly"
            && stage.scope == wiki
            && stage.selected_snapshot.as_deref() == Some(snapshot)
            && stage.algorithm_version == LEGACY_ALGORITHM,
        "retained candidate {wiki} monthly v5 stage identity is invalid"
    );
    let _ = validate_report(source_candidate_dir, wiki, snapshot, LEGACY_ALGORITHM)?;

    let source_identity = format!("retained-monthly-migration/{wiki}/{snapshot}/{source_run_id}");
    let mut inputs = vec![fingerprint::TrackedPath::new(
        format!("{source_identity}/monthly-stage-receipt"),
        receipt_path,
    )];
    for metric in MetricFamily::Monthly.metrics() {
        let path = output(source_candidate_dir, wiki, metric);
        verify_artifact(&path, wiki, metric, LEGACY_ALGORITHM)?;
        inputs.push(fingerprint::TrackedPath::new(
            format!("{source_identity}/{metric}.parquet"),
            path.clone(),
        ));
        inputs.push(fingerprint::TrackedPath::new(
            format!("{source_identity}/{metric}.parquet.receipt.json"),
            artifact_receipt::sidecar_path(&path)?,
        ));
    }
    inputs.push(fingerprint::TrackedPath::new(
        format!("{source_identity}/editor_identity_coverage.json"),
        report_path(source_candidate_dir, wiki),
    ));
    inputs.sort_by(|left, right| left.identity.cmp(&right.identity));
    Ok(inputs)
}

/// Return true only for the known authenticated monthly v5 family that can be
/// corrected from the retained GDP counts.
pub(crate) fn migration_required(
    wiki: &str,
    snapshot: &str,
    source_run_id: &str,
    source_candidate_dir: &Path,
) -> Result<bool> {
    if current_receipt_valid(source_candidate_dir, wiki, snapshot)? {
        return Ok(false);
    }
    source_migration_inputs(wiki, snapshot, source_run_id, source_candidate_dir)?;
    Ok(true)
}

fn ensure_same_bytes(source: &Path, target: &Path, wiki: &str, metric: &str) -> Result<()> {
    let (source_bytes, source_hash) = storage::sha256_file(source)?;
    let (target_bytes, target_hash) = storage::sha256_file(target)
        .with_context(|| format!("retained {wiki} monthly staging copy is missing {metric}"))?;
    ensure!(
        source_bytes == target_bytes && source_hash == target_hash,
        "retained candidate {wiki} monthly staging copy differs from its authenticated {metric}"
    );
    Ok(())
}

/// Rewrite the zero-denominator ratios in the copied GDP Parquet and record a
/// current monthly family receipt bound to the authenticated source outputs.
pub(crate) fn migrate_candidate(
    wiki: &str,
    snapshot: &str,
    source_run_id: &str,
    source_candidate_dir: &Path,
    target_candidate_dir: &Path,
) -> Result<()> {
    let inputs = source_migration_inputs(wiki, snapshot, source_run_id, source_candidate_dir)?;
    for metric in MetricFamily::Monthly.metrics() {
        ensure_same_bytes(
            &output(source_candidate_dir, wiki, metric),
            &output(target_candidate_dir, wiki, metric),
            wiki,
            metric,
        )?;
    }
    ensure_same_bytes(
        &report_path(source_candidate_dir, wiki),
        &report_path(target_candidate_dir, wiki),
        wiki,
        "editor_identity_coverage.json",
    )?;

    rewrite_gdp_ratios(
        &output(target_candidate_dir, wiki, "gdp"),
        wiki,
        target_candidate_dir,
    )?;
    let mut report = validate_report(target_candidate_dir, wiki, snapshot, LEGACY_ALGORITHM)?;
    report.algorithm_version = CURRENT_ALGORITHM.to_string();
    write_json_atomic(&report_path(target_candidate_dir, wiki), &report)?;

    let outputs = tracked_outputs(target_candidate_dir, wiki);
    fingerprint::record(
        &stage_receipt(target_candidate_dir, wiki),
        stage_spec(wiki, snapshot, CURRENT_ALGORITHM),
        &inputs,
        &outputs,
    )?;
    ensure!(
        current_receipt_valid(target_candidate_dir, wiki, snapshot)?,
        "retained monthly migration for {wiki} did not produce a current v6 receipt"
    );
    Ok(())
}

/// Verify the authenticated v5 source, the exact transformation, all current
/// output receipts, and the source evidence bound into the v6 family receipt.
pub(crate) fn validate_migration(
    wiki: &str,
    snapshot: &str,
    source_run_id: &str,
    source_candidate_dir: &Path,
    target_candidate_dir: &Path,
) -> Result<()> {
    ensure!(
        migration_required(wiki, snapshot, source_run_id, source_candidate_dir)?,
        "retained candidate {wiki} monthly migration provenance does not match its source receipt"
    );
    let expected_inputs =
        source_migration_inputs(wiki, snapshot, source_run_id, source_candidate_dir)?;
    ensure!(
        current_receipt_valid(target_candidate_dir, wiki, snapshot)?,
        "retained candidate {wiki} monthly v6 receipt is invalid"
    );
    let target_stage = fingerprint::read_receipt(&stage_receipt(target_candidate_dir, wiki))?;
    ensure!(
        target_stage.inputs.len() == expected_inputs.len(),
        "retained candidate {wiki} monthly migration has an incomplete source inventory"
    );
    for (record, input) in target_stage.inputs.iter().zip(&expected_inputs) {
        ensure!(
            record.identity == input.identity && fingerprint::artifact_matches(record, input)?,
            "retained candidate {wiki} monthly migration source evidence changed"
        );
    }

    for metric in ["gdp_user_type_share", "inequality", "labor_monthly"] {
        ensure_same_bytes(
            &output(source_candidate_dir, wiki, metric),
            &output(target_candidate_dir, wiki, metric),
            wiki,
            metric,
        )?;
    }
    validate_gdp_projection(
        &output(source_candidate_dir, wiki, "gdp"),
        &output(target_candidate_dir, wiki, "gdp"),
    )?;
    let mut expected_report =
        validate_report(source_candidate_dir, wiki, snapshot, LEGACY_ALGORITHM)?;
    expected_report.algorithm_version = CURRENT_ALGORITHM.to_string();
    let target_report = validate_report(target_candidate_dir, wiki, snapshot, CURRENT_ALGORITHM)?;
    ensure!(
        target_report == expected_report,
        "retained candidate {wiki} monthly migration changed editor identity coverage"
    );
    Ok(())
}

fn ratio_expression(
    numerator: &'static str,
    denominator: &'static str,
    name: &'static str,
) -> Expr {
    let denominator_value = col(denominator).cast(DataType::Float64);
    when(denominator_value.clone().gt(lit(0.0_f64)))
        .then(col(numerator).cast(DataType::Float64) / denominator_value)
        .otherwise(lit(NULL).cast(DataType::Float64))
        .alias(name)
}

fn rewrite_gdp_ratios(path: &Path, wiki: &str, candidate_dir: &Path) -> Result<()> {
    let source = ParquetReader::new(File::open(path)?).finish()?;
    let mut migrated = source
        .lazy()
        .with_columns([
            ratio_expression("net_bytes", "total_edits", "bytes_per_edit"),
            ratio_expression("net_bytes", "unique_editors", "bytes_per_editor"),
            ratio_expression("reverted_edits", "total_edits", "revert_rate"),
        ])
        .collect()?;
    crate::compute::write_output(&mut migrated, wiki, "gdp", candidate_dir)
}

fn validate_gdp_projection(source_path: &Path, target_path: &Path) -> Result<()> {
    let source = ParquetReader::new(File::open(source_path)?).finish()?;
    let target = ParquetReader::new(File::open(target_path)?).finish()?;
    let source_names = source
        .get_column_names()
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    let target_names = target
        .get_column_names()
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    ensure!(
        source.height() == target.height() && source_names == target_names,
        "retained monthly migration changed the GDP output shape"
    );
    let preserved_source = source
        .columns()
        .iter()
        .filter(|column| !RATIO_COLUMNS.contains(&column.name().as_str()))
        .cloned()
        .collect::<Vec<_>>();
    let preserved_target = target
        .columns()
        .iter()
        .filter(|column| !RATIO_COLUMNS.contains(&column.name().as_str()))
        .cloned()
        .collect::<Vec<_>>();
    ensure!(
        DataFrame::new_infer_height(preserved_source)?
            .equals_missing(&DataFrame::new_infer_height(preserved_target)?),
        "retained monthly migration changed a non-ratio GDP column"
    );

    for (ratio, numerator, denominator) in [
        ("bytes_per_edit", "net_bytes", "total_edits"),
        ("bytes_per_editor", "net_bytes", "unique_editors"),
        ("revert_rate", "reverted_edits", "total_edits"),
    ] {
        let numerators = source.column(numerator)?.cast(&DataType::Float64)?;
        let denominators = source.column(denominator)?.cast(&DataType::Float64)?;
        let observed = target.column(ratio)?.f64()?;
        for index in 0..source.height() {
            let numerator_value = numerators
                .f64()?
                .get(index)
                .with_context(|| format!("GDP {numerator} is null at row {index}"))?;
            let denominator_value = denominators
                .f64()?
                .get(index)
                .with_context(|| format!("GDP {denominator} is null at row {index}"))?;
            if denominator_value > 0.0 {
                ensure!(
                    observed.get(index) == Some(numerator_value / denominator_value),
                    "retained monthly migration has an incorrect {ratio} at row {index}"
                );
            } else {
                ensure!(
                    denominator_value == 0.0 && observed.get(index).is_none(),
                    "retained monthly migration did not null zero-denominator {ratio} at row {index}"
                );
            }
        }
    }
    Ok(())
}

fn write_json_atomic<T: serde::Serialize>(path: &Path, value: &T) -> Result<()> {
    let parent = path.parent().context("monthly report has no parent")?;
    fs::create_dir_all(parent)?;
    let name = path.file_name().context("monthly report has no filename")?;
    let temporary = parent.join(format!(
        ".{}.{}.tmp",
        name.to_string_lossy(),
        std::process::id()
    ));
    let result = (|| -> Result<()> {
        let mut file = File::create(&temporary)?;
        serde_json::to_writer_pretty(&mut file, value)?;
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
    fn failed_atomic_report_write_returns_an_error() -> Result<()> {
        let root = TestDir::new()?;
        let blocked_parent = root.path().join("blocked-parent");
        fs::write(&blocked_parent, b"not a directory")?;
        assert!(
            write_json_atomic(
                &blocked_parent.join("editor_identity_coverage.json"),
                &serde_json::json!({"schema_version": 1}),
            )
            .is_err()
        );
        Ok(())
    }
}
