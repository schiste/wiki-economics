use anyhow::{Context, Result};
use serde::Serialize;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use crate::snapshot_plan::SnapshotPlan;
use crate::{ingest, storage};

#[derive(Debug, Default, Serialize)]
pub struct CleanupReport {
    pub merge_temporaries: usize,
    pub site_builds: usize,
    pub run_staging_directories: usize,
    pub weekly_scratch_directories: usize,
    pub capacity_staging_directories: usize,
    pub validated_raw_dumps: usize,
    pub snapshot_generations: usize,
    pub removed: Vec<String>,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct CleanupStagingRoots<'a> {
    pub weekly: Option<&'a Path>,
    pub capacity: Option<&'a Path>,
}

pub fn clean_abandoned(
    data_dir: &Path,
    output_dir: &Path,
    site_dist_dir: &Path,
    staging: CleanupStagingRoots<'_>,
    wikis: &[String],
    current_run_id: Option<&str>,
    minimum_age: Duration,
) -> Result<CleanupReport> {
    let now = SystemTime::now();
    let mut report = CleanupReport::default();
    clean_merge_temporaries(output_dir, current_run_id, minimum_age, now, &mut report)?;
    clean_site_builds(site_dist_dir, current_run_id, minimum_age, now, &mut report)?;
    clean_run_staging(output_dir, current_run_id, minimum_age, now, &mut report)?;
    if let Some(scratch_dir) = staging.weekly {
        clean_weekly_scratch(scratch_dir, current_run_id, minimum_age, now, &mut report)?;
    }
    if let Some(capacity_dir) = staging.capacity {
        clean_capacity_staging(capacity_dir, current_run_id, minimum_age, now, &mut report)?;
    }
    for wiki in wikis {
        clean_validated_raw_dumps(data_dir, wiki, &mut report)?;
        report.snapshot_generations += storage::clean_stale_inactive_snapshots(
            data_dir,
            wiki,
            minimum_age,
            now,
            &mut report.removed,
        )?;
    }
    report.removed.sort();
    Ok(report)
}

fn clean_validated_raw_dumps(
    data_dir: &Path,
    wiki: &str,
    report: &mut CleanupReport,
) -> Result<()> {
    let raw_dir = data_dir.join("raw").join(wiki);
    for entry in read_dir_if_present(&raw_dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let filename = entry.file_name().to_string_lossy().into_owned();
        let Some(snapshot_version) = ingest::snapshot_version_from_filename(&filename, wiki) else {
            continue;
        };
        if !SnapshotPlan::load_or_resolve(data_dir, wiki, snapshot_version)?
            .0
            .filenames()?
            .iter()
            .any(|expected| expected == &filename)
        {
            continue;
        }
        let source = entry.path();
        let source_id = ingest::ingest_source_id(&source)?;
        let analytical_root =
            storage::snapshot_analytical_wiki_dir(data_dir, wiki, snapshot_version)?;
        if !storage::marker_manifest_covers_source_in(
            data_dir,
            &analytical_root,
            &source_id,
            &source,
        ) {
            continue;
        }
        remove_file(source, &mut report.removed)?;
        report.validated_raw_dumps += 1;
    }
    Ok(())
}

fn clean_capacity_staging(
    capacity_root: &Path,
    current_run_id: Option<&str>,
    minimum_age: Duration,
    now: SystemTime,
    report: &mut CleanupReport,
) -> Result<()> {
    for kind in ["output", "scratch"] {
        for entry in read_dir_if_present(&capacity_root.join(kind))? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let name = entry.file_name().to_string_lossy().into_owned();
            let Some(owner) = name.strip_prefix("capacity-") else {
                continue;
            };
            if owner.is_empty()
                || !owner.bytes().all(is_safe_id_byte)
                || current_run_id == Some(name.as_str())
                || !is_expired(&entry.path(), minimum_age, now)?
            {
                continue;
            }
            remove_dir(entry.path(), &mut report.removed)?;
            report.capacity_staging_directories += 1;
        }
    }
    Ok(())
}

fn clean_weekly_scratch(
    scratch_root: &Path,
    current_run_id: Option<&str>,
    minimum_age: Duration,
    now: SystemTime,
    report: &mut CleanupReport,
) -> Result<()> {
    for wiki_entry in read_dir_if_present(scratch_root)? {
        let wiki_entry = wiki_entry?;
        if !wiki_entry.file_type()?.is_dir() {
            continue;
        }
        for entry in read_dir_if_present(&wiki_entry.path())? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let name = entry.file_name().to_string_lossy().into_owned();
            let Some(owner) = name.strip_prefix(".page_weekly_edits-runs-") else {
                continue;
            };
            if owner.is_empty()
                || !owner.bytes().all(is_safe_id_byte)
                || current_run_id.is_some_and(|run_id| owner.starts_with(&format!("{run_id}-")))
                || !is_expired(&entry.path(), minimum_age, now)?
            {
                continue;
            }
            remove_dir(entry.path(), &mut report.removed)?;
            report.weekly_scratch_directories += 1;
        }
    }
    Ok(())
}

fn clean_merge_temporaries(
    output_dir: &Path,
    current_run_id: Option<&str>,
    minimum_age: Duration,
    now: SystemTime,
    report: &mut CleanupReport,
) -> Result<()> {
    for entry in read_dir_if_present(output_dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().into_owned();
        if !is_owned_merge_temporary(&name)
            || current_run_id.is_some_and(|run_id| name.contains(&format!(".merge.{run_id}.tmp")))
            || !is_expired(&entry.path(), minimum_age, now)?
        {
            continue;
        }
        remove_file(entry.path(), &mut report.removed)?;
        report.merge_temporaries += 1;
    }
    Ok(())
}

fn is_owned_merge_temporary(name: &str) -> bool {
    let Some(body) = name.strip_prefix('.') else {
        return false;
    };
    let Some((metric, suffix)) = body.split_once(".merge.") else {
        return false;
    };
    metric.ends_with(".parquet")
        && suffix
            .strip_suffix(".tmp")
            .is_some_and(|run| !run.is_empty() && run.bytes().all(is_safe_id_byte))
        || metric.ends_with(".parquet") && suffix == "tmp"
}

fn clean_site_builds(
    dist_dir: &Path,
    current_run_id: Option<&str>,
    minimum_age: Duration,
    now: SystemTime,
    report: &mut CleanupReport,
) -> Result<()> {
    let parent = dist_dir
        .parent()
        .context("site dist directory has no parent")?;
    let dist_name = dist_dir
        .file_name()
        .and_then(|name| name.to_str())
        .context("site dist directory has no valid filename")?;
    let prefix = format!(".{dist_name}.build.");
    let live_target = fs::read_link(dist_dir).ok().map(|target| {
        if target.is_absolute() {
            target
        } else {
            parent.join(target)
        }
    });
    for entry in read_dir_if_present(parent)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().into_owned();
        let candidate = entry.path();
        if !name.starts_with(&prefix)
            || !name[prefix.len()..].bytes().all(is_safe_id_byte)
            || live_target
                .as_ref()
                .is_some_and(|target| target == &candidate)
            || current_run_id.is_some_and(|run_id| name.starts_with(&format!("{prefix}{run_id}.")))
            || !is_expired(&candidate, minimum_age, now)?
        {
            continue;
        }
        remove_dir(candidate, &mut report.removed)?;
        report.site_builds += 1;
    }
    Ok(())
}

fn clean_run_staging(
    output_dir: &Path,
    current_run_id: Option<&str>,
    minimum_age: Duration,
    now: SystemTime,
    report: &mut CleanupReport,
) -> Result<()> {
    for entry in read_dir_if_present(output_dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().into_owned();
        let owned = [".refresh-staging.", ".publication-staging."]
            .iter()
            .find_map(|prefix| name.strip_prefix(prefix));
        if owned.is_none_or(|run_id| {
            run_id.is_empty()
                || !run_id.bytes().all(is_safe_id_byte)
                || current_run_id == Some(run_id)
        }) || !is_expired(&entry.path(), minimum_age, now)?
        {
            continue;
        }
        remove_dir(entry.path(), &mut report.removed)?;
        report.run_staging_directories += 1;
    }
    Ok(())
}

fn is_safe_id_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.')
}

fn is_expired(path: &Path, minimum_age: Duration, now: SystemTime) -> Result<bool> {
    let modified = fs::metadata(path)?.modified()?;
    Ok(now.duration_since(modified).unwrap_or_default() >= minimum_age)
}

fn read_dir_if_present(path: &Path) -> Result<Vec<std::io::Result<fs::DirEntry>>> {
    if !path.is_dir() {
        return Ok(Vec::new());
    }
    Ok(fs::read_dir(path)?.collect())
}

fn remove_file(path: PathBuf, removed: &mut Vec<String>) -> Result<()> {
    fs::remove_file(&path)?;
    removed.push(path.to_string_lossy().into_owned());
    Ok(())
}

fn remove_dir(path: PathBuf, removed: &mut Vec<String>) -> Result<()> {
    fs::remove_dir_all(&path)?;
    removed.push(path.to_string_lossy().into_owned());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::TestDir;
    use polars::prelude::{Column, DataFrame, ParquetWriter};
    use std::fs::File;

    fn write_ingested_raw_fixture(
        data_dir: &Path,
        wiki: &str,
        snapshot: &str,
        filename: &str,
        source_parent: &Path,
    ) -> Result<PathBuf> {
        let source = source_parent.join(filename);
        source.parent().map(fs::create_dir_all).transpose()?;
        fs::write(&source, b"validated raw source")?;
        let source_id = ingest::ingest_source_id(&source)?;
        let analytical_root = storage::snapshot_analytical_wiki_dir(data_dir, wiki, snapshot)?;
        let warehouse_root = storage::snapshot_warehouse_wiki_dir(data_dir, wiki, snapshot)?;
        let mut paths = Vec::new();
        for root in [&analytical_root, &warehouse_root] {
            let output = root
                .join("year=2026/year_month=2026-01")
                .join(format!("{source_id}.part-00000.parquet"));
            output.parent().map(fs::create_dir_all).transpose()?;
            let mut frame = DataFrame::new_infer_height(vec![Column::new("row".into(), [1_i64])])?;
            ParquetWriter::new(File::create(&output)?).finish(&mut frame)?;
            paths.push(output);
        }
        let (source_size_bytes, source_sha256) = storage::sha256_file(&source)?;
        storage::write_marker_manifest_in(
            data_dir,
            &analytical_root,
            &source_id,
            &storage::MarkerManifest {
                snapshot_version: Some(snapshot.to_string()),
                source: source.clone(),
                source_size_bytes,
                source_sha256,
                rows: 1,
                allow_empty: false,
                analytical_paths: vec![paths[0].clone()],
                warehouse_paths: vec![paths[1].clone()],
            },
        )
        .expect("strict fixture marker should be writable");
        Ok(source)
    }

    #[test]
    fn killed_merge_and_site_staging_are_removed_but_live_and_current_are_preserved() -> Result<()>
    {
        let root = TestDir::new()?;
        let data = root.path().join("data");
        let output = root.path().join("output");
        let site_parent = root.path().join("site");
        let dist = site_parent.join("dist");
        fs::create_dir_all(&output)?;
        fs::create_dir_all(&site_parent)?;
        let stale_merge = output.join(".metric.parquet.merge.dead-run.tmp");
        let unrelated = output.join(".notes.merge.dead-run.tmp");
        fs::write(&stale_merge, b"partial")?;
        fs::write(&unrelated, b"keep")?;
        let stale_site = site_parent.join(".dist.build.dead-run.abc123");
        let live_site = site_parent.join(".dist.build.live-run.abc123");
        let current_site = site_parent.join(".dist.build.current-run.abc123");
        for directory in [&stale_site, &live_site, &current_site] {
            fs::create_dir(directory)?;
        }
        #[cfg(unix)]
        std::os::unix::fs::symlink(live_site.file_name().context("live name")?, &dist)?;
        let run_stage = output.join(".refresh-staging.dead-run");
        fs::create_dir(&run_stage)?;

        let report = clean_abandoned(
            &data,
            &output,
            &dist,
            CleanupStagingRoots::default(),
            &[],
            Some("current-run"),
            Duration::ZERO,
        )
        .expect("abandoned staging cleanup should succeed");

        assert_eq!(report.merge_temporaries, 1);
        assert_eq!(report.site_builds, 1);
        assert_eq!(report.run_staging_directories, 1);
        assert!(!stale_merge.exists());
        assert!(!stale_site.exists());
        assert!(!run_stage.exists());
        assert!(unrelated.exists());
        assert!(live_site.exists());
        assert!(current_site.exists());
        Ok(())
    }

    #[test]
    fn stale_non_current_snapshot_is_removed_only_with_an_active_pointer() -> Result<()> {
        let root = TestDir::new()?;
        let data = root.path().join("data");
        let output = root.path().join("output");
        let dist = root.path().join("site/dist");
        fs::create_dir_all(&output)?;
        fs::create_dir_all(dist.parent().context("dist parent")?)?;
        for version in ["2026-07", "2026-08"] {
            let analytical = storage::snapshot_analytical_wiki_dir(&data, "nlwiki", version)
                .expect("fixture snapshot version should be valid");
            let warehouse = storage::snapshot_warehouse_wiki_dir(&data, "nlwiki", version)
                .expect("fixture snapshot version should be valid");
            fs::create_dir_all(analytical)?;
            fs::create_dir_all(warehouse)?;
        }
        storage::publish_test_snapshot_pointer(&data, "nlwiki", "2026-08")?;
        let inactive_state = data.join("snapshots/nlwiki/2026-07");
        fs::create_dir_all(&inactive_state)?;
        fs::write(inactive_state.join("generation-manifest.json"), b"stale")?;

        let report = clean_abandoned(
            &data,
            &output,
            &dist,
            CleanupStagingRoots::default(),
            &["nlwiki".to_string()],
            Some("current-run"),
            Duration::ZERO,
        )
        .expect("inactive generation cleanup should succeed");

        assert_eq!(report.snapshot_generations, 3);
        assert!(!storage::snapshot_analytical_wiki_dir(&data, "nlwiki", "2026-07")?.exists());
        assert!(!storage::snapshot_warehouse_wiki_dir(&data, "nlwiki", "2026-07")?.exists());
        assert!(storage::snapshot_analytical_wiki_dir(&data, "nlwiki", "2026-08")?.exists());
        assert!(storage::snapshot_warehouse_wiki_dir(&data, "nlwiki", "2026-08")?.exists());
        assert!(!inactive_state.exists());
        Ok(())
    }

    #[test]
    fn cleanup_ignores_unowned_current_and_recent_artifacts() -> Result<()> {
        let root = TestDir::new()?;
        let output = root.path().join("output");
        let site_parent = root.path().join("site");
        let dist = site_parent.join("dist");
        fs::create_dir_all(&output)?;
        fs::create_dir_all(&site_parent)?;
        for name in [
            "metric.parquet.merge.no-leading-dot.tmp",
            ".metric.parquet.no-merge.tmp",
            ".metric.parquet.merge.bad$id.tmp",
            ".metric.parquet.merge.current-run.tmp",
            ".metric.parquet.merge.tmp",
        ] {
            fs::write(output.join(name), b"keep")?;
        }
        for name in [
            ".refresh-staging.",
            ".refresh-staging.bad$id",
            ".refresh-staging.current-run",
            ".unowned-staging.dead-run",
        ] {
            fs::create_dir(output.join(name))?;
        }
        fs::create_dir(site_parent.join(".dist.build.bad$id"))?;
        let absolute_live = site_parent.join(".dist.build.live.absolute");
        fs::create_dir(&absolute_live)?;
        #[cfg(unix)]
        std::os::unix::fs::symlink(&absolute_live, &dist)?;

        let report = clean_abandoned(
            &root.path().join("missing-data"),
            &output,
            &dist,
            CleanupStagingRoots::default(),
            &[],
            Some("current-run"),
            Duration::from_secs(86_400),
        )
        .expect("recent artifact cleanup should succeed");

        assert!(report.removed.is_empty());
        assert!(read_dir_if_present(&root.path().join("missing"))?.is_empty());
        assert!(!is_owned_merge_temporary("no-leading-dot"));
        assert!(!is_owned_merge_temporary(".no-merge"));
        assert!(is_owned_merge_temporary(".metric.parquet.merge.tmp"));
        Ok(())
    }

    #[test]
    fn inactive_snapshot_cleanup_fails_closed_without_a_pointer() -> Result<()> {
        let root = TestDir::new()?;
        let data = root.path().join("data");
        let generation = storage::snapshot_analytical_wiki_dir(&data, "nlwiki", "2026-07")?;
        fs::create_dir_all(generation)?;
        let mut removed = Vec::new();

        assert_eq!(
            storage::clean_stale_inactive_snapshots(
                &data,
                "nlwiki",
                Duration::ZERO,
                SystemTime::now(),
                &mut removed,
            )
            .expect("cleanup without a pointer should safely no-op"),
            0
        );
        assert!(removed.is_empty());
        Ok(())
    }

    #[test]
    fn snapshot_cleanup_handles_missing_layers_invalid_entries_and_recent_data() -> Result<()> {
        let root = TestDir::new()?;
        let data = root.path().join("data");
        for version in ["2026-07", "2026-08"] {
            let analytical = storage::snapshot_analytical_wiki_dir(&data, "nlwiki", version)
                .expect("fixture snapshot version should be valid");
            let warehouse = storage::snapshot_warehouse_wiki_dir(&data, "nlwiki", version)
                .expect("fixture snapshot version should be valid");
            fs::create_dir_all(analytical)?;
            fs::create_dir_all(warehouse)?;
        }
        storage::publish_test_snapshot_pointer(&data, "nlwiki", "2026-08")?;
        let analytical_snapshots = storage::analytical_wiki_dir(&data, "nlwiki").join("_snapshots");
        fs::create_dir(analytical_snapshots.join("invalid"))?;
        fs::write(analytical_snapshots.join("README"), b"ignored")?;
        fs::remove_dir_all(storage::warehouse_wiki_dir(&data, "nlwiki").join("_snapshots"))?;
        let mut removed = Vec::new();

        let count = storage::clean_stale_inactive_snapshots(
            &data,
            "nlwiki",
            Duration::from_secs(86_400),
            SystemTime::now(),
            &mut removed,
        )
        .expect("recent snapshot cleanup should succeed");

        assert_eq!(count, 0);
        assert!(removed.is_empty());
        assert!(analytical_snapshots.join("2026-07").exists());
        Ok(())
    }

    #[test]
    fn cleanup_removes_only_expired_non_current_weekly_scratch() -> Result<()> {
        let root = TestDir::new()?;
        let data = root.path().join("data");
        let output = root.path().join("output");
        let dist = root.path().join("site/dist");
        let scratch = root.path().join("scratch/frwiki");
        fs::create_dir_all(&output)?;
        fs::create_dir_all(dist.parent().context("site parent")?)?;
        for name in [
            ".page_weekly_edits-runs-dead-run-10-1",
            ".page_weekly_edits-runs-current-run-11-2",
            ".unowned-weekly-scratch",
        ] {
            fs::create_dir_all(scratch.join(name))?;
        }
        fs::write(root.path().join("scratch/not-a-wiki"), b"owned file")?;
        fs::write(scratch.join("not-a-run-directory"), b"owned file")?;

        let report = clean_abandoned(
            &data,
            &output,
            &dist,
            CleanupStagingRoots {
                weekly: Some(root.path().join("scratch").as_path()),
                capacity: None,
            },
            &[],
            Some("current-run"),
            Duration::ZERO,
        )
        .expect("owned expired scratch should clean safely");

        assert_eq!(report.weekly_scratch_directories, 1);
        assert!(
            !scratch
                .join(".page_weekly_edits-runs-dead-run-10-1")
                .exists()
        );
        assert!(
            scratch
                .join(".page_weekly_edits-runs-current-run-11-2")
                .exists()
        );
        assert!(scratch.join(".unowned-weekly-scratch").exists());
        Ok(())
    }

    #[test]
    fn cleanup_propagates_an_invalid_snapshot_pointer() -> Result<()> {
        let root = TestDir::new()?;
        let data = root.path().join("data");
        let output = root.path().join("output");
        let dist = root.path().join("site/dist");
        fs::create_dir_all(&output)?;
        fs::create_dir_all(dist.parent().context("site parent")?)?;
        let pointer = storage::snapshot_pointer_path(&data, "nlwiki");
        pointer.parent().map(fs::create_dir_all).transpose()?;
        fs::write(pointer, b"truncated")?;

        assert!(
            clean_abandoned(
                &data,
                &output,
                &dist,
                CleanupStagingRoots::default(),
                &["nlwiki".to_string()],
                Some("current-run"),
                Duration::ZERO,
            )
            .is_err()
        );
        Ok(())
    }

    #[test]
    fn cleanup_removes_only_raw_dumps_covered_by_strict_exact_markers() -> Result<()> {
        let root = TestDir::new()?;
        let data = root.path().join("data");
        let output = root.path().join("output");
        let dist = root.path().join("site/dist");
        let raw = data.join("raw/nlwiki");
        fs::create_dir_all(&output)?;
        fs::create_dir_all(dist.parent().context("site parent")?)?;
        fs::create_dir_all(&raw)?;

        let valid = write_ingested_raw_fixture(
            &data,
            "nlwiki",
            "2026-07",
            "2026-07.nlwiki.2001.tsv.bz2",
            &raw,
        )
        .expect("valid raw fixture");
        let recorded_elsewhere = write_ingested_raw_fixture(
            &data,
            "nlwiki",
            "2026-07",
            "2026-07.nlwiki.2002.tsv.bz2",
            &data.join("archive"),
        )
        .expect("path-mismatch fixture");
        let path_mismatch = raw.join("2026-07.nlwiki.2002.tsv.bz2");
        fs::copy(&recorded_elsewhere, &path_mismatch)?;
        let missing_marker = raw.join("2026-07.nlwiki.2003.tsv.bz2");
        fs::write(&missing_marker, b"unvalidated")?;
        let malformed_marker_source = raw.join("2026-07.nlwiki.2004.tsv.bz2");
        fs::write(&malformed_marker_source, b"unvalidated")?;
        let malformed_marker = storage::marker_path_in(
            &storage::snapshot_analytical_wiki_dir(&data, "nlwiki", "2026-07")?,
            "2026-07.nlwiki.2004",
        );
        malformed_marker
            .parent()
            .map(fs::create_dir_all)
            .transpose()?;
        fs::write(malformed_marker, b"{")?;
        let unrelated = raw.join("notes.tsv.bz2");
        fs::write(&unrelated, b"keep")?;
        let unexpected = raw.join("2026-07.nlwiki.all-time.tsv.bz2");
        fs::write(&unexpected, b"keep")?;
        let unsafe_recorded = write_ingested_raw_fixture(
            &data,
            "nlwiki",
            "2026-07",
            "2026-07.nlwiki.2005.tsv.bz2",
            &data.join("unsafe-archive"),
        )
        .expect("unsafe-marker fixture");
        let unsafe_candidate = raw.join("2026-07.nlwiki.2005.tsv.bz2");
        fs::copy(&unsafe_recorded, &unsafe_candidate)?;
        let unsafe_marker = storage::marker_path_in(
            &storage::snapshot_analytical_wiki_dir(&data, "nlwiki", "2026-07")?,
            "2026-07.nlwiki.2005",
        );
        let mut unsafe_json: serde_json::Value =
            serde_json::from_slice(&fs::read(&unsafe_marker)?)?;
        unsafe_json["source"]["path"] = serde_json::json!("../unsafe.tsv.bz2");
        fs::write(&unsafe_marker, serde_json::to_vec(&unsafe_json)?)?;
        fs::create_dir(raw.join("2026-07.nlwiki.2006.tsv.bz2"))?;
        let report = clean_abandoned(
            &data,
            &output,
            &dist,
            CleanupStagingRoots::default(),
            &["nlwiki".to_string()],
            Some("current-run"),
            Duration::ZERO,
        )
        .expect("raw recovery cleanup should succeed");

        assert_eq!(report.validated_raw_dumps, 1);
        assert!(!valid.exists());
        assert!(path_mismatch.exists());
        assert!(missing_marker.exists());
        assert!(malformed_marker_source.exists());
        assert!(unrelated.exists());
        assert!(unexpected.exists());
        assert!(recorded_elsewhere.exists());
        assert!(unsafe_candidate.exists());
        assert!(unsafe_recorded.exists());
        Ok(())
    }

    #[test]
    fn cleanup_reaps_only_owned_expired_capacity_staging() -> Result<()> {
        let root = TestDir::new()?;
        let output = root.path().join("output");
        let dist = root.path().join("site/dist");
        let capacity = root.path().join("capacity");
        fs::create_dir_all(&output)?;
        fs::create_dir_all(dist.parent().context("site parent")?)?;
        for kind in ["output", "scratch"] {
            for name in [
                "capacity-dead",
                "capacity-current",
                "unowned",
                "capacity-bad$id",
            ] {
                fs::create_dir_all(capacity.join(kind).join(name))?;
            }
            fs::write(capacity.join(kind).join("capacity-file"), b"keep")?;
        }

        let report = clean_abandoned(
            &root.path().join("data"),
            &output,
            &dist,
            CleanupStagingRoots {
                weekly: None,
                capacity: Some(&capacity),
            },
            &[],
            Some("capacity-current"),
            Duration::ZERO,
        )
        .expect("capacity staging cleanup should succeed");

        assert_eq!(report.capacity_staging_directories, 2);
        for kind in ["output", "scratch"] {
            assert!(!capacity.join(kind).join("capacity-dead").exists());
            assert!(capacity.join(kind).join("capacity-current").exists());
            assert!(capacity.join(kind).join("unowned").exists());
            assert!(capacity.join(kind).join("capacity-bad$id").exists());
            assert!(capacity.join(kind).join("capacity-file").exists());
        }
        Ok(())
    }
}
