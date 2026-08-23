use anyhow::{Context, Result};
use serde::Serialize;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use crate::storage;

#[derive(Debug, Default, Serialize)]
pub struct CleanupReport {
    pub merge_temporaries: usize,
    pub site_builds: usize,
    pub run_staging_directories: usize,
    pub snapshot_generations: usize,
    pub removed: Vec<String>,
}

pub fn clean_abandoned(
    data_dir: &Path,
    output_dir: &Path,
    site_dist_dir: &Path,
    wikis: &[String],
    current_run_id: Option<&str>,
    minimum_age: Duration,
) -> Result<CleanupReport> {
    let now = SystemTime::now();
    let mut report = CleanupReport::default();
    clean_merge_temporaries(output_dir, current_run_id, minimum_age, now, &mut report)?;
    clean_site_builds(site_dist_dir, current_run_id, minimum_age, now, &mut report)?;
    clean_run_staging(output_dir, current_run_id, minimum_age, now, &mut report)?;
    for wiki in wikis {
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
            &[],
            Some("current-run"),
            Duration::ZERO,
        )?;

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
            fs::create_dir_all(storage::snapshot_analytical_wiki_dir(
                &data, "nlwiki", version,
            )?)?;
            fs::create_dir_all(storage::snapshot_warehouse_wiki_dir(
                &data, "nlwiki", version,
            )?)?;
        }
        storage::publish_current_snapshot(&data, "nlwiki", "2026-08")?;

        let report = clean_abandoned(
            &data,
            &output,
            &dist,
            &["nlwiki".to_string()],
            Some("current-run"),
            Duration::ZERO,
        )?;

        assert_eq!(report.snapshot_generations, 2);
        assert!(!storage::snapshot_analytical_wiki_dir(&data, "nlwiki", "2026-07")?.exists());
        assert!(!storage::snapshot_warehouse_wiki_dir(&data, "nlwiki", "2026-07")?.exists());
        assert!(storage::snapshot_analytical_wiki_dir(&data, "nlwiki", "2026-08")?.exists());
        assert!(storage::snapshot_warehouse_wiki_dir(&data, "nlwiki", "2026-08")?.exists());
        Ok(())
    }
}
