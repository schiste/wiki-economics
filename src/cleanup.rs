use anyhow::{Context, Result, ensure};
use serde::Serialize;
use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use crate::generation_lifecycle::GenerationState as GState;
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
    pub candidate_generations: usize,
    pub quarantined_artifacts: usize,
    pub removed: Vec<String>,
    pub quarantined: Vec<String>,
}

#[derive(Clone, Copy)]
enum QuarantineKind {
    RootEntry,
    Snapshot,
    RunEntry,
    RunId,
    Merge,
    Site,
    Staging,
}

impl QuarantineKind {
    fn reason(self) -> &'static str {
        match self {
            Self::RootEntry => "unexpected non-directory in candidate wiki root",
            Self::Snapshot => "invalid snapshot directory in candidate root",
            Self::RunEntry => "unexpected non-directory in candidate snapshot",
            Self::RunId => "unsafe candidate run identifier",
            Self::Merge => "malformed pipeline merge temporary",
            Self::Site => "malformed pipeline site staging directory",
            Self::Staging => "malformed pipeline run staging directory",
        }
    }
}

impl CleanupReport {
    fn q(
        &mut self,
        root: &Path,
        path: PathBuf,
        kind: QuarantineKind,
        now: SystemTime,
    ) -> Result<()> {
        quarantine(root, path, kind.reason(), now, self)
    }
}

#[derive(Clone, Copy)]
struct CandidateGeneration<'a> {
    output_dir: &'a Path,
    wiki: &'a str,
    snapshot: &'a str,
    run_id: &'a str,
}

impl<'a> CandidateGeneration<'a> {
    fn load(self) -> Result<Option<crate::generation_lifecycle::GenerationRecord>> {
        crate::generation_lifecycle::load(self.output_dir, self.wiki, self.snapshot, self.run_id)
    }

    fn adopt(
        self,
        state: GState,
        reason: &str,
    ) -> Result<crate::generation_lifecycle::GenerationRecord> {
        crate::generation_lifecycle::adopt(
            self.output_dir,
            self.wiki,
            self.snapshot,
            self.run_id,
            state,
            reason,
        )
    }

    fn retire(self, reason: &str) -> Result<crate::generation_lifecycle::GenerationRecord> {
        crate::generation_lifecycle::transition(
            self.output_dir,
            self.wiki,
            self.snapshot,
            self.run_id,
            GState::Retired,
            reason,
            None,
        )
    }
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
        let protected_result = clean_candidate_generations(
            output_dir,
            wiki,
            current_run_id,
            minimum_age,
            now,
            &mut report,
        );
        let protected = protected_result?;
        if output_dir
            .join("_prepare-locks")
            .join(format!("{wiki}.lock"))
            .is_dir()
        {
            continue;
        }
        report.snapshot_generations += storage::clean_stale_inactive_snapshots(
            data_dir,
            wiki,
            &protected,
            minimum_age,
            now,
            &mut report.removed,
        )?;
    }
    report.removed.sort();
    report.quarantined.sort();
    Ok(report)
}

fn clean_candidate_generations(
    output_dir: &Path,
    wiki: &str,
    current_run_id: Option<&str>,
    minimum_age: Duration,
    now: SystemTime,
    report: &mut CleanupReport,
) -> Result<BTreeSet<String>> {
    let root = output_dir.join("_candidates").join(wiki);
    let live_target = fs::read_link(output_dir.join(wiki)).ok().map(|target| {
        if target.is_absolute() {
            target
        } else {
            output_dir.join(target)
        }
    });
    let mut protected = BTreeSet::new();
    let mut resumable = Vec::new();
    for snapshot_entry in read_dir_if_present(&root)? {
        let snapshot_entry = snapshot_entry?;
        if !snapshot_entry.file_type()?.is_dir() {
            if is_expired(&snapshot_entry.path(), minimum_age, now)? {
                let path = snapshot_entry.path();
                report.q(output_dir, path, QuarantineKind::RootEntry, now)?;
            }
            continue;
        }
        let snapshot = snapshot_entry.file_name().to_string_lossy().into_owned();
        if storage::validate_snapshot_version(&snapshot).is_err() {
            if is_expired(&snapshot_entry.path(), minimum_age, now)? {
                let path = snapshot_entry.path();
                report.q(output_dir, path, QuarantineKind::Snapshot, now)?;
            }
            continue;
        }
        for run_entry in read_dir_if_present(&snapshot_entry.path())? {
            let run_entry = run_entry?;
            if !run_entry.file_type()?.is_dir() {
                if is_expired(&run_entry.path(), minimum_age, now)? {
                    report.q(output_dir, run_entry.path(), QuarantineKind::RunEntry, now)?;
                }
                continue;
            }
            let run_id = run_entry.file_name().to_string_lossy().into_owned();
            let candidate = run_entry.path();
            let candidate_wiki = candidate.join(wiki);
            if !run_id.bytes().all(is_safe_id_byte) {
                if is_expired(&candidate, minimum_age, now)? {
                    report.q(output_dir, candidate, QuarantineKind::RunId, now)?;
                }
                continue;
            }
            let live = live_target.as_ref() == Some(&candidate_wiki);
            let generation = CandidateGeneration {
                output_dir,
                wiki,
                snapshot: &snapshot,
                run_id: &run_id,
            };
            let state = match generation.load()? {
                Some(record) => record,
                None if live => {
                    let reason = "adopted legacy live generation";
                    generation.adopt(GState::Published, reason)?
                }
                None if candidate.join("ready.json").is_file() => {
                    let reason = "adopted legacy ready generation";
                    generation.adopt(GState::Ready, reason)?
                }
                None => {
                    let reason = "adopted resumable legacy generation";
                    generation.adopt(GState::Building, reason)?
                }
            };
            let expired = state_is_expired(state.updated_at_unix, minimum_age, now)?;
            let retained = live
                || current_run_id == Some(run_id.as_str())
                || matches!(
                    state.state,
                    GState::Ready | GState::Published | GState::Superseded
                )
                || (!expired && matches!(state.state, GState::Building | GState::Validated));
            if retained {
                protected.insert(snapshot.clone());
                if matches!(state.state, GState::Building | GState::Validated) {
                    resumable.push((
                        current_run_id == Some(run_id.as_str()),
                        state.updated_at_unix,
                        snapshot.clone(),
                        run_id.clone(),
                        candidate,
                    ));
                }
            } else {
                if state.state != GState::Retired {
                    generation.retire("resumable candidate exceeded its recovery window")?;
                }
                remove_dir(candidate, &mut report.removed)?;
                report.candidate_generations += 1;
            }
        }
        if fs::read_dir(snapshot_entry.path())?.next().is_none() {
            fs::remove_dir(snapshot_entry.path())?;
        }
    }
    resumable.sort_by(|left, right| {
        (left.0, left.1, &left.2, &left.3).cmp(&(right.0, right.1, &right.2, &right.3))
    });
    if let Some(keep) = resumable.pop() {
        for (_, _, snapshot, run_id, candidate) in resumable {
            let generation = CandidateGeneration {
                output_dir,
                wiki,
                snapshot: &snapshot,
                run_id: &run_id,
            };
            generation.retire("newer resumable candidate retained")?;
            remove_dir(candidate, &mut report.removed)?;
            report.candidate_generations += 1;
            let snapshot_root = root.join(&snapshot);
            if snapshot_root.is_dir() && fs::read_dir(&snapshot_root)?.next().is_none() {
                fs::remove_dir(snapshot_root)?;
            }
        }
        protected.insert(keep.2);
    }
    protected.retain(|snapshot| {
        read_dir_if_present(&root.join(snapshot)).is_ok_and(|entries| {
            entries
                .into_iter()
                .any(|entry| entry.is_ok_and(|entry| entry.path().is_dir()))
        })
    });
    Ok(protected)
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
        let expired = is_expired(&entry.path(), minimum_age, now)?;
        if !is_owned_merge_temporary(&name) {
            if looks_like_merge_temporary(&name) && expired {
                report.q(output_dir, entry.path(), QuarantineKind::Merge, now)?;
            }
            continue;
        }
        if current_run_id.is_some_and(|run_id| name.contains(&format!(".merge.{run_id}.tmp")))
            || !expired
        {
            continue;
        }
        remove_file(entry.path(), &mut report.removed)?;
        report.merge_temporaries += 1;
    }
    Ok(())
}

fn looks_like_merge_temporary(name: &str) -> bool {
    name.starts_with('.')
        && name.split_once(".merge.").is_some_and(|(metric, suffix)| {
            metric.ends_with(".parquet") && suffix.ends_with(".tmp")
        })
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
    clean_site_build_family(
        parent,
        dist_dir,
        dist_name,
        current_run_id,
        minimum_age,
        now,
        report,
    )?;
    let defaults_name = format!("{dist_name}-defaults");
    clean_site_build_family(
        parent,
        &parent.join(&defaults_name),
        &defaults_name,
        current_run_id,
        minimum_age,
        now,
        report,
    )
}

#[allow(clippy::too_many_arguments)]
fn clean_site_build_family(
    parent: &Path,
    live_link: &Path,
    family_name: &str,
    current_run_id: Option<&str>,
    minimum_age: Duration,
    now: SystemTime,
    report: &mut CleanupReport,
) -> Result<()> {
    let prefix = format!(".{family_name}.build.");
    let live_target = fs::read_link(live_link).ok().map(|target| {
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
        if !name.starts_with(&prefix) {
            continue;
        }
        let expired = is_expired(&candidate, minimum_age, now)?;
        if !name[prefix.len()..].bytes().all(is_safe_id_byte) {
            if expired {
                report.q(parent, candidate, QuarantineKind::Site, now)?;
            }
            continue;
        }
        if live_target
            .as_ref()
            .is_some_and(|target| target == &candidate)
            || current_run_id.is_some_and(|run_id| name.starts_with(&format!("{prefix}{run_id}.")))
            || !expired
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
        let Some(owner) = owned else {
            continue;
        };
        let expired = is_expired(&entry.path(), minimum_age, now)?;
        if owner.is_empty() || !owner.bytes().all(is_safe_id_byte) {
            if expired {
                report.q(output_dir, entry.path(), QuarantineKind::Staging, now)?;
            }
            continue;
        }
        if current_run_id == Some(owner) || !expired {
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

fn state_is_expired(updated_at_unix: u64, minimum_age: Duration, now: SystemTime) -> Result<bool> {
    let updated_at = SystemTime::UNIX_EPOCH
        .checked_add(Duration::from_secs(updated_at_unix))
        .context("generation state timestamp overflow")?;
    Ok(now.duration_since(updated_at).unwrap_or_default() >= minimum_age)
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

#[derive(Serialize)]
struct QuarantineReceipt {
    schema_version: u8,
    quarantined_at_unix: u64,
    original_relative: String,
    reason: String,
}

fn quarantine(
    output_dir: &Path,
    path: PathBuf,
    reason: &str,
    now: SystemTime,
    report: &mut CleanupReport,
) -> Result<()> {
    let relative = path
        .strip_prefix(output_dir)
        .context("quarantine source is outside the output directory")?;
    let timestamp = now.duration_since(SystemTime::UNIX_EPOCH)?.as_secs();
    let destination = output_dir
        .join("_quarantine")
        .join(timestamp.to_string())
        .join(relative);
    let parent = destination
        .parent()
        .context("quarantine destination has no parent")?;
    fs::create_dir_all(parent)?;
    ensure!(
        !destination.exists(),
        "quarantine destination already exists"
    );
    fs::rename(&path, &destination)?;
    let receipt_path = destination.with_extension(format!(
        "{}.quarantine.json",
        destination
            .extension()
            .map_or_else(|| "artifact".into(), |value| value.to_string_lossy())
    ));
    let receipt_temporary = receipt_path.with_extension(format!(
        "{}.{}.tmp",
        receipt_path
            .extension()
            .context("quarantine receipt has no extension")?
            .to_string_lossy(),
        std::process::id()
    ));
    let receipt_result = (|| -> Result<()> {
        let mut receipt = fs::File::create(&receipt_temporary)?;
        let value = QuarantineReceipt {
            schema_version: 1,
            quarantined_at_unix: timestamp,
            original_relative: relative.to_string_lossy().into_owned(),
            reason: reason.to_string(),
        };
        serde_json::to_writer_pretty(&mut receipt, &value)?;
        receipt.sync_all()?;
        fs::rename(&receipt_temporary, &receipt_path)?;
        Ok(())
    })();
    if receipt_result.is_err() {
        let _ = fs::remove_file(&receipt_temporary);
    }
    receipt_result?;
    fs::File::open(parent)?.sync_all()?;
    report.quarantined_artifacts += 1;
    report
        .quarantined
        .push(destination.to_string_lossy().into_owned());
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
                metric_input_paths: Vec::new(),
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
        let malformed_merge = output.join(".metric.parquet.merge.bad$id.tmp");
        let unrelated = output.join(".notes.merge.dead-run.tmp");
        fs::write(&stale_merge, b"partial")?;
        fs::write(&malformed_merge, b"partial")?;
        fs::write(&unrelated, b"keep")?;
        let stale_site = site_parent.join(".dist.build.dead-run.abc123");
        let malformed_site = site_parent.join(".dist.build.bad$id");
        let unrelated_site = site_parent.join("unrelated-site-directory");
        let live_site = site_parent.join(".dist.build.live-run.abc123");
        let current_site = site_parent.join(".dist.build.current-run.abc123");
        let stale_defaults = site_parent.join(".dist-defaults.build.dead-run.abc123");
        let live_defaults = site_parent.join(".dist-defaults.build.live-run.abc123");
        for directory in [
            &stale_site,
            &malformed_site,
            &unrelated_site,
            &live_site,
            &current_site,
            &stale_defaults,
            &live_defaults,
        ] {
            fs::create_dir(directory)?;
        }
        #[cfg(unix)]
        std::os::unix::fs::symlink(live_site.file_name().context("live name")?, &dist)?;
        #[cfg(unix)]
        std::os::unix::fs::symlink(
            live_defaults.file_name().context("live defaults name")?,
            site_parent.join("dist-defaults"),
        )?;
        let run_stage = output.join(".refresh-staging.dead-run");
        let malformed_run_stage = output.join(".refresh-staging.bad$id");
        fs::create_dir(&run_stage)?;
        fs::create_dir(&malformed_run_stage)?;

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
        assert_eq!(report.site_builds, 2);
        assert_eq!(report.run_staging_directories, 1);
        assert_eq!(report.quarantined_artifacts, 3);
        assert!(!stale_merge.exists());
        assert!(!malformed_merge.exists());
        assert!(!stale_site.exists());
        assert!(!stale_defaults.exists());
        assert!(!malformed_site.exists());
        assert!(!run_stage.exists());
        assert!(!malformed_run_stage.exists());
        assert!(unrelated.exists());
        assert!(unrelated_site.exists());
        assert!(live_site.exists());
        assert!(live_defaults.exists());
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
    fn candidate_cleanup_preserves_ready_inputs_and_reaps_abandoned_attempts() -> Result<()> {
        let root = TestDir::new()?;
        let data = root.path().join("data");
        let output = root.path().join("output");
        let dist = root.path().join("site/dist");
        fs::create_dir_all(&output)?;
        fs::create_dir_all(dist.parent().context("site parent")?)?;
        for version in ["2026-06", "2026-07", "2026-08"] {
            let analytical = storage::snapshot_analytical_wiki_dir(&data, "nlwiki", version)
                .expect("analytical candidate fixture path");
            let warehouse = storage::snapshot_warehouse_wiki_dir(&data, "nlwiki", version)
                .expect("warehouse candidate fixture path");
            fs::create_dir_all(analytical)?;
            fs::create_dir_all(warehouse)?;
        }
        storage::publish_test_snapshot_pointer(&data, "nlwiki", "2026-08")?;
        let ready = output.join("_candidates/nlwiki/2026-07/ready-run");
        fs::create_dir_all(ready.join("nlwiki"))?;
        fs::write(ready.join("ready.json"), b"{}")?;
        let abandoned = output.join("_candidates/nlwiki/2026-06/dead-run");
        fs::create_dir_all(&abandoned)?;

        let report = clean_abandoned(
            &data,
            &output,
            &dist,
            CleanupStagingRoots::default(),
            &["nlwiki".to_string()],
            None,
            Duration::ZERO,
        )
        .expect("candidate cleanup should succeed");

        assert_eq!(report.candidate_generations, 1);
        assert!(!abandoned.exists());
        assert!(ready.is_dir());
        assert_eq!(
            crate::generation_lifecycle::load(&output, "nlwiki", "2026-06", "dead-run")?
                .context("retired candidate state should remain")?
                .state,
            crate::generation_lifecycle::GenerationState::Retired
        );
        assert!(storage::snapshot_analytical_wiki_dir(&data, "nlwiki", "2026-07")?.is_dir());
        assert!(!storage::snapshot_analytical_wiki_dir(&data, "nlwiki", "2026-06")?.exists());
        Ok(())
    }

    #[test]
    fn candidate_cleanup_uses_lifecycle_state_and_state_age() -> Result<()> {
        let root = TestDir::new()?;
        let output = root.path().join("output");
        for run in ["building", "validated", "already-retired"] {
            fs::create_dir_all(output.join(format!("_candidates/nlwiki/2026-08/{run}")))?;
        }
        crate::generation_lifecycle::begin(&output, "nlwiki", "2026-08", "building")?;
        crate::generation_lifecycle::begin(&output, "nlwiki", "2026-08", "validated")?;
        crate::generation_lifecycle::transition(
            &output,
            "nlwiki",
            "2026-08",
            "validated",
            GState::Validated,
            "validated fixture",
            None,
        )
        .expect("validated fixture state should transition");
        crate::generation_lifecycle::adopt(
            &output,
            "nlwiki",
            "2026-08",
            "already-retired",
            GState::Retired,
            "retired fixture",
        )
        .expect("retired fixture state should be adopted");
        let mut report = CleanupReport::default();

        let protected = clean_candidate_generations(
            &output,
            "nlwiki",
            None,
            Duration::from_secs(3_600),
            SystemTime::now(),
            &mut report,
        )
        .expect("lifecycle cleanup should succeed");

        assert!(protected.contains("2026-08"));
        assert!(!output.join("_candidates/nlwiki/2026-08/building").exists());
        assert!(output.join("_candidates/nlwiki/2026-08/validated").is_dir());
        assert!(
            !output
                .join("_candidates/nlwiki/2026-08/already-retired")
                .exists()
        );
        assert_eq!(report.candidate_generations, 2);
        Ok(())
    }

    #[test]
    fn current_run_wins_the_single_resumable_candidate_slot() -> Result<()> {
        let root = TestDir::new()?;
        let output = root.path().join("output");
        for (snapshot, run) in [("2026-08", "current-run"), ("2026-07", "newer-looking-run")] {
            fs::create_dir_all(output.join(format!("_candidates/nlwiki/{snapshot}/{run}")))?;
            crate::generation_lifecycle::begin(&output, "nlwiki", snapshot, run)?;
        }
        let mut report = CleanupReport::default();

        clean_candidate_generations(
            &output,
            "nlwiki",
            Some("current-run"),
            Duration::from_secs(3_600),
            SystemTime::now(),
            &mut report,
        )
        .expect("current candidate cleanup should succeed");

        assert!(
            output
                .join("_candidates/nlwiki/2026-08/current-run")
                .is_dir()
        );
        assert!(
            !output
                .join("_candidates/nlwiki/2026-07/newer-looking-run")
                .exists()
        );
        assert!(!output.join("_candidates/nlwiki/2026-07").exists());
        assert_eq!(report.candidate_generations, 1);
        Ok(())
    }

    #[test]
    fn recent_unknown_candidate_entries_wait_for_the_recovery_window() -> Result<()> {
        let root = TestDir::new()?;
        let output = root.path().join("output");
        let candidate_root = output.join("_candidates/nlwiki");
        fs::create_dir_all(candidate_root.join("invalid-snapshot/run"))?;
        fs::create_dir_all(candidate_root.join("2026-08/bad$run"))?;
        fs::write(candidate_root.join("unexpected-file"), b"keep")?;
        fs::write(candidate_root.join("2026-08/unexpected-file"), b"keep")?;
        let mut report = CleanupReport::default();

        clean_candidate_generations(
            &output,
            "nlwiki",
            None,
            Duration::from_secs(86_400),
            SystemTime::now(),
            &mut report,
        )
        .expect("recent candidate cleanup should succeed");

        assert_eq!(report.quarantined_artifacts, 0);
        assert!(candidate_root.join("unexpected-file").is_file());
        assert!(candidate_root.join("invalid-snapshot/run").is_dir());
        assert!(candidate_root.join("2026-08/unexpected-file").is_file());
        assert!(candidate_root.join("2026-08/bad$run").is_dir());
        Ok(())
    }

    #[test]
    fn active_prepare_lock_leases_all_candidate_input_generations() -> Result<()> {
        let root = TestDir::new()?;
        let data = root.path().join("data");
        let output = root.path().join("output");
        let dist = root.path().join("site/dist");
        fs::create_dir_all(output.join("_prepare-locks/nlwiki.lock"))?;
        fs::create_dir_all(dist.parent().context("site parent")?)?;
        for version in ["2026-07", "2026-08"] {
            let analytical = storage::snapshot_analytical_wiki_dir(&data, "nlwiki", version)
                .expect("analytical lock fixture path");
            let warehouse = storage::snapshot_warehouse_wiki_dir(&data, "nlwiki", version)
                .expect("warehouse lock fixture path");
            fs::create_dir_all(analytical)?;
            fs::create_dir_all(warehouse)?;
        }
        storage::publish_test_snapshot_pointer(&data, "nlwiki", "2026-08")?;

        let report = clean_abandoned(
            &data,
            &output,
            &dist,
            CleanupStagingRoots::default(),
            &["nlwiki".to_string()],
            None,
            Duration::ZERO,
        )
        .expect("locked candidate cleanup should succeed");

        assert_eq!(report.snapshot_generations, 0);
        assert!(storage::snapshot_analytical_wiki_dir(&data, "nlwiki", "2026-07")?.is_dir());
        Ok(())
    }

    #[test]
    fn candidate_cleanup_handles_links_invalid_entries_and_unsafe_runs() -> Result<()> {
        let root = TestDir::new()?;
        let output = root.path().join("output");
        let candidate_root = output.join("_candidates/nlwiki");
        let live = candidate_root.join("2026-07/live-run");
        fs::create_dir_all(live.join("nlwiki"))?;
        fs::create_dir_all(&output)?;
        std::os::unix::fs::symlink(
            "_candidates/nlwiki/2026-07/live-run/nlwiki",
            output.join("nlwiki"),
        )
        .expect("relative live candidate symlink should be writable");
        fs::write(candidate_root.join("not-a-snapshot"), b"ignored")?;
        fs::create_dir_all(candidate_root.join("invalid-version/run"))?;
        fs::create_dir_all(candidate_root.join("2026-05"))?;
        fs::create_dir_all(candidate_root.join("2026-06/bad$run"))?;
        fs::write(candidate_root.join("2026-06/not-a-run"), b"ignored")?;
        let mut report = CleanupReport::default();

        let protected = clean_candidate_generations(
            &output,
            "nlwiki",
            None,
            Duration::ZERO,
            SystemTime::now(),
            &mut report,
        )
        .expect("candidate entry cleanup should succeed");

        assert!(protected.contains("2026-07"));
        assert!(!candidate_root.join("2026-06/bad$run").exists());
        assert_eq!(report.quarantined_artifacts, 4);
        assert_eq!(report.quarantined.len(), 4);
        assert_eq!(
            crate::generation_lifecycle::load(&output, "nlwiki", "2026-07", "live-run")?
                .context("live legacy candidate should be adopted")?
                .state,
            crate::generation_lifecycle::GenerationState::Published
        );
        assert!(!candidate_root.join("2026-05").exists());
        fs::remove_file(output.join("nlwiki"))?;
        std::os::unix::fs::symlink(live.join("nlwiki"), output.join("nlwiki"))?;
        clean_candidate_generations(
            &output,
            "nlwiki",
            None,
            Duration::ZERO,
            SystemTime::now(),
            &mut report,
        )
        .expect("absolute live candidate link should be preserved");
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
    fn quarantine_receipt_failure_keeps_the_artifact_and_cleans_file_temporaries() -> Result<()> {
        let root = TestDir::new()?;
        let output = root.path();
        let source = output.join("unknown");
        fs::write(&source, b"unknown")?;
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(42);
        let destination = output.join("_quarantine/42/unknown");
        fs::create_dir_all(destination.parent().context("quarantine parent")?)?;
        let temporary = output.join(format!(
            "_quarantine/42/unknown.artifact.quarantine.json.{}.tmp",
            std::process::id()
        ));
        fs::create_dir(&temporary)?;
        let mut report = CleanupReport::default();

        assert!(quarantine(output, source, "test", now, &mut report).is_err());
        assert!(destination.is_file());
        assert!(temporary.is_dir());
        assert_eq!(report.quarantined_artifacts, 0);
        fs::remove_dir(temporary)?;
        assert!(state_is_expired(0, Duration::ZERO, now)?);
        assert!(state_is_expired(u64::MAX, Duration::ZERO, now).is_err());
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
                &BTreeSet::new(),
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
            &BTreeSet::new(),
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
