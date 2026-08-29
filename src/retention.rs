use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::storage;

pub(crate) const RETENTION_RECEIPT_SCHEMA_VERSION: u32 = 1;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RetentionPolicy {
    pub(crate) source_recoverability: SourceRecoverability,
    pub(crate) history_input: InputRetention,
    pub(crate) patrol_source: InputRetention,
    pub(crate) computed_rollback_generations: u8,
}

impl RetentionPolicy {
    pub(crate) fn validate(&self) -> Result<()> {
        ensure!(
            self.computed_rollback_generations == 1,
            "computed rollback generations must be one until zero-retention publication is implemented"
        );
        if self.source_recoverability == SourceRecoverability::Irreplaceable {
            ensure!(
                self.history_input == InputRetention::Retain
                    && self.patrol_source == InputRetention::Retain,
                "irreplaceable sources cannot use purge-after-ready retention"
            );
        }
        Ok(())
    }

    pub(crate) fn purges_any_input(&self) -> bool {
        self.history_input == InputRetention::PurgeAfterReady
            || self.patrol_source == InputRetention::PurgeAfterReady
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SourceRecoverability {
    Redownloadable,
    Irreplaceable,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum InputRetention {
    Retain,
    PurgeAfterReady,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RetentionReceipt {
    pub(crate) schema_version: u32,
    pub(crate) wiki: String,
    pub(crate) snapshot: String,
    pub(crate) state: RetentionState,
    pub(crate) authorized_ready_sha256: String,
    pub(crate) source_plan_sha256: String,
    pub(crate) history_input: InputRetention,
    pub(crate) patrol_source: InputRetention,
    pub(crate) authorized_at_unix: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) applied_at_unix: Option<u64>,
    pub(crate) removed_bytes: u64,
    pub(crate) removed_paths: Vec<String>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RetentionState {
    Authorized,
    Applied,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub(crate) struct RetentionReport {
    pub(crate) schema_version: u32,
    pub(crate) wiki: String,
    pub(crate) snapshot: String,
    pub(crate) apply: bool,
    pub(crate) reclaimable_bytes: u64,
    pub(crate) removed_bytes: u64,
    pub(crate) paths: Vec<String>,
    pub(crate) receipt: String,
}

#[derive(Clone, Debug)]
pub(crate) struct RetentionAuthorization {
    pub(crate) wiki: String,
    pub(crate) snapshot: String,
    pub(crate) ready_sha256: String,
    pub(crate) source_plan_sha256: String,
    pub(crate) policy: RetentionPolicy,
}

pub(crate) fn receipt_path(data_dir: &Path, wiki: &str, snapshot: &str) -> Result<PathBuf> {
    storage::validate_snapshot_version(snapshot)?;
    ensure!(
        !wiki.is_empty()
            && wiki
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_'),
        "unsafe retention wiki identifier"
    );
    Ok(data_dir
        .join("retention")
        .join(wiki)
        .join(format!("{snapshot}.json")))
}

pub(crate) fn validate_purged_snapshot(
    data_dir: &Path,
    wiki: &str,
    snapshot: &str,
) -> Result<RetentionReceipt> {
    let path = receipt_path(data_dir, wiki, snapshot)?;
    let receipt: RetentionReceipt = serde_json::from_slice(
        &fs::read(&path)
            .with_context(|| format!("failed to read retention receipt {}", path.display()))?,
    )
    .with_context(|| format!("invalid retention receipt {}", path.display()))?;
    let valid_state =
        receipt.state == RetentionState::Authorized || receipt.state == RetentionState::Applied;
    ensure!(
        receipt.schema_version == RETENTION_RECEIPT_SCHEMA_VERSION
            && receipt.wiki == wiki
            && receipt.snapshot == snapshot
            && valid_state
            && receipt.authorized_ready_sha256.len() == 64
            && receipt.source_plan_sha256.len() == 64,
        "retention receipt identity is invalid"
    );
    ensure!(
        receipt.history_input == InputRetention::PurgeAfterReady,
        "retention receipt does not authorize purged history input"
    );
    let source_plan = crate::snapshot_plan::plan_path(data_dir, wiki, snapshot)?;
    let (_, observed_source_plan_sha256) = storage::sha256_file(&source_plan)
        .context("purged snapshot canonical source plan is missing")?;
    ensure!(
        observed_source_plan_sha256 == receipt.source_plan_sha256,
        "purged snapshot source plan changed after retention authorization"
    );
    Ok(receipt)
}

pub(crate) fn audit_or_apply(
    data_dir: &Path,
    authorization: RetentionAuthorization,
    apply: bool,
) -> Result<RetentionReport> {
    authorization.policy.validate()?;
    ensure!(
        authorization.policy.source_recoverability == SourceRecoverability::Redownloadable,
        "retention purge requires redownloadable sources"
    );
    ensure!(
        authorization.policy.purges_any_input(),
        "retention policy does not authorize input purging"
    );
    let paths = purge_paths(
        data_dir,
        &authorization.wiki,
        &authorization.snapshot,
        &authorization.policy,
    );
    let paths = paths?;
    let reclaimable_bytes = paths.iter().try_fold(0_u64, |total, path| {
        total
            .checked_add(path_bytes(path)?)
            .context("retention byte total overflow")
    })?;
    let receipt_path = receipt_path(data_dir, &authorization.wiki, &authorization.snapshot)?;
    let mut receipt = RetentionReceipt {
        schema_version: RETENTION_RECEIPT_SCHEMA_VERSION,
        wiki: authorization.wiki.clone(),
        snapshot: authorization.snapshot.clone(),
        state: RetentionState::Authorized,
        authorized_ready_sha256: authorization.ready_sha256,
        source_plan_sha256: authorization.source_plan_sha256,
        history_input: authorization.policy.history_input,
        patrol_source: authorization.policy.patrol_source,
        authorized_at_unix: now_unix()?,
        applied_at_unix: None,
        removed_bytes: 0,
        removed_paths: Vec::new(),
    };
    let display_paths = paths
        .iter()
        .map(|path| path.to_string_lossy().into_owned())
        .collect::<Vec<_>>();
    if apply {
        // Publish authorization before deletion. If the process dies halfway,
        // public outputs remain self-authenticating and a later apply resumes
        // the exact same allowlisted paths idempotently.
        atomic_json(&receipt_path, &receipt)?;
        let mut removed_bytes = 0_u64;
        let mut removed_paths = Vec::new();
        for path in &paths {
            if !path.exists() {
                continue;
            }
            let bytes = path_bytes(path)?;
            remove_path(path)?;
            removed_bytes = removed_bytes
                .checked_add(bytes)
                .context("removed retention byte total overflow")?;
            removed_paths.push(path.to_string_lossy().into_owned());
        }
        receipt.state = RetentionState::Applied;
        receipt.applied_at_unix = Some(now_unix()?);
        receipt.removed_bytes = removed_bytes;
        receipt.removed_paths = removed_paths;
        atomic_json(&receipt_path, &receipt)?;
    }
    Ok(RetentionReport {
        schema_version: RETENTION_RECEIPT_SCHEMA_VERSION,
        wiki: authorization.wiki,
        snapshot: authorization.snapshot,
        apply,
        reclaimable_bytes,
        removed_bytes: receipt.removed_bytes,
        paths: display_paths,
        receipt: receipt_path.to_string_lossy().into_owned(),
    })
}

fn purge_paths(
    data_dir: &Path,
    wiki: &str,
    snapshot: &str,
    policy: &RetentionPolicy,
) -> Result<Vec<PathBuf>> {
    let mut paths = Vec::new();
    if policy.history_input == InputRetention::PurgeAfterReady {
        paths.extend([
            storage::snapshot_analytical_wiki_dir(data_dir, wiki, snapshot)?,
            storage::snapshot_warehouse_wiki_dir(data_dir, wiki, snapshot)?,
            storage::snapshot_metric_input_wiki_dir(data_dir, wiki, snapshot)?,
            storage::generation_manifest_path(data_dir, wiki, snapshot)?,
            crate::compaction::manifest_path(data_dir, wiki, snapshot)?,
            crate::compaction::manifest_path(data_dir, wiki, snapshot)?
                .with_file_name("compaction-transaction.json"),
            crate::fingerprint::data_stage_receipt_path(data_dir, wiki, snapshot, "ingest"),
        ]);
    }
    if policy.patrol_source == InputRetention::PurgeAfterReady {
        paths.push(data_dir.join("patrol").join(wiki));
    }
    paths.sort();
    paths.dedup();
    Ok(paths)
}

fn path_bytes(path: &Path) -> Result<u64> {
    if !path.exists() {
        return Ok(0);
    }
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || metadata.is_file() {
        return Ok(metadata.len());
    }
    ensure!(
        metadata.is_dir(),
        "retention path has unsupported file type"
    );
    let mut bytes = 0_u64;
    for entry in fs::read_dir(path)? {
        bytes = bytes
            .checked_add(path_bytes(&entry?.path())?)
            .context("retention directory byte total overflow")?;
    }
    Ok(bytes)
}

fn remove_path(path: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || metadata.is_file() {
        fs::remove_file(path)?;
    } else {
        ensure!(
            metadata.is_dir(),
            "retention path has unsupported file type"
        );
        fs::remove_dir_all(path)?;
    }
    let parent = path.parent().context("retention path has no parent")?;
    File::open(parent)?.sync_all()?;
    Ok(())
}

fn atomic_json<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    let parent = path.parent().context("retention receipt has no parent")?;
    fs::create_dir_all(parent)?;
    let temporary = parent.join(format!(
        ".{}.{}.tmp",
        path.file_name()
            .context("retention receipt has no filename")?
            .to_string_lossy(),
        std::process::id()
    ));
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
        let _ = fs::remove_file(&temporary);
    }
    result
}

fn now_unix() -> Result<u64> {
    Ok(SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::TestDir;

    #[test]
    fn retention_policy_rejects_irreplaceable_purges() {
        for (history_input, patrol_source) in [
            (InputRetention::PurgeAfterReady, InputRetention::Retain),
            (InputRetention::Retain, InputRetention::PurgeAfterReady),
        ] {
            let policy = RetentionPolicy {
                source_recoverability: SourceRecoverability::Irreplaceable,
                history_input,
                patrol_source,
                computed_rollback_generations: 1,
            };
            assert!(policy.validate().is_err());
        }
    }

    #[test]
    fn retention_policy_rejects_unimplemented_zero_rollback() {
        let policy = RetentionPolicy {
            source_recoverability: SourceRecoverability::Redownloadable,
            history_input: InputRetention::PurgeAfterReady,
            patrol_source: InputRetention::PurgeAfterReady,
            computed_rollback_generations: 0,
        };
        assert!(policy.validate().is_err());
    }

    #[test]
    fn authorized_purge_is_exact_and_idempotent() -> Result<()> {
        let root = TestDir::new()?;
        let wiki = "testwiki";
        let snapshot = "2026-08";
        let history = storage::snapshot_metric_input_wiki_dir(root.path(), wiki, snapshot)?;
        fs::create_dir_all(&history)?;
        fs::write(history.join("part.parquet"), b"metric-input")?;
        let patrol = root.path().join("patrol").join(wiki);
        fs::create_dir_all(&patrol)?;
        fs::write(patrol.join("rights.parquet"), b"patrol-source")?;
        let keep = root
            .path()
            .join("snapshots")
            .join(wiki)
            .join(snapshot)
            .join("source-plan.json");
        fs::create_dir_all(keep.parent().context("test source plan has no parent")?)?;
        fs::write(&keep, b"plan")?;
        let (_, source_plan_sha256) = storage::sha256_file(&keep)?;
        let authorization = || RetentionAuthorization {
            wiki: wiki.to_string(),
            snapshot: snapshot.to_string(),
            ready_sha256: "a".repeat(64),
            source_plan_sha256: source_plan_sha256.clone(),
            policy: RetentionPolicy {
                source_recoverability: SourceRecoverability::Redownloadable,
                history_input: InputRetention::PurgeAfterReady,
                patrol_source: InputRetention::PurgeAfterReady,
                computed_rollback_generations: 1,
            },
        };
        let audit = audit_or_apply(root.path(), authorization(), false)?;
        assert!(audit.reclaimable_bytes > 0);
        assert!(history.is_dir());
        let applied = audit_or_apply(root.path(), authorization(), true)?;
        assert_eq!(applied.removed_bytes, audit.reclaimable_bytes);
        assert!(!history.exists());
        assert!(!patrol.exists());
        assert!(keep.is_file());
        validate_purged_snapshot(root.path(), wiki, snapshot)?;
        let repeated = audit_or_apply(root.path(), authorization(), true)?;
        assert_eq!(repeated.reclaimable_bytes, 0);
        Ok(())
    }

    #[test]
    fn retention_validation_rejects_malformed_identity_and_changed_plan() -> Result<()> {
        let root = TestDir::new()?;
        let wiki = "testwiki";
        let snapshot = "2026-08";
        assert!(receipt_path(root.path(), "../escape", snapshot).is_err());
        assert!(receipt_path(root.path(), wiki, "not-a-snapshot").is_err());

        let source_plan = crate::snapshot_plan::plan_path(root.path(), wiki, snapshot)?;
        fs::create_dir_all(source_plan.parent().context("source plan parent")?)?;
        fs::write(&source_plan, b"canonical plan")?;
        let (_, source_plan_sha256) = storage::sha256_file(&source_plan)?;
        let receipt_file = receipt_path(root.path(), wiki, snapshot)?;
        let valid = RetentionReceipt {
            schema_version: RETENTION_RECEIPT_SCHEMA_VERSION,
            wiki: wiki.to_string(),
            snapshot: snapshot.to_string(),
            state: RetentionState::Applied,
            authorized_ready_sha256: "a".repeat(64),
            source_plan_sha256,
            history_input: InputRetention::PurgeAfterReady,
            patrol_source: InputRetention::Retain,
            authorized_at_unix: 1,
            applied_at_unix: Some(2),
            removed_bytes: 3,
            removed_paths: Vec::new(),
        };
        atomic_json(&receipt_file, &valid)?;
        validate_purged_snapshot(root.path(), wiki, snapshot)?;

        let mut malformed = valid.clone();
        malformed.schema_version = 0;
        atomic_json(&receipt_file, &malformed)?;
        assert!(validate_purged_snapshot(root.path(), wiki, snapshot).is_err());
        malformed = valid.clone();
        malformed.history_input = InputRetention::Retain;
        atomic_json(&receipt_file, &malformed)?;
        assert!(validate_purged_snapshot(root.path(), wiki, snapshot).is_err());
        atomic_json(&receipt_file, &valid)?;
        fs::write(&source_plan, b"changed plan")?;
        assert!(validate_purged_snapshot(root.path(), wiki, snapshot).is_err());
        Ok(())
    }

    #[test]
    fn retention_policy_granularity_and_fail_closed_paths_are_covered() -> Result<()> {
        let root = TestDir::new()?;
        let authorization = |policy| RetentionAuthorization {
            wiki: "testwiki".to_string(),
            snapshot: "2026-08".to_string(),
            ready_sha256: "a".repeat(64),
            source_plan_sha256: "b".repeat(64),
            policy,
        };
        let irreplaceable = RetentionPolicy {
            source_recoverability: SourceRecoverability::Irreplaceable,
            history_input: InputRetention::Retain,
            patrol_source: InputRetention::Retain,
            computed_rollback_generations: 1,
        };
        assert!(audit_or_apply(root.path(), authorization(irreplaceable), false).is_err());
        let retain_all = RetentionPolicy {
            source_recoverability: SourceRecoverability::Redownloadable,
            history_input: InputRetention::Retain,
            patrol_source: InputRetention::Retain,
            computed_rollback_generations: 1,
        };
        assert!(!retain_all.purges_any_input());
        assert!(audit_or_apply(root.path(), authorization(retain_all), false).is_err());

        let patrol = root.path().join("patrol/testwiki");
        fs::create_dir_all(&patrol)?;
        fs::write(patrol.join("events.parquet"), b"events")?;
        let patrol_only = RetentionPolicy {
            source_recoverability: SourceRecoverability::Redownloadable,
            history_input: InputRetention::Retain,
            patrol_source: InputRetention::PurgeAfterReady,
            computed_rollback_generations: 1,
        };
        assert!(patrol_only.purges_any_input());
        let report = audit_or_apply(root.path(), authorization(patrol_only), true)?;
        assert!(report.removed_bytes > 0);
        assert!(!patrol.exists());
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn retention_file_kinds_and_atomic_failure_cleanup_are_covered() -> Result<()> {
        use std::os::unix::fs::symlink;
        use std::os::unix::net::UnixListener;

        let root = TestDir::new()?;
        let regular = root.path().join("regular");
        fs::write(&regular, b"regular")?;
        assert_eq!(path_bytes(&regular)?, 7);
        remove_path(&regular)?;

        let target = root.path().join("target");
        fs::write(&target, b"target")?;
        let link = root.path().join("link");
        symlink(&target, &link)?;
        assert!(path_bytes(&link)? > 0);
        remove_path(&link)?;
        assert!(target.is_file());

        let socket = root.path().join("socket");
        let _listener = UnixListener::bind(&socket)?;
        assert!(path_bytes(&socket).is_err());
        assert!(remove_path(&socket).is_err());

        let blocked = root.path().join("blocked.json");
        fs::create_dir(&blocked)?;
        fs::write(blocked.join("keep"), b"keep")?;
        assert!(atomic_json(&blocked, &serde_json::json!({"ok": true})).is_err());
        assert!(
            !root
                .path()
                .join(format!(".blocked.json.{}.tmp", std::process::id()))
                .exists()
        );
        Ok(())
    }
}
