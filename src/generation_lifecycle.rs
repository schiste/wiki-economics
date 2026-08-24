use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

const SCHEMA_VERSION: u8 = 1;

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum GenerationState {
    Building,
    Validated,
    Ready,
    Published,
    Superseded,
    Retired,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct StateTransition {
    pub(crate) state: GenerationState,
    pub(crate) at_unix: u64,
    pub(crate) reason: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) publication_run_id: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct GenerationRecord {
    pub(crate) schema_version: u8,
    pub(crate) wiki: String,
    pub(crate) snapshot: String,
    pub(crate) run_id: String,
    pub(crate) candidate_relative: String,
    pub(crate) state: GenerationState,
    pub(crate) created_at_unix: u64,
    pub(crate) updated_at_unix: u64,
    pub(crate) history: Vec<StateTransition>,
}

pub(crate) fn state_path(
    output_dir: &Path,
    wiki: &str,
    snapshot: &str,
    run_id: &str,
) -> Result<PathBuf> {
    validate_component(wiki, "wiki")?;
    crate::storage::validate_snapshot_version(snapshot)?;
    validate_component(run_id, "run ID")?;
    Ok(output_dir
        .join("_generation-state")
        .join(wiki)
        .join(snapshot)
        .join(format!("{run_id}.json")))
}

pub(crate) fn load(
    output_dir: &Path,
    wiki: &str,
    snapshot: &str,
    run_id: &str,
) -> Result<Option<GenerationRecord>> {
    let path = state_path(output_dir, wiki, snapshot, run_id)?;
    if !path.is_file() {
        return Ok(None);
    }
    let record: GenerationRecord = serde_json::from_reader(
        File::open(&path).with_context(|| format!("failed to open {}", path.display()))?,
    )
    .with_context(|| format!("failed to parse {}", path.display()))?;
    validate_record(&record, wiki, snapshot, run_id)?;
    Ok(Some(record))
}

pub(crate) fn begin(
    output_dir: &Path,
    wiki: &str,
    snapshot: &str,
    run_id: &str,
) -> Result<GenerationRecord> {
    if let Some(record) = load(output_dir, wiki, snapshot, run_id)? {
        ensure!(
            matches!(
                record.state,
                GenerationState::Building | GenerationState::Validated
            ),
            "candidate generation already reached {:?}",
            record.state
        );
        return Ok(record);
    }
    let now = now_unix()?;
    let record = GenerationRecord {
        schema_version: SCHEMA_VERSION,
        wiki: wiki.to_string(),
        snapshot: snapshot.to_string(),
        run_id: run_id.to_string(),
        candidate_relative: format!("_candidates/{wiki}/{snapshot}/{run_id}"),
        state: GenerationState::Building,
        created_at_unix: now,
        updated_at_unix: now,
        history: vec![StateTransition {
            state: GenerationState::Building,
            at_unix: now,
            reason: "candidate preparation started".to_string(),
            publication_run_id: None,
        }],
    };
    write(output_dir, &record)?;
    Ok(record)
}

pub(crate) fn adopt(
    output_dir: &Path,
    wiki: &str,
    snapshot: &str,
    run_id: &str,
    state: GenerationState,
    reason: &str,
) -> Result<GenerationRecord> {
    if let Some(record) = load(output_dir, wiki, snapshot, run_id)? {
        return Ok(record);
    }
    let now = now_unix()?;
    let record = GenerationRecord {
        schema_version: SCHEMA_VERSION,
        wiki: wiki.to_string(),
        snapshot: snapshot.to_string(),
        run_id: run_id.to_string(),
        candidate_relative: format!("_candidates/{wiki}/{snapshot}/{run_id}"),
        state,
        created_at_unix: now,
        updated_at_unix: now,
        history: vec![StateTransition {
            state,
            at_unix: now,
            reason: reason.to_string(),
            publication_run_id: None,
        }],
    };
    write(output_dir, &record)?;
    Ok(record)
}

pub(crate) fn transition(
    output_dir: &Path,
    wiki: &str,
    snapshot: &str,
    run_id: &str,
    next: GenerationState,
    reason: &str,
    publication_run_id: Option<&str>,
) -> Result<GenerationRecord> {
    let mut record = load(output_dir, wiki, snapshot, run_id)?.with_context(|| {
        format!("candidate generation state is missing for {wiki}/{snapshot}/{run_id}")
    })?;
    if record.state == next {
        return Ok(record);
    }
    ensure!(
        transition_allowed(record.state, next),
        "invalid candidate generation transition {:?} -> {:?}",
        record.state,
        next
    );
    let now = now_unix()?;
    record.state = next;
    record.updated_at_unix = now;
    record.history.push(StateTransition {
        state: next,
        at_unix: now,
        reason: reason.to_string(),
        publication_run_id: publication_run_id.map(str::to_string),
    });
    write(output_dir, &record)?;
    Ok(record)
}

fn transition_allowed(current: GenerationState, next: GenerationState) -> bool {
    matches!(
        (current, next),
        (GenerationState::Building, GenerationState::Validated)
            | (GenerationState::Validated, GenerationState::Ready)
            | (GenerationState::Ready, GenerationState::Published)
            | (GenerationState::Ready, GenerationState::Superseded)
            | (GenerationState::Published, GenerationState::Superseded)
            | (GenerationState::Superseded, GenerationState::Retired)
            | (GenerationState::Building, GenerationState::Retired)
            | (GenerationState::Validated, GenerationState::Retired)
    )
}

fn validate_record(
    record: &GenerationRecord,
    wiki: &str,
    snapshot: &str,
    run_id: &str,
) -> Result<()> {
    ensure!(
        record.schema_version == SCHEMA_VERSION,
        "unsupported candidate generation state schema"
    );
    ensure!(
        record.wiki == wiki && record.snapshot == snapshot && record.run_id == run_id,
        "candidate generation state identity mismatch"
    );
    ensure!(
        record.candidate_relative == format!("_candidates/{wiki}/{snapshot}/{run_id}"),
        "candidate generation path mismatch"
    );
    ensure!(
        record.history.last().map(|entry| entry.state) == Some(record.state),
        "candidate generation history does not end at its current state"
    );
    Ok(())
}

fn write(output_dir: &Path, record: &GenerationRecord) -> Result<()> {
    let path = state_path(output_dir, &record.wiki, &record.snapshot, &record.run_id)?;
    let parent = path
        .parent()
        .context("generation state path has no parent")?;
    fs::create_dir_all(parent)?;
    let temporary = parent.join(format!(
        ".{}.{}.tmp",
        path.file_name()
            .context("generation state path has no filename")?
            .to_string_lossy(),
        std::process::id()
    ));
    let result = (|| -> Result<()> {
        let mut file = File::create(&temporary)?;
        serde_json::to_writer_pretty(&mut file, record)?;
        file.sync_all()?;
        fs::rename(&temporary, &path)?;
        File::open(parent)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

fn validate_component(value: &str, label: &str) -> Result<()> {
    ensure!(
        !value.is_empty()
            && value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.')),
        "unsafe generation {label}"
    );
    Ok(())
}

fn now_unix() -> Result<u64> {
    Ok(SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::TestDir;

    fn must<T, E: std::fmt::Debug>(result: std::result::Result<T, E>) -> T {
        result.expect("lifecycle test operation should succeed")
    }

    #[test]
    fn lifecycle_is_atomic_validated_and_strictly_ordered() {
        let root = must(TestDir::new());
        let output = root.path();
        let building = must(begin(output, "nlwiki", "2026-08", "run-1"));
        assert_eq!(building.state, GenerationState::Building);
        assert_eq!(must(begin(output, "nlwiki", "2026-08", "run-1")), building);
        assert!(
            transition(
                output,
                "nlwiki",
                "2026-08",
                "run-1",
                GenerationState::Ready,
                "skip",
                None
            )
            .is_err()
        );
        must(transition(
            output,
            "nlwiki",
            "2026-08",
            "run-1",
            GenerationState::Validated,
            "validated",
            None,
        ));
        assert_eq!(
            must(begin(output, "nlwiki", "2026-08", "run-1")).state,
            GenerationState::Validated
        );
        must(transition(
            output,
            "nlwiki",
            "2026-08",
            "run-1",
            GenerationState::Ready,
            "ready",
            None,
        ));
        must(transition(
            output,
            "nlwiki",
            "2026-08",
            "run-1",
            GenerationState::Published,
            "published",
            Some("publish-1"),
        ));
        must(transition(
            output,
            "nlwiki",
            "2026-08",
            "run-1",
            GenerationState::Superseded,
            "superseded",
            Some("publish-2"),
        ));
        let retired = must(transition(
            output,
            "nlwiki",
            "2026-08",
            "run-1",
            GenerationState::Retired,
            "retired",
            Some("publish-3"),
        ));
        assert_eq!(retired.state, GenerationState::Retired);
        assert_eq!(retired.history.len(), 6);
        assert_eq!(
            retired.history[3].publication_run_id.as_deref(),
            Some("publish-1")
        );
        assert_eq!(
            must(load(output, "nlwiki", "2026-08", "run-1")),
            Some(retired.clone())
        );
        assert_eq!(
            must(transition(
                output,
                "nlwiki",
                "2026-08",
                "run-1",
                GenerationState::Retired,
                "again",
                None
            )),
            retired
        );
        assert!(begin(output, "nlwiki", "2026-08", "run-1").is_err());
    }

    #[test]
    fn adoption_and_corruption_fail_closed() {
        let root = must(TestDir::new());
        assert!(state_path(root.path(), "bad/wiki", "2026-08", "run").is_err());
        assert!(state_path(root.path(), "nlwiki", "bad", "run").is_err());
        assert!(state_path(root.path(), "nlwiki", "2026-08", "bad/run").is_err());
        let adopted = must(adopt(
            root.path(),
            "nlwiki",
            "2026-08",
            "legacy",
            GenerationState::Ready,
            "legacy ready",
        ));
        assert_eq!(
            must(adopt(
                root.path(),
                "nlwiki",
                "2026-08",
                "legacy",
                GenerationState::Published,
                "ignored"
            )),
            adopted
        );
        let path = must(state_path(root.path(), "nlwiki", "2026-08", "legacy"));
        let mut value: serde_json::Value = must(serde_json::from_reader(must(File::open(&path))));
        value["candidate_relative"] = serde_json::Value::String("elsewhere".to_string());
        must(fs::write(&path, must(serde_json::to_vec(&value))));
        assert!(load(root.path(), "nlwiki", "2026-08", "legacy").is_err());
        must(fs::write(&path, b"{"));
        assert!(load(root.path(), "nlwiki", "2026-08", "legacy").is_err());
        assert_eq!(
            must(load(root.path(), "nlwiki", "2026-08", "missing")),
            None
        );
    }

    #[test]
    fn incomplete_generations_can_expire_directly() {
        let root = must(TestDir::new());
        must(begin(root.path(), "nlwiki", "2026-08", "building"));
        assert_eq!(
            must(transition(
                root.path(),
                "nlwiki",
                "2026-08",
                "building",
                GenerationState::Retired,
                "expired",
                None
            ))
            .state,
            GenerationState::Retired
        );
        must(adopt(
            root.path(),
            "nlwiki",
            "2026-08",
            "validated",
            GenerationState::Validated,
            "recovered",
        ));
        assert_eq!(
            must(transition(
                root.path(),
                "nlwiki",
                "2026-08",
                "validated",
                GenerationState::Retired,
                "expired",
                None
            ))
            .state,
            GenerationState::Retired
        );
        must(adopt(
            root.path(),
            "nlwiki",
            "2026-08",
            "ready",
            GenerationState::Ready,
            "ready",
        ));
        assert!(
            transition(
                root.path(),
                "nlwiki",
                "2026-08",
                "ready",
                GenerationState::Retired,
                "invalid",
                None
            )
            .is_err()
        );
        assert!(
            transition(
                root.path(),
                "nlwiki",
                "2026-08",
                "missing",
                GenerationState::Retired,
                "missing",
                None
            )
            .is_err()
        );
    }

    #[test]
    fn failed_atomic_state_write_cleans_its_temporary_file() {
        let root = must(TestDir::new());
        must(begin(root.path(), "nlwiki", "2026-08", "write-failure"));
        let path = must(state_path(
            root.path(),
            "nlwiki",
            "2026-08",
            "write-failure",
        ));
        let temporary = path.parent().expect("state parent").join(format!(
            ".{}.{}.tmp",
            path.file_name().expect("state filename").to_string_lossy(),
            std::process::id()
        ));
        must(fs::create_dir(&temporary));
        assert!(
            transition(
                root.path(),
                "nlwiki",
                "2026-08",
                "write-failure",
                GenerationState::Validated,
                "forced write failure",
                None,
            )
            .is_err()
        );
        assert!(temporary.is_dir());
        must(fs::remove_dir(temporary));
    }
}
