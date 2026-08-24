use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use url::Url;

use crate::storage;

pub(crate) const SNAPSHOT_PLAN_SCHEMA_VERSION: u32 = 1;
pub(crate) const MEDIAWIKI_HISTORY_BASE_URL: &str =
    "https://dumps.wikimedia.org/other/mediawiki_history";
const PLAN_FILENAME: &str = "source-plan.json";
const FIRST_HISTORY_MONTH: &str = "2001-01";

/// Wikipedia language editions partitioned yearly in MediaWiki History.
/// If a project is promoted from all-time to yearly upstream, update this
/// source-layout registry and its plan fixture together.
pub(crate) const YEARLY_WIKIS: &[&str] = &[
    "arwiki", "bgwiki", "cawiki", "cebwiki", "cswiki", "dawiki", "dewiki", "eswiki", "fawiki",
    "fiwiki", "frwiki", "hewiki", "huwiki", "idwiki", "itwiki", "jawiki", "kowiki", "nlwiki",
    "nowiki", "plwiki", "ptwiki", "rowiki", "ruwiki", "shwiki", "srwiki", "svwiki", "thwiki",
    "trwiki", "ukwiki", "viwiki", "zhwiki",
];

/// Wikipedia projects with a qualified, contiguous monthly source inventory.
/// Commons contains sparse early event months and therefore needs a future
/// directory-index plan rather than an assumed continuous launch-date range.
pub(crate) const MONTHLY_WIKIS: &[&str] = &["enwiki"];
const UNQUALIFIED_MONTHLY_PROJECTS: &[&str] = &["commonswiki", "wikidatawiki"];

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(try_from = "String", into = "String")]
pub(crate) struct WikiId(String);

impl WikiId {
    pub(crate) fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        ensure!(
            !value.is_empty()
                && value.len() <= 128
                && value
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_'),
            "invalid wiki database name {value:?}"
        );
        Ok(Self(value))
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for WikiId {
    type Error = anyhow::Error;

    fn try_from(value: String) -> Result<Self> {
        Self::new(value)
    }
}

impl From<WikiId> for String {
    fn from(value: WikiId) -> Self {
        value.0
    }
}

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(try_from = "String", into = "String")]
pub(crate) struct SnapshotVersion(String);

impl SnapshotVersion {
    pub(crate) fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        storage::validate_snapshot_version(&value)?;
        Ok(Self(value))
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }

    fn year(&self) -> Result<u32> {
        self.0[..4]
            .parse()
            .with_context(|| format!("invalid snapshot year in {:?}", self.0))
    }

    fn following_month(&self) -> Result<String> {
        next_month(&self.0)
    }
}

impl TryFrom<String> for SnapshotVersion {
    type Error = anyhow::Error;

    fn try_from(value: String) -> Result<Self> {
        Self::new(value)
    }
}

impl From<SnapshotVersion> for String {
    fn from(value: SnapshotVersion) -> Self {
        value.0
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SourceLayout {
    AllTime,
    Yearly,
    Monthly,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct DateRange {
    pub(crate) start: String,
    pub(crate) end: String,
}

impl DateRange {
    fn new(start: impl Into<String>, end: impl Into<String>) -> Result<Self> {
        let value = Self {
            start: start.into(),
            end: end.into(),
        };
        value.validate()?;
        Ok(value)
    }

    fn validate(&self) -> Result<()> {
        validate_year_month(&self.start)?;
        validate_year_month(&self.end)?;
        ensure!(
            self.start <= self.end,
            "invalid event range {} through {}",
            self.start,
            self.end
        );
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct SourceSpec {
    pub(crate) source_id: String,
    pub(crate) url: Url,
    pub(crate) expected_size: Option<u64>,
    pub(crate) event_range: DateRange,
}

impl SourceSpec {
    pub(crate) fn filename(&self) -> Result<&str> {
        self.url
            .path_segments()
            .and_then(Iterator::last)
            .filter(|value| !value.is_empty())
            .context("snapshot source URL has no filename")
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct SnapshotPlan {
    pub(crate) schema_version: u32,
    pub(crate) wiki: WikiId,
    pub(crate) snapshot: SnapshotVersion,
    pub(crate) layout: SourceLayout,
    pub(crate) sources: Vec<SourceSpec>,
    pub(crate) expected_date_range: DateRange,
}

impl SnapshotPlan {
    pub(crate) fn resolve(wiki: &str, snapshot: &str) -> Result<Self> {
        Self::resolve_from_base(MEDIAWIKI_HISTORY_BASE_URL, wiki, snapshot)
    }

    pub(crate) fn resolve_from_base(base_url: &str, wiki: &str, snapshot: &str) -> Result<Self> {
        let wiki = WikiId::new(wiki)?;
        let snapshot = SnapshotVersion::new(snapshot)?;
        let layout = source_layout(wiki.as_str())?;
        let last_month = snapshot.following_month()?;
        let first_month = first_history_month(wiki.as_str(), layout);
        let ranges = source_ranges(layout, &snapshot, first_month, &last_month)?;
        let mut sources = Vec::with_capacity(ranges.len());
        for (partition, event_range) in ranges {
            let filename = match layout {
                SourceLayout::AllTime => {
                    format!("{}.{}.all-time.tsv.bz2", snapshot.as_str(), wiki.as_str())
                }
                SourceLayout::Yearly | SourceLayout::Monthly => format!(
                    "{}.{}.{}.tsv.bz2",
                    snapshot.as_str(),
                    wiki.as_str(),
                    partition
                ),
            };
            let source_id = filename
                .strip_suffix(".tsv.bz2")
                .expect("constructed source filename has the expected suffix")
                .to_string();
            let url = Url::parse(&format!(
                "{}/{}/{}/{}",
                base_url.trim_end_matches('/'),
                snapshot.as_str(),
                wiki.as_str(),
                filename
            ))
            .context("invalid MediaWiki History source URL")?;
            sources.push(SourceSpec {
                source_id,
                url,
                expected_size: None,
                event_range,
            });
        }
        Ok(Self {
            schema_version: SNAPSHOT_PLAN_SCHEMA_VERSION,
            wiki,
            snapshot,
            layout,
            sources,
            expected_date_range: DateRange::new(first_month, last_month)?,
        })
    }

    pub(crate) fn validate(&self) -> Result<()> {
        ensure!(
            self.schema_version == SNAPSHOT_PLAN_SCHEMA_VERSION,
            "unsupported snapshot source plan schema {}",
            self.schema_version
        );
        self.expected_date_range.validate()?;
        ensure!(!self.sources.is_empty(), "snapshot source plan is empty");
        ensure!(
            self.sources
                .windows(2)
                .all(|pair| pair[0].source_id < pair[1].source_id),
            "snapshot source identities are not unique and strictly sorted"
        );
        let expected = Self::resolve_from_base(
            source_base_url(self)?,
            self.wiki.as_str(),
            self.snapshot.as_str(),
        )?;
        ensure!(
            self.layout == expected.layout,
            "snapshot source layout does not match wiki {}",
            self.wiki.as_str()
        );
        ensure!(
            self.expected_date_range == expected.expected_date_range,
            "snapshot plan date range does not match its source layout"
        );
        ensure!(
            self.sources.len() == expected.sources.len(),
            "snapshot source plan expected {} sources but found {}",
            expected.sources.len(),
            self.sources.len()
        );
        for (actual, expected) in self.sources.iter().zip(&expected.sources) {
            ensure!(
                actual.source_id == expected.source_id
                    && actual.url == expected.url
                    && actual.event_range == expected.event_range,
                "snapshot source plan contains an unexpected, missing, duplicate, or out-of-order source near {:?}",
                actual.source_id
            );
            ensure!(
                actual.expected_size != Some(0),
                "snapshot source {} has an invalid zero expected size",
                actual.source_id
            );
            let filename = actual.filename()?;
            ensure!(
                filename.strip_suffix(".tsv.bz2") == Some(actual.source_id.as_str()),
                "snapshot source ID does not match URL filename for {}",
                actual.source_id
            );
        }
        Ok(())
    }

    pub(crate) fn filenames(&self) -> Result<Vec<String>> {
        self.sources
            .iter()
            .map(|source| source.filename().map(str::to_string))
            .collect()
    }

    pub(crate) fn persist(&self, data_dir: &Path) -> Result<PathBuf> {
        self.validate()?;
        let path = plan_path(data_dir, self.wiki.as_str(), self.snapshot.as_str())?;
        if path.exists() {
            let stored = Self::load(&path)?;
            ensure!(
                stored == *self,
                "persisted snapshot source plan differs from resolved plan at {}",
                path.display()
            );
            return Ok(path);
        }
        write_plan_atomic(&path, self, |from, to| fs::rename(from, to))?;
        Ok(path)
    }

    pub(crate) fn load(path: &Path) -> Result<Self> {
        let plan: Self = serde_json::from_slice(
            &fs::read(path)
                .with_context(|| format!("failed to read snapshot plan {}", path.display()))?,
        )
        .with_context(|| format!("invalid snapshot plan JSON in {}", path.display()))?;
        plan.validate()
            .with_context(|| format!("invalid snapshot plan in {}", path.display()))?;
        Ok(plan)
    }

    pub(crate) fn load_or_resolve(
        data_dir: &Path,
        wiki: &str,
        snapshot: &str,
    ) -> Result<(Self, PathBuf)> {
        let expected = Self::resolve(wiki, snapshot)?;
        let path = plan_path(data_dir, wiki, snapshot)?;
        if path.exists() {
            let stored = Self::load(&path)?;
            ensure!(
                stored == expected,
                "persisted snapshot source plan does not match {wiki} {snapshot}"
            );
            Ok((stored, path))
        } else {
            let path = expected.persist(data_dir)?;
            Ok((expected, path))
        }
    }
}

fn write_plan_atomic<F>(path: &Path, plan: &SnapshotPlan, rename: F) -> Result<()>
where
    F: FnOnce(&Path, &Path) -> std::io::Result<()>,
{
    let parent = path.parent().context("snapshot plan has no parent")?;
    fs::create_dir_all(parent)?;
    let (mut file, temporary) = create_temporary(parent)?;
    let result = (|| -> Result<()> {
        serde_json::to_writer_pretty(&mut file, plan)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        rename(&temporary, path)?;
        File::open(parent)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result?;
    Ok(())
}

pub(crate) fn plan_path(data_dir: &Path, wiki: &str, snapshot: &str) -> Result<PathBuf> {
    let wiki = WikiId::new(wiki)?;
    let snapshot = SnapshotVersion::new(snapshot)?;
    Ok(data_dir
        .join("snapshots")
        .join(wiki.as_str())
        .join(snapshot.as_str())
        .join(PLAN_FILENAME))
}

fn source_layout(wiki: &str) -> Result<SourceLayout> {
    if MONTHLY_WIKIS.contains(&wiki) {
        Ok(SourceLayout::Monthly)
    } else if YEARLY_WIKIS.contains(&wiki) {
        Ok(SourceLayout::Yearly)
    } else if UNQUALIFIED_MONTHLY_PROJECTS.contains(&wiki) {
        anyhow::bail!(
            "monthly source inventory for {wiki} is not yet qualified; directory-index discovery is required"
        )
    } else {
        Ok(SourceLayout::AllTime)
    }
}

fn first_history_month(_wiki: &str, _layout: SourceLayout) -> &'static str {
    FIRST_HISTORY_MONTH
}

fn source_ranges(
    layout: SourceLayout,
    snapshot: &SnapshotVersion,
    first_month: &str,
    last_month: &str,
) -> Result<Vec<(String, DateRange)>> {
    match layout {
        SourceLayout::AllTime => Ok(vec![(
            "all-time".to_string(),
            DateRange::new(first_month, last_month)?,
        )]),
        SourceLayout::Yearly => {
            let end_year = snapshot.year()?;
            (2001..=end_year)
                .map(|year| {
                    let end = if year == end_year {
                        last_month.to_string()
                    } else {
                        format!("{year:04}-12")
                    };
                    Ok((
                        year.to_string(),
                        DateRange::new(format!("{year:04}-01"), end)?,
                    ))
                })
                .collect()
        }
        SourceLayout::Monthly => {
            let mut month = first_month.to_string();
            let mut ranges = Vec::new();
            while month.as_str() <= last_month {
                ranges.push((month.clone(), DateRange::new(&month, &month)?));
                month = next_month(&month)?;
            }
            Ok(ranges)
        }
    }
}

fn validate_year_month(value: &str) -> Result<()> {
    storage::validate_snapshot_version(value)
}

fn next_month(value: &str) -> Result<String> {
    validate_year_month(value)?;
    let year: u32 = value[..4].parse()?;
    let month: u32 = value[5..].parse()?;
    if month == 12 {
        Ok(format!("{:04}-01", year + 1))
    } else {
        Ok(format!("{year:04}-{:02}", month + 1))
    }
}

fn source_base_url(plan: &SnapshotPlan) -> Result<&str> {
    let first = plan
        .sources
        .first()
        .context("snapshot source plan is empty")?;
    let suffix = format!(
        "/{}/{}/{}",
        plan.snapshot.as_str(),
        plan.wiki.as_str(),
        first.filename()?
    );
    first
        .url
        .as_str()
        .strip_suffix(&suffix)
        .context("snapshot source URL does not match its wiki and snapshot")
}

fn create_temporary(parent: &Path) -> Result<(File, PathBuf)> {
    for attempt in 0..100_u8 {
        let path = parent.join(format!(".source-plan-{}-{attempt}.tmp", std::process::id()));
        match OpenOptions::new().write(true).create_new(true).open(&path) {
            Ok(file) => return Ok((file, path)),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error.into()),
        }
    }
    anyhow::bail!(
        "could not allocate a temporary snapshot plan beneath {}",
        parent.display()
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::TestDir;

    #[test]
    fn resolves_yearly_and_all_time_layouts_deterministically() -> Result<()> {
        let yearly = SnapshotPlan::resolve("frwiki", "2026-02")?;
        assert_eq!(yearly.layout, SourceLayout::Yearly);
        assert_eq!(yearly.sources.len(), 26);
        assert_eq!(
            yearly.sources.first().unwrap().filename()?,
            "2026-02.frwiki.2001.tsv.bz2"
        );
        assert_eq!(
            yearly.sources.last().unwrap().event_range,
            DateRange::new("2026-01", "2026-03")?
        );

        let all_time = SnapshotPlan::resolve("simplewiki", "2026-02")?;
        assert_eq!(all_time.layout, SourceLayout::AllTime);
        assert_eq!(
            all_time.filenames()?,
            ["2026-02.simplewiki.all-time.tsv.bz2"]
        );
        Ok(())
    }

    #[test]
    fn resolves_complete_enwiki_monthly_inventory() -> Result<()> {
        let plan = SnapshotPlan::resolve("enwiki", "2026-07")?;
        assert_eq!(plan.layout, SourceLayout::Monthly);
        assert_eq!(plan.sources.len(), 308);
        assert_eq!(
            plan.sources.first().unwrap().filename()?,
            "2026-07.enwiki.2001-01.tsv.bz2"
        );
        assert_eq!(
            plan.sources.last().unwrap().filename()?,
            "2026-07.enwiki.2026-08.tsv.bz2"
        );
        assert_eq!(
            plan.expected_date_range,
            DateRange::new("2001-01", "2026-08")?
        );
        Ok(())
    }

    #[test]
    fn plan_validation_rejects_missing_duplicate_and_reordered_sources() -> Result<()> {
        let plan = SnapshotPlan::resolve("enwiki", "2001-02")?;

        let mut missing = plan.clone();
        missing.sources.remove(1);
        assert!(missing.validate().is_err());

        let mut duplicate = plan.clone();
        duplicate.sources[1] = duplicate.sources[0].clone();
        assert!(duplicate.validate().is_err());

        let mut reordered = plan;
        reordered.sources.swap(0, 1);
        assert!(reordered.validate().is_err());
        Ok(())
    }

    #[test]
    fn persisted_plan_is_atomic_immutable_and_fail_closed() -> Result<()> {
        let data_dir = TestDir::new()?;
        let second_data_dir = TestDir::new().expect("second plan directory should be writable");
        let plan = SnapshotPlan::resolve("enwiki", "2001-02")?;
        let path = plan.persist(data_dir.path())?;
        let first_bytes = fs::read(&path)?;
        let second_path = plan
            .persist(second_data_dir.path())
            .expect("second plan should persist");
        assert_eq!(
            fs::read(&second_path).expect("second plan should be readable"),
            first_bytes
        );
        assert_eq!(SnapshotPlan::load(&path)?, plan);
        assert_eq!(plan.persist(data_dir.path())?, path);
        assert_eq!(fs::read(&path)?, first_bytes);

        let mut conflicting = plan.clone();
        conflicting.sources[0].expected_size = Some(13);
        assert!(conflicting.validate().is_ok());
        assert!(conflicting.persist(data_dir.path()).is_err());

        fs::write(&path, b"{truncated")?;
        assert!(SnapshotPlan::load(&path).is_err());
        assert!(plan.persist(data_dir.path()).is_err());
        Ok(())
    }

    #[test]
    fn identifiers_and_deserialized_ranges_are_validated() -> Result<()> {
        assert!(SnapshotPlan::resolve("../enwiki", "2026-07").is_err());
        assert!(SnapshotPlan::resolve("enwiki", "2026-13").is_err());
        assert!(
            SnapshotPlan::resolve("commonswiki", "2026-07")
                .unwrap_err()
                .to_string()
                .contains("directory-index discovery")
        );

        let plan = SnapshotPlan::resolve("simplewiki", "2026-07")?;
        let mut wrong_layout = plan.clone();
        wrong_layout.layout = SourceLayout::Monthly;
        assert!(wrong_layout.validate().is_err());

        let mut unqualified_project = plan.clone();
        unqualified_project.wiki = WikiId::new("commonswiki")?;
        unqualified_project.sources[0].url = Url::parse(
            &unqualified_project.sources[0]
                .url
                .as_str()
                .replace("simplewiki", "commonswiki"),
        )
        .expect("rewritten fixture URL should remain valid");
        assert!(unqualified_project.validate().is_err());

        let mut value = serde_json::to_value(plan)?;
        value["expected_date_range"]["end"] = serde_json::json!("ancient");
        let decoded: SnapshotPlan = serde_json::from_value(value)?;
        assert!(decoded.validate().is_err());
        Ok(())
    }

    #[test]
    fn atomic_writer_cleans_failures_and_temporary_allocator_is_bounded() -> Result<()> {
        let directory = TestDir::new()?;
        let plan = SnapshotPlan::resolve("simplewiki", "2026-07")?;
        let destination = directory.path().join("failed-source-plan.json");
        let error = write_plan_atomic(&destination, &plan, |_, _| {
            Err(std::io::Error::other("injected rename failure"))
        })
        .expect_err("injected rename failure should propagate");
        assert!(error.to_string().contains("injected rename failure"));
        assert!(!destination.exists());
        assert!(fs::read_dir(directory.path())?.next().is_none());

        let pid = std::process::id();
        fs::write(
            directory.path().join(format!(".source-plan-{pid}-0.tmp")),
            b"occupied",
        )
        .expect("occupied temporary fixture should be writable");
        let (temporary, allocated) = create_temporary(directory.path())?;
        drop(temporary);
        assert!(allocated.ends_with(format!(".source-plan-{pid}-1.tmp")));
        fs::remove_file(allocated)?;

        assert!(create_temporary(&directory.path().join("missing-parent")).is_err());

        for attempt in 1..100_u8 {
            fs::write(
                directory
                    .path()
                    .join(format!(".source-plan-{pid}-{attempt}.tmp")),
                b"occupied",
            )
            .expect("temporary exhaustion fixture should be writable");
        }
        assert!(
            create_temporary(directory.path())
                .unwrap_err()
                .to_string()
                .contains("could not allocate")
        );
        Ok(())
    }
}
