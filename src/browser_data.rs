use anyhow::{Context, Result, ensure};
#[cfg(test)]
use polars::prelude::*;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;

use crate::{artifact_receipt, licensing};

pub const INDEX_FILENAME: &str = "browser-data-index.json";
pub const INDEX_SCHEMA_VERSION: u32 = 1;
pub const CACHE_SCHEMA_VERSION: u32 = 2;

pub const BROWSER_METRICS: [(&str, &str); 9] = [
    ("business_funnel", "cohort_year"),
    ("gdp", "year_month"),
    ("gdp_activity_tiers", "period_start"),
    ("gdp_user_type_share", "year_month"),
    ("inequality", "year_month"),
    ("labor_churn", "period"),
    ("labor_cohorts", "year"),
    ("labor_monthly", "year_month"),
    ("patrol", "year_month"),
];

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BrowserDataEntry {
    pub metric: String,
    pub wiki: String,
    pub minimum_date: String,
    pub maximum_date: String,
    pub file: String,
    pub rows: u64,
    pub bytes: u64,
    pub sha256: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BrowserDataIndex {
    pub schema_version: u32,
    pub cache_schema_version: u32,
    pub generation: String,
    pub license_spdx: String,
    pub entries: Vec<BrowserDataEntry>,
}

#[derive(Serialize)]
struct IndexSeed<'a> {
    schema_version: u32,
    cache_schema_version: u32,
    entries: &'a [BrowserDataEntry],
}

fn published_wikis(output_dir: &Path, allowlist: Option<&BTreeSet<String>>) -> Result<Vec<String>> {
    let mut wikis = Vec::new();
    for entry in fs::read_dir(output_dir)? {
        let entry = entry?;
        // Publication selects immutable candidates with per-wiki symlinks.
        // `Path::is_dir` follows those links while still rejecting files.
        if !entry.path().is_dir() {
            continue;
        }
        let wiki = entry.file_name().to_string_lossy().into_owned();
        if wiki.starts_with('_') || allowlist.is_some_and(|allowed| !allowed.contains(&wiki)) {
            continue;
        }
        if BROWSER_METRICS
            .iter()
            .any(|(metric, _)| entry.path().join(format!("{metric}.parquet")).is_file())
        {
            wikis.push(wiki);
        }
    }
    wikis.sort();
    Ok(wikis)
}

fn build_index(
    output_dir: &Path,
    allowlist: Option<&BTreeSet<String>>,
) -> Result<BrowserDataIndex> {
    let wikis = published_wikis(output_dir, allowlist)?;
    ensure!(
        !wikis.is_empty(),
        "no published wiki outputs found for browser data"
    );
    let mut entries = Vec::new();
    for wiki in wikis {
        for (metric, date_column) in BROWSER_METRICS {
            let source = output_dir.join(&wiki).join(format!("{metric}.parquet"));
            if !source.is_file() {
                continue;
            }
            let identity = format!("{wiki}/{metric}.parquet");
            let document = if artifact_receipt::sidecar_path(&source)?.is_file() {
                artifact_receipt::read(&source)?
            } else {
                artifact_receipt::scan_and_write(
                    &source,
                    &identity,
                    "legacy-browser-index-migration-v1",
                    "legacy-unreceipted-input",
                )
                .or_else(|_| {
                    artifact_receipt::scan_and_write_with_spec(
                        &source,
                        &identity,
                        "legacy-browser-index-migration-v1",
                        "legacy-unreceipted-input",
                        artifact_receipt::SemanticSpec {
                            date_column: Some(date_column.to_string()),
                            conservation_columns: Vec::new(),
                            ordering_contract: "wiki-major/v1".to_string(),
                            page_week_consistency: false,
                        },
                    )
                })?
            };
            let receipt = artifact_receipt::verify(
                &source,
                &document.receipt.identity,
                Some(&document.receipt_sha256),
                artifact_receipt::VerificationMode::Fast,
            )?
            .receipt;
            ensure!(
                receipt.rows > 0 && receipt.minimum_wiki == wiki && receipt.maximum_wiki == wiki,
                "browser partition receipt is empty or contains another wiki"
            );
            let minimum_date = receipt
                .minimum_date
                .context("browser partition date minimum is null")?;
            let maximum_date = receipt
                .maximum_date
                .context("browser partition date maximum is null")?;
            ensure!(
                receipt
                    .parquet_schema
                    .iter()
                    .any(|field| field.name == date_column),
                "browser partition receipt is missing {date_column}"
            );
            entries.push(BrowserDataEntry {
                metric: metric.to_string(),
                wiki: wiki.clone(),
                minimum_date,
                maximum_date,
                file: format!("browser-data/{metric}/{wiki}.parquet"),
                rows: receipt.rows,
                bytes: receipt.bytes,
                sha256: receipt.artifact_sha256,
            });
        }
    }
    entries.sort_by(|left, right| {
        (&left.metric, &left.wiki, &left.minimum_date, &left.file).cmp(&(
            &right.metric,
            &right.wiki,
            &right.minimum_date,
            &right.file,
        ))
    });
    for (metric, _) in BROWSER_METRICS {
        ensure!(
            entries.iter().any(|entry| entry.metric == metric),
            "browser data index has no partition for metric {metric}"
        );
    }
    let seed = IndexSeed {
        schema_version: INDEX_SCHEMA_VERSION,
        cache_schema_version: CACHE_SCHEMA_VERSION,
        entries: &entries,
    };
    let generation = hex::encode(Sha256::digest(serde_json::to_vec(&seed)?));
    Ok(BrowserDataIndex {
        schema_version: INDEX_SCHEMA_VERSION,
        cache_schema_version: CACHE_SCHEMA_VERSION,
        generation,
        license_spdx: licensing::ARTIFACT_LICENSE_SPDX.to_string(),
        entries,
    })
}

pub fn materialize(output_dir: &Path, allowlist: Option<&BTreeSet<String>>) -> Result<()> {
    let index = build_index(output_dir, allowlist)?;
    let destination = output_dir.join(INDEX_FILENAME);
    let temporary = output_dir.join(format!(".{INDEX_FILENAME}.{}.tmp", std::process::id()));
    let result = (|| -> Result<()> {
        let mut file = File::create(&temporary)?;
        serde_json::to_writer_pretty(&mut file, &index)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        fs::rename(&temporary, &destination)?;
        File::open(output_dir)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result.with_context(|| format!("failed to publish {}", destination.display()))
}

pub fn read_index(path: &Path) -> Result<BrowserDataIndex> {
    let index: BrowserDataIndex = serde_json::from_slice(&fs::read(path)?)?;
    ensure!(
        index.schema_version == INDEX_SCHEMA_VERSION
            && index.cache_schema_version == CACHE_SCHEMA_VERSION
            && index.license_spdx == licensing::ARTIFACT_LICENSE_SPDX
            && !index.entries.is_empty(),
        "invalid browser data index"
    );
    let seed = IndexSeed {
        schema_version: index.schema_version,
        cache_schema_version: index.cache_schema_version,
        entries: &index.entries,
    };
    ensure!(
        index.generation == hex::encode(Sha256::digest(serde_json::to_vec(&seed)?)),
        "browser data index generation does not match its entries"
    );
    Ok(index)
}

pub fn validate(output_dir: &Path, allowlist: Option<&BTreeSet<String>>) -> Result<()> {
    let recorded = read_index(&output_dir.join(INDEX_FILENAME))?;
    let expected = build_index(output_dir, allowlist)?;
    ensure!(
        recorded == expected,
        "browser data index does not match current per-wiki outputs"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::TestDir;

    fn write_metric(root: &Path, wiki: &str, metric: &str, date_column: &str) -> Result<()> {
        let directory = root.join(wiki);
        fs::create_dir_all(&directory)?;
        let mut frame = df!(
            "wiki" => &[wiki, wiki],
            date_column => &["2025-12", "2026-01"],
            "value" => &[1_i64, 2],
        )
        .expect("browser metric fixture columns have equal lengths");
        let mut file = File::create(directory.join(format!("{metric}.parquet")))?;
        ParquetWriter::new(&mut file)
            .set_parallel(false)
            .finish(&mut frame)?;
        Ok(())
    }

    fn write_complete_wiki(root: &Path, wiki: &str) -> Result<()> {
        for (metric, date_column) in BROWSER_METRICS {
            write_metric(root, wiki, metric, date_column)?;
        }
        Ok(())
    }

    #[test]
    fn index_is_deterministic_partitioned_and_excludes_weekly_data() -> Result<()> {
        let first = TestDir::new()?;
        let second = TestDir::new()?;
        for root in [first.path(), second.path()] {
            write_complete_wiki(root, "nlwiki")?;
            write_complete_wiki(root, "ptwiki")?;
            write_metric(root, "nlwiki", "page_weekly_edits", "week_start")?;
            materialize(root, None)?;
        }
        assert_eq!(
            fs::read(first.path().join(INDEX_FILENAME))?,
            fs::read(second.path().join(INDEX_FILENAME))?
        );
        let index = read_index(&first.path().join(INDEX_FILENAME))?;
        assert_eq!(index.entries.len(), BROWSER_METRICS.len() * 2);
        assert!(
            index
                .entries
                .iter()
                .all(|entry| entry.wiki == "nlwiki" || entry.wiki == "ptwiki")
        );
        assert!(
            index
                .entries
                .iter()
                .all(|entry| entry.metric != "page_weekly_edits")
        );
        assert!(
            index
                .entries
                .iter()
                .all(|entry| entry.file.contains(&entry.wiki))
        );
        Ok(())
    }

    #[test]
    fn index_respects_allowlist_and_rejects_mixed_wiki_partitions() -> Result<()> {
        let output = TestDir::new()?;
        write_complete_wiki(output.path(), "nlwiki")?;
        write_complete_wiki(output.path(), "ptwiki")?;
        materialize(output.path(), Some(&BTreeSet::from(["ptwiki".to_string()])))?;
        let index = read_index(&output.path().join(INDEX_FILENAME))?;
        assert!(index.entries.iter().all(|entry| entry.wiki == "ptwiki"));

        write_metric(output.path(), "nlwiki", "gdp", "year_month")?;
        let path = output.path().join("nlwiki/gdp.parquet");
        let mut mixed = df!(
            "wiki" => &["nlwiki", "ptwiki"],
            "year_month" => &["2025-12", "2026-01"],
            "value" => &[1_i64, 2],
        )
        .expect("mixed-wiki fixture columns have equal lengths");
        let mut file = File::create(&path)?;
        ParquetWriter::new(&mut file)
            .set_parallel(false)
            .finish(&mut mixed)?;
        assert!(materialize(output.path(), Some(&BTreeSet::from(["nlwiki".to_string()]))).is_err());

        let mut null_date = df!(
            "wiki" => &["nlwiki"],
            "year_month" => &[None::<&str>],
            "value" => &[1_i64],
        )
        .expect("null-date fixture columns have equal lengths");
        ParquetWriter::new(File::create(&path)?)
            .set_parallel(false)
            .finish(&mut null_date)?;
        assert!(materialize(output.path(), Some(&BTreeSet::from(["nlwiki".to_string()]))).is_err());

        let mut null_wiki = df!(
            "wiki" => &[None::<&str>],
            "year_month" => &["2026-01"],
            "value" => &[1_i64],
        )
        .expect("null-wiki fixture columns have equal lengths");
        ParquetWriter::new(File::create(&path)?)
            .set_parallel(false)
            .finish(&mut null_wiki)?;
        assert!(materialize(output.path(), Some(&BTreeSet::from(["nlwiki".to_string()]))).is_err());
        Ok(())
    }

    #[test]
    fn index_allows_metric_specific_wiki_coverage() -> Result<()> {
        let output = TestDir::new()?;
        write_complete_wiki(output.path(), "nlwiki")?;
        write_complete_wiki(output.path(), "svwiki")?;
        fs::remove_file(output.path().join("svwiki/patrol.parquet"))?;
        materialize(output.path(), None)?;
        let index = read_index(&output.path().join(INDEX_FILENAME))?;
        assert!(
            index
                .entries
                .iter()
                .any(|entry| entry.metric == "patrol" && entry.wiki == "nlwiki")
        );
        assert!(
            !index
                .entries
                .iter()
                .any(|entry| entry.metric == "patrol" && entry.wiki == "svwiki")
        );
        assert!(
            index
                .entries
                .iter()
                .any(|entry| entry.metric == "gdp" && entry.wiki == "svwiki")
        );
        Ok(())
    }

    #[test]
    fn empty_partition_and_interrupted_index_publication_fail_closed() -> Result<()> {
        let empty = TestDir::new()?;
        write_complete_wiki(empty.path(), "nlwiki")?;
        let mut empty_frame = df!(
            "wiki" => Vec::<String>::new(),
            "year_month" => Vec::<String>::new(),
            "value" => Vec::<i64>::new(),
        )
        .expect("empty browser fixture has a valid schema");
        ParquetWriter::new(File::create(empty.path().join("nlwiki/gdp.parquet"))?)
            .finish(&mut empty_frame)?;
        assert!(materialize(empty.path(), None).is_err());

        let interrupted = TestDir::new()?;
        write_complete_wiki(interrupted.path(), "nlwiki")?;
        fs::create_dir(interrupted.path().join(INDEX_FILENAME))?;
        assert!(materialize(interrupted.path(), None).is_err());
        assert!(
            !interrupted
                .path()
                .join(format!(".{INDEX_FILENAME}.{}.tmp", std::process::id()))
                .exists()
        );
        Ok(())
    }
}
