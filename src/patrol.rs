use anyhow::{Context, Result};
use chrono::NaiveDateTime;
use flate2::read::MultiGzDecoder;
use polars::prelude::*;
use quick_xml::Reader;
use quick_xml::events::Event;
use regex::Regex;
use reqwest::StatusCode;
use reqwest::blocking::Client;
use reqwest::header::{CONTENT_LENGTH, CONTENT_RANGE, ETAG, HeaderMap, LAST_MODIFIED, RANGE};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha1::Sha1;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs::{self, File};
use std::io::{BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::time::Duration;
use tracing::{info, warn};

use crate::{compute, fingerprint, storage};

#[cfg_attr(coverage, allow(dead_code))]
const USER_AGENT: &str = "wiki-econ/0.1 (Wikipedia economic analysis research tool)";
const PATROL_DUMP_BASE: &str = "https://dumps.wikimedia.org";
const PARQUET_BATCH_ROWS: usize = 50_000;
const SUBSTANTIAL_LOGGING_DUMP_BYTES: u64 = 1024 * 1024;
const SUBSTANTIAL_LOG_ITEMS: usize = 10_000;
const PATROL_COMPUTE_ALGORITHM_VERSION: &str = "patrol-metrics-v5-complete-snapshot-months";
const PATROL_PARSER_VERSION: &str = "patrol-logging-pinned-plan-v3-external-sort";
const REVISION_COLUMNS: &[&str] = &[
    "revision_id",
    "event_timestamp",
    "event_user_id",
    "event_user_text",
    "page_namespace",
    "event_user_is_bot_by",
    "event_user_is_anonymous",
    "event_user_is_temporary",
];
const PATROL_COLUMNS: &[&str] = &[
    "timestamp",
    "current_revision_id",
    "prev_revision_id",
    "user",
];
type AutopatrolIntervals = HashMap<String, Vec<(i64, Option<i64>)>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
enum UserType {
    Registered,
    Anonymous,
    Temporary,
    Bot,
}

impl UserType {
    fn as_str(self) -> &'static str {
        match self {
            Self::Registered => "registered",
            Self::Anonymous => "anonymous",
            Self::Temporary => "temporary",
            Self::Bot => "bot",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
struct MetricKey {
    year_month_key: i32,
    page_namespace: i32,
    user_type: UserType,
}

#[derive(Clone, Copy, Debug)]
struct RevisionMeta {
    timestamp_seconds: i64,
    year_month_key: i32,
    page_namespace: i32,
    user_type: UserType,
}

#[derive(Default)]
struct PatrolAccumulator {
    total_patrols: u64,
    patrol_new_pages: u64,
    patrol_diffs: u64,
    user_counts: HashMap<String, u32>,
    latencies_hours: Vec<f64>,
}

#[derive(Default)]
struct RevisionSummary {
    total_revisions: HashMap<MetricKey, u64>,
    patrolled_revisions: HashMap<MetricKey, u64>,
    autopatrolled_revisions: HashMap<MetricKey, u64>,
    patrolled_lookup: HashMap<i64, RevisionMeta>,
}

#[derive(Default)]
struct PatrolBatch {
    log_id: Vec<i64>,
    timestamp: Vec<String>,
    user: Vec<Option<String>>,
    user_id: Vec<Option<i64>>,
    page_title: Vec<Option<String>>,
    current_revision_id: Vec<i64>,
    prev_revision_id: Vec<i64>,
    is_auto: Vec<bool>,
}

#[derive(Default)]
struct RightsBatch {
    timestamp: Vec<String>,
    target_user: Vec<String>,
    old_groups: Vec<String>,
    new_groups: Vec<String>,
}

impl PatrolBatch {
    fn take_columns(&mut self) -> Vec<Column> {
        vec![
            Column::new("log_id".into(), std::mem::take(&mut self.log_id)),
            Column::new("timestamp".into(), std::mem::take(&mut self.timestamp)),
            Column::new("user".into(), std::mem::take(&mut self.user)),
            Column::new("user_id".into(), std::mem::take(&mut self.user_id)),
            Column::new("page_title".into(), std::mem::take(&mut self.page_title)),
            Column::new(
                "current_revision_id".into(),
                std::mem::take(&mut self.current_revision_id),
            ),
            Column::new(
                "prev_revision_id".into(),
                std::mem::take(&mut self.prev_revision_id),
            ),
            Column::new("is_auto".into(), std::mem::take(&mut self.is_auto)),
        ]
    }
}

impl RightsBatch {
    fn take_columns(&mut self) -> Vec<Column> {
        vec![
            Column::new("timestamp".into(), std::mem::take(&mut self.timestamp)),
            Column::new("target_user".into(), std::mem::take(&mut self.target_user)),
            Column::new("old_groups".into(), std::mem::take(&mut self.old_groups)),
            Column::new("new_groups".into(), std::mem::take(&mut self.new_groups)),
        ]
    }
}

#[derive(Default)]
struct LogItem {
    log_type: Option<String>,
    log_action: Option<String>,
    log_id: Option<i64>,
    timestamp: Option<String>,
    contributor_name: Option<String>,
    contributor_id: Option<i64>,
    log_title: Option<String>,
    params: Option<String>,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct LoggingParseStats {
    total_log_items: usize,
    patrol_events: usize,
    rights_events: usize,
    skipped_events: usize,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct LoggingSourceIdentity {
    source_id: String,
    remote_url: String,
    content_length: u64,
    etag: Option<String>,
    last_modified: Option<String>,
    downloaded_sha256: String,
    upstream_md5: String,
    upstream_sha1: String,
    downloaded_sha1: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PatrolSourceSummary {
    pub(crate) history_snapshot: String,
    pub(crate) logging_dump_date: String,
    pub(crate) coverage_through: String,
    pub(crate) source_plan_sha256: String,
    pub(crate) source_count: u64,
    pub(crate) remote_url: String,
    pub(crate) content_length: u64,
    pub(crate) etag: Option<String>,
    pub(crate) last_modified: Option<String>,
    pub(crate) downloaded_sha256: String,
    pub(crate) parser_version: String,
    pub(crate) total_log_items: u64,
    pub(crate) patrol_events: u64,
    pub(crate) rights_events: u64,
    pub(crate) skipped_events: u64,
    pub(crate) manifest_sha256: String,
}

pub(crate) fn source_generation_summary(
    data_dir: &Path,
    wiki: &str,
    snapshot: &str,
) -> Result<Option<PatrolSourceSummary>> {
    if !generation::exists(data_dir, wiki, snapshot) {
        return Ok(None);
    }
    let source = generation::load(data_dir, wiki, snapshot)?;
    let content_length = source.sources.iter().try_fold(0_u64, |total, item| {
        total
            .checked_add(item.content_length)
            .context("patrol source byte total overflow")
    })?;
    Ok(Some(PatrolSourceSummary {
        history_snapshot: source.plan.history_snapshot.clone(),
        logging_dump_date: source.plan.logging_dump_date.clone(),
        coverage_through: source.plan.coverage_through.clone(),
        source_plan_sha256: source.plan.plan_sha256.clone(),
        source_count: u64::try_from(source.sources.len())?,
        remote_url: source.plan.dump_status_url.to_string(),
        content_length,
        etag: (source.sources.len() == 1)
            .then(|| source.sources[0].etag.clone())
            .flatten(),
        last_modified: (source.sources.len() == 1)
            .then(|| source.sources[0].last_modified.clone())
            .flatten(),
        downloaded_sha256: generation::source_set_digest(&source.sources),
        parser_version: source.parser_version,
        total_log_items: u64::try_from(source.stats.total_log_items)?,
        patrol_events: u64::try_from(source.stats.patrol_events)?,
        rights_events: u64::try_from(source.stats.rights_events)?,
        skipped_events: u64::try_from(source.stats.skipped_events)?,
        manifest_sha256: source.manifest_sha256,
    }))
}

struct PatrolWriter {
    writer: polars::io::parquet::write::BatchedWriter<File>,
    batch: PatrolBatch,
    batch_rows: usize,
}

struct RightsWriter {
    writer: polars::io::parquet::write::BatchedWriter<File>,
    batch: RightsBatch,
    batch_rows: usize,
}

trait PatrolSink {
    fn add_patrol(&mut self, row: PatrolRow) -> Result<()>;
}

trait RightsSink {
    fn add_rights(&mut self, row: RightsRow) -> Result<()>;
}

#[cfg_attr(coverage, allow(dead_code))]
struct ReqwestPatrolTransport {
    dump_client: Client,
    api_client: Client,
}

pub(crate) struct PatrolTransportResponse {
    body: Box<dyn Read + Send>,
    content_length: Option<u64>,
    etag: Option<String>,
    last_modified: Option<String>,
}

impl PatrolTransportResponse {
    #[cfg(test)]
    pub(crate) fn from_bytes(bytes: impl Into<Vec<u8>>) -> Self {
        Self {
            body: Box::new(std::io::Cursor::new(bytes.into())),
            content_length: None,
            etag: None,
            last_modified: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn from_bytes_with_identity(
        bytes: impl Into<Vec<u8>>,
        content_length: Option<u64>,
        etag: Option<&str>,
        last_modified: Option<&str>,
    ) -> Self {
        Self {
            body: Box::new(std::io::Cursor::new(bytes.into())),
            content_length,
            etag: etag.map(str::to_string),
            last_modified: last_modified.map(str::to_string),
        }
    }
}

pub(crate) trait PatrolTransport: Sync {
    fn get(&self, url: &str, range_start: Option<u64>) -> Result<PatrolTransportResponse>;
    fn get_json(&self, url: &str) -> Result<Value>;
}

#[cfg_attr(coverage, allow(dead_code))]
fn build_transport() -> Result<ReqwestPatrolTransport> {
    Ok(ReqwestPatrolTransport {
        dump_client: Client::builder()
            .user_agent(USER_AGENT)
            .timeout(Duration::from_secs(3600))
            .redirect(crate::fetch::dumps_host_only_redirect_policy())
            .build()?,
        api_client: Client::builder()
            .user_agent(USER_AGENT)
            .timeout(Duration::from_secs(30))
            .build()?,
    })
}

impl PatrolTransport for ReqwestPatrolTransport {
    fn get(&self, url: &str, range_start: Option<u64>) -> Result<PatrolTransportResponse> {
        let mut request = self.dump_client.get(url);
        if let Some(range_start) = range_start {
            request = request.header(RANGE, format!("bytes={range_start}-"));
        }
        let response = request.send()?;
        if response.status() == StatusCode::RANGE_NOT_SATISFIABLE
            && range_start
                .is_some_and(|start| unsatisfied_range_total(response.headers()) == Some(start))
        {
            return Ok(PatrolTransportResponse {
                body: Box::new(std::io::empty()),
                content_length: range_start,
                etag: header_string(response.headers(), ETAG),
                last_modified: header_string(response.headers(), LAST_MODIFIED),
            });
        }
        let response = response.error_for_status()?;
        let headers = response.headers();
        let content_length = response_total_length(headers, range_start);
        let etag = header_string(headers, ETAG);
        let last_modified = header_string(headers, LAST_MODIFIED);
        Ok(PatrolTransportResponse {
            body: Box::new(response),
            content_length,
            etag,
            last_modified,
        })
    }

    fn get_json(&self, url: &str) -> Result<Value> {
        let response = self.api_client.get(url).send()?.error_for_status()?;
        response.json().map_err(Into::into)
    }
}

fn header_string(headers: &HeaderMap, name: reqwest::header::HeaderName) -> Option<String> {
    headers.get(name)?.to_str().ok().map(str::to_string)
}

fn response_total_length(headers: &HeaderMap, range_start: Option<u64>) -> Option<u64> {
    if let Some(total) = headers
        .get(CONTENT_RANGE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.rsplit_once('/'))
        .and_then(|(_, total)| total.parse().ok())
    {
        return Some(total);
    }
    let length = headers.get(CONTENT_LENGTH)?.to_str().ok()?.parse().ok()?;
    range_start.unwrap_or_default().checked_add(length)
}

fn unsatisfied_range_total(headers: &HeaderMap) -> Option<u64> {
    headers
        .get(CONTENT_RANGE)?
        .to_str()
        .ok()?
        .strip_prefix("bytes */")?
        .parse()
        .ok()
}

#[cfg(any(test, coverage))]
thread_local! {
    static TEST_TRANSPORT: std::cell::RefCell<Option<std::sync::Arc<dyn PatrolTransport>>> =
        std::cell::RefCell::new(None);
}

#[cfg(any(test, coverage))]
#[cfg_attr(coverage, allow(dead_code))]
static TEST_TRANSPORT_LOCK: OnceLock<std::sync::Mutex<()>> = OnceLock::new();

#[cfg(any(test, coverage))]
#[cfg_attr(coverage, allow(dead_code))]
pub(crate) struct TestTransportGuard {
    _lock: std::sync::MutexGuard<'static, ()>,
}

#[cfg(any(test, coverage))]
impl Drop for TestTransportGuard {
    fn drop(&mut self) {
        TEST_TRANSPORT.with(|cell| {
            cell.borrow_mut().take();
        });
    }
}

#[cfg(any(test, coverage))]
#[cfg_attr(coverage, allow(dead_code))]
pub(crate) fn install_test_transport(
    transport: std::sync::Arc<dyn PatrolTransport>,
) -> TestTransportGuard {
    let lock = TEST_TRANSPORT_LOCK
        .get_or_init(|| std::sync::Mutex::new(()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    TEST_TRANSPORT.with(|cell| {
        *cell.borrow_mut() = Some(transport);
    });
    TestTransportGuard { _lock: lock }
}

#[cfg(any(test, coverage))]
fn configured_test_transport() -> Option<std::sync::Arc<dyn PatrolTransport>> {
    TEST_TRANSPORT.with(|cell| cell.borrow().as_ref().cloned())
}

pub fn fetch_patrol(wiki: &str, data_dir: &Path) -> Result<()> {
    if let Some(snapshot) = storage::current_snapshot_version(data_dir, wiki)? {
        return fetch_patrol_for_snapshot(wiki, &snapshot, data_dir);
    }
    anyhow::bail!(
        "patrol fetch requires a selected history snapshot for {wiki}; prepare or select a generation first"
    )
}

pub(crate) fn fetch_patrol_for_snapshot(wiki: &str, snapshot: &str, data_dir: &Path) -> Result<()> {
    storage::validate_snapshot_version(snapshot)?;
    #[cfg(any(test, coverage))]
    if let Some(transport) = configured_test_transport() {
        generation::fetch(transport.as_ref(), wiki, snapshot, data_dir)?;
        return Ok(());
    }
    #[cfg(not(coverage))]
    {
        let transport = build_transport()?;
        generation::fetch(&transport, wiki, snapshot, data_dir)?;
        Ok(())
    }
    #[cfg(coverage)]
    anyhow::bail!("install_test_transport must be used before patrol generation fetch")
}

/// Build the publication-invisible account-creation experiment from one
/// snapshot-pinned logging generation and the matching public revision history.
/// Permanent local accounts are keyed by their MediaWiki user ID; temporary
/// accounts are reported separately and never mixed into the permanent cohort.
pub(crate) fn build_account_creation_staging_report(
    wiki: &str,
    snapshot: &str,
    data_dir: &Path,
    destination: &Path,
) -> Result<()> {
    storage::validate_snapshot_version(snapshot)?;
    #[cfg(any(test, coverage))]
    if let Some(transport) = configured_test_transport() {
        return build_account_creation_staging_report_with_transport(
            transport.as_ref(),
            wiki,
            snapshot,
            data_dir,
            destination,
        );
    }
    #[cfg(not(coverage))]
    {
        let transport = build_transport()?;
        build_account_creation_staging_report_with_transport(
            &transport,
            wiki,
            snapshot,
            data_dir,
            destination,
        )
    }
    #[cfg(coverage)]
    anyhow::bail!("install_test_transport must be used before account-creation extraction")
}

fn build_account_creation_staging_report_with_transport<T: PatrolTransport + ?Sized>(
    transport: &T,
    wiki: &str,
    snapshot: &str,
    data_dir: &Path,
    destination: &Path,
) -> Result<()> {
    let source_plan = plan::PatrolSourcePlan::load_or_resolve(transport, wiki, snapshot, data_dir)?;
    anyhow::ensure!(
        source_plan.coverage_through == snapshot,
        "account-creation logging coverage does not match history snapshot"
    );
    let staging_parent = data_dir
        .join("staging")
        .join("account-creations")
        .join(wiki);
    fs::create_dir_all(&staging_parent)?;
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_nanos();
    let staging = staging_parent.join(format!(".{snapshot}.{}.{}.tmp", std::process::id(), nonce));
    fs::create_dir(&staging)?;
    let mut accounts = HashMap::<i64, (String, bool)>::new();
    let mut temporary_by_month = BTreeMap::<String, u32>::new();
    let mut parse_stats = AccountCreationParseStats::default();
    let extraction = (|| {
        for (index, spec) in source_plan.sources.iter().enumerate() {
            let path = staging.join(format!("source-{index:04}.xml.gz"));
            let source = download_logging_source(transport, wiki, spec, &path)?;
            parse_stats.compressed_bytes = parse_stats
                .compressed_bytes
                .checked_add(source.content_length)
                .context("account-creation source byte count overflow")?;
            let source_stats = parse_account_creation_events(
                &path,
                snapshot,
                &mut accounts,
                &mut temporary_by_month,
            )?;
            parse_stats.add(source_stats)?;
            fs::remove_file(&path).context("failed to release parsed account logging source")?;
        }
        Ok::<(), anyhow::Error>(())
    })();
    if let Err(error) = extraction {
        let _ = fs::remove_dir_all(&staging);
        return Err(error);
    }
    fs::remove_dir(&staging)
        .context("failed to remove empty account-creation staging directory")?;
    anyhow::ensure!(
        parse_stats.unresolved_permanent == 0,
        "{wiki}/{snapshot} has {} permanent account-creation events without a stable target user ID",
        parse_stats.unresolved_permanent
    );
    anyhow::ensure!(
        !accounts.is_empty(),
        "{wiki}/{snapshot} has no permanent account creations in logging coverage"
    );

    let history_scan = mark_accounts_with_revisions(wiki, snapshot, data_dir, &mut accounts)?;

    let mut monthly = BTreeMap::<String, (u32, u32)>::new();
    for (month, edited) in accounts.into_values() {
        let counts = monthly.entry(month).or_default();
        counts.0 = counts.0.checked_add(1).context("account count overflow")?;
        if edited {
            counts.1 = counts
                .1
                .checked_add(1)
                .context("edited account count overflow")?;
        }
    }
    for month in temporary_by_month.keys() {
        monthly.entry(month.clone()).or_default();
    }
    let rows = monthly
        .into_iter()
        .map(|(year_month, (accounts_created, accounts_with_edits))| {
            Ok(AccountCreationMonth {
                temporary_accounts_excluded: temporary_by_month
                    .remove(&year_month)
                    .unwrap_or_default(),
                accounts_without_edits: accounts_created
                    .checked_sub(accounts_with_edits)
                    .context("account-creation conservation failure")?,
                year_month,
                accounts_created,
                accounts_with_edits,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    anyhow::ensure!(
        rows.iter().all(|row| {
            row.accounts_created == row.accounts_with_edits + row.accounts_without_edits
        }),
        "account-creation split does not conserve its cohort total"
    );
    let report = AccountCreationStagingReport {
        schema_version: 1,
        metric_version: ACCOUNT_CREATION_METRIC_VERSION,
        license_spdx: "MIT",
        attribution: "Derived from Wikimedia Foundation MediaWiki History and public logging dumps; Wikimedia projects and marks are not affiliated with this site.",
        wiki: wiki.to_string(),
        snapshot: snapshot.to_string(),
        logging_dump_date: source_plan.logging_dump_date,
        source_plan_sha256: source_plan.plan_sha256,
        compressed_source_bytes: parse_stats.compressed_bytes,
        total_log_items: parse_stats.total_log_items,
        account_creation_events: parse_stats.account_creation_events,
        permanent_accounts: rows.iter().map(|row| u64::from(row.accounts_created)).sum(),
        temporary_accounts: parse_stats.temporary_accounts,
        history_scan_mode: history_scan.mode,
        history_sources: history_scan.sources,
        history_source_bytes: history_scan.bytes,
        history_revision_rows: history_scan.revision_rows,
        definition: "Permanent local accounts created in each month, split by whether the selected public history snapshot contains at least one revision attributed to that local user ID; temporary accounts are excluded.",
        rows,
    };
    generation::atomic_json(destination, &report)?;
    info!(
        wiki,
        snapshot,
        months = report.rows.len(),
        path = %destination.display(),
        "wrote account-creation staging report"
    );
    Ok(())
}

#[derive(Clone, Copy, Debug, Default)]
struct AccountCreationParseStats {
    compressed_bytes: u64,
    total_log_items: u64,
    account_creation_events: u64,
    permanent_creation_events: u64,
    temporary_accounts: u64,
    unresolved_permanent: u64,
}

#[derive(Debug)]
struct AccountCreationHistoryScan {
    mode: &'static str,
    sources: u32,
    bytes: u64,
    revision_rows: u64,
}

fn mark_accounts_with_revisions(
    wiki: &str,
    snapshot: &str,
    data_dir: &Path,
    accounts: &mut HashMap<i64, (String, bool)>,
) -> Result<AccountCreationHistoryScan> {
    let retained = storage::snapshot_compute_layer(
        data_dir,
        wiki,
        snapshot,
        storage::GenerationLayer::Warehouse,
    )
    .and_then(|layer| storage::snapshot_fragment_files(data_dir, wiki, snapshot, layer));
    if let Ok(files) = retained
        && !files.is_empty()
    {
        let mut bytes = 0_u64;
        let mut revision_rows = 0_u64;
        for path in &files {
            bytes = bytes
                .checked_add(fs::metadata(path)?.len())
                .context("retained history byte count overflow")?;
            let columns = projection(&["event_user_id"]);
            let mut reader =
                storage::SequentialParquetReader::new(path, Some(columns), PARQUET_BATCH_ROWS)
                    .with_context(|| format!("cannot scan retained history {}", path.display()))?;
            while let Some(batch) = reader.next_batch()? {
                revision_rows = revision_rows
                    .checked_add(u64::try_from(batch.height())?)
                    .context("retained revision row count overflow")?;
                for user_id in batch.column("event_user_id")?.i64()?.iter().flatten() {
                    if let Some((_, edited)) = accounts.get_mut(&user_id) {
                        *edited = true;
                    }
                }
            }
        }
        return Ok(AccountCreationHistoryScan {
            mode: "retained_metric_input",
            sources: u32::try_from(files.len())?,
            bytes,
            revision_rows,
        });
    }

    scan_account_history_source_plan(wiki, snapshot, data_dir, accounts)
}

#[cfg(not(coverage))]
fn scan_account_history_source_plan(
    wiki: &str,
    snapshot: &str,
    data_dir: &Path,
    accounts: &mut HashMap<i64, (String, bool)>,
) -> Result<AccountCreationHistoryScan> {
    let (plan, _) = crate::snapshot_plan::SnapshotPlan::load_or_resolve(data_dir, wiki, snapshot)?;
    let run_id = format!("account-creations-{wiki}-{snapshot}-{}", std::process::id());
    scan_account_history_source_plan_with(&plan, accounts, |source| {
        crate::fetch::fetch_snapshot_source_window(
            wiki,
            snapshot,
            data_dir,
            &run_id,
            std::slice::from_ref(source),
        )?
        .into_iter()
        .next()
        .context("history source window returned no source")
    })
}

#[cfg(coverage)]
fn scan_account_history_source_plan(
    _wiki: &str,
    _snapshot: &str,
    _data_dir: &Path,
    _accounts: &mut HashMap<i64, (String, bool)>,
) -> Result<AccountCreationHistoryScan> {
    anyhow::bail!("bounded history source scan is unavailable without a test source loader")
}

fn scan_account_history_source_plan_with<F>(
    plan: &crate::snapshot_plan::SnapshotPlan,
    accounts: &mut HashMap<i64, (String, bool)>,
    mut load_source: F,
) -> Result<AccountCreationHistoryScan>
where
    F: FnMut(&crate::snapshot_plan::SourceSpec) -> Result<PathBuf>,
{
    let mut bytes = 0_u64;
    let mut revision_rows = 0_u64;
    for source in &plan.sources {
        let path = load_source(source)?;
        bytes = bytes
            .checked_add(fs::metadata(&path)?.len())
            .context("history source byte count overflow")?;
        revision_rows = revision_rows
            .checked_add(crate::ingest::scan_revision_user_ids(&path, |user_id| {
                if let Some((_, edited)) = accounts.get_mut(&user_id) {
                    *edited = true;
                }
            })?)
            .context("history revision row count overflow")?;
        fs::remove_file(&path).context("failed to release scanned history source")?;
        let parent = path
            .parent()
            .context("history source has no parent directory")?;
        let directory = File::open(parent)?;
        directory.sync_all()?;
    }
    Ok(AccountCreationHistoryScan {
        mode: "bounded_source_window",
        sources: u32::try_from(plan.sources.len())?,
        bytes,
        revision_rows,
    })
}

impl AccountCreationParseStats {
    fn add(&mut self, other: Self) -> Result<()> {
        for (target, value, label) in [
            (&mut self.total_log_items, other.total_log_items, "log item"),
            (
                &mut self.account_creation_events,
                other.account_creation_events,
                "account creation event",
            ),
            (
                &mut self.permanent_creation_events,
                other.permanent_creation_events,
                "permanent account creation event",
            ),
            (
                &mut self.temporary_accounts,
                other.temporary_accounts,
                "temporary account",
            ),
            (
                &mut self.unresolved_permanent,
                other.unresolved_permanent,
                "unresolved permanent account",
            ),
        ] {
            *target = target
                .checked_add(value)
                .with_context(|| format!("{label} count overflow"))?;
        }
        Ok(())
    }
}

pub(crate) fn preflight_patrol_for_snapshot(
    wiki: &str,
    snapshot: &str,
    data_dir: &Path,
) -> Result<()> {
    storage::validate_snapshot_version(snapshot)?;
    #[cfg(any(test, coverage))]
    if let Some(transport) = configured_test_transport() {
        generation::preflight(transport.as_ref(), wiki, snapshot, data_dir)?;
        return Ok(());
    }
    #[cfg(not(coverage))]
    {
        let transport = build_transport()?;
        generation::preflight(&transport, wiki, snapshot, data_dir)
    }
    #[cfg(coverage)]
    anyhow::bail!("install_test_transport must be used before patrol dependency preflight")
}

pub(crate) fn is_upstream_waiting(error: &anyhow::Error) -> bool {
    plan::is_upstream_waiting(error)
}

#[cfg(test)]
pub(crate) fn upstream_waiting_error_for_test() -> anyhow::Error {
    plan::UpstreamWaiting {
        wiki: "testwiki".to_string(),
        history_snapshot: "2026-08".to_string(),
        logging_dump_date: "20260901".to_string(),
        recombined_status: "waiting".to_string(),
        split_status: "waiting".to_string(),
    }
    .into()
}

#[cfg(test)]
fn fetch_patrol_with_transport<T: PatrolTransport + ?Sized>(
    wiki: &str,
    data_dir: &Path,
    transport: &T,
) -> Result<()> {
    let patrol_dir = data_dir.join("patrol").join(wiki);
    fs::create_dir_all(&patrol_dir)?;

    let xml_path = patrol_dir.join(format!("{wiki}-latest-pages-logging.xml.gz"));
    let patrol_path = patrol_dir.join("patrol.parquet");
    let rights_path = patrol_dir.join("rights.parquet");
    let meta_path = patrol_dir.join("autopatrol_groups.json");

    let snapshot = "2026-08";
    let source_plan = plan::PatrolSourcePlan::load_or_resolve(transport, wiki, snapshot, data_dir)?;
    anyhow::ensure!(
        source_plan.sources.len() == 1,
        "legacy test fetch requires one patrol source"
    );
    download_logging_source(transport, wiki, &source_plan.sources[0], &xml_path)?;

    info!(wiki = wiki, path = %xml_path.display(), "querying siteinfo API for autopatrol groups");
    let mut autopatrol_groups = fetch_autopatrol_groups(transport, wiki)?;
    if autopatrol_groups.is_empty() {
        autopatrol_groups = load_cached_autopatrol_groups(&meta_path)?;
    }
    let meta_bytes = serde_json::to_vec_pretty(&serde_json::json!({
        "wiki": wiki,
        "autopatrol_groups": autopatrol_groups,
    }))?;
    fs::write(&meta_path, meta_bytes)?;

    let patrol_temp_path = patrol_path.with_extension("parquet.tmp");
    let rights_temp_path = rights_path.with_extension("parquet.tmp");
    let _ = fs::remove_file(&patrol_temp_path);
    let _ = fs::remove_file(&rights_temp_path);
    let parsed = (|| {
        let mut patrol_writer = PatrolWriter::new(&patrol_temp_path)?;
        let mut rights_writer = RightsWriter::new(&rights_temp_path)?;
        let stats = parse_logging_events(&xml_path, &mut patrol_writer, &mut rights_writer)?;
        info!(
            wiki = wiki,
            total_log_items = stats.total_log_items,
            patrol_events = stats.patrol_events,
            rights_events = stats.rights_events,
            skipped_events = stats.skipped_events,
            "parsed patrol logging XML"
        );
        validate_logging_parse(&xml_path, stats)?;
        patrol_writer.finish()?;
        rights_writer.finish()?;
        Ok::<_, anyhow::Error>(stats)
    })();
    let stats = match parsed {
        Ok(stats) => stats,
        Err(error) => {
            let _ = fs::remove_file(&patrol_temp_path);
            let _ = fs::remove_file(&rights_temp_path);
            return Err(error);
        }
    };
    fs::rename(&patrol_temp_path, &patrol_path)?;
    fs::rename(&rights_temp_path, &rights_path)?;
    File::open(&patrol_path)?.sync_all()?;
    File::open(&rights_path)?.sync_all()?;
    File::open(&patrol_dir)?.sync_all()?;
    fs::remove_file(&xml_path).context("failed to release committed patrol XML source")?;
    File::open(&patrol_dir)?.sync_all()?;

    info!(
        wiki = wiki,
        patrol_events = stats.patrol_events,
        rights_events = stats.rights_events,
        released_source = %xml_path.display(),
        "published patrol parquet outputs"
    );
    Ok(())
}

pub fn compute_patrol(
    wiki: &str,
    data_dir: &Path,
    output_dir: &Path,
    rebuild: bool,
    limit_months: Option<usize>,
) -> Result<()> {
    anyhow::ensure!(
        !output_dir.join("ready.json").exists() && !output_dir.join("qualification.json").exists(),
        "refusing to modify an immutable ready candidate"
    );
    compute_patrol_selected(wiki, data_dir, output_dir, rebuild, limit_months, None)
}

pub(crate) fn compute_patrol_for_snapshot(
    wiki: &str,
    snapshot: &str,
    data_dir: &Path,
    output_dir: &Path,
    rebuild: bool,
    limit_months: Option<usize>,
) -> Result<()> {
    storage::validate_snapshot_version(snapshot)?;
    compute_patrol_selected(
        wiki,
        data_dir,
        output_dir,
        rebuild,
        limit_months,
        Some(snapshot),
    )
}

fn compute_patrol_selected(
    wiki: &str,
    data_dir: &Path,
    output_dir: &Path,
    rebuild: bool,
    limit_months: Option<usize>,
    snapshot: Option<&str>,
) -> Result<()> {
    if let Some(snapshot) = snapshot
        && generation::exists(data_dir, wiki, snapshot)
    {
        return incremental::compute(wiki, snapshot, data_dir, output_dir, rebuild, limit_months);
    }
    let patrol_dir = data_dir.join("patrol").join(wiki);
    let patrol_path = patrol_dir.join("patrol.parquet");
    let rights_path = patrol_dir.join("rights.parquet");
    let meta_path = patrol_dir.join("autopatrol_groups.json");
    let revision_layer = match snapshot {
        Some(snapshot) => {
            let result = storage::snapshot_compute_layer(
                data_dir,
                wiki,
                snapshot,
                storage::GenerationLayer::Warehouse,
            );
            result?
        }
        None => storage::active_compute_layer(data_dir, wiki, storage::GenerationLayer::Warehouse)?,
    };
    let revision_store_dir = match snapshot {
        Some(snapshot) => {
            storage::snapshot_layer_wiki_dir(data_dir, wiki, snapshot, revision_layer)?
        }
        None => storage::active_layer_wiki_dir(data_dir, wiki, revision_layer)?,
    };
    let revision_files = match snapshot {
        Some(snapshot) => {
            let result = storage::snapshot_fragment_files(data_dir, wiki, snapshot, revision_layer);
            result?
        }
        None => storage::active_fragment_files(data_dir, wiki, revision_layer)?,
    };

    if !patrol_path.exists() {
        anyhow::bail!("No patrol data for {wiki}. Run `patrol-fetch` first.");
    }
    if !revision_store_dir.exists() || revision_files.is_empty() {
        anyhow::bail!("No warehouse data for {wiki}. Run `ingest` first.");
    }

    let recordable_run = limit_months.is_none();
    let fingerprinted_run = !rebuild && recordable_run;
    let inputs = patrol_stage_inputs(wiki, data_dir, snapshot)?;
    let outputs = patrol_stage_outputs(wiki, output_dir);
    let receipt_path = patrol_stage_receipt(output_dir, wiki);
    let spec = fingerprint::StageSpec {
        stage: "patrol_compute",
        scope: wiki,
        selected_snapshot: snapshot,
        algorithm_version: PATROL_COMPUTE_ALGORITHM_VERSION,
    };
    if fingerprinted_run && fingerprint::reusable(&receipt_path, spec, &inputs, &outputs)? {
        crate::observability::record_stage_reused("patrol_compute", Some(wiki));
        info!(
            wiki,
            snapshot = snapshot.unwrap_or("legacy"),
            receipt = %receipt_path.display(),
            "reusing deterministic patrol compute stage"
        );
        return Ok(());
    }

    if rebuild || fingerprinted_run {
        clear_patrol_parts_dir(output_dir, wiki)?;
    } else {
        bootstrap_patrol_parts_from_final(output_dir, wiki)?;
    }

    let autopatrol_groups = load_cached_autopatrol_groups(&meta_path)?;
    info!(wiki = wiki, groups = ?autopatrol_groups, "loaded autopatrol groups");

    info!(wiki = wiki, "loading patrol data");
    let patrol_df = read_parquet_df(&patrol_path, Some(patrol_projection()))?;
    info!(
        wiki = wiki,
        rows = patrol_df.height(),
        "loaded patrol events"
    );

    let all_months = collect_patrol_months(&patrol_df)?;
    let completed_months = if rebuild {
        BTreeSet::new()
    } else {
        existing_patrol_months(output_dir, wiki)?
    };
    let mut pending_months: Vec<i32> = all_months
        .into_iter()
        .filter(|year_month| !completed_months.contains(year_month))
        .collect();
    if let Some(limit) = limit_months {
        pending_months.truncate(limit);
    }

    if pending_months.is_empty() {
        info!(wiki = wiki, "no patrol months require recomputation");
        let final_path = output_dir.join(wiki).join("patrol.parquet");
        if final_path.is_file() {
            record_patrol_stage(&receipt_path, spec, &inputs, wiki, output_dir)?;
            return Ok(());
        }
        let merged_path = merge_wiki_patrol_parts(output_dir, wiki)?;
        refresh_patrol_dashboard_artifacts(output_dir, merged_path.as_deref())?;
        record_patrol_stage(&receipt_path, spec, &inputs, wiki, output_dir)?;
        return Ok(());
    }

    info!(
        wiki = wiki,
        months = pending_months.len(),
        first = format_year_month(*pending_months.first().expect("pending months")),
        last = format_year_month(*pending_months.last().expect("pending months")),
        "computing patrol metrics incrementally"
    );
    let pending_set: HashSet<i32> = pending_months.iter().copied().collect();
    let patrolled_ids = collect_patrolled_revision_ids(&patrol_df, &pending_set)?;
    info!(
        wiki = wiki,
        revision_ids = patrolled_ids.len(),
        "collected patrolled revision ids for pending months"
    );

    info!(wiki = wiki, "building autopatrol membership timeline");
    let autopatrol_intervals = build_autopatrol_intervals(&rights_path, &autopatrol_groups)?;

    let all_month_partitions = collect_partition_files_by_month(data_dir, wiki, snapshot)?;
    let month_partitions = filter_partition_files_by_month(&all_month_partitions, &pending_set);
    let pending = &pending_set;
    let auto = &autopatrol_intervals;
    let mut summary = build_revision_summary(&month_partitions, &patrolled_ids, pending, auto)?;
    let present_patrolled_ids: HashSet<i64> = summary.patrolled_lookup.keys().copied().collect();
    let missing_ids: HashSet<i64> = patrolled_ids
        .difference(&present_patrolled_ids)
        .copied()
        .collect();
    if !missing_ids.is_empty() {
        info!(
            wiki = wiki,
            missing_revision_ids = missing_ids.len(),
            "performing external revision lookup for patrol references"
        );
        let parts = &all_month_partitions;
        let months = &pending_months;
        let window_lookup =
            load_revision_subset_by_ids_near_pending_months(parts, months, &missing_ids)?;
        summary.patrolled_lookup.extend(window_lookup);
        let resolved_patrolled_ids: HashSet<i64> =
            summary.patrolled_lookup.keys().copied().collect();
        let still_missing_ids: HashSet<i64> = missing_ids
            .difference(&resolved_patrolled_ids)
            .copied()
            .collect();
        let ids = &still_missing_ids;
        let lookup = &mut summary.patrolled_lookup;
        (!ids.is_empty())
            .then(|| extend_lookup_once(&revision_files, ids, lookup, wiki))
            .transpose()?;
    }

    let patrol_stats = aggregate_patrol_stats(&patrol_df, &pending_set, &summary.patrolled_lookup)?;
    write_patrol_month_parts(output_dir, wiki, &pending_months, &summary, &patrol_stats)?;
    let merged_path = merge_wiki_patrol_parts(output_dir, wiki)?;
    refresh_patrol_dashboard_artifacts(output_dir, merged_path.as_deref())?;
    if recordable_run {
        record_patrol_stage(&receipt_path, spec, &inputs, wiki, output_dir)?;
    }
    Ok(())
}

fn patrol_stage_inputs(
    wiki: &str,
    data_dir: &Path,
    snapshot: Option<&str>,
) -> Result<Vec<fingerprint::TrackedPath>> {
    let mut inputs = compute::compute_stage_inputs(wiki, data_dir, snapshot)?;
    if let Some(snapshot) = snapshot
        && generation::exists(data_dir, wiki, snapshot)
    {
        inputs.extend(generation::tracked_inputs(data_dir, wiki, snapshot)?);
    } else {
        let patrol_dir = data_dir.join("patrol").join(wiki);
        for name in ["autopatrol_groups.json", "patrol.parquet", "rights.parquet"] {
            inputs.push(fingerprint::TrackedPath::new(
                format!("patrol-source/{wiki}/{name}"),
                patrol_dir.join(name),
            ));
        }
    }
    inputs.sort_by(|left, right| left.identity.cmp(&right.identity));
    Ok(inputs)
}

fn patrol_stage_outputs(wiki: &str, output_dir: &Path) -> Vec<fingerprint::TrackedPath> {
    let path = output_dir.join(wiki).join("patrol.parquet");
    path.is_file()
        .then(|| fingerprint::TrackedPath::new(format!("output/{wiki}/patrol.parquet"), path))
        .into_iter()
        .collect()
}

fn patrol_stage_receipt(output_dir: &Path, wiki: &str) -> PathBuf {
    output_dir
        .join("_stages")
        .join("patrol_compute")
        .join(format!("{wiki}.json"))
}

fn record_patrol_stage(
    receipt_path: &Path,
    spec: fingerprint::StageSpec<'_>,
    inputs: &[fingerprint::TrackedPath],
    wiki: &str,
    output_dir: &Path,
) -> Result<()> {
    let outputs = patrol_stage_outputs(wiki, output_dir);
    fingerprint::record(receipt_path, spec, inputs, &outputs)?;
    Ok(())
}

pub(crate) fn reusable_candidate_files(
    wiki: &str,
    snapshot: &str,
    data_dir: &Path,
    candidate_dir: &Path,
) -> Result<Option<Vec<PathBuf>>> {
    storage::validate_snapshot_version(snapshot)?;
    let inputs = patrol_stage_inputs(wiki, data_dir, Some(snapshot))?;
    let outputs = patrol_stage_outputs(wiki, candidate_dir);
    let receipt = patrol_stage_receipt(candidate_dir, wiki);
    let spec = fingerprint::StageSpec {
        stage: "patrol_compute",
        scope: wiki,
        selected_snapshot: Some(snapshot),
        algorithm_version: PATROL_COMPUTE_ALGORITHM_VERSION,
    };
    if !fingerprint::reusable(&receipt, spec, &inputs, &outputs)? {
        return Ok(None);
    }
    let mut files = outputs
        .into_iter()
        .flat_map(|output| {
            let receipt = crate::artifact_receipt::sidecar_path(&output.path).ok();
            std::iter::once(output.path).chain(receipt)
        })
        .collect::<Vec<_>>();
    files.push(receipt);
    Ok(Some(files))
}

pub(crate) fn candidate_receipt_identity(
    wiki: &str,
    snapshot: &str,
    data_dir: &Path,
    candidate_dir: &Path,
) -> Result<String> {
    reusable_candidate_files(wiki, snapshot, data_dir, candidate_dir)?
        .context("candidate does not have a reusable patrol receipt")?;
    Ok(fingerprint::read_receipt(&patrol_stage_receipt(candidate_dir, wiki))?.fingerprint)
}

pub(crate) fn candidate_receipt_current_without_inputs(
    wiki: &str,
    snapshot: &str,
    candidate_dir: &Path,
) -> Result<bool> {
    storage::validate_snapshot_version(snapshot)?;
    fingerprint::outputs_reusable(
        &patrol_stage_receipt(candidate_dir, wiki),
        fingerprint::StageSpec {
            stage: "patrol_compute",
            scope: wiki,
            selected_snapshot: Some(snapshot),
            algorithm_version: PATROL_COMPUTE_ALGORITHM_VERSION,
        },
        &patrol_stage_outputs(wiki, candidate_dir),
    )
}

pub(crate) const fn algorithm_version() -> &'static str {
    PATROL_COMPUTE_ALGORITHM_VERSION
}

pub(crate) fn cached_sources_available(data_dir: &Path, wiki: &str) -> bool {
    let patrol_dir = data_dir.join("patrol").join(wiki);
    let metadata_valid = fs::read(patrol_dir.join("autopatrol_groups.json"))
        .ok()
        .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
        .and_then(|value| value.get("autopatrol_groups").cloned())
        .is_some_and(|groups| groups.is_array());
    metadata_valid
        && ["patrol.parquet", "rights.parquet"]
            .iter()
            .map(|name| patrol_dir.join(name))
            .all(|path| {
                File::open(path)
                    .ok()
                    .and_then(|file| ParquetReader::new(file).num_rows().ok())
                    .is_some_and(|rows| rows > 0)
            })
}

pub(crate) fn cached_sources_available_for_snapshot(
    data_dir: &Path,
    wiki: &str,
    snapshot: &str,
) -> bool {
    generation::exists(data_dir, wiki, snapshot)
        && generation::load(data_dir, wiki, snapshot).is_ok()
}

#[cfg(test)]
pub(crate) fn record_candidate_fingerprint_for_test(
    wiki: &str,
    snapshot: &str,
    data_dir: &Path,
    candidate_dir: &Path,
) -> Result<()> {
    let inputs = patrol_stage_inputs(wiki, data_dir, Some(snapshot))?;
    record_patrol_stage(
        &patrol_stage_receipt(candidate_dir, wiki),
        fingerprint::StageSpec {
            stage: "patrol_compute",
            scope: wiki,
            selected_snapshot: Some(snapshot),
            algorithm_version: PATROL_COMPUTE_ALGORITHM_VERSION,
        },
        &inputs,
        wiki,
        candidate_dir,
    )
}

fn read_parquet_df(path: &Path, columns: Option<Vec<String>>) -> Result<DataFrame> {
    let file = File::open(path)?;
    storage::prepare_sequential_read(&file);
    let result = ParquetReader::new(file)
        .with_columns(columns)
        .finish()
        .map_err(Into::into);
    storage::discard_path_cache(path);
    result
}

fn projection(columns: &[&str]) -> Vec<String> {
    columns.iter().map(|column| (*column).to_string()).collect()
}

fn patrol_projection() -> Vec<String> {
    projection(PATROL_COLUMNS)
}

fn revision_projection() -> Vec<String> {
    projection(REVISION_COLUMNS)
}

fn ensure_parent_dir(path: &Path) -> Result<()> {
    path.parent().map(fs::create_dir_all).transpose()?;
    Ok(())
}

fn extend_lookup_once(
    revision_files: &[PathBuf],
    revision_ids: &HashSet<i64>,
    lookup: &mut HashMap<i64, RevisionMeta>,
    wiki: &str,
) -> Result<()> {
    info!(
        wiki = wiki,
        missing_revision_ids = revision_ids.len(),
        "falling back to full revision lookup for unresolved patrol references"
    );
    let loaded = load_revision_subset_by_ids_once(revision_files, revision_ids)?;
    lookup.extend(loaded);
    Ok(())
}

fn download_logging_source<T: PatrolTransport + ?Sized>(
    transport: &T,
    wiki: &str,
    source: &plan::PatrolSourceSpec,
    dest_path: &Path,
) -> Result<LoggingSourceIdentity> {
    let url = source.url.as_str();
    let existing_size = dest_path.metadata().map(|meta| meta.len()).unwrap_or(0);
    info!(wiki = wiki, url = %url, resume_from = existing_size, "downloading patrol log dump");
    let mut response = transport.get(url, (existing_size > 0).then_some(existing_size))?;
    let expected_length = response.content_length;
    let etag = response.etag.take();
    let last_modified = response.last_modified.take();
    let mut file = if existing_size > 0 {
        fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(dest_path)?
    } else {
        File::create(dest_path)?
    };

    let mut buffer = vec![0_u8; 256 * 1024];
    loop {
        let bytes_read = response.body.read(&mut buffer)?;
        if bytes_read == 0 {
            break;
        }
        file.write_all(&buffer[..bytes_read])?;
    }
    // Make the downloaded bytes durable before hashing them.  The hash pass
    // advises Linux to evict each completed range; without this sync those
    // pages can remain dirty and charged to the preparation job's cgroup.
    file.flush()?;
    file.sync_all()?;
    drop(file);

    // mediawiki_history dumps do not publish checksums, so we apply a cheap
    // gzip-magic check post-download to catch CDN truncation, HTML error
    // pages served as 200, and other corruption that satisfied
    // Content-Length but is not actually a gzipped XML stream.
    if let Err(integrity_error) = verify_gzip_magic(dest_path) {
        warn!(
            path = %dest_path.display(),
            error = %integrity_error,
            "downloaded patrol dump failed gzip magic check; removing and aborting"
        );
        let _ = fs::remove_file(dest_path);
        return Err(integrity_error);
    }

    let (content_length, downloaded_sha256, downloaded_sha1) = hash_logging_source(dest_path)?;
    if expected_length.is_some_and(|expected| expected != content_length)
        || source.expected_size != content_length
    {
        let _ = fs::remove_file(dest_path);
        anyhow::bail!(
            "patrol logging source length changed during download (inventory expected {}, transport expected {:?}, received {content_length})",
            source.expected_size,
            expected_length
        );
    }
    if downloaded_sha1 != source.sha1.to_ascii_lowercase() {
        let _ = fs::remove_file(dest_path);
        anyhow::bail!(
            "patrol logging source SHA-1 mismatch for {} (expected {}, received {})",
            source.source_id,
            source.sha1,
            downloaded_sha1
        );
    }

    info!(wiki = wiki, path = %dest_path.display(), "downloaded patrol log dump");
    Ok(LoggingSourceIdentity {
        source_id: source.source_id.clone(),
        remote_url: url.to_string(),
        content_length,
        etag,
        last_modified,
        downloaded_sha256,
        upstream_md5: source.md5.clone(),
        upstream_sha1: source.sha1.clone(),
        downloaded_sha1,
    })
}

fn hash_logging_source(path: &Path) -> Result<(u64, String, String)> {
    let file = File::open(path)?;
    storage::prepare_sequential_read(&file);
    let mut reader = BufReader::new(file.try_clone()?);
    let mut sha256 = Sha256::new();
    let mut sha1 = Sha1::default();
    let mut bytes = 0_u64;
    let mut buffer = vec![0_u8; 1024 * 1024];
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        sha256.update(&buffer[..read]);
        sha1::Digest::update(&mut sha1, &buffer[..read]);
        bytes = bytes
            .checked_add(u64::try_from(read)?)
            .context("patrol source size overflow")?;
    }
    storage::discard_file_cache(&file, 0, bytes);
    Ok((
        bytes,
        hex::encode(sha256.finalize()),
        hex::encode(sha1::Digest::finalize(sha1)),
    ))
}

/// Magic-byte check for gzipped patrol log dumps. See `fetch::verify_bz2_magic`
/// for context on why this is the strongest available integrity gate for the
/// `mediawiki_history`/`pages-logging` dump endpoints.
fn verify_gzip_magic(path: &Path) -> Result<()> {
    const GZIP_MAGIC: [u8; 2] = [0x1f, 0x8b];
    let mut file = File::open(path)
        .with_context(|| format!("failed to open {} for magic-byte check", path.display()))?;
    let mut header = [0_u8; 2];
    let mut filled = 0;
    while filled < header.len() {
        match file.read(&mut header[filled..])? {
            0 => break,
            n => filled += n,
        }
    }
    if filled < GZIP_MAGIC.len() || header != GZIP_MAGIC {
        anyhow::bail!(
            "downloaded file {} does not begin with gzip magic (1f 8b); got {} byte(s) {:02x?}",
            path.display(),
            filled,
            &header[..filled]
        );
    }
    Ok(())
}

fn fetch_autopatrol_groups<T: PatrolTransport + ?Sized>(
    transport: &T,
    wiki: &str,
) -> Result<Vec<String>> {
    let Some(domain) = wiki_to_api_domain(wiki) else {
        return Ok(Vec::new());
    };
    let url = format!(
        "https://{domain}/w/api.php?action=query&meta=siteinfo&siprop=usergroups&format=json"
    );
    let value = transport.get_json(&url)?;
    let groups = value
        .get("query")
        .and_then(|query| query.get("usergroups"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|group| {
            let rights = group.get("rights")?.as_array()?;
            if !rights
                .iter()
                .any(|right| right.as_str() == Some("autopatrol"))
            {
                return None;
            }
            group.get("name")?.as_str().map(|name| name.to_string())
        })
        .collect();
    Ok(groups)
}

fn wiki_to_api_domain(wiki: &str) -> Option<String> {
    if wiki != "wiki" && wiki.ends_with("wiki") {
        return Some(format!("{}.wikipedia.org", &wiki[..wiki.len() - 4]));
    }
    None
}

fn parse_logging_events<P: PatrolSink + ?Sized, R: RightsSink + ?Sized>(
    xml_path: &Path,
    patrol_writer: &mut P,
    rights_writer: &mut R,
) -> Result<LoggingParseStats> {
    let file = File::open(xml_path)?;
    let compressed_bytes = file.metadata()?.len();
    crate::storage::prepare_sequential_read(&file);
    let decoder = MultiGzDecoder::new(BufReader::new(file.try_clone()?));
    let mut reader = Reader::from_reader(BufReader::new(decoder));
    reader.config_mut().trim_text(true);

    let mut buffer = Vec::new();
    let mut current = None::<LogItem>;
    let mut current_tag = None::<String>;
    let mut in_contributor = false;
    let mut stats = LoggingParseStats::default();

    loop {
        match reader.read_event_into(&mut buffer) {
            Ok(Event::Start(event)) => {
                let tag = local_name(&event);
                match tag.as_str() {
                    "logitem" => current = Some(LogItem::default()),
                    "contributor" => in_contributor = true,
                    _ if current.is_some() => current_tag = Some(tag),
                    _ => {}
                }
            }
            Ok(Event::End(event)) => {
                let tag = String::from_utf8_lossy(event.local_name().as_ref()).to_string();
                match tag.as_str() {
                    "contributor" => {
                        in_contributor = false;
                        current_tag = None;
                    }
                    "logitem" => {
                        if let Some(item) = current.take() {
                            stats.total_log_items += 1;
                            match item {
                                item if matches!(item.log_type.as_deref(), Some("patrol")) => {
                                    patrol_writer.add_patrol(item.into_patrol_row())?;
                                    stats.patrol_events += 1;
                                }
                                item if matches!(item.log_type.as_deref(), Some("rights")) => {
                                    rights_writer.add_rights(item.into_rights_row())?;
                                    stats.rights_events += 1;
                                }
                                _ => stats.skipped_events += 1,
                            }
                        }
                        current_tag = None;
                    }
                    _ => current_tag = None,
                }
            }
            Ok(Event::Text(text)) => {
                let decoded = text.decode()?.into_owned();
                apply_decoded_log_text(
                    current.as_mut(),
                    current_tag.as_deref(),
                    in_contributor,
                    decoded,
                );
            }
            Ok(Event::CData(text)) => {
                let decoded = text.decode()?.into_owned();
                apply_decoded_log_text(
                    current.as_mut(),
                    current_tag.as_deref(),
                    in_contributor,
                    decoded,
                );
            }
            Ok(Event::Eof) => break,
            Ok(_) => {}
            Err(err) => return Err(err.into()),
        }
        buffer.clear();
    }

    crate::storage::discard_file_cache(&file, 0, compressed_bytes);
    Ok(stats)
}

fn parse_account_creation_events(
    xml_path: &Path,
    snapshot: &str,
    accounts: &mut HashMap<i64, (String, bool)>,
    temporary_by_month: &mut BTreeMap<String, u32>,
) -> Result<AccountCreationParseStats> {
    let file = File::open(xml_path)?;
    let compressed_bytes = file.metadata()?.len();
    crate::storage::prepare_sequential_read(&file);
    let decoder = MultiGzDecoder::new(BufReader::new(file.try_clone()?));
    let mut reader = Reader::from_reader(BufReader::new(decoder));
    reader.config_mut().trim_text(true);

    let mut buffer = Vec::new();
    let mut current = None::<LogItem>;
    let mut current_tag = None::<String>;
    let mut in_contributor = false;
    let mut stats = AccountCreationParseStats::default();
    loop {
        match reader.read_event_into(&mut buffer) {
            Ok(Event::Start(event)) => {
                let tag = local_name(&event);
                match tag.as_str() {
                    "logitem" => current = Some(LogItem::default()),
                    "contributor" => in_contributor = true,
                    _ if current.is_some() => current_tag = Some(tag),
                    _ => {}
                }
            }
            Ok(Event::End(event)) => {
                let tag = String::from_utf8_lossy(event.local_name().as_ref()).to_string();
                match tag.as_str() {
                    "contributor" => {
                        in_contributor = false;
                        current_tag = None;
                    }
                    "logitem" => {
                        let item = current
                            .take()
                            .context("account logitem ended without a matching start")?;
                        stats.total_log_items = stats
                            .total_log_items
                            .checked_add(1)
                            .context("account logging item count overflow")?;
                        if matches!(item.log_type.as_deref(), Some("newusers")) {
                            stats.account_creation_events = stats
                                .account_creation_events
                                .checked_add(1)
                                .context("account creation event count overflow")?;
                            let row = item.into_new_user_row();
                            let month = row
                                .timestamp
                                .get(..7)
                                .context("newusers timestamp has no event month")?
                                .to_string();
                            storage::validate_snapshot_version(&month)?;
                            if compute::snapshot_contains_complete_month(snapshot, &month) {
                                if row.is_temporary {
                                    stats.temporary_accounts = stats
                                        .temporary_accounts
                                        .checked_add(1)
                                        .context("temporary account count overflow")?;
                                    let count = temporary_by_month.entry(month).or_default();
                                    *count = count
                                        .checked_add(1)
                                        .context("temporary account month count overflow")?;
                                } else if let Some(user_id) = row.target_user_id {
                                    stats.permanent_creation_events = stats
                                        .permanent_creation_events
                                        .checked_add(1)
                                        .context("permanent account count overflow")?;
                                    if let Some((existing_month, _)) = accounts.get(&user_id) {
                                        anyhow::ensure!(
                                            existing_month == &month,
                                            "account {user_id} has creation events in multiple months"
                                        );
                                    } else {
                                        accounts.insert(user_id, (month, false));
                                    }
                                } else {
                                    stats.unresolved_permanent = stats
                                        .unresolved_permanent
                                        .checked_add(1)
                                        .context("unresolved account count overflow")?;
                                }
                            }
                        }
                        current_tag = None;
                    }
                    _ => current_tag = None,
                }
            }
            Ok(Event::Text(text)) => apply_decoded_log_text(
                current.as_mut(),
                current_tag.as_deref(),
                in_contributor,
                text.decode()?.into_owned(),
            ),
            Ok(Event::CData(text)) => apply_decoded_log_text(
                current.as_mut(),
                current_tag.as_deref(),
                in_contributor,
                text.decode()?.into_owned(),
            ),
            Ok(Event::Eof) => break,
            Ok(_) => {}
            Err(error) => return Err(error.into()),
        }
        buffer.clear();
    }
    crate::storage::discard_file_cache(&file, 0, compressed_bytes);
    Ok(stats)
}

#[cfg(test)]
fn validate_logging_parse(xml_path: &Path, stats: LoggingParseStats) -> Result<()> {
    let compressed_bytes = fs::metadata(xml_path)?.len();
    validate_logging_parse_totals(compressed_bytes, stats, &xml_path.display().to_string())
}

fn validate_logging_parse_totals(
    compressed_bytes: u64,
    stats: LoggingParseStats,
    source_identity: &str,
) -> Result<()> {
    let substantial = compressed_bytes >= SUBSTANTIAL_LOGGING_DUMP_BYTES
        || stats.total_log_items >= SUBSTANTIAL_LOG_ITEMS;
    anyhow::ensure!(
        !substantial || stats.patrol_events + stats.rights_events > 0,
        "substantial logging dump {} produced zero patrol or rights events (compressed_bytes={}, total_log_items={}, skipped_events={})",
        source_identity,
        compressed_bytes,
        stats.total_log_items,
        stats.skipped_events,
    );
    Ok(())
}

fn local_name(event: &quick_xml::events::BytesStart<'_>) -> String {
    String::from_utf8_lossy(event.local_name().as_ref()).to_string()
}

fn apply_decoded_log_text(
    item: Option<&mut LogItem>,
    tag: Option<&str>,
    in_contributor: bool,
    value: String,
) {
    if let (Some(item), Some(tag)) = (item, tag) {
        apply_log_text(item, tag, in_contributor, value);
    }
}

fn apply_log_text(item: &mut LogItem, tag: &str, in_contributor: bool, value: String) {
    match (tag, in_contributor) {
        ("type", _) => item.log_type = Some(value),
        ("action", _) => item.log_action = Some(value),
        ("id", true) => item.contributor_id = parse_i64_opt(&value),
        ("id", false) => item.log_id = parse_i64_opt(&value),
        ("timestamp", _) => item.timestamp = Some(normalize_timestamp(&value)),
        ("username", true) => item.contributor_name = Some(value),
        ("logtitle", _) => item.log_title = Some(value),
        ("params", _) => item.params = Some(value),
        _ => {}
    }
}

fn normalize_timestamp(timestamp: &str) -> String {
    timestamp
        .replace('T', " ")
        .trim_end_matches('Z')
        .split('.')
        .next()
        .unwrap_or(timestamp)
        .to_string()
}

fn parse_i64_opt(value: &str) -> Option<i64> {
    value.trim().parse().ok()
}

impl LogItem {
    fn into_patrol_row(self) -> PatrolRow {
        let params = self.params.unwrap_or_default();
        let (current_revision_id, prev_revision_id, is_auto) = parse_patrol_params(&params);
        PatrolRow {
            log_id: self.log_id.unwrap_or(0),
            timestamp: self.timestamp.unwrap_or_default(),
            user: self.contributor_name,
            user_id: self.contributor_id,
            page_title: self.log_title,
            current_revision_id,
            prev_revision_id,
            is_auto,
        }
    }

    fn into_rights_row(self) -> RightsRow {
        let log_title = self.log_title.unwrap_or_default();
        let target_user = log_title
            .split_once(':')
            .map(|(_, rest)| rest.to_string())
            .unwrap_or(log_title);
        let params = self.params.unwrap_or_default();
        let (old_groups, new_groups) = parse_rights_params(&params);
        RightsRow {
            timestamp: self.timestamp.unwrap_or_default(),
            target_user,
            old_groups,
            new_groups,
        }
    }

    fn into_new_user_row(self) -> NewUserRow {
        let action = self.log_action.unwrap_or_default();
        let target_user_id = self
            .params
            .as_deref()
            .and_then(parse_new_user_id)
            .or_else(|| {
                matches!(action.as_str(), "create" | "autocreate" | "newusers")
                    .then_some(self.contributor_id)
                    .flatten()
            });
        let target_user = self.log_title;
        let is_temporary = target_user
            .as_deref()
            .map(|title| title.rsplit_once(':').map_or(title, |(_, user)| user))
            .is_some_and(|user| user.starts_with('~'));
        NewUserRow {
            timestamp: self.timestamp.unwrap_or_default(),
            target_user_id,
            is_temporary,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct AccountCreationMonth {
    year_month: String,
    accounts_created: u32,
    accounts_with_edits: u32,
    accounts_without_edits: u32,
    temporary_accounts_excluded: u32,
}

#[derive(Debug, Serialize)]
struct AccountCreationStagingReport {
    schema_version: u32,
    metric_version: &'static str,
    license_spdx: &'static str,
    attribution: &'static str,
    wiki: String,
    snapshot: String,
    logging_dump_date: String,
    source_plan_sha256: String,
    compressed_source_bytes: u64,
    total_log_items: u64,
    account_creation_events: u64,
    permanent_accounts: u64,
    temporary_accounts: u64,
    history_scan_mode: &'static str,
    history_sources: u32,
    history_source_bytes: u64,
    history_revision_rows: u64,
    definition: &'static str,
    rows: Vec<AccountCreationMonth>,
}

const ACCOUNT_CREATION_METRIC_VERSION: &str =
    "account-creations-v1-permanent-local-account-lifetime-public-edit";

fn parse_new_user_id(params: &str) -> Option<i64> {
    let params = params.trim();
    if let Ok(value) = params.parse::<i64>() {
        return Some(value);
    }
    static USER_ID: OnceLock<Regex> = OnceLock::new();
    USER_ID
        .get_or_init(|| {
            Regex::new(r#"(?:4::)?userid\";i:(\d+)"#).expect("newusers userid expression is valid")
        })
        .captures(params)
        .and_then(|capture| capture.get(1))
        .and_then(|value| value.as_str().parse().ok())
}

#[derive(Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
struct PatrolRow {
    timestamp: String,
    log_id: i64,
    user: Option<String>,
    user_id: Option<i64>,
    page_title: Option<String>,
    current_revision_id: i64,
    prev_revision_id: i64,
    is_auto: bool,
}

#[derive(Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
struct RightsRow {
    timestamp: String,
    target_user: String,
    old_groups: String,
    new_groups: String,
}

#[derive(Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
struct NewUserRow {
    timestamp: String,
    target_user_id: Option<i64>,
    is_temporary: bool,
}

impl PatrolWriter {
    fn new(path: &Path) -> Result<Self> {
        Self::new_with_batch_rows(path, PARQUET_BATCH_ROWS)
    }

    fn new_with_batch_rows(path: &Path, batch_rows: usize) -> Result<Self> {
        let file = File::create(path)?;
        let schema = patrol_schema();
        let writer = ParquetWriter::new(file)
            .with_compression(ParquetCompression::Zstd(None))
            .batched(&schema)?;
        Ok(Self {
            writer,
            batch: PatrolBatch::default(),
            batch_rows,
        })
    }

    fn add(&mut self, row: PatrolRow) -> Result<()> {
        self.batch.log_id.push(row.log_id);
        self.batch.timestamp.push(row.timestamp);
        self.batch.user.push(row.user);
        self.batch.user_id.push(row.user_id);
        self.batch.page_title.push(row.page_title);
        self.batch.current_revision_id.push(row.current_revision_id);
        self.batch.prev_revision_id.push(row.prev_revision_id);
        self.batch.is_auto.push(row.is_auto);
        if self.batch.log_id.len() >= self.batch_rows {
            self.flush()?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        if self.batch.log_id.is_empty() {
            return Ok(());
        }
        let df = DataFrame::new_infer_height(self.batch.take_columns())?;
        self.writer.write_batch(&df)?;
        Ok(())
    }

    fn finish(mut self) -> Result<()> {
        self.flush()?;
        self.writer.finish()?;
        Ok(())
    }
}

impl PatrolSink for PatrolWriter {
    fn add_patrol(&mut self, row: PatrolRow) -> Result<()> {
        self.add(row)
    }
}

impl RightsWriter {
    fn new(path: &Path) -> Result<Self> {
        Self::new_with_batch_rows(path, PARQUET_BATCH_ROWS)
    }

    fn new_with_batch_rows(path: &Path, batch_rows: usize) -> Result<Self> {
        let file = File::create(path)?;
        let schema = rights_schema();
        let writer = ParquetWriter::new(file)
            .with_compression(ParquetCompression::Zstd(None))
            .batched(&schema)?;
        Ok(Self {
            writer,
            batch: RightsBatch::default(),
            batch_rows,
        })
    }

    fn add(&mut self, row: RightsRow) -> Result<()> {
        self.batch.timestamp.push(row.timestamp);
        self.batch.target_user.push(row.target_user);
        self.batch.old_groups.push(row.old_groups);
        self.batch.new_groups.push(row.new_groups);
        if self.batch.timestamp.len() >= self.batch_rows {
            self.flush()?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        if self.batch.timestamp.is_empty() {
            return Ok(());
        }
        let df = DataFrame::new_infer_height(self.batch.take_columns())?;
        self.writer.write_batch(&df)?;
        Ok(())
    }

    fn finish(mut self) -> Result<()> {
        self.flush()?;
        self.writer.finish()?;
        Ok(())
    }
}

impl RightsSink for RightsWriter {
    fn add_rights(&mut self, row: RightsRow) -> Result<()> {
        self.add(row)
    }
}

fn patrol_schema() -> Schema {
    Schema::from_iter([
        Field::new("log_id".into(), DataType::Int64),
        Field::new("timestamp".into(), DataType::String),
        Field::new("user".into(), DataType::String),
        Field::new("user_id".into(), DataType::Int64),
        Field::new("page_title".into(), DataType::String),
        Field::new("current_revision_id".into(), DataType::Int64),
        Field::new("prev_revision_id".into(), DataType::Int64),
        Field::new("is_auto".into(), DataType::Boolean),
    ])
}

fn rights_schema() -> Schema {
    Schema::from_iter([
        Field::new("timestamp".into(), DataType::String),
        Field::new("target_user".into(), DataType::String),
        Field::new("old_groups".into(), DataType::String),
        Field::new("new_groups".into(), DataType::String),
    ])
}

fn load_cached_autopatrol_groups(meta_path: &Path) -> Result<Vec<String>> {
    if !meta_path.exists() {
        return Ok(Vec::new());
    }
    let value: Value = serde_json::from_slice(&fs::read(meta_path)?)?;
    Ok(value
        .get("autopatrol_groups")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|entry| entry.as_str().map(|value| value.to_string()))
        .collect())
}

fn patrol_param_regex() -> &'static Regex {
    static REGEX: OnceLock<Regex> = OnceLock::new();
    REGEX.get_or_init(|| {
        Regex::new(r#""(?P<field>[^"]+)";(?:(?:s:\d+:"(?P<str>[^"]*)")|(?:i:(?P<int>\d+)))"#)
            .expect("valid patrol param regex")
    })
}

fn rights_group_regex() -> &'static Regex {
    static REGEX: OnceLock<Regex> = OnceLock::new();
    REGEX.get_or_init(|| Regex::new(r#"s:\d+:"([^"]+)""#).expect("valid rights regex"))
}

fn parse_patrol_params(params: &str) -> (i64, i64, bool) {
    if params.trim().is_empty() {
        return (0, 0, false);
    }
    if params.trim_start().starts_with("a:") {
        let mut current_revision_id = 0;
        let mut prev_revision_id = 0;
        let mut is_auto = false;
        for captures in patrol_param_regex().captures_iter(params) {
            let field = captures
                .name("field")
                .expect("patrol params regex should always capture field")
                .as_str();
            let string_value = captures.name("str").map(|m| m.as_str());
            let int_value = captures
                .name("int")
                .and_then(|m| m.as_str().parse::<i64>().ok());
            match field {
                "4::curid" => {
                    current_revision_id = string_value
                        .and_then(|value| value.parse::<i64>().ok())
                        .or(int_value)
                        .unwrap_or(0);
                }
                "5::previd" => {
                    prev_revision_id = string_value
                        .and_then(|value| value.parse::<i64>().ok())
                        .or(int_value)
                        .unwrap_or(0);
                }
                "6::auto" => is_auto = int_value.unwrap_or_default() == 1,
                _ => {}
            }
        }
        return (current_revision_id, prev_revision_id, is_auto);
    }

    let mut lines = params.lines();
    let current_revision_id = lines
        .next()
        .and_then(|value| value.trim().parse::<i64>().ok())
        .unwrap_or_default();
    let prev_revision_id = lines
        .next()
        .and_then(|value| value.trim().parse::<i64>().ok())
        .unwrap_or_default();
    let is_auto = lines
        .next()
        .map(|value| value.trim() == "1")
        .unwrap_or(false);
    (current_revision_id, prev_revision_id, is_auto)
}

fn parse_rights_params(params: &str) -> (String, String) {
    if params.trim().is_empty() {
        return (String::new(), String::new());
    }

    if params.contains("a:") {
        let old_groups = extract_php_groups(params, "4::oldgroups");
        let new_groups = extract_php_groups(params, "5::newgroups");
        return (old_groups.join(","), new_groups.join(","));
    }

    let mut lines = params.lines();
    (
        lines.next().unwrap_or_default().trim().to_string(),
        lines.next().unwrap_or_default().trim().to_string(),
    )
}

fn extract_php_groups(params: &str, key: &str) -> Vec<String> {
    let marker = format!(r#""{key}";"#);
    let Some(start) = params.find(&marker) else {
        return Vec::new();
    };
    let slice = &params[start + marker.len()..];
    let Some(body) = extract_php_array_body(slice) else {
        return Vec::new();
    };
    let mut values = Vec::new();
    for capture in rights_group_regex().captures_iter(body) {
        let value = capture
            .get(1)
            .expect("rights regex should always capture group names")
            .as_str();
        if value.chars().all(|ch| ch.is_ascii_digit()) && value.len() == 14 {
            continue;
        }
        values.push(value.to_string());
    }
    values.sort();
    values.dedup();
    values
}

fn extract_php_array_body(value: &str) -> Option<&str> {
    let open_brace = value.find('{')?;
    let mut depth = 0_u32;
    let end_offset = value[open_brace..]
        .char_indices()
        .find_map(|(offset, ch)| match ch {
            '{' => {
                depth += 1;
                None
            }
            '}' => {
                depth = depth.checked_sub(1)?;
                (depth == 0).then_some(offset)
            }
            _ => None,
        })?;
    Some(&value[open_brace + 1..open_brace + end_offset])
}

fn collect_patrol_months(patrol_df: &DataFrame) -> Result<Vec<i32>> {
    let timestamps = patrol_df.column("timestamp")?.str()?;
    let mut months = BTreeSet::new();
    for idx in 0..patrol_df.height() {
        if let Some(timestamp) = timestamps.get(idx).and_then(parse_year_month_key) {
            months.insert(timestamp);
        }
    }
    Ok(months.into_iter().collect())
}

fn collect_patrolled_revision_ids(
    patrol_df: &DataFrame,
    pending_months: &HashSet<i32>,
) -> Result<HashSet<i64>> {
    let timestamps = patrol_df.column("timestamp")?.str()?;
    let current_revision_ids = patrol_df.column("current_revision_id")?.i64()?;
    let mut ids = HashSet::new();
    for idx in 0..patrol_df.height() {
        let Some(year_month_key) = timestamps.get(idx).and_then(parse_year_month_key) else {
            continue;
        };
        if !pending_months.contains(&year_month_key) {
            continue;
        }
        if let Some(revision_id) = current_revision_ids.get(idx) {
            ids.insert(revision_id);
        }
    }
    Ok(ids)
}

fn collect_partition_files_by_month(
    data_dir: &Path,
    wiki: &str,
    snapshot: Option<&str>,
) -> Result<BTreeMap<i32, Vec<PathBuf>>> {
    let mut by_month = BTreeMap::new();
    let partitions = match snapshot {
        Some(snapshot) => {
            let layer_result = storage::snapshot_compute_layer(
                data_dir,
                wiki,
                snapshot,
                storage::GenerationLayer::Warehouse,
            );
            let layer = layer_result?;
            let result = storage::snapshot_partition_specs(data_dir, wiki, snapshot, layer);
            result?
        }
        None => {
            let layer =
                storage::active_compute_layer(data_dir, wiki, storage::GenerationLayer::Warehouse)?;
            storage::active_partition_specs(data_dir, wiki, layer)?
        }
    };
    for spec in partitions {
        let year_month_key = parse_year_month_key(&spec.year_month).unwrap_or_default();
        by_month
            .entry(year_month_key)
            .or_insert_with(Vec::new)
            .extend(spec.files);
    }
    Ok(by_month)
}

fn filter_partition_files_by_month(
    all_month_partitions: &BTreeMap<i32, Vec<PathBuf>>,
    pending_months: &HashSet<i32>,
) -> BTreeMap<i32, Vec<PathBuf>> {
    all_month_partitions
        .iter()
        .filter(|(year_month_key, _)| pending_months.contains(year_month_key))
        .map(|(year_month_key, files)| (*year_month_key, files.clone()))
        .collect()
}

fn build_revision_summary(
    month_partitions: &BTreeMap<i32, Vec<PathBuf>>,
    patrolled_ids: &HashSet<i64>,
    pending_months: &HashSet<i32>,
    autopatrol_intervals: &AutopatrolIntervals,
) -> Result<RevisionSummary> {
    let mut summary = RevisionSummary::default();
    let ids = patrolled_ids;
    let pending = pending_months;
    let auto = autopatrol_intervals;
    for (year_month_key, files) in month_partitions {
        for path in files {
            process_revision_file(path, *year_month_key, ids, pending, auto, &mut summary)?;
        }
    }
    Ok(summary)
}

fn process_revision_file(
    path: &Path,
    year_month_key: i32,
    patrolled_ids: &HashSet<i64>,
    pending_months: &HashSet<i32>,
    autopatrol_intervals: &AutopatrolIntervals,
    summary: &mut RevisionSummary,
) -> Result<()> {
    let df = read_parquet_df(path, Some(revision_projection()))?;
    let revision_ids = df.column("revision_id")?.i64()?;
    let timestamps = df.column("event_timestamp")?.str()?;
    let user_names = df.column("event_user_text")?.str()?;
    let namespaces = df.column("page_namespace")?.i32()?;
    let user_ids = df.column("event_user_id")?.i64()?;
    let bot_by = df.column("event_user_is_bot_by")?.str()?;
    let anonymous = df.column("event_user_is_anonymous")?.bool()?;
    let temporary = df.column("event_user_is_temporary")?.bool()?;

    for idx in 0..df.height() {
        let Some(revision_id) = revision_ids.get(idx) else {
            continue;
        };
        let Some(timestamp) = timestamps.get(idx) else {
            continue;
        };
        let revision_month_key = parse_year_month_key(timestamp).unwrap_or(year_month_key);
        if !pending_months.contains(&revision_month_key) {
            continue;
        }
        let page_namespace = namespaces.get(idx).unwrap_or_default();
        let user_type = classify_user_type(
            bot_by.get(idx),
            anonymous.get(idx).unwrap_or(false),
            temporary.get(idx).unwrap_or(false),
        );
        let key = MetricKey {
            year_month_key: revision_month_key,
            page_namespace,
            user_type,
        };
        *summary.total_revisions.entry(key).or_insert(0) += 1;

        let timestamp_seconds = parse_timestamp_seconds(timestamp)
            .with_context(|| format!("invalid revision timestamp in {}", path.display()))?;
        if patrolled_ids.contains(&revision_id) {
            *summary.patrolled_revisions.entry(key).or_insert(0) += 1;
            summary.patrolled_lookup.insert(
                revision_id,
                RevisionMeta {
                    timestamp_seconds,
                    year_month_key: revision_month_key,
                    page_namespace,
                    user_type,
                },
            );
        } else if let Some(username) = user_names.get(idx)
            && user_has_autopatrol_at(autopatrol_intervals, username, timestamp_seconds)
        {
            let _ = user_ids.get(idx); // keep column access aligned for future extensions
            *summary.autopatrolled_revisions.entry(key).or_insert(0) += 1;
        }
    }

    Ok(())
}

fn load_revision_subset_by_ids_once(
    files: &[PathBuf],
    revision_ids: &HashSet<i64>,
) -> Result<HashMap<i64, RevisionMeta>> {
    let mut lookup = HashMap::new();
    if revision_ids.is_empty() {
        return Ok(lookup);
    }

    for path in files {
        let df = read_parquet_df(path, Some(revision_projection()))?;
        index_revision_lookup_df(&df, revision_ids, &mut lookup)?;
        if lookup.len() >= revision_ids.len() {
            break;
        }
    }

    Ok(lookup)
}

fn load_revision_subset_by_ids_near_pending_months(
    all_month_partitions: &BTreeMap<i32, Vec<PathBuf>>,
    pending_months: &[i32],
    revision_ids: &HashSet<i64>,
) -> Result<HashMap<i64, RevisionMeta>> {
    let mut lookup = HashMap::new();
    if revision_ids.is_empty() || pending_months.is_empty() {
        return Ok(lookup);
    }

    let candidate_months = collect_nearby_lookup_months(all_month_partitions, pending_months);
    for year_month_key in candidate_months {
        let files = all_month_partitions
            .get(&year_month_key)
            .expect("candidate month should exist in revision partition map");
        for path in files {
            let df = read_parquet_df(path, Some(revision_projection()))?;
            index_revision_lookup_df(&df, revision_ids, &mut lookup)?;
            if lookup.len() >= revision_ids.len() {
                return Ok(lookup);
            }
        }
    }

    Ok(lookup)
}

fn collect_nearby_lookup_months(
    all_month_partitions: &BTreeMap<i32, Vec<PathBuf>>,
    pending_months: &[i32],
) -> Vec<i32> {
    let month_keys: Vec<i32> = all_month_partitions.keys().copied().collect();
    let month_set: HashSet<i32> = month_keys.iter().copied().collect();
    let mut ordered = Vec::new();
    let mut seen = HashSet::new();

    for pending in pending_months {
        for offset in 0..=12 {
            if let Some(candidate) = shift_month_key(*pending, -offset)
                && month_set.contains(&candidate)
                && seen.insert(candidate)
            {
                ordered.push(candidate);
            }
        }
        if seen.insert(*pending) && month_set.contains(pending) {
            ordered.push(*pending);
        }
    }

    ordered.sort_unstable();
    ordered.reverse();
    ordered
}

fn shift_month_key(year_month_key: i32, delta_months: i32) -> Option<i32> {
    let year = year_month_key / 100;
    let month = year_month_key % 100;
    if !(1..=12).contains(&month) {
        return None;
    }
    let absolute = year.checked_mul(12)? + (month - 1) + delta_months;
    if absolute < 0 {
        return None;
    }
    let shifted_year = absolute / 12;
    let shifted_month = (absolute % 12) + 1;
    Some(shifted_year * 100 + shifted_month)
}

fn index_revision_lookup_df(
    df: &DataFrame,
    revision_ids_filter: &HashSet<i64>,
    lookup: &mut HashMap<i64, RevisionMeta>,
) -> Result<()> {
    let revision_ids = df.column("revision_id")?.i64()?;
    let timestamps = df.column("event_timestamp")?.str()?;
    let namespaces = df.column("page_namespace")?.i32()?;
    let bot_by = df.column("event_user_is_bot_by")?.str()?;
    let anonymous = df.column("event_user_is_anonymous")?.bool()?;
    let temporary = df.column("event_user_is_temporary")?.bool()?;
    for idx in 0..df.height() {
        let Some(revision_id) = revision_ids.get(idx) else {
            continue;
        };
        if !revision_ids_filter.contains(&revision_id) {
            continue;
        }
        let Some(timestamp) = timestamps.get(idx) else {
            continue;
        };
        let Some(timestamp_seconds) = parse_timestamp_seconds(timestamp) else {
            continue;
        };
        lookup.insert(
            revision_id,
            RevisionMeta {
                timestamp_seconds,
                year_month_key: parse_year_month_key(timestamp).unwrap_or_default(),
                page_namespace: namespaces.get(idx).unwrap_or_default(),
                user_type: classify_user_type(
                    bot_by.get(idx),
                    anonymous.get(idx).unwrap_or(false),
                    temporary.get(idx).unwrap_or(false),
                ),
            },
        );
    }
    Ok(())
}

fn aggregate_patrol_stats(
    patrol_df: &DataFrame,
    pending_months: &HashSet<i32>,
    revision_lookup: &HashMap<i64, RevisionMeta>,
) -> Result<HashMap<MetricKey, PatrolAccumulator>> {
    let timestamps = patrol_df.column("timestamp")?.str()?;
    let revision_ids = patrol_df.column("current_revision_id")?.i64()?;
    let prev_revision_ids = patrol_df.column("prev_revision_id")?.i64()?;
    let users = patrol_df.column("user")?.str()?;
    let mut stats: HashMap<MetricKey, PatrolAccumulator> = HashMap::new();

    for idx in 0..patrol_df.height() {
        let Some(timestamp) = timestamps.get(idx) else {
            continue;
        };
        let Some(year_month_key) = parse_year_month_key(timestamp) else {
            continue;
        };
        if !pending_months.contains(&year_month_key) {
            continue;
        }
        let revision_id = revision_ids.get(idx).unwrap_or_default();
        let meta = revision_lookup.get(&revision_id).copied();
        let key = MetricKey {
            year_month_key,
            page_namespace: meta.map(|entry| entry.page_namespace).unwrap_or_default(),
            user_type: meta
                .map(|entry| entry.user_type)
                .unwrap_or(UserType::Registered),
        };
        let accumulator = stats.entry(key).or_default();
        accumulator.total_patrols += 1;
        if prev_revision_ids.get(idx).unwrap_or_default() == 0 {
            accumulator.patrol_new_pages += 1;
        } else {
            accumulator.patrol_diffs += 1;
        }
        if let Some(user) = users.get(idx) {
            *accumulator.user_counts.entry(user.to_string()).or_insert(0) += 1;
        }
        record_patrol_latency(accumulator, meta.as_ref(), timestamp);
    }

    Ok(stats)
}

fn record_patrol_latency(
    accumulator: &mut PatrolAccumulator,
    meta: Option<&RevisionMeta>,
    timestamp: &str,
) {
    let Some(meta) = meta else {
        return;
    };
    let Some(patrol_seconds) = parse_timestamp_seconds(timestamp) else {
        return;
    };
    if patrol_seconds <= meta.timestamp_seconds {
        return;
    }
    let latency_hours = (patrol_seconds - meta.timestamp_seconds) as f64 / 3600.0;
    if latency_hours < 8_760.0 {
        accumulator.latencies_hours.push(latency_hours);
    }
}

fn write_patrol_month_parts(
    output_dir: &Path,
    wiki: &str,
    pending_months: &[i32],
    summary: &RevisionSummary,
    patrol_stats: &HashMap<MetricKey, PatrolAccumulator>,
) -> Result<()> {
    for year_month_key in pending_months {
        let rows = patrol_month_rows(*year_month_key, summary, patrol_stats);
        let path = patrol_part_path(output_dir, wiki, *year_month_key);
        ensure_parent_dir(&path)?;
        let temp_path = path.with_extension("parquet.tmp");
        write_patrol_metrics_df(&temp_path, wiki, &rows)?;
        fs::rename(temp_path, path)?;
    }

    Ok(())
}

fn patrol_month_rows(
    year_month_key: i32,
    summary: &RevisionSummary,
    patrol_stats: &HashMap<MetricKey, PatrolAccumulator>,
) -> Vec<(MetricKey, PatrolRowMetrics)> {
    let keys: BTreeSet<MetricKey> = patrol_stats
        .keys()
        .copied()
        .chain(summary.total_revisions.keys().copied())
        .filter(|key| key.year_month_key == year_month_key)
        .collect();
    keys.into_iter()
        .map(|key| {
            let patrol = patrol_stats.get(&key);
            let total_revisions = summary
                .total_revisions
                .get(&key)
                .copied()
                .unwrap_or_default();
            let patrolled_revisions = summary
                .patrolled_revisions
                .get(&key)
                .copied()
                .unwrap_or_default();
            let autopatrolled_revisions = summary
                .autopatrolled_revisions
                .get(&key)
                .copied()
                .unwrap_or_default();
            (
                key,
                PatrolRowMetrics::from_parts(
                    patrol,
                    total_revisions,
                    patrolled_revisions,
                    autopatrolled_revisions,
                ),
            )
        })
        .collect()
}

#[derive(Clone, Debug, Default)]
struct PatrolRowMetrics {
    total_patrols: u64,
    unique_patrollers: u32,
    patrol_new_pages: u64,
    patrol_diffs: u64,
    median_latency_hours: Option<f64>,
    p90_latency_hours: Option<f64>,
    patrolled_revisions: u64,
    autopatrolled_revisions: u64,
    total_revisions: u64,
    patrol_coverage_pct: f64,
    adjusted_coverage_pct: f64,
    top1_pct: f64,
    min_patrollers_50pct: u32,
}

impl PatrolRowMetrics {
    fn from_parts(
        patrol: Option<&PatrolAccumulator>,
        total_revisions: u64,
        patrolled_revisions: u64,
        autopatrolled_revisions: u64,
    ) -> Self {
        let mut latencies = patrol
            .map(|entry| entry.latencies_hours.clone())
            .unwrap_or_default();
        latencies.sort_by(f64::total_cmp);
        // Lower-median convention: for an even-length vec, return the lower of
        // the two middle elements rather than the upper. Matches the standard
        // statistics convention. Empty vec → None.
        let median_latency_hours = if latencies.is_empty() {
            None
        } else {
            latencies.get((latencies.len() - 1) / 2).copied()
        };
        let p90_latency_hours = if latencies.is_empty() {
            None
        } else {
            let index = ((latencies.len() as f64) * 0.9).floor() as usize;
            latencies.get(index.min(latencies.len() - 1)).copied()
        };
        let (unique_patrollers, top1_pct, min_patrollers_50pct) = patrol
            .map(summarize_patroller_concentration)
            .unwrap_or((0, 0.0, 0));

        let patrol_coverage_pct = if total_revisions == 0 {
            0.0
        } else {
            patrolled_revisions as f64 / total_revisions as f64 * 100.0
        };
        let adjusted_coverage_pct = if total_revisions == 0 {
            0.0
        } else {
            (patrolled_revisions + autopatrolled_revisions) as f64 / total_revisions as f64 * 100.0
        };

        Self {
            total_patrols: patrol.map(|entry| entry.total_patrols).unwrap_or_default(),
            unique_patrollers,
            patrol_new_pages: patrol
                .map(|entry| entry.patrol_new_pages)
                .unwrap_or_default(),
            patrol_diffs: patrol.map(|entry| entry.patrol_diffs).unwrap_or_default(),
            median_latency_hours,
            p90_latency_hours,
            patrolled_revisions,
            autopatrolled_revisions,
            total_revisions,
            patrol_coverage_pct,
            adjusted_coverage_pct,
            top1_pct,
            min_patrollers_50pct,
        }
    }
}

fn write_patrol_metrics_df(
    path: &Path,
    wiki: &str,
    rows: &[(MetricKey, PatrolRowMetrics)],
) -> Result<()> {
    let mut df = patrol_metrics_frame(wiki, rows)?;
    let mut file = File::create(path)?;
    ParquetWriter::new(&mut file)
        .with_compression(ParquetCompression::Zstd(None))
        .finish(&mut df)?;
    Ok(())
}

fn patrol_metrics_frame(wiki: &str, rows: &[(MetricKey, PatrolRowMetrics)]) -> Result<DataFrame> {
    let year_month: Vec<String> = rows
        .iter()
        .map(|(key, _)| format_year_month(key.year_month_key))
        .collect();
    let wiki_values: Vec<&str> = rows.iter().map(|_| wiki).collect();
    let page_namespace: Vec<i32> = rows.iter().map(|(key, _)| key.page_namespace).collect();
    let user_type: Vec<&str> = rows.iter().map(|(key, _)| key.user_type.as_str()).collect();
    let total_patrols: Vec<i64> = rows
        .iter()
        .map(|(_, row)| row.total_patrols as i64)
        .collect();
    let unique_patrollers: Vec<i32> = rows
        .iter()
        .map(|(_, row)| row.unique_patrollers as i32)
        .collect();
    let patrol_new_pages: Vec<i64> = rows
        .iter()
        .map(|(_, row)| row.patrol_new_pages as i64)
        .collect();
    let patrol_diffs: Vec<i64> = rows
        .iter()
        .map(|(_, row)| row.patrol_diffs as i64)
        .collect();
    let median_latency_hours: Vec<Option<f64>> = rows
        .iter()
        .map(|(_, row)| row.median_latency_hours)
        .collect();
    let p90_latency_hours: Vec<Option<f64>> =
        rows.iter().map(|(_, row)| row.p90_latency_hours).collect();
    let patrolled_revisions: Vec<i64> = rows
        .iter()
        .map(|(_, row)| row.patrolled_revisions as i64)
        .collect();
    let autopatrolled_revisions: Vec<i64> = rows
        .iter()
        .map(|(_, row)| row.autopatrolled_revisions as i64)
        .collect();
    let total_revisions: Vec<i64> = rows
        .iter()
        .map(|(_, row)| row.total_revisions as i64)
        .collect();
    let patrol_coverage_pct: Vec<f64> = rows
        .iter()
        .map(|(_, row)| round1(row.patrol_coverage_pct))
        .collect();
    let adjusted_coverage_pct: Vec<f64> = rows
        .iter()
        .map(|(_, row)| round1(row.adjusted_coverage_pct))
        .collect();
    let top1_pct: Vec<f64> = rows.iter().map(|(_, row)| round1(row.top1_pct)).collect();
    let min_patrollers_50pct: Vec<i32> = rows
        .iter()
        .map(|(_, row)| row.min_patrollers_50pct as i32)
        .collect();

    let columns = vec![
        Column::new("year_month".into(), year_month),
        Column::new("wiki".into(), wiki_values),
        Column::new("page_namespace".into(), page_namespace),
        Column::new("user_type".into(), user_type),
        Column::new("total_patrols".into(), total_patrols),
        Column::new("unique_patrollers".into(), unique_patrollers),
        Column::new("patrol_new_pages".into(), patrol_new_pages),
        Column::new("patrol_diffs".into(), patrol_diffs),
        Column::new("median_latency_hours".into(), median_latency_hours),
        Column::new("p90_latency_hours".into(), p90_latency_hours),
        Column::new("patrolled_revisions".into(), patrolled_revisions),
        Column::new("autopatrolled_revisions".into(), autopatrolled_revisions),
        Column::new("total_revisions".into(), total_revisions),
        Column::new("patrol_coverage_pct".into(), patrol_coverage_pct),
        Column::new("adjusted_coverage_pct".into(), adjusted_coverage_pct),
        Column::new("top1_pct".into(), top1_pct),
        Column::new("min_patrollers_50pct".into(), min_patrollers_50pct),
    ];
    DataFrame::new_infer_height(columns).map_err(Into::into)
}

fn summarize_patroller_concentration(entry: &PatrolAccumulator) -> (u32, f64, u32) {
    let unique = entry.user_counts.len() as u32;
    if entry.total_patrols == 0 {
        return (unique, 0.0, 0);
    }
    let mut counts: Vec<u32> = entry.user_counts.values().copied().collect();
    counts.sort_unstable_by(|left, right| right.cmp(left));
    let top1 =
        counts.first().copied().unwrap_or_default() as f64 / entry.total_patrols as f64 * 100.0;
    let min50 = min_patrollers_for_half_share(&counts, entry.total_patrols);
    (unique, top1, min50)
}

fn min_patrollers_for_half_share(counts: &[u32], total_patrols: u64) -> u32 {
    let threshold = total_patrols as f64 * 0.5;
    let mut cumulative = 0_u64;
    for (index, count) in counts.iter().enumerate() {
        cumulative += *count as u64;
        if cumulative as f64 >= threshold {
            return (index + 1) as u32;
        }
    }
    counts.len() as u32
}

fn patrol_parts_dir(output_dir: &Path, wiki: &str) -> PathBuf {
    output_dir.join(wiki).join("_patrol_parts")
}

/// Wipe the per-wiki `_patrol_parts` directory if it exists. Used by the
/// `--rebuild` path of `compute_patrol` to ensure stale month parts from a
/// prior run are not silently mixed with the recomputed output. Idempotent:
/// a missing directory is treated as a no-op.
fn clear_patrol_parts_dir(output_dir: &Path, wiki: &str) -> Result<()> {
    let parts_dir = patrol_parts_dir(output_dir, wiki);
    if !parts_dir.exists() {
        return Ok(());
    }
    fs::remove_dir_all(&parts_dir)?;
    Ok(())
}

fn patrol_part_path(output_dir: &Path, wiki: &str, year_month_key: i32) -> PathBuf {
    patrol_parts_dir(output_dir, wiki)
        .join(format!("{}.parquet", format_year_month(year_month_key)))
}

fn existing_patrol_months(output_dir: &Path, wiki: &str) -> Result<BTreeSet<i32>> {
    let parts_dir = patrol_parts_dir(output_dir, wiki);
    if !parts_dir.exists() {
        return Ok(BTreeSet::new());
    }
    let mut months = BTreeSet::new();
    for entry in fs::read_dir(parts_dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("parquet") {
            continue;
        }
        if let Some(month) = path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .and_then(parse_year_month_key)
        {
            months.insert(month);
        }
    }
    Ok(months)
}

fn bootstrap_patrol_parts_from_final(output_dir: &Path, wiki: &str) -> Result<()> {
    let parts_dir = patrol_parts_dir(output_dir, wiki);
    // Only treat the parts dir as already-bootstrapped if at least one
    // committed `.parquet` exists. Leftover `.parquet.tmp` files from a
    // previously interrupted run are removed here so the retry can lay down
    // a clean rename target rather than being blocked indefinitely.
    if committed_patrol_parts_exist(&parts_dir)? {
        return Ok(());
    }
    if parts_dir.exists() {
        info!(
            wiki,
            "patrol parts directory has no committed parquet; bootstrapping"
        );
    }
    let final_path = output_dir.join(wiki).join("patrol.parquet");
    if !final_path.exists() {
        return Ok(());
    }
    let df = read_parquet_df(&final_path, None)?;
    let year_months = df.column("year_month")?.str()?;
    let mut months = BTreeSet::new();
    for idx in 0..df.height() {
        if let Some(month) = year_months.get(idx).and_then(parse_year_month_key) {
            months.insert(month);
        }
    }
    for month in months {
        let month_string = format_year_month(month);
        let mask = df.column("year_month")?.str()?.equal(month_string.as_str());
        let month_df = df.filter(&mask)?;
        let final_path = patrol_part_path(output_dir, wiki, month);
        ensure_parent_dir(&final_path)?;
        let temp_path = final_path.with_extension("parquet.tmp");
        let mut month_df = month_df;
        {
            let mut file = File::create(&temp_path)?;
            ParquetWriter::new(&mut file)
                .with_compression(ParquetCompression::Zstd(None))
                .finish(&mut month_df)?;
        }
        fs::rename(&temp_path, &final_path)?;
    }
    Ok(())
}

fn committed_patrol_parts_exist(parts_dir: &Path) -> Result<bool> {
    if !parts_dir.exists() {
        return Ok(false);
    }
    let mut has_parquet = false;
    for entry in fs::read_dir(parts_dir)? {
        let path = entry?.path();
        match path.extension().and_then(|ext| ext.to_str()) {
            Some("parquet") => has_parquet = true,
            Some("tmp") => {
                let _ = fs::remove_file(path);
            }
            _ => {}
        }
    }
    Ok(has_parquet)
}

fn merge_wiki_patrol_parts(output_dir: &Path, wiki: &str) -> Result<Option<PathBuf>> {
    let parts_dir = patrol_parts_dir(output_dir, wiki);
    if !parts_dir.exists() {
        return Ok(None);
    }
    let mut part_files = storage::collect_parquet_files(&parts_dir)?;
    if part_files.is_empty() {
        return Ok(None);
    }
    part_files.sort();
    let out_dir = output_dir.join(wiki);
    fs::create_dir_all(&out_dir)?;
    let out_path = out_dir.join("patrol.parquet");
    crate::merge::merge_metric_batched("patrol.parquet", &part_files, &out_path, 250_000, None)?;
    Ok(Some(out_path))
}

fn refresh_patrol_dashboard_artifacts(
    output_dir: &Path,
    _wiki_output: Option<&Path>,
) -> Result<()> {
    let mut metric_files: Vec<PathBuf> = fs::read_dir(output_dir)?
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.file_type().map(|ty| ty.is_dir()).unwrap_or(false))
        .map(|entry| entry.path().join("patrol.parquet"))
        .filter(|path| path.exists())
        .collect();
    if metric_files.is_empty() {
        return Ok(());
    }
    metric_files.sort();
    let merged_path = output_dir.join("patrol.parquet");
    crate::merge::merge_metric_batched(
        "patrol.parquet",
        &metric_files,
        &merged_path,
        250_000,
        None,
    )?;
    Ok(())
}

fn build_autopatrol_intervals(
    rights_path: &Path,
    autopatrol_groups: &[String],
) -> Result<AutopatrolIntervals> {
    if !rights_path.exists() || autopatrol_groups.is_empty() {
        return Ok(HashMap::new());
    }
    let df = read_parquet_df(rights_path, None)?;
    let timestamps = df.column("timestamp")?.str()?;
    let users = df.column("target_user")?.str()?;
    let old_groups = df.column("old_groups")?.str()?;
    let new_groups = df.column("new_groups")?.str()?;
    let autopatrol_groups: HashSet<&str> = autopatrol_groups.iter().map(String::as_str).collect();
    let mut events: HashMap<String, Vec<(i64, bool)>> = HashMap::new();

    for idx in 0..df.height() {
        let Some(username) = users.get(idx) else {
            continue;
        };
        let Some(timestamp) = timestamps.get(idx).and_then(parse_timestamp_seconds) else {
            continue;
        };
        let old_has =
            split_groups(old_groups.get(idx)).any(|group| autopatrol_groups.contains(group));
        let new_has =
            split_groups(new_groups.get(idx)).any(|group| autopatrol_groups.contains(group));
        if old_has == new_has {
            continue;
        }
        events
            .entry(username.to_string())
            .or_default()
            .push((timestamp, new_has));
    }

    let mut intervals = HashMap::new();
    for (username, mut user_events) in events {
        user_events.sort_unstable_by_key(|(timestamp, _)| *timestamp);
        let mut current_start = None;
        let mut user_intervals = Vec::new();
        for (timestamp, has_autopatrol) in user_events {
            if has_autopatrol && current_start.is_none() {
                current_start = Some(timestamp);
            } else if !has_autopatrol && let Some(start) = current_start.take() {
                user_intervals.push((start, Some(timestamp)));
            }
        }
        if let Some(start) = current_start {
            user_intervals.push((start, None));
        }
        if !user_intervals.is_empty() {
            intervals.insert(username, user_intervals);
        }
    }

    Ok(intervals)
}

fn split_groups(value: Option<&str>) -> impl Iterator<Item = &str> {
    value
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|group| !group.is_empty())
}

fn user_has_autopatrol_at(
    intervals: &AutopatrolIntervals,
    username: &str,
    timestamp_seconds: i64,
) -> bool {
    intervals
        .get(username)
        .into_iter()
        .flatten()
        .any(|(start, end)| {
            timestamp_seconds >= *start && end.is_none_or(|end| timestamp_seconds < end)
        })
}

fn parse_year_month_key(value: &str) -> Option<i32> {
    let bytes = value.as_bytes();
    if bytes.len() < 7 {
        return None;
    }
    let year = value.get(0..4)?.parse::<i32>().ok()?;
    let month = value.get(5..7)?.parse::<i32>().ok()?;
    Some(year * 100 + month)
}

fn format_year_month(year_month_key: i32) -> String {
    let year = year_month_key / 100;
    let month = year_month_key % 100;
    format!("{year:04}-{month:02}")
}

fn parse_timestamp_seconds(value: &str) -> Option<i64> {
    // MediaWiki dump timestamps are documented as UTC, so we attach UTC
    // here without consulting a tz database. `chrono` is built with
    // `default-features = false` and ships no tz database in this binary;
    // any future need to interpret non-UTC timestamps must be a
    // deliberate change to both this call site and the chrono feature
    // flags in Cargo.toml.
    let normalized = normalize_timestamp(value);
    NaiveDateTime::parse_from_str(&normalized, "%Y-%m-%d %H:%M:%S")
        .ok()
        .map(|ts| ts.and_utc().timestamp())
}

fn classify_user_type(
    event_user_is_bot_by: Option<&str>,
    event_user_is_anonymous: bool,
    event_user_is_temporary: bool,
) -> UserType {
    if event_user_is_bot_by.is_some_and(|value| !value.is_empty() && value != "false") {
        return UserType::Bot;
    }
    if event_user_is_anonymous {
        return UserType::Anonymous;
    }
    if event_user_is_temporary {
        return UserType::Temporary;
    }
    UserType::Registered
}

fn round1(value: f64) -> f64 {
    (value * 10.0).round() / 10.0
}

mod generation;
mod incremental;
mod plan;

#[cfg(test)]
mod tests;
