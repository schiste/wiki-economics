use anyhow::{Context, Result};
use indicatif::{ProgressBar, ProgressStyle};
use rayon::prelude::*;
use reqwest::StatusCode;
use reqwest::header::{
    ACCEPT_RANGES, CONTENT_LENGTH, ETAG, HeaderMap, LAST_MODIFIED, RANGE, RETRY_AFTER,
};
use reqwest::redirect::Policy;
use serde::{Deserialize, Serialize};
use std::ffi::OsStr;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::{debug, info, warn};

use crate::fingerprint::{self, StageSpec, TrackedPath};
use crate::snapshot_plan::{MEDIAWIKI_HISTORY_BASE_URL, SnapshotPlan, SourceSpec};
#[cfg(test)]
use crate::snapshot_plan::{MONTHLY_WIKIS, YEARLY_WIKIS};

const BASE_URL: &str = MEDIAWIKI_HISTORY_BASE_URL;
pub(crate) const DUMPS_HOST: &str = "dumps.wikimedia.org";
const USER_AGENT: &str = "wiki-econ/0.1 (Wikipedia economic analysis research tool)";
/// dumps.wikimedia.org kept 429ing this client even after the Retry-After
/// fix landed, with 4 concurrent HEAD/GET streams in flight. Serialized to 1
/// so retries (below) get a real chance to land in a quiet window instead of
/// every worker re-triggering the same rate limit at once.
const FETCH_MAX_PARALLELISM: usize = 1;
const FETCH_MAX_RETRIES: usize = 6;
const FETCH_RETRY_BACKOFF_MS: u64 = 500;
/// Backoff base for 429 (rate-limited) retries when the server didn't send
/// a `Retry-After` header. Longer than `FETCH_RETRY_BACKOFF_MS`: a 429 means
/// the server is actively asking us to slow down, which the general-purpose
/// schedule (used for timeouts/5xx) doesn't respect.
const FETCH_RATE_LIMIT_BACKOFF_MS: u64 = 4_000;
/// Ceiling on the computed exponential backoff (not applied to an honored
/// `Retry-After` value, which has its own `FETCH_RETRY_AFTER_MAX_SECS`
/// clamp). Without this, doubling per attempt across `FETCH_MAX_RETRIES`
/// attempts reaches multi-minute sleeps long before the loop gives up.
const FETCH_MAX_BACKOFF_MS: u64 = 30_000;
/// Maximum delay honored from an upstream `Retry-After` header, so a
/// slow/misconfigured server value can't stall a retry loop indefinitely.
const FETCH_RETRY_AFTER_MAX_SECS: u64 = 30;
const FETCH_MAX_PARALLELISM_ENV: &str = "WIKI_ECON_FETCH_MAX_PARALLELISM";
const SNAPSHOT_MAX_LAG_ENV: &str = "WIKI_ECON_MAX_SNAPSHOT_LAG_MONTHS";
const DEFAULT_SNAPSHOT_MAX_LAG_MONTHS: u32 = 2;
const REMOTE_INVENTORY_SCHEMA_VERSION: u32 = 1;
const REMOTE_INVENTORY_FILENAME: &str = "remote-inventory.json";
const FETCH_ALGORITHM_VERSION: &str = "wikimedia-history-fetch-v4-source-window";
const SOURCE_WINDOW_STAGING_DIR: &str = ".source-window";
const SOURCE_WINDOW_DOWNLOAD_SUFFIX: &str = ".download";
/// Extra headroom required beyond the summed remote byte total before a
/// fetch is allowed to start. Not a tight budget — just enough to fail fast
/// when shared storage lacks enough space for a workload (e.g. frwiki's ~31GB
/// transient estimate on Toolforge) rather than discovering it after
/// downloading most of the dump.
const FETCH_DISK_HEADROOM_MARGIN_BYTES: u64 = 2 * 1024 * 1024 * 1024;
/// Bzip2 magic bytes ("BZh"). Every valid bz2 file begins with these three
/// bytes before the version digit. Used to surface CDN corruption / truncation
/// at fetch time, before the file is moved into the ingest pipeline. Note: the
/// upstream `mediawiki_history` dump path does NOT publish checksums (no
/// dumpstatus.json / sha1sums.txt), so end-to-end SHA verification is not
/// possible; magic-byte validation is the cheapest meaningful integrity gate
/// we can apply on top of TLS.
const BZ2_MAGIC: &[u8] = b"BZh";

/// Complete list of Wikipedia language-edition database names exposed by the
/// admin picker. Sourced from the canonical Wikimedia sitematrix and pruned
/// to wikis with a published `mediawiki_history` directory. Snapshot planning
/// decides yearly vs all-time vs monthly per-wiki; this list is purely the
/// picker's universe.
///
/// The Rust binary itself does not reference this constant directly; the
/// admin server in `site/admin-server.cjs` reads it via a regex scrape of
/// this source file, which is why the dead-code lint is suppressed.
#[allow(dead_code)]
pub(crate) const WIKIPEDIA_DATABASES: &[&str] = &[
    "aawiki",
    "abwiki",
    "acewiki",
    "adywiki",
    "afwiki",
    "akwiki",
    "alswiki",
    "altwiki",
    "amiwiki",
    "amwiki",
    "angwiki",
    "annwiki",
    "anpwiki",
    "anwiki",
    "arcwiki",
    "arwiki",
    "arywiki",
    "arzwiki",
    "astwiki",
    "aswiki",
    "atjwiki",
    "avkwiki",
    "avwiki",
    "awawiki",
    "aywiki",
    "azbwiki",
    "azwiki",
    "banwiki",
    "barwiki",
    "bat_smgwiki",
    "bawiki",
    "bbcwiki",
    "bclwiki",
    "bdrwiki",
    "be_x_oldwiki",
    "bewiki",
    "bewwiki",
    "bgwiki",
    "bhwiki",
    "biwiki",
    "bjnwiki",
    "blkwiki",
    "bmwiki",
    "bnwiki",
    "bowiki",
    "bpywiki",
    "brwiki",
    "bswiki",
    "btmwiki",
    "bugwiki",
    "bxrwiki",
    "cawiki",
    "cbk_zamwiki",
    "cdowiki",
    "cebwiki",
    "cewiki",
    "chowiki",
    "chrwiki",
    "chwiki",
    "chywiki",
    "ckbwiki",
    "cowiki",
    "crhwiki",
    "crwiki",
    "csbwiki",
    "cswiki",
    "cuwiki",
    "cvwiki",
    "cywiki",
    "dagwiki",
    "dawiki",
    "dewiki",
    "dgawiki",
    "dinwiki",
    "diqwiki",
    "dsbwiki",
    "dtpwiki",
    "dtywiki",
    "dvwiki",
    "dzwiki",
    "eewiki",
    "elwiki",
    "emlwiki",
    "enwiki",
    "eowiki",
    "eswiki",
    "etwiki",
    "euwiki",
    "extwiki",
    "fatwiki",
    "fawiki",
    "ffwiki",
    "fiu_vrowiki",
    "fiwiki",
    "fjwiki",
    "fonwiki",
    "fowiki",
    "frpwiki",
    "frrwiki",
    "frwiki",
    "furwiki",
    "fywiki",
    "gagwiki",
    "ganwiki",
    "gawiki",
    "gcrwiki",
    "gdwiki",
    "glkwiki",
    "glwiki",
    "gnwiki",
    "gomwiki",
    "gorwiki",
    "gotwiki",
    "gpewiki",
    "gucwiki",
    "gurwiki",
    "guwiki",
    "guwwiki",
    "gvwiki",
    "hakwiki",
    "hawiki",
    "hawwiki",
    "hewiki",
    "hifwiki",
    "hiwiki",
    "howiki",
    "hrwiki",
    "hsbwiki",
    "htwiki",
    "huwiki",
    "hywiki",
    "hywwiki",
    "hzwiki",
    "iawiki",
    "ibawiki",
    "idwiki",
    "iewiki",
    "iglwiki",
    "igwiki",
    "iiwiki",
    "ikwiki",
    "ilowiki",
    "inhwiki",
    "iowiki",
    "iswiki",
    "itwiki",
    "iuwiki",
    "jamwiki",
    "jawiki",
    "jbowiki",
    "jvwiki",
    "kaawiki",
    "kabwiki",
    "kajwiki",
    "kawiki",
    "kbdwiki",
    "kbpwiki",
    "kcgwiki",
    "kgewiki",
    "kgwiki",
    "kiwiki",
    "kjwiki",
    "kkwiki",
    "klwiki",
    "kmwiki",
    "kncwiki",
    "knwiki",
    "koiwiki",
    "kowiki",
    "krcwiki",
    "krwiki",
    "kshwiki",
    "kswiki",
    "kuswiki",
    "kuwiki",
    "kvwiki",
    "kwwiki",
    "kywiki",
    "ladwiki",
    "lawiki",
    "lbewiki",
    "lbwiki",
    "lezwiki",
    "lfnwiki",
    "lgwiki",
    "lijwiki",
    "liwiki",
    "lldwiki",
    "lmowiki",
    "lnwiki",
    "lowiki",
    "lrcwiki",
    "ltgwiki",
    "ltwiki",
    "lvwiki",
    "madwiki",
    "maiwiki",
    "map_bmswiki",
    "mdfwiki",
    "mgwiki",
    "mhrwiki",
    "mhwiki",
    "minwiki",
    "miwiki",
    "mkwiki",
    "mlwiki",
    "mniwiki",
    "mnwiki",
    "mnwwiki",
    "moswiki",
    "mrjwiki",
    "mrwiki",
    "mswiki",
    "mtwiki",
    "muswiki",
    "mwlwiki",
    "myvwiki",
    "mywiki",
    "mznwiki",
    "nahwiki",
    "napwiki",
    "nawiki",
    "nds_nlwiki",
    "ndswiki",
    "newiki",
    "newwiki",
    "ngwiki",
    "niawiki",
    "nlwiki",
    "nnwiki",
    "novwiki",
    "nowiki",
    "nqowiki",
    "nrmwiki",
    "nrwiki",
    "nsowiki",
    "nupwiki",
    "nvwiki",
    "nywiki",
    "ocwiki",
    "olowiki",
    "omwiki",
    "orwiki",
    "oswiki",
    "pagwiki",
    "pamwiki",
    "papwiki",
    "pawiki",
    "pcdwiki",
    "pcmwiki",
    "pdcwiki",
    "pflwiki",
    "pihwiki",
    "piwiki",
    "plwiki",
    "pmswiki",
    "pnbwiki",
    "pntwiki",
    "pplwiki",
    "pswiki",
    "ptwiki",
    "pwnwiki",
    "quwiki",
    "rkiwiki",
    "rmwiki",
    "rmywiki",
    "rnwiki",
    "roa_rupwiki",
    "roa_tarawiki",
    "rowiki",
    "rskwiki",
    "ruewiki",
    "ruwiki",
    "rwwiki",
    "sahwiki",
    "satwiki",
    "sawiki",
    "scnwiki",
    "scowiki",
    "scwiki",
    "sdwiki",
    "sewiki",
    "sgwiki",
    "shiwiki",
    "shnwiki",
    "shwiki",
    "simplewiki",
    "siwiki",
    "skrwiki",
    "skwiki",
    "slwiki",
    "smnwiki",
    "smwiki",
    "snwiki",
    "sowiki",
    "sqwiki",
    "srnwiki",
    "srwiki",
    "sswiki",
    "stqwiki",
    "stwiki",
    "suwiki",
    "svwiki",
    "swwiki",
    "sylwiki",
    "szlwiki",
    "szywiki",
    "tawiki",
    "taywiki",
    "tcywiki",
    "tddwiki",
    "tetwiki",
    "tewiki",
    "tgwiki",
    "thwiki",
    "tigwiki",
    "tiwiki",
    "tkwiki",
    "tlwiki",
    "tlywiki",
    "tnwiki",
    "tokwiki",
    "towiki",
    "tpiwiki",
    "trvwiki",
    "trwiki",
    "tswiki",
    "ttwiki",
    "tumwiki",
    "twwiki",
    "tyvwiki",
    "tywiki",
    "udmwiki",
    "ugwiki",
    "ukwiki",
    "urwiki",
    "uzwiki",
    "vecwiki",
    "vepwiki",
    "vewiki",
    "viwiki",
    "vlswiki",
    "vowiki",
    "warwiki",
    "wawiki",
    "wowiki",
    "wuuwiki",
    "xalwiki",
    "xhwiki",
    "xmfwiki",
    "yiwiki",
    "yowiki",
    "zawiki",
    "zeawiki",
    "zghwiki",
    "zh_classicalwiki",
    "zh_min_nanwiki",
    "zh_yuewiki",
    "zhwiki",
    "zuwiki",
];

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct RemoteFileInfo {
    content_length: Option<u64>,
    accepts_ranges: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct DownloadPlan {
    resume_from: u64,
    total_size: Option<u64>,
    accepts_ranges: bool,
}

#[derive(Debug)]
struct AttemptError {
    error: anyhow::Error,
    retryable: bool,
    rate_limited: bool,
    retry_after: Option<Duration>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct TransportHead {
    status: StatusCode,
    content_length: Option<u64>,
    accepts_ranges: bool,
    retry_after: Option<Duration>,
    etag: Option<String>,
    last_modified: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct RemoteInventorySource {
    source_id: String,
    url: String,
    content_length: Option<u64>,
    accepts_ranges: bool,
    etag: Option<String>,
    last_modified: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct CompletedSnapshotReceipt {
    schema_version: u32,
    wiki: String,
    snapshot: String,
    plan_sha256: String,
    source_count: usize,
    completion_check_timestamp: u64,
    sources: Vec<RemoteInventorySource>,
}

struct TransportResponse {
    status: StatusCode,
    content_length: Option<u64>,
    retry_after: Option<Duration>,
    body: Box<dyn Read + Send>,
}

trait HttpTransport: Sync {
    fn head(&self, url: &str) -> Result<TransportHead>;
    fn get(&self, url: &str, range_start: Option<u64>) -> Result<TransportResponse>;
}

fn snapshot_version_at_lag(now: chrono::DateTime<chrono::Utc>, lag_months: u32) -> String {
    use chrono::Datelike;

    let current = i64::from(now.year()) * 12 + i64::from(now.month0());
    let target = current - i64::from(lag_months);
    let year = target.div_euclid(12);
    let month = target.rem_euclid(12) + 1;
    format!("{year:04}-{month:02}")
}

fn snapshot_max_lag_months(value: Option<&OsStr>) -> Result<u32> {
    match value {
        None => Ok(DEFAULT_SNAPSHOT_MAX_LAG_MONTHS),
        Some(value) => {
            let parsed = value
                .to_str()
                .context(format!("{SNAPSHOT_MAX_LAG_ENV} is not valid UTF-8"))?
                .parse::<u32>()
                .with_context(|| format!("{SNAPSHOT_MAX_LAG_ENV} must be a positive integer"))?;
            anyhow::ensure!(
                parsed > 0,
                "{SNAPSHOT_MAX_LAG_ENV} must be at least 1 month"
            );
            Ok(parsed)
        }
    }
}

#[cfg(test)]
fn snapshot_source_exists<T: HttpTransport>(transport: &T, url: &str) -> Result<bool> {
    Ok(snapshot_source_head_with_sleep(transport, url, sleep_before_retry)?.is_some())
}

#[cfg(test)]
fn snapshot_source_exists_with_sleep<T, F>(transport: &T, url: &str, mut sleep: F) -> Result<bool>
where
    T: HttpTransport,
    F: FnMut(usize, bool, Option<Duration>),
{
    Ok(snapshot_source_head_with_sleep(transport, url, &mut sleep)?.is_some())
}

fn snapshot_source_head_with_sleep<T, F>(
    transport: &T,
    url: &str,
    mut sleep: F,
) -> Result<Option<TransportHead>>
where
    T: HttpTransport,
    F: FnMut(usize, bool, Option<Duration>),
{
    let mut last_error = None;
    for attempt in 1..=FETCH_MAX_RETRIES {
        let mut rate_limited = false;
        let mut retry_after = None;
        match transport.head(url) {
            Ok(response) if response.status.is_success() => return Ok(Some(response)),
            Ok(response) if response.status == StatusCode::NOT_FOUND => return Ok(None),
            Ok(response) if is_retryable_status(response.status) => {
                rate_limited = response.status == StatusCode::TOO_MANY_REQUESTS;
                retry_after = response.retry_after;
                last_error = Some(anyhow::anyhow!("HTTP {} for {}", response.status, url));
            }
            Ok(response) => anyhow::bail!(
                "cannot determine dump completion: HTTP {} for {}",
                response.status,
                url
            ),
            Err(error) => last_error = Some(error),
        }
        if attempt < FETCH_MAX_RETRIES {
            sleep(attempt, rate_limited, retry_after);
        }
    }
    Err(last_error.unwrap_or_else(|| anyhow::anyhow!("metadata probe failed for {url}")))
}

fn remote_inventory_path(data_dir: &Path, wiki: &str, snapshot: &str) -> Result<PathBuf> {
    Ok(crate::snapshot_plan::plan_path(data_dir, wiki, snapshot)?
        .parent()
        .context("snapshot plan has no state directory")?
        .join(REMOTE_INVENTORY_FILENAME))
}

fn validate_remote_inventory(
    data_dir: &Path,
    plan: &SnapshotPlan,
    receipt: &CompletedSnapshotReceipt,
) -> Result<()> {
    let plan_path =
        crate::snapshot_plan::plan_path(data_dir, plan.wiki.as_str(), plan.snapshot.as_str())?;
    let (_, plan_sha256) = crate::storage::sha256_file(&plan_path)?;
    anyhow::ensure!(
        receipt.schema_version == REMOTE_INVENTORY_SCHEMA_VERSION
            && receipt.wiki == plan.wiki.as_str()
            && receipt.snapshot == plan.snapshot.as_str()
            && receipt.plan_sha256 == plan_sha256
            && receipt.source_count == plan.sources.len()
            && receipt.sources.len() == plan.sources.len(),
        "completed-snapshot receipt identity mismatch"
    );
    anyhow::ensure!(
        receipt.completion_check_timestamp > 0,
        "completed-snapshot receipt has no completion timestamp"
    );
    for (observed, expected) in receipt.sources.iter().zip(&plan.sources) {
        anyhow::ensure!(
            observed.source_id == expected.source_id
                && observed.url == expected.url.as_str()
                && observed.content_length != Some(0),
            "completed-snapshot remote inventory does not match its source plan"
        );
    }
    Ok(())
}

fn read_remote_inventory(
    data_dir: &Path,
    plan: &SnapshotPlan,
) -> Result<Option<CompletedSnapshotReceipt>> {
    let path = remote_inventory_path(data_dir, plan.wiki.as_str(), plan.snapshot.as_str())?;
    if !path.is_file() {
        return Ok(None);
    }
    let receipt: CompletedSnapshotReceipt = match serde_json::from_slice(&fs::read(&path)?) {
        Ok(receipt) => receipt,
        Err(error) => {
            warn!(path = %path.display(), error = %error, "ignoring invalid completed-snapshot receipt");
            return Ok(None);
        }
    };
    if let Err(error) = validate_remote_inventory(data_dir, plan, &receipt) {
        warn!(path = %path.display(), error = %format!("{error:#}"), "ignoring stale completed-snapshot receipt");
        return Ok(None);
    }
    Ok(Some(receipt))
}

fn write_remote_inventory(
    data_dir: &Path,
    plan: &SnapshotPlan,
    receipt: &CompletedSnapshotReceipt,
) -> Result<()> {
    validate_remote_inventory(data_dir, plan, receipt)?;
    let path = remote_inventory_path(data_dir, plan.wiki.as_str(), plan.snapshot.as_str())?;
    let parent = path.parent().context("remote inventory has no parent")?;
    fs::create_dir_all(parent)?;
    let temporary = parent.join(format!(
        ".{REMOTE_INVENTORY_FILENAME}.{}.tmp",
        std::process::id()
    ));
    let result = (|| -> Result<()> {
        let mut file = File::create(&temporary)?;
        serde_json::to_writer_pretty(&mut file, receipt)?;
        file.write_all(b"\n")?;
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

fn snapshot_wiki_is_complete_cached<T: HttpTransport>(
    transport: &T,
    base_url: &str,
    data_dir: &Path,
    wiki: &str,
    version: &str,
) -> Result<bool> {
    let plan = SnapshotPlan::resolve_from_base(base_url, wiki, version)?;
    plan.persist(data_dir)?;
    if read_remote_inventory(data_dir, &plan)?.is_some() {
        info!(wiki, version, "reusing completed-snapshot remote inventory");
        return Ok(true);
    }
    let mut sources = Vec::with_capacity(plan.sources.len());
    for source in plan.sources.iter().rev() {
        let Some(head) =
            snapshot_source_head_with_sleep(transport, source.url.as_str(), sleep_before_retry)?
        else {
            info!(
                wiki,
                version,
                source = source.source_id,
                "snapshot is not complete"
            );
            return Ok(false);
        };
        sources.push(RemoteInventorySource {
            source_id: source.source_id.clone(),
            url: source.url.to_string(),
            content_length: head.content_length,
            accepts_ranges: head.accepts_ranges,
            etag: head.etag,
            last_modified: head.last_modified,
        });
    }
    sources.reverse();
    let plan_path = crate::snapshot_plan::plan_path(data_dir, wiki, version)?;
    let (_, plan_sha256) = crate::storage::sha256_file(&plan_path)?;
    let receipt = CompletedSnapshotReceipt {
        schema_version: REMOTE_INVENTORY_SCHEMA_VERSION,
        wiki: wiki.to_string(),
        snapshot: version.to_string(),
        plan_sha256,
        source_count: plan.sources.len(),
        completion_check_timestamp: SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
        sources,
    };
    write_remote_inventory(data_dir, &plan, &receipt)?;
    Ok(true)
}

fn snapshot_is_complete_cached<T: HttpTransport>(
    transport: &T,
    base_url: &str,
    data_dir: &Path,
    wikis: &[String],
    version: &str,
) -> Result<bool> {
    for wiki in wikis {
        if !snapshot_wiki_is_complete_cached(transport, base_url, data_dir, wiki, version)? {
            return Ok(false);
        }
    }
    Ok(true)
}

#[cfg(test)]
fn snapshot_is_complete<T: HttpTransport>(
    transport: &T,
    base_url: &str,
    wikis: &[String],
    version: &str,
) -> Result<bool> {
    for wiki in wikis {
        let plan = SnapshotPlan::resolve_from_base(base_url, wiki, version)?;
        for source in plan.sources.iter().rev() {
            if !snapshot_source_exists(transport, source.url.as_str())? {
                info!(
                    wiki,
                    version,
                    source = source.source_id,
                    "snapshot is not complete"
                );
                return Ok(false);
            }
        }
    }
    Ok(true)
}

#[cfg(test)]
fn resolve_latest_completed_snapshot_with_transport<T: HttpTransport>(
    transport: &T,
    base_url: &str,
    wikis: &[String],
    now: chrono::DateTime<chrono::Utc>,
    max_lag_months: u32,
) -> Result<String> {
    anyhow::ensure!(
        !wikis.is_empty(),
        "snapshot resolution requires at least one wiki"
    );
    anyhow::ensure!(max_lag_months > 0, "maximum snapshot lag must be positive");
    for lag_months in 1..=max_lag_months {
        let version = snapshot_version_at_lag(now, lag_months);
        if snapshot_is_complete(transport, base_url, wikis, &version)? {
            if lag_months > 1 {
                warn!(
                    version,
                    lag_months, "latest expected snapshot is incomplete; using bounded fallback"
                );
            }
            info!(version, lag_months, "selected completed Wikimedia snapshot");
            return Ok(version);
        }
    }
    anyhow::bail!(
        "no completed Wikimedia snapshot found for {} within the configured {} month lag",
        wikis.join(","),
        max_lag_months
    )
}

fn resolve_latest_completed_snapshot_cached_with_transport<T: HttpTransport>(
    transport: &T,
    base_url: &str,
    data_dir: &Path,
    wikis: &[String],
    now: chrono::DateTime<chrono::Utc>,
    max_lag_months: u32,
) -> Result<String> {
    anyhow::ensure!(
        !wikis.is_empty(),
        "snapshot resolution requires at least one wiki"
    );
    anyhow::ensure!(max_lag_months > 0, "maximum snapshot lag must be positive");
    for lag_months in 1..=max_lag_months {
        let version = snapshot_version_at_lag(now, lag_months);
        if snapshot_is_complete_cached(transport, base_url, data_dir, wikis, &version)? {
            let _ = (lag_months > 1).then(|| {
                warn!(
                    version,
                    lag_months, "latest expected snapshot is incomplete; using bounded fallback"
                )
            });
            info!(version, lag_months, "selected completed Wikimedia snapshot");
            return Ok(version);
        }
    }
    anyhow::bail!(
        "no completed Wikimedia snapshot found for {} within the configured {} month lag",
        wikis.join(","),
        max_lag_months
    )
}

pub fn resolve_latest_completed_snapshot(
    data_dir: &Path,
    wikis: &[String],
    now: chrono::DateTime<chrono::Utc>,
) -> Result<String> {
    let max_lag = snapshot_max_lag_months(std::env::var_os(SNAPSHOT_MAX_LAG_ENV).as_deref())?;
    let transport = build_transport()?;
    resolve_latest_completed_snapshot_cached_with_transport(
        &transport, BASE_URL, data_dir, wikis, now, max_lag,
    )
}

fn validate_completed_snapshot_with_transport<T: HttpTransport>(
    transport: &T,
    base_url: &str,
    data_dir: &Path,
    wiki: &str,
    version: &str,
) -> Result<()> {
    anyhow::ensure!(
        snapshot_wiki_is_complete_cached(transport, base_url, data_dir, wiki, version)?,
        "requested snapshot {version} is not complete for {wiki}; omit --version to use the latest completed snapshot"
    );
    Ok(())
}

/// Validate and persist the immutable remote inventory for an exact snapshot.
///
/// Explicit snapshot pins are reproducibility boundaries, so they must fail
/// closed instead of reaching the download stage with an incomplete plan.
pub fn validate_completed_snapshot(data_dir: &Path, wiki: &str, version: &str) -> Result<()> {
    let transport = build_transport()?;
    validate_completed_snapshot_with_transport(&transport, BASE_URL, data_dir, wiki, version)
}

#[derive(Clone)]
struct ReqwestTransport {
    client: reqwest::blocking::Client,
}

impl AttemptError {
    fn fatal(error: anyhow::Error) -> Self {
        Self {
            error,
            retryable: false,
            rate_limited: false,
            retry_after: None,
        }
    }

    fn retryable(error: anyhow::Error) -> Self {
        Self {
            error,
            retryable: true,
            rate_limited: false,
            retry_after: None,
        }
    }

    /// Retryable error carrying the response status that caused it, so the
    /// retry loop can honor a 429's `Retry-After` header (or fall back to a
    /// longer rate-limit-specific backoff) instead of the general schedule.
    fn retryable_status(
        status: StatusCode,
        retry_after: Option<Duration>,
        error: anyhow::Error,
    ) -> Self {
        Self {
            error,
            retryable: true,
            rate_limited: status == StatusCode::TOO_MANY_REQUESTS,
            retry_after,
        }
    }
}

/// Determine the file list for a given wiki and snapshot version.
#[cfg(test)]
pub(crate) fn build_file_list(wiki: &str, version: &str) -> Result<Vec<String>> {
    SnapshotPlan::resolve(wiki, version)?.filenames()
}

/// Maximum redirect chain length the dumps-host policy will follow before
/// giving up. Matches reqwest's stock `Policy::limited(10)` ceiling so the
/// custom policy doesn't accidentally permit infinite redirect loops.
const REDIRECT_MAX_HOPS: usize = 10;

/// Decision returned by the redirect-policy core: did we follow, reject for
/// host mismatch, or reject for hop ceiling? Pulled out of the closure so
/// the policy logic is unit-testable without standing up a real HTTP client.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum RedirectDecision {
    Follow,
    BlockedHost(String),
    TooManyHops,
}

#[cfg(test)]
pub(crate) fn evaluate_redirect(host: Option<&str>, hops: usize) -> RedirectDecision {
    evaluate_redirect_for(host, hops, DUMPS_HOST)
}

pub(crate) fn evaluate_redirect_for(
    host: Option<&str>,
    hops: usize,
    allowed_host: &str,
) -> RedirectDecision {
    if hops >= REDIRECT_MAX_HOPS {
        return RedirectDecision::TooManyHops;
    }
    match host {
        Some(host) if host == allowed_host => RedirectDecision::Follow,
        Some(host) => RedirectDecision::BlockedHost(host.to_owned()),
        None => RedirectDecision::BlockedHost(String::new()),
    }
}

/// Custom redirect policy that only follows redirects whose target host
/// matches the dumps.wikimedia.org canonical host. Bounds the blast radius of
/// an open-redirect on the upstream server to in-host targets only. A URL
/// missing a host is treated the same as a non-dumps host: the redirect is
/// rejected so the request errors loudly rather than traveling somewhere
/// unexpected.
pub(crate) fn dumps_host_only_redirect_policy() -> Policy {
    redirect_policy_for_host(DUMPS_HOST.to_owned())
}

/// Test-friendly variant: same logic as `dumps_host_only_redirect_policy`
/// but with a caller-supplied allowlisted host. Lets unit tests drive the
/// `Follow` / `TooManyHops` arms against `127.0.0.1` without making real
/// network calls to dumps.wikimedia.org.
pub(crate) fn redirect_policy_for_host(allowed_host: String) -> Policy {
    Policy::custom(move |attempt| {
        let host = attempt.url().host_str().map(str::to_owned);
        match evaluate_redirect_for(host.as_deref(), attempt.previous().len(), &allowed_host) {
            RedirectDecision::Follow => attempt.follow(),
            RedirectDecision::BlockedHost(host) => attempt.error(format!(
                "redirect to non-allowed host {host:?} blocked by policy"
            )),
            RedirectDecision::TooManyHops => {
                attempt.error(format!("redirect chain exceeded {REDIRECT_MAX_HOPS} hops"))
            }
        }
    })
}

fn build_transport() -> Result<ReqwestTransport> {
    let client = reqwest::blocking::Client::builder()
        .user_agent(USER_AGENT)
        .timeout(Duration::from_secs(3600))
        .redirect(dumps_host_only_redirect_policy())
        .build()
        .map_err(anyhow::Error::from)?;

    Ok(ReqwestTransport { client })
}

/// Verify a freshly downloaded `.tsv.bz2` file begins with the bz2 magic
/// header. Catches truncated, empty, or HTML-error-page-as-200 responses that
/// passed Content-Length validation but produced an unusable file.
pub(crate) fn verify_bz2_magic(path: &Path) -> Result<()> {
    let mut file = fs::File::open(path)
        .with_context(|| format!("failed to open {} for magic-byte check", path.display()))?;
    let mut header = [0_u8; 3];
    let read = read_filled(&mut file, &mut header)?;
    if read < BZ2_MAGIC.len() || header != BZ2_MAGIC {
        anyhow::bail!(
            "downloaded file {} does not begin with bz2 magic ('BZh'); got {} byte(s) {:02x?}",
            path.display(),
            read,
            &header[..read]
        );
    }
    Ok(())
}

/// Best-effort filled read: returns the number of bytes actually read into
/// `buf`. A short file produces a short read with no error, which is the
/// behavior the magic-byte helpers want.
fn read_filled<R: Read>(reader: &mut R, buf: &mut [u8]) -> Result<usize> {
    let mut filled = 0;
    while filled < buf.len() {
        match reader.read(&mut buf[filled..])? {
            0 => break,
            n => filled += n,
        }
    }
    Ok(filled)
}

impl HttpTransport for ReqwestTransport {
    fn head(&self, url: &str) -> Result<TransportHead> {
        self.client
            .head(url)
            .send()
            .map(Into::into)
            .map_err(anyhow::Error::from)
    }

    fn get(&self, url: &str, range_start: Option<u64>) -> Result<TransportResponse> {
        let request =
            build_get_request(&self.client, url, range_start).map_err(anyhow::Error::from)?;
        self.client
            .execute(request)
            .map(Into::into)
            .map_err(anyhow::Error::from)
    }
}

impl From<reqwest::blocking::Response> for TransportHead {
    fn from(response: reqwest::blocking::Response) -> Self {
        parse_transport_head(
            response.status(),
            response.headers(),
            response.content_length(),
        )
    }
}

impl From<reqwest::blocking::Response> for TransportResponse {
    fn from(response: reqwest::blocking::Response) -> Self {
        let retry_after = parse_retry_after(response.headers());
        build_transport_response(
            response.status(),
            response.content_length(),
            retry_after,
            Box::new(response),
        )
    }
}

fn parse_transport_head(
    status: StatusCode,
    headers: &HeaderMap,
    fallback_content_length: Option<u64>,
) -> TransportHead {
    let content_length = headers
        .get(CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .or(fallback_content_length);
    TransportHead {
        status,
        content_length,
        accepts_ranges: headers
            .get(ACCEPT_RANGES)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.eq_ignore_ascii_case("bytes")),
        retry_after: parse_retry_after(headers),
        etag: headers
            .get(ETAG)
            .and_then(|value| value.to_str().ok())
            .map(str::to_string),
        last_modified: headers
            .get(LAST_MODIFIED)
            .and_then(|value| value.to_str().ok())
            .map(str::to_string),
    }
}

/// Parses a `Retry-After` header value. Only the delta-seconds form
/// (`Retry-After: 120`) is handled — the HTTP-date form is uncommon on 429
/// responses from rate limiters and not worth a date-parsing dependency
/// here. Clamped to `FETCH_RETRY_AFTER_MAX_SECS`.
fn parse_retry_after(headers: &HeaderMap) -> Option<Duration> {
    let seconds = headers
        .get(RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim().parse::<u64>().ok())?;
    Some(Duration::from_secs(seconds.min(FETCH_RETRY_AFTER_MAX_SECS)))
}

fn build_get_request(
    client: &reqwest::blocking::Client,
    url: &str,
    range_start: Option<u64>,
) -> reqwest::Result<reqwest::blocking::Request> {
    let mut request = client.get(url);
    if let Some(range_start) = range_start {
        request = request.header(RANGE, format!("bytes={range_start}-"));
    }
    request.build()
}

fn build_transport_response(
    status: StatusCode,
    content_length: Option<u64>,
    retry_after: Option<Duration>,
    body: Box<dyn Read + Send>,
) -> TransportResponse {
    TransportResponse {
        status,
        content_length,
        retry_after,
        body,
    }
}

fn create_progress_bar(
    dest: &Path,
    total_size: Option<u64>,
    initial_position: u64,
    visible: bool,
) -> ProgressBar {
    let progress = if visible {
        ProgressBar::new(total_size.unwrap_or(0))
    } else {
        ProgressBar::hidden()
    };
    progress.set_style(
        ProgressStyle::default_bar()
            .template("{msg} [{bar:40}] {bytes}/{total_bytes} ({eta})")
            .expect("invalid progress bar template")
            .progress_chars("=> "),
    );
    progress.set_message(dest.file_name().unwrap().to_string_lossy().to_string());
    if total_size.is_some() {
        progress.set_position(initial_position);
    }
    progress
}

fn is_retryable_status(status: StatusCode) -> bool {
    status == StatusCode::REQUEST_TIMEOUT
        || status == StatusCode::TOO_MANY_REQUESTS
        || status.is_server_error()
}

/// How long to wait before the next retry attempt. Honors the server's
/// `Retry-After` header when present; otherwise falls back to an
/// exponential backoff, using a longer base for rate-limited (429)
/// responses than for other retryable statuses (timeouts, 5xx).
fn retry_delay(attempt: usize, rate_limited: bool, retry_after: Option<Duration>) -> Duration {
    if let Some(retry_after) = retry_after {
        return retry_after;
    }
    let base_ms = if rate_limited {
        FETCH_RATE_LIMIT_BACKOFF_MS
    } else {
        FETCH_RETRY_BACKOFF_MS
    };
    let multiplier = 1_u64 << attempt.saturating_sub(1);
    Duration::from_millis(base_ms.saturating_mul(multiplier).min(FETCH_MAX_BACKOFF_MS))
}

fn sleep_before_retry(attempt: usize, rate_limited: bool, retry_after: Option<Duration>) {
    let delay = retry_delay(attempt, rate_limited, retry_after);
    if rate_limited {
        debug!(
            attempt = attempt,
            delay_ms = delay.as_millis() as u64,
            honored_retry_after = retry_after.is_some(),
            "rate limited; waiting before retry"
        );
    }
    std::thread::sleep(delay);
}

fn fetch_parallelism(files: usize) -> usize {
    let raw = std::env::var_os(FETCH_MAX_PARALLELISM_ENV);
    fetch_parallelism_override(files, raw.as_deref())
}

fn fetch_parallelism_override(files: usize, raw: Option<&OsStr>) -> usize {
    let default = files.clamp(1, FETCH_MAX_PARALLELISM);
    let Some(raw) = raw else {
        return default;
    };

    match raw
        .to_str()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
    {
        Some(limit) => files.clamp(1, limit),
        None => {
            warn!(
                env_var = FETCH_MAX_PARALLELISM_ENV,
                value = %raw.to_string_lossy(),
                "ignoring invalid fetch parallelism override"
            );
            default
        }
    }
}

fn probe_remote_file<T: HttpTransport>(transport: &T, url: &str) -> Result<Option<RemoteFileInfo>> {
    let mut last_error = None;

    for attempt in 1..=FETCH_MAX_RETRIES {
        let mut rate_limited = false;
        let mut retry_after = None;

        match transport.head(url) {
            Ok(response) if response.status.is_success() => {
                return Ok(Some(RemoteFileInfo {
                    content_length: response.content_length,
                    accepts_ranges: response.accepts_ranges,
                }));
            }
            Ok(response)
                if response.status == StatusCode::METHOD_NOT_ALLOWED
                    || response.status == StatusCode::NOT_IMPLEMENTED
                    || response.status == StatusCode::FORBIDDEN =>
            {
                debug!(url = url, status = %response.status, "remote metadata probe unsupported");
                return Ok(None);
            }
            Ok(response) if response.status == StatusCode::NOT_FOUND => {
                anyhow::bail!("HTTP {} for {}", response.status, url);
            }
            Ok(response) if is_retryable_status(response.status) => {
                rate_limited = response.status == StatusCode::TOO_MANY_REQUESTS;
                retry_after = response.retry_after;
                last_error = Some(anyhow::anyhow!("HTTP {} for {}", response.status, url));
            }
            Ok(response) => {
                warn!(
                    url = url,
                    status = %response.status,
                    "metadata probe returned non-success status; continuing without validation"
                );
                return Ok(None);
            }
            Err(error) => {
                last_error = Some(error);
            }
        }

        if attempt < FETCH_MAX_RETRIES {
            sleep_before_retry(attempt, rate_limited, retry_after);
        }
    }

    last_error
        .into_iter()
        .for_each(|error| warn!(url = url, error = %error, "metadata probe failed after retries"));
    Ok(None)
}

/// Resolve compressed source sizes once for resource preflight. Keeping this
/// separate from download means the orchestrator can reject an oversized
/// snapshot before opening a candidate-generation transaction.
pub(crate) fn snapshot_source_sizes(
    data_dir: &Path,
    wiki: &str,
    snapshot: &str,
    sources: &[SourceSpec],
) -> Result<Vec<Option<u64>>> {
    let plan_path = crate::snapshot_plan::plan_path(data_dir, wiki, snapshot)?;
    let plan = if plan_path.is_file() {
        SnapshotPlan::load(&plan_path)?
    } else {
        SnapshotPlan::load_or_resolve(data_dir, wiki, snapshot)?.0
    };
    if let Some(inventory) = read_remote_inventory(data_dir, &plan)? {
        let sizes = inventory
            .sources
            .into_iter()
            .map(|source| (source.source_id, source.content_length))
            .collect::<std::collections::BTreeMap<_, _>>();
        return sources
            .iter()
            .map(|source| {
                sizes
                    .get(&source.source_id)
                    .copied()
                    .context("requested source is absent from the remote inventory")
            })
            .collect();
    }
    let transport = build_transport()?;
    snapshot_source_sizes_with_transport(&transport, sources)
}

fn snapshot_source_sizes_with_transport<T: HttpTransport>(
    transport: &T,
    sources: &[SourceSpec],
) -> Result<Vec<Option<u64>>> {
    sources
        .iter()
        .map(|source| match source.expected_size {
            Some(bytes) => Ok(Some(bytes)),
            None => Ok(probe_remote_file(transport, source.url.as_str())?
                .and_then(|remote| remote.content_length)),
        })
        .collect()
}

fn plan_download(dest: &Path, remote: Option<RemoteFileInfo>) -> Result<Option<DownloadPlan>> {
    let local_size = if dest.exists() {
        fs::metadata(dest)?.len()
    } else {
        0
    };

    if local_size == 0 {
        return Ok(Some(DownloadPlan {
            resume_from: 0,
            total_size: remote.and_then(|info| info.content_length),
            accepts_ranges: remote.is_some_and(|info| info.accepts_ranges),
        }));
    }

    let Some(remote) = remote else {
        info!(
            path = %dest.display(),
            local_bytes = local_size,
            "redownloading existing file because remote size could not be verified"
        );
        fs::remove_file(dest)?;
        return Ok(Some(DownloadPlan {
            resume_from: 0,
            total_size: None,
            accepts_ranges: false,
        }));
    };

    if let Some(total_size) = remote.content_length {
        if local_size == total_size {
            debug!(
                path = %dest.display(),
                bytes = local_size,
                "skipping existing file after size validation"
            );
            return Ok(None);
        }

        if local_size > total_size {
            info!(
                path = %dest.display(),
                local_bytes = local_size,
                remote_bytes = total_size,
                "redownloading file because local copy is larger than remote"
            );
            fs::remove_file(dest)?;
            return Ok(Some(DownloadPlan {
                resume_from: 0,
                total_size: Some(total_size),
                accepts_ranges: remote.accepts_ranges,
            }));
        }

        if remote.accepts_ranges {
            info!(
                path = %dest.display(),
                local_bytes = local_size,
                remote_bytes = total_size,
                "resuming partial download"
            );
            return Ok(Some(DownloadPlan {
                resume_from: local_size,
                total_size: Some(total_size),
                accepts_ranges: true,
            }));
        }

        info!(
            path = %dest.display(),
            local_bytes = local_size,
            remote_bytes = total_size,
            "redownloading partial file because remote server does not support range requests"
        );
        fs::remove_file(dest)?;
        return Ok(Some(DownloadPlan {
            resume_from: 0,
            total_size: Some(total_size),
            accepts_ranges: false,
        }));
    }

    info!(
        path = %dest.display(),
        local_bytes = local_size,
        "redownloading existing file because remote size is unknown"
    );
    fs::remove_file(dest)?;
    Ok(Some(DownloadPlan {
        resume_from: 0,
        total_size: None,
        accepts_ranges: remote.accepts_ranges,
    }))
}

fn download_attempt<T: HttpTransport>(
    transport: &T,
    url: &str,
    dest: &Path,
    plan: DownloadPlan,
    visible_progress: bool,
) -> std::result::Result<u64, AttemptError> {
    let range_start = (plan.resume_from > 0 && plan.accepts_ranges).then_some(plan.resume_from);
    let mut response = transport
        .get(url, range_start)
        .map_err(AttemptError::retryable)?;

    if !response.status.is_success() {
        let error = anyhow::anyhow!("HTTP {} for {}", response.status, url);
        return if is_retryable_status(response.status) {
            Err(AttemptError::retryable_status(
                response.status,
                response.retry_after,
                error,
            ))
        } else {
            Err(AttemptError::fatal(error))
        };
    }

    let append = plan.resume_from > 0 && response.status == StatusCode::PARTIAL_CONTENT;
    let progress_total = plan.total_size.or_else(|| {
        response.content_length.map(|content_length| {
            if append {
                plan.resume_from + content_length
            } else {
                content_length
            }
        })
    });
    let progress = create_progress_bar(
        dest,
        progress_total,
        if append { plan.resume_from } else { 0 },
        visible_progress,
    );

    let mut file = if append {
        OpenOptions::new()
            .append(true)
            .open(dest)
            .map_err(|error| AttemptError::fatal(error.into()))?
    } else {
        fs::File::create(dest).map_err(|error| AttemptError::fatal(error.into()))?
    };

    let download_result = (|| -> std::result::Result<u64, std::io::Error> {
        let mut buffer = [0_u8; 64 * 1024];
        let mut downloaded = if append { plan.resume_from } else { 0 };

        loop {
            let read = response.body.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            file.write_all(&buffer[..read])?;
            downloaded += read as u64;
            progress.inc(read as u64);
        }

        file.flush()?;
        file.sync_data()?;
        crate::storage::discard_file_cache(&file, 0, downloaded);
        Ok(downloaded)
    })();

    match download_result {
        Ok(downloaded) => {
            progress.finish_and_clear();
            Ok(downloaded)
        }
        Err(error) => {
            progress.abandon();
            Err(AttemptError::retryable(error.into()))
        }
    }
}

fn download_file_with_transport<T: HttpTransport>(
    transport: &T,
    url: &str,
    dest: &Path,
    visible_progress: bool,
) -> Result<()> {
    let remote = probe_remote_file(transport, url)?;
    let mut plan = match plan_download(dest, remote)? {
        Some(plan) => plan,
        None => return Ok(()),
    };

    let mut attempt = 1;
    loop {
        match download_attempt(transport, url, dest, plan, visible_progress) {
            Ok(downloaded) => {
                if let Err(integrity_error) = verify_bz2_magic(dest) {
                    warn!(
                        path = %dest.display(),
                        error = %integrity_error,
                        "downloaded file failed bz2 magic check; removing and aborting"
                    );
                    let _ = fs::remove_file(dest);
                    return Err(integrity_error);
                }
                info!(
                    path = %dest.display(),
                    bytes = downloaded,
                    expected_bytes = plan.total_size.unwrap_or(downloaded),
                    resumed = plan.resume_from > 0,
                    "downloaded dump file"
                );
                return Ok(());
            }
            Err(error) if error.retryable && attempt < FETCH_MAX_RETRIES => {
                warn!(
                    url = url,
                    path = %dest.display(),
                    attempt = attempt,
                    error = %error.error,
                    "download attempt failed; retrying"
                );
                sleep_before_retry(attempt, error.rate_limited, error.retry_after);
                if plan.accepts_ranges && dest.exists() {
                    plan.resume_from = fs::metadata(dest)?.len();
                } else {
                    let _ = fs::remove_file(dest);
                    plan.resume_from = 0;
                }
                attempt += 1;
            }
            Err(error) => {
                if !plan.accepts_ranges {
                    let _ = fs::remove_file(dest);
                }
                return Err(error.error);
            }
        }
    }
}

fn validate_source_window_run_id(run_id: &str) -> Result<()> {
    anyhow::ensure!(
        !run_id.is_empty()
            && run_id.len() <= 160
            && run_id
                .bytes()
                .all(|byte| { byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.') }),
        "invalid source-window run ID {run_id:?}"
    );
    Ok(())
}

fn source_window_staging_dir(data_dir: &Path, wiki: &str) -> PathBuf {
    data_dir
        .join("raw")
        .join(wiki)
        .join(SOURCE_WINDOW_STAGING_DIR)
}

fn source_download_temp_path(staging_dir: &Path, source_id: &str, run_id: &str) -> PathBuf {
    staging_dir.join(format!(
        ".{source_id}.{run_id}{SOURCE_WINDOW_DOWNLOAD_SUFFIX}"
    ))
}

fn adopt_source_download(
    staging_dir: &Path,
    source_id: &str,
    run_id: &str,
    final_path: &Path,
) -> Result<PathBuf> {
    let current = source_download_temp_path(staging_dir, source_id, run_id);
    let prefix = format!(".{source_id}.");
    let mut recoverable = fs::read_dir(staging_dir)?
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_file()))
        .map(|entry| entry.path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| {
                    name.starts_with(&prefix) && name.ends_with(SOURCE_WINDOW_DOWNLOAD_SUFFIX)
                })
        })
        .collect::<Vec<_>>();
    if final_path.exists() {
        recoverable.push(final_path.to_path_buf());
    }
    recoverable.sort();
    recoverable.dedup();
    anyhow::ensure!(
        recoverable.len() <= 1,
        "multiple recoverable source-window inputs found for {source_id}"
    );
    if current.exists() {
        return Ok(current);
    }
    if let Some(previous) = recoverable.pop() {
        fs::rename(&previous, &current).context("failed to adopt source-window input")?;
        fs::File::open(staging_dir)?.sync_all()?;
    }
    Ok(current)
}

fn download_snapshot_source_with_transport<T: HttpTransport>(
    transport: &T,
    source: &SourceSpec,
    data_dir: &Path,
    wiki: &str,
    run_id: &str,
) -> Result<PathBuf> {
    validate_source_window_run_id(run_id)?;
    let staging_dir = source_window_staging_dir(data_dir, wiki);
    fs::create_dir_all(&staging_dir)?;
    let filename = source.filename()?;
    let final_path = staging_dir.join(filename);
    let temp_path = adopt_source_download(&staging_dir, &source.source_id, run_id, &final_path)?;

    download_file_with_transport(transport, source.url.as_str(), &temp_path, true)?;
    if let Some(expected_size) = source.expected_size {
        let actual_size = fs::metadata(&temp_path)?.len();
        anyhow::ensure!(
            actual_size == expected_size,
            "source {} has {} bytes, expected {} from its snapshot plan",
            source.source_id,
            actual_size,
            expected_size
        );
    }
    fs::rename(&temp_path, &final_path).context("failed to commit source-window download")?;
    fs::File::open(&staging_dir)?.sync_all()?;
    Ok(final_path)
}

fn fetch_wiki_from_base_with_transport_at_parallelism<T: HttpTransport>(
    transport: &T,
    base_url: &str,
    wiki: &str,
    version: &str,
    data_dir: &Path,
    files: Vec<String>,
    parallelism: usize,
) -> Result<Vec<PathBuf>> {
    let raw_dir = data_dir.join("raw").join(wiki);
    fs::create_dir_all(&raw_dir)?;

    info!(
        wiki = wiki,
        version = version,
        files = files.len(),
        parallelism = parallelism,
        "fetching dump files"
    );

    let entries: Vec<(String, PathBuf)> = files
        .iter()
        .map(|filename| {
            (
                format!("{base_url}/{version}/{wiki}/{filename}"),
                raw_dir.join(filename),
            )
        })
        .collect();

    let paths = if parallelism == 1 {
        let mut paths = Vec::with_capacity(entries.len());
        for (url, dest) in entries {
            download_file_with_transport(transport, &url, &dest, true)?;
            paths.push(dest);
        }
        paths
    } else {
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(parallelism)
            .build()
            .context("failed to build fetch thread pool")?;
        pool.install(|| {
            entries
                .par_iter()
                .map(|(url, dest)| {
                    download_file_with_transport(transport, url, dest, false)?;
                    Ok(dest.clone())
                })
                .collect::<Result<Vec<_>>>()
        })?
    };

    info!(wiki = wiki, files = paths.len(), dest = %raw_dir.display(), "finished fetch");
    Ok(paths)
}

/// Sum the remote byte total for `files` (minus bytes already present
/// locally from a prior partial download) and compare it against the
/// filesystem's available space at `data_dir`, failing fast if a full
/// download clearly won't fit. Best-effort: files whose remote size can't
/// be determined are excluded from the total rather than blocking the
/// fetch, and if the disk-space query itself fails (e.g. unsupported
/// filesystem), the check is skipped with a warning instead of hard-failing
/// unrelated environments like local dev or CI.
fn check_disk_headroom<T: HttpTransport>(
    transport: &T,
    base_url: &str,
    wiki: &str,
    version: &str,
    files: &[String],
    data_dir: &Path,
) -> Result<()> {
    check_disk_headroom_with_available(
        transport,
        base_url,
        wiki,
        version,
        files,
        data_dir,
        |path| fs4::available_space(path),
    )
}

#[allow(clippy::too_many_arguments)]
fn check_disk_headroom_with_available<T, F>(
    transport: &T,
    base_url: &str,
    wiki: &str,
    version: &str,
    files: &[String],
    data_dir: &Path,
    available_space: F,
) -> Result<()>
where
    T: HttpTransport,
    F: FnOnce(&Path) -> std::io::Result<u64>,
{
    let raw_dir = data_dir.join("raw").join(wiki);
    let mut needed_bytes: u64 = 0;
    let mut unknown_files = 0usize;

    for filename in files {
        let url = format!("{base_url}/{version}/{wiki}/{filename}");
        let dest = raw_dir.join(filename);
        match probe_remote_file(transport, &url)?.and_then(|info| info.content_length) {
            Some(total_size) => {
                let local_size = fs::metadata(&dest).map(|meta| meta.len()).unwrap_or(0);
                needed_bytes += total_size.saturating_sub(local_size);
            }
            None => unknown_files += 1,
        }
    }

    if unknown_files > 0 {
        warn!(
            wiki = wiki,
            unknown_files = unknown_files,
            "could not determine remote size for some dump files; disk-headroom check is a lower bound"
        );
    }

    if needed_bytes == 0 {
        return Ok(());
    }

    let available_bytes = match available_space(data_dir) {
        Ok(bytes) => bytes,
        Err(error) => {
            warn!(
                error = %error,
                path = %data_dir.display(),
                "could not determine available disk space; skipping headroom check"
            );
            return Ok(());
        }
    };

    ensure_disk_headroom(wiki, data_dir, needed_bytes, available_bytes)?;

    info!(
        wiki = wiki,
        needed_bytes = needed_bytes,
        available_bytes = available_bytes,
        "disk headroom check passed"
    );
    Ok(())
}

fn ensure_disk_headroom(
    wiki: &str,
    data_dir: &Path,
    needed_bytes: u64,
    available_bytes: u64,
) -> Result<()> {
    let required_bytes = needed_bytes.saturating_add(FETCH_DISK_HEADROOM_MARGIN_BYTES);
    if available_bytes < required_bytes {
        anyhow::bail!(
            "insufficient disk space to fetch {wiki}: need ~{needed_bytes} bytes \
             (+{FETCH_DISK_HEADROOM_MARGIN_BYTES} bytes margin) but only {available_bytes} \
             bytes available at {}",
            data_dir.display()
        );
    }

    Ok(())
}

/// Fetch all dump files for a wiki.
pub fn fetch_wiki(wiki: &str, version: &str, data_dir: &Path) -> Result<Vec<PathBuf>> {
    let transport = build_transport()?;
    fetch_wiki_with_transport(&transport, BASE_URL, wiki, version, data_dir)
}

/// Return the canonical sources that do not yet have a strict, readable
/// source-level ingest commit in the candidate generation.
pub(crate) fn pending_snapshot_sources(
    wiki: &str,
    version: &str,
    data_dir: &Path,
) -> Result<Vec<SourceSpec>> {
    let (plan, _) = SnapshotPlan::load_or_resolve(data_dir, wiki, version)?;
    let mut pending = Vec::new();
    for source in plan.sources {
        if !crate::compaction::source_is_represented(data_dir, wiki, version, &source.source_id)? {
            pending.push(source);
        }
    }
    Ok(pending)
}

fn source_window_local_bytes(staging_dir: &Path, source: &SourceSpec) -> Result<u64> {
    let filename = source.filename()?;
    let prefix = format!(".{}.", source.source_id);
    let mut largest = fs::metadata(staging_dir.join(filename))
        .map(|metadata| metadata.len())
        .unwrap_or(0);
    if staging_dir.is_dir() {
        for entry in fs::read_dir(staging_dir)? {
            let entry = entry?;
            if !entry.file_type()?.is_file() {
                continue;
            }
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name.starts_with(&prefix) && name.ends_with(SOURCE_WINDOW_DOWNLOAD_SUFFIX) {
                largest = largest.max(entry.metadata()?.len());
            }
        }
    }
    Ok(largest)
}

fn check_source_window_disk_headroom<T: HttpTransport>(
    transport: &T,
    wiki: &str,
    sources: &[SourceSpec],
    data_dir: &Path,
) -> Result<()> {
    check_source_window_disk_headroom_with_available(
        transport,
        wiki,
        sources,
        data_dir,
        source_window_available_space,
    )
}

fn source_window_available_space(path: &Path) -> std::io::Result<u64> {
    fs4::available_space(path)
}

fn check_source_window_disk_headroom_with_available<T, F>(
    transport: &T,
    wiki: &str,
    sources: &[SourceSpec],
    data_dir: &Path,
    available_space: F,
) -> Result<()>
where
    T: HttpTransport,
    F: FnOnce(&Path) -> std::io::Result<u64>,
{
    let staging_dir = source_window_staging_dir(data_dir, wiki);
    let mut needed_bytes = 0_u64;
    let mut unknown_sources = 0_usize;
    for source in sources {
        let remote_size = match source.expected_size {
            Some(size) => Some(size),
            None => probe_remote_file(transport, source.url.as_str())?
                .and_then(|remote| remote.content_length),
        };
        match remote_size {
            Some(size) => {
                needed_bytes = needed_bytes
                    .checked_add(
                        size.saturating_sub(source_window_local_bytes(&staging_dir, source)?),
                    )
                    .context("source-window byte requirement overflow")?;
            }
            None => unknown_sources += 1,
        }
    }
    if unknown_sources > 0 {
        warn!(
            wiki,
            unknown_sources,
            "source-window disk check is a lower bound because source sizes are unavailable"
        );
    }
    if needed_bytes == 0 {
        return Ok(());
    }
    match available_space(data_dir) {
        Ok(available_bytes) => ensure_disk_headroom(wiki, data_dir, needed_bytes, available_bytes),
        Err(error) => {
            warn!(
                wiki,
                path = %data_dir.display(),
                error = %error,
                "could not determine source-window disk space; skipping headroom check"
            );
            Ok(())
        }
    }
}

/// Download one bounded plan window into pipeline-owned staging. Every input
/// is allowlisted by the persisted plan, and partial downloads are named with
/// both the source and run IDs so a later run can adopt and resume them.
pub(crate) fn fetch_snapshot_source_window(
    wiki: &str,
    version: &str,
    data_dir: &Path,
    run_id: &str,
    sources: &[SourceSpec],
) -> Result<Vec<PathBuf>> {
    validate_source_window_run_id(run_id)?;
    let transport = build_transport()?;
    fetch_snapshot_source_window_with_transport(
        &transport, wiki, version, data_dir, run_id, sources,
    )
}

fn fetch_snapshot_source_window_with_transport<T: HttpTransport>(
    transport: &T,
    wiki: &str,
    version: &str,
    data_dir: &Path,
    run_id: &str,
    sources: &[SourceSpec],
) -> Result<Vec<PathBuf>> {
    let (plan, _) = SnapshotPlan::load_or_resolve(data_dir, wiki, version)?;
    anyhow::ensure!(!sources.is_empty(), "source window must not be empty");
    for source in sources {
        anyhow::ensure!(
            plan.sources.contains(source),
            "source {} is not part of the persisted snapshot plan for {wiki} {version}",
            source.source_id
        );
    }
    check_source_window_disk_headroom(transport, wiki, sources, data_dir)?;

    let mut paths = Vec::with_capacity(sources.len());
    for source in sources {
        info!(
            wiki,
            version,
            source = source.source_id,
            run_id,
            "starting source-window download"
        );
        let path =
            download_snapshot_source_with_transport(transport, source, data_dir, wiki, run_id)?;
        paths.push(path);
    }
    Ok(paths)
}

/// Commit the fetch-stage receipt after every planned source is represented
/// by a strict ingest marker. At this point no raw input needs to remain.
pub(crate) fn finalize_snapshot_fetch(wiki: &str, version: &str, data_dir: &Path) -> Result<()> {
    let (plan, plan_path) = SnapshotPlan::load_or_resolve(data_dir, wiki, version)?;
    let expected = plan.filenames()?;
    for source in &plan.sources {
        anyhow::ensure!(
            crate::compaction::source_is_represented(data_dir, wiki, version, &source.source_id,)
                .unwrap_or(false),
            "cannot finalize fetch: source {} has no committed ingest or compaction proof",
            source.source_id
        );
    }
    record_fetch_stage(data_dir, wiki, version, &plan_path, &expected)
}

/// Reclaim completed source-window inputs left by an interruption between the
/// marker commit and raw deletion. Only plan-owned filenames whose strict
/// marker already validates are eligible for removal.
pub(crate) fn cleanup_committed_source_window_inputs(
    wiki: &str,
    version: &str,
    data_dir: &Path,
) -> Result<usize> {
    let staging_dir = source_window_staging_dir(data_dir, wiki);
    if !staging_dir.is_dir() {
        return Ok(0);
    }
    let (plan, _) = SnapshotPlan::load_or_resolve(data_dir, wiki, version)?;
    let mut removed = 0_usize;
    for source in plan.sources {
        if !crate::compaction::source_is_represented(data_dir, wiki, version, &source.source_id)
            .unwrap_or(false)
        {
            continue;
        }
        let filename = source.filename()?;
        let prefix = format!(".{}.", source.source_id);
        for entry in fs::read_dir(&staging_dir)? {
            let entry = entry?;
            if !entry.file_type()?.is_file() {
                continue;
            }
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name == filename
                || (name.starts_with(&prefix) && name.ends_with(SOURCE_WINDOW_DOWNLOAD_SUFFIX))
            {
                fs::remove_file(entry.path())?;
                removed += 1;
            }
        }
    }
    if fs::read_dir(&staging_dir)?.next().is_none() {
        fs::remove_dir(&staging_dir)?;
    } else {
        fs::File::open(&staging_dir)?.sync_all()?;
    }
    Ok(removed)
}

fn fetch_wiki_with_transport<T: HttpTransport>(
    transport: &T,
    base_url: &str,
    wiki: &str,
    version: &str,
    data_dir: &Path,
) -> Result<Vec<PathBuf>> {
    let (plan, plan_path) = SnapshotPlan::load_or_resolve(data_dir, wiki, version)?;
    let expected = plan.filenames()?;
    let mut files = Vec::new();
    for filename in &expected {
        let source_id = crate::ingest::ingest_source_id(Path::new(filename))?;
        if crate::compaction::source_is_represented(data_dir, wiki, version, &source_id)? {
            debug!(
                wiki = wiki,
                version = version,
                source = filename,
                "skipping download represented by valid ingest marker"
            );
        } else {
            files.push(filename.clone());
        }
    }
    let reused = expected.len() - files.len();
    info!(
        wiki = wiki,
        version = version,
        reused_sources = reused,
        download_sources = files.len(),
        "planned snapshot fetch from strict ingest markers"
    );
    if files.is_empty() {
        crate::observability::record_stage_reused("fetch", Some(wiki));
        record_fetch_stage(data_dir, wiki, version, &plan_path, &expected)?;
        return Ok(Vec::new());
    }
    check_disk_headroom(transport, base_url, wiki, version, &files, data_dir)?;
    let parallelism = fetch_parallelism(files.len());
    fetch_wiki_from_base_with_transport_at_parallelism(
        transport,
        base_url,
        wiki,
        version,
        data_dir,
        files,
        parallelism,
    )
    .and_then(|paths| {
        record_fetch_stage(data_dir, wiki, version, &plan_path, &expected)?;
        Ok(paths)
    })
}

fn record_fetch_stage(
    data_dir: &Path,
    wiki: &str,
    version: &str,
    plan_path: &Path,
    expected: &[String],
) -> Result<()> {
    let analytical_root = crate::storage::snapshot_analytical_wiki_dir(data_dir, wiki, version)?;
    let raw_root = data_dir.join("raw").join(wiki);
    let mut sources = Vec::with_capacity(expected.len() + 1);
    sources.push(TrackedPath::new("snapshot-plan", plan_path));
    for filename in expected {
        let source_id = crate::ingest::ingest_source_id(Path::new(filename))?;
        let marker = crate::storage::marker_path_in(&analytical_root, &source_id);
        let (identity, path) =
            if crate::compaction::source_is_represented(data_dir, wiki, version, &source_id)? {
                (format!("ingest-marker/{source_id}"), marker)
            } else {
                (
                    format!("remote/{version}/{wiki}/{filename}"),
                    raw_root.join(filename),
                )
            };
        sources.push(TrackedPath::new(identity, path));
    }
    fingerprint::record(
        &fingerprint::data_stage_receipt_path(data_dir, wiki, version, "fetch"),
        StageSpec {
            stage: "fetch",
            scope: wiki,
            selected_snapshot: Some(version),
            algorithm_version: FETCH_ALGORITHM_VERSION,
        },
        &sources,
        &sources,
    )
    .map(|_| ())
}

/// Delete a wiki's downloaded `.bz2` dump files, freeing the on-disk peak
/// they occupy as soon as `ingest_wiki` has consumed them. Only `fetch_wiki`
/// (writer) and `ingest_wiki` (reader) ever touch `data/raw/<wiki>`, so this
/// is safe to call immediately once ingest succeeds — no downstream stage
/// (compute, patrol) reads from it. Missing directories are a no-op, matching
/// the idempotent `find ... -delete` this replaces in `run-refresh.sh`.
pub fn cleanup_raw_dump(wiki: &str, data_dir: &Path) -> Result<()> {
    let raw_dir = data_dir.join("raw").join(wiki);
    if !raw_dir.exists() {
        return Ok(());
    }

    let mut removed = 0usize;
    for entry in fs::read_dir(&raw_dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) == Some("bz2") {
            fs::remove_file(&path)?;
            removed += 1;
        }
    }

    info!(
        wiki = wiki,
        removed_files = removed,
        path = %raw_dir.display(),
        "cleaned up raw dump files"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{TestDir, init_test_tracing};
    use chrono::{TimeZone, Utc};
    use std::collections::VecDeque;
    use std::io::{BufRead, BufReader, Cursor, ErrorKind};
    use std::net::TcpListener;
    use std::sync::{Arc, Mutex};
    use std::thread;
    use std::time::Instant;

    const TEST_URL: &str = "http://example.invalid/dump.tsv.bz2";
    type RequestLog = Arc<Mutex<Vec<String>>>;
    type TestServerHandle = thread::JoinHandle<Result<()>>;

    #[derive(Clone, Debug)]
    enum FakeHeadOutcome {
        Response(TransportHead),
        Error(&'static str),
    }

    #[derive(Clone, Debug)]
    enum FakeGetOutcome {
        Response {
            status: StatusCode,
            body: Vec<u8>,
            accepts_ranges: bool,
            fail_after: Option<usize>,
            retry_after: Option<Duration>,
        },
        Error(&'static str),
    }

    #[derive(Default)]
    struct FakeTransportState {
        head_outcomes: VecDeque<FakeHeadOutcome>,
        get_outcomes: VecDeque<FakeGetOutcome>,
        head_requests: usize,
        get_requests: usize,
        requested_ranges: Vec<Option<u64>>,
    }

    #[derive(Clone, Default)]
    struct FakeTransport {
        state: Arc<Mutex<FakeTransportState>>,
    }

    struct FlakyReader {
        cursor: Cursor<Vec<u8>>,
        fail_after: usize,
        bytes_read: usize,
        failed: bool,
    }

    impl Read for FlakyReader {
        fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
            if self.failed {
                return Err(std::io::Error::other("injected read failure"));
            }

            let remaining_before_failure = self.fail_after.saturating_sub(self.bytes_read);
            if remaining_before_failure == 0 {
                self.failed = true;
                return Err(std::io::Error::other("injected read failure"));
            }

            let limited_len = remaining_before_failure.min(buffer.len());
            let read = self.cursor.read(&mut buffer[..limited_len])?;
            self.bytes_read += read;
            Ok(read)
        }
    }

    impl FakeTransport {
        fn with_head_outcomes(head_outcomes: impl IntoIterator<Item = FakeHeadOutcome>) -> Self {
            Self {
                state: Arc::new(Mutex::new(FakeTransportState {
                    head_outcomes: head_outcomes.into_iter().collect(),
                    ..FakeTransportState::default()
                })),
            }
        }

        fn with_outcomes(
            head_outcomes: impl IntoIterator<Item = FakeHeadOutcome>,
            get_outcomes: impl IntoIterator<Item = FakeGetOutcome>,
        ) -> Self {
            Self {
                state: Arc::new(Mutex::new(FakeTransportState {
                    head_outcomes: head_outcomes.into_iter().collect(),
                    get_outcomes: get_outcomes.into_iter().collect(),
                    ..FakeTransportState::default()
                })),
            }
        }

        fn get_requests(&self) -> usize {
            self.state
                .lock()
                .expect("fake transport state")
                .get_requests
        }

        fn head_requests(&self) -> usize {
            self.state
                .lock()
                .expect("fake transport state")
                .head_requests
        }

        fn requested_ranges(&self) -> Vec<Option<u64>> {
            self.state
                .lock()
                .expect("fake transport state")
                .requested_ranges
                .clone()
        }
    }

    impl HttpTransport for FakeTransport {
        fn head(&self, _url: &str) -> Result<TransportHead> {
            let mut state = self.state.lock().expect("fake transport state");
            state.head_requests += 1;
            match state.head_outcomes.pop_front() {
                Some(FakeHeadOutcome::Response(response)) => Ok(response),
                Some(FakeHeadOutcome::Error(message)) => Err(anyhow::anyhow!(message)),
                None => Err(anyhow::anyhow!("unexpected HEAD request")),
            }
        }

        fn get(&self, _url: &str, range_start: Option<u64>) -> Result<TransportResponse> {
            let mut state = self.state.lock().expect("fake transport state");
            state.get_requests += 1;
            state.requested_ranges.push(range_start);
            let outcome = state
                .get_outcomes
                .pop_front()
                .ok_or_else(|| anyhow::anyhow!("unexpected GET request"))?;
            drop(state);

            match outcome {
                FakeGetOutcome::Error(message) => Err(anyhow::anyhow!(message)),
                FakeGetOutcome::Response {
                    status,
                    body,
                    accepts_ranges,
                    fail_after,
                    retry_after,
                } => {
                    let (status, body) = if let Some(offset) = range_start {
                        if accepts_ranges && status.is_success() {
                            (
                                StatusCode::PARTIAL_CONTENT,
                                body[offset as usize..].to_vec(),
                            )
                        } else {
                            (status, body)
                        }
                    } else {
                        (status, body)
                    };

                    let content_length = body.len() as u64;
                    let body: Box<dyn Read + Send> = match fail_after {
                        Some(fail_after) => Box::new(FlakyReader {
                            cursor: Cursor::new(body),
                            fail_after,
                            bytes_read: 0,
                            failed: false,
                        }),
                        None => Box::new(Cursor::new(body)),
                    };

                    Ok(TransportResponse {
                        status,
                        content_length: Some(content_length),
                        retry_after,
                        body,
                    })
                }
            }
        }
    }

    fn ok_head(content_length: Option<u64>, accepts_ranges: bool) -> FakeHeadOutcome {
        FakeHeadOutcome::Response(TransportHead {
            status: StatusCode::OK,
            content_length,
            accepts_ranges,
            retry_after: None,
            etag: None,
            last_modified: None,
        })
    }

    fn status_head(status: StatusCode) -> FakeHeadOutcome {
        FakeHeadOutcome::Response(TransportHead {
            status,
            content_length: None,
            accepts_ranges: false,
            retry_after: None,
            etag: None,
            last_modified: None,
        })
    }

    fn status_head_with_retry_after(status: StatusCode, retry_after: Duration) -> FakeHeadOutcome {
        FakeHeadOutcome::Response(TransportHead {
            status,
            content_length: None,
            accepts_ranges: false,
            retry_after: Some(retry_after),
            etag: None,
            last_modified: None,
        })
    }

    fn ok_get(body: &[u8], accepts_ranges: bool) -> FakeGetOutcome {
        FakeGetOutcome::Response {
            status: StatusCode::OK,
            body: body.to_vec(),
            accepts_ranges,
            fail_after: None,
            retry_after: None,
        }
    }

    fn status_get(status: StatusCode) -> FakeGetOutcome {
        FakeGetOutcome::Response {
            status,
            body: Vec::new(),
            accepts_ranges: false,
            fail_after: None,
            retry_after: None,
        }
    }

    fn status_get_with_retry_after(status: StatusCode, retry_after: Duration) -> FakeGetOutcome {
        FakeGetOutcome::Response {
            status,
            body: Vec::new(),
            accepts_ranges: false,
            fail_after: None,
            retry_after: Some(retry_after),
        }
    }

    fn remote_file(content_length: Option<u64>, accepts_ranges: bool) -> RemoteFileInfo {
        RemoteFileInfo {
            content_length,
            accepts_ranges,
        }
    }

    #[test]
    fn cleanup_raw_dump_removes_only_bz2_files() -> Result<()> {
        init_test_tracing();
        let temp_dir = TestDir::new()?;
        let raw_dir = temp_dir.path().join("raw").join("testwiki");
        fs::create_dir_all(&raw_dir)?;
        fs::write(raw_dir.join("2026-02.testwiki.all-time.tsv.bz2"), b"BZh")?;
        fs::write(raw_dir.join("notes.txt"), b"keep me")?;

        cleanup_raw_dump("testwiki", temp_dir.path())?;

        assert!(!raw_dir.join("2026-02.testwiki.all-time.tsv.bz2").exists());
        assert!(raw_dir.join("notes.txt").exists());
        Ok(())
    }

    #[test]
    fn cleanup_raw_dump_is_a_noop_for_missing_directory() -> Result<()> {
        init_test_tracing();
        let temp_dir = TestDir::new()?;
        cleanup_raw_dump("nosuchwiki", temp_dir.path())?;
        Ok(())
    }

    fn spawn_test_server(responses: Vec<String>) -> Result<(String, RequestLog, TestServerHandle)> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let address = listener.local_addr()?;
        let requests = Arc::new(Mutex::new(Vec::new()));
        let requests_for_thread = Arc::clone(&requests);
        let handle = thread::spawn(move || -> Result<()> {
            for response in responses {
                let (mut stream, _) = listener.accept()?;
                let mut reader = BufReader::new(stream.try_clone()?);
                let mut request = String::new();
                loop {
                    let mut line = String::new();
                    let read = reader.read_line(&mut line)?;
                    if read == 0 || line == "\r\n" {
                        break;
                    }
                    request.push_str(&line);
                }
                requests_for_thread
                    .lock()
                    .expect("request log")
                    .push(request);
                stream.write_all(response.as_bytes())?;
            }
            Ok(())
        });
        Ok((format!("http://{address}/dump.tsv.bz2"), requests, handle))
    }

    #[test]
    fn build_file_list_for_yearly_wiki_includes_all_years() -> Result<()> {
        init_test_tracing();
        let files = build_file_list("frwiki", "2026-02")?;
        assert_eq!(
            files.first().map(String::as_str),
            Some("2026-02.frwiki.2001.tsv.bz2")
        );
        assert_eq!(
            files.last().map(String::as_str),
            Some("2026-02.frwiki.2026.tsv.bz2")
        );
        Ok(())
    }

    #[test]
    fn build_file_list_for_small_wiki_uses_all_time_dump() -> Result<()> {
        init_test_tracing();
        let files = build_file_list("simplewiki", "2026-02")?;
        assert_eq!(files, vec!["2026-02.simplewiki.all-time.tsv.bz2"]);
        Ok(())
    }

    #[test]
    fn build_file_list_supports_monthly_wikis_through_partial_final_month() -> Result<()> {
        init_test_tracing();
        let files = build_file_list("enwiki", "2026-07")?;
        assert_eq!(files.len(), 308);
        assert_eq!(files.first().unwrap(), "2026-07.enwiki.2001-01.tsv.bz2");
        assert_eq!(files.last().unwrap(), "2026-07.enwiki.2026-08.tsv.bz2");
        Ok(())
    }

    #[test]
    fn wikipedia_databases_invariants() {
        init_test_tracing();
        // The picker universe must be a superset of every dispatch constant
        // so the admin UI never offers a wiki the CLI cannot match against.
        for wiki in YEARLY_WIKIS {
            #[rustfmt::skip]
            assert!(WIKIPEDIA_DATABASES.contains(wiki), "YEARLY_WIKIS entry {wiki} missing from WIKIPEDIA_DATABASES");
        }
        for wiki in MONTHLY_WIKIS {
            #[rustfmt::skip]
            assert!(WIKIPEDIA_DATABASES.contains(wiki), "MONTHLY_WIKIS entry {wiki} missing from WIKIPEDIA_DATABASES");
        }

        // Sorted-and-deduped — the admin server sorts again before display
        // but a sorted source list keeps diffs clean and helps reviewers.
        let mut sorted = WIKIPEDIA_DATABASES.to_vec();
        sorted.sort();
        sorted.dedup();
        assert_eq!(sorted.len(), WIKIPEDIA_DATABASES.len());
        for pair in WIKIPEDIA_DATABASES.windows(2) {
            #[rustfmt::skip]
            assert!(pair[0] < pair[1], "WIKIPEDIA_DATABASES not sorted at {} -> {}", pair[0], pair[1]);
        }
    }

    #[test]
    fn download_file_writes_response_body() -> Result<()> {
        init_test_tracing();
        let temp_dir = TestDir::new()?;
        let dest = temp_dir.path().join("download.tsv.bz2");
        let transport = FakeTransport::with_outcomes(
            [ok_head(Some(13), false)],
            [ok_get(b"BZhpayload-by", false)],
        );

        download_file_with_transport(&transport, TEST_URL, &dest, false)?;

        assert_eq!(fs::read(&dest)?, b"BZhpayload-by");
        Ok(())
    }

    #[test]
    fn download_file_returns_http_error() -> Result<()> {
        init_test_tracing();
        let temp_dir = TestDir::new()?;
        let dest = temp_dir.path().join("download.tsv.bz2");
        let transport = FakeTransport::with_outcomes(
            [ok_head(Some(0), false)],
            [status_get(StatusCode::NOT_FOUND)],
        );

        let err = download_file_with_transport(&transport, TEST_URL, &dest, false)
            .expect_err("404 should fail");

        assert!(err.to_string().contains("HTTP 404"));
        assert!(!dest.exists());
        Ok(())
    }

    #[test]
    fn download_file_honors_retry_after_on_rate_limit() -> Result<()> {
        init_test_tracing();
        let temp_dir = TestDir::new()?;
        let dest = temp_dir.path().join("download.tsv.bz2");
        let configured_retry_after = Duration::from_millis(100);
        let transport = FakeTransport::with_outcomes(
            [ok_head(Some(0), false)],
            std::iter::repeat_n(
                status_get_with_retry_after(StatusCode::TOO_MANY_REQUESTS, configured_retry_after),
                FETCH_MAX_RETRIES,
            )
            .collect::<Vec<_>>(),
        );

        let started = Instant::now();
        let err = download_file_with_transport(&transport, TEST_URL, &dest, false)
            .expect_err("exhausted 429 retries should fail");
        let elapsed = started.elapsed();

        assert!(err.to_string().contains("HTTP 429"));
        // FETCH_MAX_RETRIES - 1 inter-attempt sleeps of ~100ms each honoring
        // Retry-After — well under what the default (unlimited-status)
        // backoff schedule would take for the same number of sleeps.
        let retry_after_budget = configured_retry_after * (FETCH_MAX_RETRIES as u32 - 1) * 2;
        assert!(
            elapsed < retry_after_budget,
            "expected Retry-After to be honored instead of the rate-limit backoff, took {elapsed:?}"
        );
        Ok(())
    }

    #[test]
    fn download_file_uses_validated_existing_file() -> Result<()> {
        init_test_tracing();
        let temp_dir = TestDir::new()?;
        let dest = temp_dir.path().join("download.tsv.bz2");
        fs::write(&dest, b"BZhpayload-by")?;
        let transport = FakeTransport::with_outcomes([ok_head(Some(13), true)], []);

        download_file_with_transport(&transport, TEST_URL, &dest, false)?;

        assert_eq!(fs::read(&dest)?, b"BZhpayload-by");
        assert_eq!(transport.get_requests(), 0);
        Ok(())
    }

    #[test]
    fn download_file_redownloads_zero_length_destination() -> Result<()> {
        init_test_tracing();
        let temp_dir = TestDir::new()?;
        let dest = temp_dir.path().join("download.tsv.bz2");
        fs::write(&dest, [])?;
        let transport = FakeTransport::with_outcomes(
            [ok_head(Some(11), false)],
            [ok_get(b"BZhfresh-by", false)],
        );

        download_file_with_transport(&transport, TEST_URL, &dest, false)?;

        assert_eq!(fs::read(&dest)?, b"BZhfresh-by");
        Ok(())
    }

    #[test]
    fn download_file_rejects_payload_with_bad_magic_bytes() -> Result<()> {
        init_test_tracing();
        let temp_dir = TestDir::new()?;
        let dest = temp_dir.path().join("download.tsv.bz2");
        // Body deliberately does not start with "BZh"; this simulates a CDN
        // returning an HTML error page or a truncated/corrupted payload that
        // happens to satisfy Content-Length.
        let transport = FakeTransport::with_outcomes(
            [ok_head(Some(13), false)],
            [ok_get(b"<!DOCTYPE htm", false)],
        );

        let err = download_file_with_transport(&transport, TEST_URL, &dest, false)
            .expect_err("non-bz2 body should fail magic check");

        assert!(err.to_string().contains("bz2 magic"));
        assert!(!dest.exists(), "failed payload should be removed");
        Ok(())
    }

    #[test]
    fn verify_bz2_magic_accepts_real_magic_and_rejects_short_or_wrong_files() -> Result<()> {
        init_test_tracing();
        let temp_dir = TestDir::new()?;

        let good = temp_dir.path().join("good.bz2");
        fs::write(&good, b"BZh91AY")?;
        verify_bz2_magic(&good)?;

        let bad = temp_dir.path().join("bad.bz2");
        fs::write(&bad, b"GIF87a")?;
        let err = verify_bz2_magic(&bad).expect_err("non-bz2 should fail");
        assert!(err.to_string().contains("bz2 magic"));

        let empty = temp_dir.path().join("empty.bz2");
        fs::write(&empty, b"")?;
        let err = verify_bz2_magic(&empty).expect_err("empty file should fail");
        assert!(err.to_string().contains("does not begin with bz2 magic"));

        let tiny = temp_dir.path().join("tiny.bz2");
        fs::write(&tiny, b"BZ")?;
        let err = verify_bz2_magic(&tiny).expect_err("2-byte file should fail");
        assert!(err.to_string().contains("does not begin with bz2 magic"));

        Ok(())
    }

    #[test]
    fn redirect_policy_follows_one_hop_within_allowed_host() -> Result<()> {
        init_test_tracing();
        // Two responses on the same local port: a 302 to a different path on
        // the same host (which the policy must follow), then a 200 with a
        // small body. Drives the `Follow` arm of the closure.
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let address = listener.local_addr()?;
        let target_url = format!("http://{address}/dump.tsv.bz2");
        let redirect_url = format!("http://{address}/redirect");
        let target_url_for_thread = target_url.clone();
        let handle = thread::spawn(move || -> Result<()> {
            let responses = vec![
                format!(
                    "HTTP/1.1 302 Found\r\nLocation: {}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                    target_url_for_thread
                ),
                "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok".to_string(),
            ];
            for response in responses {
                let (mut stream, _) = listener.accept()?;
                let mut reader = BufReader::new(stream.try_clone()?);
                let mut line = String::new();
                while reader.read_line(&mut line)? > 0 && line != "\r\n" {
                    line.clear();
                }
                stream.write_all(response.as_bytes())?;
            }
            Ok(())
        });

        let client = reqwest::blocking::Client::builder()
            .redirect(redirect_policy_for_host("127.0.0.1".to_string()))
            .timeout(Duration::from_secs(2))
            .build()?;
        let response = client.get(&redirect_url).send()?;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.text()?, "ok");
        let _ = handle.join();
        Ok(())
    }
    // Note: the hop-ceiling test above intentionally leaks its server thread
    // because reqwest stops following at hop 10 and the listener's next
    // accept() blocks indefinitely. The thread dies when the test process
    // exits.

    #[test]
    fn redirect_policy_aborts_chains_past_hop_ceiling() -> Result<()> {
        init_test_tracing();
        // Repeated 302s within the allowed host. The policy's hop ceiling (10)
        // must reject the chain even though every hop is host-allowed; drives
        // the `TooManyHops` arm.
        let listener = TcpListener::bind("127.0.0.1:0")?;
        listener.set_nonblocking(false)?;
        let address = listener.local_addr()?;
        let entry_url = format!("http://{address}/hop/0");
        let address_str = address.to_string();
        // The thread is intentionally not joined: reqwest stops following at
        // the policy ceiling and the listener loop will block on its next
        // accept(). Letting the thread leak is acceptable for this test since
        // the listener+thread are dropped when the test process exits.
        thread::spawn(move || {
            for next in 1..14_u32 {
                let (mut stream, _) = listener.accept().expect("accept");
                let mut reader =
                    BufReader::new(stream.try_clone().expect("clone stream for reader"));
                let mut line = String::new();
                while reader.read_line(&mut line).unwrap_or(0) > 0 && line != "\r\n" {
                    line.clear();
                }
                let response = format!(
                    "HTTP/1.1 302 Found\r\nLocation: http://{address_str}/hop/{next}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                );
                let _ = stream.write_all(response.as_bytes());
            }
        });

        let client = reqwest::blocking::Client::builder()
            .redirect(redirect_policy_for_host("127.0.0.1".to_string()))
            .timeout(Duration::from_secs(5))
            .build()?;
        let err = client
            .get(&entry_url)
            .send()
            .expect_err("redirect chain past ceiling must fail");
        assert!(err.is_redirect());
        Ok(())
    }

    #[test]
    fn evaluate_redirect_returns_decisions_for_each_branch() {
        init_test_tracing();
        assert_eq!(
            evaluate_redirect(Some(DUMPS_HOST), 0),
            RedirectDecision::Follow
        );
        assert_eq!(
            evaluate_redirect(Some(DUMPS_HOST), REDIRECT_MAX_HOPS - 1),
            RedirectDecision::Follow,
        );
        assert_eq!(
            evaluate_redirect(Some(DUMPS_HOST), REDIRECT_MAX_HOPS),
            RedirectDecision::TooManyHops,
        );
        assert_eq!(
            evaluate_redirect(Some("evil.example.com"), 0),
            RedirectDecision::BlockedHost("evil.example.com".to_owned()),
        );
        assert_eq!(
            evaluate_redirect(None, 0),
            RedirectDecision::BlockedHost(String::new()),
        );
    }

    #[test]
    fn dumps_host_only_redirect_policy_blocks_offsite_targets() -> Result<()> {
        init_test_tracing();
        // Stand up a local server that 302s to evil.example.com. The custom
        // redirect policy must refuse to follow the redirect; the request
        // surfaces as an error rather than silently traveling to the
        // non-dumps host.
        let response =
            "HTTP/1.1 302 Found\r\nLocation: http://evil.example.com/\r\nContent-Length: 0\r\n\r\n"
                .to_string();
        let (url, _requests, handle) = spawn_test_server(vec![response])?;
        let client = reqwest::blocking::Client::builder()
            .redirect(dumps_host_only_redirect_policy())
            .timeout(Duration::from_secs(2))
            .build()?;
        let err = client
            .get(&url)
            .send()
            .expect_err("redirect to non-dumps host must fail");
        assert!(err.is_redirect());
        let _ = handle.join();
        Ok(())
    }

    #[test]
    fn download_file_resumes_partial_destination() -> Result<()> {
        init_test_tracing();
        let temp_dir = TestDir::new()?;
        let dest = temp_dir.path().join("download.tsv.bz2");
        fs::write(&dest, b"BZhpaylo")?;
        let transport = FakeTransport::with_outcomes(
            [ok_head(Some(13), true)],
            [ok_get(b"BZhpayload-by", true)],
        );

        download_file_with_transport(&transport, TEST_URL, &dest, false)?;

        assert_eq!(fs::read(&dest)?, b"BZhpayload-by");
        assert_eq!(transport.requested_ranges(), vec![Some(8)]);
        Ok(())
    }

    #[test]
    fn download_file_retries_transient_failures() -> Result<()> {
        init_test_tracing();
        let temp_dir = TestDir::new()?;
        let dest = temp_dir.path().join("download.tsv.bz2");
        let transport = FakeTransport::with_outcomes(
            [ok_head(Some(13), false)],
            [
                status_get(StatusCode::SERVICE_UNAVAILABLE),
                ok_get(b"BZhpayload-by", false),
            ],
        );

        download_file_with_transport(&transport, TEST_URL, &dest, false)?;

        assert_eq!(fs::read(&dest)?, b"BZhpayload-by");
        assert_eq!(transport.get_requests(), 2);
        Ok(())
    }

    #[test]
    fn download_file_redownloads_when_head_is_unsupported() -> Result<()> {
        init_test_tracing();
        let temp_dir = TestDir::new()?;
        let dest = temp_dir.path().join("download.tsv.bz2");
        fs::write(&dest, b"stale")?;
        let transport = FakeTransport::with_outcomes(
            [status_head(StatusCode::METHOD_NOT_ALLOWED)],
            [ok_get(b"BZhpayload-by", false)],
        );

        download_file_with_transport(&transport, TEST_URL, &dest, false)?;

        assert_eq!(fs::read(&dest)?, b"BZhpayload-by");
        Ok(())
    }

    #[test]
    fn create_progress_bar_sets_visible_length() {
        let progress = create_progress_bar(Path::new("dump.tsv.bz2"), Some(42), 7, true);
        assert_eq!(progress.length(), Some(42));
        assert_eq!(progress.position(), 7);
    }

    #[test]
    fn parse_transport_head_reads_length_and_range_support() {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_LENGTH, "13".parse().expect("content length header"));
        headers.insert(
            ACCEPT_RANGES,
            "bytes".parse().expect("accept ranges header"),
        );
        headers.insert(ETAG, "fixture-etag".parse().expect("etag header"));
        headers.insert(
            LAST_MODIFIED,
            "fixture-date".parse().expect("last-modified header"),
        );

        let head = parse_transport_head(StatusCode::OK, &headers, None);
        assert_eq!(head.status, StatusCode::OK);
        assert_eq!(head.content_length, Some(13));
        assert!(head.accepts_ranges);
        assert_eq!(head.etag.as_deref(), Some("fixture-etag"));
        assert_eq!(head.last_modified.as_deref(), Some("fixture-date"));
    }

    #[test]
    fn parse_transport_head_falls_back_to_response_length_when_header_is_invalid() {
        let mut headers = HeaderMap::new();
        headers.insert(
            CONTENT_LENGTH,
            "not-a-number"
                .parse()
                .expect("invalid content length header"),
        );

        let head = parse_transport_head(StatusCode::OK, &headers, Some(5));
        assert_eq!(head.content_length, Some(5));
        assert!(!head.accepts_ranges);
    }

    #[test]
    fn parse_retry_after_reads_delta_seconds() {
        let mut headers = HeaderMap::new();
        headers.insert(RETRY_AFTER, "5".parse().expect("retry-after header"));

        assert_eq!(parse_retry_after(&headers), Some(Duration::from_secs(5)));
    }

    #[test]
    fn parse_retry_after_clamps_to_max() {
        let mut headers = HeaderMap::new();
        headers.insert(RETRY_AFTER, "999".parse().expect("retry-after header"));

        assert_eq!(
            parse_retry_after(&headers),
            Some(Duration::from_secs(FETCH_RETRY_AFTER_MAX_SECS))
        );
    }

    #[test]
    fn parse_retry_after_ignores_non_numeric_value() {
        let mut headers = HeaderMap::new();
        headers.insert(
            RETRY_AFTER,
            "Wed, 21 Oct 2026 07:28:00 GMT"
                .parse()
                .expect("retry-after header"),
        );

        assert_eq!(parse_retry_after(&headers), None);
    }

    #[test]
    fn parse_retry_after_returns_none_when_header_missing() {
        assert_eq!(parse_retry_after(&HeaderMap::new()), None);
    }

    #[test]
    fn retry_delay_honors_retry_after_over_backoff() {
        assert_eq!(
            retry_delay(1, true, Some(Duration::from_secs(7))),
            Duration::from_secs(7)
        );
    }

    #[test]
    fn retry_delay_uses_longer_base_when_rate_limited() {
        let limited = retry_delay(1, true, None);
        let unlimited = retry_delay(1, false, None);
        assert!(limited > unlimited);
        assert_eq!(limited, Duration::from_millis(FETCH_RATE_LIMIT_BACKOFF_MS));
        assert_eq!(unlimited, Duration::from_millis(FETCH_RETRY_BACKOFF_MS));
    }

    #[test]
    fn retry_delay_doubles_per_attempt() {
        assert_eq!(
            retry_delay(2, false, None),
            Duration::from_millis(FETCH_RETRY_BACKOFF_MS * 2)
        );
        assert_eq!(
            retry_delay(3, false, None),
            Duration::from_millis(FETCH_RETRY_BACKOFF_MS * 4)
        );
    }

    #[test]
    fn retry_delay_caps_computed_backoff() {
        // With FETCH_MAX_RETRIES raised, later attempts would otherwise
        // double past several minutes; the cap keeps a single file's retry
        // loop from stalling the whole (now-serialized) fetch.
        let uncapped = retry_delay(FETCH_MAX_RETRIES, true, None);
        assert_eq!(uncapped, Duration::from_millis(FETCH_MAX_BACKOFF_MS));
    }

    #[test]
    fn build_get_request_sets_range_header() -> Result<()> {
        let transport = build_transport()?;
        let request = build_get_request(&transport.client, TEST_URL, Some(8))?;
        assert_eq!(
            request
                .headers()
                .get(RANGE)
                .and_then(|value| value.to_str().ok()),
            Some("bytes=8-")
        );
        Ok(())
    }

    #[test]
    fn build_get_request_omits_range_header_without_resume() -> Result<()> {
        let transport = build_transport()?;
        let request = build_get_request(&transport.client, TEST_URL, None)?;
        assert!(request.headers().get(RANGE).is_none());
        Ok(())
    }

    #[test]
    fn build_transport_response_preserves_metadata_and_body() -> Result<()> {
        let mut response = build_transport_response(
            StatusCode::PARTIAL_CONTENT,
            Some(5),
            None,
            Box::new(Cursor::new(b"bytes".to_vec())),
        );
        let mut body = Vec::new();
        response.body.read_to_end(&mut body)?;

        assert_eq!(response.status, StatusCode::PARTIAL_CONTENT);
        assert_eq!(response.content_length, Some(5));
        assert_eq!(body, b"bytes");
        Ok(())
    }

    #[test]
    fn reqwest_transport_head_propagates_connection_errors() -> Result<()> {
        let transport = build_transport()?;
        let err = transport
            .head("http://127.0.0.1:1/dump.tsv.bz2")
            .expect_err("closed port should fail");
        assert!(!err.to_string().is_empty());
        Ok(())
    }

    #[test]
    fn reqwest_transport_get_propagates_connection_errors() -> Result<()> {
        let transport = build_transport()?;
        let result = transport.get("http://127.0.0.1:1/dump.tsv.bz2", Some(8));
        assert!(result.is_err());
        let err = result.err().expect("checked error result");
        assert!(!err.to_string().is_empty());
        Ok(())
    }

    #[test]
    fn reqwest_transport_successfully_reads_head_and_get_responses() -> Result<()> {
        let responses = vec![
            "HTTP/1.1 200 OK\r\nContent-Length: 13\r\nAccept-Ranges: bytes\r\n\r\n".to_string(),
            "HTTP/1.1 206 Partial Content\r\nContent-Length: 5\r\n\r\nbytes".to_string(),
        ];
        let (url, requests, server) = spawn_test_server(responses)?;
        let transport = build_transport()?;

        let head = transport.head(&url)?;
        let mut response = transport.get(&url, Some(8))?;
        let mut body = Vec::new();
        response.body.read_to_end(&mut body)?;

        assert_eq!(head.status, StatusCode::OK);
        assert_eq!(head.content_length, Some(13));
        assert!(head.accepts_ranges);
        assert_eq!(response.status, StatusCode::PARTIAL_CONTENT);
        assert_eq!(response.content_length, Some(5));
        assert_eq!(body, b"bytes");

        server.join().expect("server thread")?;
        let requests = requests.lock().expect("request log");
        assert!(requests[0].starts_with("HEAD /dump.tsv.bz2 HTTP/1.1\r\n"));
        assert!(requests[1].starts_with("GET /dump.tsv.bz2 HTTP/1.1\r\n"));
        assert!(
            requests[1]
                .to_ascii_lowercase()
                .contains("range: bytes=8-\r\n")
        );
        Ok(())
    }

    #[test]
    fn fetch_wiki_uses_existing_files_without_downloading() -> Result<()> {
        init_test_tracing();
        let data_dir = TestDir::new()?;
        let wiki = "simplewiki";
        let filename = "2026-02.simplewiki.all-time.tsv.bz2";
        let raw_dir = data_dir.path().join("raw").join(wiki);
        fs::create_dir_all(&raw_dir)?;
        let existing = raw_dir.join(filename);
        fs::write(&existing, b"already-here")?;
        let transport =
            FakeTransport::with_outcomes([ok_head(Some(12), true), ok_head(Some(12), true)], []);
        let paths = fetch_wiki_with_transport(
            &transport,
            "http://example.invalid",
            wiki,
            "2026-02",
            data_dir.path(),
        )
        .expect("an existing dump should pass orchestration without downloading");

        assert_eq!(paths, vec![existing]);
        assert_eq!(transport.get_requests(), 0);
        Ok(())
    }

    #[test]
    fn fetch_wiki_skips_sources_covered_by_snapshot_ingest_markers() -> Result<()> {
        init_test_tracing();
        let data_dir = TestDir::new()?;
        let wiki = "simplewiki";
        let version = "2026-02";
        let filename = "2026-02.simplewiki.all-time.tsv.bz2";
        let analytical =
            crate::storage::snapshot_analytical_wiki_dir(data_dir.path(), wiki, version)?;
        crate::storage::write_test_marker_in(
            data_dir.path(),
            &analytical,
            &crate::ingest::ingest_source_id(Path::new(filename))?,
        )
        .expect("covered source marker should be written");
        let transport = FakeTransport::default();

        let paths = fetch_wiki_with_transport(
            &transport,
            "http://example.invalid",
            wiki,
            version,
            data_dir.path(),
        )
        .expect("covered snapshot should skip network work");

        assert!(paths.is_empty());
        assert_eq!(transport.get_requests(), 0);
        Ok(())
    }

    #[test]
    fn snapshot_resolution_uses_latest_complete_bounded_fallback() -> Result<()> {
        init_test_tracing();
        let transport = FakeTransport::with_head_outcomes([
            status_head(StatusCode::NOT_FOUND),
            ok_head(Some(42), false),
        ]);
        let now = Utc.with_ymd_and_hms(2026, 8, 22, 0, 0, 0).unwrap();

        let selected = resolve_latest_completed_snapshot_with_transport(
            &transport,
            "http://example.invalid",
            &["simplewiki".to_string()],
            now,
            2,
        )
        .expect("bounded fallback should resolve");

        assert_eq!(selected, "2026-06");
        assert_eq!(snapshot_version_at_lag(now, 8), "2025-12");
        assert_eq!(snapshot_max_lag_months(None)?, 2);
        assert_eq!(snapshot_max_lag_months(Some(OsStr::new("4")))?, 4);
        Ok(())
    }

    #[test]
    fn exact_snapshot_validation_fails_before_download_with_actionable_guidance() {
        let data_dir = TestDir::new().expect("validation fixture");
        let transport = FakeTransport::with_head_outcomes([status_head(StatusCode::NOT_FOUND)]);

        let error = validate_completed_snapshot_with_transport(
            &transport,
            "http://example.invalid",
            data_dir.path(),
            "simplewiki",
            "2026-08",
        )
        .expect_err("an unavailable exact snapshot must fail closed");

        assert!(error.to_string().contains("requested snapshot 2026-08"));
        assert!(error.to_string().contains("omit --version"));
        assert_eq!(transport.get_requests(), 0);
        assert_eq!(transport.head_requests(), 1);
    }

    #[test]
    fn exact_snapshot_validation_accepts_and_receipts_a_complete_inventory() -> Result<()> {
        let data_dir = TestDir::new()?;
        let transport = FakeTransport::with_head_outcomes([ok_head(Some(42), true)]);

        validate_completed_snapshot_with_transport(
            &transport,
            "http://example.invalid",
            data_dir.path(),
            "simplewiki",
            "2026-08",
        )
        .expect("complete remote inventory should validate");

        let plan =
            SnapshotPlan::resolve_from_base("http://example.invalid", "simplewiki", "2026-08")?;
        assert!(read_remote_inventory(data_dir.path(), &plan)?.is_some());
        assert_eq!(transport.get_requests(), 0);
        assert_eq!(transport.head_requests(), 1);

        let cached_transport = FakeTransport::default();
        validate_completed_snapshot_with_transport(
            &cached_transport,
            "http://example.invalid",
            data_dir.path(),
            "simplewiki",
            "2026-08",
        )
        .expect("the authenticated inventory should make validation a no-op");
        assert_eq!(cached_transport.head_requests(), 0);
        Ok(())
    }

    #[test]
    fn cached_snapshot_resolution_reuses_completed_fallback_inventory() -> Result<()> {
        let data_dir = TestDir::new()?;
        let now = Utc.with_ymd_and_hms(2026, 8, 22, 0, 0, 0).unwrap();
        let wikis = ["simplewiki".to_string()];
        let first = FakeTransport::with_head_outcomes([
            status_head(StatusCode::NOT_FOUND),
            ok_head(Some(42), false),
        ]);
        assert_eq!(
            resolve_latest_completed_snapshot_cached_with_transport(
                &first,
                "http://example.invalid",
                data_dir.path(),
                &wikis,
                now,
                2,
            )
            .expect("cached fallback should resolve"),
            "2026-06"
        );
        assert_eq!(first.head_requests(), 2);

        let second = FakeTransport::with_head_outcomes([status_head(StatusCode::NOT_FOUND)]);
        assert_eq!(
            resolve_latest_completed_snapshot_cached_with_transport(
                &second,
                "http://example.invalid",
                data_dir.path(),
                &wikis,
                now,
                2,
            )
            .expect("completed fallback inventory should be reusable"),
            "2026-06"
        );
        assert_eq!(second.head_requests(), 1);

        let inventory = remote_inventory_path(data_dir.path(), "simplewiki", "2026-06")?;
        fs::write(&inventory, b"{truncated")?;
        let repaired = FakeTransport::with_head_outcomes([
            status_head(StatusCode::NOT_FOUND),
            ok_head(Some(43), false),
        ]);
        assert_eq!(
            resolve_latest_completed_snapshot_cached_with_transport(
                &repaired,
                "http://example.invalid",
                data_dir.path(),
                &wikis,
                now,
                2,
            )
            .expect("invalid inventory should be repaired"),
            "2026-06"
        );
        assert_eq!(repaired.head_requests(), 2);
        Ok(())
    }

    #[test]
    fn cached_snapshot_resolution_fails_closed_on_invalid_bounds_and_missing_sources() {
        let data_dir = TestDir::new().expect("cache fixture");
        let now = Utc.with_ymd_and_hms(2026, 8, 22, 0, 0, 0).unwrap();
        assert!(
            resolve_latest_completed_snapshot_cached_with_transport(
                &FakeTransport::default(),
                "http://example.invalid",
                data_dir.path(),
                &[],
                now,
                2,
            )
            .is_err()
        );
        assert!(
            resolve_latest_completed_snapshot_cached_with_transport(
                &FakeTransport::default(),
                "http://example.invalid",
                data_dir.path(),
                &["simplewiki".to_string()],
                now,
                0,
            )
            .is_err()
        );
        let missing = FakeTransport::with_head_outcomes([
            status_head(StatusCode::NOT_FOUND),
            status_head(StatusCode::NOT_FOUND),
        ]);
        assert!(
            resolve_latest_completed_snapshot_cached_with_transport(
                &missing,
                "http://example.invalid",
                data_dir.path(),
                &["simplewiki".to_string()],
                now,
                2,
            )
            .is_err()
        );
    }

    #[test]
    fn snapshot_resolution_fails_closed_on_configuration_or_stale_dumps() {
        let now = Utc.with_ymd_and_hms(2026, 8, 22, 0, 0, 0).unwrap();
        assert!(snapshot_max_lag_months(Some(OsStr::new("0"))).is_err());
        assert!(snapshot_max_lag_months(Some(OsStr::new("old"))).is_err());
        assert!(
            resolve_latest_completed_snapshot_with_transport(
                &FakeTransport::default(),
                "http://example.invalid",
                &[],
                now,
                2,
            )
            .is_err()
        );
        assert!(
            resolve_latest_completed_snapshot_with_transport(
                &FakeTransport::default(),
                "http://example.invalid",
                &["simplewiki".to_string()],
                now,
                0,
            )
            .is_err()
        );
        let missing = FakeTransport::with_head_outcomes([
            status_head(StatusCode::NOT_FOUND),
            status_head(StatusCode::NOT_FOUND),
        ]);
        let error = resolve_latest_completed_snapshot_with_transport(
            &missing,
            "http://example.invalid",
            &["simplewiki".to_string()],
            now,
            2,
        )
        .expect_err("old snapshots must not be silently selected");
        assert!(
            error
                .to_string()
                .contains("no completed Wikimedia snapshot")
        );
    }

    #[test]
    fn snapshot_completion_probe_rejects_indeterminate_http_status() {
        let transport = FakeTransport::with_head_outcomes([status_head(StatusCode::FORBIDDEN)]);
        let error = snapshot_source_exists(&transport, TEST_URL)
            .expect_err("completion requires an authoritative status");
        assert!(
            error
                .to_string()
                .contains("cannot determine dump completion")
        );
    }

    #[test]
    fn snapshot_completion_probe_retries_and_reports_exhaustion_without_waiting() -> Result<()> {
        let retrying = FakeTransport::with_head_outcomes([
            status_head_with_retry_after(StatusCode::TOO_MANY_REQUESTS, Duration::from_secs(1)),
            ok_head(Some(42), false),
        ]);
        let mut sleeps = Vec::new();
        let exists = snapshot_source_exists_with_sleep(
            &retrying,
            TEST_URL,
            |attempt, limited, retry_after| sleeps.push((attempt, limited, retry_after)),
        )
        .expect("retry should recover");
        assert!(exists);
        assert_eq!(sleeps, vec![(1, true, Some(Duration::from_secs(1)))]);

        let failing = FakeTransport::with_head_outcomes(
            (0..FETCH_MAX_RETRIES).map(|_| FakeHeadOutcome::Error("offline")),
        );
        let error = snapshot_source_exists_with_sleep(&failing, TEST_URL, |_, _, _| {})
            .expect_err("persistent transport failure must fail closed");
        assert!(error.to_string().contains("offline"));
        Ok(())
    }

    #[test]
    fn snapshot_resolution_selects_expected_previous_month_without_warning() -> Result<()> {
        let now = Utc.with_ymd_and_hms(2026, 8, 22, 0, 0, 0).unwrap();
        let selected = resolve_latest_completed_snapshot_with_transport(
            &FakeTransport::with_head_outcomes([ok_head(Some(42), false)]),
            "http://example.invalid",
            &["simplewiki".to_string()],
            now,
            2,
        )
        .expect("previous month is complete");
        assert_eq!(selected, "2026-07");
        Ok(())
    }

    #[test]
    fn fetch_parallelism_defaults_when_env_is_unset() {
        init_test_tracing();

        assert_eq!(fetch_parallelism_override(0, None), 1);
        // Default is serialized (FETCH_MAX_PARALLELISM == 1) regardless of
        // file count, unless overridden via WIKI_ECON_FETCH_MAX_PARALLELISM.
        assert_eq!(fetch_parallelism_override(2, None), FETCH_MAX_PARALLELISM);
        assert_eq!(fetch_parallelism_override(20, None), FETCH_MAX_PARALLELISM);
    }

    #[test]
    fn fetch_parallelism_honors_env_override() {
        init_test_tracing();

        assert_eq!(fetch_parallelism_override(20, Some(OsStr::new("1"))), 1);
        assert_eq!(fetch_parallelism_override(1, Some(OsStr::new("1"))), 1);
    }

    #[test]
    fn fetch_parallelism_ignores_invalid_env_override() {
        init_test_tracing();

        assert_eq!(
            fetch_parallelism_override(20, Some(OsStr::new("0"))),
            FETCH_MAX_PARALLELISM
        );
        assert_eq!(
            fetch_parallelism_override(20, Some(OsStr::new("not-a-number"))),
            FETCH_MAX_PARALLELISM
        );
    }

    #[test]
    fn fetch_wiki_downloads_multiple_yearly_files() -> Result<()> {
        init_test_tracing();
        let data_dir = TestDir::new()?;
        let wiki = "frwiki";
        let transport = FakeTransport::with_outcomes(
            [ok_head(Some(13), false), ok_head(Some(13), false)],
            [
                ok_get(b"BZhpayload-by", false),
                ok_get(b"BZhpayload-by", false),
            ],
        );
        let files = build_file_list(wiki, "2002-01")?;
        let paths = fetch_wiki_from_base_with_transport_at_parallelism(
            &transport,
            "http://example.invalid",
            wiki,
            "2002-01",
            data_dir.path(),
            files,
            2,
        )
        .expect("parallel fetch should complete");

        assert_eq!(paths.len(), 2);
        assert!(paths.iter().all(|path| path.exists()));
        assert_eq!(transport.get_requests(), 2);
        Ok(())
    }

    #[test]
    fn fetch_wiki_persists_and_uses_monthly_snapshot_plan() -> Result<()> {
        init_test_tracing();
        let data_dir = TestDir::new()?;
        let wiki = "enwiki";
        let version = "2001-01";
        let transport = FakeTransport::with_outcomes(
            [
                ok_head(Some(13), false),
                ok_head(Some(13), false),
                ok_head(Some(13), false),
                ok_head(Some(13), false),
            ],
            [
                ok_get(b"BZhpayload-by", false),
                ok_get(b"BZhpayload-by", false),
            ],
        );

        let paths = fetch_wiki_with_transport(
            &transport,
            "http://example.invalid",
            wiki,
            version,
            data_dir.path(),
        )
        .expect("monthly plan fetch should succeed");

        assert_eq!(paths.len(), 2);
        assert!(paths[0].ends_with("2001-01.enwiki.2001-01.tsv.bz2"));
        assert!(paths[1].ends_with("2001-01.enwiki.2001-02.tsv.bz2"));
        assert_eq!(transport.get_requests(), 2);
        let plan_path = crate::snapshot_plan::plan_path(data_dir.path(), wiki, version)?;
        let plan = SnapshotPlan::load(&plan_path)?;
        assert_eq!(plan.filenames()?.len(), 2);
        Ok(())
    }

    #[test]
    fn monthly_snapshot_completion_rejects_a_missing_expected_month() -> Result<()> {
        let transport = FakeTransport::with_head_outcomes([
            ok_head(Some(13), false),
            status_head(StatusCode::NOT_FOUND),
        ]);
        assert!(
            !snapshot_is_complete(
                &transport,
                "http://example.invalid",
                &["enwiki".to_string()],
                "2001-01",
            )
            .expect("monthly completeness probe should return a result")
        );
        Ok(())
    }

    #[test]
    fn yearly_snapshot_completion_probes_only_reviewed_sparse_sources() -> Result<()> {
        let transport =
            FakeTransport::with_head_outcomes((0..25).map(|_| ok_head(Some(13), false)));
        let complete = snapshot_is_complete(
            &transport,
            "http://example.invalid",
            &["arwiki".to_string()],
            "2026-07",
        )
        .expect("the reviewed sparse Arabic inventory should be complete");
        assert!(complete);
        assert_eq!(transport.head_requests(), 25);
        Ok(())
    }

    #[test]
    fn completed_snapshot_inventory_eliminates_repeated_monthly_head_probes() -> Result<()> {
        let data_dir = TestDir::new()?;
        let wiki = "enwiki";
        let version = "2001-01";
        let first = FakeTransport::with_head_outcomes([
            FakeHeadOutcome::Response(TransportHead {
                status: StatusCode::OK,
                content_length: Some(22),
                accepts_ranges: false,
                retry_after: None,
                etag: Some("fixture-etag".to_string()),
                last_modified: Some("fixture-date".to_string()),
            }),
            ok_head(Some(11), true),
        ]);
        assert!(
            snapshot_wiki_is_complete_cached(
                &first,
                "http://example.invalid",
                data_dir.path(),
                wiki,
                version,
            )
            .expect("monthly inventory should be complete")
        );
        assert_eq!(first.head_requests(), 2);

        let second = FakeTransport::default();
        assert!(
            snapshot_wiki_is_complete_cached(
                &second,
                "http://example.invalid",
                data_dir.path(),
                wiki,
                version,
            )
            .expect("monthly inventory should be reused")
        );
        assert_eq!(second.head_requests(), 0);

        let plan_path = crate::snapshot_plan::plan_path(data_dir.path(), wiki, version)?;
        let plan = SnapshotPlan::load(&plan_path)?;
        assert_eq!(
            snapshot_source_sizes(data_dir.path(), wiki, version, &plan.sources)?,
            vec![Some(11), Some(22)]
        );
        let inventory_path = remote_inventory_path(data_dir.path(), wiki, version)?;
        let inventory: CompletedSnapshotReceipt =
            serde_json::from_slice(&fs::read(&inventory_path)?)?;
        assert_eq!(inventory.sources[1].etag.as_deref(), Some("fixture-etag"));
        assert_eq!(
            inventory.sources[1].last_modified.as_deref(),
            Some("fixture-date")
        );

        let mut stale = inventory.clone();
        stale.source_count += 1;
        fs::write(&inventory_path, serde_json::to_vec(&stale)?)?;
        assert!(read_remote_inventory(data_dir.path(), &plan)?.is_none());
        write_remote_inventory(data_dir.path(), &plan, &inventory)?;

        fs::remove_file(&inventory_path)?;
        fs::create_dir(&inventory_path)?;
        assert!(write_remote_inventory(data_dir.path(), &plan, &inventory).is_err());
        assert!(
            !inventory_path
                .parent()
                .context("inventory parent")?
                .join(format!(
                    ".{REMOTE_INVENTORY_FILENAME}.{}.tmp",
                    std::process::id()
                ))
                .exists()
        );
        Ok(())
    }

    #[test]
    fn fetch_wiki_downloads_only_years_missing_valid_markers() -> Result<()> {
        init_test_tracing();
        let data_dir = TestDir::new()?;
        let wiki = "frwiki";
        let version = "2002-01";
        let analytical =
            crate::storage::snapshot_analytical_wiki_dir(data_dir.path(), wiki, version)?;
        let covered = "2002-01.frwiki.2001.tsv.bz2";
        crate::storage::write_test_marker_in(
            data_dir.path(),
            &analytical,
            &crate::ingest::ingest_source_id(Path::new(covered))?,
        )
        .expect("covered year marker should be written");
        let transport = FakeTransport::with_outcomes(
            [ok_head(None, false), ok_head(Some(13), false)],
            [ok_get(b"BZhpayload-by", false)],
        );

        let paths = fetch_wiki_with_transport(
            &transport,
            "http://example.invalid",
            wiki,
            version,
            data_dir.path(),
        )
        .expect("only the uncovered year should download");

        assert_eq!(paths.len(), 1);
        assert!(paths[0].ends_with("2002-01.frwiki.2002.tsv.bz2"));
        assert_eq!(transport.get_requests(), 1);
        Ok(())
    }

    #[test]
    fn download_file_cleans_up_when_destination_cannot_be_created() -> Result<()> {
        init_test_tracing();
        let temp_dir = TestDir::new()?;
        let dest = temp_dir.path().join("missing").join("download.tsv.bz2");
        let transport = FakeTransport::with_outcomes(
            [ok_head(Some(11), false)],
            [ok_get(b"BZhfresh-by", false)],
        );
        let err = download_file_with_transport(&transport, TEST_URL, &dest, false)
            .expect_err("missing parent directory should fail");

        assert!(!dest.exists());
        assert!(!err.to_string().is_empty());
        Ok(())
    }

    #[test]
    fn probe_remote_file_returns_none_after_retryable_head_failures() -> Result<()> {
        init_test_tracing();
        let transport = FakeTransport::with_head_outcomes(
            std::iter::repeat_n(
                status_head(StatusCode::SERVICE_UNAVAILABLE),
                FETCH_MAX_RETRIES,
            )
            .collect::<Vec<_>>(),
        );

        assert_eq!(probe_remote_file(&transport, TEST_URL)?, None);
        Ok(())
    }

    #[test]
    fn probe_remote_file_honors_retry_after_on_rate_limit() -> Result<()> {
        init_test_tracing();
        let configured_retry_after = Duration::from_millis(100);
        let transport = FakeTransport::with_head_outcomes(
            std::iter::repeat_n(
                status_head_with_retry_after(StatusCode::TOO_MANY_REQUESTS, configured_retry_after),
                FETCH_MAX_RETRIES,
            )
            .collect::<Vec<_>>(),
        );

        let started = Instant::now();
        assert_eq!(probe_remote_file(&transport, TEST_URL)?, None);
        let elapsed = started.elapsed();

        // FETCH_MAX_RETRIES - 1 inter-attempt sleeps of ~100ms each honoring
        // Retry-After — well under what the default (unlimited-status)
        // backoff schedule would take for the same number of sleeps.
        let retry_after_budget = configured_retry_after * (FETCH_MAX_RETRIES as u32 - 1) * 2;
        assert!(
            elapsed < retry_after_budget,
            "expected Retry-After to be honored instead of the default backoff, took {elapsed:?}"
        );
        Ok(())
    }

    #[test]
    fn probe_remote_file_treats_other_non_success_status_as_unvalidated() -> Result<()> {
        init_test_tracing();
        let transport = FakeTransport::with_head_outcomes([status_head(StatusCode::IM_A_TEAPOT)]);

        assert_eq!(probe_remote_file(&transport, TEST_URL)?, None);
        Ok(())
    }

    #[test]
    fn probe_remote_file_errors_on_missing_remote_file() {
        init_test_tracing();
        let transport = FakeTransport::with_head_outcomes([status_head(StatusCode::NOT_FOUND)]);

        let err = probe_remote_file(&transport, TEST_URL).expect_err("404 should fail");
        assert!(err.to_string().contains("HTTP 404"));
    }

    #[test]
    fn probe_remote_file_treats_network_errors_as_unvalidated() -> Result<()> {
        init_test_tracing();
        let transport = FakeTransport::with_head_outcomes([
            FakeHeadOutcome::Error("timeout"),
            FakeHeadOutcome::Error("timeout"),
            FakeHeadOutcome::Error("timeout"),
        ]);

        assert_eq!(probe_remote_file(&transport, TEST_URL)?, None);
        Ok(())
    }

    #[test]
    fn check_disk_headroom_passes_when_space_is_sufficient() -> Result<()> {
        init_test_tracing();
        let temp_dir = TestDir::new()?;
        let transport = FakeTransport::with_head_outcomes([ok_head(Some(1_024), true)]);
        check_disk_headroom_with_available(
            &transport,
            "http://example.invalid",
            "testwiki",
            "2026-02",
            &["2026-02.testwiki.all-time.tsv.bz2".to_string()],
            temp_dir.path(),
            |_| Ok(FETCH_DISK_HEADROOM_MARGIN_BYTES + 1_024),
        )
        .expect("headroom check should pass");
        Ok(())
    }

    #[test]
    fn check_disk_headroom_skips_files_with_unknown_remote_size() -> Result<()> {
        init_test_tracing();
        let temp_dir = TestDir::new()?;
        let transport = FakeTransport::with_head_outcomes([ok_head(None, false)]);

        check_disk_headroom(
            &transport,
            "http://example.invalid",
            "testwiki",
            "2026-02",
            &["2026-02.testwiki.all-time.tsv.bz2".to_string()],
            temp_dir.path(),
        )
        .expect("unknown remote size should not block the fetch");
        Ok(())
    }

    #[test]
    fn check_disk_headroom_credits_already_downloaded_bytes() -> Result<()> {
        init_test_tracing();
        let temp_dir = TestDir::new()?;
        let raw_dir = temp_dir.path().join("raw").join("testwiki");
        fs::create_dir_all(&raw_dir)?;
        fs::write(raw_dir.join("2026-02.testwiki.all-time.tsv.bz2"), b"BZh")?;
        let transport = FakeTransport::with_head_outcomes([ok_head(Some(3), true)]);

        check_disk_headroom(
            &transport,
            "http://example.invalid",
            "testwiki",
            "2026-02",
            &["2026-02.testwiki.all-time.tsv.bz2".to_string()],
            temp_dir.path(),
        )
        .expect("fully downloaded bytes should require no more space");
        Ok(())
    }

    #[test]
    fn check_disk_headroom_skips_failed_space_query() {
        init_test_tracing();
        let temp_dir = TestDir::new().expect("temporary directory");
        let missing_data_dir = temp_dir.path().join("missing");
        let transport = FakeTransport::with_head_outcomes([ok_head(Some(1024), true)]);

        check_disk_headroom(
            &transport,
            "http://example.invalid",
            "testwiki",
            "2026-02",
            &["2026-02.testwiki.all-time.tsv.bz2".to_string()],
            &missing_data_dir,
        )
        .expect("an unavailable filesystem-space query is best-effort");
    }

    #[test]
    fn check_disk_headroom_fails_when_space_is_insufficient() -> Result<()> {
        init_test_tracing();
        let temp_dir = TestDir::new()?;
        let transport = FakeTransport::with_head_outcomes([ok_head(Some(u64::MAX / 2), true)]);

        let err = check_disk_headroom(
            &transport,
            "http://example.invalid",
            "testwiki",
            "2026-02",
            &["2026-02.testwiki.all-time.tsv.bz2".to_string()],
            temp_dir.path(),
        )
        .expect_err("an exabyte-scale file should never fit");

        assert!(err.to_string().contains("insufficient disk space"));
        Ok(())
    }

    #[test]
    fn plan_download_redownloads_oversized_local_file() -> Result<()> {
        init_test_tracing();
        let temp_dir = TestDir::new()?;
        let dest = temp_dir.path().join("download.tsv.bz2");
        fs::write(&dest, b"oversized-payload")?;

        let plan = plan_download(&dest, Some(remote_file(Some(4), false)))?;

        assert_eq!(
            plan,
            Some(DownloadPlan {
                resume_from: 0,
                total_size: Some(4),
                accepts_ranges: false,
            })
        );
        assert!(!dest.exists());
        Ok(())
    }

    #[test]
    fn plan_download_redownloads_when_remote_size_is_unknown() -> Result<()> {
        init_test_tracing();
        let temp_dir = TestDir::new()?;
        let dest = temp_dir.path().join("download.tsv.bz2");
        fs::write(&dest, b"stale")?;

        let plan = plan_download(&dest, Some(remote_file(None, true)))?;

        assert_eq!(
            plan,
            Some(DownloadPlan {
                resume_from: 0,
                total_size: None,
                accepts_ranges: true,
            })
        );
        assert!(!dest.exists());
        Ok(())
    }

    #[test]
    fn plan_download_redownloads_partial_file_without_range_support() -> Result<()> {
        init_test_tracing();
        let temp_dir = TestDir::new()?;
        let dest = temp_dir.path().join("download.tsv.bz2");
        fs::write(&dest, b"partial")?;

        let plan = plan_download(&dest, Some(remote_file(Some(13), false)))?;

        assert_eq!(
            plan,
            Some(DownloadPlan {
                resume_from: 0,
                total_size: Some(13),
                accepts_ranges: false,
            })
        );
        assert!(!dest.exists());
        Ok(())
    }

    #[test]
    fn download_file_retries_after_partial_read_and_resumes() -> Result<()> {
        init_test_tracing();
        let temp_dir = TestDir::new()?;
        let dest = temp_dir.path().join("download.tsv.bz2");
        let payload = b"BZhpayload-by";
        let transport = FakeTransport::with_outcomes(
            [ok_head(Some(payload.len() as u64), true)],
            [
                FakeGetOutcome::Response {
                    status: StatusCode::OK,
                    body: payload.to_vec(),
                    accepts_ranges: true,
                    fail_after: Some(7),
                    retry_after: None,
                },
                ok_get(payload, true),
            ],
        );

        download_file_with_transport(&transport, TEST_URL, &dest, false)?;

        assert_eq!(fs::read(&dest)?, payload);
        assert_eq!(transport.requested_ranges(), vec![None, Some(7)]);
        Ok(())
    }

    #[test]
    fn download_attempt_uses_response_length_for_unknown_resume_total() -> Result<()> {
        init_test_tracing();
        let temp_dir = TestDir::new()?;
        let dest = temp_dir.path().join("download.tsv.bz2");
        fs::write(&dest, b"BZhpaylo")?;
        let transport = FakeTransport::with_outcomes([], [ok_get(b"BZhpayload-by", true)]);

        let downloaded = download_attempt(
            &transport,
            TEST_URL,
            &dest,
            DownloadPlan {
                resume_from: 8,
                total_size: None,
                accepts_ranges: true,
            },
            false,
        )
        .expect("download attempt should resume successfully");

        assert_eq!(downloaded, 13);
        assert_eq!(fs::read(&dest)?, b"BZhpayload-by");
        Ok(())
    }

    #[test]
    fn download_file_removes_partial_file_after_non_resumable_failure() -> Result<()> {
        init_test_tracing();
        let temp_dir = TestDir::new()?;
        let dest = temp_dir.path().join("download.tsv.bz2");
        let mut get_outcomes = vec![FakeGetOutcome::Response {
            status: StatusCode::OK,
            body: b"BZhpayload-by".to_vec(),
            accepts_ranges: false,
            fail_after: Some(7),
            retry_after: None,
        }];
        get_outcomes.extend(std::iter::repeat_n(
            FakeGetOutcome::Error("connection dropped"),
            FETCH_MAX_RETRIES - 1,
        ));
        let transport = FakeTransport::with_outcomes([ok_head(Some(13), false)], get_outcomes);

        let err = download_file_with_transport(&transport, TEST_URL, &dest, false)
            .expect_err("non-resumable failures should bubble up");

        assert!(err.to_string().contains("connection dropped"));
        assert!(!dest.exists());
        Ok(())
    }

    #[test]
    fn flaky_reader_returns_interrupted_error_after_threshold() {
        let mut reader = FlakyReader {
            cursor: Cursor::new(b"payload".to_vec()),
            fail_after: 3,
            bytes_read: 0,
            failed: false,
        };
        let mut buffer = [0_u8; 8];

        let first = reader.read(&mut buffer).expect("first read should work");
        assert_eq!(first, 3);
        let err = reader
            .read(&mut buffer)
            .expect_err("second read should fail");
        assert_eq!(err.kind(), ErrorKind::Other);
        let err = reader
            .read(&mut buffer)
            .expect_err("third read should fail");
        assert_eq!(err.kind(), ErrorKind::Other);
    }

    #[test]
    fn fake_transport_reports_unexpected_requests() {
        let transport = FakeTransport::default();

        let head_err = transport
            .head(TEST_URL)
            .expect_err("missing HEAD outcome should error");
        assert!(head_err.to_string().contains("unexpected HEAD request"));

        let get_err = transport
            .get(TEST_URL, Some(4))
            .err()
            .expect("missing GET outcome should error");
        assert!(get_err.to_string().contains("unexpected GET request"));
    }

    #[test]
    fn fake_transport_keeps_full_body_when_range_is_not_supported() -> Result<()> {
        let transport = FakeTransport::with_outcomes(
            [],
            [FakeGetOutcome::Response {
                status: StatusCode::OK,
                body: b"BZhpayload-by".to_vec(),
                accepts_ranges: false,
                fail_after: None,
                retry_after: None,
            }],
        );

        let mut response = transport.get(TEST_URL, Some(8))?;
        let mut bytes = Vec::new();
        response.body.read_to_end(&mut bytes)?;

        assert_eq!(response.status, StatusCode::OK);
        assert_eq!(response.content_length, Some(13));
        assert_eq!(bytes, b"BZhpayload-by");
        Ok(())
    }

    #[test]
    fn public_fetch_wiki_rejects_invalid_wiki_before_network_work() -> Result<()> {
        init_test_tracing();
        let data_dir = TestDir::new()?;
        let err = fetch_wiki("../enwiki", "2026-02", data_dir.path())
            .expect_err("unsafe wiki identifier should fail");

        assert!(err.to_string().contains("invalid wiki database name"));
        Ok(())
    }

    #[test]
    fn source_window_download_commits_to_deterministic_staging() -> Result<()> {
        init_test_tracing();
        let data_dir = TestDir::new()?;
        let (plan, _) = SnapshotPlan::load_or_resolve(data_dir.path(), "testwiki", "2026-08")?;
        let source = plan.sources[0].clone();
        let payload = b"BZhpayload-by";
        let transport = FakeTransport::with_outcomes(
            [
                ok_head(Some(payload.len() as u64), true),
                ok_head(Some(payload.len() as u64), true),
            ],
            [ok_get(payload, true)],
        );

        let paths = fetch_snapshot_source_window_with_transport(
            &transport,
            "testwiki",
            "2026-08",
            data_dir.path(),
            "run-123",
            std::slice::from_ref(&source),
        )
        .expect("source-window fixture should download");

        assert_eq!(paths.len(), 1);
        assert_eq!(
            paths[0].file_name().and_then(|name| name.to_str()),
            Some(source.filename()?)
        );
        assert_eq!(fs::read(&paths[0])?, payload);
        assert_eq!(transport.requested_ranges(), vec![None]);
        let staging = source_window_staging_dir(data_dir.path(), "testwiki");
        assert_eq!(fs::read_dir(staging)?.count(), 1);
        Ok(())
    }

    #[test]
    fn source_window_adopts_and_resumes_an_interrupted_download() -> Result<()> {
        init_test_tracing();
        let data_dir = TestDir::new()?;
        let (plan, _) = SnapshotPlan::load_or_resolve(data_dir.path(), "testwiki", "2026-08")?;
        let source = plan.sources[0].clone();
        let payload = b"BZhpayload-by";
        let staging = source_window_staging_dir(data_dir.path(), "testwiki");
        fs::create_dir_all(&staging)?;
        let abandoned = source_download_temp_path(&staging, &source.source_id, "old-run");
        fs::write(&abandoned, &payload[..5])?;
        let transport = FakeTransport::with_outcomes(
            [
                ok_head(Some(payload.len() as u64), true),
                ok_head(Some(payload.len() as u64), true),
            ],
            [ok_get(payload, true)],
        );

        let paths = fetch_snapshot_source_window_with_transport(
            &transport,
            "testwiki",
            "2026-08",
            data_dir.path(),
            "new-run",
            std::slice::from_ref(&source),
        )
        .expect("abandoned source should resume");

        assert!(!abandoned.exists());
        assert_eq!(fs::read(&paths[0])?, payload);
        assert_eq!(transport.requested_ranges(), vec![Some(5)]);
        Ok(())
    }

    #[test]
    fn source_window_rejects_unplanned_sources_and_unsafe_run_ids() -> Result<()> {
        let data_dir = TestDir::new()?;
        let (plan, _) = SnapshotPlan::load_or_resolve(data_dir.path(), "testwiki", "2026-08")?;
        let source = plan.sources[0].clone();
        assert!(validate_source_window_run_id("../unsafe").is_err());
        assert!(
            fetch_snapshot_source_window_with_transport(
                &FakeTransport::default(),
                "testwiki",
                "2026-08",
                data_dir.path(),
                "safe-run",
                &[],
            )
            .is_err()
        );
        let mut changed = source;
        changed.expected_size = Some(13);
        assert!(
            fetch_snapshot_source_window_with_transport(
                &FakeTransport::default(),
                "testwiki",
                "2026-08",
                data_dir.path(),
                "safe-run",
                &[changed],
            )
            .is_err()
        );
        Ok(())
    }

    #[test]
    fn source_window_expected_size_is_fail_closed() -> Result<()> {
        let data_dir = TestDir::new()?;
        let (plan, _) = SnapshotPlan::load_or_resolve(data_dir.path(), "testwiki", "2026-08")?;
        let mut source = plan.sources[0].clone();
        source.expected_size = Some(99);
        let payload = b"BZhpayload-by";
        let transport = FakeTransport::with_outcomes(
            [ok_head(Some(payload.len() as u64), false)],
            [ok_get(payload, false)],
        );

        let error = download_snapshot_source_with_transport(
            &transport,
            &source,
            data_dir.path(),
            "testwiki",
            "size-run",
        )
        .expect_err("plan size mismatch must fail");

        assert!(error.to_string().contains("expected 99"));
        assert!(
            !source_window_staging_dir(data_dir.path(), "testwiki")
                .join(source.filename()?)
                .exists()
        );
        Ok(())
    }

    #[test]
    fn source_window_recovery_rejects_ambiguous_inputs_and_adopts_final_files() -> Result<()> {
        let data_dir = TestDir::new()?;
        let (plan, _) = SnapshotPlan::load_or_resolve(data_dir.path(), "testwiki", "2026-08")?;
        let source = &plan.sources[0];
        let staging = source_window_staging_dir(data_dir.path(), "testwiki");
        fs::create_dir_all(&staging)?;
        let final_path = staging.join(source.filename()?);
        fs::write(&final_path, b"BZhcomplete")?;

        let adopted = adopt_source_download(&staging, &source.source_id, "new-run", &final_path)?;
        assert!(adopted.exists());
        assert!(!final_path.exists());
        assert_eq!(
            adopt_source_download(&staging, &source.source_id, "new-run", &final_path)?,
            adopted
        );

        let old = source_download_temp_path(&staging, &source.source_id, "old-run");
        fs::write(&old, b"BZhpartial")?;
        assert!(
            adopt_source_download(&staging, &source.source_id, "new-run", &final_path).is_err()
        );
        Ok(())
    }

    #[test]
    fn source_window_disk_preflight_covers_known_unknown_partial_and_failure_paths() -> Result<()> {
        let data_dir = TestDir::new()?;
        let (plan, _) = SnapshotPlan::load_or_resolve(data_dir.path(), "testwiki", "2026-08")?;
        let mut source = plan.sources[0].clone();
        source.expected_size = Some(13);
        let staging = source_window_staging_dir(data_dir.path(), "testwiki");
        fs::create_dir_all(staging.join("not-a-file"))?;
        let partial = source_download_temp_path(&staging, &source.source_id, "old-run");
        fs::write(&partial, b"BZhpa")?;

        check_source_window_disk_headroom_with_available(
            &FakeTransport::default(),
            "testwiki",
            std::slice::from_ref(&source),
            data_dir.path(),
            |_| Ok(FETCH_DISK_HEADROOM_MARGIN_BYTES + 13),
        )
        .expect("known-size source should pass disk preflight");
        assert_eq!(source_window_local_bytes(&staging, &source)?, 5);

        fs::write(staging.join(source.filename()?), b"BZhpayload-by")?;
        check_source_window_disk_headroom_with_available(
            &FakeTransport::default(),
            "testwiki",
            std::slice::from_ref(&source),
            data_dir.path(),
            source_window_available_space,
        )
        .expect("fully staged source should need no additional bytes");

        fs::remove_file(staging.join(source.filename()?))?;
        let error = check_source_window_disk_headroom_with_available(
            &FakeTransport::default(),
            "testwiki",
            std::slice::from_ref(&source),
            data_dir.path(),
            |_| Err(std::io::Error::other("space unavailable")),
        );
        assert!(error.is_ok());

        let mut unknown = source.clone();
        unknown.expected_size = None;
        check_source_window_disk_headroom_with_available(
            &FakeTransport::with_outcomes([ok_head(None, false)], []),
            "testwiki",
            &[unknown],
            data_dir.path(),
            source_window_available_space,
        )
        .expect("unknown source size should remain a best-effort lower bound");
        Ok(())
    }

    #[test]
    fn snapshot_source_size_inventory_combines_pinned_and_probed_sizes() -> Result<()> {
        let mut sources = SnapshotPlan::resolve("enwiki", "2001-02")?.sources;
        sources.truncate(2);
        sources[0].expected_size = Some(11);
        let transport = FakeTransport::with_head_outcomes([ok_head(Some(22), true)]);
        assert_eq!(
            snapshot_source_sizes_with_transport(&transport, &sources)?,
            vec![Some(11), Some(22)]
        );

        sources.truncate(1);
        let data_dir = TestDir::new()?;
        assert_eq!(
            snapshot_source_sizes(data_dir.path(), "enwiki", "2001-02", &sources)?,
            vec![Some(11)]
        );
        Ok(())
    }

    #[test]
    fn source_window_public_wrapper_and_cleanup_fail_closed_without_network() -> Result<()> {
        let data_dir = TestDir::new()?;
        assert!(
            fetch_snapshot_source_window("testwiki", "2026-08", data_dir.path(), "valid-run", &[],)
                .is_err()
        );
        let staging = source_window_staging_dir(data_dir.path(), "testwiki");
        fs::create_dir_all(&staging)?;
        assert_eq!(
            cleanup_committed_source_window_inputs("testwiki", "2026-08", data_dir.path())?,
            0
        );
        assert!(!staging.exists());
        Ok(())
    }
}
