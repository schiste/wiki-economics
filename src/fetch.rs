use anyhow::{Context, Result};
use indicatif::{ProgressBar, ProgressStyle};
use rayon::prelude::*;
use reqwest::StatusCode;
use reqwest::header::{ACCEPT_RANGES, CONTENT_LENGTH, HeaderMap, RANGE};
use std::ffi::OsStr;
use std::fs;
use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;
use tracing::{debug, info, warn};

const BASE_URL: &str = "https://dumps.wikimedia.org/other/mediawiki_history";
const USER_AGENT: &str = "wiki-econ/0.1 (Wikipedia economic analysis research tool)";
const FETCH_MAX_PARALLELISM: usize = 4;
const FETCH_MAX_RETRIES: usize = 3;
const FETCH_RETRY_BACKOFF_MS: u64 = 500;
const FETCH_MAX_PARALLELISM_ENV: &str = "WIKI_ECON_FETCH_MAX_PARALLELISM";

/// Wikis partitioned yearly in the dumps (medium-sized wikis).
const YEARLY_WIKIS: &[&str] = &[
    "arwiki", "cawiki", "cswiki", "dewiki", "eswiki", "fawiki", "fiwiki", "frwiki", "hewiki",
    "huwiki", "idwiki", "itwiki", "jawiki", "kowiki", "nlwiki", "nowiki", "plwiki", "ptwiki",
    "rowiki", "ruwiki", "svwiki", "thwiki", "trwiki", "ukwiki", "viwiki", "zhwiki",
];

/// Wikis partitioned monthly (very large).
const MONTHLY_WIKIS: &[&str] = &["enwiki", "wikidatawiki", "commonswiki"];

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
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct TransportHead {
    status: StatusCode,
    content_length: Option<u64>,
    accepts_ranges: bool,
}

struct TransportResponse {
    status: StatusCode,
    content_length: Option<u64>,
    body: Box<dyn Read + Send>,
}

trait HttpTransport: Sync {
    fn head(&self, url: &str) -> Result<TransportHead>;
    fn get(&self, url: &str, range_start: Option<u64>) -> Result<TransportResponse>;
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
        }
    }

    fn retryable(error: anyhow::Error) -> Self {
        Self {
            error,
            retryable: true,
        }
    }
}

/// Determine the file list for a given wiki and snapshot version.
fn build_file_list(wiki: &str, version: &str) -> Result<Vec<String>> {
    if MONTHLY_WIKIS.contains(&wiki) {
        anyhow::bail!(
            "Monthly-partitioned wikis (enwiki, etc.) are not yet supported. Use yearly wikis."
        );
    }

    if YEARLY_WIKIS.contains(&wiki) {
        let end_year: u32 = version
            .get(..4)
            .context("Invalid version format")?
            .parse()
            .context("Invalid version format")?;
        Ok((2001..=end_year)
            .map(|year| format!("{version}.{wiki}.{year}.tsv.bz2"))
            .collect())
    } else {
        Ok(vec![format!("{version}.{wiki}.all-time.tsv.bz2")])
    }
}

fn build_transport() -> Result<ReqwestTransport> {
    let client = reqwest::blocking::Client::builder()
        .user_agent(USER_AGENT)
        .timeout(Duration::from_secs(3600))
        .build()
        .map_err(anyhow::Error::from)?;

    Ok(ReqwestTransport { client })
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
        build_transport_response(
            response.status(),
            response.content_length(),
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
    }
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
    body: Box<dyn Read + Send>,
) -> TransportResponse {
    TransportResponse {
        status,
        content_length,
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

fn sleep_before_retry(attempt: usize) {
    let multiplier = 1_u64 << attempt.saturating_sub(1);
    std::thread::sleep(Duration::from_millis(FETCH_RETRY_BACKOFF_MS * multiplier));
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
            sleep_before_retry(attempt);
        }
    }

    last_error
        .into_iter()
        .for_each(|error| warn!(url = url, error = %error, "metadata probe failed after retries"));
    Ok(None)
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
            Err(AttemptError::retryable(error))
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
                sleep_before_retry(attempt);
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

fn fetch_wiki_from_base_with_transport<T: HttpTransport>(
    transport: &T,
    base_url: &str,
    wiki: &str,
    version: &str,
    data_dir: &Path,
) -> Result<Vec<PathBuf>> {
    let raw_dir = data_dir.join("raw").join(wiki);
    fs::create_dir_all(&raw_dir)?;

    let files = build_file_list(wiki, version)?;
    let parallelism = fetch_parallelism(files.len());

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

/// Fetch all dump files for a wiki.
pub fn fetch_wiki(wiki: &str, version: &str, data_dir: &Path) -> Result<Vec<PathBuf>> {
    let transport = build_transport()?;
    fetch_wiki_from_base_with_transport(&transport, BASE_URL, wiki, version, data_dir)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{TestDir, init_test_tracing};
    use std::collections::VecDeque;
    use std::io::{BufRead, BufReader, Cursor, ErrorKind};
    use std::net::TcpListener;
    use std::sync::{Arc, Mutex};
    use std::thread;

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
        },
        Error(&'static str),
    }

    #[derive(Default)]
    struct FakeTransportState {
        head_outcomes: VecDeque<FakeHeadOutcome>,
        get_outcomes: VecDeque<FakeGetOutcome>,
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
            match self
                .state
                .lock()
                .expect("fake transport state")
                .head_outcomes
                .pop_front()
            {
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
        })
    }

    fn status_head(status: StatusCode) -> FakeHeadOutcome {
        FakeHeadOutcome::Response(TransportHead {
            status,
            content_length: None,
            accepts_ranges: false,
        })
    }

    fn ok_get(body: &[u8], accepts_ranges: bool) -> FakeGetOutcome {
        FakeGetOutcome::Response {
            status: StatusCode::OK,
            body: body.to_vec(),
            accepts_ranges,
            fail_after: None,
        }
    }

    fn status_get(status: StatusCode) -> FakeGetOutcome {
        FakeGetOutcome::Response {
            status,
            body: Vec::new(),
            accepts_ranges: false,
            fail_after: None,
        }
    }

    fn remote_file(content_length: Option<u64>, accepts_ranges: bool) -> RemoteFileInfo {
        RemoteFileInfo {
            content_length,
            accepts_ranges,
        }
    }

    fn fetch_paths_with_transport(
        transport: &FakeTransport,
        wiki: &str,
        version: &str,
        data_dir: &Path,
    ) -> Result<Vec<PathBuf>> {
        fetch_wiki_from_base_with_transport(
            transport,
            "http://example.invalid",
            wiki,
            version,
            data_dir,
        )
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
    fn build_file_list_rejects_monthly_wikis() {
        init_test_tracing();
        let err = build_file_list("enwiki", "2026-02").expect_err("monthly wikis should error");
        assert!(err.to_string().contains("not yet supported"));
    }

    #[test]
    fn download_file_writes_response_body() -> Result<()> {
        init_test_tracing();
        let temp_dir = TestDir::new()?;
        let dest = temp_dir.path().join("download.tsv.bz2");
        let transport = FakeTransport::with_outcomes(
            [ok_head(Some(13), false)],
            [ok_get(b"payload-bytes", false)],
        );

        download_file_with_transport(&transport, TEST_URL, &dest, false)?;

        assert_eq!(fs::read(&dest)?, b"payload-bytes");
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
    fn download_file_uses_validated_existing_file() -> Result<()> {
        init_test_tracing();
        let temp_dir = TestDir::new()?;
        let dest = temp_dir.path().join("download.tsv.bz2");
        fs::write(&dest, b"payload-bytes")?;
        let transport = FakeTransport::with_outcomes([ok_head(Some(13), true)], []);

        download_file_with_transport(&transport, TEST_URL, &dest, false)?;

        assert_eq!(fs::read(&dest)?, b"payload-bytes");
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
            [ok_get(b"fresh-bytes", false)],
        );

        download_file_with_transport(&transport, TEST_URL, &dest, false)?;

        assert_eq!(fs::read(&dest)?, b"fresh-bytes");
        Ok(())
    }

    #[test]
    fn download_file_resumes_partial_destination() -> Result<()> {
        init_test_tracing();
        let temp_dir = TestDir::new()?;
        let dest = temp_dir.path().join("download.tsv.bz2");
        fs::write(&dest, b"payload-")?;
        let transport = FakeTransport::with_outcomes(
            [ok_head(Some(13), true)],
            [ok_get(b"payload-bytes", true)],
        );

        download_file_with_transport(&transport, TEST_URL, &dest, false)?;

        assert_eq!(fs::read(&dest)?, b"payload-bytes");
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
                ok_get(b"payload-bytes", false),
            ],
        );

        download_file_with_transport(&transport, TEST_URL, &dest, false)?;

        assert_eq!(fs::read(&dest)?, b"payload-bytes");
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
            [ok_get(b"payload-bytes", false)],
        );

        download_file_with_transport(&transport, TEST_URL, &dest, false)?;

        assert_eq!(fs::read(&dest)?, b"payload-bytes");
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

        let head = parse_transport_head(StatusCode::OK, &headers, None);
        assert_eq!(head.status, StatusCode::OK);
        assert_eq!(head.content_length, Some(13));
        assert!(head.accepts_ranges);
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
        let transport = FakeTransport::with_outcomes([ok_head(Some(12), true)], []);
        let paths = fetch_paths_with_transport(&transport, wiki, "2026-02", data_dir.path())?;

        assert_eq!(paths, vec![existing]);
        assert_eq!(transport.get_requests(), 0);
        Ok(())
    }

    #[test]
    fn fetch_parallelism_defaults_when_env_is_unset() {
        init_test_tracing();

        assert_eq!(fetch_parallelism_override(0, None), 1);
        assert_eq!(fetch_parallelism_override(2, None), 2);
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
                ok_get(b"payload-bytes", false),
                ok_get(b"payload-bytes", false),
            ],
        );
        let paths = fetch_paths_with_transport(&transport, wiki, "2002-01", data_dir.path())?;

        assert_eq!(paths.len(), 2);
        assert!(paths.iter().all(|path| path.exists()));
        assert_eq!(transport.get_requests(), 2);
        Ok(())
    }

    #[test]
    fn download_file_cleans_up_when_destination_cannot_be_created() -> Result<()> {
        init_test_tracing();
        let temp_dir = TestDir::new()?;
        let dest = temp_dir.path().join("missing").join("download.tsv.bz2");
        let transport = FakeTransport::with_outcomes(
            [ok_head(Some(11), false)],
            [ok_get(b"fresh-bytes", false)],
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
        let transport = FakeTransport::with_head_outcomes([
            status_head(StatusCode::SERVICE_UNAVAILABLE),
            status_head(StatusCode::SERVICE_UNAVAILABLE),
            status_head(StatusCode::SERVICE_UNAVAILABLE),
        ]);

        assert_eq!(probe_remote_file(&transport, TEST_URL)?, None);
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
        let payload = b"payload-bytes";
        let transport = FakeTransport::with_outcomes(
            [ok_head(Some(payload.len() as u64), true)],
            [
                FakeGetOutcome::Response {
                    status: StatusCode::OK,
                    body: payload.to_vec(),
                    accepts_ranges: true,
                    fail_after: Some(7),
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
        fs::write(&dest, b"payload-")?;
        let transport = FakeTransport::with_outcomes([], [ok_get(b"payload-bytes", true)]);

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
        assert_eq!(fs::read(&dest)?, b"payload-bytes");
        Ok(())
    }

    #[test]
    fn download_file_removes_partial_file_after_non_resumable_failure() -> Result<()> {
        init_test_tracing();
        let temp_dir = TestDir::new()?;
        let dest = temp_dir.path().join("download.tsv.bz2");
        let transport = FakeTransport::with_outcomes(
            [ok_head(Some(13), false)],
            [
                FakeGetOutcome::Response {
                    status: StatusCode::OK,
                    body: b"payload-bytes".to_vec(),
                    accepts_ranges: false,
                    fail_after: Some(7),
                },
                FakeGetOutcome::Error("connection dropped"),
                FakeGetOutcome::Error("connection dropped"),
            ],
        );

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
                body: b"payload-bytes".to_vec(),
                accepts_ranges: false,
                fail_after: None,
            }],
        );

        let mut response = transport.get(TEST_URL, Some(8))?;
        let mut bytes = Vec::new();
        response.body.read_to_end(&mut bytes)?;

        assert_eq!(response.status, StatusCode::OK);
        assert_eq!(response.content_length, Some(13));
        assert_eq!(bytes, b"payload-bytes");
        Ok(())
    }

    #[test]
    fn public_fetch_wiki_rejects_monthly_wikis_before_network_work() -> Result<()> {
        init_test_tracing();
        let data_dir = TestDir::new()?;
        let err = fetch_wiki("enwiki", "2026-02", data_dir.path())
            .expect_err("monthly wikis should fail");

        assert!(err.to_string().contains("not yet supported"));
        Ok(())
    }
}
