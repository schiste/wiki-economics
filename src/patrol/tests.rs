use super::*;
use crate::storage;
use crate::test_support::{TestDir, init_test_tracing};
use bzip2::Compression as BzCompression;
use bzip2::write::BzEncoder;
use flate2::Compression;
use flate2::write::GzEncoder;
use serde_json::json;
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::fs;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::{Arc, Mutex};

type TestRevisionRow<'a> = (
    Option<i64>,
    Option<&'a str>,
    Option<&'a str>,
    Option<i32>,
    Option<&'a str>,
    bool,
    bool,
);
type TestRightsRow<'a> = (
    Option<&'a str>,
    Option<&'a str>,
    Option<&'a str>,
    Option<&'a str>,
);

struct FakePatrolTransport {
    get_bodies: Mutex<VecDeque<Vec<u8>>>,
    json_values: Mutex<VecDeque<Value>>,
    get_calls: Mutex<Vec<(String, Option<u64>)>>,
    json_calls: Mutex<Vec<String>>,
    response_etag: Option<String>,
    response_last_modified: Option<String>,
    response_content_length: Option<u64>,
    dump_status_values: Mutex<VecDeque<Value>>,
}

impl FakePatrolTransport {
    fn new(get_bodies: Vec<Vec<u8>>, json_values: Vec<Value>) -> Self {
        Self {
            get_bodies: Mutex::new(get_bodies.into()),
            json_values: Mutex::new(json_values.into()),
            get_calls: Mutex::new(Vec::new()),
            json_calls: Mutex::new(Vec::new()),
            response_etag: None,
            response_last_modified: None,
            response_content_length: None,
            dump_status_values: Mutex::new(VecDeque::new()),
        }
    }

    fn with_response_identity(mut self, etag: &str, last_modified: &str) -> Self {
        self.response_etag = Some(etag.to_string());
        self.response_last_modified = Some(last_modified.to_string());
        self
    }

    fn with_response_content_length(mut self, content_length: u64) -> Self {
        self.response_content_length = Some(content_length);
        self
    }

    fn with_dump_statuses(mut self, values: Vec<Value>) -> Self {
        self.dump_status_values = Mutex::new(values.into());
        self
    }

    fn get_calls(&self) -> Vec<(String, Option<u64>)> {
        self.get_calls
            .lock()
            .expect("transport get calls lock should not be poisoned")
            .clone()
    }

    fn json_calls(&self) -> Vec<String> {
        self.json_calls
            .lock()
            .expect("transport json calls lock should not be poisoned")
            .clone()
    }
}

impl PatrolTransport for FakePatrolTransport {
    fn get(&self, url: &str, range_start: Option<u64>) -> Result<PatrolTransportResponse> {
        self.get_calls
            .lock()
            .expect("transport get calls lock should not be poisoned")
            .push((url.to_string(), range_start));
        let bytes = self
            .get_bodies
            .lock()
            .expect("transport bodies lock should not be poisoned")
            .pop_front()
            .expect("test transport should have a queued body");
        let content_length = self.response_content_length.or_else(|| {
            self.response_etag
                .as_ref()
                .and_then(|_| u64::try_from(bytes.len()).ok())
        });
        Ok(PatrolTransportResponse::from_bytes_with_identity(
            bytes,
            content_length,
            self.response_etag.as_deref(),
            self.response_last_modified.as_deref(),
        ))
    }

    fn get_json(&self, url: &str) -> Result<Value> {
        if url.ends_with("/dumpstatus.json") {
            if let Some(value) = self
                .dump_status_values
                .lock()
                .expect("dump status lock should not be poisoned")
                .pop_front()
            {
                return Ok(value);
            }
            let parts = url
                .trim_end_matches("/dumpstatus.json")
                .rsplit('/')
                .collect::<Vec<_>>();
            let dump_date = parts.first().context("test dump date")?;
            let wiki = parts.get(1).context("test dump wiki")?;
            let body = self
                .get_bodies
                .lock()
                .expect("transport bodies lock should not be poisoned")
                .front()
                .cloned()
                .context("test patrol source body")?;
            let name = format!("{wiki}-{dump_date}-pages-logging.xml.gz");
            return Ok(completed_dump_status(&name, &body));
        }
        self.json_calls
            .lock()
            .expect("transport json calls lock should not be poisoned")
            .push(url.to_string());
        self.json_values
            .lock()
            .expect("transport json values lock should not be poisoned")
            .pop_front()
            .ok_or_else(|| anyhow::anyhow!("test transport should have a queued JSON response"))
    }
}

fn source_sha1(bytes: &[u8]) -> String {
    hex::encode(<sha1::Sha1 as sha1::Digest>::digest(bytes))
}

fn completed_dump_status(name: &str, bytes: &[u8]) -> Value {
    let wiki = name.split('-').next().expect("test wiki");
    let date = name.split('-').nth(1).expect("test date");
    json!({
        "jobs": {
            "xmlpagelogsdumprecombine": {
                "status": "done",
                "updated": "2026-09-04 00:00:00",
                "files": {
                    (name): {
                        "size": bytes.len(),
                        "url": format!("/{wiki}/{date}/{name}"),
                        "md5": "0".repeat(32),
                        "sha1": source_sha1(bytes),
                    }
                }
            },
            "xmlpagelogsdump": {"status": "waiting", "updated": "", "files": {}}
        }
    })
}

fn test_source_spec(wiki: &str, snapshot: &str, bytes: &[u8]) -> Result<plan::PatrolSourceSpec> {
    let year: u32 = snapshot[..4].parse()?;
    let month: u32 = snapshot[5..].parse()?;
    let (year, month) = if month == 12 {
        (year + 1, 1)
    } else {
        (year, month + 1)
    };
    let date = format!("{year:04}{month:02}01");
    let source_id = format!("{wiki}-{date}-pages-logging.xml.gz");
    Ok(plan::PatrolSourceSpec {
        url: url::Url::parse(&format!(
            "https://dumps.wikimedia.org/{wiki}/{date}/{source_id}"
        ))?,
        source_id,
        expected_size: u64::try_from(bytes.len())?,
        md5: "0".repeat(32),
        sha1: source_sha1(bytes),
    })
}

fn gzip_bytes(content: &str) -> Result<Vec<u8>> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(content.as_bytes())?;
    encoder.finish().map_err(Into::into)
}

fn history_row(wiki: &str, timestamp: &str, username: &str, revision_id: &str) -> String {
    history_row_with_user_id(wiki, timestamp, username, "1", revision_id)
}

fn history_row_with_user_id(
    wiki: &str,
    timestamp: &str,
    username: &str,
    user_id: &str,
    revision_id: &str,
) -> String {
    let mut row = vec![String::new(); crate::schema::COLUMNS.len()];
    for (name, value) in [
        ("wiki_db", wiki),
        ("event_entity", "revision"),
        ("event_type", "create"),
        ("event_timestamp", timestamp),
        ("event_user_id", user_id),
        ("event_user_text", username),
        ("event_user_is_anonymous", "false"),
        ("event_user_is_temporary", "false"),
        ("event_user_registration_timestamp", "2023-01-01 00:00:00.0"),
        ("event_user_first_edit_timestamp", timestamp),
        ("page_id", "10"),
        ("page_title", "ExamplePage"),
        ("page_namespace", "0"),
        ("page_namespace_is_content", "true"),
        ("page_is_redirect", "false"),
        ("revision_id", revision_id),
        ("revision_parent_id", "0"),
        ("revision_minor_edit", "false"),
        ("revision_text_bytes", "1200"),
        ("revision_text_bytes_diff", "100"),
        ("revision_is_identity_reverted", "false"),
        ("revision_is_identity_revert", "false"),
    ] {
        let index = crate::schema::COLUMNS
            .iter()
            .position(|column| column == &name)
            .expect("history fixture column should exist");
        row[index] = value.to_string();
    }
    row.join("\t")
}

fn ingest_history_snapshot(
    data_dir: &Path,
    wiki: &str,
    snapshot: &str,
    rows: &[String],
) -> Result<()> {
    let raw_dir = data_dir.join("raw").join(wiki);
    fs::create_dir_all(&raw_dir)?;
    let path = raw_dir.join(format!("{snapshot}.{wiki}.all-time.tsv.bz2"));
    let file = File::create(path)?;
    let mut encoder = BzEncoder::new(file, BzCompression::best());
    for row in rows {
        encoder.write_all(row.as_bytes())?;
        encoder.write_all(b"\n")?;
    }
    encoder.finish()?;
    crate::ingest::ingest_wiki_snapshot(wiki, snapshot, data_dir)?;
    Ok(())
}

fn write_gz(path: &Path, content: &str) -> Result<()> {
    fs::write(path, gzip_bytes(content)?)?;
    Ok(())
}

fn write_multi_member_gz(path: &Path, members: &[&str]) -> Result<()> {
    let mut bytes = Vec::new();
    for member in members {
        bytes.extend(gzip_bytes(member)?);
    }
    fs::write(path, bytes)?;
    Ok(())
}

fn write_revision_partition(
    root: &Path,
    wiki: &str,
    year_month: &str,
    rows: &[TestRevisionRow<'_>],
) -> Result<PathBuf> {
    let year = year_month
        .get(..4)
        .expect("year-month should include a year")
        .parse::<i32>()
        .expect("year should parse");
    let warehouse_dir = storage::warehouse_wiki_dir(root, wiki);
    let dir = storage::month_partition_dir(&warehouse_dir, year, year_month);
    fs::create_dir_all(&dir)?;
    let mut df = DataFrame::new_infer_height(vec![
        Column::new(
            "revision_id".into(),
            rows.iter()
                .map(|(revision_id, ..)| *revision_id)
                .collect::<Vec<_>>(),
        ),
        Column::new(
            "event_timestamp".into(),
            rows.iter()
                .map(|(_, timestamp, ..)| *timestamp)
                .collect::<Vec<_>>(),
        ),
        Column::new(
            "event_user_id".into(),
            rows.iter()
                .map(|(_, _, user_text, ..)| user_text.map(|_| 1_i64))
                .collect::<Vec<_>>(),
        ),
        Column::new(
            "event_user_text".into(),
            rows.iter()
                .map(|(_, _, user_text, ..)| *user_text)
                .collect::<Vec<_>>(),
        ),
        Column::new(
            "page_namespace".into(),
            rows.iter()
                .map(|(_, _, _, page_namespace, ..)| *page_namespace)
                .collect::<Vec<_>>(),
        ),
        Column::new(
            "event_user_is_bot_by".into(),
            rows.iter()
                .map(|(_, _, _, _, bot_by, ..)| *bot_by)
                .collect::<Vec<_>>(),
        ),
        Column::new(
            "event_user_is_anonymous".into(),
            rows.iter()
                .map(|(_, _, _, _, _, anonymous, _)| *anonymous)
                .collect::<Vec<_>>(),
        ),
        Column::new(
            "event_user_is_temporary".into(),
            rows.iter()
                .map(|(_, _, _, _, _, _, temporary)| *temporary)
                .collect::<Vec<_>>(),
        ),
    ])?;
    let path = dir.join("part-00000.parquet");
    let mut file = File::create(&path)?;
    ParquetWriter::new(&mut file)
        .with_compression(ParquetCompression::Zstd(None))
        .finish(&mut df)?;
    Ok(path)
}

fn write_patrol_events(path: &Path, rows: &[(Option<&str>, i64, i64, Option<&str>)]) -> Result<()> {
    let mut df = DataFrame::new_infer_height(vec![
        Column::new(
            "timestamp".into(),
            rows.iter()
                .map(|(timestamp, ..)| *timestamp)
                .collect::<Vec<_>>(),
        ),
        Column::new(
            "current_revision_id".into(),
            rows.iter()
                .map(|(_, current_revision_id, ..)| *current_revision_id)
                .collect::<Vec<_>>(),
        ),
        Column::new(
            "prev_revision_id".into(),
            rows.iter()
                .map(|(_, _, prev_revision_id, _)| *prev_revision_id)
                .collect::<Vec<_>>(),
        ),
        Column::new(
            "user".into(),
            rows.iter().map(|(_, _, _, user)| *user).collect::<Vec<_>>(),
        ),
    ])?;
    let mut file = File::create(path)?;
    ParquetWriter::new(&mut file).finish(&mut df)?;
    Ok(())
}

fn write_rights_events(path: &Path, rows: &[TestRightsRow<'_>]) -> Result<()> {
    let mut df = DataFrame::new_infer_height(vec![
        Column::new(
            "timestamp".into(),
            rows.iter()
                .map(|(timestamp, ..)| *timestamp)
                .collect::<Vec<_>>(),
        ),
        Column::new(
            "target_user".into(),
            rows.iter()
                .map(|(_, target_user, ..)| *target_user)
                .collect::<Vec<_>>(),
        ),
        Column::new(
            "old_groups".into(),
            rows.iter()
                .map(|(_, _, old_groups, _)| *old_groups)
                .collect::<Vec<_>>(),
        ),
        Column::new(
            "new_groups".into(),
            rows.iter()
                .map(|(_, _, _, new_groups)| *new_groups)
                .collect::<Vec<_>>(),
        ),
    ])?;
    let mut file = File::create(path)?;
    ParquetWriter::new(&mut file).finish(&mut df)?;
    Ok(())
}

fn read_json(path: &Path) -> Result<Value> {
    serde_json::from_slice(&fs::read(path)?).map_err(Into::into)
}

fn install_fake_transport(
    get_bodies: Vec<Vec<u8>>,
    json_values: Vec<Value>,
) -> (Arc<FakePatrolTransport>, TestTransportGuard) {
    let transport = Arc::new(FakePatrolTransport::new(get_bodies, json_values));
    let guard = install_test_transport(transport.clone());
    (transport, guard)
}

fn serve_once(response: String) -> Result<(String, std::thread::JoinHandle<Vec<u8>>)> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let addr = listener.local_addr()?;
    let handle = std::thread::spawn(move || {
        let (mut stream, _) = listener
            .accept()
            .expect("test server should accept one connection");
        let mut request = vec![0_u8; 2048];
        let size = stream
            .read(&mut request)
            .expect("test server should read a request");
        stream
            .write_all(response.as_bytes())
            .expect("test server should write a response");
        request.truncate(size);
        request
    });
    Ok((format!("http://{addr}"), handle))
}

#[test]
fn params_and_helper_functions_cover_edge_cases() {
    let mut headers = HeaderMap::new();
    headers.insert(ETAG, reqwest::header::HeaderValue::from_static("etag-v1"));
    headers.insert(
        CONTENT_RANGE,
        reqwest::header::HeaderValue::from_static("bytes 10-19/100"),
    );
    assert_eq!(header_string(&headers, ETAG), Some("etag-v1".to_string()));
    assert_eq!(response_total_length(&headers, Some(10)), Some(100));
    headers.insert(
        CONTENT_RANGE,
        reqwest::header::HeaderValue::from_static("bytes */100"),
    );
    assert_eq!(unsatisfied_range_total(&headers), Some(100));
    headers.remove(CONTENT_RANGE);
    headers.insert(
        CONTENT_LENGTH,
        reqwest::header::HeaderValue::from_static("90"),
    );
    assert_eq!(response_total_length(&headers, Some(10)), Some(100));

    assert_eq!(UserType::Registered.as_str(), "registered");
    assert_eq!(UserType::Anonymous.as_str(), "anonymous");
    assert_eq!(UserType::Temporary.as_str(), "temporary");
    assert_eq!(UserType::Bot.as_str(), "bot");

    assert_eq!(parse_patrol_params(""), (0, 0, false));
    assert_eq!(
        parse_patrol_params("6556036\n6556016\n0"),
        (6_556_036, 6_556_016, false)
    );
    assert_eq!(
        parse_patrol_params(
            r#"a:4:{s:8:"4::curid";s:8:"29704253";s:9:"5::previd";s:1:"0";s:7:"6::auto";i:1;s:7:"ignored";s:1:"x";}"#
        ),
        (29_704_253, 0, true)
    );

    assert_eq!(parse_rights_params(""), (String::new(), String::new()));
    assert_eq!(
        parse_rights_params("autopatrolled\nsysop"),
        ("autopatrolled".to_string(), "sysop".to_string())
    );
    assert_eq!(
        parse_rights_params(
            r#"a:2:{s:12:"4::oldgroups";a:4:{i:0;s:13:"autopatrolled";i:1;s:5:"sysop";i:2;s:14:"20260101000000";i:3;s:13:"autopatrolled";}s:12:"5::newgroups";a:1:{i:0;s:5:"sysop";}}"#
        ),
        ("autopatrolled,sysop".to_string(), "sysop".to_string())
    );
    assert_eq!(
        extract_php_array_body(r#"a:1:{i:0;s:5:"sysop";}"#),
        Some(r#"i:0;s:5:"sysop";"#)
    );
    assert_eq!(extract_php_array_body("no brace"), None);
    assert_eq!(extract_php_array_body("{unterminated"), None);
    assert!(extract_php_groups(r#"a:1:{s:5:"noop";i:1;}"#, "4::oldgroups").is_empty());
    assert!(extract_php_groups(r#""4::oldgroups";a:1:s:5:"sysop";"#, "4::oldgroups").is_empty());

    assert_eq!(
        split_groups(Some(" autopatrolled, , sysop ")).collect::<Vec<_>>(),
        vec!["autopatrolled", "sysop"]
    );
    assert_eq!(parse_year_month_key("2026-02"), Some(202602));
    assert_eq!(parse_year_month_key("bad"), None);
    assert_eq!(shift_month_key(202601, 1), Some(202602));
    assert_eq!(shift_month_key(202601, -1), Some(202512));
    assert_eq!(shift_month_key(202600, 1), None);
    assert_eq!(shift_month_key(1, -2), None);
    assert_eq!(
        wiki_to_api_domain("frwiki"),
        Some("fr.wikipedia.org".to_string())
    );
    assert_eq!(wiki_to_api_domain("wiki"), None);
    assert_eq!(
        classify_user_type(Some("group"), false, false),
        UserType::Bot
    );
    assert_eq!(classify_user_type(None, true, false), UserType::Anonymous);
    assert_eq!(classify_user_type(None, false, true), UserType::Temporary);
    assert_eq!(
        classify_user_type(Some("false"), false, false),
        UserType::Registered
    );

    let revision_meta = RevisionMeta {
        timestamp_seconds: parse_timestamp_seconds("2026-01-01 00:00:00").expect("timestamp"),
        year_month_key: 202601,
        page_namespace: 0,
        user_type: UserType::Registered,
    };
    let mut accumulator = PatrolAccumulator::default();
    record_patrol_latency(&mut accumulator, None, "2026-01-01 01:00:00");
    record_patrol_latency(&mut accumulator, Some(&revision_meta), "bad");
    record_patrol_latency(
        &mut accumulator,
        Some(&revision_meta),
        "2026-01-01 00:00:00",
    );
    record_patrol_latency(
        &mut accumulator,
        Some(&revision_meta),
        "2026-01-01 02:00:00",
    );
    assert_eq!(accumulator.latencies_hours, vec![2.0]);

    assert_eq!(min_patrollers_for_half_share(&[2, 1], 3), 1);
    assert_eq!(min_patrollers_for_half_share(&[1, 1], 10), 2);
}

#[test]
fn fetch_autopatrol_groups_handles_short_circuit_and_parses_rights() -> Result<()> {
    let transport = FakePatrolTransport::new(
        Vec::new(),
        vec![json!({
            "query": {
                "usergroups": [
                    { "name": "patroller", "rights": ["autopatrol", "edit"] },
                    { "name": "sysop", "rights": ["edit"] }
                ]
            }
        })],
    );

    assert_eq!(
        fetch_autopatrol_groups(&transport, "frwiki")?,
        vec!["patroller".to_string()]
    );
    assert!(fetch_autopatrol_groups(&transport, "wiki")?.is_empty());
    assert_eq!(
        transport.json_calls(),
        vec![
            "https://fr.wikipedia.org/w/api.php?action=query&meta=siteinfo&siprop=usergroups&format=json"
                .to_string()
        ]
    );

    Ok(())
}

#[test]
fn reqwest_patrol_transport_propagates_connection_errors() -> Result<()> {
    let transport = build_transport()?;
    let (dump_url, dump_handle) = serve_once(
        "HTTP/1.1 200 OK\r\nContent-Length: 3\r\nContent-Type: application/octet-stream\r\n\r\nabc"
            .to_string(),
    )?;
    let mut response = transport.get(&format!("{dump_url}/dump.xml.gz"), Some(5))?;
    let mut body = String::new();
    response.body.read_to_string(&mut body)?;
    assert_eq!(body, "abc");
    let request = String::from_utf8(
        dump_handle
            .join()
            .expect("dump server thread should finish"),
    )
    .expect("dump request should be UTF-8");
    let lower_request = request.to_ascii_lowercase();
    assert!(lower_request.contains("get /dump.xml.gz http/1.1"));
    assert!(lower_request.contains("range: bytes=5-"));

    let (json_url, json_handle) = serve_once(
        "HTTP/1.1 200 OK\r\nContent-Length: 11\r\nContent-Type: application/json\r\n\r\n{\"ok\":true}"
            .to_string(),
    )?;
    assert_eq!(
        transport.get_json(&format!("{json_url}/siteinfo"))?,
        json!({ "ok": true })
    );
    let json_request = String::from_utf8(
        json_handle
            .join()
            .expect("JSON server thread should finish"),
    )
    .expect("JSON request should be UTF-8");
    assert!(json_request.contains("GET /siteinfo HTTP/1.1"));

    let dump_err = transport
        .get("http://127.0.0.1:9/dump.xml.gz", Some(5))
        .err()
        .expect("unreachable local dump endpoint should fail");
    assert!(!dump_err.to_string().is_empty());
    let json_err = transport
        .get_json("http://127.0.0.1:9/siteinfo")
        .expect_err("unreachable local JSON endpoint should fail");
    assert!(!json_err.to_string().is_empty());
    Ok(())
}

#[test]
fn reqwest_patrol_transport_accepts_only_an_exactly_complete_range() -> Result<()> {
    let transport = build_transport()?;
    let response = "HTTP/1.1 416 Range Not Satisfiable\r\nContent-Range: bytes */3\r\nContent-Length: 0\r\n\r\n";

    let (complete_url, complete_handle) = serve_once(response.to_string())?;
    let mut complete = transport.get(&format!("{complete_url}/dump.xml.gz"), Some(3))?;
    let mut body = Vec::new();
    complete.body.read_to_end(&mut body)?;
    assert!(body.is_empty());
    complete_handle
        .join()
        .expect("complete-range server thread should finish");

    let (mismatch_url, mismatch_handle) = serve_once(response.to_string())?;
    let mismatch = match transport.get(&format!("{mismatch_url}/dump.xml.gz"), Some(2)) {
        Ok(_) => panic!("a mismatched local size must preserve the HTTP 416 error"),
        Err(error) => error,
    };
    assert!(mismatch.to_string().contains("416"));
    mismatch_handle
        .join()
        .expect("mismatched-range server thread should finish");
    Ok(())
}

#[cfg(coverage)]
#[test]
fn snapshot_patrol_entrypoints_require_a_coverage_transport() -> Result<()> {
    let data_dir = TestDir::new()?;
    assert!(fetch_patrol_for_snapshot("testwiki", "2026-08", data_dir.path()).is_err());
    assert!(preflight_patrol_for_snapshot("testwiki", "2026-08", data_dir.path()).is_err());
    Ok(())
}

#[test]
fn download_logging_dump_writes_and_resumes_existing_files() -> Result<()> {
    init_test_tracing();
    let temp_dir = TestDir::new()?;
    let dest = temp_dir.path().join("patrol.xml.gz");

    // First-write payload starts with the gzip magic (1f 8b) so the
    // post-download integrity check succeeds. The resume scenario then
    // appends additional bytes; the magic check still passes because the
    // file's first two bytes never change after the initial write.
    let first = FakePatrolTransport::new(vec![b"\x1f\x8b\x08".to_vec()], Vec::new());
    let first_spec = test_source_spec("testwiki", "2026-08", b"\x1f\x8b\x08")?;
    download_logging_source(&first, "testwiki", &first_spec, &dest)?;
    assert_eq!(fs::read(&dest)?, b"\x1f\x8b\x08");
    assert_eq!(
        first.get_calls(),
        vec![(
            "https://dumps.wikimedia.org/testwiki/20260901/testwiki-20260901-pages-logging.xml.gz"
                .to_string(),
            None,
        )]
    );

    let second = FakePatrolTransport::new(vec![b"def".to_vec()], Vec::new());
    let complete = b"\x1f\x8b\x08def";
    let second_spec = test_source_spec("testwiki", "2026-08", complete)?;
    download_logging_source(&second, "testwiki", &second_spec, &dest)?;
    assert_eq!(fs::read(&dest)?, b"\x1f\x8b\x08def");
    assert_eq!(
        second.get_calls(),
        vec![(
            "https://dumps.wikimedia.org/testwiki/20260901/testwiki-20260901-pages-logging.xml.gz"
                .to_string(),
            Some(3),
        )]
    );
    Ok(())
}

#[test]
fn download_logging_dump_rejects_payload_with_bad_gzip_magic() -> Result<()> {
    init_test_tracing();
    let temp_dir = TestDir::new()?;
    let dest = temp_dir.path().join("patrol.xml.gz");

    // Server returns bytes that are not a gzip stream. The post-download
    // magic check must reject the response and remove the corrupt file so
    // that ingest never sees it.
    let transport = FakePatrolTransport::new(vec![b"<!DOCTYPE htm".to_vec()], Vec::new());
    let spec = test_source_spec("testwiki", "2026-08", b"<!DOCTYPE htm")?;
    let err = download_logging_source(&transport, "testwiki", &spec, &dest)
        .expect_err("non-gzip payload must fail magic check");
    assert!(err.to_string().contains("gzip magic"));
    assert!(!dest.exists(), "corrupt patrol dump should be removed");
    Ok(())
}

#[test]
fn download_logging_dump_rejects_short_payload() -> Result<()> {
    init_test_tracing();
    let temp_dir = TestDir::new()?;
    let dest = temp_dir.path().join("patrol.xml.gz");

    // A single-byte response is shorter than the 2-byte gzip magic header.
    let transport = FakePatrolTransport::new(vec![b"\x1f".to_vec()], Vec::new());
    let spec = test_source_spec("testwiki", "2026-08", b"\x1f")?;
    let err = download_logging_source(&transport, "testwiki", &spec, &dest)
        .expect_err("1-byte payload must fail magic check");
    assert!(err.to_string().contains("gzip magic"));
    assert!(!dest.exists());
    Ok(())
}

#[test]
fn download_logging_dump_rejects_a_mismatched_transport_length() -> Result<()> {
    let temp_dir = TestDir::new()?;
    let dest = temp_dir.path().join("patrol.xml.gz");
    let body = gzip_bytes("<mediawiki></mediawiki>")?;
    let expected = u64::try_from(body.len())? + 1;
    let transport = FakePatrolTransport::new(vec![body.clone()], Vec::new())
        .with_response_content_length(expected);
    let spec = test_source_spec("testwiki", "2026-08", &body)?;
    let error = download_logging_source(&transport, "testwiki", &spec, &dest)
        .expect_err("a mismatched response length must fail closed");
    assert!(error.to_string().contains("length changed"));
    assert!(!dest.exists());
    Ok(())
}

#[test]
fn fetch_patrol_uses_cached_groups_fallback_and_writes_outputs() -> Result<()> {
    init_test_tracing();
    let data_dir = TestDir::new()?;
    let patrol_dir = data_dir.path().join("patrol").join("testwiki");
    fs::create_dir_all(&patrol_dir)?;
    let meta_path = patrol_dir.join("autopatrol_groups.json");
    fs::write(
        &meta_path,
        serde_json::to_vec(&json!({ "autopatrol_groups": ["cachedgroup"] }))?,
    )?;

    let xml = r#"<?xml version="1.0"?>
<mediawiki xmlns="http://www.mediawiki.org/xml/export-0.11/">
  <logitem>
    <id>1</id>
    <timestamp>2026-01-05T12:00:00Z</timestamp>
    <contributor><username>Patroller</username><id>10</id></contributor>
    <type>patrol</type>
    <logtitle>Page</logtitle>
    <params>101
100
0</params>
  </logitem>
  <logitem>
    <timestamp>2026-01-01T00:00:00Z</timestamp>
    <type>rights</type>
    <logtitle>User:Editor</logtitle>
    <params>autopatrolled
autopatrolled,sysop</params>
  </logitem>
</mediawiki>"#;
    let (transport, _guard) = install_fake_transport(
        vec![gzip_bytes(xml)?],
        vec![json!({
            "query": {
                "usergroups": [
                    { "name": "sysop", "rights": ["edit"] }
                ]
            }
        })],
    );

    fetch_patrol_with_transport("testwiki", data_dir.path(), transport.as_ref())?;

    let patrol_df = read_parquet_df(&patrol_dir.join("patrol.parquet"), None)?;
    assert_eq!(patrol_df.height(), 1);
    assert_eq!(
        patrol_df.column("current_revision_id")?.i64()?.get(0),
        Some(101)
    );
    let rights_df = read_parquet_df(&patrol_dir.join("rights.parquet"), None)?;
    assert_eq!(rights_df.height(), 1);
    assert_eq!(
        read_json(&meta_path)?
            .get("autopatrol_groups")
            .and_then(Value::as_array)
            .expect("cached groups should be written"),
        &vec![json!("cachedgroup")]
    );
    assert_eq!(
        transport.get_calls(),
        vec![(
            "https://dumps.wikimedia.org/testwiki/20260901/testwiki-20260901-pages-logging.xml.gz"
                .to_string(),
            None,
        )]
    );
    assert_eq!(
        transport.json_calls(),
        vec![
            "https://test.wikipedia.org/w/api.php?action=query&meta=siteinfo&siprop=usergroups&format=json"
                .to_string()
        ]
    );
    assert!(
        !patrol_dir
            .join("testwiki-latest-pages-logging.xml.gz")
            .exists(),
        "the compressed source should be released after both Parquet outputs commit"
    );

    let direct_dir = TestDir::new()?;
    let direct_transport = FakePatrolTransport::new(
        vec![gzip_bytes(xml)?],
        vec![json!({
            "query": {
                "usergroups": [
                    { "name": "patroller", "rights": ["autopatrol"] }
                ]
            }
        })],
    );
    fetch_patrol_with_transport("testwiki", direct_dir.path(), &direct_transport)?;
    let direct_meta = direct_dir
        .path()
        .join("patrol/testwiki/autopatrol_groups.json");
    assert_eq!(
        read_json(&direct_meta)?["autopatrol_groups"],
        json!(["patroller"])
    );
    Ok(())
}

#[test]
fn patrol_plan_waits_for_completed_inventory_then_resolves_without_history_work() -> Result<()> {
    let data_dir = TestDir::new()?;
    let waiting = json!({
        "jobs": {
            "xmlpagelogsdumprecombine": {"status": "waiting", "updated": "", "files": {}},
            "xmlpagelogsdump": {"status": "waiting", "updated": "", "files": {}}
        }
    });
    let waiting_transport =
        FakePatrolTransport::new(Vec::new(), Vec::new()).with_dump_statuses(vec![waiting]);
    let error = plan::PatrolSourcePlan::load_or_resolve(
        &waiting_transport,
        "testwiki",
        "2026-08",
        data_dir.path(),
    )
    .expect_err("an incomplete upstream dump must wait");
    assert!(plan::is_upstream_waiting(&error));
    let status: Value = serde_json::from_slice(&fs::read(plan::status_path(
        data_dir.path(),
        "testwiki",
        "2026-08",
    )?)?)?;
    assert_eq!(status["state"], "waiting_upstream");
    assert!(!plan::plan_path(data_dir.path(), "testwiki", "2026-08")?.exists());

    let body = gzip_bytes("<mediawiki></mediawiki>")?;
    let completed = completed_dump_status("testwiki-20260901-pages-logging.xml.gz", &body);
    let ready_transport =
        FakePatrolTransport::new(vec![body], Vec::new()).with_dump_statuses(vec![completed]);
    let resolved = plan::PatrolSourcePlan::load_or_resolve(
        &ready_transport,
        "testwiki",
        "2026-08",
        data_dir.path(),
    )?;
    assert_eq!(resolved.logging_dump_date, "20260901");
    assert_eq!(resolved.coverage_through, "2026-08");
    assert!(!plan::status_path(data_dir.path(), "testwiki", "2026-08")?.exists());

    let blocked_status = plan::status_path(data_dir.path(), "testwiki", "2026-08")?;
    fs::create_dir(&blocked_status)?;
    assert!(
        plan::PatrolSourcePlan::load_or_resolve(
            &ready_transport,
            "testwiki",
            "2026-08",
            data_dir.path(),
        )
        .is_err(),
        "a stale status that cannot be removed must fail closed"
    );
    Ok(())
}

#[test]
fn patrol_plan_handles_year_rollover_and_cleans_failed_atomic_publication() -> Result<()> {
    let year_root = TestDir::new()?;
    let body = gzip_bytes("<mediawiki></mediawiki>")?;
    let year_transport = FakePatrolTransport::new(vec![body.clone()], Vec::new());
    let year_plan = plan::PatrolSourcePlan::load_or_resolve(
        &year_transport,
        "testwiki",
        "2026-12",
        year_root.path(),
    )?;
    assert_eq!(year_plan.logging_dump_date, "20270101");

    let blocked_root = TestDir::new()?;
    let destination = plan::plan_path(blocked_root.path(), "testwiki", "2026-12")?;
    fs::create_dir_all(&destination)?;
    let blocked_transport = FakePatrolTransport::new(vec![body], Vec::new());
    assert!(
        plan::PatrolSourcePlan::load_or_resolve(
            &blocked_transport,
            "testwiki",
            "2026-12",
            blocked_root.path(),
        )
        .is_err()
    );
    let parent = destination.parent().context("plan parent")?;
    assert!(fs::read_dir(parent)?.all(|entry| {
        !entry
            .expect("plan directory entry")
            .file_name()
            .to_string_lossy()
            .ends_with(".tmp")
    }));
    Ok(())
}

#[test]
fn patrol_plan_uses_complete_split_inventory_and_rejects_incomplete_metadata() -> Result<()> {
    let data_dir = TestDir::new()?;
    let split = json!({
        "jobs": {
            "xmlpagelogsdumprecombine": {"status": "waiting", "updated": "", "files": {}},
            "xmlpagelogsdump": {
                "status": "done",
                "updated": "2026-09-04 00:00:00",
                "files": {
                    "testwiki-20260901-pages-logging2.xml.gz": {"size": 2, "url": "/testwiki/20260901/testwiki-20260901-pages-logging2.xml.gz", "md5": "11111111111111111111111111111111", "sha1": "1111111111111111111111111111111111111111"},
                    "testwiki-20260901-pages-logging1.xml.gz": {"size": 1, "url": "/testwiki/20260901/testwiki-20260901-pages-logging1.xml.gz", "md5": "00000000000000000000000000000000", "sha1": "0000000000000000000000000000000000000000"}
                }
            }
        }
    });
    let transport =
        FakePatrolTransport::new(Vec::new(), Vec::new()).with_dump_statuses(vec![split]);
    let resolved = plan::PatrolSourcePlan::load_or_resolve(
        &transport,
        "testwiki",
        "2026-08",
        data_dir.path(),
    )?;
    assert_eq!(resolved.layout, plan::PatrolSourceLayout::Split);
    assert_eq!(resolved.sources.len(), 2);
    assert!(resolved.sources[0].source_id.ends_with("logging1.xml.gz"));

    let bad_root = TestDir::new()?;
    let bad = json!({
        "jobs": {
            "xmlpagelogsdumprecombine": {"status": "done", "files": {
                "testwiki-20260901-pages-logging.xml.gz": {"size": 1, "url": "/testwiki/20260901/testwiki-20260901-pages-logging.xml.gz", "md5": "0", "sha1": "0"}
            }},
            "xmlpagelogsdump": {"status": "waiting", "files": {}}
        }
    });
    let bad_transport =
        FakePatrolTransport::new(Vec::new(), Vec::new()).with_dump_statuses(vec![bad]);
    assert!(
        plan::PatrolSourcePlan::load_or_resolve(
            &bad_transport,
            "testwiki",
            "2026-08",
            bad_root.path(),
        )
        .is_err()
    );
    Ok(())
}

#[test]
fn patrol_plan_orders_double_digit_split_parts_numerically() -> Result<()> {
    let data_dir = TestDir::new()?;
    let mut files = serde_json::Map::new();
    for index in (1..=12).rev() {
        let name = format!("manywiki-20260901-pages-logging{index}.xml.gz");
        files.insert(
            name.clone(),
            json!({
                "size": index,
                "url": format!("/manywiki/20260901/{name}"),
                "md5": format!("{index:032x}"),
                "sha1": format!("{index:040x}")
            }),
        );
    }
    let status = json!({
        "jobs": {
            "xmlpagelogsdumprecombine": {"status": "waiting", "files": {}},
            "xmlpagelogsdump": {
                "status": "done",
                "updated": "2026-09-04 00:00:00",
                "files": Value::Object(files)
            }
        }
    });
    let transport =
        FakePatrolTransport::new(Vec::new(), Vec::new()).with_dump_statuses(vec![status]);

    let plan = plan::PatrolSourcePlan::load_or_resolve(
        &transport,
        "manywiki",
        "2026-08",
        data_dir.path(),
    )?;
    assert!(plan.sources[1].source_id.ends_with("logging2.xml.gz"));
    assert!(plan.sources[9].source_id.ends_with("logging10.xml.gz"));
    assert!(plan.sources[11].source_id.ends_with("logging12.xml.gz"));
    Ok(())
}

#[test]
fn patrol_generation_streams_every_pinned_split_source() -> Result<()> {
    let data_dir = TestDir::new()?;
    let patrol_body = gzip_bytes(
        r#"<mediawiki><logitem><id>1</id><timestamp>2026-01-02T00:00:00Z</timestamp><contributor><username>P</username></contributor><type>patrol</type><params>2
1
0</params></logitem></mediawiki>"#,
    )?;
    let rights_body = gzip_bytes(
        r#"<mediawiki><logitem><id>2</id><timestamp>2026-01-01T00:00:00Z</timestamp><type>rights</type><logtitle>User:Editor</logtitle><params>editor
autopatrolled</params></logitem></mediawiki>"#,
    )?;
    let split = json!({
        "jobs": {
            "xmlpagelogsdumprecombine": {"status": "waiting", "updated": "", "files": {}},
            "xmlpagelogsdump": {
                "status": "done",
                "updated": "2026-09-04 00:00:00",
                "files": {
                    "splitwiki-20260901-pages-logging2.xml.gz": {
                        "size": rights_body.len(),
                        "url": "/splitwiki/20260901/splitwiki-20260901-pages-logging2.xml.gz",
                        "md5": "1".repeat(32),
                        "sha1": source_sha1(&rights_body)
                    },
                    "splitwiki-20260901-pages-logging1.xml.gz": {
                        "size": patrol_body.len(),
                        "url": "/splitwiki/20260901/splitwiki-20260901-pages-logging1.xml.gz",
                        "md5": "0".repeat(32),
                        "sha1": source_sha1(&patrol_body)
                    }
                }
            }
        }
    });
    let transport = FakePatrolTransport::new(
        vec![patrol_body, rights_body],
        vec![json!({"query": {"usergroups": []}})],
    )
    .with_dump_statuses(vec![split]);

    let generated = generation::fetch(&transport, "splitwiki", "2026-08", data_dir.path())?;
    assert_eq!(generated.plan.layout, plan::PatrolSourceLayout::Split);
    assert_eq!(generated.sources.len(), 2);
    assert_eq!(generated.stats.total_log_items, 2);
    assert_eq!(generated.stats.patrol_events, 1);
    assert_eq!(generated.stats.rights_events, 1);
    assert_eq!(generated.patrol_months.len(), 1);
    assert_eq!(generated.rights_months.len(), 1);
    assert_eq!(transport.get_calls().len(), 2);
    Ok(())
}

#[test]
fn patrol_source_checksum_mismatch_is_removed_and_fails_closed() -> Result<()> {
    let temp_dir = TestDir::new()?;
    let destination = temp_dir.path().join("patrol.xml.gz");
    let body = gzip_bytes("<mediawiki></mediawiki>")?;
    let transport = FakePatrolTransport::new(vec![body.clone()], Vec::new());
    let mut source = test_source_spec("testwiki", "2026-08", &body)?;
    source.sha1 = "f".repeat(40);

    let error = download_logging_source(&transport, "testwiki", &source, &destination)
        .expect_err("a source whose bytes do not match the pinned inventory must fail");
    assert!(error.to_string().contains("SHA-1 mismatch"));
    assert!(!destination.exists());
    Ok(())
}

#[test]
fn fetch_patrol_rejects_irrelevant_dump_without_replacing_outputs() -> Result<()> {
    let data_dir = TestDir::new()?;
    let patrol_dir = data_dir.path().join("patrol").join("testwiki");
    fs::create_dir_all(&patrol_dir)?;
    let patrol_path = patrol_dir.join("patrol.parquet");
    let rights_path = patrol_dir.join("rights.parquet");
    fs::write(&patrol_path, b"previous patrol")?;
    fs::write(&rights_path, b"previous rights")?;

    let skipped_item = "<logitem><type>move</type></logitem>";
    let xml = format!(
        "<mediawiki>{}</mediawiki>",
        skipped_item.repeat(SUBSTANTIAL_LOG_ITEMS)
    );
    let transport = FakePatrolTransport::new(
        vec![gzip_bytes(&xml)?],
        vec![json!({ "query": { "usergroups": [] } })],
    );
    let error = fetch_patrol_with_transport("testwiki", data_dir.path(), &transport)
        .expect_err("a substantial irrelevant dump must be rejected");

    assert!(error.to_string().contains("zero patrol or rights events"));
    assert_eq!(fs::read(&patrol_path)?, b"previous patrol");
    assert_eq!(fs::read(&rights_path)?, b"previous rights");
    assert!(!patrol_path.with_extension("parquet.tmp").exists());
    assert!(!rights_path.with_extension("parquet.tmp").exists());
    assert!(
        patrol_dir
            .join("testwiki-latest-pages-logging.xml.gz")
            .is_file(),
        "a rejected parse must retain its source for diagnosis and recovery"
    );
    Ok(())
}

#[test]
fn parse_logging_events_reads_all_gzip_members_and_reports_stats() -> Result<()> {
    init_test_tracing();
    let temp_dir = TestDir::new()?;
    let xml_path = temp_dir.path().join("logging.xml.gz");
    let xml = r#"<?xml version="1.0"?>
<mediawiki xmlns="http://www.mediawiki.org/xml/export-0.11/">
  <!-- comment -->
  <logitem>
    <timestamp>2026-01-01T00:00:00Z</timestamp>
    <type>move</type>
    <comment>ignored</comment>
    <logitem><type>delete</type></logitem>
  </logitem>
  <logitem>
    <id>2</id>
    <timestamp>2026-01-02T00:00:00Z</timestamp>
    <contributor><username>Patroller</username><id>11</id></contributor>
    <type>patrol</type>
    <logtitle>Page</logtitle>
    <params><![CDATA[201
200
1]]></params>
  </logitem>
  <logitem>
    <timestamp>2026-01-03T00:00:00Z</timestamp>
    <type>rights</type>
    <logtitle>User:Editor</logtitle>
    <params>editor
autopatrolled</params>
  </logitem>
  <logitem>
    <id>4</id>
    <timestamp>2026-01-04T00:00:00Z</timestamp>
    <contributor><username>NewEditor</username><id>44</id></contributor>
    <type>newusers</type>
    <action>create</action>
    <logtitle>User:NewEditor</logtitle>
    <params><![CDATA[a:1:{s:9:"4::userid";i:44;}]]></params>
  </logitem>
</mediawiki>"#;
    let second_member = xml
        .find("  <logitem>\n    <id>2</id>")
        .expect("fixture should contain the second log item");
    write_multi_member_gz(&xml_path, &[&xml[..second_member], &xml[second_member..]])?;

    let patrol_path = temp_dir.path().join("patrol.parquet");
    let rights_path = temp_dir.path().join("rights.parquet");
    let mut patrol_writer = PatrolWriter::new_with_batch_rows(&patrol_path, 10)?;
    let mut rights_writer = RightsWriter::new_with_batch_rows(&rights_path, 10)?;
    let stats = parse_logging_events(&xml_path, &mut patrol_writer, &mut rights_writer)?;
    patrol_writer.finish()?;
    rights_writer.finish()?;
    assert_eq!(
        stats,
        LoggingParseStats {
            total_log_items: 4,
            patrol_events: 1,
            rights_events: 1,
            local_account_block_events: 0,
            indefinite_block_events: 0,
            finite_block_events: 0,
            unblock_events: 0,
            unclassified_block_duration_events: 0,
            skipped_events: 2,
        }
    );
    assert_eq!(read_parquet_df(&patrol_path, None)?.height(), 1);
    assert_eq!(read_parquet_df(&rights_path, None)?.height(), 1);

    let malformed_path = temp_dir.path().join("malformed.xml.gz");
    write_gz(&malformed_path, "<mediawiki><logitem></mediawikiX>")?;
    let mut bad_patrol_writer =
        PatrolWriter::new_with_batch_rows(&temp_dir.path().join("bad-patrol.parquet"), 10)?;
    let mut bad_rights_writer =
        RightsWriter::new_with_batch_rows(&temp_dir.path().join("bad-rights.parquet"), 10)?;
    let err = parse_logging_events(
        &malformed_path,
        &mut bad_patrol_writer,
        &mut bad_rights_writer,
    )
    .expect_err("malformed XML should fail");
    assert!(!err.to_string().is_empty());
    Ok(())
}

#[test]
fn generation_fetch_is_snapshot_aware_monthly_and_provenanced() -> Result<()> {
    let data_dir = TestDir::new()?;
    let xml = r#"<mediawiki xmlns="http://www.mediawiki.org/xml/export-0.11/">
  <logitem><id>1</id><timestamp>2026-01-05T12:00:00Z</timestamp><contributor><username>Patroller</username><id>10</id></contributor><type>patrol</type><logtitle>Page</logtitle><params>101
100
0</params></logitem>
  <logitem><id>2</id><timestamp>2026-01-06T12:00:00Z</timestamp><type>rights</type><logtitle>User:Editor</logtitle><params>editor
autopatrolled</params></logitem>
  <logitem><id>3</id><timestamp>2026-02-05T12:00:00Z</timestamp><contributor><username>Patroller</username><id>10</id></contributor><type>patrol</type><logtitle>Page</logtitle><params>102
101
0</params></logitem>
  <logitem><id>4</id><timestamp>2026-02-06T12:00:00Z</timestamp><type>rights</type><logtitle>User:Editor</logtitle><params>autopatrolled
editor</params></logitem>
  <logitem><id>5</id><timestamp>2026-02-07T12:00:00Z</timestamp><contributor><username>Editor</username><id>42</id></contributor><type>newusers</type><action>create</action><logtitle>User:Editor</logtitle><params>a:1:{s:9:"4::userid";i:42;}</params></logitem>
  <logitem><id>6</id><timestamp>2026-02-08T12:00:00Z</timestamp><contributor><username>~2026-1</username><id>43</id></contributor><type>newusers</type><action>autocreate</action><logtitle>User:~2026-1</logtitle><params>43</params></logitem>
  <logitem><id>7</id><timestamp>2026-02-09T12:00:00Z</timestamp><type>block</type><action>block</action><logtitle>User:Blocked_editor</logtitle><params>infinity</params></logitem>
  <logitem><id>8</id><timestamp>2026-02-10T12:00:00Z</timestamp><type>block</type><action>block</action><logtitle>User:Restored editor</logtitle><params>infinity</params></logitem>
  <logitem><id>9</id><timestamp>2026-02-11T12:00:00Z</timestamp><type>block</type><action>unblock</action><logtitle>User:Restored_editor</logtitle><params></params></logitem>
</mediawiki>"#;
    let block_member = xml
        .find("  <logitem><id>7</id>")
        .context("generation fixture should contain a block-history member")?;
    let mut source = gzip_bytes(&xml[..block_member])?;
    source.extend(gzip_bytes(&xml[block_member..])?);
    let transport = FakePatrolTransport::new(
        vec![source.clone()],
        vec![json!({
            "query": { "usergroups": [
                { "name": "autopatrolled", "rights": ["autopatrol"] }
            ] }
        })],
    )
    .with_response_identity("\"logging-v1\"", "Wed, 26 Aug 2026 00:00:00 GMT");

    generation::preflight(&transport, "testwiki", "2026-08", data_dir.path())?;
    generation::preflight(&transport, "testwiki", "2026-08", data_dir.path())?;
    let generation = generation::fetch(&transport, "testwiki", "2026-08", data_dir.path())?;
    assert_eq!(generation.stats.total_log_items, 9);
    assert_eq!(generation.stats.patrol_events, 2);
    assert_eq!(generation.stats.rights_events, 2);
    assert_eq!(generation.stats.local_account_block_events, 3);
    assert_eq!(generation.stats.indefinite_block_events, 2);
    assert_eq!(generation.stats.unblock_events, 1);
    assert_eq!(generation.patrol_months.len(), 2);
    assert_eq!(generation.rights_months.len(), 2);
    assert_eq!(generation.block_months.len(), 1);
    assert_eq!(
        generation.sources[0].content_length,
        u64::try_from(source.len())?
    );
    assert_eq!(
        generation.sources[0].etag.as_deref(),
        Some("\"logging-v1\"")
    );
    assert_eq!(
        generation.sources[0].last_modified.as_deref(),
        Some("Wed, 26 Aug 2026 00:00:00 GMT")
    );
    assert_eq!(generation.parser_version, PATROL_PARSER_VERSION);
    let root = generation::generation_dir(data_dir.path(), "testwiki", "2026-08")?;
    assert!(root.join("generation.json").is_file());
    assert!(root.join("indefinitely-blocked-accounts.json").is_file());
    assert!(
        root.join("blocks/year=2026/month=2026-02/part-00000.parquet")
            .is_file()
    );
    assert!(!root.join("source.xml.gz").exists());
    assert!(
        root.join("patrol/year=2026/month=2026-01/part-00000.parquet")
            .is_file()
    );
    assert!(
        root.join("rights/year=2026/month=2026-02/part-00000.parquet")
            .is_file()
    );
    assert!(
        !data_dir
            .path()
            .join("patrol/testwiki/patrol.parquet")
            .exists()
    );
    let pointer: Value = serde_json::from_slice(&fs::read(
        data_dir
            .path()
            .join("patrol/testwiki/current-generation.json"),
    )?)?;
    assert_eq!(pointer["snapshot"], "2026-08");
    assert_eq!(pointer["manifest_sha256"], generation.manifest_sha256);
    assert_eq!(
        pointer["manifest_file_sha256"].as_str().map(str::len),
        Some(64)
    );
    let summary = source_generation_summary(data_dir.path(), "testwiki", "2026-08")?
        .context("test patrol generation summary is missing")?;
    assert_eq!(summary.total_log_items, 9);
    assert_eq!(summary.patrol_events, 2);
    assert_eq!(summary.rights_events, 2);
    assert_eq!(summary.local_account_block_events, 3);
    assert_eq!(summary.indefinitely_blocked_accounts, 1);
    assert_eq!(summary.block_history_months, 1);
    assert!(summary.block_history_bytes > 0);
    assert_eq!(summary.skipped_events, 2);
    assert_eq!(summary.manifest_sha256, generation.manifest_sha256);
    assert!(source_generation_summary(data_dir.path(), "testwiki", "2026-07")?.is_none());

    let reused = generation::fetch(&transport, "testwiki", "2026-08", data_dir.path())?;
    assert_eq!(reused, generation);
    generation::preflight(&transport, "testwiki", "2026-08", data_dir.path())?;
    assert_eq!(transport.get_calls().len(), 1);

    let blocked =
        generation::load_indefinitely_blocked_accounts(data_dir.path(), "testwiki", "2026-08")?;
    assert_eq!(blocked.accounts.len(), 1);
    assert_eq!(blocked.accounts[0].normalized_name, "Blocked editor");
    assert_eq!(blocked.accounts[0].latest_transition_log_id, 7);

    let block_history = read_parquet_df(
        &generation::artifact_path(&root, &generation.block_months[0])?,
        None,
    )?;
    assert_eq!(block_history.height(), 3);
    assert_eq!(
        block_history
            .column("target_user")?
            .str()?
            .iter()
            .collect::<Vec<_>>(),
        vec![
            Some("Blocked editor"),
            Some("Restored editor"),
            Some("Restored editor")
        ]
    );
    assert_eq!(
        block_history
            .column("action")?
            .str()?
            .iter()
            .collect::<Vec<_>>(),
        vec![Some("block"), Some("block"), Some("unblock")]
    );
    assert_eq!(
        block_history
            .column("resulting_state")?
            .str()?
            .iter()
            .collect::<Vec<_>>(),
        vec![Some("indefinite"), Some("indefinite"), Some("unblocked")]
    );
    assert_eq!(
        block_history
            .column("duration")?
            .str()?
            .iter()
            .collect::<Vec<_>>(),
        vec![Some("infinity"), Some("infinity"), None]
    );

    let blocked_path = root.join("indefinitely-blocked-accounts.json");
    let original_blocked = fs::read(&blocked_path)?;
    fs::write(&blocked_path, b"{}")?;
    assert!(generation::load(data_dir.path(), "testwiki", "2026-08").is_err());
    fs::write(&blocked_path, original_blocked)?;
    assert_eq!(
        generation::load(data_dir.path(), "testwiki", "2026-08")?,
        generation
    );

    let first_patrol = root.join(&generation.patrol_months[0].relative_path);
    let original_patrol = fs::read(&first_patrol)?;
    fs::write(&first_patrol, &original_patrol)?;
    assert_eq!(
        generation::load(data_dir.path(), "testwiki", "2026-08")?,
        generation
    );
    fs::write(&first_patrol, b"corrupt")?;
    assert!(generation::load(data_dir.path(), "testwiki", "2026-08").is_err());
    Ok(())
}

#[test]
fn production_block_index_fails_closed_on_an_unclassified_latest_transition() -> Result<()> {
    let data_dir = TestDir::new()?;
    let xml = r#"<mediawiki>
  <logitem><id>1</id><timestamp>2026-01-05T12:00:00Z</timestamp><contributor><username>Patroller</username><id>10</id></contributor><type>patrol</type><logtitle>Page</logtitle><params>101
100
0</params></logitem>
  <logitem><id>2</id><timestamp>2026-01-06T12:00:00Z</timestamp><type>rights</type><logtitle>User:Editor</logtitle><params>editor
autopatrolled</params></logitem>
  <logitem><id>3</id><timestamp>2026-02-09T12:00:00Z</timestamp><type>block</type><action>block</action><logtitle>User:Unclassified</logtitle><params>a:1:{broken</params></logitem>
</mediawiki>"#;
    let transport = FakePatrolTransport::new(
        vec![gzip_bytes(xml)?],
        vec![json!({"query": {"usergroups": []}})],
    );
    let error = generation::fetch(&transport, "blockwiki", "2026-08", data_dir.path())
        .expect_err("an ambiguous latest block state must not publish an index");
    assert!(error.to_string().contains("cannot be classified"));
    assert!(!generation::manifest_path(data_dir.path(), "blockwiki", "2026-08")?.exists());
    assert!(
        !data_dir
            .path()
            .join("patrol/blockwiki/current-generation.json")
            .exists()
    );
    Ok(())
}

#[test]
fn account_creation_staging_report_uses_creation_cohorts_and_lifetime_edits() -> Result<()> {
    let root = TestDir::new()?;
    let data_dir = root.path().join("data");
    let wiki = "accountwiki";
    let snapshot = "2026-08";
    ingest_history_snapshot(
        &data_dir,
        wiki,
        snapshot,
        &[
            history_row_with_user_id(wiki, "2025-03-01 00:00:00.0", "Edited", "101", "1"),
            history_row_with_user_id(wiki, "2025-03-02 00:00:00.0", "Legacy", "105", "3"),
            history_row_with_user_id(wiki, "2026-07-01 00:00:00.0", "Other", "999", "2"),
        ],
    )?;
    let logging = r#"<?xml version="1.0"?><mediawiki>
<logitem><id>1</id><timestamp>2025-01-01T00:00:00Z</timestamp><contributor><username>Edited</username><id>101</id></contributor><type>newusers</type><action>create</action><logtitle>User:Edited</logtitle><params><![CDATA[a:1:{s:9:"4::userid";i:101;}]]></params></logitem>
<logitem><id>2</id><timestamp>2025-01-02T00:00:00Z</timestamp><contributor><username>Never edited</username><id>102</id></contributor><type>newusers</type><action>create</action><logtitle>User:Never edited</logtitle><params>102</params></logitem>
<logitem><id>3</id><timestamp>2025-02-01T00:00:00Z</timestamp><contributor><username>~2025-1</username><id>103</id></contributor><type>newusers</type><action>autocreate</action><logtitle>User:~2025-1</logtitle><params>103</params></logitem>
<logitem><id>4</id><timestamp>2025-01-03T00:00:00Z</timestamp><contributor><username>Edited</username><id>101</id></contributor><type>newusers</type><action>create</action><logtitle>User:Edited</logtitle><params>101</params></logitem>
<logitem><id>5</id><timestamp>2024-12-31T00:00:00Z</timestamp><contributor><username>Edited</username><id>101</id></contributor><type>newusers</type><action>autocreate</action><logtitle>User:Edited</logtitle><params>101</params></logitem>
<logitem><id>6</id><timestamp>2026-09-01T00:00:00Z</timestamp><contributor><username>Future</username><id>104</id></contributor><type>newusers</type><action>create</action><logtitle>User:Future</logtitle><params>104</params></logitem>
<logitem><id>7</id><timestamp>2025-01-04T00:00:00Z</timestamp><type>rights</type><logtitle>User:Edited</logtitle><params></params></logitem>
<logitem><id>8</id><timestamp>2025-03-01T00:00:00Z</timestamp><contributor><username>Administrator</username><id>999</id></contributor><type>newusers</type><action>create2</action><logtitle>User:Legacy</logtitle><params></params></logitem>
<logitem><id>8</id><timestamp>2025-03-01T00:00:00Z</timestamp><contributor><username>Administrator</username><id>999</id></contributor><type>newusers</type><action>create2</action><logtitle>User:Legacy</logtitle><params></params></logitem>
<logitem><id>9</id><timestamp>2025-04-01T00:00:00Z</timestamp><type>newusers</type><action>create</action><params></params></logitem>
<logitem><id>10</id><timestamp>2025-01-05T00:00:00Z</timestamp><type>block</type><action>block</action><logtitle>User:Edited</logtitle><params>infinity</params></logitem>
<logitem><id>11</id><timestamp>2025-02-05T00:00:00Z</timestamp><type>block</type><action>reblock</action><logtitle>User:Edited</logtitle><params>2 weeks</params></logitem>
<logitem><id>12</id><timestamp>2025-02-06T00:00:00Z</timestamp><type>block</type><action>block</action><logtitle>User:Never_edited</logtitle><params><![CDATA[a:1:{s:11:"5::duration";s:8:"infinity";}]]></params></logitem>
<logitem><id>13</id><timestamp>2025-04-06T00:00:00Z</timestamp><type>block</type><action>block</action><logtitle>User:Legacy</logtitle><params xml:space="preserve" /></logitem>
<logitem><id>14</id><timestamp>2025-04-07T00:00:00Z</timestamp><type>block</type><action>block</action><logtitle>User:~2025-1</logtitle><params>infinity</params></logitem>
<logitem><id>15</id><timestamp>2025-04-08T00:00:00Z</timestamp><type>block</type><action>block</action><logtitle>User:192.0.2.1</logtitle><params>infinity</params></logitem>
<logitem><id>16</id><timestamp>2026-09-01T00:00:00Z</timestamp><type>block</type><action>block</action><logtitle>User:Edited</logtitle><params>infinity</params></logitem>
</mediawiki>"#;
    let split_at = logging
        .find("<logitem><id>2</id>")
        .context("account fixture split point")?;
    let mut logging_source = gzip_bytes(&logging[..split_at])?;
    logging_source.extend(gzip_bytes(&logging[split_at..])?);
    let transport = Arc::new(FakePatrolTransport::new(
        vec![logging_source],
        vec![json!({"query": {"usergroups": []}})],
    ));
    let _transport = install_test_transport(transport);
    let destination = root
        .path()
        .join("staging/account-creations/accountwiki.json");
    build_account_creation_staging_report(wiki, snapshot, &data_dir, &destination)?;
    let report: Value = serde_json::from_slice(&fs::read(destination)?)?;
    assert_eq!(report["schema_version"], 2);
    assert_eq!(report["wiki"], wiki);
    assert_eq!(report["snapshot"], snapshot);
    assert_eq!(report["license_spdx"], "MIT");
    assert_eq!(report["total_log_items"], 17);
    assert_eq!(report["account_creation_events"], 9);
    assert_eq!(report["permanent_account_creation_events"], 7);
    assert_eq!(report["permanent_accounts"], 4);
    assert_eq!(report["duplicate_permanent_creation_events"], 3);
    assert_eq!(report["cross_month_duplicate_events"], 1);
    assert_eq!(report["fallback_identity_accounts"], 2);
    assert_eq!(report["opaque_identity_accounts"], 1);
    assert_eq!(report["temporary_accounts"], 1);
    assert_eq!(report["local_account_block_events"], 4);
    assert_eq!(report["indefinite_block_events"], 3);
    assert_eq!(report["finite_block_events"], 1);
    assert_eq!(report["unblock_events"], 0);
    assert_eq!(report["unclassified_block_duration_events"], 0);
    assert_eq!(report["indefinitely_blocked_accounts"], 2);
    assert_eq!(report["rows"][0]["year_month"], "2024-12");
    assert_eq!(report["rows"][0]["accounts_created"], 1);
    assert_eq!(report["rows"][0]["accounts_with_edits"], 1);
    assert_eq!(report["rows"][0]["accounts_without_edits"], 0);
    assert_eq!(report["rows"][0]["indefinitely_blocked_accounts"], 0);
    assert_eq!(report["rows"][1]["year_month"], "2025-01");
    assert_eq!(report["rows"][1]["accounts_created"], 1);
    assert_eq!(report["rows"][1]["accounts_without_edits"], 1);
    assert_eq!(report["rows"][1]["indefinitely_blocked_accounts"], 1);
    assert_eq!(report["rows"][1]["indefinitely_blocked_without_edits"], 1);
    assert_eq!(report["rows"][2]["year_month"], "2025-02");
    assert_eq!(report["rows"][2]["accounts_created"], 0);
    assert_eq!(report["rows"][2]["temporary_accounts_excluded"], 1);
    assert_eq!(report["rows"][3]["year_month"], "2025-03");
    assert_eq!(report["rows"][3]["accounts_created"], 1);
    assert_eq!(report["rows"][3]["accounts_with_edits"], 1);
    assert_eq!(report["rows"][3]["indefinitely_blocked_accounts"], 1);
    assert_eq!(report["rows"][3]["indefinitely_blocked_with_edits"], 1);
    assert_eq!(report["rows"][4]["year_month"], "2025-04");
    assert_eq!(report["rows"][4]["accounts_created"], 1);
    assert_eq!(report["rows"][4]["accounts_without_edits"], 1);
    assert_eq!(
        fs::read_dir(data_dir.join("staging/account-creations").join(wiki))?.count(),
        0
    );
    Ok(())
}

#[test]
fn account_creation_duplicate_counter_fails_closed_on_overflow() {
    let mut stats = AccountCreationParseStats {
        cross_month_duplicate_events: u64::MAX,
        ..AccountCreationParseStats::default()
    };
    let error = stats
        .record_cross_month_duplicate()
        .expect_err("duplicate counter overflow must fail closed");
    assert!(
        error
            .to_string()
            .contains("duplicate account count overflow")
    );
}

#[test]
fn account_block_duration_classification_covers_current_and_legacy_formats() {
    assert_eq!(
        classify_block_duration(Some(r#"a:1:{s:11:"5::duration";s:8:"infinity";}"#)),
        AccountBlockState::Indefinite
    );
    assert_eq!(
        classify_block_duration(Some(r#"a:1:{i:0;s:8:"infinite";}"#)),
        AccountBlockState::Indefinite
    );
    assert_eq!(
        classify_block_duration(Some(r#"a:1:{s:11:"5::duration";s:7:"2 weeks";}"#)),
        AccountBlockState::NotIndefinite
    );
    assert_eq!(
        classify_block_duration(Some("infinite\nnocreate,noemail")),
        AccountBlockState::Indefinite
    );
    assert_eq!(
        classify_block_duration(Some("2 weeks nocreate")),
        AccountBlockState::NotIndefinite
    );
    assert_eq!(classify_block_duration(None), AccountBlockState::Indefinite);
    assert_eq!(
        classify_block_duration(Some("")),
        AccountBlockState::Indefinite
    );
    assert_eq!(
        classify_block_duration(Some("a:1:{broken")),
        AccountBlockState::Unclassified
    );
    assert_eq!(
        normalized_block_duration(Some(r#"a:1:{s:11:"5::duration";s:7:"2 weeks";}"#)).as_deref(),
        Some("2 weeks")
    );
    assert_eq!(
        normalized_block_duration(Some("infinity\nnocreate,noemail")).as_deref(),
        Some("infinity")
    );
    assert_eq!(normalized_block_duration(Some("")), None);
}

#[test]
fn account_block_replay_uses_latest_transition_and_ignores_non_accounts() -> Result<()> {
    let mut transitions = HashMap::new();
    let mut stats = AccountCreationParseStats::default();
    for (log_id, timestamp, action, target, params) in [
        (2, "2025-02-01 00:00:00", "unblock", "User:Example", None),
        (
            1,
            "2025-01-01 00:00:00",
            "block",
            "User:Example",
            Some("infinity"),
        ),
        (
            3,
            "2025-03-01 00:00:00",
            "block",
            "User:192.0.2.1",
            Some("infinity"),
        ),
        (
            4,
            "2025-03-01 00:00:00",
            "block",
            "User:~2025-1",
            Some("infinity"),
        ),
    ] {
        record_account_block_transition(
            LogItem {
                log_type: Some("block".to_string()),
                log_action: Some(action.to_string()),
                log_id: Some(log_id),
                timestamp: Some(timestamp.to_string()),
                log_title: Some(target.to_string()),
                params: params.map(str::to_string),
                ..LogItem::default()
            },
            "2026-08",
            &mut transitions,
            &mut stats,
        )?;
    }
    assert_eq!(transitions.len(), 1);
    assert_eq!(
        transitions["Example"].state,
        AccountBlockState::NotIndefinite
    );
    assert_eq!(stats.local_account_block_events, 2);
    assert_eq!(stats.indefinite_block_events, 1);
    assert_eq!(stats.unblock_events, 1);
    Ok(())
}

#[test]
fn account_block_helpers_fail_closed_and_cover_identity_edges() -> Result<()> {
    assert_eq!(account_block_target_name("User:"), None);
    assert_eq!(account_block_target_name("User:#42"), None);
    assert_eq!(account_block_target_name("User:2001:db8::/32"), None);
    assert_eq!(
        account_block_target_name("User:Named_account"),
        Some("Named account".to_string())
    );
    assert_eq!(
        classify_block_duration(Some("a:0:{}")),
        AccountBlockState::Indefinite
    );
    assert!(is_indefinite_duration(" NEVER "));

    let mut transitions = HashMap::new();
    let mut stats = AccountCreationParseStats::default();
    record_account_block_transition(
        LogItem {
            log_type: Some("block".to_string()),
            log_action: Some("move".to_string()),
            timestamp: Some("2025-01-01 00:00:00".to_string()),
            ..LogItem::default()
        },
        "2026-08",
        &mut transitions,
        &mut stats,
    )?;
    record_account_block_transition(
        LogItem {
            log_type: Some("block".to_string()),
            log_action: Some("block".to_string()),
            timestamp: Some("2025-01-01 00:00:00".to_string()),
            log_title: None,
            params: Some("infinity".to_string()),
            ..LogItem::default()
        },
        "2026-08",
        &mut transitions,
        &mut stats,
    )?;
    let malformed = LogItem {
        log_type: Some("block".to_string()),
        log_action: Some("block".to_string()),
        log_id: Some(5),
        timestamp: Some("2025-02-01 00:00:00".to_string()),
        log_title: Some("User:Unclassified".to_string()),
        params: Some("a:1:{broken".to_string()),
        ..LogItem::default()
    };
    record_account_block_transition(malformed, "2026-08", &mut transitions, &mut stats)?;
    assert_eq!(stats.unclassified_block_duration_events, 1);
    transitions.insert(
        "Named account".to_string(),
        AccountBlockTransition {
            timestamp: "2025-01-01 00:00:00".to_string(),
            log_id: 1,
            state: AccountBlockState::Indefinite,
        },
    );
    assert_eq!(
        block_transition_for_name(&transitions, "Named_account").map(|value| value.state),
        Some(AccountBlockState::Indefinite)
    );
    let error = resolve_indefinite_block_state(transitions.get("Unclassified"))
        .expect_err("unclassified latest block state must fail closed");
    assert!(error.to_string().contains("cannot be classified"));

    record_account_block_transition(
        LogItem {
            log_type: Some("block".to_string()),
            log_action: Some("block".to_string()),
            log_id: Some(5),
            timestamp: Some("2025-02-01 00:00:00".to_string()),
            log_title: Some("User:Unclassified".to_string()),
            params: Some("a:1:{broken".to_string()),
            ..LogItem::default()
        },
        "2026-08",
        &mut transitions,
        &mut stats,
    )?;

    let conflict = record_account_block_transition(
        LogItem {
            log_type: Some("block".to_string()),
            log_action: Some("block".to_string()),
            log_id: Some(5),
            timestamp: Some("2025-02-01 00:00:00".to_string()),
            log_title: Some("User:Unclassified".to_string()),
            params: Some("infinity".to_string()),
            ..LogItem::default()
        },
        "2026-08",
        &mut transitions,
        &mut stats,
    )
    .expect_err("one log identity cannot describe conflicting block states");
    assert!(
        conflict
            .to_string()
            .contains("conflicting block transitions")
    );

    let mut account_transitions = HashMap::from([(
        1,
        AccountBlockTransition {
            timestamp: "2025-01-01 00:00:00".to_string(),
            log_id: 1,
            state: AccountBlockState::NotIndefinite,
        },
    )]);
    update_account_block_transition(
        &mut account_transitions,
        1,
        AccountBlockTransition {
            timestamp: "2025-02-01 00:00:00".to_string(),
            log_id: 2,
            state: AccountBlockState::Indefinite,
        },
    );
    assert_eq!(account_transitions[&1].state, AccountBlockState::Indefinite);

    let mut value = u64::MAX;
    assert!(checked_increment(&mut value, "test").is_err());

    for (stats, params, expected) in [
        (
            AccountCreationParseStats {
                local_account_block_events: u64::MAX,
                ..AccountCreationParseStats::default()
            },
            "infinity",
            "local account block event count overflow",
        ),
        (
            AccountCreationParseStats {
                indefinite_block_events: u64::MAX,
                ..AccountCreationParseStats::default()
            },
            "infinity",
            "indefinite account block event count overflow",
        ),
        (
            AccountCreationParseStats {
                unclassified_block_duration_events: u64::MAX,
                ..AccountCreationParseStats::default()
            },
            "a:1:{broken",
            "unclassified account block duration event count overflow",
        ),
    ] {
        let mut stats = stats;
        let error = record_account_block_transition(
            LogItem {
                log_type: Some("block".to_string()),
                log_action: Some("block".to_string()),
                log_id: Some(100),
                timestamp: Some("2025-03-01 00:00:00".to_string()),
                log_title: Some("User:Overflow".to_string()),
                params: Some(params.to_string()),
                ..LogItem::default()
            },
            "2026-08",
            &mut HashMap::new(),
            &mut stats,
        )
        .expect_err("block counter overflow must fail closed");
        assert!(error.to_string().contains(expected));
    }
    Ok(())
}

#[test]
fn account_creation_count_overflows_fail_closed() {
    for (counts, edited, blocked, expected) in [
        (
            AccountCreationCounts {
                accounts_created: u32::MAX,
                ..AccountCreationCounts::default()
            },
            false,
            false,
            "account count overflow",
        ),
        (
            AccountCreationCounts {
                accounts_with_edits: u32::MAX,
                ..AccountCreationCounts::default()
            },
            true,
            false,
            "edited account count overflow",
        ),
        (
            AccountCreationCounts {
                indefinitely_blocked_accounts: u32::MAX,
                ..AccountCreationCounts::default()
            },
            false,
            true,
            "indefinitely blocked account count overflow",
        ),
        (
            AccountCreationCounts {
                indefinitely_blocked_with_edits: u32::MAX,
                ..AccountCreationCounts::default()
            },
            true,
            true,
            "edited indefinitely blocked account count overflow",
        ),
    ] {
        let mut monthly = BTreeMap::from([("2025-01".to_string(), counts)]);
        let error = accumulate_account_month(&mut monthly, "2025-01".to_string(), edited, blocked)
            .expect_err("counter overflow must fail closed");
        assert!(error.to_string().contains(expected));
    }
}

#[test]
fn account_creation_parser_propagates_conflicting_block_transitions() -> Result<()> {
    let directory = TestDir::new()?;
    let path = directory.path().join("logging.xml.gz");
    let xml = r#"<mediawiki>
<logitem><id>1</id><timestamp>2025-01-01T00:00:00Z</timestamp><type>newusers</type><action>create</action><params>101</params></logitem>
<logitem><id>2</id><timestamp>2025-02-01T00:00:00Z</timestamp><type>block</type><action>block</action><logtitle>User:Conflict</logtitle><params>infinity</params></logitem>
<logitem><id>2</id><timestamp>2025-02-01T00:00:00Z</timestamp><type>block</type><action>block</action><logtitle>User:Conflict</logtitle><params>2 weeks</params></logitem>
</mediawiki>"#;
    fs::write(&path, gzip_bytes(xml)?)?;
    let error = parse_account_creation_events(
        &path,
        "2026-08",
        &mut HashMap::new(),
        &mut HashMap::new(),
        &mut HashMap::new(),
        &mut HashMap::new(),
        &mut BTreeMap::new(),
    )
    .expect_err("conflicting block transitions must propagate from the parser");
    assert!(error.to_string().contains("conflicting block transitions"));
    Ok(())
}

#[test]
fn account_creation_report_rejects_a_matched_unclassified_block_duration() -> Result<()> {
    let directory = TestDir::new()?;
    let data_dir = directory.path().join("data");
    ingest_history_snapshot(
        &data_dir,
        "blockwiki",
        "2026-08",
        &[history_row_with_user_id(
            "blockwiki",
            "2025-01-02 00:00:00.0",
            "Blocked",
            "101",
            "1",
        )],
    )?;
    let logging = r#"<mediawiki>
<logitem><id>1</id><timestamp>2025-01-01T00:00:00Z</timestamp><contributor><username>Blocked</username><id>101</id></contributor><type>newusers</type><action>create</action><logtitle>User:Blocked</logtitle><params>101</params></logitem>
<logitem><id>2</id><timestamp>2025-02-01T00:00:00Z</timestamp><type>block</type><action>block</action><logtitle>User:Blocked</logtitle><params>a:1:{broken</params></logitem>
</mediawiki>"#;
    let transport = FakePatrolTransport::new(vec![gzip_bytes(logging)?], Vec::new());
    let error = build_account_creation_staging_report_with_transport(
        &transport,
        "blockwiki",
        "2026-08",
        &data_dir,
        &directory.path().join("report.json"),
    )
    .expect_err("matched unclassified block duration must fail closed");
    assert!(error.to_string().contains("cannot be classified"));
    Ok(())
}

#[test]
fn account_creation_fallback_counter_fails_closed_on_overflow() {
    let mut stats = AccountCreationParseStats {
        fallback_identity_accounts: u64::MAX,
        ..AccountCreationParseStats::default()
    };
    let error = stats
        .record_fallback_identity()
        .expect_err("fallback identity counter overflow must fail closed");
    assert!(
        error
            .to_string()
            .contains("fallback-identity account count overflow")
    );
}

#[test]
fn account_creation_opaque_counter_fails_closed_on_overflow() {
    let mut stats = AccountCreationParseStats {
        opaque_identity_accounts: u64::MAX,
        ..AccountCreationParseStats::default()
    };
    let error = stats
        .record_opaque_identity()
        .expect_err("opaque identity counter overflow must fail closed");
    assert!(
        error
            .to_string()
            .contains("opaque-identity account count overflow")
    );
}

#[cfg(coverage)]
#[test]
fn account_creation_report_propagates_missing_history_scan() -> Result<()> {
    let directory = TestDir::new()?;
    let logging = r#"<mediawiki><logitem><id>1</id><timestamp>2025-01-01T00:00:00Z</timestamp><contributor><username>Editor</username><id>101</id></contributor><type>newusers</type><action>create</action><logtitle>User:Editor</logtitle><params>101</params></logitem></mediawiki>"#;
    let transport = FakePatrolTransport::new(vec![gzip_bytes(logging)?], Vec::new());
    let error = build_account_creation_staging_report_with_transport(
        &transport,
        "simplewiki",
        "2026-08",
        directory.path(),
        &directory.path().join("report.json"),
    )
    .expect_err("coverage build cannot fetch retired history inputs");
    assert!(error.to_string().contains("test source loader"));
    Ok(())
}

#[test]
fn account_creation_extraction_fails_closed_and_cleans_logging_staging() -> Result<()> {
    for (wiki, logging, expected) in [
        (
            "malformedwiki",
            "<mediawiki><logitem></wrong>",
            "expected `</logitem>`",
        ),
        (
            "unresolvedwiki",
            r#"<mediawiki><logitem><timestamp>2025-01-01T00:00:00Z</timestamp><type>newusers</type><action>create2</action><params></params></logitem></mediawiki>"#,
            "without a usable target identity",
        ),
        (
            "temporarywiki",
            r#"<mediawiki><logitem><id>1</id><timestamp>2025-01-01T00:00:00Z</timestamp><contributor><username>~2025-1</username><id>101</id></contributor><type>newusers</type><action>autocreate</action><logtitle>User:~2025-1</logtitle><params>101</params></logitem></mediawiki>"#,
            "no permanent account creations",
        ),
    ] {
        let directory = TestDir::new()?;
        let transport = FakePatrolTransport::new(vec![gzip_bytes(logging)?], Vec::new());
        let destination = directory.path().join("report.json");
        let error = build_account_creation_staging_report_with_transport(
            &transport,
            wiki,
            "2026-08",
            directory.path(),
            &destination,
        )
        .expect_err("invalid account-creation input must fail closed");
        assert!(error.to_string().contains(expected), "{error:#}");
        assert!(!destination.exists());
        let staging = directory
            .path()
            .join("staging/account-creations")
            .join(wiki);
        assert_eq!(fs::read_dir(staging)?.count(), 0);
    }
    Ok(())
}

#[cfg(coverage)]
#[test]
fn account_creation_wrapper_requires_an_installed_coverage_transport() {
    let lock = TEST_TRANSPORT_LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    TEST_TRANSPORT.with(|cell| cell.borrow_mut().take());
    let error = build_account_creation_staging_report(
        "svwiki",
        "2026-08",
        Path::new("data"),
        Path::new("report.json"),
    )
    .expect_err("coverage extraction requires an injected transport");
    assert!(error.to_string().contains("install_test_transport"));
    drop(lock);
}

#[test]
fn account_creation_history_fallback_scans_and_reclaims_source_windows() -> Result<()> {
    let directory = TestDir::new()?;
    let source_path = directory.path().join("history.tsv.bz2");
    let mut encoder = BzEncoder::new(File::create(&source_path)?, BzCompression::best());
    for row in [
        history_row_with_user_id("simplewiki", "2025-01-01 00:00:00.0", "A", "101", "1"),
        history_row_with_user_id("simplewiki", "2025-01-02 00:00:00.0", "Other", "999", "2"),
    ] {
        encoder.write_all(row.as_bytes())?;
        encoder.write_all(b"\n")?;
    }
    encoder.finish()?;
    let source_bytes = fs::metadata(&source_path)?.len();
    let plan = crate::snapshot_plan::SnapshotPlan::resolve("simplewiki", "2026-08")?;
    assert_eq!(plan.sources.len(), 1);
    let mut accounts = HashMap::from([
        (101_i64, ("2025-01".to_string(), false)),
        (102_i64, ("2025-01".to_string(), false)),
    ]);
    let mut fallback_accounts =
        HashMap::from([(1_i64, ("2025-01".to_string(), false, Some("A".to_string())))]);
    let block_transitions = HashMap::new();
    let mut account_block_transitions = HashMap::new();
    let fallback_by_name = fallback_name_index(&fallback_accounts);
    let mut matcher = AccountRevisionMatcher {
        accounts: &mut accounts,
        fallback_accounts: &mut fallback_accounts,
        fallback_by_name,
        block_transitions: &block_transitions,
        account_block_transitions: &mut account_block_transitions,
    };
    let scan =
        scan_account_history_source_plan_with(&plan, &mut matcher, |_| Ok(source_path.clone()))?;
    drop(matcher);
    assert_eq!(scan.mode, "bounded_source_window");
    assert_eq!(scan.sources, 1);
    assert_eq!(scan.bytes, source_bytes);
    assert_eq!(scan.revision_rows, 2);
    assert!(accounts[&101].1);
    assert!(!accounts[&102].1);
    assert!(fallback_accounts[&1].1);
    assert!(!source_path.exists());
    Ok(())
}

#[cfg(coverage)]
#[test]
fn account_creation_coverage_build_rejects_uninjected_source_scan() -> Result<()> {
    let directory = TestDir::new()?;
    let error = mark_accounts_with_revisions(
        "simplewiki",
        "2026-08",
        directory.path(),
        &mut HashMap::new(),
        &mut HashMap::new(),
        &HashMap::new(),
        &mut HashMap::new(),
    )
    .expect_err("coverage build has no production downloader");
    assert!(error.to_string().contains("test source loader"));
    Ok(())
}

#[test]
fn patrol_generation_handles_empty_sources_and_cleans_failed_staging() -> Result<()> {
    let data_dir = TestDir::new()?;
    let empty = FakePatrolTransport::new(
        vec![gzip_bytes("<mediawiki></mediawiki>")?],
        vec![json!({"query": {"usergroups": []}})],
    );
    let generation = generation::fetch(&empty, "emptywiki", "2026-08", data_dir.path())?;
    assert!(generation.patrol_months.is_empty());
    assert!(generation.rights_months.is_empty());

    let invalid_month = r#"<mediawiki>
<logitem><id>1</id><timestamp>invalid</timestamp><contributor><username>P</username></contributor><type>patrol</type><params>2
1
0</params></logitem>
</mediawiki>"#;
    let failed = FakePatrolTransport::new(
        vec![gzip_bytes(invalid_month)?],
        vec![json!({"query": {"usergroups": []}})],
    );
    assert!(generation::fetch(&failed, "orderwiki", "2026-08", data_dir.path()).is_err());
    let snapshot_root = data_dir.path().join("patrol/orderwiki/generations/2026-08");
    assert!(fs::read_dir(snapshot_root)?.all(|entry| {
        !entry
            .expect("directory entry")
            .file_name()
            .to_string_lossy()
            .ends_with(".tmp")
    }));

    let incomplete_source = gzip_bytes("<mediawiki></mediawiki>")?;
    let unused = FakePatrolTransport::new(
        vec![incomplete_source],
        vec![json!({"query": {"usergroups": []}})],
    );
    generation::preflight(&unused, "incompletewiki", "2026-08", data_dir.path())?;
    let incomplete = generation::generation_dir(data_dir.path(), "incompletewiki", "2026-08")?;
    fs::create_dir_all(&incomplete)?;
    assert!(generation::fetch(&unused, "incompletewiki", "2026-08", data_dir.path()).is_err());

    let blocked = data_dir.path().join("blocked-generation.json");
    fs::create_dir(&blocked)?;
    assert!(generation::atomic_json(&blocked, &json!({"value": 1})).is_err());
    assert!(fs::read_dir(data_dir.path())?.all(|entry| {
        !entry
            .expect("directory entry")
            .file_name()
            .to_string_lossy()
            .ends_with(".tmp")
    }));
    Ok(())
}

#[test]
fn patrol_generation_sorts_unordered_multi_member_events_deterministically() -> Result<()> {
    let first_member = r#"<mediawiki>
<logitem><id>20</id><timestamp>2024-02-20T00:00:00Z</timestamp><contributor><username>P</username></contributor><type>patrol</type><params>20
3
0</params></logitem>
<logitem><id>19</id><timestamp>2024-02-10T00:00:00Z</timestamp><type>rights</type><logtitle>User:Editor</logtitle><params>editor
autopatrolled</params></logitem>
<logitem><id>30</id><timestamp>2024-02-15T00:00:00Z</timestamp><type>block</type><action>block</action><logtitle>User:Later</logtitle><params>infinity</params></logitem>
"#
    .to_string();
    let mut second_member = String::new();
    for day in (1..=9).rev() {
        second_member.push_str(&format!(
            "<logitem><id>{day}</id><timestamp>2024-01-{day:02}T00:00:00Z</timestamp><contributor><username>P</username></contributor><type>patrol</type><params>{day}\n0\n0</params></logitem>\n"
        ));
    }
    second_member.push_str(
        r#"<logitem><id>12</id><timestamp>2024-01-12T00:00:00Z</timestamp><type>block</type><action>unblock</action><logtitle>User:Earlier</logtitle><params></params></logitem>
<logitem><id>11</id><timestamp>2024-01-11T00:00:00Z</timestamp><type>block</type><action>block</action><logtitle>User:Earlier</logtitle><params>infinity</params></logitem>
<logitem><id>13</id><timestamp>2024-01-13T00:00:00Z</timestamp><type>block</type><action>block</action><logtitle>User:Later</logtitle><params>2 weeks</params></logitem>
<logitem><id>10</id><timestamp>2024-01-05T00:00:00Z</timestamp><type>rights</type><logtitle>User:Editor</logtitle><params>editor
autopatrolled</params></logitem>
</mediawiki>"#,
    );
    let source = {
        let mut bytes = gzip_bytes(&first_member)?;
        bytes.extend(gzip_bytes(&second_member)?);
        bytes
    };
    let build = |root: &Path| -> Result<generation::PatrolGeneration> {
        let transport = FakePatrolTransport::new(
            vec![source.clone()],
            vec![json!({"query": {"usergroups": [
                {"name": "autopatrolled", "rights": ["autopatrol"]}
            ]}})],
        );
        generation::fetch(&transport, "orderwiki", "2026-08", root)
    };

    let first_root = TestDir::new()?;
    let second_root = TestDir::new()?;
    let first = build(first_root.path())?;
    let second = build(second_root.path())?;
    assert_eq!(first.patrol_months.len(), 2);
    assert_eq!(first.rights_months.len(), 2);
    assert_eq!(first.block_months.len(), 2);
    assert_eq!(
        first
            .patrol_months
            .iter()
            .map(|artifact| (
                &artifact.event_month,
                &artifact.artifact_sha256,
                artifact.rows
            ))
            .collect::<Vec<_>>(),
        second
            .patrol_months
            .iter()
            .map(|artifact| (
                &artifact.event_month,
                &artifact.artifact_sha256,
                artifact.rows
            ))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        first
            .block_months
            .iter()
            .map(|artifact| (
                &artifact.event_month,
                &artifact.artifact_sha256,
                artifact.rows
            ))
            .collect::<Vec<_>>(),
        second
            .block_months
            .iter()
            .map(|artifact| (
                &artifact.event_month,
                &artifact.artifact_sha256,
                artifact.rows
            ))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        first
            .rights_months
            .iter()
            .map(|artifact| (
                &artifact.event_month,
                &artifact.artifact_sha256,
                artifact.rows
            ))
            .collect::<Vec<_>>(),
        second
            .rights_months
            .iter()
            .map(|artifact| (
                &artifact.event_month,
                &artifact.artifact_sha256,
                artifact.rows
            ))
            .collect::<Vec<_>>()
    );

    let generation_root = generation::generation_dir(first_root.path(), "orderwiki", "2026-08")?;
    let january_patrol = first
        .patrol_months
        .iter()
        .find(|artifact| artifact.event_month == "2024-01")
        .context("January patrol artifact should exist")?;
    let january_patrol = read_parquet_df(
        &generation::artifact_path(&generation_root, january_patrol)?,
        None,
    )?;
    assert_eq!(
        january_patrol
            .column("timestamp")?
            .str()?
            .iter()
            .flatten()
            .collect::<Vec<_>>(),
        vec![
            "2024-01-01 00:00:00",
            "2024-01-02 00:00:00",
            "2024-01-03 00:00:00",
            "2024-01-04 00:00:00",
            "2024-01-05 00:00:00",
            "2024-01-06 00:00:00",
            "2024-01-07 00:00:00",
            "2024-01-08 00:00:00",
            "2024-01-09 00:00:00",
        ]
    );
    let january_rights = first
        .rights_months
        .iter()
        .find(|artifact| artifact.event_month == "2024-01")
        .context("January rights artifact should exist")?;
    let january_rights = read_parquet_df(
        &generation::artifact_path(&generation_root, january_rights)?,
        None,
    )?;
    assert_eq!(
        january_rights
            .column("timestamp")?
            .str()?
            .iter()
            .flatten()
            .collect::<Vec<_>>(),
        vec!["2024-01-05 00:00:00"]
    );
    let january_blocks = first
        .block_months
        .iter()
        .find(|artifact| artifact.event_month == "2024-01")
        .context("January block artifact should exist")?;
    let january_blocks = read_parquet_df(
        &generation::artifact_path(&generation_root, january_blocks)?,
        None,
    )?;
    assert_eq!(
        january_blocks
            .column("timestamp")?
            .str()?
            .iter()
            .flatten()
            .collect::<Vec<_>>(),
        vec![
            "2024-01-11 00:00:00",
            "2024-01-12 00:00:00",
            "2024-01-13 00:00:00"
        ]
    );
    assert_eq!(
        january_blocks
            .column("resulting_state")?
            .str()?
            .iter()
            .flatten()
            .collect::<Vec<_>>(),
        vec!["indefinite", "unblocked", "finite"]
    );
    let blocked =
        generation::load_indefinitely_blocked_accounts(first_root.path(), "orderwiki", "2026-08")?;
    assert_eq!(
        blocked
            .accounts
            .iter()
            .map(|account| account.normalized_name.as_str())
            .collect::<Vec<_>>(),
        vec!["Later"]
    );
    assert!(!generation_root.join(".spool").exists());
    Ok(())
}

#[test]
fn snapshot_patrol_fetch_wrapper_and_readiness_use_the_generation_receipt() -> Result<()> {
    let data_dir = TestDir::new()?;
    let transport = Arc::new(FakePatrolTransport::new(
        vec![gzip_bytes(
            r#"<mediawiki><logitem><id>1</id><timestamp>2026-01-01T00:00:00Z</timestamp><contributor><username>P</username></contributor><type>patrol</type><params>1
0
0</params></logitem></mediawiki>"#,
        )?],
        vec![json!({"query": {"usergroups": []}})],
    ));
    let _guard = install_test_transport(transport);
    fetch_patrol_for_snapshot("wrapperwiki", "2026-08", data_dir.path())?;
    assert!(cached_sources_available_for_snapshot(
        data_dir.path(),
        "wrapperwiki",
        "2026-08"
    ));
    assert!(!cached_sources_available_for_snapshot(
        data_dir.path(),
        "wrapperwiki",
        "2026-07"
    ));
    Ok(())
}

#[test]
fn incremental_patrol_reuses_months_and_replays_rights_suffix() -> Result<()> {
    init_test_tracing();
    let root = TestDir::new()?;
    let data_dir = root.path().join("data");
    let wiki = "patrolincrementalwiki";
    let jan = history_row(wiki, "2024-01-10 12:00:00.0", "AutoUser", "101");
    let feb = history_row(wiki, "2024-02-10 12:00:00.0", "AutoUser", "102");
    let march = history_row(wiki, "2024-03-10 12:00:00.0", "AutoUser", "103");
    let groups = || {
        json!({
            "query": { "usergroups": [
                { "name": "autopatrolled", "rights": ["autopatrol"] }
            ] }
        })
    };
    let first_xml = r#"<mediawiki>
<logitem><id>1</id><timestamp>2024-01-01T00:00:00Z</timestamp><type>rights</type><logtitle>User:AutoUser</logtitle><params>editor
autopatrolled</params></logitem>
<logitem><id>2</id><timestamp>2024-01-15T00:00:00Z</timestamp><contributor><username>Patroller</username><id>2</id></contributor><type>patrol</type><logtitle>Page</logtitle><params>101
100
0</params></logitem>
</mediawiki>"#;

    ingest_history_snapshot(&data_dir, wiki, "2026-07", &[jan.clone(), feb.clone()])?;
    let first_transport = FakePatrolTransport::new(vec![gzip_bytes(first_xml)?], vec![groups()]);
    generation::fetch(&first_transport, wiki, "2026-07", &data_dir)?;
    let first_output = root.path().join("first-output");
    compute_patrol_for_snapshot(wiki, "2026-07", &data_dir, &first_output, false, None)?;
    compute_patrol_for_snapshot(wiki, "2026-07", &data_dir, &first_output, false, None)?;
    compute_patrol_for_snapshot(
        wiki,
        "2026-07",
        &data_dir,
        &root.path().join("limited-output"),
        false,
        Some(1),
    )?;
    let first = read_parquet_df(&first_output.join(wiki).join("patrol.parquet"), None)?;
    let feb_row = first
        .column("year_month")?
        .str()?
        .iter()
        .position(|value| value == Some("2024-02"))
        .context("first patrol output should contain February")?;
    assert_eq!(
        first.column("autopatrolled_revisions")?.i64()?.get(feb_row),
        Some(1)
    );
    let cache_root = data_dir
        .join("incremental/metric-cache")
        .join(wiki)
        .join("patrol_month");
    assert_eq!(storage::collect_parquet_files(&cache_root)?.len(), 2);

    let changed_rights_xml = r#"<mediawiki>
<logitem><id>1</id><timestamp>2024-01-01T00:00:00Z</timestamp><type>rights</type><logtitle>User:AutoUser</logtitle><params>editor
autopatrolled</params></logitem>
<logitem><id>2</id><timestamp>2024-01-15T00:00:00Z</timestamp><contributor><username>Patroller</username><id>2</id></contributor><type>patrol</type><logtitle>Page</logtitle><params>101
100
0</params></logitem>
<logitem><id>3</id><timestamp>2024-02-01T00:00:00Z</timestamp><type>rights</type><logtitle>User:AutoUser</logtitle><params>autopatrolled
editor</params></logitem>
</mediawiki>"#;
    ingest_history_snapshot(&data_dir, wiki, "2026-08", &[jan.clone(), feb.clone()])?;
    let second_transport =
        FakePatrolTransport::new(vec![gzip_bytes(changed_rights_xml)?], vec![groups()]);
    generation::fetch(&second_transport, wiki, "2026-08", &data_dir)?;
    let second_output = root.path().join("second-output");
    compute_patrol_for_snapshot(wiki, "2026-08", &data_dir, &second_output, false, None)?;
    assert_eq!(
        storage::collect_parquet_files(&cache_root)?.len(),
        3,
        "January should reuse while the changed rights suffix rebuilds February"
    );
    let second = read_parquet_df(&second_output.join(wiki).join("patrol.parquet"), None)?;
    let feb_row = second
        .column("year_month")?
        .str()?
        .iter()
        .position(|value| value == Some("2024-02"))
        .context("second patrol output should contain February")?;
    assert_eq!(
        second
            .column("autopatrolled_revisions")?
            .i64()?
            .get(feb_row),
        Some(0)
    );

    ingest_history_snapshot(&data_dir, wiki, "2026-09", &[jan, feb, march])?;
    let appended_xml = format!(
        "{}{}",
        changed_rights_xml.trim_end_matches("</mediawiki>"),
        r#"<logitem><id>4</id><timestamp>2024-03-15T00:00:00Z</timestamp><contributor><username>Patroller</username><id>2</id></contributor><type>patrol</type><logtitle>Page</logtitle><params>103
102
0</params></logitem></mediawiki>"#
    );
    let third_transport =
        FakePatrolTransport::new(vec![gzip_bytes(&appended_xml)?], vec![groups()]);
    generation::fetch(&third_transport, wiki, "2026-09", &data_dir)?;
    let third_output = root.path().join("third-output");
    compute_patrol_for_snapshot(wiki, "2026-09", &data_dir, &third_output, false, None)?;
    assert_eq!(
        storage::collect_parquet_files(&cache_root)?.len(),
        4,
        "an appended logging month should add one metric cache artifact"
    );

    let historical_patrol_xml = r#"<mediawiki>
<logitem><id>1</id><timestamp>2024-01-01T00:00:00Z</timestamp><type>rights</type><logtitle>User:AutoUser</logtitle><params>editor
autopatrolled</params></logitem>
<logitem><id>2</id><timestamp>2024-01-15T00:00:00Z</timestamp><contributor><username>Patroller</username><id>2</id></contributor><type>patrol</type><logtitle>Page</logtitle><params>101
100
0</params></logitem>
<logitem><id>5</id><timestamp>2024-01-20T00:00:00Z</timestamp><contributor><username>Patroller</username><id>2</id></contributor><type>patrol</type><logtitle>Page</logtitle><params>101
100
0</params></logitem>
<logitem><id>3</id><timestamp>2024-02-01T00:00:00Z</timestamp><type>rights</type><logtitle>User:AutoUser</logtitle><params>autopatrolled
editor</params></logitem>
<logitem><id>4</id><timestamp>2024-03-15T00:00:00Z</timestamp><contributor><username>Patroller</username><id>2</id></contributor><type>patrol</type><logtitle>Page</logtitle><params>103
102
0</params></logitem></mediawiki>"#;
    let july_rows = [
        history_row(wiki, "2024-01-10 12:00:00.0", "AutoUser", "101"),
        history_row(wiki, "2024-02-10 12:00:00.0", "AutoUser", "102"),
        history_row(wiki, "2024-03-10 12:00:00.0", "AutoUser", "103"),
    ];
    ingest_history_snapshot(&data_dir, wiki, "2026-10", &july_rows)?;
    let fourth_transport =
        FakePatrolTransport::new(vec![gzip_bytes(historical_patrol_xml)?], vec![groups()]);
    generation::fetch(&fourth_transport, wiki, "2026-10", &data_dir)?;
    let fourth_output = root.path().join("fourth-output");
    compute_patrol_for_snapshot(wiki, "2026-10", &data_dir, &fourth_output, false, None)?;
    assert_eq!(
        storage::collect_parquet_files(&cache_root)?.len(),
        5,
        "a historical January patrol change should rebuild January but reuse later months"
    );

    let clean_output = root.path().join("clean-output");
    compute_patrol_for_snapshot(wiki, "2026-10", &data_dir, &clean_output, true, None)?;
    let incremental_digest =
        storage::sha256_file(&fourth_output.join(wiki).join("patrol.parquet"))?;
    let clean_digest = storage::sha256_file(&clean_output.join(wiki).join("patrol.parquet"))?;
    assert_eq!(incremental_digest, clean_digest);
    Ok(())
}

#[test]
fn schema2_history_generation_uses_snapshot_scoped_patrol_recovery() -> Result<()> {
    init_test_tracing();
    let root = TestDir::new()?;
    let data_dir = root.path().join("data");
    let wiki = "patrolschema2wiki";
    let snapshot = "2026-08";
    let template = write_revision_partition(
        &data_dir,
        wiki,
        "2026-02",
        &[(
            Some(501),
            Some("2026-02-01 00:00:00"),
            Some("Editor"),
            Some(0),
            None,
            false,
            false,
        )],
    )?;
    let plan = crate::snapshot_plan::SnapshotPlan::load_or_resolve(&data_dir, wiki, snapshot)?.0;
    let source_id = &plan.sources.first().context("snapshot source")?.source_id;
    let metric_root = storage::snapshot_metric_input_wiki_dir(&data_dir, wiki, snapshot)?;
    let metric_file = storage::month_partition_dir(&metric_root, 2026, "2026-02")
        .join(format!("{source_id}.part-00000.parquet"));
    metric_file.parent().map(fs::create_dir_all).transpose()?;
    fs::copy(template, metric_file)?;
    storage::write_test_generation_manifest_from_files(&data_dir, wiki, snapshot)?;
    assert_eq!(
        storage::read_generation_manifest(&data_dir, wiki, snapshot)?.schema_version,
        2
    );

    let logging_xml = r#"<mediawiki>
<logitem><id>1</id><timestamp>2026-02-02T00:00:00Z</timestamp><contributor><username>Patroller</username><id>2</id></contributor><type>patrol</type><logtitle>Page</logtitle><params>501
500
0</params></logitem>
</mediawiki>"#;
    let transport = FakePatrolTransport::new(
        vec![gzip_bytes(logging_xml)?],
        vec![json!({"query": {"usergroups": []}})],
    );
    generation::fetch(&transport, wiki, snapshot, &data_dir)?;

    let output = root.path().join("output");
    compute_patrol_for_snapshot(wiki, snapshot, &data_dir, &output, false, None)?;
    let patrol = read_parquet_df(&output.join(wiki).join("patrol.parquet"), None)?;
    assert!(patrol.height() > 0);
    assert_eq!(patrol.column("patrolled_revisions")?.i64()?.sum(), Some(1));
    assert!(
        !crate::canonical_month::inventory_path(&data_dir, wiki, snapshot)?.exists(),
        "schema-v2 recovery must not claim canonical cross-snapshot identities"
    );
    Ok(())
}

#[test]
fn incremental_patrol_restores_and_authenticates_rights_checkpoints() -> Result<()> {
    let root = TestDir::new()?;
    let data_dir = root.path().join("data");
    let wiki = "patrolcheckpointwiki";
    let december = history_row(wiki, "2024-12-10 12:00:00.0", "AutoUser", "201");
    let january = history_row(wiki, "2025-01-10 12:00:00.0", "AutoUser", "202");
    let february = history_row(wiki, "2025-02-10 12:00:00.0", "AutoUser", "203");
    let logging_xml = r#"<mediawiki>
<logitem><id>1</id><timestamp>2024-01-01T00:00:00Z</timestamp><type>rights</type><logtitle>User:AutoUser</logtitle><params>editor
autopatrolled</params></logitem>
<logitem><id>2</id><timestamp>2024-01-02T00:00:00Z</timestamp><type>rights</type><logtitle>User:AutoUser</logtitle><params>autopatrolled
autopatrolled</params></logitem>
</mediawiki>"#;
    let groups = || {
        json!({
            "query": { "usergroups": [
                { "name": "autopatrolled", "rights": ["autopatrol"] }
            ] }
        })
    };
    let fetch = |snapshot: &str| -> Result<()> {
        let transport = FakePatrolTransport::new(vec![gzip_bytes(logging_xml)?], vec![groups()]);
        generation::fetch(&transport, wiki, snapshot, &data_dir)?;
        Ok(())
    };

    ingest_history_snapshot(&data_dir, wiki, "2026-07", std::slice::from_ref(&december))?;
    fetch("2026-07")?;
    compute_patrol_for_snapshot(
        wiki,
        "2026-07",
        &data_dir,
        &root.path().join("first"),
        false,
        None,
    )?;
    let checkpoint_root = data_dir
        .join("incremental/metric-cache")
        .join(wiki)
        .join("patrol_rights_checkpoint");
    let checkpoint_files = fingerprint::collect_tracked_files(&checkpoint_root, "checkpoint")?;
    assert_eq!(checkpoint_files.len(), 1);
    let checkpoint_path = checkpoint_files[0].path.clone();
    let checkpoint_bytes = fs::read(&checkpoint_path)?;

    ingest_history_snapshot(
        &data_dir,
        wiki,
        "2026-08",
        &[december.clone(), january.clone()],
    )?;
    fetch("2026-08")?;
    let second_output = root.path().join("second");
    compute_patrol_for_snapshot(wiki, "2026-08", &data_dir, &second_output, false, None)?;
    let second = read_parquet_df(&second_output.join(wiki).join("patrol.parquet"), None)?;
    let january_row = second
        .column("year_month")?
        .str()?
        .iter()
        .position(|month| month == Some("2025-01"))
        .context("checkpoint patrol output should contain January 2025")?;
    assert_eq!(
        second
            .column("autopatrolled_revisions")?
            .i64()?
            .get(january_row),
        Some(1),
        "the year-end checkpoint must restore the active autopatrol grant"
    );

    let mut corrupt_checkpoint: Value = serde_json::from_slice(&checkpoint_bytes)?;
    corrupt_checkpoint["active_users"] = json!(["WrongUser"]);
    fs::write(
        &checkpoint_path,
        serde_json::to_vec_pretty(&corrupt_checkpoint)?,
    )?;
    ingest_history_snapshot(&data_dir, wiki, "2026-09", &[december, january, february])?;
    fetch("2026-09")?;
    let third_output = root.path().join("third");
    let error = compute_patrol_for_snapshot(wiki, "2026-09", &data_dir, &third_output, false, None)
        .expect_err("a modified rights checkpoint must fail closed");
    assert!(error.to_string().contains("state hash changed"));

    fs::write(checkpoint_path, checkpoint_bytes)?;
    compute_patrol_for_snapshot(wiki, "2026-09", &data_dir, &third_output, false, None)?;
    let third = read_parquet_df(&third_output.join(wiki).join("patrol.parquet"), None)?;
    let february_row = third
        .column("year_month")?
        .str()?
        .iter()
        .position(|month| month == Some("2025-02"))
        .context("checkpoint patrol output should contain February 2025")?;
    assert_eq!(
        third
            .column("autopatrolled_revisions")?
            .i64()?
            .get(february_row),
        Some(1)
    );
    Ok(())
}

#[test]
fn incremental_patrol_invalidates_the_referenced_revision_month() -> Result<()> {
    let root = TestDir::new()?;
    let data_dir = root.path().join("data");
    let wiki = "patrolcrossmonthwiki";
    let first_revision = history_row(wiki, "2024-01-10 12:00:00.0", "Editor", "301");
    let second_revision = history_row(wiki, "2024-01-20 12:00:00.0", "Editor", "302");
    let first_xml = r#"<mediawiki>
<logitem><id>1</id><timestamp>2024-01-15T00:00:00Z</timestamp><contributor><username>Patroller</username><id>2</id></contributor><type>patrol</type><logtitle>Page</logtitle><params>301
300
0</params></logitem>
</mediawiki>"#;
    let second_xml = format!(
        "{}{}",
        first_xml.trim_end_matches("</mediawiki>"),
        r#"<logitem><id>2</id><timestamp>2024-02-02T00:00:00Z</timestamp><contributor><username>Patroller</username><id>2</id></contributor><type>patrol</type><logtitle>Page</logtitle><params>302
301
0</params></logitem>
<logitem><id>3</id><timestamp>2024-02-03T00:00:00Z</timestamp><contributor><username>Patroller</username><id>2</id></contributor><type>patrol</type><logtitle>Missing</logtitle><params>999
998
0</params></logitem></mediawiki>"#
    );
    let groups = || json!({"query": {"usergroups": []}});

    ingest_history_snapshot(
        &data_dir,
        wiki,
        "2026-07",
        &[first_revision.clone(), second_revision.clone()],
    )?;
    generation::fetch(
        &FakePatrolTransport::new(vec![gzip_bytes(first_xml)?], vec![groups()]),
        wiki,
        "2026-07",
        &data_dir,
    )?;
    let first_output = root.path().join("first-cross-month");
    compute_patrol_for_snapshot(wiki, "2026-07", &data_dir, &first_output, false, None)?;
    let first = read_parquet_df(&first_output.join(wiki).join("patrol.parquet"), None)?;
    assert_eq!(first.column("patrolled_revisions")?.i64()?.get(0), Some(1));

    ingest_history_snapshot(
        &data_dir,
        wiki,
        "2026-08",
        &[first_revision, second_revision],
    )?;
    generation::fetch(
        &FakePatrolTransport::new(vec![gzip_bytes(&second_xml)?], vec![groups()]),
        wiki,
        "2026-08",
        &data_dir,
    )?;
    let second_output = root.path().join("second-cross-month");
    compute_patrol_for_snapshot(wiki, "2026-08", &data_dir, &second_output, false, None)?;
    let second = read_parquet_df(&second_output.join(wiki).join("patrol.parquet"), None)?;
    let january_row = second
        .column("year_month")?
        .str()?
        .iter()
        .position(|month| month == Some("2024-01"))
        .context("cross-month patrol output should contain January")?;
    assert_eq!(
        second
            .column("patrolled_revisions")?
            .i64()?
            .get(january_row),
        Some(2),
        "a February patrol must invalidate coverage for its January revision"
    );
    let metric_cache = data_dir
        .join("incremental/metric-cache")
        .join(wiki)
        .join("patrol_month");
    assert_eq!(
        storage::collect_parquet_files(&metric_cache)?.len(),
        3,
        "only the referenced January and new February metric months should rebuild"
    );

    let clean_output = root.path().join("clean-cross-month");
    compute_patrol_for_snapshot(wiki, "2026-08", &data_dir, &clean_output, true, None)?;
    assert_eq!(
        storage::sha256_file(&second_output.join(wiki).join("patrol.parquet"))?,
        storage::sha256_file(&clean_output.join(wiki).join("patrol.parquet"))?
    );
    Ok(())
}

#[test]
fn incremental_patrol_advances_rights_through_a_month_without_metrics() -> Result<()> {
    let root = TestDir::new()?;
    let data_dir = root.path().join("data");
    let wiki = "patrolsparsemonthwiki";
    let january = history_row(wiki, "2024-01-10 12:00:00.0", "AutoUser", "401");
    let march = history_row(wiki, "2024-03-10 12:00:00.0", "AutoUser", "403");
    ingest_history_snapshot(&data_dir, wiki, "2026-08", &[january, march])?;
    let logging_xml = r#"<mediawiki>
<logitem><id>1</id><timestamp>2024-02-01T00:00:00Z</timestamp><type>rights</type><logtitle>User:AutoUser</logtitle><params>editor
autopatrolled</params></logitem>
</mediawiki>"#;
    generation::fetch(
        &FakePatrolTransport::new(
            vec![gzip_bytes(logging_xml)?],
            vec![json!({"query": {"usergroups": [
                {"name": "autopatrolled", "rights": ["autopatrol"]}
            ]}})],
        ),
        wiki,
        "2026-08",
        &data_dir,
    )?;
    let output = root.path().join("sparse-output");
    compute_patrol_for_snapshot(wiki, "2026-08", &data_dir, &output, false, None)?;
    let patrol = read_parquet_df(&output.join(wiki).join("patrol.parquet"), None)?;
    let march_row = patrol
        .column("year_month")?
        .str()?
        .iter()
        .position(|month| month == Some("2024-03"))
        .context("sparse patrol output should contain March")?;
    assert_eq!(
        patrol
            .column("autopatrolled_revisions")?
            .i64()?
            .get(march_row),
        Some(1)
    );
    Ok(())
}

#[test]
fn logging_parse_validation_rejects_substantial_irrelevant_dumps() -> Result<()> {
    let temp_dir = TestDir::new()?;
    let small_path = temp_dir.path().join("small.xml.gz");
    fs::write(&small_path, b"small")?;
    validate_logging_parse(&small_path, LoggingParseStats::default())?;
    validate_logging_parse(
        &small_path,
        LoggingParseStats {
            total_log_items: SUBSTANTIAL_LOG_ITEMS,
            skipped_events: SUBSTANTIAL_LOG_ITEMS,
            ..Default::default()
        },
    )
    .expect_err("many irrelevant log items must fail validation");

    let large_path = temp_dir.path().join("large.xml.gz");
    let large_file = File::create(&large_path)?;
    large_file.set_len(SUBSTANTIAL_LOGGING_DUMP_BYTES)?;
    validate_logging_parse(&large_path, LoggingParseStats::default())
        .expect_err("a large dump with no relevant events must fail validation");
    validate_logging_parse(
        &large_path,
        LoggingParseStats {
            rights_events: 1,
            ..Default::default()
        },
    )?;
    Ok(())
}

#[test]
fn writers_flush_empty_batches_and_at_threshold() -> Result<()> {
    init_test_tracing();
    let temp_dir = TestDir::new()?;
    let patrol_path = temp_dir.path().join("patrol.parquet");
    let rights_path = temp_dir.path().join("rights.parquet");

    let mut patrol_writer = PatrolWriter::new_with_batch_rows(&patrol_path, 2)?;
    patrol_writer.flush()?;
    patrol_writer.add(PatrolRow {
        log_id: 1,
        timestamp: "2026-01-01 00:00:00".to_string(),
        user: Some("A".to_string()),
        user_id: Some(1),
        page_title: Some("Page A".to_string()),
        current_revision_id: 10,
        prev_revision_id: 9,
        is_auto: false,
    })?;
    patrol_writer.add(PatrolRow {
        log_id: 2,
        timestamp: "2026-01-02 00:00:00".to_string(),
        user: Some("B".to_string()),
        user_id: Some(2),
        page_title: Some("Page B".to_string()),
        current_revision_id: 11,
        prev_revision_id: 0,
        is_auto: true,
    })?;
    patrol_writer.finish()?;
    assert_eq!(read_parquet_df(&patrol_path, None)?.height(), 2);

    let mut rights_writer = RightsWriter::new_with_batch_rows(&rights_path, 2)?;
    rights_writer.flush()?;
    rights_writer.add(RightsRow {
        timestamp: "2026-01-01 00:00:00".to_string(),
        target_user: "EditorA".to_string(),
        old_groups: String::new(),
        new_groups: "autopatrolled".to_string(),
    })?;
    rights_writer.add(RightsRow {
        timestamp: "2026-01-02 00:00:00".to_string(),
        target_user: "EditorA".to_string(),
        old_groups: "autopatrolled".to_string(),
        new_groups: String::new(),
    })?;
    rights_writer.finish()?;
    assert_eq!(read_parquet_df(&rights_path, None)?.height(), 2);

    Ok(())
}

#[test]
fn writer_add_signatures_are_fallible() -> Result<()> {
    // Locks in the contract that `add` returns `Result<()>` instead of panicking
    // on flush failure. If this stops compiling because the return type drifts,
    // the panic-removal in PatrolWriter::add / RightsWriter::add was reverted.
    init_test_tracing();
    let temp_dir = TestDir::new()?;

    let mut patrol = PatrolWriter::new_with_batch_rows(&temp_dir.path().join("p.parquet"), 100)?;
    let patrol_result: Result<()> = patrol.add(PatrolRow {
        log_id: 1,
        timestamp: "2026-01-01 00:00:00".to_string(),
        user: None,
        user_id: None,
        page_title: None,
        current_revision_id: 0,
        prev_revision_id: 0,
        is_auto: false,
    });
    patrol_result?;
    patrol.finish()?;

    let mut rights = RightsWriter::new_with_batch_rows(&temp_dir.path().join("r.parquet"), 100)?;
    let rights_result: Result<()> = rights.add(RightsRow {
        timestamp: "2026-01-01 00:00:00".to_string(),
        target_user: "u".to_string(),
        old_groups: String::new(),
        new_groups: "autopatrolled".to_string(),
    });
    rights_result?;
    rights.finish()?;
    Ok(())
}

#[test]
fn collect_and_process_revision_helpers_cover_invalid_rows_and_autopatrol() -> Result<()> {
    init_test_tracing();

    let patrol_df = DataFrame::new_infer_height(vec![
        Column::new(
            "timestamp".into(),
            vec![
                Some("bad"),
                Some("2026-01-05 12:00:00"),
                Some("2026-02-01 00:00:00"),
            ],
        ),
        Column::new("current_revision_id".into(), vec![1_i64, 2, 3]),
    ])?;
    assert_eq!(collect_patrol_months(&patrol_df)?, vec![202601, 202602]);

    let pending_months = HashSet::from([202601]);
    assert_eq!(
        collect_patrolled_revision_ids(&patrol_df, &pending_months)?,
        HashSet::from([2])
    );

    let temp_dir = TestDir::new()?;
    let path = write_revision_partition(
        temp_dir.path(),
        "testwiki",
        "2026-01",
        &[
            (
                None,
                Some("2026-01-01 00:00:00"),
                Some("SkipId"),
                Some(0),
                None,
                false,
                false,
            ),
            (Some(100), None, Some("SkipTs"), Some(0), None, false, false),
            (
                Some(101),
                Some("2026-02-01 00:00:00"),
                Some("WrongMonth"),
                Some(0),
                None,
                false,
                false,
            ),
            (
                Some(102),
                Some("2026-01-05 10:00:00"),
                Some("Patrolled"),
                Some(0),
                Some("group"),
                false,
                false,
            ),
            (
                Some(103),
                Some("2026-01-05 11:00:00"),
                Some("AutoUser"),
                Some(1),
                None,
                false,
                false,
            ),
        ],
    )?;
    let mut intervals = HashMap::new();
    intervals.insert(
        "AutoUser".to_string(),
        vec![(
            parse_timestamp_seconds("2026-01-01 00:00:00").expect("timestamp"),
            None,
        )],
    );
    let summary = build_revision_summary(
        &BTreeMap::from([(202601, vec![path])]),
        &HashSet::from([102_i64]),
        &HashSet::from([202601_i32]),
        &intervals,
    )?;

    let patrolled_key = MetricKey {
        year_month_key: 202601,
        page_namespace: 0,
        user_type: UserType::Bot,
    };
    let autopatrolled_key = MetricKey {
        year_month_key: 202601,
        page_namespace: 1,
        user_type: UserType::Registered,
    };
    assert_eq!(summary.total_revisions.get(&patrolled_key), Some(&1));
    assert_eq!(summary.patrolled_revisions.get(&patrolled_key), Some(&1));
    assert_eq!(
        summary.autopatrolled_revisions.get(&autopatrolled_key),
        Some(&1)
    );
    Ok(())
}

#[test]
fn revision_lookup_helpers_cover_search_paths_and_sorting() -> Result<()> {
    init_test_tracing();
    let temp_dir = TestDir::new()?;
    let older = write_revision_partition(
        temp_dir.path(),
        "testwiki",
        "2024-01",
        &[(
            Some(401),
            Some("2024-01-10 00:00:00"),
            Some("Old"),
            Some(2),
            None,
            true,
            false,
        )],
    )?;
    let nearby = write_revision_partition(
        temp_dir.path(),
        "testwiki",
        "2026-01",
        &[
            (
                Some(201),
                Some("2026-01-10 00:00:00"),
                Some("Near"),
                Some(0),
                None,
                false,
                false,
            ),
            (
                Some(202),
                Some("bad"),
                Some("BadTs"),
                Some(0),
                None,
                false,
                false,
            ),
        ],
    )?;
    let pending = write_revision_partition(
        temp_dir.path(),
        "testwiki",
        "2026-02",
        &[(
            Some(301),
            Some("2026-02-10 00:00:00"),
            Some("Pending"),
            Some(0),
            None,
            false,
            true,
        )],
    )?;

    let warehouse_dir = storage::warehouse_wiki_dir(temp_dir.path(), "testwiki");
    let all_months = BTreeMap::from([
        (202600, vec![]),
        (202401, vec![older.clone()]),
        (202601, vec![nearby.clone()]),
        (202602, vec![pending.clone()]),
    ]);
    assert_eq!(
        collect_nearby_lookup_months(&all_months, &[202602]),
        vec![202602, 202601]
    );
    assert_eq!(
        collect_nearby_lookup_months(&all_months, &[202600]),
        vec![202600]
    );
    let warehouse_files = storage::collect_parquet_files(&warehouse_dir)?;
    assert!(load_revision_subset_by_ids_once(&warehouse_files, &HashSet::new())?.is_empty());
    assert!(
        load_revision_subset_by_ids_near_pending_months(&all_months, &[], &HashSet::from([201]))?
            .is_empty()
    );
    assert!(
        load_revision_subset_by_ids_near_pending_months(&all_months, &[202602], &HashSet::new(),)?
            .is_empty()
    );

    let nearby_lookup = load_revision_subset_by_ids_near_pending_months(
        &all_months,
        &[202602],
        &HashSet::from([201_i64, 401]),
    )?;
    assert!(nearby_lookup.contains_key(&201));
    assert!(!nearby_lookup.contains_key(&401));
    let early_lookup = load_revision_subset_by_ids_near_pending_months(
        &all_months,
        &[202602],
        &HashSet::from([201_i64]),
    )?;
    assert_eq!(early_lookup.len(), 1);

    let full_lookup =
        load_revision_subset_by_ids_once(&warehouse_files, &HashSet::from([201_i64, 401]))?;
    assert_eq!(
        full_lookup.get(&401).map(|meta| meta.page_namespace),
        Some(2)
    );

    let lookup_df = DataFrame::new_infer_height(vec![
        Column::new(
            "revision_id".into(),
            vec![None, Some(10_i64), Some(11_i64), Some(12_i64), Some(13_i64)],
        ),
        Column::new(
            "event_timestamp".into(),
            vec![
                Some("2026-01-01 00:00:00"),
                Some("2026-01-02 00:00:00"),
                None,
                Some("bad"),
                Some("2026-01-03 00:00:00"),
            ],
        ),
        Column::new(
            "page_namespace".into(),
            vec![Some(0_i32), Some(1), Some(2), Some(3), Some(4)],
        ),
        Column::new(
            "event_user_is_bot_by".into(),
            vec![None::<&str>, None, None, None, None],
        ),
        Column::new(
            "event_user_is_anonymous".into(),
            vec![false, false, false, false, true],
        ),
        Column::new(
            "event_user_is_temporary".into(),
            vec![false, false, false, false, false],
        ),
    ])?;
    let mut direct_lookup = HashMap::new();
    index_revision_lookup_df(
        &lookup_df,
        &HashSet::from([10_i64, 11, 12, 13]),
        &mut direct_lookup,
    )?;
    assert_eq!(direct_lookup.len(), 2);
    assert_eq!(
        direct_lookup.get(&10).map(|meta| meta.page_namespace),
        Some(1)
    );
    assert_eq!(
        direct_lookup.get(&13).map(|meta| meta.user_type),
        Some(UserType::Anonymous)
    );
    Ok(())
}

#[test]
fn aggregate_stats_and_row_metrics_cover_edge_branches() -> Result<()> {
    let patrol_df = DataFrame::new_infer_height(vec![
        Column::new(
            "timestamp".into(),
            vec![
                None,
                Some("bad"),
                Some("2026-02-01 00:00:00"),
                Some("2026-01-01 00:00:00"),
                Some("2026-01-01 02:00:00"),
            ],
        ),
        Column::new("current_revision_id".into(), vec![1_i64, 2, 99, 3, 4]),
        Column::new("prev_revision_id".into(), vec![0_i64, 0, 0, 2, 0]),
        Column::new(
            "user".into(),
            vec![
                Some("A"),
                Some("B"),
                Some("SkipMonth"),
                Some("Patroller"),
                Some("Patroller"),
            ],
        ),
    ])?;
    let pending_months = HashSet::from([202601]);
    let revision_lookup = HashMap::from([
        (
            3_i64,
            RevisionMeta {
                timestamp_seconds: parse_timestamp_seconds("2025-01-01 00:00:00")
                    .expect("timestamp"),
                year_month_key: 202501,
                page_namespace: 0,
                user_type: UserType::Registered,
            },
        ),
        (
            4_i64,
            RevisionMeta {
                timestamp_seconds: parse_timestamp_seconds("2026-01-01 00:00:00")
                    .expect("timestamp"),
                year_month_key: 202601,
                page_namespace: 1,
                user_type: UserType::Temporary,
            },
        ),
    ]);
    let stats = aggregate_patrol_stats(&patrol_df, &pending_months, &revision_lookup)?;
    assert_eq!(stats.len(), 2);
    assert!(stats.values().all(|entry| entry.total_patrols >= 1));

    let no_totals = PatrolRowMetrics::from_parts(None, 0, 0, 0);
    assert_eq!(no_totals.p90_latency_hours, None);
    assert_eq!(no_totals.patrol_coverage_pct, 0.0);
    assert_eq!(no_totals.adjusted_coverage_pct, 0.0);

    let zero_patrols = PatrolAccumulator::default();
    let zero_metrics = PatrolRowMetrics::from_parts(Some(&zero_patrols), 10, 0, 0);
    assert_eq!(zero_metrics.top1_pct, 0.0);
    assert_eq!(zero_metrics.min_patrollers_50pct, 0);

    let mut busy_patrols = PatrolAccumulator {
        total_patrols: 3,
        patrol_new_pages: 1,
        patrol_diffs: 2,
        user_counts: HashMap::from([("Alpha".to_string(), 2_u32), ("Beta".to_string(), 1_u32)]),
        latencies_hours: vec![1.0, 3.0, 9.0],
    };
    let busy_metrics = PatrolRowMetrics::from_parts(Some(&busy_patrols), 6, 3, 1);
    assert_eq!(busy_metrics.unique_patrollers, 2);
    assert_eq!(busy_metrics.min_patrollers_50pct, 1);
    assert_eq!(busy_metrics.median_latency_hours, Some(3.0));
    assert_eq!(busy_metrics.p90_latency_hours, Some(9.0));
    busy_patrols.latencies_hours.clear();

    let temp_dir = TestDir::new()?;
    let output_path = temp_dir.path().join("out").join("testwiki");
    let summary = RevisionSummary::default();
    write_patrol_month_parts(
        temp_dir.path().join("out").as_path(),
        "testwiki",
        &[202601],
        &summary,
        &HashMap::new(),
    )?;
    assert!(
        output_path
            .join("_patrol_parts")
            .join("2026-01.parquet")
            .exists()
    );
    Ok(())
}

#[test]
fn artifact_helpers_cover_bootstrap_merge_and_refresh() -> Result<()> {
    init_test_tracing();
    let temp_dir = TestDir::new()?;
    assert!(load_cached_autopatrol_groups(&temp_dir.path().join("missing.json"))?.is_empty());
    let output_dir = temp_dir.path().join("output");
    fs::create_dir_all(&output_dir)?;

    bootstrap_patrol_parts_from_final(&output_dir, "testwiki")?;
    assert!(merge_wiki_patrol_parts(&output_dir, "testwiki")?.is_none());
    fs::create_dir_all(output_dir.join("testwiki").join("_patrol_parts"))?;
    assert!(merge_wiki_patrol_parts(&output_dir, "testwiki")?.is_none());
    bootstrap_patrol_parts_from_final(&output_dir, "testwiki")?;
    refresh_patrol_dashboard_artifacts(&output_dir, None)?;
    assert!(!output_dir.join("patrol.parquet").exists());

    let rows = vec![
        (
            MetricKey {
                year_month_key: 202601,
                page_namespace: 0,
                user_type: UserType::Registered,
            },
            PatrolRowMetrics {
                total_patrols: 2,
                unique_patrollers: 1,
                patrol_new_pages: 1,
                patrol_diffs: 1,
                median_latency_hours: Some(2.0),
                p90_latency_hours: Some(4.0),
                patrolled_revisions: 2,
                autopatrolled_revisions: 0,
                total_revisions: 3,
                patrol_coverage_pct: 66.6,
                adjusted_coverage_pct: 66.6,
                top1_pct: 100.0,
                min_patrollers_50pct: 1,
            },
        ),
        (
            MetricKey {
                year_month_key: 202602,
                page_namespace: 1,
                user_type: UserType::Anonymous,
            },
            PatrolRowMetrics::default(),
        ),
    ];
    let final_path = output_dir.join("testwiki").join("patrol.parquet");
    fs::create_dir_all(
        final_path
            .parent()
            .expect("final path should have a parent"),
    )?;
    write_patrol_metrics_df(&final_path, "testwiki", &rows)?;

    bootstrap_patrol_parts_from_final(&output_dir, "testwiki")?;
    let parts_dir = output_dir.join("testwiki").join("_patrol_parts");
    assert!(parts_dir.join("2026-01.parquet").exists());
    assert!(parts_dir.join("2026-02.parquet").exists());
    fs::write(parts_dir.join("ignore.txt"), "skip")?;
    assert_eq!(
        existing_patrol_months(&output_dir, "testwiki")?,
        BTreeSet::from([202601, 202602])
    );
    bootstrap_patrol_parts_from_final(&output_dir, "testwiki")?;

    let merged = merge_wiki_patrol_parts(&output_dir, "testwiki")?;
    assert_eq!(merged.as_deref(), Some(final_path.as_path()));
    refresh_patrol_dashboard_artifacts(&output_dir, merged.as_deref())?;
    assert!(output_dir.join("patrol.parquet").exists());
    assert!(!output_dir.join("defaults_patrol.json").exists());
    let corrupt_dir = output_dir.join("zzwiki");
    fs::create_dir_all(&corrupt_dir)?;
    fs::write(corrupt_dir.join("patrol.parquet"), b"not parquet")?;
    assert!(refresh_patrol_dashboard_artifacts(&output_dir, None).is_err());
    Ok(())
}

#[test]
fn clear_patrol_parts_dir_is_idempotent_and_removes_existing_dir() -> Result<()> {
    init_test_tracing();
    let temp_dir = TestDir::new()?;
    let output_dir = temp_dir.path().join("output");

    // Missing dir → no-op (covers the early-return path).
    clear_patrol_parts_dir(&output_dir, "missingwiki")?;

    // Existing dir with a stray file → removed.
    let parts_dir = output_dir.join("livewiki").join("_patrol_parts");
    fs::create_dir_all(&parts_dir)?;
    fs::write(parts_dir.join("stale.parquet"), b"stale")?;
    clear_patrol_parts_dir(&output_dir, "livewiki")?;
    assert!(!parts_dir.exists());
    Ok(())
}

#[test]
fn cached_patrol_sources_require_three_nonempty_files() -> Result<()> {
    let data_dir = TestDir::new()?;
    let patrol_dir = data_dir.path().join("patrol/testwiki");
    fs::create_dir_all(&patrol_dir)?;
    assert!(!cached_sources_available(data_dir.path(), "testwiki"));
    fs::write(
        patrol_dir.join("autopatrol_groups.json"),
        b"{\"autopatrol_groups\":[]}",
    )?;
    write_patrol_events(
        &patrol_dir.join("patrol.parquet"),
        &[(Some("2026-01-01 00:00:00"), 1, 0, Some("Patroller"))],
    )?;
    write_rights_events(
        &patrol_dir.join("rights.parquet"),
        &[(
            Some("2026-01-01 00:00:00"),
            Some("Editor"),
            Some(""),
            Some("sysop"),
        )],
    )?;
    assert!(cached_sources_available(data_dir.path(), "testwiki"));
    fs::write(patrol_dir.join("rights.parquet"), b"")?;
    assert!(!cached_sources_available(data_dir.path(), "testwiki"));
    Ok(())
}

#[test]
fn bootstrap_patrol_parts_writes_atomically_and_ignores_stray_tmp_files() -> Result<()> {
    init_test_tracing();
    let temp_dir = TestDir::new()?;
    let output_dir = temp_dir.path().join("output");

    // Build a small final patrol.parquet so bootstrap has something to split.
    let rows: Vec<(MetricKey, PatrolRowMetrics)> = vec![(
        MetricKey {
            year_month_key: 202601,
            page_namespace: 0,
            user_type: UserType::Registered,
        },
        PatrolRowMetrics::default(),
    )];
    let final_path = output_dir.join("testwiki").join("patrol.parquet");
    fs::create_dir_all(
        final_path
            .parent()
            .expect("final path should have a parent"),
    )?;
    write_patrol_metrics_df(&final_path, "testwiki", &rows)?;

    // Simulate an interrupted prior bootstrap: stray .parquet.tmp from a
    // different month, with garbage content. Pre-rename atomicity, the old
    // direct File::create path could leave a corrupt .parquet here that
    // existing_patrol_months would then count as complete. With the rename
    // pattern, .parquet.tmp survivors are filtered out by extension.
    let parts_dir = output_dir.join("testwiki").join("_patrol_parts");
    fs::create_dir_all(&parts_dir)?;
    let stray_tmp = parts_dir.join("2025-12.parquet.tmp");
    fs::write(&stray_tmp, b"not parquet")?;

    bootstrap_patrol_parts_from_final(&output_dir, "testwiki")?;

    // Bootstrap produced the real month file…
    assert!(parts_dir.join("2026-01.parquet").exists());
    // …and did not leave its own .parquet.tmp behind on success.
    assert!(!parts_dir.join("2026-01.parquet.tmp").exists());
    // The stray pre-existing .tmp must not be promoted to a complete month.
    assert!(!parts_dir.join("2025-12.parquet").exists());
    assert_eq!(
        existing_patrol_months(&output_dir, "testwiki")?,
        BTreeSet::from([202601])
    );
    Ok(())
}

#[test]
fn autopatrol_intervals_cover_empty_invalid_and_closed_ranges() -> Result<()> {
    let temp_dir = TestDir::new()?;
    let rights_path = temp_dir.path().join("rights.parquet");

    assert!(build_autopatrol_intervals(&rights_path, &[])?.is_empty());

    write_rights_events(
        &rights_path,
        &[
            (
                Some("2026-01-01 00:00:00"),
                None,
                Some(""),
                Some("autopatrolled"),
            ),
            (Some("bad"), Some("BadTs"), Some(""), Some("autopatrolled")),
            (
                Some("2026-01-02 00:00:00"),
                Some("NoChange"),
                Some("sysop"),
                Some("sysop"),
            ),
            (
                Some("2026-01-03 00:00:00"),
                Some("GrantThenRevoke"),
                Some(""),
                Some("autopatrolled"),
            ),
            (
                Some("2026-01-04 00:00:00"),
                Some("GrantThenRevoke"),
                Some("autopatrolled"),
                Some(""),
            ),
            (
                Some("2026-01-05 00:00:00"),
                Some("OpenEnded"),
                Some(""),
                Some("autopatrolled"),
            ),
        ],
    )?;

    let intervals = build_autopatrol_intervals(&rights_path, &[String::from("autopatrolled")])?;
    assert_eq!(
        intervals.get("GrantThenRevoke"),
        Some(&vec![(
            parse_timestamp_seconds("2026-01-03 00:00:00").expect("timestamp"),
            Some(parse_timestamp_seconds("2026-01-04 00:00:00").expect("timestamp")),
        )])
    );
    assert!(user_has_autopatrol_at(
        &intervals,
        "GrantThenRevoke",
        parse_timestamp_seconds("2026-01-03 12:00:00").expect("timestamp"),
    ));
    assert!(!user_has_autopatrol_at(
        &intervals,
        "GrantThenRevoke",
        parse_timestamp_seconds("2026-01-04 00:00:00").expect("timestamp"),
    ));
    assert!(user_has_autopatrol_at(
        &intervals,
        "OpenEnded",
        parse_timestamp_seconds("2026-02-01 00:00:00").expect("timestamp"),
    ));
    Ok(())
}

#[test]
fn compute_patrol_reports_missing_inputs_and_executes_rebuild_lookup_and_no_pending_paths()
-> Result<()> {
    init_test_tracing();
    let temp_dir = TestDir::new()?;
    let data_dir = temp_dir.path().join("data");
    let output_dir = temp_dir.path().join("output");

    let err = compute_patrol("testwiki", &data_dir, &output_dir, false, None)
        .expect_err("missing patrol data should fail");
    assert!(err.to_string().contains("patrol-fetch"));

    let patrol_dir = data_dir.join("patrol").join("testwiki");
    fs::create_dir_all(&patrol_dir)?;
    write_patrol_events(
        &patrol_dir.join("patrol.parquet"),
        &[(Some("2026-02-05 12:00:00"), 201, 200, Some("PatrollerA"))],
    )?;
    write_rights_events(&patrol_dir.join("rights.parquet"), &[])?;
    fs::write(
        patrol_dir.join("autopatrol_groups.json"),
        serde_json::to_vec(&json!({ "autopatrol_groups": ["autopatrolled"] }))?,
    )?;
    let err = compute_patrol("testwiki", &data_dir, &output_dir, false, None)
        .expect_err("missing warehouse data should fail");
    assert!(err.to_string().contains("ingest"));

    write_patrol_events(
        &patrol_dir.join("patrol.parquet"),
        &[
            (Some("2026-02-05 12:00:00"), 201, 200, Some("PatrollerA")),
            (Some("2026-02-06 12:00:00"), 401, 0, Some("PatrollerB")),
        ],
    )?;
    write_rights_events(
        &patrol_dir.join("rights.parquet"),
        &[(
            Some("2026-01-01 00:00:00"),
            Some("AutoUser"),
            Some(""),
            Some("autopatrolled"),
        )],
    )?;
    write_revision_partition(
        &data_dir,
        "testwiki",
        "2026-02",
        &[(
            Some(202),
            Some("2026-02-01 08:00:00"),
            Some("AutoUser"),
            Some(0),
            None,
            false,
            false,
        )],
    )?;
    write_revision_partition(
        &data_dir,
        "testwiki",
        "2026-01",
        &[(
            Some(201),
            Some("2026-01-31 23:00:00"),
            Some("NearBy"),
            Some(0),
            None,
            false,
            false,
        )],
    )?;
    write_revision_partition(
        &data_dir,
        "testwiki",
        "2024-01",
        &[(
            Some(401),
            Some("2024-01-15 12:00:00"),
            Some("FarAway"),
            Some(1),
            None,
            false,
            false,
        )],
    )?;

    let stale_parts_dir = output_dir.join("testwiki").join("_patrol_parts");
    fs::create_dir_all(&stale_parts_dir)?;
    fs::write(stale_parts_dir.join("stale.txt"), "remove me")?;

    compute_patrol("testwiki", &data_dir, &output_dir, false, Some(1))?;
    compute_patrol("testwiki", &data_dir, &output_dir, true, None)?;
    assert!(
        output_dir
            .join("testwiki")
            .join("_patrol_parts")
            .join("2026-02.parquet")
            .exists()
    );
    assert!(output_dir.join("testwiki").join("patrol.parquet").exists());
    assert!(!output_dir.join("defaults_patrol.json").exists());

    let wiki_output = output_dir.join("testwiki").join("patrol.parquet");
    let dashboard_output = output_dir.join("patrol.parquet");
    let before = [
        fs::metadata(&wiki_output)?.modified()?,
        fs::metadata(&dashboard_output)?.modified()?,
    ];
    std::thread::sleep(std::time::Duration::from_millis(10));
    compute_patrol("testwiki", &data_dir, &output_dir, false, None)?;
    let after = [
        fs::metadata(&wiki_output)?.modified()?,
        fs::metadata(&dashboard_output)?.modified()?,
    ];
    assert_eq!(
        after, before,
        "a no-op patrol refresh must preserve all published artifacts"
    );

    compute_patrol("testwiki", &data_dir, &output_dir, false, Some(1))?;

    fs::remove_file(&wiki_output)?;
    fs::remove_file(&dashboard_output)?;
    compute_patrol("testwiki", &data_dir, &output_dir, false, Some(1))?;
    assert!(wiki_output.exists());
    assert!(dashboard_output.exists());
    assert!(!output_dir.join("defaults_patrol.json").exists());

    let snapshot_wiki = "snapshotwiki";
    let snapshot = "2026-08";
    let template = write_revision_partition(
        &data_dir,
        snapshot_wiki,
        "2026-02",
        &[(
            Some(501),
            Some("2026-02-01 00:00:00"),
            Some("SnapshotUser"),
            Some(0),
            None,
            false,
            false,
        )],
    )?;
    let plan =
        crate::snapshot_plan::SnapshotPlan::load_or_resolve(&data_dir, snapshot_wiki, snapshot)?.0;
    let source_id = &plan.sources.first().context("snapshot source")?.source_id;
    let metric_root = storage::snapshot_metric_input_wiki_dir(&data_dir, snapshot_wiki, snapshot)?;
    let metric_file = storage::month_partition_dir(&metric_root, 2026, "2026-02")
        .join(format!("{source_id}.part-00000.parquet"));
    metric_file.parent().map(fs::create_dir_all).transpose()?;
    fs::copy(template, metric_file)?;
    storage::write_test_generation_manifest_from_files(&data_dir, snapshot_wiki, snapshot)?;
    assert_eq!(
        collect_partition_files_by_month(&data_dir, snapshot_wiki, Some(snapshot))?.len(),
        1
    );
    let error =
        compute_patrol_for_snapshot(snapshot_wiki, snapshot, &data_dir, &output_dir, false, None)
            .expect_err("snapshot patrol inputs are intentionally absent");
    assert!(error.to_string().contains("patrol-fetch"));
    Ok(())
}
