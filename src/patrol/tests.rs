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

fn gzip_bytes(content: &str) -> Result<Vec<u8>> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(content.as_bytes())?;
    encoder.finish().map_err(Into::into)
}

fn history_row(wiki: &str, timestamp: &str, username: &str, revision_id: &str) -> String {
    let mut row = vec![String::new(); crate::schema::COLUMNS.len()];
    for (name, value) in [
        ("wiki_db", wiki),
        ("event_entity", "revision"),
        ("event_type", "create"),
        ("event_timestamp", timestamp),
        ("event_user_id", "1"),
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
    download_logging_dump(&first, "testwiki", &dest)?;
    assert_eq!(fs::read(&dest)?, b"\x1f\x8b\x08");
    assert_eq!(
        first.get_calls(),
        vec![(
            "https://dumps.wikimedia.org/testwiki/latest/testwiki-latest-pages-logging.xml.gz"
                .to_string(),
            None,
        )]
    );

    let second = FakePatrolTransport::new(vec![b"def".to_vec()], Vec::new());
    download_logging_dump(&second, "testwiki", &dest)?;
    assert_eq!(fs::read(&dest)?, b"\x1f\x8b\x08def");
    assert_eq!(
        second.get_calls(),
        vec![(
            "https://dumps.wikimedia.org/testwiki/latest/testwiki-latest-pages-logging.xml.gz"
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
    let err = download_logging_dump(&transport, "testwiki", &dest)
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
    let err = download_logging_dump(&transport, "testwiki", &dest)
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
    let transport =
        FakePatrolTransport::new(vec![body], Vec::new()).with_response_content_length(expected);
    let error = download_logging_dump(&transport, "testwiki", &dest)
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

    fetch_patrol("testwiki", data_dir.path())?;

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
            "https://dumps.wikimedia.org/testwiki/latest/testwiki-latest-pages-logging.xml.gz"
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
            total_log_items: 3,
            patrol_events: 1,
            rights_events: 1,
            skipped_events: 1,
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
</mediawiki>"#;
    let source = gzip_bytes(xml)?;
    let transport = FakePatrolTransport::new(
        vec![source.clone()],
        vec![json!({
            "query": { "usergroups": [
                { "name": "autopatrolled", "rights": ["autopatrol"] }
            ] }
        })],
    )
    .with_response_identity("\"logging-v1\"", "Wed, 26 Aug 2026 00:00:00 GMT");

    let generation = generation::fetch(&transport, "testwiki", "2026-08", data_dir.path())?;
    assert_eq!(generation.stats.total_log_items, 4);
    assert_eq!(generation.stats.patrol_events, 2);
    assert_eq!(generation.stats.rights_events, 2);
    assert_eq!(generation.patrol_months.len(), 2);
    assert_eq!(generation.rights_months.len(), 2);
    assert_eq!(
        generation.source.content_length,
        u64::try_from(source.len())?
    );
    assert_eq!(generation.source.etag.as_deref(), Some("\"logging-v1\""));
    assert_eq!(
        generation.source.last_modified.as_deref(),
        Some("Wed, 26 Aug 2026 00:00:00 GMT")
    );
    assert_eq!(generation.parser_version, PATROL_PARSER_VERSION);
    let root = generation::generation_dir(data_dir.path(), "testwiki", "2026-08")?;
    assert!(root.join("generation.json").is_file());
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
    assert_eq!(summary.total_log_items, 4);
    assert_eq!(summary.patrol_events, 2);
    assert_eq!(summary.rights_events, 2);
    assert_eq!(summary.skipped_events, 0);
    assert_eq!(summary.manifest_sha256, generation.manifest_sha256);
    assert!(source_generation_summary(data_dir.path(), "testwiki", "2026-07")?.is_none());

    let reused = generation::fetch(&transport, "testwiki", "2026-08", data_dir.path())?;
    assert_eq!(reused, generation);
    assert_eq!(transport.get_calls().len(), 1);

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
fn patrol_generation_handles_empty_sources_and_cleans_failed_staging() -> Result<()> {
    let data_dir = TestDir::new()?;
    let empty = FakePatrolTransport::new(
        vec![gzip_bytes("<mediawiki></mediawiki>")?],
        vec![json!({"query": {"usergroups": []}})],
    );
    let generation = generation::fetch(&empty, "emptywiki", "2026-08", data_dir.path())?;
    assert!(generation.patrol_months.is_empty());
    assert!(generation.rights_months.is_empty());

    let out_of_order = r#"<mediawiki>
<logitem><id>1</id><timestamp>2024-02-02T00:00:00Z</timestamp><contributor><username>P</username></contributor><type>patrol</type><params>2
1
0</params></logitem>
<logitem><id>2</id><timestamp>2024-01-02T00:00:00Z</timestamp><contributor><username>P</username></contributor><type>patrol</type><params>1
0
0</params></logitem>
</mediawiki>"#;
    let failed = FakePatrolTransport::new(
        vec![gzip_bytes(out_of_order)?],
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

    let incomplete = generation::generation_dir(data_dir.path(), "incompletewiki", "2026-08")?;
    fs::create_dir_all(&incomplete)?;
    let unused = FakePatrolTransport::new(Vec::new(), Vec::new());
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
