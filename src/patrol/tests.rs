use super::*;
use crate::storage;
use crate::test_support::{TestDir, init_test_tracing};
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
}

impl FakePatrolTransport {
    fn new(get_bodies: Vec<Vec<u8>>, json_values: Vec<Value>) -> Self {
        Self {
            get_bodies: Mutex::new(get_bodies.into()),
            json_values: Mutex::new(json_values.into()),
            get_calls: Mutex::new(Vec::new()),
            json_calls: Mutex::new(Vec::new()),
        }
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
        Ok(PatrolTransportResponse::from_bytes(bytes))
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

fn write_gz(path: &Path, content: &str) -> Result<()> {
    fs::write(path, gzip_bytes(content)?)?;
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
fn download_logging_dump_writes_and_resumes_existing_files() -> Result<()> {
    init_test_tracing();
    let temp_dir = TestDir::new()?;
    let dest = temp_dir.path().join("patrol.xml.gz");

    let first = FakePatrolTransport::new(vec![b"abc".to_vec()], Vec::new());
    download_logging_dump(&first, "testwiki", &dest)?;
    assert_eq!(fs::read(&dest)?, b"abc");
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
    assert_eq!(fs::read(&dest)?, b"abcdef");
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
    Ok(())
}

#[test]
fn parse_logging_events_handles_cdata_unknown_tags_and_errors() -> Result<()> {
    init_test_tracing();
    let temp_dir = TestDir::new()?;
    let xml_path = temp_dir.path().join("logging.xml.gz");
    write_gz(
        &xml_path,
        r#"<?xml version="1.0"?>
<mediawiki xmlns="http://www.mediawiki.org/xml/export-0.11/">
  <!-- comment -->
  <logitem>
    <timestamp>2026-01-01T00:00:00Z</timestamp>
    <type>move</type>
    <comment>ignored</comment>
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
</mediawiki>"#,
    )?;

    let patrol_path = temp_dir.path().join("patrol.parquet");
    let rights_path = temp_dir.path().join("rights.parquet");
    let mut patrol_writer = PatrolWriter::new_with_batch_rows(&patrol_path, 10)?;
    let mut rights_writer = RightsWriter::new_with_batch_rows(&rights_path, 10)?;
    let (patrol_count, rights_count) =
        parse_logging_events(&xml_path, &mut patrol_writer, &mut rights_writer)?;
    patrol_writer.finish()?;
    rights_writer.finish()?;
    assert_eq!((patrol_count, rights_count), (1, 0));

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
    assert!(load_revision_subset_by_ids_once(&warehouse_dir, &HashSet::new())?.is_empty());
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
        load_revision_subset_by_ids_once(&warehouse_dir, &HashSet::from([201_i64, 401]))?;
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
                page_namespace: 0,
                user_type: UserType::Registered,
            },
        ),
        (
            4_i64,
            RevisionMeta {
                timestamp_seconds: parse_timestamp_seconds("2026-01-01 00:00:00")
                    .expect("timestamp"),
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
fn artifact_helpers_cover_bootstrap_merge_refresh_and_defaults() -> Result<()> {
    init_test_tracing();
    let temp_dir = TestDir::new()?;
    assert!(load_cached_autopatrol_groups(&temp_dir.path().join("missing.json"))?.is_empty());
    let output_dir = temp_dir.path().join("output");
    fs::create_dir_all(&output_dir)?;

    assert!(merge_wiki_patrol_parts(&output_dir, "testwiki")?.is_none());
    fs::create_dir_all(output_dir.join("testwiki").join("_patrol_parts"))?;
    assert!(merge_wiki_patrol_parts(&output_dir, "testwiki")?.is_none());
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
    assert!(output_dir.join("defaults_patrol.json").exists());
    Ok(())
}

#[test]
fn write_defaults_patrol_json_filters_rows_and_nulls() -> Result<()> {
    let temp_dir = TestDir::new()?;
    let output_dir = temp_dir.path().join("output");
    fs::create_dir_all(&output_dir)?;
    let merged_path = output_dir.join("patrol.parquet");

    let mut df = DataFrame::new_infer_height(vec![
        Column::new(
            "year_month".into(),
            vec![
                Some("2026-01"),
                None,
                Some("2026-02"),
                Some("2026-03"),
                Some("2027-01"),
            ],
        ),
        Column::new(
            "wiki".into(),
            vec![
                Some("testwiki"),
                Some("nullmonth"),
                None,
                Some("nswiki"),
                Some("futurewiki"),
            ],
        ),
        Column::new(
            "page_namespace".into(),
            vec![Some(0_i32), Some(0), Some(0), Some(1), Some(0)],
        ),
        Column::new(
            "user_type".into(),
            vec![
                Some("registered"),
                Some("registered"),
                Some("registered"),
                Some("anonymous"),
                Some("registered"),
            ],
        ),
        Column::new("total_patrols".into(), vec![1_i64, 2, 3, 4, 5]),
        Column::new("unique_patrollers".into(), vec![1_i32, 2, 3, 4, 5]),
        Column::new("patrol_new_pages".into(), vec![1_i64, 2, 3, 4, 5]),
        Column::new("patrol_diffs".into(), vec![0_i64, 0, 0, 0, 0]),
        Column::new(
            "median_latency_hours".into(),
            vec![None, Some(1.0), None, Some(2.0), Some(3.0)],
        ),
        Column::new(
            "p90_latency_hours".into(),
            vec![None, Some(2.0), None, Some(4.0), Some(6.0)],
        ),
        Column::new("patrolled_revisions".into(), vec![1_i64, 2, 3, 4, 5]),
        Column::new("autopatrolled_revisions".into(), vec![0_i64, 0, 1, 0, 0]),
        Column::new("total_revisions".into(), vec![1_i64, 2, 3, 4, 5]),
        Column::new(
            "patrol_coverage_pct".into(),
            vec![None, Some(50.0), None, Some(75.0), Some(100.0)],
        ),
        Column::new(
            "adjusted_coverage_pct".into(),
            vec![None, Some(50.0), None, Some(75.0), Some(100.0)],
        ),
        Column::new(
            "top1_pct".into(),
            vec![None, Some(50.0), None, Some(75.0), Some(100.0)],
        ),
        Column::new("min_patrollers_50pct".into(), vec![1_i32, 2, 3, 4, 5]),
    ])?;
    let mut file = File::create(&merged_path)?;
    ParquetWriter::new(&mut file).finish(&mut df)?;

    write_defaults_patrol_json(&output_dir, &merged_path)?;

    let defaults = read_json(&output_dir.join("defaults_patrol.json"))?;
    assert_eq!(defaults.get("defaultWiki"), Some(&json!("futurewiki")));
    assert_eq!(defaults.get("maxMonth"), Some(&json!("2027-01")));
    assert!(
        defaults
            .get("wikis")
            .and_then(Value::as_array)
            .expect("wikis should be an array")
            .iter()
            .any(|entry| entry.get("wiki") == Some(&json!("testwiki")))
    );
    Ok(())
}

#[test]
fn write_defaults_patrol_json_handles_empty_merged_metric() -> Result<()> {
    let temp_dir = TestDir::new()?;
    let output_dir = temp_dir.path().join("output");
    fs::create_dir_all(&output_dir)?;
    let merged_path = output_dir.join("patrol.parquet");

    let mut df = DataFrame::new(
        0,
        vec![
            Column::new_empty("year_month".into(), &DataType::String),
            Column::new_empty("wiki".into(), &DataType::String),
            Column::new_empty("page_namespace".into(), &DataType::Int32),
            Column::new_empty("user_type".into(), &DataType::String),
            Column::new_empty("total_patrols".into(), &DataType::Int64),
            Column::new_empty("unique_patrollers".into(), &DataType::Int32),
            Column::new_empty("patrol_new_pages".into(), &DataType::Int64),
            Column::new_empty("patrol_diffs".into(), &DataType::Int64),
            Column::new_empty("median_latency_hours".into(), &DataType::Float64),
            Column::new_empty("p90_latency_hours".into(), &DataType::Float64),
            Column::new_empty("patrolled_revisions".into(), &DataType::Int64),
            Column::new_empty("autopatrolled_revisions".into(), &DataType::Int64),
            Column::new_empty("total_revisions".into(), &DataType::Int64),
            Column::new_empty("patrol_coverage_pct".into(), &DataType::Float64),
            Column::new_empty("adjusted_coverage_pct".into(), &DataType::Float64),
            Column::new_empty("top1_pct".into(), &DataType::Float64),
            Column::new_empty("min_patrollers_50pct".into(), &DataType::Int32),
        ],
    )?;
    let mut file = File::create(&merged_path)?;
    ParquetWriter::new(&mut file).finish(&mut df)?;

    write_defaults_patrol_json(&output_dir, &merged_path)?;

    let defaults = read_json(&output_dir.join("defaults_patrol.json"))?;
    assert_eq!(defaults.get("defaultWiki"), Some(&Value::Null));
    assert_eq!(defaults.get("maxMonth"), Some(&Value::Null));
    assert_eq!(defaults.get("wikis"), Some(&json!([])));
    assert_eq!(defaults.get("rangeByWiki"), Some(&json!([])));
    assert_eq!(defaults.get("patrol"), Some(&json!([])));
    Ok(())
}

#[test]
fn aggregate_defaults_row_average_returns_none_for_zero_count() {
    let row = AggregateDefaultsRow::default();
    assert_eq!(row.average(10.0, 0), None);
    assert_eq!(row.average(10.0, 4), Some(2.5));
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
    assert!(output_dir.join("defaults_patrol.json").exists());

    compute_patrol("testwiki", &data_dir, &output_dir, false, None)?;
    Ok(())
}
