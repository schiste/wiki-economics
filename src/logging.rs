//! Typed, streaming decoder for MediaWiki pages-logging XML dumps.

use anyhow::{Context, Result};
use flate2::read::MultiGzDecoder;
use quick_xml::Reader;
use quick_xml::events::Event;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::sync::OnceLock;

#[derive(Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub(super) struct PatrolEvent {
    pub(super) timestamp: String,
    pub(super) log_id: i64,
    pub(super) user: Option<String>,
    pub(super) user_id: Option<i64>,
    pub(super) page_title: Option<String>,
    pub(super) current_revision_id: i64,
    pub(super) prev_revision_id: i64,
    pub(super) is_auto: bool,
}

#[derive(Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub(super) struct RightsEvent {
    pub(super) timestamp: String,
    pub(super) target_user: String,
    pub(super) old_groups: String,
    pub(super) new_groups: String,
}

#[derive(Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub(super) struct AccountCreationEvent {
    pub(super) log_id: Option<i64>,
    pub(super) timestamp: String,
    pub(super) target_user_id: Option<i64>,
    pub(super) target_user: Option<String>,
    pub(super) is_temporary: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(super) struct BlockEvent {
    pub(super) action: String,
    pub(super) log_id: Option<i64>,
    pub(super) timestamp: String,
    pub(super) log_title: Option<String>,
    pub(super) params: Option<String>,
}

#[derive(Debug, Eq, PartialEq)]
pub(super) enum LoggingEvent {
    Patrol(PatrolEvent),
    Rights(RightsEvent),
    Block(BlockEvent),
    AccountCreation(AccountCreationEvent),
    Other {
        log_type: Option<String>,
        log_action: Option<String>,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct StreamSummary {
    pub(super) compressed_bytes: u64,
    pub(super) total_log_items: usize,
}

#[derive(Default)]
struct RawLogItem {
    log_type: Option<String>,
    log_action: Option<String>,
    log_id: Option<i64>,
    timestamp: Option<String>,
    contributor_name: Option<String>,
    contributor_id: Option<i64>,
    log_title: Option<String>,
    params: Option<String>,
}

/// Decode every gzip member and emit one typed event for each `<logitem>`.
///
/// The callback is deliberately synchronous: callers can update bounded
/// accumulators or Parquet writers without materializing the logging dump.
pub(super) fn stream_file(
    xml_path: &Path,
    mut consume: impl FnMut(LoggingEvent) -> Result<()>,
) -> Result<StreamSummary> {
    let file = File::open(xml_path)?;
    let compressed_bytes = file.metadata()?.len();
    crate::storage::prepare_sequential_read(&file);
    let decoder = MultiGzDecoder::new(BufReader::new(file.try_clone()?));
    let mut reader = Reader::from_reader(BufReader::new(decoder));
    reader.config_mut().trim_text(true);

    let mut buffer = Vec::new();
    let mut current = None::<RawLogItem>;
    let mut current_tag = None::<String>;
    let mut in_contributor = false;
    let mut total_log_items = 0_usize;
    loop {
        match reader.read_event_into(&mut buffer) {
            Ok(Event::Start(event)) => {
                let tag = String::from_utf8_lossy(event.local_name().as_ref()).to_string();
                match tag.as_str() {
                    "logitem" => current = Some(RawLogItem::default()),
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
                            total_log_items = total_log_items
                                .checked_add(1)
                                .context("logging item count overflow")?;
                            consume(item.into_event())?;
                        }
                        current_tag = None;
                    }
                    _ => current_tag = None,
                }
            }
            Ok(Event::Text(text)) => apply_decoded_text(
                current.as_mut(),
                current_tag.as_deref(),
                in_contributor,
                text.decode()?.into_owned(),
            ),
            Ok(Event::CData(text)) => apply_decoded_text(
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
    Ok(StreamSummary {
        compressed_bytes,
        total_log_items,
    })
}

fn apply_decoded_text(
    item: Option<&mut RawLogItem>,
    tag: Option<&str>,
    in_contributor: bool,
    value: String,
) {
    let (Some(item), Some(tag)) = (item, tag) else {
        return;
    };
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

impl RawLogItem {
    fn into_event(self) -> LoggingEvent {
        match self.log_type.as_deref() {
            Some("patrol") => LoggingEvent::Patrol(self.into_patrol()),
            Some("rights") => LoggingEvent::Rights(self.into_rights()),
            Some("block") => LoggingEvent::Block(self.into_block()),
            Some("newusers") => LoggingEvent::AccountCreation(self.into_account_creation()),
            _ => LoggingEvent::Other {
                log_type: self.log_type,
                log_action: self.log_action,
            },
        }
    }

    fn into_patrol(self) -> PatrolEvent {
        let (current_revision_id, prev_revision_id, is_auto) =
            parse_patrol_params(self.params.as_deref().unwrap_or_default());
        PatrolEvent {
            log_id: self.log_id.unwrap_or_default(),
            timestamp: self.timestamp.unwrap_or_default(),
            user: self.contributor_name,
            user_id: self.contributor_id,
            page_title: self.log_title,
            current_revision_id,
            prev_revision_id,
            is_auto,
        }
    }

    fn into_rights(self) -> RightsEvent {
        let log_title = self.log_title.unwrap_or_default();
        let target_user = log_title
            .split_once(':')
            .map(|(_, rest)| rest.to_string())
            .unwrap_or(log_title);
        let (old_groups, new_groups) =
            parse_rights_params(self.params.as_deref().unwrap_or_default());
        RightsEvent {
            timestamp: self.timestamp.unwrap_or_default(),
            target_user,
            old_groups,
            new_groups,
        }
    }

    fn into_block(self) -> BlockEvent {
        BlockEvent {
            action: self.log_action.unwrap_or_default(),
            log_id: self.log_id,
            timestamp: self.timestamp.unwrap_or_default(),
            log_title: self.log_title,
            params: self.params,
        }
    }

    fn into_account_creation(self) -> AccountCreationEvent {
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
        let target_user = self.log_title.and_then(|title| {
            let user = title
                .split_once(':')
                .map_or(title.as_str(), |(_, user)| user)
                .trim();
            (!user.is_empty()).then(|| user.to_string())
        });
        let is_temporary = target_user
            .as_deref()
            .is_some_and(|user| user.starts_with('~'));
        AccountCreationEvent {
            log_id: self.log_id.filter(|log_id| *log_id > 0),
            timestamp: self.timestamp.unwrap_or_default(),
            target_user_id,
            target_user,
            is_temporary,
        }
    }
}

pub(super) fn normalize_timestamp(timestamp: &str) -> String {
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

pub(super) fn parse_new_user_id(params: &str) -> Option<i64> {
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

fn patrol_param_regex() -> &'static Regex {
    static REGEX: OnceLock<Regex> = OnceLock::new();
    REGEX.get_or_init(|| {
        Regex::new(r#""(?P<field>[^"]+)";(?:(?:s:\d+:"(?P<str>[^"]*)")|(?:i:(?P<int>\d+)))"#)
            .expect("valid patrol param regex")
    })
}

pub(super) fn parse_patrol_params(params: &str) -> (i64, i64, bool) {
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
            let string_value = captures.name("str").map(|value| value.as_str());
            let int_value = captures
                .name("int")
                .and_then(|value| value.as_str().parse::<i64>().ok());
            match field {
                "4::curid" => {
                    current_revision_id = string_value
                        .and_then(|value| value.parse::<i64>().ok())
                        .or(int_value)
                        .unwrap_or_default();
                }
                "5::previd" => {
                    prev_revision_id = string_value
                        .and_then(|value| value.parse::<i64>().ok())
                        .or(int_value)
                        .unwrap_or_default();
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
    let is_auto = lines.next().is_some_and(|value| value.trim() == "1");
    (current_revision_id, prev_revision_id, is_auto)
}

fn rights_group_regex() -> &'static Regex {
    static REGEX: OnceLock<Regex> = OnceLock::new();
    REGEX.get_or_init(|| Regex::new(r#"s:\d+:"([^"]+)""#).expect("valid rights regex"))
}

pub(super) fn parse_rights_params(params: &str) -> (String, String) {
    if params.trim().is_empty() {
        return (String::new(), String::new());
    }
    if params.contains("a:") {
        return (
            extract_php_groups(params, "4::oldgroups").join(","),
            extract_php_groups(params, "5::newgroups").join(","),
        );
    }
    let mut lines = params.lines();
    (
        lines.next().unwrap_or_default().trim().to_string(),
        lines.next().unwrap_or_default().trim().to_string(),
    )
}

pub(super) fn extract_php_groups(params: &str, key: &str) -> Vec<String> {
    let marker = format!(r#""{key}";"#);
    let Some(start) = params.find(&marker) else {
        return Vec::new();
    };
    let slice = &params[start + marker.len()..];
    let Some(body) = extract_php_array_body(slice) else {
        return Vec::new();
    };
    let mut values = rights_group_regex()
        .captures_iter(body)
        .filter_map(|capture| capture.get(1).map(|value| value.as_str()))
        .filter(|value| !(value.chars().all(|ch| ch.is_ascii_digit()) && value.len() == 14))
        .map(str::to_string)
        .collect::<Vec<_>>();
    values.sort();
    values.dedup();
    values
}

pub(super) fn extract_php_array_body(value: &str) -> Option<&str> {
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
