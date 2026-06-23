//! Access-controlled corpus projection for workspace search.

use std::collections::HashSet;

use chrono::{DateTime, Utc};
use lethe_core::domain::{Observation, ProjectionRef};
use lethe_engine::projection::runner::Projector;
use regex::Regex;
use serde::{Deserialize, Serialize};
use unicode_normalization::UnicodeNormalization;

pub const CORPUS_PROJECTION_ID: &str = "proj:corpus";

#[derive(Debug, Clone)]
pub struct CorpusConfig {
    pub channel_allow_regex: Regex,
    pub channel_opt_in: HashSet<String>,
    pub exclude_bot_authors: bool,
    pub opt_out_people: HashSet<String>,
    pub allowed_folder_ids: HashSet<String>,
    pub broad_visibility_threshold: SharingThreshold,
    pub excluded_file_ids: HashSet<String>,
    pub exclude_form_response_sheets: bool,
}

impl Default for CorpusConfig {
    fn default() -> Self {
        Self {
            channel_allow_regex: Regex::new(r"^\d{3}_").expect("valid default channel regex"),
            channel_opt_in: HashSet::new(),
            exclude_bot_authors: true,
            opt_out_people: HashSet::new(),
            allowed_folder_ids: HashSet::new(),
            broad_visibility_threshold: SharingThreshold::Domain,
            excluded_file_ids: HashSet::new(),
            exclude_form_response_sheets: true,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum SharingThreshold {
    SpecificUsers,
    Domain,
    AnyoneWithLink,
    Public,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CorpusRecord {
    pub record_id: String,
    pub source_type: String,
    pub anchor_url: String,
    pub source_title: String,
    pub source_location: Option<String>,
    pub timestamp: DateTime<Utc>,
    pub text: String,
    pub normalized_text: String,
    #[serde(default)]
    pub thread_ts: Option<String>,
    #[serde(default)]
    pub container: Option<String>,
    #[serde(default)]
    pub metadata: serde_json::Value,
}

#[derive(Debug, Clone)]
pub struct CorpusProjector {
    config: CorpusConfig,
}

impl CorpusProjector {
    pub fn new(config: CorpusConfig) -> Self {
        Self { config }
    }

    pub fn default_config() -> Self {
        Self::new(CorpusConfig::default())
    }

    pub fn project_observations(&self, observations: &[Observation]) -> Vec<CorpusRecord> {
        let form_response_sheet_ids = observations
            .iter()
            .filter_map(linked_form_sheet_id)
            .collect::<HashSet<_>>();

        let mut records = Vec::new();
        for observation in observations {
            if observation.schema.as_str() == "schema:bot-answer-log" {
                continue;
            }
            match observation.schema.as_str() {
                "schema:slack-message" => {
                    if let Some(record) = self.slack_record(observation) {
                        records.push(record);
                    }
                }
                "schema:workspace-object-snapshot" => {
                    records.extend(self.workspace_records(observation, &form_response_sheet_ids));
                }
                _ => {}
            }
        }
        records.sort_by(|left, right| {
            right
                .timestamp
                .cmp(&left.timestamp)
                .then_with(|| left.record_id.cmp(&right.record_id))
        });
        records
    }

    fn slack_record(&self, observation: &Observation) -> Option<CorpusRecord> {
        let channel = string_at(&observation.payload, &["channel_name"])?;
        let channel_id = string_at(&observation.payload, &["channel_id"]).unwrap_or(channel);
        let is_public = bool_at(&observation.payload, &["is_public_channel"]).unwrap_or(false);
        let opted_in = self.config.channel_opt_in.contains(channel)
            || self.config.channel_opt_in.contains(channel_id);
        if !is_public {
            return None;
        }
        if !opted_in && !self.config.channel_allow_regex.is_match(channel) {
            return None;
        }
        if self.config.exclude_bot_authors
            && (bool_at(&observation.payload, &["is_bot"]).unwrap_or(false)
                || string_at(&observation.payload, &["user_id"])
                    .is_some_and(|id| id.starts_with('B')))
        {
            return None;
        }
        if is_opted_out(
            &self.config.opt_out_people,
            &observation.payload,
            &["user_id", "email", "user_name"],
        ) {
            return None;
        }

        let text = string_at(&observation.payload, &["text"])
            .unwrap_or("")
            .to_owned();
        let ts = string_at(&observation.payload, &["ts"]).unwrap_or("");
        let thread_ts = string_at(&observation.payload, &["thread_ts"]).map(str::to_owned);
        let anchor = string_at(&observation.payload, &["permalink"])
            .map(str::to_owned)
            .unwrap_or_else(|| format!("slack://{channel_id}/{ts}"));
        Some(record(
            format!("corpus:slack:{channel_id}:{ts}"),
            "slack",
            anchor,
            channel.to_owned(),
            Some(format!("#{channel}")),
            observation.published,
            text,
            thread_ts,
            Some(channel.to_owned()),
            serde_json::json!({
                "observation_id": observation.id,
                "channel_id": channel_id,
                "author": string_at(&observation.payload, &["user_name"]),
            }),
        ))
    }

    fn workspace_records(
        &self,
        observation: &Observation,
        form_response_sheet_ids: &HashSet<String>,
    ) -> Vec<CorpusRecord> {
        let Some(service) = string_at(&observation.payload, &["artifact", "service"]) else {
            return vec![];
        };
        let object_type =
            string_at(&observation.payload, &["artifact", "objectType"]).unwrap_or("");
        if service == "forms" && object_type == "form-response-content" {
            return vec![];
        }
        if service == "sheets"
            && self.config.exclude_form_response_sheets
            && (bool_at(&observation.payload, &["metadata", "formResponseSheet"]).unwrap_or(false)
                || string_at(&observation.payload, &["artifact", "sourceObjectId"])
                    .is_some_and(|id| form_response_sheet_ids.contains(id)))
        {
            return vec![];
        }
        if service == "drive" && !self.drive_allowed(observation) {
            return vec![];
        }
        if is_opted_out(
            &self.config.opt_out_people,
            &observation.payload,
            &["relations.owner", "owner", "author"],
        ) {
            return vec![];
        }

        match service {
            "docs" => self.docs_records(observation),
            "sheets" => self.sheet_records(observation),
            "forms" => self.form_records(observation, object_type),
            "slides" => self.slide_records(observation),
            "drive" => self.drive_records(observation),
            _ => vec![],
        }
    }

    fn drive_allowed(&self, observation: &Observation) -> bool {
        let source_id = string_at(&observation.payload, &["artifact", "sourceObjectId"]);
        if source_id.is_some_and(|id| self.config.excluded_file_ids.contains(id)) {
            return false;
        }
        let parent_allowed = observation
            .payload
            .pointer("/metadata/parentIds")
            .and_then(serde_json::Value::as_array)
            .unwrap_or(&Vec::new())
            .iter()
            .filter_map(serde_json::Value::as_str)
            .any(|parent| {
                self.config.allowed_folder_ids.is_empty()
                    || self.config.allowed_folder_ids.contains(parent)
            });
        if !parent_allowed {
            return false;
        }
        let level = string_at(&observation.payload, &["metadata", "sharingLevel"])
            .and_then(parse_sharing_level)
            .unwrap_or(SharingThreshold::SpecificUsers);
        level >= self.config.broad_visibility_threshold
    }

    fn docs_records(&self, observation: &Observation) -> Vec<CorpusRecord> {
        let title = title(observation);
        let base_url = canonical_uri(observation);
        let Some(chunks) = observation
            .payload
            .pointer("/native/chunks")
            .and_then(serde_json::Value::as_array)
        else {
            return vec![];
        };
        chunks
            .iter()
            .enumerate()
            .filter_map(|(idx, chunk)| {
                let text = string_at(chunk, &["text"]).unwrap_or("").to_owned();
                if text.trim().is_empty() {
                    return None;
                }
                let heading = string_at(chunk, &["heading"]).map(str::to_owned);
                let anchor = string_at(chunk, &["anchor"])
                    .map(|anchor| format!("{base_url}#{anchor}"))
                    .unwrap_or_else(|| base_url.clone());
                Some(record(
                    format!("corpus:docs:{}:{idx}", observation.id),
                    "docs",
                    anchor,
                    title.clone(),
                    heading,
                    observation.published,
                    text,
                    None,
                    string_at(&observation.payload, &["artifact", "containerId"])
                        .map(str::to_owned),
                    serde_json::json!({"observation_id": observation.id}),
                ))
            })
            .collect()
    }

    fn sheet_records(&self, observation: &Observation) -> Vec<CorpusRecord> {
        let title = title(observation);
        let base_url = canonical_uri(observation);
        let Some(tabs) = observation
            .payload
            .pointer("/native/tabs")
            .and_then(serde_json::Value::as_array)
        else {
            return vec![];
        };
        let mut records = Vec::new();
        for tab in tabs {
            let tab_name = string_at(tab, &["name"]).unwrap_or("Sheet");
            if let Some(rows) = tab.pointer("/rows").and_then(serde_json::Value::as_array) {
                for row in rows {
                    let row_number = row
                        .get("rowNumber")
                        .and_then(serde_json::Value::as_u64)
                        .unwrap_or(0);
                    let text = cells_to_text(row);
                    if text.trim().is_empty() {
                        continue;
                    }
                    records.push(record(
                        format!("corpus:sheets:{}:{tab_name}:{row_number}", observation.id),
                        "sheets",
                        format!("{base_url}#gid={tab_name}&range={row_number}:{row_number}"),
                        title.clone(),
                        Some(format!("{tab_name} row {row_number}")),
                        observation.published,
                        text,
                        None,
                        Some(tab_name.to_owned()),
                        serde_json::json!({"observation_id": observation.id}),
                    ));
                }
            }
        }
        records
    }

    fn form_records(&self, observation: &Observation, object_type: &str) -> Vec<CorpusRecord> {
        let title = title(observation);
        let base_url = canonical_uri(observation);
        let text = if object_type == "form-response-fact" {
            format!(
                "{} answered at {}",
                string_at(&observation.payload, &["response", "respondent"]).unwrap_or("unknown"),
                string_at(&observation.payload, &["response", "submittedAt"]).unwrap_or("")
            )
        } else {
            let questions = observation
                .payload
                .pointer("/native/questions")
                .and_then(serde_json::Value::as_array)
                .map(|questions| {
                    questions
                        .iter()
                        .filter_map(|q| string_at(q, &["title"]))
                        .collect::<Vec<_>>()
                        .join("\n")
                })
                .unwrap_or_default();
            format!("{}\n{}", title, questions)
        };
        vec![record(
            format!("corpus:forms:{}:{object_type}", observation.id),
            "forms",
            base_url,
            title,
            Some(object_type.to_owned()),
            observation.published,
            text,
            None,
            None,
            serde_json::json!({"observation_id": observation.id}),
        )]
    }

    fn slide_records(&self, observation: &Observation) -> Vec<CorpusRecord> {
        let title = title(observation);
        let url = canonical_uri(observation);
        let text = observation
            .payload
            .pointer("/native/slides")
            .and_then(serde_json::Value::as_array)
            .map(|slides| slides.iter().map(value_text).collect::<Vec<_>>().join("\n"))
            .unwrap_or_else(|| value_text(&observation.payload));
        vec![record(
            format!("corpus:slides:{}", observation.id),
            "slides",
            url,
            title,
            None,
            observation.published,
            text,
            None,
            string_at(&observation.payload, &["artifact", "containerId"]).map(str::to_owned),
            serde_json::json!({"observation_id": observation.id}),
        )]
    }

    fn drive_records(&self, observation: &Observation) -> Vec<CorpusRecord> {
        let title = title(observation);
        let text = string_at(&observation.payload, &["native", "text"])
            .map(str::to_owned)
            .unwrap_or_else(|| title.clone());
        vec![record(
            format!("corpus:drive:{}", observation.id),
            "drive",
            canonical_uri(observation),
            title,
            string_at(&observation.payload, &["artifact", "objectType"]).map(str::to_owned),
            observation.published,
            text,
            None,
            string_at(&observation.payload, &["artifact", "containerId"]).map(str::to_owned),
            serde_json::json!({
                "observation_id": observation.id,
                "sharing_level": string_at(&observation.payload, &["metadata", "sharingLevel"]),
            }),
        )]
    }
}

impl Projector for CorpusProjector {
    type Input = Observation;
    type Output = CorpusRecord;

    fn project(&self, inputs: &[Observation]) -> Vec<CorpusRecord> {
        self.project_observations(inputs)
    }
}

pub fn normalized_text(text: &str) -> String {
    text.nfkc().collect()
}

pub fn projection_watermark(records: &[CorpusRecord]) -> String {
    let mut latest: Option<DateTime<Utc>> = None;
    for record in records {
        latest = Some(latest.map_or(record.timestamp, |current| current.max(record.timestamp)));
    }
    format!(
        "{}:{}",
        ProjectionRef::new(CORPUS_PROJECTION_ID),
        latest
            .map(|ts| ts.to_rfc3339())
            .unwrap_or_else(|| "empty".to_owned())
    )
}

fn record(
    record_id: String,
    source_type: &str,
    anchor_url: String,
    source_title: String,
    source_location: Option<String>,
    timestamp: DateTime<Utc>,
    text: String,
    thread_ts: Option<String>,
    container: Option<String>,
    metadata: serde_json::Value,
) -> CorpusRecord {
    CorpusRecord {
        record_id,
        source_type: source_type.to_owned(),
        anchor_url,
        source_title,
        source_location,
        timestamp,
        normalized_text: normalized_text(&text),
        text,
        thread_ts,
        container,
        metadata,
    }
}

fn title(observation: &Observation) -> String {
    string_at(&observation.payload, &["title"])
        .unwrap_or("Untitled")
        .to_owned()
}

fn canonical_uri(observation: &Observation) -> String {
    string_at(&observation.payload, &["artifact", "canonicalUri"])
        .unwrap_or("")
        .to_owned()
}

fn linked_form_sheet_id(observation: &Observation) -> Option<String> {
    if observation.schema.as_str() == "schema:workspace-object-snapshot"
        && string_at(&observation.payload, &["artifact", "service"]) == Some("forms")
    {
        string_at(&observation.payload, &["metadata", "linkedSheetId"]).map(str::to_owned)
    } else {
        None
    }
}

fn is_opted_out(opt_out: &HashSet<String>, payload: &serde_json::Value, paths: &[&str]) -> bool {
    paths.iter().any(|path| {
        let parts = path.split('.').collect::<Vec<_>>();
        string_at(payload, &parts).is_some_and(|value| opt_out.contains(value))
    })
}

fn string_at<'a>(value: &'a serde_json::Value, path: &[&str]) -> Option<&'a str> {
    let mut current = value;
    for segment in path {
        current = current.get(*segment)?;
    }
    current.as_str()
}

fn bool_at(value: &serde_json::Value, path: &[&str]) -> Option<bool> {
    let mut current = value;
    for segment in path {
        current = current.get(*segment)?;
    }
    current.as_bool()
}

fn parse_sharing_level(value: &str) -> Option<SharingThreshold> {
    match value {
        "specific-users" | "specific_users" => Some(SharingThreshold::SpecificUsers),
        "domain" => Some(SharingThreshold::Domain),
        "anyone-with-link" | "anyone_with_link" => Some(SharingThreshold::AnyoneWithLink),
        "public" => Some(SharingThreshold::Public),
        "private" => Some(SharingThreshold::SpecificUsers),
        _ => None,
    }
}

fn cells_to_text(row: &serde_json::Value) -> String {
    row.pointer("/cells")
        .and_then(serde_json::Value::as_array)
        .map(|cells| {
            cells
                .iter()
                .filter_map(|cell| {
                    let value = string_at(cell, &["value"])?;
                    let header = string_at(cell, &["header"]).unwrap_or("");
                    Some(if header.is_empty() {
                        value.to_owned()
                    } else {
                        format!("{header}: {value}")
                    })
                })
                .collect::<Vec<_>>()
                .join("\n")
        })
        .unwrap_or_default()
}

fn value_text(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(text) => text.clone(),
        serde_json::Value::Array(values) => {
            values.iter().map(value_text).collect::<Vec<_>>().join("\n")
        }
        serde_json::Value::Object(map) => {
            map.values().map(value_text).collect::<Vec<_>>().join("\n")
        }
        _ => String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lethe_core::domain::*;

    fn obs(schema: &str, payload: serde_json::Value) -> Observation {
        Observation {
            id: Observation::new_id(),
            schema: SchemaRef::new(schema),
            schema_version: SemVer::new("1.0.0"),
            observer: ObserverRef::new("obs:test"),
            source_system: None,
            actor: None,
            authority_model: AuthorityModel::LakeAuthoritative,
            capture_model: CaptureModel::Event,
            subject: EntityRef::new("entity:test"),
            target: None,
            payload,
            attachments: vec![],
            published: Utc::now(),
            recorded_at: Utc::now(),
            consent: None,
            idempotency_key: IdempotencyKey::new("test"),
            meta: serde_json::json!({"canonical_json": "{}"}),
        }
    }

    #[test]
    fn slack_filter_rejects_non_matching_channels_and_bots() {
        let projector = CorpusProjector::default_config();
        let hidden = obs(
            "schema:slack-message",
            serde_json::json!({
                "channel_name": "general",
                "channel_id": "C1",
                "is_public_channel": true,
                "is_bot": false,
                "text": "hello",
                "ts": "1.000000"
            }),
        );
        let bot = obs(
            "schema:slack-message",
            serde_json::json!({
                "channel_name": "123_event",
                "channel_id": "C2",
                "is_public_channel": true,
                "is_bot": true,
                "text": "bot",
                "ts": "2.000000"
            }),
        );
        assert!(projector.project_observations(&[hidden, bot]).is_empty());
    }

    #[test]
    fn form_response_content_never_enters_corpus() {
        let projector = CorpusProjector::default_config();
        let content = obs(
            "schema:workspace-object-snapshot",
            serde_json::json!({
                "title": "Survey",
                "artifact": {
                    "service": "forms",
                    "objectType": "form-response-content",
                    "canonicalUri": "https://forms"
                },
                "response": {"answers": {"secret": "x"}}
            }),
        );
        assert!(projector.project_observations(&[content]).is_empty());
    }

    #[test]
    fn nfkc_normalizes_fullwidth_digits() {
        assert_eq!(normalized_text("１２３"), "123");
    }
}
