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
    pub mode: CorpusMode,
    pub channel_allow_regex: Regex,
    pub channel_opt_in: HashSet<String>,
    pub exclude_bot_authors: bool,
    pub opt_out_people: HashSet<String>,
    pub allowed_folder_ids: HashSet<String>,
    pub broad_visibility_threshold: SharingThreshold,
    pub excluded_file_ids: HashSet<String>,
    pub exclude_form_response_sheets: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CorpusMode {
    WorkspaceFiltered,
    PersonalAllText,
}

impl Default for CorpusConfig {
    fn default() -> Self {
        Self {
            mode: CorpusMode::WorkspaceFiltered,
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

    pub fn personal_all_text_config() -> Self {
        Self::new(CorpusConfig {
            mode: CorpusMode::PersonalAllText,
            ..CorpusConfig::default()
        })
    }

    pub fn project_observations(&self, observations: &[Observation]) -> Vec<CorpusRecord> {
        match self.config.mode {
            CorpusMode::WorkspaceFiltered => self.project_workspace_filtered(observations),
            CorpusMode::PersonalAllText => self.project_personal_all_text(observations),
        }
    }

    fn project_workspace_filtered(&self, observations: &[Observation]) -> Vec<CorpusRecord> {
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

    fn project_personal_all_text(&self, observations: &[Observation]) -> Vec<CorpusRecord> {
        let mut records = observations
            .iter()
            .filter_map(personal_record)
            .collect::<Vec<_>>();
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
        if self.config.allowed_folder_ids.is_empty() {
            return false;
        }
        let Some(parent_ids) = observation
            .payload
            .pointer("/metadata/parentIds")
            .and_then(serde_json::Value::as_array)
        else {
            return false;
        };
        let parent_allowed = parent_ids
            .iter()
            .filter_map(serde_json::Value::as_str)
            .any(|parent| self.config.allowed_folder_ids.contains(parent));
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

fn personal_record(observation: &Observation) -> Option<CorpusRecord> {
    let source_type = personal_source_type(observation);
    let text = personal_text(observation, &source_type)?;
    if text.trim().is_empty() {
        return None;
    }
    let thread_key = personal_thread_key(observation, &source_type);
    let metadata = personal_metadata(observation, &source_type, thread_key.as_deref());

    Some(record(
        format!("corpus:{source_type}:{}", observation.id.as_str()),
        &source_type,
        personal_anchor_url(observation, &source_type),
        personal_title(observation, &source_type),
        personal_location(observation, &source_type),
        observation.published,
        text,
        thread_key,
        personal_container(observation, &source_type),
        metadata,
    ))
}

fn personal_source_type(observation: &Observation) -> String {
    let source_system = observation
        .source_system
        .as_ref()
        .map(|source| source.as_str());
    let schema = observation.schema.as_str();

    if matches!(source_system, Some("sys:claude-ai")) || schema == "schema:claude-message" {
        return "claude-ai".to_owned();
    }
    if matches!(source_system, Some("sys:github")) || schema == "schema:github-event" {
        return github_source_type(&observation.payload).to_owned();
    }
    if matches!(source_system, Some("sys:claude-code")) || schema.contains("claude-code") {
        return "claude-code".to_owned();
    }
    if matches!(source_system, Some("sys:codex")) || schema.contains("codex") {
        return "codex".to_owned();
    }

    source_system
        .and_then(|source| source.strip_prefix("sys:"))
        .map(str::to_owned)
        .unwrap_or_else(|| schema.strip_prefix("schema:").unwrap_or(schema).to_owned())
}

fn github_source_type(payload: &serde_json::Value) -> &'static str {
    match string_at(payload, &["object_type"]).unwrap_or("") {
        "issue" => "github-issue",
        "pull_request" => "github-pr",
        "issue_comment" | "pull_request_review" | "pull_request_review_comment" => "github-comment",
        "commit" => "github-commit",
        _ => "github-event",
    }
}

fn personal_text(observation: &Observation, source_type: &str) -> Option<String> {
    match source_type {
        "claude-ai" => string_at(&observation.payload, &["text"]).map(str::to_owned),
        "github-issue" | "github-pr" | "github-comment" | "github-commit" | "github-event" => {
            github_text(&observation.payload)
        }
        "claude-code" | "codex" => coding_agent_text(&observation.payload),
        _ => {
            let text = value_text(&observation.payload);
            (!text.trim().is_empty()).then_some(text)
        }
    }
}

fn github_text(payload: &serde_json::Value) -> Option<String> {
    let parts = match string_at(payload, &["object_type"]).unwrap_or("") {
        "issue" | "pull_request" => vec![
            string_at(payload, &["title"]).unwrap_or(""),
            string_at(payload, &["body"]).unwrap_or(""),
        ],
        "issue_comment" | "pull_request_review_comment" => {
            vec![string_at(payload, &["body"]).unwrap_or("")]
        }
        "pull_request_review" => vec![
            string_at(payload, &["state"]).unwrap_or(""),
            string_at(payload, &["body"]).unwrap_or(""),
        ],
        "commit" => vec![string_at(payload, &["message"]).unwrap_or("")],
        _ => return Some(value_text(payload)),
    };
    let text = parts
        .into_iter()
        .filter(|part| !part.trim().is_empty())
        .collect::<Vec<_>>()
        .join("\n");
    (!text.trim().is_empty()).then_some(text)
}

fn coding_agent_text(payload: &serde_json::Value) -> Option<String> {
    let mut parts = Vec::new();
    for path in [
        &["text"][..],
        &["content"][..],
        &["message"][..],
        &["tool", "name"][..],
        &["tool_name"][..],
        &["target"][..],
        &["target_ref"][..],
        &["path"][..],
        &["pattern"][..],
    ] {
        if let Some(value) = string_at(payload, path)
            && !value.trim().is_empty()
        {
            parts.push(value.to_owned());
        }
    }
    if let Some(values) = payload
        .pointer("/target_refs")
        .and_then(serde_json::Value::as_array)
    {
        parts.extend(
            values
                .iter()
                .filter_map(serde_json::Value::as_str)
                .filter(|value| !value.trim().is_empty())
                .map(ToOwned::to_owned),
        );
    }
    let text = parts.join("\n");
    (!text.trim().is_empty()).then_some(text)
}

fn personal_anchor_url(observation: &Observation, source_type: &str) -> String {
    if let Some(url) = string_at(&observation.payload, &["html_url"])
        .or_else(|| string_at(&observation.payload, &["url"]))
        .or_else(|| string_at(&observation.payload, &["permalink"]))
        .or_else(|| string_at(&observation.payload, &["artifact", "canonicalUri"]))
    {
        return url.to_owned();
    }

    match source_type {
        "claude-ai" => {
            let conversation = string_at(&observation.payload, &["conversation_uuid"])
                .unwrap_or(observation.id.as_str());
            let message = string_at(&observation.payload, &["message_uuid"]).unwrap_or("");
            format!("claude-ai://conversation/{conversation}/message/{message}")
        }
        "github-issue" | "github-pr" | "github-comment" | "github-commit" | "github-event" => {
            let repo = string_at(&observation.payload, &["repo"]).unwrap_or("unknown");
            let object_type = string_at(&observation.payload, &["object_type"]).unwrap_or("event");
            let object_id = string_at(&observation.payload, &["sha"])
                .map(str::to_owned)
                .or_else(|| number_like(&observation.payload, "number"))
                .or_else(|| number_like(&observation.payload, "id"))
                .or_else(|| string_at(&observation.payload, &["event_key"]).map(str::to_owned))
                .unwrap_or_else(|| observation.id.as_str().to_owned());
            format!("github://{repo}/{object_type}/{object_id}")
        }
        "claude-code" | "codex" => {
            let session = coding_session_id(&observation.payload, &observation.meta)
                .unwrap_or_else(|| observation.id.as_str().to_owned());
            let message = coding_message_id(&observation.payload, &observation.meta)
                .unwrap_or_else(|| observation.id.as_str().to_owned());
            format!("{source_type}://session/{session}/message/{message}")
        }
        _ => format!("observation://{}", observation.id.as_str()),
    }
}

fn personal_title(observation: &Observation, source_type: &str) -> String {
    match source_type {
        "claude-ai" => format!(
            "claude.ai conversation {}",
            string_at(&observation.payload, &["conversation_uuid"]).unwrap_or("unknown")
        ),
        "github-issue" | "github-pr" => string_at(&observation.payload, &["title"])
            .unwrap_or("GitHub item")
            .to_owned(),
        "github-comment" => format!(
            "GitHub comment in {}",
            string_at(&observation.payload, &["repo"]).unwrap_or("unknown")
        ),
        "github-commit" => format!(
            "GitHub commit {}",
            string_at(&observation.payload, &["sha"]).unwrap_or("unknown")
        ),
        "claude-code" | "codex" => format!(
            "{source_type} session {}",
            coding_session_id(&observation.payload, &observation.meta)
                .unwrap_or_else(|| "unknown".to_owned())
        ),
        _ => title(observation),
    }
}

fn personal_location(observation: &Observation, source_type: &str) -> Option<String> {
    match source_type {
        "claude-ai" => string_at(&observation.payload, &["sender"]).map(str::to_owned),
        "github-issue" | "github-pr" | "github-comment" | "github-commit" | "github-event" => {
            string_at(&observation.payload, &["object_type"]).map(str::to_owned)
        }
        "claude-code" | "codex" => string_at(&observation.payload, &["role"])
            .or_else(|| string_at(&observation.payload, &["sender"]))
            .map(str::to_owned),
        _ => None,
    }
}

fn personal_container(observation: &Observation, source_type: &str) -> Option<String> {
    match source_type {
        "github-issue" | "github-pr" | "github-comment" | "github-commit" | "github-event" => {
            string_at(&observation.payload, &["repo"]).map(str::to_owned)
        }
        "claude-ai" => string_at(&observation.payload, &["conversation_uuid"]).map(str::to_owned),
        "claude-code" | "codex" => coding_session_id(&observation.payload, &observation.meta),
        _ => string_at(&observation.payload, &["artifact", "containerId"]).map(str::to_owned),
    }
}

fn personal_thread_key(observation: &Observation, source_type: &str) -> Option<String> {
    match source_type {
        "claude-ai" => string_at(&observation.payload, &["conversation_uuid"])
            .map(|conversation| format!("claude-ai:conversation:{conversation}")),
        "claude-code" | "codex" => {
            let session = coding_session_id(&observation.payload, &observation.meta)?;
            let root = coding_parent_session_id(&observation.payload, &observation.meta)
                .unwrap_or_else(|| session.clone());
            Some(format!("{source_type}:session:{root}"))
        }
        _ => None,
    }
}

fn personal_metadata(
    observation: &Observation,
    source_type: &str,
    thread_key: Option<&str>,
) -> serde_json::Value {
    let mut metadata = serde_json::Map::new();
    metadata.insert(
        "observation_id".to_owned(),
        serde_json::Value::String(observation.id.as_str().to_owned()),
    );
    metadata.insert(
        "schema".to_owned(),
        serde_json::Value::String(observation.schema.as_str().to_owned()),
    );
    metadata.insert(
        "source_type".to_owned(),
        serde_json::Value::String(source_type.to_owned()),
    );
    if let Some(source_system) = &observation.source_system {
        metadata.insert(
            "source_system".to_owned(),
            serde_json::Value::String(source_system.as_str().to_owned()),
        );
    }
    if let Some(thread_key) = thread_key {
        metadata.insert(
            "thread_key".to_owned(),
            serde_json::Value::String(thread_key.to_owned()),
        );
    }
    if let Some(object_type) = string_at(&observation.payload, &["object_type"]) {
        metadata.insert(
            "object_type".to_owned(),
            serde_json::Value::String(object_type.to_owned()),
        );
    }
    if let Some(repo) = string_at(&observation.payload, &["repo"]) {
        metadata.insert(
            "repo".to_owned(),
            serde_json::Value::String(repo.to_owned()),
        );
    }
    if matches!(source_type, "claude-code" | "codex") {
        if let Some(session_id) = coding_session_id(&observation.payload, &observation.meta) {
            metadata.insert(
                "session_id".to_owned(),
                serde_json::Value::String(session_id),
            );
        }
        if let Some(parent_session_id) =
            coding_parent_session_id(&observation.payload, &observation.meta)
        {
            metadata.insert(
                "parent_session_id".to_owned(),
                serde_json::Value::String(parent_session_id),
            );
        }
        metadata.insert(
            "is_sidechain".to_owned(),
            serde_json::Value::Bool(coding_is_sidechain(&observation.payload, &observation.meta)),
        );
        if let Some(message_id) = coding_message_id(&observation.payload, &observation.meta) {
            metadata.insert(
                "message_id".to_owned(),
                serde_json::Value::String(message_id),
            );
        }
        if let Some(parent_message_id) =
            string_owned_at(&observation.payload, &["parent_message_uuid"])
                .or_else(|| string_owned_at(&observation.payload, &["parent_message_id"]))
        {
            metadata.insert(
                "parent_message_id".to_owned(),
                serde_json::Value::String(parent_message_id),
            );
        }
    }
    serde_json::Value::Object(metadata)
}

fn number_like(payload: &serde_json::Value, field: &str) -> Option<String> {
    payload.get(field).and_then(|value| {
        value
            .as_i64()
            .map(|value| value.to_string())
            .or_else(|| value.as_str().map(str::to_owned))
    })
}

fn coding_session_id(payload: &serde_json::Value, meta: &serde_json::Value) -> Option<String> {
    first_string_owned(
        payload,
        meta,
        &[
            &["session_id"][..],
            &["sessionId"][..],
            &["session", "id"][..],
            &["session", "session_id"][..],
        ],
    )
}

fn coding_parent_session_id(
    payload: &serde_json::Value,
    meta: &serde_json::Value,
) -> Option<String> {
    first_string_owned(
        payload,
        meta,
        &[
            &["parent_session_id"][..],
            &["parentSessionId"][..],
            &["parent_session"][..],
            &["session", "parent_session_id"][..],
            &["sidechain", "parent_session_id"][..],
        ],
    )
}

fn coding_message_id(payload: &serde_json::Value, meta: &serde_json::Value) -> Option<String> {
    first_string_owned(
        payload,
        meta,
        &[
            &["message_uuid"][..],
            &["message_id"][..],
            &["messageId"][..],
            &["uuid"][..],
        ],
    )
}

fn coding_is_sidechain(payload: &serde_json::Value, meta: &serde_json::Value) -> bool {
    bool_at(payload, &["is_sidechain"])
        .or_else(|| bool_at(payload, &["sidechain"]))
        .or_else(|| bool_at(meta, &["is_sidechain"]))
        .unwrap_or_else(|| coding_parent_session_id(payload, meta).is_some())
}

fn first_string_owned(
    payload: &serde_json::Value,
    meta: &serde_json::Value,
    paths: &[&[&str]],
) -> Option<String> {
    paths
        .iter()
        .find_map(|path| string_owned_at(payload, path))
        .or_else(|| paths.iter().find_map(|path| string_owned_at(meta, path)))
}

fn string_owned_at(value: &serde_json::Value, path: &[&str]) -> Option<String> {
    string_at(value, path).map(str::to_owned)
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
        obs_with_source(schema, None, payload)
    }

    fn obs_with_source(
        schema: &str,
        source_system: Option<&str>,
        payload: serde_json::Value,
    ) -> Observation {
        Observation {
            id: Observation::new_id(),
            schema: SchemaRef::new(schema),
            schema_version: SemVer::new("1.0.0"),
            observer: ObserverRef::new("obs:test"),
            source_system: source_system.map(SourceSystemRef::new),
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

    fn drive_obs(file_id: &str, parent_id: &str, sharing_level: &str) -> Observation {
        obs(
            "schema:workspace-object-snapshot",
            serde_json::json!({
                "title": "Drive file",
                "artifact": {
                    "service": "drive",
                    "objectType": "file",
                    "sourceObjectId": file_id,
                    "canonicalUri": "https://drive/file"
                },
                "metadata": {
                    "parentIds": [parent_id],
                    "sharingLevel": sharing_level
                },
                "native": {"text": "drive text"}
            }),
        )
    }

    fn corpus_with_allowed_folder() -> CorpusProjector {
        CorpusProjector::new(CorpusConfig {
            allowed_folder_ids: ["folder-allowed".to_owned()].into_iter().collect(),
            ..CorpusConfig::default()
        })
    }

    #[test]
    fn drive_files_below_sharing_threshold_are_excluded() {
        let projector = corpus_with_allowed_folder();
        let private = drive_obs("file-private", "folder-allowed", "specific-users");
        let domain = drive_obs("file-domain", "folder-allowed", "domain");

        let records = projector.project_observations(&[private, domain]);

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].source_type, "drive");
        assert_eq!(records[0].metadata["sharing_level"], "domain");
    }

    #[test]
    fn drive_files_are_denied_when_allowed_folders_is_empty() {
        let projector = CorpusProjector::default_config();
        let file = drive_obs("file-domain", "folder-allowed", "domain");

        assert!(projector.project_observations(&[file]).is_empty());
    }

    #[test]
    fn drive_files_outside_allowed_folders_are_excluded() {
        let projector = corpus_with_allowed_folder();
        let file = drive_obs("file-domain", "folder-other", "domain");

        assert!(projector.project_observations(&[file]).is_empty());
    }

    #[test]
    fn excluded_drive_file_ids_are_excluded() {
        let projector = CorpusProjector::new(CorpusConfig {
            allowed_folder_ids: ["folder-allowed".to_owned()].into_iter().collect(),
            excluded_file_ids: ["file-denied".to_owned()].into_iter().collect(),
            ..CorpusConfig::default()
        });
        let file = drive_obs("file-denied", "folder-allowed", "domain");

        assert!(projector.project_observations(&[file]).is_empty());
    }

    #[test]
    fn linked_form_response_sheets_are_excluded() {
        let projector = CorpusProjector::default_config();
        let form = obs(
            "schema:workspace-object-snapshot",
            serde_json::json!({
                "title": "Survey",
                "artifact": {
                    "service": "forms",
                    "objectType": "form",
                    "canonicalUri": "https://forms/form"
                },
                "metadata": {"linkedSheetId": "sheet-1"}
            }),
        );
        let sheet = obs(
            "schema:workspace-object-snapshot",
            serde_json::json!({
                "title": "Survey responses",
                "artifact": {
                    "service": "sheets",
                    "objectType": "spreadsheet",
                    "sourceObjectId": "sheet-1",
                    "canonicalUri": "https://sheets/sheet-1"
                },
                "native": {
                    "tabs": [{
                        "name": "Responses",
                        "rows": [{
                            "rowNumber": 2,
                            "cells": [{"header": "Email", "value": "a@example.test"}]
                        }]
                    }]
                }
            }),
        );

        let records = projector.project_observations(&[form, sheet]);

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].source_type, "forms");
    }

    #[test]
    fn direct_form_response_sheet_metadata_is_excluded() {
        let projector = CorpusProjector::default_config();
        let sheet = obs(
            "schema:workspace-object-snapshot",
            serde_json::json!({
                "title": "Survey responses",
                "artifact": {
                    "service": "sheets",
                    "objectType": "spreadsheet",
                    "canonicalUri": "https://sheets/sheet-1"
                },
                "metadata": {"formResponseSheet": true},
                "native": {
                    "tabs": [{
                        "name": "Responses",
                        "rows": [{
                            "rowNumber": 2,
                            "cells": [{"header": "Email", "value": "a@example.test"}]
                        }]
                    }]
                }
            }),
        );

        assert!(projector.project_observations(&[sheet]).is_empty());
    }

    #[test]
    fn opted_out_drive_owner_is_excluded() {
        let projector = CorpusProjector::new(CorpusConfig {
            allowed_folder_ids: ["folder-allowed".to_owned()].into_iter().collect(),
            opt_out_people: ["owner@example.test".to_owned()].into_iter().collect(),
            ..CorpusConfig::default()
        });
        let mut file = drive_obs("file-domain", "folder-allowed", "domain");
        file.payload["relations"] = serde_json::json!({"owner": "owner@example.test"});

        assert!(projector.project_observations(&[file]).is_empty());
    }

    #[test]
    fn docs_chunks_create_heading_records() {
        let projector = CorpusProjector::default_config();
        let doc = obs(
            "schema:workspace-object-snapshot",
            serde_json::json!({
                "title": "Planning Doc",
                "artifact": {
                    "service": "docs",
                    "objectType": "document",
                    "canonicalUri": "https://docs/doc-1",
                    "containerId": "folder-1"
                },
                "native": {
                    "chunks": [
                        {"heading": "Intro", "anchor": "h.intro", "text": "Alpha"},
                        {"heading": "Plan", "text": "Beta"}
                    ]
                }
            }),
        );

        let records = projector.project_observations(&[doc]);

        assert_eq!(records.len(), 2);
        assert_eq!(records[0].source_location.as_deref(), Some("Intro"));
        assert_eq!(records[0].anchor_url, "https://docs/doc-1#h.intro");
        assert_eq!(records[1].source_location.as_deref(), Some("Plan"));
        assert_eq!(records[1].text, "Beta");
    }

    #[test]
    fn sheets_rows_create_row_records() {
        let projector = CorpusProjector::default_config();
        let sheet = obs(
            "schema:workspace-object-snapshot",
            serde_json::json!({
                "title": "Inventory",
                "artifact": {
                    "service": "sheets",
                    "objectType": "spreadsheet",
                    "canonicalUri": "https://sheets/sheet-1"
                },
                "native": {
                    "tabs": [{
                        "name": "Items",
                        "rows": [
                            {
                                "rowNumber": 2,
                                "cells": [
                                    {"header": "Name", "value": "Cable"},
                                    {"header": "Count", "value": "3"}
                                ]
                            },
                            {"rowNumber": 3, "cells": [{}]}
                        ]
                    }]
                }
            }),
        );

        let records = projector.project_observations(&[sheet]);

        assert_eq!(records.len(), 1);
        assert!(records[0].record_id.ends_with(":Items:2"));
        assert_eq!(records[0].source_type, "sheets");
        assert_eq!(records[0].source_location.as_deref(), Some("Items row 2"));
        assert_eq!(records[0].text, "Name: Cable\nCount: 3");
    }

    #[test]
    fn bot_answer_log_schema_never_enters_corpus() {
        let projector = CorpusProjector::default_config();
        let answer = obs(
            "schema:bot-answer-log",
            serde_json::json!({
                "title": "Answer",
                "artifact": {
                    "service": "docs",
                    "objectType": "document",
                    "canonicalUri": "https://docs/answer"
                },
                "native": {"chunks": [{"text": "should stay out"}]}
            }),
        );

        assert!(projector.project_observations(&[answer]).is_empty());
    }

    #[test]
    fn personal_all_text_indexes_personal_lake_source_types() {
        let projector = CorpusProjector::personal_all_text_config();
        let records = projector.project_observations(&[
            obs_with_source(
                "schema:claude-message",
                Some("sys:claude-ai"),
                serde_json::json!({
                    "conversation_uuid": "conv-1",
                    "message_uuid": "msg-1",
                    "sender": "human",
                    "text": "needle claude ai"
                }),
            ),
            obs_with_source(
                "schema:github-event",
                Some("sys:github"),
                serde_json::json!({
                    "object_type": "issue",
                    "repo": "owner/repo",
                    "number": 1,
                    "title": "needle issue",
                    "body": "body"
                }),
            ),
            obs_with_source(
                "schema:github-event",
                Some("sys:github"),
                serde_json::json!({
                    "object_type": "pull_request",
                    "repo": "owner/repo",
                    "number": 2,
                    "title": "needle pr",
                    "body": "body"
                }),
            ),
            obs_with_source(
                "schema:github-event",
                Some("sys:github"),
                serde_json::json!({
                    "object_type": "issue_comment",
                    "repo": "owner/repo",
                    "id": 3,
                    "body": "needle comment"
                }),
            ),
            obs_with_source(
                "schema:github-event",
                Some("sys:github"),
                serde_json::json!({
                    "object_type": "commit",
                    "repo": "owner/repo",
                    "sha": "abc",
                    "message": "needle commit"
                }),
            ),
            obs_with_source(
                "schema:claude-code-message",
                Some("sys:claude-code"),
                serde_json::json!({
                    "session_id": "main-session",
                    "message_uuid": "cc-1",
                    "role": "assistant",
                    "text": "needle claude code"
                }),
            ),
            obs_with_source(
                "schema:codex-message",
                Some("sys:codex"),
                serde_json::json!({
                    "session_id": "codex-session",
                    "message_uuid": "codex-1",
                    "role": "assistant",
                    "text": "needle codex"
                }),
            ),
        ]);

        let source_types = records
            .iter()
            .map(|record| record.source_type.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(
            source_types,
            std::collections::BTreeSet::from([
                "claude-ai",
                "github-issue",
                "github-pr",
                "github-comment",
                "github-commit",
                "claude-code",
                "codex",
            ])
        );
    }

    #[test]
    fn personal_all_text_preserves_coding_agent_sidechain_metadata() {
        let projector = CorpusProjector::personal_all_text_config();
        let records = projector.project_observations(&[obs_with_source(
            "schema:claude-code-message",
            Some("sys:claude-code"),
            serde_json::json!({
                "session_id": "child-session",
                "parent_session_id": "main-session",
                "is_sidechain": true,
                "message_uuid": "child-message",
                "role": "assistant",
                "text": "sidechain finding"
            }),
        )]);

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].source_type, "claude-code");
        assert_eq!(
            records[0].metadata["thread_key"],
            "claude-code:session:main-session"
        );
        assert_eq!(records[0].metadata["session_id"], "child-session");
        assert_eq!(records[0].metadata["parent_session_id"], "main-session");
        assert_eq!(records[0].metadata["is_sidechain"], true);
    }

    #[test]
    fn nfkc_normalizes_fullwidth_digits() {
        assert_eq!(normalized_text("１２３"), "123");
    }
}
