use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

use chrono::{DateTime, SecondsFormat, TimeZone, Utc};
use lethe_adapter_api::error::AdapterError;
use lethe_adapter_api::idempotency::{
    CANONICAL_JSON_META_KEY, OBJECT_ID_META_KEY, canonical_json, identity_key,
    normalize_canonical_body,
};
use lethe_adapter_api::traits::ObservationDraft;
use lethe_core::domain::{
    AuthorityModel, CaptureModel, EntityRef, ObserverRef, SchemaRef, SemVer, SourceSystemRef,
};
use serde::Deserialize;
use serde_json::Value;

pub const CHATGPT_MESSAGE_SCHEMA: &str = "schema:chatgpt-message";
pub const CHATGPT_MESSAGE_SCHEMA_VERSION: &str = "1.0.0";
const OBSERVER_ID: &str = "obs:chatgpt-importer";
const SOURCE_SYSTEM: &str = "sys:chatgpt";

#[derive(Debug, Clone, Default)]
pub struct ChatGptImportFilter {
    pub from: Option<DateTime<Utc>>,
    pub to: Option<DateTime<Utc>>,
    pub conversation_ids: HashSet<String>,
    pub backfill: bool,
}

#[derive(Debug, Clone, Default)]
pub struct ChatGptImportBatch {
    pub drafts: Vec<ObservationDraft>,
    pub audit: ChatGptImportAudit,
}

#[derive(Debug, Clone, Default)]
pub struct ChatGptImportAudit {
    pub files_read: usize,
    pub conversations_read: usize,
    pub messages_seen: usize,
    pub skipped_records: Vec<ChatGptSkippedRecord>,
}

#[derive(Debug, Clone)]
pub struct ChatGptSkippedRecord {
    pub path: String,
    pub conversation_id: Option<String>,
    pub message_id: Option<String>,
    pub reason: String,
}

#[derive(Debug, Clone)]
pub struct ChatGptImporter {
    adapter_version: SemVer,
}

impl ChatGptImporter {
    pub fn new(adapter_version: SemVer) -> Self {
        Self { adapter_version }
    }

    pub fn import_archive_root(
        &self,
        archive_root: &Path,
        filter: &ChatGptImportFilter,
    ) -> Result<ChatGptImportBatch, AdapterError> {
        let chatgpt_root = archive_root.join("chatgpt");
        if !chatgpt_root.is_dir() {
            return Err(AdapterError::MalformedResponse {
                message: format!(
                    "archive root must contain chatgpt directory: {}",
                    archive_root.display()
                ),
            });
        }
        let mut batch = ChatGptImportBatch::default();
        for path in json_files(&chatgpt_root)? {
            let text = fs::read_to_string(&path).map_err(|error| {
                AdapterError::Other(format!("failed to read {}: {error}", path.display()))
            })?;
            batch.audit.files_read += 1;
            self.import_json_str(&text, &path.display().to_string(), filter, &mut batch)?;
        }
        Ok(batch)
    }

    pub fn import_json_str(
        &self,
        text: &str,
        path: &str,
        filter: &ChatGptImportFilter,
        batch: &mut ChatGptImportBatch,
    ) -> Result<(), AdapterError> {
        let conversations =
            parse_conversations(text).map_err(|error| AdapterError::MalformedResponse {
                message: format!("invalid ChatGPT export {path}: {error}"),
            })?;
        for conversation in conversations {
            if !filter.conversation_ids.is_empty()
                && !filter.conversation_ids.contains(&conversation.id)
            {
                continue;
            }
            batch.audit.conversations_read += 1;
            self.map_conversation(conversation, path, filter, batch);
        }
        Ok(())
    }

    fn map_conversation(
        &self,
        conversation: RawConversation,
        path: &str,
        filter: &ChatGptImportFilter,
        batch: &mut ChatGptImportBatch,
    ) {
        let conversation_id = conversation.id;
        let conversation_title = conversation.title;
        let mut nodes = conversation.mapping.into_values().collect::<Vec<_>>();
        nodes.sort_by(|left, right| left.id.cmp(&right.id));
        for node in nodes {
            let node_id = node.id;
            let parent = node.parent;
            let Some(message) = node.message else {
                continue;
            };
            batch.audit.messages_seen += 1;
            match self.map_message(
                &conversation_id,
                conversation_title.as_deref(),
                &node_id,
                parent.as_deref(),
                message,
                filter,
            ) {
                Ok(Some(draft)) => batch.drafts.push(draft),
                Ok(None) => {}
                Err(reason) => batch.audit.skipped_records.push(ChatGptSkippedRecord {
                    path: path.to_owned(),
                    conversation_id: Some(conversation_id.clone()),
                    message_id: Some(node_id),
                    reason,
                }),
            }
        }
    }

    fn map_message(
        &self,
        conversation_id: &str,
        conversation_title: Option<&str>,
        message_id: &str,
        parent_message_id: Option<&str>,
        message: RawMessage,
        filter: &ChatGptImportFilter,
    ) -> Result<Option<ObservationDraft>, String> {
        let published = parse_chatgpt_time(message.create_time)
            .ok_or_else(|| "message create_time is missing or invalid".to_owned())?;
        if filter.from.is_some_and(|from| published < from) {
            return Ok(None);
        }
        if filter.to.is_some_and(|to| published > to) {
            return Ok(None);
        }
        let sender = message.author.role;
        let text = content_text(&message.content)?;
        if text.trim().is_empty() {
            return Ok(None);
        }
        let canonical_tuple = serde_json::json!({
            "sender": sender,
            "body": normalize_canonical_body(&text),
            "event_time": published.to_rfc3339_opts(SecondsFormat::Micros, true),
            "attachment_sha256": [],
        });
        let canonical_json = canonical_json(&canonical_tuple);
        let object_id = format!("{conversation_id}:{message_id}");
        let idempotency_key = identity_key("chatgpt", &object_id, &canonical_json);
        let mut meta = serde_json::json!({
            "sourceAdapterVersion": self.adapter_version.as_str(),
            OBJECT_ID_META_KEY: object_id,
            CANONICAL_JSON_META_KEY: canonical_json,
            "conversation_id": conversation_id,
            "message_id": message_id,
        });
        if filter.backfill {
            meta["backfill"] = serde_json::Value::Bool(true);
        }

        Ok(Some(ObservationDraft {
            schema: SchemaRef::new(CHATGPT_MESSAGE_SCHEMA),
            schema_version: SemVer::new(CHATGPT_MESSAGE_SCHEMA_VERSION),
            observer: ObserverRef::new(OBSERVER_ID),
            source_system: Some(SourceSystemRef::new(SOURCE_SYSTEM)),
            authority_model: AuthorityModel::LakeAuthoritative,
            capture_model: CaptureModel::Event,
            subject: EntityRef::new(format!("message:chatgpt:{message_id}")),
            target: None,
            payload: serde_json::json!({
                "conversation_id": conversation_id,
                "conversation_title": conversation_title,
                "message_id": message_id,
                "parent_message_id": parent_message_id,
                "sender": sender,
                "text": text,
            }),
            attachments: vec![],
            published,
            idempotency_key,
            meta,
        }))
    }
}

#[derive(Debug, Deserialize)]
struct RawConversation {
    id: String,
    #[serde(default)]
    title: Option<String>,
    mapping: std::collections::HashMap<String, RawNode>,
}

#[derive(Debug, Deserialize)]
struct RawNode {
    id: String,
    #[serde(default)]
    parent: Option<String>,
    #[serde(default)]
    message: Option<RawMessage>,
}

#[derive(Debug, Deserialize)]
struct RawMessage {
    author: RawAuthor,
    content: RawContent,
    #[serde(default)]
    create_time: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct RawAuthor {
    role: String,
}

#[derive(Debug, Deserialize)]
struct RawContent {
    content_type: String,
    #[serde(default)]
    parts: Vec<Value>,
}

fn parse_conversations(text: &str) -> serde_json::Result<Vec<RawConversation>> {
    let value = serde_json::from_str::<Value>(text)?;
    if value.get("mapping").is_some() {
        serde_json::from_value::<RawConversation>(value).map(|conversation| vec![conversation])
    } else {
        serde_json::from_value::<Vec<RawConversation>>(value)
    }
}

fn content_text(content: &RawContent) -> Result<String, String> {
    match content.content_type.as_str() {
        "text" | "multimodal_text" => {
            let mut parts = Vec::new();
            for part in &content.parts {
                match part {
                    Value::String(text) => parts.push(text.clone()),
                    Value::Object(object) => {
                        if let Some(text) = object.get("text").and_then(Value::as_str) {
                            parts.push(text.to_owned());
                        }
                    }
                    Value::Null => {}
                    _ => return Err("message content part has unsupported shape".to_owned()),
                }
            }
            Ok(parts.join("\n"))
        }
        other => Err(format!("unsupported content_type {other}")),
    }
}

fn parse_chatgpt_time(value: Option<f64>) -> Option<DateTime<Utc>> {
    let seconds = value?;
    let whole = seconds.trunc() as i64;
    let nanos = ((seconds.fract()) * 1_000_000_000_f64).round() as u32;
    Utc.timestamp_opt(whole, nanos).single()
}

fn json_files(root: &Path) -> Result<Vec<PathBuf>, AdapterError> {
    let mut files = Vec::new();
    collect_json_files(root, &mut files)?;
    files.sort();
    Ok(files)
}

fn collect_json_files(path: &Path, files: &mut Vec<PathBuf>) -> Result<(), AdapterError> {
    for entry in fs::read_dir(path).map_err(|error| {
        AdapterError::Other(format!("failed to read {}: {error}", path.display()))
    })? {
        let entry = entry.map_err(|error| {
            AdapterError::Other(format!(
                "failed to read directory entry in {}: {error}",
                path.display()
            ))
        })?;
        let entry_path = entry.path();
        if entry_path.is_dir() {
            collect_json_files(&entry_path, files)?;
        } else if entry_path.extension().and_then(|ext| ext.to_str()) == Some("json") {
            files.push(entry_path);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture() -> String {
        serde_json::json!([{
            "id": "conv-1",
            "title": "Fixture",
            "mapping": {
                "msg-1": {
                    "id": "msg-1",
                    "parent": null,
                    "message": {
                        "author": { "role": "user" },
                        "content": { "content_type": "text", "parts": ["hello"] },
                        "create_time": 1780000000.0
                    }
                },
                "bad": {
                    "id": "bad",
                    "parent": "msg-1",
                    "message": {
                        "author": { "role": "assistant" },
                        "content": { "content_type": "unknown", "parts": [] },
                        "create_time": 1780000001.0
                    }
                }
            }
        }])
        .to_string()
    }

    #[test]
    fn parses_fixture_and_quarantines_bad_records() {
        let importer = ChatGptImporter::new(SemVer::new("1.0.0"));
        let mut batch = ChatGptImportBatch::default();
        importer
            .import_json_str(
                &fixture(),
                "chatgpt/conversations.json",
                &ChatGptImportFilter::default(),
                &mut batch,
            )
            .unwrap();
        assert_eq!(batch.drafts.len(), 1);
        assert_eq!(batch.audit.skipped_records.len(), 1);
        assert_eq!(batch.drafts[0].published.timestamp(), 1_780_000_000);
        assert!(
            batch.drafts[0]
                .idempotency_key
                .as_str()
                .starts_with("chatgpt:conv-1:msg-1:")
        );
    }

    #[test]
    fn date_and_conversation_filters_are_applied() {
        let importer = ChatGptImporter::new(SemVer::new("1.0.0"));
        let mut filter = ChatGptImportFilter::default();
        filter.conversation_ids.insert("conv-1".to_owned());
        filter.from = Some(Utc.timestamp_opt(1_780_000_001, 0).single().unwrap());
        let mut batch = ChatGptImportBatch::default();
        importer
            .import_json_str(
                &fixture(),
                "chatgpt/conversations.json",
                &filter,
                &mut batch,
            )
            .unwrap();
        assert!(batch.drafts.is_empty());
    }
}
