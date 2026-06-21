use std::collections::HashMap;
use std::io::{Cursor, Read};

use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use zip::ZipArchive;

use lethe_adapter_api::error::AdapterError;
use lethe_adapter_api::idempotency::{
    CANONICAL_JSON_META_KEY, OBJECT_ID_META_KEY, canonical_json, identity_key,
    normalize_canonical_body,
};
use lethe_adapter_api::traits::ObservationDraft;
use lethe_core::domain::{
    AuthorityModel, CaptureModel, EntityRef, ObserverRef, SchemaRef, SemVer, SourceSystemRef,
};

pub const CLAUDE_MESSAGE_SCHEMA: &str = "schema:claude-message";
pub const CLAUDE_MESSAGE_SCHEMA_VERSION: &str = "1.0.0";
const OBSERVER_ID: &str = "obs:claude-ai-importer";
const SOURCE_SYSTEM: &str = "sys:claude-ai";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClaudeExport {
    pub conversations: Vec<ClaudeConversation>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClaudeConversation {
    pub uuid: String,
    #[serde(default)]
    pub messages: Vec<ClaudeMessage>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClaudeMessage {
    #[serde(default)]
    pub uuid: Option<String>,
    #[serde(default)]
    pub parent_message_uuid: Option<String>,
    pub sender: String,
    pub text: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
struct IndexedMessage {
    index: usize,
    message: ClaudeMessage,
}

pub struct ClaudeAiImporter {
    adapter_version: SemVer,
}

impl ClaudeAiImporter {
    pub fn new(adapter_version: SemVer) -> Self {
        Self { adapter_version }
    }

    pub fn import_zip(&self, bytes: &[u8]) -> Result<Vec<ObservationDraft>, AdapterError> {
        let mut archive =
            ZipArchive::new(Cursor::new(bytes)).map_err(|err| AdapterError::MalformedResponse {
                message: format!("invalid claude.ai zip: {err}"),
            })?;
        let mut drafts = Vec::new();

        for index in 0..archive.len() {
            let mut file =
                archive
                    .by_index(index)
                    .map_err(|err| AdapterError::MalformedResponse {
                        message: format!("invalid zip entry: {err}"),
                    })?;
            if !file.name().ends_with(".json") {
                continue;
            }
            let mut text = String::new();
            file.read_to_string(&mut text)
                .map_err(|err| AdapterError::MalformedResponse {
                    message: format!("invalid json entry: {err}"),
                })?;
            drafts.extend(self.import_json_str(&text)?);
        }

        Ok(drafts)
    }

    pub fn import_json_str(&self, json: &str) -> Result<Vec<ObservationDraft>, AdapterError> {
        let export = parse_export(json)?;
        let mut drafts = Vec::new();
        for conversation in export.conversations {
            drafts.extend(self.map_conversation(conversation));
        }
        Ok(drafts)
    }

    pub fn map_conversation(&self, conversation: ClaudeConversation) -> Vec<ObservationDraft> {
        let object_ids = object_ids_for_messages(&conversation);
        conversation
            .messages
            .into_iter()
            .enumerate()
            .map(|(index, message)| {
                self.map_message(&conversation.uuid, &object_ids[&index], message)
            })
            .collect()
    }

    fn map_message(
        &self,
        conversation_uuid: &str,
        object_id: &str,
        message: ClaudeMessage,
    ) -> ObservationDraft {
        let canonical_tuple = serde_json::json!({
            "sender": message.sender,
            "body": normalize_canonical_body(&message.text),
            "event_time": message.created_at.to_rfc3339_opts(SecondsFormat::Micros, true),
            "attachment_sha256": [],
        });
        let canonical_json = canonical_json(&canonical_tuple);
        let idempotency_key = identity_key("claude-ai", object_id, &canonical_json);

        ObservationDraft {
            schema: SchemaRef::new(CLAUDE_MESSAGE_SCHEMA),
            schema_version: SemVer::new(CLAUDE_MESSAGE_SCHEMA_VERSION),
            observer: ObserverRef::new(OBSERVER_ID),
            source_system: Some(SourceSystemRef::new(SOURCE_SYSTEM)),
            authority_model: AuthorityModel::LakeAuthoritative,
            capture_model: CaptureModel::Event,
            subject: EntityRef::new(format!("message:claude-ai:{object_id}")),
            target: None,
            payload: serde_json::json!({
                "conversation_uuid": conversation_uuid,
                "message_uuid": message.uuid,
                "parent_message_uuid": message.parent_message_uuid,
                "sender": message.sender,
                "text": message.text,
            }),
            attachments: vec![],
            published: message.created_at,
            idempotency_key,
            meta: serde_json::json!({
                "sourceAdapterVersion": self.adapter_version.as_str(),
                OBJECT_ID_META_KEY: object_id,
                CANONICAL_JSON_META_KEY: canonical_json,
            }),
        }
    }
}

fn parse_export(json: &str) -> Result<ClaudeExport, AdapterError> {
    serde_json::from_str::<ClaudeExport>(json)
        .or_else(|_| {
            serde_json::from_str::<Vec<ClaudeConversation>>(json)
                .map(|conversations| ClaudeExport { conversations })
        })
        .map_err(|err| AdapterError::MalformedResponse {
            message: format!("invalid claude.ai export json: {err}"),
        })
}

fn object_ids_for_messages(conversation: &ClaudeConversation) -> HashMap<usize, String> {
    let mut children: HashMap<Option<String>, Vec<IndexedMessage>> = HashMap::new();
    for (index, message) in conversation.messages.iter().cloned().enumerate() {
        children
            .entry(message.parent_message_uuid.clone())
            .or_default()
            .push(IndexedMessage { index, message });
    }
    for siblings in children.values_mut() {
        siblings.sort_by(|left, right| {
            (
                left.message.created_at,
                left.message.sender.as_str(),
                left.message.text.as_str(),
                left.index,
            )
                .cmp(&(
                    right.message.created_at,
                    right.message.sender.as_str(),
                    right.message.text.as_str(),
                    right.index,
                ))
        });
    }

    let mut object_ids = HashMap::new();
    assign_object_ids(&conversation.uuid, None, "", &children, &mut object_ids);
    object_ids
}

fn assign_object_ids(
    conversation_uuid: &str,
    parent_uuid: Option<String>,
    parent_path: &str,
    children: &HashMap<Option<String>, Vec<IndexedMessage>>,
    object_ids: &mut HashMap<usize, String>,
) {
    let Some(siblings) = children.get(&parent_uuid) else {
        return;
    };

    for (sibling_index, indexed) in siblings.iter().enumerate() {
        let path = if parent_path.is_empty() {
            sibling_index.to_string()
        } else {
            format!("{parent_path}.{sibling_index}")
        };
        let object_id = indexed
            .message
            .uuid
            .clone()
            .unwrap_or_else(|| format!("conversation:{conversation_uuid}:path:{path}"));
        object_ids.insert(indexed.index, object_id);
        if let Some(uuid) = indexed.message.uuid.clone() {
            assign_object_ids(conversation_uuid, Some(uuid), &path, children, object_ids);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message(uuid: Option<&str>, parent: Option<&str>, text: &str) -> ClaudeMessage {
        ClaudeMessage {
            uuid: uuid.map(ToOwned::to_owned),
            parent_message_uuid: parent.map(ToOwned::to_owned),
            sender: "assistant".into(),
            text: text.into(),
            created_at: chrono::DateTime::parse_from_rfc3339("2026-05-01T10:00:00Z")
                .unwrap()
                .to_utc(),
        }
    }

    #[test]
    fn derives_missing_uuid_from_conversation_path() {
        let importer = ClaudeAiImporter::new(SemVer::new("1.0.0"));
        let conversation = ClaudeConversation {
            uuid: "conv-1".into(),
            messages: vec![
                message(Some("root"), None, "root"),
                message(None, Some("root"), "reply"),
            ],
        };

        let drafts = importer.map_conversation(conversation);

        assert_eq!(drafts[0].meta[OBJECT_ID_META_KEY], "root");
        assert_eq!(
            drafts[1].meta[OBJECT_ID_META_KEY],
            "conversation:conv-1:path:0.0"
        );
    }

    #[test]
    fn same_json_import_produces_same_identity_keys() {
        let importer = ClaudeAiImporter::new(SemVer::new("1.0.0"));
        let json = serde_json::json!({
            "conversations": [{
                "uuid": "conv-1",
                "messages": [{
                    "uuid": null,
                    "parent_message_uuid": null,
                    "sender": "human",
                    "text": "hello",
                    "created_at": "2026-05-01T10:00:00Z"
                }]
            }]
        })
        .to_string();

        let first = importer.import_json_str(&json).unwrap();
        let second = importer.import_json_str(&json).unwrap();

        assert_eq!(first[0].idempotency_key, second[0].idempotency_key);
    }
}
