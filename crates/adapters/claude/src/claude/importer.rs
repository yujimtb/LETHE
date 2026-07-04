use std::collections::HashMap;
use std::io::{Cursor, Read};

use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Deserializer, Serialize, de};
use serde_json::Value;
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
    #[serde(alias = "chat_messages")]
    pub messages: Vec<ClaudeMessage>,
}

#[derive(Debug, Clone, Serialize)]
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
            let Some(export) = parse_zip_entry(file.name(), &text)? else {
                continue;
            };
            for conversation in export.conversations {
                drafts.extend(self.map_conversation(conversation));
            }
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

fn parse_zip_entry(entry_name: &str, json: &str) -> Result<Option<ClaudeExport>, AdapterError> {
    let value =
        serde_json::from_str::<Value>(json).map_err(|err| AdapterError::MalformedResponse {
            message: format!("invalid claude.ai json entry {entry_name}: {err}"),
        })?;

    if value.get("conversations").is_some() {
        return serde_json::from_value::<ClaudeExport>(value)
            .map(Some)
            .map_err(|err| AdapterError::MalformedResponse {
                message: format!("invalid claude.ai conversations entry {entry_name}: {err}"),
            });
    }

    if value_is_conversation_array(&value) {
        return serde_json::from_value::<Vec<ClaudeConversation>>(value)
            .map(|conversations| Some(ClaudeExport { conversations }))
            .map_err(|err| AdapterError::MalformedResponse {
                message: format!("invalid claude.ai conversation array {entry_name}: {err}"),
            });
    }

    if value_is_conversation_object(&value) {
        return serde_json::from_value::<ClaudeConversation>(value)
            .map(|conversation| {
                Some(ClaudeExport {
                    conversations: vec![conversation],
                })
            })
            .map_err(|err| AdapterError::MalformedResponse {
                message: format!("invalid claude.ai conversation entry {entry_name}: {err}"),
            });
    }

    if is_known_metadata_entry(entry_name) {
        return Ok(None);
    }

    Err(AdapterError::MalformedResponse {
        message: format!("unsupported claude.ai json entry: {entry_name}"),
    })
}

fn value_is_conversation_array(value: &Value) -> bool {
    match value {
        Value::Array(conversations) => conversations.iter().all(value_is_conversation_object),
        _ => false,
    }
}

fn value_is_conversation_object(value: &Value) -> bool {
    value
        .as_object()
        .map(|object| {
            object.get("uuid").is_some()
                && (object.get("messages").is_some() || object.get("chat_messages").is_some())
        })
        .unwrap_or(false)
}

fn is_known_metadata_entry(entry_name: &str) -> bool {
    matches!(entry_name, "users.json" | "memories.json")
}

impl<'de> Deserialize<'de> for ClaudeMessage {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct RawClaudeMessage {
            #[serde(default)]
            uuid: Option<String>,
            #[serde(default)]
            parent_message_uuid: Option<String>,
            #[serde(default)]
            sender: Option<String>,
            #[serde(default)]
            role: Option<String>,
            #[serde(default)]
            text: Option<Value>,
            #[serde(default)]
            content: Option<Value>,
            created_at: DateTime<Utc>,
        }

        let raw = RawClaudeMessage::deserialize(deserializer)?;
        let sender = raw
            .sender
            .or(raw.role)
            .or_else(|| {
                raw.content
                    .as_ref()
                    .and_then(|content| nested_string_field(content, "role"))
            })
            .ok_or_else(|| de::Error::missing_field("sender"))?;
        let text = match (raw.text.as_ref(), raw.content.as_ref()) {
            (Some(text), _) => message_text(text).map_err(de::Error::custom)?,
            (None, Some(content)) => message_text(content).map_err(de::Error::custom)?,
            (None, None) => return Err(de::Error::missing_field("text")),
        };

        Ok(Self {
            uuid: raw.uuid,
            parent_message_uuid: raw.parent_message_uuid,
            sender,
            text,
            created_at: raw.created_at,
        })
    }
}

fn nested_string_field(value: &Value, field: &str) -> Option<String> {
    value
        .as_object()
        .and_then(|object| object.get(field))
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
}

fn message_text(value: &Value) -> Result<String, String> {
    match value {
        Value::String(text) => Ok(text.clone()),
        Value::Object(object) => {
            let Some(content) = object.get("content") else {
                return Err("message content object has no content field".to_owned());
            };
            message_text(content)
        }
        Value::Null => Err("message text is null".to_owned()),
        _ => Err("message text must be a string or content object".to_owned()),
    }
}

fn object_ids_for_messages(conversation: &ClaudeConversation) -> HashMap<usize, String> {
    let mut children: HashMap<Option<String>, Vec<IndexedMessage>> = HashMap::new();
    let mut message_uuids = std::collections::HashSet::new();
    for (index, message) in conversation.messages.iter().cloned().enumerate() {
        if let Some(uuid) = message.uuid.clone() {
            message_uuids.insert(uuid);
        }
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
    let mut missing_parent_uuids = children
        .keys()
        .filter_map(Clone::clone)
        .filter(|parent_uuid| !message_uuids.contains(parent_uuid))
        .collect::<Vec<_>>();
    missing_parent_uuids.sort();
    for (orphan_root_index, parent_uuid) in missing_parent_uuids.into_iter().enumerate() {
        assign_object_ids(
            &conversation.uuid,
            Some(parent_uuid),
            &format!("orphan:{orphan_root_index}"),
            &children,
            &mut object_ids,
        );
    }
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
    use std::io::Write;

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

    #[test]
    fn derives_missing_uuid_for_orphaned_parent_branch() {
        let importer = ClaudeAiImporter::new(SemVer::new("1.0.0"));
        let conversation = ClaudeConversation {
            uuid: "conv-1".into(),
            messages: vec![message(None, Some("missing-parent"), "orphan reply")],
        };

        let drafts = importer.map_conversation(conversation);

        assert_eq!(
            drafts[0].meta[OBJECT_ID_META_KEY],
            "conversation:conv-1:path:orphan:0.0"
        );
    }

    #[test]
    fn zip_import_skips_known_metadata_and_accepts_actual_export_shapes() {
        let importer = ClaudeAiImporter::new(SemVer::new("1.0.0"));
        let mut zip = zip::ZipWriter::new(Cursor::new(Vec::new()));
        let options = zip::write::SimpleFileOptions::default();

        zip.start_file("users.json", options).unwrap();
        zip.write_all(br#"{"uuid":"user-1","email_address":"person@example.com"}"#)
            .unwrap();
        zip.start_file("memories.json", options).unwrap();
        zip.write_all(br#"{"conversations_memory":[],"account_uuid":"account-1"}"#)
            .unwrap();
        zip.start_file("conversations.json", options).unwrap();
        zip.write_all(
            br#"[{
                "uuid": "conv-1",
                "chat_messages": [{
                    "uuid": "msg-1",
                    "parent_message_uuid": null,
                    "sender": "human",
                    "text": "hello",
                    "created_at": "2026-05-01T10:00:00Z"
                }]
            }]"#,
        )
        .unwrap();
        zip.start_file("design_chats/design-1.json", options)
            .unwrap();
        zip.write_all(
            br#"{
                "uuid": "design-1",
                "messages": [{
                    "uuid": "design-msg-1",
                    "role": "assistant",
                    "content": {
                        "content": "design reply",
                        "role": "assistant"
                    },
                    "created_at": "2026-05-01T11:00:00Z"
                }]
            }"#,
        )
        .unwrap();

        let zip_bytes = zip.finish().unwrap().into_inner();
        let drafts = importer.import_zip(&zip_bytes).unwrap();

        assert_eq!(drafts.len(), 2);
        assert_eq!(drafts[0].payload["text"], "hello");
        assert_eq!(drafts[1].payload["text"], "design reply");
        assert_eq!(drafts[1].payload["sender"], "assistant");
    }

    #[test]
    fn zip_import_rejects_unknown_json_entries() {
        let importer = ClaudeAiImporter::new(SemVer::new("1.0.0"));
        let mut zip = zip::ZipWriter::new(Cursor::new(Vec::new()));
        let options = zip::write::SimpleFileOptions::default();

        zip.start_file("unexpected.json", options).unwrap();
        zip.write_all(br#"{"uuid":"not-a-conversation"}"#).unwrap();

        let zip_bytes = zip.finish().unwrap().into_inner();
        let error = importer.import_zip(&zip_bytes).unwrap_err();

        assert!(error.to_string().contains("unexpected.json"));
    }
}
