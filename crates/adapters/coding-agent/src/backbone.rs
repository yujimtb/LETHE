use std::collections::BTreeMap;

use chrono::{DateTime, SecondsFormat, Utc};
use lethe_adapter_api::idempotency::{
    CANONICAL_JSON_META_KEY, OBJECT_ID_META_KEY, canonical_json, identity_key,
    normalize_canonical_body,
};
use lethe_adapter_api::traits::ObservationDraft;
use lethe_core::domain::{
    AuthorityModel, CaptureModel, EntityRef, ObserverRef, SchemaRef, SemVer, SourceSystemRef,
};
use serde_json::Value;

pub const CODING_AGENT_MESSAGE_SCHEMA: &str = "schema:coding-agent-message";
pub const CODING_AGENT_MESSAGE_SCHEMA_VERSION: &str = "1.0.0";

#[derive(Debug, Clone)]
pub struct CodingAgentSourceConfig {
    pub source_key: &'static str,
    pub observer_id: &'static str,
    pub source_system_id: &'static str,
}

#[derive(Debug, Clone, PartialEq)]
pub enum BackboneItem {
    Message {
        role: String,
        text: String,
    },
    ToolCall {
        tool_name: String,
        references: BTreeMap<String, Value>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct BackboneRecord {
    pub session_id: String,
    pub transcript_id: String,
    pub parent_message_id: Option<String>,
    pub is_sidechain: bool,
    pub parent_thread_id: Option<String>,
    pub thread_source: String,
    pub object_id: String,
    pub published: DateTime<Utc>,
    pub item: BackboneItem,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BackboneHistoryRecord {
    pub record: BackboneRecord,
    pub source_path: String,
    pub line_number: usize,
    pub raw: Vec<u8>,
}

impl BackboneRecord {
    pub fn to_observation_draft(
        &self,
        config: &CodingAgentSourceConfig,
        adapter_version: &SemVer,
    ) -> ObservationDraft {
        let canonical_tuple = self.canonical_tuple(config.source_key);
        let canonical_json = canonical_json(&canonical_tuple);
        let idempotency_key = identity_key(config.source_key, &self.object_id, &canonical_json);
        let payload = self.payload(config.source_key);

        ObservationDraft {
            schema: SchemaRef::new(CODING_AGENT_MESSAGE_SCHEMA),
            schema_version: SemVer::new(CODING_AGENT_MESSAGE_SCHEMA_VERSION),
            observer: ObserverRef::new(config.observer_id),
            source_system: Some(SourceSystemRef::new(config.source_system_id)),
            authority_model: AuthorityModel::LakeAuthoritative,
            capture_model: CaptureModel::Event,
            subject: EntityRef::new(format!("message:{}:{}", config.source_key, self.object_id)),
            target: None,
            payload,
            attachments: vec![],
            published: self.published,
            idempotency_key,
            meta: serde_json::json!({
                "sourceAdapterVersion": adapter_version.as_str(),
                "coding_agent_source": config.source_key,
                "session_id": self.session_id,
                "transcript_id": self.transcript_id,
                "thread_source": self.thread_source,
                "parent_thread_id": self.parent_thread_id,
                "source_container": self.session_id,
                OBJECT_ID_META_KEY: self.object_id,
                CANONICAL_JSON_META_KEY: canonical_json,
            }),
        }
    }

    fn canonical_tuple(&self, source_key: &str) -> Value {
        serde_json::json!({
            "agent": source_key,
                "thread": {
                    "session_id": self.session_id,
                    "transcript_id": self.transcript_id,
                    "parent_message_id": self.parent_message_id,
                    "is_sidechain": self.is_sidechain,
                    "parent_thread_id": self.parent_thread_id,
                    "thread_source": self.thread_source,
                },
            "published": self.published.to_rfc3339_opts(SecondsFormat::Micros, true),
            "item": match &self.item {
                BackboneItem::Message { role, text } => serde_json::json!({
                    "kind": "message",
                    "role": role,
                    "body": normalize_canonical_body(text),
                }),
                BackboneItem::ToolCall {
                    tool_name,
                    references,
                } => serde_json::json!({
                    "kind": "tool_call",
                    "tool_name": tool_name,
                    "references": references,
                }),
            },
        })
    }

    fn payload(&self, source_key: &str) -> Value {
        let item = match &self.item {
            BackboneItem::Message { role, text } => serde_json::json!({
                "kind": "message",
                "role": role,
                "text": text,
            }),
            BackboneItem::ToolCall {
                tool_name,
                references,
            } => serde_json::json!({
                "kind": "tool_call",
                "tool_name": tool_name,
                "references": references,
            }),
        };

        serde_json::json!({
            "agent": source_key,
            "session_id": self.session_id,
            "transcript_id": self.transcript_id,
            "parent_message_id": self.parent_message_id,
            "is_sidechain": self.is_sidechain,
            "parent_thread_id": self.parent_thread_id,
            "thread_source": self.thread_source,
            "object_id": self.object_id,
            "item": item,
        })
    }
}
