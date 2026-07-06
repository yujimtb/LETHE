use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use lethe_adapter_api::config::AdapterConfig;
use lethe_adapter_api::error::AdapterError;
use lethe_adapter_api::heartbeat::heartbeat_draft;
use lethe_adapter_api::idempotency::{
    CANONICAL_JSON_META_KEY, CanonicalTupleBuilder, OBJECT_ID_META_KEY, ObjectIdExtractor,
    declare_canonical_identity, normalize_canonical_body,
};
use lethe_adapter_api::traits::{FetchResult, ObservationDraft, RawData, SourceAdapter};
use lethe_core::domain::{
    AuthorityModel, CaptureModel, EntityRef, ObserverRef, SchemaRef, SemVer, SourceSystemRef,
};

pub const DISCORD_MESSAGE_SCHEMA: &str = "schema:discord-message";
pub const DISCORD_MESSAGE_SCHEMA_VERSION: &str = "1.0.0";

const OBSERVER_ID: &str = "obs:discord-importer";
const SOURCE_SYSTEM: &str = "sys:discord";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DiscordMessage {
    pub channel_id: String,
    pub message_id: String,
    pub timestamp: DateTime<Utc>,
    pub author_id: String,
    pub author_name: String,
    pub content: String,
    pub is_dm: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub guild_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub guild_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub channel_name: Option<String>,
    #[serde(default)]
    pub mentions: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub referenced_message_id: Option<String>,
}

pub struct DiscordAdapter {
    pub config: AdapterConfig,
    pub last_successful_capture: Option<DateTime<Utc>>,
}

impl DiscordAdapter {
    pub fn new(config: AdapterConfig) -> Self {
        Self {
            config,
            last_successful_capture: None,
        }
    }

    pub fn map_message(&self, message: &DiscordMessage) -> ObservationDraft {
        let identity = declare_canonical_identity("discord", self, self, message);
        let thread_ref = format!(
            "discord:thread:{}",
            message
                .referenced_message_id
                .as_deref()
                .unwrap_or(message.message_id.as_str())
        );

        ObservationDraft {
            schema: SchemaRef::new(DISCORD_MESSAGE_SCHEMA),
            schema_version: SemVer::new(DISCORD_MESSAGE_SCHEMA_VERSION),
            observer: ObserverRef::new(OBSERVER_ID),
            source_system: Some(SourceSystemRef::new(SOURCE_SYSTEM)),
            authority_model: AuthorityModel::LakeAuthoritative,
            capture_model: CaptureModel::Event,
            subject: EntityRef::new(format!(
                "message:discord:{}:{}",
                message.channel_id, message.message_id
            )),
            target: None,
            payload: serde_json::json!({
                "channel_id": message.channel_id,
                "message_id": message.message_id,
                "timestamp": message.timestamp,
                "author_id": message.author_id,
                "author_name": message.author_name,
                "content": message.content,
                "is_dm": message.is_dm,
                "guild_id": message.guild_id,
                "guild_name": message.guild_name,
                "channel_name": message.channel_name,
                "mentions": message.mentions,
                "referenced_message_id": message.referenced_message_id,
            }),
            attachments: vec![],
            published: message.timestamp,
            idempotency_key: identity.idempotency_key,
            meta: serde_json::json!({
                "sourceAdapterVersion": self.config.adapter_version.as_str(),
                OBJECT_ID_META_KEY: identity.object_id,
                CANONICAL_JSON_META_KEY: identity.canonical_json,
                "communication_channel_kind": "discord",
                "communication_channel_external_id": message.channel_id,
                "communication_sender_id": message.author_id,
                "communication_thread_ref": thread_ref,
            }),
        }
    }
}

impl ObjectIdExtractor<DiscordMessage> for DiscordAdapter {
    fn object_id(&self, value: &DiscordMessage) -> String {
        format!("{}:{}", value.channel_id, value.message_id)
    }
}

impl CanonicalTupleBuilder<DiscordMessage> for DiscordAdapter {
    fn canonical_tuple(&self, value: &DiscordMessage) -> serde_json::Value {
        serde_json::json!({
            "sender": value.author_id,
            "body": normalize_canonical_body(&value.content),
            "event_time": value.timestamp,
        })
    }
}

impl SourceAdapter for DiscordAdapter {
    fn fetch_incremental(
        &self,
        _cursor: Option<&lethe_adapter_api::traits::Cursor>,
    ) -> FetchResult {
        FetchResult::Error(AdapterError::Other(
            "Discord gateway subscription lives in runtime supervisor; submit DiscordMessage raw data to LETHE import endpoint".into(),
        ))
    }

    fn fetch_snapshot(&self, _target_id: &str) -> FetchResult {
        FetchResult::Error(AdapterError::Other(
            "Discord snapshot fetch is not implemented in LETHE".into(),
        ))
    }

    fn to_observations(&self, raw: &RawData) -> Result<Vec<ObservationDraft>, AdapterError> {
        let message =
            serde_json::from_value::<DiscordMessage>(raw.data.clone()).map_err(|error| {
                AdapterError::MalformedResponse {
                    message: format!("Discord raw data decode error: {error}"),
                }
            })?;
        Ok(vec![self.map_message(&message)])
    }

    fn heartbeat(&self) -> ObservationDraft {
        heartbeat_draft(
            &ObserverRef::new(OBSERVER_ID),
            &SourceSystemRef::new(SOURCE_SYSTEM),
            Utc::now(),
            0,
            self.last_successful_capture,
        )
    }

    fn observer_ref(&self) -> &ObserverRef {
        &self.config.observer_id
    }

    fn source_system_ref(&self) -> &SourceSystemRef {
        &self.config.source_system_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lethe_adapter_api::config::{BackoffStrategy, RateLimitConfig, RetryConfig, SchemaBinding};
    use std::time::Duration;

    fn test_config() -> AdapterConfig {
        AdapterConfig {
            observer_id: ObserverRef::new(OBSERVER_ID),
            source_system_id: SourceSystemRef::new(SOURCE_SYSTEM),
            adapter_version: SemVer::new("1.0.0"),
            authority_model: AuthorityModel::LakeAuthoritative,
            capture_model: CaptureModel::Event,
            schemas: vec![SchemaRef::new(DISCORD_MESSAGE_SCHEMA)],
            schema_bindings: vec![SchemaBinding {
                schema: SchemaRef::new(DISCORD_MESSAGE_SCHEMA),
                versions: ">=1.0.0 <2.0.0".into(),
            }],
            poll_interval: Duration::from_secs(300),
            heartbeat_interval: Duration::from_secs(60),
            rate_limit: RateLimitConfig {
                requests_per_second: 10,
                burst: 5,
            },
            retry: RetryConfig {
                max_retries: 3,
                backoff: BackoffStrategy::Exponential,
                max_wait: Duration::from_secs(30),
            },
            credential_ref: "runtime-supervisor:discord".into(),
        }
    }

    fn message() -> DiscordMessage {
        DiscordMessage {
            channel_id: "D01".into(),
            message_id: "M01".into(),
            timestamp: DateTime::parse_from_rfc3339("2026-07-06T00:10:00Z")
                .unwrap()
                .to_utc(),
            author_id: "U01".into(),
            author_name: "alice".into(),
            content: "hello".into(),
            is_dm: true,
            guild_id: None,
            guild_name: None,
            channel_name: Some("dm".into()),
            mentions: vec![],
            referenced_message_id: None,
        }
    }

    #[test]
    fn maps_discord_dm_message() {
        let adapter = DiscordAdapter::new(test_config());
        let draft = adapter.map_message(&message());

        assert_eq!(draft.schema.as_str(), DISCORD_MESSAGE_SCHEMA);
        assert!(
            draft
                .idempotency_key
                .as_str()
                .starts_with("discord:D01:M01:")
        );
        assert_eq!(draft.payload["is_dm"], true);
        assert_eq!(draft.meta["communication_thread_ref"], "discord:thread:M01");
    }

    #[test]
    fn same_message_is_idempotent() {
        let adapter = DiscordAdapter::new(test_config());
        let first = adapter.map_message(&message());
        let second = adapter.map_message(&message());

        assert_eq!(first.idempotency_key, second.idempotency_key);
    }
}
