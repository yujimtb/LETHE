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

pub const GMAIL_MESSAGE_SCHEMA: &str = "schema:gmail-message";
pub const GMAIL_MESSAGE_SCHEMA_VERSION: &str = "1.0.0";

const OBSERVER_ID: &str = "obs:gmail-importer";
const SOURCE_SYSTEM: &str = "sys:gmail";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GmailMessage {
    pub account_id: String,
    pub message_id: String,
    pub thread_id: String,
    pub date: String,
    pub from: String,
    #[serde(default)]
    pub to: Vec<String>,
    #[serde(default)]
    pub cc: Vec<String>,
    pub subject: String,
    pub text: String,
    #[serde(default)]
    pub references: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub in_reply_to: Option<String>,
    #[serde(default)]
    pub labels: Vec<String>,
}

pub struct GmailAdapter {
    pub config: AdapterConfig,
    pub last_successful_capture: Option<DateTime<Utc>>,
}

impl GmailAdapter {
    pub fn new(config: AdapterConfig) -> Self {
        Self {
            config,
            last_successful_capture: None,
        }
    }

    pub fn map_message(&self, message: &GmailMessage) -> Result<ObservationDraft, AdapterError> {
        let published = DateTime::parse_from_rfc2822(&message.date)
            .map_err(|error| AdapterError::MalformedResponse {
                message: format!("invalid Gmail Date header: {error}"),
            })?
            .with_timezone(&Utc);
        let identity = declare_canonical_identity("gmail", self, self, message);
        let thread_ref = format!("gmail:thread:{}", message.thread_id);

        Ok(ObservationDraft {
            schema: SchemaRef::new(GMAIL_MESSAGE_SCHEMA),
            schema_version: SemVer::new(GMAIL_MESSAGE_SCHEMA_VERSION),
            observer: ObserverRef::new(OBSERVER_ID),
            source_system: Some(SourceSystemRef::new(SOURCE_SYSTEM)),
            authority_model: AuthorityModel::LakeAuthoritative,
            capture_model: CaptureModel::Event,
            subject: EntityRef::new(format!("message:gmail:{}", message.message_id)),
            target: None,
            payload: serde_json::json!({
                "account_id": message.account_id,
                "message_id": message.message_id,
                "thread_id": message.thread_id,
                "date": message.date,
                "from": message.from,
                "to": message.to,
                "cc": message.cc,
                "subject": message.subject,
                "text": message.text,
                "references": message.references,
                "in_reply_to": message.in_reply_to,
                "labels": message.labels,
            }),
            attachments: vec![],
            published,
            idempotency_key: identity.idempotency_key,
            meta: serde_json::json!({
                "sourceAdapterVersion": self.config.adapter_version.as_str(),
                OBJECT_ID_META_KEY: identity.object_id,
                CANONICAL_JSON_META_KEY: identity.canonical_json,
                "communication_channel_kind": "gmail",
                "communication_channel_external_id": message.account_id,
                "communication_sender_id": message.from,
                "communication_thread_ref": thread_ref,
            }),
        })
    }
}

impl ObjectIdExtractor<GmailMessage> for GmailAdapter {
    fn object_id(&self, value: &GmailMessage) -> String {
        value.message_id.clone()
    }
}

impl CanonicalTupleBuilder<GmailMessage> for GmailAdapter {
    fn canonical_tuple(&self, value: &GmailMessage) -> serde_json::Value {
        serde_json::json!({
            "sender": value.from,
            "subject": normalize_canonical_body(&value.subject),
            "body": normalize_canonical_body(&value.text),
            "event_time": value.date,
        })
    }
}

impl SourceAdapter for GmailAdapter {
    fn fetch_incremental(
        &self,
        _cursor: Option<&lethe_adapter_api::traits::Cursor>,
    ) -> FetchResult {
        FetchResult::Error(AdapterError::Other(
            "Gmail polling lives in runtime supervisor; submit GmailMessage raw data to LETHE import endpoint".into(),
        ))
    }

    fn fetch_snapshot(&self, _target_id: &str) -> FetchResult {
        FetchResult::Error(AdapterError::Other(
            "Gmail snapshot fetch is not implemented in LETHE".into(),
        ))
    }

    fn to_observations(&self, raw: &RawData) -> Result<Vec<ObservationDraft>, AdapterError> {
        let message =
            serde_json::from_value::<GmailMessage>(raw.data.clone()).map_err(|error| {
                AdapterError::MalformedResponse {
                    message: format!("Gmail raw data decode error: {error}"),
                }
            })?;
        Ok(vec![self.map_message(&message)?])
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
            schemas: vec![SchemaRef::new(GMAIL_MESSAGE_SCHEMA)],
            schema_bindings: vec![SchemaBinding {
                schema: SchemaRef::new(GMAIL_MESSAGE_SCHEMA),
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
            credential_ref: "runtime-supervisor:gmail".into(),
        }
    }

    fn message() -> GmailMessage {
        GmailMessage {
            account_id: "me@example.test".into(),
            message_id: "<msg-1@example.test>".into(),
            thread_id: "thread-1".into(),
            date: "Mon, 06 Jul 2026 09:10:00 +0900".into(),
            from: "sender@example.test".into(),
            to: vec!["me@example.test".into()],
            cc: vec![],
            subject: "Hello".into(),
            text: "Body".into(),
            references: vec!["<root@example.test>".into()],
            in_reply_to: Some("<root@example.test>".into()),
            labels: vec!["INBOX".into()],
        }
    }

    #[test]
    fn maps_gmail_message_with_date_as_published_and_thread_headers() {
        let adapter = GmailAdapter::new(test_config());
        let draft = adapter.map_message(&message()).unwrap();

        assert_eq!(draft.schema.as_str(), GMAIL_MESSAGE_SCHEMA);
        assert_eq!(draft.published.to_rfc3339(), "2026-07-06T00:10:00+00:00");
        assert!(
            draft
                .idempotency_key
                .as_str()
                .starts_with("gmail:<msg-1@example.test>:")
        );
        assert_eq!(draft.payload["references"][0], "<root@example.test>");
        assert_eq!(
            draft.meta["communication_thread_ref"],
            "gmail:thread:thread-1"
        );
    }

    #[test]
    fn same_message_is_idempotent() {
        let adapter = GmailAdapter::new(test_config());
        let first = adapter.map_message(&message()).unwrap();
        let second = adapter.map_message(&message()).unwrap();

        assert_eq!(first.idempotency_key, second.idempotency_key);
    }

    #[test]
    fn invalid_date_is_rejected() {
        let adapter = GmailAdapter::new(test_config());
        let mut message = message();
        message.date = "not a date".into();

        let err = adapter.map_message(&message).unwrap_err();

        assert!(matches!(err, AdapterError::MalformedResponse { .. }));
    }
}
