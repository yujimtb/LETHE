//! M10 — Slack → Observation mapper + SlackAdapter implementation
//!
//! Pure mapping is in `map_message` / `map_channel_snapshot`.
//! IO (fetch) is delegated to the `SlackClient` trait.

use std::collections::HashMap;

use chrono::{DateTime, Utc};

use lethe_adapter_api::config::AdapterConfig;
use lethe_adapter_api::error::AdapterError;
use lethe_adapter_api::heartbeat::heartbeat_draft;
use lethe_adapter_api::idempotency::*;
use lethe_adapter_api::traits::*;
use lethe_core::domain::{
    AuthorityModel, BlobRef, CaptureModel, EntityRef, ObserverRef, SchemaRef, SemVer,
    SourceSystemRef,
};

use super::client::*;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

pub const SLACK_MESSAGE_SCHEMA: &str = "schema:slack-message";
pub const SLACK_MESSAGE_SCHEMA_VERSION: &str = "1.0.0";
pub const SLACK_CHANNEL_SCHEMA: &str = "schema:slack-channel-snapshot";
pub const SLACK_CHANNEL_SCHEMA_VERSION: &str = "1.0.0";

const OBSERVER_ID: &str = "obs:slack-crawler";
const SOURCE_SYSTEM: &str = "sys:slack";

// ---------------------------------------------------------------------------
// SlackAdapter
// ---------------------------------------------------------------------------

pub struct SlackAdapter<C: SlackClient> {
    pub client: C,
    pub config: AdapterConfig,
    /// Per-channel cursor: channel_id → last known ts.
    pub cursors: HashMap<String, String>,
    pub last_successful_capture: Option<DateTime<Utc>>,
}

impl<C: SlackClient> SlackAdapter<C> {
    pub fn new(client: C, config: AdapterConfig) -> Self {
        Self {
            client,
            config,
            cursors: HashMap::new(),
            last_successful_capture: None,
        }
    }

    /// Map a single Slack message to an ObservationDraft.
    pub fn map_message(&self, msg: &SlackMessage) -> Result<ObservationDraft, AdapterError> {
        let published = parse_slack_ts(&msg.ts).ok_or_else(|| AdapterError::MalformedResponse {
            message: format!("invalid Slack timestamp: {}", msg.ts),
        })?;
        let ingress_kind = msg
            .ingress_kind
            .ok_or_else(|| AdapterError::MalformedResponse {
                message: "Slack message missing ingress_kind".to_string(),
            })?;
        let identity = declare_canonical_identity("slack", self, self, msg);

        let subject = EntityRef::new(format!("message:slack:{}-{}", msg.channel_id, msg.ts));

        let mut meta = serde_json::json!({
            "sourceAdapterVersion": self.config.adapter_version.as_str(),
            OBJECT_ID_META_KEY: identity.object_id,
            CANONICAL_JSON_META_KEY: identity.canonical_json,
            "communication_channel_kind": "slack",
            "communication_channel_external_id": msg.channel_id,
            "communication_sender_id": msg.user_id,
            "communication_thread_ref": format!("slack:thread:{}", msg.thread_ts.as_deref().unwrap_or(msg.ts.as_str())),
        });

        if msg.message_type == SlackMessageType::Delete {
            meta["retracts"] = serde_json::json!({
                "source_object_id": identity.object_id,
            });
        }

        let mut payload = serde_json::json!({
            "channel_id": msg.channel_id,
            "channel_name": msg.channel_name,
            "ts": msg.ts,
            "user_id": msg.user_id,
            "user_name": msg.user_name,
            "text": msg.text,
            "ingress_kind": ingress_kind,
            "mentions": msg.mentions,
            "message_type": msg.message_type,
        });

        if let Some(ref email) = msg.email {
            payload["email"] = serde_json::json!(email);
        }

        if let Some(ref thread_ts) = msg.thread_ts {
            payload["thread_ts"] = serde_json::json!(thread_ts);
        }
        if let Some(ref edited) = msg.edited {
            payload["edited"] = serde_json::json!({
                "user": edited.user,
                "ts": edited.ts,
            });
        }
        if !msg.reactions.is_empty() {
            payload["reactions"] = serde_json::to_value(&msg.reactions).unwrap_or_default();
        }
        if !msg.files.is_empty() {
            payload["files"] = serde_json::to_value(&msg.files).unwrap_or_default();
        }
        if msg.reply_count > 0 {
            payload["reply_count"] = serde_json::json!(msg.reply_count);
            payload["reply_users_count"] = serde_json::json!(msg.reply_users_count);
        }

        let attachments: Vec<BlobRef> = msg
            .files
            .iter()
            .filter_map(|f| f.blob_ref.as_ref().map(|r| BlobRef::new(r.clone())))
            .collect();

        Ok(ObservationDraft {
            schema: SchemaRef::new(SLACK_MESSAGE_SCHEMA),
            schema_version: SemVer::new(SLACK_MESSAGE_SCHEMA_VERSION),
            observer: ObserverRef::new(OBSERVER_ID),
            source_system: Some(SourceSystemRef::new(SOURCE_SYSTEM)),
            authority_model: AuthorityModel::LakeAuthoritative,
            capture_model: CaptureModel::Event,
            subject,
            target: None,
            payload,
            attachments,
            published,
            idempotency_key: identity.idempotency_key,
            client_ref: None,
            meta,
        })
    }

    /// Map a Slack channel snapshot to an ObservationDraft.
    pub fn map_channel_snapshot(&self, snap: &SlackChannelSnapshot) -> ObservationDraft {
        let idem_key = lethe_core::domain::IdempotencyKey::new(format!(
            "slack:channel:{}:snapshot:{}",
            snap.channel_id,
            snap.snapshot_at.format("%Y-%m-%dT%H:%M")
        ));

        let payload = serde_json::json!({
            "channel_id": snap.channel_id,
            "channel_name": snap.channel_name,
            "purpose": snap.purpose,
            "topic": snap.topic,
            "member_count": snap.member_count,
            "members": snap.members,
            "is_archived": snap.is_archived,
            "snapshot_at": snap.snapshot_at,
        });

        ObservationDraft {
            schema: SchemaRef::new(SLACK_CHANNEL_SCHEMA),
            schema_version: SemVer::new(SLACK_CHANNEL_SCHEMA_VERSION),
            observer: ObserverRef::new(OBSERVER_ID),
            source_system: Some(SourceSystemRef::new(SOURCE_SYSTEM)),
            authority_model: AuthorityModel::LakeAuthoritative,
            capture_model: CaptureModel::Event,
            subject: EntityRef::new(format!("channel:slack:{}", snap.channel_id)),
            target: None,
            payload,
            attachments: vec![],
            published: snap.snapshot_at,
            idempotency_key: idem_key,
            client_ref: None,
            meta: serde_json::json!({
                "sourceAdapterVersion": self.config.adapter_version.as_str(),
            }),
        }
    }

    /// Update the per-channel cursor after a successful fetch.
    pub fn update_cursor(&mut self, channel_id: &str, latest_ts: &str) {
        self.cursors
            .insert(channel_id.to_string(), latest_ts.to_string());
    }

    /// Get the current cursor for a channel.
    pub fn get_cursor(&self, channel_id: &str) -> Option<&str> {
        self.cursors.get(channel_id).map(String::as_str)
    }
}

impl<C: SlackClient> ObjectIdExtractor<SlackMessage> for SlackAdapter<C> {
    fn object_id(&self, msg: &SlackMessage) -> String {
        format!("channel:{}:ts:{}", msg.channel_id, msg.ts)
    }
}

impl<C: SlackClient> CanonicalTupleBuilder<SlackMessage> for SlackAdapter<C> {
    fn canonical_tuple(&self, msg: &SlackMessage) -> serde_json::Value {
        let attachment_sha256 = msg
            .files
            .iter()
            .filter_map(|file| file.blob_ref.as_ref())
            .filter_map(|blob_ref| blob_ref.strip_prefix("blob:sha256:"))
            .map(ToOwned::to_owned)
            .collect::<Vec<_>>();
        let canonical_body = if msg.message_type == SlackMessageType::Delete {
            "[deleted]"
        } else {
            msg.text.as_str()
        };

        serde_json::json!({
            "sender": msg.user_id,
            "body": normalize_canonical_body(canonical_body),
            "event_time": msg.ts,
            "attachment_sha256": attachment_sha256,
        })
    }
}

impl<C: SlackClient> SourceAdapter for SlackAdapter<C> {
    fn fetch_incremental(&self, _cursor: Option<&Cursor>) -> FetchResult {
        FetchResult::Error(AdapterError::Other(
            "SlackAdapter::fetch_incremental requires an explicit channel ID; use SlackClient::conversations_history".into(),
        ))
    }

    fn fetch_snapshot(&self, target_id: &str) -> FetchResult {
        match self.client.conversations_info(target_id) {
            Ok(snap) => FetchResult::Ok {
                items: vec![RawData {
                    data: serde_json::to_value(&snap).unwrap_or_default(),
                    blobs: vec![],
                }],
                next_cursor: None,
                has_more: false,
            },
            Err(e) => FetchResult::Error(e),
        }
    }

    fn to_observations(&self, raw: &RawData) -> Result<Vec<ObservationDraft>, AdapterError> {
        // Try to deserialize as SlackMessage first, then as channel snapshot.
        if let Ok(msg) = serde_json::from_value::<SlackMessage>(raw.data.clone()) {
            Ok(vec![self.map_message(&msg)?])
        } else if let Ok(snap) = serde_json::from_value::<SlackChannelSnapshot>(raw.data.clone()) {
            Ok(vec![self.map_channel_snapshot(&snap)])
        } else {
            Err(AdapterError::MalformedResponse {
                message: "Slack raw data is neither a message nor a channel snapshot".to_string(),
            })
        }
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

/// Parse Slack's epoch.micro timestamp to DateTime<Utc>.
fn parse_slack_ts(ts: &str) -> Option<DateTime<Utc>> {
    let (seconds, fractional) = ts.split_once('.')?;
    if fractional.len() != 6 || !fractional.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let secs: i64 = seconds.parse().ok()?;
    let micros: u32 = fractional.parse().ok()?;
    DateTime::from_timestamp(secs, micros * 1000)
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use lethe_adapter_api::config::*;
    use lethe_adapter_api::error::AdapterError;
    use std::time::Duration;

    fn test_config() -> AdapterConfig {
        AdapterConfig {
            observer_id: ObserverRef::new(OBSERVER_ID),
            source_system_id: SourceSystemRef::new(SOURCE_SYSTEM),
            adapter_version: SemVer::new("1.0.0"),
            authority_model: AuthorityModel::LakeAuthoritative,
            capture_model: CaptureModel::Event,
            schemas: vec![
                SchemaRef::new(SLACK_MESSAGE_SCHEMA),
                SchemaRef::new(SLACK_CHANNEL_SCHEMA),
            ],
            schema_bindings: vec![SchemaBinding {
                schema: SchemaRef::new(SLACK_MESSAGE_SCHEMA),
                versions: ">=1.0.0 <2.0.0".into(),
            }],
            poll_interval: Duration::from_secs(300),
            heartbeat_interval: Duration::from_secs(60),
            rate_limit: RateLimitConfig {
                requests_per_second: 50,
                burst: 10,
            },
            retry: RetryConfig {
                max_retries: 3,
                backoff: BackoffStrategy::Exponential,
                max_wait: Duration::from_secs(30),
            },
            credential_ref: "secret:slack-token".into(),
        }
    }

    fn sample_message() -> SlackMessage {
        SlackMessage {
            channel_id: "C01ABC".into(),
            channel_name: "general".into(),
            ts: "1234567890.123456".into(),
            thread_ts: None,
            user_id: "U01XYZ".into(),
            user_name: "tanaka".into(),
            email: Some("tanaka@example.jp".into()),
            text: "Hello everyone!".into(),
            ingress_kind: Some(SlackIngressKind::Channel),
            mentions: vec![],
            message_type: SlackMessageType::Message,
            edited: None,
            reactions: vec![],
            files: vec![],
            reply_count: 0,
            reply_users_count: 0,
        }
    }

    #[test]
    fn map_regular_message() {
        let adapter = SlackAdapter::new(FixtureSlackClient::new(), test_config());
        let msg = sample_message();
        let draft = adapter.map_message(&msg).unwrap();

        assert_eq!(draft.schema.as_str(), SLACK_MESSAGE_SCHEMA);
        assert!(
            draft
                .idempotency_key
                .as_str()
                .starts_with("slack:channel:C01ABC:ts:1234567890.123456:")
        );
        assert!(draft.meta[CANONICAL_JSON_META_KEY].is_string());
        assert_eq!(
            draft.subject.as_str(),
            "message:slack:C01ABC-1234567890.123456"
        );
        assert_eq!(draft.payload["text"], "Hello everyone!");
        assert_eq!(draft.payload["ingress_kind"], "channel");
        assert_eq!(draft.payload["message_type"], "message");
        assert_eq!(draft.authority_model, AuthorityModel::LakeAuthoritative);
        assert_eq!(draft.capture_model, CaptureModel::Event);
    }

    #[test]
    fn map_dm_mention_and_channel_ingress_kinds() {
        let adapter = SlackAdapter::new(FixtureSlackClient::new(), test_config());
        let mut dm = sample_message();
        dm.channel_id = "D01DM".into();
        dm.ingress_kind = Some(SlackIngressKind::DirectMessage);
        let mut mention = sample_message();
        mention.text = "<@U-BOT> ping".into();
        mention.mentions = vec!["U-BOT".into()];
        mention.ingress_kind = Some(SlackIngressKind::Mention);
        let channel = sample_message();

        let dm = adapter.map_message(&dm).unwrap();
        let mention = adapter.map_message(&mention).unwrap();
        let channel = adapter.map_message(&channel).unwrap();

        assert_eq!(dm.payload["ingress_kind"], "direct_message");
        assert_eq!(mention.payload["ingress_kind"], "mention");
        assert_eq!(mention.payload["mentions"][0], "U-BOT");
        assert_eq!(channel.payload["ingress_kind"], "channel");
        assert!(
            dm.idempotency_key
                .as_str()
                .starts_with("slack:channel:D01DM:ts:")
        );
    }

    #[test]
    fn missing_ingress_kind_is_rejected() {
        let adapter = SlackAdapter::new(FixtureSlackClient::new(), test_config());
        let mut msg = sample_message();
        msg.ingress_kind = None;

        let err = adapter.map_message(&msg).unwrap_err();

        assert!(matches!(err, AdapterError::MalformedResponse { .. }));
    }

    #[test]
    fn map_edit_message() {
        let adapter = SlackAdapter::new(FixtureSlackClient::new(), test_config());
        let mut msg = sample_message();
        msg.message_type = SlackMessageType::Edit;
        msg.text = "Hello everyone! (edited)".into();
        msg.edited = Some(SlackEdited {
            user: "U01XYZ".into(),
            ts: "1234567891.000000".into(),
        });

        let draft = adapter.map_message(&msg).unwrap();
        assert!(
            draft
                .idempotency_key
                .as_str()
                .starts_with("slack:channel:C01ABC:ts:1234567890.123456:")
        );
        assert_eq!(draft.payload["message_type"], "edit");
        assert!(draft.payload["edited"].is_object());
    }

    #[test]
    fn map_delete_message() {
        let adapter = SlackAdapter::new(FixtureSlackClient::new(), test_config());
        let mut msg = sample_message();
        msg.message_type = SlackMessageType::Delete;

        let draft = adapter.map_message(&msg).unwrap();
        assert!(
            draft
                .idempotency_key
                .as_str()
                .starts_with("slack:channel:C01ABC:ts:1234567890.123456:")
        );
        assert_eq!(draft.payload["message_type"], "delete");
        assert_eq!(
            draft.meta["retracts"]["source_object_id"],
            "channel:C01ABC:ts:1234567890.123456"
        );
    }

    #[test]
    fn map_thread_reply() {
        let adapter = SlackAdapter::new(FixtureSlackClient::new(), test_config());
        let mut msg = sample_message();
        msg.thread_ts = Some("1234567880.000000".into());

        let draft = adapter.map_message(&msg).unwrap();
        assert_eq!(draft.payload["thread_ts"], "1234567880.000000");
    }

    #[test]
    fn map_file_share() {
        let adapter = SlackAdapter::new(FixtureSlackClient::new(), test_config());
        let mut msg = sample_message();
        msg.message_type = SlackMessageType::FileShare;
        msg.files = vec![SlackFile {
            id: "F01DEF".into(),
            name: "photo.jpg".into(),
            mimetype: "image/jpeg".into(),
            size: 12345,
            download_url: None,
            blob_ref: Some("blob:sha256:abcdef".into()),
        }];

        let draft = adapter.map_message(&msg).unwrap();
        assert_eq!(draft.attachments.len(), 1);
        assert_eq!(draft.attachments[0].as_str(), "blob:sha256:abcdef");
        assert!(draft.payload["files"].is_array());
    }

    #[test]
    fn map_channel_snapshot() {
        let adapter = SlackAdapter::new(FixtureSlackClient::new(), test_config());
        let snap = SlackChannelSnapshot {
            channel_id: "C01ABC".into(),
            channel_name: "general".into(),
            purpose: Some("General discussion".into()),
            topic: Some("Welcome!".into()),
            member_count: 42,
            members: vec!["U01".into(), "U02".into()],
            is_archived: false,
            snapshot_at: Utc::now(),
        };

        let draft = adapter.map_channel_snapshot(&snap);
        assert_eq!(draft.schema.as_str(), SLACK_CHANNEL_SCHEMA);
        assert_eq!(draft.payload["channel_id"], "C01ABC");
        assert_eq!(draft.payload["member_count"], 42);
    }

    #[test]
    fn same_message_same_idempotency_key() {
        let adapter = SlackAdapter::new(FixtureSlackClient::new(), test_config());
        let msg = sample_message();
        let d1 = adapter.map_message(&msg).unwrap();
        let d2 = adapter.map_message(&msg).unwrap();
        assert_eq!(d1.idempotency_key, d2.idempotency_key);
    }

    #[test]
    fn reactions_do_not_change_message_identity_key() {
        let adapter = SlackAdapter::new(FixtureSlackClient::new(), test_config());
        let msg = sample_message();
        let mut with_reaction = msg.clone();
        with_reaction.reactions = vec![SlackReaction {
            name: "thumbsup".into(),
            count: 1,
            users: vec!["U02".into()],
        }];

        let d1 = adapter.map_message(&msg).unwrap();
        let d2 = adapter.map_message(&with_reaction).unwrap();

        lethe_adapter_api::conformance::canonical_identity_stable_under_side_state_change(&d1, &d2);
    }

    #[test]
    fn edit_wrapper_without_body_change_does_not_change_message_identity_key() {
        let adapter = SlackAdapter::new(FixtureSlackClient::new(), test_config());
        let msg = sample_message();
        let mut wrapper_changed = msg.clone();
        wrapper_changed.message_type = SlackMessageType::Edit;
        wrapper_changed.edited = Some(SlackEdited {
            user: "U01XYZ".into(),
            ts: "1234567891.000000".into(),
        });

        let d1 = adapter.map_message(&msg).unwrap();
        let d2 = adapter.map_message(&wrapper_changed).unwrap();

        lethe_adapter_api::conformance::canonical_identity_stable_under_side_state_change(&d1, &d2);
    }

    #[test]
    fn body_edit_changes_message_identity_key() {
        let adapter = SlackAdapter::new(FixtureSlackClient::new(), test_config());
        let msg = sample_message();
        let mut edited = msg.clone();
        edited.text = "Hello everyone! edited".into();
        edited.message_type = SlackMessageType::Edit;
        edited.edited = Some(SlackEdited {
            user: "U01XYZ".into(),
            ts: "1234567891.000000".into(),
        });

        let d1 = adapter.map_message(&msg).unwrap();
        let d2 = adapter.map_message(&edited).unwrap();

        lethe_adapter_api::conformance::canonical_identity_changes_on_content_change(&d1, &d2);
    }

    #[test]
    fn fetch_incremental_requires_explicit_channel_context() {
        let page = SlackHistoryPage {
            messages: vec![sample_message()],
            has_more: false,
            next_cursor: None,
        };
        let client = FixtureSlackClient::new().with_history(vec![page]);
        let adapter = SlackAdapter::new(client, test_config());

        let result = adapter.fetch_incremental(None);
        match result {
            FetchResult::Error(AdapterError::Other(message)) => {
                assert!(message.contains("explicit channel ID"));
            }
            FetchResult::Ok { .. } => panic!("expected unsupported fetch_incremental error"),
            FetchResult::Error(e) => panic!("unexpected error: {e}"),
        }
    }

    #[test]
    fn to_observations_round_trip() {
        let adapter = SlackAdapter::new(FixtureSlackClient::new(), test_config());
        let msg = sample_message();
        let raw = RawData {
            data: serde_json::to_value(&msg).unwrap(),
            blobs: vec![],
        };
        let drafts = adapter.to_observations(&raw).unwrap();
        assert_eq!(drafts.len(), 1);
        assert_eq!(drafts[0].payload["text"], "Hello everyone!");
    }

    #[test]
    fn heartbeat_generated() {
        let adapter = SlackAdapter::new(FixtureSlackClient::new(), test_config());
        let hb = adapter.heartbeat();
        assert_eq!(hb.schema.as_str(), "schema:observer-heartbeat");
        assert_eq!(hb.payload["status"], "alive");
    }

    #[test]
    fn cursor_management() {
        let mut adapter = SlackAdapter::new(FixtureSlackClient::new(), test_config());
        assert!(adapter.get_cursor("C01ABC").is_none());

        adapter.update_cursor("C01ABC", "1234567890.123456");
        assert_eq!(adapter.get_cursor("C01ABC"), Some("1234567890.123456"));

        adapter.update_cursor("C01ABC", "1234567891.000000");
        assert_eq!(adapter.get_cursor("C01ABC"), Some("1234567891.000000"));
    }

    #[test]
    fn adapter_metadata_in_observations() {
        let adapter = SlackAdapter::new(FixtureSlackClient::new(), test_config());
        let draft = adapter.map_message(&sample_message()).unwrap();
        assert_eq!(draft.meta["sourceAdapterVersion"], "1.0.0");
        assert_eq!(draft.schema_version.as_str(), SLACK_MESSAGE_SCHEMA_VERSION);
    }

    #[test]
    fn invalid_timestamp_is_rejected_without_wall_clock_fallback() {
        let adapter = SlackAdapter::new(FixtureSlackClient::new(), test_config());
        let mut msg = sample_message();
        msg.ts = "not-a-slack-timestamp".to_string();

        let err = adapter.map_message(&msg).unwrap_err();

        assert!(matches!(err, AdapterError::MalformedResponse { .. }));
    }

    #[test]
    fn parse_slack_ts_works() {
        let dt = parse_slack_ts("1234567890.123456").unwrap();
        assert_eq!(dt.timestamp(), 1234567890);
    }

    #[test]
    fn parse_slack_ts_rejects_missing_or_malformed_fraction() {
        assert!(parse_slack_ts("1234567890").is_none());
        assert!(parse_slack_ts("1234567890.123").is_none());
        assert!(parse_slack_ts("1234567890.abcdef").is_none());
    }
}
