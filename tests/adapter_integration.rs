//! Integration tests: Adapter → Lake ingest pipeline
//!
//! Verifies that:
//! 1. Slack adapter observations can be ingested through the IngestionGate
//! 2. Google Slides adapter observations can be ingested
//! 3. Duplicate re-sends are correctly deduplicated
//! 4. Heartbeat observations are ingested
//! 5. Replay produces identical results

use chrono::Utc;
use lethe::adapter::claude::importer::*;
use lethe::adapter::config::*;
use lethe::adapter::gslides::client::*;
use lethe::adapter::gslides::mapper::*;
use lethe::adapter::slack::client::*;
use lethe::adapter::slack::mapper::*;
use lethe::adapter::traits::*;
use lethe::domain::*;
use lethe::lake::*;
use lethe::registry::*;
use std::time::Duration;

// ===========================================================================
// Helpers
// ===========================================================================

fn setup_registry_for_slack() -> RegistryStore {
    let mut reg = RegistryStore::new();
    reg.register_source_system(SourceSystem {
        id: SourceSystemRef::new("sys:slack"),
        name: "Slack".into(),
        provider: Some("Slack".into()),
        api_version: Some("v1".into()),
        source_class: SourceClass::ImmutableText,
    })
    .unwrap();
    reg.register_observer(Observer {
        id: ObserverRef::new("obs:slack-crawler"),
        name: "Slack Crawler".into(),
        observer_type: ObserverType::Crawler,
        source_system: SourceSystemRef::new("sys:slack"),
        adapter_version: SemVer::new("1.0.0"),
        schemas: vec![
            SchemaRef::new("schema:slack-message"),
            SchemaRef::new("schema:slack-channel-snapshot"),
            SchemaRef::new("schema:observer-heartbeat"),
        ],
        authority_model: AuthorityModel::LakeAuthoritative,
        capture_model: CaptureModel::Event,
        owner: "lethe".into(),
        trust_level: TrustLevel::Automated,
    })
    .unwrap();
    reg.register_schema(ObservationSchema {
        id: SchemaRef::new("schema:slack-message"),
        name: "Slack Message".into(),
        version: SemVer::new("1.0.0"),
        subject_type: EntityTypeRef::new("et:message"),
        target_type: None,
        payload_schema: serde_json::json!({"type": "object"}),
        source_contracts: vec![],
        attachment_config: None,
        registered_by: None,
        registered_at: None,
    })
    .unwrap();
    reg.register_schema(ObservationSchema {
        id: SchemaRef::new("schema:slack-channel-snapshot"),
        name: "Slack Channel Snapshot".into(),
        version: SemVer::new("1.0.0"),
        subject_type: EntityTypeRef::new("et:*"),
        target_type: None,
        payload_schema: serde_json::json!({"type": "object"}),
        source_contracts: vec![],
        attachment_config: None,
        registered_by: None,
        registered_at: None,
    })
    .unwrap();
    reg.register_schema(ObservationSchema {
        id: SchemaRef::new("schema:observer-heartbeat"),
        name: "Observer Heartbeat".into(),
        version: SemVer::new("1.0.0"),
        subject_type: EntityTypeRef::new("et:observer"),
        target_type: None,
        payload_schema: serde_json::json!({"type": "object"}),
        source_contracts: vec![],
        attachment_config: None,
        registered_by: None,
        registered_at: None,
    })
    .unwrap();
    reg
}

fn setup_registry_for_gslides() -> RegistryStore {
    let mut reg = RegistryStore::new();
    reg.register_source_system(SourceSystem {
        id: SourceSystemRef::new("sys:google-slides"),
        name: "Google Slides".into(),
        provider: Some("Google".into()),
        api_version: Some("v1".into()),
        source_class: SourceClass::MutableMultimodal,
    })
    .unwrap();
    reg.register_observer(Observer {
        id: ObserverRef::new("obs:gslides-crawler"),
        name: "GSlides Crawler".into(),
        observer_type: ObserverType::Crawler,
        source_system: SourceSystemRef::new("sys:google-slides"),
        adapter_version: SemVer::new("1.0.0"),
        schemas: vec![
            SchemaRef::new("schema:workspace-object-snapshot"),
            SchemaRef::new("schema:observer-heartbeat"),
        ],
        authority_model: AuthorityModel::SourceAuthoritative,
        capture_model: CaptureModel::Snapshot,
        owner: "lethe".into(),
        trust_level: TrustLevel::Automated,
    })
    .unwrap();
    reg.register_schema(ObservationSchema {
        id: SchemaRef::new("schema:workspace-object-snapshot"),
        name: "Workspace Object Snapshot".into(),
        version: SemVer::new("1.0.0"),
        subject_type: EntityTypeRef::new("et:document"),
        target_type: None,
        payload_schema: serde_json::json!({"type": "object"}),
        source_contracts: vec![],
        attachment_config: None,
        registered_by: None,
        registered_at: None,
    })
    .unwrap();
    // Heartbeat schema needed for gslides observer too
    reg.register_schema(ObservationSchema {
        id: SchemaRef::new("schema:observer-heartbeat"),
        name: "Observer Heartbeat".into(),
        version: SemVer::new("1.0.0"),
        subject_type: EntityTypeRef::new("et:observer"),
        target_type: None,
        payload_schema: serde_json::json!({"type": "object"}),
        source_contracts: vec![],
        attachment_config: None,
        registered_by: None,
        registered_at: None,
    })
    .unwrap();
    reg
}

fn setup_registry_for_claude() -> RegistryStore {
    let mut reg = RegistryStore::new();
    reg.register_source_system(SourceSystem {
        id: SourceSystemRef::new("sys:claude-ai"),
        name: "Claude.ai".into(),
        provider: Some("Anthropic".into()),
        api_version: Some("export-v1".into()),
        source_class: SourceClass::MutableText,
    })
    .unwrap();
    reg.register_observer(Observer {
        id: ObserverRef::new("obs:claude-ai-importer"),
        name: "Claude.ai Importer".into(),
        observer_type: ObserverType::Crawler,
        source_system: SourceSystemRef::new("sys:claude-ai"),
        adapter_version: SemVer::new("1.0.0"),
        schemas: vec![SchemaRef::new("schema:claude-message")],
        authority_model: AuthorityModel::LakeAuthoritative,
        capture_model: CaptureModel::Event,
        owner: "lethe".into(),
        trust_level: TrustLevel::Automated,
    })
    .unwrap();
    reg.register_schema(ObservationSchema {
        id: SchemaRef::new("schema:claude-message"),
        name: "Claude.ai Message".into(),
        version: SemVer::new("1.0.0"),
        subject_type: EntityTypeRef::new("et:message"),
        target_type: None,
        payload_schema: serde_json::json!({"type": "object"}),
        source_contracts: vec![],
        attachment_config: None,
        registered_by: None,
        registered_at: None,
    })
    .unwrap();
    reg
}

fn slack_config() -> AdapterConfig {
    AdapterConfig {
        observer_id: ObserverRef::new("obs:slack-crawler"),
        source_system_id: SourceSystemRef::new("sys:slack"),
        adapter_version: SemVer::new("1.0.0"),
        authority_model: AuthorityModel::LakeAuthoritative,
        capture_model: CaptureModel::Event,
        schemas: vec![SchemaRef::new("schema:slack-message")],
        schema_bindings: vec![SchemaBinding {
            schema: SchemaRef::new("schema:slack-message"),
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

fn gslides_config() -> AdapterConfig {
    AdapterConfig {
        observer_id: ObserverRef::new("obs:gslides-crawler"),
        source_system_id: SourceSystemRef::new("sys:google-slides"),
        adapter_version: SemVer::new("1.0.0"),
        authority_model: AuthorityModel::SourceAuthoritative,
        capture_model: CaptureModel::Snapshot,
        schemas: vec![SchemaRef::new("schema:workspace-object-snapshot")],
        schema_bindings: vec![SchemaBinding {
            schema: SchemaRef::new("schema:workspace-object-snapshot"),
            versions: ">=1.0.0 <2.0.0".into(),
        }],
        poll_interval: Duration::from_secs(600),
        heartbeat_interval: Duration::from_secs(60),
        rate_limit: RateLimitConfig {
            requests_per_second: 10,
            burst: 5,
        },
        retry: RetryConfig {
            max_retries: 3,
            backoff: BackoffStrategy::Exponential,
            max_wait: Duration::from_secs(60),
        },
        credential_ref: "secret:google-sa".into(),
    }
}

/// Convert an ObservationDraft to an IngestRequest.
fn draft_to_ingest(draft: &ObservationDraft) -> IngestRequest {
    IngestRequest {
        schema: draft.schema.clone(),
        schema_version: draft.schema_version.clone(),
        observer: draft.observer.clone(),
        source_system: draft.source_system.clone(),
        authority_model: draft.authority_model,
        capture_model: draft.capture_model,
        subject: draft.subject.clone(),
        target: draft.target.clone(),
        payload: draft.payload.clone(),
        attachments: draft.attachments.clone(),
        published: draft.published,
        idempotency_key: draft.idempotency_key.clone(),
        meta: draft.meta.clone(),
    }
}

fn sample_slack_message() -> SlackMessage {
    SlackMessage {
        channel_id: "C01ABC".into(),
        channel_name: "general".into(),
        ts: "1234567890.123456".into(),
        thread_ts: None,
        user_id: "U01XYZ".into(),
        user_name: "tanaka".into(),
        email: Some("tanaka@example.jp".into()),
        text: "Hello everyone!".into(),
        message_type: SlackMessageType::Message,
        edited: None,
        reactions: vec![],
        files: vec![],
        reply_count: 0,
        reply_users_count: 0,
    }
}

// ===========================================================================
// Slack → Lake integration
// ===========================================================================

#[test]
fn slack_message_ingested_through_gate() {
    let reg = setup_registry_for_slack();
    let mut lake = LakeStore::new();
    let blobs = BlobStore::new();

    let adapter = SlackAdapter::new(FixtureSlackClient::new(), slack_config());
    let draft = adapter.map_message(&sample_slack_message());
    let req = draft_to_ingest(&draft);

    let mut gate = IngestionGate {
        registry: &reg,
        lake: &mut lake,
        blobs: &blobs,
    };
    let result = gate.ingest(req);
    assert!(matches!(result, IngestResult::Ingested { .. }));
    assert_eq!(lake.len(), 1);

    let obs = &lake.list()[0];
    assert_eq!(obs.schema.as_str(), "schema:slack-message");
    assert_eq!(obs.payload["text"], "Hello everyone!");
}

#[test]
fn slack_duplicate_resend_deduplicated() {
    let reg = setup_registry_for_slack();
    let mut lake = LakeStore::new();
    let blobs = BlobStore::new();

    let adapter = SlackAdapter::new(FixtureSlackClient::new(), slack_config());
    let draft = adapter.map_message(&sample_slack_message());

    // First ingest
    let mut gate = IngestionGate {
        registry: &reg,
        lake: &mut lake,
        blobs: &blobs,
    };
    let r1 = gate.ingest(draft_to_ingest(&draft));
    assert!(matches!(r1, IngestResult::Ingested { .. }));

    // Second ingest (same idempotency key)
    let mut gate = IngestionGate {
        registry: &reg,
        lake: &mut lake,
        blobs: &blobs,
    };
    let r2 = gate.ingest(draft_to_ingest(&draft));
    assert!(matches!(r2, IngestResult::Duplicate { .. }));
    assert_eq!(lake.len(), 1);
}

#[test]
fn slack_edit_and_delete_are_separate_observations() {
    let reg = setup_registry_for_slack();
    let mut lake = LakeStore::new();
    let blobs = BlobStore::new();

    let adapter = SlackAdapter::new(FixtureSlackClient::new(), slack_config());

    // Original message
    let msg = sample_slack_message();
    let mut gate = IngestionGate {
        registry: &reg,
        lake: &mut lake,
        blobs: &blobs,
    };
    gate.ingest(draft_to_ingest(&adapter.map_message(&msg)));

    // Edit
    let mut edit = msg.clone();
    edit.message_type = SlackMessageType::Edit;
    edit.text = "Hello (edited)".into();
    edit.edited = Some(SlackEdited {
        user: "U01XYZ".into(),
        ts: "1234567891.000000".into(),
    });
    let mut gate = IngestionGate {
        registry: &reg,
        lake: &mut lake,
        blobs: &blobs,
    };
    gate.ingest(draft_to_ingest(&adapter.map_message(&edit)));

    // Delete
    let mut del = msg.clone();
    del.message_type = SlackMessageType::Delete;
    let mut gate = IngestionGate {
        registry: &reg,
        lake: &mut lake,
        blobs: &blobs,
    };
    gate.ingest(draft_to_ingest(&adapter.map_message(&del)));

    assert_eq!(lake.len(), 3);

    // Each has a distinct idempotency key
    let keys: Vec<&str> = lake
        .list()
        .iter()
        .map(|o| o.idempotency_key.as_str())
        .collect();
    assert_eq!(keys.len(), 3);
    assert!(keys[0] != keys[1] && keys[1] != keys[2]);
}

#[test]
fn slack_reaction_change_is_duplicate_not_conflict() {
    let reg = setup_registry_for_slack();
    let mut lake = LakeStore::new();
    let blobs = BlobStore::new();
    let adapter = SlackAdapter::new(FixtureSlackClient::new(), slack_config());
    let msg = sample_slack_message();
    let mut reacted = msg.clone();
    reacted.reactions = vec![SlackReaction {
        name: "thumbsup".into(),
        count: 1,
        users: vec!["U02".into()],
    }];

    let mut gate = IngestionGate {
        registry: &reg,
        lake: &mut lake,
        blobs: &blobs,
    };
    let r1 = gate.ingest(draft_to_ingest(&adapter.map_message(&msg)));
    assert!(matches!(r1, IngestResult::Ingested { .. }));

    let mut gate = IngestionGate {
        registry: &reg,
        lake: &mut lake,
        blobs: &blobs,
    };
    let r2 = gate.ingest(draft_to_ingest(&adapter.map_message(&reacted)));
    assert!(matches!(r2, IngestResult::Duplicate { .. }));
    assert_eq!(lake.len(), 1);
}

#[test]
fn slack_file_share_with_blob() {
    let reg = setup_registry_for_slack();
    let mut lake = LakeStore::new();
    let mut blobs = BlobStore::new();

    let blob_ref = blobs.put(b"fake image data");

    let adapter = SlackAdapter::new(FixtureSlackClient::new(), slack_config());
    let mut msg = sample_slack_message();
    msg.message_type = SlackMessageType::FileShare;
    msg.files = vec![SlackFile {
        id: "F01".into(),
        name: "photo.jpg".into(),
        mimetype: "image/jpeg".into(),
        size: 1234,
        download_url: None,
        blob_ref: Some(blob_ref.as_str().to_string()),
    }];

    let draft = adapter.map_message(&msg);
    let mut gate = IngestionGate {
        registry: &reg,
        lake: &mut lake,
        blobs: &blobs,
    };
    let result = gate.ingest(draft_to_ingest(&draft));
    assert!(matches!(result, IngestResult::Ingested { .. }));
    assert_eq!(lake.list()[0].attachments.len(), 1);
}

#[test]
fn slack_heartbeat_ingested() {
    let reg = setup_registry_for_slack();
    let mut lake = LakeStore::new();
    let blobs = BlobStore::new();

    let adapter = SlackAdapter::new(FixtureSlackClient::new(), slack_config());
    let hb = adapter.heartbeat();

    let mut gate = IngestionGate {
        registry: &reg,
        lake: &mut lake,
        blobs: &blobs,
    };
    let result = gate.ingest(draft_to_ingest(&hb));
    assert!(matches!(result, IngestResult::Ingested { .. }));
    assert_eq!(lake.list()[0].payload["status"], "alive");
}

#[test]
fn slack_channel_snapshot_ingested() {
    let reg = setup_registry_for_slack();
    let mut lake = LakeStore::new();
    let blobs = BlobStore::new();

    let adapter = SlackAdapter::new(FixtureSlackClient::new(), slack_config());
    let snap = SlackChannelSnapshot {
        channel_id: "C01ABC".into(),
        channel_name: "general".into(),
        purpose: Some("General discussion".into()),
        topic: None,
        member_count: 10,
        members: vec!["U01".into()],
        is_archived: false,
        snapshot_at: Utc::now(),
    };
    let draft = adapter.map_channel_snapshot(&snap);

    let mut gate = IngestionGate {
        registry: &reg,
        lake: &mut lake,
        blobs: &blobs,
    };
    let result = gate.ingest(draft_to_ingest(&draft));
    assert!(matches!(result, IngestResult::Ingested { .. }));
}

// ===========================================================================
// Google Slides → Lake integration
// ===========================================================================

#[test]
fn gslides_revision_ingested_through_gate() {
    let reg = setup_registry_for_gslides();
    let mut lake = LakeStore::new();
    let blobs = BlobStore::new();

    let adapter = GoogleSlidesAdapter::new(FixtureGoogleSlidesClient::new(), gslides_config());
    let rev = SlideRevision {
        presentation_id: "pres123".into(),
        revision_id: "rev456".into(),
        modified_time: Utc::now() - chrono::TimeDelta::hours(1),
        last_modifying_user: Some("editor@example.com".into()),
    };
    let meta = PresentationMeta {
        presentation_id: "pres123".into(),
        title: "寮紹介スライド".into(),
        container_id: Some("folder789".into()),
        canonical_uri: "https://docs.google.com/presentation/d/pres123".into(),
        owner: Some("owner@example.com".into()),
        editors: vec!["editor@example.com".into()],
        viewers: vec![],
    };

    let draft = adapter.map_revision(&rev, &meta, None, vec![]);
    let req = draft_to_ingest(&draft);

    let mut gate = IngestionGate {
        registry: &reg,
        lake: &mut lake,
        blobs: &blobs,
    };
    let result = gate.ingest(req);
    assert!(matches!(result, IngestResult::Ingested { .. }));
    assert_eq!(lake.len(), 1);

    let obs = &lake.list()[0];
    assert_eq!(obs.schema.as_str(), "schema:workspace-object-snapshot");
    assert_eq!(obs.authority_model, AuthorityModel::SourceAuthoritative);
    assert_eq!(obs.capture_model, CaptureModel::Snapshot);
}

#[test]
fn gslides_duplicate_revision_deduplicated() {
    let reg = setup_registry_for_gslides();
    let mut lake = LakeStore::new();
    let blobs = BlobStore::new();

    let adapter = GoogleSlidesAdapter::new(FixtureGoogleSlidesClient::new(), gslides_config());
    let rev = SlideRevision {
        presentation_id: "pres123".into(),
        revision_id: "rev456".into(),
        modified_time: Utc::now(),
        last_modifying_user: None,
    };
    let meta = PresentationMeta {
        presentation_id: "pres123".into(),
        title: "Test".into(),
        container_id: None,
        canonical_uri: "https://docs.google.com/presentation/d/pres123".into(),
        owner: None,
        editors: vec![],
        viewers: vec![],
    };

    let draft = adapter.map_revision(&rev, &meta, None, vec![]);

    let mut gate = IngestionGate {
        registry: &reg,
        lake: &mut lake,
        blobs: &blobs,
    };
    gate.ingest(draft_to_ingest(&draft));

    let mut gate = IngestionGate {
        registry: &reg,
        lake: &mut lake,
        blobs: &blobs,
    };
    let r2 = gate.ingest(draft_to_ingest(&draft));
    assert!(matches!(r2, IngestResult::Duplicate { .. }));
    assert_eq!(lake.len(), 1);
}

#[test]
fn gslides_with_blob_attachments() {
    let reg = setup_registry_for_gslides();
    let mut lake = LakeStore::new();
    let mut blobs = BlobStore::new();

    let native_blob = blobs.put(b"{\"slides\":[]}");
    let rendered_blob = blobs.put(b"PNG image data");

    let adapter = GoogleSlidesAdapter::new(FixtureGoogleSlidesClient::new(), gslides_config());
    let rev = SlideRevision {
        presentation_id: "pres123".into(),
        revision_id: "rev789".into(),
        modified_time: Utc::now(),
        last_modifying_user: None,
    };
    let meta = PresentationMeta {
        presentation_id: "pres123".into(),
        title: "Test".into(),
        container_id: None,
        canonical_uri: "https://docs.google.com/presentation/d/pres123".into(),
        owner: None,
        editors: vec![],
        viewers: vec![],
    };

    let draft = adapter.map_revision(&rev, &meta, Some(native_blob), vec![rendered_blob]);

    let mut gate = IngestionGate {
        registry: &reg,
        lake: &mut lake,
        blobs: &blobs,
    };
    let result = gate.ingest(draft_to_ingest(&draft));
    assert!(matches!(result, IngestResult::Ingested { .. }));
    assert_eq!(lake.list()[0].attachments.len(), 2);
}

#[test]
fn gslides_heartbeat_ingested() {
    let reg = setup_registry_for_gslides();
    let mut lake = LakeStore::new();
    let blobs = BlobStore::new();

    let adapter = GoogleSlidesAdapter::new(FixtureGoogleSlidesClient::new(), gslides_config());
    let hb = adapter.heartbeat();

    let mut gate = IngestionGate {
        registry: &reg,
        lake: &mut lake,
        blobs: &blobs,
    };
    let result = gate.ingest(draft_to_ingest(&hb));
    assert!(matches!(result, IngestResult::Ingested { .. }));
}

// ===========================================================================
// Claude.ai → Lake integration
// ===========================================================================

#[test]
fn claude_reexport_missing_message_is_noop() {
    let reg = setup_registry_for_claude();
    let mut lake = LakeStore::new();
    let blobs = BlobStore::new();
    let importer = ClaudeAiImporter::new(SemVer::new("1.0.0"));
    let first_export = serde_json::json!({
        "conversations": [{
            "uuid": "conv-1",
            "messages": [
                {
                    "uuid": "msg-root",
                    "parent_message_uuid": null,
                    "sender": "human",
                    "text": "hello",
                    "created_at": "2026-05-01T10:00:00Z"
                },
                {
                    "uuid": null,
                    "parent_message_uuid": "msg-root",
                    "sender": "assistant",
                    "text": "hi",
                    "created_at": "2026-05-01T10:00:01Z"
                }
            ]
        }]
    })
    .to_string();
    let second_export_missing_reply = serde_json::json!({
        "conversations": [{
            "uuid": "conv-1",
            "messages": [{
                "uuid": "msg-root",
                "parent_message_uuid": null,
                "sender": "human",
                "text": "hello",
                "created_at": "2026-05-01T10:00:00Z"
            }]
        }]
    })
    .to_string();

    for draft in importer.import_json_str(&first_export).unwrap() {
        let mut gate = IngestionGate {
            registry: &reg,
            lake: &mut lake,
            blobs: &blobs,
        };
        assert!(matches!(
            gate.ingest(draft_to_ingest(&draft)),
            IngestResult::Ingested { .. }
        ));
    }
    assert_eq!(lake.len(), 2);

    for draft in importer
        .import_json_str(&second_export_missing_reply)
        .unwrap()
    {
        let mut gate = IngestionGate {
            registry: &reg,
            lake: &mut lake,
            blobs: &blobs,
        };
        assert!(matches!(
            gate.ingest(draft_to_ingest(&draft)),
            IngestResult::Duplicate { .. }
        ));
    }

    assert_eq!(lake.len(), 2);
}

// ===========================================================================
// Cross-adapter / replay tests
// ===========================================================================

#[test]
fn replay_same_input_same_output() {
    // Verify that mapping the same raw data twice produces identical drafts
    // (determinism requirement).
    let adapter = SlackAdapter::new(FixtureSlackClient::new(), slack_config());
    let msg = sample_slack_message();

    let d1 = adapter.map_message(&msg);
    let d2 = adapter.map_message(&msg);

    assert_eq!(d1.idempotency_key, d2.idempotency_key);
    assert_eq!(d1.subject, d2.subject);
    assert_eq!(d1.payload, d2.payload);
    assert_eq!(d1.schema, d2.schema);
    assert_eq!(d1.authority_model, d2.authority_model);
}

#[test]
fn revision_snapshot_vs_event_capture_typed() {
    // Verify that Slack (event) and GSlides (snapshot) produce different
    // capture models in their drafts.
    let slack_adapter = SlackAdapter::new(FixtureSlackClient::new(), slack_config());
    let gslides_adapter =
        GoogleSlidesAdapter::new(FixtureGoogleSlidesClient::new(), gslides_config());

    let slack_draft = slack_adapter.map_message(&sample_slack_message());
    assert_eq!(slack_draft.capture_model, CaptureModel::Event);
    assert_eq!(
        slack_draft.authority_model,
        AuthorityModel::LakeAuthoritative
    );

    let rev = SlideRevision {
        presentation_id: "p1".into(),
        revision_id: "r1".into(),
        modified_time: Utc::now(),
        last_modifying_user: None,
    };
    let meta = PresentationMeta {
        presentation_id: "p1".into(),
        title: "T".into(),
        container_id: None,
        canonical_uri: "https://docs.google.com/presentation/d/p1".into(),
        owner: None,
        editors: vec![],
        viewers: vec![],
    };
    let gslides_draft = gslides_adapter.map_revision(&rev, &meta, None, vec![]);
    assert_eq!(gslides_draft.capture_model, CaptureModel::Snapshot);
    assert_eq!(
        gslides_draft.authority_model,
        AuthorityModel::SourceAuthoritative
    );
}

#[test]
fn watermark_tracks_adapter_ingestion() {
    let reg = setup_registry_for_slack();
    let mut lake = LakeStore::new();
    let blobs = BlobStore::new();

    let adapter = SlackAdapter::new(FixtureSlackClient::new(), slack_config());
    let msg1 = sample_slack_message();
    let mut msg2 = sample_slack_message();
    msg2.ts = "1234567891.000000".into();

    let mut gate = IngestionGate {
        registry: &reg,
        lake: &mut lake,
        blobs: &blobs,
    };
    gate.ingest(draft_to_ingest(&adapter.map_message(&msg1)));

    let wm = lake.watermark().unwrap();

    let mut gate = IngestionGate {
        registry: &reg,
        lake: &mut lake,
        blobs: &blobs,
    };
    gate.ingest(draft_to_ingest(&adapter.map_message(&msg2)));

    let delta = lake.since(wm.position);
    assert_eq!(delta.len(), 1);
    assert_eq!(delta[0].payload["ts"], "1234567891.000000");
}
