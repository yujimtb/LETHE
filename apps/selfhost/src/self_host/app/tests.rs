use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use chrono::Utc;
use lethe_adapter_api::retry::ResilientExecutor;
use lethe_adapter_api::traits::ObservationDraft;
use lethe_adapter_gslides::gslides::client::{PresentationNative, SlideNative, SlideRevision};
use lethe_adapter_slack::slack::client::{SlackMessage, SlackMessageType};
use lethe_core::domain::supplemental::InputAnchorSet;

use super::{
    AppCore, AppService, GoogleSourceRuntime, SelfHostError, SlackSourceRuntime,
    classify_slack_ingress, extract_slide_text_fragments, infer_profile_name_from_fragments,
    known_thread_roots_from_observations, latest_revision_to_capture, namespace_draft,
    non_empty_state, ranked_self_intro_slide_indices, thread_cursor_key, thread_root_ts,
};
use crate::self_host::config::{
    ApiTokenConfig, CorpusProjectionConfig, FreshnessConfig, GoogleConfig, JsonWebKey,
    JsonWebKeySet, McpOAuthConfig, OpsConfig, ResourceLimits, SecretString, SelfHostConfig,
    SlackConfig, SlideAiConfig, SupplementalConfig,
};
use crate::self_host::google::HttpGoogleSlidesClient;
use crate::self_host::slack::HttpSlackClient;
use lethe_core::domain::{
    ActorRef, AuthorityModel, CaptureModel, EntityRef, IdempotencyKey, IngestResult, Mutability,
    Observation, ObserverRef, ProjectionRef, SchemaRef, SemVer, SourceSystemRef, SupplementalId,
    SupplementalRecord,
};
use lethe_derivation_gemini::GeminiSlideAnalyzer;
use lethe_policy::governance::audit::InMemoryAuditLog;
use lethe_runtime::runtime::partition::RoutingKeyOrder;
use lethe_storage_sqlite::persistence::SqlitePersistence;

#[test]
fn non_empty_state_filters_blank_values() {
    assert_eq!(non_empty_state(None), None);
    assert_eq!(non_empty_state(Some(String::new())), None);
    assert_eq!(non_empty_state(Some("   ".to_string())), None);
    assert_eq!(
        non_empty_state(Some("1234567890.123456".to_string())).as_deref(),
        Some("1234567890.123456")
    );
}

#[test]
fn source_instance_namespace_separates_identical_source_keys() {
    let draft = ObservationDraft {
        schema: SchemaRef::new("schema:test"),
        schema_version: SemVer::new("1.0.0"),
        observer: ObserverRef::new("obs:test"),
        source_system: Some(SourceSystemRef::new("sys:test")),
        authority_model: AuthorityModel::LakeAuthoritative,
        capture_model: CaptureModel::Event,
        subject: EntityRef::new("entity:test"),
        target: None,
        payload: serde_json::json!({}),
        attachments: vec![],
        published: Utc::now(),
        idempotency_key: IdempotencyKey::new("same-key"),
        meta: serde_json::json!({
            "canonical_json": "{}",
            "source_container": "same-container",
        }),
    };

    let first = namespace_draft(draft.clone(), "instance-a");
    let second = namespace_draft(draft, "instance-b");

    assert_ne!(first.idempotency_key, second.idempotency_key);
    assert_eq!(
        first
            .meta
            .get("source_container")
            .and_then(serde_json::Value::as_str),
        Some("instance-a:same-container")
    );
}

#[test]
fn app_core_new_rejects_duplicate_persisted_observations() {
    fn observation(id: &str, key: &str) -> Observation {
        Observation {
            id: Observation::new_id(),
            schema: SchemaRef::new("schema:test"),
            schema_version: SemVer::new("1.0.0"),
            observer: ObserverRef::new("obs:test"),
            source_system: None,
            actor: None,
            authority_model: AuthorityModel::LakeAuthoritative,
            capture_model: CaptureModel::Event,
            subject: EntityRef::new(format!("entity:{id}")),
            target: None,
            payload: serde_json::json!({ "id": id }),
            attachments: vec![],
            published: Utc::now(),
            recorded_at: Utc::now(),
            consent: None,
            idempotency_key: IdempotencyKey::new(key),
            meta: serde_json::json!({
                "canonical_json": serde_json::json!({
                    "source": "test",
                    "object_id": key,
                    "body": "duplicate"
                }).to_string(),
            }),
        }
    }

    let observations = vec![observation("one", "dup-key"), observation("two", "dup-key")];

    let err = AppCore::new(observations, vec![], vec![]).unwrap_err();
    assert!(matches!(err, SelfHostError::Ingestion(_)));
}

#[test]
fn latest_revision_to_capture_prefers_newest_revision() {
    let revisions = vec![
        SlideRevision {
            presentation_id: "pres-1".into(),
            revision_id: "rev-1".into(),
            modified_time: chrono::DateTime::parse_from_rfc3339("2026-03-24T10:00:00Z")
                .unwrap()
                .to_utc(),
            last_modifying_user: None,
        },
        SlideRevision {
            presentation_id: "pres-1".into(),
            revision_id: "rev-2".into(),
            modified_time: chrono::DateTime::parse_from_rfc3339("2026-03-24T11:00:00Z")
                .unwrap()
                .to_utc(),
            last_modifying_user: None,
        },
    ];

    assert_eq!(
        latest_revision_to_capture(&revisions).map(|revision| revision.revision_id.as_str()),
        Some("rev-2")
    );
}

fn test_config(db: PathBuf, blobs: PathBuf) -> SelfHostConfig {
    SelfHostConfig {
        bind_addr: "127.0.0.1:0".into(),
        mcp_bind_addr: "127.0.0.1:0".into(),
        mcp_oauth: test_mcp_oauth(),
        database_path: db.clone(),
        blob_dir: blobs,
        secret_encryption_key: [7; 32],
        poll_interval: std::time::Duration::from_secs(300),
        routing_key_order: RoutingKeyOrder::MonthYearSourceContainerPublished,
        api_tokens: vec![ApiTokenConfig {
            token: SecretString::new("test-api-token").unwrap(),
            scopes: vec!["*".into()],
        }],
        resource_limits: ResourceLimits {
            max_blob_bytes: 10 * 1024 * 1024,
            max_payload_bytes: 1024 * 1024,
            max_sync_items: 10_000,
            max_page_size: 100,
            max_leaf_observations: 100_000,
            retention_days: 30,
        },
        corpus: CorpusProjectionConfig {
            mode: lethe_projection_corpus::CorpusMode::WorkspaceFiltered,
            index_dir: db.with_extension("corpus-index"),
            writer_heap_bytes: 32 * 1024 * 1024,
            rebuild_page_size: 512,
        },
        freshness: FreshnessConfig {
            threshold_seconds: std::collections::BTreeMap::from([
                ("sys:claude-ai".to_owned(), 36 * 3600),
                ("sys:chatgpt".to_owned(), 36 * 3600),
                ("sys:claude-code".to_owned(), 48 * 3600),
                ("sys:codex".to_owned(), 48 * 3600),
            ]),
        },
        ops: OpsConfig {
            backfill_nightly_budget_items: 1000,
        },
        slack_sources: vec![SlackConfig {
            id: "slack-test".into(),
            bot_token: SecretString::new("xoxb-test-token").unwrap(),
            thread_token: SecretString::new("xoxp-test-thread-token").unwrap(),
            channel_ids: vec!["C01ABC".into()],
            mention_user_ids: vec!["U-BOT".into()],
        }],
        channels: test_channels(),
        google_sources: vec![GoogleConfig {
            id: "google-test".into(),
            access_token: Some(SecretString::new("ya29.test-token").unwrap()),
            client_id: None,
            client_secret: None,
            refresh_token: None,
            presentation_ids: vec!["pres123".into()],
        }],
        slide_analysis_limit: Some(10),
        slide_ai: Some(SlideAiConfig {
            api_key: SecretString::new("test-gemini-key").unwrap(),
            model: "test-gemini-model".into(),
        }),
        supplemental: SupplementalConfig {
            reject_unregistered_kinds: true,
        },
    }
}

fn test_mcp_oauth() -> McpOAuthConfig {
    McpOAuthConfig {
        resource_url: "https://mcp.example.test".into(),
        protected_resource_metadata_url:
            "https://mcp.example.test/.well-known/oauth-protected-resource".into(),
        issuer: "https://issuer.example.test/".into(),
        audience: "lethe-test".into(),
        jwks_path: PathBuf::from("test-jwks.json"),
        jwks: JsonWebKeySet {
            keys: vec![JsonWebKey {
                kty: "EC".into(),
                kid: "test-key".into(),
                alg: Some("ES256".into()),
                crv: Some("P-256".into()),
                x: Some("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA".into()),
                y: Some("BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB".into()),
                n: None,
                e: None,
            }],
        },
    }
}

fn test_service(config: SelfHostConfig, persistence: SqlitePersistence) -> AppService {
    let persistence: Arc<Mutex<Box<dyn lethe_storage_api::StoragePorts>>> =
        Arc::new(Mutex::new(Box::new(persistence)));
    let corpus_config = config.corpus.projector_config();
    let search_index = super::search_index::SearchIndexManager::bootstrap(
        lethe_search_index::IndexRoot::new(
            &config.corpus.index_dir,
            config.corpus.writer_heap_bytes,
            corpus_config.fingerprint(),
        )
        .unwrap(),
        lethe_projection_corpus::CorpusProjector::new(corpus_config),
        config.corpus.rebuild_page_size,
        Arc::clone(&persistence),
    );
    AppService {
        core: Arc::new(Mutex::new(
            AppCore::new_with_config(
                vec![],
                vec![],
                vec![],
                super::freshness_thresholds(&config),
                config.channels.clone(),
            )
            .unwrap(),
        )),
        persistence,
        search_index,
        config: Arc::new(config.clone()),
        slack_sources: vec![SlackSourceRuntime {
            config: config.slack_sources[0].clone(),
            client: HttpSlackClient::new(config.slack_sources[0].bot_token.expose().to_owned())
                .unwrap(),
            replies_client: HttpSlackClient::new(
                config.slack_sources[0].thread_token.expose().to_owned(),
            )
            .unwrap(),
        }],
        google_sources: vec![GoogleSourceRuntime {
            config: config.google_sources[0].clone(),
            client: HttpGoogleSlidesClient::new(&config.google_sources[0]).unwrap(),
        }],
        slide_analyzer: config
            .slide_ai
            .as_ref()
            .map(|slide_ai| GeminiSlideAnalyzer::new(slide_ai.api_key.expose(), &slide_ai.model))
            .transpose()
            .unwrap(),
        resilient_executor: Arc::new(ResilientExecutor::new(
            3,
            std::time::Duration::from_secs(60),
        )),
        audit_log: Arc::new(InMemoryAuditLog::new()),
        non_corpus_rebuild_count: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
    }
}

fn wait_for_search_index_ready(service: &AppService) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while service.search_index.health_dependency().status != "ok" {
        assert!(
            std::time::Instant::now() < deadline,
            "search index did not become ready"
        );
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
}

fn test_channels() -> Vec<lethe_registry::registry::ChannelRecord> {
    vec![
        lethe_registry::registry::ChannelRecord {
            id: "chan:slack-test:C01ABC".into(),
            kind: lethe_registry::registry::ChannelKind::Slack,
            source_instance_id: "slack-test".into(),
            external_id: "C01ABC".into(),
            connection_ref: "source:slack-test".into(),
            default_consent_scope: "org_federated".into(),
            reply_slo_seconds: 1800,
            freshness_threshold_seconds: 1800,
            break_glass_channel: false,
            break_glass_senders: vec!["U-URGENT".into()],
            enabled: true,
        },
        lethe_registry::registry::ChannelRecord {
            id: "chan:slack-test:payload-limit".into(),
            kind: lethe_registry::registry::ChannelKind::Slack,
            source_instance_id: "payload-limit-test".into(),
            external_id: "C01ABC".into(),
            connection_ref: "source:payload-limit-test".into(),
            default_consent_scope: "personal".into(),
            reply_slo_seconds: 1800,
            freshness_threshold_seconds: 1800,
            break_glass_channel: false,
            break_glass_senders: vec![],
            enabled: true,
        },
    ]
}

#[test]
fn bootstrap_rebuilds_snapshot_from_persisted_observations() {
    let root = std::env::temp_dir().join(format!("lethe-self-host-test-{}", uuid::Uuid::now_v7()));
    let db = root.join("lethe.sqlite3");
    let blobs = root.join("blobs");
    let persistence = SqlitePersistence::open(&db, &blobs, &[7; 32]).unwrap();
    let observation = Observation {
        id: Observation::new_id(),
        schema: SchemaRef::new("schema:slack-message"),
        schema_version: SemVer::new("1.0.0"),
        observer: ObserverRef::new("obs:slack-crawler"),
        source_system: Some(SourceSystemRef::new("sys:slack")),
        actor: None,
        authority_model: AuthorityModel::LakeAuthoritative,
        capture_model: CaptureModel::Event,
        subject: EntityRef::new("message:slack:C01ABC-bootstrap"),
        target: None,
        payload: serde_json::json!({
            "channel_id": "C01ABC",
            "channel_name": "general",
            "ts": "bootstrap",
            "thread_ts": "bootstrap",
            "user_id": "U-BOOTSTRAP",
            "user_name": "Bootstrap User",
            "email": "bootstrap@example.test",
            "text": "persisted bootstrap needle",
        }),
        attachments: vec![],
        published: Utc::now(),
        recorded_at: Utc::now(),
        consent: None,
        idempotency_key: IdempotencyKey::new("slack:C01ABC:bootstrap"),
        meta: serde_json::json!({
            "canonical_json": serde_json::json!({
                "source": "slack",
                "object_id": "bootstrap-needle",
                "body": "persisted bootstrap needle"
            }).to_string(),
            "source_container": "C01ABC",
        }),
    };
    persistence.persist_observation(&observation).unwrap();
    drop(persistence);

    let mut config = test_config(db.clone(), blobs.clone());
    config.corpus.mode = lethe_projection_corpus::CorpusMode::PersonalAllText;
    let service = AppService::bootstrap(config.clone()).unwrap();
    let built_at = service.core_lock().unwrap().snapshot.built_at;
    let materialized = service
        .persistence_lock()
        .unwrap()
        .projection_records(&ProjectionRef::new("proj:person-page"))
        .unwrap()
        .unwrap();
    assert_eq!(materialized["format_version"], 6);
    assert_eq!(materialized["observation_count"], 1);
    assert_eq!(materialized["last_append_seq"], 1);
    assert_eq!(materialized["person_message_count"], 1);
    assert_eq!(materialized["reply_slo_count"], 0);
    assert!(
        materialized["snapshot"]["person_page"]["messages"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert!(
        materialized["snapshot"]["reply_slo"]["rows"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert!(
        materialized["snapshot"]["reply_slo"]["overdue"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        service
            .persistence_lock()
            .unwrap()
            .projection_item_count(&ProjectionRef::new("proj:person-page"))
            .unwrap(),
        1
    );
    let person_id = service.core_lock().unwrap().snapshot.person_page.profiles[0]
        .person_id
        .as_str()
        .to_owned();
    assert_eq!(
        service
            .person_messages_response(&person_id, None, None)
            .unwrap()
            .data
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert!(materialized["snapshot"].get("corpus").is_none());
    let request = lethe_api::api::grep::GrepRequest {
        pattern: "bootstrap needle".into(),
        limit: Some(3),
        ..lethe_api::api::grep::GrepRequest::default()
    };
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    let response = loop {
        match service.corpus_grep_response(&request) {
            Ok(response) => break response,
            Err(SelfHostError::SearchIndexUnavailable { .. })
                if std::time::Instant::now() < deadline =>
            {
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
            Err(error) => panic!("corpus index did not become ready: {error}"),
        }
    };

    assert_eq!(response.data.matches.len(), 1);
    assert!(
        response
            .data
            .projection_watermark
            .starts_with("proj:corpus:")
    );
    let (combined_response, source_summaries) = service
        .corpus_grep_response_with_source_summaries(&request)
        .unwrap();
    assert_eq!(combined_response.data.matches.len(), 1);
    assert_eq!(
        source_summaries
            .iter()
            .map(|summary| summary.records)
            .sum::<usize>(),
        1
    );
    drop(service);

    let restarted = AppService::bootstrap(config).unwrap();
    assert_eq!(restarted.core_lock().unwrap().snapshot.built_at, built_at);
    assert!(
        restarted
            .core_lock()
            .unwrap()
            .snapshot
            .person_page
            .messages
            .is_empty()
    );
    assert_eq!(
        restarted
            .person_messages_response(&person_id, None, None)
            .unwrap()
            .data
            .as_array()
            .unwrap()
            .len(),
        1
    );

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn thread_root_ts_returns_parent_thread_identifier() {
    let message = SlackMessage {
        channel_id: "C01ABC".into(),
        channel_name: "general".into(),
        ts: "1234567890.123456".into(),
        thread_ts: None,
        user_id: "U1".into(),
        user_name: "alice".into(),
        email: None,
        text: "hello".into(),
        ingress_kind: Some(lethe_adapter_slack::slack::client::SlackIngressKind::Channel),
        mentions: vec![],
        message_type: SlackMessageType::Message,
        edited: None,
        reactions: vec![],
        files: vec![],
        reply_count: 2,
        reply_users_count: 1,
    };

    assert_eq!(thread_root_ts(&message), Some("1234567890.123456"));
}

#[test]
fn classify_slack_ingress_distinguishes_dm_mention_and_channel() {
    assert_eq!(
        classify_slack_ingress("D01", &[], &["U-BOT".into()]),
        lethe_adapter_slack::slack::client::SlackIngressKind::DirectMessage
    );
    assert_eq!(
        classify_slack_ingress("C01", &["U-BOT".into()], &["U-BOT".into()]),
        lethe_adapter_slack::slack::client::SlackIngressKind::Mention
    );
    assert_eq!(
        classify_slack_ingress("C01", &[], &["U-BOT".into()]),
        lethe_adapter_slack::slack::client::SlackIngressKind::Channel
    );
}

#[test]
fn thread_cursor_key_is_stable() {
    assert_eq!(
        thread_cursor_key("C01ABC", "1234567890.123456"),
        "slack:C01ABC:thread:1234567890.123456:oldest_ts"
    );
}

#[test]
fn known_thread_roots_from_observations_finds_thread_parents() {
    fn slack_observation(
        channel_id: &str,
        ts: &str,
        thread_ts: Option<&str>,
        reply_count: Option<u64>,
    ) -> Observation {
        let mut payload = serde_json::json!({
            "channel_id": channel_id,
            "ts": ts,
            "text": "hello",
        });
        if let Some(thread_ts) = thread_ts {
            payload["thread_ts"] = serde_json::json!(thread_ts);
        }
        if let Some(reply_count) = reply_count {
            payload["reply_count"] = serde_json::json!(reply_count);
        }

        Observation {
            id: Observation::new_id(),
            schema: SchemaRef::new("schema:slack-message"),
            schema_version: SemVer::new("1.0.0"),
            observer: ObserverRef::new("obs:slack-crawler"),
            source_system: Some(SourceSystemRef::new("sys:slack")),
            actor: None,
            authority_model: AuthorityModel::LakeAuthoritative,
            capture_model: CaptureModel::Event,
            subject: EntityRef::new(format!("message:slack:{channel_id}:{ts}")),
            target: None,
            payload,
            attachments: vec![],
            published: Utc::now(),
            recorded_at: Utc::now(),
            consent: None,
            idempotency_key: IdempotencyKey::new(format!("slack:{channel_id}:{ts}")),
            meta: serde_json::json!({}),
        }
    }

    let roots = known_thread_roots_from_observations(
        &[
            slack_observation("C01ABC", "100.000001", None, Some(2)),
            slack_observation("C01ABC", "101.000001", Some("100.000001"), None),
            slack_observation("C02XYZ", "200.000001", None, Some(3)),
            slack_observation("C01ABC", "102.000001", None, Some(0)),
        ],
        "C01ABC",
    );

    assert_eq!(
        roots,
        std::collections::BTreeSet::from(["100.000001".to_string()])
    );
}

#[test]
fn ranked_self_intro_slide_indices_prioritize_profile_like_slides() {
    let presentation = PresentationNative {
        presentation_id: "deck-1".into(),
        title: "2026 Slides".into(),
        locale: None,
        slides: vec![
            SlideNative {
                object_id: "agenda".into(),
                page_elements: vec![serde_json::json!({
                    "shape": {
                        "text": {
                            "textElements": [{ "textRun": { "content": "Agenda\n" } }]
                        }
                    }
                })],
            },
            SlideNative {
                object_id: "profile".into(),
                page_elements: vec![
                    serde_json::json!({
                        "shape": {
                            "text": {
                                "textElements": [
                                    { "textRun": { "content": "自己紹介\n" } },
                                    { "textRun": { "content": "田中太郎\n" } },
                                    { "textRun": { "content": "tanaka@example.jp\n" } },
                                    { "textRun": { "content": "趣味: 写真\n" } }
                                ]
                            }
                        }
                    }),
                    serde_json::json!({ "image": { "contentUrl": "https://example.com/pic.png" } }),
                ],
            },
        ],
        page_size: None,
    };

    let ranked = ranked_self_intro_slide_indices(&presentation, 2);
    assert_eq!(ranked[0], 1);
}

#[test]
fn ranked_self_intro_slide_indices_include_lower_scoring_slides_within_limit() {
    let presentation = PresentationNative {
        presentation_id: "deck-2".into(),
        title: "2026 Slides".into(),
        locale: None,
        slides: vec![
            SlideNative {
                object_id: "profile".into(),
                page_elements: vec![serde_json::json!({
                    "shape": {
                        "text": {
                            "textElements": [
                                { "textRun": { "content": "自己紹介\n" } },
                                { "textRun": { "content": "田中太郎\n" } }
                            ]
                        }
                    }
                })],
            },
            SlideNative {
                object_id: "neutral".into(),
                page_elements: vec![serde_json::json!({
                    "shape": {
                        "text": {
                            "textElements": [{ "textRun": { "content": "写真\n" } }]
                        }
                    }
                })],
            },
        ],
        page_size: None,
    };

    let ranked = ranked_self_intro_slide_indices(&presentation, 2);
    assert_eq!(ranked, vec![0, 1]);
}

#[test]
fn extract_slide_text_fragments_and_name_inference_use_text_runs() {
    let slide = SlideNative {
        object_id: "profile".into(),
        page_elements: vec![serde_json::json!({
            "shape": {
                "text": {
                    "textElements": [
                        { "textRun": { "content": "田中太郎\n" } },
                        { "textRun": { "content": "自己紹介\n" } }
                    ]
                }
            }
        })],
    };

    let fragments = extract_slide_text_fragments(&slide);
    assert!(fragments.iter().any(|fragment| fragment == "田中太郎"));
    assert_eq!(
        infer_profile_name_from_fragments(&fragments).as_deref(),
        Some("田中太郎")
    );
}

#[test]
fn ingest_draft_duplicate_is_decided_by_persistence_without_cache_append() {
    let root = std::env::temp_dir().join(format!("lethe-self-host-test-{}", uuid::Uuid::now_v7()));
    let db = root.join("lethe.sqlite3");
    let blobs = root.join("blobs");
    let persistence = SqlitePersistence::open(&db, &blobs, &[7; 32]).unwrap();
    let persisted_observation = Observation {
        id: Observation::new_id(),
        schema: SchemaRef::new("schema:slack-message"),
        schema_version: SemVer::new("1.0.0"),
        observer: ObserverRef::new("obs:slack-crawler"),
        source_system: Some(SourceSystemRef::new("sys:slack")),
        actor: None,
        authority_model: AuthorityModel::LakeAuthoritative,
        capture_model: CaptureModel::Event,
        subject: EntityRef::new("message:slack:existing"),
        target: None,
        payload: serde_json::json!({"text": "persisted"}),
        attachments: vec![],
        published: Utc::now(),
        recorded_at: Utc::now(),
        consent: None,
        idempotency_key: IdempotencyKey::new("slack:C01ABC:dup-ts"),
        meta: serde_json::json!({
            "canonical_json": serde_json::json!({
                "source": "slack",
                "object_id": "channel:C01ABC:ts:dup-ts",
                "body": "persisted"
            }).to_string(),
            "source_container": "slack-test:C01ABC",
        }),
    };
    persistence
        .persist_observation(&persisted_observation)
        .unwrap();

    let config = test_config(db.clone(), blobs.clone());
    let service = test_service(config, persistence);

    let draft = ObservationDraft {
        schema: SchemaRef::new("schema:slack-message"),
        schema_version: SemVer::new("1.0.0"),
        observer: ObserverRef::new("obs:slack-crawler"),
        source_system: Some(SourceSystemRef::new("sys:slack")),
        authority_model: AuthorityModel::LakeAuthoritative,
        capture_model: CaptureModel::Event,
        subject: EntityRef::new("message:slack:new"),
        target: None,
        payload: serde_json::json!({
            "channel_id": "C01ABC",
            "channel_name": "general",
            "ts": "dup-ts",
            "user_id": "U1",
            "user_name": "alice",
            "text": "new"
        }),
        attachments: vec![],
        published: Utc::now(),
        idempotency_key: IdempotencyKey::new("slack:C01ABC:dup-ts"),
        meta: serde_json::json!({
            "canonical_json": serde_json::json!({
                "source": "slack",
                "object_id": "channel:C01ABC:ts:dup-ts",
                "body": "persisted"
            }).to_string(),
            "source_container": "slack-test:C01ABC",
            "source_instance": "slack-test",
        }),
    };

    let result = service.ingest_draft(draft).unwrap();
    assert!(matches!(result, IngestResult::Duplicate { .. }));
    assert_eq!(
        service
            .persistence_lock()
            .unwrap()
            .observation_page(0, 10)
            .unwrap()
            .len(),
        1
    );

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn app_core_holds_only_materialized_non_corpus_state() {
    let core = AppCore::new(vec![], vec![], vec![]).unwrap();
    let snapshot = serde_json::to_value(&core.snapshot).unwrap();

    assert!(snapshot.get("corpus").is_none());
    assert!(core.snapshot.lineage.input_refs.is_empty());
    assert_eq!(core.observation_stats.count, 0);
    assert_eq!(core.observation_stats.max_append_seq, 0);
}

#[test]
fn materialized_snapshot_selection_is_versioned_strict_and_stats_bound() {
    let stats = lethe_storage_api::ObservationStats {
        count: 0,
        max_append_seq: 0,
    };
    let materialized =
        super::MaterializedProjectionSnapshot::build(vec![], vec![], vec![], vec![], stats)
            .unwrap();
    let fingerprint = materialized.supplemental_fingerprint.clone();
    let value = serde_json::to_value(&materialized).unwrap();

    assert!(
        super::current_materialized_snapshot(value.clone(), stats, &fingerprint, 0, 0)
            .unwrap()
            .is_some()
    );
    assert!(
        super::current_materialized_snapshot(
            value.clone(),
            lethe_storage_api::ObservationStats {
                count: 1,
                max_append_seq: 1,
            },
            &fingerprint,
            0,
            0,
        )
        .unwrap()
        .is_none()
    );

    let mut version_mismatch = value.clone();
    version_mismatch["format_version"] = serde_json::json!(7);
    assert!(
        super::current_materialized_snapshot(version_mismatch, stats, &fingerprint, 0, 0)
            .unwrap()
            .is_none()
    );

    let mut malformed_current = value;
    malformed_current["unexpected"] = serde_json::json!(true);
    assert!(matches!(
        super::current_materialized_snapshot(malformed_current, stats, &fingerprint, 0, 0),
        Err(SelfHostError::Json(_))
    ));

    assert!(matches!(
        super::current_materialized_snapshot(
            serde_json::to_value(&materialized).unwrap(),
            stats,
            &fingerprint,
            1,
            0,
        ),
        Err(SelfHostError::Ingestion(_))
    ));
    assert!(matches!(
        super::current_materialized_snapshot(
            serde_json::to_value(&materialized).unwrap(),
            stats,
            &fingerprint,
            0,
            1,
        ),
        Err(SelfHostError::Ingestion(_))
    ));
}

#[test]
fn materialized_person_message_manifest_rejects_resident_rows_and_count_drift() {
    let published = chrono::DateTime::parse_from_rfc3339("2026-07-13T00:00:00Z")
        .unwrap()
        .to_utc();
    let observation = wave2_slack_observation(
        "U-VALIDATE",
        "Validate User",
        Some("validate@example.test"),
        "validate",
        published,
    );
    let stats = lethe_storage_api::ObservationStats {
        count: 1,
        max_append_seq: 1,
    };
    let materialized = super::MaterializedProjectionSnapshot::build_at(
        vec![observation],
        vec![],
        vec![],
        vec![],
        stats,
        published,
    )
    .unwrap();
    assert!(materialized.snapshot.person_page.messages.is_empty());
    assert_eq!(materialized.person_message_count, 1);

    assert_eq!(materialized.reply_slo_count, 1);
    assert!(materialized.snapshot.reply_slo.rows.is_empty());
    assert!(materialized.snapshot.reply_slo.overdue.is_empty());
    let (item, reply_slo_item) = match pending_projection_item_commit(&materialized) {
        lethe_storage_api::ProjectionItemCommit::Replace { items } => (
            items
                .iter()
                .find(|item| item.owner_key != super::REPLY_SLO_ITEM_OWNER)
                .unwrap()
                .clone(),
            items
                .iter()
                .find(|item| item.owner_key == super::REPLY_SLO_ITEM_OWNER)
                .unwrap()
                .clone(),
        ),
        lethe_storage_api::ProjectionItemCommit::Delta { .. } => panic!("expected replace"),
    };
    let mut resident = materialized.clone();
    resident
        .snapshot
        .person_page
        .messages
        .push(super::person_message_from_projection_item(&item).unwrap());
    assert!(matches!(
        resident.validate(),
        Err(SelfHostError::Ingestion(_))
    ));

    let mut count_drift = materialized.clone();
    count_drift.person_message_count = 2;
    assert!(matches!(
        count_drift.validate(),
        Err(SelfHostError::Ingestion(_))
    ));

    let mut reply_count_drift = materialized.clone();
    reply_count_drift.reply_slo_count = 2;
    assert!(matches!(
        reply_count_drift.validate(),
        Err(SelfHostError::Ingestion(_))
    ));

    let mut resident_reply = materialized.clone();
    resident_reply
        .snapshot
        .reply_slo
        .rows
        .push(super::reply_slo_from_projection_item(&reply_slo_item).unwrap());
    assert!(matches!(
        resident_reply.validate(),
        Err(SelfHostError::Ingestion(_))
    ));

    let mut pending_drift = materialized;
    pending_drift
        .pending_item_commit
        .as_mut()
        .unwrap()
        .base_person_message_count = 1;
    assert!(matches!(
        pending_drift.validate(),
        Err(SelfHostError::Ingestion(_))
    ));
}

fn freshness_only_observation(
    schema: &str,
    source_system: &str,
    key: &str,
    published: chrono::DateTime<Utc>,
) -> Observation {
    Observation {
        id: Observation::new_id(),
        schema: SchemaRef::new(schema),
        schema_version: SemVer::new("1.0.0"),
        observer: ObserverRef::new("obs:bulk-test"),
        source_system: Some(SourceSystemRef::new(source_system)),
        actor: None,
        authority_model: AuthorityModel::LakeAuthoritative,
        capture_model: CaptureModel::Event,
        subject: EntityRef::new(format!("message:{key}")),
        target: None,
        payload: serde_json::json!({"text": key}),
        attachments: vec![],
        published,
        recorded_at: published + chrono::Duration::seconds(1),
        consent: None,
        idempotency_key: IdempotencyKey::new(key),
        meta: serde_json::json!({
            "canonical_json": serde_json::json!({
                "source": source_system,
                "object_id": key,
                "body": key,
            }).to_string(),
            "source_container": "bulk-test",
        }),
    }
}

#[test]
fn non_corpus_delta_classification_is_an_explicit_closed_whitelist() {
    let published = chrono::DateTime::parse_from_rfc3339("2026-07-13T00:00:00Z")
        .unwrap()
        .to_utc();
    for schema in [
        "schema:claude-message",
        "schema:chatgpt-message",
        "schema:github-event",
        "schema:coding-agent-message",
        "schema:gmail-message",
        "schema:discord-message",
    ] {
        let observation = freshness_only_observation(schema, "sys:claude-ai", schema, published);
        assert_eq!(
            super::classify_non_corpus_delta(&[observation]),
            super::NonCorpusDeltaKind::FreshnessOnly
        );
    }

    let unknown = freshness_only_observation(
        "schema:future-message",
        "sys:claude-ai",
        "unknown",
        published,
    );
    assert_eq!(
        super::classify_non_corpus_delta(&[unknown]),
        super::NonCorpusDeltaKind::FullRebuild
    );

    let mut reply_relevant = freshness_only_observation(
        "schema:gmail-message",
        "sys:gmail",
        "reply-relevant",
        published,
    );
    reply_relevant.meta["communication_sender_id"] = serde_json::json!("sender@example.test");
    assert_eq!(
        super::classify_non_corpus_delta(&[reply_relevant.clone()]),
        super::NonCorpusDeltaKind::FreshnessOnly
    );
    reply_relevant.meta["communication_channel_id"] = serde_json::json!("chan:gmail");
    reply_relevant.meta["communication_thread_ref"] = serde_json::json!("gmail:thread:1");
    reply_relevant.meta["communication"] = serde_json::json!({
        "reply_due_at": "2026-07-13T01:00:00Z"
    });
    assert_eq!(
        super::classify_non_corpus_delta(&[reply_relevant]),
        super::NonCorpusDeltaKind::FullRebuild
    );
}

fn apply_projection_item_commit(
    rows: &mut std::collections::BTreeMap<String, lethe_storage_api::ProjectionItem>,
    commit: &lethe_storage_api::ProjectionItemCommit,
) {
    match commit {
        lethe_storage_api::ProjectionItemCommit::Replace { items } => {
            rows.clear();
            for item in items {
                assert!(rows.insert(item.item_key.clone(), item.clone()).is_none());
            }
        }
        lethe_storage_api::ProjectionItemCommit::Delta {
            inserts,
            updates,
            deletes,
        } => {
            for item_key in deletes {
                rows.remove(item_key);
            }
            for item in updates.iter().chain(inserts) {
                rows.insert(item.item_key.clone(), item.clone());
            }
        }
    }
}

#[derive(Default)]
struct TestComponentProjectionLookup {
    observations: std::collections::BTreeMap<String, lethe_storage_api::StoredObservation>,
    rows: std::collections::BTreeMap<String, lethe_storage_api::ProjectionItem>,
    requested_observations: std::cell::RefCell<Vec<String>>,
}

impl super::ComponentProjectionLookup for TestComponentProjectionLookup {
    fn stored_observation(
        &self,
        observation_id: &lethe_core::domain::ObservationId,
    ) -> Result<Option<lethe_storage_api::StoredObservation>, SelfHostError> {
        self.requested_observations
            .borrow_mut()
            .push(observation_id.as_str().to_owned());
        Ok(self.observations.get(observation_id.as_str()).cloned())
    }

    fn person_message_items(
        &self,
        owner_key: &str,
    ) -> Result<Vec<lethe_storage_api::ProjectionItem>, SelfHostError> {
        Ok(self
            .rows
            .values()
            .filter(|item| item.owner_key == owner_key)
            .cloned()
            .collect())
    }
}

fn pending_projection_item_commit(
    materialized: &super::MaterializedProjectionSnapshot,
) -> &lethe_storage_api::ProjectionItemCommit {
    &materialized
        .pending_item_commit
        .as_ref()
        .expect("new materialization must carry an item commit")
        .commit
}

#[test]
fn freshness_only_incremental_materialization_matches_full_rebuild() {
    let first_at = chrono::DateTime::parse_from_rfc3339("2026-07-12T00:00:00Z")
        .unwrap()
        .to_utc();
    let final_at = chrono::DateTime::parse_from_rfc3339("2026-07-13T00:00:00Z")
        .unwrap()
        .to_utc();
    let thresholds = vec![
        lethe_projection_cognition::FreshnessThreshold {
            source_id: "sys:chatgpt".to_owned(),
            max_age_seconds: 86_400,
        },
        lethe_projection_cognition::FreshnessThreshold {
            source_id: "sys:claude-ai".to_owned(),
            max_age_seconds: 86_400,
        },
    ];
    let initial = freshness_only_observation(
        "schema:claude-message",
        "sys:claude-ai",
        "initial",
        first_at,
    );
    let appended = vec![
        freshness_only_observation(
            "schema:chatgpt-message",
            "sys:chatgpt",
            "appended-chatgpt",
            final_at - chrono::Duration::hours(1),
        ),
        freshness_only_observation(
            "schema:claude-message",
            "sys:claude-ai",
            "appended-claude",
            final_at - chrono::Duration::hours(2),
        ),
    ];
    let initial_materialized = super::MaterializedProjectionSnapshot::build_at(
        vec![initial.clone()],
        vec![],
        thresholds.clone(),
        vec![],
        lethe_storage_api::ObservationStats {
            count: 1,
            max_append_seq: 7,
        },
        first_at,
    )
    .unwrap();
    let core = AppCore::from_materialized(
        initial_materialized,
        vec![],
        vec![],
        thresholds.clone(),
        vec![],
    )
    .unwrap();
    let final_stats = lethe_storage_api::ObservationStats {
        count: 3,
        max_append_seq: 12,
    };
    let incremental = super::MaterializedProjectionSnapshot::compact_incremental_delta(
        &core,
        &appended,
        final_stats,
        final_at,
        &TestComponentProjectionLookup::default(),
    )
    .unwrap();

    let mut all = vec![initial];
    all.extend(appended.iter().cloned());
    all.reverse();
    let all = all
        .into_iter()
        .map(|observation| {
            serde_json::from_str::<Observation>(&serde_json::to_string(&observation).unwrap())
                .unwrap()
        })
        .collect();
    let full = super::MaterializedProjectionSnapshot::build_at(
        all,
        vec![],
        thresholds,
        vec![],
        final_stats,
        final_at,
    )
    .unwrap();

    assert_eq!(
        incremental.canonical_observation_fingerprint,
        full.canonical_observation_fingerprint
    );
    assert_eq!(
        incremental.snapshot.lineage.build_id,
        full.snapshot.lineage.build_id
    );
    assert_eq!(
        serde_json::to_value(incremental).unwrap(),
        serde_json::to_value(full).unwrap()
    );
}

fn freshness_only_draft(key: &str) -> ObservationDraft {
    ObservationDraft {
        schema: SchemaRef::new("schema:claude-message"),
        schema_version: SemVer::new("1.0.0"),
        observer: ObserverRef::new("obs:claude-ai-importer"),
        source_system: Some(SourceSystemRef::new("sys:claude-ai")),
        authority_model: AuthorityModel::LakeAuthoritative,
        capture_model: CaptureModel::Event,
        subject: EntityRef::new(format!("conversation:claude:{key}")),
        target: None,
        payload: serde_json::json!({"text": key}),
        attachments: vec![],
        published: Utc::now(),
        idempotency_key: IdempotencyKey::new(key),
        meta: serde_json::json!({
            "canonical_json": serde_json::json!({
                "source": "claude-ai",
                "object_id": key,
                "body": key,
            }).to_string(),
            "source_container": "claude-import",
        }),
    }
}

fn wave2_slack_observation(
    user_id: &str,
    user_name: &str,
    email: Option<&str>,
    key: &str,
    published: chrono::DateTime<Utc>,
) -> Observation {
    let reply_due_at = published + chrono::Duration::minutes(30);
    Observation {
        id: Observation::new_id(),
        schema: SchemaRef::new("schema:slack-message"),
        schema_version: SemVer::new("1.0.0"),
        observer: ObserverRef::new("obs:slack-crawler"),
        source_system: Some(SourceSystemRef::new("sys:slack")),
        actor: None,
        authority_model: AuthorityModel::LakeAuthoritative,
        capture_model: CaptureModel::Event,
        subject: EntityRef::new(format!("message:slack:C01ABC-{key}")),
        target: None,
        payload: serde_json::json!({
            "channel_id": "C01ABC",
            "channel_name": "general",
            "ts": key,
            "thread_ts": key,
            "user_id": user_id,
            "user_name": user_name,
            "email": email,
            "text": format!("wave2 message {key}"),
            "permalink": format!("https://example.test/C01ABC/{key}"),
            "is_public_channel": true,
            "visibility_status": "public",
            "is_bot": false,
            "ingress_kind": "channel",
            "mentions": [],
            "message_type": "message",
            "authority": 1,
        }),
        attachments: vec![],
        published,
        recorded_at: published + chrono::Duration::seconds(1),
        consent: None,
        idempotency_key: IdempotencyKey::new(format!("slack:C01ABC:{key}")),
        meta: serde_json::json!({
            "canonical_json": serde_json::json!({
                "sender": user_id,
                "body": format!("wave2 message {key}"),
                "event_time": key,
            }).to_string(),
            "source_container": "C01ABC",
            "communication_channel_kind": "slack",
            "communication_channel_external_id": "C01ABC",
            "communication_channel_id": "chan:slack-test:C01ABC",
            "communication_sender_id": user_id,
            "communication_thread_ref": format!("slack:thread:{key}"),
            "communication": {
                "reply_due_at": reply_due_at.to_rfc3339(),
            },
        }),
    }
}

fn component_google_observation(
    email: &str,
    key: &str,
    published: chrono::DateTime<Utc>,
) -> Observation {
    Observation {
        id: Observation::new_id(),
        schema: SchemaRef::new("schema:workspace-object-snapshot"),
        schema_version: SemVer::new("1.0.0"),
        observer: ObserverRef::new("obs:gslides-crawler"),
        source_system: Some(SourceSystemRef::new("sys:google-slides")),
        actor: None,
        authority_model: AuthorityModel::SourceAuthoritative,
        capture_model: CaptureModel::Snapshot,
        subject: EntityRef::new(format!("document:gslide:{key}")),
        target: None,
        payload: serde_json::json!({
            "title": format!("profile {key}"),
            "relations": {
                "owner": email,
                "editors": [email],
            },
        }),
        attachments: vec![],
        published,
        recorded_at: published + chrono::Duration::seconds(1),
        consent: None,
        idempotency_key: IdempotencyKey::new(format!("gslides:{key}:r1")),
        meta: serde_json::json!({}),
    }
}

fn component_consent_observation(
    identifier: &str,
    status: &str,
    key: &str,
    published: chrono::DateTime<Utc>,
) -> Observation {
    Observation {
        id: Observation::new_id(),
        schema: SchemaRef::new("schema:consent-decision"),
        schema_version: SemVer::new("1.0.0"),
        observer: ObserverRef::new("obs:consent-test"),
        source_system: Some(SourceSystemRef::new("sys:consent")),
        actor: None,
        authority_model: AuthorityModel::LakeAuthoritative,
        capture_model: CaptureModel::Event,
        subject: EntityRef::new(format!("consent:{key}")),
        target: None,
        payload: serde_json::json!({
            "identifier": identifier,
            "status": status,
        }),
        attachments: vec![],
        published,
        recorded_at: published + chrono::Duration::seconds(1),
        consent: None,
        idempotency_key: IdempotencyKey::new(format!("consent:{key}")),
        meta: serde_json::json!({}),
    }
}

fn component_projection_lookup(
    observations: &[Observation],
    rows: std::collections::BTreeMap<String, lethe_storage_api::ProjectionItem>,
) -> TestComponentProjectionLookup {
    TestComponentProjectionLookup {
        observations: observations
            .iter()
            .enumerate()
            .map(|(index, observation)| {
                (
                    observation.id.as_str().to_owned(),
                    lethe_storage_api::StoredObservation {
                        leaf_id: "leaf:test".to_owned(),
                        append_seq: u64::try_from(index + 1).unwrap(),
                        observation: observation.clone(),
                    },
                )
            })
            .collect(),
        rows,
        requested_observations: std::cell::RefCell::new(Vec::new()),
    }
}

#[test]
fn slack_late_bridge_reprojects_only_affected_components_and_matches_full_rebuild() {
    let initial_at = chrono::DateTime::parse_from_rfc3339("2026-07-12T00:00:00Z")
        .unwrap()
        .to_utc();
    let final_at = initial_at + chrono::Duration::hours(1);
    let initial = vec![
        wave2_slack_observation(
            "U-A",
            "Unrelated A",
            Some("a@example.test"),
            "1.000001",
            initial_at,
        ),
        wave2_slack_observation(
            "U-B",
            "Unrelated B",
            Some("b@example.test"),
            "2.000001",
            initial_at + chrono::Duration::minutes(1),
        ),
        component_google_observation(
            "d@example.test",
            "component-d",
            initial_at + chrono::Duration::minutes(2),
        ),
        wave2_slack_observation(
            "U-C",
            "Bridge",
            Some("c@example.test"),
            "3.000001",
            initial_at + chrono::Duration::minutes(3),
        ),
    ];
    let appended = wave2_slack_observation(
        "U-C",
        "Bridge",
        Some("d@example.test"),
        "4.000001",
        final_at,
    );
    let initial_stats = lethe_storage_api::ObservationStats {
        count: 4,
        max_append_seq: 4,
    };
    let initial_materialized = super::MaterializedProjectionSnapshot::build_at(
        initial.clone(),
        vec![],
        vec![],
        vec![],
        initial_stats,
        initial_at,
    )
    .unwrap();
    let mut incremental_rows = std::collections::BTreeMap::new();
    apply_projection_item_commit(
        &mut incremental_rows,
        pending_projection_item_commit(&initial_materialized),
    );
    let bridge_message_id = format!("pm:{:020}:{}", 4, initial[3].id);
    assert_eq!(
        incremental_rows[&bridge_message_id].owner_key,
        "person:component-3"
    );
    let core =
        AppCore::from_materialized(initial_materialized, vec![], vec![], vec![], vec![]).unwrap();
    let mut all = initial.clone();
    all.push(appended.clone());
    let lookup = component_projection_lookup(&all, incremental_rows.clone());
    let final_stats = lethe_storage_api::ObservationStats {
        count: 5,
        max_append_seq: 5,
    };

    let incremental = super::MaterializedProjectionSnapshot::compact_incremental_delta(
        &core,
        std::slice::from_ref(&appended),
        final_stats,
        final_at,
        &lookup,
    )
    .unwrap();
    let lethe_storage_api::ProjectionItemCommit::Delta {
        inserts: _,
        updates,
        deletes,
    } = pending_projection_item_commit(&incremental)
    else {
        panic!("component re-projection must publish a delta");
    };
    assert!(
        updates
            .iter()
            .any(|item| item.item_key == bridge_message_id)
    );
    assert!(!deletes.contains(&bridge_message_id));
    apply_projection_item_commit(
        &mut incremental_rows,
        pending_projection_item_commit(&incremental),
    );
    let full = super::MaterializedProjectionSnapshot::build_at(
        all,
        vec![],
        vec![],
        vec![],
        final_stats,
        final_at,
    )
    .unwrap();
    let mut full_rows = std::collections::BTreeMap::new();
    apply_projection_item_commit(&mut full_rows, pending_projection_item_commit(&full));

    let requested = lookup
        .requested_observations
        .borrow()
        .iter()
        .cloned()
        .collect::<std::collections::BTreeSet<_>>();
    assert!(!requested.contains(initial[0].id.as_str()));
    assert!(!requested.contains(initial[1].id.as_str()));
    assert_eq!(requested.len(), 3);
    assert_eq!(
        incremental_rows[&bridge_message_id].owner_key,
        "person:component-2"
    );
    assert_eq!(incremental_rows, full_rows);
    assert_eq!(
        serde_json::to_value(incremental).unwrap(),
        serde_json::to_value(full).unwrap()
    );
}

#[test]
fn slack_identifier_consent_change_reprojects_component_and_matches_full_rebuild() {
    let initial_at = chrono::DateTime::parse_from_rfc3339("2026-07-12T00:00:00Z")
        .unwrap()
        .to_utc();
    let final_at = initial_at + chrono::Duration::hours(1);
    let initial = vec![
        wave2_slack_observation("U-A", "Affected", None, "1.000001", initial_at),
        wave2_slack_observation(
            "U-B",
            "Unrelated",
            Some("unrelated@example.test"),
            "2.000001",
            initial_at + chrono::Duration::minutes(1),
        ),
        component_consent_observation(
            "opted-out@example.test",
            "opted_out",
            "identifier-opt-out",
            initial_at + chrono::Duration::minutes(2),
        ),
    ];
    let appended = wave2_slack_observation(
        "U-A",
        "Affected",
        Some("opted-out@example.test"),
        "3.000001",
        final_at,
    );
    let initial_stats = lethe_storage_api::ObservationStats {
        count: 3,
        max_append_seq: 3,
    };
    let initial_materialized = super::MaterializedProjectionSnapshot::build_at(
        initial.clone(),
        vec![],
        vec![],
        vec![],
        initial_stats,
        initial_at,
    )
    .unwrap();
    let mut incremental_rows = std::collections::BTreeMap::new();
    apply_projection_item_commit(
        &mut incremental_rows,
        pending_projection_item_commit(&initial_materialized),
    );
    let core =
        AppCore::from_materialized(initial_materialized, vec![], vec![], vec![], vec![]).unwrap();
    let mut all = initial.clone();
    all.push(appended.clone());
    let lookup = component_projection_lookup(&all, incremental_rows.clone());
    let final_stats = lethe_storage_api::ObservationStats {
        count: 4,
        max_append_seq: 4,
    };

    let incremental = super::MaterializedProjectionSnapshot::compact_incremental_delta(
        &core,
        std::slice::from_ref(&appended),
        final_stats,
        final_at,
        &lookup,
    )
    .unwrap();
    apply_projection_item_commit(
        &mut incremental_rows,
        pending_projection_item_commit(&incremental),
    );
    let full = super::MaterializedProjectionSnapshot::build_at(
        all,
        vec![],
        vec![],
        vec![],
        final_stats,
        final_at,
    )
    .unwrap();
    let mut full_rows = std::collections::BTreeMap::new();
    apply_projection_item_commit(&mut full_rows, pending_projection_item_commit(&full));

    let requested = lookup
        .requested_observations
        .borrow()
        .iter()
        .cloned()
        .collect::<std::collections::BTreeSet<_>>();
    assert!(!requested.contains(initial[1].id.as_str()));
    assert!(!requested.contains(initial[2].id.as_str()));
    assert_eq!(requested.len(), 2);
    assert_eq!(incremental.person_message_count, 1);
    assert_eq!(incremental_rows, full_rows);
    assert_eq!(
        serde_json::to_value(incremental).unwrap(),
        serde_json::to_value(full).unwrap()
    );
}

#[test]
fn component_reprojection_is_invariant_to_slack_batch_partition() {
    let initial_at = chrono::DateTime::parse_from_rfc3339("2026-07-12T00:00:00Z")
        .unwrap()
        .to_utc();
    let final_at = initial_at + chrono::Duration::hours(2);
    let initial = vec![
        component_google_observation("alpha@example.test", "alpha", initial_at),
        component_google_observation(
            "beta@example.test",
            "beta",
            initial_at + chrono::Duration::minutes(1),
        ),
        wave2_slack_observation(
            "U-GAMMA",
            "Gamma",
            Some("gamma@example.test"),
            "1.000001",
            initial_at + chrono::Duration::minutes(2),
        ),
    ];
    let deltas = [
        wave2_slack_observation(
            "U-ALPHA",
            "Alpha",
            Some("alpha@example.test"),
            "2.000001",
            initial_at + chrono::Duration::minutes(3),
        ),
        wave2_slack_observation(
            "U-BETA",
            "Beta",
            Some("beta@example.test"),
            "3.000001",
            initial_at + chrono::Duration::minutes(4),
        ),
        wave2_slack_observation(
            "U-ALPHA",
            "Alpha",
            Some("beta@example.test"),
            "4.000001",
            initial_at + chrono::Duration::minutes(5),
        ),
    ];
    let mut all = initial.clone();
    all.extend(deltas.iter().cloned());
    let final_stats = lethe_storage_api::ObservationStats {
        count: u64::try_from(all.len()).unwrap(),
        max_append_seq: u64::try_from(all.len()).unwrap(),
    };
    let full = super::MaterializedProjectionSnapshot::build_at(
        all.clone(),
        vec![],
        vec![],
        vec![],
        final_stats,
        final_at,
    )
    .unwrap();
    let mut full_rows = std::collections::BTreeMap::new();
    apply_projection_item_commit(&mut full_rows, pending_projection_item_commit(&full));

    for partition in [vec![1, 1, 1], vec![2, 1], vec![1, 2], vec![3]] {
        let initial_stats = lethe_storage_api::ObservationStats {
            count: u64::try_from(initial.len()).unwrap(),
            max_append_seq: u64::try_from(initial.len()).unwrap(),
        };
        let initial_materialized = super::MaterializedProjectionSnapshot::build_at(
            initial.clone(),
            vec![],
            vec![],
            vec![],
            initial_stats,
            initial_at,
        )
        .unwrap();
        let mut rows = std::collections::BTreeMap::new();
        apply_projection_item_commit(
            &mut rows,
            pending_projection_item_commit(&initial_materialized),
        );
        let mut core =
            AppCore::from_materialized(initial_materialized, vec![], vec![], vec![], vec![])
                .unwrap();
        let mut consumed = 0;

        for &batch_size in &partition {
            let batch = &deltas[consumed..consumed + batch_size];
            consumed += batch_size;
            let stats = lethe_storage_api::ObservationStats {
                count: u64::try_from(initial.len() + consumed).unwrap(),
                max_append_seq: u64::try_from(initial.len() + consumed).unwrap(),
            };
            let lookup =
                component_projection_lookup(&all[..initial.len() + consumed], rows.clone());
            let incremental = super::MaterializedProjectionSnapshot::compact_incremental_delta(
                &core, batch, stats, final_at, &lookup,
            )
            .unwrap();
            if consumed == deltas.len() {
                assert!(
                    !lookup
                        .requested_observations
                        .borrow()
                        .iter()
                        .any(|id| id == initial[2].id.as_str()),
                    "stable component ID must keep Gamma outside partition {partition:?}"
                );
            }
            apply_projection_item_commit(&mut rows, pending_projection_item_commit(&incremental));
            core =
                AppCore::from_materialized(*incremental, vec![], vec![], vec![], vec![]).unwrap();
        }

        assert_eq!(consumed, deltas.len());
        assert_eq!(rows, full_rows, "partition {partition:?}");
        let incremental_value = serde_json::to_value(super::MaterializedProjectionSnapshot {
            format_version: super::NON_CORPUS_MATERIALIZATION_VERSION,
            last_append_seq: core.observation_stats.max_append_seq,
            observation_count: core.observation_stats.count,
            canonical_observation_fingerprint: core.canonical_observation_fingerprint.clone(),
            supplemental_fingerprint: core.supplemental_fingerprint.clone(),
            compact_state: core.compact_state.clone(),
            person_consents: core.person_consents.clone(),
            person_message_count: core.person_message_count,
            reply_slo_count: core.reply_slo_count,
            snapshot: core.snapshot.clone(),
            pending_item_commit: None,
        })
        .unwrap();
        assert_eq!(
            incremental_value,
            serde_json::to_value(&full).unwrap(),
            "partition {partition:?}"
        );
    }
}

#[test]
fn paged_materialization_matches_full_build_and_publishes_atomically() {
    let root = std::env::temp_dir().join(format!(
        "lethe-paged-materialization-test-{}",
        uuid::Uuid::now_v7()
    ));
    let db = root.join("lethe.sqlite3");
    let blobs = root.join("blobs");
    let persistence = SqlitePersistence::open(&db, &blobs, &[7; 32]).unwrap();
    let built_at = chrono::DateTime::parse_from_rfc3339("2026-07-13T12:00:00Z")
        .unwrap()
        .to_utc();
    let observations = vec![
        wave2_slack_observation(
            "U01",
            "Alice",
            Some("alice@example.test"),
            "1.000001",
            built_at - chrono::Duration::hours(3),
        ),
        freshness_only_observation(
            "schema:claude-message",
            "sys:claude-ai",
            "paged-claude",
            built_at - chrono::Duration::hours(2),
        ),
        wave2_slack_observation(
            "U02",
            "Bob",
            Some("bob@example.test"),
            "2.000001",
            built_at - chrono::Duration::hours(1),
        ),
    ];
    for observation in &observations {
        persistence.persist_observation(observation).unwrap();
    }
    let stats = persistence.observation_stats().unwrap();
    let thresholds = vec![
        lethe_projection_cognition::FreshnessThreshold {
            source_id: "chan:slack-test:C01ABC".to_owned(),
            max_age_seconds: 172_800,
        },
        lethe_projection_cognition::FreshnessThreshold {
            source_id: "sys:claude-ai".to_owned(),
            max_age_seconds: 172_800,
        },
    ];
    let full = super::MaterializedProjectionSnapshot::build_at(
        observations,
        vec![],
        thresholds.clone(),
        vec![],
        stats,
        built_at,
    )
    .unwrap();
    let mut expected_rows = std::collections::BTreeMap::new();
    apply_projection_item_commit(&mut expected_rows, pending_projection_item_commit(&full));
    let expected_manifest = serde_json::to_value(&full).unwrap();

    for page_size in [1, 128] {
        let paged = super::rebuild_materialized_snapshot_paged(
            &persistence,
            &[],
            &thresholds,
            &[],
            stats,
            page_size,
            built_at,
        )
        .unwrap();
        assert_eq!(serde_json::to_value(&paged).unwrap(), expected_manifest);
        assert_eq!(
            persistence
                .projection_records(&ProjectionRef::new("proj:person-page"))
                .unwrap()
                .unwrap(),
            expected_manifest
        );

        let owners = expected_rows
            .values()
            .map(|item| item.owner_key.clone())
            .collect::<std::collections::BTreeSet<_>>();
        for owner in owners {
            let actual = persistence
                .projection_items_by_owner(&ProjectionRef::new("proj:person-page"), &owner)
                .unwrap()
                .into_iter()
                .map(|item| (item.item_key.clone(), item))
                .collect::<std::collections::BTreeMap<_, _>>();
            let expected = expected_rows
                .values()
                .filter(|item| item.owner_key == owner)
                .cloned()
                .map(|item| (item.item_key.clone(), item))
                .collect::<std::collections::BTreeMap<_, _>>();
            assert_eq!(actual, expected);
        }
        let staging = ProjectionRef::new(super::NON_CORPUS_REBUILD_STAGING_PROJECTION_ID);
        assert_eq!(persistence.projection_item_count(&staging).unwrap(), 0);
        assert!(persistence.projection_records(&staging).unwrap().is_none());
    }

    drop(persistence);
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn supplemental_delta_matches_full_build_and_updates_one_reply_row() {
    let root = std::env::temp_dir().join(format!(
        "lethe-supplemental-delta-test-{}",
        uuid::Uuid::now_v7()
    ));
    let db = root.join("lethe.sqlite3");
    let blobs = root.join("blobs");
    let persistence = SqlitePersistence::open(&db, &blobs, &[7; 32]).unwrap();
    let published = chrono::DateTime::parse_from_rfc3339("2026-07-13T08:00:00Z")
        .unwrap()
        .to_utc();
    let observation = wave2_slack_observation(
        "U01",
        "Alice",
        Some("alice@example.test"),
        "10.000001",
        published,
    );
    persistence.persist_observation(&observation).unwrap();
    drop(persistence);

    let mut config = test_config(db.clone(), blobs.clone());
    config.corpus.mode = lethe_projection_corpus::CorpusMode::PersonalAllText;
    let thresholds = super::freshness_thresholds(&config);
    let channels = config.channels.clone();
    let service = AppService::bootstrap(config).unwrap();
    let draft_id = SupplementalId::new("sup:00000000-0000-7000-8000-000000000201");
    let draft = service
        .write_supplemental(super::SupplementalWriteRequest {
            id: draft_id.clone(),
            kind: "reply-draft@1".to_owned(),
            derived_from: InputAnchorSet {
                observations: vec![observation.id.clone()],
                blobs: Vec::new(),
                supplementals: Vec::new(),
            },
            payload: serde_json::json!({
                "channel": "slack",
                "recipient": "U01",
                "body": "reply body",
                "drafted_at": "2026-07-13T08:05:00Z",
                "thread_ref": "slack:thread:10.000001"
            }),
            created_by: ActorRef::new("actor:test"),
            mutability: Mutability::AppendOnly,
            model_version: None,
            consent_metadata: None,
            lineage: None,
        })
        .unwrap();
    let pending_item = service
        .persistence_lock()
        .unwrap()
        .projection_item_by_key(
            &ProjectionRef::new("proj:person-page"),
            &format!("reply-slo:{}", observation.id),
        )
        .unwrap()
        .unwrap();
    assert!(
        super::reply_slo_from_projection_item(&pending_item)
            .unwrap()
            .sent_at
            .is_none()
    );

    let send = service
        .write_supplemental(super::SupplementalWriteRequest {
            id: SupplementalId::new("sup:00000000-0000-7000-8000-000000000202"),
            kind: "send-record@1".to_owned(),
            derived_from: InputAnchorSet {
                observations: Vec::new(),
                blobs: Vec::new(),
                supplementals: vec![draft_id],
            },
            payload: serde_json::json!({
                "channel": "slack",
                "sent_at": "2026-07-13T08:10:00Z",
                "mode": "approved"
            }),
            created_by: ActorRef::new("actor:test"),
            mutability: Mutability::AppendOnly,
            model_version: None,
            consent_metadata: None,
            lineage: None,
        })
        .unwrap();
    let sent_item = service
        .persistence_lock()
        .unwrap()
        .projection_item_by_key(
            &ProjectionRef::new("proj:person-page"),
            &format!("reply-slo:{}", observation.id),
        )
        .unwrap()
        .unwrap();
    assert_eq!(
        super::reply_slo_from_projection_item(&sent_item)
            .unwrap()
            .sent_at,
        Some(
            chrono::DateTime::parse_from_rfc3339("2026-07-13T08:10:00Z")
                .unwrap()
                .to_utc()
        )
    );

    let core = service.core_lock().unwrap();
    let stats = core.observation_stats;
    let final_built_at = core.snapshot.built_at;
    drop(core);
    let full = super::MaterializedProjectionSnapshot::build_at(
        vec![observation],
        vec![draft, send],
        thresholds,
        channels,
        stats,
        final_built_at,
    )
    .unwrap();
    let persisted_manifest = service
        .persistence_lock()
        .unwrap()
        .projection_records(&ProjectionRef::new("proj:person-page"))
        .unwrap()
        .unwrap();
    assert_eq!(persisted_manifest, serde_json::to_value(&full).unwrap());
    let mut expected_rows = std::collections::BTreeMap::new();
    apply_projection_item_commit(&mut expected_rows, pending_projection_item_commit(&full));
    assert_eq!(expected_rows[&sent_item.item_key], sent_item);

    drop(service);
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn wave2_slack_incremental_materialization_matches_normalized_full_rebuild() {
    let initial_at = chrono::DateTime::parse_from_rfc3339("2026-07-12T00:00:00Z")
        .unwrap()
        .to_utc();
    let final_at = chrono::DateTime::parse_from_rfc3339("2026-07-13T00:00:00Z")
        .unwrap()
        .to_utc();
    let thresholds = vec![lethe_projection_cognition::FreshnessThreshold {
        source_id: "chan:slack-test:C01ABC".to_owned(),
        max_age_seconds: 172_800,
    }];
    let initial = vec![
        wave2_slack_observation(
            "U01",
            "Alice",
            Some("alice@example.test"),
            "1.000001",
            initial_at,
        ),
        wave2_slack_observation(
            "U02",
            "Bob",
            Some("bob@example.test"),
            "2.000001",
            initial_at + chrono::Duration::minutes(1),
        ),
    ];
    let appended = vec![
        wave2_slack_observation(
            "U01",
            "Alice Updated",
            Some("alice@example.test"),
            "3.000001",
            final_at - chrono::Duration::minutes(2),
        ),
        wave2_slack_observation(
            "U03",
            "Carol",
            None,
            "4.000001",
            final_at - chrono::Duration::minutes(1),
        ),
    ];
    assert_eq!(
        super::classify_non_corpus_delta(&appended),
        super::NonCorpusDeltaKind::SlackMessage
    );
    let initial_materialized = super::MaterializedProjectionSnapshot::build_at(
        initial.clone(),
        vec![],
        thresholds.clone(),
        vec![],
        lethe_storage_api::ObservationStats {
            count: 2,
            max_append_seq: 2,
        },
        initial_at,
    )
    .unwrap();
    assert_eq!(
        initial_materialized
            .snapshot
            .identity
            .resolved_persons
            .len(),
        2
    );
    assert!(
        initial_materialized
            .snapshot
            .person_page
            .messages
            .is_empty()
    );
    assert_eq!(initial_materialized.person_message_count, 2);
    let mut incremental_message_rows = std::collections::BTreeMap::new();
    apply_projection_item_commit(
        &mut incremental_message_rows,
        pending_projection_item_commit(&initial_materialized),
    );
    let core = AppCore::from_materialized(
        initial_materialized,
        vec![],
        vec![],
        thresholds.clone(),
        vec![],
    )
    .unwrap();
    let final_stats = lethe_storage_api::ObservationStats {
        count: 4,
        max_append_seq: 4,
    };
    let mut lookup_observations = initial.clone();
    lookup_observations.extend(appended.iter().cloned());
    let lookup =
        component_projection_lookup(&lookup_observations, incremental_message_rows.clone());
    let incremental = super::MaterializedProjectionSnapshot::compact_incremental_delta(
        &core,
        &appended,
        final_stats,
        final_at,
        &lookup,
    )
    .unwrap();
    apply_projection_item_commit(
        &mut incremental_message_rows,
        pending_projection_item_commit(&incremental),
    );

    let mut all = initial;
    all.extend(appended);
    let full = super::MaterializedProjectionSnapshot::build_at(
        all,
        vec![],
        thresholds,
        vec![],
        final_stats,
        final_at,
    )
    .unwrap();

    assert_eq!(incremental.snapshot.identity.resolved_persons.len(), 3);
    assert_eq!(incremental.snapshot.person_page.profiles.len(), 3);
    assert!(incremental.snapshot.person_page.messages.is_empty());
    assert_eq!(incremental.person_message_count, 4);
    assert_eq!(incremental.reply_slo_count, 4);
    assert!(incremental.snapshot.reply_slo.rows.is_empty());
    assert!(incremental.snapshot.reply_slo.overdue.is_empty());
    assert_eq!(incremental.compact_state.identity.nodes().len(), 3);
    let mut full_message_rows = std::collections::BTreeMap::new();
    apply_projection_item_commit(
        &mut full_message_rows,
        pending_projection_item_commit(&full),
    );
    assert_eq!(incremental_message_rows, full_message_rows);
    assert_eq!(
        serde_json::to_value(incremental).unwrap(),
        serde_json::to_value(full).unwrap()
    );
}

fn wave2_slack_draft(index: usize) -> ObservationDraft {
    let user_index = index % 100;
    let user_id = format!("U{user_index:04}");
    let key = format!("{index:010}.000001");
    ObservationDraft {
        schema: SchemaRef::new("schema:slack-message"),
        schema_version: SemVer::new("1.0.0"),
        observer: ObserverRef::new("obs:slack-crawler"),
        source_system: Some(SourceSystemRef::new("sys:slack")),
        authority_model: AuthorityModel::LakeAuthoritative,
        capture_model: CaptureModel::Event,
        subject: EntityRef::new(format!("message:slack:C01ABC-{key}")),
        target: None,
        payload: serde_json::json!({
            "channel_id": "C01ABC",
            "channel_name": "general",
            "ts": key,
            "thread_ts": key,
            "user_id": user_id,
            "user_name": format!("User {user_index}"),
            "email": format!("user-{user_index}@example.test"),
            "text": format!("Wave2 bulk message {index}"),
            "permalink": format!("https://example.test/C01ABC/{key}"),
            "is_public_channel": true,
            "visibility_status": "public",
            "is_bot": false,
            "ingress_kind": "channel",
            "mentions": [],
            "message_type": "message",
            "authority": 1,
        }),
        attachments: vec![],
        published: Utc::now(),
        idempotency_key: IdempotencyKey::new(format!("slack:C01ABC:{key}")),
        meta: serde_json::json!({
            "canonical_json": serde_json::json!({
                "sender": user_id,
                "body": format!("Wave2 bulk message {index}"),
                "event_time": key,
            }).to_string(),
            "source_container": "C01ABC",
            "communication_channel_kind": "slack",
            "communication_channel_external_id": "C01ABC",
            "communication_sender_id": user_id,
            "communication_thread_ref": format!("slack:thread:{key}"),
        }),
    }
}

#[test]
fn duplicate_bulk_records_do_not_change_incremental_fingerprint() {
    let root = std::env::temp_dir().join(format!("lethe-self-host-test-{}", uuid::Uuid::now_v7()));
    let db = root.join("lethe.sqlite3");
    let blobs = root.join("blobs");
    let persistence = SqlitePersistence::open(&db, &blobs, &[7; 32]).unwrap();
    let mut config = test_config(db, blobs);
    config.corpus.mode = lethe_projection_corpus::CorpusMode::PersonalAllText;
    let service = test_service(config, persistence);
    wait_for_search_index_ready(&service);

    let first = service
        .ingest_observation_drafts(vec![wave2_slack_draft(0)], "slack-test")
        .unwrap();
    assert_eq!(first.ingested, 1);
    let (before, before_item_count) = {
        let persistence = service.persistence_lock().unwrap();
        (
            persistence
                .projection_records(&ProjectionRef::new("proj:person-page"))
                .unwrap()
                .unwrap(),
            persistence
                .projection_item_count(&ProjectionRef::new("proj:person-page"))
                .unwrap(),
        )
    };

    let duplicate = service
        .ingest_observation_drafts(vec![wave2_slack_draft(0)], "slack-test")
        .unwrap();
    assert_eq!(duplicate.ingested, 0);
    assert_eq!(duplicate.duplicates, 1);
    let (after, after_item_count) = {
        let persistence = service.persistence_lock().unwrap();
        (
            persistence
                .projection_records(&ProjectionRef::new("proj:person-page"))
                .unwrap()
                .unwrap(),
            persistence
                .projection_item_count(&ProjectionRef::new("proj:person-page"))
                .unwrap(),
        )
    };

    assert_eq!(
        before["canonical_observation_fingerprint"],
        after["canonical_observation_fingerprint"]
    );
    assert_eq!(before["observation_count"], after["observation_count"]);
    assert_eq!(before["reply_slo_count"], 1);
    assert_eq!(after["reply_slo_count"], 1);
    assert_eq!(before_item_count, 2);
    assert_eq!(after_item_count, before_item_count);
    assert_eq!(
        service
            .non_corpus_rebuild_count
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn person_message_item_sql_failure_does_not_install_manifest_in_core() {
    let root = std::env::temp_dir().join(format!("lethe-self-host-test-{}", uuid::Uuid::now_v7()));
    let db = root.join("lethe.sqlite3");
    let blobs = root.join("blobs");
    let persistence = SqlitePersistence::open(&db, &blobs, &[7; 32]).unwrap();
    let mut config = test_config(db.clone(), blobs);
    config.corpus.mode = lethe_projection_corpus::CorpusMode::PersonalAllText;
    let service = test_service(config, persistence);

    wait_for_search_index_ready(&service);

    let connection = rusqlite::Connection::open(&db).unwrap();
    connection
        .execute_batch(
            "CREATE TRIGGER reject_person_message_item
             BEFORE INSERT ON projection_materialization_items
             WHEN NEW.projection_id = 'proj:person-page'
             BEGIN
                 SELECT RAISE(FAIL, 'forced person message item failure');
             END;",
        )
        .unwrap();
    drop(connection);

    let error = service
        .ingest_observation_drafts(vec![wave2_slack_draft(1)], "slack-test")
        .unwrap_err();
    assert!(
        matches!(error, SelfHostError::Storage(_)),
        "unexpected error: {error:?}"
    );
    let core = service.core_lock().unwrap();
    assert_eq!(core.observation_stats.count, 0);
    assert_eq!(core.person_message_count, 0);
    assert_eq!(core.reply_slo_count, 0);
    assert!(core.snapshot.person_page.profiles.is_empty());
    assert!(core.snapshot.person_page.messages.is_empty());
    assert!(core.snapshot.reply_slo.rows.is_empty());
    drop(core);
    let persistence = service.persistence_lock().unwrap();
    assert_eq!(persistence.observation_stats().unwrap().count, 1);
    assert!(
        persistence
            .projection_records(&ProjectionRef::new("proj:person-page"))
            .unwrap()
            .is_none()
    );
    assert_eq!(
        persistence
            .projection_item_count(&ProjectionRef::new("proj:person-page"))
            .unwrap(),
        0
    );
    drop(persistence);
    drop(service);

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn five_thousand_corpus_only_records_do_not_trigger_full_observation_load() {
    let root = std::env::temp_dir().join(format!("lethe-self-host-test-{}", uuid::Uuid::now_v7()));
    let db = root.join("lethe.sqlite3");
    let blobs = root.join("blobs");
    let persistence = SqlitePersistence::open(&db, &blobs, &[7; 32]).unwrap();
    let mut config = test_config(db, blobs);
    config.corpus.mode = lethe_projection_corpus::CorpusMode::PersonalAllText;
    let service = test_service(config, persistence);
    let drafts = (0..5_000)
        .map(|index| freshness_only_draft(&format!("bulk-{index:05}")))
        .collect();

    let report = service
        .ingest_observation_drafts(drafts, "bulk-memory-test")
        .unwrap();

    assert_eq!(report.ingested, 5_000);
    assert_eq!(report.duplicates, 0);
    assert_eq!(report.quarantined, 0);
    assert_eq!(service.core_lock().unwrap().observation_stats.count, 5_000);
    assert_eq!(
        service
            .non_corpus_rebuild_count
            .load(std::sync::atomic::Ordering::Relaxed),
        0,
        "freshness-only bulk import must not call load_observations"
    );

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn five_thousand_wave2_slack_records_use_compact_identity_without_full_load() {
    let root = std::env::temp_dir().join(format!("lethe-self-host-test-{}", uuid::Uuid::now_v7()));
    let db = root.join("lethe.sqlite3");
    let blobs = root.join("blobs");
    let persistence = SqlitePersistence::open(&db, &blobs, &[7; 32]).unwrap();
    let mut config = test_config(db, blobs);
    config.corpus.mode = lethe_projection_corpus::CorpusMode::PersonalAllText;
    let service = test_service(config.clone(), persistence);
    let drafts = (0..5_000).map(wave2_slack_draft).collect();

    let report = service
        .ingest_observation_drafts(drafts, "slack-test")
        .unwrap();

    assert_eq!(report.ingested, 5_000);
    assert_eq!(report.duplicates, 0);
    assert_eq!(report.quarantined, 0);
    let core = service.core_lock().unwrap();
    assert_eq!(core.observation_stats.count, 5_000);
    assert_eq!(core.compact_state.identity.nodes().len(), 100);
    assert_eq!(core.snapshot.identity.resolved_persons.len(), 100);
    assert_eq!(core.snapshot.person_page.profiles.len(), 100);
    assert!(core.snapshot.person_page.messages.is_empty());
    assert_eq!(core.person_message_count, 5_000);
    assert_eq!(core.reply_slo_count, 5_000);
    assert!(core.snapshot.reply_slo.rows.is_empty());
    assert!(core.snapshot.reply_slo.overdue.is_empty());
    let person_id = core.snapshot.person_page.profiles[0]
        .person_id
        .as_str()
        .to_owned();
    let expected_owner_messages = core
        .snapshot
        .person_page
        .activities
        .iter()
        .find(|activity| activity.person_id.as_str() == person_id)
        .unwrap()
        .total_messages;
    drop(core);
    {
        let persistence = service.persistence_lock().unwrap();
        assert_eq!(
            persistence
                .projection_item_count(&ProjectionRef::new("proj:person-page"))
                .unwrap(),
            10_000
        );
        assert_eq!(
            persistence
                .projection_item_count_by_owner(
                    &ProjectionRef::new("proj:person-page"),
                    super::REPLY_SLO_ITEM_OWNER,
                )
                .unwrap(),
            5_000
        );
        assert_eq!(
            persistence
                .projection_item_count_by_owner(
                    &ProjectionRef::new("proj:person-page"),
                    &person_id,
                )
                .unwrap(),
            u64::try_from(expected_owner_messages).unwrap()
        );
    }
    let messages = service
        .person_messages_response(&person_id, None, None)
        .unwrap();
    assert_eq!(
        messages.data.as_array().unwrap().len(),
        expected_owner_messages
    );
    let detail = service
        .person_detail_response(&person_id, None, None)
        .unwrap();
    assert_eq!(
        detail.data["recent_messages"].as_array().unwrap().len(),
        expected_owner_messages
    );
    let timeline = service
        .person_timeline_response(&person_id, None, None)
        .unwrap();
    assert_eq!(
        timeline.data.as_array().unwrap().len(),
        expected_owner_messages
    );
    assert_eq!(service.reply_slo_response().unwrap().data.rows.len(), 5_000);
    assert_eq!(
        service
            .non_corpus_rebuild_count
            .load(std::sync::atomic::Ordering::Relaxed),
        0,
        "Wave2-compatible Slack bulk import must not call load_observations"
    );
    let built_at = service.core_lock().unwrap().snapshot.built_at;
    drop(service);

    let restarted = AppService::bootstrap(config).unwrap();
    let restarted_core = restarted.core_lock().unwrap();
    assert_eq!(restarted_core.snapshot.built_at, built_at);
    assert!(restarted_core.snapshot.person_page.messages.is_empty());
    assert_eq!(restarted_core.person_message_count, 5_000);
    assert_eq!(restarted_core.reply_slo_count, 5_000);
    assert!(restarted_core.snapshot.reply_slo.rows.is_empty());
    assert!(restarted_core.snapshot.reply_slo.overdue.is_empty());
    drop(restarted_core);
    assert_eq!(
        restarted
            .non_corpus_rebuild_count
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );
    assert_eq!(
        restarted
            .person_messages_response(&person_id, None, None)
            .unwrap()
            .data
            .as_array()
            .unwrap()
            .len(),
        expected_owner_messages
    );
    assert_eq!(
        restarted.reply_slo_response().unwrap().data.rows.len(),
        5_000
    );
    drop(restarted);

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn ingest_observation_drafts_enforces_payload_limit_before_bulk_append() {
    let root = std::env::temp_dir().join(format!("lethe-self-host-test-{}", uuid::Uuid::now_v7()));
    let db = root.join("lethe.sqlite3");
    let blobs = root.join("blobs");
    let persistence = SqlitePersistence::open(&db, &blobs, &[7; 32]).unwrap();
    let mut config = test_config(db.clone(), blobs);
    config.resource_limits.max_payload_bytes = 1;
    let service = test_service(config, persistence);

    let draft = ObservationDraft {
        schema: SchemaRef::new("schema:slack-message"),
        schema_version: SemVer::new("1.0.0"),
        observer: ObserverRef::new("obs:slack-crawler"),
        source_system: Some(SourceSystemRef::new("sys:slack")),
        authority_model: AuthorityModel::LakeAuthoritative,
        capture_model: CaptureModel::Event,
        subject: EntityRef::new("message:slack:too-large"),
        target: None,
        payload: serde_json::json!({"text": "too large"}),
        attachments: vec![],
        published: Utc::now(),
        idempotency_key: IdempotencyKey::new("slack:C01ABC:too-large"),
        meta: serde_json::json!({
            "canonical_json": serde_json::json!({
                "source": "slack",
                "object_id": "channel:C01ABC:ts:too-large",
                "body": "too large"
            }).to_string(),
            "source_container": "slack-test:C01ABC",
        }),
    };

    let err = service
        .ingest_observation_drafts(vec![draft], "payload-limit-test")
        .unwrap_err();
    assert!(matches!(
        err,
        SelfHostError::Ingestion(message) if message.contains("exceeds configured maximum 1")
    ));
    assert_eq!(
        service
            .persistence_lock()
            .unwrap()
            .observation_page(0, 10)
            .unwrap()
            .len(),
        0
    );

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn app_core_restores_persisted_slide_analysis_supplemental() {
    let observation = Observation {
        id: Observation::new_id(),
        schema: SchemaRef::new("schema:workspace-object-snapshot"),
        schema_version: SemVer::new("1.0.0"),
        observer: ObserverRef::new("obs:gslides-crawler"),
        source_system: Some(SourceSystemRef::new("sys:google-slides")),
        actor: None,
        authority_model: AuthorityModel::SourceAuthoritative,
        capture_model: CaptureModel::Snapshot,
        subject: EntityRef::new("document:gslides:pres123"),
        target: None,
        payload: serde_json::json!({
            "title": "自己紹介",
            "artifact": { "sourceObjectId": "pres123" },
            "relations": {
                "owner": "tanaka@example.jp",
                "editors": ["tanaka@example.jp"]
            }
        }),
        attachments: vec![],
        published: Utc::now(),
        recorded_at: Utc::now(),
        consent: None,
        idempotency_key: IdempotencyKey::new("gslides:pres123:rev:r1"),
        meta: serde_json::json!({}),
    };
    let supplemental = SupplementalRecord {
        id: SupplementalId::new("sup:slide-analysis:pres123:slide-1"),
        kind: "slide-analysis".into(),
        derived_from: InputAnchorSet {
            observations: vec![observation.id.clone()],
            blobs: vec![],
            supplementals: vec![],
        },
        payload: serde_json::json!({
            "name": "田中太郎",
            "bio_text": "私は田中太郎です",
            "source_slide_object_id": "slide-1",
            "source_document_id": "document:gslides:pres123#slide:slide-1"
        }),
        created_by: ActorRef::new("actor:test"),
        created_at: Utc::now(),
        mutability: Mutability::ManagedCache,
        record_version: Some("1".into()),
        model_version: Some("fixture".into()),
        consent_metadata: None,
        lineage: None,
    };

    let core = AppCore::new(vec![observation], vec![], vec![supplemental]).unwrap();
    assert_eq!(
        core.snapshot.person_page.profiles[0]
            .self_intro_text
            .as_deref(),
        Some("私は田中太郎です")
    );
}
