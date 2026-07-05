use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use chrono::Utc;
use lethe_adapter_api::retry::ResilientExecutor;
use lethe_adapter_api::traits::ObservationDraft;
use lethe_adapter_gslides::gslides::client::{PresentationNative, SlideNative, SlideRevision};
use lethe_adapter_slack::slack::client::{SlackMessage, SlackMessageType};
use lethe_core::domain::supplemental::InputAnchorSet;

use super::{
    AppCore, AppService, GoogleSourceRuntime, ProjectionSnapshot, SelfHostError,
    SlackSourceRuntime, extract_slide_text_fragments, infer_profile_name_from_fragments,
    known_thread_roots_from_observations, latest_revision_to_capture, namespace_draft,
    non_empty_state, ranked_self_intro_slide_indices, thread_cursor_key, thread_root_ts,
};
use crate::self_host::config::{
    ApiTokenConfig, CorpusProjectionConfig, GoogleConfig, JsonWebKey, JsonWebKeySet,
    McpOAuthConfig, ResourceLimits, SecretString, SelfHostConfig, SlackConfig, SlideAiConfig,
    SupplementalConfig,
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
        database_path: db,
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
        },
        slack_sources: vec![SlackConfig {
            id: "slack-test".into(),
            bot_token: SecretString::new("xoxb-test-token").unwrap(),
            thread_token: SecretString::new("xoxp-test-thread-token").unwrap(),
            channel_ids: vec!["C01ABC".into()],
        }],
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

#[test]
fn bootstrap_rebuilds_snapshot_from_persisted_observations() {
    let root = std::env::temp_dir().join(format!("lethe-self-host-test-{}", uuid::Uuid::now_v7()));
    let db = root.join("lethe.sqlite3");
    let blobs = root.join("blobs");
    let persistence = SqlitePersistence::open(&db, &blobs, &[7; 32]).unwrap();
    let observation = Observation {
        id: Observation::new_id(),
        schema: SchemaRef::new("schema:claude-message"),
        schema_version: SemVer::new("1.0.0"),
        observer: ObserverRef::new("obs:claude-importer"),
        source_system: Some(SourceSystemRef::new("sys:claude-ai")),
        actor: None,
        authority_model: AuthorityModel::LakeAuthoritative,
        capture_model: CaptureModel::Event,
        subject: EntityRef::new("conversation:claude:bootstrap"),
        target: None,
        payload: serde_json::json!({"text": "persisted bootstrap needle"}),
        attachments: vec![],
        published: Utc::now(),
        recorded_at: Utc::now(),
        consent: None,
        idempotency_key: IdempotencyKey::new("claude:bootstrap:needle"),
        meta: serde_json::json!({
            "canonical_json": serde_json::json!({
                "source": "claude-ai",
                "object_id": "bootstrap-needle",
                "body": "persisted bootstrap needle"
            }).to_string(),
            "source_container": "claude-bootstrap",
        }),
    };
    persistence.persist_observation(&observation).unwrap();
    persistence
        .materialize_projection(
            &ProjectionRef::new("proj:person-page"),
            &serde_json::to_value(ProjectionSnapshot::default()).unwrap(),
        )
        .unwrap();
    drop(persistence);

    let mut config = test_config(db.clone(), blobs.clone());
    config.corpus.mode = lethe_projection_corpus::CorpusMode::PersonalAllText;
    let service = AppService::bootstrap(config).unwrap();
    let response = service
        .corpus_grep_response(&lethe_api::api::grep::GrepRequest {
            pattern: "bootstrap needle".into(),
            limit: Some(3),
            ..lethe_api::api::grep::GrepRequest::default()
        })
        .unwrap();

    assert_eq!(response.data.matches.len(), 1);
    assert!(
        response
            .data
            .projection_watermark
            .starts_with("proj:corpus:")
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
    let service = AppService {
        core: Arc::new(Mutex::new(AppCore::new(vec![], vec![], vec![]).unwrap())),
        persistence: Arc::new(Mutex::new(Box::new(persistence))),
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
    };

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
        }),
    };

    let result = service.ingest_draft(draft).unwrap();
    assert!(matches!(result, IngestResult::Duplicate { .. }));
    assert_eq!(service.core_lock().unwrap().lake.len(), 0);
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
