use std::path::PathBuf;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use lethe_adapter_coding_agent::codex::CodexImporter;
use lethe_core::domain::supplemental::InputAnchorSet;
use lethe_core::domain::{
    ActorRef, AuthorityModel, CaptureModel, EntityRef, IdempotencyKey, Mutability, Observation,
    ObserverRef, SchemaRef, SemVer, SourceSystemRef, SupplementalId, SupplementalRecord,
};
use lethe_projection_corpus::{CorpusConfig, CorpusMode};
use lethe_runtime::runtime::partition::RoutingKeyOrder;
use lethe_selfhost::self_host::app::{AppService, ProjectionSnapshot};
use lethe_selfhost::self_host::config::{
    ApiTokenConfig, CorpusProjectionConfig, GoogleConfig, JsonWebKey, JsonWebKeySet,
    McpOAuthConfig, ResourceLimits, SecretString, SelfHostConfig, SlackConfig, SlideAiConfig,
    SupplementalConfig,
};
use lethe_selfhost::self_host::server::build_router;
use lethe_storage_sqlite::persistence::SqlitePersistence;
use tower::util::ServiceExt;

fn temp_paths() -> (PathBuf, PathBuf, PathBuf) {
    let root = std::env::temp_dir().join(format!("lethe-self-host-test-{}", uuid::Uuid::now_v7()));
    let db = root.join("lethe.sqlite3");
    let blobs = root.join("blobs");
    (root, db, blobs)
}

fn slack_observation(
    user_id: &str,
    email: &str,
    name: &str,
    text: &str,
    channel: &str,
    key: &str,
) -> Observation {
    Observation {
        id: Observation::new_id(),
        schema: SchemaRef::new("schema:slack-message"),
        schema_version: SemVer::new("1.0.0"),
        observer: ObserverRef::new("obs:slack-crawler"),
        source_system: Some(SourceSystemRef::new("sys:slack")),
        actor: None,
        authority_model: AuthorityModel::LakeAuthoritative,
        capture_model: CaptureModel::Event,
        subject: EntityRef::new(format!("message:slack:{key}")),
        target: None,
        payload: serde_json::json!({
            "user_id": user_id,
            "user_name": name,
            "email": email,
            "text": text,
            "channel": channel,
            "channel_id": format!("chan:{channel}"),
            "channel_name": channel,
        }),
        attachments: vec![],
        published: chrono::Utc::now(),
        recorded_at: chrono::Utc::now(),
        consent: None,
        idempotency_key: IdempotencyKey::new(key),
        meta: serde_json::json!({
            "canonical_json": serde_json::json!({
                "source": "slack",
                "object_id": key,
                "body": text,
            }).to_string(),
            "source_container": format!("slack-test:{channel}"),
        }),
    }
}

fn gslides_observation(editors: &[&str], owner: &str, title: &str, key: &str) -> Observation {
    Observation {
        id: Observation::new_id(),
        schema: SchemaRef::new("schema:workspace-object-snapshot"),
        schema_version: SemVer::new("1.0.0"),
        observer: ObserverRef::new("obs:gslides-crawler"),
        source_system: Some(SourceSystemRef::new("sys:google-slides")),
        actor: None,
        authority_model: AuthorityModel::SourceAuthoritative,
        capture_model: CaptureModel::Snapshot,
        subject: EntityRef::new(format!("document:gslides:{key}")),
        target: None,
        payload: serde_json::json!({
            "title": title,
            "relations": {
                "editors": editors,
                "owner": owner,
            },
            "revision": {
                "sourceRevisionId": format!("rev-{key}"),
            }
        }),
        attachments: vec![],
        published: chrono::Utc::now(),
        recorded_at: chrono::Utc::now(),
        consent: None,
        idempotency_key: IdempotencyKey::new(format!("gslides-{key}")),
        meta: serde_json::json!({
            "canonical_json": serde_json::json!({
                "source": "google-slides",
                "object_id": key,
                "title": title,
            }).to_string(),
            "source_container": "google-test",
        }),
    }
}

fn test_config(db: PathBuf, blobs: PathBuf) -> SelfHostConfig {
    test_config_with_corpus(db, blobs, CorpusMode::WorkspaceFiltered)
}

fn personal_test_config(db: PathBuf, blobs: PathBuf) -> SelfHostConfig {
    test_config_with_corpus(db, blobs, CorpusMode::PersonalAllText)
}

fn supplemental_write_config(db: PathBuf, blobs: PathBuf) -> SelfHostConfig {
    let mut config = test_config(db, blobs);
    config.api_tokens = vec![ApiTokenConfig {
        token: SecretString::new("write-token").unwrap(),
        scopes: vec!["write:supplemental".into()],
    }];
    config
}

fn supplemental_read_write_config(
    db: PathBuf,
    blobs: PathBuf,
    corpus_mode: CorpusMode,
) -> SelfHostConfig {
    let mut config = test_config_with_corpus(db, blobs, corpus_mode);
    config.api_tokens = vec![ApiTokenConfig {
        token: SecretString::new("integration-token").unwrap(),
        scopes: vec!["read:corpus".into(), "write:supplemental".into()],
    }];
    config
}

fn test_config_with_corpus(db: PathBuf, blobs: PathBuf, corpus_mode: CorpusMode) -> SelfHostConfig {
    if db.exists() {
        let persistence = SqlitePersistence::open(&db, &blobs, &[7; 32]).unwrap();
        let observations = persistence.load_observations().unwrap();
        if !observations.is_empty() {
            let snapshot = ProjectionSnapshot::build(
                observations,
                persistence.load_supplementals().unwrap(),
                CorpusConfig {
                    mode: corpus_mode,
                    ..CorpusConfig::default()
                },
            )
            .unwrap();
            persistence
                .materialize_projection(
                    &lethe_core::domain::ProjectionRef::new("proj:person-page"),
                    &serde_json::to_value(snapshot).unwrap(),
                )
                .unwrap();
        }
    }
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
            scopes: vec![
                "read:persons".into(),
                "read:timeline".into(),
                "read:corpus".into(),
                "read:answer-log".into(),
            ],
        }],
        resource_limits: ResourceLimits {
            max_blob_bytes: 10 * 1024 * 1024,
            max_payload_bytes: 1024 * 1024,
            max_sync_items: 10_000,
            max_page_size: 100,
            max_leaf_observations: 100_000,
            retention_days: 30,
        },
        corpus: CorpusProjectionConfig { mode: corpus_mode },
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

fn corpus_slack_observation(channel: &str, text: &str, key: &str, is_bot: bool) -> Observation {
    Observation {
        id: Observation::new_id(),
        schema: SchemaRef::new("schema:slack-message"),
        schema_version: SemVer::new("1.0.0"),
        observer: ObserverRef::new("obs:slack-crawler"),
        source_system: Some(SourceSystemRef::new("sys:slack")),
        actor: None,
        authority_model: AuthorityModel::LakeAuthoritative,
        capture_model: CaptureModel::Event,
        subject: EntityRef::new(format!("message:slack:{key}")),
        target: None,
        payload: serde_json::json!({
            "user_id": if is_bot { "B123" } else { "U123" },
            "user_name": if is_bot { "bot" } else { "Ada" },
            "text": text,
            "channel": channel,
            "channel_id": format!("C-{channel}"),
            "channel_name": channel,
            "is_public_channel": true,
            "is_bot": is_bot,
            "ts": format!("{}.000000", key.len() + 1),
            "thread_ts": "1.000000",
            "permalink": format!("https://slack.example/{channel}/{key}"),
        }),
        attachments: vec![],
        published: chrono::Utc::now(),
        recorded_at: chrono::Utc::now(),
        consent: None,
        idempotency_key: IdempotencyKey::new(format!("corpus-slack:{key}")),
        meta: serde_json::json!({
            "canonical_json": serde_json::json!({"source": "slack", "object_id": key, "body": text}).to_string(),
            "source_container": format!("slack-test:{channel}"),
        }),
    }
}

fn form_response_content_observation(key: &str) -> Observation {
    Observation {
        id: Observation::new_id(),
        schema: SchemaRef::new("schema:workspace-object-snapshot"),
        schema_version: SemVer::new("1.0.0"),
        observer: ObserverRef::new("obs:gforms-crawler"),
        source_system: Some(SourceSystemRef::new("sys:google-forms")),
        actor: None,
        authority_model: AuthorityModel::SourceAuthoritative,
        capture_model: CaptureModel::Snapshot,
        subject: EntityRef::new(format!("document:gforms:{key}")),
        target: None,
        payload: serde_json::json!({
            "title": "Secret form",
            "artifact": {
                "provider": "google",
                "service": "forms",
                "objectType": "form-response-content",
                "sourceObjectId": key,
                "canonicalUri": format!("https://docs.google.com/forms/d/{key}")
            },
            "response": {
                "answers": {"secret": "個別回答"}
            }
        }),
        attachments: vec![],
        published: chrono::Utc::now(),
        recorded_at: chrono::Utc::now(),
        consent: None,
        idempotency_key: IdempotencyKey::new(format!("form-content:{key}")),
        meta: serde_json::json!({
            "canonical_json": serde_json::json!({"form": key}).to_string(),
            "source_container": "google-forms",
        }),
    }
}

fn answer_log_observation(key: &str) -> Observation {
    Observation {
        id: Observation::new_id(),
        schema: SchemaRef::new("schema:bot-answer-log"),
        schema_version: SemVer::new("1.0.0"),
        observer: ObserverRef::new("obs:search-bot"),
        source_system: Some(SourceSystemRef::new("sys:lethe-internal")),
        actor: None,
        authority_model: AuthorityModel::LakeAuthoritative,
        capture_model: CaptureModel::Event,
        subject: EntityRef::new(format!("answer-log:{key}")),
        target: None,
        payload: serde_json::json!({
            "question": "忘れ物はどこですか",
            "answer": "受付にあります",
            "citations": [{"url": "https://slack.example/123_event/a", "record_id": "corpus:slack:C-123_event:1.000000", "source_type": "slack"}],
            "used_queries": ["忘れ物"],
            "asker": "user@example.com",
            "ts": chrono::Utc::now().to_rfc3339(),
            "model": "test",
            "usage": {},
            "confidence": "medium",
            "unknowns": []
        }),
        attachments: vec![],
        published: chrono::Utc::now(),
        recorded_at: chrono::Utc::now(),
        consent: None,
        idempotency_key: IdempotencyKey::new(format!("answer-log:{key}")),
        meta: serde_json::json!({
            "canonical_json": serde_json::json!({"answer": key}).to_string(),
            "source_container": "answer-log",
        }),
    }
}

fn fixed_time(second: u32) -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::parse_from_rfc3339(&format!("2026-07-05T00:00:{second:02}Z"))
        .unwrap()
        .to_utc()
}

fn claim_supplemental(
    id: &str,
    observation: &Observation,
    statement: &str,
    verification_mode: &str,
    second: u32,
) -> SupplementalRecord {
    SupplementalRecord {
        id: SupplementalId::new(id),
        kind: "claim@1".into(),
        derived_from: InputAnchorSet {
            observations: vec![observation.id.clone()],
            blobs: vec![],
            supplementals: vec![],
        },
        payload: serde_json::json!({
            "statement": statement,
            "verification_mode": verification_mode
        }),
        created_by: ActorRef::new("actor:test"),
        created_at: fixed_time(second),
        mutability: Mutability::AppendOnly,
        record_version: None,
        model_version: Some("fixture-model".into()),
        consent_metadata: None,
        lineage: None,
    }
}

fn claim_transition_supplemental(
    id: &str,
    target: &str,
    to_state: &str,
    second: u32,
) -> SupplementalRecord {
    SupplementalRecord {
        id: SupplementalId::new(id),
        kind: "claim-transition@1".into(),
        derived_from: InputAnchorSet {
            observations: vec![],
            blobs: vec![],
            supplementals: vec![SupplementalId::new(target)],
        },
        payload: serde_json::json!({
            "to_state": to_state,
            "reason": "fixture"
        }),
        created_by: ActorRef::new("actor:test"),
        created_at: fixed_time(second),
        mutability: Mutability::AppendOnly,
        record_version: None,
        model_version: None,
        consent_metadata: None,
        lineage: None,
    }
}

fn verification_result_supplemental(
    id: &str,
    target: &str,
    verdict: &str,
    second: u32,
) -> SupplementalRecord {
    SupplementalRecord {
        id: SupplementalId::new(id),
        kind: "verification-result@1".into(),
        derived_from: InputAnchorSet {
            observations: vec![],
            blobs: vec![],
            supplementals: vec![SupplementalId::new(target)],
        },
        payload: serde_json::json!({
            "verdict": verdict,
            "reasoning": "fixture"
        }),
        created_by: ActorRef::new("actor:test"),
        created_at: fixed_time(second),
        mutability: Mutability::AppendOnly,
        record_version: None,
        model_version: None,
        consent_metadata: None,
        lineage: None,
    }
}

fn decision_supplemental(
    id: &str,
    observation: &Observation,
    statement: &str,
    rationale: &str,
    supersedes: Vec<&str>,
    second: u32,
) -> SupplementalRecord {
    SupplementalRecord {
        id: SupplementalId::new(id),
        kind: "decision@1".into(),
        derived_from: InputAnchorSet {
            observations: vec![observation.id.clone()],
            blobs: vec![],
            supplementals: vec![],
        },
        payload: serde_json::json!({
            "statement": statement,
            "rationale": rationale,
            "supersedes": supersedes
        }),
        created_by: ActorRef::new("actor:test"),
        created_at: fixed_time(second),
        mutability: Mutability::AppendOnly,
        record_version: None,
        model_version: None,
        consent_metadata: None,
        lineage: None,
    }
}

fn supplemental_id() -> SupplementalId {
    SupplementalId::new(format!("sup:{}", uuid::Uuid::now_v7()))
}

fn claim_supplemental_body(id: &SupplementalId, observation: &Observation) -> serde_json::Value {
    serde_json::json!({
        "id": id.as_str(),
        "kind": "claim@1",
        "derived_from": {
            "observations": [observation.id.as_str()],
            "blobs": [],
            "supplementals": []
        },
        "payload": {
            "statement": "Track B claim",
            "verification_mode": "check"
        },
        "created_by": "actor:extraction-pass",
        "mutability": "append_only",
        "model_version": "model:test"
    })
}

fn claim_transition_supplemental_body(
    id: &SupplementalId,
    claim_id: &SupplementalId,
    to_state: &str,
) -> serde_json::Value {
    serde_json::json!({
        "id": id.as_str(),
        "kind": "claim-transition@1",
        "derived_from": {
            "observations": [],
            "blobs": [],
            "supplementals": [claim_id.as_str()]
        },
        "payload": {
            "to_state": to_state,
            "reason": "Track I integration test"
        },
        "created_by": "actor:integration-test",
        "mutability": "append_only",
        "model_version": null
    })
}

fn decision_supplemental_body(
    id: &SupplementalId,
    observation: &Observation,
    statement: &str,
) -> serde_json::Value {
    serde_json::json!({
        "id": id.as_str(),
        "kind": "decision@1",
        "derived_from": {
            "observations": [observation.id.as_str()],
            "blobs": [],
            "supplementals": []
        },
        "payload": {
            "statement": statement,
            "rationale": "Track I decision search integration"
        },
        "created_by": "actor:integration-test",
        "mutability": "append_only",
        "model_version": "model:test"
    })
}

fn post_supplemental(
    app: axum::Router,
    token: &str,
    body: serde_json::Value,
) -> (StatusCode, serde_json::Value) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let response = runtime
        .block_on(async {
            app.oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/supplementals")
                    .header("authorization", format!("Bearer {token}"))
                    .header("content-type", "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
        })
        .unwrap();
    let status = response.status();
    let body = runtime
        .block_on(async { axum::body::to_bytes(response.into_body(), usize::MAX).await })
        .unwrap();
    (status, serde_json::from_slice(&body).unwrap())
}

fn get_json(app: axum::Router, token: &str, uri: &str) -> (StatusCode, serde_json::Value) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let response = runtime
        .block_on(async {
            app.oneshot(
                Request::builder()
                    .uri(uri)
                    .header("authorization", format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
        })
        .unwrap();
    let status = response.status();
    let body = runtime
        .block_on(async { axum::body::to_bytes(response.into_body(), usize::MAX).await })
        .unwrap();
    (status, serde_json::from_slice(&body).unwrap())
}

fn personal_text_observation(
    schema: &str,
    source_system: &str,
    subject: &str,
    payload: serde_json::Value,
    key: &str,
    published: &str,
) -> Observation {
    Observation {
        id: Observation::new_id(),
        schema: SchemaRef::new(schema),
        schema_version: SemVer::new("1.0.0"),
        observer: ObserverRef::new(format!("obs:{source_system}:test")),
        source_system: Some(SourceSystemRef::new(source_system)),
        actor: None,
        authority_model: AuthorityModel::LakeAuthoritative,
        capture_model: CaptureModel::Event,
        subject: EntityRef::new(subject),
        target: None,
        payload,
        attachments: vec![],
        published: chrono::DateTime::parse_from_rfc3339(published)
            .unwrap()
            .to_utc(),
        recorded_at: chrono::Utc::now(),
        consent: None,
        idempotency_key: IdempotencyKey::new(key),
        meta: serde_json::json!({
            "canonical_json": serde_json::json!({"key": key}).to_string(),
            "source_container": source_system,
        }),
    }
}

fn codex_integration_jsonl() -> String {
    [
        serde_json::json!({
            "timestamp": "2026-07-05T00:00:00.000Z",
            "type": "session_meta",
            "payload": {
                "session_id": "track-i-codex-session",
                "id": "track-i-codex-transcript",
                "timestamp": "2026-07-05T00:00:00.000Z",
                "cwd": "D:\\repo",
                "originator": "codex-tui",
                "source": "cli",
                "thread_source": "user"
            }
        }),
        serde_json::json!({
            "timestamp": "2026-07-05T00:00:01.000Z",
            "type": "response_item",
            "payload": {
                "type": "message",
                "id": "msg-track-i",
                "role": "user",
                "content": [
                    {
                        "type": "input_text",
                        "text": "Track I Codex imported observation anchor"
                    }
                ]
            }
        }),
    ]
    .into_iter()
    .map(|value| value.to_string())
    .collect::<Vec<_>>()
    .join("\n")
}

#[test]
fn self_host_persons_endpoint_returns_projection_data() {
    let (root, db, blobs) = temp_paths();
    let persistence = SqlitePersistence::open(&db, &blobs, &[7; 32]).unwrap();
    persistence
        .persist_observation(&slack_observation(
            "U100",
            "tanaka@example.jp",
            "田中太郎",
            "おはよう",
            "general",
            "s1",
        ))
        .unwrap();
    persistence
        .persist_observation(&gslides_observation(
            &["tanaka@example.jp"],
            "tanaka@example.jp",
            "田中の自己紹介",
            "g1",
        ))
        .unwrap();

    let app = build_router(AppService::bootstrap(test_config(db, blobs)).unwrap());

    let runtime = tokio::runtime::Runtime::new().unwrap();
    let response = runtime
        .block_on(async {
            app.clone()
                .oneshot(
                    Request::builder()
                        .uri("/api/projections/proj:person-page/records")
                        .header("authorization", "Bearer test-api-token")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
        })
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = runtime
        .block_on(async { axum::body::to_bytes(response.into_body(), usize::MAX).await })
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(
        json["projection_metadata"]["projection_id"],
        "proj:person-page"
    );
    assert!(
        json["projection_metadata"]["lineage_ref"]
            .as_str()
            .is_some_and(|value| value.starts_with("lineage:person-page:build-"))
    );
    assert_eq!(json["data"]["total"], 1);
    assert_eq!(json["data"]["data"][0]["display_name"], "田中太郎");

    let lineage_response = runtime
        .block_on(async {
            app.oneshot(
                Request::builder()
                    .uri("/api/projections/proj:person-page/lineage")
                    .header("authorization", "Bearer test-api-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
        })
        .unwrap();
    assert_eq!(lineage_response.status(), StatusCode::OK);
    let lineage_body = runtime
        .block_on(async { axum::body::to_bytes(lineage_response.into_body(), usize::MAX).await })
        .unwrap();
    let lineage_json: serde_json::Value = serde_json::from_slice(&lineage_body).unwrap();
    assert_eq!(lineage_json["projection_id"], "proj:person-page");
    assert_eq!(lineage_json["input_refs"].as_array().unwrap().len(), 2);

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn personal_corpus_grep_hits_all_text_source_types() {
    let (root, db, blobs) = temp_paths();
    let persistence = SqlitePersistence::open(&db, &blobs, &[7; 32]).unwrap();
    let needle = "track-g-cross-source-needle";
    let observations = vec![
        personal_text_observation(
            "schema:claude-message",
            "sys:claude-ai",
            "message:claude-ai:m1",
            serde_json::json!({
                "conversation_uuid": "claude-conv",
                "message_uuid": "claude-msg",
                "sender": "human",
                "text": format!("{needle} claude ai")
            }),
            "personal:claude-ai",
            "2026-07-01T00:00:00Z",
        ),
        personal_text_observation(
            "schema:github-event",
            "sys:github",
            "github:owner/repo#issue#1",
            serde_json::json!({
                "object_type": "issue",
                "repo": "owner/repo",
                "number": 1,
                "title": format!("{needle} issue"),
                "body": "issue body"
            }),
            "personal:github-issue",
            "2026-07-01T00:01:00Z",
        ),
        personal_text_observation(
            "schema:github-event",
            "sys:github",
            "github:owner/repo#pr#2",
            serde_json::json!({
                "object_type": "pull_request",
                "repo": "owner/repo",
                "number": 2,
                "title": format!("{needle} pr"),
                "body": "pr body"
            }),
            "personal:github-pr",
            "2026-07-01T00:02:00Z",
        ),
        personal_text_observation(
            "schema:github-event",
            "sys:github",
            "github:owner/repo#issue_comment#3",
            serde_json::json!({
                "object_type": "issue_comment",
                "repo": "owner/repo",
                "id": 3,
                "body": format!("{needle} comment")
            }),
            "personal:github-comment",
            "2026-07-01T00:03:00Z",
        ),
        personal_text_observation(
            "schema:github-event",
            "sys:github",
            "github:owner/repo#commit#abc",
            serde_json::json!({
                "object_type": "commit",
                "repo": "owner/repo",
                "sha": "abc",
                "message": format!("{needle} commit")
            }),
            "personal:github-commit",
            "2026-07-01T00:04:00Z",
        ),
        personal_text_observation(
            "schema:claude-code-message",
            "sys:claude-code",
            "message:claude-code:main:m1",
            serde_json::json!({
                "session_id": "cc-main",
                "message_uuid": "cc-msg",
                "role": "assistant",
                "text": format!("{needle} claude code")
            }),
            "personal:claude-code",
            "2026-07-01T00:05:00Z",
        ),
        personal_text_observation(
            "schema:codex-message",
            "sys:codex",
            "message:codex:main:m1",
            serde_json::json!({
                "session_id": "codex-main",
                "message_uuid": "codex-msg",
                "role": "assistant",
                "text": format!("{needle} codex")
            }),
            "personal:codex",
            "2026-07-01T00:06:00Z",
        ),
    ];
    for observation in observations {
        persistence.persist_observation(&observation).unwrap();
    }

    let app = build_router(AppService::bootstrap(personal_test_config(db, blobs)).unwrap());
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let response = runtime
        .block_on(async {
            app.oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/projections/proj:corpus/grep")
                    .header("authorization", "Bearer test-api-token")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({"pattern": needle, "limit": 20}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
        })
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = runtime
        .block_on(async { axum::body::to_bytes(response.into_body(), usize::MAX).await })
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let source_types = json["data"]["matches"]
        .as_array()
        .unwrap()
        .iter()
        .map(|matched| matched["source_type"].as_str().unwrap())
        .collect::<std::collections::BTreeSet<_>>();

    assert_eq!(
        source_types,
        std::collections::BTreeSet::from([
            "claude-ai",
            "github-issue",
            "github-pr",
            "github-comment",
            "github-commit",
            "claude-code",
            "codex",
        ])
    );

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn coding_agent_get_thread_preserves_parent_child_sessions() {
    let (root, db, blobs) = temp_paths();
    let persistence = SqlitePersistence::open(&db, &blobs, &[7; 32]).unwrap();
    let main = personal_text_observation(
        "schema:claude-code-message",
        "sys:claude-code",
        "message:claude-code:main:m1",
        serde_json::json!({
            "session_id": "main-session",
            "message_uuid": "main-message",
            "role": "user",
            "text": "main session context"
        }),
        "thread:main",
        "2026-07-01T00:00:00Z",
    );
    let child = personal_text_observation(
        "schema:claude-code-message",
        "sys:claude-code",
        "message:claude-code:child:m1",
        serde_json::json!({
            "session_id": "child-session",
            "parent_session_id": "main-session",
            "is_sidechain": true,
            "message_uuid": "child-message",
            "role": "assistant",
            "text": "delegated sidechain conclusion"
        }),
        "thread:child",
        "2026-07-01T00:01:00Z",
    );
    persistence.persist_observation(&main).unwrap();
    persistence.persist_observation(&child).unwrap();

    let app = build_router(AppService::bootstrap(personal_test_config(db, blobs)).unwrap());
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let grep_response = runtime
        .block_on(async {
            app.clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/api/projections/proj:corpus/grep")
                        .header("authorization", "Bearer test-api-token")
                        .header("content-type", "application/json")
                        .body(Body::from(
                            serde_json::json!({
                                "pattern": "delegated sidechain conclusion",
                                "limit": 5
                            })
                            .to_string(),
                        ))
                        .unwrap(),
                )
                .await
        })
        .unwrap();
    assert_eq!(grep_response.status(), StatusCode::OK);
    let grep_body = runtime
        .block_on(async { axum::body::to_bytes(grep_response.into_body(), usize::MAX).await })
        .unwrap();
    let grep_json: serde_json::Value = serde_json::from_slice(&grep_body).unwrap();
    let child_record_id = grep_json["data"]["matches"][0]["record_id"]
        .as_str()
        .unwrap();

    let thread_response = runtime
        .block_on(async {
            app.oneshot(
                Request::builder()
                    .uri(format!(
                        "/api/projections/proj:corpus/threads/{child_record_id}"
                    ))
                    .header("authorization", "Bearer test-api-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
        })
        .unwrap();
    assert_eq!(thread_response.status(), StatusCode::OK);
    let thread_body = runtime
        .block_on(async { axum::body::to_bytes(thread_response.into_body(), usize::MAX).await })
        .unwrap();
    let thread_json: serde_json::Value = serde_json::from_slice(&thread_body).unwrap();

    assert_eq!(
        thread_json["data"]["structure"]["thread_key"],
        "claude-code:session:main-session"
    );
    assert_eq!(
        thread_json["data"]["structure"]["root_session"]["session_id"],
        "main-session"
    );
    assert_eq!(
        thread_json["data"]["structure"]["sidechains"][0]["session_id"],
        "child-session"
    );
    assert_eq!(
        thread_json["data"]["structure"]["sidechains"][0]["parent_session_id"],
        "main-session"
    );
    assert_eq!(thread_json["data"]["records"].as_array().unwrap().len(), 2);
    assert!(
        thread_json["data"]["structure"]["sidechains"][0]["record_ids"]
            .as_array()
            .unwrap()
            .iter()
            .any(|record_id| record_id.as_str() == Some(child_record_id))
    );

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn opted_out_person_is_excluded_by_filtering_projection() {
    let (root, db, blobs) = temp_paths();
    let persistence = SqlitePersistence::open(&db, &blobs, &[7; 32]).unwrap();
    persistence
        .persist_observation(&slack_observation(
            "U200",
            "optout@example.jp",
            "非表示対象",
            "private",
            "general",
            "optout-s1",
        ))
        .unwrap();
    persistence
        .persist_observation(&gslides_observation(
            &["optout@example.jp"],
            "optout@example.jp",
            "非表示対象の自己紹介",
            "optout-g1",
        ))
        .unwrap();
    persistence
        .persist_observation(&Observation {
            id: Observation::new_id(),
            schema: SchemaRef::new("schema:consent-decision"),
            schema_version: SemVer::new("1.0.0"),
            observer: ObserverRef::new("obs:consent-ledger"),
            source_system: Some(SourceSystemRef::new("sys:lethe-governance")),
            actor: Some(EntityRef::new("person:reviewer")),
            authority_model: AuthorityModel::LakeAuthoritative,
            capture_model: CaptureModel::Event,
            subject: EntityRef::new("person:consent-subject"),
            target: None,
            payload: serde_json::json!({
                "status": "opted_out",
                "identifier": "optout@example.jp",
                "reason": "subject request"
            }),
            attachments: vec![],
            published: chrono::Utc::now(),
            recorded_at: chrono::Utc::now(),
            consent: None,
            idempotency_key: IdempotencyKey::new("consent:optout@example.jp:opted-out"),
            meta: serde_json::json!({
                "canonical_json": serde_json::json!({
                    "identifier": "optout@example.jp",
                    "status": "opted_out"
                }).to_string(),
                "source_container": "governance"
            }),
        })
        .unwrap();

    let app = build_router(AppService::bootstrap(test_config(db, blobs)).unwrap());
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let response = runtime
        .block_on(async {
            app.oneshot(
                Request::builder()
                    .uri("/api/projections/proj:person-page/records")
                    .header("authorization", "Bearer test-api-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
        })
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = runtime
        .block_on(async { axum::body::to_bytes(response.into_body(), usize::MAX).await })
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["data"]["total"], 0);

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn projection_blob_endpoint_requires_auth_and_rejects_raw_cas_access() {
    let (root, db, blobs) = temp_paths();
    let persistence = SqlitePersistence::open(&db, &blobs, &[7; 32]).unwrap();
    let blob_ref = persistence.persist_blob(b"png-bytes").unwrap();
    let unreferenced_blob_ref = persistence.persist_blob(b"private-bytes").unwrap();
    let slack = slack_observation(
        "U100",
        "tanaka@example.jp",
        "田中太郎",
        "hello",
        "general",
        "blob-s1",
    );
    let slide = gslides_observation(
        &["tanaka@example.jp"],
        "tanaka@example.jp",
        "田中の自己紹介",
        "blob-g1",
    );
    persistence.persist_observation(&slack).unwrap();
    persistence.persist_observation(&slide).unwrap();
    persistence
        .persist_supplemental(&SupplementalRecord {
            id: SupplementalId::new("sup:blob-profile"),
            kind: "slide-analysis".into(),
            derived_from: InputAnchorSet {
                observations: vec![slide.id.clone()],
                blobs: vec![blob_ref.clone()],
                supplementals: vec![],
            },
            payload: serde_json::json!({
                "email": "tanaka@example.jp",
                "generated_email": null,
                "name": "田中太郎",
                "bio_text": "自己紹介",
                "profile_pic": null,
                "gallery_images": [],
                "properties": {},
                "attributes": [],
                "source_slide_object_id": "slide-1",
                "source_document_id": "document:gslides:blob-g1#slide:slide-1",
                "source_canonical_uri": null,
                "thumbnail_blob_ref": blob_ref.as_str(),
                "thumbnail_url": null,
                "companion_to_slide_object_id": null
            }),
            created_by: ActorRef::new("actor:test"),
            created_at: chrono::Utc::now(),
            mutability: Mutability::ManagedCache,
            record_version: Some("1".into()),
            model_version: Some("fixture".into()),
            consent_metadata: None,
            lineage: None,
        })
        .unwrap();
    let blob_hash = blob_ref
        .as_str()
        .strip_prefix("blob:sha256:")
        .unwrap()
        .to_string();
    let unreferenced_hash = unreferenced_blob_ref
        .as_str()
        .strip_prefix("blob:sha256:")
        .unwrap()
        .to_string();

    let app = build_router(AppService::bootstrap(test_config(db, blobs)).unwrap());

    let runtime = tokio::runtime::Runtime::new().unwrap();
    let unauthenticated_response = runtime
        .block_on(async {
            app.clone()
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/api/projections/proj:person-page/blobs/{blob_hash}"
                        ))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
        })
        .unwrap();
    assert_eq!(unauthenticated_response.status(), StatusCode::UNAUTHORIZED);

    let raw_cas_response = runtime
        .block_on(async {
            app.clone()
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/api/projections/proj:person-page/blobs/{unreferenced_hash}"
                        ))
                        .header("authorization", "Bearer test-api-token")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
        })
        .unwrap();
    assert_eq!(raw_cas_response.status(), StatusCode::NOT_FOUND);

    let response = runtime
        .block_on(async {
            app.oneshot(
                Request::builder()
                    .uri(format!(
                        "/api/projections/proj:person-page/blobs/{blob_hash}"
                    ))
                    .header("authorization", "Bearer test-api-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
        })
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok()),
        Some("image/png")
    );
    let body = runtime
        .block_on(async { axum::body::to_bytes(response.into_body(), usize::MAX).await })
        .unwrap();
    assert_eq!(body.as_ref(), b"png-bytes");

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn self_host_person_detail_hides_restricted_identities() {
    let (root, db, blobs) = temp_paths();
    let persistence = SqlitePersistence::open(&db, &blobs, &[7; 32]).unwrap();
    persistence
        .persist_observation(&slack_observation(
            "U100",
            "tanaka@example.jp",
            "田中太郎",
            "会議開始",
            "project-a",
            "s2",
        ))
        .unwrap();
    persistence
        .persist_observation(&gslides_observation(
            &["tanaka@example.jp"],
            "tanaka@example.jp",
            "田中の自己紹介",
            "g2",
        ))
        .unwrap();

    let app = build_router(AppService::bootstrap(test_config(db, blobs)).unwrap());

    let runtime = tokio::runtime::Runtime::new().unwrap();
    let list_response = runtime
        .block_on(async {
            app.clone()
                .oneshot(
                    Request::builder()
                        .uri("/api/projections/proj:person-page/records")
                        .header("authorization", "Bearer test-api-token")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
        })
        .unwrap();
    let list_body = runtime
        .block_on(async { axum::body::to_bytes(list_response.into_body(), usize::MAX).await })
        .unwrap();
    let list_json: serde_json::Value = serde_json::from_slice(&list_body).unwrap();
    let person_id = list_json["data"]["data"][0]["person_id"].as_str().unwrap();

    let detail_response = runtime
        .block_on(async {
            app.oneshot(
                Request::builder()
                    .uri(format!(
                        "/api/projections/proj:person-page/records/{person_id}"
                    ))
                    .header("authorization", "Bearer test-api-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
        })
        .unwrap();

    assert_eq!(detail_response.status(), StatusCode::OK);
    let detail_body = runtime
        .block_on(async { axum::body::to_bytes(detail_response.into_body(), usize::MAX).await })
        .unwrap();
    let detail_json: serde_json::Value = serde_json::from_slice(&detail_body).unwrap();

    assert_eq!(detail_json["data"]["display_name"], "田中太郎");
    assert!(detail_json["data"].get("identities").is_none());
    assert_eq!(
        detail_json["data"]["related_slides"][0]["title"],
        "田中の自己紹介"
    );

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn corpus_grep_filters_before_exposure_and_supports_pagination() {
    let (root, db, blobs) = temp_paths();
    let persistence = SqlitePersistence::open(&db, &blobs, &[7; 32]).unwrap();
    persistence
        .persist_observation(&corpus_slack_observation(
            "123_event",
            "部屋１２３の忘れ物",
            "a",
            false,
        ))
        .unwrap();
    persistence
        .persist_observation(&corpus_slack_observation(
            "general",
            "これは出てはいけない",
            "b",
            false,
        ))
        .unwrap();
    persistence
        .persist_observation(&corpus_slack_observation(
            "123_event",
            "bot 投稿",
            "c",
            true,
        ))
        .unwrap();
    persistence
        .persist_observation(&form_response_content_observation("form-secret"))
        .unwrap();

    let app = build_router(AppService::bootstrap(test_config(db, blobs)).unwrap());
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let response = runtime
        .block_on(async {
            app.clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/api/projections/proj:corpus/grep")
                        .header("authorization", "Bearer test-api-token")
                        .header("content-type", "application/json")
                        .body(Body::from(
                            serde_json::json!({
                                "pattern": "123|忘れ物",
                                "limit": 1
                            })
                            .to_string(),
                        ))
                        .unwrap(),
                )
                .await
        })
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = runtime
        .block_on(async { axum::body::to_bytes(response.into_body(), usize::MAX).await })
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["data"]["matches"].as_array().unwrap().len(), 1);
    assert_eq!(json["data"]["complete"], true);
    assert_eq!(json["data"]["matches"][0]["snippet"], "部屋１２３の忘れ物");
    assert!(
        !json.to_string().contains("これは出てはいけない"),
        "non-allowed Slack channel leaked into grep result"
    );
    assert!(
        !json.to_string().contains("個別回答"),
        "form response content leaked into grep result"
    );

    let record_id = json["data"]["matches"][0]["record_id"].as_str().unwrap();
    let record_response = runtime
        .block_on(async {
            app.clone()
                .oneshot(
                    Request::builder()
                        .uri(format!("/api/projections/proj:corpus/records/{record_id}"))
                        .header("authorization", "Bearer test-api-token")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
        })
        .unwrap();
    assert_eq!(record_response.status(), StatusCode::OK);

    let resolve_response = runtime
        .block_on(async {
            app.oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/projections/proj:corpus/resolve-link")
                    .header("authorization", "Bearer test-api-token")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({"url": "https://slack.example/123_event/a"}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
        })
        .unwrap();
    assert_eq!(resolve_response.status(), StatusCode::OK);

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn prior_qa_search_returns_answer_log_as_non_primary_source() {
    let (root, db, blobs) = temp_paths();
    let persistence = SqlitePersistence::open(&db, &blobs, &[7; 32]).unwrap();
    persistence
        .persist_observation(&answer_log_observation("a1"))
        .unwrap();
    persistence
        .persist_observation(&corpus_slack_observation(
            "123_event",
            "一次ソースの忘れ物",
            "primary",
            false,
        ))
        .unwrap();

    let app = build_router(AppService::bootstrap(test_config(db, blobs)).unwrap());
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let response = runtime
        .block_on(async {
            app.clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/api/projections/proj:answer-log/prior-qa-search")
                        .header("authorization", "Bearer test-api-token")
                        .header("content-type", "application/json")
                        .body(Body::from(
                            serde_json::json!({"query": "忘れ物", "limit": 10}).to_string(),
                        ))
                        .unwrap(),
                )
                .await
        })
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = runtime
        .block_on(async { axum::body::to_bytes(response.into_body(), usize::MAX).await })
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["data"]["is_primary_source"], false);
    assert_eq!(json["data"]["matches"][0]["is_primary_source"], false);
    assert_eq!(json["data"]["matches"][0]["answer"], "受付にあります");

    let grep_response = runtime
        .block_on(async {
            app.oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/projections/proj:corpus/grep")
                    .header("authorization", "Bearer test-api-token")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({"pattern": "受付にあります"}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
        })
        .unwrap();
    let grep_body = runtime
        .block_on(async { axum::body::to_bytes(grep_response.into_body(), usize::MAX).await })
        .unwrap();
    let grep_json: serde_json::Value = serde_json::from_slice(&grep_body).unwrap();
    assert_eq!(grep_json["data"]["matches"].as_array().unwrap().len(), 0);

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn supplemental_post_requires_write_scope_and_does_not_write_on_forbidden() {
    let (root, db, blobs) = temp_paths();
    let persistence = SqlitePersistence::open(&db, &blobs, &[7; 32]).unwrap();
    let observation = slack_observation(
        "U300",
        "writer@example.jp",
        "Writer",
        "anchor",
        "general",
        "sup-auth",
    );
    persistence.persist_observation(&observation).unwrap();

    let app = build_router(AppService::bootstrap(test_config(db.clone(), blobs.clone())).unwrap());
    let body = claim_supplemental_body(&supplemental_id(), &observation);
    let (status, json) = post_supplemental(app, "test-api-token", body);

    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(json["error"], "forbidden");
    assert!(persistence.load_supplementals().unwrap().is_empty());

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn supplemental_post_returns_201_and_persists_across_restart() {
    let (root, db, blobs) = temp_paths();
    let persistence = SqlitePersistence::open(&db, &blobs, &[7; 32]).unwrap();
    let observation = slack_observation(
        "U301",
        "writer@example.jp",
        "Writer",
        "anchor",
        "general",
        "sup-ok",
    );
    persistence.persist_observation(&observation).unwrap();

    let app = build_router(
        AppService::bootstrap(supplemental_write_config(db.clone(), blobs.clone())).unwrap(),
    );
    let id = supplemental_id();
    let (status, json) = post_supplemental(
        app,
        "write-token",
        claim_supplemental_body(&id, &observation),
    );

    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(json["data"]["id"], id.as_str());
    assert_eq!(json["data"]["kind"], "claim@1");
    assert_eq!(json["data"]["created_by"], "actor:extraction-pass");
    assert!(json["data"]["created_at"].as_str().is_some());

    let _restarted = build_router(
        AppService::bootstrap(supplemental_write_config(db.clone(), blobs.clone())).unwrap(),
    );
    let persisted = persistence.load_supplementals().unwrap();
    assert_eq!(persisted.len(), 1);
    assert_eq!(persisted[0].id, id);

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn supplemental_post_maps_store_invariants_to_422_details() {
    let (root, db, blobs) = temp_paths();
    let persistence = SqlitePersistence::open(&db, &blobs, &[7; 32]).unwrap();
    let observation = slack_observation(
        "U302",
        "writer@example.jp",
        "Writer",
        "anchor",
        "general",
        "sup-invalid",
    );
    persistence.persist_observation(&observation).unwrap();
    let app = build_router(
        AppService::bootstrap(supplemental_write_config(db.clone(), blobs.clone())).unwrap(),
    );

    let mut empty_anchor = claim_supplemental_body(&supplemental_id(), &observation);
    empty_anchor["derived_from"] = serde_json::json!({
        "observations": [],
        "blobs": [],
        "supplementals": []
    });
    let (status, json) = post_supplemental(app.clone(), "write-token", empty_anchor);
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(json["error"], "empty_anchor");
    assert_eq!(json["details"]["field"], "derived_from");

    let mut unresolved_observation = claim_supplemental_body(&supplemental_id(), &observation);
    unresolved_observation["derived_from"]["observations"] = serde_json::json!(["obs:missing"]);
    let (status, json) = post_supplemental(app.clone(), "write-token", unresolved_observation);
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(json["error"], "unresolved_anchor");
    assert_eq!(json["details"]["unresolved_observations"][0], "obs:missing");

    let mut unresolved_supplemental = claim_supplemental_body(&supplemental_id(), &observation);
    unresolved_supplemental["derived_from"]["observations"] = serde_json::json!([]);
    unresolved_supplemental["derived_from"]["supplementals"] =
        serde_json::json!(["sup:00000000-0000-0000-0000-000000000001"]);
    let (status, json) = post_supplemental(app, "write-token", unresolved_supplemental);
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(json["error"], "unresolved_anchor");
    assert_eq!(
        json["details"]["unresolved_supplementals"][0],
        "sup:00000000-0000-0000-0000-000000000001"
    );

    assert!(persistence.load_supplementals().unwrap().is_empty());
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn supplemental_post_same_id_conflicts_but_same_content_different_uuid_is_allowed() {
    let (root, db, blobs) = temp_paths();
    let persistence = SqlitePersistence::open(&db, &blobs, &[7; 32]).unwrap();
    let observation = slack_observation(
        "U303",
        "writer@example.jp",
        "Writer",
        "anchor",
        "general",
        "sup-conflict",
    );
    persistence.persist_observation(&observation).unwrap();
    let app = build_router(
        AppService::bootstrap(supplemental_write_config(db.clone(), blobs.clone())).unwrap(),
    );

    let first_id = supplemental_id();
    let first_body = claim_supplemental_body(&first_id, &observation);
    let (status, _) = post_supplemental(app.clone(), "write-token", first_body.clone());
    assert_eq!(status, StatusCode::CREATED);

    let (status, json) = post_supplemental(app.clone(), "write-token", first_body);
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(json["error"], "append_only_conflict");

    let second_id = supplemental_id();
    let second_body = claim_supplemental_body(&second_id, &observation);
    let (status, _) = post_supplemental(app, "write-token", second_body);
    assert_eq!(status, StatusCode::CREATED);

    let persisted = persistence.load_supplementals().unwrap();
    assert_eq!(persisted.len(), 2);
    assert!(persisted.iter().any(|record| record.id == first_id));
    assert!(persisted.iter().any(|record| record.id == second_id));

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn supplemental_post_rejects_claim_missing_verification_mode_before_write() {
    let (root, db, blobs) = temp_paths();
    let persistence = SqlitePersistence::open(&db, &blobs, &[7; 32]).unwrap();
    let observation = slack_observation(
        "U304",
        "writer@example.jp",
        "Writer",
        "anchor",
        "general",
        "sup-schema",
    );
    persistence.persist_observation(&observation).unwrap();
    let app = build_router(
        AppService::bootstrap(supplemental_write_config(db.clone(), blobs.clone())).unwrap(),
    );
    let mut body = claim_supplemental_body(&supplemental_id(), &observation);
    body["payload"] = serde_json::json!({
        "statement": "verification mode is missing"
    });

    let (status, json) = post_supplemental(app, "write-token", body);

    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(json["error"], "payload_schema_violation");
    assert!(
        json["details"]["violations"]
            .as_array()
            .unwrap()
            .iter()
            .any(|violation| violation["field"] == "verification_mode")
    );
    assert!(persistence.load_supplementals().unwrap().is_empty());

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn supplemental_post_updates_claim_queue_projection_state() {
    let (root, db, blobs) = temp_paths();
    let persistence = SqlitePersistence::open(&db, &blobs, &[7; 32]).unwrap();
    let observation = slack_observation(
        "U305",
        "writer@example.jp",
        "Writer",
        "Track I claim anchor",
        "general",
        "track-i-claim",
    );
    persistence.persist_observation(&observation).unwrap();

    let app = build_router(
        AppService::bootstrap(supplemental_read_write_config(
            db,
            blobs,
            CorpusMode::WorkspaceFiltered,
        ))
        .unwrap(),
    );
    let claim_id = supplemental_id();
    let (status, json) = post_supplemental(
        app.clone(),
        "integration-token",
        claim_supplemental_body(&claim_id, &observation),
    );
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(json["data"]["id"], claim_id.as_str());

    let (status, open_json) = get_json(
        app.clone(),
        "integration-token",
        "/projections/claim-queue?state=open&limit=10",
    );
    assert_eq!(status, StatusCode::OK);
    assert!(
        open_json["data"]["groups"]
            .as_array()
            .unwrap()
            .iter()
            .flat_map(|group| group["members"].as_array().unwrap())
            .any(|member| {
                member["representative_id"] == claim_id.as_str() && member["state"] == "open"
            }),
        "claim POST did not appear in open claim_queue projection: {open_json}"
    );

    let transition_id = supplemental_id();
    let (status, transition_json) = post_supplemental(
        app.clone(),
        "integration-token",
        claim_transition_supplemental_body(&transition_id, &claim_id, "parked"),
    );
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(transition_json["data"]["id"], transition_id.as_str());

    let (status, parked_json) = get_json(
        app,
        "integration-token",
        "/projections/claim-queue?state=parked&limit=10",
    );
    assert_eq!(status, StatusCode::OK);
    assert!(
        parked_json["data"]["groups"]
            .as_array()
            .unwrap()
            .iter()
            .flat_map(|group| group["members"].as_array().unwrap())
            .any(|member| {
                member["representative_id"] == claim_id.as_str() && member["state"] == "parked"
            }),
        "claim-transition POST did not update claim_queue projection: {parked_json}"
    );

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn decision_post_anchored_to_imported_codex_observation_is_searchable() {
    let (root, db, blobs) = temp_paths();
    let service = AppService::bootstrap(supplemental_read_write_config(
        db.clone(),
        blobs.clone(),
        CorpusMode::PersonalAllText,
    ))
    .unwrap();
    let batch = CodexImporter::new(SemVer::new("1.0.0"))
        .import_jsonl_str(&codex_integration_jsonl(), "codex/sessions/track-i.jsonl")
        .unwrap();
    assert_eq!(batch.drafts.len(), 1);
    let report = service
        .ingest_observation_drafts(batch.drafts, "codex-track-i")
        .unwrap();
    assert_eq!(report.ingested, 1);

    let persisted = SqlitePersistence::open(&db, &blobs, &[7; 32]).unwrap();
    let observation = persisted
        .load_observations()
        .unwrap()
        .into_iter()
        .find(|observation| {
            observation
                .source_system
                .as_ref()
                .is_some_and(|source| source.as_str() == "sys:codex")
        })
        .expect("Codex importer did not persist a sys:codex observation");
    let app = build_router(service);
    let decision_id = supplemental_id();
    let statement = "Track I Codex decision ledger entry";
    let (status, json) = post_supplemental(
        app.clone(),
        "integration-token",
        decision_supplemental_body(&decision_id, &observation, statement),
    );
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(json["data"]["id"], decision_id.as_str());

    let (status, decision_json) = get_json(
        app,
        "integration-token",
        "/projections/decisions?q=Track%20I%20Codex%20decision&limit=10",
    );
    assert_eq!(status, StatusCode::OK);
    let decision = decision_json["data"]["matches"]
        .as_array()
        .unwrap()
        .iter()
        .find(|decision| decision["id"] == decision_id.as_str())
        .unwrap_or_else(|| {
            panic!("decision POST did not appear in decision search: {decision_json}")
        });
    assert_eq!(decision["statement"], statement);
    assert_eq!(
        decision["derived_from"]["observations"][0],
        observation.id.as_str()
    );

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn claim_queue_api_filters_pages_and_searches_decisions() {
    let (root, db, blobs) = temp_paths();
    let persistence = SqlitePersistence::open(&db, &blobs, &[7; 32]).unwrap();
    let observations = vec![
        corpus_slack_observation("123_event", "claim source 1", "claim-1", false),
        corpus_slack_observation("123_event", "claim source 2", "claim-2", false),
        corpus_slack_observation("123_event", "claim source 3", "claim-3", false),
        corpus_slack_observation("123_event", "claim source 4", "claim-4", false),
        corpus_slack_observation("123_event", "claim source 5", "claim-5", false),
    ];
    for observation in &observations {
        persistence.persist_observation(observation).unwrap();
    }
    for supplemental in [
        claim_supplemental("sup:claim-open-1", &observations[0], "Open one", "check", 1),
        claim_supplemental(
            "sup:claim-open-2",
            &observations[1],
            "Open two",
            "generate",
            2,
        ),
        claim_supplemental(
            "sup:claim-open-3",
            &observations[2],
            "Open three",
            "check",
            3,
        ),
        claim_supplemental(
            "sup:claim-parked",
            &observations[3],
            "Parked one",
            "check",
            4,
        ),
        claim_supplemental(
            "sup:claim-verified",
            &observations[4],
            "Verified one",
            "check",
            5,
        ),
        claim_transition_supplemental("sup:park", "sup:claim-parked", "parked", 6),
        verification_result_supplemental("sup:verified", "sup:claim-verified", "consistent", 7),
        decision_supplemental(
            "sup:decision-a",
            &observations[0],
            "Use adapter A",
            "old rationale",
            vec![],
            8,
        ),
        decision_supplemental(
            "sup:decision-b",
            &observations[0],
            "Use adapter B",
            "replaces adapter A",
            vec!["sup:decision-a"],
            9,
        ),
    ] {
        persistence.persist_supplemental(&supplemental).unwrap();
    }

    let app = build_router(AppService::bootstrap(test_config(db, blobs)).unwrap());
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let first_page = runtime
        .block_on(async {
            app.clone()
                .oneshot(
                    Request::builder()
                        .uri("/projections/claim-queue?state=open&limit=2")
                        .header("authorization", "Bearer test-api-token")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
        })
        .unwrap();
    assert_eq!(first_page.status(), StatusCode::OK);
    let body = runtime
        .block_on(async { axum::body::to_bytes(first_page.into_body(), usize::MAX).await })
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        json["projection_metadata"]["projection_id"],
        "proj:claim-queue"
    );
    assert_eq!(json["data"]["total"], 3);
    assert_eq!(json["data"]["groups"].as_array().unwrap().len(), 2);
    assert_eq!(json["data"]["next_cursor"], "2");
    for group in json["data"]["groups"].as_array().unwrap() {
        for member in group["members"].as_array().unwrap() {
            assert_eq!(member["state"], "open");
        }
    }

    let second_page = runtime
        .block_on(async {
            app.clone()
                .oneshot(
                    Request::builder()
                        .uri("/projections/claim-queue?state=open&limit=2&cursor=2")
                        .header("authorization", "Bearer test-api-token")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
        })
        .unwrap();
    assert_eq!(second_page.status(), StatusCode::OK);
    let second_body = runtime
        .block_on(async { axum::body::to_bytes(second_page.into_body(), usize::MAX).await })
        .unwrap();
    let second_json: serde_json::Value = serde_json::from_slice(&second_body).unwrap();
    assert_eq!(second_json["data"]["groups"].as_array().unwrap().len(), 1);
    assert!(second_json["data"].get("next_cursor").is_none());

    let decision_response = runtime
        .block_on(async {
            app.oneshot(
                Request::builder()
                    .uri("/projections/decisions?q=adapter%20A&limit=10")
                    .header("authorization", "Bearer test-api-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
        })
        .unwrap();
    assert_eq!(decision_response.status(), StatusCode::OK);
    let decision_body = runtime
        .block_on(async { axum::body::to_bytes(decision_response.into_body(), usize::MAX).await })
        .unwrap();
    let decision_json: serde_json::Value = serde_json::from_slice(&decision_body).unwrap();
    let matches = decision_json["data"]["matches"].as_array().unwrap();
    let old_decision = matches
        .iter()
        .find(|decision| decision["id"] == "sup:decision-a")
        .unwrap_or_else(|| {
            panic!("decision response did not include superseded decision: {decision_json}")
        });
    assert_eq!(old_decision["superseded_by"], "sup:decision-b");

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn api_rejects_unauthenticated_projection_access() {
    let (root, db, blobs) = temp_paths();
    let app = build_router(AppService::bootstrap(test_config(db, blobs)).unwrap());

    let runtime = tokio::runtime::Runtime::new().unwrap();
    let response = runtime
        .block_on(async {
            app.oneshot(
                Request::builder()
                    .uri("/api/projections/proj:person-page/records")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
        })
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn timeline_endpoint_rejects_missing_timeline_scope() {
    let (root, db, blobs) = temp_paths();
    let mut config = test_config(db, blobs);
    config.api_tokens = vec![ApiTokenConfig {
        token: SecretString::new("person-only-token").unwrap(),
        scopes: vec!["read:persons".into()],
    }];
    let app = build_router(AppService::bootstrap(config).unwrap());

    let runtime = tokio::runtime::Runtime::new().unwrap();
    let response = runtime
        .block_on(async {
            app.oneshot(
                Request::builder()
                    .uri("/api/projections/proj:person-page/records/person:test/timeline")
                    .header("authorization", "Bearer person-only-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
        })
        .unwrap();

    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    let _ = std::fs::remove_dir_all(root);
}
