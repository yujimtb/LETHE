use std::path::PathBuf;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use lethe_core::domain::supplemental::InputAnchorSet;
use lethe_core::domain::{
    ActorRef, AuthorityModel, CaptureModel, EntityRef, IdempotencyKey, Mutability, Observation,
    ObserverRef, SchemaRef, SemVer, SourceSystemRef, SupplementalId, SupplementalRecord,
};
use lethe_runtime::runtime::partition::RoutingKeyOrder;
use lethe_selfhost::self_host::app::{AppService, ProjectionSnapshot};
use lethe_selfhost::self_host::config::{
    ApiTokenConfig, GoogleConfig, ResourceLimits, SecretString, SelfHostConfig, SlackConfig,
    SlideAiConfig,
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
    if db.exists() {
        let persistence = SqlitePersistence::open(&db, &blobs, &[7; 32]).unwrap();
        let observations = persistence.load_observations().unwrap();
        if !observations.is_empty() {
            let snapshot =
                ProjectionSnapshot::build(observations, persistence.load_supplementals().unwrap())
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
