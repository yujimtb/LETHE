use std::path::PathBuf;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use lethe_core::domain::supplemental::InputAnchorSet;
use lethe_core::domain::{
    ActorRef, AuthorityModel, CaptureModel, EntityRef, IdempotencyKey, Mutability, Observation,
    ObserverRef, SchemaRef, SemVer, SourceSystemRef, SupplementalId, SupplementalRecord,
};
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
        api_tokens: vec![ApiTokenConfig {
            token: SecretString::new("test-api-token").unwrap(),
            scopes: vec!["read:persons".into(), "read:timeline".into()],
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
        slide_analysis_limit: 10,
        slide_ai: SlideAiConfig {
            api_key: SecretString::new("test-gemini-key").unwrap(),
            model: "test-gemini-model".into(),
        },
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
