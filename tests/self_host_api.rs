use std::path::PathBuf;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use lethe::domain::{
    AuthorityModel, CaptureModel, EntityRef, IdempotencyKey, Observation, ObserverRef, SchemaRef,
    SemVer, SourceSystemRef,
};
use lethe::self_host::app::AppService;
use lethe::self_host::config::{
    ApiTokenConfig, GoogleConfig, ResourceLimits, SecretString, SelfHostConfig, SlackConfig,
};
use lethe::self_host::persistence::SqlitePersistence;
use lethe::self_host::server::build_router;
use tower::util::ServiceExt;

fn temp_paths() -> (PathBuf, PathBuf, PathBuf) {
    let root = std::env::temp_dir().join(format!("lethe-self-host-test-{}", uuid::Uuid::now_v7()));
    let db = root.join("lethe.sqlite3");
    let blobs = root.join("blobs");
    (root, db, blobs)
}

fn slack_observation(user_id: &str, email: &str, name: &str, text: &str, channel: &str, key: &str) -> Observation {
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
        idempotency_key: Some(IdempotencyKey::new(key)),
        meta: serde_json::json!({}),
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
        idempotency_key: Some(IdempotencyKey::new(format!("gslides-{key}"))),
        meta: serde_json::json!({}),
    }
}

fn test_config(db: PathBuf, blobs: PathBuf) -> SelfHostConfig {
    SelfHostConfig {
        bind_addr: "127.0.0.1:0".into(),
        public_base_url: None,
        database_path: db,
        blob_dir: blobs,
        poll_interval: std::time::Duration::from_secs(300),
        api_tokens: vec![ApiTokenConfig {
            token: SecretString::new("test-api-token").unwrap(),
            scopes: vec!["projection:read".into(), "blob:read".into()],
        }],
        resource_limits: ResourceLimits {
            max_blob_bytes: 10 * 1024 * 1024,
            max_payload_bytes: 1024 * 1024,
            max_sync_items: 10_000,
            max_page_size: 100,
        },
        sources: vec![],
        slack: SlackConfig {
            bot_token: "xoxb-test-token".into(),
            thread_token: None,
            channel_ids: vec!["C01ABC".into()],
        },
        google: GoogleConfig {
            access_token: Some("ya29.test-token".into()),
            client_id: None,
            client_secret: None,
            refresh_token: None,
            presentation_ids: vec!["pres123".into()],
        },
        slide_analysis_limit: 10,
        slide_ai: None,
        notion: None,
    }
}

#[test]
fn self_host_persons_endpoint_returns_projection_data() {
    let (root, db, blobs) = temp_paths();
    let persistence = SqlitePersistence::open(&db, &blobs).unwrap();
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
            app.oneshot(
                Request::builder()
                    .uri("/api/persons")
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

    assert_eq!(json["projection_metadata"]["projection_id"], "proj:person-page");
    assert_eq!(json["data"]["total"], 1);
    assert_eq!(json["data"]["data"][0]["display_name"], "田中太郎");

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn public_blob_endpoint_serves_persisted_blob_bytes() {
    let (root, db, blobs) = temp_paths();
    let persistence = SqlitePersistence::open(&db, &blobs).unwrap();
    let blob_ref = persistence.persist_blob(b"png-bytes").unwrap();
    let blob_hash = blob_ref
        .as_str()
        .strip_prefix("blob:sha256:")
        .unwrap()
        .to_string();

    let app = build_router(AppService::bootstrap(test_config(db, blobs)).unwrap());

    let runtime = tokio::runtime::Runtime::new().unwrap();
    let response = runtime
        .block_on(async {
            app.oneshot(
                Request::builder()
                    .uri(format!("/public/blobs/{blob_hash}"))
                    .header("authorization", "Bearer test-api-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
        })
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("content-type").and_then(|value| value.to_str().ok()),
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
    let persistence = SqlitePersistence::open(&db, &blobs).unwrap();
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
                        .uri("/api/persons")
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
                    .uri(format!("/api/persons/{person_id}"))
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
    assert_eq!(detail_json["data"]["related_slides"][0]["title"], "田中の自己紹介");

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
