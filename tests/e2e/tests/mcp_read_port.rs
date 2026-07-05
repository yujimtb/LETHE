use std::path::PathBuf;

use axum::body::Body;
use axum::http::header::{AUTHORIZATION, CONTENT_TYPE, WWW_AUTHENTICATE};
use axum::http::{Request, StatusCode};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use chrono::{Duration, Utc};
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
    McpOAuthConfig, ResourceLimits, SecretString, SelfHostConfig, SupplementalConfig,
};
use lethe_selfhost::self_host::mcp::build_mcp_router;
use lethe_selfhost::self_host::server::build_router;
use lethe_storage_sqlite::persistence::SqlitePersistence;
use ring::rand::SystemRandom;
use ring::signature::{ECDSA_P256_SHA256_FIXED_SIGNING, EcdsaKeyPair, KeyPair};
use tower::util::ServiceExt;

const TEST_ISSUER: &str = "https://issuer.example.test/";
const TEST_AUDIENCE: &str = "lethe-test";
const TEST_KID: &str = "test-key";

struct JwtSigner {
    key_pair: EcdsaKeyPair,
}

fn temp_paths() -> (PathBuf, PathBuf, PathBuf) {
    let root = std::env::temp_dir().join(format!("lethe-mcp-test-{}", uuid::Uuid::now_v7()));
    let db = root.join("lethe.sqlite3");
    let blobs = root.join("blobs");
    (root, db, blobs)
}

fn signer_and_oauth() -> (JwtSigner, McpOAuthConfig) {
    let rng = SystemRandom::new();
    let pkcs8 = EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, &rng).unwrap();
    let key_pair =
        EcdsaKeyPair::from_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, pkcs8.as_ref(), &rng).unwrap();
    let public_key = key_pair.public_key().as_ref();
    assert_eq!(public_key.len(), 65);
    assert_eq!(public_key[0], 0x04);
    let x = URL_SAFE_NO_PAD.encode(&public_key[1..33]);
    let y = URL_SAFE_NO_PAD.encode(&public_key[33..65]);
    (
        JwtSigner { key_pair },
        McpOAuthConfig {
            resource_url: "https://mcp.example.test/mcp".into(),
            protected_resource_metadata_url:
                "https://mcp.example.test/.well-known/oauth-protected-resource".into(),
            issuer: TEST_ISSUER.into(),
            audience: TEST_AUDIENCE.into(),
            jwks_path: PathBuf::from("test-jwks.json"),
            jwks: JsonWebKeySet {
                keys: vec![JsonWebKey {
                    kty: "EC".into(),
                    kid: TEST_KID.into(),
                    alg: Some("ES256".into()),
                    crv: Some("P-256".into()),
                    x: Some(x),
                    y: Some(y),
                    n: None,
                    e: None,
                }],
            },
        },
    )
}

fn sign_jwt(signer: &JwtSigner, audience: &str, exp: i64) -> String {
    let header = serde_json::json!({
        "alg": "ES256",
        "kid": TEST_KID,
        "typ": "JWT"
    });
    let claims = serde_json::json!({
        "iss": TEST_ISSUER,
        "sub": "user:test",
        "aud": audience,
        "exp": exp,
        "iat": Utc::now().timestamp()
    });
    let signing_input = format!("{}.{}", encode_json(&header), encode_json(&claims));
    let rng = SystemRandom::new();
    let signature = signer
        .key_pair
        .sign(&rng, signing_input.as_bytes())
        .unwrap();
    format!(
        "{}.{}",
        signing_input,
        URL_SAFE_NO_PAD.encode(signature.as_ref())
    )
}

fn encode_json(value: &serde_json::Value) -> String {
    URL_SAFE_NO_PAD.encode(serde_json::to_vec(value).unwrap())
}

fn observation(text: &str, key: &str) -> Observation {
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
            "user_id": "U123",
            "user_name": "Ada",
            "text": text,
            "channel": "123_event",
            "channel_id": "C-123_event",
            "channel_name": "123_event",
            "is_public_channel": true,
            "is_bot": false,
            "ts": format!("{}.000000", key.len() + 1),
            "thread_ts": "1.000000",
            "permalink": format!("https://slack.example/123_event/{key}"),
        }),
        attachments: vec![],
        published: Utc::now(),
        recorded_at: Utc::now(),
        consent: None,
        idempotency_key: IdempotencyKey::new(format!("mcp:{key}")),
        meta: serde_json::json!({
            "canonical_json": serde_json::json!({"source": "slack", "object_id": key, "body": text}).to_string(),
            "source_container": "slack-test:123_event",
        }),
    }
}

fn test_config(db: PathBuf, blobs: PathBuf, oauth: McpOAuthConfig) -> SelfHostConfig {
    if db.exists() {
        let persistence = SqlitePersistence::open(&db, &blobs, &[7; 32]).unwrap();
        let observations = persistence.load_observations().unwrap();
        if !observations.is_empty() {
            let snapshot = ProjectionSnapshot::build(
                observations,
                persistence.load_supplementals().unwrap(),
                CorpusConfig {
                    mode: CorpusMode::WorkspaceFiltered,
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
        bind_addr: "127.0.0.1:8080".into(),
        mcp_bind_addr: "127.0.0.1:8090".into(),
        mcp_oauth: oauth,
        database_path: db,
        blob_dir: blobs,
        secret_encryption_key: [7; 32],
        poll_interval: std::time::Duration::from_secs(300),
        routing_key_order: RoutingKeyOrder::MonthYearSourceContainerPublished,
        api_tokens: vec![ApiTokenConfig {
            token: SecretString::new("internal-api-token").unwrap(),
            scopes: vec!["read:corpus".into()],
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
            mode: CorpusMode::WorkspaceFiltered,
        },
        slack_sources: vec![],
        google_sources: Vec::<GoogleConfig>::new(),
        slide_analysis_limit: None,
        slide_ai: None,
        supplemental: SupplementalConfig {
            reject_unregistered_kinds: true,
        },
    }
}

fn service_with_records(oauth: McpOAuthConfig) -> (PathBuf, AppService) {
    let (root, db, blobs) = temp_paths();
    let persistence = SqlitePersistence::open(&db, &blobs, &[7; 32]).unwrap();
    let first = observation("部屋１２３の忘れ物", "a");
    let first_id = first.id.clone();
    persistence.persist_observation(&first).unwrap();
    persistence
        .persist_observation(&observation("追加の文脈", "b"))
        .unwrap();
    persistence
        .persist_supplemental(&claim_supplemental(&first_id))
        .unwrap();
    persistence
        .persist_supplemental(&decision_supplemental(&first_id))
        .unwrap();
    let service = AppService::bootstrap(test_config(db, blobs, oauth)).unwrap();
    (root, service)
}

fn claim_supplemental(observation_id: &lethe_core::domain::ObservationId) -> SupplementalRecord {
    SupplementalRecord {
        id: SupplementalId::new("sup:mcp-claim"),
        kind: "claim@1".into(),
        derived_from: InputAnchorSet {
            observations: vec![observation_id.clone()],
            blobs: vec![],
            supplementals: vec![],
        },
        payload: serde_json::json!({
            "statement": "MCP claim fixture",
            "verification_mode": "generate"
        }),
        created_by: ActorRef::new("actor:test"),
        created_at: Utc::now(),
        mutability: Mutability::AppendOnly,
        record_version: None,
        model_version: Some("fixture".into()),
        consent_metadata: None,
        lineage: None,
    }
}

fn decision_supplemental(observation_id: &lethe_core::domain::ObservationId) -> SupplementalRecord {
    SupplementalRecord {
        id: SupplementalId::new("sup:mcp-decision"),
        kind: "decision@1".into(),
        derived_from: InputAnchorSet {
            observations: vec![observation_id.clone()],
            blobs: vec![],
            supplementals: vec![],
        },
        payload: serde_json::json!({
            "statement": "adapter A 方針",
            "rationale": "MCP decision search fixture"
        }),
        created_by: ActorRef::new("actor:test"),
        created_at: Utc::now(),
        mutability: Mutability::AppendOnly,
        record_version: None,
        model_version: Some("fixture".into()),
        consent_metadata: None,
        lineage: None,
    }
}

fn valid_token(signer: &JwtSigner) -> String {
    sign_jwt(
        signer,
        TEST_AUDIENCE,
        (Utc::now() + Duration::hours(1)).timestamp(),
    )
}

fn mcp_request(token: &str, body: serde_json::Value) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri("/mcp")
        .header(AUTHORIZATION, format!("Bearer {token}"))
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(body.to_string()))
        .unwrap()
}

fn jsonrpc(method: &str, params: serde_json::Value) -> serde_json::Value {
    serde_json::json!({
        "jsonrpc": "2.0",
        "id": "test-id",
        "method": method,
        "params": params
    })
}

fn tool_call(name: &str, arguments: serde_json::Value) -> serde_json::Value {
    jsonrpc(
        "tools/call",
        serde_json::json!({
            "name": name,
            "arguments": arguments
        }),
    )
}

fn response_json(
    runtime: &tokio::runtime::Runtime,
    response: axum::response::Response,
) -> serde_json::Value {
    let body = runtime
        .block_on(async { axum::body::to_bytes(response.into_body(), usize::MAX).await })
        .unwrap();
    serde_json::from_slice(&body).unwrap()
}

#[test]
fn protected_resource_metadata_contract_is_public() {
    let (_signer, oauth) = signer_and_oauth();
    let (_root, service) = service_with_records(oauth);
    let app = build_mcp_router(service);
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let response = runtime
        .block_on(async {
            app.oneshot(
                Request::builder()
                    .uri("/.well-known/oauth-protected-resource")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
        })
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let json = response_json(&runtime, response);
    assert_eq!(json["resource"], "https://mcp.example.test/mcp");
    assert_eq!(json["authorization_servers"][0], TEST_ISSUER);
    assert_eq!(json["issuer"], TEST_ISSUER);
    assert_eq!(json["bearer_methods_supported"][0], "header");
}

#[test]
fn mcp_jwt_validation_rejects_expired_and_wrong_audience_and_accepts_valid() {
    let (signer, oauth) = signer_and_oauth();
    let (_root, service) = service_with_records(oauth);
    let app = build_mcp_router(service);
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let initialize = jsonrpc("initialize", serde_json::json!({}));

    let expired = sign_jwt(
        &signer,
        TEST_AUDIENCE,
        (Utc::now() - Duration::minutes(1)).timestamp(),
    );
    let expired_response = runtime
        .block_on(async {
            app.clone()
                .oneshot(mcp_request(&expired, initialize.clone()))
                .await
        })
        .unwrap();
    assert_eq!(expired_response.status(), StatusCode::UNAUTHORIZED);
    assert!(
        expired_response
            .headers()
            .get(WWW_AUTHENTICATE)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.contains("resource_metadata="))
    );

    let wrong_audience = sign_jwt(
        &signer,
        "other-audience",
        (Utc::now() + Duration::hours(1)).timestamp(),
    );
    let wrong_audience_response = runtime
        .block_on(async {
            app.clone()
                .oneshot(mcp_request(&wrong_audience, initialize.clone()))
                .await
        })
        .unwrap();
    assert_eq!(wrong_audience_response.status(), StatusCode::UNAUTHORIZED);

    let ok_response = runtime
        .block_on(async {
            app.oneshot(mcp_request(&valid_token(&signer), initialize))
                .await
        })
        .unwrap();
    assert_eq!(ok_response.status(), StatusCode::OK);
    let json = response_json(&runtime, ok_response);
    assert_eq!(json["result"]["serverInfo"]["name"], "lethe-mcp-read-port");
}

#[test]
fn mcp_and_internal_api_routes_are_separate() {
    let (signer, oauth) = signer_and_oauth();
    let (_root, service) = service_with_records(oauth);
    let internal = build_router(service.clone());
    let mcp = build_mcp_router(service);
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let token = valid_token(&signer);

    let internal_mcp_response = runtime
        .block_on(async {
            internal
                .oneshot(mcp_request(
                    &token,
                    jsonrpc("initialize", serde_json::json!({})),
                ))
                .await
        })
        .unwrap();
    assert_eq!(internal_mcp_response.status(), StatusCode::NOT_FOUND);

    let mcp_admin_response = runtime
        .block_on(async {
            mcp.oneshot(
                Request::builder()
                    .uri("/health/deep")
                    .header(AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
        })
        .unwrap();
    assert_eq!(mcp_admin_response.status(), StatusCode::NOT_FOUND);
}

#[test]
fn five_mcp_tools_have_contracts_and_read_via_projection() {
    let (signer, oauth) = signer_and_oauth();
    let (_root, service) = service_with_records(oauth);
    let app = build_mcp_router(service);
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let token = valid_token(&signer);

    let tools_response = runtime
        .block_on(async {
            app.clone()
                .oneshot(mcp_request(
                    &token,
                    jsonrpc("tools/list", serde_json::json!({})),
                ))
                .await
        })
        .unwrap();
    assert_eq!(tools_response.status(), StatusCode::OK);
    let tools_json = response_json(&runtime, tools_response);
    let tools = tools_json["result"]["tools"].as_array().unwrap();
    let names = tools
        .iter()
        .map(|tool| tool["name"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        names,
        vec![
            "search_lake",
            "get_record",
            "get_thread",
            "claim_queue",
            "search_decisions"
        ]
    );
    for tool in tools {
        let description = tool["description"].as_str().unwrap();
        assert!(!description.trim().is_empty());
        assert!(description.len() <= 100);
        assert!(!description.to_ascii_lowercase().contains("write"));
    }

    let search_response = runtime
        .block_on(async {
            app.clone()
                .oneshot(mcp_request(
                    &token,
                    tool_call("search_lake", serde_json::json!({"query": "忘れ物"})),
                ))
                .await
        })
        .unwrap();
    assert_eq!(search_response.status(), StatusCode::OK);
    let search_json = response_json(&runtime, search_response);
    let matches = search_json["result"]["structuredContent"]["data"]["matches"]
        .as_array()
        .unwrap();
    assert_eq!(matches.len(), 1);
    let record_id = matches[0]["record_id"].as_str().unwrap();

    let record_response = runtime
        .block_on(async {
            app.clone()
                .oneshot(mcp_request(
                    &token,
                    tool_call("get_record", serde_json::json!({ "record_id": record_id })),
                ))
                .await
        })
        .unwrap();
    assert_eq!(record_response.status(), StatusCode::OK);
    let record_json = response_json(&runtime, record_response);
    assert_eq!(
        record_json["result"]["structuredContent"]["data"]["record"]["record_id"],
        record_id
    );

    let thread_response = runtime
        .block_on(async {
            app.clone()
                .oneshot(mcp_request(
                    &token,
                    tool_call("get_thread", serde_json::json!({ "record_id": record_id })),
                ))
                .await
        })
        .unwrap();
    assert_eq!(thread_response.status(), StatusCode::OK);
    let thread_json = response_json(&runtime, thread_response);
    assert_eq!(
        thread_json["result"]["structuredContent"]["data"]["records"]
            .as_array()
            .unwrap()
            .len(),
        2
    );

    let claim_response = runtime
        .block_on(async {
            app.clone()
                .oneshot(mcp_request(
                    &token,
                    tool_call(
                        "claim_queue",
                        serde_json::json!({
                            "state": "open",
                            "verification_mode": "generate",
                            "limit": 10
                        }),
                    ),
                ))
                .await
        })
        .unwrap();
    assert_eq!(claim_response.status(), StatusCode::OK);
    let claim_json = response_json(&runtime, claim_response);
    assert_eq!(
        claim_json["result"]["structuredContent"]["projection_metadata"]["projection_id"],
        "proj:claim-queue"
    );
    assert_eq!(
        claim_json["result"]["structuredContent"]["data"]["groups"]
            .as_array()
            .unwrap()
            .len(),
        1
    );

    let decisions_response = runtime
        .block_on(async {
            app.clone()
                .oneshot(mcp_request(
                    &token,
                    tool_call("search_decisions", serde_json::json!({"query": "方針"})),
                ))
                .await
        })
        .unwrap();
    assert_eq!(decisions_response.status(), StatusCode::OK);
    let decisions_json = response_json(&runtime, decisions_response);
    assert_eq!(
        decisions_json["result"]["structuredContent"]["data"]["matches"]
            .as_array()
            .unwrap()
            .len(),
        1
    );

    let missing_response = runtime
        .block_on(async {
            app.oneshot(mcp_request(
                &token,
                tool_call(
                    "get_record",
                    serde_json::json!({ "record_id": "corpus:missing" }),
                ),
            ))
            .await
        })
        .unwrap();
    assert_eq!(missing_response.status(), StatusCode::OK);
    let missing_json = response_json(&runtime, missing_response);
    assert!(
        missing_json["error"]["message"]
            .as_str()
            .is_some_and(|message| message.contains("RecordNotFound"))
    );
}
