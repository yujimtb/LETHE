use std::path::PathBuf;

use axum::body::Body;
use axum::http::header::{AUTHORIZATION, CONTENT_TYPE, WWW_AUTHENTICATE};
use axum::http::{Request, StatusCode};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use chrono::{DateTime, Duration, Utc};
use lethe_adapter_api::traits::ObservationDraft;
use lethe_adapter_chatgpt::{ChatGptImportBatch, ChatGptImportFilter, ChatGptImporter};
use lethe_core::domain::supplemental::InputAnchorSet;
use lethe_core::domain::{
    ActorRef, AuthorityModel, CaptureModel, EntityRef, IdempotencyKey, Mutability, Observation,
    ObserverRef, SchemaRef, SemVer, SourceSystemRef, SupplementalId, SupplementalRecord,
};
use lethe_projection_corpus::CorpusMode;
use lethe_runtime::runtime::partition::RoutingKeyOrder;
use lethe_selfhost::self_host::app::AppService;
use lethe_selfhost::self_host::config::{
    ApiTokenConfig, CorpusProjectionConfig, FreshnessConfig, GoogleConfig, JsonWebKey,
    JsonWebKeySet, McpOAuthConfig, OpsConfig, ResourceLimits, SecretString, SelfHostConfig,
    SupplementalConfig,
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

fn sign_jwt(signer: &JwtSigner, audience: &str, exp: i64, scope: &str) -> String {
    let claims = serde_json::json!({
        "iss": TEST_ISSUER,
        "sub": "user:test",
        "aud": audience,
        "exp": exp,
        "iat": Utc::now().timestamp(),
        "scope": scope
    });
    sign_jwt_claims(signer, claims)
}

fn sign_jwt_claims(signer: &JwtSigner, claims: serde_json::Value) -> String {
    let header = serde_json::json!({
        "alg": "ES256",
        "kid": TEST_KID,
        "typ": "JWT"
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
    observation_at(text, key, Utc::now())
}

fn observation_at(text: &str, key: &str, published: DateTime<Utc>) -> Observation {
    let source_sequence = key.as_bytes().iter().fold(1_u64, |accumulator, byte| {
        accumulator
            .checked_mul(257)
            .and_then(|value| value.checked_add(u64::from(*byte)))
            .expect("test Slack timestamp must fit u64")
    });
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
            "ts": format!("{source_sequence}.000000"),
            "thread_ts": "1.000000",
            "permalink": format!("https://slack.example/123_event/{key}"),
        }),
        attachments: vec![],
        published,
        recorded_at: Utc::now(),
        consent: None,
        idempotency_key: IdempotencyKey::new(format!("mcp:{key}")),
        meta: serde_json::json!({
            "canonical_json": serde_json::json!({"source": "slack", "object_id": key, "body": text}).to_string(),
            "source_container": "slack-test:123_event",
        }),
    }
}

fn chatgpt_fixture_drafts() -> Vec<ObservationDraft> {
    let fixture = serde_json::json!([{
        "id": "chatgpt-e2e-conversation",
        "title": "MCP write fixture",
        "mapping": {
            "msg-user": {
                "id": "msg-user",
                "parent": null,
                "message": {
                    "author": { "role": "user" },
                    "content": { "content_type": "text", "parts": ["remember this decision context"] },
                    "create_time": 1780000200.0
                }
            }
        }
    }])
    .to_string();
    let importer = ChatGptImporter::new(SemVer::new("1.0.0"));
    let mut batch = ChatGptImportBatch::default();
    importer
        .import_json_str(
            &fixture,
            "chatgpt/conversations.json",
            &ChatGptImportFilter::default(),
            &mut batch,
        )
        .unwrap();
    assert!(batch.audit.skipped_records.is_empty());
    assert_eq!(batch.drafts.len(), 1);
    batch.drafts
}

fn test_config(db: PathBuf, blobs: PathBuf, oauth: McpOAuthConfig) -> SelfHostConfig {
    SelfHostConfig {
        bind_addr: "127.0.0.1:8080".into(),
        mcp_bind_addr: "127.0.0.1:8090".into(),
        mcp_oauth: oauth,
        database_path: db.clone(),
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
            index_dir: db.with_extension("corpus-index"),
            writer_heap_bytes: 32 * 1024 * 1024,
            rebuild_page_size: 512,
        },
        freshness: FreshnessConfig {
            threshold_seconds: std::collections::BTreeMap::from([(
                "sys:slack".to_owned(),
                36 * 3600,
            )]),
        },
        ops: OpsConfig {
            backfill_nightly_budget_items: 1000,
        },
        channels: vec![],
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
    let (root, service, _) = service_with_records_and_first_id(oauth);
    (root, service)
}

fn wait_for_search_index_ready(service: &AppService) {
    wait_for_search_index_status(service, "ok");
}

fn wait_for_search_index_status(service: &AppService, expected: &str) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let health = service.health().unwrap();
        if health.dependencies.iter().any(|dependency| {
            dependency.name == "corpus_search_index" && dependency.status == expected
        }) {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "search index did not reach {expected}: {:?}",
            health.dependencies
        );
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
}

#[test]
fn failed_search_index_returns_explicit_mcp_internal_error() {
    let (signer, oauth) = signer_and_oauth();
    let (root, db, blobs) = temp_paths();
    std::fs::create_dir_all(&root).unwrap();
    let unusable_index_path = root.join("corpus-index-is-a-file");
    std::fs::write(&unusable_index_path, b"not a directory").unwrap();
    let mut config = test_config(db, blobs, oauth);
    config.corpus.index_dir = unusable_index_path;
    let service = AppService::bootstrap(config).unwrap();
    wait_for_search_index_status(&service, "failed");
    let app = build_mcp_router(service);
    let runtime = tokio::runtime::Runtime::new().unwrap();

    let response = runtime
        .block_on(async {
            app.oneshot(mcp_request(
                &valid_token(&signer),
                tool_call("search_lake", serde_json::json!({"query": "needle"})),
            ))
            .await
        })
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let json = response_json(&runtime, response);
    assert_eq!(json["error"]["code"], -32000);
    assert!(
        json["error"]["message"]
            .as_str()
            .is_some_and(|message| message.contains("search_index_failed"))
    );

    let _ = std::fs::remove_dir_all(root);
}

fn bootstrap_ready(config: SelfHostConfig) -> AppService {
    let service = AppService::bootstrap(config).unwrap();
    wait_for_search_index_ready(&service);
    service
}

fn service_with_matching_records(oauth: McpOAuthConfig, count: usize) -> (PathBuf, AppService) {
    let (root, db, blobs) = temp_paths();
    let persistence = SqlitePersistence::open(&db, &blobs, &[7; 32]).unwrap();
    for index in 0..count {
        persistence
            .persist_observation(&observation(
                &format!("needle record {index}"),
                &format!("bulk-{index}"),
            ))
            .unwrap();
    }
    let service = bootstrap_ready(test_config(db, blobs, oauth));
    (root, service)
}

fn service_with_observations(
    oauth: McpOAuthConfig,
    observations: Vec<Observation>,
) -> (PathBuf, AppService) {
    let (root, db, blobs) = temp_paths();
    let persistence = SqlitePersistence::open(&db, &blobs, &[7; 32]).unwrap();
    for observation in observations {
        persistence.persist_observation(&observation).unwrap();
    }
    let service = bootstrap_ready(test_config(db, blobs, oauth));
    (root, service)
}

fn service_with_records_and_first_id(
    oauth: McpOAuthConfig,
) -> (PathBuf, AppService, lethe_core::domain::ObservationId) {
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
    let service = bootstrap_ready(test_config(db, blobs, oauth));
    (root, service, first_id)
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
    scoped_token(signer, "mcp:read write:supplemental")
}

fn read_only_token(signer: &JwtSigner) -> String {
    scoped_token(signer, "mcp:read")
}

fn scoped_token(signer: &JwtSigner, scope: &str) -> String {
    sign_jwt(
        signer,
        TEST_AUDIENCE,
        (Utc::now() + Duration::hours(1)).timestamp(),
        scope,
    )
}

fn permissions_token(signer: &JwtSigner, permissions: Vec<&str>) -> String {
    sign_jwt_claims(
        signer,
        serde_json::json!({
            "iss": TEST_ISSUER,
            "sub": "user:test",
            "aud": TEST_AUDIENCE,
            "exp": (Utc::now() + Duration::hours(1)).timestamp(),
            "iat": Utc::now().timestamp(),
            "permissions": permissions
        }),
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
    for uri in [
        "/.well-known/oauth-protected-resource",
        "/.well-known/oauth-protected-resource/mcp",
    ] {
        let response = runtime
            .block_on(async {
                app.clone()
                    .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
                    .await
            })
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let json = response_json(&runtime, response);
        assert_eq!(json["resource"], "https://mcp.example.test/mcp");
        assert_eq!(json["authorization_servers"][0], TEST_ISSUER);
        assert_eq!(json["issuer"], TEST_ISSUER);
        assert_eq!(json["bearer_methods_supported"][0], "header");
        assert_eq!(json["scopes_supported"][0], "mcp:read");
        assert_eq!(json["scopes_supported"][1], "write:supplemental");
    }
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
        "mcp:read",
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
    assert!(
        expired_response
            .headers()
            .get(WWW_AUTHENTICATE)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.contains("scope=\"mcp:read write:supplemental\""))
    );

    let wrong_audience = sign_jwt(
        &signer,
        "other-audience",
        (Utc::now() + Duration::hours(1)).timestamp(),
        "mcp:read",
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
fn mcp_jwt_validation_accepts_auth0_permissions_claim_for_refreshed_tokens() {
    let (signer, oauth) = signer_and_oauth();
    let (_root, service) = service_with_records(oauth);
    let app = build_mcp_router(service);
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let token = permissions_token(&signer, vec!["mcp:read"]);

    let response = runtime
        .block_on(async {
            app.oneshot(mcp_request(
                &token,
                tool_call("search_lake", serde_json::json!({"query": "忘れ物"})),
            ))
            .await
        })
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let json = response_json(&runtime, response);
    assert!(
        json.get("error").is_none(),
        "unexpected MCP error: {json:#}"
    );
    let matches = json["result"]["structuredContent"]["data"]["matches"]
        .as_array()
        .unwrap();
    assert_eq!(matches.len(), 1);
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
fn six_mcp_tools_have_contracts_and_read_via_projection() {
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
            "search_decisions",
            "write_supplemental"
        ]
    );
    for tool in tools {
        let description = tool["description"].as_str().unwrap();
        assert!(!description.trim().is_empty());
        if tool["name"] == "write_supplemental" {
            assert!(description.contains("post-processing"));
            assert_eq!(tool["annotations"]["readOnlyHint"], false);
            assert_eq!(
                tool["securitySchemes"][0]["scopes"][0],
                "write:supplemental"
            );
        } else {
            match tool["name"].as_str().unwrap() {
                "search_lake" => {
                    assert!(description.contains("AND"));
                    assert!(description.contains("fullwidth-space"));
                    assert!(description.contains("regex"));
                    assert!(description.contains("capped at 20"));
                    assert!(description.contains("240 chars"));
                    assert!(description.contains("matched_ranges"));
                    assert!(description.contains("UTF-8 byte offsets"));
                    assert!(description.contains("Valid source_types"));
                    assert!(description.contains("slack"));
                    assert_eq!(
                        tool["inputSchema"]["properties"]["order"]["enum"][0],
                        "newest_first"
                    );
                    assert_eq!(
                        tool["inputSchema"]["properties"]["order"]["enum"][1],
                        "oldest_first"
                    );
                    assert_eq!(
                        tool["inputSchema"]["properties"]["from"]["format"],
                        "date-time"
                    );
                    assert_eq!(
                        tool["inputSchema"]["properties"]["to"]["format"],
                        "date-time"
                    );
                    assert_eq!(tool["inputSchema"]["properties"]["limit"]["maximum"], 20);
                }
                "search_decisions" => {
                    assert!(description.contains("AND"));
                    assert!(description.contains("partial match"));
                    assert!(description.contains("fullwidth-space"));
                    assert!(description.contains("capped at 20"));
                    assert_eq!(tool["inputSchema"]["properties"]["limit"]["maximum"], 20);
                }
                "claim_queue" => {
                    assert!(description.contains("capped at 20"));
                    assert_eq!(tool["inputSchema"]["properties"]["limit"]["maximum"], 20);
                }
                "get_thread" => {
                    assert!(description.contains("cursor"));
                    assert_eq!(tool["inputSchema"]["properties"]["limit"]["maximum"], 20);
                }
                _ => assert!(description.len() <= 100),
            }
            assert!(!description.to_ascii_lowercase().contains("write"));
            assert_eq!(tool["annotations"]["readOnlyHint"], true);
            assert_eq!(tool["securitySchemes"][0]["scopes"][0], "mcp:read");
        }
        assert_eq!(tool["securitySchemes"][0]["type"], "oauth2");
        assert_eq!(tool["_meta"]["securitySchemes"], tool["securitySchemes"]);
        assert_eq!(tool["annotations"]["destructiveHint"], false);
        assert_eq!(tool["annotations"]["openWorldHint"], false);
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
    assert_eq!(matches[0]["thread_key"], "1.000000");
    assert!(matches[0]["metadata"]["observation_id"].is_null());
    assert!(matches[0]["metadata"]["channel_id"].is_string());
    assert!(
        search_json["result"]["_meta"]["lethe/available_source_types"]
            .as_array()
            .unwrap()
            .iter()
            .any(|source| source["source_type"] == "slack" && source["records"] == 2)
    );
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
    assert_eq!(
        thread_json["result"]["structuredContent"]["data"]["complete"],
        true
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

#[test]
fn search_lake_clamps_mcp_limit_and_reports_effective_limit() {
    let (signer, oauth) = signer_and_oauth();
    let (_root, service) = service_with_matching_records(oauth, 25);
    let app = build_mcp_router(service);
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let token = valid_token(&signer);

    let response = runtime
        .block_on(async {
            app.oneshot(mcp_request(
                &token,
                tool_call(
                    "search_lake",
                    serde_json::json!({"query": "needle", "limit": 50}),
                ),
            ))
            .await
        })
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let json = response_json(&runtime, response);
    let matches = json["result"]["structuredContent"]["data"]["matches"]
        .as_array()
        .unwrap();
    assert_eq!(matches.len(), 20);
    assert!(json["result"]["structuredContent"]["data"]["next_cursor"].is_string());
    let limit = &json["result"]["_meta"]["lethe/response_limit"];
    assert_eq!(limit["requested_limit"], 50);
    assert_eq!(limit["effective_limit"], 20);
    assert_eq!(limit["max_limit"], 20);
    assert_eq!(limit["limit_clamped"], true);
}

#[test]
fn search_lake_accepts_time_filters_and_order() {
    let (signer, oauth) = signer_and_oauth();
    let observations = vec![
        observation_at(
            "needle before range",
            "old",
            DateTime::parse_from_rfc3339("2026-06-30T00:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
        ),
        observation_at(
            "needle first in range",
            "first",
            DateTime::parse_from_rfc3339("2026-07-02T00:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
        ),
        observation_at(
            "needle second in range",
            "second",
            DateTime::parse_from_rfc3339("2026-07-03T00:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
        ),
    ];
    let (_root, service) = service_with_observations(oauth, observations);
    let app = build_mcp_router(service);
    let runtime = tokio::runtime::Runtime::new().unwrap();

    let response = runtime
        .block_on(async {
            app.oneshot(mcp_request(
                &valid_token(&signer),
                tool_call(
                    "search_lake",
                    serde_json::json!({
                        "query": "needle",
                        "from": "2026-07-01T00:00:00Z",
                        "to": "2026-07-31T23:59:59Z",
                        "order": "oldest_first",
                        "limit": 5
                    }),
                ),
            ))
            .await
        })
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let json = response_json(&runtime, response);
    let matches = json["result"]["structuredContent"]["data"]["matches"]
        .as_array()
        .unwrap();
    let snippets = matches
        .iter()
        .map(|matched| matched["snippet"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        snippets,
        vec!["needle first in range", "needle second in range"]
    );
}

#[test]
fn search_lake_rejects_unknown_source_type_with_valid_values() {
    let (signer, oauth) = signer_and_oauth();
    let (_root, service) = service_with_records(oauth);
    let app = build_mcp_router(service);
    let runtime = tokio::runtime::Runtime::new().unwrap();

    let response = runtime
        .block_on(async {
            app.oneshot(mcp_request(
                &valid_token(&signer),
                tool_call(
                    "search_lake",
                    serde_json::json!({"query": "忘れ物", "source_types": ["gpt"]}),
                ),
            ))
            .await
        })
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let json = response_json(&runtime, response);
    assert_eq!(json["error"]["code"], -32602);
    let message = json["error"]["message"].as_str().unwrap();
    assert!(message.contains("unknown source_type"));
    assert!(message.contains("'gpt'"));
    assert!(message.contains("chatgpt"));
    assert!(message.contains("slack"));
}

#[test]
fn search_lake_accepts_known_source_type_absent_from_current_corpus() {
    let (signer, oauth) = signer_and_oauth();
    let (_root, service) = service_with_records(oauth);
    let app = build_mcp_router(service);
    let runtime = tokio::runtime::Runtime::new().unwrap();

    let response = runtime
        .block_on(async {
            app.oneshot(mcp_request(
                &valid_token(&signer),
                tool_call(
                    "search_lake",
                    serde_json::json!({"query": "忘れ物", "source_types": ["codex"]}),
                ),
            ))
            .await
        })
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let json = response_json(&runtime, response);
    assert_eq!(
        json["result"]["structuredContent"]["data"]["matches"]
            .as_array()
            .unwrap()
            .len(),
        0
    );
}

#[test]
fn get_thread_defaults_to_safe_page_and_uses_cursor() {
    let (signer, oauth) = signer_and_oauth();
    let (_root, service) = service_with_matching_records(oauth, 25);
    let app = build_mcp_router(service);
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let token = valid_token(&signer);

    let first_response = runtime
        .block_on(async {
            app.clone()
                .oneshot(mcp_request(
                    &token,
                    tool_call("get_thread", serde_json::json!({"record_id": "1.000000"})),
                ))
                .await
        })
        .unwrap();

    assert_eq!(first_response.status(), StatusCode::OK);
    let first_json = response_json(&runtime, first_response);
    assert_eq!(
        first_json["result"]["structuredContent"]["data"]["records"]
            .as_array()
            .unwrap()
            .len(),
        20
    );
    assert_eq!(
        first_json["result"]["structuredContent"]["data"]["complete"],
        false
    );
    assert_eq!(
        first_json["result"]["structuredContent"]["data"]["next_cursor"],
        "20"
    );
    assert_eq!(
        first_json["result"]["_meta"]["lethe/response_limit"]["effective_limit"],
        20
    );

    let second_response = runtime
        .block_on(async {
            app.oneshot(mcp_request(
                &token,
                tool_call(
                    "get_thread",
                    serde_json::json!({"record_id": "1.000000", "cursor": "20"}),
                ),
            ))
            .await
        })
        .unwrap();
    assert_eq!(second_response.status(), StatusCode::OK);
    let second_json = response_json(&runtime, second_response);
    assert_eq!(
        second_json["result"]["structuredContent"]["data"]["records"]
            .as_array()
            .unwrap()
            .len(),
        5
    );
    assert_eq!(
        second_json["result"]["structuredContent"]["data"]["complete"],
        true
    );
    assert!(second_json["result"]["structuredContent"]["data"]["next_cursor"].is_null());
}

#[test]
fn write_supplemental_requires_write_scope_and_refreshes_projection() {
    let (signer, oauth) = signer_and_oauth();
    let (_root, service, observation_id) = service_with_records_and_first_id(oauth);
    let app = build_mcp_router(service);
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let arguments = serde_json::json!({
        "id": "sup:00000000-0000-7000-8000-000000000001",
        "kind": "decision@1",
        "derived_from": {
            "observations": [observation_id.as_str()],
            "blobs": [],
            "supplementals": []
        },
        "payload": {
            "statement": "MCP write decision",
            "rationale": "write path fixture"
        },
        "created_by": "actor:mcp-test",
        "mutability": "append_only"
    });

    let read_only_response = runtime
        .block_on(async {
            app.clone()
                .oneshot(mcp_request(
                    &read_only_token(&signer),
                    tool_call("write_supplemental", arguments.clone()),
                ))
                .await
        })
        .unwrap();
    assert_eq!(read_only_response.status(), StatusCode::OK);
    let read_only_json = response_json(&runtime, read_only_response);
    assert_eq!(read_only_json["result"]["isError"], true);
    let challenge = read_only_json["result"]["_meta"]["mcp/www_authenticate"][0]
        .as_str()
        .unwrap();
    assert!(challenge.contains("error=\"insufficient_scope\""));
    assert!(challenge.contains("scope=\"write:supplemental\""));

    let unresolved_arguments = serde_json::json!({
        "id": "sup:00000000-0000-7000-8000-000000000002",
        "kind": "decision@1",
        "derived_from": {
            "observations": ["obs:missing"],
            "blobs": [],
            "supplementals": []
        },
        "payload": {
            "statement": "MCP unresolved decision",
            "rationale": "missing anchor fixture"
        },
        "created_by": "actor:mcp-test",
        "mutability": "append_only"
    });
    let unresolved_response = runtime
        .block_on(async {
            app.clone()
                .oneshot(mcp_request(
                    &valid_token(&signer),
                    tool_call("write_supplemental", unresolved_arguments),
                ))
                .await
        })
        .unwrap();
    assert_eq!(unresolved_response.status(), StatusCode::OK);
    let unresolved_json = response_json(&runtime, unresolved_response);
    assert_eq!(unresolved_json["error"]["code"], -32602);
    assert!(
        unresolved_json["error"]["message"]
            .as_str()
            .is_some_and(|message| message.contains("SupplementalValidation:unresolved_anchor"))
    );

    let write_response = runtime
        .block_on(async {
            app.clone()
                .oneshot(mcp_request(
                    &valid_token(&signer),
                    tool_call("write_supplemental", arguments),
                ))
                .await
        })
        .unwrap();
    assert_eq!(write_response.status(), StatusCode::OK);
    let write_json = response_json(&runtime, write_response);
    assert_eq!(
        write_json["result"]["structuredContent"]["record"]["kind"],
        "decision@1"
    );

    let search_response = runtime
        .block_on(async {
            app.oneshot(mcp_request(
                &valid_token(&signer),
                tool_call(
                    "search_decisions",
                    serde_json::json!({"query": "write decision"}),
                ),
            ))
            .await
        })
        .unwrap();
    let search_json = response_json(&runtime, search_response);
    let matches = search_json["result"]["structuredContent"]["data"]["matches"]
        .as_array()
        .unwrap();
    assert!(
        matches
            .iter()
            .any(|item| item["statement"] == "MCP write decision")
    );
}

#[test]
fn chatgpt_fixture_import_then_mcp_write_decision_is_searchable() {
    let (signer, oauth) = signer_and_oauth();
    let (_root, db, blobs) = temp_paths();
    let service = bootstrap_ready(test_config(db.clone(), blobs.clone(), oauth));
    let report = service
        .ingest_observation_drafts(chatgpt_fixture_drafts(), "chatgpt-e2e")
        .unwrap();
    assert_eq!(report.ingested, 1);
    assert_eq!(report.duplicates, 0);
    assert_eq!(report.quarantined, 0);

    let persistence = SqlitePersistence::open(&db, &blobs, &[7; 32]).unwrap();
    let observations = persistence.load_observations().unwrap();
    let chatgpt_observation_id = observations
        .iter()
        .find(|observation| {
            observation
                .source_system
                .as_ref()
                .is_some_and(|source| source.as_str() == "sys:chatgpt")
        })
        .map(|observation| observation.id.clone())
        .unwrap();
    drop(persistence);

    let app = build_mcp_router(service);
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let arguments = serde_json::json!({
        "id": "sup:00000000-0000-7000-8000-000000000101",
        "kind": "decision@1",
        "derived_from": {
            "observations": [chatgpt_observation_id.as_str()],
            "blobs": [],
            "supplementals": []
        },
        "payload": {
            "statement": "ChatGPT fixture decision",
            "rationale": "cross-client mcp write fixture"
        },
        "created_by": "actor:mcp-test",
        "mutability": "append_only"
    });

    let write_response = runtime
        .block_on(async {
            app.clone()
                .oneshot(mcp_request(
                    &valid_token(&signer),
                    tool_call("write_supplemental", arguments),
                ))
                .await
        })
        .unwrap();
    assert_eq!(write_response.status(), StatusCode::OK);
    let write_json = response_json(&runtime, write_response);
    assert_eq!(
        write_json["result"]["structuredContent"]["record"]["kind"],
        "decision@1"
    );

    let search_response = runtime
        .block_on(async {
            app.oneshot(mcp_request(
                &valid_token(&signer),
                tool_call(
                    "search_decisions",
                    serde_json::json!({"query": "fixture decision"}),
                ),
            ))
            .await
        })
        .unwrap();
    assert_eq!(search_response.status(), StatusCode::OK);
    let search_json = response_json(&runtime, search_response);
    let matches = search_json["result"]["structuredContent"]["data"]["matches"]
        .as_array()
        .unwrap();
    assert!(
        matches
            .iter()
            .any(|item| item["statement"] == "ChatGPT fixture decision")
    );
}
