use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};

use axum::body::Body;
use axum::http::Request;
use chrono::Utc;
use lethe_adapter_api::idempotency::identity_key;
use lethe_adapter_api::retry::ResilientExecutor;
use lethe_adapter_api::traits::ObservationDraft;
use lethe_adapter_gslides::gslides::client::{PresentationNative, SlideNative, SlideRevision};
use lethe_adapter_slack::slack::client::{FixtureSlackClient, SlackMessage, SlackMessageType};
use lethe_adapter_slack::slack::mapper::SlackAdapter;
use lethe_api::api::grep::GrepRequest;
use lethe_core::domain::supplemental::InputAnchorSet;

use super::{
    AppCore, AppService, CompactProjectionState, GoogleSourceRuntime, ImportItemResult,
    ImportOutcome, SearchJobStatus, SelfHostError, SlackSourceRuntime, classify_slack_ingress,
    discovered_slack_threads, extract_slide_text_fragments, infer_profile_name_from_fragments,
    latest_revision_to_capture, namespace_draft, non_empty_state, ranked_self_intro_slide_indices,
    thread_root_ts,
};
use crate::self_host::config::{
    ApiTokenConfig, CorpusProjectionConfig, FreshnessConfig, GoogleConfig, JsonWebKey,
    JsonWebKeySet, McpOAuthConfig, OperationalLedgerConfig, OpsConfig, ResourceLimits,
    SecretString, SelfHostConfig, SlackConfig, SlideAiConfig, SupplementalConfig,
};
use crate::self_host::google::HttpGoogleSlidesClient;
use crate::self_host::slack::HttpSlackClient;
use lethe_core::domain::{
    ActorRef, AuthorityModel, CaptureModel, EntityRef, IdempotencyKey, IngestResult, Mutability,
    Observation, ObservationId, ObserverRef, ProjectionRef, SchemaRef, SemVer, SourceSystemRef,
    SupplementalId, SupplementalRecord,
};
use lethe_derivation_gemini::GeminiSlideAnalyzer;
use lethe_engine::lake::ConsentDecisionResolver;
use lethe_policy::governance::types::ConsentStatus;
use lethe_projection_corpus::PrivacyFilter;
use lethe_runtime::runtime::partition::RoutingKeyOrder;
use lethe_storage_api::{
    OperationalAppendOutcome, OperationalAppendRequest, SlackThreadKey, StoredObservation,
};
use lethe_storage_sqlite::persistence::{SqliteOperationalEventStore, SqlitePersistence};
use tower::ServiceExt;

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
fn capture_gate_and_projection_use_identical_latest_consent_ordering() {
    let target = Observation {
        id: Observation::new_id(),
        schema: SchemaRef::new("schema:claude-message"),
        schema_version: SemVer::new("1.0.0"),
        observer: ObserverRef::new("obs:test"),
        source_system: Some(SourceSystemRef::new("sys:claude-ai")),
        actor: None,
        authority_model: AuthorityModel::LakeAuthoritative,
        capture_model: CaptureModel::Event,
        subject: EntityRef::new("person:1"),
        target: None,
        payload: serde_json::json!({"text": "consent parity", "email": "person-1"}),
        attachments: Vec::new(),
        published: "2026-01-01T00:00:00Z".parse().unwrap(),
        recorded_at: "2026-01-01T00:00:01Z".parse().unwrap(),
        consent: None,
        idempotency_key: IdempotencyKey::new("parity-target"),
        meta: serde_json::json!({}),
    };
    let decision = |id: &str, status: &str, published: &str| {
        let mut observation = target.clone();
        observation.id = ObservationId::new(id);
        observation.schema = SchemaRef::new("schema:consent-decision");
        observation.payload = serde_json::json!({
            "status": status,
            "identifier": "person-1",
        });
        observation.published = published.parse().unwrap();
        observation.recorded_at = observation.published + chrono::Duration::seconds(1);
        observation
    };
    let newest_unrestricted = decision("consent:new", "unrestricted", "2026-01-03T00:00:00Z");
    let late_old_opt_out = decision("consent:old", "opted_out", "2026-01-02T00:00:00Z");
    let observations = vec![target.clone(), newest_unrestricted, late_old_opt_out];
    let compact = CompactProjectionState::build(&observations).unwrap();
    let capture_status = compact.resolve(&target.subject, &["person-1".to_owned()], None);
    let projection = PrivacyFilter::from_observations(&observations);
    assert_eq!(capture_status, ConsentStatus::Unrestricted);
    assert!(projection.visible(&target));
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
        client_ref: None,
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
        operational_ledger: OperationalLedgerConfig::Sqlite {
            data_space_id: lethe_core::domain::DataSpaceId::new("space:test"),
            database_path: db.with_extension("operational.sqlite3"),
            blob_dir: db.with_extension("operational-blobs"),
            secret_encryption_key: [8; 32],
        },
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
            max_concurrent_imports: 2,
            max_import_drafts: 10_000,
            max_page_size: 100,
            max_search_job_workers: 2,
            max_search_job_records: 1_000,
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

#[test]
fn operational_ledger_survives_interface_service_restart() {
    let root = std::env::temp_dir().join(format!(
        "lethe-self-host-operational-test-{}",
        uuid::Uuid::now_v7()
    ));
    let db = root.join("lethe.sqlite3");
    let blobs = root.join("blobs");
    let config = test_config(db, blobs);
    let service = AppService::bootstrap(config.clone()).unwrap();
    let event = lethe_storage_api::conformance::sample_operational_event(
        &lethe_core::domain::DataSpaceId::new("space:test"),
        "event:selfhost:1",
        "work:selfhost",
        1,
        "operational:selfhost:1",
    );

    let outcomes = service
        .append_operational_events(&[OperationalAppendRequest {
            event,
            expected_stream_version: 0,
        }])
        .unwrap();
    assert!(matches!(
        outcomes.as_slice(),
        [OperationalAppendOutcome::Appended {
            stream_version: 1,
            ..
        }]
    ));
    let blob_ref = service.put_operational_blob(b"raw conversation").unwrap();
    assert_eq!(
        service.get_operational_blob(&blob_ref).unwrap(),
        Some(b"raw conversation".to_vec())
    );
    drop(service);

    let restarted = AppService::bootstrap(config).unwrap();
    assert_eq!(restarted.operational_event_stats().unwrap().count, 1);
    assert_eq!(
        restarted
            .operational_events_for_stream("work:selfhost", 0, 10)
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn v10_manifest_shape_rebuilds_on_restart_without_body_cache() {
    let root = std::env::temp_dir().join(format!(
        "lethe-self-host-v10-manifest-test-{}",
        uuid::Uuid::now_v7()
    ));
    let db = root.join("lethe.sqlite3");
    let blobs = root.join("blobs");
    let config = test_config(db.clone(), blobs.clone());
    let persistence = SqlitePersistence::open(&db, &blobs, &[7; 32]).unwrap();
    let target = wave2_slack_observation(
        "U-V10",
        "V10 manifest target",
        Some("v10@example.test"),
        "1.000001",
        "2026-07-12T00:00:00Z".parse().unwrap(),
    );
    persistence.persist_observation(&target).unwrap();
    drop(persistence);

    let service = AppService::bootstrap(config.clone()).unwrap();
    service.wait_for_non_corpus_rebuild().unwrap();
    let mut legacy_manifest = service
        .persistence_lock()
        .unwrap()
        .projection_records(&ProjectionRef::new("proj:person-page"))
        .unwrap()
        .unwrap();
    legacy_manifest["format_version"] = serde_json::json!(10);
    service
        .persistence_lock()
        .unwrap()
        .materialize_projection(&ProjectionRef::new("proj:person-page"), &legacy_manifest)
        .unwrap();
    drop(service);

    let restarted = AppService::bootstrap(config).unwrap();
    restarted.wait_for_non_corpus_rebuild().unwrap();
    assert_eq!(
        restarted
            .non_corpus_rebuild_count
            .load(std::sync::atomic::Ordering::Relaxed),
        1
    );
    let rebuilt_manifest = restarted
        .persistence_lock()
        .unwrap()
        .projection_records(&ProjectionRef::new("proj:person-page"))
        .unwrap()
        .unwrap();
    assert_eq!(
        rebuilt_manifest["format_version"],
        super::NON_CORPUS_MATERIALIZATION_VERSION
    );
    assert_eq!(restarted.reply_slo_response().unwrap().data.rows.len(), 1);

    drop(restarted);
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn current_schema_and_manifest_restore_without_second_boot_rebuild() {
    let root = std::env::temp_dir().join(format!(
        "lethe-self-host-current-restore-test-{}",
        uuid::Uuid::now_v7()
    ));
    let db = root.join("lethe.sqlite3");
    let blobs = root.join("blobs");
    let config = test_config(db, blobs);

    let service = AppService::bootstrap(config.clone()).unwrap();
    service.wait_for_non_corpus_rebuild().unwrap();
    let report = service
        .ingest_observation_drafts(vec![wave2_slack_draft(41)], "slack-test")
        .unwrap();
    assert_eq!(report.ingested, 1);
    wait_for_append_consumer(&service);
    let first_manifest = service
        .persistence_lock()
        .unwrap()
        .projection_records(&ProjectionRef::new("proj:person-page"))
        .unwrap()
        .unwrap();
    assert_eq!(
        first_manifest["format_version"],
        super::NON_CORPUS_MATERIALIZATION_VERSION
    );
    drop(service);

    let restarted = AppService::bootstrap(config).unwrap();
    assert_eq!(
        restarted
            .non_corpus_rebuild_count
            .load(std::sync::atomic::Ordering::Relaxed),
        0,
        "a current schema and manifest must restore without a second-boot rebuild"
    );
    assert!(
        restarted
            .non_corpus_rebuild_reasons
            .lock()
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        restarted
            .persons_response(
                None,
                None,
                &lethe_api::api::pagination::PaginationParams::default(),
            )
            .unwrap()
            .data["total"],
        1
    );

    drop(restarted);
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn schema_migration_applied_on_restart_forces_migration_rebuild() {
    let root = std::env::temp_dir().join(format!(
        "lethe-self-host-migration-rebuild-test-{}",
        uuid::Uuid::now_v7()
    ));
    let db = root.join("lethe.sqlite3");
    let blobs = root.join("blobs");
    let config = test_config(db.clone(), blobs);

    let service = AppService::bootstrap(config.clone()).unwrap();
    service.wait_for_non_corpus_rebuild().unwrap();
    let report = service
        .ingest_observation_drafts(vec![wave2_slack_draft(42)], "slack-test")
        .unwrap();
    assert_eq!(report.ingested, 1);
    wait_for_append_consumer(&service);
    drop(service);

    let connection = rusqlite::Connection::open(&db).unwrap();
    assert_eq!(
        connection
            .execute("DELETE FROM schema_migrations WHERE version = 14", [])
            .unwrap(),
        1
    );
    drop(connection);

    let restarted = AppService::bootstrap(config).unwrap();
    assert_eq!(
        restarted
            .non_corpus_rebuild_count
            .load(std::sync::atomic::Ordering::Relaxed),
        1
    );
    assert_eq!(
        restarted
            .non_corpus_rebuild_reasons
            .lock()
            .unwrap()
            .as_slice(),
        ["migration"]
    );
    restarted.wait_for_non_corpus_rebuild().unwrap();
    assert_eq!(
        restarted
            .persons_response(
                None,
                None,
                &lethe_api::api::pagination::PaginationParams::default(),
            )
            .unwrap()
            .data["total"],
        1
    );

    drop(restarted);
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn health_and_operational_read_do_not_occupy_async_worker_while_storage_is_locked() {
    let root = std::env::temp_dir().join(format!(
        "lethe-self-host-health-concurrency-test-{}",
        uuid::Uuid::now_v7()
    ));
    let db = root.join("lethe.sqlite3");
    let blobs = root.join("blobs");
    let (service, root) = tokio::task::spawn_blocking(move || {
        let persistence = SqlitePersistence::open(&db, &blobs, &[7; 32]).unwrap();
        (test_service(test_config(db, blobs), persistence), root)
    })
    .await
    .unwrap();
    let router = crate::self_host::server::build_router(service.clone());

    let (locked_tx, locked_rx) = std::sync::mpsc::channel();
    let release = Arc::new(AtomicBool::new(false));
    let locker_release = Arc::clone(&release);
    let locker_service = service.clone();
    let locker = tokio::task::spawn_blocking(move || {
        let _core = locker_service.core_lock().unwrap();
        let _persistence = locker_service.persistence_lock().unwrap();
        locked_tx.send(()).unwrap();
        while !locker_release.load(Ordering::Acquire) {
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    });
    locked_rx.recv().unwrap();

    let health_task = tokio::spawn(
        router.clone().oneshot(
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .unwrap(),
        ),
    );
    let operational_page_task = tokio::spawn(
        router.oneshot(
            Request::builder()
                .uri("/api/operational-events?after_cursor=0&limit=100")
                .header("authorization", "Bearer test-api-token")
                .body(Body::empty())
                .unwrap(),
        ),
    );
    let releaser = std::thread::spawn({
        let release = Arc::clone(&release);
        move || {
            std::thread::sleep(std::time::Duration::from_millis(200));
            release.store(true, Ordering::Release);
        }
    });

    let worker_remained_available = tokio::time::timeout(
        std::time::Duration::from_millis(100),
        tokio::time::sleep(std::time::Duration::from_millis(20)),
    )
    .await
    .is_ok();
    assert!(
        worker_remained_available,
        "health and operational authorization lock waits must not block the single Tokio worker"
    );

    let health_response = health_task.await.unwrap().unwrap();
    let operational_page_response = operational_page_task.await.unwrap().unwrap();
    assert_eq!(health_response.status(), axum::http::StatusCode::OK);
    assert_eq!(
        operational_page_response.status(),
        axum::http::StatusCode::OK
    );
    releaser.join().unwrap();
    locker.await.unwrap();
    tokio::task::spawn_blocking(move || {
        drop(service);
        let _ = std::fs::remove_dir_all(root);
    })
    .await
    .unwrap();
}

fn test_service(config: SelfHostConfig, persistence: SqlitePersistence) -> AppService {
    let persistence: Arc<Mutex<Box<dyn lethe_storage_api::StoragePorts>>> =
        Arc::new(Mutex::new(Box::new(persistence)));
    let mut persistence_read_pool = Vec::with_capacity(4);
    for _ in 0..4 {
        persistence_read_pool.push(Arc::new(Mutex::new(Box::new(
            SqlitePersistence::open_with_routing_key_order(
                &config.database_path,
                &config.blob_dir,
                &config.secret_encryption_key,
                config.routing_key_order,
            )
            .unwrap(),
        )
            as Box<dyn lethe_storage_api::StoragePorts>)));
    }
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
        Arc::clone(&persistence_read_pool[0]),
    );
    let search_job_queue =
        super::start_search_job_workers(config.resource_limits.max_search_job_workers).unwrap();
    let OperationalLedgerConfig::Sqlite {
        data_space_id,
        database_path,
        blob_dir,
        secret_encryption_key,
    } = &config.operational_ledger
    else {
        panic!("test_service requires the explicit SQLite operational backend");
    };
    let operational_ledger: Arc<Mutex<Box<dyn lethe_storage_api::OperationalStoragePorts>>> =
        Arc::new(Mutex::new(Box::new(
            SqliteOperationalEventStore::open(
                data_space_id.clone(),
                database_path,
                blob_dir,
                secret_encryption_key,
            )
            .unwrap(),
        )));
    let mut operational_ledger_read_pool = Vec::with_capacity(4);
    for _ in 0..4 {
        operational_ledger_read_pool.push(Arc::new(Mutex::new(Box::new(
            SqliteOperationalEventStore::open(
                data_space_id.clone(),
                database_path,
                blob_dir,
                secret_encryption_key,
            )
            .unwrap(),
        )
            as Box<dyn lethe_storage_api::OperationalStoragePorts>)));
    }
    let history_projection = Arc::new(Mutex::new(
        lethe_history::HistoryProjection::rebuild(operational_ledger.lock().unwrap().as_ref())
            .unwrap(),
    ));
    let core = AppCore::new_with_config(
        vec![],
        vec![],
        vec![],
        super::freshness_thresholds(&config),
        config.channels.clone(),
    )
    .unwrap();
    let core_snapshot = Arc::new(arc_swap::ArcSwap::from_pointee(core.clone()));
    let service = AppService {
        core: Arc::new(Mutex::new(core)),
        core_snapshot,
        persistence,
        persistence_read_pool,
        persistence_read_next: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        operational_ledger,
        operational_ledger_read_pool,
        operational_ledger_read_next: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        history_projection,
        derived_projection_lane: Arc::new(Mutex::new(())),
        bulk_import_operation: Arc::new(Mutex::new(())),
        non_bulk_projection_operation: Arc::new(Mutex::new(())),
        import_in_flight: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        search_index,
        search_jobs: Arc::new(Mutex::new(BTreeMap::new())),
        search_job_sequence: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        search_job_queue,
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
        append_consumer_in_flight: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        append_consumer_error: Arc::new(Mutex::new(None)),
        search_index_catch_up_in_flight: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        non_corpus_rebuild_in_flight: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        non_corpus_rebuild_error: Arc::new(Mutex::new(None)),
        non_corpus_rebuild_count: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        non_corpus_rebuild_reasons: Arc::new(Mutex::new(Vec::new())),
        non_corpus_rebuild_page_count: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        non_corpus_rebuild_page_delay: None,
        publish_count: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        search_job_test_gate: None,
        search_job_test_fault: None,
    };
    service
        .persistence
        .lock()
        .unwrap()
        .set_state("append_consumer:person-page", "0")
        .unwrap();
    service
}

#[test]
fn regex_search_job_lifecycle_reaches_a_terminal_state() {
    let root = std::env::temp_dir().join(format!(
        "lethe-self-host-search-job-test-{}",
        uuid::Uuid::now_v7()
    ));
    let db = root.join("lethe.sqlite3");
    let blobs = root.join("blobs");
    let config = test_config(db.clone(), blobs.clone());
    let persistence = SqlitePersistence::open(&db, &blobs, &[7; 32]).unwrap();
    let service = test_service(config, persistence);
    wait_for_search_index_ready(&service);

    let queued = service
        .submit_corpus_search_job(GrepRequest {
            pattern: "[a-z]+".to_owned(),
            ..GrepRequest::default()
        })
        .unwrap();
    assert_eq!(queued.status, "queued");

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    let terminal = loop {
        let status = service.search_job_status(&queued.job_id).unwrap();
        if status.status == "completed" || status.status == "failed" {
            break status;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "search job did not reach a terminal state"
        );
        std::thread::sleep(std::time::Duration::from_millis(10));
    };
    assert_eq!(terminal.status, "completed");
    assert!(terminal.result.is_some());
    assert!(terminal.error.is_none());

    drop(service);
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn regex_search_job_queue_waits_and_rejects_after_bounded_capacity() {
    let root = std::env::temp_dir().join(format!(
        "lethe-self-host-search-job-queue-test-{}",
        uuid::Uuid::now_v7()
    ));
    let db = root.join("lethe.sqlite3");
    let blobs = root.join("blobs");
    let mut config = test_config(db.clone(), blobs.clone());
    config.resource_limits.max_search_job_workers = 1;
    let persistence = SqlitePersistence::open(&db, &blobs, &[7; 32]).unwrap();
    let mut service = test_service(config, persistence);
    wait_for_search_index_ready(&service);
    let gate = Arc::new(std::sync::Barrier::new(2));
    service.search_job_test_gate = Some(Arc::clone(&gate));

    let first = service
        .submit_corpus_search_job(GrepRequest {
            pattern: "[a-z]+".to_owned(),
            ..GrepRequest::default()
        })
        .unwrap();
    wait_for_search_job_status(&service, &first.job_id, |status| status == "running");
    service.search_job_test_gate = None;

    let second = service
        .submit_corpus_search_job(GrepRequest {
            pattern: "[0-9]+".to_owned(),
            ..GrepRequest::default()
        })
        .unwrap();
    assert_eq!(second.status, "queued");
    assert_eq!(
        service.search_job_status(&second.job_id).unwrap().status,
        "queued"
    );
    assert!(
        service
            .submit_corpus_search_job(GrepRequest {
                pattern: "[A-Z]+".to_owned(),
                ..GrepRequest::default()
            })
            .is_err()
    );

    gate.wait();
    let first_terminal = wait_for_search_job_terminal(&service, &first.job_id);
    let second_terminal = wait_for_search_job_terminal(&service, &second.job_id);
    assert_eq!(first_terminal.status, "completed");
    assert_eq!(second_terminal.status, "completed");

    drop(service);
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn regex_search_job_worker_error_and_panic_become_failed_terminal_states() {
    let root = std::env::temp_dir().join(format!(
        "lethe-self-host-search-job-failure-test-{}",
        uuid::Uuid::now_v7()
    ));
    let db = root.join("lethe.sqlite3");
    let blobs = root.join("blobs");
    let config = test_config(db.clone(), blobs.clone());
    let persistence = SqlitePersistence::open(&db, &blobs, &[7; 32]).unwrap();
    let mut service = test_service(config, persistence);
    wait_for_search_index_ready(&service);

    service.search_job_test_fault = Some(super::SearchJobTestFault::Error);
    let error_job = service
        .submit_corpus_search_job(GrepRequest {
            pattern: "[a-z]+".to_owned(),
            ..GrepRequest::default()
        })
        .unwrap();
    let error_terminal = wait_for_search_job_terminal(&service, &error_job.job_id);
    assert_eq!(error_terminal.status, "failed");
    assert!(
        error_terminal
            .error
            .as_deref()
            .is_some_and(|error| error.contains("injected search job worker failure"))
    );

    service.search_job_test_fault = Some(super::SearchJobTestFault::Panic);
    let panic_job = service
        .submit_corpus_search_job(GrepRequest {
            pattern: "[0-9]+".to_owned(),
            ..GrepRequest::default()
        })
        .unwrap();
    let panic_terminal = wait_for_search_job_terminal(&service, &panic_job.job_id);
    assert_eq!(panic_terminal.status, "failed");
    assert!(
        panic_terminal
            .error
            .as_deref()
            .is_some_and(|error| error.contains("search job worker panicked"))
    );

    drop(service);
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn terminal_search_jobs_are_evicted_oldest_first() {
    let root = std::env::temp_dir().join(format!(
        "lethe-self-host-search-job-retention-test-{}",
        uuid::Uuid::now_v7()
    ));
    let db = root.join("lethe.sqlite3");
    let blobs = root.join("blobs");
    let mut config = test_config(db.clone(), blobs.clone());
    config.resource_limits.max_search_job_records = 1;
    let persistence = SqlitePersistence::open(&db, &blobs, &[7; 32]).unwrap();
    let service = test_service(config, persistence);
    wait_for_search_index_ready(&service);

    let first = service
        .submit_corpus_search_job(GrepRequest {
            pattern: "[a-z]+".to_owned(),
            ..GrepRequest::default()
        })
        .unwrap();
    assert_eq!(
        wait_for_search_job_terminal(&service, &first.job_id).status,
        "completed"
    );

    let second = service
        .submit_corpus_search_job(GrepRequest {
            pattern: "[0-9]+".to_owned(),
            ..GrepRequest::default()
        })
        .unwrap();
    assert_eq!(
        wait_for_search_job_terminal(&service, &second.job_id).status,
        "completed"
    );
    assert!(matches!(
        service.search_job_status(&first.job_id),
        Err(SelfHostError::NotFound(_))
    ));
    assert_eq!(
        service.search_job_status(&second.job_id).unwrap().status,
        "completed"
    );

    drop(service);
    let _ = std::fs::remove_dir_all(root);
}

fn wait_for_search_job_status(
    service: &AppService,
    job_id: &str,
    predicate: impl Fn(&str) -> bool,
) -> SearchJobStatus {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let status = service.search_job_status(job_id).unwrap();
        if predicate(&status.status) {
            return status;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "search job {job_id} did not reach the expected state"
        );
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
}

fn wait_for_search_job_terminal(service: &AppService, job_id: &str) -> SearchJobStatus {
    wait_for_search_job_status(service, job_id, |status| {
        status == "completed" || status == "failed"
    })
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

fn wait_for_append_consumer(service: &AppService) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    while service
        .append_consumer_in_flight
        .load(std::sync::atomic::Ordering::Acquire)
    {
        assert!(
            std::time::Instant::now() < deadline,
            "append-seq consumer did not complete"
        );
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    let error = service.append_consumer_error.lock().unwrap().clone();
    assert!(error.is_none(), "append-seq consumer failed: {error:?}");
}

fn wait_for_append_consumer_stopped(service: &AppService) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    while service
        .append_consumer_in_flight
        .load(std::sync::atomic::Ordering::Acquire)
    {
        assert!(
            std::time::Instant::now() < deadline,
            "append-seq consumer did not stop"
        );
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
}

fn wait_for_search_index_high_water(service: &AppService, expected: u64) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        if let Ok(metadata) = service.search_index.execute(|index| index.metadata())
            && metadata.last_append_seq >= expected
        {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "search index did not reach append sequence {expected}"
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
fn bootstrap_migrates_legacy_manifest_to_current_version_without_data_loss() {
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
    let legacy_corpus = lethe_projection_corpus::CorpusProjector::personal_all_text_config()
        .project_observations(std::slice::from_ref(&observation));
    let expected_search_record_ids = legacy_corpus
        .iter()
        .map(|record| record.record_id.clone())
        .collect::<Vec<_>>();
    let mut legacy_manifest = serde_json::to_value(super::ProjectionSnapshot::default()).unwrap();
    legacy_manifest["corpus"] = serde_json::to_value(&legacy_corpus).unwrap();
    assert!(legacy_manifest.get("format_version").is_none());
    persistence
        .materialize_projection(&ProjectionRef::new("proj:person-page"), &legacy_manifest)
        .unwrap();
    let canonical_stats = persistence.observation_stats().unwrap();
    drop(persistence);

    let mut config = test_config(db.clone(), blobs.clone());
    config.corpus.mode = lethe_projection_corpus::CorpusMode::PersonalAllText;
    let service = AppService::bootstrap(config.clone()).unwrap();
    service.wait_for_non_corpus_rebuild().unwrap();
    assert_eq!(
        service
            .persistence_lock()
            .unwrap()
            .observation_stats()
            .unwrap(),
        canonical_stats
    );
    let built_at = service.core_lock().unwrap().snapshot.built_at;
    let materialized = service
        .persistence_lock()
        .unwrap()
        .projection_records(&ProjectionRef::new("proj:person-page"))
        .unwrap()
        .unwrap();
    assert_eq!(
        materialized["format_version"],
        super::NON_CORPUS_MATERIALIZATION_VERSION
    );
    assert_eq!(materialized["observation_count"], 1);
    assert_eq!(materialized["last_append_seq"], 1);
    assert_eq!(materialized["person_message_count"], 1);
    assert_eq!(materialized["reply_slo_count"], 0);
    assert!(materialized["snapshot"].get("person_page").is_none());
    assert!(materialized["snapshot"].get("reply_slo").is_none());
    assert_eq!(
        service
            .persistence_lock()
            .unwrap()
            .projection_item_count(&ProjectionRef::new("proj:person-page"))
            .unwrap(),
        materialized["identity_event_count"].as_u64().unwrap()
            + materialized["person_component_count"].as_u64().unwrap()
            + materialized["person_slide_count"].as_u64().unwrap()
            + materialized["person_message_count"].as_u64().unwrap()
            + materialized["reply_slo_count"].as_u64().unwrap()
    );
    let person_id = service
        .core_lock()
        .unwrap()
        .person_components
        .values()
        .next()
        .unwrap()
        .person
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
    let migrated_matches = response.data.matches.clone();
    assert_eq!(
        migrated_matches
            .iter()
            .map(|record| record.record_id.clone())
            .collect::<Vec<_>>(),
        expected_search_record_ids
    );
    assert!(
        migrated_matches[0]
            .snippet
            .contains("persisted bootstrap needle")
    );
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
    restarted.wait_for_non_corpus_rebuild().unwrap();
    assert_eq!(restarted.search_index.rebuild_started(), 0);
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
    assert_eq!(
        restarted
            .corpus_grep_response(&request)
            .unwrap()
            .data
            .matches,
        migrated_matches
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
fn incremental_thread_discovery_finds_roots_and_out_of_order_replies_without_duplicates() {
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
            meta: serde_json::json!({"source_instance": "slack-test"}),
        }
    }

    let observations = [
        slack_observation("C01ABC", "101.000001", Some("100.000001"), None),
        slack_observation("C01ABC", "100.000001", None, Some(2)),
        slack_observation("C01ABC", "102.000001", Some("100.000001"), None),
        slack_observation("C02XYZ", "201.000001", Some("200.000001"), None),
        slack_observation("C01ABC", "103.000001", None, Some(0)),
    ]
    .into_iter()
    .enumerate()
    .map(|(index, observation)| StoredObservation {
        leaf_id: "lake:test".into(),
        append_seq: u64::try_from(index + 1).unwrap(),
        observation,
    })
    .collect::<Vec<_>>();
    let roots = discovered_slack_threads(&observations).unwrap();
    let roots = roots
        .into_iter()
        .map(|thread| (thread.key, thread.observation_append_seq))
        .collect::<std::collections::BTreeMap<_, _>>();

    assert_eq!(roots.len(), 2);
    assert_eq!(
        roots[&SlackThreadKey {
            source_instance: "slack-test".into(),
            channel_id: "C01ABC".into(),
            thread_ts: "100.000001".into(),
        }],
        1
    );
    assert_eq!(
        roots[&SlackThreadKey {
            source_instance: "slack-test".into(),
            channel_id: "C02XYZ".into(),
            thread_ts: "200.000001".into(),
        }],
        4
    );
}

#[test]
fn thread_catalog_sync_matches_full_rediscovery_without_repolling_idle_threads() {
    fn message(ts: &str, thread_ts: Option<&str>, reply_count: u32) -> SlackMessage {
        SlackMessage {
            channel_id: String::new(),
            channel_name: "general".into(),
            ts: ts.into(),
            thread_ts: thread_ts.map(str::to_owned),
            user_id: format!("U-{ts}"),
            user_name: format!("user-{ts}"),
            email: None,
            text: format!("message-{ts}"),
            ingress_kind: None,
            mentions: vec![],
            message_type: SlackMessageType::Message,
            edited: None,
            reactions: vec![],
            files: vec![],
            reply_count,
            reply_users_count: u32::from(reply_count > 0),
        }
    }

    let root = std::env::temp_dir().join(format!("lethe-self-host-test-{}", uuid::Uuid::now_v7()));
    let db = root.join("lethe.sqlite3");
    let blobs = root.join("blobs");
    let persistence = SqlitePersistence::open(&db, &blobs, &[7; 32]).unwrap();
    let service = test_service(test_config(db, blobs), persistence);
    let adapter = SlackAdapter::new(FixtureSlackClient::new(), service.slack_adapter_config());
    let file_client = FixtureSlackClient::new();
    let mut replies_client = FixtureSlackClient::new();
    let root_one = "1700000000.000001";
    let reply_one = "1700000001.000001";
    replies_client
        .replies
        .insert(root_one.into(), vec![message(reply_one, Some(root_one), 0)]);

    let mut latest_ts = None;
    assert!(matches!(
        service
            .ingest_slack_message(
                &adapter,
                &file_client,
                "slack-test",
                "C01ABC",
                message(root_one, None, 1),
                &mut latest_ts,
            )
            .unwrap(),
        IngestResult::Ingested { .. }
    ));
    service.refresh_slack_thread_catalog().unwrap();
    assert_eq!(
        service
            .persistence_lock()
            .unwrap()
            .slack_thread_discovery_high_water()
            .unwrap(),
        1
    );

    let generation_one = service
        .persistence_lock()
        .unwrap()
        .advance_slack_thread_poll_generation()
        .unwrap();
    let thread = service
        .persistence_lock()
        .unwrap()
        .slack_threads_to_poll("slack-test", "C01ABC", generation_one, 10)
        .unwrap()
        .remove(0);
    assert_eq!(
        service
            .sync_thread_replies(
                &adapter,
                &file_client,
                &replies_client,
                &thread,
                generation_one,
            )
            .unwrap(),
        (1, 0, 1)
    );

    let generation_two = service
        .persistence_lock()
        .unwrap()
        .advance_slack_thread_poll_generation()
        .unwrap();
    let thread = service
        .persistence_lock()
        .unwrap()
        .slack_threads_to_poll("slack-test", "C01ABC", generation_two, 10)
        .unwrap()
        .remove(0);
    assert_eq!(
        service
            .sync_thread_replies(
                &adapter,
                &file_client,
                &replies_client,
                &thread,
                generation_two,
            )
            .unwrap(),
        (0, 0, 0)
    );
    assert_eq!(replies_client.reply_call_count(), 2);

    let root_two = "1700000002.000001";
    let reply_two = "1700000003.000001";
    replies_client
        .replies
        .insert(root_two.into(), vec![message(reply_two, Some(root_two), 0)]);
    assert!(matches!(
        service
            .ingest_slack_message(
                &adapter,
                &file_client,
                "slack-test",
                "C01ABC",
                message(root_two, None, 1),
                &mut latest_ts,
            )
            .unwrap(),
        IngestResult::Ingested { .. }
    ));
    let generation_three = service
        .persistence_lock()
        .unwrap()
        .advance_slack_thread_poll_generation()
        .unwrap();
    let threads = service
        .persistence_lock()
        .unwrap()
        .slack_threads_to_poll("slack-test", "C01ABC", generation_three, 10)
        .unwrap();
    assert_eq!(
        threads.len(),
        1,
        "idle historical thread must not be re-polled"
    );
    assert_eq!(threads[0].key.thread_ts, root_two);
    assert_eq!(
        service
            .sync_thread_replies(
                &adapter,
                &file_client,
                &replies_client,
                &threads[0],
                generation_three,
            )
            .unwrap(),
        (1, 0, 1)
    );
    assert_eq!(
        replies_client.reply_call_count(),
        3,
        "two cataloged roots must produce one delta-based remote call"
    );

    let generation_four = service
        .persistence_lock()
        .unwrap()
        .advance_slack_thread_poll_generation()
        .unwrap();
    let root_two_entry = service
        .persistence_lock()
        .unwrap()
        .slack_threads_to_poll("slack-test", "C01ABC", generation_four, 10)
        .unwrap()
        .remove(0);
    service
        .sync_thread_replies(
            &adapter,
            &file_client,
            &replies_client,
            &root_two_entry,
            generation_four,
        )
        .unwrap();

    let late_reply = "1700000004.000001";
    replies_client.replies.insert(
        root_one.into(),
        vec![
            message(reply_one, Some(root_one), 0),
            message(late_reply, Some(root_one), 0),
        ],
    );
    let due_generation = generation_two + super::IDLE_THREAD_RECHECK_INTERVAL;
    let mut generation = generation_four;
    while generation < due_generation {
        generation = service
            .persistence_lock()
            .unwrap()
            .advance_slack_thread_poll_generation()
            .unwrap();
    }
    let due_threads = service
        .persistence_lock()
        .unwrap()
        .slack_threads_to_poll("slack-test", "C01ABC", generation, 10)
        .unwrap();
    let root_one_entry = due_threads
        .iter()
        .find(|entry| entry.key.thread_ts == root_one)
        .unwrap();
    assert_eq!(
        service
            .sync_thread_replies(
                &adapter,
                &file_client,
                &replies_client,
                root_one_entry,
                generation,
            )
            .unwrap(),
        (1, 0, 1)
    );

    let observations = service
        .persistence_lock()
        .unwrap()
        .observation_page(0, 20)
        .unwrap();
    let actual_timestamps = observations
        .iter()
        .filter_map(|stored| stored.observation.payload.get("ts"))
        .filter_map(serde_json::Value::as_str)
        .collect::<std::collections::BTreeSet<_>>();
    let full_rediscovery_timestamps =
        std::collections::BTreeSet::from([root_one, reply_one, root_two, reply_two, late_reply]);
    assert_eq!(actual_timestamps, full_rediscovery_timestamps);
    assert_eq!(observations.len(), full_rediscovery_timestamps.len());
    service.refresh_slack_thread_catalog().unwrap();
    let persistence = service.persistence_lock().unwrap();
    assert_eq!(
        persistence.slack_thread_discovery_high_water().unwrap(),
        persistence.observation_stats().unwrap().max_append_seq
    );
    assert_eq!(
        persistence
            .slack_thread_catalog("slack-test", "C01ABC")
            .unwrap()
            .len(),
        2
    );
    drop(persistence);
    service.refresh_slack_thread_catalog().unwrap();
    assert_eq!(
        service
            .persistence_lock()
            .unwrap()
            .slack_thread_catalog("slack-test", "C01ABC")
            .unwrap()
            .len(),
        2,
        "replaying an empty discovery tail must not duplicate catalog entries"
    );

    let _ = std::fs::remove_dir_all(root);
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
            "communication_channel_kind": "slack",
            "communication_channel_external_id": "C01ABC",
            "communication_sender_id": "U1",
            "communication_thread_ref": "slack:thread:dup-ts",
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
        client_ref: None,
        meta: serde_json::json!({
            "canonical_json": serde_json::json!({
                "source": "slack",
                "object_id": "channel:C01ABC:ts:dup-ts",
                "body": "persisted"
            }).to_string(),
            "source_container": "slack-test:C01ABC",
            "source_instance": "slack-test",
            "communication_channel_kind": "slack",
            "communication_channel_external_id": "C01ABC",
            "communication_sender_id": "U1",
            "communication_thread_ref": "slack:thread:dup-ts",
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
    let root = std::env::temp_dir().join(format!(
        "lethe-materialized-selection-test-{}",
        uuid::Uuid::now_v7()
    ));
    let persistence =
        SqlitePersistence::open(&root.join("lethe.db"), &root.join("blobs"), &[7; 32]).unwrap();
    let stats = lethe_storage_api::ObservationStats {
        count: 0,
        max_append_seq: 0,
    };
    let materialized =
        super::MaterializedProjectionSnapshot::build(vec![], vec![], vec![], vec![], stats)
            .unwrap();
    let fingerprint = materialized.supplemental_fingerprint.clone();
    let value = materialized.manifest_value().unwrap();

    assert!(matches!(
        super::current_materialized_snapshot(
            &persistence,
            value.clone(),
            stats,
            &fingerprint,
            0,
            0
        )
        .unwrap(),
        super::MaterializedSnapshotRestore::Restored(_)
    ));
    assert!(matches!(
        super::current_materialized_snapshot(
            &persistence,
            value.clone(),
            lethe_storage_api::ObservationStats {
                count: 1,
                max_append_seq: 1,
            },
            &fingerprint,
            0,
            0,
        )
        .unwrap(),
        super::MaterializedSnapshotRestore::RebuildRequired { reason }
            if reason.contains("canonical watermark")
    ));

    let mut legacy_version = value.clone();
    legacy_version["format_version"] =
        serde_json::json!(super::NON_CORPUS_MATERIALIZATION_VERSION - 1);
    assert!(matches!(
        super::current_materialized_snapshot(
            &persistence,
            legacy_version,
            stats,
            &fingerprint,
            0,
            0
        )
        .unwrap(),
        super::MaterializedSnapshotRestore::RebuildRequired { reason }
            if reason.contains("older than current")
    ));

    let mut missing_version = value.clone();
    missing_version
        .as_object_mut()
        .unwrap()
        .remove("format_version");
    assert!(matches!(
        super::current_materialized_snapshot(
            &persistence,
            missing_version,
            stats,
            &fingerprint,
            0,
            0
        )
        .unwrap(),
        super::MaterializedSnapshotRestore::RebuildRequired { reason }
            if reason.contains("has no format_version")
    ));

    let mut non_numeric_version = value.clone();
    non_numeric_version["format_version"] = serde_json::json!("legacy");
    assert!(matches!(
        super::current_materialized_snapshot(
            &persistence,
            non_numeric_version,
            stats,
            &fingerprint,
            0,
            0
        )
        .unwrap(),
        super::MaterializedSnapshotRestore::RebuildRequired { reason }
            if reason.contains("not an unsigned integer")
    ));

    let mut future_version = value.clone();
    future_version["format_version"] =
        serde_json::json!(super::NON_CORPUS_MATERIALIZATION_VERSION + 1);
    assert!(matches!(
        super::current_materialized_snapshot(
            &persistence,
            future_version,
            stats,
            &fingerprint,
            0,
            0
        ),
        Err(SelfHostError::Ingestion(message)) if message.contains("newer than supported")
    ));

    assert!(matches!(
        super::current_materialized_snapshot(
            &persistence,
            serde_json::json!([]),
            stats,
            &fingerprint,
            0,
            0
        ),
        Err(SelfHostError::Ingestion(message)) if message.contains("not a JSON object")
    ));

    let mut malformed_current = value;
    malformed_current["unexpected"] = serde_json::json!(true);
    assert!(matches!(
        super::current_materialized_snapshot(
            &persistence,
            malformed_current,
            stats,
            &fingerprint,
            0,
            0
        ),
        Err(SelfHostError::Json(_))
    ));

    assert!(matches!(
        super::current_materialized_snapshot(
            &persistence,
            materialized.manifest_value().unwrap(),
            stats,
            &fingerprint,
            1,
            0,
        ),
        Err(SelfHostError::Ingestion(_))
    ));
    assert!(matches!(
        super::current_materialized_snapshot(
            &persistence,
            materialized.manifest_value().unwrap(),
            stats,
            &fingerprint,
            0,
            1,
        ),
        Err(SelfHostError::Ingestion(_))
    ));
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn supplemental_projection_cache_and_fingerprint_match_full_replay_after_each_delta() {
    let built_at = chrono::DateTime::parse_from_rfc3339("2026-07-15T00:01:00Z")
        .unwrap()
        .to_utc();
    let at = |second: u32| {
        chrono::DateTime::parse_from_rfc3339(&format!("2026-07-15T00:00:{second:02}Z"))
            .unwrap()
            .to_utc()
    };
    let record = |id: &str,
                  kind: &str,
                  payload: serde_json::Value,
                  derived_from: InputAnchorSet,
                  second: u32| SupplementalRecord {
        id: SupplementalId::new(id),
        kind: kind.to_owned(),
        derived_from,
        payload,
        created_by: ActorRef::new("actor:test"),
        created_at: at(second),
        mutability: Mutability::AppendOnly,
        record_version: None,
        model_version: None,
        consent_metadata: None,
        lineage: None,
    };
    let observation_anchor = |id: &str| InputAnchorSet {
        observations: vec![lethe_core::domain::ObservationId::new(id)],
        blobs: Vec::new(),
        supplementals: Vec::new(),
    };
    let supplemental_anchor = |id: &str| InputAnchorSet {
        observations: Vec::new(),
        blobs: Vec::new(),
        supplementals: vec![SupplementalId::new(id)],
    };
    let records = vec![
        record(
            "sup:summary-cache",
            "session-summary@1",
            serde_json::json!({"summary": "summary", "project": "alpha"}),
            observation_anchor("obs:source"),
            1,
        ),
        record(
            "sup:parking-cache",
            "parking@1",
            serde_json::json!({
                "statement": "park",
                "resume_context": "context",
                "project": "alpha"
            }),
            observation_anchor("obs:source"),
            2,
        ),
        record(
            "sup:claim-cache",
            "claim@1",
            serde_json::json!({
                "statement": "claim",
                "verification_mode": "check",
                "project": "alpha"
            }),
            observation_anchor("obs:source"),
            3,
        ),
        record(
            "sup:transition-cache",
            "claim-transition@1",
            serde_json::json!({"to_state": "parked"}),
            supplemental_anchor("sup:claim-cache"),
            4,
        ),
        record(
            "sup:decision-cache",
            "decision@1",
            serde_json::json!({"statement": "decide", "project": "alpha"}),
            observation_anchor("obs:source"),
            5,
        ),
        record(
            "sup:draft-cache",
            "reply-draft@1",
            serde_json::json!({
                "channel": "slack",
                "recipient": "U01",
                "body": "reply",
                "expires_at": "2026-07-15T00:00:30Z"
            }),
            observation_anchor("obs:incoming"),
            6,
        ),
        record(
            "sup:approval-cache",
            "reply-approval@1",
            serde_json::json!({"decision": "approved", "interface": "api"}),
            supplemental_anchor("sup:draft-cache"),
            7,
        ),
        record(
            "sup:send-cache",
            "send-record@1",
            serde_json::json!({
                "mode": "approved",
                "sent_at": "2026-07-15T00:00:08Z"
            }),
            supplemental_anchor("sup:draft-cache"),
            8,
        ),
    ];

    let mut cache = super::SupplementalProjectionCache::from_records(&[]);
    let mut prefix = Vec::new();
    let mut fingerprint = super::supplemental_fingerprint(&[]).unwrap();
    for current in records {
        cache.replace(None, &current);
        fingerprint =
            super::supplemental_fingerprint_after_delta(&fingerprint, None, &current).unwrap();
        prefix.push(current);

        assert_eq!(
            fingerprint,
            super::supplemental_fingerprint(&prefix).unwrap()
        );
        let incremental_claim_queue = cache.claim_queue();
        let full_claim_queue =
            lethe_projection_claim_queue::ClaimQueueProjector.project_records(&prefix);
        assert_eq!(
            serde_json::to_value(&incremental_claim_queue).unwrap(),
            serde_json::to_value(&full_claim_queue).unwrap()
        );
        let incremental_cognition = cache.cognition(&incremental_claim_queue, built_at);
        let full_cognition = lethe_projection_cognition::CognitionStateProjector::new(built_at)
            .project_with_claim_queue(&prefix, &full_claim_queue);
        assert_eq!(
            serde_json::to_value(&incremental_cognition).unwrap(),
            serde_json::to_value(&full_cognition).unwrap()
        );
        assert_eq!(
            serde_json::to_value(cache.card_queue.projection(built_at)).unwrap(),
            serde_json::to_value(
                lethe_projection_cognition::CardQueueProjector::new(built_at)
                    .project_records(&prefix)
            )
            .unwrap()
        );
    }
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
                .find(|item| item.item_key.starts_with("pm:"))
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
    resident.snapshot.person_page.messages.push(
        super::person_message_from_projection_item(&item, &materialized.compact_state).unwrap(),
    );
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
    pending_drift.pending_item_commit.as_mut().unwrap().commit =
        lethe_storage_api::ProjectionItemCommit::Delta {
            inserts: Vec::new(),
            updates: Vec::new(),
            deletes: Vec::new(),
        };
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
fn non_corpus_delta_classification_uses_declared_incremental_folds_im03() {
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
            super::classify_non_corpus_delta_with_reason(&[observation]).kind,
            super::NonCorpusDeltaKind::FreshnessOnly
        );
    }

    let unknown = freshness_only_observation(
        "schema:future-message",
        "sys:claude-ai",
        "unknown",
        published,
    );
    let unknown_classification = super::classify_non_corpus_delta_with_reason(&[unknown]);
    assert_eq!(
        unknown_classification.kind,
        super::NonCorpusDeltaKind::DeclaredSchemaSkip
    );

    let mut reply_relevant = freshness_only_observation(
        "schema:gmail-message",
        "sys:gmail",
        "reply-relevant",
        published,
    );
    reply_relevant.meta["communication_sender_id"] = serde_json::json!("sender@example.test");
    assert_eq!(
        super::classify_non_corpus_delta_with_reason(&[reply_relevant.clone()]).kind,
        super::NonCorpusDeltaKind::FreshnessOnly
    );
    reply_relevant.meta["communication_channel_id"] = serde_json::json!("chan:gmail");
    reply_relevant.meta["communication_thread_ref"] = serde_json::json!("gmail:thread:1");
    reply_relevant.meta["communication"] = serde_json::json!({
        "reply_due_at": "2026-07-13T01:00:00Z"
    });
    let reply_slo_classification = super::classify_non_corpus_delta_with_reason(&[reply_relevant]);
    assert_eq!(
        reply_slo_classification.kind,
        super::NonCorpusDeltaKind::Communication
    );
    assert_eq!(
        reply_slo_classification.materialization_mode(),
        "incremental"
    );
    assert_eq!(reply_slo_classification.kind_as_str(), "communication");

    let missing_slack_user_id = freshness_only_observation(
        "schema:slack-message",
        "sys:slack",
        "missing-user-id",
        published,
    );
    let slack_classification =
        super::classify_non_corpus_delta_with_reason(&[missing_slack_user_id]);
    assert_eq!(
        slack_classification.kind,
        super::NonCorpusDeltaKind::SlackMessage
    );
    let empty_classification = super::classify_non_corpus_delta_with_reason(&[]);
    assert_eq!(empty_classification.kind, super::NonCorpusDeltaKind::NoOp);
    assert_eq!(
        empty_classification.materialization_mode(),
        "not_applicable"
    );
    assert_eq!(empty_classification.kind_as_str(), "no_op");
    assert_eq!(unknown_classification.materialization_mode(), "incremental");
    assert_eq!(unknown_classification.kind_as_str(), "declared_schema_skip");
    let registry = super::seed_registry();
    super::validate_projection_fold_declarations(&registry, super::PROJECTION_FOLD_DECLARATIONS)
        .unwrap();
    let drifted =
        &super::PROJECTION_FOLD_DECLARATIONS[..super::PROJECTION_FOLD_DECLARATIONS.len() - 1];
    assert!(super::validate_projection_fold_declarations(&registry, drifted).is_err());
}

#[test]
fn observation_import_timer_records_commit_stage_duration() {
    let mut timer = super::ObservationImportTimer::new();
    timer.record_stage(
        super::ImportTimingStage::SpawnBlockingWait,
        std::time::Duration::from_millis(7),
    );
    timer.record_stage(
        super::ImportTimingStage::PersistenceLockWait,
        std::time::Duration::from_millis(3),
    );
    timer.record_stage(
        super::ImportTimingStage::PersistenceLockWait,
        std::time::Duration::from_millis(5),
    );
    timer.record_stage(
        super::ImportTimingStage::LedgerAppend,
        std::time::Duration::from_millis(11),
    );

    let timing = timer.finish();
    assert_eq!(timing.spawn_blocking_wait_ms, 7);
    assert_eq!(timing.persistence_lock_wait_ms, 8);
    assert_eq!(timing.ledger_append_ms, 11);
    assert!(timing.total_ms >= timing.spawn_blocking_wait_ms);
    assert_eq!(timing.non_corpus_materialize_ms, 0);
    assert_eq!(timing.search_index_catch_up_ms, 0);
    assert_eq!(timing.audit_ms, 0);
    assert_eq!(timing.app_core_clone_ms, 0);
    assert_eq!(timing.publish_clone_ms, 0);
}

#[test]
fn observation_import_timing_log_declares_required_fields() {
    let fields = super::ObservationImportTimingLog::field_names();
    for required in [
        "schema_names",
        "subject_kinds",
        "bulk_operation_lock_wait_ms",
        "persistence_lock_wait_ms",
        "spawn_blocking_wait_ms",
        "ledger_append_ms",
        "non_corpus_materialize_ms",
        "non_corpus_materialize_mode",
        "non_corpus_classification",
        "full_rebuild_reason",
        "search_index_catch_up_ms",
        "audit_ms",
        "total_ms",
    ] {
        assert!(fields.contains(&required), "missing log field {required}");
    }
}

#[test]
fn communication_message_keeps_freshness_and_reply_projection_cp04() {
    let published = Utc::now();
    let mut observation = freshness_only_observation(
        "schema:gmail-message",
        "sys:gmail",
        "cp04-message",
        published,
    );
    observation.meta["communication_channel_id"] = serde_json::json!("chan:gmail");
    observation.meta["communication_sender_id"] = serde_json::json!("sender@example.test");
    observation.meta["communication_thread_ref"] = serde_json::json!("gmail:thread:cp04");
    observation.meta["communication"] = serde_json::json!({
        "reply_due_at": (published + chrono::Duration::hours(1)).to_rfc3339()
    });
    let snapshot = super::ProjectionSnapshot::build(
        vec![observation],
        vec![],
        vec![super::FreshnessThreshold {
            source_id: "chan:gmail".to_owned(),
            max_age_seconds: 300,
        }],
        vec![],
    )
    .unwrap();
    assert_eq!(snapshot.freshness.sources.len(), 1);
    assert_eq!(snapshot.reply_slo.rows.len(), 1);
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
    requested_privacy_keys: std::cell::RefCell<Vec<String>>,
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

    fn observations_for_privacy_key_page(
        &self,
        privacy_key: &str,
        after_append_seq: u64,
        limit: usize,
    ) -> Result<Vec<lethe_storage_api::StoredObservation>, SelfHostError> {
        self.requested_privacy_keys
            .borrow_mut()
            .push(privacy_key.to_owned());
        let mut page = self
            .observations
            .values()
            .filter(|stored| {
                stored.append_seq > after_append_seq
                    && lethe_core::domain::observation_privacy_keys(&stored.observation)
                        .contains(privacy_key)
            })
            .cloned()
            .collect::<Vec<_>>();
        page.sort_by_key(|stored| stored.append_seq);
        page.truncate(limit);
        Ok(page)
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
    let mut core = AppCore::from_materialized(
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
    let incremental_commit = super::apply_compact_incremental_delta(
        &mut core,
        &appended,
        final_stats,
        final_at,
        &TestComponentProjectionLookup::default(),
    )
    .unwrap();
    assert!(matches!(
        incremental_commit,
        lethe_storage_api::ProjectionItemCommit::Delta { .. }
    ));

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
        core.canonical_observation_fingerprint,
        full.canonical_observation_fingerprint
    );
    assert_eq!(
        core.snapshot.lineage.build_id,
        full.snapshot.lineage.build_id
    );
    assert_eq!(
        core.manifest_value().unwrap(),
        full.manifest_value().unwrap()
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
        client_ref: None,
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
        requested_privacy_keys: std::cell::RefCell::new(Vec::new()),
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
        "identity-node:00000000000000000003"
    );
    let mut core =
        AppCore::from_materialized(initial_materialized, vec![], vec![], vec![], vec![]).unwrap();
    let mut all = initial.clone();
    all.push(appended.clone());
    let lookup = component_projection_lookup(&all, incremental_rows.clone());
    let final_stats = lethe_storage_api::ObservationStats {
        count: 5,
        max_append_seq: 5,
    };

    let incremental_commit = super::apply_compact_incremental_delta(
        &mut core,
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
    } = &incremental_commit
    else {
        panic!("component re-projection must publish a delta");
    };
    assert!(
        !updates
            .iter()
            .any(|item| item.item_key == bridge_message_id)
    );
    assert!(!deletes.contains(&bridge_message_id));
    apply_projection_item_commit(&mut incremental_rows, &incremental_commit);
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
    assert_eq!(requested.len(), 1);
    assert_eq!(
        incremental_rows[&bridge_message_id].owner_key,
        "identity-node:00000000000000000003"
    );
    assert_eq!(incremental_rows, full_rows);
    assert_eq!(
        core.manifest_value().unwrap(),
        full.manifest_value().unwrap()
    );
    assert_eq!(
        serde_json::to_value(&core.compact_state).unwrap(),
        serde_json::to_value(&full.compact_state).unwrap()
    );
    assert_eq!(
        serde_json::to_value(&core.person_components).unwrap(),
        serde_json::to_value(&full.person_components).unwrap()
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
    let mut core =
        AppCore::from_materialized(initial_materialized, vec![], vec![], vec![], vec![]).unwrap();
    let mut all = initial.clone();
    all.push(appended.clone());
    let lookup = component_projection_lookup(&all, incremental_rows.clone());
    let final_stats = lethe_storage_api::ObservationStats {
        count: 4,
        max_append_seq: 4,
    };

    let incremental_commit = super::apply_compact_incremental_delta(
        &mut core,
        std::slice::from_ref(&appended),
        final_stats,
        final_at,
        &lookup,
    )
    .unwrap();
    apply_projection_item_commit(&mut incremental_rows, &incremental_commit);
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
    assert_eq!(requested.len(), 1);
    assert_eq!(core.person_message_count, 1);
    assert_eq!(incremental_rows, full_rows);
    assert_eq!(
        core.manifest_value().unwrap(),
        full.manifest_value().unwrap()
    );
    assert_eq!(
        serde_json::to_value(&core.compact_state).unwrap(),
        serde_json::to_value(&full.compact_state).unwrap()
    );
    assert_eq!(
        serde_json::to_value(&core.person_components).unwrap(),
        serde_json::to_value(&full.person_components).unwrap()
    );
}

#[test]
fn communication_incremental_consent_repulls_after_state_restore() {
    let initial_at = chrono::DateTime::parse_from_rfc3339("2026-07-12T00:00:00Z")
        .unwrap()
        .to_utc();
    let target = wave2_slack_observation(
        "U-A",
        "Affected",
        Some("person@example.test"),
        "1.000001",
        initial_at,
    );
    let root = std::env::temp_dir().join(format!(
        "lethe-communication-reconsent-storage-test-{}",
        uuid::Uuid::now_v7()
    ));
    let storage =
        SqlitePersistence::open(&root.join("lethe.sqlite3"), &root.join("blobs"), &[7; 32])
            .unwrap();
    storage.persist_observation(&target).unwrap();
    let initial_stats = lethe_storage_api::ObservationStats {
        count: 1,
        max_append_seq: 1,
    };
    let mut initial_materialized = super::MaterializedProjectionSnapshot::build_at(
        vec![target.clone()],
        vec![],
        vec![],
        vec![],
        initial_stats,
        initial_at,
    )
    .unwrap();
    initial_materialized.communication_projection = serde_json::from_value(
        serde_json::to_value(&initial_materialized.communication_projection).unwrap(),
    )
    .unwrap();
    let mut core =
        AppCore::from_materialized(initial_materialized, vec![], vec![], vec![], vec![]).unwrap();
    assert_eq!(core.communication_projection.len(), 1);

    let opt_out = component_consent_observation(
        "person@example.test",
        "opted_out",
        "restore-opt-out",
        initial_at + chrono::Duration::minutes(1),
    );
    let mut persisted_opt_out = opt_out.clone();
    persisted_opt_out.meta = serde_json::json!({
        "canonical_json": "{\"kind\":\"consent\"}",
        "source_container": "consent-test",
    });
    storage.persist_observation(&persisted_opt_out).unwrap();
    let lookup_after_opt_out = super::StorageComponentProjectionLookup { storage: &storage };
    let opt_out_stats = lethe_storage_api::ObservationStats {
        count: 2,
        max_append_seq: 2,
    };
    super::apply_compact_incremental_delta(
        &mut core,
        std::slice::from_ref(&opt_out),
        opt_out_stats,
        initial_at + chrono::Duration::minutes(1),
        &lookup_after_opt_out,
    )
    .unwrap();
    assert!(core.communication_projection.is_empty());

    let reconsent = component_consent_observation(
        "person@example.test",
        "unrestricted",
        "restore-reconsent",
        initial_at + chrono::Duration::minutes(2),
    );
    let mut persisted_reconsent = reconsent.clone();
    persisted_reconsent.meta = serde_json::json!({
        "canonical_json": "{\"kind\":\"consent\"}",
        "source_container": "consent-test",
    });
    storage.persist_observation(&persisted_reconsent).unwrap();
    let lookup_after_reconsent = super::StorageComponentProjectionLookup { storage: &storage };
    let reconsent_stats = lethe_storage_api::ObservationStats {
        count: 3,
        max_append_seq: 3,
    };
    super::apply_compact_incremental_delta(
        &mut core,
        std::slice::from_ref(&reconsent),
        reconsent_stats,
        initial_at + chrono::Duration::minutes(2),
        &lookup_after_reconsent,
    )
    .unwrap();
    assert_eq!(core.communication_projection.len(), 1);
    assert_eq!(
        storage
            .observations_for_privacy_key_page("person@example.test", 0, 1)
            .unwrap()
            .len(),
        1
    );
    drop(storage);
    let _ = std::fs::remove_dir_all(root);
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
            let incremental_commit =
                super::apply_compact_incremental_delta(&mut core, batch, stats, final_at, &lookup)
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
            apply_projection_item_commit(&mut rows, &incremental_commit);
        }

        assert_eq!(consumed, deltas.len());
        assert_eq!(rows, full_rows, "partition {partition:?}");
        assert_eq!(
            core.manifest_value().unwrap(),
            full.manifest_value().unwrap(),
            "partition {partition:?}"
        );
        assert_eq!(
            serde_json::to_value(&core.person_components).unwrap(),
            serde_json::to_value(&full.person_components).unwrap(),
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
    let first_draft_id = SupplementalId::new("sup:paged-reply-draft-first");
    let supplementals = vec![
        SupplementalRecord {
            id: first_draft_id.clone(),
            kind: "reply-draft@1".to_owned(),
            derived_from: InputAnchorSet {
                observations: vec![observations[0].id.clone()],
                blobs: Vec::new(),
                supplementals: Vec::new(),
            },
            payload: serde_json::json!({
                "channel": "slack",
                "recipient": "U01",
                "body": "first reply",
                "drafted_at": built_at - chrono::Duration::minutes(170),
            }),
            created_by: ActorRef::new("actor:test"),
            created_at: built_at - chrono::Duration::minutes(170),
            mutability: Mutability::AppendOnly,
            record_version: None,
            model_version: None,
            consent_metadata: None,
            lineage: None,
        },
        SupplementalRecord {
            id: SupplementalId::new("sup:paged-reply-draft-third"),
            kind: "reply-draft@1".to_owned(),
            derived_from: InputAnchorSet {
                observations: vec![observations[2].id.clone()],
                blobs: Vec::new(),
                supplementals: Vec::new(),
            },
            payload: serde_json::json!({
                "channel": "slack",
                "recipient": "U02",
                "body": "third reply",
                "drafted_at": built_at - chrono::Duration::minutes(50),
            }),
            created_by: ActorRef::new("actor:test"),
            created_at: built_at - chrono::Duration::minutes(50),
            mutability: Mutability::AppendOnly,
            record_version: None,
            model_version: None,
            consent_metadata: None,
            lineage: None,
        },
        SupplementalRecord {
            id: SupplementalId::new("sup:paged-send-later"),
            kind: "send-record@1".to_owned(),
            derived_from: InputAnchorSet {
                observations: Vec::new(),
                blobs: Vec::new(),
                supplementals: vec![first_draft_id.clone()],
            },
            payload: serde_json::json!({
                "channel": "slack",
                "sent_at": built_at - chrono::Duration::minutes(140),
                "mode": "automatic",
            }),
            created_by: ActorRef::new("actor:test"),
            created_at: built_at - chrono::Duration::minutes(140),
            mutability: Mutability::AppendOnly,
            record_version: None,
            model_version: None,
            consent_metadata: None,
            lineage: None,
        },
        SupplementalRecord {
            id: SupplementalId::new("sup:paged-send-earliest"),
            kind: "send-record@1".to_owned(),
            derived_from: InputAnchorSet {
                observations: Vec::new(),
                blobs: Vec::new(),
                supplementals: vec![first_draft_id],
            },
            payload: serde_json::json!({
                "channel": "slack",
                "sent_at": built_at - chrono::Duration::minutes(150),
                "mode": "automatic",
            }),
            created_by: ActorRef::new("actor:test"),
            created_at: built_at - chrono::Duration::minutes(130),
            mutability: Mutability::AppendOnly,
            record_version: None,
            model_version: None,
            consent_metadata: None,
            lineage: None,
        },
    ];
    let full = super::MaterializedProjectionSnapshot::build_at(
        observations,
        supplementals.clone(),
        thresholds.clone(),
        vec![],
        stats,
        built_at,
    )
    .unwrap();
    let mut expected_rows = std::collections::BTreeMap::new();
    apply_projection_item_commit(&mut expected_rows, pending_projection_item_commit(&full));
    let expected_manifest = full.manifest_value().unwrap();

    for page_size in [1, 128] {
        let paged = super::rebuild_materialized_snapshot_paged(
            &persistence,
            &supplementals,
            &thresholds,
            &[],
            stats,
            page_size,
            built_at,
        )
        .unwrap();
        assert_eq!(paged.manifest_value().unwrap(), expected_manifest);
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
    assert_eq!(persisted_manifest, full.manifest_value().unwrap());
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
    let draft_id = SupplementalId::new("sup:incremental-reply-draft");
    let supplementals = vec![
        SupplementalRecord {
            id: draft_id.clone(),
            kind: "reply-draft@1".to_owned(),
            derived_from: InputAnchorSet {
                observations: vec![initial[0].id.clone()],
                blobs: Vec::new(),
                supplementals: Vec::new(),
            },
            payload: serde_json::json!({
                "channel": "slack",
                "recipient": "U01",
                "body": "indexed reply",
                "drafted_at": initial_at + chrono::Duration::minutes(5),
            }),
            created_by: ActorRef::new("actor:test"),
            created_at: initial_at + chrono::Duration::minutes(5),
            mutability: Mutability::AppendOnly,
            record_version: None,
            model_version: None,
            consent_metadata: None,
            lineage: None,
        },
        SupplementalRecord {
            id: SupplementalId::new("sup:incremental-send-record"),
            kind: "send-record@1".to_owned(),
            derived_from: InputAnchorSet {
                observations: Vec::new(),
                blobs: Vec::new(),
                supplementals: vec![draft_id],
            },
            payload: serde_json::json!({
                "channel": "slack",
                "sent_at": initial_at + chrono::Duration::minutes(10),
                "mode": "automatic",
            }),
            created_by: ActorRef::new("actor:test"),
            created_at: initial_at + chrono::Duration::minutes(10),
            mutability: Mutability::AppendOnly,
            record_version: None,
            model_version: None,
            consent_metadata: None,
            lineage: None,
        },
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
        super::classify_non_corpus_delta_with_reason(&appended).kind,
        super::NonCorpusDeltaKind::Communication
    );
    let initial_materialized = super::MaterializedProjectionSnapshot::build_at(
        initial.clone(),
        supplementals.clone(),
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
    let mut core = AppCore::from_materialized(
        initial_materialized,
        vec![],
        supplementals.clone(),
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
    let incremental_commit = super::apply_compact_incremental_delta(
        &mut core,
        &appended,
        final_stats,
        final_at,
        &lookup,
    )
    .unwrap();
    let incremental_reply_keys = match &incremental_commit {
        lethe_storage_api::ProjectionItemCommit::Delta {
            inserts,
            updates,
            deletes,
        } => {
            assert!(
                updates
                    .iter()
                    .all(|item| item.owner_key != super::REPLY_SLO_ITEM_OWNER)
            );
            assert!(deletes.is_empty());
            inserts
                .iter()
                .filter(|item| item.owner_key == super::REPLY_SLO_ITEM_OWNER)
                .map(|item| item.item_key.clone())
                .collect::<std::collections::BTreeSet<_>>()
        }
        lethe_storage_api::ProjectionItemCommit::Replace { .. } => {
            panic!("observation delta unexpectedly replaced all ReplySLO rows")
        }
    };
    assert_eq!(
        incremental_reply_keys,
        appended
            .iter()
            .map(|observation| format!("reply-slo:{}", observation.id))
            .collect()
    );
    apply_projection_item_commit(&mut incremental_message_rows, &incremental_commit);

    let mut all = initial;
    all.extend(appended);
    let full = super::MaterializedProjectionSnapshot::build_at(
        all,
        supplementals,
        thresholds,
        vec![],
        final_stats,
        final_at,
    )
    .unwrap();

    assert_eq!(core.person_components.len(), 3);
    assert!(core.snapshot.person_page.messages.is_empty());
    assert_eq!(core.person_message_count, 4);
    assert_eq!(core.reply_slo_count, 4);
    assert!(core.snapshot.reply_slo.rows.is_empty());
    assert!(core.snapshot.reply_slo.overdue.is_empty());
    assert_eq!(core.compact_state.identity.nodes().len(), 3);
    let mut full_message_rows = std::collections::BTreeMap::new();
    apply_projection_item_commit(
        &mut full_message_rows,
        pending_projection_item_commit(&full),
    );
    assert_eq!(incremental_message_rows, full_message_rows);
    assert_eq!(
        core.manifest_value().unwrap(),
        full.manifest_value().unwrap()
    );
    assert_eq!(
        serde_json::to_value(&core.person_components).unwrap(),
        serde_json::to_value(&full.person_components).unwrap()
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
        client_ref: None,
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

fn v2_slack_draft(index: usize, published: chrono::DateTime<Utc>) -> ObservationDraft {
    let mut draft = wave2_slack_draft(index);
    let key = format!("{index:010}.000001");
    let object_id = format!("channel:C01ABC:ts:{key}");
    let canonical_json = draft
        .meta
        .get("canonical_json")
        .and_then(serde_json::Value::as_str)
        .expect("wave2 fixture must contain canonical_json")
        .to_owned();
    let meta = draft.meta.as_object_mut().unwrap();
    meta.insert("object_id".to_owned(), serde_json::json!(object_id));
    draft.idempotency_key = identity_key("slack-test", &object_id, &canonical_json);
    draft.published = published;
    draft.client_ref = Some(format!("client-{index}"));
    draft
}

fn consent_decision_draft(key: &str, identifier: &str, status: &str) -> ObservationDraft {
    ObservationDraft {
        schema: SchemaRef::new("schema:consent-decision"),
        schema_version: SemVer::new("1.0.0"),
        observer: ObserverRef::new("obs:consent-ledger"),
        source_system: Some(SourceSystemRef::new("sys:lethe-governance")),
        authority_model: AuthorityModel::LakeAuthoritative,
        capture_model: CaptureModel::Event,
        subject: EntityRef::new(format!("person:{key}")),
        target: None,
        payload: serde_json::json!({
            "status": status,
            "identifier": identifier,
        }),
        attachments: vec![],
        published: Utc::now(),
        idempotency_key: IdempotencyKey::new(format!("consent:{key}:{status}")),
        client_ref: None,
        meta: serde_json::json!({
            "canonical_json": serde_json::json!({
                "identifier": identifier,
                "status": status,
            }).to_string(),
            "source_container": "consent-test",
        }),
    }
}

#[test]
fn v2_import_returns_per_item_results_and_keeps_valid_items_on_quarantine() {
    let root = std::env::temp_dir().join(format!("lethe-v2-import-{}", uuid::Uuid::now_v7()));
    let db = root.join("lethe.sqlite3");
    let blobs = root.join("blobs");
    let persistence = SqlitePersistence::open(&db, &blobs, &[7; 32]).unwrap();
    let service = test_service(test_config(db, blobs), persistence);
    let now = Utc::now();
    let mut drafts = (0..9)
        .map(|index| v2_slack_draft(index, now - chrono::Duration::minutes(1)))
        .collect::<Vec<_>>();
    drafts.push(v2_slack_draft(9, now + chrono::Duration::minutes(11)));

    let report = service
        .ingest_observation_drafts_v2(drafts, "slack-test")
        .unwrap();

    assert_eq!(report.results.len(), 10);
    assert_eq!(report.ingested, 9);
    assert_eq!(report.quarantined, 1);
    assert_eq!(report.rejected, 0);
    assert!(
        report
            .results
            .iter()
            .take(9)
            .all(|result| result.outcome == ImportOutcome::Ingested)
    );
    assert_eq!(report.results[9].outcome, ImportOutcome::Quarantined);
    assert_eq!(
        report.results[9].error_code.as_deref(),
        Some("clock_skew_future")
    );
    assert!(report.results[9].ticket.is_some());
    assert_eq!(
        service
            .persistence_lock()
            .unwrap()
            .observation_stats()
            .unwrap()
            .count,
        9
    );

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn v2_identity_is_server_validated_and_retry_time_does_not_change_identity() {
    let root = std::env::temp_dir().join(format!("lethe-v2-identity-{}", uuid::Uuid::now_v7()));
    let db = root.join("lethe.sqlite3");
    let blobs = root.join("blobs");
    let persistence = SqlitePersistence::open(&db, &blobs, &[7; 32]).unwrap();
    let service = test_service(test_config(db, blobs), persistence);
    let first = v2_slack_draft(10, Utc::now() - chrono::Duration::minutes(2));
    let retry = v2_slack_draft(10, Utc::now() - chrono::Duration::minutes(1));

    let report = service
        .ingest_observation_drafts_v2(vec![first, retry], "slack-test")
        .unwrap();

    assert_eq!(report.results[0].outcome, ImportOutcome::Ingested);
    assert_eq!(report.results[1].outcome, ImportOutcome::Duplicate);
    assert_eq!(
        report.results[0].observation_id,
        report.results[1].existing_id
    );
    assert_eq!(report.summary.ingested, 1);
    assert_eq!(report.summary.duplicates, 1);

    let mut mismatched = v2_slack_draft(11, Utc::now() - chrono::Duration::minutes(1));
    mismatched.idempotency_key = IdempotencyKey::new("client-controlled-key");
    let report = service
        .ingest_observation_drafts_v2(vec![mismatched], "slack-test")
        .unwrap();
    assert_eq!(report.results[0].outcome, ImportOutcome::Rejected);
    assert_eq!(
        report.results[0].error_code.as_deref(),
        Some("identity_mismatch")
    );

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn v1_append_then_v2_canary_uses_bridge_duplicate_without_ledger_delta() {
    let root = std::env::temp_dir().join(format!("lethe-v1-v2-bridge-{}", uuid::Uuid::now_v7()));
    let db = root.join("lethe.sqlite3");
    let blobs = root.join("blobs");
    let persistence = SqlitePersistence::open(&db, &blobs, &[7; 32]).unwrap();
    let service = test_service(test_config(db, blobs), persistence);

    let mut v1_draft = wave2_slack_draft(42);
    let object_id = "channel:C01ABC:ts:0000000042.000001";
    v1_draft
        .meta
        .as_object_mut()
        .unwrap()
        .insert("object_id".to_owned(), serde_json::json!(object_id));
    let v1 = service
        .ingest_observation_drafts(vec![v1_draft], "slack-test")
        .unwrap();
    let existing_id = v1.results[0]
        .observation_id
        .clone()
        .expect("v1 canary must append");
    service.apply_identity_bridge_batch(32).unwrap();

    let v2 = service
        .ingest_observation_drafts_v2(vec![v2_slack_draft(42, Utc::now())], "slack-test")
        .unwrap();
    assert_eq!(v2.ingested, 0);
    assert_eq!(v2.duplicates, 1);
    assert_eq!(v2.results[0].existing_id, Some(existing_id));
    assert_eq!(
        v2.results[0].error_code.as_deref(),
        Some("duplicate.existing_id")
    );
    assert_eq!(
        service
            .persistence_lock()
            .unwrap()
            .observation_stats()
            .unwrap()
            .count,
        1
    );

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn v2_payload_limit_is_an_item_error_with_actual_and_maximum() {
    let root =
        std::env::temp_dir().join(format!("lethe-v2-payload-limit-{}", uuid::Uuid::now_v7()));
    let db = root.join("lethe.sqlite3");
    let blobs = root.join("blobs");
    let persistence = SqlitePersistence::open(&db, &blobs, &[7; 32]).unwrap();
    let mut config = test_config(db, blobs);
    config.resource_limits.max_payload_bytes = 1;
    let service = test_service(config, persistence);

    let report = service
        .ingest_observation_drafts_v2(
            vec![v2_slack_draft(0, Utc::now() - chrono::Duration::minutes(1))],
            "slack-test",
        )
        .unwrap();

    assert_eq!(report.ingested, 0);
    assert_eq!(report.rejected, 1);
    assert_eq!(
        report.results[0].error_code.as_deref(),
        Some("payload_too_large")
    );
    assert_eq!(
        report.results[0]
            .details
            .as_ref()
            .and_then(|details| details.get("max_bytes"))
            .and_then(serde_json::Value::as_u64),
        Some(1)
    );

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn v2_transient_failures_are_machine_classified() {
    let result = super::transient_item(
        "client-transient".to_owned(),
        "storage temporarily unavailable".to_owned(),
        serde_json::json!({"stage": "durable_append"}),
    );

    assert_eq!(result.outcome, ImportOutcome::Rejected);
    assert_eq!(
        result.failure_class,
        Some(super::ImportFailureClass::Transient)
    );
    assert_eq!(result.error_code.as_deref(), Some("transient_failure"));
    assert_eq!(
        super::error_code_for_failure(lethe_core::domain::FailureClass::RetryableEffectFailure),
        "transient_failure"
    );
}

fn normalized_non_corpus_manifest(mut value: serde_json::Value) -> serde_json::Value {
    value
        .pointer_mut("/snapshot")
        .and_then(serde_json::Value::as_object_mut)
        .unwrap()
        .remove("built_at");
    value
        .pointer_mut("/snapshot/lineage")
        .and_then(serde_json::Value::as_object_mut)
        .unwrap()
        .remove("built_at");
    for collection in ["sources", "missing"] {
        for source in value
            .pointer_mut(&format!("/snapshot/freshness/{collection}"))
            .and_then(serde_json::Value::as_array_mut)
            .unwrap()
        {
            source.as_object_mut().unwrap().remove("age_seconds");
        }
    }
    value
}

#[test]
fn bulk_duplicate_only_session_is_a_true_no_op() {
    let root = std::env::temp_dir().join(format!(
        "lethe-bulk-duplicate-only-test-{}",
        uuid::Uuid::now_v7()
    ));
    let db = root.join("lethe.sqlite3");
    let blobs = root.join("blobs");
    let persistence = SqlitePersistence::open(&db, &blobs, &[7; 32]).unwrap();
    let service = test_service(test_config(db, blobs), persistence);

    let mut seed = wave2_slack_draft(0);
    seed.meta.as_object_mut().unwrap().insert(
        "object_id".to_owned(),
        serde_json::json!("channel:C01ABC:ts:0000000000.000001"),
    );
    service
        .ingest_observation_drafts(vec![seed], "slack-test")
        .unwrap();
    service.apply_identity_bridge_batch(32).unwrap();
    wait_for_append_consumer(&service);
    let session = service.begin_bulk_import_session().unwrap();
    let before_snapshot = service.core_snapshot();
    let before_publish_count = service.publish_count();
    let before_rebuild_count = service
        .non_corpus_rebuild_count
        .load(std::sync::atomic::Ordering::Relaxed);
    let before_audit_count = service
        .persistence_lock()
        .unwrap()
        .audit_event_page(None, 1_000)
        .unwrap()
        .len();

    for index in 0..20 {
        let report = if index % 2 == 0 {
            service
                .ingest_observation_drafts_with_session(
                    vec![wave2_slack_draft(0)],
                    "slack-test",
                    Some(&session.session_id),
                )
                .unwrap()
        } else {
            service
                .ingest_observation_drafts_v2_with_session(
                    vec![v2_slack_draft(0, Utc::now())],
                    "slack-test",
                    Some(&session.session_id),
                )
                .unwrap()
        };
        assert_eq!(report.ingested, 0);
        assert_eq!(report.duplicates, 1);
    }

    // Each of the 20 duplicate-only requests still records its own audit
    // event even though the batch is a true no-op for the projection state;
    // check this before ending the session, since session-end itself emits
    // one more audit event of its own.
    assert_eq!(
        service
            .persistence_lock()
            .unwrap()
            .audit_event_page(None, 1_000)
            .unwrap()
            .len(),
        before_audit_count + 20,
        "dup-only bulk requests must each retain an audit event"
    );

    let completed = service
        .end_bulk_import_session(&session.session_id)
        .unwrap();
    assert_eq!(completed.state, super::BulkImportSessionPhase::Ready);
    assert_eq!(completed.base_append_seq, completed.target_append_seq);
    assert!(Arc::ptr_eq(&before_snapshot, &service.core_snapshot()));
    assert_eq!(service.publish_count(), before_publish_count);
    assert_eq!(
        service
            .non_corpus_rebuild_count
            .load(std::sync::atomic::Ordering::Relaxed),
        before_rebuild_count
    );
    eprintln!(
        "bulk dup-only 20 requests: publish_count={} rebuild_count={} state=ready",
        service.publish_count(),
        service
            .non_corpus_rebuild_count
            .load(std::sync::atomic::Ordering::Relaxed)
    );

    drop(before_snapshot);
    drop(service);
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn bulk_session_consent_observation_after_first_append_triggers_minimal_publish() {
    let root = std::env::temp_dir().join(format!(
        "lethe-bulk-consent-minimal-publish-test-{}",
        uuid::Uuid::now_v7()
    ));
    let db = root.join("lethe.sqlite3");
    let blobs = root.join("blobs");
    let persistence = SqlitePersistence::open(&db, &blobs, &[7; 32]).unwrap();
    let service = test_service(test_config(db, blobs), persistence);
    wait_for_search_index_ready(&service);

    let session = service.begin_bulk_import_session().unwrap();

    // The very first append in a session always publishes (first_append
    // branch), independent of consent content. Get that out of the way so
    // the subsequent assertion isolates the `contains_consent` branch.
    let first_report = service
        .ingest_observation_drafts_with_session(
            vec![wave2_slack_draft(0)],
            "slack-test",
            Some(&session.session_id),
        )
        .unwrap();
    assert_eq!(first_report.ingested, 1);

    let publish_count_before_consent = service.publish_count();
    let rebuild_count_before_consent = service
        .non_corpus_rebuild_count
        .load(std::sync::atomic::Ordering::Relaxed);

    let subject = EntityRef::new("person:bulk-consent-subject");
    let identifier = "bulk-consent-identifier";
    let consent_report = service
        .ingest_observation_drafts_with_session(
            vec![consent_decision_draft(
                "bulk-consent-subject",
                identifier,
                "opted_out",
            )],
            "slack-test",
            Some(&session.session_id),
        )
        .unwrap();
    assert_eq!(consent_report.ingested, 1);

    // `materialize_bulk_import_append` takes the `contains_consent` branch
    // here (first_append is now false), which must still capture the
    // consent decision and publish a minimal snapshot immediately rather
    // than deferring it to session end.
    assert_eq!(
        service.publish_count(),
        publish_count_before_consent + 1,
        "consent observation mid-session must trigger a minimal publish"
    );
    assert_eq!(
        service
            .non_corpus_rebuild_count
            .load(std::sync::atomic::Ordering::Relaxed),
        rebuild_count_before_consent,
        "minimal consent publish must not trigger a background non-corpus rebuild"
    );
    assert_eq!(
        service
            .core_snapshot()
            .compact_state
            .resolve(&subject, &[identifier.to_owned()], None),
        ConsentStatus::OptedOut,
        "published snapshot must reflect the captured consent decision"
    );

    wait_for_search_index_high_water(&service, 2);
    let completed = service
        .end_bulk_import_session(&session.session_id)
        .unwrap();
    assert_eq!(completed.state, super::BulkImportSessionPhase::Ready);

    drop(service);
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn bulk_import_session_defers_non_corpus_keeps_corpus_ready_and_rebuilds_once() {
    let bulk_root =
        std::env::temp_dir().join(format!("lethe-bulk-session-test-{}", uuid::Uuid::now_v7()));
    let bulk_db = bulk_root.join("lethe.sqlite3");
    let bulk_blobs = bulk_root.join("blobs");
    let bulk_persistence = SqlitePersistence::open(&bulk_db, &bulk_blobs, &[7; 32]).unwrap();
    let mut bulk_config = test_config(bulk_db, bulk_blobs);
    bulk_config.corpus.mode = lethe_projection_corpus::CorpusMode::PersonalAllText;
    let bulk_service = test_service(bulk_config, bulk_persistence);
    wait_for_search_index_ready(&bulk_service);

    let session = bulk_service.begin_bulk_import_session().unwrap();
    assert_eq!(session.state, super::BulkImportSessionPhase::Deferred);
    let publish_count_before_appends = bulk_service.publish_count();
    let health = bulk_service.health().unwrap();
    let bulk_dependency = health
        .dependencies
        .iter()
        .find(|dependency| dependency.name == "bulk_import_session")
        .unwrap();
    assert_eq!(health.status, "degraded");
    assert_eq!(bulk_dependency.status, "deferred");
    assert!(
        bulk_dependency
            .detail
            .as_deref()
            .unwrap()
            .contains(&session.session_id)
    );

    let missing_session_error = bulk_service
        .ingest_observation_drafts(vec![wave2_slack_draft(99)], "slack-test")
        .unwrap_err();
    assert!(matches!(
        missing_session_error,
        SelfHostError::BulkImportSessionConflict {
            code: "bulk_import_session_id_required",
            ..
        }
    ));
    assert_eq!(
        bulk_service
            .persistence_lock()
            .unwrap()
            .observation_stats()
            .unwrap()
            .count,
        0
    );

    for indices in [[0usize, 1usize], [2usize, 3usize]] {
        let report = bulk_service
            .ingest_observation_drafts_with_session(
                indices.into_iter().map(wave2_slack_draft).collect(),
                "slack-test",
                Some(&session.session_id),
            )
            .unwrap();
        assert_eq!(report.ingested, 2);
        assert_eq!(
            bulk_service
                .non_corpus_rebuild_count
                .load(std::sync::atomic::Ordering::Relaxed),
            0
        );
    }

    assert_eq!(
        bulk_service.publish_count(),
        publish_count_before_appends + 1,
        "bulk appends publish stale state only at the first actual append"
    );

    assert_eq!(bulk_service.core_lock().unwrap().observation_stats.count, 0);
    assert!(matches!(
        bulk_service.persons_response(
            None,
            None,
            &lethe_api::api::pagination::PaginationParams::default(),
        ),
        Err(SelfHostError::ProjectionStale(_))
    ));
    wait_for_search_index_high_water(&bulk_service, 4);
    let grep = bulk_service
        .corpus_grep_response(&lethe_api::api::grep::GrepRequest {
            pattern: "Wave2 bulk message 3".to_owned(),
            limit: Some(10),
            ..lethe_api::api::grep::GrepRequest::default()
        })
        .unwrap();
    assert_eq!(grep.data.matches.len(), 1);

    let completed = bulk_service
        .end_bulk_import_session(&session.session_id)
        .unwrap();
    assert_eq!(completed.state, super::BulkImportSessionPhase::Ready);
    assert_eq!(completed.target_append_seq, 4);
    assert_eq!(completed.target_observation_count, 4);
    assert_eq!(
        bulk_service
            .non_corpus_rebuild_count
            .load(std::sync::atomic::Ordering::Relaxed),
        1
    );
    assert!(bulk_service.publish_count() <= publish_count_before_appends + 2);
    assert_eq!(
        bulk_service
            .persons_response(
                None,
                None,
                &lethe_api::api::pagination::PaginationParams::default(),
            )
            .unwrap()
            .data["total"],
        4
    );

    let retried = bulk_service
        .end_bulk_import_session(&session.session_id)
        .unwrap();
    assert_eq!(retried.target_append_seq, completed.target_append_seq);
    assert_eq!(
        bulk_service
            .non_corpus_rebuild_count
            .load(std::sync::atomic::Ordering::Relaxed),
        1,
        "successful end retry must not rebuild again"
    );

    let observations = bulk_service
        .persistence_lock()
        .unwrap()
        .observation_page(0, 100)
        .unwrap()
        .into_iter()
        .map(|stored| stored.observation)
        .collect::<Vec<_>>();

    let reference_root = std::env::temp_dir().join(format!(
        "lethe-bulk-reference-test-{}",
        uuid::Uuid::now_v7()
    ));
    let reference_db = reference_root.join("lethe.sqlite3");
    let reference_blobs = reference_root.join("blobs");
    let reference_persistence =
        SqlitePersistence::open(&reference_db, &reference_blobs, &[7; 32]).unwrap();
    let mut reference_config = test_config(reference_db, reference_blobs);
    reference_config.corpus.mode = lethe_projection_corpus::CorpusMode::PersonalAllText;
    let reference_service = test_service(reference_config, reference_persistence);
    for observation in &observations {
        reference_service
            .persistence_lock()
            .unwrap()
            .append_observation(observation)
            .unwrap();
    }
    reference_service.trigger_append_consumer();
    wait_for_append_consumer(&reference_service);

    let bulk_manifest = normalized_non_corpus_manifest(
        bulk_service
            .persistence_lock()
            .unwrap()
            .projection_records(&ProjectionRef::new("proj:person-page"))
            .unwrap()
            .unwrap(),
    );
    let reference_manifest = normalized_non_corpus_manifest(
        reference_service
            .persistence_lock()
            .unwrap()
            .projection_records(&ProjectionRef::new("proj:person-page"))
            .unwrap()
            .unwrap(),
    );
    assert_eq!(bulk_manifest, reference_manifest);

    let mut owners = bulk_service
        .core_lock()
        .unwrap()
        .snapshot
        .person_page
        .profiles
        .iter()
        .map(|profile| profile.person_id.as_str().to_owned())
        .collect::<std::collections::BTreeSet<_>>();
    owners.insert(super::REPLY_SLO_ITEM_OWNER.to_owned());
    for owner in owners {
        let bulk_items = bulk_service
            .persistence_lock()
            .unwrap()
            .projection_items_by_owner(&ProjectionRef::new("proj:person-page"), &owner)
            .unwrap();
        let reference_items = reference_service
            .persistence_lock()
            .unwrap()
            .projection_items_by_owner(&ProjectionRef::new("proj:person-page"), &owner)
            .unwrap();
        assert_eq!(bulk_items, reference_items, "owner {owner} differs");
    }

    drop(reference_service);
    drop(bulk_service);
    let _ = std::fs::remove_dir_all(reference_root);
    let _ = std::fs::remove_dir_all(bulk_root);
}

#[test]
fn bulk_first_append_stale_publication_waits_for_derived_projection_lane() {
    fn assert_waits_for_lane<F, T>(service: &AppService, operation: F) -> T
    where
        F: FnOnce(AppService) -> T + Send + 'static,
        T: Send + 'static,
    {
        let lane = service.derived_projection_lane.lock().unwrap();
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let worker_service = service.clone();
        let worker = std::thread::spawn(move || {
            started_tx.send(()).unwrap();
            done_tx.send(operation(worker_service)).unwrap();
        });
        started_rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .expect("lane test worker did not start");

        let early_result = done_rx.recv_timeout(std::time::Duration::from_millis(100));
        let completed_while_lane_held = early_result.is_ok();
        drop(lane);
        let result = match early_result {
            Ok(result) => result,
            Err(_) => done_rx
                .recv_timeout(std::time::Duration::from_secs(5))
                .expect("lane test worker did not finish after lane release"),
        };
        worker.join().unwrap();
        assert!(
            !completed_while_lane_held,
            "stale publication completed while derived lane was held"
        );
        result
    }

    let root = std::env::temp_dir().join(format!(
        "lethe-derived-lane-stale-test-{}",
        uuid::Uuid::now_v7()
    ));
    let db = root.join("lethe.sqlite3");
    let blobs = root.join("blobs");
    let persistence = SqlitePersistence::open(&db, &blobs, &[7; 32]).unwrap();
    let service = test_service(test_config(db, blobs), persistence);

    let lane = service.derived_projection_lane.lock().unwrap();
    let session = service.begin_bulk_import_session().unwrap();
    drop(lane);
    let legacy_session_id = session.session_id.clone();
    assert_waits_for_lane(&service, move |service| {
        service.ingest_observation_drafts_with_session(
            vec![wave2_slack_draft(0)],
            "slack-test",
            Some(&legacy_session_id),
        )
    })
    .unwrap();
    service
        .ingest_observation_drafts_v2_with_session(
            vec![v2_slack_draft(1, Utc::now())],
            "slack-test",
            Some(&session.session_id),
        )
        .unwrap();

    drop(service);
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn active_bulk_session_rejects_supplemental_write_and_source_sync_without_advancing_state() {
    let root = std::env::temp_dir().join(format!(
        "lethe-bulk-session-guard-test-{}",
        uuid::Uuid::now_v7()
    ));
    let db = root.join("lethe.sqlite3");
    let blobs = root.join("blobs");
    let persistence = SqlitePersistence::open(&db, &blobs, &[7; 32]).unwrap();
    let mut config = test_config(db, blobs);
    config.corpus.mode = lethe_projection_corpus::CorpusMode::PersonalAllText;
    let service = test_service(config, persistence);
    wait_for_search_index_ready(&service);

    let session = service.begin_bulk_import_session().unwrap();
    let supplemental_id = SupplementalId::new("sup:00000000-0000-7000-8000-000000000301");
    let (before_stats, before_catalog_high_water) = {
        let persistence = service.persistence_lock().unwrap();
        (
            persistence.observation_stats().unwrap(),
            persistence.slack_thread_discovery_high_water().unwrap(),
        )
    };

    let supplemental_error = service
        .write_supplemental(super::SupplementalWriteRequest {
            id: supplemental_id.clone(),
            kind: "reply-draft@1".to_owned(),
            derived_from: InputAnchorSet::default(),
            payload: serde_json::json!({
                "channel": "slack",
                "recipient": "U01",
                "body": "deferred session must reject this"
            }),
            created_by: ActorRef::new("actor:test"),
            mutability: Mutability::AppendOnly,
            model_version: None,
            consent_metadata: None,
            lineage: None,
        })
        .unwrap_err();
    assert!(matches!(
        supplemental_error,
        SelfHostError::BulkImportSessionConflict {
            code: "bulk_import_session_active",
            ..
        }
    ));

    let sync_error = service.sync_all().unwrap_err();
    assert!(matches!(
        sync_error,
        SelfHostError::BulkImportSessionConflict {
            code: "bulk_import_session_active",
            ..
        }
    ));

    let persistence = service.persistence_lock().unwrap();
    let after_stats = persistence.observation_stats().unwrap();
    assert_eq!(after_stats.count, before_stats.count);
    assert_eq!(after_stats.max_append_seq, before_stats.max_append_seq);
    assert_eq!(
        persistence.slack_thread_discovery_high_water().unwrap(),
        before_catalog_high_water
    );
    assert!(
        persistence
            .supplemental_by_id(&supplemental_id)
            .unwrap()
            .is_none()
    );
    drop(persistence);
    assert_eq!(
        service
            .end_bulk_import_session(&session.session_id)
            .unwrap()
            .state,
        super::BulkImportSessionPhase::Ready
    );

    drop(service);
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn bootstrap_recovers_abandoned_bulk_import_session() {
    let root = std::env::temp_dir().join(format!(
        "lethe-bulk-session-recovery-test-{}",
        uuid::Uuid::now_v7()
    ));
    let db = root.join("lethe.sqlite3");
    let blobs = root.join("blobs");
    let persistence = SqlitePersistence::open(&db, &blobs, &[7; 32]).unwrap();
    let mut config = test_config(db, blobs);
    config.corpus.mode = lethe_projection_corpus::CorpusMode::PersonalAllText;
    let service = test_service(config.clone(), persistence);
    wait_for_search_index_ready(&service);

    let session = service.begin_bulk_import_session().unwrap();
    service
        .ingest_observation_drafts_with_session(
            vec![wave2_slack_draft(7)],
            "slack-test",
            Some(&session.session_id),
        )
        .unwrap();
    assert!(matches!(
        service.persons_response(
            None,
            None,
            &lethe_api::api::pagination::PaginationParams::default(),
        ),
        Err(SelfHostError::ProjectionStale(_))
    ));
    wait_for_search_index_high_water(&service, 1);
    drop(service);

    let restarted = AppService::bootstrap(config).unwrap();
    let recovered = restarted
        .end_bulk_import_session(&session.session_id)
        .unwrap();
    assert_eq!(recovered.state, super::BulkImportSessionPhase::Ready);
    assert_eq!(recovered.target_append_seq, 1);
    assert_eq!(
        restarted
            .persons_response(
                None,
                None,
                &lethe_api::api::pagination::PaginationParams::default(),
            )
            .unwrap()
            .data["total"],
        1
    );
    assert_eq!(
        restarted
            .persistence_lock()
            .unwrap()
            .projection_item_count(&ProjectionRef::new("proj:person-page"))
            .unwrap(),
        4
    );

    drop(restarted);
    let _ = std::fs::remove_dir_all(root);
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
    wait_for_append_consumer(&service);
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
    wait_for_append_consumer(&service);
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
    assert_eq!(before_item_count, 4);
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
fn person_message_item_sql_failure_marks_in_memory_projection_stale() {
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

    let report = service
        .ingest_observation_drafts(vec![wave2_slack_draft(1)], "slack-test")
        .unwrap();
    assert_eq!(report.ingested, 1);
    wait_for_append_consumer_stopped(&service);
    assert!(
        service
            .append_consumer_error
            .lock()
            .unwrap()
            .as_deref()
            .is_some()
    );
    let core = service.core_lock().unwrap();
    assert_eq!(core.observation_stats.count, 0);
    assert_eq!(core.person_message_count, 0);
    assert_eq!(core.reply_slo_count, 0);
    assert_eq!(
        core.catalog
            .get(&ProjectionRef::new("proj:person-page"))
            .unwrap()
            .status,
        lethe_core::domain::ProjectionStatus::Stale
    );
    assert!(core.snapshot.person_page.profiles.is_empty());
    assert!(core.snapshot.person_page.messages.is_empty());
    assert!(core.snapshot.reply_slo.rows.is_empty());
    drop(core);
    let persistence = service.persistence_lock().unwrap();
    assert_eq!(persistence.observation_stats().unwrap().count, 1);
    assert!(
        persistence
            .audit_event_page(None, 100)
            .unwrap()
            .iter()
            .any(|event| event.event_json.contains("projection_consumer_failure"))
    );
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
fn read_lane_does_not_wait_for_writer_lane() {
    let root = std::env::temp_dir().join(format!("lethe-self-host-test-{}", uuid::Uuid::now_v7()));
    let db = root.join("lethe.sqlite3");
    let blobs = root.join("blobs");
    let persistence = SqlitePersistence::open(&db, &blobs, &[7; 32]).unwrap();
    let config = test_config(db, blobs);
    let service = test_service(config, persistence);
    let writer_guard = service.persistence_lock().unwrap();
    let read_lane = Arc::clone(&service.persistence_read_pool[0]);
    let (sender, receiver) = std::sync::mpsc::channel();
    let reader = std::thread::spawn(move || {
        let result = read_lane.lock().unwrap().observation_stats();
        sender.send(result).unwrap();
    });

    let stats = receiver
        .recv_timeout(std::time::Duration::from_secs(1))
        .expect("read lane was blocked by writer lane")
        .unwrap();
    assert_eq!(stats.count, 0);
    drop(writer_guard);
    reader.join().unwrap();

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn background_rebuild_allows_bounded_v2_import_and_hands_off_tail() {
    let root = std::env::temp_dir().join(format!(
        "lethe-rebuild-import-concurrency-test-{}",
        uuid::Uuid::now_v7()
    ));
    let db = root.join("lethe.sqlite3");
    let blobs = root.join("blobs");
    let persistence = SqlitePersistence::open(&db, &blobs, &[7; 32]).unwrap();
    let published = chrono::DateTime::parse_from_rfc3339("2026-07-24T00:00:00Z")
        .unwrap()
        .to_utc();
    for index in 0..40 {
        persistence
            .persist_observation(&freshness_only_observation(
                "schema:github-event",
                "sys:github",
                &format!("rebuild-seed-{index:03}"),
                published + chrono::Duration::seconds(index),
            ))
            .unwrap();
    }
    let mut config = test_config(db, blobs);
    config.corpus.rebuild_page_size = 1;
    let mut service = test_service(config, persistence);
    service.non_corpus_rebuild_page_delay = Some(std::time::Duration::from_millis(25));

    let core = service.core_snapshot();
    service
        .refresh_materialized_snapshot_with_reason(&core, "recovery")
        .unwrap();
    drop(core);
    for _ in 0..2_000 {
        if service
            .non_corpus_rebuild_page_count
            .load(std::sync::atomic::Ordering::Acquire)
            > 0
        {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    assert!(
        service
            .non_corpus_rebuild_in_flight
            .load(std::sync::atomic::Ordering::Acquire),
        "import must be issued while the background rebuild is still running"
    );

    let started_at = std::time::Instant::now();
    let report = service
        .ingest_observation_drafts_v2(vec![v2_slack_draft(9_999, Utc::now())], "slack-test")
        .unwrap();
    let response_elapsed = started_at.elapsed();
    assert!(
        response_elapsed < std::time::Duration::from_secs(5),
        "v2 import waited {response_elapsed:?} during background rebuild"
    );
    assert_eq!(report.ingested, 1);
    assert!(matches!(
        report.results.as_slice(),
        [ImportItemResult {
            outcome: ImportOutcome::Ingested,
            ..
        }]
    ));

    service.wait_for_non_corpus_rebuild().unwrap();
    wait_for_append_consumer(&service);
    assert_eq!(
        service
            .non_corpus_rebuild_page_count
            .load(std::sync::atomic::Ordering::Relaxed),
        80,
        "the fixed 40-row high-water must be read exactly twice without a full retry"
    );
    assert_eq!(
        service
            .persistence_lock()
            .unwrap()
            .observation_stats()
            .unwrap()
            .count,
        41
    );
    assert_eq!(service.core_snapshot().observation_stats.count, 41);
    let manifest = service
        .persistence_lock()
        .unwrap()
        .projection_records(&ProjectionRef::new("proj:person-page"))
        .unwrap()
        .unwrap();
    assert_eq!(manifest["observation_count"], 41);
    assert_eq!(manifest["last_append_seq"], 41);

    drop(service);
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn background_rebuild_and_source_sync_do_not_block_bounded_v1_import() {
    let root = std::env::temp_dir().join(format!(
        "lethe-rebuild-sync-import-concurrency-test-{}",
        uuid::Uuid::now_v7()
    ));
    let db = root.join("lethe.sqlite3");
    let blobs = root.join("blobs");
    let persistence = SqlitePersistence::open(&db, &blobs, &[7; 32]).unwrap();
    let published = chrono::DateTime::parse_from_rfc3339("2026-07-24T00:00:00Z")
        .unwrap()
        .to_utc();
    for index in 0..4 {
        persistence
            .persist_observation(&freshness_only_observation(
                "schema:github-event",
                "sys:github",
                &format!("rebuild-sync-seed-{index:03}"),
                published + chrono::Duration::seconds(index),
            ))
            .unwrap();
    }
    let mut config = test_config(db, blobs);
    config.corpus.rebuild_page_size = 1;
    let mut service = test_service(config, persistence);
    service.slack_sources.clear();
    service.google_sources.clear();
    service.non_corpus_rebuild_page_delay = Some(std::time::Duration::from_secs(1));

    let core = service.core_snapshot();
    service
        .refresh_materialized_snapshot_with_reason(&core, "recovery")
        .unwrap();
    drop(core);
    for _ in 0..2_000 {
        if service
            .non_corpus_rebuild_page_count
            .load(std::sync::atomic::Ordering::Acquire)
            > 0
        {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    assert!(
        service
            .non_corpus_rebuild_in_flight
            .load(std::sync::atomic::Ordering::Acquire),
        "sync and import must start while the background rebuild is running"
    );

    let sync_service = service.clone();
    let (sync_sender, sync_receiver) = std::sync::mpsc::channel();
    let sync_thread = std::thread::spawn(move || {
        sync_sender.send(sync_service.sync_all()).unwrap();
    });
    let mut sync_started = false;
    for _ in 0..2_000 {
        match service.non_bulk_projection_operation.try_lock() {
            Ok(operation) => drop(operation),
            Err(std::sync::TryLockError::WouldBlock) => {
                sync_started = true;
                break;
            }
            Err(std::sync::TryLockError::Poisoned(_)) => {
                panic!("non-bulk projection operation lock was poisoned")
            }
        }
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    assert!(
        sync_started,
        "source sync did not enter its projection operation"
    );
    assert!(
        service
            .non_corpus_rebuild_in_flight
            .load(std::sync::atomic::Ordering::Acquire),
        "source sync must be waiting for the active background rebuild"
    );
    let bulk_begin_started_at = std::time::Instant::now();
    let bulk_begin_error = service.begin_bulk_import_session().unwrap_err();
    assert!(
        bulk_begin_started_at.elapsed() < std::time::Duration::from_secs(1),
        "bulk session begin waited for source sync instead of failing fast"
    );
    assert!(matches!(
        bulk_begin_error,
        SelfHostError::BulkImportSessionConflict {
            code: "bulk_import_non_bulk_projection_active",
            ..
        }
    ));

    let started_at = std::time::Instant::now();
    let report = service
        .ingest_observation_drafts(
            vec![freshness_only_draft("rebuild-sync-import")],
            "sync-rebuild-test",
        )
        .unwrap();
    let response_elapsed = started_at.elapsed();
    assert!(
        response_elapsed < std::time::Duration::from_secs(5),
        "v1 import waited {response_elapsed:?} behind source sync during background rebuild"
    );
    assert_eq!(report.ingested, 1);

    service.wait_for_non_corpus_rebuild().unwrap();
    sync_receiver
        .recv_timeout(std::time::Duration::from_secs(10))
        .expect("source sync did not finish after background rebuild")
        .unwrap();
    sync_thread.join().unwrap();
    wait_for_append_consumer(&service);
    assert_eq!(
        service
            .persistence_lock()
            .unwrap()
            .observation_stats()
            .unwrap()
            .count,
        5
    );
    assert_eq!(service.core_snapshot().observation_stats.count, 5);

    drop(service);
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn empty_google_source_latest_workspace_lookup_does_not_read_canonical_storage() {
    let root = std::env::temp_dir().join(format!(
        "lethe-empty-google-sync-scan-test-{}",
        uuid::Uuid::now_v7()
    ));
    let db = root.join("lethe.sqlite3");
    let blobs = root.join("blobs");
    let persistence = SqlitePersistence::open(&db, &blobs, &[7; 32]).unwrap();
    persistence
        .persist_observation(&freshness_only_observation(
            "schema:github-event",
            "sys:github",
            "empty-google-source-seed",
            Utc::now(),
        ))
        .unwrap();
    let mut service = test_service(test_config(db, blobs), persistence);
    service.google_sources.clear();

    let writer_guard = service.persistence.lock().unwrap();
    let lookup_service = service.clone();
    let (sender, receiver) = std::sync::mpsc::channel();
    let lookup_thread = std::thread::spawn(move || {
        sender
            .send(lookup_service.latest_workspace_slide_observations())
            .unwrap();
    });
    let observations = receiver
        .recv_timeout(std::time::Duration::from_secs(1))
        .expect("empty Google source lookup attempted to read canonical storage")
        .unwrap();
    assert!(observations.is_empty());
    drop(writer_guard);
    lookup_thread.join().unwrap();

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
    wait_for_append_consumer(&service);
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
fn import_publish_count_is_bounded_per_request_and_consumer_batch() {
    let root = std::env::temp_dir().join(format!(
        "lethe-self-host-publish-count-test-{}",
        uuid::Uuid::now_v7()
    ));
    let db = root.join("lethe.sqlite3");
    let blobs = root.join("blobs");
    let persistence = SqlitePersistence::open(&db, &blobs, &[7; 32]).unwrap();
    let service = test_service(test_config(db.clone(), blobs.clone()), persistence);

    let report = service
        .ingest_observation_drafts(
            (0..1_000)
                .map(|index| freshness_only_draft(&format!("publish-one-{index:04}")))
                .collect(),
            "publish-count-one-request",
        )
        .unwrap();
    assert_eq!(report.ingested, 1_000);
    wait_for_append_consumer(&service);
    let one_request_publishes = service.publish_count();
    eprintln!("publish_count(1000 in one request)={one_request_publishes}");
    assert!(one_request_publishes <= 2);
    drop(service);

    let persistence = SqlitePersistence::open(&db, &blobs, &[7; 32]).unwrap();
    let service = test_service(test_config(db, blobs), persistence);
    for request_index in 0..40 {
        let report = service
            .ingest_observation_drafts(
                (0..25)
                    .map(|item_index| {
                        freshness_only_draft(&format!(
                            "publish-serial-{request_index:02}-{item_index:02}"
                        ))
                    })
                    .collect(),
                "publish-count-serial",
            )
            .unwrap();
        assert_eq!(report.ingested, 25);
    }
    wait_for_append_consumer(&service);
    let serial_publishes = service.publish_count();
    eprintln!("publish_count(25x40 serial requests)={serial_publishes}");
    assert!(serial_publishes <= 43);

    drop(service);
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn duplicate_only_imports_keep_snapshot_generation_and_counters_stable() {
    let root = std::env::temp_dir().join(format!(
        "lethe-duplicate-only-generation-test-{}",
        uuid::Uuid::now_v7()
    ));
    let db = root.join("lethe.sqlite3");
    let blobs = root.join("blobs");
    let persistence = SqlitePersistence::open(&db, &blobs, &[7; 32]).unwrap();
    let service = test_service(test_config(db, blobs), persistence);

    assert_eq!(
        service
            .ingest_observation_drafts(vec![freshness_only_draft("duplicate-only")], "dup-test")
            .unwrap()
            .ingested,
        1
    );
    wait_for_append_consumer(&service);
    let before_snapshot = service.core_snapshot();
    let before_publish_count = service.publish_count();
    let before_rebuild_count = service
        .non_corpus_rebuild_count
        .load(std::sync::atomic::Ordering::Relaxed);
    let before_audit_count = service
        .persistence_lock()
        .unwrap()
        .audit_event_page(None, 1_000)
        .unwrap()
        .len();

    for _ in 0..20 {
        let report = service
            .ingest_observation_drafts(vec![freshness_only_draft("duplicate-only")], "dup-test")
            .unwrap();
        assert_eq!(report.ingested, 0);
        assert_eq!(report.duplicates, 1);
    }

    let after_snapshot = service.core_snapshot();
    assert!(Arc::ptr_eq(&before_snapshot, &after_snapshot));
    assert_eq!(service.publish_count(), before_publish_count);
    assert_eq!(
        service
            .non_corpus_rebuild_count
            .load(std::sync::atomic::Ordering::Relaxed),
        before_rebuild_count
    );
    assert_eq!(
        service
            .persistence_lock()
            .unwrap()
            .audit_event_page(None, 1_000)
            .unwrap()
            .len(),
        before_audit_count + 20
    );
    eprintln!(
        "dup-only 20 requests: publish_count={} rebuild_count={} Arc::ptr_eq=true",
        service.publish_count(),
        service
            .non_corpus_rebuild_count
            .load(std::sync::atomic::Ordering::Relaxed)
    );

    drop(after_snapshot);
    drop(before_snapshot);
    drop(service);
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn published_snapshot_releases_old_generation() {
    let root = std::env::temp_dir().join(format!(
        "lethe-published-snapshot-weak-test-{}",
        uuid::Uuid::now_v7()
    ));
    let db = root.join("lethe.sqlite3");
    let blobs = root.join("blobs");
    let persistence = SqlitePersistence::open(&db, &blobs, &[7; 32]).unwrap();
    let service = test_service(test_config(db, blobs), persistence);

    let before = service.core_snapshot();
    let old_generation = std::sync::Arc::downgrade(&before);
    drop(before);
    {
        let core = service.core_lock().unwrap();
        service.publish_core_snapshot(&core);
    }
    assert!(old_generation.upgrade().is_none());

    drop(service);
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn mixed_import_keeps_append_side_effects_bounded_and_catches_up_search() {
    let root = std::env::temp_dir().join(format!(
        "lethe-mixed-import-side-effects-test-{}",
        uuid::Uuid::now_v7()
    ));
    let db = root.join("lethe.sqlite3");
    let blobs = root.join("blobs");
    let persistence = SqlitePersistence::open(&db, &blobs, &[7; 32]).unwrap();
    let service = test_service(test_config(db, blobs), persistence);

    service
        .ingest_observation_drafts(vec![freshness_only_draft("mixed-existing")], "mixed-test")
        .unwrap();
    wait_for_append_consumer(&service);
    let before_publish_count = service.publish_count();
    let before_rebuild_count = service
        .non_corpus_rebuild_count
        .load(std::sync::atomic::Ordering::Relaxed);

    let report = service
        .ingest_observation_drafts(
            vec![
                freshness_only_draft("mixed-existing"),
                freshness_only_draft("mixed-new"),
            ],
            "mixed-test",
        )
        .unwrap();
    assert_eq!(report.ingested, 1);
    assert_eq!(report.duplicates, 1);
    wait_for_append_consumer(&service);
    wait_for_search_index_high_water(&service, 2);
    assert!(service.publish_count() <= before_publish_count + 1);
    assert_eq!(
        service
            .non_corpus_rebuild_count
            .load(std::sync::atomic::Ordering::Relaxed),
        before_rebuild_count
    );

    drop(service);
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn import_admission_and_draft_limit_fail_fast_for_v1_and_v2() {
    let root = std::env::temp_dir().join(format!(
        "lethe-self-host-import-limit-test-{}",
        uuid::Uuid::now_v7()
    ));
    let db = root.join("lethe.sqlite3");
    let blobs = root.join("blobs");
    let persistence = SqlitePersistence::open(&db, &blobs, &[7; 32]).unwrap();
    let mut config = test_config(db.clone(), blobs.clone());
    config.resource_limits.max_concurrent_imports = 1;
    config.resource_limits.max_import_drafts = 1;
    let service = test_service(config, persistence);

    let permit = service.try_acquire_import_permit().unwrap();
    assert!(matches!(
        service.ingest_observation_drafts(vec![freshness_only_draft("busy-v1")], "limits"),
        Err(SelfHostError::ImportConcurrencyLimit { maximum: 1 })
    ));
    assert!(matches!(
        service.ingest_observation_drafts_v2(vec![freshness_only_draft("busy-v2")], "limits"),
        Err(SelfHostError::ImportConcurrencyLimit { maximum: 1 })
    ));
    drop(permit);

    let now = Utc::now();
    let drafts = vec![
        v2_slack_draft(1, now),
        v2_slack_draft(2, now + chrono::Duration::seconds(1)),
    ];
    assert!(matches!(
        service.ingest_observation_drafts(drafts.clone(), "slack-test"),
        Err(SelfHostError::Ingestion(detail)) if detail.contains("draft count 2")
    ));
    let v2_report = service
        .ingest_observation_drafts_v2(drafts, "slack-test")
        .unwrap();
    assert_eq!(v2_report.ingested, 1);
    assert_eq!(v2_report.rejected, 1);
    assert!(matches!(
        v2_report.results.as_slice(),
        [_, ImportItemResult {
            outcome: ImportOutcome::Rejected,
            error_code: Some(code),
            details: Some(details),
            ..
        }] if code == "draft_count_exceeded"
            && details["actual"] == 2
            && details["maximum"] == 1
    ));
    assert_eq!(
        service
            .persistence_lock()
            .unwrap()
            .observation_stats()
            .unwrap()
            .count,
        1
    );

    drop(service);
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
    let total = 5_000;
    let drafts = (0..total).map(wave2_slack_draft).collect();

    let report = service
        .ingest_observation_drafts(drafts, "slack-test")
        .unwrap();

    assert_eq!(report.ingested, total);
    assert_eq!(report.duplicates, 0);
    assert_eq!(report.quarantined, 0);
    wait_for_append_consumer(&service);
    let core = service.core_lock().unwrap();
    assert_eq!(core.observation_stats.count, total as u64);
    assert_eq!(core.compact_state.identity.nodes().len(), 100);
    assert_eq!(core.person_components.len(), 100);
    assert!(core.snapshot.identity.resolved_persons.is_empty());
    assert!(core.snapshot.person_page.profiles.is_empty());
    assert!(core.snapshot.person_page.messages.is_empty());
    assert_eq!(core.person_message_count, total as u64);
    assert_eq!(core.reply_slo_count, total as u64);
    assert!(core.snapshot.reply_slo.rows.is_empty());
    assert!(core.snapshot.reply_slo.overdue.is_empty());
    let component = core.person_components.values().next().unwrap();
    let person_id = component.person.person_id.as_str().to_owned();
    let expected_owner_messages = component.activity.as_ref().unwrap().total_messages;
    let fact_owners = core
        .compact_state
        .identity
        .component_members_for_person(&person_id)
        .unwrap()
        .iter()
        .map(|node_id| super::identity_node_owner(*node_id))
        .collect::<Vec<_>>();
    drop(core);
    {
        let persistence = service.persistence_lock().unwrap();
        assert_eq!(
            persistence
                .projection_item_count(&ProjectionRef::new("proj:person-page"))
                .unwrap(),
            (total * 3 + 100) as u64
        );
        assert_eq!(
            persistence
                .projection_item_count_by_owner(
                    &ProjectionRef::new("proj:person-page"),
                    super::REPLY_SLO_ITEM_OWNER,
                )
                .unwrap(),
            total as u64
        );
        let owner_message_count = fact_owners
            .iter()
            .map(|owner| {
                persistence
                    .projection_items_by_owner(&ProjectionRef::new("proj:person-page"), owner)
                    .unwrap()
                    .into_iter()
                    .filter(|item| item.item_key.starts_with("pm:"))
                    .count()
            })
            .sum::<usize>();
        assert_eq!(owner_message_count, expected_owner_messages);
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
    assert_eq!(service.reply_slo_response().unwrap().data.rows.len(), total);
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
    assert_eq!(restarted_core.person_message_count, total as u64);
    assert_eq!(restarted_core.reply_slo_count, total as u64);
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
        total
    );
    drop(restarted);

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn communication_import_latency_p95_is_bounded_without_full_rebuild_im07() {
    let root = std::env::temp_dir().join(format!(
        "lethe-communication-p95-test-{}",
        uuid::Uuid::now_v7()
    ));
    let db = root.join("lethe.sqlite3");
    let blobs = root.join("blobs");
    let persistence = SqlitePersistence::open(&db, &blobs, &[7; 32]).unwrap();
    let mut config = test_config(db, blobs);
    config.corpus.mode = lethe_projection_corpus::CorpusMode::PersonalAllText;
    let service = test_service(config, persistence);
    let mut previous_target = 0;
    for target in [500, 2_500, 5_000] {
        let seed = (previous_target..target)
            .map(|index| freshness_only_draft(&format!("p95-seed-{index:05}")))
            .collect::<Vec<_>>();
        service.ingest_observation_drafts(seed, "p95-seed").unwrap();

        let mut samples = Vec::new();
        for index in 0..10 {
            let published = Utc::now();
            let mut draft = freshness_only_draft(&format!("p95-communication-{target}-{index}"));
            draft.published = published;
            draft.meta["communication_channel_id"] = serde_json::json!("chan:p95");
            draft.meta["communication_sender_id"] = serde_json::json!("sender:p95");
            draft.meta["communication_thread_ref"] =
                serde_json::json!(format!("thread:p95:{target}:{index}"));
            draft.meta["communication"] = serde_json::json!({
                "reply_due_at": (published + chrono::Duration::hours(1)).to_rfc3339(),
            });
            let started = std::time::Instant::now();
            service
                .ingest_observation_drafts(vec![draft], "p95-communication")
                .unwrap();
            samples.push(started.elapsed());
        }
        samples.sort_unstable();
        let p95_index = (samples.len() * 95).div_ceil(100) - 1;
        assert!(
            samples[p95_index] < std::time::Duration::from_secs(2),
            "communication import p95 at {target} observations was {:?}",
            samples[p95_index]
        );
        previous_target = target;
    }
    assert_eq!(
        service
            .non_corpus_rebuild_count
            .load(std::sync::atomic::Ordering::Relaxed),
        0,
        "normal communication imports must not start a full rebuild"
    );

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
        client_ref: None,
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
        core.person_components
            .values()
            .next()
            .unwrap()
            .profile
            .as_ref()
            .unwrap()
            .self_intro_text
            .as_deref(),
        Some("私は田中太郎です")
    );
}
