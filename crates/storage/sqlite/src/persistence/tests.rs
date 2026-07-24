use super::*;
use chrono::Utc;

use lethe_core::domain::{
    ActorRef, AuthorityModel, CaptureModel, DataSpaceId, EntityRef, IdempotencyKey, Mutability,
    Observation, ObserverRef, OperationalEventId, SchemaRef, SemVer, SourceSystemRef,
    SupplementalId, SupplementalRecord, supplemental::InputAnchorSet,
};
use lethe_runtime::runtime::partition::{RoutingKeyOrder, routing_keyspec_json_for_order};
use lethe_storage_api::{OperationalAppendRequest, OperationalEvent, OperationalEventStore};

fn sample_observation() -> Observation {
    let canonical_json = serde_json::json!({
        "source": "test",
        "object_id": "sample-key",
        "body": "world"
    })
    .to_string();
    Observation {
        id: Observation::new_id(),
        schema: SchemaRef::new("schema:test"),
        schema_version: SemVer::new("1.0.0"),
        observer: ObserverRef::new("obs:test"),
        source_system: Some(SourceSystemRef::new("sys:test")),
        actor: None,
        authority_model: AuthorityModel::LakeAuthoritative,
        capture_model: CaptureModel::Event,
        subject: EntityRef::new("entity:test"),
        target: None,
        payload: serde_json::json!({"hello": "world"}),
        attachments: vec![],
        published: Utc::now(),
        recorded_at: Utc::now(),
        consent: None,
        idempotency_key: IdempotencyKey::new("sample-key"),
        meta: serde_json::json!({
            CANONICAL_JSON_META_KEY: canonical_json,
            "source_container": "test",
        }),
    }
}

fn sample_observation_with_identity(identity_key: &str, body: &str) -> Observation {
    let canonical_json = serde_json::json!({
        "source": "test",
        "object_id": identity_key,
        "body": body
    })
    .to_string();
    let mut observation = sample_observation();
    observation.id = Observation::new_id();
    observation.idempotency_key = IdempotencyKey::new(identity_key);
    observation.meta = serde_json::json!({
        CANONICAL_JSON_META_KEY: canonical_json,
        "source_container": "test",
    });
    observation
}

fn bridge_observation(
    source_instance_id: &str,
    object_id: &str,
    canonical_json: &str,
    identity_key: &str,
) -> Observation {
    let mut observation = sample_observation();
    observation.id = Observation::new_id();
    observation.idempotency_key = IdempotencyKey::new(identity_key);
    observation.meta = serde_json::json!({
        "source_instance": source_instance_id,
        "object_id": object_id,
        CANONICAL_JSON_META_KEY: canonical_json,
        "source_container": format!("{source_instance_id}:test"),
    });
    observation
}

fn history_identity(source_instance_id: &str, object_id: &str, canonical_json: &str) -> String {
    bridge_identity(source_instance_id, object_id, canonical_json)
}

fn legacy_history_observation(
    source_instance_id: &str,
    source_session_id: &str,
    canonical_json: &str,
    identity_key: &str,
) -> Observation {
    let mut observation = sample_observation();
    observation.schema = SchemaRef::new("schema:history-message");
    observation.payload = serde_json::from_str(canonical_json).unwrap();
    observation.idempotency_key = IdempotencyKey::new(identity_key);
    observation.meta = serde_json::json!({
        CANONICAL_JSON_META_KEY: canonical_json,
        "source_container": source_session_id,
        "source_instance_id": source_instance_id,
    });
    observation
}

fn history_event_request(
    data_space_id: &DataSpaceId,
    event_id: &str,
    stream_id: &str,
    observation: Observation,
) -> OperationalAppendRequest {
    let mut observation = observation;
    observation
        .meta
        .as_object_mut()
        .unwrap()
        .insert("event_id".to_owned(), serde_json::json!(event_id));
    let occurred_at = observation.published;
    OperationalAppendRequest {
        expected_stream_version: 0,
        event: OperationalEvent {
            event_id: OperationalEventId::new(event_id),
            data_space_id: data_space_id.clone(),
            stream_id: stream_id.to_owned(),
            stream_version: 1,
            event_type: "history.message_imported".to_owned(),
            occurred_at,
            actor_type: "history_source".to_owned(),
            actor_id: Some("owner".to_owned()),
            correlation_id: None,
            causation_id: None,
            observation,
        },
    }
}

fn bridge_identity(source_instance_id: &str, object_id: &str, canonical_json: &str) -> String {
    format!(
        "{source_instance_id}:{object_id}:{}",
        hex::encode(sha2::Sha256::digest(canonical_json.as_bytes()))
    )
}

fn replace_with_legacy_canonical_json_observations_table(
    store: &SqlitePersistence,
    observation: &Observation,
) {
    let canonical_json = observation
        .meta
        .get(CANONICAL_JSON_META_KEY)
        .and_then(serde_json::Value::as_str)
        .unwrap();
    store
        .conn
        .execute_batch(
            "
            DROP INDEX IF EXISTS observations_leaf_append;
            DROP TABLE observations;
            DROP TABLE retired_projection_materializations;
            DROP TABLE projection_materialization_heads;
            DELETE FROM schema_migrations WHERE version >= 9;
            INSERT INTO schema_migrations (version, name, applied_at)
            VALUES (8, 'global_observation_identity_registry', '2026-07-22T00:00:00Z');
            CREATE TABLE observations (
                append_seq INTEGER PRIMARY KEY AUTOINCREMENT,
                id TEXT NOT NULL UNIQUE,
                identity_key TEXT NOT NULL UNIQUE,
                canonical_json TEXT NOT NULL,
                recorded_at TEXT NOT NULL,
                observation_json TEXT NOT NULL
            );
            ",
        )
        .unwrap();
    store
        .conn
        .execute(
            "INSERT INTO observations (
                id, identity_key, canonical_json, recorded_at, observation_json
             ) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                observation.id.as_str(),
                observation.idempotency_key.as_str(),
                canonical_json,
                observation.recorded_at.to_rfc3339(),
                serde_json::to_string(observation).unwrap(),
            ],
        )
        .unwrap();
}

#[test]
fn persist_and_reload_observation() {
    let tmp = std::env::temp_dir().join(format!("lethe-test-{}", uuid::Uuid::now_v7()));
    let db = tmp.join("test.sqlite3");
    let blob_dir = tmp.join("blobs");
    let store = SqlitePersistence::open(&db, &blob_dir, &[7; 32]).unwrap();
    let observation = sample_observation();

    store.persist_observation(&observation).unwrap();
    let observations = store.load_observations().unwrap();
    assert_eq!(observations.len(), 1);
    assert_eq!(observations[0].schema, observation.schema);

    let _ = fs::remove_dir_all(tmp);
}

#[test]
fn observation_stats_report_empty_and_appended_high_water() {
    let tmp = std::env::temp_dir().join(format!("lethe-test-{}", uuid::Uuid::now_v7()));
    let db = tmp.join("test.sqlite3");
    let blob_dir = tmp.join("blobs");
    let store = SqlitePersistence::open(&db, &blob_dir, &[7; 32]).unwrap();

    assert_eq!(
        store.observation_stats().unwrap(),
        lethe_storage_api::ObservationStats {
            count: 0,
            max_append_seq: 0,
        }
    );

    let observation = sample_observation();
    store.append_observation_idempotent(&observation).unwrap();

    assert_eq!(
        store.observation_stats().unwrap(),
        lethe_storage_api::ObservationStats {
            count: 1,
            max_append_seq: 1,
        }
    );

    let _ = fs::remove_dir_all(tmp);
}

#[test]
fn durable_append_and_audit_enqueue_commit_or_rollback_as_one_transaction() {
    let tmp = std::env::temp_dir().join(format!("lethe-test-{}", uuid::Uuid::now_v7()));
    let db = tmp.join("test.sqlite3");
    let blob_dir = tmp.join("blobs");
    let store = SqlitePersistence::open(&db, &blob_dir, &[7; 32]).unwrap();
    let first = sample_observation();
    let audit = lethe_storage_api::AuditEventRecord {
        id: "audit:append-1".to_owned(),
        timestamp: "2026-07-23T00:00:00Z".to_owned(),
        actor: "actor:test".to_owned(),
        event_json: serde_json::json!({"kind": "write_execution"}).to_string(),
    };

    store
        .append_observations_idempotent_with_audit(
            std::slice::from_ref(&first),
            std::slice::from_ref(&audit),
        )
        .unwrap();
    assert_eq!(store.observation_stats().unwrap().count, 1);
    assert_eq!(
        store.audit_event_page(None, 10).unwrap(),
        vec![audit.clone()]
    );

    let second = sample_observation_with_identity("append-2", "second");
    assert!(
        store
            .append_observations_idempotent_with_audit(
                std::slice::from_ref(&second),
                std::slice::from_ref(&audit),
            )
            .is_err()
    );
    assert_eq!(store.observation_stats().unwrap().count, 1);
    assert!(store.observation_by_id(&second.id).unwrap().is_none());

    let _ = fs::remove_dir_all(tmp);
}

#[test]
fn audit_event_page_uses_timestamp_and_id_keyset_at_boundary() {
    let tmp = std::env::temp_dir().join(format!("lethe-audit-page-{}", uuid::Uuid::now_v7()));
    let db = tmp.join("test.sqlite3");
    let blob_dir = tmp.join("blobs");
    let store = SqlitePersistence::open(&db, &blob_dir, &[7; 32]).unwrap();
    let timestamp = "2026-07-23T00:00:00Z";

    for id in ["audit:1", "audit:2", "audit:3"] {
        store
            .record_audit_event(
                id,
                timestamp,
                "actor:test",
                &serde_json::json!({"id": id}).to_string(),
            )
            .unwrap();
    }

    let first_page = store.audit_event_page(None, 2).unwrap();
    assert_eq!(
        first_page
            .iter()
            .map(|event| event.id.as_str())
            .collect::<Vec<_>>(),
        vec!["audit:1", "audit:2"]
    );
    let cursor = lethe_storage_api::AuditEventCursor {
        timestamp: first_page.last().unwrap().timestamp.clone(),
        id: first_page.last().unwrap().id.clone(),
    };
    let second_page = store.audit_event_page(Some(&cursor), 2).unwrap();
    assert_eq!(
        second_page
            .iter()
            .map(|event| event.id.as_str())
            .collect::<Vec<_>>(),
        vec!["audit:3"]
    );

    let _ = fs::remove_dir_all(tmp);
}

fn sample_supplemental(observation_id: &lethe_core::domain::ObservationId) -> SupplementalRecord {
    SupplementalRecord {
        id: SupplementalId::new("sup:test"),
        kind: "slide-analysis".into(),
        derived_from: InputAnchorSet {
            observations: vec![observation_id.clone()],
            blobs: vec![],
            supplementals: vec![],
        },
        payload: serde_json::json!({"bio_text": "hello"}),
        created_by: ActorRef::new("actor:test"),
        created_at: Utc::now(),
        mutability: Mutability::ManagedCache,
        record_version: Some("1".into()),
        model_version: Some("fixture".into()),
        consent_metadata: None,
        lineage: None,
    }
}

fn sample_supplemental_with_id(
    id: &str,
    observation_id: &lethe_core::domain::ObservationId,
) -> SupplementalRecord {
    let mut record = sample_supplemental(observation_id);
    record.id = SupplementalId::new(id);
    record
}

#[test]
fn duplicate_persist_observation_surfaces_constraint_error() {
    let tmp = std::env::temp_dir().join(format!("lethe-test-{}", uuid::Uuid::now_v7()));
    let db = tmp.join("test.sqlite3");
    let blob_dir = tmp.join("blobs");
    let store = SqlitePersistence::open(&db, &blob_dir, &[7; 32]).unwrap();
    let observation = sample_observation();

    store.persist_observation(&observation).unwrap();
    let err = store.persist_observation(&observation).unwrap_err();
    assert!(matches!(err, PersistenceError::SchemaInvariant(_)));

    let _ = fs::remove_dir_all(tmp);
}

#[test]
fn idempotent_append_returns_duplicate_for_same_canonical_json() {
    let tmp = std::env::temp_dir().join(format!("lethe-test-{}", uuid::Uuid::now_v7()));
    let db = tmp.join("test.sqlite3");
    let blob_dir = tmp.join("blobs");
    let store = SqlitePersistence::open(&db, &blob_dir, &[7; 32]).unwrap();
    let observation = sample_observation();

    let first = store.append_observation_idempotent(&observation).unwrap();
    let second = store.append_observation_idempotent(&observation).unwrap();

    assert_eq!(
        first,
        DurableAppendOutcome::Appended(observation.id.clone())
    );
    assert_eq!(
        second,
        DurableAppendOutcome::Duplicate(observation.id.clone())
    );

    let _ = fs::remove_dir_all(tmp);
}

#[test]
fn idempotent_append_detects_canonical_json_collision() {
    let tmp = std::env::temp_dir().join(format!("lethe-test-{}", uuid::Uuid::now_v7()));
    let db = tmp.join("test.sqlite3");
    let blob_dir = tmp.join("blobs");
    let store = SqlitePersistence::open(&db, &blob_dir, &[7; 32]).unwrap();
    let observation = sample_observation();
    let mut collision = observation.clone();
    collision.id = Observation::new_id();
    collision.meta = serde_json::json!({
        CANONICAL_JSON_META_KEY: serde_json::json!({
            "source": "test",
            "object_id": "sample-key",
            "body": "changed"
        }).to_string(),
        "source_container": "test",
    });

    store.append_observation_idempotent(&observation).unwrap();
    let outcome = store.append_observation_idempotent(&collision).unwrap();

    assert_eq!(
        outcome,
        DurableAppendOutcome::CanonicalCollision(observation.id.clone())
    );

    let _ = fs::remove_dir_all(tmp);
}

#[test]
fn identity_registry_is_global_across_routing_time_changes() {
    let tmp = std::env::temp_dir().join(format!("lethe-test-{}", uuid::Uuid::now_v7()));
    let store =
        SqlitePersistence::open(&tmp.join("test.sqlite3"), &tmp.join("blobs"), &[7; 32]).unwrap();
    let mut first = sample_observation_with_identity("global-retry", "stable");
    first.published = chrono::DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
        .unwrap()
        .to_utc();
    let mut retry = first.clone();
    retry.id = Observation::new_id();
    retry.published = chrono::DateTime::parse_from_rfc3339("2026-07-01T00:00:00Z")
        .unwrap()
        .to_utc();
    let mut retry_again = retry.clone();
    retry_again.id = Observation::new_id();
    retry_again.published = chrono::DateTime::parse_from_rfc3339("2027-01-01T00:00:00Z")
        .unwrap()
        .to_utc();

    store.append_observation_idempotent(&first).unwrap();
    assert_eq!(
        store.append_observation_idempotent(&retry).unwrap(),
        DurableAppendOutcome::Duplicate(first.id.clone())
    );
    assert_eq!(
        store.append_observation_idempotent(&retry_again).unwrap(),
        DurableAppendOutcome::Duplicate(first.id.clone())
    );
    assert_eq!(store.load_observations().unwrap().len(), 1);
    let registry_count: i64 = store
        .conn
        .query_row(
            "SELECT COUNT(*) FROM observation_identity_registry WHERE identity_key = ?1",
            ["global-retry"],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(registry_count, 1);

    let _ = fs::remove_dir_all(tmp);
}

#[test]
fn identity_lookup_and_legacy_fallback_use_indexes() {
    let tmp = std::env::temp_dir().join(format!("lethe-test-{}", uuid::Uuid::now_v7()));
    let store =
        SqlitePersistence::open(&tmp.join("test.sqlite3"), &tmp.join("blobs"), &[7; 32]).unwrap();

    let registry_plan = explain_query_plan(
        &store,
        "SELECT r.observation_id, r.canonical_json_sha256, o.observation_json
         FROM observation_identity_registry r
         JOIN observations o ON o.id = r.observation_id
         WHERE r.identity_key = ?1",
    );
    assert_no_table_scan(&registry_plan, "observation_identity_registry");
    assert_no_table_scan(&registry_plan, "observations");

    let fallback_plan = explain_query_plan(
        &store,
        "SELECT id, canonical_json_sha256, observation_json
         FROM observations
         WHERE identity_key = ?1
         ORDER BY append_seq
         LIMIT 1",
    );
    assert!(
        fallback_plan
            .iter()
            .any(|detail| detail.contains("observations_identity_append")),
        "legacy identity fallback must use observations_identity_append: {fallback_plan:?}"
    );
    assert_no_table_scan(&fallback_plan, "observations");

    let _ = fs::remove_dir_all(tmp);
}

fn explain_query_plan(store: &SqlitePersistence, sql: &str) -> Vec<String> {
    let mut statement = store
        .conn
        .prepare(&format!("EXPLAIN QUERY PLAN {sql}"))
        .unwrap();
    statement
        .query_map([""], |row| row.get::<_, String>(3))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
}

fn assert_no_table_scan(plan: &[String], table: &str) {
    assert!(
        !plan.iter().any(|detail| {
            detail.contains(&format!("SCAN {table}"))
                || detail.contains(&format!("SCAN {} ", table))
        }),
        "query plan unexpectedly scans {table}: {plan:?}"
    );
}

#[test]
fn bulk_idempotent_append_uses_one_transaction_and_preserves_outcomes() {
    let tmp = std::env::temp_dir().join(format!("lethe-test-{}", uuid::Uuid::now_v7()));
    let db = tmp.join("test.sqlite3");
    let blob_dir = tmp.join("blobs");
    let store = SqlitePersistence::open(&db, &blob_dir, &[7; 32]).unwrap();

    let mut first = sample_observation();
    first.idempotency_key = IdempotencyKey::new("bulk-a");
    first.meta = serde_json::json!({
        CANONICAL_JSON_META_KEY: serde_json::json!({
            "source": "test",
            "object_id": "bulk-a",
            "body": "first"
        }).to_string(),
        "source_container": "test",
    });

    let mut duplicate = first.clone();
    duplicate.id = Observation::new_id();

    let mut collision = first.clone();
    collision.id = Observation::new_id();
    collision.meta = serde_json::json!({
        CANONICAL_JSON_META_KEY: serde_json::json!({
            "source": "test",
            "object_id": "bulk-a",
            "body": "changed"
        }).to_string(),
        "source_container": "test",
    });

    let mut second = sample_observation();
    second.id = Observation::new_id();
    second.idempotency_key = IdempotencyKey::new("bulk-b");
    second.meta = serde_json::json!({
        CANONICAL_JSON_META_KEY: serde_json::json!({
            "source": "test",
            "object_id": "bulk-b",
            "body": "second"
        }).to_string(),
        "source_container": "test",
    });

    let outcomes = store
        .append_observations_idempotent(&[first.clone(), duplicate, collision, second.clone()])
        .unwrap();

    assert_eq!(
        outcomes,
        vec![
            DurableAppendOutcome::Appended(first.id.clone()),
            DurableAppendOutcome::Duplicate(first.id.clone()),
            DurableAppendOutcome::CanonicalCollision(first.id.clone()),
            DurableAppendOutcome::Appended(second.id.clone()),
        ]
    );
    assert_eq!(store.load_observations().unwrap().len(), 2);

    let _ = fs::remove_dir_all(tmp);
}

#[test]
fn rehome_mode_a_preserves_stored_identity_and_times() {
    let tmp = std::env::temp_dir().join(format!("lethe-test-{}", uuid::Uuid::now_v7()));
    let db = tmp.join("test.sqlite3");
    let blob_dir = tmp.join("blobs");
    let store = SqlitePersistence::open(&db, &blob_dir, &[7; 32]).unwrap();
    let mut first = sample_observation();
    first.idempotency_key = IdempotencyKey::new("first");
    first.meta = serde_json::json!({
        CANONICAL_JSON_META_KEY: serde_json::json!({
            "source": "test",
            "object_id": "first",
            "body": "first"
        }).to_string(),
        "source_container": "test",
    });
    store.persist_observation(&first).unwrap();

    let mut observation = sample_observation();
    observation.id = Observation::new_id();
    observation.idempotency_key = IdempotencyKey::new("rehome-mode-a");
    observation.published = chrono::DateTime::parse_from_rfc3339("2026-05-01T08:30:00Z")
        .unwrap()
        .to_utc();
    observation.recorded_at = chrono::DateTime::parse_from_rfc3339("2026-05-01T08:31:00Z")
        .unwrap()
        .to_utc();
    observation.meta = serde_json::json!({
        CANONICAL_JSON_META_KEY: serde_json::json!({
            "source": "test",
            "object_id": "rehome-mode-a",
            "body": "stored"
        }).to_string(),
        "source_container": "test",
    });

    let outcome = store
        .rehome_observation(&observation, RehomeMode::StoredIdentity)
        .unwrap();

    assert_eq!(
        outcome,
        DurableAppendOutcome::Appended(observation.id.clone())
    );
    let (append_seq, json): (i64, String) = store
        .conn
        .query_row(
            "SELECT append_seq, observation_json FROM observations WHERE id = ?1",
            [observation.id.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    let stored = serde_json::from_str::<Observation>(&json).unwrap();

    assert_eq!(append_seq, 2);
    assert_eq!(stored.id, observation.id);
    assert_eq!(stored.published, observation.published);
    assert_eq!(stored.recorded_at, observation.recorded_at);
    assert_eq!(stored.idempotency_key, observation.idempotency_key);

    let _ = fs::remove_dir_all(tmp);
}

#[test]
fn rehome_mode_b_reserializes_identity_and_canonical_json() {
    let tmp = std::env::temp_dir().join(format!("lethe-test-{}", uuid::Uuid::now_v7()));
    let db = tmp.join("test.sqlite3");
    let blob_dir = tmp.join("blobs");
    let store = SqlitePersistence::open(&db, &blob_dir, &[7; 32]).unwrap();
    let observation = sample_observation();
    let new_key = IdempotencyKey::new("identity-v2");
    let new_canonical_json = serde_json::json!({
        "source": "test-v2",
        "object_id": "sample-key",
        "body": "world"
    })
    .to_string();

    let outcome = store
        .rehome_observation(
            &observation,
            RehomeMode::RecomputedIdentity {
                identity_key: new_key.clone(),
                canonical_json: new_canonical_json.clone(),
            },
        )
        .unwrap();

    assert_eq!(
        outcome,
        DurableAppendOutcome::Appended(observation.id.clone())
    );
    let (identity_key, canonical_json, json): (String, String, String) = store
        .conn
        .query_row(
            "SELECT identity_key, canonical_json_sha256, observation_json FROM observations WHERE id = ?1",
            [observation.id.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    let stored = serde_json::from_str::<Observation>(&json).unwrap();

    assert_eq!(identity_key, new_key.as_str());
    assert_eq!(canonical_json, canonical_json_sha256(&new_canonical_json));
    assert_eq!(stored.idempotency_key, new_key);
    assert_eq!(
        stored.meta[CANONICAL_JSON_META_KEY].as_str(),
        Some(new_canonical_json.as_str())
    );
    assert_eq!(stored.id, observation.id);
    assert_eq!(stored.published, observation.published);
    assert_eq!(stored.recorded_at, observation.recorded_at);

    let _ = fs::remove_dir_all(tmp);
}

#[test]
fn persist_and_reload_supplemental() {
    let tmp = std::env::temp_dir().join(format!("lethe-test-{}", uuid::Uuid::now_v7()));
    let db = tmp.join("test.sqlite3");
    let blob_dir = tmp.join("blobs");
    let store = SqlitePersistence::open(&db, &blob_dir, &[7; 32]).unwrap();
    let observation = sample_observation();
    let supplemental = sample_supplemental(&observation.id);

    store.persist_observation(&observation).unwrap();
    store.persist_supplemental(&supplemental).unwrap();
    let supplementals = store.load_supplementals().unwrap();

    assert_eq!(supplementals.len(), 1);
    assert_eq!(supplementals[0].id, supplemental.id);
    assert_eq!(supplementals[0].kind, "slide-analysis");

    let _ = fs::remove_dir_all(tmp);
}

#[test]
fn migration_ledger_records_current_schema_version() {
    let tmp = std::env::temp_dir().join(format!("lethe-test-{}", uuid::Uuid::now_v7()));
    let db = tmp.join("test.sqlite3");
    let blob_dir = tmp.join("blobs");
    let store = SqlitePersistence::open(&db, &blob_dir, &[7; 32]).unwrap();
    assert!(
        store.schema_migrations_applied_on_open(),
        "fresh schema must report migrations applied by this open"
    );
    let version: i64 = store
        .conn
        .query_row(
            "SELECT version FROM schema_migrations WHERE version = ?1",
            [CURRENT_SCHEMA_VERSION],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(version, CURRENT_SCHEMA_VERSION);
    drop(store);

    let reopened = SqlitePersistence::open(&db, &blob_dir, &[7; 32]).unwrap();
    assert!(
        !reopened.schema_migrations_applied_on_open(),
        "current schema must not report historical migration records as newly applied"
    );
    drop(reopened);

    let _ = fs::remove_dir_all(tmp);
}

#[test]
fn schema_v14_requires_v13_reconsent_prerequisite() {
    let tmp = std::env::temp_dir().join(format!("lethe-test-{}", uuid::Uuid::now_v7()));
    let database_path = tmp.join("test.sqlite3");
    let blob_dir = tmp.join("blobs");
    let store = SqlitePersistence::open(&database_path, &blob_dir, &[7; 32]).unwrap();
    store
        .conn
        .execute(
            "DELETE FROM schema_migrations WHERE version = ?1",
            [SCHEMA_VERSION_RECONSENT_PRIVACY_INDEX],
        )
        .unwrap();
    drop(store);

    let result = SqlitePersistence::open(&database_path, &blob_dir, &[7; 32]);
    assert!(matches!(
        result,
        Err(PersistenceError::SchemaInvariant(reason))
            if reason == "schema migration v14 is recorded without prerequisite v13"
    ));

    let _ = fs::remove_dir_all(tmp);
}

#[test]
fn reconsent_privacy_reverse_index_tracks_subject_and_identifier_keys() {
    let tmp = std::env::temp_dir().join(format!("lethe-test-{}", uuid::Uuid::now_v7()));
    let db = tmp.join("test.sqlite3");
    let blob_dir = tmp.join("blobs");
    let store = SqlitePersistence::open(&db, &blob_dir, &[7; 32]).unwrap();
    let mut observation = sample_observation();
    observation.payload["email"] = serde_json::json!("person@example.test");
    store.append_observation_idempotent(&observation).unwrap();

    assert_eq!(
        store
            .observations_for_privacy_key("entity:test")
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        store
            .observations_for_privacy_key("person@example.test")
            .unwrap()
            .len(),
        1
    );

    let _ = fs::remove_dir_all(tmp);
}

#[test]
fn v13_privacy_backfill_streaming_matches_pre_migration_rows_across_pages() {
    let tmp = std::env::temp_dir().join(format!("lethe-v13-streaming-{}", uuid::Uuid::now_v7()));
    let db = tmp.join("test.sqlite3");
    let blob_dir = tmp.join("blobs");
    let store = SqlitePersistence::open(&db, &blob_dir, &[7; 32]).unwrap();
    for index in 0..1_025 {
        store
            .append_observation_idempotent(&sample_observation_with_identity(
                &format!("streaming-{index:04}"),
                "streaming",
            ))
            .unwrap();
    }
    let expected_ids = store
        .observations_for_privacy_key("entity:test")
        .unwrap()
        .into_iter()
        .map(|stored| stored.observation.id.as_str().to_owned())
        .collect::<std::collections::BTreeSet<_>>();
    store
        .conn
        .execute_batch(
            "
            DROP TRIGGER cutover_transition_log_no_update;
            DROP TRIGGER cutover_transition_log_no_delete;
            DROP INDEX cutover_credentials_active;
            DROP INDEX cutover_transition_unit_seq;
            DROP INDEX identity_bridge_gaps_source_append;
            DROP INDEX identity_bridge_candidates_source_append;
            DROP INDEX identity_bridge_candidates_key_append;
            DROP TABLE cutover_unit_metrics;
            DROP TABLE cutover_credentials;
            DROP TABLE cutover_transition_log;
            DROP TABLE identity_bridge_watermark;
            DROP TABLE identity_bridge_gaps;
            DROP TABLE identity_bridge_candidates;
            DROP INDEX observation_privacy_keys_append;
            DROP TABLE observation_privacy_keys;
            DROP TABLE retired_projection_materializations;
            DROP TABLE projection_materialization_heads;
            DELETE FROM schema_migrations WHERE version >= 13;
            ",
        )
        .unwrap();
    drop(store);

    let migrated = SqlitePersistence::open(&db, &blob_dir, &[7; 32]).unwrap();
    let actual_ids = migrated
        .observations_for_privacy_key("entity:test")
        .unwrap()
        .into_iter()
        .map(|stored| stored.observation.id.as_str().to_owned())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(actual_ids, expected_ids);
    assert_eq!(
        migrated
            .observations_for_privacy_key_page("entity:test", 0, 17)
            .unwrap()
            .len(),
        17
    );

    drop(migrated);
    let _ = fs::remove_dir_all(tmp);
}

#[test]
fn v13_privacy_backfill_rolls_back_and_retries_from_first_page() {
    let tmp = std::env::temp_dir().join(format!("lethe-v13-rollback-{}", uuid::Uuid::now_v7()));
    let db = tmp.join("test.sqlite3");
    let blob_dir = tmp.join("blobs");
    let store = SqlitePersistence::open(&db, &blob_dir, &[7; 32]).unwrap();
    for index in 0..513 {
        store
            .append_observation_idempotent(&sample_observation_with_identity(
                &format!("rollback-{index:04}"),
                "rollback",
            ))
            .unwrap();
    }
    let bad_append_seq = 513_u64;
    let valid_json: String = store
        .conn
        .query_row(
            "SELECT observation_json FROM observations WHERE append_seq = ?1",
            [bad_append_seq],
            |row| row.get(0),
        )
        .unwrap();
    store
        .conn
        .execute_batch(
            "
            DROP TRIGGER cutover_transition_log_no_update;
            DROP TRIGGER cutover_transition_log_no_delete;
            DROP INDEX cutover_credentials_active;
            DROP INDEX cutover_transition_unit_seq;
            DROP INDEX identity_bridge_gaps_source_append;
            DROP INDEX identity_bridge_candidates_source_append;
            DROP INDEX identity_bridge_candidates_key_append;
            DROP TABLE cutover_unit_metrics;
            DROP TABLE cutover_credentials;
            DROP TABLE cutover_transition_log;
            DROP TABLE identity_bridge_watermark;
            DROP TABLE identity_bridge_gaps;
            DROP TABLE identity_bridge_candidates;
            DROP INDEX observation_privacy_keys_append;
            DROP TABLE observation_privacy_keys;
            DROP TABLE retired_projection_materializations;
            DROP TABLE projection_materialization_heads;
            DELETE FROM schema_migrations WHERE version >= 13;
            ",
        )
        .unwrap();
    store
        .conn
        .execute(
            "UPDATE observations SET observation_json = ?1 WHERE append_seq = ?2",
            rusqlite::params!["{not-json", bad_append_seq],
        )
        .unwrap();
    drop(store);

    assert!(SqlitePersistence::open(&db, &blob_dir, &[7; 32]).is_err());

    let repair = rusqlite::Connection::open(&db).unwrap();
    let table_count: i64 = repair
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'observation_privacy_keys'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(table_count, 0);
    let migration_count: i64 = repair
        .query_row(
            "SELECT COUNT(*) FROM schema_migrations WHERE version >= 13",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(migration_count, 0);
    repair
        .execute(
            "UPDATE observations SET observation_json = ?1 WHERE append_seq = ?2",
            rusqlite::params![valid_json, bad_append_seq],
        )
        .unwrap();
    drop(repair);

    let migrated = SqlitePersistence::open(&db, &blob_dir, &[7; 32]).unwrap();
    assert_eq!(
        migrated
            .observations_for_privacy_key("entity:test")
            .unwrap()
            .len(),
        513
    );
    drop(migrated);
    let _ = fs::remove_dir_all(tmp);
}

#[test]
fn schema_v15_converges_from_fresh_v12_and_v13_upgrade_paths() {
    let fresh_tmp = std::env::temp_dir().join(format!("lethe-test-{}", uuid::Uuid::now_v7()));
    let fresh = SqlitePersistence::open(
        &fresh_tmp.join("test.sqlite3"),
        &fresh_tmp.join("blobs"),
        &[7; 32],
    )
    .unwrap();
    let fresh_signature = schema_object_signature(&fresh);
    let current_ledger = vec![
        (
            SCHEMA_VERSION_IDENTITY_LOOKUP_INDEX,
            "observation_identity_lookup_index".to_owned(),
        ),
        (
            SCHEMA_VERSION_LOCK_SPLIT_SCALARS,
            "append_commit_lock_split_scalars".to_owned(),
        ),
        (
            SCHEMA_VERSION_KEYSET_READS,
            "indexed_keyset_reads".to_owned(),
        ),
        (
            SCHEMA_VERSION_PRIVACY_PROJECTION,
            "privacy_projection_visibility".to_owned(),
        ),
        (
            SCHEMA_VERSION_RECONSENT_PRIVACY_INDEX,
            "reconsent_privacy_reverse_index".to_owned(),
        ),
        (
            SCHEMA_VERSION_CUTOVER_BRIDGE,
            "v1_v2_cutover_bridge".to_owned(),
        ),
        (
            SCHEMA_VERSION_ATOMIC_PROJECTION_HEAD,
            "atomic_projection_generation_head".to_owned(),
        ),
    ];
    assert_eq!(migration_ledger(&fresh), current_ledger);
    drop(fresh);

    let v12_tmp = std::env::temp_dir().join(format!("lethe-test-{}", uuid::Uuid::now_v7()));
    {
        let seed = SqlitePersistence::open(
            &v12_tmp.join("test.sqlite3"),
            &v12_tmp.join("blobs"),
            &[7; 32],
        )
        .unwrap();
        seed.append_observation_idempotent(&sample_observation())
            .unwrap();
        seed.conn
            .execute_batch(
                "
                DROP TRIGGER cutover_transition_log_no_update;
                DROP TRIGGER cutover_transition_log_no_delete;
                DROP INDEX cutover_credentials_active;
                DROP INDEX cutover_transition_unit_seq;
                DROP INDEX identity_bridge_gaps_source_append;
                DROP INDEX identity_bridge_candidates_source_append;
                DROP INDEX identity_bridge_candidates_key_append;
                DROP TABLE cutover_unit_metrics;
                DROP TABLE cutover_credentials;
                DROP TABLE cutover_transition_log;
                DROP TABLE identity_bridge_watermark;
                DROP TABLE identity_bridge_gaps;
                DROP TABLE identity_bridge_candidates;
                DROP INDEX observation_privacy_keys_append;
                DROP TABLE observation_privacy_keys;
                DROP TABLE retired_projection_materializations;
                DROP TABLE projection_materialization_heads;
                DELETE FROM schema_migrations WHERE version >= 13;
                ",
            )
            .unwrap();
    }
    let v12 = SqlitePersistence::open(
        &v12_tmp.join("test.sqlite3"),
        &v12_tmp.join("blobs"),
        &[7; 32],
    )
    .unwrap();
    assert_eq!(schema_object_signature(&v12), fresh_signature);
    assert_eq!(migration_ledger(&v12), current_ledger);
    assert_eq!(v12.observation_stats().unwrap().count, 1);
    assert_eq!(
        v12.observations_for_privacy_key("entity:test")
            .unwrap()
            .len(),
        1
    );
    drop(v12);

    let v13_tmp = std::env::temp_dir().join(format!("lethe-test-{}", uuid::Uuid::now_v7()));
    {
        let seed = SqlitePersistence::open(
            &v13_tmp.join("test.sqlite3"),
            &v13_tmp.join("blobs"),
            &[7; 32],
        )
        .unwrap();
        seed.append_observation_idempotent(&sample_observation())
            .unwrap();
        seed.conn
            .execute_batch(
                "
                DROP TRIGGER cutover_transition_log_no_update;
                DROP TRIGGER cutover_transition_log_no_delete;
                DROP INDEX cutover_credentials_active;
                DROP INDEX cutover_transition_unit_seq;
                DROP INDEX identity_bridge_gaps_source_append;
                DROP INDEX identity_bridge_candidates_source_append;
                DROP INDEX identity_bridge_candidates_key_append;
                DROP TABLE cutover_unit_metrics;
                DROP TABLE cutover_credentials;
                DROP TABLE cutover_transition_log;
                DROP TABLE identity_bridge_watermark;
                DROP TABLE identity_bridge_gaps;
                DROP TABLE identity_bridge_candidates;
                DROP TABLE retired_projection_materializations;
                DROP TABLE projection_materialization_heads;
                DELETE FROM schema_migrations WHERE version >= 14;
                ",
            )
            .unwrap();
    }
    let v13 = SqlitePersistence::open(
        &v13_tmp.join("test.sqlite3"),
        &v13_tmp.join("blobs"),
        &[7; 32],
    )
    .unwrap();
    assert_eq!(schema_object_signature(&v13), fresh_signature);
    assert_eq!(migration_ledger(&v13), current_ledger);
    assert_eq!(v13.observation_stats().unwrap().count, 1);

    let _ = fs::remove_dir_all(fresh_tmp);
    let _ = fs::remove_dir_all(v12_tmp);
    let _ = fs::remove_dir_all(v13_tmp);
}

#[test]
fn schema_v15_upgrades_true_v14_projection_shape_and_backfills_heads() {
    let tmp = std::env::temp_dir().join(format!(
        "lethe-v14-projection-upgrade-{}",
        uuid::Uuid::now_v7()
    ));
    let database_path = tmp.join("test.sqlite3");
    let blob_dir = tmp.join("blobs");
    let projection = lethe_core::domain::ProjectionRef::new("proj:v14-upgrade");
    let expected_item = ProjectionItem {
        item_key: "item-v14".to_owned(),
        owner_key: "owner-v14".to_owned(),
        sort_key: "001".to_owned(),
        value: serde_json::json!({
            "blob": format!("blob:sha256:{}", "b".repeat(64))
        }),
    };
    {
        let store = SqlitePersistence::open(&database_path, &blob_dir, &[7; 32]).unwrap();
        store
            .commit_projection_items(
                &projection,
                &serde_json::json!({"schema": 14}),
                &ProjectionItemCommit::Replace {
                    items: vec![expected_item.clone()],
                },
            )
            .unwrap();
        let storage_projection_id = active_storage_projection_id(&store.conn, &projection)
            .unwrap()
            .unwrap();
        store.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        store
            .conn
            .execute(
                "UPDATE projection_materialization_items
                 SET projection_id = ?1
                 WHERE projection_id = ?2",
                params![projection.as_str(), storage_projection_id],
            )
            .unwrap();
        store
            .conn
            .execute(
                "UPDATE projection_visible_blob_refs
                 SET projection_id = ?1
                 WHERE projection_id = ?2",
                params![projection.as_str(), storage_projection_id],
            )
            .unwrap();
        store
            .conn
            .execute_batch(
                "
                DROP TABLE retired_projection_materializations;
                DROP TABLE projection_materialization_heads;
                DELETE FROM schema_migrations WHERE version = 15;
                COMMIT;
                ",
            )
            .unwrap();
    }

    let upgraded = SqlitePersistence::open(&database_path, &blob_dir, &[7; 32]).unwrap();
    assert!(upgraded.schema_migrations_applied_on_open());
    assert_eq!(
        active_storage_projection_id(&upgraded.conn, &projection)
            .unwrap()
            .as_deref(),
        Some(projection.as_str())
    );
    assert_eq!(
        upgraded
            .projection_items_by_owner(&projection, "owner-v14")
            .unwrap(),
        vec![expected_item]
    );
    assert_eq!(
        upgraded
            .conn
            .query_row(
                "SELECT name FROM schema_migrations WHERE version = 15",
                [],
                |row| row.get::<_, String>(0),
            )
            .unwrap(),
        "atomic_projection_generation_head"
    );

    let _ = fs::remove_dir_all(tmp);
}

#[test]
fn schema_v13_upgrades_true_v11_operational_event_shape() {
    let tmp = std::env::temp_dir().join(format!("lethe-test-{}", uuid::Uuid::now_v7()));
    let database_path = tmp.join("operational.sqlite3");
    let blob_dir = tmp.join("blobs");
    let data_space = lethe_core::domain::DataSpaceId::new("space:v10-upgrade");
    {
        let seed = SqliteOperationalEventStore::open(
            data_space.clone(),
            &database_path,
            &blob_dir,
            &[7; 32],
        )
        .unwrap();
        let mut event = lethe_storage_api::conformance::sample_operational_event(
            &data_space,
            "event:v10-upgrade",
            "stream:v10-upgrade",
            1,
            "idempotency:v10-upgrade",
        );
        event.actor_id = Some("actor:v10".to_owned());
        event.causation_id = Some(lethe_core::domain::OperationalEventId::new(
            "event:caused-by-v10",
        ));
        event.correlation_id = Some("correlation:v10".to_owned());
        seed.append_operational_event(&OperationalAppendRequest {
            expected_stream_version: 0,
            event,
        })
        .unwrap();

        seed.persistence()
            .conn
            .execute_batch(
                "
                DROP INDEX operational_events_correlation_cursor;
                DROP INDEX operational_events_causation_cursor;
                DROP INDEX operational_events_actor_cursor;
                ALTER TABLE operational_events DROP COLUMN correlation_id;
                ALTER TABLE operational_events DROP COLUMN causation_id;
                ALTER TABLE operational_events DROP COLUMN actor_id;
                DROP INDEX projection_visible_blob_refs_subject_lookup;
                ALTER TABLE projection_visible_blob_refs DROP COLUMN subject_key;
                DROP TABLE retired_projection_materializations;
                DROP TABLE projection_materialization_heads;
                DELETE FROM schema_migrations WHERE version >= 11;
                ",
            )
            .unwrap();
        let columns = connection_table_columns(seed.persistence());
        assert!(!columns.contains("correlation_id"));
        assert!(!columns.contains("causation_id"));
        assert!(!columns.contains("actor_id"));
        let visible_columns = seed
            .persistence()
            .conn
            .prepare("PRAGMA table_info(projection_visible_blob_refs)")
            .unwrap()
            .query_map([], |row| row.get::<_, String>(1))
            .unwrap()
            .collect::<Result<std::collections::BTreeSet<_>, _>>()
            .unwrap();
        assert!(!visible_columns.contains("subject_key"));
    }

    let upgraded =
        SqliteOperationalEventStore::open(data_space, &database_path, &blob_dir, &[7; 32]).unwrap();
    let columns = connection_table_columns(upgraded.persistence());
    assert!(columns.contains("correlation_id"));
    assert!(columns.contains("causation_id"));
    assert!(columns.contains("actor_id"));
    let visible_columns = upgraded
        .persistence()
        .conn
        .prepare("PRAGMA table_info(projection_visible_blob_refs)")
        .unwrap()
        .query_map([], |row| row.get::<_, String>(1))
        .unwrap()
        .collect::<Result<std::collections::BTreeSet<_>, _>>()
        .unwrap();
    assert!(visible_columns.contains("subject_key"));
    let scalar_values: (Option<String>, Option<String>, Option<String>) = upgraded
        .persistence()
        .conn
        .query_row(
            "SELECT correlation_id, causation_id, actor_id
             FROM operational_events WHERE event_id = 'event:v10-upgrade'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(
        scalar_values,
        (
            Some("correlation:v10".to_owned()),
            Some("event:caused-by-v10".to_owned()),
            Some("actor:v10".to_owned()),
        )
    );
    for index in [
        "operational_events_correlation_cursor",
        "operational_events_causation_cursor",
        "operational_events_actor_cursor",
    ] {
        let exists: Option<i64> = upgraded
            .persistence()
            .conn
            .query_row(
                "SELECT 1 FROM sqlite_master WHERE type = 'index' AND name = ?1",
                [index],
                |row| row.get(0),
            )
            .optional()
            .unwrap();
        assert_eq!(exists, Some(1), "missing migrated index {index}");
    }

    let _ = fs::remove_dir_all(tmp);
}

fn connection_table_columns(store: &SqlitePersistence) -> std::collections::BTreeSet<String> {
    store
        .conn
        .prepare("PRAGMA table_info(operational_events)")
        .unwrap()
        .query_map([], |row| row.get::<_, String>(1))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}

fn migration_ledger(store: &SqlitePersistence) -> Vec<(i64, String)> {
    let mut statement = store
        .conn
        .prepare("SELECT version, name FROM schema_migrations ORDER BY version")
        .unwrap();
    statement
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
}

fn schema_object_signature(store: &SqlitePersistence) -> Vec<(String, String, String)> {
    let mut statement = store
        .conn
        .prepare(
            "SELECT type, name, sql
             FROM sqlite_master
             WHERE name NOT LIKE 'sqlite_%'
             ORDER BY type, name",
        )
        .unwrap();
    statement
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
}

#[test]
fn open_migrates_legacy_canonical_json_column_and_keeps_bulk_dedupe_idempotent() {
    let tmp = std::env::temp_dir().join(format!("lethe-test-{}", uuid::Uuid::now_v7()));
    let db = tmp.join("test.sqlite3");
    let blob_dir = tmp.join("blobs");
    let legacy_observation = sample_observation();
    {
        let store = SqlitePersistence::open(&db, &blob_dir, &[7; 32]).unwrap();
        replace_with_legacy_canonical_json_observations_table(&store, &legacy_observation);
    }

    let store = SqlitePersistence::open(&db, &blob_dir, &[7; 32]).unwrap();
    let columns = {
        let mut statement = store
            .conn
            .prepare("PRAGMA table_info(observations)")
            .unwrap();
        let rows = statement
            .query_map([], |row| row.get::<_, String>(1))
            .unwrap();
        rows.collect::<Result<Vec<_>, _>>().unwrap()
    };
    assert!(columns.contains(&"leaf_id".to_owned()));
    assert!(columns.contains(&"routing_key".to_owned()));
    assert!(columns.contains(&"canonical_json_sha256".to_owned()));
    assert!(!columns.contains(&"canonical_json".to_owned()));
    let stored_hash: String = store
        .conn
        .query_row(
            "SELECT canonical_json_sha256 FROM observations WHERE id = ?1",
            [legacy_observation.id.as_str()],
            |row| row.get(0),
        )
        .unwrap();
    let canonical_json = legacy_observation
        .meta
        .get(CANONICAL_JSON_META_KEY)
        .and_then(serde_json::Value::as_str)
        .unwrap();
    assert_eq!(stored_hash, canonical_json_sha256(canonical_json));
    let registry_count: i64 = store
        .conn
        .query_row(
            "SELECT COUNT(*) FROM observation_identity_registry WHERE identity_key = ?1",
            [legacy_observation.idempotency_key.as_str()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(registry_count, 1);

    let mut duplicate = legacy_observation.clone();
    duplicate.id = Observation::new_id();
    let fresh = sample_observation_with_identity("legacy-migration-fresh", "fresh");
    let outcomes = store
        .append_observations_idempotent(&[duplicate, fresh.clone()])
        .unwrap();

    assert_eq!(
        outcomes,
        vec![
            DurableAppendOutcome::Duplicate(legacy_observation.id.clone()),
            DurableAppendOutcome::Appended(fresh.id.clone()),
        ]
    );
    assert_eq!(store.load_observations().unwrap().len(), 2);

    let _ = fs::remove_dir_all(tmp);
}

#[test]
fn schema_v8_backfill_keeps_oldest_cross_leaf_identity_duplicate() {
    let tmp = std::env::temp_dir().join(format!("lethe-test-{}", uuid::Uuid::now_v7()));
    let db = tmp.join("test.sqlite3");
    let blob_dir = tmp.join("blobs");
    let store = SqlitePersistence::open(&db, &blob_dir, &[7; 32]).unwrap();
    let oldest = sample_observation_with_identity("legacy-cross-leaf", "stable");
    let mut newer = oldest.clone();
    newer.id = Observation::new_id();

    store
        .conn
        .execute_batch(
            "
            DROP INDEX IF EXISTS observations_leaf_append;
            DROP TABLE observations;
            DROP TABLE observation_identity_registry;
            DROP TABLE retired_projection_materializations;
            DROP TABLE projection_materialization_heads;
            DELETE FROM schema_migrations WHERE version >= 9;
            INSERT INTO schema_migrations (version, name, applied_at)
            VALUES (8, 'global_observation_identity_registry', '2026-07-22T00:00:00Z');
            CREATE TABLE observations (
                append_seq INTEGER PRIMARY KEY AUTOINCREMENT,
                id TEXT NOT NULL UNIQUE,
                leaf_id TEXT NOT NULL CHECK (leaf_id LIKE 'lake:%'),
                routing_key TEXT NOT NULL,
                identity_key TEXT NOT NULL,
                canonical_json_sha256 TEXT NOT NULL,
                recorded_at TEXT NOT NULL,
                observation_json TEXT NOT NULL,
                UNIQUE (leaf_id, identity_key)
            );
            ",
        )
        .unwrap();
    for (append_seq, leaf_id, observation) in [
        (2_i64, "lake:newer", &newer),
        (1_i64, "lake:oldest", &oldest),
    ] {
        let canonical_json = observation
            .meta
            .get(CANONICAL_JSON_META_KEY)
            .and_then(serde_json::Value::as_str)
            .unwrap();
        store
            .conn
            .execute(
                "INSERT INTO observations (
                    append_seq, id, leaf_id, routing_key, identity_key,
                    canonical_json_sha256, recorded_at, observation_json
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                params![
                    append_seq,
                    observation.id.as_str(),
                    leaf_id,
                    format!("routing:{leaf_id}"),
                    observation.idempotency_key.as_str(),
                    canonical_json_sha256(canonical_json),
                    observation.recorded_at.to_rfc3339(),
                    serde_json::to_string(observation).unwrap(),
                ],
            )
            .unwrap();
    }
    drop(store);

    let store = SqlitePersistence::open(&db, &blob_dir, &[7; 32]).unwrap();
    let (winner_id, registry_count): (String, i64) = store
        .conn
        .query_row(
            "SELECT (
                 SELECT observation_id
                 FROM observation_identity_registry
                 WHERE identity_key = ?1
             ), (
                 SELECT COUNT(*)
                 FROM observation_identity_registry
                 WHERE identity_key = ?1
             )",
            [oldest.idempotency_key.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(winner_id, oldest.id.as_str());
    assert_eq!(registry_count, 1);

    let mut retry = oldest.clone();
    retry.id = Observation::new_id();
    assert_eq!(
        store.append_observation_idempotent(&retry).unwrap(),
        DurableAppendOutcome::Duplicate(oldest.id.clone())
    );
    assert_eq!(store.load_observations().unwrap().len(), 2);

    let _ = fs::remove_dir_all(tmp);
}

#[test]
fn slack_thread_append_catalog_and_due_queue_are_durable() {
    let tmp = std::env::temp_dir().join(format!("lethe-test-{}", uuid::Uuid::now_v7()));
    let db = tmp.join("test.sqlite3");
    let blob_dir = tmp.join("blobs");
    let key = SlackThreadKey {
        source_instance: "slack-primary".into(),
        channel_id: "C01ABC".into(),
        thread_ts: "1700000000.000001".into(),
    };
    {
        let store = SqlitePersistence::open(&db, &blob_dir, &[7; 32]).unwrap();
        let observation = sample_observation();
        assert!(matches!(
            store.append_slack_observation(&observation, &key).unwrap(),
            DurableAppendOutcome::Appended(_)
        ));
        assert_eq!(store.slack_thread_discovery_high_water().unwrap(), 0);
        let catalog = store
            .slack_thread_catalog("slack-primary", "C01ABC")
            .unwrap();
        assert_eq!(catalog.len(), 1);
        assert_eq!(catalog[0].key, key);
        assert_eq!(catalog[0].reply_cursor, "1700000000.000001");
        assert!(catalog[0].active);
        assert_eq!(catalog[0].discovered_append_seq, 1);
    }

    let store = SqlitePersistence::open(&db, &blob_dir, &[7; 32]).unwrap();
    let generation = store.advance_slack_thread_poll_generation().unwrap();
    let due = store
        .slack_threads_to_poll("slack-primary", "C01ABC", generation, 10)
        .unwrap();
    assert_eq!(due.len(), 1);
    store
        .complete_slack_thread_poll(&key, generation, "1700000000.000002", false, generation + 8)
        .unwrap();
    let next_generation = store.advance_slack_thread_poll_generation().unwrap();
    assert!(
        store
            .slack_threads_to_poll("slack-primary", "C01ABC", next_generation, 10)
            .unwrap()
            .is_empty()
    );
    let mut due_generation = next_generation;
    while due_generation < generation + 8 {
        due_generation = store.advance_slack_thread_poll_generation().unwrap();
    }
    assert_eq!(
        store
            .slack_threads_to_poll("slack-primary", "C01ABC", due_generation, 10)
            .unwrap()
            .len(),
        1
    );

    let _ = fs::remove_dir_all(tmp);
}

#[test]
fn slack_thread_discovery_rolls_back_catalog_when_high_water_batch_is_invalid() {
    let tmp = std::env::temp_dir().join(format!("lethe-test-{}", uuid::Uuid::now_v7()));
    let db = tmp.join("test.sqlite3");
    let blob_dir = tmp.join("blobs");
    let store = SqlitePersistence::open(&db, &blob_dir, &[7; 32]).unwrap();
    store
        .append_observation_idempotent(&sample_observation())
        .unwrap();
    let key = SlackThreadKey {
        source_instance: "slack-primary".into(),
        channel_id: "C01ABC".into(),
        thread_ts: "1700000000.000001".into(),
    };

    let error = store
        .commit_slack_thread_discovery(
            1,
            &[DiscoveredSlackThread {
                key,
                observation_append_seq: 2,
            }],
        )
        .unwrap_err();
    assert!(matches!(error, PersistenceError::SchemaInvariant(_)));
    assert_eq!(store.slack_thread_discovery_high_water().unwrap(), 0);
    assert!(
        store
            .slack_thread_catalog("slack-primary", "C01ABC")
            .unwrap()
            .is_empty()
    );

    let _ = fs::remove_dir_all(tmp);
}

fn projection_item(item_key: &str, owner_key: &str, sort_key: &str) -> ProjectionItem {
    ProjectionItem {
        item_key: item_key.to_owned(),
        owner_key: owner_key.to_owned(),
        sort_key: sort_key.to_owned(),
        value: serde_json::json!({"item": item_key}),
    }
}

#[test]
fn projection_item_replace_reopens_with_owner_isolation_and_stable_order() {
    let tmp = std::env::temp_dir().join(format!("lethe-test-{}", uuid::Uuid::now_v7()));
    let db = tmp.join("test.sqlite3");
    let blob_dir = tmp.join("blobs");
    let projection = lethe_core::domain::ProjectionRef::new("proj:item-test");
    let store = SqlitePersistence::open(&db, &blob_dir, &[7; 32]).unwrap();

    store
        .commit_projection_items(
            &projection,
            &serde_json::json!({"generation": 1}),
            &ProjectionItemCommit::Replace {
                items: vec![projection_item("obsolete", "owner-a", "999")],
            },
        )
        .unwrap();
    store
        .commit_projection_items(
            &projection,
            &serde_json::json!({"generation": 2}),
            &ProjectionItemCommit::Replace {
                items: vec![
                    projection_item("item-z", "owner-a", "001"),
                    projection_item("item-b", "owner-a", "000"),
                    projection_item("item-a", "owner-a", "001"),
                    projection_item("item-other", "owner-b", "000"),
                ],
            },
        )
        .unwrap();

    assert_eq!(
        store
            .projection_items_by_owner(&projection, "owner-a")
            .unwrap()
            .into_iter()
            .map(|item| item.item_key)
            .collect::<Vec<_>>(),
        vec!["item-b", "item-a", "item-z"]
    );
    assert_eq!(
        store
            .projection_items_by_owner(&projection, "owner-b")
            .unwrap()
            .into_iter()
            .map(|item| item.item_key)
            .collect::<Vec<_>>(),
        vec!["item-other"]
    );
    assert_eq!(
        store
            .projection_item_count_by_owner(&projection, "owner-a")
            .unwrap(),
        3
    );
    assert_eq!(store.projection_item_count(&projection).unwrap(), 4);
    assert_eq!(
        store
            .projection_item_count_by_owner(&projection, "missing-owner")
            .unwrap(),
        0
    );
    drop(store);

    let reopened = SqlitePersistence::open(&db, &blob_dir, &[7; 32]).unwrap();
    assert_eq!(
        reopened.projection_records(&projection).unwrap(),
        Some(serde_json::json!({"generation": 2}))
    );
    assert_eq!(
        reopened
            .projection_items_by_owner(&projection, "owner-a")
            .unwrap()
            .into_iter()
            .map(|item| item.item_key)
            .collect::<Vec<_>>(),
        vec!["item-b", "item-a", "item-z"]
    );
    assert!(
        reopened
            .projection_items_by_owner(&projection, "owner-b ")
            .unwrap()
            .is_empty(),
        "owner lookup must use exact equality"
    );
    drop(reopened);
    let _ = fs::remove_dir_all(tmp);
}

#[test]
fn persisted_sync_state_round_trip_is_strict_and_restart_safe() {
    let tmp = std::env::temp_dir().join(format!("lethe-test-{}", uuid::Uuid::now_v7()));
    let db = tmp.join("test.sqlite3");
    let blob_dir = tmp.join("blobs");
    let completed_at = "2026-07-23T12:34:56Z".parse().unwrap();
    let store = SqlitePersistence::open(&db, &blob_dir, &[7; 32]).unwrap();
    store
        .record_sync_state(
            "all",
            &PersistedSyncState {
                metrics: SyncMetricRecord {
                    fetched: 11,
                    ingested: 7,
                    skipped: 2,
                    failed: 1,
                    quarantined: 1,
                    latency_ms: 321,
                },
                completed_at,
                error: Some("one source failed".to_owned()),
            },
        )
        .unwrap();
    drop(store);

    let reopened = SqlitePersistence::open(&db, &blob_dir, &[7; 32]).unwrap();
    assert_eq!(
        reopened.load_sync_state("all").unwrap(),
        Some(PersistedSyncState {
            metrics: SyncMetricRecord {
                fetched: 11,
                ingested: 7,
                skipped: 2,
                failed: 1,
                quarantined: 1,
                latency_ms: 321,
            },
            completed_at,
            error: Some("one source failed".to_owned()),
        })
    );
    assert!(reopened.load_sync_state("missing").unwrap().is_none());

    let _ = fs::remove_dir_all(tmp);
}

#[test]
fn visible_blob_reference_index_tracks_projection_item_commit_atomically() {
    let tmp = std::env::temp_dir().join(format!("lethe-test-{}", uuid::Uuid::now_v7()));
    let db = tmp.join("test.sqlite3");
    let blob_dir = tmp.join("blobs");
    let projection = lethe_core::domain::ProjectionRef::new("proj:blob-test");
    let blob_ref = lethe_core::domain::BlobRef::new(format!("blob:sha256:{}", "a".repeat(64)));
    let store = SqlitePersistence::open(&db, &blob_dir, &[7; 32]).unwrap();

    store
        .commit_projection_items(
            &projection,
            &serde_json::json!({"generation": 1}),
            &ProjectionItemCommit::Replace {
                items: vec![
                    ProjectionItem {
                        item_key: "person-component:person-1".to_owned(),
                        owner_key: "__person_components__".to_owned(),
                        sort_key: "person-1".to_owned(),
                        value: serde_json::json!({"image": blob_ref.as_str()}),
                    },
                    ProjectionItem {
                        item_key: "person-component:person-2".to_owned(),
                        owner_key: "__person_components__".to_owned(),
                        sort_key: "person-2".to_owned(),
                        value: serde_json::json!({"image": blob_ref.as_str()}),
                    },
                ],
            },
        )
        .unwrap();
    assert!(
        store
            .projection_blob_ref_visible(&projection, &blob_ref)
            .unwrap()
    );
    let storage_projection_id = active_storage_projection_id(&store.conn, &projection)
        .unwrap()
        .unwrap();
    let subject_key: String = store
        .conn
        .query_row(
            "SELECT subject_key FROM projection_visible_blob_refs
             WHERE projection_id = ?1 AND blob_ref = ?2",
            rusqlite::params![storage_projection_id, blob_ref.as_str()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(subject_key, "person-component:person-1");
    let visible_subjects: Vec<String> = store
        .conn
        .prepare(
            "SELECT subject_key FROM projection_visible_blob_refs
             WHERE projection_id = ?1 AND blob_ref = ?2 ORDER BY subject_key",
        )
        .unwrap()
        .query_map(
            rusqlite::params![storage_projection_id, blob_ref.as_str()],
            |row| row.get(0),
        )
        .unwrap()
        .map(|row| row.unwrap())
        .collect();
    assert_eq!(
        visible_subjects,
        vec![
            "person-component:person-1".to_owned(),
            "person-component:person-2".to_owned()
        ]
    );

    store
        .commit_projection_items(
            &projection,
            &serde_json::json!({"generation": 2}),
            &ProjectionItemCommit::Delta {
                inserts: Vec::new(),
                updates: vec![ProjectionItem {
                    item_key: "person-component:person-1".to_owned(),
                    owner_key: "__person_components__".to_owned(),
                    sort_key: "person-1".to_owned(),
                    value: serde_json::json!({"image": "redacted"}),
                }],
                deletes: Vec::new(),
            },
        )
        .unwrap();
    assert!(
        store
            .projection_blob_ref_visible(&projection, &blob_ref)
            .unwrap()
    );
    let remaining_subject: String = store
        .conn
        .query_row(
            "SELECT subject_key FROM projection_visible_blob_refs
             WHERE projection_id = ?1 AND blob_ref = ?2",
            rusqlite::params![storage_projection_id, blob_ref.as_str()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(remaining_subject, "person-component:person-2");

    let _ = fs::remove_dir_all(tmp);
}

#[test]
fn projection_item_keyset_boundary_keeps_same_sort_key_and_uses_owner_index() {
    let tmp = std::env::temp_dir().join(format!("lethe-test-{}", uuid::Uuid::now_v7()));
    let store =
        SqlitePersistence::open(&tmp.join("test.sqlite3"), &tmp.join("blobs"), &[7; 32]).unwrap();
    let projection = lethe_core::domain::ProjectionRef::new("proj:keyset-test");
    store
        .commit_projection_items(
            &projection,
            &serde_json::json!({"generation": 1}),
            &ProjectionItemCommit::Replace {
                items: vec![
                    projection_item("item-a", "owner", "001"),
                    projection_item("item-b", "owner", "001"),
                    projection_item("item-c", "owner", "002"),
                ],
            },
        )
        .unwrap();
    let first = store
        .projection_items_page(&projection, &["owner".to_owned()], None, None, 2)
        .unwrap();
    assert_eq!(
        first
            .iter()
            .map(|item| item.item_key.as_str())
            .collect::<Vec<_>>(),
        ["item-a", "item-b"]
    );
    let after = format!("{}\u{001f}{}", first[1].sort_key, first[1].item_key);
    let second = store
        .projection_items_page(&projection, &["owner".to_owned()], None, Some(&after), 2)
        .unwrap();
    assert_eq!(
        second
            .iter()
            .map(|item| item.item_key.as_str())
            .collect::<Vec<_>>(),
        ["item-c"]
    );

    let mut statement = store
        .conn
        .prepare(
            "EXPLAIN QUERY PLAN
             SELECT item_key FROM projection_materialization_items
             WHERE projection_id = ?1 AND owner_key = ?2 AND sort_key > ?3
             ORDER BY sort_key, item_key LIMIT ?4",
        )
        .unwrap();
    let plan = statement
        .query_map(
            rusqlite::params!["proj:keyset-test", "owner", "000", 2],
            |row| row.get::<_, String>(3),
        )
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert!(
        plan.iter()
            .any(|detail| detail.contains("projection_materialization_items_owner_order")),
        "keyset page did not use the owner/sort index: {plan:?}"
    );
    let _ = fs::remove_dir_all(tmp);
}

#[test]
fn projection_item_delta_inserts_updates_and_deletes_atomically() {
    let tmp = std::env::temp_dir().join(format!("lethe-test-{}", uuid::Uuid::now_v7()));
    let db = tmp.join("test.sqlite3");
    let blob_dir = tmp.join("blobs");
    let projection = lethe_core::domain::ProjectionRef::new("proj:item-delta");
    let store = SqlitePersistence::open(&db, &blob_dir, &[7; 32]).unwrap();
    store
        .commit_projection_items(
            &projection,
            &serde_json::json!({"generation": 1}),
            &ProjectionItemCommit::Replace {
                items: vec![
                    projection_item("item-a", "owner-a", "001"),
                    projection_item("item-b", "owner-a", "002"),
                ],
            },
        )
        .unwrap();

    let mut updated = projection_item("item-b", "owner-b", "003");
    updated.value = serde_json::json!({"updated": true});
    store
        .commit_projection_items(
            &projection,
            &serde_json::json!({"generation": 2}),
            &ProjectionItemCommit::Delta {
                inserts: vec![projection_item("item-c", "owner-a", "000")],
                updates: vec![updated.clone()],
                deletes: vec!["item-a".to_owned()],
            },
        )
        .unwrap();

    assert_eq!(
        store.projection_records(&projection).unwrap(),
        Some(serde_json::json!({"generation": 2}))
    );
    assert_eq!(
        store
            .projection_items_by_owner(&projection, "owner-a")
            .unwrap(),
        vec![projection_item("item-c", "owner-a", "000")]
    );
    assert_eq!(
        store
            .projection_items_by_owner(&projection, "owner-b")
            .unwrap(),
        vec![updated]
    );
    assert_eq!(
        store
            .projection_item_count_by_owner(&projection, "owner-a")
            .unwrap(),
        1
    );
    assert_eq!(store.projection_item_count(&projection).unwrap(), 2);

    let _ = fs::remove_dir_all(tmp);
}

#[test]
fn supplemental_and_projection_delta_commit_atomically() {
    let tmp = std::env::temp_dir().join(format!("lethe-test-{}", uuid::Uuid::now_v7()));
    let db = tmp.join("test.sqlite3");
    let blob_dir = tmp.join("blobs");
    let projection = lethe_core::domain::ProjectionRef::new("proj:supplemental-atomic");
    let store = SqlitePersistence::open(&db, &blob_dir, &[7; 32]).unwrap();
    let original = projection_item("item-a", "owner-a", "001");
    store
        .commit_projection_items(
            &projection,
            &serde_json::json!({"generation": 1}),
            &ProjectionItemCommit::Replace {
                items: vec![original],
            },
        )
        .unwrap();
    let observation = sample_observation();
    let supplemental = sample_supplemental_with_id("sup:atomic-success", &observation.id);
    let mut updated = projection_item("item-a", "owner-b", "002");
    updated.value = serde_json::json!({"updated": true});
    let inserted = projection_item("item-b", "owner-b", "003");
    let manifest = serde_json::json!({"generation": 2, "item_count": 2});

    store
        .commit_supplemental_and_projection(
            &supplemental,
            &projection,
            &manifest,
            &ProjectionItemCommit::Delta {
                inserts: vec![inserted.clone()],
                updates: vec![updated.clone()],
                deletes: vec![],
            },
        )
        .unwrap();

    let stored = store.supplemental_by_id(&supplemental.id).unwrap().unwrap();
    assert_eq!(stored.id, supplemental.id);
    assert_eq!(stored.payload, supplemental.payload);
    assert_eq!(
        store.projection_records(&projection).unwrap(),
        Some(manifest)
    );
    assert_eq!(
        store.projection_item_by_key(&projection, "item-a").unwrap(),
        Some(updated)
    );
    assert_eq!(
        store.projection_item_by_key(&projection, "item-b").unwrap(),
        Some(inserted)
    );

    let _ = fs::remove_dir_all(tmp);
}

#[test]
fn supplemental_projection_duplicate_id_rolls_back_projection_delta_and_manifest() {
    let tmp = std::env::temp_dir().join(format!("lethe-test-{}", uuid::Uuid::now_v7()));
    let db = tmp.join("test.sqlite3");
    let blob_dir = tmp.join("blobs");
    let projection = lethe_core::domain::ProjectionRef::new("proj:supplemental-duplicate");
    let store = SqlitePersistence::open(&db, &blob_dir, &[7; 32]).unwrap();
    let original_manifest = serde_json::json!({"generation": 1});
    let original_item = projection_item("item-a", "owner-a", "001");
    store
        .commit_projection_items(
            &projection,
            &original_manifest,
            &ProjectionItemCommit::Replace {
                items: vec![original_item.clone()],
            },
        )
        .unwrap();
    let observation = sample_observation();
    let original_supplemental =
        sample_supplemental_with_id("sup:atomic-duplicate", &observation.id);
    store.persist_supplemental(&original_supplemental).unwrap();
    let mut duplicate = original_supplemental.clone();
    duplicate.payload = serde_json::json!({"bio_text": "must not replace"});

    let error = store
        .commit_supplemental_and_projection(
            &duplicate,
            &projection,
            &serde_json::json!({"generation": 2}),
            &ProjectionItemCommit::Delta {
                inserts: vec![projection_item("item-b", "owner-a", "002")],
                updates: vec![projection_item("item-a", "owner-b", "003")],
                deletes: vec![],
            },
        )
        .unwrap_err();
    assert!(matches!(
        error,
        PersistenceError::SchemaInvariant(message)
            if message.contains("supplemental append requires absent id")
    ));
    assert_eq!(
        store
            .supplemental_by_id(&original_supplemental.id)
            .unwrap()
            .unwrap()
            .payload,
        original_supplemental.payload
    );
    assert_eq!(
        store.projection_records(&projection).unwrap(),
        Some(original_manifest)
    );
    assert_eq!(
        store.projection_item_by_key(&projection, "item-a").unwrap(),
        Some(original_item)
    );
    assert!(
        store
            .projection_item_by_key(&projection, "item-b")
            .unwrap()
            .is_none()
    );

    let _ = fs::remove_dir_all(tmp);
}

#[test]
fn supplemental_projection_item_precondition_failures_roll_back_everything() {
    let tmp = std::env::temp_dir().join(format!("lethe-test-{}", uuid::Uuid::now_v7()));
    let db = tmp.join("test.sqlite3");
    let blob_dir = tmp.join("blobs");
    let projection = lethe_core::domain::ProjectionRef::new("proj:supplemental-item-failure");
    let store = SqlitePersistence::open(&db, &blob_dir, &[7; 32]).unwrap();
    let original_manifest = serde_json::json!({"generation": 1});
    let original_item = projection_item("item-a", "owner-a", "001");
    store
        .commit_projection_items(
            &projection,
            &original_manifest,
            &ProjectionItemCommit::Replace {
                items: vec![original_item.clone()],
            },
        )
        .unwrap();
    let observation = sample_observation();
    let cases = [
        (
            sample_supplemental_with_id("sup:atomic-update-missing", &observation.id),
            ProjectionItemCommit::Delta {
                inserts: vec![],
                updates: vec![projection_item("missing", "owner-a", "002")],
                deletes: vec![],
            },
            "delta update requires existing item_key missing",
        ),
        (
            sample_supplemental_with_id("sup:atomic-insert-conflict", &observation.id),
            ProjectionItemCommit::Delta {
                inserts: vec![projection_item("item-a", "owner-b", "003")],
                updates: vec![],
                deletes: vec![],
            },
            "delta insert requires absent item_key item-a",
        ),
    ];

    for (record, item_delta, expected_message) in cases {
        let error = store
            .commit_supplemental_and_projection(
                &record,
                &projection,
                &serde_json::json!({"generation": 999}),
                &item_delta,
            )
            .unwrap_err();
        assert!(matches!(
            error,
            PersistenceError::SchemaInvariant(message)
                if message.contains(expected_message)
        ));
        assert!(store.supplemental_by_id(&record.id).unwrap().is_none());
        assert_eq!(
            store.projection_records(&projection).unwrap(),
            Some(original_manifest.clone())
        );
        assert_eq!(
            store.projection_item_by_key(&projection, "item-a").unwrap(),
            Some(original_item.clone())
        );
        assert_eq!(store.projection_item_count(&projection).unwrap(), 1);
    }

    let replace_record =
        sample_supplemental_with_id("sup:atomic-replace-rejected", &observation.id);
    assert!(matches!(
        store.commit_supplemental_and_projection(
            &replace_record,
            &projection,
            &serde_json::json!({"generation": 999}),
            &ProjectionItemCommit::Replace { items: vec![] },
        ),
        Err(PersistenceError::SchemaInvariant(message))
            if message.contains("requires a projection item delta")
    ));
    assert!(
        store
            .supplemental_by_id(&replace_record.id)
            .unwrap()
            .is_none()
    );
    assert_eq!(
        store.projection_records(&projection).unwrap(),
        Some(original_manifest)
    );
    assert_eq!(
        store.projection_item_by_key(&projection, "item-a").unwrap(),
        Some(original_item)
    );

    let _ = fs::remove_dir_all(tmp);
}

#[test]
fn supplemental_projection_manifest_sql_failure_rolls_back_supplemental_and_items() {
    let tmp = std::env::temp_dir().join(format!("lethe-test-{}", uuid::Uuid::now_v7()));
    let db = tmp.join("test.sqlite3");
    let blob_dir = tmp.join("blobs");
    let projection = lethe_core::domain::ProjectionRef::new("proj:supplemental-manifest-failure");
    let store = SqlitePersistence::open(&db, &blob_dir, &[7; 32]).unwrap();
    let original_manifest = serde_json::json!({"generation": 1});
    let original_item = projection_item("item-a", "owner-a", "001");
    store
        .commit_projection_items(
            &projection,
            &original_manifest,
            &ProjectionItemCommit::Replace {
                items: vec![original_item.clone()],
            },
        )
        .unwrap();
    store
        .conn
        .execute_batch(
            "CREATE TRIGGER reject_supplemental_projection_manifest
             BEFORE UPDATE ON projection_materializations
             WHEN OLD.projection_id = 'proj:supplemental-manifest-failure'
             BEGIN
                 SELECT RAISE(ABORT, 'forced supplemental projection manifest failure');
             END;",
        )
        .unwrap();
    let observation = sample_observation();
    let supplemental = sample_supplemental_with_id("sup:atomic-manifest-failure", &observation.id);

    assert!(matches!(
        store.commit_supplemental_and_projection(
            &supplemental,
            &projection,
            &serde_json::json!({"generation": 2}),
            &ProjectionItemCommit::Delta {
                inserts: vec![projection_item("item-b", "owner-a", "002")],
                updates: vec![],
                deletes: vec![],
            },
        ),
        Err(PersistenceError::Sqlite(_))
    ));
    assert!(
        store
            .supplemental_by_id(&supplemental.id)
            .unwrap()
            .is_none()
    );
    assert_eq!(
        store.projection_records(&projection).unwrap(),
        Some(original_manifest)
    );
    assert_eq!(
        store.projection_item_by_key(&projection, "item-a").unwrap(),
        Some(original_item)
    );
    assert!(
        store
            .projection_item_by_key(&projection, "item-b")
            .unwrap()
            .is_none()
    );

    let _ = fs::remove_dir_all(tmp);
}

#[test]
fn invalid_projection_item_commits_do_not_change_manifest_or_items() {
    let tmp = std::env::temp_dir().join(format!("lethe-test-{}", uuid::Uuid::now_v7()));
    let db = tmp.join("test.sqlite3");
    let blob_dir = tmp.join("blobs");
    let projection = lethe_core::domain::ProjectionRef::new("proj:item-invalid");
    let store = SqlitePersistence::open(&db, &blob_dir, &[7; 32]).unwrap();
    let original_manifest = serde_json::json!({"generation": 1});
    let original_item = projection_item("item-a", "owner-a", "001");
    store
        .commit_projection_items(
            &projection,
            &original_manifest,
            &ProjectionItemCommit::Replace {
                items: vec![original_item.clone()],
            },
        )
        .unwrap();

    let invalid_commits = vec![
        ProjectionItemCommit::Replace {
            items: vec![projection_item(" ", "owner-a", "001")],
        },
        ProjectionItemCommit::Replace {
            items: vec![projection_item("item-b", "\t", "001")],
        },
        ProjectionItemCommit::Replace {
            items: vec![projection_item("item-b", "owner-a", "\n")],
        },
        ProjectionItemCommit::Replace {
            items: vec![
                projection_item("duplicate", "owner-a", "001"),
                projection_item("duplicate", "owner-b", "002"),
            ],
        },
        ProjectionItemCommit::Delta {
            inserts: vec![projection_item("same", "owner-a", "001")],
            updates: vec![],
            deletes: vec!["same".to_owned()],
        },
        ProjectionItemCommit::Delta {
            inserts: vec![],
            updates: vec![],
            deletes: vec!["delete".to_owned(), "delete".to_owned()],
        },
        ProjectionItemCommit::Delta {
            inserts: vec![projection_item("same", "owner-a", "001")],
            updates: vec![projection_item("same", "owner-b", "002")],
            deletes: vec![],
        },
    ];
    for invalid in invalid_commits {
        assert!(
            store
                .commit_projection_items(
                    &projection,
                    &serde_json::json!({"generation": 999}),
                    &invalid,
                )
                .is_err()
        );
        assert_eq!(
            store.projection_records(&projection).unwrap(),
            Some(original_manifest.clone())
        );
        assert_eq!(
            store
                .projection_items_by_owner(&projection, "owner-a")
                .unwrap(),
            vec![original_item.clone()]
        );
    }
    assert!(store.projection_items_by_owner(&projection, " ").is_err());

    let _ = fs::remove_dir_all(tmp);
}

#[test]
fn projection_item_delta_precondition_failures_roll_back_manifest_and_items() {
    let tmp = std::env::temp_dir().join(format!("lethe-test-{}", uuid::Uuid::now_v7()));
    let db = tmp.join("test.sqlite3");
    let blob_dir = tmp.join("blobs");
    let projection = lethe_core::domain::ProjectionRef::new("proj:item-preconditions");
    let store = SqlitePersistence::open(&db, &blob_dir, &[7; 32]).unwrap();
    let original_manifest = serde_json::json!({"generation": 1});
    let original_item = projection_item("item-a", "owner-a", "001");
    store
        .commit_projection_items(
            &projection,
            &original_manifest,
            &ProjectionItemCommit::Replace {
                items: vec![original_item.clone()],
            },
        )
        .unwrap();

    let cases = [
        (
            ProjectionItemCommit::Delta {
                inserts: vec![
                    projection_item("item-new", "owner-a", "002"),
                    projection_item("item-a", "owner-a", "003"),
                ],
                updates: vec![],
                deletes: vec![],
            },
            "delta insert requires absent item_key item-a",
        ),
        (
            ProjectionItemCommit::Delta {
                inserts: vec![],
                updates: vec![
                    projection_item("item-a", "owner-b", "004"),
                    projection_item("item-missing", "owner-a", "005"),
                ],
                deletes: vec![],
            },
            "delta update requires existing item_key item-missing",
        ),
        (
            ProjectionItemCommit::Delta {
                inserts: vec![],
                updates: vec![],
                deletes: vec!["item-a".to_owned(), "item-missing".to_owned()],
            },
            "delta delete requires existing item_key item-missing",
        ),
    ];

    for (commit, expected_message) in cases {
        let error = store
            .commit_projection_items(
                &projection,
                &serde_json::json!({"generation": 999}),
                &commit,
            )
            .unwrap_err();
        assert!(
            matches!(
                error,
                PersistenceError::SchemaInvariant(message)
                    if message.contains(expected_message)
            ),
            "unexpected delta precondition error"
        );
        assert_eq!(
            store.projection_records(&projection).unwrap(),
            Some(original_manifest.clone())
        );
        assert_eq!(
            store
                .projection_items_by_owner(&projection, "owner-a")
                .unwrap(),
            vec![original_item.clone()]
        );
        assert!(
            store
                .projection_items_by_owner(&projection, "owner-b")
                .unwrap()
                .is_empty()
        );
    }

    let _ = fs::remove_dir_all(tmp);
}

#[test]
fn projection_item_staging_pages_publish_atomically_and_consume_staging() {
    let tmp = std::env::temp_dir().join(format!("lethe-test-{}", uuid::Uuid::now_v7()));
    let db = tmp.join("test.sqlite3");
    let blob_dir = tmp.join("blobs");
    let target = lethe_core::domain::ProjectionRef::new("proj:item-publish-target");
    let staging = lethe_core::domain::ProjectionRef::new("proj:item-publish-staging");
    let store = SqlitePersistence::open(&db, &blob_dir, &[7; 32]).unwrap();
    store
        .commit_projection_items(
            &target,
            &serde_json::json!({"generation": 1}),
            &ProjectionItemCommit::Replace {
                items: vec![projection_item("old", "owner-old", "999")],
            },
        )
        .unwrap();
    store
        .commit_projection_items(
            &staging,
            &serde_json::json!({"page": 0}),
            &ProjectionItemCommit::Replace { items: vec![] },
        )
        .unwrap();
    store
        .commit_projection_items(
            &staging,
            &serde_json::json!({"page": 1}),
            &ProjectionItemCommit::Delta {
                inserts: vec![
                    projection_item("item-b", "owner-a", "002"),
                    projection_item("item-a", "owner-a", "001"),
                ],
                updates: vec![],
                deletes: vec![],
            },
        )
        .unwrap();
    store
        .commit_projection_items(
            &staging,
            &serde_json::json!({"page": 2}),
            &ProjectionItemCommit::Delta {
                inserts: vec![projection_item("item-c", "owner-b", "001")],
                updates: vec![],
                deletes: vec![],
            },
        )
        .unwrap();
    assert_eq!(
        store.projection_item_by_key(&staging, "item-b").unwrap(),
        Some(projection_item("item-b", "owner-a", "002"))
    );
    assert!(
        store
            .projection_item_by_key(&staging, "missing")
            .unwrap()
            .is_none()
    );
    assert!(store.projection_item_by_key(&staging, " ").is_err());

    let final_manifest = serde_json::json!({"generation": 2, "item_count": 3});
    store
        .publish_projection_items_from_staging(&target, &staging, &final_manifest, 3)
        .unwrap();

    assert_eq!(
        store.projection_records(&target).unwrap(),
        Some(final_manifest)
    );
    assert_eq!(
        store.projection_items_by_owner(&target, "owner-a").unwrap(),
        vec![
            projection_item("item-a", "owner-a", "001"),
            projection_item("item-b", "owner-a", "002"),
        ]
    );
    assert_eq!(
        store.projection_items_by_owner(&target, "owner-b").unwrap(),
        vec![projection_item("item-c", "owner-b", "001")]
    );
    assert_eq!(store.projection_item_count(&target).unwrap(), 3);
    assert_eq!(store.projection_records(&staging).unwrap(), None);
    assert_eq!(store.projection_item_count(&staging).unwrap(), 0);
    assert!(
        store
            .projection_item_by_key(&staging, "item-a")
            .unwrap()
            .is_none()
    );

    let _ = fs::remove_dir_all(tmp);
}

#[test]
fn projection_generation_publish_is_constant_size_and_cleanup_resumes_after_reopen() {
    let tmp = std::env::temp_dir().join(format!(
        "lethe-projection-generation-publish-{}",
        uuid::Uuid::now_v7()
    ));
    let db = tmp.join("test.sqlite3");
    let blob_dir = tmp.join("blobs");
    let target = lethe_core::domain::ProjectionRef::new("proj:generation-publish-target");
    let staging = lethe_core::domain::ProjectionRef::new("proj:generation-publish-staging");
    let store = SqlitePersistence::open(&db, &blob_dir, &[7; 32]).unwrap();
    let item_count = 5_000_usize;
    let old_items = (0..item_count)
        .map(|index| {
            projection_item(
                &format!("old-{index:05}"),
                "owner-old",
                &format!("{index:05}"),
            )
        })
        .collect::<Vec<_>>();
    let staged_items = (0..item_count)
        .map(|index| {
            projection_item(
                &format!("new-{index:05}"),
                "owner-new",
                &format!("{index:05}"),
            )
        })
        .collect::<Vec<_>>();
    store
        .commit_projection_items(
            &target,
            &serde_json::json!({"generation": 1}),
            &ProjectionItemCommit::Replace { items: old_items },
        )
        .unwrap();
    store
        .commit_projection_items(
            &staging,
            &serde_json::json!({"generation": 2}),
            &ProjectionItemCommit::Replace {
                items: staged_items,
            },
        )
        .unwrap();

    let changes_before_publish = store.conn.total_changes();
    let publish_started_at = std::time::Instant::now();
    store
        .publish_projection_items_from_staging(
            &target,
            &staging,
            &serde_json::json!({"generation": 2, "item_count": item_count}),
            item_count as u64,
        )
        .unwrap();
    let publish_elapsed = publish_started_at.elapsed();
    let publish_changes = store
        .conn
        .total_changes()
        .checked_sub(changes_before_publish)
        .unwrap();
    assert!(
        publish_changes < 32,
        "generation-head publish changed {publish_changes} rows for {item_count} items"
    );
    assert!(
        publish_elapsed < std::time::Duration::from_secs(2),
        "generation-head publish took {publish_elapsed:?} for {item_count} items"
    );
    assert_eq!(
        store.projection_item_count(&target).unwrap(),
        item_count as u64
    );
    assert_eq!(store.projection_item_count(&staging).unwrap(), 0);
    drop(store);

    let reopened = SqlitePersistence::open(&db, &blob_dir, &[7; 32]).unwrap();
    assert_eq!(
        reopened.projection_item_count(&target).unwrap(),
        item_count as u64,
        "the new generation must remain live before retired-row cleanup resumes"
    );
    let mut deleted_items = 0_u64;
    loop {
        let changes_before_page = reopened.conn.total_changes();
        let report = reopened.cleanup_retired_projection_generation(128).unwrap();
        let page_changes = reopened
            .conn
            .total_changes()
            .checked_sub(changes_before_page)
            .unwrap();
        assert!(report.deleted_items <= 128);
        assert!(report.deleted_visible_blob_refs <= 128);
        assert!(
            page_changes <= 258,
            "cleanup page changed {page_changes} rows"
        );
        deleted_items += report.deleted_items;
        if !report.has_more {
            break;
        }
    }
    assert_eq!(deleted_items, item_count as u64);
    assert_eq!(
        reopened.projection_item_count(&target).unwrap(),
        item_count as u64
    );
    assert_eq!(
        reopened
            .conn
            .query_row(
                "SELECT COUNT(*) FROM retired_projection_materializations",
                [],
                |row| row.get::<_, u64>(0),
            )
            .unwrap(),
        0
    );
    let small_target =
        lethe_core::domain::ProjectionRef::new("proj:generation-publish-small-target");
    let small_staging =
        lethe_core::domain::ProjectionRef::new("proj:generation-publish-small-staging");
    reopened
        .commit_projection_items(
            &small_target,
            &serde_json::json!({"generation": 1}),
            &ProjectionItemCommit::Replace {
                items: vec![projection_item("old", "owner-old", "00000")],
            },
        )
        .unwrap();
    reopened
        .commit_projection_items(
            &small_staging,
            &serde_json::json!({"generation": 2}),
            &ProjectionItemCommit::Replace {
                items: vec![projection_item("new", "owner-new", "00000")],
            },
        )
        .unwrap();
    let changes_before_small_publish = reopened.conn.total_changes();
    reopened
        .publish_projection_items_from_staging(
            &small_target,
            &small_staging,
            &serde_json::json!({"generation": 2, "item_count": 1}),
            1,
        )
        .unwrap();
    let small_publish_changes = reopened
        .conn
        .total_changes()
        .checked_sub(changes_before_small_publish)
        .unwrap();
    assert_eq!(
        publish_changes, small_publish_changes,
        "final publish mutations must be independent of projection item count"
    );

    let _ = fs::remove_dir_all(tmp);
}

#[test]
fn projection_item_staging_publish_preconditions_and_sql_failure_preserve_both_sides() {
    let tmp = std::env::temp_dir().join(format!("lethe-test-{}", uuid::Uuid::now_v7()));
    let db = tmp.join("test.sqlite3");
    let blob_dir = tmp.join("blobs");
    let target = lethe_core::domain::ProjectionRef::new("proj:item-publish-target");
    let staging = lethe_core::domain::ProjectionRef::new("proj:item-publish-staging");
    let missing = lethe_core::domain::ProjectionRef::new("proj:item-publish-missing");
    let store = SqlitePersistence::open(&db, &blob_dir, &[7; 32]).unwrap();
    let original_manifest = serde_json::json!({"generation": 1});
    let original_item = projection_item("old", "owner-old", "999");
    let staged_items = vec![
        projection_item("stage-a", "owner-a", "001"),
        projection_item("stage-b", "owner-a", "002"),
    ];
    store
        .commit_projection_items(
            &target,
            &original_manifest,
            &ProjectionItemCommit::Replace {
                items: vec![original_item.clone()],
            },
        )
        .unwrap();

    assert!(matches!(
        store.publish_projection_items_from_staging(
            &target,
            &target,
            &serde_json::json!({"generation": 2}),
            1,
        ),
        Err(PersistenceError::SchemaInvariant(message))
            if message.contains("must differ")
    ));
    assert!(matches!(
        store.publish_projection_items_from_staging(
            &target,
            &missing,
            &serde_json::json!({"generation": 2}),
            0,
        ),
        Err(PersistenceError::SchemaInvariant(message))
            if message.contains("does not exist")
    ));

    store
        .commit_projection_items(
            &staging,
            &serde_json::json!({"page": 1}),
            &ProjectionItemCommit::Replace {
                items: staged_items.clone(),
            },
        )
        .unwrap();
    assert!(matches!(
        store.publish_projection_items_from_staging(
            &target,
            &staging,
            &serde_json::json!({"generation": 2}),
            3,
        ),
        Err(PersistenceError::SchemaInvariant(message))
            if message.contains("contains 2 items, expected 3")
    ));
    assert_eq!(
        store.projection_records(&target).unwrap(),
        Some(original_manifest.clone())
    );
    assert_eq!(
        store
            .projection_items_by_owner(&target, "owner-old")
            .unwrap(),
        vec![original_item.clone()]
    );
    assert_eq!(store.projection_item_count(&staging).unwrap(), 2);

    store
        .conn
        .execute_batch(
            "CREATE TRIGGER reject_staging_publish
             BEFORE UPDATE OF storage_projection_id ON projection_materialization_heads
             WHEN OLD.projection_id = 'proj:item-publish-target'
             BEGIN
                 SELECT RAISE(ABORT, 'forced staging publish failure');
             END;",
        )
        .unwrap();
    assert!(matches!(
        store.publish_projection_items_from_staging(
            &target,
            &staging,
            &serde_json::json!({"generation": 2}),
            2,
        ),
        Err(PersistenceError::Sqlite(_))
    ));
    assert_eq!(
        store.projection_records(&target).unwrap(),
        Some(original_manifest)
    );
    assert_eq!(
        store
            .projection_items_by_owner(&target, "owner-old")
            .unwrap(),
        vec![original_item]
    );
    assert_eq!(
        store
            .projection_items_by_owner(&staging, "owner-a")
            .unwrap(),
        staged_items
    );
    assert_eq!(
        store.projection_records(&staging).unwrap(),
        Some(serde_json::json!({"page": 1}))
    );

    let _ = fs::remove_dir_all(tmp);
}

#[test]
fn projection_item_sql_failure_rolls_back_manifest_and_all_mutations() {
    let tmp = std::env::temp_dir().join(format!("lethe-test-{}", uuid::Uuid::now_v7()));
    let db = tmp.join("test.sqlite3");
    let blob_dir = tmp.join("blobs");
    let projection = lethe_core::domain::ProjectionRef::new("proj:item-rollback");
    let store = SqlitePersistence::open(&db, &blob_dir, &[7; 32]).unwrap();
    let original_manifest = serde_json::json!({"generation": 1});
    let original_item = projection_item("item-a", "owner-a", "001");
    store
        .commit_projection_items(
            &projection,
            &original_manifest,
            &ProjectionItemCommit::Replace {
                items: vec![original_item.clone()],
            },
        )
        .unwrap();
    store
        .conn
        .execute_batch(
            "CREATE TRIGGER reject_projection_item
             BEFORE INSERT ON projection_materialization_items
             WHEN NEW.item_key = 'item-fail'
             BEGIN
                 SELECT RAISE(ABORT, 'forced projection item failure');
             END;",
        )
        .unwrap();

    let result = store.commit_projection_items(
        &projection,
        &serde_json::json!({"generation": 2}),
        &ProjectionItemCommit::Delta {
            inserts: vec![
                projection_item("item-before-failure", "owner-a", "002"),
                projection_item("item-fail", "owner-a", "003"),
            ],
            updates: vec![],
            deletes: vec!["item-a".to_owned()],
        },
    );
    assert!(matches!(result, Err(PersistenceError::Sqlite(_))));
    assert_eq!(
        store.projection_records(&projection).unwrap(),
        Some(original_manifest)
    );
    assert_eq!(
        store
            .projection_items_by_owner(&projection, "owner-a")
            .unwrap(),
        vec![original_item]
    );

    let _ = fs::remove_dir_all(tmp);
}

#[test]
fn open_records_partition_initialize_with_pinned_keyspecs() {
    let tmp = std::env::temp_dir().join(format!("lethe-test-{}", uuid::Uuid::now_v7()));
    let db = tmp.join("test.sqlite3");
    let blob_dir = tmp.join("blobs");
    let store = SqlitePersistence::open(&db, &blob_dir, &[7; 32]).unwrap();

    let (event_type, leaf_id, routing, identity): (String, String, String, String) = store
        .conn
        .query_row(
            "SELECT event_type, leaf_id, routing_keyspec_json, identity_keyspec_json
                 FROM partition_log
                 WHERE event_type = 'initialize'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();

    assert_eq!(event_type, "initialize");
    assert!(leaf_id.starts_with("lake:"));
    assert_eq!(
        routing,
        lethe_runtime::runtime::partition::routing_keyspec_json().unwrap()
    );
    assert_eq!(
        identity,
        lethe_runtime::runtime::partition::identity_keyspec_json().unwrap()
    );

    let _ = fs::remove_dir_all(tmp);
}

#[test]
fn open_with_personal_routing_records_year_month_keyspec() {
    let tmp = std::env::temp_dir().join(format!("lethe-test-{}", uuid::Uuid::now_v7()));
    let db = tmp.join("test.sqlite3");
    let blob_dir = tmp.join("blobs");
    let store = SqlitePersistence::open_with_routing_key_order(
        &db,
        &blob_dir,
        &[7; 32],
        RoutingKeyOrder::YearMonthSourceContainerPublished,
    )
    .unwrap();

    let routing: String = store
        .conn
        .query_row(
            "SELECT routing_keyspec_json FROM partition_log WHERE event_type = 'initialize'",
            [],
            |row| row.get(0),
        )
        .unwrap();

    assert_eq!(
        routing,
        routing_keyspec_json_for_order(RoutingKeyOrder::YearMonthSourceContainerPublished).unwrap()
    );

    let _ = fs::remove_dir_all(tmp);
}

#[test]
fn opening_existing_db_with_different_routing_keyspec_fails_fast() {
    let tmp = std::env::temp_dir().join(format!("lethe-test-{}", uuid::Uuid::now_v7()));
    let db = tmp.join("test.sqlite3");
    let blob_dir = tmp.join("blobs");
    SqlitePersistence::open_with_routing_key_order(
        &db,
        &blob_dir,
        &[7; 32],
        RoutingKeyOrder::MonthYearSourceContainerPublished,
    )
    .unwrap();

    let err = match SqlitePersistence::open_with_routing_key_order(
        &db,
        &blob_dir,
        &[7; 32],
        RoutingKeyOrder::YearMonthSourceContainerPublished,
    ) {
        Ok(_) => panic!("expected keyspec mismatch"),
        Err(err) => err,
    };

    assert!(matches!(err, PersistenceError::SchemaInvariant(_)));

    let _ = fs::remove_dir_all(tmp);
}

#[test]
fn split_prepare_is_logged_without_changing_replayed_tree() {
    let tmp = std::env::temp_dir().join(format!("lethe-test-{}", uuid::Uuid::now_v7()));
    let db = tmp.join("test.sqlite3");
    let blob_dir = tmp.join("blobs");
    let store = SqlitePersistence::open(&db, &blob_dir, &[7; 32]).unwrap();
    let root: String = store
        .conn
        .query_row(
            "SELECT leaf_id FROM partition_log WHERE event_type = 'initialize'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let left = format!("lake:{}", uuid::Uuid::now_v7());
    let right = format!("lake:{}", uuid::Uuid::now_v7());

    let seq = store.append_split_prepare(&root, &left, &right).unwrap();
    let tree = store.load_partition_tree().unwrap();

    assert_eq!(seq, 2);
    assert_eq!(tree.current_leaf_ids(), vec![root]);

    let _ = fs::remove_dir_all(tmp);
}

#[test]
fn split_commit_records_capacity_bit_and_replays_tree() {
    let tmp = std::env::temp_dir().join(format!("lethe-test-{}", uuid::Uuid::now_v7()));
    let db = tmp.join("test.sqlite3");
    let blob_dir = tmp.join("blobs");
    let store = SqlitePersistence::open(&db, &blob_dir, &[7; 32]).unwrap();
    let root: String = store
        .conn
        .query_row(
            "SELECT leaf_id FROM partition_log WHERE event_type = 'initialize'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let left = format!("lake:{}", uuid::Uuid::now_v7());
    let right = format!("lake:{}", uuid::Uuid::now_v7());

    let seq = store.append_split_commit(&root, &left, &right, 2).unwrap();
    let (bit_index, reason): (i64, String) = store
        .conn
        .query_row(
            "SELECT bit_index, reason FROM partition_log WHERE event_seq = ?1",
            [seq],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    let tree = store.load_partition_tree().unwrap();

    assert_eq!(bit_index, 2);
    assert_eq!(reason, "capacity");
    assert_eq!(tree.current_leaf_ids(), vec![left, right]);

    let _ = fs::remove_dir_all(tmp);
}

#[test]
fn failover_and_recover_events_record_control_plane_boundaries() {
    let tmp = std::env::temp_dir().join(format!("lethe-test-{}", uuid::Uuid::now_v7()));
    let db = tmp.join("test.sqlite3");
    let blob_dir = tmp.join("blobs");
    let store = SqlitePersistence::open(&db, &blob_dir, &[7; 32]).unwrap();
    let root: String = store
        .conn
        .query_row(
            "SELECT leaf_id FROM partition_log WHERE event_type = 'initialize'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let failover_id = format!("spool:{}", uuid::Uuid::now_v7());

    let failover_seq = store.append_failover(&root, &failover_id).unwrap();
    let recover_seq = store.append_recover(&root, &failover_id).unwrap();
    let events = store
        .conn
        .prepare("SELECT event_seq, event_type FROM partition_log ORDER BY event_seq")
        .unwrap()
        .query_map([], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(failover_seq, 2);
    assert_eq!(recover_seq, 3);
    assert_eq!(events[1].1, "failover");
    assert_eq!(events[2].1, "recover");

    let _ = fs::remove_dir_all(tmp);
}

#[test]
fn partition_log_is_append_only() {
    let tmp = std::env::temp_dir().join(format!("lethe-test-{}", uuid::Uuid::now_v7()));
    let db = tmp.join("test.sqlite3");
    let blob_dir = tmp.join("blobs");
    let store = SqlitePersistence::open(&db, &blob_dir, &[7; 32]).unwrap();

    let err = store
        .conn
        .execute(
            "UPDATE partition_log SET event_json = '{}' WHERE event_type = 'initialize'",
            [],
        )
        .unwrap_err();
    assert!(matches!(err, rusqlite::Error::SqliteFailure(_, _)));

    let _ = fs::remove_dir_all(tmp);
}

#[test]
fn garbage_collect_orphan_blobs_removes_unreferenced_files() {
    let tmp = std::env::temp_dir().join(format!("lethe-test-{}", uuid::Uuid::now_v7()));
    let db = tmp.join("test.sqlite3");
    let blob_dir = tmp.join("blobs");
    let store = SqlitePersistence::open(&db, &blob_dir, &[7; 32]).unwrap();
    let orphan = blob_dir.join("f".repeat(64));
    fs::write(&orphan, b"orphan").unwrap();

    let removed = store.garbage_collect_orphan_blobs().unwrap();
    assert_eq!(removed, 1);
    assert!(!orphan.exists());

    let _ = fs::remove_dir_all(tmp);
}

#[test]
fn sqlite_implements_storage_port_conformance_suite() {
    let tmp = std::env::temp_dir().join(format!("lethe-test-{}", uuid::Uuid::now_v7()));
    let store =
        SqlitePersistence::open(&tmp.join("test.sqlite3"), &tmp.join("blobs"), &[7; 32]).unwrap();

    lethe_storage_api::conformance::observation_store_round_trip(&store);
    lethe_storage_api::conformance::blob_store_round_trip(&store);
    lethe_storage_api::conformance::materializer_round_trip(&store);

    let _ = fs::remove_dir_all(tmp);
}

#[test]
fn identity_bridge_projection_is_incremental_idempotent_and_resumable() {
    let tmp = std::env::temp_dir().join(format!("lethe-test-{}", uuid::Uuid::now_v7()));
    let store =
        SqlitePersistence::open(&tmp.join("test.sqlite3"), &tmp.join("blobs"), &[7; 32]).unwrap();
    let canonical_json = serde_json::json!({"body": "one"}).to_string();
    let v1 = bridge_observation(
        "unit-a",
        "object-1",
        &canonical_json,
        "unit-a:legacy:object-1",
    );
    store
        .append_observations_v1_with_admission("unit-a", None, std::slice::from_ref(&v1), &[])
        .unwrap();

    let first = store.identity_bridge_apply_batch(16).unwrap();
    assert_eq!(first.read_count, 1);
    assert_eq!(first.candidate_count, 1);
    assert_eq!(first.gap_count, 0);
    assert_eq!(store.identity_bridge_watermark().unwrap(), 1);

    let retry = store.identity_bridge_apply_batch(16).unwrap();
    assert_eq!(retry.read_count, 0);
    let candidate_count: u64 = store
        .conn
        .query_row(
            "SELECT COUNT(*) FROM identity_bridge_candidates",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(candidate_count, 1);

    let second_json = serde_json::json!({"body": "two"}).to_string();
    let second = bridge_observation("unit-a", "object-2", &second_json, "unit-a:legacy:object-2");
    store
        .append_observations_v1_with_admission("unit-a", None, &[second], &[])
        .unwrap();
    let tail = store.identity_bridge_apply_batch(16).unwrap();
    assert_eq!(tail.read_count, 1);
    assert_eq!(tail.previous_watermark, 1);
    assert_eq!(tail.watermark, 2);

    let resolution = store
        .identity_bridge_resolve(
            &bridge_identity("unit-a", "object-1", &canonical_json),
            &canonical_json,
        )
        .unwrap();
    assert_eq!(resolution.winner, Some(v1.id));
    assert_eq!(resolution.multiplicity, 1);
    assert!(!resolution.canonical_collision);

    let _ = fs::remove_dir_all(tmp);
}

#[test]
fn legacy_history_message_is_resolved_by_v2_operational_append_without_ledger_delta() {
    let tmp = std::env::temp_dir().join(format!("lethe-test-{}", uuid::Uuid::now_v7()));
    let database = tmp.join("test.sqlite3");
    let blob_dir = tmp.join("blobs");
    let data_space_id = DataSpaceId::new("space:personal");
    let store =
        SqliteOperationalEventStore::open(data_space_id.clone(), &database, &blob_dir, &[7; 32])
            .unwrap();
    let source_instance_id = "codex-personal";
    let source_session_id = "session-1";
    let source_message_id = "message-1";
    let object_id = format!("{source_session_id}:{source_message_id}");
    let canonical_json = serde_json::json!({
        "source_instance_id": source_instance_id,
        "source_session_id": source_session_id,
        "source_message_id": source_message_id,
        "text": "hello"
    })
    .to_string();
    let legacy = legacy_history_observation(
        source_instance_id,
        source_session_id,
        &canonical_json,
        "history-message:legacy",
    );
    let mut legacy = legacy;
    legacy.meta.as_object_mut().unwrap().insert(
        "data_space_id".to_owned(),
        serde_json::json!("space:personal"),
    );
    let event_id = "event:history-message:legacy";
    let stream_id = "history-message:legacy";
    store
        .append_operational_event(&history_event_request(
            &data_space_id,
            event_id,
            stream_id,
            legacy.clone(),
        ))
        .unwrap();

    let mut v2 = legacy.clone();
    v2.id = Observation::new_id();
    v2.idempotency_key = IdempotencyKey::new(history_identity(
        source_instance_id,
        &object_id,
        &canonical_json,
    ));
    v2.meta = serde_json::json!({
        "source_instance": source_instance_id,
        "object_id": object_id,
        CANONICAL_JSON_META_KEY: canonical_json,
        "source_container": source_session_id,
        "data_space_id": "space:personal",
    });
    let outcome = store
        .append_operational_events_v2_with_bridge(
            source_instance_id,
            None,
            &[history_event_request(
                &data_space_id,
                event_id,
                stream_id,
                v2,
            )],
        )
        .unwrap();
    assert!(matches!(
        outcome.as_slice(),
        [lethe_storage_api::OperationalAppendOutcome::Duplicate { .. }]
    ));
    assert_eq!(store.persistence().observation_stats().unwrap().count, 1);
    assert_eq!(store.operational_event_stats().unwrap().count, 1);
    assert_eq!(store.persistence().identity_bridge_watermark().unwrap(), 1);

    store
        .persistence()
        .cutover_register(source_instance_id, "owner:test", "history admission")
        .unwrap();
    store
        .persistence()
        .cutover_begin_drain(source_instance_id, "owner:test", "history fence")
        .unwrap();
    let fixture = CutoverFixture {
        object_id: object_id.clone(),
        canonical_json: canonical_json.clone(),
        expected_identity_key: history_identity(source_instance_id, &object_id, &canonical_json),
        expected_observation_id: Some(legacy.id.clone()),
    };
    let active = store
        .persistence()
        .cutover_activate(
            source_instance_id,
            "owner:test",
            "history activate",
            &fixture,
        )
        .unwrap();
    let new_object_id = "session-1:message-2";
    let new_canonical_json = serde_json::json!({
        "source_instance_id": source_instance_id,
        "source_session_id": source_session_id,
        "source_message_id": "message-2",
        "text": "new"
    })
    .to_string();
    let mut new_v2 = legacy.clone();
    new_v2.id = Observation::new_id();
    new_v2.idempotency_key = IdempotencyKey::new(history_identity(
        source_instance_id,
        new_object_id,
        &new_canonical_json,
    ));
    new_v2.meta = serde_json::json!({
        "source_instance": source_instance_id,
        "object_id": new_object_id,
        CANONICAL_JSON_META_KEY: new_canonical_json,
        "source_container": source_session_id,
        "data_space_id": "space:personal",
    });
    let new_request = history_event_request(
        &data_space_id,
        "event:history-message:new",
        "history-message:new",
        new_v2,
    );
    let missing_generation = store.append_operational_events_v2_with_bridge(
        source_instance_id,
        None,
        std::slice::from_ref(&new_request),
    );
    assert!(
        matches!(
            &missing_generation,
            Err(lethe_storage_api::StorageError::CutoverAdmissionDenied(_))
        ),
        "unexpected missing-generation result: {missing_generation:?}"
    );
    let stale_generation = store.append_operational_events_v2_with_bridge(
        source_instance_id,
        Some(active.generation + 1),
        std::slice::from_ref(&new_request),
    );
    assert!(matches!(
        stale_generation,
        Err(lethe_storage_api::StorageError::CutoverAdmissionDenied(_))
    ));
    store
        .append_operational_events_v2_with_bridge(
            source_instance_id,
            Some(active.generation),
            std::slice::from_ref(&new_request),
        )
        .unwrap();
    assert_eq!(
        store
            .persistence()
            .cutover_state(source_instance_id)
            .unwrap()
            .phase,
        CutoverPhase::V2Committed
    );
    let committed_missing_generation = store.append_operational_events_v2_with_bridge(
        source_instance_id,
        None,
        std::slice::from_ref(&new_request),
    );
    assert!(matches!(
        committed_missing_generation,
        Err(lethe_storage_api::StorageError::CutoverAdmissionDenied(_))
    ));

    let _ = fs::remove_dir_all(tmp);
}

#[test]
fn v2_operational_append_is_denied_for_v1_active_and_draining_units() {
    let tmp = std::env::temp_dir().join(format!("lethe-test-{}", uuid::Uuid::now_v7()));
    let data_space_id = DataSpaceId::new("space:cutover-admission");
    let store = SqliteOperationalEventStore::open(
        data_space_id.clone(),
        &tmp.join("test.sqlite3"),
        &tmp.join("blobs"),
        &[7; 32],
    )
    .unwrap();
    let source_instance_id = "cutover-admission-unit";
    let canonical_json = serde_json::json!({"body": "must not append"}).to_string();
    let mut observation = bridge_observation(
        source_instance_id,
        "object-1",
        &canonical_json,
        &bridge_identity(source_instance_id, "object-1", &canonical_json),
    );
    observation.meta.as_object_mut().unwrap().insert(
        "data_space_id".to_owned(),
        serde_json::json!(data_space_id.as_str()),
    );
    let request = history_event_request(
        &data_space_id,
        "event:cutover-admission",
        "cutover-admission-stream",
        observation,
    );

    let registered = store
        .persistence()
        .cutover_register(source_instance_id, "owner:test", "register")
        .unwrap();
    assert_eq!(registered.phase, lethe_storage_api::CutoverPhase::V1Active);
    assert_eq!(registered.generation, 1);
    let v1_active = store.append_operational_events_v2_with_bridge(
        source_instance_id,
        Some(registered.generation),
        std::slice::from_ref(&request),
    );
    assert!(
        matches!(
            &v1_active,
            Err(lethe_storage_api::StorageError::CutoverAdmissionDenied(reason))
                if reason.contains("v1_active") && reason.contains("not admitting v2")
        ),
        "unexpected v1_active admission result: {v1_active:?}"
    );

    let draining = store
        .persistence()
        .cutover_begin_drain(source_instance_id, "owner:test", "drain")
        .unwrap();
    assert_eq!(draining.phase, lethe_storage_api::CutoverPhase::Draining);
    assert_eq!(draining.generation, 1);
    let draining_result = store.append_operational_events_v2_with_bridge(
        source_instance_id,
        Some(draining.generation),
        std::slice::from_ref(&request),
    );
    assert!(
        matches!(
            &draining_result,
            Err(lethe_storage_api::StorageError::CutoverAdmissionDenied(reason))
                if reason.contains("draining") && reason.contains("not admitting v2")
        ),
        "unexpected draining admission result: {draining_result:?}"
    );
    assert_eq!(store.operational_event_stats().unwrap().count, 0);

    let _ = fs::remove_dir_all(tmp);
}

#[test]
fn v2_bridge_duplicate_preserves_ledger_delta_and_canonical_collision() {
    let tmp = std::env::temp_dir().join(format!("lethe-test-{}", uuid::Uuid::now_v7()));
    let store =
        SqlitePersistence::open(&tmp.join("test.sqlite3"), &tmp.join("blobs"), &[7; 32]).unwrap();
    let canonical_json = serde_json::json!({"body": "same"}).to_string();
    let v1 = bridge_observation(
        "unit-a",
        "object-1",
        &canonical_json,
        "unit-a:legacy:object-1",
    );
    store
        .append_observations_v1_with_admission("unit-a", None, std::slice::from_ref(&v1), &[])
        .unwrap();
    store.identity_bridge_apply_batch(16).unwrap();

    let v2_identity = bridge_identity("unit-a", "object-1", &canonical_json);
    let v2 = bridge_observation("unit-a", "object-1", &canonical_json, &v2_identity);
    let outcomes = store
        .append_observations_v2_with_bridge("unit-a", None, &[v2], &[])
        .unwrap();
    assert_eq!(
        outcomes,
        vec![lethe_storage_api::AppendOutcome::Duplicate(v1.id.clone())]
    );
    assert_eq!(store.observation_stats().unwrap().count, 1);

    let mismatch_json = serde_json::json!({"body": "different"}).to_string();
    store
        .conn
        .execute(
            "INSERT INTO identity_bridge_candidates (
                v2_identity_key, observation_id, source_instance_id, append_seq,
                canonical_json, canonical_json_sha256
             ) VALUES (?1, 'legacy-collision', 'unit-a', 999, ?2, ?3)",
            params![
                v2_identity,
                mismatch_json,
                canonical_json_sha256(&mismatch_json)
            ],
        )
        .unwrap();
    let mismatch_outcome = store
        .append_observations_v2_with_bridge(
            "unit-a",
            None,
            &[bridge_observation(
                "unit-a",
                "object-1",
                &canonical_json,
                &bridge_identity("unit-a", "object-1", &canonical_json),
            )],
            &[],
        )
        .unwrap();
    assert!(matches!(
        mismatch_outcome.as_slice(),
        [lethe_storage_api::AppendOutcome::CanonicalCollision(_)]
    ));
    assert_eq!(store.observation_stats().unwrap().count, 1);

    let _ = fs::remove_dir_all(tmp);
}

#[test]
fn cutover_fence_activation_generation_and_rollback_boundary_are_durable() {
    let tmp = std::env::temp_dir().join(format!("lethe-test-{}", uuid::Uuid::now_v7()));
    let store =
        SqlitePersistence::open(&tmp.join("test.sqlite3"), &tmp.join("blobs"), &[7; 32]).unwrap();
    let canonical_json = serde_json::json!({"body": "canary"}).to_string();
    let v1 = bridge_observation(
        "unit-a",
        "object-1",
        &canonical_json,
        "unit-a:legacy:object-1",
    );
    store
        .append_observations_v1_with_admission("unit-a", None, std::slice::from_ref(&v1), &[])
        .unwrap();
    store.identity_bridge_apply_batch(16).unwrap();
    store
        .cutover_register("unit-a", "owner:test", "register")
        .unwrap();
    let draining = store
        .cutover_begin_drain("unit-a", "owner:test", "fence")
        .unwrap();
    assert_eq!(draining.phase, lethe_storage_api::CutoverPhase::Draining);
    assert_eq!(draining.fence_append_seq, Some(1));

    let fixture = lethe_storage_api::CutoverFixture {
        object_id: "object-1".to_owned(),
        canonical_json: canonical_json.clone(),
        expected_identity_key: bridge_identity("unit-a", "object-1", &canonical_json),
        expected_observation_id: Some(v1.id.clone()),
    };
    let readiness = store.cutover_readiness("unit-a", Some(&fixture)).unwrap();
    assert!(readiness.ready, "{readiness:?}");
    let active = store
        .cutover_activate("unit-a", "owner:test", "activate", &fixture)
        .unwrap();
    assert_eq!(active.phase, lethe_storage_api::CutoverPhase::V2Active);
    assert_eq!(active.generation, 2);

    let stale = store.cutover_admit("unit-a", lethe_storage_api::CutoverApiVersion::V1, Some(1));
    assert!(matches!(
        stale,
        Err(lethe_storage_api::StorageError::CutoverAdmissionDenied(_))
    ));
    assert!(
        store
            .cutover_admit("unit-a", lethe_storage_api::CutoverApiVersion::V2, Some(2),)
            .is_ok()
    );
    store
        .cutover_register("unit-b", "owner:test", "register independent unit")
        .unwrap();
    assert!(
        store
            .cutover_admit("unit-b", lethe_storage_api::CutoverApiVersion::V1, Some(1),)
            .is_ok()
    );
    assert!(matches!(
        store.cutover_admit("unit-a", lethe_storage_api::CutoverApiVersion::V1, Some(2)),
        Err(lethe_storage_api::StorageError::CutoverAdmissionDenied(_))
    ));

    let new_json = serde_json::json!({"body": "new"}).to_string();
    let new_identity = bridge_identity("unit-a", "object-2", &new_json);
    let new_observation = bridge_observation("unit-a", "object-2", &new_json, &new_identity);
    let outcomes = store
        .append_observations_v2_with_bridge("unit-a", Some(2), &[new_observation], &[])
        .unwrap();
    assert!(matches!(
        outcomes.as_slice(),
        [lethe_storage_api::AppendOutcome::Appended(_)]
    ));
    assert_eq!(
        store.cutover_state("unit-a").unwrap().phase,
        lethe_storage_api::CutoverPhase::V2Committed
    );
    assert_eq!(store.identity_bridge_apply_batch(16).unwrap().read_count, 1);
    let v2_resolution = store
        .identity_bridge_resolve(&new_identity, &new_json)
        .unwrap();
    assert_eq!(v2_resolution.winner, None);
    assert_eq!(v2_resolution.multiplicity, 0);
    let rollback = store.cutover_rollback("unit-a", "owner:test", "unsafe");
    assert!(
        matches!(rollback, Err(lethe_storage_api::StorageError::CutoverRollbackRefused(reason)) if reason.contains("forward-fix"))
    );

    let health = store.cutover_health("unit-a").unwrap();
    assert_eq!(health.bridge_duplicate_hit_count, 0);
    assert_eq!(health.stale_v1_rejection_count, 2);
    assert_eq!(health.state.v2_ingested, 1);

    let _ = fs::remove_dir_all(tmp);
}

#[test]
fn pre_commit_rollback_returns_to_v1_without_deleting_bridge_state() {
    let tmp = std::env::temp_dir().join(format!("lethe-test-{}", uuid::Uuid::now_v7()));
    let store =
        SqlitePersistence::open(&tmp.join("test.sqlite3"), &tmp.join("blobs"), &[7; 32]).unwrap();
    store
        .cutover_register("unit-b", "owner:test", "register")
        .unwrap();
    store
        .cutover_begin_drain("unit-b", "owner:test", "fence")
        .unwrap();
    let state = store
        .cutover_rollback("unit-b", "owner:test", "pre-commit rollback")
        .unwrap();
    assert_eq!(state.phase, lethe_storage_api::CutoverPhase::V1Active);
    assert_eq!(state.generation, 2);
    assert_eq!(store.identity_bridge_watermark().unwrap(), 0);

    let _ = fs::remove_dir_all(tmp);
}

#[test]
fn cutover_state_fold_rejects_invalid_transition_log() {
    let tmp = std::env::temp_dir().join(format!("lethe-test-{}", uuid::Uuid::now_v7()));
    let store =
        SqlitePersistence::open(&tmp.join("test.sqlite3"), &tmp.join("blobs"), &[7; 32]).unwrap();
    store
        .cutover_register("invalid-unit", "owner:test", "register")
        .unwrap();
    store
        .conn
        .execute(
            "INSERT INTO cutover_transition_log (
                source_instance_id, from_phase, to_phase, authority, reason,
                generation, fence_append_seq, first_v2_append_seq, committed_at
             ) VALUES ('invalid-unit', 'v1_active', 'v2_active', 'owner:test',
                       'invalid direct activation', 2, NULL, NULL, ?1)",
            [chrono::Utc::now().to_rfc3339()],
        )
        .unwrap();
    assert!(matches!(
        store.cutover_state("invalid-unit"),
        Err(lethe_storage_api::StorageError::Invariant(reason))
            if reason.contains("invalid cutover transition")
    ));

    let _ = fs::remove_dir_all(tmp);
}

#[test]
fn schema_v15_upgrades_true_v13_cutover_shape() {
    let tmp = std::env::temp_dir().join(format!("lethe-test-{}", uuid::Uuid::now_v7()));
    let database_path = tmp.join("test.sqlite3");
    let blob_dir = tmp.join("blobs");
    {
        let store = SqlitePersistence::open(&database_path, &blob_dir, &[7; 32]).unwrap();
        store
            .conn
            .execute_batch(
                "
                DROP TRIGGER cutover_transition_log_no_update;
                DROP TRIGGER cutover_transition_log_no_delete;
                DROP INDEX cutover_credentials_active;
                DROP INDEX cutover_transition_unit_seq;
                DROP INDEX identity_bridge_gaps_source_append;
                DROP INDEX identity_bridge_candidates_source_append;
                DROP INDEX identity_bridge_candidates_key_append;
                DROP TABLE cutover_unit_metrics;
                DROP TABLE cutover_credentials;
                DROP TABLE cutover_transition_log;
                DROP TABLE identity_bridge_watermark;
                DROP TABLE identity_bridge_gaps;
                DROP TABLE identity_bridge_candidates;
                DROP TABLE retired_projection_materializations;
                DROP TABLE projection_materialization_heads;
                DELETE FROM schema_migrations WHERE version >= 14;
                ",
            )
            .unwrap();
    }
    let reopened = SqlitePersistence::open(&database_path, &blob_dir, &[7; 32]).unwrap();
    assert_eq!(
        reopened
            .conn
            .query_row(
                "SELECT name FROM schema_migrations WHERE version = 13",
                [],
                |row| row.get::<_, String>(0),
            )
            .unwrap(),
        "reconsent_privacy_reverse_index"
    );
    assert_eq!(
        reopened
            .conn
            .query_row(
                "SELECT name FROM schema_migrations WHERE version = 14",
                [],
                |row| row.get::<_, String>(0),
            )
            .unwrap(),
        "v1_v2_cutover_bridge"
    );
    assert_eq!(
        reopened
            .conn
            .query_row(
                "SELECT name FROM schema_migrations WHERE version = 15",
                [],
                |row| row.get::<_, String>(0),
            )
            .unwrap(),
        "atomic_projection_generation_head"
    );
    assert_eq!(reopened.identity_bridge_watermark().unwrap(), 0);

    let _ = fs::remove_dir_all(tmp);
}

#[test]
fn identity_bridge_persists_missing_metadata_as_a_gap_without_guessing() {
    let tmp = std::env::temp_dir().join(format!("lethe-test-{}", uuid::Uuid::now_v7()));
    let store =
        SqlitePersistence::open(&tmp.join("test.sqlite3"), &tmp.join("blobs"), &[7; 32]).unwrap();
    let observation = sample_observation();
    store.append_observation_idempotent(&observation).unwrap();
    let report = store.identity_bridge_apply_batch(8).unwrap();
    assert_eq!(report.read_count, 1);
    assert_eq!(report.candidate_count, 0);
    assert_eq!(report.gap_count, 1);
    let gap: (u64, String) = store
        .conn
        .query_row(
            "SELECT append_seq, reason FROM identity_bridge_gaps",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(gap.0, 1);
    assert!(gap.1.contains("source_instance"));
    assert_eq!(store.identity_bridge_apply_batch(8).unwrap().read_count, 0);

    let _ = fs::remove_dir_all(tmp);
}

#[test]
fn identity_bridge_batch_failure_does_not_advance_watermark() {
    let tmp = std::env::temp_dir().join(format!("lethe-test-{}", uuid::Uuid::now_v7()));
    let store =
        SqlitePersistence::open(&tmp.join("test.sqlite3"), &tmp.join("blobs"), &[7; 32]).unwrap();
    store
        .append_observations_v1_with_admission(
            "atomic-unit",
            None,
            &[bridge_observation(
                "atomic-unit",
                "object-1",
                &serde_json::json!({"body": "one"}).to_string(),
                "atomic-unit:legacy:1",
            )],
            &[],
        )
        .unwrap();

    assert!(
        store
            .identity_bridge_apply_batch_with_failure_for_test(8)
            .is_err()
    );
    assert_eq!(store.identity_bridge_watermark().unwrap(), 0);
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM identity_bridge_candidates",
                [],
                |row| row.get::<_, u64>(0),
            )
            .unwrap(),
        0
    );
    assert_eq!(store.identity_bridge_apply_batch(8).unwrap().read_count, 1);

    let _ = fs::remove_dir_all(tmp);
}

#[test]
fn identity_bridge_steady_state_reads_only_new_tail_after_bounded_bootstrap() {
    let tmp = std::env::temp_dir().join(format!("lethe-test-{}", uuid::Uuid::now_v7()));
    let store =
        SqlitePersistence::open(&tmp.join("test.sqlite3"), &tmp.join("blobs"), &[7; 32]).unwrap();
    let history = (0..1024)
        .map(|index| {
            let canonical_json = serde_json::json!({"body": index}).to_string();
            bridge_observation(
                "large-unit",
                &format!("object-{index}"),
                &canonical_json,
                &format!("large-unit:legacy:{index}"),
            )
        })
        .collect::<Vec<_>>();
    store
        .append_observations_v1_with_admission("large-unit", None, &history, &[])
        .unwrap();
    let mut processed = 0;
    loop {
        let report = store.identity_bridge_apply_batch(64).unwrap();
        processed += report.read_count;
        if report.read_count == 0 {
            break;
        }
    }
    assert_eq!(processed, 1024);
    let tail = (0..7)
        .map(|index| {
            let n = 1024 + index;
            let canonical_json = serde_json::json!({"body": n}).to_string();
            bridge_observation(
                "large-unit",
                &format!("object-{n}"),
                &canonical_json,
                &format!("large-unit:legacy:{n}"),
            )
        })
        .collect::<Vec<_>>();
    store
        .append_observations_v1_with_admission("large-unit", None, &tail, &[])
        .unwrap();
    let tail_report = store.identity_bridge_apply_batch(64).unwrap();
    assert_eq!(tail_report.previous_watermark, 1024);
    assert_eq!(tail_report.read_count, 7);
    assert_eq!(store.identity_bridge_apply_batch(64).unwrap().read_count, 0);

    let _ = fs::remove_dir_all(tmp);
}

#[test]
fn cutover_inventory_reports_shared_producers_credentials_and_renames() {
    let tmp = std::env::temp_dir().join(format!("lethe-test-{}", uuid::Uuid::now_v7()));
    let store =
        SqlitePersistence::open(&tmp.join("test.sqlite3"), &tmp.join("blobs"), &[7; 32]).unwrap();
    let canonical_one = serde_json::json!({"body": "one"}).to_string();
    let mut first = bridge_observation(
        "shared-unit",
        "object-1",
        &canonical_one,
        "shared-unit:legacy:1",
    );
    first.meta.as_object_mut().unwrap().extend([
        ("producer_id".to_owned(), serde_json::json!("producer-a")),
        (
            "credential_id".to_owned(),
            serde_json::json!("credential-shared"),
        ),
    ]);
    let canonical_two = serde_json::json!({"body": "two"}).to_string();
    let mut second = bridge_observation(
        "shared-unit",
        "object-2",
        &canonical_two,
        "shared-unit:legacy:2",
    );
    second.meta.as_object_mut().unwrap().extend([
        ("producer_id".to_owned(), serde_json::json!("producer-b")),
        (
            "credential_id".to_owned(),
            serde_json::json!("credential-shared"),
        ),
        (
            "source_instance_id".to_owned(),
            serde_json::json!("renamed-unit"),
        ),
    ]);
    store
        .append_observations_v1_with_admission("shared-unit", None, &[first, second], &[])
        .unwrap();
    let inventory = store.cutover_inventory().unwrap();
    let item = inventory
        .iter()
        .find(|item| item.source_instance_id == "shared-unit")
        .unwrap();
    assert_eq!(item.producer_ids, vec!["producer-a", "producer-b"]);
    assert_eq!(item.credential_ids, vec!["credential-shared"]);
    assert!(
        item.blockers
            .iter()
            .any(|blocker| blocker.contains("multiple producers"))
    );
    assert!(
        item.blockers
            .iter()
            .any(|blocker| blocker.contains("one credential reference"))
    );
    assert!(
        item.blockers
            .iter()
            .any(|blocker| blocker.contains("rename detected"))
    );

    let _ = fs::remove_dir_all(tmp);
}

#[test]
fn capacity_split_physically_rehomes_parent_rows_before_commit() {
    let tmp = std::env::temp_dir().join(format!("lethe-test-{}", uuid::Uuid::now_v7()));
    let store =
        SqlitePersistence::open(&tmp.join("test.sqlite3"), &tmp.join("blobs"), &[7; 32]).unwrap();
    let first = sample_observation();
    let mut second = sample_observation();
    second.id = Observation::new_id();
    second.idempotency_key = IdempotencyKey::new("sample-key-2");
    second.published += chrono::TimeDelta::days(35);
    second.meta = serde_json::json!({
        CANONICAL_JSON_META_KEY: serde_json::json!({
            "source": "test",
            "object_id": "sample-key-2",
            "body": "world"
        }).to_string(),
        "source_container": "test",
    });
    store.persist_observation(&first).unwrap();
    store.persist_observation(&second).unwrap();

    assert!(store.split_leaf_if_capacity(2).unwrap());
    let positions = store.leaf_positions().unwrap();
    assert_eq!(positions.len(), 2);
    assert_eq!(
        positions
            .iter()
            .map(|position| position.append_seq)
            .filter(|v| *v > 0)
            .count(),
        2
    );
    let tree = store.load_partition_tree().unwrap();
    assert_eq!(tree.current_leaf_ids().len(), 2);

    let _ = fs::remove_dir_all(tmp);
}

#[test]
fn projection_leaf_watermark_is_persistent_and_monotonic() {
    let tmp = std::env::temp_dir().join(format!("lethe-test-{}", uuid::Uuid::now_v7()));
    let store =
        SqlitePersistence::open(&tmp.join("test.sqlite3"), &tmp.join("blobs"), &[7; 32]).unwrap();
    let projection = lethe_core::domain::ProjectionRef::new("proj:test");
    let leaf = store
        .load_partition_tree()
        .unwrap()
        .root_leaf_id()
        .to_owned();
    let mut watermark = store.projection_leaf_watermark(&projection, &leaf).unwrap();
    assert_eq!(watermark.append_seq, 0);
    watermark.append_seq = 7;
    store.commit_projection_leaf_watermark(&watermark).unwrap();
    assert_eq!(
        store
            .projection_leaf_watermark(&projection, &leaf)
            .unwrap()
            .append_seq,
        7
    );

    watermark.append_seq = 6;
    assert!(store.commit_projection_leaf_watermark(&watermark).is_err());

    let _ = fs::remove_dir_all(tmp);
}

#[test]
fn blue_green_migration_rewrites_identity_and_retires_old_structure() {
    let tmp = std::env::temp_dir().join(format!("lethe-test-{}", uuid::Uuid::now_v7()));
    let store =
        SqlitePersistence::open(&tmp.join("test.sqlite3"), &tmp.join("blobs"), &[7; 32]).unwrap();
    let observation = sample_observation();
    let id = observation.id.clone();
    store.persist_observation(&observation).unwrap();

    store
        .blue_green_migrate("routing-keyspec/v2", "identity-keyspec/v2", |observation| {
            Ok(BlueGreenTransform {
                identity_key: IdempotencyKey::new(format!(
                    "v2:{}",
                    observation.idempotency_key.as_str()
                )),
                canonical_json: serde_json::json!({
                    "version": 2,
                    "id": observation.id.as_str(),
                })
                .to_string(),
                routing_key: "v2-routing-key".to_owned(),
            })
        })
        .unwrap();

    let stored = store.observation_by_id(&id).unwrap().unwrap();
    assert_eq!(stored.observation.id, id);
    assert_eq!(stored.observation.idempotency_key.as_str(), "v2:sample-key");
    let expected_canonical = serde_json::json!({"version": 2, "id": id.as_str()}).to_string();
    assert_eq!(
        stored
            .observation
            .meta
            .get(CANONICAL_JSON_META_KEY)
            .and_then(serde_json::Value::as_str),
        Some(expected_canonical.as_str())
    );
    let history_count: i64 = store
        .conn
        .query_row("SELECT COUNT(*) FROM keyspec_history", [], |row| row.get(0))
        .unwrap();
    assert_eq!(history_count, 1);
    let initialize_count: i64 = store
        .conn
        .query_row("SELECT COUNT(*) FROM partition_log", [], |row| row.get(0))
        .unwrap();
    assert_eq!(initialize_count, 1);

    let _ = fs::remove_dir_all(tmp);
}

#[test]
fn persisted_secrets_are_encrypted_at_rest() {
    let tmp = std::env::temp_dir().join(format!("lethe-test-{}", uuid::Uuid::now_v7()));
    let store =
        SqlitePersistence::open(&tmp.join("test.sqlite3"), &tmp.join("blobs"), &[7; 32]).unwrap();
    store
        .put_encrypted_secret("secret:google-refresh", b"plain-refresh-token")
        .unwrap();

    let stored_ciphertext: Vec<u8> = store
        .conn
        .query_row(
            "SELECT ciphertext FROM encrypted_secrets WHERE secret_ref = ?1",
            ["secret:google-refresh"],
            |row| row.get(0),
        )
        .unwrap();
    assert_ne!(stored_ciphertext, b"plain-refresh-token");
    assert_eq!(
        store.get_encrypted_secret("secret:google-refresh").unwrap(),
        Some(b"plain-refresh-token".to_vec())
    );

    let _ = fs::remove_dir_all(tmp);
}

#[test]
fn operational_event_store_conforms_and_pins_data_space() {
    let tmp = std::env::temp_dir().join(format!("lethe-test-{}", uuid::Uuid::now_v7()));
    let database_path = tmp.join("personal.sqlite3");
    let store = SqliteOperationalEventStore::open(
        lethe_core::domain::DataSpaceId::new("space:personal"),
        &database_path,
        &tmp.join("blobs"),
        &[7; 32],
    )
    .unwrap();
    lethe_storage_api::conformance::operational_event_store_round_trip(&store);
    lethe_storage_api::conformance::blob_store_round_trip(&store);

    drop(store);
    let mismatch = SqliteOperationalEventStore::open(
        lethe_core::domain::DataSpaceId::new("space:company"),
        &database_path,
        &tmp.join("blobs"),
        &[7; 32],
    );
    assert!(matches!(
        mismatch,
        Err(PersistenceError::SchemaInvariant(message))
            if message.contains("space:personal")
    ));
    let _ = fs::remove_dir_all(tmp);
}

#[test]
fn operational_filter_keyset_uses_correlation_index() {
    let tmp = std::env::temp_dir().join(format!("lethe-test-{}", uuid::Uuid::now_v7()));
    let data_space = lethe_core::domain::DataSpaceId::new("space:indexed");
    let store = SqliteOperationalEventStore::open(
        data_space.clone(),
        &tmp.join("operational.sqlite3"),
        &tmp.join("blobs"),
        &[7; 32],
    )
    .unwrap();
    let mut event = lethe_storage_api::conformance::sample_operational_event(
        &data_space,
        "event:indexed",
        "stream:indexed",
        1,
        "idempotency:indexed",
    );
    event.causation_id = Some(lethe_core::domain::OperationalEventId::new(
        "event:caused-by",
    ));
    let occurred_at = event.occurred_at;
    store
        .append_operational_event(&OperationalAppendRequest {
            expected_stream_version: 0,
            event,
        })
        .unwrap();
    let rows = store
        .operational_events_by_filter(
            &OperationalEventFilter {
                correlation_id: Some("correlation:conformance".to_owned()),
                ..Default::default()
            },
            0,
            10,
        )
        .unwrap();
    assert_eq!(rows.len(), 1);
    for filter in [
        OperationalEventFilter {
            causation_id: Some(lethe_core::domain::OperationalEventId::new(
                "event:caused-by",
            )),
            ..Default::default()
        },
        OperationalEventFilter {
            event_type: Some("work_item_created".to_owned()),
            ..Default::default()
        },
        OperationalEventFilter {
            stream_id: Some("stream:indexed".to_owned()),
            ..Default::default()
        },
        OperationalEventFilter {
            actor_id: Some("owner".to_owned()),
            occurred_at_from: Some(occurred_at - chrono::TimeDelta::seconds(1)),
            occurred_at_to: Some(occurred_at + chrono::TimeDelta::seconds(1)),
            ..Default::default()
        },
    ] {
        assert_eq!(
            store
                .operational_events_by_filter(&filter, 0, 10)
                .unwrap()
                .len(),
            1
        );
    }
    let mut statement = store
        .persistence()
        .conn
        .prepare(
            "EXPLAIN QUERY PLAN
             SELECT cursor FROM operational_events
             WHERE data_space_id = ?1 AND correlation_id = ?2 AND cursor > ?3
             ORDER BY cursor LIMIT ?4",
        )
        .unwrap();
    let plan = statement
        .query_map(
            rusqlite::params!["space:indexed", "correlation:conformance", 0, 10],
            |row| row.get::<_, String>(3),
        )
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert!(
        plan.iter()
            .any(|detail| detail.contains("operational_events_correlation_cursor")),
        "operational filter did not use correlation index: {plan:?}"
    );
    let _ = fs::remove_dir_all(tmp);
}

#[test]
fn blob_batch_prevalidates_limits_and_retries_after_atomic_index_failure() {
    let tmp = std::env::temp_dir().join(format!("lethe-test-{}", uuid::Uuid::now_v7()));
    let blob_dir = tmp.join("blobs");
    let store = SqliteOperationalEventStore::open(
        lethe_core::domain::DataSpaceId::new("space:personal"),
        &tmp.join("personal.sqlite3"),
        &blob_dir,
        &[7; 32],
    )
    .unwrap();

    let too_large = store.put_blobs(&[b"first", b"exceeds-limit"], 5);
    assert!(matches!(too_large, Err(StorageError::Invariant(_))));
    assert_eq!(fs::read_dir(&blob_dir).unwrap().count(), 0);
    assert_eq!(
        store
            .persistence()
            .conn
            .query_row("SELECT COUNT(*) FROM blobs", [], |row| row.get::<_, u64>(0))
            .unwrap(),
        0
    );

    let second_digest = hex::encode(sha2::Sha256::digest(b"second"));
    store
        .persistence()
        .conn
        .execute_batch(&format!(
            "CREATE TRIGGER reject_second_blob
             BEFORE INSERT ON blobs
             WHEN NEW.blob_ref = 'blob:sha256:{second_digest}'
             BEGIN
                 SELECT RAISE(ABORT, 'injected batch index failure');
             END;"
        ))
        .unwrap();
    assert!(store.put_blobs(&[b"first", b"second"], 1024).is_err());
    assert_eq!(
        store
            .persistence()
            .conn
            .query_row("SELECT COUNT(*) FROM blobs", [], |row| row.get::<_, u64>(0))
            .unwrap(),
        0
    );
    assert_eq!(fs::read_dir(&blob_dir).unwrap().count(), 2);

    store
        .persistence()
        .conn
        .execute_batch("DROP TRIGGER reject_second_blob")
        .unwrap();
    let blob_refs = store.put_blobs(&[b"first", b"second"], 1024).unwrap();
    assert_eq!(
        blob_refs,
        vec![
            BlobRef::new(format!(
                "blob:sha256:{}",
                hex::encode(sha2::Sha256::digest(b"first"))
            )),
            BlobRef::new(format!("blob:sha256:{second_digest}")),
        ]
    );
    assert_eq!(
        store
            .persistence()
            .conn
            .query_row("SELECT COUNT(*) FROM blobs", [], |row| row.get::<_, u64>(0))
            .unwrap(),
        2
    );
    assert_eq!(
        store.get_blob(&blob_refs[0]).unwrap(),
        Some(b"first".to_vec())
    );
    assert_eq!(
        store.get_blob(&blob_refs[1]).unwrap(),
        Some(b"second".to_vec())
    );

    let _ = fs::remove_dir_all(tmp);
}

#[test]
fn operational_archive_replays_into_an_empty_sqlite_lake() {
    let tmp = std::env::temp_dir().join(format!("lethe-test-{}", uuid::Uuid::now_v7()));
    let source = SqliteOperationalEventStore::open(
        lethe_core::domain::DataSpaceId::new("space:personal"),
        &tmp.join("source.sqlite3"),
        &tmp.join("source-blobs"),
        &[7; 32],
    )
    .unwrap();
    let event = lethe_storage_api::conformance::sample_operational_event(
        source.data_space_id(),
        "event:archive:1",
        "work:archive",
        1,
        "operational:archive:1",
    );
    source
        .append_operational_event(&OperationalAppendRequest {
            event,
            expected_stream_version: 0,
        })
        .unwrap();
    let blob_ref = source.put_blob(b"archived conversation", 1024).unwrap();
    let blob_manifest =
        lethe_storage_api::operational_blob_manifest(&source, std::slice::from_ref(&blob_ref))
            .unwrap();
    let archive =
        lethe_storage_api::export_operational_archive(&source, Utc::now(), blob_manifest).unwrap();
    let signed = lethe_storage_api::sign_operational_archive(&archive, b"archive-key").unwrap();
    let verified = lethe_storage_api::verify_operational_archive(&signed, b"archive-key").unwrap();

    let target = SqliteOperationalEventStore::open(
        lethe_core::domain::DataSpaceId::new("space:personal"),
        &tmp.join("target.sqlite3"),
        &tmp.join("target-blobs"),
        &[9; 32],
    )
    .unwrap();
    target.put_blob(b"archived conversation", 1024).unwrap();
    lethe_storage_api::verify_operational_archive_blobs(&target, &verified).unwrap();
    let outcomes = lethe_storage_api::replay_operational_archive(&target, &verified).unwrap();
    assert!(matches!(
        outcomes.as_slice(),
        [OperationalAppendOutcome::Appended {
            stream_version: 1,
            ..
        }]
    ));
    assert_eq!(target.operational_event_stats().unwrap().count, 1);
    let _ = fs::remove_dir_all(tmp);
}
