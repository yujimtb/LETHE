use super::*;
use chrono::Utc;

use lethe_core::domain::{
    ActorRef, AuthorityModel, CaptureModel, EntityRef, IdempotencyKey, Mutability, Observation,
    ObserverRef, SchemaRef, SemVer, SourceSystemRef, SupplementalId, SupplementalRecord,
    supplemental::InputAnchorSet,
};
use lethe_runtime::runtime::partition::{RoutingKeyOrder, routing_keyspec_json_for_order};

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
            "SELECT identity_key, canonical_json, observation_json FROM observations WHERE id = ?1",
            [observation.id.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    let stored = serde_json::from_str::<Observation>(&json).unwrap();

    assert_eq!(identity_key, new_key.as_str());
    assert_eq!(canonical_json, new_canonical_json);
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
    let version: i64 = store
        .conn
        .query_row(
            "SELECT version FROM schema_migrations WHERE version = ?1",
            [CURRENT_SCHEMA_VERSION],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(version, CURRENT_SCHEMA_VERSION);

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
